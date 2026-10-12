"""Append-only ledger for supporting-only conditional option research.

The ledger is intentionally independent from the production ranking and
authorization stores.  It freezes at most ten pre-market structures, then
records any number of market-open observations against those exact structures.
Every durable record participates in one global hash chain.
"""
from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import threading
from collections.abc import Callable, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from types import MappingProxyType
from typing import Any
from uuid import uuid4

from options_copilot.gateway.external_readonly_feed import (
    OPEN_REPRICE_PURPOSE,
    PREMARKET_ACCOUNT_PURPOSE,
)
from options_copilot.news.models import (
    ConditionalOptionLeg,
    ConditionalOptionPreselection,
    OptionContractRef,
    OptionLegSide,
    OptionRight,
    PreselectionPhase,
    PreselectionTerminalScenario,
    UNDERLYING_QUOTE_BASIS_SCHEMA,
    UnderlyingQuoteBasis,
)
from options_copilot.news.preselection import (
    EvaluatedPreselection,
    EvaluatedPreselectionBatch,
    build_preselection_pools,
    evaluate_preselection,
    evaluate_preselection_batch,
    strategy_structure_hash,
)
from options_copilot.storage.canonical import (
    canonical_hash,
    canonical_json,
    datetime_text,
    freeze_json,
    thaw_json,
    utc_datetime,
)


SCHEMA_VERSION = 3
GENESIS_HASH = "0" * 64
PREMARKET_LIMIT = 10
OPEN_REPRICE_WRITER = "INDEPENDENT_TOP10_PRODUCER_V1"
_ENTRY_TYPES = frozenset(
    {
        "PREMARKET_ROW",
        "PREMARKET_HEAD",
        "OPEN_OBSERVATION",
        "OPEN_BATCH_ROW",
        "OPEN_BATCH_HEAD",
    }
)
_IDENTIFIER_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,255}\Z")
_SOURCE_BATCH_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
_HASH_RE = re.compile(r"[0-9a-f]{64}\Z")
_TABLES = (
    "ledger_entries",
    "premarket_runs",
    "premarket_rows",
    "open_observations",
    "open_batches",
)
_AUTHORITY_FIELDS: dict[str, object] = {
    "decision_authority": "SUPPORTING_ONLY",
    "approval_eligible": False,
    "instruction_creation_allowed": False,
    "order_allowed": False,
}


class NewsPreselectionStoreError(RuntimeError):
    """Base error for durable preselection-ledger failures."""


class NewsPreselectionStoreConflict(NewsPreselectionStoreError):
    """An identifier or immutable parent binding conflicts with stored truth."""


class NewsPreselectionStoreCorruption(NewsPreselectionStoreError):
    """The append chain or one of its relational projections is invalid."""


@dataclass(frozen=True, slots=True)
class StoredPremarketRow:
    row_id: str
    run_id: str
    research_rank: int
    preselection_id: str
    strategy_hash: str
    structure_identity: Mapping[str, object]
    candidate: Mapping[str, object]
    evaluation: Mapping[str, object]
    sequence: int
    content_hash: str
    previous_hash: str
    row_hash: str

    def __post_init__(self) -> None:
        for name in ("structure_identity", "candidate", "evaluation"):
            frozen = freeze_json(getattr(self, name))
            if not isinstance(frozen, Mapping):
                raise TypeError(f"{name} must be a mapping")
            object.__setattr__(self, name, frozen)

    def as_dict(self) -> dict[str, object]:
        result = _mapping_copy(self.candidate)
        result.update(_mapping_copy(self.evaluation))
        result.update(
            {
                "row_id": self.row_id,
                "run_id": self.run_id,
                "research_rank": self.research_rank,
                "structure_identity": _mapping_copy(self.structure_identity),
                "ledger_sequence": self.sequence,
                "content_hash": self.content_hash,
                "parent_row_hash": self.row_hash,
                "identity_schema": self.structure_identity.get("schema"),
                "production_parent_eligible": self.production_parent_eligible,
                "production_parent_blocker": (
                    None
                    if self.production_parent_eligible
                    else "LEGACY_V1_IBKR_IDENTITY_INCOMPLETE"
                ),
                "contract_refs": [item.as_dict() for item in self.contract_refs],
                **_AUTHORITY_FIELDS,
            }
        )
        return result

    @property
    def production_parent_eligible(self) -> bool:
        return self.structure_identity.get("schema") == (
            "options_copilot.conditional_option_strategy.v2"
        ) and bool(self.contract_refs)

    @property
    def contract_refs(self) -> tuple[OptionContractRef, ...]:
        """Typed eight-field identities used by the market-open producer."""

        legs = self.structure_identity.get("legs")
        if not isinstance(legs, tuple):
            return ()
        refs: list[OptionContractRef] = []
        try:
            for leg in legs:
                if not isinstance(leg, Mapping):
                    return ()
                refs.append(_contract_ref_from_identity(leg))
        except (KeyError, TypeError, ValueError, InvalidOperation):
            return ()
        return tuple(refs)


@dataclass(frozen=True, slots=True)
class StoredPremarketRun:
    run_id: str
    created_at: datetime
    requested_count: int
    available_count: int
    source_batch_purpose: str | None
    source_batch_id: str | None
    source_batch_hash: str | None
    rows: tuple[StoredPremarketRow, ...]
    sequence: int
    content_hash: str
    previous_hash: str
    head_hash: str

    def as_dict(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "created_at": self.created_at.isoformat(),
            "requested_count": self.requested_count,
            "available_count": self.available_count,
            "source_batch_purpose": self.source_batch_purpose,
            "source_batch_id": self.source_batch_id,
            "source_batch_hash": self.source_batch_hash,
            "rows": [item.as_dict() for item in self.rows],
            "ledger_sequence": self.sequence,
            "content_hash": self.content_hash,
            "head_hash": self.head_hash,
            **_AUTHORITY_FIELDS,
        }


@dataclass(frozen=True, slots=True)
class StoredOpenObservation:
    observation_id: str
    parent_run_id: str
    parent_head_hash: str
    parent_row_hash: str
    premarket_rank: int
    preselection_id: str
    strategy_hash: str
    observed_at: datetime
    structure_identity: Mapping[str, object]
    candidate: Mapping[str, object]
    evaluation: Mapping[str, object]
    sequence: int
    content_hash: str
    previous_hash: str
    observation_hash: str
    batch_id: str | None = None
    batch_head_hash: str | None = None
    scheduled_for: datetime | None = None

    def __post_init__(self) -> None:
        for name in ("structure_identity", "candidate", "evaluation"):
            frozen = freeze_json(getattr(self, name))
            if not isinstance(frozen, Mapping):
                raise TypeError(f"{name} must be a mapping")
            object.__setattr__(self, name, frozen)

    def as_dict(self) -> dict[str, object]:
        result = _mapping_copy(self.candidate)
        result.update(_mapping_copy(self.evaluation))
        result.update(
            {
                "observation_id": self.observation_id,
                "parent_run_id": self.parent_run_id,
                "parent_head_hash": self.parent_head_hash,
                "parent_row_hash": self.parent_row_hash,
                "premarket_rank": self.premarket_rank,
                "observed_at": self.observed_at.isoformat(),
                "structure_identity": _mapping_copy(self.structure_identity),
                "ledger_sequence": self.sequence,
                "content_hash": self.content_hash,
                "observation_hash": self.observation_hash,
                "batch_id": self.batch_id,
                "batch_head_hash": self.batch_head_hash,
                "scheduled_for": (
                    None if self.scheduled_for is None else self.scheduled_for.isoformat()
                ),
                **_AUTHORITY_FIELDS,
            }
        )
        return result


@dataclass(frozen=True, slots=True)
class StoredOpenBatch:
    batch_id: str
    parent_run_id: str
    parent_head_hash: str
    scheduled_for: datetime
    observed_at: datetime
    quote_batch_id: str
    source_batch_purpose: str | None
    source_batch_id: str | None
    source_batch_hash: str | None
    blockers: tuple[str, ...]
    rows: tuple[StoredOpenObservation, ...]
    sequence: int
    content_hash: str
    previous_hash: str
    head_hash: str

    @property
    def action_pool_eligible(self) -> bool:
        return not self.blockers

    @property
    def outcome(self) -> str:
        return "SUPPORTING_ONLY" if self.action_pool_eligible else "NO_TRADE"

    def as_dict(self) -> dict[str, object]:
        return {
            "batch_id": self.batch_id,
            "parent_run_id": self.parent_run_id,
            "parent_head_hash": self.parent_head_hash,
            "scheduled_for": self.scheduled_for.isoformat(),
            "observed_at": self.observed_at.isoformat(),
            "quote_batch_id": self.quote_batch_id,
            "source_batch_purpose": self.source_batch_purpose,
            "source_batch_id": self.source_batch_id,
            "source_batch_hash": self.source_batch_hash,
            "blockers": list(self.blockers),
            "outcome": self.outcome,
            "action_pool_eligible": self.action_pool_eligible,
            "rows": [row.as_dict() for row in self.rows],
            "ledger_sequence": self.sequence,
            "content_hash": self.content_hash,
            "head_hash": self.head_hash,
            **_AUTHORITY_FIELDS,
        }


@dataclass(frozen=True, slots=True)
class PreselectionReplay:
    premarket: StoredPremarketRun
    open_observations: tuple[StoredOpenObservation, ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "premarket": self.premarket.as_dict(),
            "open_observations": [item.as_dict() for item in self.open_observations],
            **_AUTHORITY_FIELDS,
        }


@dataclass(frozen=True, slots=True)
class LedgerPreselectionSnapshot:
    """One atomic, read-only projection of the independent Top-10 ledger."""

    preselections: tuple[ConditionalOptionPreselection, ...]
    lineage: Mapping[tuple[str, str], Mapping[str, object]]
    coverage: Mapping[str, object]

    def __post_init__(self) -> None:
        checked_lineage = {
            key: MappingProxyType(dict(value)) for key, value in self.lineage.items()
        }
        object.__setattr__(self, "lineage", MappingProxyType(checked_lineage))
        object.__setattr__(self, "coverage", MappingProxyType(dict(self.coverage)))


class NewsPreselectionStore:
    """SQLite/WAL ledger with no approval, instruction, or order operations."""

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
            self._connection.execute("PRAGMA busy_timeout=10000")
            self._journal_mode = str(
                self._connection.execute("PRAGMA journal_mode=WAL").fetchone()[0]
            ).lower()
            self._connection.execute("PRAGMA synchronous=FULL")
            self._connection.execute("PRAGMA foreign_keys=ON")
            self._migrate()
            self.assert_integrity()
        except BaseException:
            self._connection.close()
            self._closed = True
            raise

    def __enter__(self) -> "NewsPreselectionStore":
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
            value, str(value)
        )

    @property
    def schema_version(self) -> int:
        self._ensure_open()
        return int(self._connection.execute("PRAGMA user_version").fetchone()[0])

    def close(self) -> None:
        with self._lock:
            if not self._closed:
                self._connection.close()
                self._closed = True

    def append_premarket_run(
        self,
        run_id: str,
        candidates: Sequence[ConditionalOptionPreselection],
        *,
        now: datetime | None = None,
        source_batch_purpose: str | None = None,
        source_batch_id: str | None = None,
        source_batch_hash: str | None = None,
    ) -> StoredPremarketRun:
        """Freeze one ranked pre-market set and append its immutable head."""

        normalized_run_id = _identifier("run_id", run_id)
        source_batch_purpose, source_batch_id, source_batch_hash = _source_batch_binding(
            source_batch_purpose,
            source_batch_id,
            source_batch_hash,
            expected_purpose=PREMARKET_ACCOUNT_PURPOSE,
        )
        candidate_rows = tuple(candidates)
        if len(candidate_rows) > PREMARKET_LIMIT:
            raise ValueError("pre-market run cannot contain more than ten rows")
        if any(not isinstance(item, ConditionalOptionPreselection) for item in candidate_rows):
            raise TypeError("candidates must contain ConditionalOptionPreselection values")
        if any(item.phase is not PreselectionPhase.PRE_MARKET for item in candidate_rows):
            raise ValueError("pre-market run accepts PRE_MARKET rows only")
        identifiers = [item.preselection_id for item in candidate_rows]
        hashes = [item.strategy_hash for item in candidate_rows]
        if len(set(identifiers)) != len(identifiers):
            raise ValueError("pre-market preselection_id values must be unique")
        if len(set(hashes)) != len(hashes):
            raise ValueError("pre-market strategy_hash values must be unique")

        checked_at = utc_datetime(now or self._clock(), field="now")
        for item in candidate_rows:
            _validated_structure(item)
        evaluated = build_preselection_pools(candidate_rows, now=checked_at)[0]
        if len(evaluated) != len(candidate_rows):
            raise ValueError("pre-market rows could not be ranked deterministically")

        templates: list[dict[str, object]] = []
        for research_rank, item in enumerate(evaluated, start=1):
            candidate = item.candidate
            row_id = _premarket_row_id(normalized_run_id, candidate.preselection_id)
            templates.append(
                {
                    "schema": "options_copilot.news_premarket_row.v2",
                    "row_id": row_id,
                    "run_id": normalized_run_id,
                    "research_rank": research_rank,
                    "preselection_id": candidate.preselection_id,
                    "strategy_hash": candidate.strategy_hash,
                    "structure_identity": _structure_identity(candidate),
                    "candidate": candidate.as_dict(),
                    "evaluation": _evaluation_document(item),
                }
            )

        with self._transaction():
            self.assert_integrity()
            if self._connection.execute(
                "SELECT 1 FROM premarket_runs WHERE run_id=?", (normalized_run_id,)
            ).fetchone() is not None:
                raise NewsPreselectionStoreConflict("run_id already exists")

            sequence, previous = self._tail()
            inserted_rows: list[dict[str, object]] = []
            for template in templates:
                sequence += 1
                entry = _entry(
                    sequence,
                    "PREMARKET_ROW",
                    str(template["row_id"]),
                    template,
                    previous,
                )
                self._insert_entry(entry)
                inserted_rows.append({**template, **entry})
                previous = str(entry["entry_hash"])

            head_payload = {
                "schema": "options_copilot.news_premarket_head.v3",
                "run_id": normalized_run_id,
                "created_at": datetime_text(checked_at),
                "requested_count": PREMARKET_LIMIT,
                "available_count": len(inserted_rows),
                "source_batch_purpose": source_batch_purpose,
                "source_batch_id": source_batch_id,
                "source_batch_hash": source_batch_hash,
                "rows": [
                    {
                        "row_id": str(item["row_id"]),
                        "row_hash": str(item["entry_hash"]),
                        "research_rank": int(item["research_rank"]),
                        "preselection_id": str(item["preselection_id"]),
                        "strategy_hash": str(item["strategy_hash"]),
                    }
                    for item in inserted_rows
                ],
                **_AUTHORITY_FIELDS,
            }
            sequence += 1
            head_entry = _entry(
                sequence,
                "PREMARKET_HEAD",
                f"head:{normalized_run_id}",
                head_payload,
                previous,
            )
            self._insert_entry(head_entry)
            self._connection.execute(
                """
                INSERT INTO premarket_runs(
                    run_id,created_at,requested_count,available_count,
                    source_batch_purpose,source_batch_id,source_batch_hash,
                    head_sequence,head_hash,content_hash
                ) VALUES(?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    normalized_run_id,
                    datetime_text(checked_at),
                    PREMARKET_LIMIT,
                    len(inserted_rows),
                    source_batch_purpose,
                    source_batch_id,
                    source_batch_hash,
                    head_entry["sequence"],
                    head_entry["entry_hash"],
                    head_entry["content_hash"],
                ),
            )
            for item in inserted_rows:
                self._connection.execute(
                    """
                    INSERT INTO premarket_rows(
                        row_id,run_id,research_rank,preselection_id,strategy_hash,
                        structure_json,candidate_json,evaluation_json,
                        entry_sequence,row_hash,content_hash
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        item["row_id"],
                        normalized_run_id,
                        item["research_rank"],
                        item["preselection_id"],
                        item["strategy_hash"],
                        canonical_json(item["structure_identity"]),
                        canonical_json(item["candidate"]),
                        canonical_json(item["evaluation"]),
                        item["sequence"],
                        item["entry_hash"],
                        item["content_hash"],
                    ),
                )
            return self._read_run_locked(run_id=normalized_run_id)

    def append_open_observation(
        self,
        parent_head_hash: str,
        parent_row_hash: str,
        candidate: ConditionalOptionPreselection,
        *,
        observation_id: str | None = None,
        now: datetime | None = None,
    ) -> StoredOpenObservation:
        """Append a reprice while rejecting every immutable structure change."""

        head_hash = _digest("parent_head_hash", parent_head_hash)
        row_hash = _digest("parent_row_hash", parent_row_hash)
        if not isinstance(candidate, ConditionalOptionPreselection):
            raise TypeError("candidate must be a ConditionalOptionPreselection")
        if candidate.phase is not PreselectionPhase.OPEN_REPRICED:
            raise ValueError("open observation requires OPEN_REPRICED phase")
        structure = _validated_structure(candidate)
        checked_at = utc_datetime(now or self._clock(), field="now")
        evaluated = evaluate_preselection(candidate, now=checked_at)
        normalized_observation_id = (
            _identifier("observation_id", observation_id)
            if observation_id is not None
            else f"obs_{uuid4().hex}"
        )

        with self._transaction():
            self.assert_integrity()
            parent = self._connection.execute(
                """
                SELECT r.run_id,r.head_hash,p.row_id,p.row_hash,p.research_rank,
                       p.preselection_id,p.strategy_hash,p.structure_json
                FROM premarket_rows AS p
                JOIN premarket_runs AS r ON r.run_id=p.run_id
                WHERE r.head_hash=? AND p.row_hash=?
                """,
                (head_hash, row_hash),
            ).fetchone()
            if parent is None:
                raise NewsPreselectionStoreConflict(
                    "parent pre-market row/head binding does not exist"
                )
            if self._connection.execute(
                "SELECT 1 FROM open_observations WHERE observation_id=?",
                (normalized_observation_id,),
            ).fetchone() is not None:
                raise NewsPreselectionStoreConflict("observation_id already exists")
            if (
                candidate.preselection_id != str(parent["preselection_id"])
                or candidate.strategy_hash != str(parent["strategy_hash"])
                or canonical_json(structure) != str(parent["structure_json"])
            ):
                raise NewsPreselectionStoreConflict(
                    "open observation changed the frozen pre-market structure"
                )

            payload = {
                "schema": "options_copilot.news_open_observation.v2",
                "observation_id": normalized_observation_id,
                "parent_run_id": str(parent["run_id"]),
                "parent_head_hash": head_hash,
                "parent_row_hash": row_hash,
                "premarket_rank": int(parent["research_rank"]),
                "preselection_id": candidate.preselection_id,
                "strategy_hash": candidate.strategy_hash,
                "observed_at": datetime_text(checked_at),
                "structure_identity": structure,
                "candidate": candidate.as_dict(),
                "evaluation": _evaluation_document(
                    evaluated,
                    batch_blockers=(
                        "LEGACY_SINGLE_OBSERVATION_NOT_BATCH_ELIGIBLE",
                    ),
                ),
                **_AUTHORITY_FIELDS,
            }
            sequence, previous = self._tail()
            entry = _entry(
                sequence + 1,
                "OPEN_OBSERVATION",
                f"open:{normalized_observation_id}",
                payload,
                previous,
            )
            self._insert_entry(entry)
            evaluation = payload["evaluation"]
            assert isinstance(evaluation, Mapping)
            self._connection.execute(
                """
                INSERT INTO open_observations(
                    observation_id,parent_run_id,parent_head_hash,parent_row_hash,
                    premarket_rank,preselection_id,strategy_hash,observed_at,
                    structure_json,candidate_json,evaluation_json,quote_batch_id,
                    oldest_quote_asof,entry_sequence,observation_hash,content_hash
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    normalized_observation_id,
                    parent["run_id"],
                    head_hash,
                    row_hash,
                    parent["research_rank"],
                    candidate.preselection_id,
                    candidate.strategy_hash,
                    datetime_text(checked_at),
                    canonical_json(structure),
                    canonical_json(payload["candidate"]),
                    canonical_json(evaluation),
                    evaluation.get("quote_batch_id"),
                    evaluation.get("oldest_quote_asof"),
                    entry["sequence"],
                    entry["entry_hash"],
                    entry["content_hash"],
                ),
            )
            row = self._connection.execute(
                "SELECT * FROM open_observations WHERE observation_id=?",
                (normalized_observation_id,),
            ).fetchone()
            if row is None:
                raise NewsPreselectionStoreCorruption("inserted observation is missing")
            return self._stored_open(row)

    def append_open_batch(
        self,
        parent_head_hash: str,
        candidates: Sequence[ConditionalOptionPreselection],
        *,
        batch_id: str,
        scheduled_for: datetime,
        observed_at: datetime,
        batch_blockers: Sequence[str] = (),
        source_batch_purpose: str | None = None,
        source_batch_id: str | None = None,
        source_batch_hash: str | None = None,
    ) -> StoredOpenBatch:
        """Atomically append the complete repriced set and one batch head."""

        head_hash = _digest("parent_head_hash", parent_head_hash)
        normalized_batch_id = _identifier("batch_id", batch_id)
        source_batch_purpose, source_batch_id, source_batch_hash = _source_batch_binding(
            source_batch_purpose,
            source_batch_id,
            source_batch_hash,
            expected_purpose=OPEN_REPRICE_PURPOSE,
        )
        scheduled = utc_datetime(scheduled_for, field="scheduled_for")
        observed = utc_datetime(observed_at, field="observed_at")
        if observed < scheduled:
            raise ValueError("observed_at cannot precede scheduled_for")
        candidate_rows = tuple(candidates)
        if any(not isinstance(item, ConditionalOptionPreselection) for item in candidate_rows):
            raise TypeError("candidates must contain ConditionalOptionPreselection values")
        if len(candidate_rows) > PREMARKET_LIMIT:
            raise ValueError("open batch cannot contain more than ten rows")
        if any(item.phase is not PreselectionPhase.OPEN_REPRICED for item in candidate_rows):
            raise ValueError("open batch accepts OPEN_REPRICED rows only")
        identifiers = [item.preselection_id for item in candidate_rows]
        if len(set(identifiers)) != len(identifiers):
            raise ValueError("open batch preselection_id values must be unique")
        structures = [_validated_structure(item) for item in candidate_rows]
        quote_batch_values = [leg.quote_batch_id for item in candidate_rows for leg in item.legs]
        quote_asof_values = [leg.quote_asof for item in candidate_rows for leg in item.legs]
        if (
            not quote_batch_values
            or any(value is None for value in quote_batch_values)
            or len(set(quote_batch_values)) != 1
        ):
            raise ValueError("open batch must use one complete quote_batch_id")
        if (
            not quote_asof_values
            or any(value is None for value in quote_asof_values)
            or len(set(quote_asof_values)) != 1
        ):
            raise ValueError("open batch must use one complete quote_asof")

        with self._transaction():
            self.assert_integrity()
            if self._connection.execute(
                "SELECT 1 FROM open_batches WHERE batch_id=?", (normalized_batch_id,)
            ).fetchone() is not None:
                raise NewsPreselectionStoreConflict("batch_id already exists")
            run = self._connection.execute(
                "SELECT * FROM premarket_runs WHERE head_hash=?", (head_hash,)
            ).fetchone()
            if run is None:
                raise NewsPreselectionStoreConflict("parent pre-market head does not exist")
            parent_rows = self._connection.execute(
                "SELECT * FROM premarket_rows WHERE run_id=? ORDER BY research_rank",
                (run["run_id"],),
            ).fetchall()
            expected_ids = tuple(str(row["preselection_id"]) for row in parent_rows)
            if len(identifiers) != len(expected_ids) or set(identifiers) != set(expected_ids):
                raise NewsPreselectionStoreConflict(
                    "open batch candidate IDs must exactly match the parent run"
                )
            parents = {str(row["preselection_id"]): row for row in parent_rows}
            for candidate, structure in zip(candidate_rows, structures):
                parent = parents[candidate.preselection_id]
                parent_structure = _canonical_sql_json(parent, "structure_json")
                if parent_structure.get("schema") != "options_copilot.conditional_option_strategy.v2":
                    raise NewsPreselectionStoreConflict(
                        "legacy v1 identity is insufficient for a production parent"
                    )
                if (
                    candidate.strategy_hash != str(parent["strategy_hash"])
                    or canonical_json(structure) != str(parent["structure_json"])
                ):
                    raise NewsPreselectionStoreConflict(
                        "open batch changed the frozen parent strategy identity"
                    )

            evaluated = evaluate_preselection_batch(
                candidate_rows,
                now=observed,
                expected_preselection_ids=expected_ids,
                blockers=batch_blockers,
            )
            if evaluated.quote_batch_id is None:
                raise ValueError("open batch must use one complete quote_batch_id")
            if evaluated.quote_asof is None:
                raise ValueError("open batch must use one complete quote_asof")
            ordered = sorted(
                zip(candidate_rows, structures),
                key=lambda item: int(parents[item[0].preselection_id]["research_rank"]),
            )
            return self._append_open_batch_locked(
                run=run,
                parents=parents,
                candidates=tuple(item[0] for item in ordered),
                structures=tuple(item[1] for item in ordered),
                batch_id=normalized_batch_id,
                scheduled_for=scheduled,
                observed_at=observed,
                evaluated=evaluated,
                source_batch_purpose=source_batch_purpose,
                source_batch_id=source_batch_id,
                source_batch_hash=source_batch_hash,
            )

    def _append_open_batch_locked(
        self,
        *,
        run: sqlite3.Row,
        parents: Mapping[str, sqlite3.Row],
        candidates: tuple[ConditionalOptionPreselection, ...],
        structures: Sequence[Mapping[str, object]],
        batch_id: str,
        scheduled_for: datetime,
        observed_at: datetime,
        evaluated: EvaluatedPreselectionBatch,
        source_batch_purpose: str | None,
        source_batch_id: str | None,
        source_batch_hash: str | None,
    ) -> StoredOpenBatch:
        sequence, previous = self._tail()
        inserted: list[dict[str, object]] = []
        evaluation_by_id = {
            item.candidate.preselection_id: item for item in evaluated.evaluations
        }
        for candidate, structure in zip(candidates, structures):
            parent = parents[candidate.preselection_id]
            observation_id = _open_batch_row_id(batch_id, candidate.preselection_id)
            evaluation = _evaluation_document(
                evaluation_by_id[candidate.preselection_id],
                batch_blockers=evaluated.blockers,
            )
            payload = {
                "schema": "options_copilot.news_open_batch_row.v2",
                "batch_id": batch_id,
                "observation_id": observation_id,
                "parent_run_id": str(run["run_id"]),
                "parent_head_hash": str(run["head_hash"]),
                "parent_row_hash": str(parent["row_hash"]),
                "premarket_rank": int(parent["research_rank"]),
                "preselection_id": candidate.preselection_id,
                "strategy_hash": candidate.strategy_hash,
                "scheduled_for": datetime_text(scheduled_for),
                "observed_at": datetime_text(observed_at),
                "structure_identity": structure,
                "candidate": candidate.as_dict(),
                "evaluation": evaluation,
                **_AUTHORITY_FIELDS,
            }
            sequence += 1
            entry = _entry(
                sequence,
                "OPEN_BATCH_ROW",
                f"open-batch-row:{observation_id}",
                payload,
                previous,
            )
            self._insert_entry(entry)
            inserted.append({**payload, **entry})
            previous = str(entry["entry_hash"])

        head_payload = _open_batch_head_document(
            batch_id=batch_id,
            run=run,
            scheduled_for=scheduled_for,
            observed_at=observed_at,
            evaluated=evaluated,
            rows=inserted,
            source_batch_purpose=source_batch_purpose,
            source_batch_id=source_batch_id,
            source_batch_hash=source_batch_hash,
        )
        sequence += 1
        head_entry = _entry(
            sequence,
            "OPEN_BATCH_HEAD",
            f"open-batch-head:{batch_id}",
            head_payload,
            previous,
        )
        self._insert_entry(head_entry)
        self._connection.execute(
            """
            INSERT INTO open_batches(
                batch_id,parent_run_id,parent_head_hash,scheduled_for,observed_at,
                quote_batch_id,quote_asof,available_count,blockers_json,
                source_batch_purpose,source_batch_id,source_batch_hash,
                head_sequence,head_hash,content_hash
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                batch_id,
                run["run_id"],
                run["head_hash"],
                datetime_text(scheduled_for),
                datetime_text(observed_at),
                evaluated.quote_batch_id,
                datetime_text(evaluated.quote_asof),  # type: ignore[arg-type]
                len(inserted),
                canonical_json(list(evaluated.blockers)),
                source_batch_purpose,
                source_batch_id,
                source_batch_hash,
                head_entry["sequence"],
                head_entry["entry_hash"],
                head_entry["content_hash"],
            ),
        )
        for item in inserted:
            self._insert_open_batch_projection(
                item, batch_id=batch_id, batch_head_hash=str(head_entry["entry_hash"])
            )
        return self._read_open_batch_locked(batch_id)

    def _insert_open_batch_projection(
        self,
        item: Mapping[str, object],
        *,
        batch_id: str,
        batch_head_hash: str,
    ) -> None:
        evaluation = item["evaluation"]
        assert isinstance(evaluation, Mapping)
        self._connection.execute(
            """
            INSERT INTO open_observations(
                observation_id,parent_run_id,parent_head_hash,parent_row_hash,
                premarket_rank,preselection_id,strategy_hash,observed_at,
                structure_json,candidate_json,evaluation_json,quote_batch_id,
                oldest_quote_asof,entry_sequence,observation_hash,content_hash,
                batch_id,batch_head_hash,scheduled_for
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                item["observation_id"], item["parent_run_id"], item["parent_head_hash"],
                item["parent_row_hash"], item["premarket_rank"], item["preselection_id"],
                item["strategy_hash"], item["observed_at"],
                canonical_json(item["structure_identity"]), canonical_json(item["candidate"]),
                canonical_json(evaluation), evaluation.get("quote_batch_id"),
                evaluation.get("oldest_quote_asof"), item["sequence"], item["entry_hash"],
                item["content_hash"], batch_id, batch_head_hash, item["scheduled_for"],
            ),
        )

    def latest_premarket(self) -> StoredPremarketRun | None:
        self._ensure_open()
        with self._lock:
            self.assert_integrity()
            row = self._connection.execute(
                "SELECT run_id FROM premarket_runs ORDER BY head_sequence DESC LIMIT 1"
            ).fetchone()
            return None if row is None else self._read_run_locked(run_id=str(row[0]))

    def latest_open(self) -> tuple[StoredOpenObservation, ...]:
        """Return the newest observation per frozen row in the latest run."""

        self._ensure_open()
        with self._lock:
            self.assert_integrity()
            run = self._connection.execute(
                "SELECT run_id FROM premarket_runs ORDER BY head_sequence DESC LIMIT 1"
            ).fetchone()
            if run is None:
                return ()
            rows = self._connection.execute(
                """
                SELECT * FROM open_observations
                WHERE parent_run_id=?
                ORDER BY entry_sequence DESC
                """,
                (str(run[0]),),
            ).fetchall()
            latest: dict[str, sqlite3.Row] = {}
            for row in rows:
                latest.setdefault(str(row["parent_row_hash"]), row)
            return tuple(
                self._stored_open(row)
                for row in sorted(
                    latest.values(), key=lambda item: int(item["premarket_rank"])
                )
            )

    def latest_replay(self) -> PreselectionReplay | None:
        """Read the latest run and latest-open-per-row under one store lock.

        This is the recovery seam used by runtime composition.  Keeping both
        projections inside one lock prevents a newly appended run from being
        paired with observations belonging to an older head.
        """

        self._ensure_open()
        with self._lock:
            self.assert_integrity()
            latest = self._connection.execute(
                "SELECT run_id FROM premarket_runs ORDER BY head_sequence DESC LIMIT 1"
            ).fetchone()
            if latest is None:
                return None
            run_id = str(latest["run_id"])
            premarket = self._read_run_locked(run_id=run_id)
            rows = self._connection.execute(
                "SELECT * FROM open_observations WHERE parent_run_id=? "
                "ORDER BY entry_sequence DESC",
                (run_id,),
            ).fetchall()
            newest: dict[str, sqlite3.Row] = {}
            for row in rows:
                newest.setdefault(str(row["parent_row_hash"]), row)
            observations = tuple(
                self._stored_open(row)
                for row in sorted(
                    newest.values(), key=lambda item: int(item["premarket_rank"])
                )
            )
            if any(
                item.parent_run_id != premarket.run_id
                or item.parent_head_hash != premarket.head_hash
                for item in observations
            ):
                raise NewsPreselectionStoreCorruption(
                    "latest replay contains mismatched run lineage"
                )
            return PreselectionReplay(premarket, observations)

    def read_run(
        self,
        run_id: str | None = None,
        *,
        head_hash: str | None = None,
    ) -> StoredPremarketRun:
        if (run_id is None) == (head_hash is None):
            raise ValueError("provide exactly one of run_id or head_hash")
        self._ensure_open()
        with self._lock:
            self.assert_integrity()
            return self._read_run_locked(
                run_id=None if run_id is None else _identifier("run_id", run_id),
                head_hash=None
                if head_hash is None
                else _digest("head_hash", head_hash),
            )

    def read_open_observations(
        self,
        run_id: str,
    ) -> tuple[StoredOpenObservation, ...]:
        normalized = _identifier("run_id", run_id)
        self._ensure_open()
        with self._lock:
            self.assert_integrity()
            if self._connection.execute(
                "SELECT 1 FROM premarket_runs WHERE run_id=?", (normalized,)
            ).fetchone() is None:
                raise KeyError(normalized)
            rows = self._connection.execute(
                "SELECT * FROM open_observations WHERE parent_run_id=? "
                "ORDER BY entry_sequence",
                (normalized,),
            ).fetchall()
            return tuple(self._stored_open(item) for item in rows)

    def read_open_batch(self, batch_id: str) -> StoredOpenBatch:
        normalized = _identifier("batch_id", batch_id)
        self._ensure_open()
        with self._lock:
            self.assert_integrity()
            return self._read_open_batch_locked(normalized)

    def latest_open_batch(self) -> StoredOpenBatch | None:
        self._ensure_open()
        with self._lock:
            self.assert_integrity()
            row = self._connection.execute(
                "SELECT batch_id FROM open_batches ORDER BY head_sequence DESC LIMIT 1"
            ).fetchone()
            return None if row is None else self._read_open_batch_locked(str(row[0]))

    def replay(self, run_id: str) -> PreselectionReplay:
        return PreselectionReplay(
            self.read_run(run_id),
            self.read_open_observations(run_id),
        )

    def verify_integrity(self) -> bool:
        self.assert_integrity()
        return True

    def assert_integrity(self) -> None:
        self._ensure_open()
        with self._lock:
            entries = self._connection.execute(
                "SELECT * FROM ledger_entries ORDER BY sequence"
            ).fetchall()
            previous = GENESIS_HASH
            by_sequence: dict[int, sqlite3.Row] = {}
            by_hash: dict[str, sqlite3.Row] = {}
            for expected_sequence, row in enumerate(entries, start=1):
                sequence = int(row["sequence"])
                if sequence != expected_sequence:
                    raise NewsPreselectionStoreCorruption(
                        "global ledger sequence contains a gap"
                    )
                payload = _canonical_sql_json(row, "payload_json")
                if str(row["entry_type"]) not in _ENTRY_TYPES:
                    raise NewsPreselectionStoreCorruption("unknown ledger entry type")
                content_hash = canonical_hash(payload)
                if content_hash != str(row["content_hash"]):
                    raise NewsPreselectionStoreCorruption(
                        f"ledger content hash mismatch at sequence {sequence}"
                    )
                if str(row["previous_hash"]) != previous:
                    raise NewsPreselectionStoreCorruption(
                        f"ledger previous hash mismatch at sequence {sequence}"
                    )
                expected_hash = _chain_hash(
                    sequence,
                    str(row["entry_type"]),
                    str(row["record_id"]),
                    content_hash,
                    previous,
                )
                if str(row["entry_hash"]) != expected_hash:
                    raise NewsPreselectionStoreCorruption(
                        f"ledger entry hash mismatch at sequence {sequence}"
                    )
                previous = expected_hash
                by_sequence[sequence] = row
                by_hash[expected_hash] = row

            self._assert_run_projections(by_sequence, by_hash)
            self._assert_observation_projections(by_sequence)
            self._assert_open_batch_projections(by_sequence)
            projected_sequences = {
                int(row[0])
                for sql in (
                    "SELECT entry_sequence FROM premarket_rows",
                    "SELECT head_sequence FROM premarket_runs",
                    "SELECT entry_sequence FROM open_observations",
                    "SELECT head_sequence FROM open_batches",
                )
                for row in self._connection.execute(sql).fetchall()
            }
            if projected_sequences != set(by_sequence):
                raise NewsPreselectionStoreCorruption(
                    "ledger contains an orphan or unchained relational projection"
                )
            if self._connection.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                raise NewsPreselectionStoreCorruption("SQLite integrity check failed")

    def _assert_run_projections(
        self,
        by_sequence: Mapping[int, sqlite3.Row],
        by_hash: Mapping[str, sqlite3.Row],
    ) -> None:
        runs = self._connection.execute(
            "SELECT * FROM premarket_runs ORDER BY head_sequence"
        ).fetchall()
        for run in runs:
            (
                source_batch_purpose,
                source_batch_id,
                source_batch_hash,
            ) = _stored_source_batch_binding(
                run,
                expected_purpose=PREMARKET_ACCOUNT_PURPOSE,
            )
            head = by_sequence.get(int(run["head_sequence"]))
            if (
                head is None
                or str(head["entry_type"]) != "PREMARKET_HEAD"
                or str(head["record_id"]) != f"head:{run['run_id']}"
                or str(head["entry_hash"]) != str(run["head_hash"])
                or str(head["content_hash"]) != str(run["content_hash"])
            ):
                raise NewsPreselectionStoreCorruption("pre-market head projection mismatch")
            rows = self._connection.execute(
                "SELECT * FROM premarket_rows WHERE run_id=? ORDER BY research_rank",
                (run["run_id"],),
            ).fetchall()
            if int(run["available_count"]) != len(rows) or int(
                run["requested_count"]
            ) != PREMARKET_LIMIT:
                raise NewsPreselectionStoreCorruption("pre-market run count mismatch")
            head_payload = _canonical_sql_json(head, "payload_json")
            expected_rows: list[dict[str, object]] = []
            for expected_rank, row in enumerate(rows, start=1):
                entry = by_sequence.get(int(row["entry_sequence"]))
                if (
                    int(row["research_rank"]) != expected_rank
                    or entry is None
                    or str(entry["entry_type"]) != "PREMARKET_ROW"
                    or str(entry["record_id"]) != str(row["row_id"])
                    or str(entry["entry_hash"]) != str(row["row_hash"])
                    or str(entry["content_hash"]) != str(row["content_hash"])
                    or str(row["row_hash"]) not in by_hash
                ):
                    raise NewsPreselectionStoreCorruption(
                        "pre-market row projection mismatch"
                    )
                payload = _canonical_sql_json(entry, "payload_json")
                structure = _canonical_sql_json(row, "structure_json")
                candidate = _canonical_sql_json(row, "candidate_json")
                evaluation = _canonical_sql_json(row, "evaluation_json")
                if (
                    payload.get("structure_identity") != structure
                    or payload.get("candidate") != candidate
                    or payload.get("evaluation") != evaluation
                    or payload.get("strategy_hash") != row["strategy_hash"]
                    or _structure_hash_from_identity(structure) != row["strategy_hash"]
                    or not _evaluation_is_self_consistent(evaluation)
                ):
                    raise NewsPreselectionStoreCorruption(
                        "pre-market row immutable document mismatch"
                    )
                expected_rows.append(
                    {
                        "row_id": str(row["row_id"]),
                        "row_hash": str(row["row_hash"]),
                        "research_rank": expected_rank,
                        "preselection_id": str(row["preselection_id"]),
                        "strategy_hash": str(row["strategy_hash"]),
                    }
                )
            head_schema = head_payload.get("schema")
            if head_schema not in {
                "options_copilot.news_premarket_head.v1",
                "options_copilot.news_premarket_head.v2",
                "options_copilot.news_premarket_head.v3",
            }:
                raise NewsPreselectionStoreCorruption("pre-market head schema is invalid")
            expected_head = {
                "schema": head_schema,
                "run_id": str(run["run_id"]),
                "created_at": str(run["created_at"]),
                "requested_count": PREMARKET_LIMIT,
                "available_count": len(rows),
                "rows": expected_rows,
                **_AUTHORITY_FIELDS,
            }
            if head_schema == "options_copilot.news_premarket_head.v3":
                expected_head.update(
                    {
                        "source_batch_purpose": source_batch_purpose,
                        "source_batch_id": source_batch_id,
                        "source_batch_hash": source_batch_hash,
                    }
                )
            elif any(
                value is not None
                for value in (
                    source_batch_purpose,
                    source_batch_id,
                    source_batch_hash,
                )
            ):
                raise NewsPreselectionStoreCorruption(
                    "legacy pre-market head cannot claim source batch lineage"
                )
            if head_payload != expected_head:
                raise NewsPreselectionStoreCorruption(
                    "pre-market head immutable document mismatch"
                )

    def _assert_observation_projections(
        self,
        by_sequence: Mapping[int, sqlite3.Row],
    ) -> None:
        rows = self._connection.execute(
            "SELECT * FROM open_observations ORDER BY entry_sequence"
        ).fetchall()
        for row in rows:
            entry = by_sequence.get(int(row["entry_sequence"]))
            if (
                entry is None
                or str(entry["entry_type"])
                not in {"OPEN_OBSERVATION", "OPEN_BATCH_ROW"}
                or str(entry["entry_hash"]) != str(row["observation_hash"])
                or str(entry["content_hash"]) != str(row["content_hash"])
            ):
                raise NewsPreselectionStoreCorruption(
                    "open observation projection mismatch"
                )
            payload = _canonical_sql_json(entry, "payload_json")
            expected_record_id = (
                f"open:{row['observation_id']}"
                if str(entry["entry_type"]) == "OPEN_OBSERVATION"
                else f"open-batch-row:{row['observation_id']}"
            )
            structure = _canonical_sql_json(row, "structure_json")
            candidate = _canonical_sql_json(row, "candidate_json")
            evaluation = _canonical_sql_json(row, "evaluation_json")
            parent = self._connection.execute(
                """
                SELECT p.preselection_id,p.strategy_hash,p.structure_json
                FROM premarket_rows AS p
                JOIN premarket_runs AS r ON r.run_id=p.run_id
                WHERE p.run_id=? AND p.row_hash=? AND r.head_hash=?
                """,
                (row["parent_run_id"], row["parent_row_hash"], row["parent_head_hash"]),
            ).fetchone()
            if (
                parent is None
                or str(entry["record_id"]) != expected_record_id
                or str(parent["preselection_id"]) != str(row["preselection_id"])
                or str(parent["strategy_hash"]) != str(row["strategy_hash"])
                or str(parent["structure_json"]) != canonical_json(structure)
                or payload.get("structure_identity") != structure
                or payload.get("candidate") != candidate
                or payload.get("evaluation") != evaluation
                or payload.get("parent_head_hash") != row["parent_head_hash"]
                or payload.get("parent_row_hash") != row["parent_row_hash"]
                or payload.get("parent_run_id") != row["parent_run_id"]
                or payload.get("observation_id") != row["observation_id"]
                or payload.get("premarket_rank") != row["premarket_rank"]
                or payload.get("preselection_id") != row["preselection_id"]
                or payload.get("observed_at") != row["observed_at"]
                or payload.get("strategy_hash") != row["strategy_hash"]
                or _structure_hash_from_identity(structure) != row["strategy_hash"]
                or evaluation.get("quote_batch_id") != row["quote_batch_id"]
                or evaluation.get("oldest_quote_asof") != row["oldest_quote_asof"]
                or not _evaluation_is_self_consistent(evaluation)
            ):
                raise NewsPreselectionStoreCorruption(
                    "open observation immutable parent binding mismatch"
                )

    def _assert_open_batch_projections(
        self,
        by_sequence: Mapping[int, sqlite3.Row],
    ) -> None:
        batches = self._connection.execute(
            "SELECT * FROM open_batches ORDER BY head_sequence"
        ).fetchall()
        for batch in batches:
            (
                source_batch_purpose,
                source_batch_id,
                source_batch_hash,
            ) = _stored_source_batch_binding(
                batch,
                expected_purpose=OPEN_REPRICE_PURPOSE,
            )
            head = by_sequence.get(int(batch["head_sequence"]))
            rows = self._connection.execute(
                "SELECT * FROM open_observations WHERE batch_id=? ORDER BY premarket_rank",
                (batch["batch_id"],),
            ).fetchall()
            if (
                head is None
                or str(head["entry_type"]) != "OPEN_BATCH_HEAD"
                or str(head["record_id"]) != f"open-batch-head:{batch['batch_id']}"
                or str(head["entry_hash"]) != str(batch["head_hash"])
                or str(head["content_hash"]) != str(batch["content_hash"])
                or int(batch["available_count"]) != len(rows)
            ):
                raise NewsPreselectionStoreCorruption("open batch head projection mismatch")
            payload = _canonical_sql_json(head, "payload_json")
            blockers = json.loads(str(batch["blockers_json"]))
            expected_rows = [
                {
                    "observation_id": str(row["observation_id"]),
                    "observation_hash": str(row["observation_hash"]),
                    "parent_row_hash": str(row["parent_row_hash"]),
                    "premarket_rank": int(row["premarket_rank"]),
                    "preselection_id": str(row["preselection_id"]),
                    "strategy_hash": str(row["strategy_hash"]),
                }
                for row in rows
            ]
            head_schema = payload.get("schema")
            if head_schema not in {
                "options_copilot.news_open_batch_head.v2",
                "options_copilot.news_open_batch_head.v3",
            }:
                raise NewsPreselectionStoreCorruption("open batch head schema is invalid")
            expected = {
                "schema": head_schema,
                "batch_id": str(batch["batch_id"]),
                "parent_run_id": str(batch["parent_run_id"]),
                "parent_head_hash": str(batch["parent_head_hash"]),
                "scheduled_for": str(batch["scheduled_for"]),
                "observed_at": str(batch["observed_at"]),
                "quote_batch_id": str(batch["quote_batch_id"]),
                "quote_asof": str(batch["quote_asof"]),
                "available_count": len(rows),
                "blockers": blockers,
                "outcome": "NO_TRADE" if blockers else "SUPPORTING_ONLY",
                "action_pool_eligible": not blockers,
                "rows": expected_rows,
                **_AUTHORITY_FIELDS,
            }
            if head_schema == "options_copilot.news_open_batch_head.v3":
                expected.update(
                    {
                        "source_batch_purpose": source_batch_purpose,
                        "source_batch_id": source_batch_id,
                        "source_batch_hash": source_batch_hash,
                    }
                )
            elif any(
                value is not None
                for value in (
                    source_batch_purpose,
                    source_batch_id,
                    source_batch_hash,
                )
            ):
                raise NewsPreselectionStoreCorruption(
                    "legacy open batch head cannot claim source batch lineage"
                )
            if payload != expected or any(
                row["batch_head_hash"] != batch["head_hash"]
                or row["scheduled_for"] != batch["scheduled_for"]
                or row["parent_head_hash"] != batch["parent_head_hash"]
                for row in rows
            ):
                raise NewsPreselectionStoreCorruption(
                    "open batch immutable document mismatch"
                )

    def _read_run_locked(
        self,
        *,
        run_id: str | None = None,
        head_hash: str | None = None,
    ) -> StoredPremarketRun:
        if run_id is not None:
            run = self._connection.execute(
                "SELECT * FROM premarket_runs WHERE run_id=?", (run_id,)
            ).fetchone()
            missing = run_id
        else:
            run = self._connection.execute(
                "SELECT * FROM premarket_runs WHERE head_hash=?", (head_hash,)
            ).fetchone()
            missing = str(head_hash)
        if run is None:
            raise KeyError(missing)
        entry = self._connection.execute(
            "SELECT * FROM ledger_entries WHERE sequence=?", (run["head_sequence"],)
        ).fetchone()
        if entry is None:
            raise NewsPreselectionStoreCorruption("pre-market head entry is missing")
        rows = self._connection.execute(
            "SELECT * FROM premarket_rows WHERE run_id=? ORDER BY research_rank",
            (run["run_id"],),
        ).fetchall()
        (
            source_batch_purpose,
            source_batch_id,
            source_batch_hash,
        ) = _stored_source_batch_binding(
            run,
            expected_purpose=PREMARKET_ACCOUNT_PURPOSE,
        )
        return StoredPremarketRun(
            run_id=str(run["run_id"]),
            created_at=_timestamp(run["created_at"]),
            requested_count=int(run["requested_count"]),
            available_count=int(run["available_count"]),
            source_batch_purpose=source_batch_purpose,
            source_batch_id=source_batch_id,
            source_batch_hash=source_batch_hash,
            rows=tuple(self._stored_premarket_row(item) for item in rows),
            sequence=int(entry["sequence"]),
            content_hash=str(entry["content_hash"]),
            previous_hash=str(entry["previous_hash"]),
            head_hash=str(entry["entry_hash"]),
        )

    def _read_open_batch_locked(self, batch_id: str) -> StoredOpenBatch:
        batch = self._connection.execute(
            "SELECT * FROM open_batches WHERE batch_id=?", (batch_id,)
        ).fetchone()
        if batch is None:
            raise KeyError(batch_id)
        entry = self._connection.execute(
            "SELECT * FROM ledger_entries WHERE sequence=?", (batch["head_sequence"],)
        ).fetchone()
        if entry is None:
            raise NewsPreselectionStoreCorruption("open batch head entry is missing")
        rows = self._connection.execute(
            "SELECT * FROM open_observations WHERE batch_id=? ORDER BY premarket_rank",
            (batch_id,),
        ).fetchall()
        blockers = json.loads(str(batch["blockers_json"]))
        if not isinstance(blockers, list) or not all(isinstance(item, str) for item in blockers):
            raise NewsPreselectionStoreCorruption("open batch blockers are invalid")
        (
            source_batch_purpose,
            source_batch_id,
            source_batch_hash,
        ) = _stored_source_batch_binding(
            batch,
            expected_purpose=OPEN_REPRICE_PURPOSE,
        )
        return StoredOpenBatch(
            batch_id=str(batch["batch_id"]),
            parent_run_id=str(batch["parent_run_id"]),
            parent_head_hash=str(batch["parent_head_hash"]),
            scheduled_for=_timestamp(batch["scheduled_for"]),
            observed_at=_timestamp(batch["observed_at"]),
            quote_batch_id=str(batch["quote_batch_id"]),
            source_batch_purpose=source_batch_purpose,
            source_batch_id=source_batch_id,
            source_batch_hash=source_batch_hash,
            blockers=tuple(blockers),
            rows=tuple(self._stored_open(row) for row in rows),
            sequence=int(entry["sequence"]),
            content_hash=str(entry["content_hash"]),
            previous_hash=str(entry["previous_hash"]),
            head_hash=str(entry["entry_hash"]),
        )

    def _stored_premarket_row(self, row: sqlite3.Row) -> StoredPremarketRow:
        entry = self._connection.execute(
            "SELECT * FROM ledger_entries WHERE sequence=?", (row["entry_sequence"],)
        ).fetchone()
        if entry is None:
            raise NewsPreselectionStoreCorruption("pre-market row entry is missing")
        return StoredPremarketRow(
            row_id=str(row["row_id"]),
            run_id=str(row["run_id"]),
            research_rank=int(row["research_rank"]),
            preselection_id=str(row["preselection_id"]),
            strategy_hash=str(row["strategy_hash"]),
            structure_identity=_canonical_sql_json(row, "structure_json"),
            candidate=_canonical_sql_json(row, "candidate_json"),
            evaluation=_canonical_sql_json(row, "evaluation_json"),
            sequence=int(entry["sequence"]),
            content_hash=str(entry["content_hash"]),
            previous_hash=str(entry["previous_hash"]),
            row_hash=str(entry["entry_hash"]),
        )

    def _stored_open(self, row: sqlite3.Row) -> StoredOpenObservation:
        entry = self._connection.execute(
            "SELECT * FROM ledger_entries WHERE sequence=?", (row["entry_sequence"],)
        ).fetchone()
        if entry is None:
            raise NewsPreselectionStoreCorruption("open observation entry is missing")
        return StoredOpenObservation(
            observation_id=str(row["observation_id"]),
            parent_run_id=str(row["parent_run_id"]),
            parent_head_hash=str(row["parent_head_hash"]),
            parent_row_hash=str(row["parent_row_hash"]),
            premarket_rank=int(row["premarket_rank"]),
            preselection_id=str(row["preselection_id"]),
            strategy_hash=str(row["strategy_hash"]),
            observed_at=_timestamp(row["observed_at"]),
            structure_identity=_canonical_sql_json(row, "structure_json"),
            candidate=_canonical_sql_json(row, "candidate_json"),
            evaluation=_canonical_sql_json(row, "evaluation_json"),
            sequence=int(entry["sequence"]),
            content_hash=str(entry["content_hash"]),
            previous_hash=str(entry["previous_hash"]),
            observation_hash=str(entry["entry_hash"]),
            batch_id=None if row["batch_id"] is None else str(row["batch_id"]),
            batch_head_hash=(
                None if row["batch_head_hash"] is None else str(row["batch_head_hash"])
            ),
            scheduled_for=(
                None if row["scheduled_for"] is None else _timestamp(row["scheduled_for"])
            ),
        )

    def _tail(self) -> tuple[int, str]:
        row = self._connection.execute(
            "SELECT sequence,entry_hash FROM ledger_entries ORDER BY sequence DESC LIMIT 1"
        ).fetchone()
        return (0, GENESIS_HASH) if row is None else (int(row[0]), str(row[1]))

    def _insert_entry(self, entry: Mapping[str, object]) -> None:
        self._connection.execute(
            """
            INSERT INTO ledger_entries(
                sequence,entry_type,record_id,payload_json,content_hash,
                previous_hash,entry_hash
            ) VALUES(?,?,?,?,?,?,?)
            """,
            (
                entry["sequence"],
                entry["entry_type"],
                entry["record_id"],
                entry["payload_json"],
                entry["content_hash"],
                entry["previous_hash"],
                entry["entry_hash"],
            ),
        )

    def _migrate(self) -> None:
        version = int(self._connection.execute("PRAGMA user_version").fetchone()[0])
        if version > SCHEMA_VERSION:
            raise RuntimeError(f"preselection schema {version} is newer than supported")
        if version == 0:
            self._create_schema()
        elif version == 1:
            self._migrate_v1_to_v2()
            self._migrate_v2_to_v3()
        elif version == 2:
            self._migrate_v2_to_v3()
        elif version == SCHEMA_VERSION:
            self._validate_schema()
        else:
            raise RuntimeError(f"unsupported preselection schema {version}")

    def _migrate_v1_to_v2(self) -> None:
        """Add batch projections while preserving every legacy chain byte."""

        self._connection.execute("PRAGMA foreign_keys=OFF")
        try:
            self._connection.executescript(
                """
                BEGIN IMMEDIATE;
                CREATE TABLE ledger_entries_v2 (
                    sequence INTEGER PRIMARY KEY,
                    entry_type TEXT NOT NULL CHECK(entry_type IN ('PREMARKET_ROW','PREMARKET_HEAD','OPEN_OBSERVATION','OPEN_BATCH_ROW','OPEN_BATCH_HEAD')),
                    record_id TEXT NOT NULL UNIQUE,
                    payload_json TEXT NOT NULL,
                    content_hash TEXT NOT NULL,
                    previous_hash TEXT NOT NULL,
                    entry_hash TEXT NOT NULL UNIQUE
                );
                INSERT INTO ledger_entries_v2
                    SELECT sequence,entry_type,record_id,payload_json,content_hash,previous_hash,entry_hash
                    FROM ledger_entries ORDER BY sequence;
                DROP TABLE ledger_entries;
                ALTER TABLE ledger_entries_v2 RENAME TO ledger_entries;
                ALTER TABLE open_observations ADD COLUMN batch_id TEXT;
                ALTER TABLE open_observations ADD COLUMN batch_head_hash TEXT;
                ALTER TABLE open_observations ADD COLUMN scheduled_for TEXT;
                CREATE TABLE open_batches (
                    batch_id TEXT PRIMARY KEY,
                    parent_run_id TEXT NOT NULL,
                    parent_head_hash TEXT NOT NULL,
                    scheduled_for TEXT NOT NULL,
                    observed_at TEXT NOT NULL,
                    quote_batch_id TEXT NOT NULL,
                    quote_asof TEXT NOT NULL,
                    available_count INTEGER NOT NULL CHECK(available_count BETWEEN 0 AND 10),
                    blockers_json TEXT NOT NULL,
                    head_sequence INTEGER NOT NULL UNIQUE,
                    head_hash TEXT NOT NULL UNIQUE,
                    content_hash TEXT NOT NULL,
                    FOREIGN KEY(parent_run_id,parent_head_hash)
                        REFERENCES premarket_runs(run_id,head_hash),
                    FOREIGN KEY(head_sequence) REFERENCES ledger_entries(sequence),
                    FOREIGN KEY(head_hash) REFERENCES ledger_entries(entry_hash)
                );
                CREATE INDEX open_batches_parent_idx
                    ON open_batches(parent_run_id,head_sequence);
                PRAGMA user_version=2;
                COMMIT;
                """
            )
        finally:
            self._connection.execute("PRAGMA foreign_keys=ON")
        if self._connection.execute("PRAGMA foreign_key_check").fetchone() is not None:
            raise RuntimeError("preselection v1 to v2 migration broke foreign keys")

    def _migrate_v2_to_v3(self) -> None:
        """Add nullable source lineage without rewriting legacy chain bytes."""

        self._connection.executescript(
            """
            BEGIN IMMEDIATE;
            ALTER TABLE premarket_runs ADD COLUMN source_batch_purpose TEXT;
            ALTER TABLE premarket_runs ADD COLUMN source_batch_id TEXT;
            ALTER TABLE premarket_runs ADD COLUMN source_batch_hash TEXT;
            ALTER TABLE open_batches ADD COLUMN source_batch_purpose TEXT;
            ALTER TABLE open_batches ADD COLUMN source_batch_id TEXT;
            ALTER TABLE open_batches ADD COLUMN source_batch_hash TEXT;
            PRAGMA user_version=3;
            COMMIT;
            """
        )
        if self._connection.execute("PRAGMA foreign_key_check").fetchone() is not None:
            raise RuntimeError("preselection v2 to v3 migration broke foreign keys")
        self._validate_schema()

    def _create_schema(self) -> None:
        self._connection.executescript(
            """
            BEGIN IMMEDIATE;
            CREATE TABLE ledger_entries (
                sequence INTEGER PRIMARY KEY,
                entry_type TEXT NOT NULL CHECK(entry_type IN ('PREMARKET_ROW','PREMARKET_HEAD','OPEN_OBSERVATION','OPEN_BATCH_ROW','OPEN_BATCH_HEAD')),
                record_id TEXT NOT NULL UNIQUE,
                payload_json TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                previous_hash TEXT NOT NULL,
                entry_hash TEXT NOT NULL UNIQUE
            );
            CREATE TABLE premarket_runs (
                run_id TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                requested_count INTEGER NOT NULL CHECK(requested_count=10),
                available_count INTEGER NOT NULL CHECK(available_count BETWEEN 0 AND 10),
                source_batch_purpose TEXT,
                source_batch_id TEXT,
                source_batch_hash TEXT,
                head_sequence INTEGER NOT NULL UNIQUE,
                head_hash TEXT NOT NULL UNIQUE,
                content_hash TEXT NOT NULL,
                CHECK(
                    (
                        source_batch_purpose IS NULL
                        AND source_batch_id IS NULL
                        AND source_batch_hash IS NULL
                    )
                    OR (
                        source_batch_purpose='PREMARKET_ACCOUNT'
                        AND source_batch_id IS NOT NULL
                        AND source_batch_hash IS NOT NULL
                        AND length(source_batch_hash)=64
                        AND source_batch_hash NOT GLOB '*[^0-9a-f]*'
                    )
                ),
                UNIQUE(run_id,head_hash),
                FOREIGN KEY(head_sequence) REFERENCES ledger_entries(sequence),
                FOREIGN KEY(head_hash) REFERENCES ledger_entries(entry_hash)
            );
            CREATE TABLE premarket_rows (
                row_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                research_rank INTEGER NOT NULL CHECK(research_rank BETWEEN 1 AND 10),
                preselection_id TEXT NOT NULL,
                strategy_hash TEXT NOT NULL,
                structure_json TEXT NOT NULL,
                candidate_json TEXT NOT NULL,
                evaluation_json TEXT NOT NULL,
                entry_sequence INTEGER NOT NULL UNIQUE,
                row_hash TEXT NOT NULL UNIQUE,
                content_hash TEXT NOT NULL,
                UNIQUE(run_id,research_rank),
                UNIQUE(run_id,preselection_id),
                UNIQUE(run_id,strategy_hash),
                UNIQUE(run_id,row_hash),
                FOREIGN KEY(run_id) REFERENCES premarket_runs(run_id),
                FOREIGN KEY(entry_sequence) REFERENCES ledger_entries(sequence),
                FOREIGN KEY(row_hash) REFERENCES ledger_entries(entry_hash)
            );
            CREATE TABLE open_observations (
                observation_id TEXT PRIMARY KEY,
                parent_run_id TEXT NOT NULL,
                parent_head_hash TEXT NOT NULL,
                parent_row_hash TEXT NOT NULL,
                premarket_rank INTEGER NOT NULL CHECK(premarket_rank BETWEEN 1 AND 10),
                preselection_id TEXT NOT NULL,
                strategy_hash TEXT NOT NULL,
                observed_at TEXT NOT NULL,
                structure_json TEXT NOT NULL,
                candidate_json TEXT NOT NULL,
                evaluation_json TEXT NOT NULL,
                quote_batch_id TEXT,
                oldest_quote_asof TEXT,
                entry_sequence INTEGER NOT NULL UNIQUE,
                observation_hash TEXT NOT NULL UNIQUE,
                content_hash TEXT NOT NULL,
                batch_id TEXT,
                batch_head_hash TEXT,
                scheduled_for TEXT,
                FOREIGN KEY(parent_run_id,parent_head_hash)
                    REFERENCES premarket_runs(run_id,head_hash),
                FOREIGN KEY(parent_run_id,parent_row_hash)
                    REFERENCES premarket_rows(run_id,row_hash),
                FOREIGN KEY(entry_sequence) REFERENCES ledger_entries(sequence),
                FOREIGN KEY(observation_hash) REFERENCES ledger_entries(entry_hash)
            );
            CREATE INDEX open_observations_parent_idx
                ON open_observations(parent_run_id,parent_row_hash,entry_sequence);
            CREATE TABLE open_batches (
                batch_id TEXT PRIMARY KEY,
                parent_run_id TEXT NOT NULL,
                parent_head_hash TEXT NOT NULL,
                scheduled_for TEXT NOT NULL,
                observed_at TEXT NOT NULL,
                quote_batch_id TEXT NOT NULL,
                quote_asof TEXT NOT NULL,
                available_count INTEGER NOT NULL CHECK(available_count BETWEEN 0 AND 10),
                blockers_json TEXT NOT NULL,
                source_batch_purpose TEXT,
                source_batch_id TEXT,
                source_batch_hash TEXT,
                head_sequence INTEGER NOT NULL UNIQUE,
                head_hash TEXT NOT NULL UNIQUE,
                content_hash TEXT NOT NULL,
                CHECK(
                    (
                        source_batch_purpose IS NULL
                        AND source_batch_id IS NULL
                        AND source_batch_hash IS NULL
                    )
                    OR (
                        source_batch_purpose='OPEN_REPRICE'
                        AND source_batch_id IS NOT NULL
                        AND source_batch_hash IS NOT NULL
                        AND length(source_batch_hash)=64
                        AND source_batch_hash NOT GLOB '*[^0-9a-f]*'
                    )
                ),
                FOREIGN KEY(parent_run_id,parent_head_hash)
                    REFERENCES premarket_runs(run_id,head_hash),
                FOREIGN KEY(head_sequence) REFERENCES ledger_entries(sequence),
                FOREIGN KEY(head_hash) REFERENCES ledger_entries(entry_hash)
            );
            CREATE INDEX open_batches_parent_idx
                ON open_batches(parent_run_id,head_sequence);
            PRAGMA user_version=3;
            COMMIT;
            """
        )
        self._install_triggers()

    def _validate_schema(self) -> None:
        required = {
            "ledger_entries": {"sequence", "entry_type", "record_id", "payload_json", "content_hash", "previous_hash", "entry_hash"},
            "premarket_runs": {"run_id", "created_at", "requested_count", "available_count", "source_batch_purpose", "source_batch_id", "source_batch_hash", "head_sequence", "head_hash", "content_hash"},
            "premarket_rows": {"row_id", "run_id", "research_rank", "preselection_id", "strategy_hash", "structure_json", "candidate_json", "evaluation_json", "entry_sequence", "row_hash", "content_hash"},
            "open_observations": {"observation_id", "parent_run_id", "parent_head_hash", "parent_row_hash", "premarket_rank", "preselection_id", "strategy_hash", "observed_at", "structure_json", "candidate_json", "evaluation_json", "quote_batch_id", "oldest_quote_asof", "entry_sequence", "observation_hash", "content_hash", "batch_id", "batch_head_hash", "scheduled_for"},
            "open_batches": {"batch_id", "parent_run_id", "parent_head_hash", "scheduled_for", "observed_at", "quote_batch_id", "quote_asof", "available_count", "blockers_json", "source_batch_purpose", "source_batch_id", "source_batch_hash", "head_sequence", "head_hash", "content_hash"},
        }
        for table, columns in required.items():
            actual = {
                str(row[1])
                for row in self._connection.execute(f"PRAGMA table_info({table})")
            }
            if not columns <= actual:
                raise RuntimeError(f"malformed preselection table: {table}")
        self._install_triggers()

    def _install_triggers(self) -> None:
        statements: list[str] = []
        for table in _TABLES:
            statements.extend(
                (
                    f"CREATE TRIGGER IF NOT EXISTS {table}_no_update BEFORE UPDATE ON {table} BEGIN SELECT RAISE(ABORT, 'immutable preselection ledger: update forbidden'); END;",
                    f"CREATE TRIGGER IF NOT EXISTS {table}_no_delete BEFORE DELETE ON {table} BEGIN SELECT RAISE(ABORT, 'immutable preselection ledger: delete forbidden'); END;",
                )
            )
        for table, expected_purpose in (
            ("premarket_runs", PREMARKET_ACCOUNT_PURPOSE),
            ("open_batches", OPEN_REPRICE_PURPOSE),
        ):
            statements.append(
                f"""
                CREATE TRIGGER IF NOT EXISTS {table}_source_lineage_insert
                BEFORE INSERT ON {table}
                WHEN
                    ((NEW.source_batch_purpose IS NULL) != (NEW.source_batch_id IS NULL))
                    OR ((NEW.source_batch_id IS NULL) != (NEW.source_batch_hash IS NULL))
                    OR (
                        NEW.source_batch_purpose IS NOT NULL
                        AND NEW.source_batch_purpose != '{expected_purpose}'
                    )
                    OR (
                        NEW.source_batch_id IS NOT NULL
                        AND (
                            length(NEW.source_batch_id) NOT BETWEEN 1 AND 128
                            OR substr(NEW.source_batch_id,1,1) NOT GLOB '[A-Za-z0-9]'
                            OR NEW.source_batch_id GLOB '*[^A-Za-z0-9._:-]*'
                        )
                    )
                    OR (
                        NEW.source_batch_hash IS NOT NULL
                        AND (
                            length(NEW.source_batch_hash) != 64
                            OR NEW.source_batch_hash GLOB '*[^0-9a-f]*'
                        )
                    )
                BEGIN
                    SELECT RAISE(ABORT, 'invalid source batch lineage');
                END;
                """
            )
        self._connection.executescript("\n".join(statements))

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
            raise RuntimeError("preselection store is closed")


class LedgerBackedPreselectionProvider:
    """Strict supporting-only recovery provider backed only by this ledger.

    The provider has no producer, ranking, approval, bridge, instruction, or
    broker ports.  It restores already-frozen rows for display and reports the
    absent open-reprice producer explicitly through coverage.
    """

    SOURCE = "INDEPENDENT_TOP10_LEDGER"

    def __init__(
        self,
        store: NewsPreselectionStore,
        *,
        require_external_source_lineage: bool = False,
    ) -> None:
        if not isinstance(store, NewsPreselectionStore):
            raise TypeError("store must be a NewsPreselectionStore")
        if not isinstance(require_external_source_lineage, bool):
            raise TypeError("require_external_source_lineage must be a bool")
        self._store = store
        self._require_external_source_lineage = require_external_source_lineage

    @property
    def health(self) -> str:
        try:
            self.read_snapshot()
        except Exception:
            return "DEGRADED"
        return "READY"

    @property
    def health_reason(self) -> str | None:
        return (
            None
            if self.health == "READY"
            else "PRESELECTION_LEDGER_UNREADABLE"
        )

    def preselections(self) -> tuple[ConditionalOptionPreselection, ...]:
        return self.read_snapshot().preselections

    def lineage(self) -> dict[tuple[str, str], dict[str, object]]:
        snapshot = self.read_snapshot()
        return {key: dict(value) for key, value in snapshot.lineage.items()}

    def coverage(self) -> dict[str, object]:
        try:
            return dict(self.read_snapshot().coverage)
        except Exception:
            return _coverage_document(
                available_count=0,
                status="UNAVAILABLE",
                reason="PRESELECTION_LEDGER_UNREADABLE",
                run=None,
                open_count=0,
            )

    def ledger_status(self) -> dict[str, object]:
        try:
            coverage = dict(self.read_snapshot().coverage)
        except Exception:
            coverage = _coverage_document(
                available_count=0,
                status="UNAVAILABLE",
                reason="PRESELECTION_LEDGER_UNREADABLE",
                run=None,
                open_count=0,
            )
            return {**coverage, "health": "DEGRADED", "readable": False}
        return {**coverage, "health": "READY", "readable": True}

    def read_snapshot(self) -> LedgerPreselectionSnapshot:
        replay = self._store.latest_replay()
        if replay is None:
            return LedgerPreselectionSnapshot(
                (),
                {},
                _coverage_document(
                    available_count=0,
                    status="UNAVAILABLE",
                    reason="OPEN_REPRICE_PRODUCER_UNAVAILABLE",
                    ledger_reason="NO_PREMARKET_LEDGER_RUN",
                    run=None,
                    open_count=0,
                ),
            )

        run = replay.premarket
        if (
            run.requested_count != PREMARKET_LIMIT
            or run.available_count != len(run.rows)
        ):
            raise NewsPreselectionStoreCorruption(
                "latest pre-market count projection is invalid"
            )
        candidates: list[ConditionalOptionPreselection] = []
        lineage: dict[tuple[str, str], dict[str, object]] = {}
        parent_rows: dict[str, StoredPremarketRow] = {}
        for row in run.rows:
            candidate = _candidate_from_document(
                row.candidate,
                expected_phase=PreselectionPhase.PRE_MARKET,
            )
            if (
                row.run_id != run.run_id
                or candidate.preselection_id != row.preselection_id
                or candidate.strategy_hash != row.strategy_hash
                or row.row_hash in parent_rows
            ):
                raise NewsPreselectionStoreCorruption(
                    "pre-market candidate lineage is invalid"
                )
            key = (candidate.preselection_id, candidate.phase.value)
            if key in lineage:
                raise NewsPreselectionStoreCorruption(
                    "duplicate pre-market candidate lineage"
                )
            parent_rows[row.row_hash] = row
            candidates.append(candidate)
            lineage[key] = {
                "source": self.SOURCE,
                "preselection_id": candidate.preselection_id,
                "phase": candidate.phase.value,
                "run_id": run.run_id,
                "run_created_at": run.created_at.isoformat(),
                "head_hash": run.head_hash,
                "row_id": row.row_id,
                "row_hash": row.row_hash,
                "premarket_rank": row.research_rank,
                "source_batch_purpose": run.source_batch_purpose,
                "source_batch_id": run.source_batch_id,
                "source_batch_hash": run.source_batch_hash,
                "production_parent_eligible": row.production_parent_eligible,
                "production_parent_blocker": (
                    None
                    if row.production_parent_eligible
                    else "LEGACY_V1_IBKR_IDENTITY_INCOMPLETE"
                ),
            }

        external_source_reason: str | None = None
        if self._require_external_source_lineage and not _source_lineage_is_complete(
            run,
            expected_purpose=PREMARKET_ACCOUNT_PURPOSE,
        ):
            external_source_reason = "EXTERNAL_SOURCE_LINEAGE_INCOMPLETE"

        open_observations = replay.open_observations
        stored_open_batch: StoredOpenBatch | None = None
        open_batch_ids = {item.batch_id for item in open_observations}
        if open_observations and None not in open_batch_ids and len(open_batch_ids) == 1:
            try:
                stored_open_batch = self._store.read_open_batch(
                    str(next(iter(open_batch_ids)))
                )
            except (KeyError, NewsPreselectionStoreError):
                stored_open_batch = None
        if external_source_reason is not None:
            open_observations = ()
        elif self._require_external_source_lineage and open_observations:
            if (
                stored_open_batch is None
                or stored_open_batch.parent_run_id != run.run_id
                or stored_open_batch.parent_head_hash != run.head_hash
                or not _source_lineage_is_complete(
                    stored_open_batch,
                    expected_purpose=OPEN_REPRICE_PURPOSE,
                )
            ):
                external_source_reason = "EXTERNAL_SOURCE_LINEAGE_INCOMPLETE"
                open_observations = ()

        atomic_batch_reason: str | None = None
        if all(row.production_parent_eligible for row in run.rows) and open_observations:
            batch_ids = {item.batch_id for item in open_observations}
            batch_head_hashes = {item.batch_head_hash for item in open_observations}
            if (
                len(open_observations) != run.available_count
                or None in batch_ids
                or len(batch_ids) != 1
                or None in batch_head_hashes
                or len(batch_head_hashes) != 1
                or any(item.scheduled_for is None for item in open_observations)
            ):
                atomic_batch_reason = "OPEN_REPRICE_ATOMIC_BATCH_INCOMPLETE"
                open_observations = ()

        seen_open_rows: set[str] = set()
        for observation in open_observations:
            parent = parent_rows.get(observation.parent_row_hash)
            candidate = _candidate_from_document(
                observation.candidate,
                expected_phase=PreselectionPhase.OPEN_REPRICED,
            )
            if (
                parent is None
                or observation.parent_run_id != run.run_id
                or observation.parent_head_hash != run.head_hash
                or observation.parent_row_hash in seen_open_rows
                or observation.preselection_id != parent.preselection_id
                or observation.strategy_hash != parent.strategy_hash
                or candidate.preselection_id != observation.preselection_id
                or candidate.strategy_hash != observation.strategy_hash
                or observation.premarket_rank != parent.research_rank
            ):
                raise NewsPreselectionStoreCorruption(
                    "open candidate lineage is invalid"
                )
            key = (candidate.preselection_id, candidate.phase.value)
            if key in lineage:
                raise NewsPreselectionStoreCorruption(
                    "duplicate open candidate lineage"
                )
            seen_open_rows.add(observation.parent_row_hash)
            candidates.append(candidate)
            lineage[key] = {
                "source": self.SOURCE,
                "preselection_id": candidate.preselection_id,
                "phase": candidate.phase.value,
                "run_id": run.run_id,
                "run_created_at": run.created_at.isoformat(),
                "head_hash": run.head_hash,
                "row_id": parent.row_id,
                "row_hash": parent.row_hash,
                "premarket_rank": parent.research_rank,
                "observation_id": observation.observation_id,
                "observed_at": observation.observed_at.isoformat(),
                "observation_hash": observation.observation_hash,
                "batch_id": observation.batch_id,
                "batch_head_hash": observation.batch_head_hash,
                "scheduled_for": (
                    None
                    if observation.scheduled_for is None
                    else observation.scheduled_for.isoformat()
                ),
                "quote_batch_id": observation.evaluation.get("quote_batch_id"),
                "source_batch_purpose": (
                    None
                    if stored_open_batch is None
                    else stored_open_batch.source_batch_purpose
                ),
                "source_batch_id": (
                    None
                    if stored_open_batch is None
                    else stored_open_batch.source_batch_id
                ),
                "source_batch_hash": (
                    None
                    if stored_open_batch is None
                    else stored_open_batch.source_batch_hash
                ),
            }

        open_count = len(open_observations)
        producer_status = "UNAVAILABLE"
        reason: str | None = "OPEN_REPRICE_PRODUCER_UNAVAILABLE"
        if external_source_reason is not None:
            status, ledger_reason = "UNAVAILABLE", external_source_reason
        elif any(not row.production_parent_eligible for row in run.rows):
            status, ledger_reason = (
                "UNAVAILABLE",
                "LEGACY_V1_IBKR_IDENTITY_INCOMPLETE",
            )
        elif atomic_batch_reason is not None:
            status, ledger_reason = "UNAVAILABLE", atomic_batch_reason
        elif run.available_count == 0:
            status, ledger_reason = "UNAVAILABLE", "PREMARKET_LEDGER_RUN_EMPTY"
        elif open_count < run.available_count:
            status, ledger_reason = "PARTIAL", None
            producer_status = "NOT_STARTED"
            reason = "OPEN_REPRICE_NOT_STARTED"
        elif run.available_count < PREMARKET_LIMIT:
            status, ledger_reason = "PARTIAL", "TOP10_PREMARKET_COVERAGE_INCOMPLETE"
            producer_status = "AVAILABLE"
            reason = "TOP10_PREMARKET_COVERAGE_INCOMPLETE"
        else:
            status, ledger_reason = "AVAILABLE", None
            producer_status = "AVAILABLE"
            reason = None
        coverage = _coverage_document(
            available_count=run.available_count,
            status=status,
            reason=reason,
            ledger_reason=ledger_reason,
            run=run,
            open_count=open_count,
            producer_status=producer_status,
            open_batch=(open_observations[0] if open_observations else None),
        )
        return LedgerPreselectionSnapshot(tuple(candidates), lineage, coverage)


def _validated_structure(
    candidate: ConditionalOptionPreselection,
) -> dict[str, object]:
    identity = _structure_identity(candidate)
    if not candidate.legs:
        raise ValueError("a frozen pre-market structure requires at least one leg")
    seen_con_ids: set[int] = set()
    for index, leg in enumerate(candidate.legs, start=1):
        required = {
            "con_id": leg.con_id,
            "local_symbol": leg.local_symbol,
            "trading_class": leg.trading_class,
            "multiplier": leg.multiplier,
            "exchange": leg.exchange,
            "expiry": leg.expiry,
            "strike": leg.strike,
            "right": leg.right,
            "side": leg.side,
            "ratio": leg.ratio,
            "quantity": leg.quantity,
        }
        if any(value is None for value in required.values()):
            missing = ",".join(name for name, value in required.items() if value is None)
            raise ValueError(f"leg {index} structure identity is incomplete: {missing}")
        assert leg.con_id is not None
        if leg.con_id in seen_con_ids:
            raise ValueError("frozen structure contains duplicate con_id")
        seen_con_ids.add(leg.con_id)
        if leg.underlying != candidate.underlying:
            raise ValueError("leg underlying does not match candidate underlying")
    expected = strategy_structure_hash(
        candidate.underlying,
        candidate.strategy_type,
        candidate.legs,
    )
    if candidate.strategy_hash != expected:
        raise ValueError("candidate strategy_hash does not match its structure")
    return identity


def _structure_identity(
    candidate: ConditionalOptionPreselection,
) -> dict[str, object]:
    return {
        "schema": "options_copilot.conditional_option_strategy.v2",
        "underlying": candidate.underlying,
        "strategy_type": candidate.strategy_type,
        "legs": [
            {
                "con_id": leg.con_id,
                "local_symbol": leg.local_symbol,
                "trading_class": leg.trading_class,
                "multiplier": leg.multiplier,
                "exchange": leg.exchange,
                # Preserve canonical date/decimal tags.  Converting either to
                # a plain string would produce a different strategy hash from
                # ``strategy_structure_hash`` even though it displays alike.
                "expiry": leg.expiry,
                "strike": leg.strike,
                "right": None if leg.right is None else leg.right.value,
                "side": None if leg.side is None else leg.side.value,
                "ratio": leg.ratio,
                "quantity": leg.quantity,
            }
            for leg in candidate.legs
        ],
    }


def _legacy_structure_identity(
    candidate: ConditionalOptionPreselection,
) -> dict[str, object]:
    return {
        "schema": "options_copilot.conditional_option_strategy.v1",
        "underlying": candidate.underlying,
        "strategy_type": candidate.strategy_type,
        "legs": [
            {
                "underlying": leg.underlying,
                "con_id": leg.con_id,
                "expiry": leg.expiry,
                "strike": leg.strike,
                "right": None if leg.right is None else leg.right.value,
                "side": None if leg.side is None else leg.side.value,
                "ratio": leg.ratio,
                "quantity": leg.quantity,
            }
            for leg in candidate.legs
        ],
    }


def _contract_ref_from_identity(value: Mapping[str, object]) -> OptionContractRef:
    return OptionContractRef(
        con_id=value["con_id"],  # type: ignore[arg-type]
        local_symbol=value["local_symbol"],  # type: ignore[arg-type]
        trading_class=value["trading_class"],  # type: ignore[arg-type]
        multiplier=value["multiplier"],  # type: ignore[arg-type]
        exchange=value["exchange"],  # type: ignore[arg-type]
        expiry=value["expiry"],  # type: ignore[arg-type]
        strike=value["strike"],  # type: ignore[arg-type]
        right=value["right"],  # type: ignore[arg-type]
    )


def _structure_hash_from_identity(identity: Mapping[str, object]) -> str:
    legs = identity.get("legs")
    if not isinstance(legs, list):
        raise NewsPreselectionStoreCorruption("stored structure legs are invalid")
    schema = identity.get("schema")
    if schema not in {
        "options_copilot.conditional_option_strategy.v1",
        "options_copilot.conditional_option_strategy.v2",
    }:
        raise NewsPreselectionStoreCorruption("stored structure schema is invalid")
    return canonical_hash(
        {
            "schema": schema,
            "underlying": identity.get("underlying"),
            "strategy_type": identity.get("strategy_type"),
            "legs": legs,
        }
    )


def _evaluation_document(
    value: EvaluatedPreselection,
    *,
    batch_blockers: Sequence[str] = (),
) -> dict[str, object]:
    blockers = tuple(dict.fromkeys((*value.blockers, *batch_blockers)))
    eligible = not blockers
    return {
        "blockers": list(blockers),
        "quote_batch_id": value.quote_batch_id,
        "oldest_quote_asof": (
            None
            if value.oldest_quote_asof is None
            else value.oldest_quote_asof.isoformat()
        ),
        "maximum_quote_age_seconds": (
            None
            if value.maximum_quote_age_seconds is None
            else str(value.maximum_quote_age_seconds)
        ),
        "risk_adjusted_ev": (
            None if value.risk_adjusted_ev is None else str(value.risk_adjusted_ev)
        ),
        "action_pool_eligible": eligible,
        "research_only": not eligible,
        **_AUTHORITY_FIELDS,
    }


def _evaluation_is_self_consistent(value: Mapping[str, object]) -> bool:
    blockers = value.get("blockers")
    eligible = value.get("action_pool_eligible")
    return bool(
        isinstance(blockers, list)
        and isinstance(eligible, bool)
        and eligible is (not blockers)
        and value.get("research_only") is (not eligible)
        and all(value.get(name) == expected for name, expected in _AUTHORITY_FIELDS.items())
    )


def _entry(
    sequence: int,
    entry_type: str,
    record_id: str,
    payload: Mapping[str, object],
    previous_hash: str,
) -> dict[str, object]:
    payload_json = canonical_json(payload)
    content_hash = canonical_hash(payload)
    return {
        "sequence": sequence,
        "entry_type": entry_type,
        "record_id": record_id,
        "payload_json": payload_json,
        "content_hash": content_hash,
        "previous_hash": previous_hash,
        "entry_hash": _chain_hash(
            sequence,
            entry_type,
            record_id,
            content_hash,
            previous_hash,
        ),
    }


def _chain_hash(
    sequence: int,
    entry_type: str,
    record_id: str,
    content_hash: str,
    previous_hash: str,
) -> str:
    return hashlib.sha256(
        f"{sequence}:{entry_type}:{record_id}:{content_hash}:{previous_hash}".encode(
            "utf-8"
        )
    ).hexdigest()


def _premarket_row_id(run_id: str, preselection_id: str) -> str:
    digest = hashlib.sha256(f"{run_id}\0{preselection_id}".encode("utf-8")).hexdigest()
    return f"pmr_{digest}"


def _open_batch_row_id(batch_id: str, preselection_id: str) -> str:
    digest = hashlib.sha256(
        f"{batch_id}\0{preselection_id}".encode("utf-8")
    ).hexdigest()
    return f"obr_{digest}"


def _open_batch_head_document(
    *,
    batch_id: str,
    run: sqlite3.Row,
    scheduled_for: datetime,
    observed_at: datetime,
    evaluated: EvaluatedPreselectionBatch,
    rows: Sequence[Mapping[str, object]],
    source_batch_purpose: str | None,
    source_batch_id: str | None,
    source_batch_hash: str | None,
) -> dict[str, object]:
    assert evaluated.quote_batch_id is not None
    assert evaluated.quote_asof is not None
    return {
        "schema": "options_copilot.news_open_batch_head.v3",
        "batch_id": batch_id,
        "parent_run_id": str(run["run_id"]),
        "parent_head_hash": str(run["head_hash"]),
        "scheduled_for": datetime_text(scheduled_for),
        "observed_at": datetime_text(observed_at),
        "quote_batch_id": evaluated.quote_batch_id,
        "quote_asof": datetime_text(evaluated.quote_asof),
        "available_count": len(rows),
        "blockers": list(evaluated.blockers),
        "source_batch_purpose": source_batch_purpose,
        "source_batch_id": source_batch_id,
        "source_batch_hash": source_batch_hash,
        "outcome": evaluated.outcome,
        "action_pool_eligible": evaluated.action_pool_eligible,
        "rows": [
            {
                "observation_id": str(item["observation_id"]),
                "observation_hash": str(item["entry_hash"]),
                "parent_row_hash": str(item["parent_row_hash"]),
                "premarket_rank": int(item["premarket_rank"]),
                "preselection_id": str(item["preselection_id"]),
                "strategy_hash": str(item["strategy_hash"]),
            }
            for item in rows
        ],
        **_AUTHORITY_FIELDS,
    }


def _canonical_sql_json(row: sqlite3.Row, column: str) -> dict[str, Any]:
    try:
        value = json.loads(str(row[column]))
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise NewsPreselectionStoreCorruption(
            f"invalid canonical JSON column: {column}"
        ) from exc
    if not isinstance(value, dict) or canonical_json(value) != str(row[column]):
        raise NewsPreselectionStoreCorruption(
            f"non-canonical JSON column: {column}"
        )
    return value


def _mapping_copy(value: Mapping[str, object]) -> dict[str, object]:
    thawed = thaw_json(value)
    if not isinstance(thawed, dict):
        raise TypeError("stored read model must be an object")
    return thawed


def _identifier(field: str, value: object) -> str:
    if not isinstance(value, str) or _IDENTIFIER_RE.fullmatch(value) is None:
        raise ValueError(f"{field} is not a valid identifier")
    return value


def _digest(field: str, value: object) -> str:
    if not isinstance(value, str) or _HASH_RE.fullmatch(value) is None:
        raise ValueError(f"{field} must be a lowercase SHA-256 digest")
    return value


def _source_batch_binding(
    source_batch_purpose: object,
    source_batch_id: object,
    source_batch_hash: object,
    *,
    expected_purpose: str,
) -> tuple[str | None, str | None, str | None]:
    if (
        source_batch_purpose is None
        and source_batch_id is None
        and source_batch_hash is None
    ):
        return None, None, None
    if (
        source_batch_purpose is None
        or source_batch_id is None
        or source_batch_hash is None
    ):
        raise ValueError("source batch purpose, ID, and hash must be provided together")
    if source_batch_purpose != expected_purpose:
        raise ValueError(
            f"source_batch_purpose must be {expected_purpose}"
        )
    if (
        not isinstance(source_batch_id, str)
        or _SOURCE_BATCH_ID_RE.fullmatch(source_batch_id) is None
    ):
        raise ValueError("source_batch_id is not a valid external batch identifier")
    return (
        expected_purpose,
        source_batch_id,
        _digest("source_batch_hash", source_batch_hash),
    )


def _stored_source_batch_binding(
    row: sqlite3.Row,
    *,
    expected_purpose: str,
) -> tuple[str | None, str | None, str | None]:
    try:
        return _source_batch_binding(
            row["source_batch_purpose"],
            row["source_batch_id"],
            row["source_batch_hash"],
            expected_purpose=expected_purpose,
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise NewsPreselectionStoreCorruption(
            "stored source batch lineage is invalid"
        ) from exc


def _source_lineage_is_complete(
    value: object,
    *,
    expected_purpose: str,
) -> bool:
    return bool(
        getattr(value, "source_batch_purpose", None) == expected_purpose
        and _SOURCE_BATCH_ID_RE.fullmatch(
            str(getattr(value, "source_batch_id", "") or "")
        )
        is not None
        and _HASH_RE.fullmatch(
            str(getattr(value, "source_batch_hash", "") or "")
        )
        is not None
    )


def _timestamp(value: object) -> datetime:
    return utc_datetime(datetime.fromisoformat(str(value)))


def _candidate_from_document(
    value: Mapping[str, object],
    *,
    expected_phase: PreselectionPhase,
) -> ConditionalOptionPreselection:
    """Strictly reconstruct a typed candidate from the immutable document."""

    required_keys = {
        "preselection_id",
        "underlying",
        "strategy_type",
        "phase",
        "legs",
        "risk_defined",
        "maximum_loss_usd",
        "estimated_cost_usd",
        "cost_after_ev_usd",
        "entry_condition",
        "invalidation_condition",
        "profit_target_condition",
        "stop_loss_condition",
        "evidence_ids",
        "evidence_hashes",
        "strategy_hash",
        "research_summary",
        "decision_authority",
        "approval_eligible",
        "instruction_creation_allowed",
    }
    optional_keys = {
        "terminal_scenarios",
        "scenario_asof",
        "scenario_hash",
        "execution_cost_contract_version",
        "execution_cost_contract_hash",
        "risk_policy_version",
        "risk_policy_hash",
        "broker_snapshot_hash",
        "account_snapshot_hash",
        "strategy_nav_usd",
        "strategy_nav_post_hash",
        "economics_quote_batch_id",
        "economics_quote_asof",
        "ranking_snapshot_id",
        "ranking_candidate_hash",
        "payoff_hash",
        "economics_calculation_hash",
        "debit_usd",
        "credit_usd",
        "net_entry_cost_usd",
        "estimated_commission_usd",
        "estimated_entry_slippage_usd",
        "estimated_exit_slippage_usd",
        "estimated_slippage_usd",
        "expected_value_before_costs_usd",
        "risk_fraction",
        "underlying_quote_basis",
        "underlying_quote_basis_hash",
    }
    document = _mapping_copy(value)
    keys = set(document)
    if not required_keys.issubset(keys) or not keys.issubset(
        required_keys | optional_keys
    ):
        raise NewsPreselectionStoreCorruption(
            "stored preselection fields are invalid"
        )
    if (
        document.get("decision_authority") != "SUPPORTING_ONLY"
        or document.get("approval_eligible") is not False
        or document.get("instruction_creation_allowed") is not False
        or document.get("phase") != expected_phase.value
    ):
        raise NewsPreselectionStoreCorruption(
            "stored preselection authority or phase is invalid"
        )
    legs_value = document.get("legs")
    evidence_ids = document.get("evidence_ids")
    evidence_hashes = document.get("evidence_hashes")
    if (
        not isinstance(legs_value, list)
        or not isinstance(evidence_ids, list)
        or not all(isinstance(item, str) for item in evidence_ids)
        or not isinstance(evidence_hashes, list)
        or not all(isinstance(item, str) for item in evidence_hashes)
        or not isinstance(document.get("risk_defined"), bool)
    ):
        raise NewsPreselectionStoreCorruption(
            "stored preselection collection or bool type is invalid"
        )
    for name in (
        "preselection_id",
        "underlying",
        "strategy_type",
        "strategy_hash",
        "research_summary",
    ):
        if not isinstance(document.get(name), str):
            raise NewsPreselectionStoreCorruption(
                f"stored preselection {name} type is invalid"
            )
    for name in (
        "entry_condition",
        "invalidation_condition",
        "profit_target_condition",
        "stop_loss_condition",
    ):
        if document.get(name) is not None and not isinstance(document.get(name), str):
            raise NewsPreselectionStoreCorruption(
                f"stored preselection {name} type is invalid"
            )
    try:
        candidate = ConditionalOptionPreselection(
            preselection_id=str(document["preselection_id"]),
            underlying=str(document["underlying"]),
            strategy_type=str(document["strategy_type"]),
            phase=expected_phase,
            legs=tuple(_leg_from_document(item) for item in legs_value),
            risk_defined=document["risk_defined"],
            maximum_loss_usd=_optional_decimal(
                document["maximum_loss_usd"], "maximum_loss_usd"
            ),
            estimated_cost_usd=_optional_decimal(
                document["estimated_cost_usd"], "estimated_cost_usd"
            ),
            cost_after_ev_usd=_optional_decimal(
                document["cost_after_ev_usd"], "cost_after_ev_usd"
            ),
            entry_condition=document["entry_condition"],
            invalidation_condition=document["invalidation_condition"],
            profit_target_condition=document["profit_target_condition"],
            stop_loss_condition=document["stop_loss_condition"],
            evidence_ids=tuple(evidence_ids),
            evidence_hashes=tuple(evidence_hashes),
            strategy_hash=str(document["strategy_hash"]),
            research_summary=str(document["research_summary"]),
            underlying_quote_basis=_underlying_quote_basis_from_document(
                document.get("underlying_quote_basis")
            ),
            underlying_quote_basis_hash=_optional_text(
                document.get("underlying_quote_basis_hash")
            ),
            terminal_scenarios=_terminal_scenarios_from_document(
                document.get("terminal_scenarios")
            ),
            scenario_asof=_optional_datetime(
                document.get("scenario_asof"), "scenario_asof"
            ),
            scenario_hash=_optional_text(document.get("scenario_hash")),
            execution_cost_contract_version=_optional_text(
                document.get("execution_cost_contract_version")
            ),
            execution_cost_contract_hash=_optional_text(
                document.get("execution_cost_contract_hash")
            ),
            risk_policy_version=_optional_text(
                document.get("risk_policy_version")
            ),
            risk_policy_hash=_optional_text(document.get("risk_policy_hash")),
            broker_snapshot_hash=_optional_text(
                document.get("broker_snapshot_hash")
            ),
            account_snapshot_hash=_optional_text(
                document.get("account_snapshot_hash")
            ),
            strategy_nav_usd=_optional_decimal(
                document.get("strategy_nav_usd"), "strategy_nav_usd"
            ),
            strategy_nav_post_hash=_optional_text(
                document.get("strategy_nav_post_hash")
            ),
            economics_quote_batch_id=_optional_text(
                document.get("economics_quote_batch_id")
            ),
            economics_quote_asof=_optional_datetime(
                document.get("economics_quote_asof"), "economics_quote_asof"
            ),
            ranking_snapshot_id=_optional_text(
                document.get("ranking_snapshot_id")
            ),
            ranking_candidate_hash=_optional_text(
                document.get("ranking_candidate_hash")
            ),
            payoff_hash=_optional_text(document.get("payoff_hash")),
            economics_calculation_hash=_optional_text(
                document.get("economics_calculation_hash")
            ),
            debit_usd=_optional_decimal(document.get("debit_usd"), "debit_usd"),
            credit_usd=_optional_decimal(document.get("credit_usd"), "credit_usd"),
            net_entry_cost_usd=_optional_decimal(
                document.get("net_entry_cost_usd"), "net_entry_cost_usd"
            ),
            estimated_commission_usd=_optional_decimal(
                document.get("estimated_commission_usd"),
                "estimated_commission_usd",
            ),
            estimated_entry_slippage_usd=_optional_decimal(
                document.get("estimated_entry_slippage_usd"),
                "estimated_entry_slippage_usd",
            ),
            estimated_exit_slippage_usd=_optional_decimal(
                document.get("estimated_exit_slippage_usd"),
                "estimated_exit_slippage_usd",
            ),
            estimated_slippage_usd=_optional_decimal(
                document.get("estimated_slippage_usd"),
                "estimated_slippage_usd",
            ),
            expected_value_before_costs_usd=_optional_decimal(
                document.get("expected_value_before_costs_usd"),
                "expected_value_before_costs_usd",
            ),
            risk_fraction=_optional_decimal(
                document.get("risk_fraction"), "risk_fraction"
            ),
        )
        if all(leg.contract_ref is not None for leg in candidate.legs):
            _validated_structure(candidate)
        elif candidate.strategy_hash != canonical_hash(_legacy_structure_identity(candidate)):
            raise ValueError("legacy candidate strategy_hash mismatch")
    except (TypeError, ValueError, InvalidOperation) as exc:
        raise NewsPreselectionStoreCorruption(
            "stored preselection cannot be reconstructed"
        ) from exc
    return candidate


def _underlying_quote_basis_from_document(
    value: object,
) -> UnderlyingQuoteBasis | None:
    if value is None:
        return None
    fields = {
        "symbol",
        "contract_id",
        "exchange",
        "source",
        "observed_at",
        "bid",
        "ask",
        "last",
        "close",
        "market_data_type",
        "schema",
    }
    if not isinstance(value, Mapping) or set(value) != fields:
        raise ValueError("underlying quote basis fields are invalid")
    if value["schema"] != UNDERLYING_QUOTE_BASIS_SCHEMA:
        raise ValueError("underlying quote basis schema is invalid")
    contract_id = value["contract_id"]
    market_data_type = value["market_data_type"]
    if isinstance(contract_id, bool) or not isinstance(contract_id, int):
        raise TypeError("underlying quote contract_id must be an integer")
    if isinstance(market_data_type, bool) or not isinstance(market_data_type, int):
        raise TypeError("underlying quote market_data_type must be an integer")
    observed_at = _optional_datetime(value["observed_at"], "underlying observed_at")
    close = _optional_decimal(value["close"], "underlying close")
    if observed_at is None or close is None:
        raise ValueError("underlying quote basis is incomplete")
    return UnderlyingQuoteBasis(
        symbol=str(value["symbol"]),
        contract_id=contract_id,
        exchange=str(value["exchange"]),
        source=str(value["source"]),
        observed_at=observed_at,
        bid=_optional_decimal(value["bid"], "underlying bid"),
        ask=_optional_decimal(value["ask"], "underlying ask"),
        last=_optional_decimal(value["last"], "underlying last"),
        close=close,
        market_data_type=market_data_type,
    )


def _terminal_scenarios_from_document(
    value: object,
) -> tuple[PreselectionTerminalScenario, ...]:
    if value is None:
        return ()
    if not isinstance(value, Sequence) or isinstance(
        value, (str, bytes, bytearray, memoryview)
    ):
        raise ValueError("terminal_scenarios must be an array")
    rows: list[PreselectionTerminalScenario] = []
    for item in value:
        if not isinstance(item, Mapping) or set(item) != {
            "terminal_underlying_price",
            "probability",
        }:
            raise ValueError("terminal scenario fields are invalid")
        rows.append(
            PreselectionTerminalScenario(
                terminal_underlying_price=_required_decimal(
                    item["terminal_underlying_price"],
                    "terminal_underlying_price",
                ),
                probability=_required_decimal(item["probability"], "probability"),
            )
        )
    return tuple(rows)


def _leg_from_document(value: object) -> ConditionalOptionLeg:
    legacy_keys = {
        "underlying",
        "con_id",
        "expiry",
        "strike",
        "right",
        "side",
        "ratio",
        "quantity",
        "bid",
        "ask",
        "quote_asof",
        "quote_batch_id",
        "implied_volatility",
        "delta",
        "gamma",
        "theta",
        "vega",
        "volume",
        "open_interest",
        "dte",
    }
    v2_keys = legacy_keys | {
        "local_symbol",
        "trading_class",
        "multiplier",
        "exchange",
    }
    if not isinstance(value, dict) or frozenset(value) not in {
        frozenset(legacy_keys),
        frozenset(v2_keys),
    }:
        raise NewsPreselectionStoreCorruption("stored option leg fields are invalid")
    for name in ("underlying", "quote_batch_id", "local_symbol", "trading_class", "exchange"):
        if value.get(name) is not None and not isinstance(value.get(name), str):
            raise NewsPreselectionStoreCorruption(
                f"stored option leg {name} type is invalid"
            )
    for name in ("con_id", "ratio", "quantity", "volume", "open_interest", "dte", "multiplier"):
        item = value.get(name)
        if item is not None and (isinstance(item, bool) or not isinstance(item, int)):
            raise NewsPreselectionStoreCorruption(
                f"stored option leg {name} type is invalid"
            )
    try:
        expiry = _optional_date(value.get("expiry"), "expiry")
        quote_asof = _optional_datetime(value.get("quote_asof"), "quote_asof")
        right = _optional_enum(value.get("right"), OptionRight, "right")
        side = _optional_enum(value.get("side"), OptionLegSide, "side")
        return ConditionalOptionLeg(
            underlying=value["underlying"],
            con_id=value["con_id"],
            expiry=expiry,
            strike=_optional_decimal(value["strike"], "strike"),
            right=right,
            side=side,
            ratio=value["ratio"],
            quantity=value["quantity"],
            bid=_optional_decimal(value["bid"], "bid"),
            ask=_optional_decimal(value["ask"], "ask"),
            quote_asof=quote_asof,
            quote_batch_id=value["quote_batch_id"],
            implied_volatility=_optional_decimal(
                value["implied_volatility"], "implied_volatility"
            ),
            delta=_optional_decimal(value["delta"], "delta"),
            gamma=_optional_decimal(value["gamma"], "gamma"),
            theta=_optional_decimal(value["theta"], "theta"),
            vega=_optional_decimal(value["vega"], "vega"),
            volume=value["volume"],
            open_interest=value["open_interest"],
            dte=value["dte"],
            local_symbol=value.get("local_symbol"),
            trading_class=value.get("trading_class"),
            multiplier=value.get("multiplier"),
            exchange=value.get("exchange"),
        )
    except (TypeError, ValueError, InvalidOperation) as exc:
        raise NewsPreselectionStoreCorruption(
            "stored option leg cannot be reconstructed"
        ) from exc


def _optional_decimal(value: object, field: str) -> Decimal | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError(f"{field} must be a decimal string or None")
    parsed = Decimal(value)
    if not parsed.is_finite():
        raise ValueError(f"{field} must be finite")
    return parsed


def _required_decimal(value: object, field: str) -> Decimal:
    parsed = _optional_decimal(value, field)
    if parsed is None:
        raise ValueError(f"{field} is required")
    return parsed


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise TypeError("optional text must be a nonblank string or None")
    return value.strip()


def _optional_date(value: object, field: str) -> date | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError(f"{field} must be an ISO date string or None")
    parsed = date.fromisoformat(value)
    if parsed.isoformat() != value:
        raise ValueError(f"{field} must use canonical ISO date format")
    return parsed


def _optional_datetime(value: object, field: str) -> datetime | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError(f"{field} must be an ISO datetime string or None")
    parsed = datetime.fromisoformat(value)
    return utc_datetime(parsed, field=field)


def _optional_enum(value: object, enum_type: type, field: str):
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError(f"{field} must be an enum string or None")
    try:
        return enum_type(value)
    except ValueError as exc:
        raise ValueError(f"invalid {field}") from exc


def _coverage_document(
    *,
    available_count: int,
    status: str,
    reason: str | None,
    ledger_reason: str | None = None,
    run: StoredPremarketRun | None,
    open_count: int,
    producer_status: str = "UNAVAILABLE",
    open_batch: StoredOpenObservation | None = None,
) -> dict[str, object]:
    return {
        "requested_count": PREMARKET_LIMIT,
        "available_count": available_count,
        "source": LedgerBackedPreselectionProvider.SOURCE,
        "status": status,
        "reason": reason,
        "ledger_reason": ledger_reason,
        "latest_run_id": None if run is None else run.run_id,
        "latest_head_hash": None if run is None else run.head_hash,
        "freeze_slot": None if run is None else run.created_at.isoformat(),
        "open_count": open_count,
        "latest_open_batch_id": None if open_batch is None else open_batch.batch_id,
        "latest_open_batch_head_hash": (
            None if open_batch is None else open_batch.batch_head_hash
        ),
        "reprice_slot": (
            None
            if open_batch is None or open_batch.scheduled_for is None
            else open_batch.scheduled_for.isoformat()
        ),
        "open_reprice_producer_status": producer_status,
        "open_reprice_writer": OPEN_REPRICE_WRITER,
        **_AUTHORITY_FIELDS,
    }


__all__ = [
    "GENESIS_HASH",
    "LedgerBackedPreselectionProvider",
    "LedgerPreselectionSnapshot",
    "NewsPreselectionStore",
    "NewsPreselectionStoreConflict",
    "NewsPreselectionStoreCorruption",
    "NewsPreselectionStoreError",
    "PREMARKET_LIMIT",
    "PreselectionReplay",
    "SCHEMA_VERSION",
    "StoredOpenObservation",
    "StoredOpenBatch",
    "StoredPremarketRow",
    "StoredPremarketRun",
]
