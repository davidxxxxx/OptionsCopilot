"""Append-only point-in-time persistence for read-only news analyses.

The store freezes classifier output against the exact evidence, classifier
contract, and IBKR research inputs used to produce it.  It has no proposal,
approval, bridge, instruction, or order authority.
"""
from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import threading

from options_copilot.storage.canonical import canonical_hash, canonical_json

from .models import (
    AnalyzedNews,
    AnalysisStage,
    ClassifiedEvent,
    EventCategory,
    ImpactDirection,
    ImpactHorizon,
    MarketConfirmation,
    NewsAuthority,
    NewsInput,
    OptionTradabilityInput,
    ScoreBand,
)


SCHEMA_VERSION = 1
INPUT_CONTRACT_VERSION = "news-analysis-input.v1"
ANALYZER_CONTRACT_VERSION = "news-analysis-service.v1"
GENESIS_HASH = "0" * 64
_HASH_RE = re.compile(r"[0-9a-f]{64}\Z")
_SAFE_IDENTIFIER_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:/-]{0,255}\Z")
_SECRETISH_RE = re.compile(
    r"(?:bearer|credential|password|secret|token|(?:sk|pk)[_-][A-Za-z0-9_-]{8,})",
    re.IGNORECASE,
)


class NewsAnalysisStoreError(RuntimeError):
    """Base error for the point-in-time news-analysis ledger."""


class NewsAnalysisStoreCorruption(NewsAnalysisStoreError):
    """Raised when persisted analysis state cannot be trusted."""


@dataclass(frozen=True, slots=True)
class IntegrityVerificationProgress:
    """Bounded, process-local progress for a genesis ledger verification."""

    batch_rows: int
    verified_rows: int
    remaining_rows: int
    complete: bool


def analysis_store_path(evidence_path: str | Path) -> Path:
    """Return the dedicated analysis DB beside an evidence DB."""

    source = Path(evidence_path)
    suffix = source.suffix or ".sqlite3"
    stem = source.stem if source.suffix else source.name
    return source.with_name(f"{stem}.news-analysis{suffix}")


def analysis_contract(classifier: object) -> dict[str, object]:
    """Build a secret-free, versioned classifier/analyzer identity.

    Only type identity plus explicitly advertised safe ``contract_version``
    and ``model_id`` strings are observed.  Arbitrary object representation or
    classifier state is never serialized.
    """

    if not callable(getattr(classifier, "classify", None)):
        raise TypeError("news classifier must implement classify")
    return {
        "analyzer_id": "options_copilot.news.service.NewsAnalysisService",
        "analyzer_contract_version": ANALYZER_CONTRACT_VERSION,
        "classifier": _classifier_contract(classifier, seen=set()),
    }


def analysis_input_document(
    *,
    news: NewsInput,
    evidence_content_hashes: Sequence[str],
    analyzer_contract: Mapping[str, object],
    ibkr_binding: Mapping[str, object] | None,
) -> dict[str, object]:
    """Create the canonical fingerprint input for one analysis observation."""

    if not isinstance(news, NewsInput):
        raise TypeError("news must be a NewsInput")
    hashes = sorted(_digest(item, "evidence_content_hash") for item in evidence_content_hashes)
    if not hashes:
        raise ValueError("at least one evidence content hash is required")
    contract = _validate_analyzer_contract(analyzer_contract)
    binding = None if ibkr_binding is None else _validate_binding_document(ibkr_binding)
    base: dict[str, object] = {
        "input_contract_version": INPUT_CONTRACT_VERSION,
        "evidence_content_hashes": hashes,
        "analyzer_contract": contract,
        "news": _news_document(news),
        "ibkr_binding": binding,
    }
    return {**base, "fingerprint": canonical_hash(base)}


class NewsAnalysisStore:
    """SQLite WAL/FULL append-only ledger for immutable analyses."""

    def __init__(
        self,
        path: str | Path,
        *,
        defer_integrity_check: bool = False,
    ) -> None:
        if not isinstance(defer_integrity_check, bool):
            raise TypeError("defer_integrity_check must be a bool")
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._closed = False
        self._integrity_verified = False
        self._verification_next_sequence = 1
        self._verification_prior_hash = GENESIS_HASH
        self._verification_verified_rows = 0
        self._connection = sqlite3.connect(
            str(self.path),
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
            synchronous = int(
                self._connection.execute("PRAGMA synchronous").fetchone()[0]
            )
            self._synchronous = {
                0: "off",
                1: "normal",
                2: "full",
                3: "extra",
            }.get(synchronous, str(synchronous))
            self._migrate()
            if not defer_integrity_check:
                self.assert_integrity()
        except BaseException:
            self._connection.close()
            self._closed = True
            raise

    def __enter__(self) -> "NewsAnalysisStore":
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
    def integrity_verified(self) -> bool:
        self._ensure_open()
        with self._lock:
            return self._integrity_verified

    @property
    def count(self) -> int:
        self._ensure_open()
        with self._lock:
            return int(
                self._connection.execute(
                    "SELECT COUNT(*) FROM news_analysis_records"
                ).fetchone()[0]
            )

    def close(self) -> None:
        with self._lock:
            if not self._closed:
                self._connection.close()
                self._closed = True

    def resolve(
        self,
        input_document: Mapping[str, object],
        factory: Callable[[], AnalyzedNews],
    ) -> AnalyzedNews:
        """Load one exact analysis or append the factory's first result.

        The coordinator verifies the full chain once per rebuild; this method
        then verifies any loaded row and the append tail. Corruption therefore
        never falls through to ``factory`` and cannot be hidden by re-analysis.
        """

        if not callable(factory):
            raise TypeError("analysis factory must be callable")
        self._ensure_open()
        with self._lock:
            self._require_integrity_verified()
            normalized_input = _validate_input_document(input_document)
            fingerprint = str(normalized_input["fingerprint"])
            persisted = self._lookup_normalized(normalized_input)
            if persisted is not None:
                return persisted

            analysis = factory()
            if not isinstance(analysis, AnalyzedNews):
                raise TypeError("analysis factory must return AnalyzedNews")
            expected_news = _news_from_document(normalized_input["news"])
            if analysis.news != expected_news:
                raise ValueError("analysis factory result is not bound to the input news")
            document = _record_document(normalized_input, analysis)
            # Validate the complete schema and every input/output binding before
            # the immutable append.  A programming bug must not permanently
            # poison an append-only store and then fail only on read-back.
            validated_analysis = _checked_analysis_from_document(
                document,
                expected_input=normalized_input,
            )
            immutable_json = canonical_json(document)
            content_hash = hashlib.sha256(immutable_json.encode("utf-8")).hexdigest()
            with self._transaction():
                # ``factory`` intentionally runs outside the write transaction so
                # a slow classifier cannot hold the ledger's SQLite write lock.
                # Another store/process may have committed this fingerprint in
                # the meantime, so resolve the race while BEGIN IMMEDIATE still
                # excludes competing writers.  The winner is treated exactly as
                # a normal persisted read: both its chain binding and its full
                # immutable input/output document must validate before reuse.
                winner = self._connection.execute(
                    "SELECT * FROM news_analysis_records WHERE fingerprint=?",
                    (fingerprint,),
                ).fetchone()
                if winner is not None:
                    self._assert_row_integrity(winner)
                    return _checked_analysis_from_document(
                        _decode_row_document(winner),
                        expected_input=normalized_input,
                    )
                tail = self._connection.execute(
                    "SELECT * FROM news_analysis_records "
                    "ORDER BY sequence DESC LIMIT 1"
                ).fetchone()
                if tail is not None:
                    self._assert_row_integrity(tail)
                sequence = 1 if tail is None else int(tail["sequence"]) + 1
                prior_hash = GENESIS_HASH if tail is None else str(tail["row_hash"])
                row_hash = _row_hash(sequence, prior_hash, content_hash)
                self._connection.execute(
                    """INSERT INTO news_analysis_records(
                        sequence,fingerprint,analysis_id,analyzed_at,immutable_json,
                        content_hash,prior_hash,row_hash,decision_authority
                    ) VALUES(?,?,?,?,?,?,?,?,?)""",
                    (
                        sequence,
                        fingerprint,
                        analysis.analysis_id,
                        _time_text(analysis.analyzed_at),
                        immutable_json,
                        content_hash,
                        prior_hash,
                        row_hash,
                        "SUPPORTING_ONLY",
                    ),
                )
            return validated_analysis

    def lookup(
        self,
        input_document: Mapping[str, object],
    ) -> AnalyzedNews | None:
        """Return a validated persisted hit without invoking a classifier."""

        self._ensure_open()
        with self._lock:
            self._require_integrity_verified()
            return self._lookup_normalized(
                _validate_input_document(input_document)
            )

    def verify_integrity(self) -> bool:
        self.assert_integrity()
        return True

    def assert_integrity(self) -> None:
        self._ensure_open()
        with self._lock:
            self._reset_integrity_verification()
            try:
                while not self._integrity_verified:
                    self._verify_integrity_batch_locked(limit=512)
                self._assert_sqlite_integrity_locked()
            except BaseException:
                self._integrity_verified = False
                raise

    def verify_integrity_batch(
        self,
        limit: int,
    ) -> IntegrityVerificationProgress:
        """Verify at most ``limit`` rows from genesis and report progress.

        Progress is process-local and never restored from the database. This
        avoids treating a checkpoint from the ledger under inspection as proof
        that an earlier prefix was trustworthy.
        """

        if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
            raise ValueError("integrity verification limit must be a positive integer")
        self._ensure_open()
        with self._lock:
            if self._integrity_verified:
                return IntegrityVerificationProgress(
                    batch_rows=0,
                    verified_rows=self._verification_verified_rows,
                    remaining_rows=0,
                    complete=True,
                )
            try:
                return self._verify_integrity_batch_locked(limit=limit)
            except BaseException:
                self._integrity_verified = False
                raise

    def _verify_integrity_batch_locked(
        self,
        *,
        limit: int,
    ) -> IntegrityVerificationProgress:
        batch_rows = 0
        with self._transaction():
            rows = self._connection.execute(
                "SELECT * FROM news_analysis_records WHERE sequence>=? "
                "ORDER BY sequence LIMIT ?",
                (self._verification_next_sequence, limit),
            ).fetchall()
            for row in rows:
                self._verification_prior_hash = self._assert_integrity_row(
                    row,
                    expected_sequence=self._verification_next_sequence,
                    prior_hash=self._verification_prior_hash,
                )
                self._verification_next_sequence += 1
                self._verification_verified_rows += 1
                batch_rows += 1
            max_sequence = int(
                self._connection.execute(
                    "SELECT COALESCE(MAX(sequence), 0) "
                    "FROM news_analysis_records"
                ).fetchone()[0]
            )
            remaining_rows = max(
                0,
                max_sequence - self._verification_next_sequence + 1,
            )
            if remaining_rows == 0:
                self._integrity_verified = True
            return IntegrityVerificationProgress(
                batch_rows=batch_rows,
                verified_rows=self._verification_verified_rows,
                remaining_rows=remaining_rows,
                complete=self._integrity_verified,
            )

    def _assert_sqlite_integrity_locked(self) -> None:
        if self._connection.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise NewsAnalysisStoreCorruption("SQLite integrity check failed")

    def _assert_integrity_row(
        self,
        row: sqlite3.Row,
        *,
        expected_sequence: int,
        prior_hash: str,
    ) -> str:
        sequence = int(row["sequence"])
        if sequence != expected_sequence:
            raise NewsAnalysisStoreCorruption(
                "news analysis sequence contains a gap"
            )
        immutable_json = str(row["immutable_json"])
        document = _decode_row_document(row)
        if canonical_json(document) != immutable_json:
            raise NewsAnalysisStoreCorruption(
                f"immutable document mismatch at sequence {sequence}"
            )
        content_hash = hashlib.sha256(immutable_json.encode("utf-8")).hexdigest()
        if content_hash != str(row["content_hash"]):
            raise NewsAnalysisStoreCorruption(
                f"content hash mismatch at sequence {sequence}"
            )
        if str(row["prior_hash"]) != prior_hash:
            raise NewsAnalysisStoreCorruption(
                f"prior hash mismatch at sequence {sequence}"
            )
        expected_row_hash = _row_hash(sequence, prior_hash, content_hash)
        if str(row["row_hash"]) != expected_row_hash:
            raise NewsAnalysisStoreCorruption(
                f"row hash mismatch at sequence {sequence}"
            )
        analysis = _checked_analysis_from_document(document)
        if str(row["fingerprint"]) != str(document["fingerprint"]):
            raise NewsAnalysisStoreCorruption(
                f"fingerprint column mismatch at sequence {sequence}"
            )
        if str(row["analysis_id"]) != analysis.analysis_id:
            raise NewsAnalysisStoreCorruption(
                f"analysis ID column mismatch at sequence {sequence}"
            )
        if str(row["analyzed_at"]) != _time_text(analysis.analyzed_at):
            raise NewsAnalysisStoreCorruption(
                f"analysis time column mismatch at sequence {sequence}"
            )
        if str(row["decision_authority"]) != "SUPPORTING_ONLY":
            raise NewsAnalysisStoreCorruption(
                f"analysis authority mismatch at sequence {sequence}"
            )
        return expected_row_hash

    def _reset_integrity_verification(self) -> None:
        self._integrity_verified = False
        self._verification_next_sequence = 1
        self._verification_prior_hash = GENESIS_HASH
        self._verification_verified_rows = 0

    def _require_integrity_verified(self) -> None:
        if not self._integrity_verified:
            raise NewsAnalysisStoreError(
                "news analysis integrity verification is incomplete"
            )

    def _lookup_normalized(
        self,
        normalized_input: Mapping[str, object],
    ) -> AnalyzedNews | None:
        fingerprint = str(normalized_input["fingerprint"])
        row = self._connection.execute(
            "SELECT * FROM news_analysis_records WHERE fingerprint=?",
            (fingerprint,),
        ).fetchone()
        if row is None:
            return None
        self._assert_row_integrity(row)
        return _checked_analysis_from_document(
            _decode_row_document(row),
            expected_input=normalized_input,
        )

    def _assert_row_integrity(self, row: sqlite3.Row) -> None:
        """Verify one loaded row and its direct chain predecessor."""

        sequence = int(row["sequence"])
        predecessor = self._connection.execute(
            "SELECT row_hash FROM news_analysis_records WHERE sequence=?",
            (sequence - 1,),
        ).fetchone()
        expected_prior = (
            GENESIS_HASH if sequence == 1 else None if predecessor is None else str(predecessor[0])
        )
        if expected_prior is None or str(row["prior_hash"]) != expected_prior:
            raise NewsAnalysisStoreCorruption(
                f"prior hash mismatch at sequence {sequence}"
            )
        self._assert_integrity_row(
            row,
            expected_sequence=sequence,
            prior_hash=expected_prior,
        )

    def _migrate(self) -> None:
        version = int(self._connection.execute("PRAGMA user_version").fetchone()[0])
        if version > SCHEMA_VERSION:
            raise NewsAnalysisStoreError(
                f"news analysis schema {version} is newer than supported"
            )
        if version == 0:
            tables = self._connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name='news_analysis_records'"
            ).fetchall()
            if tables:
                raise NewsAnalysisStoreCorruption(
                    "unversioned news analysis schema is not trusted"
                )
            self._create_schema()
        else:
            self._validate_schema()

    def _create_schema(self) -> None:
        self._connection.executescript(
            """
            BEGIN IMMEDIATE;
            CREATE TABLE news_analysis_records (
                sequence INTEGER PRIMARY KEY,
                fingerprint TEXT NOT NULL UNIQUE,
                analysis_id TEXT NOT NULL,
                analyzed_at TEXT NOT NULL,
                immutable_json TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                prior_hash TEXT NOT NULL,
                row_hash TEXT NOT NULL UNIQUE,
                decision_authority TEXT NOT NULL
            );
            CREATE TRIGGER news_analysis_records_no_update
            BEFORE UPDATE ON news_analysis_records
            BEGIN SELECT RAISE(ABORT, 'immutable news analysis: update forbidden'); END;
            CREATE TRIGGER news_analysis_records_no_delete
            BEFORE DELETE ON news_analysis_records
            BEGIN SELECT RAISE(ABORT, 'immutable news analysis: delete forbidden'); END;
            PRAGMA user_version=1;
            COMMIT;
            """
        )

    def _validate_schema(self) -> None:
        expected_columns = {
            "sequence",
            "fingerprint",
            "analysis_id",
            "analyzed_at",
            "immutable_json",
            "content_hash",
            "prior_hash",
            "row_hash",
            "decision_authority",
        }
        columns = {
            str(row[1])
            for row in self._connection.execute(
                "PRAGMA table_info(news_analysis_records)"
            )
        }
        if columns != expected_columns:
            raise NewsAnalysisStoreCorruption("malformed news analysis schema")
        triggers = {
            str(row[0])
            for row in self._connection.execute(
                "SELECT name FROM sqlite_master WHERE type='trigger' "
                "AND tbl_name='news_analysis_records'"
            )
        }
        expected_triggers = {
            "news_analysis_records_no_update",
            "news_analysis_records_no_delete",
        }
        if triggers != expected_triggers:
            raise NewsAnalysisStoreCorruption(
                "news analysis append-only triggers are missing or unexpected"
            )

    @contextmanager
    def _transaction(self):
        self._ensure_open()
        self._lock.acquire()
        try:
            self._connection.execute("BEGIN IMMEDIATE")
            yield
            self._connection.execute("COMMIT")
        except BaseException:
            if self._connection.in_transaction:
                self._connection.execute("ROLLBACK")
            raise
        finally:
            self._lock.release()

    def _ensure_open(self) -> None:
        if self._closed:
            raise NewsAnalysisStoreError("news analysis store is closed")


def _record_document(
    input_document: Mapping[str, object],
    analysis: AnalyzedNews,
) -> dict[str, object]:
    return {
        "record_schema_version": SCHEMA_VERSION,
        "fingerprint": input_document["fingerprint"],
        "input": dict(input_document),
        "analysis": _analysis_document(analysis),
        "decision_authority": "SUPPORTING_ONLY",
        "approval_eligible": False,
        "instruction_creation_allowed": False,
        "order_creation_allowed": False,
    }


def _analysis_document(analysis: AnalyzedNews) -> dict[str, object]:
    return {
        "analysis_id": analysis.analysis_id,
        "analyzed_at": _time_text(analysis.analyzed_at),
        "classification": {
            "category": analysis.classification.category.value,
            "symbols": list(analysis.classification.symbols),
            "direction": analysis.classification.direction.value,
            "horizon": analysis.classification.horizon.value,
            "confidence": str(analysis.classification.confidence),
            "counter_evidence": list(analysis.classification.counter_evidence),
            "evidence_ids": list(analysis.classification.evidence_ids),
            "classifier": analysis.classification.classifier,
        },
        "stage": analysis.stage.value,
        "event_impact": analysis.event_impact.value,
        "option_tradability": analysis.option_tradability.value,
        "combined_opportunity": analysis.combined_opportunity.value,
        "event_impact_score": str(analysis.event_impact_score),
        "option_tradability_score": str(analysis.option_tradability_score),
        "combined_opportunity_score": str(analysis.combined_opportunity_score),
        "tradability_data": _tradability_document(analysis.tradability_data),
        "market_confirmation": _confirmation_document(analysis.market_confirmation),
        "rank": None,
        "rank_one": False,
        "decision_authority": "SUPPORTING_ONLY",
        "approval_eligible": False,
    }


def _analysis_from_document(
    document: object,
    *,
    expected_input: Mapping[str, object] | None = None,
) -> AnalyzedNews:
    record = _mapping(document, "analysis record")
    _exact_keys(
        record,
        {
            "record_schema_version",
            "fingerprint",
            "input",
            "analysis",
            "decision_authority",
            "approval_eligible",
            "instruction_creation_allowed",
            "order_creation_allowed",
        },
        "analysis record",
    )
    if record["record_schema_version"] != SCHEMA_VERSION:
        raise NewsAnalysisStoreCorruption("unsupported analysis record schema")
    for name in (
        "approval_eligible",
        "instruction_creation_allowed",
        "order_creation_allowed",
    ):
        if record[name] is not False:
            raise NewsAnalysisStoreCorruption(f"analysis record {name} must be false")
    if record["decision_authority"] != "SUPPORTING_ONLY":
        raise NewsAnalysisStoreCorruption("analysis record authority is invalid")
    input_document = _validate_input_document(
        _mapping(record["input"], "analysis input"),
        corruption=True,
    )
    if record["fingerprint"] != input_document["fingerprint"]:
        raise NewsAnalysisStoreCorruption("analysis fingerprint binding mismatch")
    if expected_input is not None and canonical_json(input_document) != canonical_json(
        _validate_input_document(expected_input)
    ):
        raise NewsAnalysisStoreCorruption("stored analysis input binding mismatch")
    news = _news_from_document(input_document["news"])
    raw = _mapping(record["analysis"], "analysis output")
    _exact_keys(
        raw,
        {
            "analysis_id",
            "analyzed_at",
            "classification",
            "stage",
            "event_impact",
            "option_tradability",
            "combined_opportunity",
            "event_impact_score",
            "option_tradability_score",
            "combined_opportunity_score",
            "tradability_data",
            "market_confirmation",
            "rank",
            "rank_one",
            "decision_authority",
            "approval_eligible",
        },
        "analysis output",
    )
    if raw["decision_authority"] != "SUPPORTING_ONLY" or raw["approval_eligible"] is not False:
        raise NewsAnalysisStoreCorruption("analysis output authority is invalid")
    if raw["rank"] is not None or raw["rank_one"] is not False:
        raise NewsAnalysisStoreCorruption("persisted analysis must be unranked")
    classification = _classification_from_document(raw["classification"], news)
    tradability = _tradability_from_document(raw["tradability_data"])
    confirmation = _confirmation_from_document(raw["market_confirmation"])
    binding = input_document["ibkr_binding"]
    if binding is None:
        if tradability is not None or confirmation is not None:
            raise NewsAnalysisStoreCorruption("analysis invented an IBKR binding")
    else:
        binding_map = _mapping(binding, "IBKR binding")
        if canonical_json(raw["tradability_data"]) != canonical_json(
            binding_map["tradability"]
        ):
            raise NewsAnalysisStoreCorruption("tradability output binding mismatch")
        if confirmation is not None and canonical_json(raw["market_confirmation"]) != canonical_json(
            binding_map["confirmation"]
        ):
            raise NewsAnalysisStoreCorruption("confirmation output binding mismatch")
    try:
        result = AnalyzedNews(
            analysis_id=_nonblank(raw["analysis_id"], "analysis_id"),
            news=news,
            classification=classification,
            analyzed_at=_parse_time(raw["analyzed_at"], "analyzed_at"),
            stage=AnalysisStage(str(raw["stage"])),
            event_impact=ScoreBand(str(raw["event_impact"])),
            option_tradability=ScoreBand(str(raw["option_tradability"])),
            combined_opportunity=ScoreBand(str(raw["combined_opportunity"])),
            event_impact_score=_decimal(raw["event_impact_score"], "event_impact_score"),
            option_tradability_score=_decimal(
                raw["option_tradability_score"], "option_tradability_score"
            ),
            combined_opportunity_score=_decimal(
                raw["combined_opportunity_score"], "combined_opportunity_score"
            ),
            tradability_data=tradability,
            market_confirmation=confirmation,
        )
    except (TypeError, ValueError) as exc:
        raise NewsAnalysisStoreCorruption("stored analysis output is invalid") from exc
    if result.stage is AnalysisStage.MARKET_CONFIRMED and binding is None:
        raise NewsAnalysisStoreCorruption("market-confirmed analysis lacks input binding")
    return result


def _checked_analysis_from_document(
    document: object,
    *,
    expected_input: Mapping[str, object] | None = None,
) -> AnalyzedNews:
    """Normalize all malformed persisted/factory documents to corruption."""

    try:
        return _analysis_from_document(document, expected_input=expected_input)
    except NewsAnalysisStoreCorruption:
        raise
    except (KeyError, TypeError, ValueError) as exc:
        raise NewsAnalysisStoreCorruption("stored analysis document is invalid") from exc


def _validate_input_document(
    value: Mapping[str, object],
    *,
    corruption: bool = False,
) -> dict[str, object]:
    try:
        source = _mapping(value, "analysis input")
        _exact_keys(
            source,
            {
                "input_contract_version",
                "evidence_content_hashes",
                "analyzer_contract",
                "news",
                "ibkr_binding",
                "fingerprint",
            },
            "analysis input",
        )
        if source["input_contract_version"] != INPUT_CONTRACT_VERSION:
            raise ValueError("unsupported analysis input contract")
        raw_hashes = _sequence(source["evidence_content_hashes"], "evidence hashes")
        hashes = [_digest(item, "evidence_content_hash") for item in raw_hashes]
        if not hashes or hashes != sorted(hashes):
            raise ValueError("evidence content hashes must be non-empty and sorted")
        contract = _validate_analyzer_contract(
            _mapping(source["analyzer_contract"], "analyzer contract")
        )
        news = _news_from_document(source["news"])
        binding = (
            None
            if source["ibkr_binding"] is None
            else _validate_binding_document(
                _mapping(source["ibkr_binding"], "IBKR binding")
            )
        )
        base: dict[str, object] = {
            "input_contract_version": INPUT_CONTRACT_VERSION,
            "evidence_content_hashes": hashes,
            "analyzer_contract": contract,
            "news": _news_document(news),
            "ibkr_binding": binding,
        }
        fingerprint = _digest(source["fingerprint"], "fingerprint")
        if canonical_hash(base) != fingerprint:
            raise ValueError("analysis input fingerprint mismatch")
        return {**base, "fingerprint": fingerprint}
    except NewsAnalysisStoreCorruption:
        raise
    except (TypeError, ValueError) as exc:
        if corruption:
            raise NewsAnalysisStoreCorruption("stored analysis input is invalid") from exc
        raise


def _news_document(news: NewsInput) -> dict[str, object]:
    return {
        "event_id": news.event_id,
        "headline": news.headline,
        "summary": news.summary,
        "source": news.source,
        "source_url": news.source_url,
        "published_at": _time_text(news.published_at),
        "first_seen_at": _time_text(news.first_seen_at),
        "evidence_ids": list(news.evidence_ids),
        "symbols": list(news.symbols),
        "authority": news.authority.value,
        "conflicting_evidence_ids": list(news.conflicting_evidence_ids),
        "is_complete": news.is_complete,
    }


def _news_from_document(value: object) -> NewsInput:
    raw = _mapping(value, "news input")
    _exact_keys(
        raw,
        {
            "event_id",
            "headline",
            "summary",
            "source",
            "source_url",
            "published_at",
            "first_seen_at",
            "evidence_ids",
            "symbols",
            "authority",
            "conflicting_evidence_ids",
            "is_complete",
        },
        "news input",
    )
    try:
        return NewsInput(
            event_id=_nonblank(raw["event_id"], "event_id"),
            headline=_nonblank(raw["headline"], "headline"),
            summary=str(raw["summary"]),
            source=_nonblank(raw["source"], "source"),
            source_url=_nonblank(raw["source_url"], "source_url"),
            published_at=_parse_time(raw["published_at"], "published_at"),
            first_seen_at=_parse_time(raw["first_seen_at"], "first_seen_at"),
            evidence_ids=tuple(
                _nonblank(item, "evidence_id")
                for item in _sequence(raw["evidence_ids"], "evidence_ids")
            ),
            symbols=tuple(
                _nonblank(item, "symbol")
                for item in _sequence(raw["symbols"], "symbols")
            ),
            authority=NewsAuthority(str(raw["authority"])),
            conflicting_evidence_ids=tuple(
                _nonblank(item, "conflicting_evidence_id")
                for item in _sequence(
                    raw["conflicting_evidence_ids"], "conflicting_evidence_ids"
                )
            ),
            is_complete=_boolean(raw["is_complete"], "is_complete"),
        )
    except (TypeError, ValueError) as exc:
        raise NewsAnalysisStoreCorruption("stored news input is invalid") from exc


def _classification_from_document(value: object, news: NewsInput) -> ClassifiedEvent:
    raw = _mapping(value, "classification")
    _exact_keys(
        raw,
        {
            "category",
            "symbols",
            "direction",
            "horizon",
            "confidence",
            "counter_evidence",
            "evidence_ids",
            "classifier",
        },
        "classification",
    )
    try:
        result = ClassifiedEvent(
            category=EventCategory(str(raw["category"])),
            symbols=tuple(
                _nonblank(item, "symbol")
                for item in _sequence(raw["symbols"], "classification symbols")
            ),
            direction=ImpactDirection(str(raw["direction"])),
            horizon=ImpactHorizon(str(raw["horizon"])),
            confidence=_decimal(raw["confidence"], "confidence"),
            counter_evidence=tuple(
                _nonblank(item, "counter_evidence")
                for item in _sequence(raw["counter_evidence"], "counter_evidence")
            ),
            evidence_ids=tuple(
                _nonblank(item, "evidence_id")
                for item in _sequence(raw["evidence_ids"], "classification evidence_ids")
            ),
            classifier=_nonblank(raw["classifier"], "classifier"),
        )
    except (TypeError, ValueError) as exc:
        raise NewsAnalysisStoreCorruption("stored classification is invalid") from exc
    if not set(result.symbols) <= set(news.symbols):
        raise NewsAnalysisStoreCorruption("classification invented a symbol")
    if not set(result.evidence_ids) <= set(news.evidence_ids):
        raise NewsAnalysisStoreCorruption("classification invented evidence")
    return result


def _tradability_document(value: OptionTradabilityInput | None) -> dict[str, object] | None:
    if value is None:
        return None
    return {
        "symbol": value.symbol,
        "source": value.source,
        "observed_at": _time_text(value.observed_at),
        "bid": None if value.bid is None else str(value.bid),
        "ask": None if value.ask is None else str(value.ask),
        "volume": value.volume,
        "open_interest": value.open_interest,
    }


def _tradability_from_document(value: object) -> OptionTradabilityInput | None:
    if value is None:
        return None
    raw = _mapping(value, "tradability")
    _exact_keys(
        raw,
        {"symbol", "source", "observed_at", "bid", "ask", "volume", "open_interest"},
        "tradability",
    )
    try:
        return OptionTradabilityInput(
            symbol=_nonblank(raw["symbol"], "symbol"),
            source=_nonblank(raw["source"], "source"),
            observed_at=_parse_time(raw["observed_at"], "observed_at"),
            bid=None if raw["bid"] is None else _decimal(raw["bid"], "bid"),
            ask=None if raw["ask"] is None else _decimal(raw["ask"], "ask"),
            volume=_optional_nonnegative_int(raw["volume"], "volume"),
            open_interest=_optional_nonnegative_int(
                raw["open_interest"], "open_interest"
            ),
        )
    except (TypeError, ValueError) as exc:
        raise NewsAnalysisStoreCorruption("stored tradability input is invalid") from exc


def _confirmation_document(value: MarketConfirmation | None) -> dict[str, object] | None:
    if value is None:
        return None
    return {
        "source": value.source,
        "observed_at": _time_text(value.observed_at),
        "direction": value.direction.value,
        "evidence_ids": list(value.evidence_ids),
    }


def _confirmation_from_document(value: object) -> MarketConfirmation | None:
    if value is None:
        return None
    raw = _mapping(value, "market confirmation")
    _exact_keys(
        raw,
        {"source", "observed_at", "direction", "evidence_ids"},
        "market confirmation",
    )
    try:
        return MarketConfirmation(
            source=_nonblank(raw["source"], "source"),
            observed_at=_parse_time(raw["observed_at"], "observed_at"),
            direction=ImpactDirection(str(raw["direction"])),
            evidence_ids=tuple(
                _nonblank(item, "evidence_id")
                for item in _sequence(raw["evidence_ids"], "confirmation evidence_ids")
            ),
        )
    except (TypeError, ValueError) as exc:
        raise NewsAnalysisStoreCorruption("stored market confirmation is invalid") from exc


def _validate_binding_document(value: Mapping[str, object]) -> dict[str, object]:
    raw = _mapping(value, "IBKR binding")
    _exact_keys(
        raw,
        {"symbol", "quote_snapshot_id", "tradability", "confirmation"},
        "IBKR binding",
    )
    tradability = _tradability_from_document(raw["tradability"])
    confirmation = _confirmation_from_document(raw["confirmation"])
    if tradability is None or confirmation is None:
        raise ValueError("IBKR binding requires tradability and confirmation")
    symbol = _nonblank(raw["symbol"], "symbol").upper()
    snapshot_id = _nonblank(raw["quote_snapshot_id"], "quote_snapshot_id")
    if not _SAFE_IDENTIFIER_RE.fullmatch(snapshot_id):
        raise ValueError("IBKR quote snapshot identifier is invalid")
    if tradability.symbol != symbol:
        raise ValueError("IBKR binding symbol mismatch")
    if tradability.observed_at != confirmation.observed_at:
        raise ValueError("IBKR binding timestamps do not match")
    if snapshot_id not in confirmation.evidence_ids:
        raise ValueError("IBKR confirmation is not bound to the quote snapshot")
    return {
        "symbol": symbol,
        "quote_snapshot_id": snapshot_id,
        "tradability": _tradability_document(tradability),
        "confirmation": _confirmation_document(confirmation),
    }


def _classifier_contract(classifier: object, *, seen: set[int]) -> dict[str, object]:
    identity = id(classifier)
    if identity in seen:
        raise ValueError("classifier contract contains a cycle")
    seen.add(identity)
    class_type = type(classifier)
    raw_class_id = f"{class_type.__module__}.{class_type.__qualname__}"
    if _SAFE_IDENTIFIER_RE.fullmatch(raw_class_id) and not _SECRETISH_RE.search(
        raw_class_id
    ):
        class_id = raw_class_id
    else:
        class_id = (
            "class:sha256:"
            f"{hashlib.sha256(raw_class_id.encode('utf-8')).hexdigest()}"
        )
    state = getattr(classifier, "__dict__", {})
    if not isinstance(state, Mapping):
        state = {}
    class_state = vars(class_type)
    contract_version = _advertised_identifier(
        state.get("contract_version", class_state.get("contract_version"))
    ) or "1"
    model_id = _advertised_identifier(
        state.get("model_id", class_state.get("model_id"))
    ) or _advertised_identifier(state.get("_model", class_state.get("_model")))
    components: dict[str, object] = {}
    for public_name, private_name in (
        ("primary", "_primary"),
        ("fallback", "_fallback"),
        ("classifier", "_classifier"),
    ):
        component = state.get(private_name)
        if component is not None and callable(getattr(component, "classify", None)):
            components[public_name] = _classifier_contract(component, seen=seen)
    seen.remove(identity)
    return {
        "class_id": class_id,
        "contract_version": contract_version,
        "model_id": model_id,
        "components": components,
    }


def _validate_analyzer_contract(value: Mapping[str, object]) -> dict[str, object]:
    raw = _mapping(value, "analyzer contract")
    _exact_keys(
        raw,
        {"analyzer_id", "analyzer_contract_version", "classifier"},
        "analyzer contract",
    )
    analyzer_id = _safe_identifier(raw["analyzer_id"], "analyzer_id")
    analyzer_version = _safe_identifier(
        raw["analyzer_contract_version"], "analyzer_contract_version"
    )
    classifier = _validate_classifier_contract(raw["classifier"])
    return {
        "analyzer_id": analyzer_id,
        "analyzer_contract_version": analyzer_version,
        "classifier": classifier,
    }


def _validate_classifier_contract(value: object) -> dict[str, object]:
    raw = _mapping(value, "classifier contract")
    _exact_keys(
        raw,
        {"class_id", "contract_version", "model_id", "components"},
        "classifier contract",
    )
    components_raw = _mapping(raw["components"], "classifier components")
    if not set(components_raw) <= {"primary", "fallback", "classifier"}:
        raise ValueError("classifier contract contains an unknown component")
    return {
        "class_id": _safe_identifier(raw["class_id"], "classifier class_id"),
        "contract_version": _safe_identifier(
            raw["contract_version"], "classifier contract_version"
        ),
        "model_id": (
            None
            if raw["model_id"] is None
            else _safe_identifier(raw["model_id"], "classifier model_id")
        ),
        "components": {
            str(name): _validate_classifier_contract(component)
            for name, component in sorted(components_raw.items())
        },
    }


def _decode_row_document(row: sqlite3.Row) -> Mapping[str, object]:
    try:
        document = json.loads(str(row["immutable_json"]), parse_constant=_reject_constant)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise NewsAnalysisStoreCorruption("news analysis JSON is invalid") from exc
    if not isinstance(document, Mapping):
        raise NewsAnalysisStoreCorruption("news analysis document is not an object")
    return document


def _row_hash(sequence: int, prior_hash: str, content_hash: str) -> str:
    return hashlib.sha256(
        f"{sequence}:{prior_hash}:{content_hash}".encode("ascii")
    ).hexdigest()


def _time_text(value: datetime) -> str:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds")


def _parse_time(value: object, name: str) -> datetime:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be timestamp text")
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return parsed.astimezone(timezone.utc)


def _digest(value: object, name: str) -> str:
    if not isinstance(value, str) or _HASH_RE.fullmatch(value) is None:
        raise ValueError(f"{name} must be a lowercase SHA-256 digest")
    return value


def _safe_identifier(value: object, name: str) -> str:
    text = _nonblank(value, name)
    if _SAFE_IDENTIFIER_RE.fullmatch(text) is None or _SECRETISH_RE.search(text):
        raise ValueError(f"{name} is not a safe identifier")
    return text


def _advertised_identifier(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text or _SAFE_IDENTIFIER_RE.fullmatch(text) is None or _SECRETISH_RE.search(text):
        return None
    return text


def _nonblank(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} cannot be blank")
    return value.strip()


def _decimal(value: object, name: str) -> Decimal:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be decimal text")
    try:
        result = Decimal(value)
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{name} is invalid") from exc
    if not result.is_finite():
        raise ValueError(f"{name} must be finite")
    return result


def _optional_nonnegative_int(value: object, name: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{name} must be a non-negative integer or null")
    return value


def _boolean(value: object, name: str) -> bool:
    if not isinstance(value, bool):
        raise TypeError(f"{name} must be a bool")
    return value


def _mapping(value: object, name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        raise TypeError(f"{name} must be an object")
    return value


def _sequence(value: object, name: str) -> Sequence[object]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise TypeError(f"{name} must be an array")
    return value


def _exact_keys(value: Mapping[str, object], expected: set[str], name: str) -> None:
    if set(value) != expected:
        raise ValueError(f"{name} fields do not match the schema")


def _reject_constant(value: str) -> object:
    raise ValueError(f"non-finite JSON constant is forbidden: {value}")


__all__ = [
    "ANALYZER_CONTRACT_VERSION",
    "INPUT_CONTRACT_VERSION",
    "IntegrityVerificationProgress",
    "NewsAnalysisStore",
    "NewsAnalysisStoreCorruption",
    "NewsAnalysisStoreError",
    "analysis_contract",
    "analysis_input_document",
    "analysis_store_path",
]
