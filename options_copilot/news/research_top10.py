"""Durable managed-plugin Top-10 research envelopes.

This module is deliberately separate from the executable-v1 option pipeline.
It stores point-in-time, contract-level research that may have unavailable
Greeks, market-data classification, or broker session hours.  Missing values
remain ``None`` and must carry explicit blockers.  ``risk_cap_verified`` means
only that declared fields and deterministic arithmetic are internally
consistent; an evidence hash does not establish IBKR source trust.  No value
produced here is approval eligible and this module exposes no broker,
instruction, or order primitive.
"""
from __future__ import annotations

import argparse
from collections.abc import Mapping, Sequence
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import json
import os
from pathlib import Path
import re
import sqlite3
import sys
from zoneinfo import ZoneInfo

from options_copilot.operations.readiness import (
    SecretLikeFieldError,
    assert_no_secret_like,
)
from options_copilot.storage.canonical import canonical_hash


RESEARCH_TOP10_SCHEMA = "options_copilot.managed_plugin_research_top10.v2"
RESEARCH_TOP10_VERSION = 2
RESEARCH_TOP10_READ_MODEL_SCHEMA = "options_copilot.research_top10_read_model.v2"
RESEARCH_TOP10_SOURCE = "IBKR_MANAGED_PLUGIN"
RESEARCH_TOP10_DIRECT_SOURCE = "IBKR_DIRECT_READONLY"
PREMARKET_RESEARCH = "PREMARKET_RESEARCH"
INDICATIVE_REPRICE = "INDICATIVE_REPRICE"
INTRADAY_RECOVERY = "INTRADAY_RECOVERY"
TARGET_COUNT = 10
NORMAL_RISK_FRACTION = Decimal("0.10")
MAXIMUM_IMPORT_BYTES = 1 * 1024 * 1024
MAXIMUM_READ_MODEL_BYTES = 1 * 1024 * 1024
MAXIMUM_TEXT_CHARS = 4096
NEW_YORK = ZoneInfo("America/New_York")

_CENT = Decimal("0.01")
_PHASES = frozenset(
    {PREMARKET_RESEARCH, INDICATIVE_REPRICE, INTRADAY_RECOVERY}
)
_SOURCES = frozenset({RESEARCH_TOP10_SOURCE, RESEARCH_TOP10_DIRECT_SOURCE})
_DEBIT_STRATEGIES = frozenset({"BULL_CALL_VERTICAL", "BEAR_PUT_VERTICAL"})
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
_BLOCKER = re.compile(r"[A-Z][A-Z0-9_:-]{2,127}\Z")
_LOCAL_PATH = re.compile(r"(?:\b[A-Za-z]:[\\/]|\\\\[^\\\s]+\\)")
_FUTURE_TOLERANCE = timedelta(seconds=5)
_EXACT_QUOTE_MAX_AGE = timedelta(seconds=5)
_EXACT_QUOTE_MAX_SKEW = timedelta(seconds=2)
_POLICY_ASSUMED_MULTIPLIER = 100

_AUTHORITY_FIELDS = {
    "decision_authority",
    "approval_eligible",
    "instruction_creation_allowed",
    "order_allowed",
    "action_pool_eligible",
}
_TOP_FIELDS = {
    "schema",
    "version",
    "batch_id",
    "phase",
    "trading_date",
    "observed_at",
    "source",
    "strategy_nav_usd",
    "normal_risk_fraction",
    "target_count",
    "parent_content_hash",
    "session",
    "blockers",
    "candidates",
    *_AUTHORITY_FIELDS,
}
_SESSION_FIELDS = {
    "liquid_hours",
    "trading_hours",
    "timezone_id",
    "observed_at",
    "source",
    "blockers",
}
_CANDIDATE_FIELDS = {
    "research_id",
    "rank",
    "underlying",
    "strategy_type",
    "expiration",
    "dte",
    "quantity",
    "entry_debit_usd",
    "indicative_entry_debit_usd",
    "execution_cost_cap_usd",
    "maximum_loss_usd",
    "indicative_maximum_loss_usd",
    "cost_after_ev_usd",
    "indicative_cost_after_ev_usd",
    "assumed_multiplier",
    "risk_cap_verified",
    "trade_status",
    "entry_condition",
    "invalidation_condition",
    "profit_target_condition",
    "stop_loss_condition",
    "research_summary",
    "evidence_ids",
    "evidence_hashes",
    "blockers",
    "legs",
    *_AUTHORITY_FIELDS,
}
_LEG_FIELDS = {
    "contract_id",
    "contract_id_ex",
    "side",
    "right",
    "strike",
    "expiration",
    "exchange",
    "trading_class",
    "local_symbol",
    "multiplier",
    "standard_or_adjusted",
    "bid",
    "ask",
    "collected_at",
    "quote_asof",
    "implied_volatility",
    "delta",
    "gamma",
    "theta",
    "vega",
    "volume",
    "open_interest",
    "market_data_type",
    "identity_evidence_hash",
    "quote_evidence_hash",
    "blockers",
}
_GREEK_BLOCKERS = {
    "delta": "DELTA_UNAVAILABLE",
    "gamma": "GAMMA_UNAVAILABLE",
    "theta": "THETA_UNAVAILABLE",
    "vega": "VEGA_UNAVAILABLE",
}
_READ_MODEL_FIELDS = {
    "schema",
    "status",
    "phase",
    "trading_date",
    "observed_at",
    "batch_id",
    "content_hash",
    "parent_content_hash",
    "source",
    "strategy_nav_usd",
    "normal_risk_fraction",
    "target_count",
    "available_count",
    "target_met",
    "reason_codes",
    "blockers",
    "session",
    "candidates",
    "premarket",
    "open_repriced",
    "stages",
    *_AUTHORITY_FIELDS,
    "action_pool_count",
}
_STAGE_FIELDS = {
    "phase",
    "content_hash",
    "parent_content_hash",
    "batch_id",
    "trading_date",
    "observed_at",
    "source",
    "strategy_nav_usd",
    "normal_risk_fraction",
    "target_count",
    "session",
    "blockers",
    "available_count",
}


class ResearchTop10Error(ValueError):
    """Base error for the isolated research-v2 channel."""


class ResearchTop10ValidationError(ResearchTop10Error):
    """One managed-plugin research envelope is incomplete or unsafe."""


class ResearchTop10Conflict(ResearchTop10Error):
    """An immutable trading-date/phase row already has different content."""


def default_research_top10_store_path() -> Path:
    explicit = os.getenv("OPTIONS_COPILOT_RESEARCH_TOP10_PATH", "").strip()
    if explicit:
        return Path(explicit)
    data_dir = os.getenv("OPTIONS_COPILOT_DATA_DIR", "").strip()
    root = Path(data_dir) if data_dir else Path("data") / "options_copilot"
    return root / "research_top10.sqlite3"


def validate_research_top10_envelope(value: object) -> dict[str, object]:
    _assert_safe_content(value)
    row = _exact_mapping(value, _TOP_FIELDS, "research envelope")
    if row["schema"] != RESEARCH_TOP10_SCHEMA:
        raise ResearchTop10ValidationError("research envelope schema is unsupported")
    version = row["version"]
    if isinstance(version, bool) or version != RESEARCH_TOP10_VERSION:
        raise ResearchTop10ValidationError("research envelope version is unsupported")
    _supporting_only(row, "research envelope")

    phase = _choice(row["phase"], _PHASES, "phase")
    trading_date = _date(row["trading_date"], "trading_date")
    observed_at = _timestamp(row["observed_at"], "observed_at")
    observed_et = observed_at.astimezone(NEW_YORK)
    if observed_et.date() != trading_date:
        raise ResearchTop10ValidationError(
            "research observed_at and trading_date do not match"
        )
    if phase == PREMARKET_RESEARCH and observed_et.time() >= time(9, 30):
        raise ResearchTop10ValidationError("premarket research must precede 09:30 ET")
    if phase == INDICATIVE_REPRICE and observed_et.time() < time(9, 30):
        raise ResearchTop10ValidationError("indicative reprice cannot precede 09:30 ET")
    if phase == INTRADAY_RECOVERY and observed_et.time() < time(9, 30):
        raise ResearchTop10ValidationError("intraday recovery cannot precede 09:30 ET")
    source = _choice(row["source"], _SOURCES, "source")
    if phase == INTRADAY_RECOVERY and source != RESEARCH_TOP10_DIRECT_SOURCE:
        raise ResearchTop10ValidationError(
            "intraday recovery requires the direct read-only source"
        )
    if phase != INTRADAY_RECOVERY and source != RESEARCH_TOP10_SOURCE:
        raise ResearchTop10ValidationError("research source is unsupported")
    target_count = _positive_integer(row["target_count"], "target_count")
    if target_count != TARGET_COUNT:
        raise ResearchTop10ValidationError("research target_count must remain ten")
    nav = _positive_decimal(row["strategy_nav_usd"], "strategy_nav_usd")
    if _decimal(row["normal_risk_fraction"], "normal_risk_fraction") != NORMAL_RISK_FRACTION:
        raise ResearchTop10ValidationError("normal risk fraction must remain 10%")
    parent_hash = row["parent_content_hash"]
    if phase in {PREMARKET_RESEARCH, INTRADAY_RECOVERY}:
        if parent_hash is not None:
            raise ResearchTop10ValidationError(
                "parentless research cannot declare a parent content hash"
            )
    else:
        parent_hash = _digest(parent_hash, "parent_content_hash")

    session = _normalise_session(row["session"])
    blockers = list(_blockers(row["blockers"], "blockers"))
    raw_candidates = _array(row["candidates"], "candidates")
    if not raw_candidates or len(raw_candidates) > TARGET_COUNT:
        raise ResearchTop10ValidationError(
            "research candidates must contain between one and ten rows"
        )
    candidates = tuple(
        _normalise_candidate(
            candidate,
            index=index,
            phase=phase,
            trading_date=trading_date,
            strategy_nav=nav,
            observed_at=observed_at,
        )
        for index, candidate in enumerate(raw_candidates)
    )
    ranks = tuple(int(candidate["rank"]) for candidate in candidates)
    if ranks != tuple(range(1, len(candidates) + 1)):
        raise ResearchTop10ValidationError(
            "research ranks must be contiguous and start at one"
        )
    identifiers = tuple(str(candidate["research_id"]) for candidate in candidates)
    if len(set(identifiers)) != len(identifiers):
        raise ResearchTop10ValidationError("research_id values must be unique")
    contract_ids = [
        int(leg["contract_id"])
        for candidate in candidates
        for leg in candidate["legs"]  # type: ignore[union-attr]
    ]
    if len(contract_ids) != len(set(contract_ids)):
        raise ResearchTop10ValidationError(
            "contract ids must be unique across the research Top-10"
        )
    if len(candidates) < TARGET_COUNT and "TOP10_RESEARCH_SHORTFALL" not in blockers:
        blockers.append("TOP10_RESEARCH_SHORTFALL")
    if len(candidates) == TARGET_COUNT and "TOP10_RESEARCH_SHORTFALL" in blockers:
        raise ResearchTop10ValidationError(
            "TOP10_RESEARCH_SHORTFALL conflicts with ten candidates"
        )

    return {
        "schema": RESEARCH_TOP10_SCHEMA,
        "version": RESEARCH_TOP10_VERSION,
        "batch_id": _identifier(row["batch_id"], "batch_id"),
        "phase": phase,
        "trading_date": trading_date.isoformat(),
        "observed_at": _timestamp_text(observed_at),
        "source": source,
        "strategy_nav_usd": _decimal_text(nav, money=True),
        "normal_risk_fraction": "0.10",
        "target_count": TARGET_COUNT,
        "parent_content_hash": parent_hash,
        "session": session,
        "blockers": list(dict.fromkeys(blockers)),
        "candidates": list(candidates),
        **_authority_projection(),
    }


class ResearchTop10Store:
    """Append-only SQLite store with immutable per-date phase slots."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(
            self.path,
            isolation_level=None,
            timeout=5.0,
        )
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA foreign_keys=ON")
        self._connection.execute("PRAGMA journal_mode=WAL")
        self._connection.execute("PRAGMA synchronous=FULL")
        self._initialise()

    def __enter__(self) -> "ResearchTop10Store":
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def close(self) -> None:
        connection = self._connection
        if connection is not None:
            connection.close()
            self._connection = None  # type: ignore[assignment]

    def import_envelope(self, value: object) -> dict[str, object]:
        payload = validate_research_top10_envelope(value)
        observed_at = _timestamp(payload["observed_at"], "observed_at")
        if observed_at > _utc_now() + _FUTURE_TOLERANCE:
            raise ResearchTop10ValidationError(
                "research observed_at is more than five seconds in the future"
            )
        content_hash = canonical_hash(payload)
        rendered = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        if len(rendered.encode("utf-8")) > MAXIMUM_IMPORT_BYTES:
            raise ResearchTop10ValidationError(
                "research envelope exceeds one MiB"
            )
        phase = str(payload["phase"])
        trading_date = str(payload["trading_date"])
        connection = self._require_open()
        try:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                """
                SELECT content_hash,payload_json FROM research_top10_batches
                WHERE trading_date=? AND phase=?
                """,
                (trading_date, phase),
            ).fetchone()
            if existing is not None:
                _load_stored_payload(
                    str(existing["payload_json"]),
                    expected_content_hash=str(existing["content_hash"]),
                )
                if str(existing["content_hash"]) != content_hash:
                    raise ResearchTop10Conflict(
                        "research phase already contains different immutable content"
                    )
                connection.execute("COMMIT")
                return _import_result(payload, content_hash, "IDEMPOTENT")

            if phase == INDICATIVE_REPRICE:
                parent = connection.execute(
                    """
                    SELECT content_hash,payload_json FROM research_top10_batches
                    WHERE trading_date=? AND phase=?
                    """,
                    (trading_date, PREMARKET_RESEARCH),
                ).fetchone()
                if parent is None:
                    raise ResearchTop10Conflict(
                        "indicative reprice parent premarket research is missing"
                    )
                if str(parent["content_hash"]) != payload["parent_content_hash"]:
                    raise ResearchTop10Conflict(
                        "indicative reprice parent content hash does not match"
                    )
                parent_payload = _load_stored_payload(
                    str(parent["payload_json"]),
                    expected_content_hash=str(parent["content_hash"]),
                )
                _validate_parent_binding(parent_payload, payload)

            created_at = _timestamp_text(_utc_now())
            connection.execute(
                """
                INSERT INTO research_top10_batches(
                    trading_date,phase,batch_id,observed_at,content_hash,
                    parent_content_hash,payload_json,created_at
                ) VALUES(?,?,?,?,?,?,?,?)
                """,
                (
                    trading_date,
                    phase,
                    payload["batch_id"],
                    payload["observed_at"],
                    content_hash,
                    payload["parent_content_hash"],
                    rendered,
                    created_at,
                ),
            )
            connection.execute("COMMIT")
        except ResearchTop10Error:
            _rollback(connection)
            raise
        except sqlite3.IntegrityError as exc:
            _rollback(connection)
            raise ResearchTop10Conflict(
                "research batch identity conflicts with immutable content"
            ) from exc
        except Exception:
            _rollback(connection)
            raise
        return _import_result(payload, content_hash, "IMPORTED")

    def read_model(self) -> dict[str, object]:
        connection = self._require_open()
        latest_date = connection.execute(
            "SELECT MAX(trading_date) AS trading_date FROM research_top10_batches"
        ).fetchone()
        if latest_date is None or latest_date["trading_date"] is None:
            return unavailable_research_top10_read_model()
        rows = connection.execute(
            """
            SELECT phase,content_hash,payload_json
            FROM research_top10_batches
            WHERE trading_date=?
            ORDER BY CASE phase
                WHEN 'PREMARKET_RESEARCH' THEN 1
                WHEN 'INTRADAY_RECOVERY' THEN 2
                ELSE 3
            END
            """,
            (str(latest_date["trading_date"]),),
        ).fetchall()
        if not rows:
            return unavailable_research_top10_read_model()
        decoded = [
            (
                str(row["phase"]),
                str(row["content_hash"]),
                _load_stored_payload(
                    str(row["payload_json"]),
                    expected_content_hash=str(row["content_hash"]),
                ),
            )
            for row in rows
        ]
        active_phase, active_hash, active = decoded[-1]
        candidates = list(active["candidates"])  # type: ignore[arg-type]
        by_phase = {phase: payload for phase, _digest_value, payload in decoded}
        premarket_payload = by_phase.get(PREMARKET_RESEARCH)
        reprice_payload = by_phase.get(INDICATIVE_REPRICE)
        intraday_payload = by_phase.get(INTRADAY_RECOVERY)
        premarket = (
            []
            if premarket_payload is None
            else list(premarket_payload["candidates"])  # type: ignore[arg-type]
        )
        open_repriced = (
            []
            if reprice_payload is None and intraday_payload is None
            else list(
                (
                    reprice_payload
                    if reprice_payload is not None
                    else intraday_payload
                )["candidates"]  # type: ignore[index]
            )
        )
        reasons: list[str] = ["SOURCE_TRUST_NOT_ESTABLISHED"]
        reasons.extend(active["blockers"])  # type: ignore[arg-type]
        session = active["session"]
        if isinstance(session, Mapping):
            reasons.extend(session.get("blockers", ()))
        for candidate in candidates:
            if not isinstance(candidate, Mapping):
                continue
            reasons.extend(candidate.get("blockers", ()))
            legs = candidate.get("legs", ())
            if isinstance(legs, Sequence):
                for leg in legs:
                    if isinstance(leg, Mapping):
                        reasons.extend(leg.get("blockers", ()))
        target_met = len(candidates) == TARGET_COUNT
        return {
            "schema": RESEARCH_TOP10_READ_MODEL_SCHEMA,
            "status": "AVAILABLE" if target_met else "DEGRADED",
            "phase": active_phase,
            "trading_date": active["trading_date"],
            "observed_at": active["observed_at"],
            "batch_id": active["batch_id"],
            "content_hash": active_hash,
            "parent_content_hash": active["parent_content_hash"],
            "source": active["source"],
            "strategy_nav_usd": active["strategy_nav_usd"],
            "normal_risk_fraction": active["normal_risk_fraction"],
            "target_count": TARGET_COUNT,
            "available_count": len(candidates),
            "target_met": target_met,
            "reason_codes": list(dict.fromkeys(str(item) for item in reasons)),
            "blockers": list(active["blockers"]),  # type: ignore[arg-type]
            "session": session,
            "candidates": candidates,
            "premarket": premarket,
            "open_repriced": open_repriced,
            "stages": [
                _stage_projection(payload, digest)
                for _phase, digest, payload in decoded
            ],
            **_authority_projection(),
            "action_pool_count": 0,
        }

    def batch_count(self) -> int:
        row = self._require_open().execute(
            "SELECT COUNT(*) AS count FROM research_top10_batches"
        ).fetchone()
        return 0 if row is None else int(row["count"])

    def _initialise(self) -> None:
        connection = self._require_open()
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS research_top10_batches(
                trading_date TEXT NOT NULL,
                phase TEXT NOT NULL CHECK(
                    phase IN (
                        'PREMARKET_RESEARCH',
                        'INDICATIVE_REPRICE',
                        'INTRADAY_RECOVERY'
                    )
                ),
                batch_id TEXT NOT NULL UNIQUE,
                observed_at TEXT NOT NULL,
                content_hash TEXT NOT NULL UNIQUE,
                parent_content_hash TEXT,
                payload_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY(trading_date,phase)
            );
            CREATE TRIGGER IF NOT EXISTS research_top10_no_update
            BEFORE UPDATE ON research_top10_batches
            BEGIN SELECT RAISE(ABORT,'immutable research Top-10: update forbidden'); END;
            CREATE TRIGGER IF NOT EXISTS research_top10_no_delete
            BEFORE DELETE ON research_top10_batches
            BEGIN SELECT RAISE(ABORT,'immutable research Top-10: delete forbidden'); END;
            """
        )

    def _require_open(self) -> sqlite3.Connection:
        if self._connection is None:
            raise RuntimeError("research Top-10 store is closed")
        return self._connection


def import_research_top10(
    payload: object,
    *,
    store_path: str | Path | None = None,
) -> dict[str, object]:
    path = default_research_top10_store_path() if store_path is None else Path(store_path)
    with ResearchTop10Store(path) as store:
        return store.import_envelope(payload)


def read_research_top10(
    *,
    store_path: str | Path | None = None,
) -> dict[str, object]:
    path = default_research_top10_store_path() if store_path is None else Path(store_path)
    if not path.is_file():
        return unavailable_research_top10_read_model()
    try:
        with ResearchTop10Store(path) as store:
            return store.read_model()
    except Exception:
        return unavailable_research_top10_read_model("RESEARCH_TOP10_STORE_INVALID")


def unavailable_research_top10_read_model(
    reason: str = "RESEARCH_TOP10_NOT_AVAILABLE",
) -> dict[str, object]:
    return {
        "schema": RESEARCH_TOP10_READ_MODEL_SCHEMA,
        "status": "UNAVAILABLE",
        "phase": None,
        "trading_date": None,
        "observed_at": None,
        "batch_id": None,
        "content_hash": None,
        "parent_content_hash": None,
        "source": None,
        "strategy_nav_usd": None,
        "normal_risk_fraction": None,
        "target_count": TARGET_COUNT,
        "available_count": 0,
        "target_met": False,
        "reason_codes": [reason],
        "blockers": [reason],
        "session": None,
        "candidates": [],
        "premarket": [],
        "open_repriced": [],
        "stages": [],
        **_authority_projection(),
        "action_pool_count": 0,
    }


def safe_research_top10_read_model(value: object) -> dict[str, object]:
    """Accept only the exact harmless projection, otherwise return UNAVAILABLE."""

    try:
        rendered = _bounded_safe_json(value)
        row = _exact_mapping(value, _READ_MODEL_FIELDS, "research read model")
        if row["schema"] != RESEARCH_TOP10_READ_MODEL_SCHEMA:
            raise ResearchTop10ValidationError("research read model schema is invalid")
        _supporting_only(row, "research read model")
        if (
            isinstance(row["action_pool_count"], bool)
            or row["action_pool_count"] != 0
        ):
            raise ResearchTop10ValidationError("research action pool must remain empty")
        candidates = _array(row["candidates"], "candidates")
        premarket = _array(row["premarket"], "premarket")
        open_repriced = _array(row["open_repriced"], "open_repriced")
        if len(candidates) > TARGET_COUNT:
            raise ResearchTop10ValidationError("research read model exceeds ten rows")
        target_count = _positive_integer(row["target_count"], "target_count")
        if target_count != TARGET_COUNT:
            raise ResearchTop10ValidationError("research target_count must remain ten")
        available_count = _nonnegative_integer(
            row["available_count"], "available_count"
        )
        if available_count != len(candidates):
            raise ResearchTop10ValidationError(
                "research available_count does not match candidate rows"
            )
        target_met = _boolean(row["target_met"], "target_met")
        if target_met != (len(candidates) == TARGET_COUNT):
            raise ResearchTop10ValidationError(
                "research target_met does not match candidate rows"
            )
        expected_status = (
            "AVAILABLE" if target_met else "DEGRADED" if candidates else "UNAVAILABLE"
        )
        if row["status"] != expected_status:
            raise ResearchTop10ValidationError(
                "research status does not match candidate rows"
            )
        stages = _array(row["stages"], "stages")
        if not candidates:
            _validate_unavailable_read_model(row, premarket, open_repriced, stages)
            return json.loads(rendered)

        if "SOURCE_TRUST_NOT_ESTABLISHED" not in _text_array(
            row["reason_codes"], "reason_codes"
        ):
            raise ResearchTop10ValidationError(
                "research read model must disclose unestablished source trust"
            )
        validated_stages: list[tuple[Mapping[str, object], Mapping[str, object]]] = []
        for index, stage in enumerate(stages):
            stage_row = _exact_mapping(stage, _STAGE_FIELDS, f"stages[{index}]")
            phase = _choice(stage_row["phase"], _PHASES, f"stages[{index}].phase")
            stage_candidates = (
                premarket if phase == PREMARKET_RESEARCH else open_repriced
            )
            validated_stages.append(
                (
                    stage_row,
                    _validate_stage_envelope(
                        stage_row,
                        stage_candidates,
                        name=f"stages[{index}]",
                    ),
                )
            )

        phases = [str(stage["phase"]) for stage, _payload in validated_stages]
        if phases == [INTRADAY_RECOVERY]:
            expected_phases = [INTRADAY_RECOVERY]
        elif phases == [PREMARKET_RESEARCH, INTRADAY_RECOVERY]:
            expected_phases = [PREMARKET_RESEARCH, INTRADAY_RECOVERY]
        else:
            expected_phases = [PREMARKET_RESEARCH]
            if open_repriced:
                expected_phases.append(INDICATIVE_REPRICE)
        if phases != expected_phases or (
            INTRADAY_RECOVERY not in phases and not premarket
        ):
            raise ResearchTop10ValidationError(
                "research stage lineage is incomplete or out of order"
            )
        active_stage, active_envelope = validated_stages[-1]
        _validate_active_read_projection(row, active_stage, active_envelope, candidates)
        if phases == [PREMARKET_RESEARCH, INDICATIVE_REPRICE]:
            parent_envelope = validated_stages[0][1]
            if active_envelope["parent_content_hash"] != active_stage["parent_content_hash"]:
                raise ResearchTop10ValidationError(
                    "research reprice parent hash projection differs"
                )
            if active_envelope["parent_content_hash"] != validated_stages[0][0]["content_hash"]:
                raise ResearchTop10ValidationError(
                    "research reprice parent content hash differs"
                )
            _validate_parent_binding(parent_envelope, active_envelope)
        return json.loads(rendered)
    except Exception:
        return unavailable_research_top10_read_model("RESEARCH_TOP10_INVALID")


def _validate_unavailable_read_model(
    row: Mapping[str, object],
    premarket: Sequence[Mapping[str, object]],
    open_repriced: Sequence[Mapping[str, object]],
    stages: Sequence[Mapping[str, object]],
) -> None:
    if premarket or open_repriced or stages or row["status"] != "UNAVAILABLE":
        raise ResearchTop10ValidationError(
            "unavailable research read model cannot retain stage data"
        )
    nullable = (
        "phase",
        "trading_date",
        "observed_at",
        "batch_id",
        "content_hash",
        "parent_content_hash",
        "source",
        "strategy_nav_usd",
        "normal_risk_fraction",
        "session",
    )
    if any(row[field] is not None for field in nullable):
        raise ResearchTop10ValidationError(
            "unavailable research read model contains active metadata"
        )


def _validate_stage_envelope(
    stage: Mapping[str, object],
    candidates: Sequence[Mapping[str, object]],
    *,
    name: str,
) -> Mapping[str, object]:
    available_count = _nonnegative_integer(
        stage["available_count"], f"{name}.available_count"
    )
    if not candidates or available_count != len(candidates):
        raise ResearchTop10ValidationError(
            f"{name} candidate count does not match its stage"
        )
    if _positive_integer(stage["target_count"], f"{name}.target_count") != TARGET_COUNT:
        raise ResearchTop10ValidationError(f"{name} target_count must remain ten")
    envelope = validate_research_top10_envelope(
        {
            "schema": RESEARCH_TOP10_SCHEMA,
            "version": RESEARCH_TOP10_VERSION,
            "batch_id": stage["batch_id"],
            "phase": stage["phase"],
            "trading_date": stage["trading_date"],
            "observed_at": stage["observed_at"],
            "source": stage["source"],
            "strategy_nav_usd": stage["strategy_nav_usd"],
            "normal_risk_fraction": stage["normal_risk_fraction"],
            "target_count": stage["target_count"],
            "parent_content_hash": stage["parent_content_hash"],
            "session": stage["session"],
            "blockers": stage["blockers"],
            "candidates": list(candidates),
            **_authority_projection(),
        }
    )
    content_hash = _digest(stage["content_hash"], f"{name}.content_hash")
    if canonical_hash(envelope) != content_hash:
        raise ResearchTop10ValidationError(f"{name} content hash does not match")
    return envelope


def _validate_active_read_projection(
    row: Mapping[str, object],
    stage: Mapping[str, object],
    envelope: Mapping[str, object],
    candidates: Sequence[Mapping[str, object]],
) -> None:
    fields = (
        "phase",
        "trading_date",
        "observed_at",
        "batch_id",
        "content_hash",
        "parent_content_hash",
        "source",
        "strategy_nav_usd",
        "normal_risk_fraction",
        "target_count",
        "session",
        "blockers",
    )
    if any(row[field] != stage[field] for field in fields):
        raise ResearchTop10ValidationError(
            "research active projection does not match its immutable stage"
        )
    if list(candidates) != list(envelope["candidates"]):  # type: ignore[arg-type]
        raise ResearchTop10ValidationError(
            "research active candidates do not match the current phase"
        )


def load_research_top10_import(path: str | Path) -> Mapping[str, object]:
    if str(path) == "-":
        raw = sys.stdin.buffer.read(MAXIMUM_IMPORT_BYTES + 1)
    else:
        source = Path(path)
        try:
            size = source.stat().st_size
            if size <= 0 or size > MAXIMUM_IMPORT_BYTES:
                raise ResearchTop10ValidationError("research import size is invalid")
            raw = source.read_bytes()
        except ResearchTop10ValidationError:
            raise
        except OSError as exc:
            raise ResearchTop10ValidationError("research import is unavailable") from exc
    if not raw or len(raw) > MAXIMUM_IMPORT_BYTES:
        raise ResearchTop10ValidationError("research import size is invalid")
    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique_object)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ResearchTop10ValidationError("research import JSON is invalid") from exc
    return _mapping(value, "research import")


def _normalise_candidate(
    value: object,
    *,
    index: int,
    phase: str,
    trading_date: date,
    strategy_nav: Decimal,
    observed_at: datetime,
) -> dict[str, object]:
    name = f"candidates[{index}]"
    row = _exact_mapping(value, _CANDIDATE_FIELDS, name)
    _supporting_only(row, name)
    rank = _positive_integer(row["rank"], f"{name}.rank")
    strategy = _text(row["strategy_type"], f"{name}.strategy_type").upper()
    if strategy not in _DEBIT_STRATEGIES:
        raise ResearchTop10ValidationError(
            f"{name} must be a supported debit vertical"
        )
    expiration = _date(row["expiration"], f"{name}.expiration")
    dte = _nonnegative_integer(row["dte"], f"{name}.dte")
    if dte != (expiration - trading_date).days or not 14 <= dte <= 35:
        raise ResearchTop10ValidationError(f"{name} must remain within 14-35 DTE")
    quantity = _positive_integer(row["quantity"], f"{name}.quantity")
    blockers = _blockers(row["blockers"], f"{name}.blockers")
    legs = tuple(
        _normalise_leg(
            leg,
            name=f"{name}.legs[{leg_index}]",
            phase=phase,
            parent_observed_at=observed_at,
        )
        for leg_index, leg in enumerate(_array(row["legs"], f"{name}.legs"))
    )
    if len(legs) != 2:
        raise ResearchTop10ValidationError(f"{name} must be a two-leg debit vertical")
    _validate_vertical(strategy, legs, expiration, name)
    buy = next(leg for leg in legs if leg["side"] == "BUY")
    sell = next(leg for leg in legs if leg["side"] == "SELL")
    cost_cap = _money(
        row["execution_cost_cap_usd"],
        f"{name}.execution_cost_cap_usd",
    )
    if cost_cap >= strategy_nav * NORMAL_RISK_FRACTION:
        raise ResearchTop10ValidationError(
            f"{name} known execution cost reaches or breaches the normal 10% cap"
        )
    risk_cap_verified = _boolean(
        row["risk_cap_verified"], f"{name}.risk_cap_verified"
    )
    if row["trade_status"] != "NO_TRADE":
        raise ResearchTop10ValidationError(f"{name}.trade_status must remain NO_TRADE")

    exact_inputs_verified = _exact_risk_inputs_verified(legs, observed_at)
    entry_debit = _optional_money(
        row["entry_debit_usd"], f"{name}.entry_debit_usd"
    )
    indicative_entry_debit = _optional_money(
        row["indicative_entry_debit_usd"],
        f"{name}.indicative_entry_debit_usd",
    )
    maximum_loss = _optional_money(
        row["maximum_loss_usd"], f"{name}.maximum_loss_usd"
    )
    indicative_maximum_loss = _optional_money(
        row["indicative_maximum_loss_usd"],
        f"{name}.indicative_maximum_loss_usd",
    )
    after_cost_ev = _optional_money(
        row["cost_after_ev_usd"], f"{name}.cost_after_ev_usd"
    )
    indicative_after_cost_ev = _optional_money(
        row["indicative_cost_after_ev_usd"],
        f"{name}.indicative_cost_after_ev_usd",
    )
    if (
        after_cost_ev is not None or indicative_after_cost_ev is not None
    ) and "EXPECTED_PAYOFF_UNAVAILABLE" in blockers:
        raise ResearchTop10ValidationError(
            f"{name} available EV conflicts with EXPECTED_PAYOFF_UNAVAILABLE"
        )
    assumed_multiplier = _optional_positive_integer(
        row["assumed_multiplier"], f"{name}.assumed_multiplier"
    )
    quotes_available = all(
        leg["bid"] is not None and leg["ask"] is not None for leg in legs
    )
    if quotes_available and "QUOTE_UNAVAILABLE" in blockers:
        raise ResearchTop10ValidationError(
            f"{name} numeric leg quotes conflict with QUOTE_UNAVAILABLE"
        )
    if assumed_multiplier is not None:
        _validate_assumed_multiplier_against_legs(
            legs,
            assumed_multiplier=assumed_multiplier,
            name=name,
        )

    if risk_cap_verified:
        if not exact_inputs_verified:
            raise ResearchTop10ValidationError(
                f"{name}.risk_cap_verified requires standard live synchronised inputs"
            )
        if assumed_multiplier is not None or any(
            value is not None
            for value in (
                indicative_entry_debit,
                indicative_maximum_loss,
                indicative_after_cost_ev,
            )
        ):
            raise ResearchTop10ValidationError(
                f"{name} verified risk cannot retain indicative multiplier assumptions"
            )
        if entry_debit is None or maximum_loss is None or after_cost_ev is None:
            raise ResearchTop10ValidationError(
                f"{name} verified risk requires exact debit, maximum loss, and EV"
            )
        multiplier = int(buy["multiplier"])
        calculated_debit = _calculated_debit(
            buy=buy,
            sell=sell,
            multiplier=multiplier,
            quantity=quantity,
            name=name,
        )
        if entry_debit != calculated_debit:
            raise ResearchTop10ValidationError(
                f"{name} entry debit does not match leg quotes"
            )
        if maximum_loss != entry_debit + cost_cap:
            raise ResearchTop10ValidationError(f"{name} maximum loss is not exact")
        width = abs(
            _decimal(buy["strike"], "buy strike")
            - _decimal(sell["strike"], "sell strike")
        )
        maximum_payout = width * Decimal(multiplier) * Decimal(quantity)
        if entry_debit > maximum_payout:
            raise ResearchTop10ValidationError(f"{name} debit exceeds vertical width")
        if entry_debit + cost_cap + after_cost_ev > maximum_payout:
            raise ResearchTop10ValidationError(
                f"{name} expected payoff exceeds vertical maximum payout"
            )
        if maximum_loss > strategy_nav * NORMAL_RISK_FRACTION:
            raise ResearchTop10ValidationError(
                f"{name} breaches the normal 10% max-loss cap"
            )
    else:
        if any(value is not None for value in (entry_debit, maximum_loss, after_cost_ev)):
            raise ResearchTop10ValidationError(
                f"{name} unverified identity requires null entry_debit_usd, "
                "maximum_loss_usd, and cost_after_ev_usd"
            )
        _require_blockers(
            blockers,
            {
                "ENTRY_DEBIT_UNVERIFIED",
                "MAXIMUM_LOSS_UNVERIFIED",
                "AFTER_COST_EV_UNVERIFIED",
                "RISK_CAP_UNVERIFIED",
            },
            name,
        )
        indicative_values = (
            indicative_entry_debit,
            indicative_maximum_loss,
            indicative_after_cost_ev,
        )
        if not quotes_available:
            if phase not in {PREMARKET_RESEARCH, INTRADAY_RECOVERY}:
                raise ResearchTop10ValidationError(
                    f"{name} INDICATIVE_REPRICE requires numeric bid and ask"
                )
            if any(value is not None for value in indicative_values):
                raise ResearchTop10ValidationError(
                    f"{name} unquoted premarket research requires null indicative economics"
                )
            if assumed_multiplier is None:
                raise ResearchTop10ValidationError(
                    f"{name} unquoted premarket research must retain assumed_multiplier"
                )
            _require_blockers(
                blockers,
                {
                    "ASSUMED_MULTIPLIER_USED",
                    "QUOTE_UNAVAILABLE",
                    "EXPECTED_PAYOFF_UNAVAILABLE",
                },
                name,
            )
        elif all(value is not None for value in indicative_values):
            if assumed_multiplier is None:
                raise ResearchTop10ValidationError(
                    f"{name} indicative economics require assumed_multiplier"
                )
            _require_blockers(blockers, {"ASSUMED_MULTIPLIER_USED"}, name)
            calculated_debit = _calculated_debit(
                buy=buy,
                sell=sell,
                multiplier=assumed_multiplier,
                quantity=quantity,
                name=name,
            )
            if indicative_entry_debit != calculated_debit:
                raise ResearchTop10ValidationError(
                    f"{name} indicative debit does not match the declared assumption"
                )
            if indicative_maximum_loss != indicative_entry_debit + cost_cap:
                raise ResearchTop10ValidationError(
                    f"{name} indicative maximum loss does not match the declared assumption"
                )
            width = abs(
                _decimal(buy["strike"], "buy strike")
                - _decimal(sell["strike"], "sell strike")
            )
            maximum_payout = (
                width * Decimal(assumed_multiplier) * Decimal(quantity)
            )
            if indicative_entry_debit > maximum_payout:
                raise ResearchTop10ValidationError(
                    f"{name} indicative debit exceeds vertical width"
                )
            if (
                indicative_entry_debit
                + cost_cap
                + indicative_after_cost_ev
                > maximum_payout
            ):
                raise ResearchTop10ValidationError(
                    f"{name} expected payoff exceeds vertical maximum payout"
                )
            if indicative_maximum_loss > strategy_nav * NORMAL_RISK_FRACTION:
                raise ResearchTop10ValidationError(
                    f"{name} indicative maximum loss breaches the normal 10% cap"
                )
        elif (
            phase in {INDICATIVE_REPRICE, INTRADAY_RECOVERY}
            and indicative_entry_debit is not None
            and indicative_maximum_loss is not None
            and indicative_after_cost_ev is None
        ):
            if assumed_multiplier is None:
                raise ResearchTop10ValidationError(
                    f"{name} indicative economics require assumed_multiplier"
                )
            _require_blockers(
                blockers,
                {"ASSUMED_MULTIPLIER_USED", "EXPECTED_PAYOFF_UNAVAILABLE"},
                name,
            )
            calculated_debit = _calculated_debit(
                buy=buy,
                sell=sell,
                multiplier=assumed_multiplier,
                quantity=quantity,
                name=name,
            )
            if indicative_entry_debit != calculated_debit:
                raise ResearchTop10ValidationError(
                    f"{name} indicative debit does not match the declared assumption"
                )
            if indicative_maximum_loss != indicative_entry_debit + cost_cap:
                raise ResearchTop10ValidationError(
                    f"{name} indicative maximum loss does not match the declared assumption"
                )
            width = abs(
                _decimal(buy["strike"], "buy strike")
                - _decimal(sell["strike"], "sell strike")
            )
            if (
                indicative_entry_debit
                > width * Decimal(assumed_multiplier) * Decimal(quantity)
            ):
                raise ResearchTop10ValidationError(
                    f"{name} indicative debit exceeds vertical width"
                )
            if indicative_maximum_loss > strategy_nav * NORMAL_RISK_FRACTION:
                raise ResearchTop10ValidationError(
                    f"{name} indicative maximum loss breaches the normal 10% cap"
                )
        elif any(value is not None for value in indicative_values):
            raise ResearchTop10ValidationError(
                f"{name} indicative economics cannot be partial"
            )
        else:
            raise ResearchTop10ValidationError(
                f"{name} quoted research requires complete indicative economics"
            )
    evidence_ids = _text_array(row["evidence_ids"], f"{name}.evidence_ids")
    evidence_hashes = _digest_array(row["evidence_hashes"], f"{name}.evidence_hashes")
    if not evidence_ids or len(evidence_ids) != len(evidence_hashes):
        raise ResearchTop10ValidationError(f"{name} evidence is incomplete")
    return {
        "research_id": _identifier(row["research_id"], f"{name}.research_id"),
        "rank": rank,
        "underlying": _symbol(row["underlying"], f"{name}.underlying"),
        "strategy_type": strategy,
        "expiration": expiration.isoformat(),
        "dte": dte,
        "quantity": quantity,
        "entry_debit_usd": _optional_money_text(entry_debit),
        "indicative_entry_debit_usd": _optional_money_text(
            indicative_entry_debit
        ),
        "execution_cost_cap_usd": _decimal_text(cost_cap, money=True),
        "maximum_loss_usd": _optional_money_text(maximum_loss),
        "indicative_maximum_loss_usd": _optional_money_text(
            indicative_maximum_loss
        ),
        "cost_after_ev_usd": _optional_money_text(after_cost_ev),
        "indicative_cost_after_ev_usd": _optional_money_text(
            indicative_after_cost_ev
        ),
        "assumed_multiplier": assumed_multiplier,
        "risk_cap_verified": risk_cap_verified,
        "trade_status": "NO_TRADE",
        "entry_condition": _text(row["entry_condition"], f"{name}.entry_condition"),
        "invalidation_condition": _text(
            row["invalidation_condition"], f"{name}.invalidation_condition"
        ),
        "profit_target_condition": _text(
            row["profit_target_condition"], f"{name}.profit_target_condition"
        ),
        "stop_loss_condition": _text(
            row["stop_loss_condition"], f"{name}.stop_loss_condition"
        ),
        "research_summary": _text(row["research_summary"], f"{name}.research_summary"),
        "evidence_ids": list(evidence_ids),
        "evidence_hashes": list(evidence_hashes),
        "blockers": list(blockers),
        "legs": list(legs),
        **_authority_projection(),
    }


def _normalise_leg(
    value: object,
    *,
    name: str,
    phase: str,
    parent_observed_at: datetime,
) -> dict[str, object]:
    row = _exact_mapping(value, _LEG_FIELDS, name)
    blockers = _blockers(row["blockers"], f"{name}.blockers")
    blocker_set = set(blockers)
    contract_id = _positive_integer(row["contract_id"], f"{name}.contract_id")
    contract_id_ex = _text(row["contract_id_ex"], f"{name}.contract_id_ex")
    if not contract_id_ex.startswith(f"{contract_id}@"):
        raise ResearchTop10ValidationError(f"{name}.contract_id_ex is not contract-bound")
    local_symbol = _optional_text(row["local_symbol"], f"{name}.local_symbol")
    _validate_optional_fact(
        local_symbol,
        blockers=blocker_set,
        unavailable_blocker="LOCAL_SYMBOL_UNAVAILABLE",
        name=f"{name}.local_symbol",
    )
    multiplier = _optional_positive_integer(
        row["multiplier"], f"{name}.multiplier"
    )
    _validate_optional_fact(
        multiplier,
        blockers=blocker_set,
        unavailable_blocker="MULTIPLIER_UNAVAILABLE",
        name=f"{name}.multiplier",
    )
    standard_or_adjusted = row["standard_or_adjusted"]
    _validate_optional_fact(
        standard_or_adjusted,
        blockers=blocker_set,
        unavailable_blocker="STANDARD_ADJUSTED_UNVERIFIED",
        name=f"{name}.standard_or_adjusted",
    )
    if standard_or_adjusted is not None:
        standard_or_adjusted = _choice(
            standard_or_adjusted,
            frozenset({"STANDARD", "ADJUSTED"}),
            f"{name}.standard_or_adjusted",
        )
        if (
            standard_or_adjusted == "ADJUSTED"
            and "ADJUSTED_CONTRACT_UNSUPPORTED" not in blocker_set
        ):
            raise ResearchTop10ValidationError(
                f"{name} adjusted contract requires ADJUSTED_CONTRACT_UNSUPPORTED"
            )
    bid = _optional_decimal(row["bid"], f"{name}.bid")
    ask = _optional_decimal(row["ask"], f"{name}.ask")
    if (bid is None) != (ask is None):
        raise ResearchTop10ValidationError(
            f"{name} bid and ask must both be numeric or both be null"
        )
    if bid is None:
        if phase not in {PREMARKET_RESEARCH, INTRADAY_RECOVERY}:
            raise ResearchTop10ValidationError(
                f"{name} INDICATIVE_REPRICE requires numeric bid and ask"
            )
        if "QUOTE_UNAVAILABLE" not in blocker_set:
            raise ResearchTop10ValidationError(
                f"{name} null bid and ask require QUOTE_UNAVAILABLE"
            )
        if row["quote_asof"] is not None:
            raise ResearchTop10ValidationError(
                f"{name} unquoted bid and ask require null quote_asof"
            )
    else:
        if "QUOTE_UNAVAILABLE" in blocker_set:
            raise ResearchTop10ValidationError(
                f"{name} numeric bid and ask conflict with QUOTE_UNAVAILABLE"
            )
        if bid < 0 or ask is None or ask <= 0:
            raise ResearchTop10ValidationError(f"{name} quote is invalid")
        if bid > ask:
            raise ResearchTop10ValidationError(f"{name} quote is crossed")
    collected_at = _timestamp(row["collected_at"], f"{name}.collected_at")
    if collected_at > parent_observed_at:
        raise ResearchTop10ValidationError(
            f"{name}.collected_at cannot be after research observed_at"
        )
    quote_asof = row["quote_asof"]
    _validate_optional_fact(
        quote_asof,
        blockers=blocker_set,
        unavailable_blocker="QUOTE_EXCHANGE_TIMESTAMP_UNAVAILABLE",
        name=f"{name}.quote_asof",
    )
    if quote_asof is not None:
        quote_asof = _timestamp(quote_asof, f"{name}.quote_asof")
        if quote_asof > collected_at:
            raise ResearchTop10ValidationError(
                f"{name}.quote_asof cannot be after collected_at"
            )
    implied_volatility = _optional_decimal(
        row["implied_volatility"], f"{name}.implied_volatility"
    )
    if implied_volatility is None and "IMPLIED_VOLATILITY_UNAVAILABLE" not in blocker_set:
        raise ResearchTop10ValidationError(
            f"{name} null implied volatility requires IMPLIED_VOLATILITY_UNAVAILABLE"
        )
    if implied_volatility is not None and implied_volatility < 0:
        raise ResearchTop10ValidationError(f"{name} implied volatility is invalid")
    greeks: dict[str, Decimal | None] = {}
    for field, blocker in _GREEK_BLOCKERS.items():
        value = _optional_decimal(row[field], f"{name}.{field}")
        if value is None and blocker not in blocker_set:
            raise ResearchTop10ValidationError(
                f"{name} null {field} requires {blocker}"
            )
        greeks[field] = value
    delta = greeks["delta"]
    if delta is not None and not Decimal("-1") <= delta <= Decimal("1"):
        raise ResearchTop10ValidationError(f"{name}.delta is outside -1 to 1")
    for field in ("gamma", "vega"):
        value = greeks[field]
        if value is not None and value < 0:
            raise ResearchTop10ValidationError(f"{name}.{field} cannot be negative")
    volume = _optional_nonnegative_integer(row["volume"], f"{name}.volume")
    if volume is None and "VOLUME_UNAVAILABLE" not in blocker_set:
        raise ResearchTop10ValidationError(
            f"{name} null volume requires VOLUME_UNAVAILABLE"
        )
    open_interest = _optional_nonnegative_integer(
        row["open_interest"], f"{name}.open_interest"
    )
    if open_interest is None and "OPEN_INTEREST_UNAVAILABLE" not in blocker_set:
        raise ResearchTop10ValidationError(
            f"{name} null open interest requires OPEN_INTEREST_UNAVAILABLE"
        )
    market_data_type = row["market_data_type"]
    if market_data_type is None:
        if "MARKET_DATA_TYPE_UNVERIFIED" not in blocker_set:
            raise ResearchTop10ValidationError(
                f"{name} null market_data_type requires MARKET_DATA_TYPE_UNVERIFIED"
            )
    else:
        market_data_type = _positive_integer(
            market_data_type, f"{name}.market_data_type"
        )
        if market_data_type not in {1, 2, 3, 4}:
            raise ResearchTop10ValidationError(f"{name}.market_data_type is invalid")
        if market_data_type != 1 and "MARKET_DATA_NOT_LIVE" not in blocker_set:
            raise ResearchTop10ValidationError(
                f"{name} non-live market data requires MARKET_DATA_NOT_LIVE"
            )
    return {
        "contract_id": contract_id,
        "contract_id_ex": contract_id_ex,
        "side": _choice(row["side"], frozenset({"BUY", "SELL"}), f"{name}.side"),
        "right": _choice(row["right"], frozenset({"C", "P"}), f"{name}.right"),
        "strike": _decimal_text(_positive_decimal(row["strike"], f"{name}.strike")),
        "expiration": _date(row["expiration"], f"{name}.expiration").isoformat(),
        "exchange": _text(row["exchange"], f"{name}.exchange").upper(),
        "trading_class": _text(row["trading_class"], f"{name}.trading_class"),
        "local_symbol": local_symbol,
        "multiplier": multiplier,
        "standard_or_adjusted": standard_or_adjusted,
        "bid": _optional_decimal_text(bid),
        "ask": _optional_decimal_text(ask),
        "collected_at": _timestamp_text(collected_at),
        "quote_asof": None if quote_asof is None else _timestamp_text(quote_asof),
        "implied_volatility": _optional_decimal_text(implied_volatility),
        "delta": _optional_decimal_text(greeks["delta"]),
        "gamma": _optional_decimal_text(greeks["gamma"]),
        "theta": _optional_decimal_text(greeks["theta"]),
        "vega": _optional_decimal_text(greeks["vega"]),
        "volume": volume,
        "open_interest": open_interest,
        "market_data_type": market_data_type,
        "identity_evidence_hash": _digest(
            row["identity_evidence_hash"], f"{name}.identity_evidence_hash"
        ),
        "quote_evidence_hash": _digest(
            row["quote_evidence_hash"], f"{name}.quote_evidence_hash"
        ),
        "blockers": list(blockers),
    }


def _normalise_session(value: object) -> dict[str, object]:
    row = _exact_mapping(value, _SESSION_FIELDS, "session")
    blockers = _blockers(row["blockers"], "session.blockers")
    values = tuple(row[field] for field in (
        "liquid_hours", "trading_hours", "timezone_id", "observed_at", "source"
    ))
    if all(item is None for item in values):
        if "BROKER_SESSION_HOURS_UNAVAILABLE" not in blockers:
            raise ResearchTop10ValidationError(
                "null session fields require BROKER_SESSION_HOURS_UNAVAILABLE"
            )
        return {
            "liquid_hours": None,
            "trading_hours": None,
            "timezone_id": None,
            "observed_at": None,
            "source": None,
            "blockers": list(blockers),
        }
    if any(item is None for item in values):
        raise ResearchTop10ValidationError("session fields cannot be partial")
    return {
        "liquid_hours": _text(row["liquid_hours"], "session.liquid_hours"),
        "trading_hours": _text(row["trading_hours"], "session.trading_hours"),
        "timezone_id": _text(row["timezone_id"], "session.timezone_id"),
        "observed_at": _timestamp_text(
            _timestamp(row["observed_at"], "session.observed_at")
        ),
        "source": _text(row["source"], "session.source"),
        "blockers": list(blockers),
    }


def _validate_vertical(
    strategy: str,
    legs: tuple[dict[str, object], ...],
    expiration: date,
    name: str,
) -> None:
    if any(leg["expiration"] != expiration.isoformat() for leg in legs):
        raise ResearchTop10ValidationError(f"{name} leg expiration differs")
    if len({leg["multiplier"] for leg in legs}) != 1:
        raise ResearchTop10ValidationError(f"{name} multiplier differs within vertical")
    if len({leg["standard_or_adjusted"] for leg in legs}) != 1:
        raise ResearchTop10ValidationError(
            f"{name} standard/adjusted identity differs within vertical"
        )
    sides = {str(leg["side"]) for leg in legs}
    if sides != {"BUY", "SELL"}:
        raise ResearchTop10ValidationError(f"{name} must have one BUY and one SELL leg")
    buy = next(leg for leg in legs if leg["side"] == "BUY")
    sell = next(leg for leg in legs if leg["side"] == "SELL")
    buy_strike = _decimal(buy["strike"], "buy strike")
    sell_strike = _decimal(sell["strike"], "sell strike")
    if strategy == "BULL_CALL_VERTICAL":
        valid = buy["right"] == sell["right"] == "C" and buy_strike < sell_strike
    else:
        valid = buy["right"] == sell["right"] == "P" and buy_strike > sell_strike
    if not valid:
        raise ResearchTop10ValidationError(f"{name} is not the declared debit vertical")


def _exact_risk_inputs_verified(
    legs: Sequence[Mapping[str, object]],
    observed_at: datetime,
) -> bool:
    if not all(
        leg["local_symbol"] is not None
        and leg["multiplier"] == 100
        and leg["standard_or_adjusted"] == "STANDARD"
        and leg["market_data_type"] == 1
        and leg["bid"] is not None
        and leg["ask"] is not None
        and leg["quote_asof"] is not None
        for leg in legs
    ):
        return False
    quote_times = [
        _timestamp(leg["quote_asof"], "quote_asof")
        for leg in legs
    ]
    if any(
        observed_at - quoted_at < timedelta(0)
        or observed_at - quoted_at > _EXACT_QUOTE_MAX_AGE
        for quoted_at in quote_times
    ):
        return False
    return max(quote_times) - min(quote_times) <= _EXACT_QUOTE_MAX_SKEW


def _validate_assumed_multiplier_against_legs(
    legs: Sequence[Mapping[str, object]],
    *,
    assumed_multiplier: int,
    name: str,
) -> None:
    known = {
        int(leg["multiplier"])
        for leg in legs
        if leg.get("multiplier") is not None
    }
    if known and assumed_multiplier not in known:
        raise ResearchTop10ValidationError(
            f"{name} assumed_multiplier differs from known contract multiplier"
        )
    if any(leg.get("multiplier") is None for leg in legs) and (
        assumed_multiplier != _POLICY_ASSUMED_MULTIPLIER
    ):
        raise ResearchTop10ValidationError(
            f"{name} unavailable contract multiplier requires policy assumed_multiplier"
        )


def _calculated_debit(
    *,
    buy: Mapping[str, object],
    sell: Mapping[str, object],
    multiplier: int,
    quantity: int,
    name: str,
) -> Decimal:
    debit = (
        (_decimal(buy["ask"], "buy ask") - _decimal(sell["bid"], "sell bid"))
        * Decimal(multiplier)
        * Decimal(quantity)
    ).quantize(_CENT, rounding=ROUND_HALF_UP)
    if debit <= 0:
        raise ResearchTop10ValidationError(
            f"{name} must be a positive debit vertical"
        )
    return debit


def _validate_parent_binding(
    parent: Mapping[str, object],
    child: Mapping[str, object],
) -> None:
    for field in ("strategy_nav_usd", "normal_risk_fraction"):
        if parent.get(field) != child.get(field):
            raise ResearchTop10Conflict(
                f"indicative reprice parent {field} differs"
            )
    parent_candidates = _array(parent.get("candidates"), "parent candidates")
    child_candidates = _array(child.get("candidates"), "child candidates")
    if len(parent_candidates) != len(child_candidates):
        raise ResearchTop10Conflict("indicative reprice parent candidate count differs")
    if tuple(_candidate_parent_identity(item) for item in parent_candidates) != tuple(
        _candidate_parent_identity(item) for item in child_candidates
    ):
        raise ResearchTop10Conflict("indicative reprice parent identity differs")
    for index, (parent_candidate, child_candidate) in enumerate(
        zip(parent_candidates, child_candidates, strict=True)
    ):
        parent_row = _mapping(parent_candidate, f"parent candidates[{index}]")
        child_row = _mapping(child_candidate, f"child candidates[{index}]")
        parent_debit, parent_after_cost_ev = _candidate_bound_economics(
            parent_row,
            name=f"parent candidates[{index}]",
        )
        child_debit, child_after_cost_ev = _candidate_bound_economics(
            child_row,
            name=f"child candidates[{index}]",
        )
        child_blockers = set(
            _blockers(child_row.get("blockers"), f"child candidates[{index}].blockers")
        )
        if parent_debit is None and parent_after_cost_ev is None:
            if (
                child_after_cost_ev is not None
                or "EXPECTED_PAYOFF_UNAVAILABLE" not in child_blockers
            ):
                raise ResearchTop10Conflict(
                    "indicative reprice parent expected payoff is unavailable"
                )
            continue
        if parent_debit is None or parent_after_cost_ev is None:
            raise ResearchTop10Conflict(
                "indicative reprice parent expected payoff is incomplete"
            )
        if child_debit is None or child_after_cost_ev is None:
            raise ResearchTop10Conflict(
                "indicative reprice child expected payoff is incomplete"
            )
        expected_payoff = (
            parent_debit
            + _money(
                parent_row.get("execution_cost_cap_usd"),
                f"parent candidates[{index}].execution_cost_cap_usd",
            )
            + parent_after_cost_ev
        )
        expected_child_ev = (
            expected_payoff
            - child_debit
            - _money(
                child_row.get("execution_cost_cap_usd"),
                f"child candidates[{index}].execution_cost_cap_usd",
            )
        )
        if (
            child_after_cost_ev != expected_child_ev
            or "EXPECTED_PAYOFF_UNAVAILABLE" in child_blockers
        ):
            raise ResearchTop10Conflict(
                "indicative reprice child expected payoff differs from parent"
            )


def _candidate_bound_economics(
    value: Mapping[str, object],
    *,
    name: str,
) -> tuple[Decimal | None, Decimal | None]:
    if value.get("risk_cap_verified") is True:
        debit_field = "entry_debit_usd"
        ev_field = "cost_after_ev_usd"
    else:
        debit_field = "indicative_entry_debit_usd"
        ev_field = "indicative_cost_after_ev_usd"
    return (
        _optional_money(value.get(debit_field), f"{name}.{debit_field}"),
        _optional_money(value.get(ev_field), f"{name}.{ev_field}"),
    )


def _candidate_parent_identity(value: Mapping[str, object]) -> str:
    legs = _array(value.get("legs"), "candidate legs")
    return canonical_hash(
        {
            "research_id": value.get("research_id"),
            "rank": value.get("rank"),
            "underlying": value.get("underlying"),
            "strategy_type": value.get("strategy_type"),
            "expiration": value.get("expiration"),
            "dte": value.get("dte"),
            "quantity": value.get("quantity"),
            "execution_cost_cap_usd": value.get("execution_cost_cap_usd"),
            "assumed_multiplier": value.get("assumed_multiplier"),
            "entry_condition": value.get("entry_condition"),
            "invalidation_condition": value.get("invalidation_condition"),
            "profit_target_condition": value.get("profit_target_condition"),
            "stop_loss_condition": value.get("stop_loss_condition"),
            "research_summary": value.get("research_summary"),
            "evidence_ids": value.get("evidence_ids"),
            "evidence_hashes": value.get("evidence_hashes"),
            "legs": [
                {
                    key: leg.get(key)
                    for key in (
                        "contract_id",
                        "contract_id_ex",
                        "side",
                        "right",
                        "strike",
                        "expiration",
                        "exchange",
                        "trading_class",
                        "local_symbol",
                        "multiplier",
                        "standard_or_adjusted",
                        "identity_evidence_hash",
                    )
                }
                for leg in legs
            ],
        }
    )


def _stage_projection(
    payload: Mapping[str, object],
    content_hash: str,
) -> dict[str, object]:
    return {
        "phase": payload["phase"],
        "content_hash": content_hash,
        "parent_content_hash": payload["parent_content_hash"],
        "batch_id": payload["batch_id"],
        "trading_date": payload["trading_date"],
        "observed_at": payload["observed_at"],
        "source": payload["source"],
        "strategy_nav_usd": payload["strategy_nav_usd"],
        "normal_risk_fraction": payload["normal_risk_fraction"],
        "target_count": payload["target_count"],
        "session": payload["session"],
        "blockers": payload["blockers"],
        "available_count": len(payload["candidates"]),  # type: ignore[arg-type]
    }


def _import_result(
    payload: Mapping[str, object],
    content_hash: str,
    status: str,
) -> dict[str, object]:
    return {
        "schema": "options_copilot.research_top10_import_result.v2",
        "status": status,
        "phase": payload["phase"],
        "trading_date": payload["trading_date"],
        "batch_id": payload["batch_id"],
        "content_hash": content_hash,
        "available_count": len(payload["candidates"]),  # type: ignore[arg-type]
        "target_count": TARGET_COUNT,
        "decision_authority": "SUPPORTING_ONLY",
        "approval_eligible": False,
        "instruction_creation_allowed": False,
        "order_allowed": False,
        "action_pool_eligible": False,
    }


def _authority_projection() -> dict[str, object]:
    return {
        "decision_authority": "SUPPORTING_ONLY",
        "approval_eligible": False,
        "instruction_creation_allowed": False,
        "order_allowed": False,
        "action_pool_eligible": False,
    }


def _supporting_only(row: Mapping[str, object], name: str) -> None:
    if (
        row.get("decision_authority") != "SUPPORTING_ONLY"
        or row.get("approval_eligible") is not False
        or row.get("instruction_creation_allowed") is not False
        or row.get("order_allowed") is not False
        or row.get("action_pool_eligible") is not False
    ):
        raise ResearchTop10ValidationError(f"{name} must remain SUPPORTING_ONLY")


def _bounded_safe_json(value: object) -> str:
    _assert_safe_content(value)
    rendered = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    if len(rendered.encode("utf-8")) > MAXIMUM_READ_MODEL_BYTES:
        raise ResearchTop10ValidationError("research read model exceeds one MiB")
    return rendered


def _assert_safe_content(value: object) -> None:
    try:
        assert_no_secret_like(value, path="$.research_top10")
    except SecretLikeFieldError as exc:
        raise ResearchTop10ValidationError(
            "research content contains secret-like material"
        ) from exc
    _assert_no_local_paths(value, path="$.research_top10")


def _assert_no_local_paths(value: object, *, path: str) -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            _assert_no_local_paths(item, path=f"{path}.{key}")
        return
    if isinstance(value, Sequence) and not isinstance(
        value, (str, bytes, bytearray, memoryview)
    ):
        for index, item in enumerate(value):
            _assert_no_local_paths(item, path=f"{path}[{index}]")
        return
    if isinstance(value, str) and _LOCAL_PATH.search(value):
        raise ResearchTop10ValidationError(
            f"research content contains a local path at {path}"
        )


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _exact_mapping(
    value: object,
    fields: set[str],
    name: str,
) -> Mapping[str, object]:
    row = _mapping(value, name)
    if set(row) != fields:
        raise ResearchTop10ValidationError(f"{name} fields are incomplete")
    return row


def _mapping(value: object, name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or not all(isinstance(key, str) for key in value):
        raise ResearchTop10ValidationError(f"{name} must be an object")
    return value


def _array(value: object, name: str) -> tuple[Mapping[str, object], ...]:
    if isinstance(value, (str, bytes, bytearray, memoryview)) or not isinstance(
        value, Sequence
    ):
        raise ResearchTop10ValidationError(f"{name} must be an array")
    return tuple(_mapping(item, f"{name}[]") for item in value)


def _text(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ResearchTop10ValidationError(f"{name} cannot be blank")
    text = value.strip()
    if len(text) > MAXIMUM_TEXT_CHARS:
        raise ResearchTop10ValidationError(
            f"{name} exceeds {MAXIMUM_TEXT_CHARS} characters"
        )
    return text


def _optional_text(value: object, name: str) -> str | None:
    return None if value is None else _text(value, name)


def _boolean(value: object, name: str) -> bool:
    if not isinstance(value, bool):
        raise ResearchTop10ValidationError(f"{name} must be a boolean")
    return value


def _identifier(value: object, name: str) -> str:
    text = _text(value, name)
    if _IDENTIFIER.fullmatch(text) is None:
        raise ResearchTop10ValidationError(f"{name} is invalid")
    return text


def _symbol(value: object, name: str) -> str:
    text = _text(value, name).upper()
    if len(text) > 16 or not re.fullmatch(r"[A-Z0-9.-]+", text):
        raise ResearchTop10ValidationError(f"{name} is invalid")
    return text


def _choice(value: object, choices: frozenset[str], name: str) -> str:
    text = _text(value, name).upper()
    if text not in choices:
        raise ResearchTop10ValidationError(f"{name} is unsupported")
    return text


def _digest(value: object, name: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise ResearchTop10ValidationError(f"{name} must be a SHA-256 digest")
    return value


def _date(value: object, name: str) -> date:
    if isinstance(value, datetime):
        raise ResearchTop10ValidationError(f"{name} cannot be a datetime")
    if isinstance(value, date):
        return value
    if not isinstance(value, str):
        raise ResearchTop10ValidationError(f"{name} must be a date")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ResearchTop10ValidationError(f"{name} must be a date") from exc


def _timestamp(value: object, name: str) -> datetime:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ResearchTop10ValidationError(f"{name} is invalid") from exc
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ResearchTop10ValidationError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _timestamp_text(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds")


def _decimal(value: object, name: str) -> Decimal:
    if isinstance(value, bool) or isinstance(value, float):
        raise ResearchTop10ValidationError(f"{name} must be an exact decimal")
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ResearchTop10ValidationError(f"{name} must be a decimal") from exc
    if not result.is_finite():
        raise ResearchTop10ValidationError(f"{name} must be finite")
    return result


def _positive_decimal(value: object, name: str) -> Decimal:
    result = _decimal(value, name)
    if result <= 0:
        raise ResearchTop10ValidationError(f"{name} must be positive")
    return result


def _nonnegative_decimal(value: object, name: str) -> Decimal:
    result = _decimal(value, name)
    if result < 0:
        raise ResearchTop10ValidationError(f"{name} cannot be negative")
    return result


def _optional_decimal(value: object, name: str) -> Decimal | None:
    return None if value is None else _decimal(value, name)


def _money(value: object, name: str) -> Decimal:
    result = _positive_decimal(value, name)
    if result != result.quantize(_CENT):
        raise ResearchTop10ValidationError(f"{name} must use exact cents")
    return result


def _optional_money(value: object, name: str) -> Decimal | None:
    return None if value is None else _money(value, name)


def _decimal_text(value: Decimal, *, money: bool = False) -> str:
    if money:
        return format(value.quantize(_CENT), ".2f")
    normalized = value.normalize()
    return "0" if not normalized else format(normalized, "f")


def _optional_decimal_text(value: Decimal | None) -> str | None:
    return None if value is None else _decimal_text(value)


def _optional_money_text(value: Decimal | None) -> str | None:
    return None if value is None else _decimal_text(value, money=True)


def _positive_integer(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ResearchTop10ValidationError(f"{name} must be a positive integer")
    return value


def _nonnegative_integer(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ResearchTop10ValidationError(f"{name} must be a non-negative integer")
    return value


def _optional_nonnegative_integer(value: object, name: str) -> int | None:
    return None if value is None else _nonnegative_integer(value, name)


def _optional_positive_integer(value: object, name: str) -> int | None:
    return None if value is None else _positive_integer(value, name)


def _validate_optional_fact(
    value: object,
    *,
    blockers: set[str],
    unavailable_blocker: str,
    name: str,
) -> None:
    if value is None and unavailable_blocker not in blockers:
        raise ResearchTop10ValidationError(
            f"{name} null requires {unavailable_blocker}"
        )
    if value is not None and unavailable_blocker in blockers:
        raise ResearchTop10ValidationError(
            f"{name} conflicts with {unavailable_blocker}"
        )


def _require_blockers(
    blockers: Sequence[str],
    required: set[str],
    name: str,
) -> None:
    missing = sorted(required.difference(blockers))
    if missing:
        raise ResearchTop10ValidationError(
            f"{name} requires blocker {missing[0]}"
        )


def _blockers(value: object, name: str) -> tuple[str, ...]:
    if isinstance(value, (str, bytes, bytearray, memoryview)) or not isinstance(
        value, Sequence
    ):
        raise ResearchTop10ValidationError(f"{name} must be an array")
    result: list[str] = []
    for item in value:
        text = _text(item, name).upper()
        if _BLOCKER.fullmatch(text) is None:
            raise ResearchTop10ValidationError(f"{name} contains an invalid blocker")
        if text in result:
            raise ResearchTop10ValidationError(f"{name} contains a duplicate blocker")
        result.append(text)
    return tuple(result)


def _text_array(value: object, name: str) -> tuple[str, ...]:
    if isinstance(value, (str, bytes, bytearray, memoryview)) or not isinstance(
        value, Sequence
    ):
        raise ResearchTop10ValidationError(f"{name} must be an array")
    result = tuple(_text(item, name) for item in value)
    if len(result) != len(set(result)):
        raise ResearchTop10ValidationError(f"{name} contains duplicates")
    return result


def _digest_array(value: object, name: str) -> tuple[str, ...]:
    if isinstance(value, (str, bytes, bytearray, memoryview)) or not isinstance(
        value, Sequence
    ):
        raise ResearchTop10ValidationError(f"{name} must be an array")
    result = tuple(_digest(item, name) for item in value)
    if len(result) != len(set(result)):
        raise ResearchTop10ValidationError(f"{name} contains duplicates")
    return result


def _load_stored_payload(
    value: str,
    *,
    expected_content_hash: str,
) -> Mapping[str, object]:
    try:
        row = json.loads(value, object_pairs_hook=_unique_object)
    except json.JSONDecodeError as exc:
        raise ResearchTop10ValidationError("stored research payload is invalid") from exc
    normalized = validate_research_top10_envelope(row)
    if json.dumps(normalized, sort_keys=True, separators=(",", ":")) != json.dumps(
        row, sort_keys=True, separators=(",", ":")
    ):
        raise ResearchTop10ValidationError("stored research payload is noncanonical")
    if canonical_hash(normalized) != _digest(
        expected_content_hash, "stored content_hash"
    ):
        raise ResearchTop10ValidationError(
            "stored research payload content hash does not match"
        )
    return normalized


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ResearchTop10ValidationError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _rollback(connection: sqlite3.Connection) -> None:
    try:
        connection.execute("ROLLBACK")
    except sqlite3.Error:
        pass


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Import one managed-plugin supporting-only research Top-10 envelope"
    )
    parser.add_argument("--input", required=True, help="UTF-8 JSON path or - for stdin")
    parser.add_argument("--store", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(list(argv) if argv is not None else None)
    try:
        result = import_research_top10(
            load_research_top10_import(args.input),
            store_path=args.store,
        )
    except Exception:
        sys.stderr.write("NO_TRADE: RESEARCH_TOP10_IMPORT_INVALID\n")
        return 2
    sys.stdout.write(json.dumps(result, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())


__all__ = [
    "INDICATIVE_REPRICE",
    "INTRADAY_RECOVERY",
    "PREMARKET_RESEARCH",
    "RESEARCH_TOP10_DIRECT_SOURCE",
    "RESEARCH_TOP10_READ_MODEL_SCHEMA",
    "RESEARCH_TOP10_SCHEMA",
    "RESEARCH_TOP10_SOURCE",
    "RESEARCH_TOP10_VERSION",
    "ResearchTop10Conflict",
    "ResearchTop10Error",
    "ResearchTop10Store",
    "ResearchTop10ValidationError",
    "default_research_top10_store_path",
    "import_research_top10",
    "load_research_top10_import",
    "main",
    "read_research_top10",
    "safe_research_top10_read_model",
    "unavailable_research_top10_read_model",
    "validate_research_top10_envelope",
]
