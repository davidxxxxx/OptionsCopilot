"""Restart-safe, fail-closed cadence state for bounded news source lanes."""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import re
from typing import Any
from uuid import uuid4


CADENCE_SCHEMA = "options_copilot.source_cadence.v1"
_UTC = timezone.utc


@dataclass(frozen=True, slots=True)
class CadencePolicy:
    interval_seconds: int
    failure_retry_seconds: int | None = None


_POLICIES = {
    "SEC:NEWS": CadencePolicy(90),
    "FINNHUB:NEWS": CadencePolicy(90),
    "JIN10:NEWS": CadencePolicy(90),
    "COMPANY_IR:NEWS": CadencePolicy(900),
    "NASDAQ:CALENDAR": CadencePolicy(900),
    "FINNHUB:CALENDAR": CadencePolicy(900),
    "OFFICIAL_CALENDAR:OFFICIAL_CALENDAR": CadencePolicy(900, 300),
    "REACTION:REACTION": CadencePolicy(90),
    "ALPHA_VANTAGE:NEWS": CadencePolicy(86400),
}


def canonical_source_id(value: object) -> str:
    raw = str(value or "").strip().upper()
    normalized = re.sub(r"[^A-Z0-9]+", "_", raw).strip("_")
    aliases = {
        "COMPANYIREVENTPROVIDER": "COMPANY_IR",
        "COMPANY_IR_EVENT_PROVIDER": "COMPANY_IR",
        "ALPHAVANTAGE": "ALPHA_VANTAGE",
        "ALPHA_VANTAGE_NEWS": "ALPHA_VANTAGE",
        "FINNHUBNEWS": "FINNHUB",
        "FINNHUBCALENDAR": "FINNHUB",
    }
    return aliases.get(normalized, normalized or "UNKNOWN")


def cadence_policy(source_id: str, source_kind: str) -> CadencePolicy:
    return _POLICIES.get(
        f"{canonical_source_id(source_id)}:{source_kind.strip().upper()}",
        CadencePolicy(90),
    )


def _aware(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("cadence timestamps must be timezone-aware")
    return value.astimezone(_UTC)


def _timestamp(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(_UTC)


class SourceCadenceStore:
    """Atomic JSON cadence state; malformed state suppresses all provider calls."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._corrupt = False
        self._rows: dict[str, dict[str, Any]] = {}
        if self.path.exists():
            try:
                document = json.loads(self.path.read_text(encoding="utf-8"))
                if not isinstance(document, Mapping) or document.get("schema") != CADENCE_SCHEMA:
                    raise ValueError("invalid cadence schema")
                rows = document.get("sources")
                if not isinstance(rows, Mapping):
                    raise ValueError("invalid cadence sources")
                loaded_rows: dict[str, dict[str, Any]] = {}
                for key, value in rows.items():
                    if not isinstance(key, str) or not isinstance(value, Mapping):
                        raise ValueError("invalid cadence row")
                    loaded_rows[key] = dict(value)
                self._validate_rows(loaded_rows)
                self._rows = loaded_rows
            except (OSError, UnicodeError, json.JSONDecodeError, ValueError, TypeError):
                self._rows = {}
                self._corrupt = True

    @property
    def corrupt(self) -> bool:
        return self._corrupt

    def register(self, source_id: str, source_kind: str, *, configured: bool = True) -> None:
        source = canonical_source_id(source_id)
        kind = source_kind.strip().upper()
        key = f"{source}:{kind}"
        policy = cadence_policy(source, kind)
        row = self._rows.setdefault(
            key,
            {
                "source_id": source,
                "source_kind": kind,
                "interval_seconds": policy.interval_seconds,
                "failure_retry_seconds": policy.failure_retry_seconds,
                "configured": bool(configured),
                "last_attempt": None,
                "last_success": None,
                "next_due": None,
                "failure_code": None,
                "attempt_count": 0,
                "success_count": 0,
                "skip_count": 0,
            },
        )
        row["configured"] = bool(configured)
        row["interval_seconds"] = policy.interval_seconds
        row["failure_retry_seconds"] = policy.failure_retry_seconds

    def reconcile(
        self,
        lanes: Sequence[tuple[str, str]],
        *,
        persist: bool = True,
    ) -> None:
        """Bind persisted history to the currently active provider set."""

        for row in self._rows.values():
            row["configured"] = False
        for source_id, source_kind in lanes:
            self.register(source_id, source_kind, configured=True)
        if persist and not self._corrupt:
            self._write()

    def due(self, source_id: str, source_kind: str, *, now: datetime) -> tuple[bool, str]:
        if self._corrupt:
            return False, "CADENCE_STATE_CORRUPT"
        now_utc = _aware(now)
        row = self._row(source_id, source_kind)
        if row.get("configured") is not True:
            return False, "SOURCE_UNCONFIGURED"
        next_due = _timestamp(row.get("next_due"))
        if next_due is not None and now_utc < next_due:
            row["skip_count"] = int(row.get("skip_count", 0)) + 1
            self._write()
            return False, "CADENCE_NOT_DUE"
        return True, "CADENCE_DUE"

    def record(
        self,
        source_id: str,
        source_kind: str,
        *,
        now: datetime,
        success: bool,
        failure_code: str | None = None,
    ) -> None:
        if self._corrupt:
            return
        now_utc = _aware(now)
        row = self._row(source_id, source_kind)
        row["last_attempt"] = now_utc.isoformat()
        row["attempt_count"] = int(row.get("attempt_count", 0)) + 1
        policy = cadence_policy(str(row["source_id"]), str(row["source_kind"]))
        delay = policy.interval_seconds
        if success:
            row["last_success"] = now_utc.isoformat()
            row["success_count"] = int(row.get("success_count", 0)) + 1
            row["failure_code"] = None
        else:
            row["failure_code"] = _failure_code(failure_code)
            if policy.failure_retry_seconds is not None:
                delay = policy.failure_retry_seconds
        row["next_due"] = (now_utc + timedelta(seconds=delay)).isoformat()
        self._write()

    def projections(self, *, now: datetime) -> list[dict[str, object]]:
        now_utc = _aware(now)
        if self._corrupt:
            corrupt_rows = tuple(self._rows.values()) or ({
                "source_id": "ALL",
                "source_kind": "ALL",
                "configured": False,
            },)
            return [
                {
                    "schema": CADENCE_SCHEMA,
                    "source_id": row["source_id"],
                    "source_kind": row["source_kind"],
                    "configured": row.get("configured") is True,
                    "authority": "SUPPORTING_ONLY",
                    "cadence_status": "SUPPRESSED",
                    "freshness": "UNAVAILABLE",
                    "failure_code": "CADENCE_STATE_CORRUPT",
                    "last_attempt": None,
                    "last_success": None,
                    "next_due": None,
                    "attempt_count": 0,
                    "success_count": 0,
                    "skip_count": 0,
                }
                for row in corrupt_rows
            ]
        result: list[dict[str, object]] = []
        for key in sorted(self._rows):
            row = self._rows[key]
            last_success = _timestamp(row.get("last_success"))
            next_due = _timestamp(row.get("next_due"))
            freshness = "NEVER"
            if last_success is not None:
                interval = int(row.get("interval_seconds", 90))
                freshness = "CURRENT" if now_utc <= last_success + timedelta(seconds=interval) else "STALE"
            result.append({
                "schema": CADENCE_SCHEMA,
                "source_id": row["source_id"],
                "source_kind": row["source_kind"],
                "configured": row.get("configured") is True,
                "authority": "SUPPORTING_ONLY",
                "cadence_status": (
                    "SUPPRESSED"
                    if row.get("configured") is not True
                    else "DUE" if next_due is None or now_utc >= next_due else "WAITING"
                ),
                "interval_seconds": int(row.get("interval_seconds", 90)),
                "last_attempt": row.get("last_attempt"),
                "last_success": row.get("last_success"),
                "next_due": row.get("next_due"),
                "freshness": freshness,
                "failure_code": row.get("failure_code"),
                "attempt_count": int(row.get("attempt_count", 0)),
                "success_count": int(row.get("success_count", 0)),
                "skip_count": int(row.get("skip_count", 0)),
            })
        return result

    def _row(self, source_id: str, source_kind: str) -> dict[str, Any]:
        source = canonical_source_id(source_id)
        kind = source_kind.strip().upper()
        key = f"{source}:{kind}"
        if key not in self._rows:
            self.register(source, kind)
        return self._rows[key]

    def _validate_rows(self, rows: Mapping[str, Mapping[str, Any]]) -> None:
        for key, row in rows.items():
            source_id = row.get("source_id")
            source_kind = row.get("source_kind")
            if (
                not isinstance(source_id, str)
                or not source_id.strip()
                or canonical_source_id(source_id) != source_id
            ):
                raise ValueError("invalid cadence source id")
            if source_kind not in {
                "NEWS",
                "CALENDAR",
                "OFFICIAL_CALENDAR",
                "REACTION",
            }:
                raise ValueError("invalid cadence source kind")
            if key != f"{canonical_source_id(row.get('source_id'))}:{str(row.get('source_kind') or '').upper()}":
                raise ValueError("cadence key mismatch")
            for name in ("last_attempt", "last_success", "next_due"):
                if row.get(name) is not None and _timestamp(row.get(name)) is None:
                    raise ValueError("invalid cadence timestamp")
            if not isinstance(row.get("configured"), bool):
                raise ValueError("invalid cadence configured state")
            interval = row.get("interval_seconds")
            if isinstance(interval, bool) or not isinstance(interval, int) or interval <= 0:
                raise ValueError("invalid cadence interval")
            failure_retry = row.get("failure_retry_seconds")
            if failure_retry is not None and (
                isinstance(failure_retry, bool)
                or not isinstance(failure_retry, int)
                or failure_retry <= 0
            ):
                raise ValueError("invalid cadence failure retry")
            failure_code = row.get("failure_code")
            if failure_code is not None and (
                not isinstance(failure_code, str)
                or re.fullmatch(r"[A-Z0-9_]{1,96}", failure_code) is None
            ):
                raise ValueError("invalid cadence failure code")
            for name in ("attempt_count", "success_count", "skip_count"):
                value = row.get(name, 0)
                if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                    raise ValueError("invalid cadence count")

    def _write(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_name(f".{self.path.name}.{uuid4().hex}.tmp")
        document = {"schema": CADENCE_SCHEMA, "sources": self._rows}
        try:
            with temporary.open("w", encoding="utf-8", newline="\n") as handle:
                json.dump(document, handle, sort_keys=True, separators=(",", ":"))
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self.path)
        except OSError:
            self._corrupt = True
        finally:
            try:
                if temporary.exists():
                    temporary.unlink()
            except OSError:
                self._corrupt = True


def _failure_code(value: object) -> str:
    normalized = re.sub(r"[^A-Z0-9_]+", "_", str(value or "REQUEST_FAILED").strip().upper()).strip("_")
    return (normalized or "REQUEST_FAILED")[:96]


__all__ = [
    "CADENCE_SCHEMA",
    "CadencePolicy",
    "SourceCadenceStore",
    "cadence_policy",
    "canonical_source_id",
]
