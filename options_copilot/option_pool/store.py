"""Append-only SQLite store for multi-strategy option pool snapshots."""

from __future__ import annotations

import json
import sqlite3
import threading
from typing import Callable
from datetime import datetime
from pathlib import Path

from options_copilot.storage.canonical import canonical_hash, canonical_json, freeze_json, thaw_json
from options_copilot.strategies import StrategyKind

from .models import (
    V1_SCHEMA,
    V2_SCHEMA,
    OptionStructureDecision,
    OptionStructurePoolSnapshot,
    StructureDisposition,
    ThesisClass,
)


GENESIS_HASH = "0" * 64


class OptionStructurePoolStoreCorruption(RuntimeError):
    pass


class OptionStructurePoolStore:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._connection = sqlite3.connect(self.path, isolation_level=None, check_same_thread=False)
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA journal_mode=WAL")
        self._connection.execute("PRAGMA synchronous=FULL")
        self._migrate()

    def close(self) -> None:
        self._connection.close()

    def __enter__(self) -> "OptionStructurePoolStore":
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()

    def append(
        self,
        snapshot: OptionStructurePoolSnapshot,
        *,
        commit_guard: Callable[[], bool] | None = None,
    ) -> OptionStructurePoolSnapshot:
        body = canonical_json(snapshot.as_dict())
        with self._lock:
            self.assert_integrity()
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                existing = self._connection.execute(
                    "SELECT snapshot_hash FROM option_structure_pools WHERE scan_run_id=?",
                    (snapshot.scan_run_id,),
                ).fetchone()
                if existing is not None:
                    if str(existing[0]) != snapshot.snapshot_hash:
                        raise ValueError("option structure pool scan_run_id conflict")
                    if commit_guard is not None and not commit_guard():
                        raise TimeoutError("option structure pool commit cancelled")
                    self._connection.execute("COMMIT")
                    return snapshot
                previous = self._connection.execute(
                    "SELECT chain_hash FROM option_structure_pools ORDER BY sequence DESC LIMIT 1"
                ).fetchone()
                previous_hash = GENESIS_HASH if previous is None else str(previous[0])
                chain_hash = canonical_hash({"previous": previous_hash, "snapshot_hash": snapshot.snapshot_hash})
                self._connection.execute(
                    "INSERT INTO option_structure_pools(scan_run_id, observed_at, body_json, snapshot_hash, previous_chain_hash, chain_hash) VALUES(?,?,?,?,?,?)",
                    (snapshot.scan_run_id, snapshot.observed_at.isoformat(), body, snapshot.snapshot_hash, previous_hash, chain_hash),
                )
                if commit_guard is not None and not commit_guard():
                    raise TimeoutError("option structure pool commit cancelled")
                self._connection.execute("COMMIT")
                return snapshot
            except BaseException:
                self._connection.execute("ROLLBACK")
                raise

    def latest(self) -> OptionStructurePoolSnapshot | None:
        rows = self.recent(limit=1)
        return None if not rows else rows[0]

    def recent(self, *, limit: int = 20) -> tuple[OptionStructurePoolSnapshot, ...]:
        if (
            isinstance(limit, bool)
            or not isinstance(limit, int)
            or not 1 <= limit <= 100
        ):
            raise ValueError("option structure pool recent limit must be 1-100")
        with self._lock:
            self.assert_integrity()
            rows = self._connection.execute(
                "SELECT scan_run_id, observed_at, body_json "
                "FROM option_structure_pools ORDER BY sequence DESC LIMIT ?",
                (limit,),
            ).fetchall()
            snapshots: list[OptionStructurePoolSnapshot] = []
            for row in rows:
                snapshot = _snapshot_from_json(str(row["body_json"]))
                _assert_row_identity(row, snapshot)
                snapshots.append(snapshot)
            return tuple(snapshots)

    def replay(self, scan_run_id: str) -> OptionStructurePoolSnapshot:
        replay_key = scan_run_id.strip() if isinstance(scan_run_id, str) else ""
        if not replay_key:
            raise ValueError("option structure pool replay key is invalid")
        self.assert_integrity()
        row = self._connection.execute(
            "SELECT scan_run_id, observed_at, body_json FROM option_structure_pools WHERE scan_run_id=?",
            (replay_key,),
        ).fetchone()
        if row is None:
            raise KeyError(replay_key)
        snapshot = _snapshot_from_json(str(row["body_json"]))
        if snapshot.scan_run_id != replay_key:
            raise OptionStructurePoolStoreCorruption(
                "option structure pool replay key mismatch"
            )
        _assert_row_identity(row, snapshot)
        return snapshot

    def assert_integrity(self) -> None:
        definitions = {str(row[0]): str(row[1] or "") for row in self._connection.execute("SELECT name, sql FROM sqlite_master WHERE type='trigger'")}
        for name, operation in (("option_structure_pools_no_update", "UPDATE"), ("option_structure_pools_no_delete", "DELETE")):
            if _normalize(definitions.get(name, "")) != _normalize(_trigger_sql(name, operation)):
                raise OptionStructurePoolStoreCorruption("option structure pool immutable trigger invalid")
        previous = GENESIS_HASH
        for row in self._connection.execute("SELECT * FROM option_structure_pools ORDER BY sequence"):
            try:
                snapshot = _snapshot_from_json(str(row["body_json"]))
            except OptionStructurePoolStoreCorruption:
                raise
            except (TypeError, ValueError, KeyError) as exc:
                raise OptionStructurePoolStoreCorruption(
                    "option structure pool body invalid"
                ) from exc
            _assert_row_identity(row, snapshot)
            if snapshot.snapshot_hash != str(row["snapshot_hash"]):
                raise OptionStructurePoolStoreCorruption("option structure pool snapshot hash mismatch")
            if str(row["previous_chain_hash"]) != previous:
                raise OptionStructurePoolStoreCorruption("option structure pool predecessor mismatch")
            expected = canonical_hash({"previous": previous, "snapshot_hash": snapshot.snapshot_hash})
            if str(row["chain_hash"]) != expected:
                raise OptionStructurePoolStoreCorruption("option structure pool chain hash mismatch")
            previous = expected

    def _migrate(self) -> None:
        self._connection.execute(
            "CREATE TABLE IF NOT EXISTS option_structure_pools(sequence INTEGER PRIMARY KEY AUTOINCREMENT, scan_run_id TEXT NOT NULL UNIQUE, observed_at TEXT NOT NULL, body_json TEXT NOT NULL, snapshot_hash TEXT NOT NULL UNIQUE, previous_chain_hash TEXT NOT NULL, chain_hash TEXT NOT NULL UNIQUE)"
        )
        for name, operation in (("option_structure_pools_no_update", "UPDATE"), ("option_structure_pools_no_delete", "DELETE")):
            self._connection.execute(_trigger_sql(name, operation, if_not_exists=True))
        self.assert_integrity()
        version = int(self._connection.execute("PRAGMA user_version").fetchone()[0])
        if version > 2:
            raise OptionStructurePoolStoreCorruption("option structure pool schema is newer than supported")
        # Existing v1 rows remain byte-for-byte immutable.  Version 2 only
        # changes the body contract used for subsequent append operations.
        if version < 2:
            self._connection.execute("PRAGMA user_version=2")


def _trigger_sql(name: str, operation: str, *, if_not_exists: bool = False) -> str:
    qualifier = " IF NOT EXISTS" if if_not_exists else ""
    return f"CREATE TRIGGER{qualifier} {name} BEFORE {operation} ON option_structure_pools BEGIN SELECT RAISE(ABORT, 'option_structure_pools immutable'); END"


def _normalize(value: str) -> str:
    return " ".join(value.replace("IF NOT EXISTS", "").split()).upper()


def _snapshot_from_json(value: str) -> OptionStructurePoolSnapshot:
    raw = thaw_json(freeze_json(json.loads(value)))
    if not isinstance(raw, dict) or raw.get("schema") not in {V1_SCHEMA, V2_SCHEMA}:
        raise OptionStructurePoolStoreCorruption("option structure pool body invalid")
    schema = str(raw["schema"])
    decisions = tuple(
        OptionStructureDecision(
            underlying=str(item["underlying"]),
            thesis_class=ThesisClass(str(item["thesis_class"])),
            structure=StrategyKind(str(item["structure"])),
            disposition=StructureDisposition(str(item["disposition"])),
            reason_codes=tuple(item["reason_codes"]),
            candidate_id=item.get("candidate_id"),
            candidate_hash=item.get("candidate_hash"),
            exact_economics=item.get("exact_economics"),
            thesis_observed_at=(
                None
                if item.get("thesis_observed_at") is None
                else datetime.fromisoformat(str(item["thesis_observed_at"]))
            ),
            equity_pool_reference=item.get("equity_pool_reference"),
            equity_thesis_evidence=item.get("equity_thesis_evidence"),
            candidate_identity=item.get("candidate_identity"),
            schema=schema,
        )
        for item in raw["decisions"]
    )
    snapshot = OptionStructurePoolSnapshot(
        scan_run_id=str(raw["scan_run_id"]),
        observed_at=datetime.fromisoformat(str(raw["observed_at"])),
        decisions=decisions,
        generation_reason_codes=tuple(raw.get("generation_reason_codes", ())),
        schema=schema,
    )
    if snapshot.snapshot_hash != raw.get("snapshot_hash"):
        raise OptionStructurePoolStoreCorruption("option structure pool embedded hash mismatch")
    return snapshot


def _assert_row_identity(
    row: sqlite3.Row,
    snapshot: OptionStructurePoolSnapshot,
) -> None:
    if str(row["scan_run_id"]) != snapshot.scan_run_id:
        raise OptionStructurePoolStoreCorruption(
            "option structure pool scan_run_id column mismatch"
        )
    if str(row["observed_at"]) != snapshot.observed_at.isoformat():
        raise OptionStructurePoolStoreCorruption(
            "option structure pool observed_at column mismatch"
        )


__all__ = ["OptionStructurePoolStore", "OptionStructurePoolStoreCorruption"]
