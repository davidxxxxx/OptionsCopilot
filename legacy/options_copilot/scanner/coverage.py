"""Restart-safe ordinary research coverage; never expands eligibility or budgets."""
from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
import threading

from options_copilot.storage.canonical import canonical_hash, utc_datetime
from options_copilot.storage.evidence import EvidenceRecord, EvidenceStore


_KIND = "ORDINARY_SCAN_COVERAGE"
_SCHEMA = "options_copilot.ordinary_scan_coverage.v1"


class OrdinaryScanCoverage:
    """Move actually visited names behind pending names in an append-only queue.

    The caller serializes an ordinary acquisition and its coverage append. A
    failed append must fail the acquisition closed. Manual scans do not use
    this queue. Each record is a full cursor snapshot for restart reconstruction.
    """

    def __init__(self, store: EvidenceStore) -> None:
        self._store = store
        self.lock = threading.RLock()

    def arrange(self, symbols: Sequence[str]) -> tuple[str, ...]:
        eligible = tuple(dict.fromkeys(symbols))
        if any(not isinstance(item, str) or not item.strip() for item in eligible):
            raise ValueError("coverage symbols must be nonempty strings")
        self._store.assert_integrity()
        rows = self._store.query(kinds=(_KIND,), limit=1)
        if not rows:
            return eligible
        row = rows[0]
        payload = row.record.payload
        pending = payload.get("pending_symbols")
        if (
            row.status != "ACTIVE"
            or row.content_hash != canonical_hash(row.record.immutable_document())
            or payload.get("schema") != _SCHEMA
            or payload.get("affects_eligibility") is not False
            or not isinstance(pending, (list, tuple))
            or any(not isinstance(item, str) or not item for item in pending)
            or len(set(pending)) != len(pending)
        ):
            raise ValueError("ordinary coverage evidence invalid")
        allowed = set(eligible)
        ordered = tuple(item for item in pending if item in allowed)
        return ordered + tuple(item for item in eligible if item not in ordered)

    def record(
        self,
        *,
        scan_run_id: str,
        ordered_symbols: Sequence[str],
        visited_symbols: Sequence[str],
        observed_at: datetime,
    ) -> dict[str, object]:
        now = utc_datetime(observed_at, field="coverage observed_at")
        ordered = tuple(dict.fromkeys(ordered_symbols))
        visited = tuple(dict.fromkeys(visited_symbols))
        if not set(visited).issubset(ordered):
            raise ValueError("coverage visit outside eligible universe")
        pending = tuple(item for item in ordered if item not in visited) + tuple(
            item for item in ordered if item in visited
        )
        document = {
            "schema": _SCHEMA,
            "scan_run_id": scan_run_id,
            "ordered_symbols": ordered,
            "visited_symbols": visited,
            "pending_symbols": pending,
            "affects_eligibility": False,
            "affects_pacing_limits": False,
        }
        stored = self._store.append(EvidenceRecord(
            identity=f"ordinary-coverage:{scan_run_id}",
            kind=_KIND,
            symbol=None,
            provider="RUNTIME_ORDINARY_SCANNER",
            source_id=scan_run_id,
            published_at=now,
            first_seen_at=now,
            ingested_at=now,
            observed_at=now,
            payload=document,
        )).evidence
        return {**document, "evidence_hash": stored.content_hash, "row_hash": stored.row_hash}


__all__ = ["OrdinaryScanCoverage"]
