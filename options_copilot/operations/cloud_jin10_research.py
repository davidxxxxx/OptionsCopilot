"""One-shot cloud acceptance path for public Jin10 flash research.

This module is deliberately separate from the production runtime credential
composition.  It consumes only the managed network-secret placeholder, calls
only the fixed Jin10 MCP endpoint, writes only a new local research ledger,
and never composes broker, account, approval, or order services.
"""
from __future__ import annotations

import argparse
from collections.abc import Callable, Mapping, Sequence
from contextlib import contextmanager
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import signal
import threading
from typing import Any

from options_copilot.config import DEFAULT_NEWS_CORE_SYMBOLS
from options_copilot.news_runtime import NewsCoordinator
from options_copilot.providers.jin10 import Jin10EventProvider
from options_copilot.providers.jin10_mcp import JIN10_MCP_URL, Jin10McpHttpClient
from options_copilot.storage.canonical import canonical_hash
from options_copilot.storage.evidence import EvidenceStore, StoredEvidence


NETWORK_SECRET_NAME = "JIN10_MCP_NETWORK_TOKEN"
SUMMARY_FILENAME = "jin10_cloud_research_summary.json"
EVIDENCE_FILENAME = "jin10_cloud_news_evidence.sqlite3"
MAXIMUM_FLASH_ITEMS = 20
TOTAL_TIMEOUT_SECONDS = 60.0
MCP_TIMEOUT_SECONDS = 50.0
_REASON = re.compile(r"[A-Z][A-Z0-9_]{0,63}\Z")


class CloudJin10ResearchError(RuntimeError):
    """Fixed-code failure with no credential or remote payload context."""

    def __init__(self, reason: str) -> None:
        checked = str(reason or "").strip().upper()
        self.reason = checked if _REASON.fullmatch(checked) else "RESEARCH_FAILED"
        super().__init__(self.reason)


class _CloudJin10DeadlineExpired(BaseException):
    """Uncatchable by provider/transport ``except Exception`` boundaries."""


class NetworkEnvironmentSecretReader:
    """Read only the managed Jin10 network placeholder without persisting it."""

    def __init__(self, environment: Mapping[str, str] | None = None) -> None:
        self._environment = os.environ if environment is None else environment

    def get(self, name: str) -> str | None:
        if name != NETWORK_SECRET_NAME:
            return None
        value = self._environment.get(NETWORK_SECRET_NAME)
        if value is None:
            return None
        if (
            not isinstance(value, str)
            or not value
            or value != value.strip()
            or len(value) > 4096
            or any(ord(char) < 33 for char in value)
        ):
            raise CloudJin10ResearchError("AUTHENTICATION_UNAVAILABLE")
        return value


def run_cloud_jin10_research(
    output_dir: str | Path,
) -> dict[str, object]:
    """Run the non-injectable live cloud acceptance entrypoint."""

    try:
        with _end_to_end_deadline(TOTAL_TIMEOUT_SECONDS):
            return _run_cloud_jin10_research(
                output_dir,
                environment=None,
                opener=None,
                clock=None,
                symbols=DEFAULT_NEWS_CORE_SYMBOLS,
                transport_mode="LIVE_DEFAULT",
            )
    except _CloudJin10DeadlineExpired:
        raise CloudJin10ResearchError("TOTAL_TIMEOUT") from None


def _run_cloud_jin10_research(
    output_dir: str | Path,
    *,
    environment: Mapping[str, str] | None = None,
    opener: object | None = None,
    clock: Callable[[], datetime] | None = None,
    symbols: Sequence[str] = DEFAULT_NEWS_CORE_SYMBOLS,
    transport_mode: str,
) -> dict[str, object]:
    """Dependency-injectable core used only by offline tests."""

    if transport_mode not in {"LIVE_DEFAULT", "TEST_INJECTED"}:
        raise CloudJin10ResearchError("TRANSPORT_MODE_INVALID")
    injected = any(value is not None for value in (environment, opener, clock)) or tuple(
        symbols
    ) != tuple(DEFAULT_NEWS_CORE_SYMBOLS)
    if transport_mode == "LIVE_DEFAULT" and injected:
        raise CloudJin10ResearchError("LIVE_DEPENDENCY_INJECTION_FORBIDDEN")
    if transport_mode == "TEST_INJECTED" and not injected:
        raise CloudJin10ResearchError("TEST_DEPENDENCY_INJECTION_REQUIRED")

    directory = Path(output_dir).resolve()
    try:
        directory.mkdir(parents=True, exist_ok=False)
    except FileExistsError as exc:
        raise CloudJin10ResearchError("OUTPUT_DIRECTORY_EXISTS") from exc
    evidence_path = directory / EVIDENCE_FILENAME
    summary_path = directory / SUMMARY_FILENAME
    selected_clock = clock or (lambda: datetime.now(timezone.utc))
    mcp_client = Jin10McpHttpClient(
        opener=opener,
        timeout_seconds=8.0,
        overall_timeout_seconds=MCP_TIMEOUT_SECONDS,
        flash_only=True,
    )
    provider = Jin10EventProvider(
        NetworkEnvironmentSecretReader(environment),
        mcp_client=mcp_client,
        now=selected_clock,
        secret_name=NETWORK_SECRET_NAME,
        maximum_news_items=MAXIMUM_FLASH_ITEMS,
    )
    coordinator = NewsCoordinator(
        evidence_path,
        news_providers=(provider,),
        calendar_providers=(),
        official_calendar_provider=None,
        ibkr_binding_provider=None,
        preselection_provider=None,
        reaction_provider=None,
        classifier=None,
        shadow_advisory=None,
        phase2_advisory=None,
        shadow_writer=None,
        core_symbols=tuple(symbols),
        poll_interval_seconds=90,
        cadence_path=None,
        clock=selected_clock,
    )
    first_readback: tuple[tuple[str, str, str], ...] = ()
    refresh: Mapping[str, object] = {}
    try:
        refresh = coordinator.refresh_once()
        if provider.health != "READY":
            reason = str(provider.health_reason or "PROVIDER_NOT_READY").upper()
            raise CloudJin10ResearchError(reason)
        first_rows = _jin10_rows(coordinator.evidence_store)
        if not first_rows:
            raise CloudJin10ResearchError("NO_USABLE_RECORDS")
        if len(first_rows) > MAXIMUM_FLASH_ITEMS:
            raise CloudJin10ResearchError("ITEM_LIMIT_EXCEEDED")
        cleanup = mcp_client.session_cleanup()
        if cleanup["result"] not in {"CONFIRMED", "NOT_REQUIRED"}:
            raise CloudJin10ResearchError("SESSION_CLEANUP_FAILED")
        first_readback = _row_identities(first_rows)
    finally:
        coordinator.close()

    with EvidenceStore(evidence_path) as reopened:
        reopened.assert_integrity()
        reopened_rows = _jin10_rows(reopened)
        if _row_identities(reopened_rows) != first_readback:
            raise CloudJin10ResearchError("STORE_REREAD_MISMATCH")
        records = [_record_summary(row) for row in reopened_rows]
        summary: dict[str, Any] = {
            "schema": "options-copilot.cloud-jin10-research.v1",
            "status": "PASS" if transport_mode == "LIVE_DEFAULT" else "TEST_PASS",
            "mode": "ONE_SHOT_PUBLIC_RESEARCH",
            "transport": {
                "mode": transport_mode,
                "live_default_dependencies": transport_mode == "LIVE_DEFAULT",
                "test_injection": transport_mode == "TEST_INJECTED",
            },
            "endpoint": JIN10_MCP_URL,
            "source_tool": "list_flash",
            "request": {
                "arguments": {},
                "pages_requested": 1,
                "maximum_items": MAXIMUM_FLASH_ITEMS,
                "mcp_timeout_seconds": MCP_TIMEOUT_SECONDS,
                "end_to_end_timeout_seconds": TOTAL_TIMEOUT_SECONDS,
            },
            "coverage": {
                "status": "BOUNDED_FIRST_PAGE_ONLY",
                "complete_24h": False,
                "next_cursor_followed": False,
            },
            "provider_health": {
                "status": provider.health,
                "reason": provider.health_reason,
            },
            "session_cleanup": cleanup,
            "store": {
                "filename": EVIDENCE_FILENAME,
                "record_count": len(records),
                "reopened": True,
                "integrity": "PASS",
                "journal_mode": reopened.journal_mode,
                "synchronous": reopened.synchronous,
                "records": records,
            },
            "coordinator": {
                "news_count": int(refresh.get("news_count", 0)),
                "approval_eligible": False,
            },
            "authority": {
                "decision_authority": "SUPPORTING_ONLY",
                "production_eligible": False,
                "account_accessed": False,
                "ibkr_accessed": False,
                "order_creation_allowed": False,
                "order_submission_allowed": False,
            },
            "credential": {
                "source_name": NETWORK_SECRET_NAME,
                "persisted": False,
                "copied_to_local_token": False,
                "copied_to_dpapi": False,
            },
        }
        summary["canonical_sha256"] = canonical_hash(summary)
    _write_exclusive_json(summary_path, summary)
    return summary


@contextmanager
def _end_to_end_deadline(seconds: float):
    """Interrupt the live entrypoint at its wall-clock acceptance deadline."""

    if (
        threading.current_thread() is not threading.main_thread()
        or not hasattr(signal, "SIGALRM")
        or not hasattr(signal, "setitimer")
    ):
        raise CloudJin10ResearchError("HARD_DEADLINE_UNAVAILABLE")
    existing_timer = signal.getitimer(signal.ITIMER_REAL)
    if existing_timer[0] > 0 or existing_timer[1] > 0:
        raise CloudJin10ResearchError("HARD_DEADLINE_ALREADY_ACTIVE")
    previous_handler = signal.getsignal(signal.SIGALRM)

    def _timeout(_signum, _frame) -> None:
        raise _CloudJin10DeadlineExpired

    signal.signal(signal.SIGALRM, _timeout)
    signal.setitimer(signal.ITIMER_REAL, float(seconds))
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0.0)
        signal.signal(signal.SIGALRM, previous_handler)


def _jin10_rows(store: EvidenceStore) -> tuple[StoredEvidence, ...]:
    return tuple(
        row
        for row in store.query(kinds=("NEWS",), limit=MAXIMUM_FLASH_ITEMS + 1)
        if row.record.payload.get("source") == "Jin10"
    )


def _row_identities(
    rows: Sequence[StoredEvidence],
) -> tuple[tuple[str, str, str], ...]:
    return tuple((row.evidence_id, row.content_hash, row.row_hash) for row in rows)


def _record_summary(row: StoredEvidence) -> dict[str, object]:
    return {
        "sequence": row.sequence,
        "stored_source_id": row.record.source_id,
        "published_at": row.record.published_at.isoformat(),
        "received_at": row.record.observed_at.isoformat(),
        "content_hash": row.content_hash,
        "row_hash": row.row_hash,
        "decision_authority": row.decision_authority,
        "approval_eligible": False,
        "production_eligible": False,
    }


def _write_exclusive_json(path: Path, document: Mapping[str, object]) -> None:
    payload = json.dumps(document, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    try:
        with path.open("x", encoding="utf-8", newline="\n") as stream:
            stream.write(payload)
    except FileExistsError as exc:
        raise CloudJin10ResearchError("SUMMARY_ALREADY_EXISTS") from exc


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m options_copilot.operations.cloud_jin10_research",
        description="Run one bounded public Jin10 flash research acceptance cycle.",
    )
    parser.add_argument("--output-dir", required=True, type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result = run_cloud_jin10_research(args.output_dir)
    except CloudJin10ResearchError as exc:
        print(json.dumps({"status": "FAILED", "reason": exc.reason}, sort_keys=True))
        return 1
    print(
        json.dumps(
            {
                "status": result["status"],
                "canonical_sha256": result["canonical_sha256"],
                "summary_file": SUMMARY_FILENAME,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
