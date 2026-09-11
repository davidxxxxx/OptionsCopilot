"""Only an actual leased preparation can send bounded native history requests."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
import threading
from time import monotonic
from types import SimpleNamespace

import pytest

from options_copilot.history_source_runtime import ScheduledHistoryProducer
from options_copilot.market.session_calendar import UsOptionsSessionCalendar
from options_copilot.scanner.operation_context import ScheduledOperationContext
from options_copilot.scanner.scheduler import DAILY_OPERATION_PIPELINES, ScanRunStore, ScanSlot
from options_copilot.storage.canonical import canonical_hash


SLOT = datetime(2026, 9, 8, 20, 40, tzinfo=timezone.utc)
NOW = SLOT + timedelta(seconds=1)


def calendar(*, early=False):
    hours = "20260908:0930-1300;20260909:0930-1600" if early else "20260908:0930-1600;20260909:0930-1600"
    return UsOptionsSessionCalendar().normalize(
        liquid_hours=hours, trading_hours=hours, timezone_id="America/New_York",
        observed_at=NOW, source="IBKR_REQ_CONTRACT_DETAILS_READONLY", now=NOW,
    )


def context(store):
    operation = "NEXT_SESSION_PREPARATION"
    owner = "daily-operation.next_session_preparation"
    acquired = store.acquire(
        ScanSlot(SLOT.date(), SLOT, kind=operation), pipeline_version=DAILY_OPERATION_PIPELINES[operation],
        owner=owner, now=NOW, lease_seconds=120,
    )
    return ScheduledOperationContext(
        scan_run_id=acquired.run.scan_run_id, owner=owner, operation=operation,
        pipeline_version=acquired.run.pipeline_version, trading_date=SLOT.date(), slot_at=SLOT,
        deadline_at=NOW + timedelta(seconds=60), cancel_event=threading.Event(),
        _store=store, _clock=lambda: NOW, _closing_event=threading.Event(),
        _monotonic_deadline=monotonic() + 60,
    )


class SourceStore:
    def __init__(self):
        self.manifests = []
        self.claimed = set()
        self.completed = []
        self.rows = ()

    def status(self):
        return {"status": "VERIFIED", "intent_count": len(self.claimed)}

    def find_fragments(self, **kwargs):
        return self.rows

    def freeze_manifest(self, parent, requests, *, guard):
        assert guard()
        self.manifests.append((parent, requests))
        return {"manifest_id": canonical_hash(parent), "request_count": len(requests)}

    def claim_for_send(self, manifest_id, request_hash, basis_hash, *, guard):
        assert guard()
        key = (manifest_id, request_hash, basis_hash)
        if key in self.claimed:
            return SimpleNamespace(permit=None)
        self.claimed.add(key)
        return SimpleNamespace(permit=SimpleNamespace(claim_id=canonical_hash(key)))

    def complete(self, permit, fragment, *, guard):
        assert guard()
        self.completed.append(fragment)
        return {"claim_id": permit.claim_id}


class Gateway:
    def __init__(self):
        self.calls = []
        self.preparations = []
        self.on_send = lambda: None

    def prepare_native_history(self, symbol, *, kind, cutoff, incremental):
        self.preparations.append((symbol, kind, incremental))
        if symbol == "UNKNOWN":
            raise ValueError("cached identity missing")
        return {
            "symbol": symbol, "con_id": 756733, "kind": kind,
            "request_hash": canonical_hash((symbol, kind, incremental)),
            "basis_hash": "b" * 64,
        }

    def read_native_history(self, request, *, before_send, operation_guard, remaining_seconds):
        assert operation_guard() and remaining_seconds() > 0
        claim_id = before_send()
        self.calls.append(request)
        self.on_send()
        return {"claim_id": claim_id, "status": "DELIVERED"}


def test_missing_or_fabricated_context_does_not_prepare_write_or_send():
    gateway, store = Gateway(), SourceStore()
    producer = ScheduledHistoryProducer(gateway, store, closing=lambda: False, clock=lambda: NOW)
    for ctx in (None, SimpleNamespace(operation_token="scan.fake")):
        outcome = producer.run(calendar(), SLOT, operation_context=ctx, symbols=("SPY",))
        assert outcome["status"] == "NOT_RUN"
    assert gateway.preparations == gateway.calls == store.manifests == []


@pytest.mark.parametrize("change", ["cancelled", "closed", "terminal", "wrong_slot", "wrong_calendar"])
def test_parent_failure_is_zero_request_zero_manifest(tmp_path, change):
    with ScanRunStore(tmp_path / "runs.sqlite3") as runs:
        ctx = context(runs)
        if change == "cancelled":
            ctx.cancel_event.set()
        if change == "terminal":
            runs.complete(ctx.scan_run_id, owner=ctx.owner, result_hash="done", now=NOW)
        if change == "wrong_slot":
            ctx = replace(ctx, slot_at=SLOT + timedelta(minutes=1))
        gateway, store = Gateway(), SourceStore()
        producer = ScheduledHistoryProducer(gateway, store, closing=lambda: change == "closed", clock=lambda: NOW)
        outcome = producer.run(calendar(early=change == "wrong_calendar"), SLOT, operation_context=ctx, symbols=("SPY",))
        assert outcome["status"] == "NOT_RUN"
        assert gateway.calls == gateway.preparations == store.manifests == []


def test_frozen_manifest_bounds_requests_and_repeat_never_sends_again(tmp_path):
    with ScanRunStore(tmp_path / "runs.sqlite3") as runs:
        ctx = context(runs)
        gateway, store = Gateway(), SourceStore()
        producer = ScheduledHistoryProducer(gateway, store, closing=lambda: False, clock=lambda: NOW)
        outcome = producer.run(calendar(), SLOT, operation_context=ctx, symbols=("SPY", "QQQ", "IWM", "DIA"))
        assert outcome["status"] == "PARTIAL"
        assert outcome["deferred_symbols"] == ["DIA"]
        assert len(gateway.calls) == len(store.completed) == 6
        assert store.manifests[0][0]["parent_run_id"] == ctx.scan_run_id
        assert outcome["model_input_complete"] is outcome["order_allowed"] is False
        producer.run(calendar(), SLOT, operation_context=ctx, symbols=("SPY", "QQQ", "IWM", "DIA"))
        assert len(gateway.calls) == 6


def test_cancel_during_request_retains_intent_but_never_commits_late_result(tmp_path):
    with ScanRunStore(tmp_path / "runs.sqlite3") as runs:
        ctx = context(runs)
        gateway, store = Gateway(), SourceStore()
        gateway.on_send = ctx.cancel_event.set
        producer = ScheduledHistoryProducer(gateway, store, closing=lambda: False, clock=lambda: NOW)
        outcome = producer.run(calendar(), SLOT, operation_context=ctx, symbols=("SPY",))
        assert outcome["status"] == "FAILED"
        assert len(gateway.calls) == len(store.claimed) == 1
        assert store.completed == []


def test_recent_native_fragments_select_increment_without_claiming_mergeability(tmp_path):
    with ScanRunStore(tmp_path / "runs.sqlite3") as runs:
        ctx = context(runs)
        gateway, store = Gateway(), SourceStore()
        store.rows = ({
            "prepared_request": {"kind": "PRICE_HISTORY", "request_contract": {"incremental": False}, "basis_hash": "b" * 64},
            "reference": {"first_seen_at": (NOW - timedelta(days=1)).isoformat()},
            "fragment": {"status": "DELIVERED", "response": {"response_end_received": True, "received_bar_count": 250, "bars": [
                {"session_date": (NOW - timedelta(days=offset)).date().isoformat(), "valid_close": True, "date_eligible": True}
                for offset in range(1, 251)
            ]}},
        },)
        producer = ScheduledHistoryProducer(gateway, store, closing=lambda: False, clock=lambda: NOW)
        outcome = producer.run(calendar(), SLOT, operation_context=ctx, symbols=("UNKNOWN", "SPY"))
        assert outcome["status"] == "PARTIAL"
        assert ("SPY", "PRICE_HISTORY", True) in gateway.preparations
        assert ("SPY", "IV_HISTORY", True) not in gateway.preparations
        assert outcome["adjustment_vintages_mergeable"] is False


def test_status_is_cache_only_and_detached():
    gateway, store = Gateway(), SourceStore()
    producer = ScheduledHistoryProducer(gateway, store, closing=lambda: False)
    producer.status()["latest_runtime_run"]["status"] = "FAKE"
    assert producer.status()["status"] == "WIRED_NOT_RUN"
    assert gateway.preparations == gateway.calls == store.manifests == []


def test_current_store_failure_does_not_inherit_a_historical_success():
    gateway, store = Gateway(), SourceStore()
    producer = ScheduledHistoryProducer(gateway, store, closing=lambda: False)
    producer._latest["status"] = "OBSERVATIONS_PERSISTED"
    store.status = lambda: {"status": "UNAVAILABLE"}
    status = producer.status()
    assert status["status"] == "UNAVAILABLE"
    assert status["latest_runtime_run"]["status"] == "OBSERVATIONS_PERSISTED"
    assert status["reason_codes"] == ["HISTORY_SOURCE_STORE_UNAVAILABLE"]


def test_restored_manifests_are_not_reported_as_current_runtime_runs():
    store = SourceStore()
    store.status = lambda: {"status": "VERIFIED", "manifest_count": 1}
    producer = ScheduledHistoryProducer(Gateway(), store, closing=lambda: False)
    status = producer.status()
    assert status["status"] == "HISTORICAL_EVIDENCE_RESTORED"
    assert status["latest_runtime_run"]["status"] == "WIRED_NOT_RUN"
