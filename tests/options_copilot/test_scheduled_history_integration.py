"""A real local scheduled lease reaches persisted cache and the candidate lane."""

from __future__ import annotations

from copy import deepcopy
from datetime import timedelta
import threading
from types import SimpleNamespace

import pytest

from options_copilot.feature_source_resolution import FeatureSourceResolver, validate_feature_source_binding
from options_copilot.history_source_contracts import (
    build_native_history_request, make_native_history_result, native_history_wire_parameters,
)
from options_copilot.history_source_runtime import ScheduledHistoryProducer
from options_copilot.scanner.scheduler import ScanRunStore
from options_copilot.scanner.service import ScanSchedulerLoop, ScanSchedulerService
from options_copilot.storage.canonical import canonical_hash
from options_copilot.storage.history_sources import HistorySourceStore
import tests.options_copilot.test_production_feature_source_binding as binding_tests
from tests.options_copilot.test_production_feature_source_binding import lane
from tests.options_copilot.test_scheduled_history_runtime import NOW, SLOT, calendar


class NativeGateway:
    def __init__(self):
        self.sent = []

    def prepare_native_history(self, symbol, *, kind, cutoff, incremental):
        return build_native_history_request(
            contract={"symbol": symbol, "con_id": 756733, "sec_type": "STK", "currency": "USD", "exchange": "SMART", "primary_exchange": "ARCA"},
            kind=kind, cutoff=cutoff, incremental=incremental, prepared_at=NOW,
        )

    def read_native_history(self, request, *, before_send, operation_guard, remaining_seconds):
        assert operation_guard() and remaining_seconds() > 0
        claim = before_send()
        self.sent.append(request)
        return make_native_history_result(
            request, claim_id=claim, requested_at=NOW, available_at=NOW + timedelta(seconds=1),
            response={
                "broker_request_id": len(self.sent), "send_state": "DISPATCHED", "response_end_received": True,
                "response_end": {"start": "20260908", "end": "20260908"},
                "wire_parameters": native_history_wire_parameters(request),
                "start_generation": 1, "end_generation": 1, "broker_error_codes": [],
                "bars": [{"raw_date": "20260908", "session_date": "2026-09-08", "open": "0.20", "high": "0.23", "low": "0.19", "close": "0.21", "volume": None, "valid_close": True, "date_eligible": True}],
                "received_bar_count": 1, "timed_out": False, "epoch_valid": True,
            }, reason_codes=[],
        )


def acquire_scheduled(tmp_path):
    source_path = tmp_path / "history.sqlite3"
    gateway = NativeGateway()
    completion = threading.Event()
    with ScanRunStore(tmp_path / "runs.sqlite3") as runs, HistorySourceStore(source_path, clock=lambda: NOW + timedelta(seconds=2)) as sources:
        producer = ScheduledHistoryProducer(gateway, sources, closing=lambda: False, clock=lambda: NOW + timedelta(seconds=2))

        def callback(calendar_value, scheduled_for, checked_at, *, operation_context, **kwargs):
            assert scheduled_for == SLOT and checked_at == NOW
            child = producer.run(calendar_value, scheduled_for, operation_context=operation_context, symbols=("SPY",))
            completion.set()
            return {"status": "DEGRADED", "reason_codes": ["ORIGINAL_POOL_EMPTY"], "decision_authority": "SUPPORTING_ONLY", "historical_sources": child}

        loop = ScanSchedulerLoop(
            ScanSchedulerService(runs, SimpleNamespace(run_slot=lambda *args: {"decision": "NO_TRADE"}), pipeline_version="integration"),
            SimpleNamespace(snapshot=lambda **kwargs: calendar()), clock=lambda: NOW,
        )
        loop.bind_daily_callbacks(next_session_preparation=callback)
        try:
            loop.tick_once()
            assert completion.wait(timeout=2)
            loop.tick_once()
            durable = runs.latest_daily_result("NEXT_SESSION_PREPARATION")
            assert durable is not None
            assert durable.payload["status"] == "DEGRADED"
            assert durable.payload["reason_codes"] == ["ORIGINAL_POOL_EMPTY"]
            assert durable.payload["historical_sources"]["status"] == "OBSERVATIONS_PERSISTED"
            assert durable.payload["historical_sources"]["parent"]["parent_run_id"] == durable.scan_run_id
            assert len(gateway.sent) == 2
            assert sources.status()["completed_count"] == 2
        finally:
            assert loop.close() is True
    return source_path


def test_scheduled_lease_to_reopened_cache_to_actual_candidate_acquisition(tmp_path, lane, monkeypatch):
    path = acquire_scheduled(tmp_path)
    decision_at = NOW + timedelta(seconds=3)
    monkeypatch.setattr(binding_tests, "NOW", decision_at)
    with HistorySourceStore(path) as reopened:
        resolver = FeatureSourceResolver(SimpleNamespace(read=lambda **kwargs: ()), history_store=reopened)
        before = resolver.resolve(symbol="SPY", con_id=756733, expiration=binding_tests.EXPIRY, cutoff=NOW)
        assert before["scheduled_history"]["fragments"] == []
        acquisition, _, _, events = lane(resolver)
        binding_tests._acquire(acquisition)
        actual = acquisition.feature_source_bindings()
        assert events[0] == "history" and "quotes" in events
        assert len(actual["bindings"]) == 1
        binding = actual["bindings"][0]
        assert binding["schema"] == "options_copilot.feature_source_binding.v2"
        assert len(binding["scheduled_history"]["fragments"]) == 2
        assert binding["market_score"] is binding["volatility_score"] is None
        assert binding["model_input_complete"] is binding["production_eligible"] is False
        assert "NATIVE_HISTORY_FRAGMENTS_NOT_MODEL_AUTHORITY" in binding["reason_codes"]
        assert all(row["first_seen_at"] <= decision_at.isoformat() for row in binding["scheduled_history"]["fragments"])


@pytest.mark.parametrize("change", ["first_seen_future", "source_after_ingestion", "mergeable", "fake_complete", "wrong_kind"])
def test_v2_native_reference_self_rehash_cannot_promote_or_backdate(tmp_path, change):
    path = acquire_scheduled(tmp_path)
    decision_at = NOW + timedelta(seconds=3)
    with HistorySourceStore(path) as reopened:
        resolver = FeatureSourceResolver(SimpleNamespace(read=lambda **kwargs: ()), history_store=reopened)
        raw = resolver.resolve(symbol="SPY", con_id=756733, expiration=binding_tests.EXPIRY, cutoff=decision_at)
    changed = deepcopy(raw)
    native = changed["scheduled_history"]
    if change == "first_seen_future":
        native["fragments"][0]["first_seen_at"] = (decision_at + timedelta(days=1)).isoformat()
    elif change == "source_after_ingestion":
        native["fragments"][0]["available_at"] = decision_at.isoformat()
    elif change == "mergeable":
        native["adjustment_vintages_mergeable"] = True
    elif change == "fake_complete":
        changed["model_input_complete"] = True
    else:
        native["fragments"][0]["kind"] = "ATM_IV"
    changed.pop("content_hash")
    changed["content_hash"] = canonical_hash(changed)
    with pytest.raises(ValueError):
        validate_feature_source_binding(changed, symbol="SPY", con_id=756733, expiration=binding_tests.EXPIRY, cutoff=decision_at)
