"""Real local source ingestion and cache consumption preserve missing authority."""

from __future__ import annotations

import asyncio
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
import threading
from types import SimpleNamespace

import httpx
import pytest

from options_copilot.api import OptionsCopilotServices, create_app
from options_copilot.feature_source_resolution import FeatureSourceResolver, validate_feature_source_binding
from options_copilot.storage.canonical import canonical_hash
from options_copilot.storage.feature_sources import FeatureSourceObservationStore, FeatureSourceStoreError
from tests.options_copilot.test_feature_sources_diagnostic import REQUEST, _runtime, _source


NOW = datetime(2026, 9, 8, 16, tzinfo=timezone.utc)
EXPIRATION = date(2026, 9, 25)


def observation(kind):
    raw = _source(kind, "SPY", NOW)
    raw["requested_at"] = (NOW + timedelta(seconds=1)).isoformat()
    raw["available_at"] = (NOW + timedelta(seconds=2)).isoformat()
    if kind == "CURRENT_IV":
        raw["received_at"] = raw["available_at"]
    raw.pop("content_hash")
    return {**raw, "content_hash": canonical_hash(raw)}


def test_reopened_source_store_resolves_same_refs_at_real_ingestion_cutoff(tmp_path):
    path = tmp_path / "source.sqlite3"
    with FeatureSourceObservationStore(path, clock=lambda: NOW + timedelta(seconds=3)) as store:
        references = {
            kind: store.append(observation(kind), operation_id="diagnostic.001", cutoff=NOW)
            for kind in ("PRICE_HISTORY", "IV_HISTORY", "CURRENT_IV")
        }
    with FeatureSourceObservationStore(path) as reopened:
        resolver = FeatureSourceResolver(reopened)
        before = resolver.resolve(symbol="SPY", con_id=756733, expiration=EXPIRATION, cutoff=NOW)
        assert before["sources"] == {}
        instant = NOW + timedelta(seconds=4)
        result = resolver.resolve(symbol="SPY", con_id=756733, expiration=EXPIRATION, cutoff=instant)
        for kind, reference in references.items():
            bound = result["sources"][kind]
            assert bound["source_hash"] == observation(kind)["content_hash"]
            assert bound["row_hash"] == reference["row_hash"]
            assert bound["request_hash"] == reference["request_hash"]
            assert bound["basis_hash"] == reference["basis_hash"]
            assert bound["first_seen_at"] == reference["first_seen_at"]
        assert result["expiration"] == EXPIRATION.isoformat()
        assert result["market_score"] is result["volatility_score"] is None
        assert result["model_input_complete"] is result["production_eligible"] is False
        assert "FEATURE_PRODUCTION_AUTHORITY_UNRESOLVED" in result["reason_codes"]
        assert "IV_BASIS_UNRESOLVED" in result["reason_codes"]
        wrong = resolver.resolve(symbol="SPY", con_id=1, expiration=EXPIRATION, cutoff=instant)
        assert wrong["sources"] == {}
        stale = resolver.resolve(symbol="SPY", con_id=756733, expiration=EXPIRATION, cutoff=instant + timedelta(seconds=10))
        assert "CURRENT_IV_OBSERVATION_STALE_OR_UNAVAILABLE" in stale["reason_codes"]


@pytest.mark.parametrize("changes", [
    {"model_input_complete": True}, {"market_score": "0"}, {"con_id": True},
    {"production_eligible": True}, {"decision_authority": "MODEL_INPUT_COMPLETE"},
    {"symbol": "AAPL"}, {"expiration": "2026-09-18"},
    {"cutoff": (NOW + timedelta(seconds=1)).isoformat()},
    {"reason_codes": ["ALL_READY"]}, {"extra_authority": True},
])
def test_self_rehashed_binding_cannot_change_identity_or_authority(changes):
    reader = SimpleNamespace(read=lambda **_: ())
    payload = FeatureSourceResolver(reader).resolve(symbol="SPY", con_id=756733, expiration=EXPIRATION, cutoff=NOW)
    payload.update(deepcopy(changes))
    payload.pop("content_hash")
    payload["content_hash"] = canonical_hash(payload)
    with pytest.raises(ValueError):
        validate_feature_source_binding(payload, symbol="SPY", con_id=756733, expiration=EXPIRATION, cutoff=NOW)


def test_cache_failure_is_redacted_and_does_not_return_partial_success():
    def fail(**_):
        raise RuntimeError("private-sensitive-database-path")
    result = FeatureSourceResolver(SimpleNamespace(read=fail)).resolve(
        symbol="SPY", con_id=756733, expiration=EXPIRATION, cutoff=NOW,
    )
    assert result["sources"] == {}
    assert "FEATURE_SOURCE_CACHE_UNAVAILABLE_OR_INVALID" in result["reason_codes"]
    assert "private-sensitive" not in repr(result)


@pytest.mark.parametrize("reason", ["FEATURE_SOURCE_STORE_VERIFICATION_BUDGET_EXCEEDED", "FEATURE_SOURCE_STORE_INVALID"])
def test_known_cache_failure_preserves_precise_stable_reason(reason):
    def fail(**_):
        raise FeatureSourceStoreError(reason)
    result = FeatureSourceResolver(SimpleNamespace(read=fail)).resolve(
        symbol="SPY", con_id=756733, expiration=EXPIRATION, cutoff=NOW,
    )
    assert result["sources"] == {}
    assert reason in result["reason_codes"]


def attach_source_store(runtime, path):
    runtime.feature_source_store = FeatureSourceObservationStore(path)
    runtime._feature_source_persistence_lock = threading.RLock()
    runtime._feature_source_persistence = {"status": "NOT_RUN", "references": {}}
    runtime._closing = False
    runtime.runtime_services = SimpleNamespace(broker_evidence_acquisition=None)


def test_confirmed_diagnostic_persists_once_and_cache_get_never_fetches(tmp_path):
    runtime = _runtime()
    attach_source_store(runtime, tmp_path / "source.sqlite3")
    try:
        result = runtime.feature_sources_diagnostic(REQUEST)
        assert result["status"] == "OBSERVED"
        status = runtime.feature_source_cache()
        assert status["cache"]["observation_count"] == 3
        assert status["latest_ingestion"]["status"] == "PERSISTED"
        assert status["scheduled_producer"]["status"] == "UNWIRED"
        assert len(runtime.production_composition.gateway.calls) == 3
        app = create_app(OptionsCopilotServices(
            health_provider=lambda: {}, bootstrap_provider=lambda: {},
            candidates_provider=lambda: [], positions_provider=lambda: [], learning_provider=lambda: {},
            feature_source_cache_provider=runtime.feature_source_cache,
        ))
        async def read():
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://testserver") as client:
                return await client.get("/api/diagnostics/feature-source-cache")
        response = asyncio.run(read())
        assert response.status_code == 200
        assert response.json()["model_input_complete"] is False
        assert response.json()["cache"]["observation_count"] == 3
        assert runtime.feature_sources_diagnostic(REQUEST)["status"] == "WAIT"
        assert len(runtime.production_composition.gateway.calls) == 3
    finally:
        runtime.feature_source_store.close()


def test_closing_during_diagnostic_fences_persistence_and_preserves_store(tmp_path):
    runtime = _runtime()
    attach_source_store(runtime, tmp_path / "source.sqlite3")
    runtime._close_lock = threading.Lock()
    runtime._shutdown_health = {}
    runtime._feature_source_diagnostic_lock.acquire()
    try:
        assert runtime.close() is False
        assert runtime._shutdown_health["reason"] == "FEATURE_SOURCE_DIAGNOSTIC_SHUTDOWN_TIMEOUT"
        assert runtime._closing is True
        assert runtime.feature_source_store.status()["observation_count"] == 0
        source = observation("PRICE_HISTORY")
        ref, reason = runtime._persist_feature_source(source, operation_id="diagnostic.close", cutoff=NOW)
        assert ref is None and reason == "FEATURE_SOURCE_RUNTIME_CLOSING"
    finally:
        runtime._feature_source_diagnostic_lock.release()
        runtime.feature_source_store.close()
    result = runtime.feature_sources_diagnostic(REQUEST)
    assert result["status"] == "UNAVAILABLE"
    assert runtime.production_composition.gateway.calls == []


def test_closing_drains_manual_scan_before_closing_stores(tmp_path):
    runtime = _runtime()
    attach_source_store(runtime, tmp_path / "source.sqlite3")
    runtime._close_lock = threading.Lock()
    runtime._shutdown_health = {}
    runtime._immediate_scan_lock = threading.Lock()
    runtime._immediate_scan_lock.acquire()
    try:
        assert runtime.close() is False
        assert runtime._shutdown_health["reason"] == "MANUAL_SCAN_SHUTDOWN_TIMEOUT"
        assert runtime.feature_source_store.status()["observation_count"] == 0
    finally:
        runtime._immediate_scan_lock.release()
        runtime.feature_source_store.close()
    result = runtime.immediate_scan()
    assert result["scan_run_id"] is None
    assert "RUNTIME_CLOSING" in repr(result)
    assert runtime.production_composition.gateway.calls == []
