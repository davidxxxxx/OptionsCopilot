"""Confirmed IV semantics do not confer native-source or production authority."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest
from fastapi.testclient import TestClient

from options_copilot.api import OptionsCopilotServices, create_app
from options_copilot.feature_source_resolution import FeatureSourceResolver, validate_feature_source_binding
from options_copilot.gateway import BrokerSnapshotStatus
from options_copilot.storage.canonical import canonical_hash
from options_copilot.storage.feature_sources import FeatureSourceObservationStore
from tests.options_copilot import test_production_feature_source_binding as production_fixture
from tests.options_copilot.test_ema20_source_binding import (
    CONFIRMED as EMA_CONFIRMED, LEGACY_V1, LEGACY_V2, _rehash, _request, _resolve,
)
from tests.options_copilot.test_feature_source_resolution import attach_source_store, observation, NOW
from tests.options_copilot.test_feature_sources_diagnostic import _runtime
from tests.options_copilot.test_production_feature_source_binding import lane


CONFIRMED = datetime(2026, 9, 10, 1, 57, 44, tzinfo=timezone.utc)


def _align_production_clock(monkeypatch):
    # This boundary falls on the previous NY date. The older daytime fixture
    # builds UTC-date rows; use prior NY dates so identity checks actually pass.
    original_history = production_fixture._history

    def history(symbol, *, end_at):
        value = original_history(symbol, end_at=end_at)
        local_day = CONFIRMED.astimezone(ZoneInfo("America/New_York")).date()
        value = replace(value, points=tuple(
            replace(point, trading_date=local_day - timedelta(days=15 - index))
            for index, point in enumerate(value.points)
        ), content_hash="")
        return replace(value, content_hash=canonical_hash(value.hash_payload()))

    monkeypatch.setattr(production_fixture, "NOW", CONFIRMED)
    monkeypatch.setattr(production_fixture, "_history", history)


@pytest.mark.parametrize("scheduled,expected_hash", [
    (False, "9bb8259fdea721eef1eb2b988761494c54df27f4d3340908109990ce1e088418"),
    (True, "fa1ac031f2d54096e79088516466db25eb32babd20656dd691a7f753e8049dcf"),
])
def test_frozen_v3_hashes_and_archive_readback_stay_unchanged(scheduled, expected_hash):
    document = _resolve(EMA_CONFIRMED, scheduled=scheduled)
    assert document["content_hash"] == expected_hash
    assert validate_feature_source_binding(document, **_request(EMA_CONFIRMED)) == document


@pytest.mark.parametrize("scheduled", [False, True])
@pytest.mark.parametrize("offset", [-0.000001, 0, 1, 86400])
def test_iv_boundary_keeps_real_basis_unresolved(scheduled, offset):
    cutoff = CONFIRMED + timedelta(seconds=offset)
    result = _resolve(cutoff, scheduled=scheduled)
    assert result["schema"].endswith(".v3" if offset < 0 else ".v4")
    assert "IV_BASIS_UNRESOLVED" in result["reason_codes"]
    if offset < 0:
        assert "iv_percentile_convention" not in result
    else:
        from options_copilot.analytics.iv_percentile import iv_percentile_convention

        assert result["iv_percentile_convention"] == iv_percentile_convention(cutoff)
        assert "IV_PRODUCTION_POLICY_UNVERIFIED" in result["reason_codes"]
    assert result["market_score"] is result["volatility_score"] is None
    assert result["production_eligible"] is result["model_input_complete"] is False


@pytest.mark.parametrize("version", [1, 2, 3])
def test_current_producer_rejects_old_schemas_without_breaking_archives(version, lane, monkeypatch):
    legacy = [LEGACY_V1, LEGACY_V2, _resolve(EMA_CONFIRMED)][version - 1]
    document = _rehash({**deepcopy(legacy), "cutoff": CONFIRMED.isoformat()})
    assert validate_feature_source_binding(document, **_request(CONFIRMED)) == document
    with pytest.raises(ValueError):
        validate_feature_source_binding(document, **_request(CONFIRMED), require_current_convention=True)
    _align_production_clock(monkeypatch)
    acquisition, _, _, _ = lane(SimpleNamespace(resolve=lambda **_: deepcopy(document)))
    report = production_fixture._acquire(acquisition)["feature_source_bindings"]
    assert report["bindings"] == ()
    assert "FEATURE_SOURCE_BINDING_INVALID" in report["reason_codes"]


@pytest.mark.parametrize("mutation", [
    {"production_eligible": True}, {"human_signature_verified": True},
    {"production_policy_status": "VERIFIED"}, {"scope": "MODEL_AUTHORITY"},
    {"confirmation_observed_at": EMA_CONFIRMED.isoformat()},
    {"completed_session_window": 251}, {"incomparable_behavior": "CALCULATE_ANYWAY"},
    {"extra": "SOURCE_VERIFIED"}, {"human_signature_verified": 0},
])
def test_self_rehashing_iv_convention_does_not_grant_authority(mutation):
    document = _resolve(CONFIRMED)
    document["iv_percentile_convention"].update(mutation)
    document["iv_percentile_convention"] = _rehash(document["iv_percentile_convention"])
    with pytest.raises(ValueError):
        validate_feature_source_binding(_rehash(document), **_request(CONFIRMED))


@pytest.mark.parametrize("cutoff", [EMA_CONFIRMED, CONFIRMED])
@pytest.mark.parametrize("reason", [
    "IV_BASIS_VERIFIED", "IV_PRODUCTION_POLICY_VERIFIED", "IV_CONVENTION_UNAPPROVED",
])
def test_contradictory_iv_reasons_reject_even_with_valid_outer_hash(cutoff, reason):
    document = _resolve(cutoff)
    document["reason_codes"].append(reason)
    with pytest.raises(ValueError):
        validate_feature_source_binding(_rehash(document), **_request(cutoff))


@pytest.mark.parametrize("kind", [
    "missing_spec", "null_spec", "missing_basis_reason", "missing_policy_reason",
    "premature", "scheduled_authority", "score_promotion", "cutoff_mismatch",
])
def test_malformed_or_premature_v4_rejected(kind):
    document = _resolve(CONFIRMED, scheduled=True)
    cutoff = CONFIRMED
    if kind == "missing_spec":
        document.pop("iv_percentile_convention")
    elif kind == "null_spec":
        document["iv_percentile_convention"] = None
    elif kind == "missing_basis_reason":
        document["reason_codes"].remove("IV_BASIS_UNRESOLVED")
    elif kind == "missing_policy_reason":
        document["reason_codes"].remove("IV_PRODUCTION_POLICY_UNVERIFIED")
    elif kind == "premature":
        cutoff -= timedelta(microseconds=1)
        document["cutoff"] = cutoff.isoformat()
    elif kind == "scheduled_authority":
        document["scheduled_history"]["historical_session_coverage_verified"] = True
    elif kind == "score_promotion":
        document["volatility_score"] = "0.5"
    else:
        document["cutoff"] = (CONFIRMED + timedelta(seconds=1)).isoformat()
    with pytest.raises(ValueError):
        validate_feature_source_binding(_rehash(document), **_request(cutoff))


def test_actual_production_retains_v4_but_quarantines_legacy_iv(lane, monkeypatch):
    _align_production_clock(monkeypatch)
    acquisition, _, store, calls = lane(FeatureSourceResolver(SimpleNamespace(read=lambda **_: ())))
    result = production_fixture._acquire(acquisition)
    binding = result["feature_source_bindings"]["bindings"][0]
    assert binding["schema"].endswith(".v4")
    assert "IV_BASIS_UNRESOLVED" in result["feature_source_bindings"]["reason_codes"]
    assert "IV_PRODUCTION_POLICY_UNVERIFIED" in result["feature_source_bindings"]["reason_codes"]
    assert calls == ["history", "quotes"]
    assert result["reasons"] == (
        "ATM_SURFACE_SOURCE_UNVERIFIED",
        "IV_BASIS_UNRESOLVED",
        "FEATURE_PRODUCTION_AUTHORITY_UNRESOLVED",
    )
    snapshot = result["broker_snapshot"]
    assert snapshot.status is BrokerSnapshotStatus.COMPLETE
    assert snapshot.verify_hash()
    assert result["broker_snapshot_hash"] == snapshot.snapshot_hash
    assert result["feature_source_bindings"] == acquisition.feature_source_bindings()
    assert not {"atm_iv", "atm_iv_by_underlying", "volatility_by_underlying", "market_score", "volatility_score"} & result.keys()
    assert store.query(kinds=("BROKER_SNAPSHOT",), limit=1) == ()
    assert store.verify_integrity() is True


def test_semantic_confirmation_never_rewrites_observation_references(tmp_path):
    with FeatureSourceObservationStore(tmp_path / "source.sqlite3", clock=lambda: NOW + timedelta(seconds=3)) as store:
        store.append(observation("PRICE_HISTORY"), operation_id="iv-semantic-test", cutoff=NOW)
        resolver = FeatureSourceResolver(store)
        old = resolver.resolve(**_request(CONFIRMED - timedelta(microseconds=1)))
        current = resolver.resolve(**_request(CONFIRMED))
        assert old["sources"]["PRICE_HISTORY"]
        assert {k: v for k, v in old["sources"]["PRICE_HISTORY"].items() if k != "projection_age_seconds"} == {
            k: v for k, v in current["sources"]["PRICE_HISTORY"].items() if k != "projection_age_seconds"
        }
        assert store.status()["observation_count"] == 1


@pytest.mark.parametrize("cutoff", [CONFIRMED - timedelta(microseconds=1), CONFIRMED])
def test_cache_api_exposes_iv_semantics_not_consumption(tmp_path, monkeypatch, cutoff):
    import options_copilot.runtime as runtime_module

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return cutoff

    runtime = _runtime()
    attach_source_store(runtime, tmp_path / "api.sqlite3")
    monkeypatch.setattr(runtime_module, "datetime", Clock)
    try:
        app = create_app(OptionsCopilotServices(
            health_provider=lambda: {}, bootstrap_provider=lambda: {},
            candidates_provider=lambda: [], positions_provider=lambda: [], learning_provider=lambda: {},
            feature_source_cache_provider=runtime.feature_source_cache,
        ))
        with TestClient(app) as client:
            body = client.get("/api/diagnostics/feature-source-cache").json()
        if cutoff < CONFIRMED:
            assert body["iv_percentile_convention"] is None
        else:
            assert body["iv_percentile_convention"]["scope"] == "CALCULATION_SEMANTICS_ONLY"
        assert body["latest_consumption"]["status"] == "WIRED_NOT_RUN"
        assert body["model_input_complete"] is body["production_eligible"] is False
        assert runtime.production_composition.gateway.calls == []
        assert body["cache"]["observation_count"] == 0
        assert _rehash(body) == body
    finally:
        runtime.feature_source_store.close()
