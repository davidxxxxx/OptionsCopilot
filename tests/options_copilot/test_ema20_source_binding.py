"""Confirmed EMA semantics remain separate from source and model authority."""

from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from options_copilot.api import OptionsCopilotServices, create_app
from options_copilot.feature_source_resolution import FeatureSourceResolver, validate_feature_source_binding
from options_copilot.storage.canonical import canonical_hash
from tests.options_copilot import test_production_feature_source_binding as production_fixture
from tests.options_copilot.test_feature_source_resolution import attach_source_store, observation
from tests.options_copilot.test_feature_sources_diagnostic import _runtime
from tests.options_copilot.test_production_feature_source_binding import lane
from options_copilot.storage.feature_sources import FeatureSourceObservationStore


CONFIRMED = datetime(2026, 9, 9, 7, 51, 50, tzinfo=timezone.utc)
LEGACY_CUTOFF = datetime(2026, 9, 8, 16, tzinfo=timezone.utc)
EXPIRATION = date(2026, 9, 25)
LEGACY_REASONS = [
    "FEATURE_HISTORY_CALENDAR_UNVERIFIED", "FEATURE_HISTORY_SECDEF_UNRESOLVED",
    "EMA_CONVENTION_UNAPPROVED", "IV_BASIS_UNRESOLVED",
    "FEATURE_BENCHMARK_MAPPING_UNRESOLVED", "OPTION_SURFACE_UNBOUND",
    "FEATURE_PRODUCTION_AUTHORITY_UNRESOLVED", "PRICE_HISTORY_OBSERVATION_MISSING",
    "IV_HISTORY_OBSERVATION_MISSING", "CURRENT_IV_OBSERVATION_MISSING",
]
LEGACY_V1 = {
    "con_id": 756733, "cutoff": "2026-09-08T16:00:00+00:00",
    "decision_authority": "OBSERVATION_ONLY", "expiration": "2026-09-25",
    "market_score": None, "model_input_complete": False, "production_eligible": False,
    "reason_codes": LEGACY_REASONS, "schema": "options_copilot.feature_source_binding.v1",
    "sources": {}, "status": "INCOMPLETE", "symbol": "SPY", "volatility_score": None,
    "content_hash": "b6f6e21d2c02d7728a981e22af94bc1d305b35f4112fe383a04f0305e2e1ac2c",
}
LEGACY_V2 = {
    **LEGACY_V1, "schema": "options_copilot.feature_source_binding.v2",
    "reason_codes": [*LEGACY_REASONS, "SCHEDULED_HISTORY_FRAGMENTS_MISSING"],
    "scheduled_history": {
        "adjustment_vintages_mergeable": False, "fragments": [], "has_more": False,
        "historical_session_coverage_verified": False, "next_before_sequence": None,
        "status": "MISSING",
    },
    "content_hash": "fe66726c9557a8f11f3e261c298cb643894abe06291448b302c503dc314854b4",
}


def _request(cutoff=CONFIRMED):
    return dict(symbol="SPY", con_id=756733, expiration=EXPIRATION, cutoff=cutoff)


def _resolve(cutoff=CONFIRMED, *, scheduled=False):
    return FeatureSourceResolver(
        SimpleNamespace(read=lambda **_: ()),
        history_store=SimpleNamespace(find_fragments=lambda **_: ()) if scheduled else None,
    ).resolve(**_request(cutoff))


def _rehash(document):
    body = {key: value for key, value in document.items() if key != "content_hash"}
    return {**body, "content_hash": canonical_hash(body)}


@pytest.mark.parametrize("document,scheduled", [(LEGACY_V1, False), (LEGACY_V2, True)])
def test_frozen_legacy_bindings_and_replays_are_unchanged(document, scheduled):
    assert validate_feature_source_binding(deepcopy(document), **_request(LEGACY_CUTOFF)) == document
    assert _resolve(LEGACY_CUTOFF, scheduled=scheduled) == document


@pytest.mark.parametrize("legacy", [LEGACY_V1, LEGACY_V2])
def test_post_confirmation_legacy_is_archive_only_not_current_production(legacy):
    # An old deployment can emit its old schema after the semantic decision;
    # preserve archive readback but never accept it from a current producer.
    document = _rehash({**deepcopy(legacy), "cutoff": CONFIRMED.isoformat()})
    assert validate_feature_source_binding(document, **_request()) == document
    with pytest.raises(ValueError):
        validate_feature_source_binding(document, **_request(), require_current_convention=True)


@pytest.mark.parametrize("legacy", [LEGACY_V1, LEGACY_V2])
def test_actual_production_rejects_post_confirmation_legacy_binding(legacy, lane, monkeypatch):
    document = _rehash({**deepcopy(legacy), "cutoff": CONFIRMED.isoformat()})
    monkeypatch.setattr(production_fixture, "NOW", CONFIRMED)
    acquisition, _, _, _ = lane(SimpleNamespace(resolve=lambda **_: deepcopy(document)))
    report = production_fixture._acquire(acquisition)["feature_source_bindings"]
    assert report["bindings"] == ()
    assert "FEATURE_SOURCE_BINDING_INVALID" in report["reason_codes"]


@pytest.mark.parametrize("cutoff", [LEGACY_CUTOFF, CONFIRMED])
def test_contradictory_ema_reason_cannot_be_self_rehashed(cutoff):
    document = _resolve(cutoff)
    document["reason_codes"].append("EMA_PRODUCTION_POLICY_VERIFIED")
    with pytest.raises(ValueError):
        validate_feature_source_binding(_rehash(document), **_request(cutoff))


@pytest.mark.parametrize("scheduled", [False, True])
@pytest.mark.parametrize("offset", [0, 1, 86400])
def test_new_bindings_resolve_only_semantics_and_keep_policy_missing(scheduled, offset):
    from options_copilot.analytics.iv_percentile import IV_PERCENTILE_CONFIRMATION_OBSERVED_AT

    cutoff = CONFIRMED + timedelta(seconds=offset)
    result = _resolve(cutoff, scheduled=scheduled)
    version = "v4" if cutoff >= IV_PERCENTILE_CONFIRMATION_OBSERVED_AT else "v3"
    assert result["schema"] == f"options_copilot.feature_source_binding.{version}"
    convention = result["ema20_convention"]
    assert convention["scope"] == "CALCULATION_SEMANTICS_ONLY"
    assert convention["production_eligible"] is False
    assert convention["human_signature_verified"] is False
    assert convention["production_policy_status"] == "UNVERIFIED"
    assert "EMA_CONVENTION_UNAPPROVED" not in result["reason_codes"]
    assert "EMA_PRODUCTION_POLICY_UNVERIFIED" in result["reason_codes"]
    assert set(LEGACY_REASONS) - {"EMA_CONVENTION_UNAPPROVED"} <= set(result["reason_codes"])
    assert result["scheduled_history"] == (LEGACY_V2["scheduled_history"] if scheduled else None)
    assert result["market_score"] is result["volatility_score"] is None
    assert result["model_input_complete"] is result["production_eligible"] is False


def test_confirmation_is_not_backdated_even_by_one_microsecond():
    result = _resolve(CONFIRMED - timedelta(microseconds=1))
    assert result["schema"].endswith(".v1")
    assert "EMA_CONVENTION_UNAPPROVED" in result["reason_codes"]
    assert "ema20_convention" not in result


@pytest.mark.parametrize("mutation", [
    {"production_eligible": True}, {"human_signature_verified": True},
    {"production_policy_status": "VERIFIED"}, {"scope": "MODEL_AUTHORITY"},
    {"convention_id": "EMA252"}, {"extra": "authority"},
    {"period": 21}, {"window": 252}, {"updates": 39},
    {"alpha": {"numerator": 2, "denominator": 20}},
    {"seed": {"method": "FIRST_CLOSE", "observations": 1}},
    {"price_basis": "IBKR_TRADES"},
    {"decimal_precision": 29}, {"decimal_rounding": "ROUND_UP"},
    {"human_signature_verified": 0},
    {"confirmation_observed_at": "2026-09-08T07:51:50+00:00"},
])
def test_rehashed_convention_cannot_be_changed_or_promoted(mutation):
    document = _resolve()
    document["ema20_convention"].update(mutation)
    document["ema20_convention"] = _rehash(document["ema20_convention"])
    with pytest.raises(ValueError):
        validate_feature_source_binding(_rehash(document), **_request())


@pytest.mark.parametrize("kind", ["missing_spec", "null_spec", "missing_policy_reason", "old_reason", "premature", "scheduled_authority"])
def test_malformed_or_premature_v3_is_rejected(kind):
    document = _resolve(scheduled=True)
    cutoff = CONFIRMED
    if kind == "missing_spec":
        document.pop("ema20_convention")
    elif kind == "null_spec":
        document["ema20_convention"] = None
    elif kind == "missing_policy_reason":
        document["reason_codes"].remove("EMA_PRODUCTION_POLICY_UNVERIFIED")
    elif kind == "old_reason":
        document["reason_codes"].append("EMA_CONVENTION_UNAPPROVED")
    elif kind == "premature":
        cutoff -= timedelta(microseconds=1)
        document["cutoff"] = cutoff.isoformat()
    else:
        document["scheduled_history"]["historical_session_coverage_verified"] = True
    with pytest.raises(ValueError):
        validate_feature_source_binding(_rehash(document), **_request(cutoff))


def test_real_cache_refs_do_not_change_when_only_semantics_become_known(tmp_path):
    with FeatureSourceObservationStore(tmp_path / "source.sqlite3", clock=lambda: LEGACY_CUTOFF + timedelta(seconds=3)) as store:
        store.append(observation("PRICE_HISTORY"), operation_id="semantic-test", cutoff=LEGACY_CUTOFF)
        resolver = FeatureSourceResolver(store)
        old = resolver.resolve(**_request(CONFIRMED - timedelta(microseconds=1)))
        current = resolver.resolve(**_request())
        old_ref, new_ref = old["sources"]["PRICE_HISTORY"], current["sources"]["PRICE_HISTORY"]
        assert {key: value for key, value in old_ref.items() if key != "projection_age_seconds"} == {
            key: value for key, value in new_ref.items() if key != "projection_age_seconds"
        }
        assert store.status()["observation_count"] == 1
        assert "FEATURE_HISTORY_CALENDAR_UNVERIFIED" in current["reason_codes"]


def test_actual_production_acquisition_consumes_v3_without_extra_requests(lane, monkeypatch):
    monkeypatch.setattr(production_fixture, "NOW", CONFIRMED)
    acquisition, _, store, calls = lane(FeatureSourceResolver(SimpleNamespace(read=lambda **_: ())))
    result = production_fixture._acquire(acquisition)
    binding = result["feature_source_bindings"]["bindings"][0]
    assert binding["schema"].endswith(".v3")
    assert "EMA_PRODUCTION_POLICY_UNVERIFIED" in result["feature_source_bindings"]["reason_codes"]
    assert calls == ["history", "quotes"]
    assert "market_score" not in result and "volatility_score" not in result
    persisted = store.query(kinds=("BROKER_SNAPSHOT",), limit=1)[0].record.payload
    assert persisted["feature_source_bindings"]["bindings"][0]["content_hash"] == binding["content_hash"]
    assert store.verify_integrity() is True


@pytest.mark.parametrize("cutoff", [CONFIRMED - timedelta(microseconds=1), CONFIRMED])
def test_cache_api_shows_convention_without_claiming_consumption_or_acquiring(tmp_path, monkeypatch, cutoff):
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
            response = client.get("/api/diagnostics/feature-source-cache")
        assert response.status_code == 200
        body = response.json()
        if cutoff < CONFIRMED:
            assert body["ema20_convention"] is None
        else:
            assert body["ema20_convention"]["scope"] == "CALCULATION_SEMANTICS_ONLY"
        assert body["latest_consumption"]["status"] == "WIRED_NOT_RUN"
        assert body["model_input_complete"] is body["production_eligible"] is False
        assert runtime.production_composition.gateway.calls == []
        assert body["cache"]["observation_count"] == 0
        assert _rehash(body) == body
    finally:
        runtime.feature_source_store.close()
