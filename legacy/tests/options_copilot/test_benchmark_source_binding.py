"""QQQ/SPY mapping confirmation does not authorize sources or production D/V."""

from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from options_copilot.api import OptionsCopilotServices, create_app
from options_copilot.feature_source_resolution import FeatureSourceResolver, validate_feature_source_binding
from options_copilot.gateway import BrokerSnapshotStatus
from options_copilot.storage.canonical import canonical_hash
from options_copilot.storage.feature_sources import FeatureSourceObservationStore
from tests.options_copilot import test_production_feature_source_binding as production_fixture
from tests.options_copilot.test_feature_source_resolution import attach_source_store, NOW
from tests.options_copilot.test_feature_sources_diagnostic import _runtime, _source
from tests.options_copilot.test_production_feature_source_binding import lane


CONFIRMED = datetime(2026, 9, 10, 12, 27, 58, tzinfo=timezone.utc)
EXPIRATION = date(2026, 10, 16)


def _request(cutoff=CONFIRMED, *, symbol="QQQ"):
    return dict(symbol=symbol, con_id=756733, expiration=EXPIRATION, cutoff=cutoff)


def _resolve(cutoff=CONFIRMED, *, symbol="QQQ", scheduled=False):
    return FeatureSourceResolver(
        SimpleNamespace(read=lambda **_: ()),
        history_store=SimpleNamespace(find_fragments=lambda **_: ()) if scheduled else None,
    ).resolve(**_request(cutoff, symbol=symbol))


def _rehash(document):
    body = {key: value for key, value in document.items() if key != "content_hash"}
    return {**body, "content_hash": canonical_hash(body)}


def _qqq_lane(lane, monkeypatch, resolver):
    original = production_fixture.OptionContractRef

    def contract(**values):
        values.update(symbol="QQQ", trading_class="QQQ", local_symbol=f"QQQ-{values['contract_id']}-C")
        return original(**values)

    monkeypatch.setattr(production_fixture, "NOW", CONFIRMED)
    monkeypatch.setattr(production_fixture, "OptionContractRef", contract)
    return lane(resolver, expirations=(EXPIRATION, EXPIRATION))


@pytest.mark.parametrize("symbol,scheduled,expected", [
    ("QQQ", False, "c403d7c841bdb09d416aa59875d4db57fa17845b991a23f745664bd9eebfdd08"),
    ("QQQ", True, "1887a2fb9c7467ba60c3c89181cfc72edf804c749283edee956db7beb21401d6"),
    ("SPY", False, "bf309dbf043d3998ad63cccf6c6e0458bcaaa60239f0a083b5d005c23288afc0"),
    ("SPY", True, "4dfb5ccfb8695c6688a67535515e39cc9fe9e069525efd520ac49878f0a97a8d"),
])
def test_existing_v4_and_non_qqq_hashes_remain_unchanged(symbol, scheduled, expected):
    cutoff = CONFIRMED - timedelta(microseconds=1) if symbol == "QQQ" else CONFIRMED
    document = _resolve(cutoff, symbol=symbol, scheduled=scheduled)
    assert document["content_hash"] == expected
    assert document["schema"].endswith(".v4")
    assert validate_feature_source_binding(document, **_request(cutoff, symbol=symbol)) == document


@pytest.mark.parametrize("scheduled", [False, True])
@pytest.mark.parametrize("offset", [-0.000001, 0, 1, 86400])
def test_qqq_confirmation_changes_only_mapping_decision(scheduled, offset):
    cutoff = CONFIRMED + timedelta(seconds=offset)
    document = _resolve(cutoff, scheduled=scheduled)
    assert document["schema"].endswith(".v4" if offset < 0 else ".v5")
    if offset < 0:
        assert "benchmark_convention" not in document
        assert "FEATURE_BENCHMARK_MAPPING_UNRESOLVED" in document["reason_codes"]
    else:
        from options_copilot.analytics.benchmark import benchmark_convention

        assert document["benchmark_convention"] == benchmark_convention("QQQ", cutoff)
        assert "FEATURE_BENCHMARK_MAPPING_UNRESOLVED" not in document["reason_codes"]
        assert "FEATURE_BENCHMARK_SOURCE_UNVERIFIED" in document["reason_codes"]
        assert "BENCHMARK_PRODUCTION_POLICY_UNVERIFIED" in document["reason_codes"]
    assert {"IV_BASIS_UNRESOLVED", "OPTION_SURFACE_UNBOUND", "FEATURE_PRODUCTION_AUTHORITY_UNRESOLVED"}.issubset(document["reason_codes"])
    assert document["market_score"] is document["volatility_score"] is None
    assert document["model_input_complete"] is document["production_eligible"] is False


@pytest.mark.parametrize("symbol", ["SPY", "XLF", "XLV", "IWM", "QQQM", "AAPL"])
def test_confirmation_does_not_approve_other_symbol_benchmarks(symbol):
    document = _resolve(symbol=symbol)
    assert document["schema"].endswith(".v4")
    assert "benchmark_convention" not in document
    assert "FEATURE_BENCHMARK_MAPPING_UNRESOLVED" in document["reason_codes"]


@pytest.mark.parametrize("version,cutoff,scheduled", [
    (1, datetime(2026, 9, 8, 16, tzinfo=timezone.utc), False),
    (2, datetime(2026, 9, 8, 16, tzinfo=timezone.utc), True),
    (3, datetime(2026, 9, 9, 7, 51, 50, tzinfo=timezone.utc), False),
    (4, datetime(2026, 9, 10, 1, 57, 44, tzinfo=timezone.utc), False),
])
def test_current_qqq_producer_rejects_downgrade_but_archives_survive(version, cutoff, scheduled, lane, monkeypatch):
    document = _resolve(cutoff, scheduled=scheduled)
    assert document["schema"].endswith(f".v{version}")
    document = _rehash({**document, "cutoff": CONFIRMED.isoformat()})
    assert validate_feature_source_binding(document, **_request()) == document
    with pytest.raises(ValueError):
        validate_feature_source_binding(document, **_request(), require_current_convention=True)
    acquisition, _, _, calls = _qqq_lane(lane, monkeypatch, SimpleNamespace(resolve=lambda **_: deepcopy(document)))
    result = production_fixture._acquire(acquisition)
    assert result["feature_source_bindings"]["bindings"] == ()
    assert "FEATURE_SOURCE_BINDING_INVALID" in result["feature_source_bindings"]["reason_codes"]
    assert calls == ["history", "quotes"]


@pytest.mark.parametrize("mutation", [
    {"benchmark_symbol": "QQQ"}, {"benchmark_symbol": "XLK"}, {"symbol": "SPY"},
    {"scope": "MODEL_AUTHORITY"}, {"human_signature_verified": True},
    {"human_signature_verified": 0}, {"production_eligible": True},
    {"production_policy_status": "VERIFIED"},
    {"confirmation_observed_at": (CONFIRMED - timedelta(seconds=1)).isoformat()},
    {"extra": "ALL_ETFS_APPROVED"},
])
def test_rehashing_benchmark_metadata_cannot_change_mapping_or_authority(mutation):
    document = _resolve()
    document["benchmark_convention"].update(mutation)
    document["benchmark_convention"] = _rehash(document["benchmark_convention"])
    with pytest.raises(ValueError):
        validate_feature_source_binding(_rehash(document), **_request())


@pytest.mark.parametrize("reason", [
    "FEATURE_BENCHMARK_MAPPING_UNRESOLVED", "FEATURE_BENCHMARK_SOURCE_VERIFIED",
    "BENCHMARK_PRODUCTION_POLICY_VERIFIED", "BENCHMARK_CONVENTION_UNAPPROVED",
])
def test_contradictory_qqq_benchmark_reasons_reject_even_with_new_hash(reason):
    document = _resolve()
    document["reason_codes"].append(reason)
    with pytest.raises(ValueError):
        validate_feature_source_binding(_rehash(document), **_request())


@pytest.mark.parametrize("kind", [
    "missing_spec", "null_spec", "missing_source_reason", "missing_policy_reason",
    "premature", "cross_symbol", "scheduled_authority", "score_promotion", "cutoff_mismatch",
])
def test_malformed_premature_or_cross_symbol_v5_rejected(kind):
    document = _resolve(scheduled=True)
    request = _request()
    if kind == "missing_spec":
        document.pop("benchmark_convention")
    elif kind == "null_spec":
        document["benchmark_convention"] = None
    elif kind == "missing_source_reason":
        document["reason_codes"].remove("FEATURE_BENCHMARK_SOURCE_UNVERIFIED")
    elif kind == "missing_policy_reason":
        document["reason_codes"].remove("BENCHMARK_PRODUCTION_POLICY_UNVERIFIED")
    elif kind == "premature":
        request["cutoff"] -= timedelta(microseconds=1)
        document["cutoff"] = request["cutoff"].isoformat()
    elif kind == "cross_symbol":
        request["symbol"] = document["symbol"] = "SPY"
    elif kind == "scheduled_authority":
        document["scheduled_history"]["historical_session_coverage_verified"] = True
    elif kind == "score_promotion":
        document["market_score"] = "0.5"
    else:
        document["cutoff"] = (CONFIRMED + timedelta(seconds=1)).isoformat()
    with pytest.raises(ValueError):
        validate_feature_source_binding(_rehash(document), **request)


def test_actual_production_keeps_v5_binding_and_iv_quarantine(lane, monkeypatch):
    acquisition, _, store, calls = _qqq_lane(
        lane, monkeypatch, FeatureSourceResolver(SimpleNamespace(read=lambda **_: ())),
    )
    result = production_fixture._acquire(acquisition)
    binding = result["feature_source_bindings"]["bindings"][0]
    assert binding["schema"].endswith(".v5")
    assert binding["benchmark_convention"]["benchmark_symbol"] == "SPY"
    assert calls == ["history", "quotes"]
    snapshot = result["broker_snapshot"]
    assert snapshot.status is BrokerSnapshotStatus.COMPLETE
    assert snapshot.verify_hash()
    assert result["broker_snapshot_hash"] == snapshot.snapshot_hash
    assert result["feature_source_bindings"] == acquisition.feature_source_bindings()
    assert result["reasons"] == (
        "ATM_SURFACE_SOURCE_UNVERIFIED", "IV_BASIS_UNRESOLVED", "FEATURE_PRODUCTION_AUTHORITY_UNRESOLVED",
    )
    assert not {"atm_iv", "atm_iv_by_underlying", "volatility_by_underlying", "market_score", "volatility_score"} & result.keys()
    assert store.query(kinds=("BROKER_SNAPSHOT",), limit=1) == ()
    assert store.verify_integrity() is True


def test_qqq_confirmation_preserves_real_store_observation_refs(tmp_path):
    source = _source("PRICE_HISTORY", "QQQ", NOW)
    source["requested_at"] = (NOW + timedelta(seconds=1)).isoformat()
    source["available_at"] = (NOW + timedelta(seconds=2)).isoformat()
    source = _rehash(source)
    with FeatureSourceObservationStore(tmp_path / "source.sqlite3", clock=lambda: NOW + timedelta(seconds=3)) as store:
        store.append(source, operation_id="qqq-benchmark", cutoff=NOW)
        resolver = FeatureSourceResolver(store)
        old = resolver.resolve(**_request(CONFIRMED - timedelta(microseconds=1)))
        current = resolver.resolve(**_request())
        assert old["sources"]["PRICE_HISTORY"]
        assert {k: v for k, v in old["sources"]["PRICE_HISTORY"].items() if k != "projection_age_seconds"} == {
            k: v for k, v in current["sources"]["PRICE_HISTORY"].items() if k != "projection_age_seconds"
        }
        assert store.status()["observation_count"] == 1


@pytest.mark.parametrize("cutoff", [CONFIRMED - timedelta(microseconds=1), CONFIRMED])
def test_cache_api_exposes_qqq_choice_without_consumption_or_acquisition(tmp_path, monkeypatch, cutoff):
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
            assert body["qqq_benchmark_convention"] is None
        else:
            assert body["qqq_benchmark_convention"]["benchmark_symbol"] == "SPY"
            assert body["qqq_benchmark_convention"]["scope"] == "CALCULATION_SEMANTICS_ONLY"
        assert body["latest_consumption"]["status"] == "WIRED_NOT_RUN"
        assert body["model_input_complete"] is body["production_eligible"] is False
        assert runtime.production_composition.gateway.calls == []
        assert body["cache"]["observation_count"] == 0
        assert _rehash(body) == body
    finally:
        runtime.feature_source_store.close()
