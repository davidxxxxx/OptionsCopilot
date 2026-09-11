"""Current production must not derive ATM or native IV from selected-leg means."""
from __future__ import annotations

from dataclasses import replace
from datetime import date, timedelta
from decimal import Decimal
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from options_copilot.analytics.iv_percentile import IV_PERCENTILE_CONFIRMATION_OBSERVED_AT
from options_copilot.gateway import BrokerSnapshotStatus
from options_copilot.storage.canonical import canonical_hash
from tests.options_copilot import test_production_feature_source_binding as binding_tests
from tests.options_copilot.test_production_feature_source_binding import lane


@pytest.fixture(autouse=True)
def prior_new_york_history(monkeypatch):
    original = binding_tests._history

    def history(symbol, *, end_at):
        value = original(symbol, end_at=end_at)
        prior_date = end_at.astimezone(ZoneInfo("America/New_York")).date()
        value = replace(value, points=tuple(
            point for point in value.points if point.trading_date < prior_date
        ))
        return replace(value, content_hash=canonical_hash(value.hash_payload()))

    monkeypatch.setattr(binding_tests, "_history", history)


@pytest.mark.parametrize("offset", [timedelta(0), timedelta(microseconds=1), timedelta(days=1)])
def test_current_acquisition_quarantines_cross_expiry_leg_iv_means(lane, monkeypatch, offset):
    cutoff = IV_PERCENTILE_CONFIRMATION_OBSERVED_AT + offset
    monkeypatch.setattr(binding_tests, "NOW", cutoff)
    acquisition, source, store, calls = lane(
        SimpleNamespace(resolve=binding_tests._binding),
        expirations=(date(2026, 9, 25), date(2026, 9, 25), date(2026, 10, 2)),
    )
    read_quotes = source.option_quote_batch

    def varied_quotes(requested):
        batch = read_quotes(requested)
        return replace(batch, quotes=tuple(
            replace(row, implied_volatility=iv)
            for row, iv in zip(batch.quotes, (Decimal("0.15"), Decimal("0.25"), Decimal("0.80")), strict=True)
        ))

    source.option_quote_batch = varied_quotes
    result = binding_tests._acquire(acquisition)

    assert "IV_BASIS_UNRESOLVED" in result["reasons"]
    assert "ATM_SURFACE_SOURCE_UNVERIFIED" in result["reasons"]
    assert "FEATURE_PRODUCTION_AUTHORITY_UNRESOLVED" in result["reasons"]
    assert result["broker_snapshot"].status is BrokerSnapshotStatus.COMPLETE
    assert result["broker_snapshot"].verify_hash()
    assert result["broker_snapshot_hash"] == result["broker_snapshot"].snapshot_hash
    assert len(result["feature_source_bindings"]["bindings"]) == 2
    assert result["feature_source_bindings"] == acquisition.feature_source_bindings()
    assert result["feature_source_bindings"]["production_eligible"] is False
    assert not {"atm_iv", "atm_iv_by_underlying", "volatility_by_underlying", "market_score", "volatility_score"} & result.keys()
    assert calls == ["history", "quotes"]
    assert store.query(kinds=("BROKER_SNAPSHOT",), limit=1) == ()
    assert store.verify_integrity()


def test_pre_confirmation_fixture_retains_legacy_derivation(lane, monkeypatch):
    monkeypatch.setattr(
        binding_tests, "NOW",
        IV_PERCENTILE_CONFIRMATION_OBSERVED_AT - timedelta(microseconds=1),
    )
    acquisition, _, store, calls = lane(SimpleNamespace(resolve=binding_tests._binding))
    result = binding_tests._acquire(acquisition)
    assert result["reasons"] == ()
    assert result["atm_iv"] == Decimal("0.20")
    assert result["volatility_by_underlying"]["SPY"]["iv_history"]
    assert calls == ["history", "quotes"]
    assert len(store.query(kinds=("BROKER_SNAPSHOT",), limit=1)) == 1


@pytest.mark.parametrize("forged_resolver", [False, True])
def test_context_and_resolver_cannot_bypass_server_clock_quarantine(lane, monkeypatch, forged_resolver):
    monkeypatch.setattr(binding_tests, "NOW", IV_PERCENTILE_CONFIRMATION_OBSERVED_AT)

    def resolve(**request):
        value = binding_tests._binding(**request)
        if forged_resolver:
            value.update(model_input_complete=True, production_eligible=True, reason_codes=[])
            value.pop("content_hash")
            value["content_hash"] = canonical_hash(value)
        return value

    acquisition, _, store, calls = lane(SimpleNamespace(resolve=resolve))
    result = acquisition.acquire(
        scan_run_id="forged-feature-context", universe={},
        context={
            "cutoff": "2026-09-01T00:00:00Z", "production_eligible": True,
            "model_input_complete": True, "atm_iv": Decimal("0.2"),
            "volatility_score": Decimal("0"),
        },
    )
    assert "IV_BASIS_UNRESOLVED" in result["reasons"]
    assert "volatility_by_underlying" not in result
    assert "atm_iv" not in result
    assert result["feature_source_bindings"]["production_eligible"] is False
    if forged_resolver:
        assert "FEATURE_SOURCE_BINDING_INVALID" in result["feature_source_bindings"]["reason_codes"]
    assert calls == ["history", "quotes"]
    assert store.query(kinds=("BROKER_SNAPSHOT",), limit=1) == ()


def test_current_incomplete_snapshot_keeps_original_quote_failure(lane, monkeypatch):
    monkeypatch.setattr(binding_tests, "NOW", IV_PERCENTILE_CONFIRMATION_OBSERVED_AT)
    acquisition, _, store, calls = lane(SimpleNamespace(resolve=binding_tests._binding))
    missing = SimpleNamespace(reason_codes=("QUOTE_EXCHANGE_TIMESTAMP_UNAVAILABLE:101",))
    monkeypatch.setattr(acquisition.broker_snapshot_builder, "build", lambda _: missing)
    result = binding_tests._acquire(acquisition)
    assert result["reasons"] == missing.reason_codes
    assert result["broker_snapshot"] is missing
    assert len(result["feature_source_bindings"]["bindings"]) == 1
    assert calls == ["history"]
    assert store.query(kinds=("BROKER_SNAPSHOT",), limit=1) == ()


def test_acquisition_crossing_confirmation_boundary_cannot_publish_legacy_iv(lane, monkeypatch):
    before = IV_PERCENTILE_CONFIRMATION_OBSERVED_AT - timedelta(microseconds=1)
    monkeypatch.setattr(binding_tests, "NOW", before)
    acquisition, _, store, calls = lane(SimpleNamespace(resolve=binding_tests._binding))
    moments = iter((before, before, IV_PERCENTILE_CONFIRMATION_OBSERVED_AT))
    acquisition._clock = lambda: next(moments)
    result = binding_tests._acquire(acquisition)
    assert "IV_BASIS_UNRESOLVED" in result["reasons"]
    assert "atm_iv" not in result
    assert result["broker_snapshot"].built_at == before
    assert result["feature_source_bindings"]["cutoff"] == before.isoformat()
    assert calls == ["history", "quotes"]
    assert store.query(kinds=("BROKER_SNAPSHOT",), limit=1) == ()


def test_current_quarantine_preserves_independent_supporting_positioning(lane, monkeypatch):
    monkeypatch.setattr(binding_tests, "NOW", IV_PERCENTILE_CONFIRMATION_OBSERVED_AT)
    acquisition, _, store, calls = lane(SimpleNamespace(resolve=binding_tests._binding))
    result = acquisition.acquire(
        scan_run_id="quarantined-with-positioning",
        universe={"finalists": ({"symbol": "SPY", "spot": Decimal("102")},)},
        context={},
    )
    positioning = acquisition.positioning()
    assert "IV_BASIS_UNRESOLVED" in result["reasons"]
    assert positioning["count"] == 1
    assert positioning["decision_authority"] == "SUPPORTING_ONLY"
    assert positioning["supporting_only"] is True
    for field in ("affects_eligibility", "approval_allowed", "instruction_allowed", "order_allowed"):
        assert positioning[field] is False
    row = positioning["positioning"][0]
    assert row["broker_snapshot_hash"] == result["broker_snapshot_hash"]
    assert row["scan_run_id"] == "quarantined-with-positioning"
    assert row["chain_scope"] == "FROZEN_FINALIST_LEGS_ONLY"
    assert "atm_iv" not in result and "volatility_by_underlying" not in result
    assert calls == ["history", "quotes"]
    assert store.query(kinds=("BROKER_SNAPSHOT",), limit=1) == ()
