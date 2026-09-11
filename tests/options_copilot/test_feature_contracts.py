"""Offline data contracts; synthetic records confer no source or model authority."""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, localcontext

import pytest

from options_copilot.analytics.feature_contracts import (
    FeatureContractError, FeatureHistoryBatch, FeatureHistoryPoint,
    HistoryRequestContract, SeriesBasisContract, select_feature_window,
)


NOW = datetime(2026, 9, 7, 2, tzinfo=timezone.utc)


def request(**changes):
    values = dict(
        source="IBKR", adapter_version="fixture-0.9.86", con_id=101,
        symbol="XOM", secdef_hash="a" * 64, what_to_show="ADJUSTED_LAST",
        bar_size="1 day", use_rth=True, exchange_timezone="America/New_York",
        request_end=NOW, duration="1 Y", start_session=date(2025, 9, 1),
        end_session=date(2026, 9, 4), adjustment_basis="SPLIT_DIVIDEND_ADJUSTED",
        value_unit="USD_PER_SHARE", volume_unit="SHARES",
    )
    values.update(changes)
    return HistoryRequestContract(**values)


def basis(**changes):
    values = dict(
        series_kind="PRICE", calendar_hash="b" * 64,
        methodology_id="fixture-adjusted-close", methodology_version="1",
        adjustment_basis="SPLIT_DIVIDEND_ADJUSTED", value_unit="USD_PER_SHARE",
        sampling="SESSION_CLOSE", horizon="NOT_APPLICABLE", tenor_days=None,
        rolling_rule="NOT_APPLICABLE", moneyness="NOT_APPLICABLE",
        option_side="NOT_APPLICABLE", interpolation="NONE",
    )
    values.update(changes)
    return SeriesBasisContract(**values)


def point(day=date(2026, 9, 4), **changes):
    values = dict(session_date=day, session_close_at=datetime(2026, 9, 4, 20, tzinfo=timezone.utc),
                  close=Decimal("103.123456789012345"), volume=Decimal(12000))
    values.update(changes)
    return FeatureHistoryPoint(**values)


def batch(**changes):
    values = dict(request=request(), basis=basis(), points=(point(),),
                  available_at=NOW, source_revision_hash="c" * 64, request_fingerprint="e" * 64)
    values.update(changes)
    return FeatureHistoryBatch(**values)


@pytest.mark.parametrize("changes", (
    {"use_rth": False}, {"duration": "2 Y"}, {"request_end": NOW - timedelta(seconds=1)},
    {"exchange_timezone": "UTC"}, {"adapter_version": "fixture-next"},
    {"secdef_hash": "d" * 64}, {"con_id": 102}, {"volume_unit": "LOTS"},
))
def test_request_identity_covers_acquisition_semantics(changes):
    assert request(**changes).contract_hash != request().contract_hash


@pytest.mark.parametrize("changes", (
    {"calendar_hash": "d" * 64}, {"methodology_version": "2"},
    {"sampling": "16:00_ET"}, {"interpolation": "LINEAR_TOTAL_VARIANCE"},
))
def test_basis_identity_covers_methodology_not_just_iv_label(changes):
    assert basis(**changes).contract_hash != basis().contract_hash


def test_round_trip_revalidates_hashes_and_preserves_exact_decimals():
    original = batch()
    doc = original.as_dict()
    assert doc["decision_authority"] == "OBSERVATION_ONLY"
    assert doc["status"] == "RESEARCH_ONLY"
    assert doc["request_contract_hash"] == original.request.contract_hash
    assert FeatureHistoryBatch.from_document(doc) == original
    with localcontext() as ctx:
        ctx.prec = 9
        assert batch().batch_hash == original.batch_hash
    doc["points"][0]["close"] = "999"
    with pytest.raises(FeatureContractError, match="FEATURE_BATCH_HASH_MISMATCH"):
        FeatureHistoryBatch.from_document(doc)


@pytest.mark.parametrize("field,value", (("request_contract_hash", "f" * 64),
    ("basis_contract_hash", "f" * 64), ("decision_authority", "EXECUTABLE"),
    ("status", "MODEL_EVALUATED"), ("schema", "unknown.v2")))
def test_payload_cannot_forge_identity_or_promote_authority(field, value):
    doc = batch().as_dict()
    doc[field] = value
    with pytest.raises(FeatureContractError):
        FeatureHistoryBatch.from_document(doc)


@pytest.mark.parametrize("changes", ({"con_id": True}, {"use_rth": 1},
    {"request_end": NOW.replace(tzinfo=None)}, {"secdef_hash": "x"},
    {"end_session": date(2027, 1, 1)}, {"what_to_show": "OPTION_IMPLIED_VOLATILITY"}))
def test_request_rejects_malformed_or_conflicting_semantics(changes):
    with pytest.raises((FeatureContractError, ValueError, TypeError)):
        request(**changes)


@pytest.mark.parametrize("value", (True, 1.2, Decimal("NaN"), Decimal("Infinity"), Decimal(0)))
def test_point_price_is_strict_finite_positive_decimal(value):
    with pytest.raises(FeatureContractError):
        point(close=value)


def test_zero_volume_differs_from_unknown_but_negative_is_invalid():
    assert point(volume=Decimal(0)).volume == 0
    assert point(volume=None).volume is None
    with pytest.raises(FeatureContractError):
        point(volume=Decimal(-1))


def test_batch_rejects_future_duplicate_out_of_range_and_basis_mismatch():
    for changes in (
        {"points": (point(), point())},
        {"available_at": NOW - timedelta(days=4)},
        {"request": request(start_session=date(2026, 9, 5), end_session=date(2026, 9, 5))},
        {"basis": basis(adjustment_basis="SPLIT_ADJUSTED")},
    ):
        with pytest.raises(FeatureContractError):
            batch(**changes)


def test_feature_specific_windows_do_not_require_252_price_bars():
    sessions = tuple(date(2025, 1, 1) + timedelta(days=i) for i in range(252))
    # Synthetic calendar explicitly supplied to the pure selector; no live authority.
    rows = tuple(point(day, session_close_at=datetime.combine(day, datetime.min.time(), timezone.utc)) for day in sessions)
    assert len(select_feature_window(rows[-60:], sessions, feature="EMA20")) == 60
    assert len(select_feature_window(rows[-21:], sessions, feature="BENCHMARK_RETURN20")) == 21
    assert len(select_feature_window(rows, sessions, feature="IV_PERCENTILE")) == 252
    for feature, length, reason in (("EMA20", 59, "FEATURE_EMA_WARMUP_INSUFFICIENT"),
                                    ("IV_PERCENTILE", 251, "IV_HISTORY_INSUFFICIENT")):
        with pytest.raises(FeatureContractError, match=reason):
            select_feature_window(rows[-length:], sessions, feature=feature)
    with pytest.raises(FeatureContractError, match="FEATURE_SESSION_ALIGNMENT_INVALID"):
        select_feature_window(rows[-22:-1], sessions, feature="BENCHMARK_RETURN20")


def test_missing_unknown_and_extra_contract_fields_fail_closed():
    doc = batch().as_dict()
    doc["request_contract"]["unhashed_parameter"] = "unexpected"
    with pytest.raises(FeatureContractError):
        FeatureHistoryBatch.from_document(doc)


def test_legacy_document_cannot_be_upgraded_by_new_label():
    with pytest.raises(FeatureContractError, match="LEGACY_BASIS_UNRESOLVED"):
        FeatureHistoryBatch.from_document({"schema": "old", "points": []})


@pytest.mark.parametrize("value", (Decimal("1e999999999"), Decimal("1e-999999999"), Decimal("1." + "1" * 80)))
def test_extreme_decimal_rejected_before_fixed_point_serialization(value):
    with pytest.raises(FeatureContractError, match="FEATURE_PRICE_INVALID"):
        point(close=value)


def test_unknown_timezone_is_a_structured_contract_failure():
    with pytest.raises(FeatureContractError, match="FEATURE_REQUEST_TIMEZONE_INVALID"):
        request(exchange_timezone="Invalid/Missing")


def test_empty_iv_basis_is_not_a_comparable_basis():
    with pytest.raises(FeatureContractError, match="FEATURE_SERIES_HORIZON_INVALID"):
        basis(series_kind="IV", adjustment_basis="NOT_APPLICABLE", value_unit="ANNUALIZED_FRACTION")
