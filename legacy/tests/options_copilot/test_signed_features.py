"""Offline preview regressions; fixtures confer no source or policy authority."""
from __future__ import annotations

from dataclasses import asdict, replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, localcontext

import pytest

from options_copilot.analytics.scenarios import InitialPolicyResolver
from options_copilot.analytics.benchmark import (
    BENCHMARK_CONFIRMATION_OBSERVED_AT,
    benchmark_convention,
)
from options_copilot.analytics.ema20 import EMA20_CONFIRMATION_OBSERVED_AT, ema20_convention
from options_copilot.analytics.feature_contracts import SeriesBasisContract
from options_copilot.analytics.iv_percentile import (
    IV_PERCENTILE_CONFIRMATION_OBSERVED_AT,
    iv_percentile_convention,
)
from options_copilot.analytics.signed_features import (
    DailyFeatureBar, FeatureSurfacePoint, FeatureUnavailable,
    SECTOR_BENCHMARKS, build_market_feature_preview, build_signed_market_features,
)
from options_copilot.storage.canonical import canonical_hash


NOW = datetime(2026, 9, 8, 14, tzinfo=timezone.utc)
NEAR, NEXT = date(2026, 9, 18), date(2026, 10, 16)


def _fixture():
    days = []
    day = date(2026, 9, 4)
    while len(days) < 252:
        if day.weekday() < 5:
            days.append(day)
        day -= timedelta(days=1)
    days = tuple(reversed(days))  # Explicit test calendar, not live exchange authority.
    prices = tuple(DailyFeatureBar(day, Decimal(100)+Decimal(i)/100+Decimal(i % 3)/10, Decimal(1000+i)) for i, day in enumerate(days))
    benchmark = tuple(DailyFeatureBar(day, Decimal(100)+Decimal(i)/200, Decimal(2000)) for i, day in enumerate(days))
    iv_history = tuple((day, Decimal("0.2")+Decimal(i)/10000) for i, day in enumerate(days))
    rows = ((NEAR, "C", "90", ".6", ".25"), (NEAR, "C", "110", ".2", ".30"),
            (NEAR, "P", "85", "-.15", ".35"), (NEAR, "P", "95", "-.35", ".30"),
            (NEXT, "C", "90", ".65", ".28"), (NEXT, "C", "110", ".3", ".32"))
    surface = tuple(FeatureSurfacePoint(index+1, "XOM", expiry, Decimal(strike), right, Decimal(iv), Decimal(delta), NOW, NOW, Decimal("1"), Decimal("1.10"), 1, "a"*64) for index, (expiry, right, strike, delta, iv) in enumerate(rows))
    prices = prices[-60:]
    benchmark = tuple(replace(row, volume=None) for row in benchmark[-21:])
    calendar = {
        "schema": "options_copilot.feature_calendar_preview.v1", "source": "TEST_ONLY",
        "available_at": NOW-timedelta(days=10),
        "sessions": tuple({"session_date": day, "close_at": datetime(day.year, day.month, day.day, 20, tzinfo=timezone.utc)} for day in days),
        "regular_expirations": (NEAR, NEXT),
    }
    iv_basis = SeriesBasisContract(series_kind="IV", calendar_hash=canonical_hash(calendar),
        methodology_id="TEST_ONLY", methodology_version="1", adjustment_basis="NOT_APPLICABLE",
        value_unit="ANNUALIZED_FRACTION", sampling="CLOSE", horizon="CONSTANT_TENOR",
        tenor_days=30, rolling_rule="TEST_ONLY", moneyness="ATM", option_side="CALL_PUT", interpolation="LINEAR")
    source = {
        "source": "IBKR", "price_basis": "ADJUSTED_LAST", "iv_basis": "OPTION_IMPLIED_VOLATILITY",
        "calendar": calendar, "calendar_hash": canonical_hash(calendar),
        "price_hash": canonical_hash(tuple(asdict(row) for row in prices)),
        "benchmark_hash": canonical_hash(tuple(asdict(row) for row in benchmark)),
        "iv_history_hash": canonical_hash(iv_history),
        "surface_hash": canonical_hash(tuple(asdict(row) for row in surface)),
        "spot_hash": canonical_hash({"symbol": "XOM", "spot": Decimal(100), "observed_at": NOW}),
        "iv_basis_hash": iv_basis.contract_hash,
        "percentile_current_hash": canonical_hash({"value": Decimal(".3"), "observed_at": NOW, "basis_contract_hash": iv_basis.contract_hash}),
    }
    return dict(symbol="XOM", sector="ENERGY", benchmark_symbol="XLE", prices=prices,
                benchmark=benchmark, iv_history=iv_history, surface=surface, spot=Decimal(100),
                spot_observed_at=NOW, expiration=NEAR, next_expiration=NEXT, cutoff=NOW,
                policy=InitialPolicyResolver().resolve(now=NOW), source_manifest=source,
                percentile_current_iv=Decimal(".3"), percentile_observed_at=NOW,
                percentile_basis=iv_basis, iv_history_basis=iv_basis)


def _move_fixture_to(data, cutoff):
    data["cutoff"] = cutoff
    data["spot_observed_at"] = cutoff
    data["percentile_observed_at"] = cutoff
    data["surface"] = tuple(
        replace(point, observed_at=cutoff, exchange_time=cutoff)
        for point in data["surface"]
    )
    data["source_manifest"]["surface_hash"] = canonical_hash(
        tuple(asdict(row) for row in data["surface"])
    )
    data["source_manifest"]["spot_hash"] = canonical_hash({
        "symbol": data["symbol"],
        "spot": data["spot"],
        "observed_at": data["spot_observed_at"],
    })
    data["source_manifest"]["percentile_current_hash"] = canonical_hash({
        "value": data["percentile_current_iv"],
        "observed_at": data["percentile_observed_at"],
        "basis_contract_hash": data["percentile_basis"].contract_hash,
    })
    return data


def _qqq_fixture(cutoff=BENCHMARK_CONFIRMATION_OBSERVED_AT):
    data = _move_fixture_to(_fixture(), cutoff)
    data["symbol"] = "QQQ"
    data["sector"] = "INFORMATION_TECHNOLOGY"
    data["benchmark_symbol"] = "SPY"
    data["surface"] = tuple(
        replace(point, symbol="QQQ") for point in data["surface"]
    )
    data["source_manifest"]["surface_hash"] = canonical_hash(
        tuple(asdict(row) for row in data["surface"])
    )
    data["source_manifest"]["spot_hash"] = canonical_hash({
        "symbol": "QQQ",
        "spot": data["spot"],
        "observed_at": data["spot_observed_at"],
    })
    return data


def _symbol_fixture(symbol, sector, benchmark_symbol, cutoff):
    data = _move_fixture_to(_fixture(), cutoff)
    data["symbol"] = symbol
    data["sector"] = sector
    data["benchmark_symbol"] = benchmark_symbol
    data["surface"] = tuple(
        replace(point, symbol=symbol) for point in data["surface"]
    )
    data["source_manifest"]["surface_hash"] = canonical_hash(
        tuple(asdict(row) for row in data["surface"])
    )
    data["source_manifest"]["spot_hash"] = canonical_hash({
        "symbol": symbol,
        "spot": data["spot"],
        "observed_at": data["spot_observed_at"],
    })
    return data


def test_preview_produces_bound_decimal_features_without_authority():
    data = _fixture()
    result = build_market_feature_preview(**data)
    assert result.atm_iv == Decimal("0.275")
    assert -1 <= result.market_score <= 1
    assert -1 <= result.volatility_score <= 1
    assert result.manifest_hash == canonical_hash(result.manifest)
    assert result.manifest_hash == "fe318dbb4a8da53292855070389ac4d6a70a9ca04562afa45af33ae89bef3ba0"
    assert result.manifest["feature_status"] == "RESEARCH_ONLY"
    assert result.manifest["decision_authority"] == "SUPPORTING_ONLY"
    assert result.manifest["model_input_complete"] is False
    assert result.manifest["production_eligible"] is False
    assert result.manifest["affects_eligibility"] is False
    assert result.manifest["unresolved_decisions"] == ("D1_EMA_SEMANTICS", "D2_IV_BASIS")
    assert result.manifest["calendar_authority_status"] == "UNRESOLVED"
    assert result.manifest["input_mode"] == "OFFLINE_UNVERIFIED"
    assert result.manifest["point_in_time_verified"] is False
    assert result.manifest["acquisition_contracts_verified"] is False
    assert "policy_hash" not in result.manifest
    features = result.manifest["features"]
    assert features["iv_percentile"] == Decimal(1)
    assert features["term_structure"] == (Decimal(".275")-Decimal(".30"))/Decimal(".30")
    assert features["skew_tail_pressure"] == (Decimal(".325")-Decimal(".29375"))/Decimal(".275")
    assert result.market_score == sum(features[name]*weight for name, weight in (
        ("trend_20d", Decimal(".35")), ("momentum_5d", Decimal(".25")),
        ("sector_relative_strength_20d", Decimal(".20")), ("price_volume_confirmation", Decimal(".20"))))
    with localcontext() as ctx:
        ctx.prec = 9
        repeated = build_market_feature_preview(**data)
    assert repeated == result


def test_post_confirmation_preview_versions_only_ema_semantics_metadata():
    data = _fixture()
    data["cutoff"] = EMA20_CONFIRMATION_OBSERVED_AT
    data["spot_observed_at"] = EMA20_CONFIRMATION_OBSERVED_AT
    data["percentile_observed_at"] = EMA20_CONFIRMATION_OBSERVED_AT
    data["surface"] = tuple(
        replace(
            point,
            observed_at=EMA20_CONFIRMATION_OBSERVED_AT,
            exchange_time=EMA20_CONFIRMATION_OBSERVED_AT,
        )
        for point in data["surface"]
    )
    data["source_manifest"]["surface_hash"] = canonical_hash(
        tuple(asdict(row) for row in data["surface"])
    )
    data["source_manifest"]["spot_hash"] = canonical_hash(
        {
            "symbol": data["symbol"],
            "spot": data["spot"],
            "observed_at": data["spot_observed_at"],
        }
    )
    data["source_manifest"]["percentile_current_hash"] = canonical_hash(
        {
            "value": data["percentile_current_iv"],
            "observed_at": data["percentile_observed_at"],
            "basis_contract_hash": data["percentile_basis"].contract_hash,
        }
    )

    result = build_market_feature_preview(**data)

    assert result.manifest["schema"] == "options_copilot.market_feature_preview.v3"
    assert result.manifest["formula_version"] == "offline-market-features.ema60.v3"
    assert result.manifest["unresolved_decisions"] == ("D2_IV_BASIS",)
    assert result.manifest["ema20_convention"] == ema20_convention(data["cutoff"])
    for field in (
        "point_in_time_verified",
        "acquisition_contracts_verified",
        "model_input_complete",
        "production_eligible",
        "affects_eligibility",
    ):
        assert result.manifest[field] is False
    assert result.manifest["decision_authority"] == "SUPPORTING_ONLY"
    assert result.manifest_hash == "578b278c8270d179b11c875cd5b3106f04f2866e4cc1395c6c07040d6a615e62"


def test_post_iv_confirmation_preview_selects_but_does_not_apply_native_convention():
    data = _move_fixture_to(_fixture(), IV_PERCENTILE_CONFIRMATION_OBSERVED_AT)

    result = build_market_feature_preview(**data)

    assert result.manifest["schema"] == "options_copilot.market_feature_preview.v4"
    assert result.manifest["formula_version"] == "offline-market-features.ema60.iv252.v4"
    assert result.manifest["unresolved_decisions"] == ()
    assert result.manifest["ema20_convention"] == ema20_convention(data["cutoff"])
    assert result.manifest["iv_percentile_convention"] == iv_percentile_convention(data["cutoff"])
    assert result.manifest["convention_application_status"] == "NOT_APPLIED"
    assert result.manifest["iv_native_comparability_verified"] is False
    assert result.manifest["source_authority_status"] == "UNRESOLVED"
    assert result.manifest["policy_compatibility"] == "UNRESOLVED"
    for field in (
        "point_in_time_verified",
        "acquisition_contracts_verified",
        "model_input_complete",
        "production_eligible",
        "affects_eligibility",
    ):
        assert result.manifest[field] is False
    assert result.manifest["decision_authority"] == "SUPPORTING_ONLY"
    assert result.manifest_hash == "3c98e987db0b623e6fb9682445a006db93d80616473a8afa9f6c75d0e751b3d4"


def test_post_benchmark_confirmation_qqq_preview_binds_spy_mapping_only():
    data = _qqq_fixture()

    result = build_market_feature_preview(**data)

    convention = benchmark_convention("QQQ", data["cutoff"])
    assert result.manifest["schema"] == "options_copilot.market_feature_preview.v5"
    assert result.manifest["formula_version"] == "offline-market-features.ema60.iv252.qqq-spy.v5"
    assert result.manifest["benchmark_symbol"] == "SPY"
    assert result.manifest["benchmark_convention"] == convention
    assert result.manifest["benchmark_mapping_hash"] == canonical_hash({
        "symbol": "QQQ",
        "benchmark_symbol": "SPY",
        "benchmark_convention": convention,
    })
    assert result.manifest_hash == canonical_hash(result.manifest)
    assert result.manifest["decision_authority"] == "SUPPORTING_ONLY"
    assert result.manifest["production_eligible"] is False
    assert result.manifest["model_input_complete"] is False
    assert result.manifest["affects_eligibility"] is False


@pytest.mark.parametrize("benchmark_symbol", ("QQQ", "XLK"))
def test_post_confirmation_qqq_rejects_self_or_wrong_benchmark(benchmark_symbol):
    data = _qqq_fixture()
    data["benchmark_symbol"] = benchmark_symbol

    with pytest.raises(FeatureUnavailable, match="^SECTOR_BENCHMARK_MISMATCH$"):
        build_market_feature_preview(**data)


@pytest.mark.parametrize("sector", ("UNKNOWN", "ETF", "A", "A" * 64))
def test_post_confirmation_qqq_accepts_bounded_sector_label_without_taxonomy_inference(sector):
    data = _qqq_fixture()
    data["sector"] = sector

    result = build_market_feature_preview(**data)

    assert result.manifest["schema"] == "options_copilot.market_feature_preview.v5"
    assert result.manifest["sector"] == sector
    assert result.manifest["benchmark_symbol"] == "SPY"


@pytest.mark.parametrize(
    "sector",
    (
        None,
        1,
        "",
        " ",
        "unknown",
        " UNKNOWN",
        "!",
        "ETF/UNKNOWN",
        "ETF\nUNKNOWN",
        "1ETF",
        "_ETF",
        "A１",
        "A" * 65,
    ),
)
def test_post_confirmation_qqq_rejects_malformed_sector_labels(sector):
    data = _qqq_fixture()
    data["sector"] = sector

    with pytest.raises(FeatureUnavailable, match="^SECTOR_BENCHMARK_UNDEFINED$"):
        build_market_feature_preview(**data)


def test_post_confirmation_other_etf_keeps_existing_v4_sector_mapping():
    data = _symbol_fixture(
        "SPY",
        "INFORMATION_TECHNOLOGY",
        "XLK",
        BENCHMARK_CONFIRMATION_OBSERVED_AT,
    )

    result = build_market_feature_preview(**data)

    assert result.manifest["schema"] == "options_copilot.market_feature_preview.v4"
    assert result.manifest["formula_version"] == "offline-market-features.ema60.iv252.v4"
    assert result.manifest["benchmark_symbol"] == "XLK"
    assert "benchmark_convention" not in result.manifest
    assert result.manifest["benchmark_mapping_hash"] == canonical_hash(
        dict(SECTOR_BENCHMARKS)
    )


def test_pre_boundary_qqq_keeps_v4_sector_mapping_and_hash_behavior():
    data = _symbol_fixture(
        "QQQ",
        "INFORMATION_TECHNOLOGY",
        "XLK",
        BENCHMARK_CONFIRMATION_OBSERVED_AT - timedelta(microseconds=1),
    )

    result = build_market_feature_preview(**data)

    assert result.manifest["schema"] == "options_copilot.market_feature_preview.v4"
    assert result.manifest["formula_version"] == "offline-market-features.ema60.iv252.v4"
    assert result.manifest["benchmark_symbol"] == "XLK"
    assert "benchmark_convention" not in result.manifest
    assert result.manifest["benchmark_mapping_hash"] == canonical_hash(
        dict(SECTOR_BENCHMARKS)
    )


def test_qqq_relative_strength_numerically_depends_on_spy_history():
    baseline_data = _qqq_fixture()
    baseline = build_market_feature_preview(**baseline_data)
    changed_data = _qqq_fixture()
    changed_data["benchmark"] = (
        *changed_data["benchmark"][:-1],
        replace(
            changed_data["benchmark"][-1],
            close=changed_data["benchmark"][-1].close * Decimal("1.10"),
        ),
    )
    changed_data["source_manifest"]["benchmark_hash"] = canonical_hash(
        tuple(asdict(row) for row in changed_data["benchmark"])
    )

    changed = build_market_feature_preview(**changed_data)

    assert changed.manifest["features"]["sector_relative_strength_20d"] != (
        baseline.manifest["features"]["sector_relative_strength_20d"]
    )
    assert changed.market_score != baseline.market_score


def test_post_iv_confirmation_cdf_uses_current_underlying_iv_not_surface_atm():
    data = _move_fixture_to(_fixture(), IV_PERCENTILE_CONFIRMATION_OBSERVED_AT)
    data["percentile_current_iv"] = Decimal(".2125")
    data["source_manifest"]["percentile_current_hash"] = canonical_hash({
        "value": data["percentile_current_iv"],
        "observed_at": data["percentile_observed_at"],
        "basis_contract_hash": data["percentile_basis"].contract_hash,
    })

    result = build_market_feature_preview(**data)

    assert result.atm_iv == Decimal(".275")
    assert result.manifest["features"]["iv_percentile"] == Decimal(0)
    assert result.manifest["percentile_current_iv"] == Decimal(".2125")


def test_equal_unresolved_native_contracts_remain_unusable_after_confirmation():
    data = _move_fixture_to(_fixture(), IV_PERCENTILE_CONFIRMATION_OBSERVED_AT)
    unresolved = replace(
        data["iv_history_basis"],
        horizon="PROVIDER_NATIVE_UNRESOLVED",
    )
    data["iv_history_basis"] = unresolved
    data["percentile_basis"] = unresolved

    with pytest.raises(FeatureUnavailable, match="^IV_BASIS_UNRESOLVED$"):
        build_market_feature_preview(**data)


def test_duplicate_iv_session_is_not_a_252_session_history():
    data = _move_fixture_to(_fixture(), IV_PERCENTILE_CONFIRMATION_OBSERVED_AT)
    data["iv_history"] = (
        (data["iv_history"][1][0], data["iv_history"][0][1]),
        *data["iv_history"][1:],
    )

    with pytest.raises(FeatureUnavailable, match="^FEATURE_IV_SESSIONS_MISMATCH$"):
        build_market_feature_preview(**data)


def test_current_session_iv_history_row_is_not_prior_completed_history():
    data = _move_fixture_to(_fixture(), IV_PERCENTILE_CONFIRMATION_OBSERVED_AT)
    calendar = data["source_manifest"]["calendar"]
    calendar["sessions"] = (
        *calendar["sessions"][:-1],
        {**calendar["sessions"][-1], "close_at": data["cutoff"]},
    )
    calendar_hash = canonical_hash(calendar)
    data["source_manifest"]["calendar_hash"] = calendar_hash
    data["iv_history_basis"] = replace(
        data["iv_history_basis"],
        calendar_hash=calendar_hash,
    )
    data["percentile_basis"] = data["iv_history_basis"]
    data["source_manifest"]["iv_basis_hash"] = data[
        "iv_history_basis"
    ].contract_hash
    data["source_manifest"]["percentile_current_hash"] = canonical_hash({
        "value": data["percentile_current_iv"],
        "observed_at": data["percentile_observed_at"],
        "basis_contract_hash": data["percentile_basis"].contract_hash,
    })

    with pytest.raises(
        FeatureUnavailable,
        match="^FEATURE_SESSION_CALENDAR_INVALID$",
    ):
        build_market_feature_preview(**data)


@pytest.mark.parametrize("field,reason", (
    ("prices", "FEATURE_EMA_WARMUP_INSUFFICIENT"),
    ("benchmark", "FEATURE_BENCHMARK_HISTORY_INSUFFICIENT"),
    ("iv_history", "IV_HISTORY_INSUFFICIENT"),
))
def test_short_histories_are_not_imputed(field, reason):
    data = _fixture()
    data[field] = data[field][:-1]
    with pytest.raises(FeatureUnavailable, match=f"^{reason}$"):
        build_market_feature_preview(**data)


@pytest.mark.parametrize("field,value,reason", (
    ("sector", "COMMODITY_ETF", "SECTOR_BENCHMARK_UNDEFINED"),
    ("benchmark_symbol", "SPY", "SECTOR_BENCHMARK_MISMATCH"),
    ("spot", 100.0, "FEATURE_SPOT_INVALID"),
    ("spot_observed_at", NOW-timedelta(seconds=6), "FEATURE_QUOTE_STALE_OR_FUTURE"),
    ("spot_observed_at", NOW+timedelta(seconds=1), "FEATURE_QUOTE_STALE_OR_FUTURE"),
    ("next_expiration", NEAR, "FEATURE_REGULAR_EXPIRATION_INVALID"),
))
def test_invalid_or_unsupported_inputs_are_exactly_rejected(field, value, reason):
    data = _fixture()
    data[field] = value
    with pytest.raises(FeatureUnavailable, match=f"^{reason}$"):
        build_market_feature_preview(**data)


def test_tampered_raw_price_cannot_reuse_source_hash():
    data = _fixture()
    data["prices"] = (*data["prices"][:-1], replace(data["prices"][-1], close=Decimal(900)))
    with pytest.raises(FeatureUnavailable, match="^FEATURE_SOURCE_HASH_MISMATCH$"):
        build_market_feature_preview(**data)


def test_surface_requires_real_brackets_not_nearest_leg_iv():
    data = _fixture()
    data["surface"] = tuple(row for row in data["surface"] if row.right == "C")
    data["source_manifest"]["surface_hash"] = canonical_hash(tuple(asdict(row) for row in data["surface"]))
    with pytest.raises(FeatureUnavailable, match="^FEATURE_SURFACE_BRACKET_MISSING$"):
        build_market_feature_preview(**data)


@pytest.mark.parametrize("changes,reason", (
    ({"market_data_type": 2}, "FEATURE_SURFACE_IDENTITY_INVALID"),
    ({"symbol": "AMD"}, "FEATURE_SURFACE_IDENTITY_INVALID"),
    ({"exchange_time": NOW-timedelta(seconds=6)}, "FEATURE_QUOTE_STALE_OR_FUTURE"),
    ({"delta": Decimal("NaN")}, "FEATURE_SURFACE_VALUE_INVALID"),
    ({"delta": Decimal("-.5")}, "FEATURE_SURFACE_DELTA_INVALID"),
))
def test_surface_fields_cannot_bypass_validation(changes, reason):
    data = _fixture()
    data["surface"] = (replace(data["surface"][0], **changes), *data["surface"][1:])
    with pytest.raises(FeatureUnavailable, match=f"^{reason}$"):
        build_market_feature_preview(**data)


def test_old_signed_entry_point_cannot_activate_preview_semantics():
    with pytest.raises(FeatureUnavailable, match="^FEATURE_PRODUCTION_AUTHORITY_UNRESOLVED$"):
        build_signed_market_features(**_fixture())


def test_extra_cached_prices_do_not_change_fixed_ema_window():
    data = _fixture()
    expected = build_market_feature_preview(**data)
    day = data["prices"][0].trading_date-timedelta(days=1)
    data["prices"] = (DailyFeatureBar(day, Decimal(700), None), *data["prices"])
    data["source_manifest"]["price_hash"] = canonical_hash(tuple(asdict(row) for row in data["prices"]))
    actual = build_market_feature_preview(**data)
    assert actual.market_score == expected.market_score
    assert actual.manifest["ema_20d"] == expected.manifest["ema_20d"]
    closes = [row.close for row in data["prices"][-60:]]
    ema = sum(closes[:20])/20
    for close in closes[20:]:
        ema = Decimal(2)/21*close+(1-Decimal(2)/21)*ema
    assert actual.manifest["ema_20d"] == ema


def test_current_percentile_value_not_candidate_atm_controls_cdf():
    data = _fixture()
    data["percentile_current_iv"] = Decimal(".2125")
    data["source_manifest"]["percentile_current_hash"] = canonical_hash({
        "value": data["percentile_current_iv"], "observed_at": NOW,
        "basis_contract_hash": data["percentile_basis"].contract_hash})
    result = build_market_feature_preview(**data)
    assert result.atm_iv == Decimal(".275")
    assert result.manifest["features"]["iv_percentile"] == Decimal(0)  # 126 of 252, including tie.


def test_matching_basis_label_does_not_override_different_contract():
    data = _fixture()
    data["percentile_basis"] = replace(data["percentile_basis"], tenor_days=60)
    with pytest.raises(FeatureUnavailable, match="^IV_BASIS_MISMATCH$"):
        build_market_feature_preview(**data)


def test_bare_calendar_digest_is_not_calendar_content():
    data = _fixture()
    del data["source_manifest"]["calendar"]
    with pytest.raises(FeatureUnavailable, match="^FEATURE_SESSION_CALENDAR_INCOMPLETE$"):
        build_market_feature_preview(**data)


def test_tampered_calendar_rejected_even_with_valid_digest_shape():
    data = _fixture()
    data["source_manifest"]["calendar"]["regular_expirations"] = (NEAR, date(2026, 10, 23))
    with pytest.raises(FeatureUnavailable, match="^FEATURE_CALENDAR_HASH_MISMATCH$"):
        build_market_feature_preview(**data)


@pytest.mark.parametrize("volume,reason", ((None, "FEATURE_VOLUME_INVALID"), (Decimal(-1), "FEATURE_VOLUME_INVALID")))
def test_missing_and_negative_volume_are_not_zero(volume, reason):
    data = _fixture()
    data["prices"] = (*data["prices"][:-1], replace(data["prices"][-1], volume=volume))
    with pytest.raises(FeatureUnavailable, match=f"^{reason}$"):
        build_market_feature_preview(**data)


def test_legitimate_zero_volume_is_preserved_but_zero_denominator_fails():
    data = _fixture()
    data["prices"] = (*data["prices"][:-1], replace(data["prices"][-1], volume=Decimal(0)))
    data["source_manifest"]["price_hash"] = canonical_hash(tuple(asdict(row) for row in data["prices"]))
    assert build_market_feature_preview(**data).manifest["features"]["price_volume_confirmation"] == Decimal(0)
    data["prices"] = tuple(replace(row, volume=Decimal(0)) for row in data["prices"])
    data["source_manifest"]["price_hash"] = canonical_hash(tuple(asdict(row) for row in data["prices"]))
    with pytest.raises(FeatureUnavailable, match="^FEATURE_VOLUME_DENOMINATOR_ZERO$"):
        build_market_feature_preview(**data)


def test_calendar_shifted_regular_expiry_is_previewed_not_hardcoded_friday():
    data = _fixture()
    shifted = date(2026, 9, 17)  # Synthetic exchange exception, not an assertion about the real calendar.
    data["expiration"] = shifted
    data["surface"] = tuple(replace(row, expiration=shifted) if row.expiration == NEAR else row for row in data["surface"])
    source = data["source_manifest"]
    source["calendar"]["regular_expirations"] = (shifted, NEXT)
    source["calendar_hash"] = canonical_hash(source["calendar"])
    data["iv_history_basis"] = replace(data["iv_history_basis"], calendar_hash=source["calendar_hash"])
    data["percentile_basis"] = data["iv_history_basis"]
    source["iv_basis_hash"] = data["iv_history_basis"].contract_hash
    source["percentile_current_hash"] = canonical_hash({"value": data["percentile_current_iv"], "observed_at": NOW,
        "basis_contract_hash": data["percentile_basis"].contract_hash})
    source["surface_hash"] = canonical_hash(tuple(asdict(row) for row in data["surface"]))
    result = build_market_feature_preview(**data)
    assert result.expiration == shifted
    assert result.manifest["production_eligible"] is False


def test_exchange_time_after_receipt_is_not_a_coherent_quote():
    data = _fixture()
    data["surface"] = (replace(data["surface"][0], observed_at=NOW-timedelta(seconds=1)), *data["surface"][1:])
    with pytest.raises(FeatureUnavailable, match="FEATURE_QUOTE_SOURCE_AFTER_RECEIPT"):
        build_market_feature_preview(**data)


def test_preview_extreme_decimal_cannot_reach_hash_or_formula():
    data = _fixture()
    data["spot"] = Decimal("1e999999999")
    with pytest.raises(FeatureUnavailable, match="FEATURE_SPOT_INVALID"):
        build_market_feature_preview(**data)
