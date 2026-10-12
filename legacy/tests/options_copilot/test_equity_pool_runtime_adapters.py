"""PIT tests for cached production equity-pool adapters."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from options_copilot.equity_pool import (
    FactorEvidence,
    FactorKind,
    FactorStatus,
    read_hash_bound_factor,
    read_runtime_factor,
    read_runtime_liquidity,
)
from options_copilot.storage.canonical import canonical_hash


NOW = datetime(2026, 8, 21, 13, 30, tzinfo=timezone.utc)


def _factor(kind: FactorKind, observed: datetime, signal: str) -> FactorEvidence:
    return FactorEvidence(
        factor=kind, status=FactorStatus.AVAILABLE,
        signed_signal=Decimal(signal), confidence=Decimal("0.8"), horizon="1D",
        observed_at=observed, effective_at=observed,
        valid_until=observed + timedelta(hours=1),
        source_hashes=(canonical_hash({"source": kind.value, "time": observed}),),
        reasons=("HASH_BOUND_CACHE",),
        payload_hash=canonical_hash({"payload": kind.value, "time": observed, "signal": signal}),
    )


def _record(symbol: str, factor: FactorEvidence, *, tampered: bool = False):
    body = factor.as_dict()
    return {
        "symbol": symbol,
        "factor": body,
        "record_hash": "f" * 64 if tampered else canonical_hash({"symbol": symbol, "factor": body}),
    }


def test_hash_bound_reader_matches_kind_and_latest_valid_pit_vintage() -> None:
    rows = (
        _record("QQQ", _factor(FactorKind.REGIME, NOW - timedelta(minutes=20), "0.2")),
        _record("QQQ", _factor(FactorKind.POSITIONING, NOW - timedelta(minutes=10), "-0.2")),
        _record("QQQ", _factor(FactorKind.REGIME, NOW - timedelta(minutes=5), "0.6")),
        _record("QQQ", _factor(FactorKind.REGIME, NOW + timedelta(minutes=1), "1")),
        _record("QQQ", _factor(FactorKind.REGIME, NOW - timedelta(minutes=2), "0.9"), tampered=True),
        _record("QQQ", _factor(FactorKind.REGIME, NOW - timedelta(hours=2), "-1")),
    )
    result = read_hash_bound_factor(
        {"equity_pool_factors": rows}, symbol="QQQ", kind=FactorKind.REGIME, as_of=NOW,
    )
    assert result is not None
    assert result.signed_signal == Decimal("0.6")
    assert result.observed_at == NOW - timedelta(minutes=5)


def test_actual_positioning_shape_and_measured_liquidity_present_or_missing() -> None:
    positioning = {
        "schema_version": "options_copilot.positioning_feed.v1",
        "positioning": ({
            "underlying": "QQQ", "decision_authority": "SUPPORTING_ONLY",
            "data_asof": NOW - timedelta(minutes=2),
            "estimated_net_gex_usd_per_one_percent": Decimal("1000"),
            "gex_usable_contract_rate": Decimal("0.75"),
            "broker_snapshot_hash": "b" * 64,
        },),
    }
    factor = read_runtime_factor(
        positioning, symbol="QQQ", kind=FactorKind.POSITIONING, as_of=NOW,
    )
    assert factor is not None
    assert factor.signed_signal == Decimal("1")
    assert factor.confidence == Decimal("0.75")

    basis = {
        "observed_at": NOW - timedelta(minutes=1),
        "bid": Decimal("499.90"), "ask": Decimal("500.10"),
        "last": Decimal("500"), "close": Decimal("495"),
    }
    payload = {"candidates": ({
        "underlying": "QQQ", "underlying_quote_basis": basis,
        "underlying_quote_basis_hash": canonical_hash(basis),
    },)}
    liquidity = read_runtime_liquidity(payload, symbol="QQQ", as_of=NOW)
    assert liquidity is not None
    assert liquidity.status is FactorStatus.AVAILABLE
    assert liquidity.reasons == ("MEASURED_UNDERLYING_BID_ASK_SPREAD",)
    trend = read_runtime_factor(
        payload,
        symbol="QQQ",
        kind=FactorKind.TREND_VOLATILITY,
        as_of=NOW,
    )
    assert trend is not None
    assert trend.signed_signal == (Decimal("500") - Decimal("495")) / Decimal(
        "495"
    ) * Decimal("20")
    assert trend.confidence == Decimal("1") - Decimal("0.20") / Decimal(
        "500"
    )
    assert read_runtime_liquidity(payload, symbol="SPY", as_of=NOW) is None
    tampered = {"candidates": ({
        "underlying": "QQQ", "underlying_quote_basis": basis,
        "underlying_quote_basis_hash": "f" * 64,
    },)}
    assert read_runtime_liquidity(tampered, symbol="QQQ", as_of=NOW) is None
    future = dict(basis, observed_at=NOW + timedelta(seconds=1))
    assert read_runtime_liquidity({"candidates": ({
        "underlying": "QQQ", "underlying_quote_basis": future,
        "underlying_quote_basis_hash": canonical_hash(future),
    },)}, symbol="QQQ", as_of=NOW) is None
    stale = dict(basis, observed_at=NOW - timedelta(minutes=16))
    assert read_runtime_liquidity({"candidates": ({
        "underlying": "QQQ", "underlying_quote_basis": stale,
        "underlying_quote_basis_hash": canonical_hash(stale),
    },)}, symbol="QQQ", as_of=NOW) is None
