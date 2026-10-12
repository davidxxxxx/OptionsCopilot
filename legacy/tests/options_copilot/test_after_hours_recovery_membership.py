"""Strict unordered membership checks for append-only after-hours recovery."""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from contextlib import closing
from dataclasses import fields
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

import options_copilot.runtime as runtime_module
from options_copilot.config import OptionsCopilotConfig
from options_copilot.market.session_calendar import UsOptionsSessionCalendar
from options_copilot.runtime import OptionsCopilotRuntime, RuntimeServices
from options_copilot.storage.canonical import canonical_hash


CHECKED_AT = datetime(2026, 8, 4, 20, 40, 30, tzinfo=timezone.utc)
OBSERVED_AT = CHECKED_AT - timedelta(minutes=1)
SYMBOLS = ("SPY", "XLF", "TLT")


def _candidate(rank: int, symbol: str) -> dict[str, object]:
    basis = {
        "schema": "options_copilot.indicative_underlying_quote_basis.v1",
        "symbol": symbol,
        "contract_id": 30_000 + rank,
        "exchange": "SMART",
        "source": "IBKR_AFTER_HOURS_UNDERLYING_READONLY",
        "observed_at": OBSERVED_AT,
        "bid": Decimal("99"),
        "ask": Decimal("101"),
        "last": Decimal("100"),
        "close": Decimal("100"),
        "market_data_type": 4,
        "decision_authority": "SUPPORTING_ONLY",
    }
    return {
        "research_id": f"recovery.{symbol.lower()}",
        "rank": rank,
        "underlying": symbol,
        "sector": symbol,
        "source_scan": "MOST_ACTIVE" if symbol == "TLT" else "CORE_UNIVERSE",
        "underlying_quote_basis": {
            **basis,
            "observed_at": OBSERVED_AT.isoformat(),
            "bid": "99",
            "ask": "101",
            "last": "100",
            "close": "100",
        },
        "underlying_quote_basis_hash": canonical_hash(basis),
        "legs": tuple(
            {
                "side": side,
                "contract_id": 40_000 + (rank * 2) + offset,
                "contract_id_ex": f"{40_000 + (rank * 2) + offset}@SMART",
                "local_symbol": f"{symbol}  260904C{strike * 1000:08d}",
                "expiration": "2026-09-04",
                "strike": str(strike),
                "right": "C",
                "exchange": "SMART",
                "trading_class": symbol,
                "multiplier": 100,
            }
            for offset, (side, strike) in enumerate((("BUY", 100), ("SELL", 101)))
        ),
    }


def _forbidden(*_args: object, **_kwargs: object) -> None:
    pytest.fail("recovery must reuse the verified append-only materialization")


def _row_counts(runtime: OptionsCopilotRuntime) -> tuple[int, ...]:
    counts: list[int] = []
    for store, tables in (
        (runtime.equity_pool_store, ("equity_pool_snapshots", "equity_pool_rows")),
        (runtime.option_pool_store, ("option_structure_pools",)),
    ):
        with closing(
            sqlite3.connect(f"{store.path.as_uri()}?mode=ro", uri=True)
        ) as connection:
            for table in tables:
                counts.append(
                    connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                )
    return tuple(counts)


@pytest.fixture
def materialized(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[tuple[OptionsCopilotRuntime, dict[str, object], Mapping[str, object]]]:
    monkeypatch.setattr(runtime_module, "build_production_composition", _forbidden)
    injected = RuntimeServices(
        **{field.name: None for field in fields(RuntimeServices)}
    )
    runtime = OptionsCopilotRuntime(
        OptionsCopilotConfig(data_dir=tmp_path / "data", log_dir=tmp_path / "logs"),
        runtime_services=injected,
    )
    after_hours = {
        "schema": "options_copilot.after_hours_indicative.v1",
        "status": "DEGRADED",
        "observed_at": OBSERVED_AT.isoformat(),
        "priced_count": 1,
        "requested_count": 3,
        "reason_codes": ["AFTER_HOURS_INDICATIVE_PARTIAL"],
        "candidates": [
            _candidate(rank, symbol) for rank, symbol in enumerate(SYMBOLS, 1)
        ],
        "campaign": {
            "completed_underlyings": 3,
            "target_underlyings": 10,
            "remaining_underlyings": 7,
            "continue_after_pacing_window": True,
        },
        "pacing_usage": {
            "schema": "options_copilot.after_hours_pacing_usage.v1",
            "status": "APPROVED",
            "capability_hash": "a" * 64,
            "authority": "READ_ONLY_MARKET_DATA",
        },
    }
    calendar = UsOptionsSessionCalendar().normalize(
        liquid_hours="20260804:0930-1600;20260805:0930-1600",
        trading_hours="20260804:0930-1600;20260805:0930-1600",
        timezone_id="America/New_York",
        observed_at=CHECKED_AT,
        source="IBKR_REQ_CONTRACT_DETAILS_READONLY",
        now=CHECKED_AT,
    )
    try:
        runtime._after_hours_indicative_best = after_hours
        runtime._after_hours_store.write(after_hours)
        prepared = runtime.next_session_preparation(calendar, CHECKED_AT, CHECKED_AT)
        verified = runtime.verified_after_hours_payload()
        assert "formal_research_pools" in verified
        monkeypatch.setattr(runtime.equity_pool, "build", _forbidden)
        monkeypatch.setattr(runtime.option_pool, "capture_research_candidates", _forbidden)
        monkeypatch.setattr(runtime.equity_pool_store, "append", _forbidden)
        monkeypatch.setattr(runtime.option_pool_store, "append", _forbidden)
        monkeypatch.setattr(runtime_module, "captured_records_from_quotes", _forbidden)
        yield runtime, verified, prepared
    finally:
        runtime.close()


def _recover(
    runtime: OptionsCopilotRuntime,
    verified: Mapping[str, object],
    symbols: Sequence[str],
) -> dict[str, object] | None:
    formal = verified["formal_research_pools"]
    assert isinstance(formal, Mapping)
    option = runtime.option_pool.store.latest()
    assert option is not None
    return runtime._recover_after_hours_formal_pools(
        campaign_hash=formal["campaign_hash"],
        revision_hash=formal["materialization_revision_hash"],
        campaign_observed_at=OBSERVED_AT,
        materialization_slot=datetime.fromisoformat(formal["materialized_at"]),
        rows=tuple({"symbol": symbol} for symbol in symbols),
        candidate_identity_manifest=tuple(
            row.candidate_identity for row in option.decisions
        ),
        underlying_basis_bound_count=len(SYMBOLS),
        underlying_basis_missing_count=0,
        migration_reasons=(),
    )


def test_source_priority_reorder_recovers_exact_descriptor_without_appends(
    materialized,
) -> None:
    runtime, verified, prepared = materialized
    equity = runtime.equity_pool.latest_payload()
    option = runtime.option_pool.latest_payload()
    reference = equity["equity_pool_reference"]
    assert tuple(reference["discovered_symbols"]) == ("TLT", "SPY", "XLF")
    assert tuple(row["underlying"] for row in verified["candidates"]) == SYMBOLS
    original_reference_hash = canonical_hash(reference)
    original_descriptor_hash = verified["formal_research_pools"]["descriptor_hash"]
    before = _row_counts(runtime)
    assert before == (1, 3, 1)

    replay = dict(verified)
    replay.pop("formal_research_pools")
    replay["reason_codes"] = [
        "AFTER_HOURS_INDICATIVE_PARTIAL",
        "AFTER_HOURS_FORMAL_MATERIALIZATION_CONFLICT",
    ]
    recovered = runtime._materialize_after_hours_research_pools(replay, now=CHECKED_AT)
    assert recovered is not None
    assert recovered["descriptor_hash"] == original_descriptor_hash
    runtime._after_hours_indicative_best = replay
    runtime._after_hours_store.write(replay)
    runtime._restore_after_hours_formal_pools()
    restored = runtime._after_hours_store.read()
    assert restored is not None
    formal = restored["formal_research_pools"]
    assert formal["descriptor_hash"] == original_descriptor_hash
    assert formal["equity_pool_reference_hash"] == original_reference_hash
    restored_reference = runtime.equity_pool.latest_payload()["equity_pool_reference"]
    assert canonical_hash(restored_reference) == original_reference_hash
    assert _row_counts(runtime) == before
    assert "AFTER_HOURS_FORMAL_MATERIALIZATION_CONFLICT" not in restored["reason_codes"]
    assert prepared["executable_count"] == 0
    assert option["research_only_count"] == 3
    assert option["exact_count"] == 0
    for payload in (prepared, recovered, formal):
        assert payload["decision_authority"] == "SUPPORTING_ONLY"
        assert payload["approval_eligible"] is False
        assert payload["instruction_creation_allowed"] is False
        assert payload["order_allowed"] is False
    assert formal["decision"] == "NO_TRADE"


@pytest.mark.parametrize(
    "symbols",
    (
        ("SPY", "SPY", "TLT"),
        ("SPY", "TLT"),
        ("SPY", "XLF", "TLT", "IWM"),
        ("SPY", "XLF", "IWM"),
        ("SPY", "XLF", ""),
        ("SPY", "XLF", "  "),
        ("spy", " SPY ", "TLT"),
    ),
    ids=(
        "duplicate", "missing", "additional", "substituted", "blank", "whitespace",
        "normalized-duplicate",
    ),
)
def test_recovery_rejects_invalid_expected_membership(materialized, symbols) -> None:
    runtime, verified, _prepared = materialized
    before = _row_counts(runtime)
    assert _recover(runtime, verified, symbols) is None
    assert _row_counts(runtime) == before


def _override_reference(
    runtime: OptionsCopilotRuntime,
    monkeypatch: pytest.MonkeyPatch,
    symbols: object,
) -> None:
    # Simulate an invalid reference at this guard without altering test ledgers.
    # The end-to-end case above separately exercises both real chain validators.
    payload = runtime.equity_pool.latest_payload()
    reference = {**payload["equity_pool_reference"], "discovered_symbols": symbols}
    option = runtime.option_pool.store.latest()
    assert option is not None
    decisions = tuple(
        SimpleNamespace(
            candidate_identity=row.candidate_identity,
            equity_pool_reference=reference,
            equity_thesis_evidence=row.equity_thesis_evidence,
            disposition=row.disposition,
            reason_codes=row.reason_codes,
            exact_economics=row.exact_economics,
        )
        for row in option.decisions
    )
    snapshot = SimpleNamespace(
        observed_at=option.observed_at,
        scan_run_id=option.scan_run_id,
        snapshot_hash=option.snapshot_hash,
        decisions=decisions,
    )
    monkeypatch.setattr(
        runtime.equity_pool,
        "latest_payload",
        lambda: {**payload, "equity_pool_reference": reference},
    )
    monkeypatch.setattr(runtime.option_pool.store, "latest", lambda: snapshot)


@pytest.mark.parametrize(
    "symbols",
    (
        ("TLT", "SPY", "SPY"),
        ("TLT", "SPY"),
        ("TLT", "SPY", "XLF", "IWM"),
        ("TLT", "SPY", "IWM"),
        ("TLT", "SPY", ""),
        ("TLT", "SPY", "  "),
        ("TLT", "SPY", ["XLF"]),
        "TLT,SPY,XLF",
        None,
    ),
    ids=(
        "duplicate", "missing", "additional", "substituted", "blank", "whitespace",
        "non-string", "string-container", "null-container",
    ),
)
def test_recovery_rejects_invalid_persisted_membership(
    materialized,
    monkeypatch: pytest.MonkeyPatch,
    symbols,
) -> None:
    runtime, verified, _prepared = materialized
    before = _row_counts(runtime)
    _override_reference(runtime, monkeypatch, symbols)
    assert _recover(runtime, verified, SYMBOLS) is None
    assert _row_counts(runtime) == before


@pytest.mark.parametrize("symbols", (("SPY", "SPY", "TLT"), ("SPY", "", "TLT")))
def test_recovery_rejects_matching_but_invalid_membership_on_both_sides(
    materialized,
    monkeypatch: pytest.MonkeyPatch,
    symbols,
) -> None:
    runtime, verified, _prepared = materialized
    before = _row_counts(runtime)
    _override_reference(runtime, monkeypatch, symbols)
    assert _recover(runtime, verified, symbols) is None
    assert _row_counts(runtime) == before
