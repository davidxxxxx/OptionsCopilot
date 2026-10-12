"""Durable production cache and bootstrap tests for G035."""

from __future__ import annotations

from dataclasses import MISSING, fields, replace
from contextlib import nullcontext
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
import options_copilot.equity_pool.evidence_cache as evidence_cache_module

from options_copilot.equity_pool import (
    EquityPoolService,
    EquityPoolStore,
    FactorKind,
    FactorStatus,
    UnderlyingEvidenceCache,
    UnderlyingEvidenceCacheCorruption,
    captured_record_from_quote,
    captured_records_from_quotes,
)
from options_copilot.equity_pool.service import _fundamentals_factor
from options_copilot.fundamentals import fundamental_supporting_source_hash
from options_copilot.gateway import UnderlyingQuoteSnapshot, UnderlyingScanResult
from options_copilot.production_runtime import (
    ProductionPipelineInputs,
    _equity_pool_projection,
    _underlying_capture_completed_at,
)
from options_copilot.scanner.pacing import BudgetDecision
from options_copilot.runtime import RuntimeServices
from options_copilot.storage.canonical import canonical_hash, canonical_json, datetime_text


NOW = datetime(2026, 8, 21, 13, 30, tzinfo=timezone.utc)


def _runtime_services(cache: UnderlyingEvidenceCache) -> RuntimeServices:
    values = {}
    for field in fields(RuntimeServices):
        if not field.init or field.default is not MISSING or field.default_factory is not MISSING:
            continue
        values[field.name] = None
    return RuntimeServices(**values, equity_evidence_cache=cache)


def _scanner_rows():
    sectors = ("Energy", "Financials", "Health Care", "Industrials", "Utilities", "Materials")
    return tuple({
        "symbol": f"S{index:03d}", "rank": index, "source_scan": "TOP_PERC_GAIN",
        "contract_id": 1000 + index, "exchange": "NYSE", "industry": sectors[index % len(sectors)],
        "category": "US STOCK", "subcategory": "COMMON",
    } for index in range(30))


def _quote(symbol: str, index: int, observed: datetime = NOW) -> UnderlyingQuoteSnapshot:
    return UnderlyingQuoteSnapshot(
        symbol=symbol, contract_id=1000 + index, exchange="NYSE",
        observed_at=observed, source="IBKR_SNAPSHOT",
        bid=Decimal("109.90"), ask=Decimal("110.10"), last=Decimal("110"),
        close=Decimal("100"), volume=100000, market_data_type=1,
    )


def test_captured_trend_confidence_uses_quote_quality_not_move_magnitude() -> None:
    quote = replace(
        _quote("SPY", 1),
        bid=Decimal("499.95"),
        ask=Decimal("500.05"),
        last=Decimal("500"),
        close=Decimal("497.75"),
    )

    record = captured_record_from_quote(quote, captured_at=NOW)

    expected_signal = (Decimal("500") - Decimal("497.75")) / Decimal(
        "497.75"
    ) * Decimal("20")
    expected_confidence = Decimal("1") - Decimal("0.10") / Decimal("500")
    assert record.trend.signed_signal == expected_signal
    assert record.trend.confidence == expected_confidence
    assert record.trend.confidence > Decimal("0.99")

    incomplete = captured_record_from_quote(
        replace(quote, last=None),
        captured_at=NOW,
    )
    assert incomplete.trend.status is FactorStatus.MISSING
    assert incomplete.trend.signed_signal is None
    assert incomplete.trend.confidence is None


def test_fresh_small_move_thesis_reaches_structure_layer_without_lowering_gate(
    tmp_path,
) -> None:
    cache = UnderlyingEvidenceCache(tmp_path / "small-move-underlying.sqlite3")
    pool_store = EquityPoolStore(tmp_path / "small-move-pool.sqlite3")
    scanner_rows = (
        {
            "symbol": "SPY",
            "rank": 1,
            "source_scan": "MOST_ACTIVE",
            "contract_id": 756733,
            "exchange": "ARCA",
            "industry": "Index",
            "category": "ETF",
            "subcategory": "ETF",
        },
    )
    try:
        service = EquityPoolService(
            pool_store,
            news_reader=lambda: {
                "news": (
                    {
                        "id": "news.spy",
                        "symbols": ("SPY",),
                        "symbol_binding": {
                            "status": "VERIFIED_PROVIDER_RELATED",
                        },
                        "classification": {
                            "direction": "BULLISH",
                            "confidence": 65.0,
                        },
                        "event_impact_score": 65.0,
                        "observed_at": NOW.isoformat(),
                    },
                ),
            },
            factor_readers={
                kind: lambda symbol, as_of, factor_kind=kind: cache.read_factor(
                    symbol,
                    factor_kind,
                    as_of,
                )
                for kind in (
                    FactorKind.REGIME,
                    FactorKind.TREND_VOLATILITY,
                    FactorKind.POSITIONING,
                )
            },
            liquidity_reader=lambda symbol, as_of: cache.read_liquidity(
                symbol,
                as_of,
            ),
            clock=lambda: NOW + timedelta(minutes=1),
        )
        first = service.build(
            scanner_rows=scanner_rows,
            slot=NOW,
            pacing_usage={"scanner": {"used": 1, "limit": 3}},
        )
        assert first.acquisition_targets == ("SPY",)
        cache.append(
            captured_records_from_quotes(
                (
                    replace(
                        _quote("SPY", 1),
                        bid=Decimal("499.95"),
                        ask=Decimal("500.05"),
                        last=Decimal("500"),
                        close=Decimal("497.75"),
                    ),
                ),
                captured_at=NOW,
            )[0]
        )

        refreshed = service.build(
            scanner_rows=scanner_rows,
            slot=NOW + timedelta(minutes=1),
            pacing_usage={"scanner": {"used": 1, "limit": 3}},
        )
        deep_scan_symbols, _, evidence, _, _ = _equity_pool_projection(
            refreshed
        )

        assert refreshed.selected_symbols == ("SPY",)
        assert refreshed.stored.snapshot.selected[0].score.uncertainty <= Decimal(
            "0.55"
        )
        assert deep_scan_symbols == ("SPY",)
        assert evidence is not None
        assert evidence["deep_scan_exclusions"] == ()
    finally:
        pool_store.close()
        cache.close()


def _insert_records_without_reverification(
    cache: UnderlyingEvidenceCache,
    records,
) -> None:
    previous_hash = "0" * 64
    cache._connection.execute("BEGIN IMMEDIATE")
    try:
        for record in records:
            chain_hash = canonical_hash(
                {"previous": previous_hash, "record_hash": record.record_hash}
            )
            cache._connection.execute(
                "INSERT INTO underlying_evidence("
                "symbol, observed_at, valid_until, record_json, record_hash, "
                "previous_chain_hash, chain_hash) VALUES(?,?,?,?,?,?,?)",
                (
                    record.symbol,
                    datetime_text(record.observed_at),
                    datetime_text(record.valid_until),
                    canonical_json(record.as_dict()),
                    record.record_hash,
                    previous_hash,
                    chain_hash,
                ),
            )
            previous_hash = chain_hash
        cache._connection.execute("COMMIT")
    except BaseException:
        cache._connection.execute("ROLLBACK")
        raise


def _authentic_fundamentals_value(
    *,
    as_of: datetime = NOW,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "symbol": "S001",
        "as_of": as_of.isoformat(),
        "record_count": 1,
        "records": ({"content_hash": "c" * 64},),
        "categories": {"EARNINGS": {"status": "AVAILABLE"}},
        "revisions": (
            {
                "metric": "EPS",
                "observed_at": NOW - timedelta(days=1),
                "previous_value": Decimal("1"),
                "delta": Decimal("0.25"),
            },
        ),
        "revision_count": 1,
        "provider_health": {"SEC": {"status": "READY"}},
        "last_refresh_at": NOW.isoformat(),
        "point_in_time_semantics": "FIRST_OBSERVED_AT_OR_BEFORE_CUTOFF",
        "decision_authority": "SUPPORTING_ONLY",
    }
    source_hash = fundamental_supporting_source_hash(payload)
    assert source_hash is not None
    return {"source_hash": source_hash, "payload": payload}


def test_runtime_services_cache_bootstraps_targets_then_later_selects(tmp_path) -> None:
    cache = UnderlyingEvidenceCache(tmp_path / "underlying.sqlite3")
    services = _runtime_services(cache)
    pool_store = EquityPoolStore(tmp_path / "pool.sqlite3")
    try:
        service = EquityPoolService(
            pool_store,
            factor_readers={
                kind: lambda symbol, as_of, factor_kind=kind: services.equity_evidence_cache.read_factor(symbol, factor_kind, as_of)
                for kind in (FactorKind.REGIME, FactorKind.TREND_VOLATILITY, FactorKind.POSITIONING)
            },
            liquidity_reader=lambda symbol, as_of: services.equity_evidence_cache.read_liquidity(symbol, as_of),
            clock=lambda: NOW,
        )
        first = service.build(scanner_rows=_scanner_rows(), slot=NOW, pacing_usage={"scanner": {"used": 3, "limit": 3}})
        assert first.selected_symbols == ()
        assert 0 < len(first.acquisition_targets) <= 30
        assert set(first.acquisition_targets) <= {row["symbol"] for row in _scanner_rows()}

        records = captured_records_from_quotes(
            tuple(_quote(symbol, index) for index, symbol in enumerate(first.acquisition_targets)),
            captured_at=NOW,
        )
        hashes = tuple(cache.append(record) for record in records)
        assert len(hashes) == len(first.acquisition_targets)
        assert cache.replay(hashes[0]).record_hash == hashes[0]

        second_slot = NOW + timedelta(minutes=1)
        second = service.build(scanner_rows=_scanner_rows(), slot=second_slot, pacing_usage={"scanner": {"used": 3, "limit": 3}})
        assert 0 < len(second.selected_symbols) <= 30
        assert second.acquisition_targets == ()
        assert second.stored.snapshot.entry_authority is False
        assert all(row.entry_eligible is False for row in second.stored.snapshot.selected)
        assert second.stored.snapshot.snapshot_hash == pool_store.replay(second.stored.snapshot.pool_id, allocator=service.allocator).snapshot_hash
        assert cache.latest(second.selected_symbols[0], as_of=second_slot) is not None

        with pytest.raises(ValueError, match="future or stale"):
            captured_records_from_quotes((_quote("FUTURE", 99, NOW + timedelta(seconds=1)),), captured_at=NOW)
        assert cache.latest(second.selected_symbols[0], as_of=NOW + timedelta(minutes=16)) is None
        cache._connection.execute("DROP TRIGGER underlying_evidence_no_update")
        cache._connection.execute("UPDATE underlying_evidence SET record_hash=? WHERE sequence=1", ("f" * 64,))
        cache._connection.execute("CREATE TRIGGER underlying_evidence_no_update BEFORE UPDATE ON underlying_evidence BEGIN SELECT RAISE(ABORT, 'underlying_evidence immutable'); END")
        with pytest.raises(UnderlyingEvidenceCacheCorruption, match="record hash mismatch"):
            cache.assert_integrity()
    finally:
        pool_store.close()
        cache.close()


def test_cache_rejects_same_name_noop_immutable_trigger(tmp_path) -> None:
    cache = UnderlyingEvidenceCache(tmp_path / "underlying.sqlite3")
    try:
        cache._connection.execute("DROP TRIGGER underlying_evidence_no_update")
        cache._connection.execute(
            "CREATE TRIGGER underlying_evidence_no_update BEFORE UPDATE ON "
            "underlying_evidence BEGIN SELECT 1; END"
        )
        with pytest.raises(
            UnderlyingEvidenceCacheCorruption,
            match="immutable trigger invalid",
        ):
            cache.assert_integrity()
    finally:
        cache.close()


def test_150_symbol_build_uses_one_verified_bounded_cache_snapshot(
    tmp_path,
    monkeypatch,
) -> None:
    cache = UnderlyingEvidenceCache(tmp_path / "underlying.sqlite3")
    pool_store = EquityPoolStore(tmp_path / "pool.sqlite3")
    integrity_calls = 0
    statements: list[str] = []
    original_integrity = cache.assert_integrity

    def counted_integrity() -> None:
        nonlocal integrity_calls
        integrity_calls += 1
        original_integrity()

    try:
        monkeypatch.setattr(cache, "assert_integrity", counted_integrity)
        monkeypatch.setattr(
            cache,
            "latest",
            lambda *_args, **_kwargs: pytest.fail("per-symbol cache reads are forbidden"),
        )
        monkeypatch.setattr(
            cache,
            "read_factor",
            lambda *_args, **_kwargs: pytest.fail("per-factor cache reads are forbidden"),
        )
        monkeypatch.setattr(
            cache,
            "read_liquidity",
            lambda *_args, **_kwargs: pytest.fail("per-symbol liquidity reads are forbidden"),
        )
        cache._connection.set_trace_callback(statements.append)
        service = EquityPoolService(
            pool_store,
            evidence_batch_reader=cache.snapshot_for,
            clock=lambda: NOW,
        )
        scan_codes = ("MOST_ACTIVE", "TOP_PERC_GAIN", "TOP_PERC_LOSE")
        rows = tuple(
            {
                "symbol": f"S{index:03d}",
                "rank": index % 50,
                "source_scan": scan_codes[index // 50],
                "contract_id": 1000 + index,
                "exchange": "NYSE",
                "industry": "Industrials",
                "category": "US STOCK",
                "subcategory": "COMMON",
            }
            for index in range(150)
        )

        service.build(
            scanner_rows=rows,
            slot=NOW,
            pacing_usage={"scanner": {"used": 3, "limit": 3}},
        )

        bounded_reads = tuple(
            statement
            for statement in statements
            if "FROM UNDERLYING_EVIDENCE" in statement.upper()
            and "SYMBOL IN" in statement.upper()
        )
        assert integrity_calls == 1
        assert len(bounded_reads) == 1
        assert all(f"'S{index:03d}'" in bounded_reads[0] for index in range(150))
    finally:
        cache._connection.set_trace_callback(None)
        pool_store.close()
        cache.close()


def test_snapshot_for_fails_closed_on_tampered_cache(tmp_path) -> None:
    cache = UnderlyingEvidenceCache(tmp_path / "underlying.sqlite3")
    try:
        cache.append(captured_records_from_quotes((_quote("S001", 1),), captured_at=NOW)[0])
        cache._connection.execute("DROP TRIGGER underlying_evidence_no_update")
        cache._connection.execute(
            "UPDATE underlying_evidence SET record_hash=? WHERE sequence=1",
            ("f" * 64,),
        )
        cache._connection.execute(
            "CREATE TRIGGER underlying_evidence_no_update BEFORE UPDATE ON "
            "underlying_evidence BEGIN SELECT RAISE(ABORT, "
            "'underlying_evidence immutable'); END"
        )

        with pytest.raises(
            UnderlyingEvidenceCacheCorruption,
            match="record hash mismatch",
        ):
            cache.snapshot_for(("S001",), NOW)
    finally:
        cache.close()


@pytest.mark.parametrize(
    ("column", "tampered_value"),
    (
        ("symbol", "WRONG"),
        ("observed_at", datetime_text(NOW - timedelta(days=1))),
        ("valid_until", datetime_text(NOW + timedelta(days=1))),
    ),
)
def test_integrity_binds_authority_columns_to_hashed_record(
    tmp_path,
    column,
    tampered_value,
) -> None:
    cache = UnderlyingEvidenceCache(tmp_path / f"authority-{column}.sqlite3")
    try:
        cache.append(captured_records_from_quotes((_quote("S001", 1),), captured_at=NOW)[0])
        cache._connection.execute("DROP TRIGGER underlying_evidence_no_update")
        cache._connection.execute(
            f"UPDATE underlying_evidence SET {column}=? WHERE sequence=1",
            (tampered_value,),
        )
        cache._connection.execute(
            "CREATE TRIGGER underlying_evidence_no_update BEFORE UPDATE ON "
            "underlying_evidence BEGIN SELECT RAISE(ABORT, "
            "'underlying_evidence immutable'); END"
        )

        with pytest.raises(
            UnderlyingEvidenceCacheCorruption,
            match="authority column mismatch",
        ):
            cache.assert_integrity()
    finally:
        cache.close()


def test_snapshot_revalidates_decoded_record_cutoff_after_query(tmp_path, monkeypatch) -> None:
    cache = UnderlyingEvidenceCache(tmp_path / "snapshot-cutoff.sqlite3")
    expired_at = NOW - timedelta(minutes=20)
    record = captured_record_from_quote(
        _quote("S001", 1, expired_at),
        captured_at=expired_at,
    )
    original_connection = cache._connection

    class ForgedCursor:
        def fetchall(self):
            return (
                {
                    "symbol": record.symbol,
                    "observed_at": datetime_text(record.observed_at),
                    "valid_until": datetime_text(record.valid_until),
                    "record_json": canonical_json(record.as_dict()),
                },
            )

    class ForgedConnection:
        def execute(self, statement, parameters=()):
            if "ROW_NUMBER() OVER" in statement:
                return ForgedCursor()
            return original_connection.execute(statement, parameters)

    try:
        monkeypatch.setattr(cache, "assert_integrity", lambda: None)
        cache._connection = ForgedConnection()

        with pytest.raises(
            UnderlyingEvidenceCacheCorruption,
            match="snapshot cutoff mismatch",
        ):
            cache.snapshot_for(("S001",), NOW)
    finally:
        cache._connection = original_connection
        cache.close()


def test_snapshot_sql_decodes_only_latest_valid_revision_per_symbol(
    tmp_path,
    monkeypatch,
) -> None:
    cache = UnderlyingEvidenceCache(tmp_path / "history.sqlite3")
    symbols = tuple(f"S{index:03d}" for index in range(150))
    records = []
    for revision in range(3):
        observed_at = NOW - timedelta(minutes=2 - revision)
        records.extend(
            captured_record_from_quote(
                _quote(symbol, index, observed_at),
                captured_at=observed_at,
            )
            for index, symbol in enumerate(symbols)
        )
    _insert_records_without_reverification(cache, records)
    integrity_calls = 0
    snapshot_decodes = 0
    integrity_complete = False
    original_integrity = cache.assert_integrity
    original_decode = evidence_cache_module._record_from_json

    def counted_integrity() -> None:
        nonlocal integrity_calls, integrity_complete
        integrity_calls += 1
        original_integrity()
        integrity_complete = True

    def counted_decode(value: str):
        nonlocal snapshot_decodes
        if integrity_complete:
            snapshot_decodes += 1
        return original_decode(value)

    try:
        monkeypatch.setattr(cache, "assert_integrity", counted_integrity)
        monkeypatch.setattr(evidence_cache_module, "_record_from_json", counted_decode)

        view = cache.snapshot_for(symbols, NOW)

        assert integrity_calls == 1
        assert len(view.records) == 150
        assert snapshot_decodes == 150
        assert all(record.observed_at == NOW for _symbol, record in view.records)
    finally:
        cache.close()


@pytest.mark.parametrize(
    "source_hash",
    [None, "not-a-digest", "b" * 64],
)
def test_fundamentals_missing_malformed_or_mismatched_source_hash_fails_closed(
    source_hash,
) -> None:
    value = _authentic_fundamentals_value()
    value["source_hash"] = source_hash
    result = _fundamentals_factor(
        value,
        symbol="S001",
        slot=NOW,
    )

    assert result.status is FactorStatus.MISSING
    assert result.reasons == ("FUNDAMENTALS_SOURCE_HASH_INVALID",)
    assert result.source_hashes == ()


def test_fundamentals_valid_source_hash_is_preserved_exactly() -> None:
    value = _authentic_fundamentals_value()
    source_hash = value["source_hash"]
    result = _fundamentals_factor(
        value,
        symbol="S001",
        slot=NOW,
    )

    assert result.status is FactorStatus.AVAILABLE
    assert result.source_hashes[0] == source_hash


@pytest.mark.parametrize(
    "producer_cutoff",
    [NOW - timedelta(seconds=1), NOW + timedelta(seconds=1)],
)
def test_authentic_fundamentals_cutoff_mismatch_never_enters_pool_allocation(
    tmp_path,
    producer_cutoff,
) -> None:
    value = _authentic_fundamentals_value(as_of=producer_cutoff)
    authentic_source_hash = value["source_hash"]
    store = EquityPoolStore(tmp_path / "cutoff-pool.sqlite3")
    try:
        service = EquityPoolService(
            store,
            fundamentals_reader=lambda _symbol, _slot: value,
            clock=lambda: NOW,
        )
        result = service.build(
            scanner_rows=_scanner_rows()[1:2],
            slot=NOW,
            pacing_usage={"scanner": {"used": 1, "limit": 3}},
        )
        factor = next(
            item
            for item in result.stored.normalized_inputs[0].factors
            if item.factor is FactorKind.FUNDAMENTALS
        )

        assert factor.status is FactorStatus.MISSING
        assert factor.reasons == ("FUNDAMENTALS_SOURCE_HASH_INVALID",)
        assert factor.source_hashes == ()
        assert authentic_source_hash not in factor.source_hashes
    finally:
        store.close()


class _AllowedPacing:
    ready = True
    capability_hash = "a" * 64

    def lease(self, request_class: str):
        return nullcontext(BudgetDecision(True, request_class, None, 1, 30))

    def usage(self) -> dict[str, dict[str, int]]:
        return {
            request_class: {
                "used": 3 if request_class == "scanner" else 1,
                "limit": 4 if request_class == "scanner" else 30,
            }
            for request_class in ("scanner", "snapshot_quote", "secdef", "historical", "account")
        }


class _PositionModeGateway:
    market_data_pacing_enabled = False

    def __init__(self) -> None:
        self.calls: list[str] = []

    def positions(self):
        self.calls.append("positions")
        return ({"contract_id": 724, "symbol": "QQQ", "security_type": "OPT", "quantity": "1"},)

    def scan_underlyings(self, *, scan_codes, rows_per_scan):
        self.calls.append("scan_underlyings")
        index = len(tuple(item for item in self.calls if item == "scan_underlyings"))
        return (
            UnderlyingScanResult(
                rank=index,
                symbol=f"S{index:03d}",
                contract_id=1000 + index,
                exchange="NYSE",
                source_scan=str(scan_codes[0]),
                industry=("Energy", "Financials", "Health Care")[index - 1],
                category="US STOCK",
                subcategory="COMMON",
            ),
        )

    def underlying_quotes(self, symbols):
        self.calls.append("underlying_quotes")
        return tuple(
            _quote(symbol, index)
            for index, symbol in enumerate(symbols, start=1)
        )

    def __getattr__(self, name: str):
        if name in {
            "working_orders",
            "unsubmitted_instructions",
            "option_expirations",
            "qualify_option_contracts",
            "option_contract_definitions",
            "option_quote_batch",
        }:
            def forbidden(*_args, **_kwargs):
                self.calls.append(name)
                raise AssertionError(f"{name} must not run in position mode")

            return forbidden
        raise AttributeError(name)


def test_open_derivative_position_refreshes_stock_only_pool_and_management(tmp_path) -> None:
    cache = UnderlyingEvidenceCache(tmp_path / "underlying.sqlite3")
    pool_store = EquityPoolStore(tmp_path / "pool.sqlite3")
    gateway = _PositionModeGateway()
    management_calls: list[str] = []
    try:
        service = EquityPoolService(
            pool_store,
            factor_readers={
                kind: lambda symbol, as_of, factor_kind=kind: cache.read_factor(symbol, factor_kind, as_of)
                for kind in (FactorKind.REGIME, FactorKind.TREND_VOLATILITY, FactorKind.POSITIONING)
            },
            liquidity_reader=lambda symbol, as_of: cache.read_liquidity(symbol, as_of),
            clock=lambda: NOW,
        )
        inputs = ProductionPipelineInputs(
            gateway,  # type: ignore[arg-type]
            _AllowedPacing(),  # type: ignore[arg-type]
            object(),
            core_symbols=(),
            equity_pool_builder=service.build,
            equity_evidence_capture=lambda rows, slot: tuple(
                cache.append(record)
                for record in captured_records_from_quotes(tuple(rows), captured_at=slot)
            ),
            clock=lambda: NOW,
        )
        inputs.bind_management_refresher(
            lambda: management_calls.append("refresh")
        )

        payload = inputs.run(scan_run_id="position-mode", slot_at=NOW)

        latest = pool_store.latest()
        assert payload["status"] == "POSITION_MANAGEMENT_ONLY"
        assert payload["universe"]["coarse_contracts"] == ()
        assert management_calls == ["refresh"]
        assert latest is not None
        assert latest.snapshot.position_mode.value == "BLOCKED_OPEN_POSITION"
        assert latest.snapshot.entry_authority is False
        assert all(row.entry_eligible is False for row in latest.snapshot.selected)
        assert gateway.calls.count("scan_underlyings") == 3
        assert payload["decision"] == "NO_TRADE"
        assert payload["funnel_trace"]["discovered_underlyings"] == 3
        assert payload["funnel_trace"]["deep_scan_requested"] == 0
        assert payload["funnel_trace"]["deep_scan_completed"] == 0
        assert payload["funnel_trace"]["scanner_completed_scan_codes"] == (
            "MOST_ACTIVE",
            "TOP_PERC_GAIN",
            "TOP_PERC_LOSE",
        )
        assert payload["funnel_trace"]["scanner_failed_scan_codes"] == ()
        assert payload["funnel_trace"]["scanner_source_row_counts"] == (
            {"scan_code": "MOST_ACTIVE", "row_count": 1},
            {"scan_code": "TOP_PERC_GAIN", "row_count": 1},
            {"scan_code": "TOP_PERC_LOSE", "row_count": 1},
        )
        assert "underlying_quotes" in gateway.calls
        assert not {
            "working_orders",
            "unsubmitted_instructions",
            "option_expirations",
            "qualify_option_contracts",
            "option_contract_definitions",
            "option_quote_batch",
        }.intersection(gateway.calls)
        assert cache.latest("S001", as_of=NOW) is not None
        assert payload["universe"]["coarse_contracts"] == ()
    finally:
        pool_store.close()
        cache.close()


def test_capture_uses_batch_completion_after_slot_and_preserves_pit_cutoff(tmp_path) -> None:
    quote_time = NOW + timedelta(seconds=2)
    completion_time = NOW + timedelta(seconds=3)

    class Gateway(_PositionModeGateway):
        def positions(self):
            self.calls.append("positions")
            return ()

        def working_orders(self):
            self.calls.append("working_orders")
            return ()

        def unsubmitted_instructions(self):
            self.calls.append("unsubmitted_instructions")
            return ()

        def underlying_quotes(self, symbols):
            self.calls.append("underlying_quotes")
            return tuple(
                _quote(symbol, index, quote_time)
                for index, symbol in enumerate(symbols, start=1)
            )

    cache = UnderlyingEvidenceCache(tmp_path / "underlying.sqlite3")
    pool_store = EquityPoolStore(tmp_path / "pool.sqlite3")
    captured_at_values: list[datetime] = []
    try:
        service = EquityPoolService(
            pool_store,
            factor_readers={
                kind: lambda symbol, as_of, factor_kind=kind: cache.read_factor(
                    symbol, factor_kind, as_of
                )
                for kind in (
                    FactorKind.REGIME,
                    FactorKind.TREND_VOLATILITY,
                    FactorKind.POSITIONING,
                )
            },
            liquidity_reader=lambda symbol, as_of: cache.read_liquidity(symbol, as_of),
            clock=lambda: completion_time,
        )

        def capture(rows, captured_at):
            captured_at_values.append(captured_at)
            return tuple(
                cache.append(record)
                for record in captured_records_from_quotes(
                    tuple(rows),
                    captured_at=captured_at,
                )
            )

        inputs = ProductionPipelineInputs(
            Gateway(),  # type: ignore[arg-type]
            _AllowedPacing(),  # type: ignore[arg-type]
            object(),
            core_symbols=(),
            equity_pool_builder=service.build,
            equity_evidence_capture=capture,
            clock=lambda: completion_time,
        )
        coarse_calls: list[tuple[str, ...]] = []

        def coarse(**kwargs):
            coarse_calls.append(tuple(kwargs["symbols"]))
            return type("Read", (), {
                "candidates": (), "reason_codes": (), "missing_symbols": (),
                "excluded_symbols": (), "quote_excluded_symbols": (),
                "quote_exclusion_reasons": (),
            })()

        inputs._coarse_candidate_outcome = coarse

        payload = inputs.run(scan_run_id="after-slot-quote", slot_at=NOW)

        assert payload["universe"]["coarse_contracts"] == ()
        assert captured_at_values == [completion_time]
        assert cache.latest("S001", as_of=NOW) is None
        assert cache.latest("S001", as_of=quote_time) is not None
        latest = pool_store.latest()
        assert latest is not None
        assert latest.snapshot.slot == completion_time
        assert 0 < len(latest.snapshot.selected) <= 30
        assert coarse_calls == [()]
        assert payload["funnel_trace"]["deep_scan_requested"] == 0
    finally:
        pool_store.close()
        cache.close()


def test_ready_pool_skips_secondary_liquidity_capture_on_executable_path(
    tmp_path,
) -> None:
    scan_slot = NOW + timedelta(minutes=1)
    completion_time = scan_slot + timedelta(seconds=1)

    class Gateway(_PositionModeGateway):
        def __init__(self) -> None:
            super().__init__()
            self.quote_batches: list[tuple[str, ...]] = []

        def positions(self):
            self.calls.append("positions")
            return ()

        def working_orders(self):
            self.calls.append("working_orders")
            return ()

        def unsubmitted_instructions(self):
            self.calls.append("unsubmitted_instructions")
            return ()

        def underlying_quotes(self, symbols):
            batch = tuple(symbols)
            self.calls.append("underlying_quotes")
            self.quote_batches.append(batch)
            return tuple(
                _quote(symbol, index)
                for index, symbol in enumerate(batch, start=1)
            )

    cache = UnderlyingEvidenceCache(tmp_path / "partial-bootstrap.sqlite3")
    pool_store = EquityPoolStore(tmp_path / "partial-bootstrap-pool.sqlite3")
    gateway = Gateway()
    try:
        cache.append(
            captured_record_from_quote(
                _quote("S001", 1),
                captured_at=NOW,
            )
        )
        service = EquityPoolService(
            pool_store,
            news_reader=lambda: {
                "news": ({
                    "id": "news.s001",
                    "symbols": ("S001",),
                    "symbol_binding": {
                        "status": "VERIFIED_PROVIDER_RELATED",
                    },
                    "classification": {
                        "direction": "BULLISH",
                        "confidence": 90.0,
                    },
                    "event_impact_score": 90.0,
                    "observed_at": NOW.isoformat(),
                },),
            },
            factor_readers={
                kind: lambda symbol, as_of, factor_kind=kind: cache.read_factor(
                    symbol,
                    factor_kind,
                    as_of,
                )
                for kind in (
                    FactorKind.REGIME,
                    FactorKind.TREND_VOLATILITY,
                    FactorKind.POSITIONING,
                )
            },
            liquidity_reader=lambda symbol, as_of: cache.read_liquidity(
                symbol,
                as_of,
            ),
            clock=lambda: completion_time,
        )
        inputs = ProductionPipelineInputs(
            gateway,  # type: ignore[arg-type]
            _AllowedPacing(),  # type: ignore[arg-type]
            object(),
            core_symbols=(),
            equity_pool_builder=service.build,
            equity_evidence_capture=lambda rows, captured_at: tuple(
                cache.append(record)
                for record in captured_records_from_quotes(
                    tuple(rows),
                    captured_at=captured_at,
                )
            ),
            clock=lambda: completion_time,
        )
        coarse_calls: list[tuple[str, ...]] = []

        def coarse(**kwargs):
            coarse_calls.append(tuple(kwargs["symbols"]))
            return type("Read", (), {
                "candidates": (), "reason_codes": (), "missing_symbols": (),
                "excluded_symbols": (), "quote_excluded_symbols": (),
                "quote_exclusion_reasons": (),
            })()

        inputs._coarse_candidate_outcome = coarse

        payload = inputs.run(
            scan_run_id="partial-pool-bootstrap",
            slot_at=scan_slot,
        )

        latest = pool_store.latest()
        assert latest is not None
        assert {item.symbol for item in latest.snapshot.selected} == {"S001"}
        quoted_symbols = {
            symbol
            for batch in gateway.quote_batches
            for symbol in batch
        }
        assert quoted_symbols == set()
        assert coarse_calls == [("S001",)]
        assert payload["funnel_trace"]["deep_scan_requested"] == 1
    finally:
        pool_store.close()
        cache.close()


def test_isolated_partial_capture_rebuilds_from_verified_rows_only(tmp_path) -> None:
    quote_time = NOW + timedelta(seconds=2)
    completion_time = NOW + timedelta(seconds=3)

    class Gateway(_PositionModeGateway):
        def positions(self):
            self.calls.append("positions")
            return ()

        def working_orders(self):
            self.calls.append("working_orders")
            return ()

        def unsubmitted_instructions(self):
            self.calls.append("unsubmitted_instructions")
            return ()

        def underlying_quotes(self, symbols):
            self.calls.append("underlying_quotes")
            return tuple(
                _quote(symbol, index, quote_time)
                for index, symbol in enumerate(symbols, start=1)
                if symbol != "S002"
            )

    cache = UnderlyingEvidenceCache(tmp_path / "partial-underlying.sqlite3")
    pool_store = EquityPoolStore(tmp_path / "partial-pool.sqlite3")
    try:
        service = EquityPoolService(
            pool_store,
            factor_readers={
                kind: lambda symbol, as_of, factor_kind=kind: cache.read_factor(
                    symbol, factor_kind, as_of
                )
                for kind in (
                    FactorKind.REGIME,
                    FactorKind.TREND_VOLATILITY,
                    FactorKind.POSITIONING,
                )
            },
            liquidity_reader=lambda symbol, as_of: cache.read_liquidity(
                symbol, as_of
            ),
            clock=lambda: completion_time,
        )

        def capture(rows, captured_at):
            return tuple(
                cache.append(record)
                for record in captured_records_from_quotes(
                    tuple(rows),
                    captured_at=captured_at,
                )
            )

        inputs = ProductionPipelineInputs(
            Gateway(),  # type: ignore[arg-type]
            _AllowedPacing(),  # type: ignore[arg-type]
            object(),
            core_symbols=(),
            equity_pool_builder=service.build,
            equity_evidence_capture=capture,
            clock=lambda: completion_time,
        )
        coarse_calls: list[tuple[str, ...]] = []

        def coarse(**kwargs):
            coarse_calls.append(tuple(kwargs["symbols"]))
            return type("Read", (), {
                "candidates": (), "reason_codes": (), "missing_symbols": (),
                "excluded_symbols": (), "quote_excluded_symbols": (),
                "quote_exclusion_reasons": (),
            })()

        inputs._coarse_candidate_outcome = coarse

        payload = inputs.run(scan_run_id="partial-after-slot-quote", slot_at=NOW)

        latest = pool_store.latest()
        assert latest is not None
        assert latest.snapshot.slot == completion_time
        selected_symbols = tuple(item.symbol for item in latest.snapshot.selected)
        assert selected_symbols
        assert "S002" not in selected_symbols
        assert coarse_calls == [()]
        assert payload["funnel_trace"]["deep_scan_requested"] == 0
    finally:
        pool_store.close()
        cache.close()


def test_future_quote_cannot_advance_trusted_capture_boundary() -> None:
    completion_time = NOW + timedelta(seconds=3)
    future_quote = _quote("FUTURE", 99, NOW + timedelta(days=1))

    with pytest.raises(
        ValueError,
        match="after trusted capture completion",
    ):
        _underlying_capture_completed_at(
            (future_quote,),
            clock=lambda: completion_time,
        )
