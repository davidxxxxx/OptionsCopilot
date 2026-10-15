from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

from options_copilot.gateway import (
    MarketDataPacingError,
    OptionContractRef,
    OptionExpiration,
)
from options_copilot.news.models import OptionLegSide, OptionRight, PreselectionPhase
from options_copilot.news.preselection import strategy_structure_hash
from options_copilot.news.preselection_producer import (
    ResolvedStructure,
    Top10StructureResolution,
    _candidate_from_mapping,
)
from options_copilot.option_pool import option_candidate_identity
from options_copilot.production_runtime import (
    DirectTop10StructureSource,
    DurableOptionPoolTop10StructureSource,
)
from options_copilot.storage.canonical import canonical_hash


SLOT = datetime(2026, 8, 4, 13, 20, tzinfo=timezone.utc)
EXPIRY = date(2026, 8, 25)


def _durable_decision(
    symbol: str,
    *,
    expiration: date = EXPIRY,
    first_con_id: int = 101,
    reason_codes: tuple[str, ...] = (
        "AFTER_HOURS_EXACT_IDENTITY_RESEARCH_ONLY",
        "EXECUTABLE_LEG_QUOTE_INCOMPLETE",
        "OPTION_GREEKS_INCOMPLETE",
        "OPTION_LIQUIDITY_EVIDENCE_INCOMPLETE",
        "STRUCTURE_PAYOFF_EVIDENCE_INCOMPLETE",
        "AFTER_COST_ECONOMICS_INCOMPLETE",
    ),
    include_thesis: bool = True,
    include_payload_thesis_hash: bool = True,
    direction_label: str = "BULLISH",
) -> tuple[object, str, str]:
    thesis = {
        "schema": "options_copilot.equity_thesis_evidence.v1",
        "symbol": symbol,
        "direction_label": direction_label,
        "direction_score": Decimal("45"),
        "uncertainty": Decimal("0.20"),
        "observed_at": SLOT - timedelta(hours=17),
        "source_hashes": ("7" * 64,),
        "canonical_input_hash": "8" * 64,
        "selected_rank": 1,
    }
    thesis_hash = canonical_hash(thesis)
    payload = {
        "candidate_id": f"after-hours.{symbol}.debit",
        "symbol": symbol,
        "structure": "DEBIT_VERTICAL",
        "legs": (
            {
                "con_id": first_con_id,
                "contract_id_ex": f"{first_con_id}@SMART",
                "expiration": expiration.isoformat(),
                "strike": "99",
                "right": "CALL",
                "side": "BUY",
                "ratio": 1,
                "multiplier": 100,
                "exchange": "SMART",
                "local_symbol": f"{symbol} {expiration:%y%m%d}C00099000",
                "trading_class": symbol,
            },
            {
                "con_id": first_con_id + 1,
                "contract_id_ex": f"{first_con_id + 1}@SMART",
                "expiration": expiration.isoformat(),
                "strike": "101",
                "right": "CALL",
                "side": "SELL",
                "ratio": 1,
                "multiplier": 100,
                "exchange": "SMART",
                "local_symbol": f"{symbol} {expiration:%y%m%d}C00101000",
                "trading_class": symbol,
            },
        ),
    }
    if include_thesis:
        payload["equity_thesis_evidence"] = thesis
    if include_payload_thesis_hash:
        payload["equity_thesis_hash"] = thesis_hash
    candidate_hash = canonical_hash(payload)
    candidate_identity = option_candidate_identity(payload)
    return (
        SimpleNamespace(
            underlying=symbol,
            disposition="RESEARCH_ONLY",
            reason_codes=reason_codes,
            equity_thesis_evidence=(thesis if include_thesis else None),
            candidate_id=payload["candidate_id"],
            candidate_hash=candidate_hash,
            candidate_identity=candidate_identity,
            exact_economics=payload,
        ),
        candidate_hash,
        candidate_identity,
    )


def _underlying_quote(
    symbol: str,
    *,
    observed_at: datetime = SLOT,
    market_data_type: int = 1,
    close: Decimal = Decimal("99"),
) -> object:
    return SimpleNamespace(
        symbol=symbol,
        contract_id=10_000 + sum(
            (index + 1) * ord(character)
            for index, character in enumerate(symbol)
        ),
        exchange="NASDAQ",
        observed_at=observed_at,
        source="IBKR_REQ_TICKERS_READONLY",
        bid=Decimal("99.90"),
        ask=Decimal("100.10"),
        last=Decimal("100"),
        close=close,
        market_data_type=market_data_type,
    )


class _Pacing:
    def __init__(self, *, ready: bool = True) -> None:
        self.ready = ready
        self.calls: list[str] = []
        self._usage: dict[str, int] = {"scanner": 0}

    def decision(self, request_class: str) -> object:
        self.calls.append(request_class)
        if self.ready:
            self.record(request_class)
        return SimpleNamespace(allowed=self.ready)

    def record(self, request_class: str) -> None:
        self._usage[request_class] = self._usage.get(request_class, 0) + 1

    def usage(self) -> dict[str, dict[str, int]]:
        return {
            request_class: {
                "used": used,
                "limit": 4 if request_class == "scanner" else 30,
            }
            for request_class, used in self._usage.items()
        }


class _Gateway:
    def __init__(self, count: int = 12, *, quote_time: datetime = SLOT) -> None:
        self.symbols = tuple(f"S{index:02d}" for index in range(count))
        self.calls: list[tuple[object, ...]] = []
        self.quote_time = quote_time

    def scan_underlyings(
        self,
        *,
        scan_codes: tuple[str, ...],
        rows_per_scan: int,
    ) -> tuple[object, ...]:
        self.calls.append(("scan", scan_codes, rows_per_scan))
        return tuple(
            SimpleNamespace(
                symbol=symbol,
                rank=index,
                source_scan=scan_codes[0],
            )
            for index, symbol in enumerate(self.symbols)
        )

    def underlying_quotes(self, symbols: tuple[str, ...]) -> tuple[object, ...]:
        self.calls.append(("underlying_quotes", *symbols))
        return tuple(
            _underlying_quote(symbol, observed_at=self.quote_time)
            for symbol in symbols
        )

    def option_expirations(
        self,
        symbol: str,
        *,
        min_dte: int,
        max_dte: int,
    ) -> tuple[OptionExpiration, ...]:
        self.calls.append(("expirations", symbol, min_dte, max_dte))
        return (
            OptionExpiration(
                expiration=EXPIRY,
                trading_class=symbol,
                exchange="SMART",
                multiplier=100,
                strikes=(
                    Decimal("90"),
                    Decimal("95"),
                    Decimal("100"),
                    Decimal("105"),
                    Decimal("110"),
                ),
            ),
        )

    def qualify_option_contracts(
        self,
        symbol: str,
        expiration: date,
        strikes: tuple[Decimal, Decimal],
        *,
        exchange: str,
        trading_class: str,
        rights: tuple[str, ...],
    ) -> tuple[OptionContractRef, ...]:
        self.calls.append(("qualify", symbol, expiration, strikes, rights))
        right = rights[0]
        return tuple(
            OptionContractRef(
                contract_id=(int(symbol[1:]) + 1) * 1000 + index,
                contract_id_ex=f"{(int(symbol[1:]) + 1) * 1000 + index}@SMART",
                symbol=symbol,
                local_symbol=f"{symbol}-{right}-{strike}",
                expiration=expiration,
                strike=strike,
                right=right,  # type: ignore[arg-type]
                exchange=exchange,
                trading_class=trading_class,
                multiplier=100,
                currency="USD",
            )
            for index, strike in enumerate(strikes, start=1)
        )


def test_direct_source_discovers_ten_unique_exact_verticals_without_quotes() -> None:
    gateway = _Gateway()
    pacing = _Pacing()
    source = DirectTop10StructureSource(gateway, pacing, clock=lambda: SLOT)

    result = source.resolve_top10(scheduled_for=SLOT)

    assert len(result) == 10
    assert all(isinstance(item, ResolvedStructure) for item in result)
    assert len({item.candidate.underlying for item in result}) == 10
    assert gateway.calls[:3] == [
        ("scan", ("MOST_ACTIVE",), 50),
        ("scan", ("TOP_PERC_GAIN",), 50),
        ("scan", ("TOP_PERC_LOSE",), 50),
    ]
    assert pacing.calls[:3] == ["scanner", "scanner", "scanner"]
    quote_calls = [call for call in gateway.calls if call[0] == "underlying_quotes"]
    assert quote_calls == [
        ("underlying_quotes", "S00", "S01", "S02", "S03"),
        ("underlying_quotes", "S04", "S05", "S06", "S07"),
        ("underlying_quotes", "S08", "S09", "S10", "S11"),
    ]
    assert [call for call in gateway.calls if call[0] == "expirations"] == [
        ("expirations", f"S{index:02d}", 14, 35) for index in range(12)
    ]
    assert pacing.calls == [
        "scanner",
        "scanner",
        "scanner",
        *("secdef" for _ in range(12)),
        "snapshot_quote",
        "snapshot_quote",
        "snapshot_quote",
        *("secdef" for _ in range(10)),
    ]


def test_direct_source_accepts_prior_scanner_usage_within_rolling_limit() -> None:
    gateway = _Gateway(count=1)
    pacing = _Pacing()
    pacing.record("scanner")

    result = DirectTop10StructureSource(
        gateway,
        pacing,
        clock=lambda: SLOT,
    ).resolve_top10(scheduled_for=SLOT)

    assert len(result) == 1
    assert pacing.usage()["scanner"] == {"used": 4, "limit": 4}
    assert pacing.calls[:3] == ["scanner", "scanner", "scanner"]


def test_direct_source_does_not_default_missing_scanner_provenance() -> None:
    class MissingSourceGateway(_Gateway):
        def scan_underlyings(
            self,
            *,
            scan_codes: tuple[str, ...],
            rows_per_scan: int,
        ) -> tuple[object, ...]:
            self.calls.append(("scan", scan_codes, rows_per_scan))
            return tuple(
                SimpleNamespace(symbol=symbol, rank=index)
                for index, symbol in enumerate(self.symbols)
            )

    source = DirectTop10StructureSource(
        MissingSourceGateway(count=2),
        _Pacing(),
        clock=lambda: SLOT,
        core_symbols=(),
    )
    result = source.resolve_top10(scheduled_for=SLOT)

    assert result.structures == ()
    assert "DISCOVERY_PROVENANCE_UNAVAILABLE" in result.reason_codes
    assert result.missing_symbols == ("S00", "S01")


def test_gateway_paced_research_source_does_not_inherit_two_candidate_action_cap() -> None:
    pacing = _Pacing()

    class Gateway(_Gateway):
        market_data_pacing_enabled = True

        def scan_underlyings(self, **kwargs) -> tuple[object, ...]:
            pacing.record("scanner")
            return super().scan_underlyings(**kwargs)

    gateway = Gateway(count=12)
    source = DirectTop10StructureSource(
        gateway,
        pacing,
        clock=lambda: SLOT,
    )
    result = source.resolve_top10(scheduled_for=SLOT)

    assert len(result.structures) == 10
    assert len(source.discovery_metadata) == 10
    for metadata in source.discovery_metadata:
        basis = metadata["underlying_quote_basis"]
        assert metadata["underlying_quote_basis_hash"] == canonical_hash(basis)
    assert tuple(item.candidate.underlying for item in result.structures) == tuple(
        f"S{index:02d}" for index in range(10)
    )
    assert [call for call in gateway.calls if call[0] == "expirations"] == [
        ("expirations", f"S{index:02d}", 14, 35) for index in range(12)
    ]
    assert all(item.candidate.phase is PreselectionPhase.PRE_MARKET for item in result)
    assert all(item.candidate.risk_defined for item in result)
    assert all(item.candidate.maximum_loss_usd is None for item in result)

    for item in result:
        candidate = item.candidate
        assert len(candidate.legs) == 2
        assert tuple(leg.side for leg in candidate.legs) == (
            OptionLegSide.BUY,
            OptionLegSide.SELL,
        )
        assert all(leg.contract_ref is not None for leg in candidate.legs)
        assert all(leg.multiplier == 100 and leg.dte == 21 for leg in candidate.legs)
        assert all(
            getattr(leg, field) is None
            for leg in candidate.legs
            for field in leg._DYNAMIC_QUOTE_FIELDS
        )
        assert candidate.strategy_hash == strategy_structure_hash(
            candidate.underlying,
            candidate.strategy_type,
            candidate.legs,
        )
        assert candidate.underlying_quote_basis is not None
        assert candidate.underlying_quote_basis_hash == (
            candidate.underlying_quote_basis.basis_hash
        )
        assert candidate.underlying_quote_basis_hash in candidate.evidence_hashes
        replayed = _candidate_from_mapping(candidate.as_dict())
        assert replayed.underlying_quote_basis == candidate.underlying_quote_basis
        assert replayed.underlying_quote_basis_hash == (
            candidate.underlying_quote_basis_hash
        )
        open_copy = replace(candidate, phase=PreselectionPhase.OPEN_REPRICED)
        assert open_copy.underlying_quote_basis == candidate.underlying_quote_basis
        assert open_copy.underlying_quote_basis_hash == (
            candidate.underlying_quote_basis_hash
        )

    bullish = result[0].candidate
    assert {leg.right for leg in bullish.legs} == {OptionRight.CALL}
    assert bullish.legs[0].strike < bullish.legs[1].strike


@pytest.mark.parametrize(
    ("mutate", "reason"),
    [
        (
            lambda row: setattr(row, "observed_at", SLOT - timedelta(seconds=6)),
            "UNDERLYING_QUOTE_STALE_OR_FUTURE",
        ),
        (
            lambda row: setattr(row, "observed_at", SLOT + timedelta(microseconds=1)),
            "UNDERLYING_QUOTE_STALE_OR_FUTURE",
        ),
        (
            lambda row: setattr(row, "market_data_type", 3),
            "UNDERLYING_QUOTE_MARKET_DATA_DELAYED",
        ),
        (
            lambda row: setattr(row, "market_data_type", 4),
            "UNDERLYING_QUOTE_MARKET_DATA_DELAYED",
        ),
        (
            lambda row: setattr(row, "contract_id", 0),
            "UNDERLYING_QUOTE_IDENTITY_INCOMPLETE",
        ),
        (
            lambda row: setattr(row, "source", "UNVERIFIED"),
            "UNDERLYING_QUOTE_IDENTITY_INCOMPLETE",
        ),
    ],
)
def test_direct_source_rejects_untrusted_underlying_direction_basis(
    mutate,
    reason: str,
) -> None:
    class Gateway(_Gateway):
        def underlying_quotes(self, symbols: tuple[str, ...]) -> tuple[object, ...]:
            row = _underlying_quote(symbols[0])
            mutate(row)
            return (row,)

    result = DirectTop10StructureSource(
        Gateway(count=1),
        _Pacing(),
        clock=lambda: SLOT,
        core_symbols=("S00",),
        include_scanner=False,
    ).resolve_top10(scheduled_for=SLOT)

    assert result.structures == ()
    assert result.reason_codes == (reason,)
    assert result.missing_symbols == ("S00",)


def test_direct_source_binds_quote_mutation_even_when_structure_is_unchanged() -> None:
    class Gateway(_Gateway):
        def __init__(self, close: Decimal) -> None:
            super().__init__(count=1)
            self.close = close

        def underlying_quotes(self, symbols: tuple[str, ...]) -> tuple[object, ...]:
            return tuple(
                _underlying_quote(symbol, close=self.close)
                for symbol in symbols
            )

    first = DirectTop10StructureSource(
        Gateway(Decimal("99")),
        _Pacing(),
        clock=lambda: SLOT,
        core_symbols=("S00",),
        include_scanner=False,
    ).resolve_top10(scheduled_for=SLOT)[0].candidate
    mutated = DirectTop10StructureSource(
        Gateway(Decimal("98")),
        _Pacing(),
        clock=lambda: SLOT,
        core_symbols=("S00",),
        include_scanner=False,
    ).resolve_top10(scheduled_for=SLOT)[0].candidate

    assert first.preselection_id == mutated.preselection_id
    assert first.strategy_hash == mutated.strategy_hash
    assert first.underlying_quote_basis_hash != mutated.underlying_quote_basis_hash
    assert first.evidence_hashes != mutated.evidence_hashes
    assert first.underlying_quote_basis is not None
    with pytest.raises(ValueError, match="basis hash does not match"):
        replace(
            first,
            underlying_quote_basis=replace(
                first.underlying_quote_basis,
                close=Decimal("97"),
            ),
        )


def test_direct_source_fails_closed_before_broker_reads_without_pacing() -> None:
    gateway = _Gateway()
    source = DirectTop10StructureSource(
        gateway,
        _Pacing(ready=False),
        clock=lambda: SLOT,
    )

    result = source.resolve_top10(scheduled_for=SLOT)

    assert result.structures == ()
    assert result.reason_codes == ("PACING_CAPABILITY_MISSING",)
    assert gateway.calls == []


def test_direct_source_surfaces_wire_level_scanner_pacing_denial() -> None:
    pacing = _Pacing()

    class Gateway(_Gateway):
        market_data_pacing_enabled = True

        def scan_underlyings(self, **_kwargs) -> tuple[object, ...]:
            raise MarketDataPacingError(
                "scanner",
                "PACING_REQUEST_WINDOW_EXHAUSTED",
            )

    result = DirectTop10StructureSource(
        Gateway(count=1),
        pacing,
        clock=lambda: SLOT,
        core_symbols=(),
    ).resolve_top10(scheduled_for=SLOT)

    assert result.structures == ()
    assert result.reason_codes == (
        "IBKR_SCANNER_PACING_DENIED",
        "IBKR_SCANNER_PACING_REQUEST_WINDOW_EXHAUSTED",
    )
    assert pacing.usage()["scanner"] == {"used": 0, "limit": 4}


def test_direct_source_does_not_fall_back_to_opposite_option_direction() -> None:
    class Gateway(_Gateway):
        def option_expirations(self, symbol: str, **_kwargs) -> tuple[OptionExpiration, ...]:
            return (
                OptionExpiration(
                    expiration=EXPIRY,
                    trading_class=symbol,
                    exchange="SMART",
                    multiplier=100,
                    strikes=(Decimal("95"), Decimal("100")),
                ),
            )

        def underlying_quotes(self, symbols: tuple[str, ...]) -> tuple[object, ...]:
            return tuple(_underlying_quote(symbol) for symbol in symbols)

        def qualify_option_contracts(self, *_args, **_kwargs):
            raise AssertionError("opposite-direction strikes reached qualification")

    result = DirectTop10StructureSource(
        Gateway(count=1),
        _Pacing(),
        clock=lambda: SLOT,
        core_symbols=("S00",),
        include_scanner=False,
    ).resolve_top10(scheduled_for=SLOT)

    assert result.structures == ()


def test_direct_source_can_skip_scanner_and_use_only_liquid_core_fallbacks() -> None:
    gateway = _Gateway()
    pacing = _Pacing()
    source = DirectTop10StructureSource(
        gateway,
        pacing,
        clock=lambda: SLOT,
        core_symbols=tuple(f"S{index:02d}" for index in range(10)),
        include_scanner=False,
    )

    result = source.resolve_top10(scheduled_for=SLOT)

    assert len(result) == 10
    assert tuple(item.candidate.underlying for item in result) == tuple(
        f"S{index:02d}" for index in range(10)
    )
    assert all(call[0] != "scan" for call in gateway.calls)
    assert gateway.calls[:10] == [
        ("expirations", f"S{index:02d}", 14, 35) for index in range(10)
    ]
    assert gateway.calls[10:13] == [
        ("underlying_quotes", "S00", "S01", "S02", "S03"),
        ("underlying_quotes", "S04", "S05", "S06", "S07"),
        ("underlying_quotes", "S08", "S09"),
    ]
    assert pacing.calls == [
        *("secdef" for _ in range(10)),
        "snapshot_quote",
        "snapshot_quote",
        "snapshot_quote",
        *("secdef" for _ in range(10)),
    ]


def test_direct_source_excludes_non_optionable_underlyings_before_quotes() -> None:
    class Gateway(_Gateway):
        def option_expirations(
            self,
            symbol: str,
            *,
            min_dte: int,
            max_dte: int,
        ) -> tuple[OptionExpiration, ...]:
            self.calls.append(("expirations", symbol, min_dte, max_dte))
            return ()

        def underlying_quotes(self, symbols: tuple[str, ...]) -> tuple[object, ...]:
            raise AssertionError(f"non-optionable symbols reached quotes: {symbols}")

    gateway = Gateway(count=25)
    pacing = _Pacing()
    source = DirectTop10StructureSource(
        gateway,
        pacing,
        clock=lambda: SLOT,
        core_symbols=(),
    )

    result = source.resolve_top10(scheduled_for=SLOT)

    assert result.structures == ()
    assert result.reason_codes == ("NO_OPTIONABLE_UNDERLYINGS",)
    assert [call for call in gateway.calls if call[0] == "expirations"] == [
        ("expirations", f"S{index:02d}", 14, 35) for index in range(14)
    ]
    assert all(call[0] != "underlying_quotes" for call in gateway.calls)


def test_direct_source_rechecks_dte_boundaries_before_quotes() -> None:
    eastern_date = date(2026, 8, 4)
    crossover_slot = datetime(2026, 8, 5, 1, 0, tzinfo=timezone.utc)

    class Gateway(_Gateway):
        def option_expirations(
            self,
            symbol: str,
            *,
            min_dte: int,
            max_dte: int,
        ) -> tuple[OptionExpiration, ...]:
            self.calls.append(("expirations", symbol, min_dte, max_dte))
            dte = (13, 14, 35, 36)[int(symbol[1:])]
            return (
                OptionExpiration(
                    expiration=eastern_date + timedelta(days=dte),
                    trading_class=symbol,
                    exchange="SMART",
                    multiplier=100,
                    strikes=(Decimal("95"), Decimal("100"), Decimal("105")),
                ),
            )

    gateway = Gateway(count=4, quote_time=crossover_slot)
    source = DirectTop10StructureSource(
        gateway,
        _Pacing(),
        clock=lambda: crossover_slot,
        core_symbols=(),
    )

    result = source.resolve_top10(scheduled_for=crossover_slot)

    assert tuple(item.candidate.underlying for item in result) == ("S01", "S02")
    assert tuple(item.candidate.legs[0].dte for item in result) == (14, 35)
    assert [call for call in gateway.calls if call[0] == "underlying_quotes"] == [
        ("underlying_quotes", "S01", "S02"),
    ]


def test_secdef_leases_cover_real_chain_and_qualification_calls() -> None:
    class Pacing:
        ready = True
        reason = None

        def __init__(self) -> None:
            self.active: str | None = None
            self.calls: list[str] = []

        @contextmanager
        def lease(self, request_class: str):
            assert self.active is None
            self.active = request_class
            self.calls.append(request_class)
            try:
                yield SimpleNamespace(allowed=True, reason=None)
            finally:
                self.active = None

    pacing = Pacing()

    class Gateway(_Gateway):
        def option_expirations(self, *args, **kwargs):  # type: ignore[no-untyped-def]
            assert pacing.active == "secdef"
            return super().option_expirations(*args, **kwargs)

        def underlying_quotes(self, *args, **kwargs):  # type: ignore[no-untyped-def]
            assert pacing.active == "snapshot_quote"
            return super().underlying_quotes(*args, **kwargs)

        def qualify_option_contracts(self, *args, **kwargs):  # type: ignore[no-untyped-def]
            assert pacing.active == "secdef"
            return super().qualify_option_contracts(*args, **kwargs)

    result = DirectTop10StructureSource(
        Gateway(count=1),
        pacing,
        clock=lambda: SLOT,
        core_symbols=("S00",),
        include_scanner=False,
    ).resolve_top10(scheduled_for=SLOT)

    assert len(result) == 1
    assert pacing.calls == ["secdef", "snapshot_quote", "secdef"]


def test_qualification_pacing_denial_discards_partial_top10() -> None:
    class Pacing:
        ready = True
        reason = None

        def __init__(self) -> None:
            self.secdef_calls = 0

        @contextmanager
        def lease(self, request_class: str):
            if request_class == "secdef":
                self.secdef_calls += 1
                allowed = self.secdef_calls <= 15
                reason = None if allowed else "PACING_REQUEST_WINDOW_EXHAUSTED"
            else:
                allowed = True
                reason = None
            yield SimpleNamespace(allowed=allowed, reason=reason)

    symbols = tuple(f"S{index:02d}" for index in range(10))
    result = DirectTop10StructureSource(
        _Gateway(count=10),
        Pacing(),
        clock=lambda: SLOT,
        core_symbols=symbols,
        include_scanner=False,
    ).resolve_top10(scheduled_for=SLOT)

    assert result.structures == ()
    assert result.reason_codes == (
        "OPTION_QUALIFICATION_PACING_DENIED",
        "OPTION_QUALIFICATION_PACING_REQUEST_WINDOW_EXHAUSTED",
    )
    assert result.missing_symbols == symbols[5:]


def test_qualification_transport_failure_excludes_only_failed_symbol() -> None:
    class Gateway(_Gateway):
        def qualify_option_contracts(self, symbol: str, *args, **kwargs):  # type: ignore[no-untyped-def]
            if symbol == "S05":
                raise RuntimeError("sanitized transport failure")
            return super().qualify_option_contracts(symbol, *args, **kwargs)

    symbols = tuple(f"S{index:02d}" for index in range(11))
    result = DirectTop10StructureSource(
        Gateway(count=11),
        _Pacing(),
        clock=lambda: SLOT,
        core_symbols=symbols,
        include_scanner=False,
    ).resolve_top10(scheduled_for=SLOT)

    assert len(result.structures) == 10
    assert result.reason_codes == ()
    assert result.missing_symbols == ()
    assert {row.candidate.underlying for row in result.structures} == (
        set(symbols) - {"S05"}
    )
    later = next(
        row.candidate
        for row in result.structures
        if row.candidate.underlying == "S06"
    )
    assert "IBKR_DIRECT_SYMBOL_EXCLUDED:S05" in later.evidence_ids


def test_durable_option_pool_source_reuses_hash_verified_exact_identity() -> None:
    observed_at = SLOT - timedelta(hours=17)
    decision, candidate_hash, candidate_identity = _durable_decision("SPY")
    snapshot_hash = "d" * 64
    snapshot = SimpleNamespace(
        scan_run_id="after-hours-formal.test",
        observed_at=observed_at,
        snapshot_hash=snapshot_hash,
        decisions=(decision,),
    )

    class Store:
        def latest(self):
            return snapshot

    class ForbiddenFallback:
        def resolve_top10(self, *, scheduled_for):
            raise AssertionError(f"durable identity unexpectedly fell back: {scheduled_for}")

    result = DurableOptionPoolTop10StructureSource(
        Store(),
        fallback=ForbiddenFallback(),
    ).resolve_top10(scheduled_for=SLOT)

    assert len(result.structures) == 1
    candidate = result.structures[0].candidate
    assert candidate.underlying == "SPY"
    assert candidate.strategy_type == "DEBIT_VERTICAL"
    assert candidate.phase is PreselectionPhase.PRE_MARKET
    assert candidate.underlying_quote_basis is None
    assert candidate.evidence_hashes == (
        snapshot_hash,
        candidate_hash,
        candidate_identity,
        canonical_hash(decision.equity_thesis_evidence),
    )
    assert tuple(leg.con_id for leg in candidate.legs) == (101, 102)
    assert all(
        getattr(leg, field) is None
        for leg in candidate.legs
        for field in leg._DYNAMIC_QUOTE_FIELDS
    )


def test_actual_coarse_producer_survives_research_store_and_next_day_parent(tmp_path) -> None:
    from options_copilot.production_runtime import ProductionPipelineInputs
    from options_copilot.decision.pipeline import _research_option_pool_candidate_documents
    from options_copilot.option_pool import OptionStructurePoolService, OptionStructurePoolStore
    from options_copilot.option_pool.models import normalize_equity_thesis_row

    reference = {
        "schema": "options_copilot.equity_pool_reference.v1",
        "snapshot_id": "1" * 64, "snapshot_hash": "2" * 64,
        "input_manifest_hash": "3" * 64, "rows_hash": "4" * 64,
        "policy_hash": "5" * 64, "taxonomy_hash": "6" * 64, "scoring_hash": "7" * 64,
        "selected_symbols": ("S00",), "discovered_symbols": ("S00",),
        "discovery_count": 1, "selected_count": 1, "excluded_count": 0,
        "exclusion_stats": {},
    }
    raw_thesis = _durable_decision("S00")[0].equity_thesis_evidence
    thesis = normalize_equity_thesis_row(raw_thesis, expected_symbol="S00")
    rows = (thesis,)
    theses = {
        "schema": "options_copilot.equity_theses.v1",
        "equity_pool_reference_hash": canonical_hash(reference),
        "rows": rows, "rows_hash": canonical_hash(rows),
    }
    producer = ProductionPipelineInputs(
        _Gateway(count=1), _Pacing(), object(), core_symbols=(), clock=lambda: SLOT,
    )
    coarse = producer._coarse_candidate_outcome(
        scan_run_id="scan.lineage", slot_at=SLOT, symbols=("S00",), equity_theses=theses,
    )
    assert coarse.candidates, coarse.reason_codes
    documents = _research_option_pool_candidate_documents(
        coarse.candidates, equity_pool_reference=reference, equity_theses=theses,
    )
    assert documents
    with OptionStructurePoolStore(tmp_path / "pool.sqlite3") as store:
        snapshot = OptionStructurePoolService(store).capture_research_candidates(
            scan_run_id="scan.lineage.research", candidates=documents, observed_at=SLOT,
            equity_pool_reference=reference, equity_theses=theses,
            research_reason_codes=("REGULAR_SESSION_EXACT_IDENTITY_RESEARCH_ONLY",),
        )
        assert all("STRUCTURE_TEMPLATE_SEMANTICS_INVALID" not in item.reason_codes
                   for item in snapshot.decisions)
        class ForbiddenFallback:
            def resolve_top10(self, **kwargs):
                pytest.fail("real producer identity must not need fallback")

        result = DurableOptionPoolTop10StructureSource(store, fallback=ForbiddenFallback()).resolve_top10(
            scheduled_for=SLOT + timedelta(days=1),
        )
        assert result.structures, result.reason_codes
        assert all(item.candidate.phase is PreselectionPhase.PRE_MARKET for item in result)
        assert all(leg.bid is None and leg.ask is None for item in result
                   for leg in item.candidate.legs)


def test_durable_option_pool_source_uses_recent_thesis_bound_regular_scan() -> None:
    unbound, _, _ = _durable_decision("QQQ", include_thesis=False)
    bound, _, _ = _durable_decision("SPY")
    latest = SimpleNamespace(
        scan_run_id="after-hours-formal.latest-unbound",
        observed_at=SLOT - timedelta(hours=12),
        snapshot_hash="7" * 64,
        decisions=(unbound,),
    )
    earlier = SimpleNamespace(
        scan_run_id="scan.1234567890abcdef",
        observed_at=SLOT - timedelta(hours=18),
        snapshot_hash="8" * 64,
        decisions=(bound,),
    )

    class Store:
        def recent(self, *, limit):
            assert 1 <= limit <= 100
            return (latest, earlier)

        def latest(self):
            raise AssertionError("recent snapshot selection must be used")

    class ForbiddenFallback:
        def resolve_top10(self, *, scheduled_for):
            raise AssertionError(f"valid regular scan unexpectedly fell back: {scheduled_for}")

    result = DurableOptionPoolTop10StructureSource(
        Store(),
        fallback=ForbiddenFallback(),
    ).resolve_top10(scheduled_for=SLOT)

    assert tuple(item.candidate.underlying for item in result.structures) == ("SPY",)
    assert result.reason_codes == ()
    assert result.structures[0].candidate.evidence_ids[0] == (
        "DURABLE_OPTION_POOL:scan.1234567890abcdef"
    )


def test_durable_option_pool_source_does_not_backfill_from_thesis_unbound_fallback() -> None:
    valid, _, _ = _durable_decision("SPY")
    expired = tuple(
        _durable_decision(
            f"Q{index:02d}",
            expiration=SLOT.date() + timedelta(days=6),
            first_con_id=201 + (index * 2),
        )[0]
        for index in range(9)
    )
    snapshot = SimpleNamespace(
        scan_run_id="after-hours-formal.partial",
        observed_at=SLOT - timedelta(hours=17),
        snapshot_hash="e" * 64,
        decisions=(valid, *expired),
    )

    class Store:
        def latest(self):
            return snapshot

    duplicate_spy, _, _ = _durable_decision("SPY", first_con_id=301)
    duplicate_snapshot = SimpleNamespace(
        scan_run_id="after-hours-formal.duplicate",
        observed_at=SLOT - timedelta(hours=17),
        snapshot_hash="f" * 64,
        decisions=(duplicate_spy,),
    )

    class DuplicateStore:
        def latest(self):
            return duplicate_snapshot

    class UnusedFallback:
        def resolve_top10(self, *, scheduled_for):
            raise AssertionError(f"single valid row must not fall back: {scheduled_for}")

    duplicate = DurableOptionPoolTop10StructureSource(
        DuplicateStore(),
        fallback=UnusedFallback(),
    ).resolve_top10(scheduled_for=SLOT).structures[0]
    fallback_symbols = tuple(f"S{index:02d}" for index in range(9))
    direct = DirectTop10StructureSource(
        _Gateway(count=9),
        _Pacing(),
        clock=lambda: SLOT,
        core_symbols=fallback_symbols,
        include_scanner=False,
    ).resolve_top10(scheduled_for=SLOT)

    class Fallback:
        def resolve_top10(self, *, scheduled_for):
            assert scheduled_for == SLOT
            return Top10StructureResolution((duplicate, *direct.structures))

    result = DurableOptionPoolTop10StructureSource(
        Store(),
        fallback=Fallback(),
    ).resolve_top10(scheduled_for=SLOT)

    assert len(result.structures) == 1
    assert result.reason_codes == ()
    assert result.missing_symbols == ()
    assert result.structures[0].candidate.underlying == "SPY"
    assert all(
        item.candidate.phase is PreselectionPhase.PRE_MARKET
        for item in result.structures
    )


def test_durable_option_pool_source_caps_parent_to_open_snapshot_leg_budget() -> None:
    decisions = tuple(
        _durable_decision(
            f"S{index:02d}",
            first_con_id=1001 + (index * 2),
        )[0]
        for index in range(10)
    )
    snapshot = SimpleNamespace(
        scan_run_id="after-hours-formal.snapshot-budget",
        observed_at=SLOT - timedelta(hours=17),
        snapshot_hash="9" * 64,
        decisions=decisions,
    )

    class Store:
        def latest(self):
            return snapshot

    class ForbiddenFallback:
        def resolve_top10(self, *, scheduled_for):
            raise AssertionError(f"valid durable pool fell back: {scheduled_for}")

    result = DurableOptionPoolTop10StructureSource(
        Store(),
        fallback=ForbiddenFallback(),
        maximum_snapshot_contracts=14,
    ).resolve_top10(scheduled_for=SLOT)

    assert len(result.structures) == 7
    assert sum(len(item.candidate.legs) for item in result.structures) == 14
    assert result.reason_codes == ()
    assert result.missing_symbols == ()


def test_durable_option_pool_source_keeps_valid_rows_when_fallback_is_incomplete() -> None:
    valid, _, _ = _durable_decision("SPY")
    expired, _, _ = _durable_decision(
        "QQQ",
        expiration=SLOT.date() + timedelta(days=6),
        first_con_id=201,
    )
    snapshot = SimpleNamespace(
        scan_run_id="after-hours-formal.partial-incomplete",
        observed_at=SLOT - timedelta(hours=17),
        snapshot_hash="a" * 64,
        decisions=(valid, expired),
    )

    class Store:
        def latest(self):
            return snapshot

    class IncompleteFallback:
        def resolve_top10(self, *, scheduled_for):
            assert scheduled_for == SLOT
            return Top10StructureResolution(
                (),
                reason_codes=("UNDERLYING_QUOTES_UNAVAILABLE",),
                missing_symbols=("IWM",),
            )

    result = DurableOptionPoolTop10StructureSource(
        Store(),
        fallback=IncompleteFallback(),
    ).resolve_top10(scheduled_for=SLOT)

    assert tuple(item.candidate.underlying for item in result.structures) == ("SPY",)
    assert result.reason_codes == ()
    assert result.missing_symbols == ()


def test_durable_option_pool_source_keeps_valid_rows_when_fallback_raises() -> None:
    valid, _, _ = _durable_decision("SPY")
    expired, _, _ = _durable_decision(
        "QQQ",
        expiration=SLOT.date() + timedelta(days=6),
        first_con_id=201,
    )
    snapshot = SimpleNamespace(
        scan_run_id="after-hours-formal.partial-error",
        observed_at=SLOT - timedelta(hours=17),
        snapshot_hash="b" * 64,
        decisions=(valid, expired),
    )

    class Store:
        def latest(self):
            return snapshot

    class FailingFallback:
        def resolve_top10(self, *, scheduled_for):
            assert scheduled_for == SLOT
            raise RuntimeError("bounded fallback failed")

    result = DurableOptionPoolTop10StructureSource(
        Store(),
        fallback=FailingFallback(),
    ).resolve_top10(scheduled_for=SLOT)

    assert tuple(item.candidate.underlying for item in result.structures) == ("SPY",)
    assert result.reason_codes == ()
    assert result.missing_symbols == ()


def test_durable_option_pool_source_fails_closed_on_ledger_corruption() -> None:
    class Store:
        def latest(self):
            raise RuntimeError("append-only ledger integrity failed")

    class ForbiddenFallback:
        def resolve_top10(self, *, scheduled_for):
            raise AssertionError(f"corrupt durable pool must not fall back: {scheduled_for}")

    result = DurableOptionPoolTop10StructureSource(
        Store(),
        fallback=ForbiddenFallback(),
    ).resolve_top10(scheduled_for=SLOT)

    assert result.structures == ()
    assert result.reason_codes == ("OPTION_POOL_LEDGER_INVALID",)


@pytest.mark.parametrize(
    "static_reason",
    (
        "EQUITY_THESIS_EVIDENCE_UNAVAILABLE",
        "THESIS_INVALIDATION_EVIDENCE_INCOMPLETE",
        "ASSIGNMENT_AND_EX_DIVIDEND_EVIDENCE_INCOMPLETE",
    ),
)
def test_durable_option_pool_source_rejects_static_evidence_gaps_without_fallback(
    static_reason: str,
) -> None:
    decision, _, _ = _durable_decision(
        "SPY",
        include_thesis=static_reason != "EQUITY_THESIS_EVIDENCE_UNAVAILABLE",
        reason_codes=(
            "AFTER_HOURS_EXACT_IDENTITY_RESEARCH_ONLY",
            "EXECUTABLE_LEG_QUOTE_INCOMPLETE",
            static_reason,
        ),
    )
    snapshot = SimpleNamespace(
        scan_run_id="after-hours-formal.static-gap",
        observed_at=SLOT - timedelta(hours=17),
        snapshot_hash="1" * 64,
        decisions=(decision,),
    )

    class Store:
        def latest(self):
            return snapshot

    class ForbiddenFallback:
        def resolve_top10(self, *, scheduled_for):
            raise AssertionError(f"static durable gap must not fall back: {scheduled_for}")

    result = DurableOptionPoolTop10StructureSource(
        Store(),
        fallback=ForbiddenFallback(),
    ).resolve_top10(scheduled_for=SLOT)

    assert result.structures == ()
    assert result.reason_codes[0] == "OPTION_POOL_STATIC_EVIDENCE_INCOMPLETE"
    assert static_reason in result.reason_codes
    assert result.missing_symbols == ("SPY",)


def test_durable_option_pool_source_requires_hash_bound_thesis_identity() -> None:
    decision, _, _ = _durable_decision(
        "SPY",
        include_payload_thesis_hash=False,
    )
    snapshot = SimpleNamespace(
        scan_run_id="after-hours-formal.missing-thesis-hash",
        observed_at=SLOT - timedelta(hours=17),
        snapshot_hash="2" * 64,
        decisions=(decision,),
    )

    class Store:
        def latest(self):
            return snapshot

    class ForbiddenFallback:
        def resolve_top10(self, *, scheduled_for):
            raise AssertionError(f"unbound thesis must not fall back: {scheduled_for}")

    result = DurableOptionPoolTop10StructureSource(
        Store(),
        fallback=ForbiddenFallback(),
    ).resolve_top10(scheduled_for=SLOT)

    assert result.structures == ()
    assert result.reason_codes == (
        "OPTION_POOL_STATIC_EVIDENCE_INCOMPLETE",
        "EQUITY_THESIS_HASH_INVALID",
    )
    assert result.missing_symbols == ("SPY",)


def test_durable_option_pool_source_marks_direct_fallback_thesis_unbound() -> None:
    decision, _, _ = _durable_decision("SPY")
    durable_snapshot = SimpleNamespace(
        scan_run_id="after-hours-formal.bound-source",
        observed_at=SLOT - timedelta(hours=17),
        snapshot_hash="3" * 64,
        decisions=(decision,),
    )

    class DurableStore:
        def latest(self):
            return durable_snapshot

    class ForbiddenFallback:
        def resolve_top10(self, *, scheduled_for):
            raise AssertionError(f"valid durable source fell back: {scheduled_for}")

    resolved = DurableOptionPoolTop10StructureSource(
        DurableStore(),
        fallback=ForbiddenFallback(),
    ).resolve_top10(scheduled_for=SLOT).structures[0]

    class EmptyStore:
        def latest(self):
            return None

    class DirectFallback:
        def resolve_top10(self, *, scheduled_for):
            assert scheduled_for == SLOT
            return Top10StructureResolution((resolved,))

    result = DurableOptionPoolTop10StructureSource(
        EmptyStore(),
        fallback=DirectFallback(),
    ).resolve_top10(scheduled_for=SLOT)

    assert result.structures == (resolved,)
    assert result.reason_codes == (
        "PREMARKET_EQUITY_THESIS_SOURCE_UNAVAILABLE",
    )
    assert result.missing_symbols == ()
