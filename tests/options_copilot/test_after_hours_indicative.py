"""After-hours option marks remain indicative and fail closed."""

from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

from options_copilot.after_hours_indicative import (
    AfterHoursIndicativeStore,
    after_hours_candidate_cache_identity,
    after_hours_campaign_progress_status,
    build_after_hours_indicative_read_model,
)
from options_copilot.gateway import (
    BatchedOptionQuote,
    BrokerConnectionError,
    MarketDataPacingError,
    OptionQuoteBatch,
    QuoteBatchStatus,
)
from options_copilot.runtime import (
    _after_hours_cache_fresh,
    _after_hours_identity,
    _project_after_hours_passive_freshness,
    _after_hours_research_row_from_cached,
    _after_hours_research_from_resolution,
    _should_replace_after_hours_best,
)
from types import SimpleNamespace


NOW = datetime(2026, 8, 12, 1, 0, tzinfo=timezone.utc)


def test_campaign_progress_accepts_bounded_partial_and_rejects_forged_counts() -> None:
    partial = {
        "campaign": {
            "completed_underlyings": 8,
            "target_underlyings": 10,
            "remaining_underlyings": 2,
        }
    }

    assert after_hours_campaign_progress_status(partial) == "PARTIAL"
    assert after_hours_campaign_progress_status(
        {
            "campaign": {
                "completed_underlyings": 10,
                "target_underlyings": 10,
                "remaining_underlyings": 0,
            }
        }
    ) == "COMPLETE"
    for forged in (
        {**partial, "campaign": {**partial["campaign"], "remaining_underlyings": 1}},
        {**partial, "campaign": {**partial["campaign"], "completed_underlyings": 11}},
        {**partial, "campaign": {**partial["campaign"], "completed_underlyings": 0}},
        {**partial, "campaign": {**partial["campaign"], "target_underlyings": True}},
    ):
        assert after_hours_campaign_progress_status(forged) is None


def test_runtime_retention_helpers_bind_exact_contracts_and_expire() -> None:
    model = build_after_hours_indicative_read_model(
        _research(),
        quote_provider=_Quotes(),
        strategy_nav_usd=Decimal("2000"),
        clock=lambda: NOW,
    )

    assert len(_after_hours_identity(model)) == 1
    assert _after_hours_cache_fresh(model, now=NOW)
    assert _after_hours_cache_fresh(
        model,
        now=NOW.replace(hour=20),
    ) is False
    assert _after_hours_identity(
        {"reason_codes": ["PACING_COOLDOWN_ACTIVE"], "candidates": []}
    ) == ()


def test_runtime_recovers_exact_research_identity_from_legacy_cache() -> None:
    cached = build_after_hours_indicative_read_model(
        _research(),
        quote_provider=_Quotes(),
        strategy_nav_usd=Decimal("2000"),
    )["candidates"][0]
    for leg in cached["legs"]:
        leg.pop("contract_id_ex", None)
        leg.pop("exchange", None)
        leg.pop("trading_class", None)
        leg.pop("multiplier", None)

    restored = _after_hours_research_row_from_cached(cached)

    assert restored is not None
    assert restored["legs"][0]["contract_id_ex"] == "101@SMART"
    assert restored["legs"][0]["exchange"] == "SMART"
    assert restored["legs"][0]["trading_class"] == "AAPL"
    assert restored["legs"][0]["multiplier"] == 100
    assert restored["legs"][0]["ratio"] == 1


def test_runtime_cached_research_round_trip_preserves_ratio_and_rejects_malformed() -> None:
    cached = build_after_hours_indicative_read_model(
        _research(),
        quote_provider=_Quotes(),
        strategy_nav_usd=Decimal("2000"),
        clock=lambda: NOW,
    )["candidates"][0]
    cached["legs"][0]["ratio"] = 2

    restored = _after_hours_research_row_from_cached(cached)

    assert restored is not None
    assert [leg["ratio"] for leg in restored["legs"]] == [2, 1]
    for invalid in (True, 1.5, 0, -1, "bad"):
        malformed = deepcopy(cached)
        malformed["legs"][0]["ratio"] = invalid
        assert _after_hours_research_row_from_cached(malformed) is None


def test_after_hours_store_survives_restart_and_detects_tampering(tmp_path) -> None:
    path = tmp_path / "after-hours.json"
    store = AfterHoursIndicativeStore(path)
    payload = build_after_hours_indicative_read_model(
        _research(),
        quote_provider=_Quotes(),
        strategy_nav_usd=Decimal("2000"),
        clock=lambda: NOW,
    )

    store.write(payload)
    assert store.read() == payload

    body = path.read_text(encoding="utf-8").replace(
        '"priced_count":1',
        '"priced_count":0',
    )
    path.write_text(body, encoding="utf-8")
    try:
        store.read()
    except RuntimeError as exc:
        assert "hash mismatch" in str(exc)
    else:
        raise AssertionError("tampered cache must fail closed")


def test_after_hours_store_commit_guard_leaves_no_late_cache(tmp_path) -> None:
    path = tmp_path / "after-hours.json"
    payload = build_after_hours_indicative_read_model(
        _research(),
        quote_provider=_Quotes(),
        strategy_nav_usd=Decimal("2000"),
    )
    try:
        AfterHoursIndicativeStore(path).write(
            payload,
            commit_guard=lambda: False,
        )
    except TimeoutError:
        pass
    else:
        raise AssertionError("cancelled cache commit must fail")
    assert not path.exists()


def test_runtime_resolution_projection_maps_call_put_to_ibkr_right_codes() -> None:
    candidate = SimpleNamespace(
        as_dict=lambda: {
            "preselection_id": "research.xlf",
            "underlying": "XLF",
            "legs": [
                {
                    "side": "BUY",
                    "con_id": 101,
                    "local_symbol": "XLF CALL 50",
                    "expiry": "2026-08-28",
                    "strike": "50",
                    "right": "CALL",
                    "exchange": "SMART",
                    "trading_class": "XLF",
                    "multiplier": 100,
                },
                {
                    "side": "SELL",
                    "con_id": 102,
                    "local_symbol": "XLF CALL 51",
                    "expiry": "2026-08-28",
                    "strike": "51",
                    "right": "CALL",
                    "exchange": "SMART",
                    "trading_class": "XLF",
                    "multiplier": 100,
                },
            ],
        }
    )
    resolution = SimpleNamespace(
        structures=(SimpleNamespace(candidate=candidate),),
        reason_codes=(),
        missing_symbols=(),
    )

    payload = _after_hours_research_from_resolution(
        resolution,
        metadata=(
            {"symbol": "XLF", "source_scan": "CORE_UNIVERSE"},
        ),
    )

    assert payload["candidates"][0]["strategy_type"] == "BULL_CALL_VERTICAL"
    assert [leg["right"] for leg in payload["candidates"][0]["legs"]] == ["C", "C"]
    assert payload["candidates"][0]["sector"] == "FINANCIALS"


def test_runtime_resolution_projection_preserves_multi_leg_strategy_and_ratios() -> None:
    candidate = SimpleNamespace(
        as_dict=lambda: {
            "preselection_id": "research.spy.condor",
            "underlying": "SPY",
            "strategy_type": "IRON_CONDOR",
            "legs": [
                {
                    "side": side,
                    "ratio": ratio,
                    "con_id": 200 + index,
                    "local_symbol": f"SPY LEG {index}",
                    "expiry": "2026-08-28",
                    "strike": strike,
                    "right": right,
                    "exchange": "SMART",
                    "trading_class": "SPY",
                    "multiplier": 100,
                }
                for index, (side, ratio, strike, right) in enumerate(
                    (
                        ("BUY", 1, "90", "PUT"),
                        ("SELL", 1, "95", "PUT"),
                        ("SELL", 1, "105", "CALL"),
                        ("BUY", 1, "110", "CALL"),
                    ),
                    start=1,
                )
            ],
        }
    )
    resolution = SimpleNamespace(
        structures=(SimpleNamespace(candidate=candidate),),
        reason_codes=(),
        missing_symbols=(),
    )

    payload = _after_hours_research_from_resolution(
        resolution,
        metadata=({"symbol": "SPY", "source_scan": "CORE_UNIVERSE"},),
    )

    row = payload["candidates"][0]
    assert row["strategy_type"] == "IRON_CONDOR"
    assert len(row["legs"]) == 4
    assert [leg["ratio"] for leg in row["legs"]] == [1, 1, 1, 1]


def _research() -> dict[str, object]:
    return {
        "candidates": [
            {
                "research_id": "research.aapl",
                "rank": 1,
                "underlying": "AAPL",
                "strategy_type": "BULL_CALL_VERTICAL",
                "sector": "Technology",
                "source_scan": "MOST_ACTIVE",
                "quantity": 1,
                "execution_cost_cap_usd": "10.00",
                "legs": [
                    {
                        "side": "BUY",
                        "contract_id": 101,
                        "contract_id_ex": "101@SMART",
                        "local_symbol": "AAPL CALL 200",
                        "expiration": "2026-08-28",
                        "strike": "200",
                        "right": "C",
                        "exchange": "SMART",
                        "trading_class": "AAPL",
                        "multiplier": 100,
                    },
                    {
                        "side": "SELL",
                        "contract_id": 102,
                        "contract_id_ex": "102@SMART",
                        "local_symbol": "AAPL CALL 205",
                        "expiration": "2026-08-28",
                        "strike": "205",
                        "right": "C",
                        "exchange": "SMART",
                        "trading_class": "AAPL",
                        "multiplier": 100,
                    },
                ],
            }
        ]
    }


def _research_many(symbols: tuple[str, ...]) -> dict[str, object]:
    template = _research()["candidates"][0]
    assert isinstance(template, dict)
    candidates: list[dict[str, object]] = []
    for rank, symbol in enumerate(symbols, start=1):
        legs = template["legs"]
        assert isinstance(legs, list)
        candidates.append(
            {
                **template,
                "research_id": f"research.{symbol.lower()}",
                "rank": rank,
                "underlying": symbol,
                "legs": [
                    {
                        **leg,
                        "contract_id": rank * 100 + index,
                        "contract_id_ex": f"{rank * 100 + index}@SMART",
                        "local_symbol": f"{symbol} LEG {index}",
                        "trading_class": symbol,
                    }
                    for index, leg in enumerate(legs, start=1)
                ],
            }
        )
    return {"candidates": candidates}


class _Quotes:
    def option_indicative_quote_batch(self, contracts):
        assert [item.contract_id for item in contracts] == [101, 102]
        return OptionQuoteBatch(
            batch_id="after-hours-1",
            status=QuoteBatchStatus.COMPLETE,
            requested_at=NOW,
            completed_at=NOW,
            observed_at=NOW,
            source="IBKR_AFTER_HOURS_INDICATIVE_READONLY:REQUESTED_TYPE_4",
            quotes=(
                _quote(101, bid="1.40", ask="1.50", market_data_type=2),
                _quote(102, bid="0.60", ask="0.70", market_data_type=2),
            ),
        )


class _LastOnlyQuotes:
    def option_indicative_quote_batch(self, contracts):
        return OptionQuoteBatch(
            batch_id="after-hours-2",
            status=QuoteBatchStatus.COMPLETE,
            requested_at=NOW,
            completed_at=NOW,
            observed_at=NOW,
            source="IBKR_AFTER_HOURS_INDICATIVE_READONLY:REQUESTED_TYPE_4",
            quotes=tuple(
                _quote(item.contract_id, last="1.10" if index == 0 else "0.50")
                for index, item in enumerate(contracts)
            ),
        )


class _PreviousCloseQuotes:
    def option_indicative_quote_batch(self, contracts):
        return OptionQuoteBatch(
            batch_id="after-hours-historical-close",
            status=QuoteBatchStatus.COMPLETE,
            requested_at=NOW,
            completed_at=NOW,
            observed_at=NOW,
            source=(
                "IBKR_AFTER_HOURS_INDICATIVE_READONLY:REQUESTED_TYPE_4"
                "+HISTORICAL_PREVIOUS_CLOSE_READONLY"
            ),
            quotes=tuple(
                _quote(
                    item.contract_id,
                    close="1.30" if index == 0 else "0.55",
                )
                for index, item in enumerate(contracts)
            ),
        )


class _PreviousTradeQuotes:
    def option_indicative_quote_batch(self, contracts):
        return OptionQuoteBatch(
            batch_id="after-hours-historical-trade",
            status=QuoteBatchStatus.COMPLETE,
            requested_at=NOW,
            completed_at=NOW,
            observed_at=NOW,
            source="IBKR_AFTER_HOURS_INDICATIVE_READONLY+HISTORICAL_OPTION_MARK_READONLY",
            quotes=tuple(
                _quote(
                    item.contract_id,
                    last="1.20" if index == 0 else "0.60",
                    research_price_basis="PREVIOUS_SESSION_LAST_TRADE",
                )
                for index, item in enumerate(contracts)
            ),
        )


class _BlockedQuotes(_Quotes):
    def option_indicative_quote_batch(self, contracts):
        batch = super().option_indicative_quote_batch(contracts)
        return OptionQuoteBatch(
            batch_id=batch.batch_id,
            status=QuoteBatchStatus.PARTIAL,
            requested_at=batch.requested_at,
            completed_at=batch.completed_at,
            observed_at=batch.observed_at,
            source=batch.source,
            quotes=batch.quotes,
            blockers=("INDICATIVE_MARK_SOURCE_DEGRADED",),
        )


class _PartialBboCommonCloseQuotes:
    def option_indicative_quote_batch(self, contracts):
        return OptionQuoteBatch(
            batch_id="after-hours-common-close",
            status=QuoteBatchStatus.COMPLETE,
            requested_at=NOW,
            completed_at=NOW,
            observed_at=NOW,
            source="IBKR_AFTER_HOURS_INDICATIVE_READONLY",
            quotes=(
                _quote(contracts[0].contract_id, bid="1.80", ask="5.60", close="2.00"),
                _quote(contracts[1].contract_id, ask="4.80", close="1.81"),
            ),
        )


class _DisconnectedQuotes:
    def option_indicative_quote_batch(self, _contracts):
        raise BrokerConnectionError("private broker detail")


class _PacingDeniedQuotes:
    def option_indicative_quote_batch(self, _contracts):
        raise MarketDataPacingError("streaming_quote", "PACING_CAPABILITY_MISSING")


class _ProgressiveQuotes:
    def __init__(self, *, fail_call: int | None = None, pacing_call: int | None = None):
        self.calls: list[tuple[int, ...]] = []
        self.fail_call = fail_call
        self.pacing_call = pacing_call

    def option_indicative_quote_batch(self, contracts):
        ids = tuple(item.contract_id for item in contracts)
        self.calls.append(ids)
        call = len(self.calls)
        if call == self.fail_call:
            raise TimeoutError("one bounded candidate timed out")
        if call == self.pacing_call:
            raise MarketDataPacingError(
                "historical_option",
                "PACING_REQUEST_WINDOW_EXHAUSTED",
            )
        return OptionQuoteBatch(
            batch_id=f"candidate-batch-{call}",
            status=QuoteBatchStatus.COMPLETE,
            requested_at=NOW,
            completed_at=NOW,
            observed_at=NOW,
            source="IBKR_AFTER_HOURS_INDICATIVE_READONLY",
            quotes=(
                _quote(ids[0], last="1.10"),
                _quote(ids[1], last="0.50"),
            ),
        )


def _quote(
    contract_id: int,
    *,
    bid: str | None = None,
    ask: str | None = None,
    last: str | None = None,
    close: str | None = None,
    market_data_type: int | None = 4,
    research_price_basis: str | None = None,
) -> BatchedOptionQuote:
    return BatchedOptionQuote(
        contract_id=contract_id,
        batch_id="after-hours",
        request_id=str(contract_id),
        requested_at=NOW,
        observed_at=NOW,
        completed_at=NOW,
        source="IBKR_AFTER_HOURS_INDICATIVE_READONLY",
        bid=None if bid is None else Decimal(bid),
        ask=None if ask is None else Decimal(ask),
        last=None if last is None else Decimal(last),
        close=None if close is None else Decimal(close),
        exchange_time=NOW,
        market_data_type=market_data_type,
        research_price_basis=research_price_basis,
    )


def test_after_hours_frozen_bbo_calculates_conservative_debit_and_risk() -> None:
    model = build_after_hours_indicative_read_model(
        _research(),
        quote_provider=_Quotes(),
        strategy_nav_usd=Decimal("2000"),
        clock=lambda: NOW,
    )

    candidate = model["candidates"][0]
    assert model["status"] == "AVAILABLE"
    assert model["decision"] == "NO_TRADE"
    assert model["approval_eligible"] is False
    assert candidate["indicative_entry_debit_usd"] == "90.00"
    assert candidate["indicative_maximum_loss_usd"] == "100.00"
    assert candidate["indicative_maximum_profit_usd"] == "400.00"
    assert candidate["breakeven_price"] == "201.00"
    assert candidate["strategy_nav_fraction"] == "0.05"
    assert candidate["indicative_price_basis"] == "FROZEN_BBO"
    assert candidate["direction"] == "BULLISH"
    assert candidate["sector"] == "Technology"
    assert candidate["source_scan"] == "MOST_ACTIVE"
    assert candidate["research_summary"] is None
    assert candidate["entry_condition"] is None
    assert candidate["trade_status"] == "NO_TRADE"


def test_after_hours_dte_uses_new_york_trading_date_across_utc_midnight() -> None:
    before_new_york_midnight = datetime(
        2026,
        8,
        12,
        3,
        59,
        tzinfo=timezone.utc,
    )
    after_new_york_midnight = before_new_york_midnight + timedelta(minutes=2)

    before = build_after_hours_indicative_read_model(
        _research(),
        quote_provider=_Quotes(),
        strategy_nav_usd=Decimal("2000"),
        clock=lambda: before_new_york_midnight,
    )["candidates"][0]
    after = build_after_hours_indicative_read_model(
        _research(),
        quote_provider=_Quotes(),
        strategy_nav_usd=Decimal("2000"),
        clock=lambda: after_new_york_midnight,
    )["candidates"][0]

    assert before["dte"] == 17
    assert [leg["dte"] for leg in before["legs"]] == [17, 17]
    assert after["dte"] == 16
    assert [leg["dte"] for leg in after["legs"]] == [16, 16]


def test_after_hours_expired_contract_dte_fails_closed() -> None:
    after_expiration = datetime(2026, 8, 29, 4, 1, tzinfo=timezone.utc)

    candidate = build_after_hours_indicative_read_model(
        _research(),
        quote_provider=_Quotes(),
        strategy_nav_usd=Decimal("2000"),
        clock=lambda: after_expiration,
    )["candidates"][0]

    assert candidate["dte"] is None
    assert [leg["dte"] for leg in candidate["legs"]] == [None, None]
    assert "OPTION_CONTRACT_EXPIRED" in candidate["blockers"]
    assert candidate["pricing_status"] == "UNAVAILABLE"
    assert candidate["indicative_entry_debit_usd"] is None
    assert candidate["trade_status"] == "NO_TRADE"


def test_after_hours_mixed_expirations_do_not_claim_candidate_dte() -> None:
    research = _research()
    research["candidates"][0]["legs"][1]["expiration"] = "2026-09-04"

    candidate = build_after_hours_indicative_read_model(
        research,
        quote_provider=_Quotes(),
        strategy_nav_usd=Decimal("2000"),
        clock=lambda: datetime(2026, 8, 12, 4, 1, tzinfo=timezone.utc),
    )["candidates"][0]

    assert candidate["dte"] is None
    assert [leg["dte"] for leg in candidate["legs"]] == [16, 23]
    assert "OPTION_EXPIRATION_MISMATCH" in candidate["blockers"]
    assert candidate["pricing_status"] == "UNAVAILABLE"
    assert candidate["trade_status"] == "NO_TRADE"


def test_after_hours_same_basis_last_marks_can_calculate_without_claiming_bbo() -> None:
    model = build_after_hours_indicative_read_model(
        _research(),
        quote_provider=_LastOnlyQuotes(),
        strategy_nav_usd=Decimal("2000"),
        clock=lambda: NOW,
    )

    candidate = model["candidates"][0]
    assert candidate["indicative_entry_debit_usd"] == "60.00"
    assert candidate["indicative_price_basis"] == "LAST"
    assert candidate["approval_eligible"] is False
    assert candidate["order_allowed"] is False


def test_after_hours_previous_closes_fill_each_leg_cost_without_trade_authority() -> None:
    model = build_after_hours_indicative_read_model(
        _research(),
        quote_provider=_PreviousCloseQuotes(),
        strategy_nav_usd=Decimal("2000"),
        clock=lambda: NOW,
    )

    candidate = model["candidates"][0]
    assert model["status"] == "AVAILABLE"
    assert model["decision"] == "NO_TRADE"
    assert candidate["indicative_entry_debit_usd"] == "75.00"
    assert candidate["indicative_maximum_loss_usd"] == "85.00"
    assert candidate["strategy_nav_fraction"] == "0.0425"
    assert candidate["indicative_price_basis"] == "PREVIOUS_CLOSE"
    assert [leg["price_basis"] for leg in candidate["legs"]] == [
        "PREVIOUS_CLOSE",
        "PREVIOUS_CLOSE",
    ]
    assert candidate["decision_authority"] == "SUPPORTING_ONLY"
    assert candidate["approval_eligible"] is False
    assert candidate["order_allowed"] is False


def test_after_hours_previous_session_trades_are_explicitly_non_executable() -> None:
    model = build_after_hours_indicative_read_model(
        _research(),
        quote_provider=_PreviousTradeQuotes(),
        strategy_nav_usd=Decimal("2000"),
        clock=lambda: NOW,
    )

    candidate = model["candidates"][0]
    assert candidate["indicative_entry_debit_usd"] == "60.00"
    assert candidate["indicative_maximum_loss_usd"] == "70.00"
    assert candidate["indicative_price_basis"] == "PREVIOUS_SESSION_LAST_TRADE"
    assert model["decision"] == "NO_TRADE"
    assert candidate["decision_authority"] == "SUPPORTING_ONLY"
    assert candidate["order_allowed"] is False


def test_after_hours_priced_candidate_exposes_non_executable_boundary() -> None:
    model = build_after_hours_indicative_read_model(
        _research(),
        quote_provider=_Quotes(),
        strategy_nav_usd=Decimal("2000"),
        clock=lambda: NOW,
    )

    candidate = model["candidates"][0]
    assert model["status"] == "AVAILABLE"
    assert candidate["pricing_status"] == "AVAILABLE"
    assert candidate["indicative_entry_debit_usd"] == "90.00"
    assert candidate["indicative_maximum_loss_usd"] == "100.00"
    assert candidate["quote_status"] == "UNAVAILABLE"
    assert candidate["greeks_status"] == "UNAVAILABLE"
    assert candidate["liquidity_status"] == "UNAVAILABLE"
    assert candidate["indicative_cost_after_ev_usd"] is None
    assert candidate["blockers"] == [
        "AFTER_HOURS_RESEARCH_ONLY",
        "FRESH_EXECUTABLE_OPTION_EVIDENCE_REQUIRED",
        "EXECUTABLE_LEG_QUOTE_INCOMPLETE",
        "OPTION_GREEKS_INCOMPLETE",
        "OPTION_LIQUIDITY_EVIDENCE_INCOMPLETE",
        "STRUCTURE_PAYOFF_EVIDENCE_INCOMPLETE",
        "AFTER_COST_ECONOMICS_INCOMPLETE",
    ]
    assert candidate["trade_status"] == "NO_TRADE"


def test_generic_debit_call_vertical_derives_bullish_structure_direction() -> None:
    research = _research()
    research["candidates"][0]["strategy_type"] = "DEBIT_VERTICAL"

    candidate = build_after_hours_indicative_read_model(
        research,
        quote_provider=_Quotes(),
        strategy_nav_usd=Decimal("2000"),
        clock=lambda: NOW,
    )["candidates"][0]

    assert candidate["strategy_type"] == "DEBIT_VERTICAL"
    assert candidate["direction"] == "BULLISH"
    assert candidate["trade_status"] == "NO_TRADE"


def test_generic_debit_put_vertical_derives_bearish_structure_direction() -> None:
    research = _research()
    candidate_row = research["candidates"][0]
    candidate_row["strategy_type"] = "DEBIT_VERTICAL"
    buy_leg, sell_leg = candidate_row["legs"]
    buy_leg.update({"right": "P", "strike": "205", "local_symbol": "AAPL PUT 205"})
    sell_leg.update({"right": "P", "strike": "200", "local_symbol": "AAPL PUT 200"})

    candidate = build_after_hours_indicative_read_model(
        research,
        quote_provider=_Quotes(),
        strategy_nav_usd=Decimal("2000"),
        clock=lambda: NOW,
    )["candidates"][0]

    assert candidate["strategy_type"] == "DEBIT_VERTICAL"
    assert candidate["direction"] == "BEARISH"
    assert candidate["indicative_maximum_profit_usd"] == "400.00"
    assert candidate["breakeven_price"] == "204.00"
    assert candidate["trade_status"] == "NO_TRADE"


def test_generic_debit_vertical_keeps_ambiguous_legs_direction_unverified() -> None:
    research = _research()
    candidate_row = research["candidates"][0]
    candidate_row["strategy_type"] = "DEBIT_VERTICAL"
    candidate_row["legs"][1]["right"] = "P"

    candidate = build_after_hours_indicative_read_model(
        research,
        quote_provider=_Quotes(),
        strategy_nav_usd=Decimal("2000"),
    )["candidates"][0]

    assert candidate["direction"] == "NEUTRAL_OR_UNSPECIFIED"
    assert "INDICATIVE_VERTICAL_GEOMETRY_INVALID" in candidate["blockers"]
    assert candidate["indicative_maximum_profit_usd"] is None
    assert candidate["breakeven_price"] is None
    assert candidate["trade_status"] == "NO_TRADE"


def test_after_hours_nonpositive_after_cost_upside_is_explicitly_blocked() -> None:
    research = _research()
    research["candidates"][0]["execution_cost_cap_usd"] = "450.00"

    candidate = build_after_hours_indicative_read_model(
        research,
        quote_provider=_Quotes(),
        strategy_nav_usd=Decimal("10000"),
        clock=lambda: NOW,
    )["candidates"][0]

    assert candidate["indicative_entry_debit_usd"] == "90.00"
    assert candidate["indicative_maximum_loss_usd"] == "540.00"
    assert candidate["indicative_maximum_profit_usd"] == "-40.00"
    assert candidate["breakeven_price"] == "205.40"
    assert "INDICATIVE_AFTER_COST_UPSIDE_NONPOSITIVE" in candidate["blockers"]
    assert candidate["trade_status"] == "NO_TRADE"


def test_after_hours_batch_blocker_degrades_even_when_all_legs_are_priced() -> None:
    model = build_after_hours_indicative_read_model(
        _research(),
        quote_provider=_BlockedQuotes(),
        strategy_nav_usd=Decimal("2000"),
        clock=lambda: NOW,
    )

    candidate = model["candidates"][0]
    assert model["status"] == "DEGRADED"
    assert model["reason_codes"] == ["INDICATIVE_MARK_SOURCE_DEGRADED"]
    assert candidate["pricing_status"] == "AVAILABLE"
    assert candidate["indicative_price_basis"] == "FROZEN_BBO"


def test_after_hours_risk_blocker_preserves_same_price_basis() -> None:
    model = build_after_hours_indicative_read_model(
        _research(),
        quote_provider=_Quotes(),
        strategy_nav_usd=Decimal("500"),
        clock=lambda: NOW,
    )

    candidate = model["candidates"][0]
    assert "INDICATIVE_RISK_CAP_EXCEEDED" in candidate["blockers"]
    assert candidate["indicative_price_basis"] == "FROZEN_BBO"


def test_after_hours_uses_common_close_instead_of_mixing_leg_bases() -> None:
    model = build_after_hours_indicative_read_model(
        _research(),
        quote_provider=_PartialBboCommonCloseQuotes(),
        strategy_nav_usd=Decimal("2000"),
        clock=lambda: NOW,
    )

    candidate = model["candidates"][0]
    assert candidate["pricing_status"] == "AVAILABLE"
    assert candidate["indicative_entry_debit_usd"] == "19.00"
    assert candidate["indicative_price_basis"] == "PREVIOUS_CLOSE"
    assert [leg["price_basis"] for leg in candidate["legs"]] == [
        "PREVIOUS_CLOSE",
        "PREVIOUS_CLOSE",
    ]


def test_after_hours_reports_disconnected_broker_without_leaking_exception() -> None:
    model = build_after_hours_indicative_read_model(
        _research(),
        quote_provider=_DisconnectedQuotes(),
        strategy_nav_usd=Decimal("2000"),
    )

    assert model["reason_codes"] == ["IBKR_READONLY_GATEWAY_DISCONNECTED"]
    assert "private broker detail" not in str(model)


def test_after_hours_reports_precise_pacing_blocker() -> None:
    model = build_after_hours_indicative_read_model(
        _research(),
        quote_provider=_PacingDeniedQuotes(),
        strategy_nav_usd=Decimal("2000"),
    )

    assert model["reason_codes"] == ["PACING_CAPABILITY_MISSING"]


def test_after_hours_prices_each_candidate_in_its_own_bounded_batch() -> None:
    provider = _ProgressiveQuotes(fail_call=1)

    model = build_after_hours_indicative_read_model(
        _research_many(("AAA", "BBB", "CCC")),
        quote_provider=provider,
        strategy_nav_usd=Decimal("2000"),
        clock=lambda: NOW,
    )

    assert provider.calls == [(101, 102), (201, 202), (301, 302)]
    assert [item["pricing_status"] for item in model["candidates"]] == [
        "UNAVAILABLE",
        "AVAILABLE",
        "AVAILABLE",
    ]
    assert model["priced_count"] == 2
    assert model["quote_batch_count"] == 2
    assert model["decision"] == "NO_TRADE"


def test_after_hours_stops_on_pacing_and_preserves_completed_candidates() -> None:
    provider = _ProgressiveQuotes(pacing_call=2)

    model = build_after_hours_indicative_read_model(
        _research_many(("AAA", "BBB", "CCC")),
        quote_provider=provider,
        strategy_nav_usd=Decimal("2000"),
        clock=lambda: NOW,
    )

    assert provider.calls == [(101, 102), (201, 202)]
    assert [item["pricing_status"] for item in model["candidates"]] == [
        "AVAILABLE",
        "UNAVAILABLE",
        "UNAVAILABLE",
    ]
    assert "PACING_REQUEST_WINDOW_EXHAUSTED" in model["reason_codes"]
    assert model["attempted_candidate_count"] == 2


def test_after_hours_next_heartbeat_reuses_prices_and_only_fills_gaps() -> None:
    first_provider = _ProgressiveQuotes(pacing_call=2)
    research = _research_many(("AAA", "BBB", "CCC"))
    first = build_after_hours_indicative_read_model(
        research,
        quote_provider=first_provider,
        strategy_nav_usd=Decimal("2000"),
        clock=lambda: NOW,
    )
    second_provider = _ProgressiveQuotes()

    second = build_after_hours_indicative_read_model(
        research,
        quote_provider=second_provider,
        strategy_nav_usd=Decimal("2000"),
        previous_read_model=first,
        clock=lambda: NOW + timedelta(minutes=5),
    )

    assert second_provider.calls == [(201, 202), (301, 302)]
    assert second["priced_count"] == 3
    assert second["reused_priced_count"] == 1
    assert second["status"] == "AVAILABLE"
    assert second["decision"] == "NO_TRADE"


def test_after_hours_fresh_cache_reuse_preserves_original_observation_time() -> None:
    first = build_after_hours_indicative_read_model(
        _research(),
        quote_provider=_Quotes(),
        strategy_nav_usd=Decimal("2000"),
        clock=lambda: NOW,
    )
    provider = _ProgressiveQuotes()

    second = build_after_hours_indicative_read_model(
        _research(),
        quote_provider=provider,
        strategy_nav_usd=Decimal("2000"),
        previous_read_model=first,
        clock=lambda: NOW + timedelta(minutes=5),
    )

    assert provider.calls == []
    assert second["observed_at"] == first["observed_at"] == NOW.isoformat()
    assert second["freshness_status"] == "CURRENT"
    assert second["reused_priced_count"] == 1


def test_after_hours_ratio_change_invalidates_cache_and_preserves_lineage() -> None:
    first = build_after_hours_indicative_read_model(
        _research(),
        quote_provider=_Quotes(),
        strategy_nav_usd=Decimal("2000"),
        clock=lambda: NOW,
    )
    changed = _research()
    changed["candidates"][0]["legs"][0]["ratio"] = 2
    provider = _ProgressiveQuotes()

    second = build_after_hours_indicative_read_model(
        changed,
        quote_provider=provider,
        strategy_nav_usd=Decimal("2000"),
        previous_read_model=first,
        clock=lambda: NOW + timedelta(minutes=5),
    )

    assert provider.calls == [(101, 102)]
    assert second["reused_priced_count"] == 0
    candidate = second["candidates"][0]
    assert [leg["ratio"] for leg in candidate["legs"]] == [2, 1]
    assert candidate["pricing_status"] == "UNAVAILABLE"
    assert candidate["indicative_entry_debit_usd"] is None
    assert "INDICATIVE_VERTICAL_RATIO_UNSUPPORTED" in candidate["blockers"]


def test_cached_marks_recalculate_current_quantity_cost_nav_and_risk() -> None:
    first = build_after_hours_indicative_read_model(
        _research(),
        quote_provider=_Quotes(),
        strategy_nav_usd=Decimal("2000"),
        clock=lambda: NOW,
    )
    changed = _research()
    changed["candidates"][0]["quantity"] = 2
    changed["candidates"][0]["execution_cost_cap_usd"] = "25.00"
    provider = _ProgressiveQuotes()

    second = build_after_hours_indicative_read_model(
        changed,
        quote_provider=provider,
        strategy_nav_usd=Decimal("1000"),
        previous_read_model=first,
        clock=lambda: NOW + timedelta(minutes=5),
    )

    assert provider.calls == []
    candidate = second["candidates"][0]
    assert candidate["quantity"] == 2
    assert candidate["indicative_entry_debit_usd"] == "180.00"
    assert candidate["execution_cost_cap_usd"] == "25.00"
    assert candidate["indicative_maximum_loss_usd"] == "205.00"
    assert candidate["strategy_nav_fraction"] == "0.205"
    assert "INDICATIVE_RISK_CAP_EXCEEDED" in candidate["blockers"]


def test_unsupported_ratio_reuses_marks_without_repeating_broker_request() -> None:
    research = _research()
    research["candidates"][0]["legs"][0]["ratio"] = 2
    first_provider = _ProgressiveQuotes()
    first = build_after_hours_indicative_read_model(
        research,
        quote_provider=first_provider,
        strategy_nav_usd=Decimal("2000"),
        clock=lambda: NOW,
    )
    second_provider = _ProgressiveQuotes()

    second = build_after_hours_indicative_read_model(
        research,
        quote_provider=second_provider,
        strategy_nav_usd=Decimal("2000"),
        previous_read_model=first,
        clock=lambda: NOW + timedelta(minutes=5),
    )

    assert first_provider.calls == [(101, 102)]
    assert second_provider.calls == []
    assert second["reused_mark_evidence_count"] == 1
    assert second["mark_evidence_count"] == 1
    assert second["candidates"][0]["mark_evidence_status"] == "AVAILABLE"
    assert second["candidates"][0]["pricing_status"] == "UNAVAILABLE"
    assert second["decision"] == "NO_TRADE"
    assert second["approval_eligible"] is False
    assert second["instruction_creation_allowed"] is False
    assert second["order_allowed"] is False

    stale = _project_after_hours_passive_freshness(
        second,
        now=NOW + timedelta(minutes=21),
    )
    assert stale["candidates"][0]["pricing_status"] == "UNAVAILABLE"
    assert stale["candidates"][0]["mark_evidence_status"] == "STALE"
    assert stale["candidates"][0]["freshness_status"] == "STALE"
    assert "AFTER_HOURS_CACHED_MARKS_STALE" in stale["candidates"][0]["blockers"]


def test_unsupported_front_candidate_does_not_starve_later_candidate() -> None:
    research = _research_many(("AAA", "BBB"))
    research["candidates"][0]["legs"][0]["ratio"] = 2
    first_provider = _ProgressiveQuotes()
    first = build_after_hours_indicative_read_model(
        research,
        quote_provider=first_provider,
        strategy_nav_usd=Decimal("2000"),
        maximum_quote_attempts=1,
        clock=lambda: NOW,
    )
    second_provider = _ProgressiveQuotes()

    second = build_after_hours_indicative_read_model(
        research,
        quote_provider=second_provider,
        strategy_nav_usd=Decimal("2000"),
        maximum_quote_attempts=1,
        previous_read_model=first,
        clock=lambda: NOW + timedelta(minutes=5),
    )

    assert first_provider.calls == [(101, 102)]
    assert second_provider.calls == [(201, 202)]
    assert [item["mark_evidence_status"] for item in second["candidates"]] == [
        "AVAILABLE",
        "AVAILABLE",
    ]
    assert second["candidates"][1]["pricing_status"] == "AVAILABLE"


def test_invalid_exact_ratios_fail_before_any_broker_request() -> None:
    for invalid in (True, 1.5, 0, -1, "bad"):
        research = _research()
        research["candidates"][0]["legs"][0]["ratio"] = invalid
        provider = _ProgressiveQuotes()

        result = build_after_hours_indicative_read_model(
            research,
            quote_provider=provider,
            strategy_nav_usd=Decimal("2000"),
            clock=lambda: NOW,
        )

        assert provider.calls == []
        assert result["reason_codes"] == ["RESEARCH_LEG_RATIO_INVALID"]
        assert result["decision"] == "NO_TRADE"
        assert result["approval_eligible"] is False
        assert result["instruction_creation_allowed"] is False
        assert result["order_allowed"] is False


def test_runtime_outer_retention_never_overlays_old_one_to_one_economics() -> None:
    best = build_after_hours_indicative_read_model(
        _research(),
        quote_provider=_Quotes(),
        strategy_nav_usd=Decimal("2000"),
        clock=lambda: NOW,
    )
    changed = _research()
    changed["candidates"][0]["legs"][0]["ratio"] = 2
    latest = build_after_hours_indicative_read_model(
        changed,
        quote_provider=_Quotes(),
        strategy_nav_usd=Decimal("2000"),
        clock=lambda: NOW + timedelta(minutes=1),
    )

    assert best["priced_count"] == 1
    assert latest["priced_count"] == 0
    assert _after_hours_identity(latest) != _after_hours_identity(best)
    assert _should_replace_after_hours_best(
        latest,
        best,
        now=NOW + timedelta(minutes=1),
    )


def test_runtime_outer_retention_never_masks_current_invalid_identity() -> None:
    best = build_after_hours_indicative_read_model(
        _research(),
        quote_provider=_Quotes(),
        strategy_nav_usd=Decimal("2000"),
        clock=lambda: NOW,
    )
    invalid = _research()
    invalid["candidates"][0]["legs"][0]["ratio"] = True
    provider = _ProgressiveQuotes()
    latest = build_after_hours_indicative_read_model(
        invalid,
        quote_provider=provider,
        strategy_nav_usd=Decimal("2000"),
        previous_read_model=best,
        clock=lambda: NOW + timedelta(minutes=1),
    )

    assert provider.calls == []
    assert latest["reason_codes"] == ["RESEARCH_LEG_RATIO_INVALID"]
    assert latest["candidates"] == []
    assert _should_replace_after_hours_best(
        latest,
        best,
        now=NOW + timedelta(minutes=1),
    )


def test_cache_identity_binds_research_strategy_contract_side_and_ratio() -> None:
    base = _research()["candidates"][0]
    original = after_hours_candidate_cache_identity(base)
    mutations = (
        ("research_id", None, "research.changed"),
        ("strategy_type", None, "BEAR_PUT_VERTICAL"),
        ("contract_id", 0, 999),
        ("side", 0, "SELL"),
        ("ratio", 0, 2),
    )

    for field, leg_index, value in mutations:
        changed = deepcopy(base)
        if leg_index is None:
            changed[field] = value
        else:
            changed["legs"][leg_index][field] = value
        assert after_hours_candidate_cache_identity(changed) != original


def test_after_hours_expired_cache_is_stale_and_never_restamped() -> None:
    first = build_after_hours_indicative_read_model(
        _research(),
        quote_provider=_Quotes(),
        strategy_nav_usd=Decimal("2000"),
        clock=lambda: NOW,
    )
    provider = _ProgressiveQuotes(fail_call=1)

    expired = build_after_hours_indicative_read_model(
        _research(),
        quote_provider=provider,
        strategy_nav_usd=Decimal("2000"),
        previous_read_model=first,
        clock=lambda: NOW + timedelta(minutes=16),
    )

    assert provider.calls == [(101, 102)]
    assert expired["status"] == "DEGRADED"
    assert expired["freshness_status"] == "STALE"
    assert expired["observed_at"] == first["observed_at"] == NOW.isoformat()
    assert expired["reused_priced_count"] == 0
    assert expired["stale_reused_count"] == 1
    assert expired["candidates"][0]["pricing_status"] == "STALE"
    assert "AFTER_HOURS_CACHED_MARKS_STALE" in expired["reason_codes"]


def test_passive_after_hours_read_preserves_fresh_cached_marks() -> None:
    cached = build_after_hours_indicative_read_model(
        _research(),
        quote_provider=_Quotes(),
        strategy_nav_usd=Decimal("2000"),
        clock=lambda: NOW,
    )

    projected = _project_after_hours_passive_freshness(
        cached,
        now=NOW + timedelta(minutes=15),
    )

    assert projected == cached
    assert projected["observed_at"] == NOW.isoformat()
    assert projected["freshness_status"] == "CURRENT"
    assert projected["candidates"][0]["pricing_status"] == "AVAILABLE"


def test_passive_after_hours_read_downgrades_retained_cache_after_fifteen_minutes() -> None:
    cached = build_after_hours_indicative_read_model(
        _research(),
        quote_provider=_Quotes(),
        strategy_nav_usd=Decimal("2000"),
        clock=lambda: NOW,
    )

    projected = _project_after_hours_passive_freshness(
        cached,
        now=NOW + timedelta(minutes=15, seconds=1),
    )

    assert projected["status"] == "DEGRADED"
    assert projected["freshness_status"] == "STALE"
    assert projected["observed_at"] == cached["observed_at"] == NOW.isoformat()
    assert projected["decision"] == "NO_TRADE"
    assert projected["decision_authority"] == "SUPPORTING_ONLY"
    assert projected["approval_eligible"] is False
    assert projected["instruction_creation_allowed"] is False
    assert projected["order_allowed"] is False
    assert projected["candidates"][0]["pricing_status"] == "STALE"
    assert projected["candidates"][0]["freshness_status"] == "STALE"
    assert "AFTER_HOURS_CACHED_MARKS_STALE" in projected["reason_codes"]
    assert "AFTER_HOURS_CACHED_MARKS_STALE" in projected["candidates"][0]["blockers"]


def test_after_hours_campaign_bounds_each_pass_and_accumulates_ten() -> None:
    research = _research_many(tuple(f"S{index}" for index in range(1, 11)))
    previous = None
    providers: list[_ProgressiveQuotes] = []
    for _ in range(3):
        provider = _ProgressiveQuotes()
        providers.append(provider)
        previous = build_after_hours_indicative_read_model(
            research,
            quote_provider=provider,
            strategy_nav_usd=Decimal("2000"),
            maximum_quote_attempts=4,
            previous_read_model=previous,
            clock=lambda: NOW,
        )

    assert [len(provider.calls) for provider in providers] == [4, 4, 2]
    assert previous is not None
    assert previous["priced_count"] == 10
    assert previous["status"] == "AVAILABLE"
    assert previous["decision"] == "NO_TRADE"
