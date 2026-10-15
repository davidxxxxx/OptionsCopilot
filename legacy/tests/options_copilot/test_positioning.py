from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal

from options_copilot.analytics.positioning import calculate_positioning
from options_copilot.gateway.ibkr_readonly import OptionContractRef, OptionQuoteSnapshot


NOW = datetime(2026, 8, 3, 14, 0, tzinfo=timezone.utc)


def _quote(strike: str, right: str, oi: int, gamma: str) -> OptionQuoteSnapshot:
    contract = OptionContractRef(
        contract_id=int(strike) * 10 + (1 if right == "C" else 2),
        contract_id_ex=f"{strike}{right}@SMART",
        symbol="SPY",
        local_symbol=f"SPY-{strike}-{right}",
        expiration=date(2026, 8, 21),
        strike=Decimal(strike),
        right=right,
        exchange="SMART",
        trading_class="SPY",
        multiplier=100,
    )
    return OptionQuoteSnapshot(
        contract=contract,
        observed_at=NOW,
        exchange_time=NOW,
        bid=Decimal("1"),
        ask=Decimal("1.1"),
        last=Decimal("1.05"),
        close=Decimal("1"),
        volume=100,
        open_interest=oi,
        implied_volatility=Decimal("0.2"),
        delta=Decimal("0.4"),
        gamma=Decimal(gamma),
        theta=Decimal("-0.03"),
        vega=Decimal("0.1"),
        market_data_type=1,
    )


def test_max_pain_walls_and_gex_are_transparent_supporting_signals() -> None:
    result = calculate_positioning(
        (
            _quote("620", "C", 100, "0.02"),
            _quote("625", "C", 500, "0.03"),
            _quote("630", "C", 200, "0.02"),
            _quote("620", "P", 300, "0.02"),
            _quote("625", "P", 100, "0.03"),
            _quote("630", "P", 600, "0.02"),
        ),
        underlying_spot=Decimal("625"),
    )
    assert result.max_pain == Decimal("625")
    assert result.call_wall == Decimal("625")
    assert result.put_wall == Decimal("630")
    assert result.call_open_interest == 800
    assert result.put_open_interest == 1000
    assert result.put_call_open_interest_ratio == Decimal("1.25")
    assert result.estimated_net_gex_usd_per_one_percent is not None
    assert result.supporting_only is True
    assert "does not identify dealer direction" in result.warning


def test_no_open_interest_returns_no_structural_levels() -> None:
    result = calculate_positioning(
        (_quote("625", "C", 0, "0.02"), _quote("625", "P", 0, "0.02")),
        underlying_spot=Decimal("625"),
    )
    assert result.max_pain is None
    assert result.call_wall is None and result.put_wall is None
    assert result.put_call_open_interest_ratio is None
