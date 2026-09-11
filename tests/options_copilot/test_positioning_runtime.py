from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from options_copilot.analytics.positioning_runtime import (
    PositioningProjectionStatus,
    project_positioning,
)
from options_copilot.gateway.ibkr_readonly import (
    OptionContractRef,
    OptionQuoteSnapshot,
)


NOW = datetime(2026, 8, 5, 14, 0, tzinfo=timezone.utc)
EXPIRATION = date(2026, 8, 21)


def _quote(
    strike: str,
    right: str,
    *,
    observed_at: datetime = NOW - timedelta(seconds=4),
    open_interest: int | None = 100,
    delta: Decimal | None = Decimal("0.4"),
    gamma: Decimal | None = Decimal("0.02"),
    theta: Decimal | None = Decimal("-0.03"),
    vega: Decimal | None = Decimal("0.1"),
    market_data_type: int | None = 1,
    symbol: str = "SPY",
    expiration: date = EXPIRATION,
    contract_id: int | None = None,
) -> OptionQuoteSnapshot:
    numeric_strike = Decimal(strike)
    identity = contract_id or int(numeric_strike * 10) + (1 if right == "C" else 2)
    contract = OptionContractRef(
        contract_id=identity,
        contract_id_ex=f"{identity}@SMART",
        symbol=symbol,
        local_symbol=f"{symbol}-{expiration.isoformat()}-{strike}-{right}",
        expiration=expiration,
        strike=numeric_strike,
        right=right,  # type: ignore[arg-type]
        exchange="SMART",
        trading_class=symbol,
        multiplier=100,
    )
    return OptionQuoteSnapshot(
        contract=contract,
        observed_at=observed_at,
        exchange_time=observed_at,
        bid=Decimal("1"),
        ask=Decimal("1.1"),
        last=Decimal("1.05"),
        close=Decimal("1"),
        volume=100,
        open_interest=open_interest,
        implied_volatility=Decimal("0.2"),
        delta=delta,
        gamma=gamma,
        theta=theta,
        vega=vega,
        market_data_type=market_data_type,
    )


def _complete_chain() -> tuple[OptionQuoteSnapshot, ...]:
    return (
        _quote("620", "C", open_interest=100, gamma=Decimal("0.02")),
        _quote("625", "C", open_interest=500, gamma=Decimal("0.03")),
        _quote("630", "C", open_interest=200, gamma=Decimal("0.02")),
        _quote("620", "P", open_interest=300, gamma=Decimal("0.02")),
        _quote("625", "P", open_interest=100, gamma=Decimal("0.03")),
        _quote("630", "P", open_interest=600, gamma=Decimal("0.02")),
    )


def test_projects_complete_chain_as_supporting_only_observation() -> None:
    result = project_positioning(
        _complete_chain(),
        underlying_spot=Decimal("625"),
        now=NOW,
        expected_contract_count=6,
    )

    assert result.status is PositioningProjectionStatus.READY
    assert result.decision_authority == "SUPPORTING_ONLY"
    assert result.supporting_only is True
    assert result.affects_eligibility is False
    assert result.approval_allowed is False
    assert result.instruction_allowed is False
    assert result.order_allowed is False
    assert result.reasons == ()

    # Whole-chain freshness is conservative: the oldest row owns the as-of.
    assert result.data_asof == NOW - timedelta(seconds=4)
    assert result.data_age_seconds == Decimal("4")
    assert result.newest_data_asof == NOW - timedelta(seconds=4)
    assert result.maximum_data_age_seconds == Decimal("5")

    assert result.observed_contract_count == 6
    assert result.unique_contract_count == 6
    assert result.expected_contract_count == 6
    assert result.option_chain_coverage_rate == Decimal("1")
    assert result.missing_open_interest_rate == Decimal("0")
    assert result.missing_greeks_rate == Decimal("0")
    assert result.missing_gamma_rate == Decimal("0")
    assert result.gex_usable_contract_rate == Decimal("1")

    assert result.max_pain == Decimal("625")
    assert result.call_wall == Decimal("625")
    assert result.put_wall == Decimal("630")
    assert result.put_call_open_interest_ratio == Decimal("1.25")
    assert result.estimated_net_gex_usd_per_one_percent is not None
    assert any("dealer direction" in item for item in result.limitations)
    assert any("eligibility" in item for item in result.limitations)


def test_partial_missing_stale_delayed_chain_degrades_without_hiding_rates() -> None:
    rows = (
        _quote(
            "625",
            "C",
            observed_at=NOW - timedelta(seconds=8),
            open_interest=400,
            market_data_type=3,
        ),
        _quote(
            "625",
            "P",
            observed_at=NOW - timedelta(seconds=7),
            open_interest=None,
            delta=None,
            gamma=None,
            theta=None,
            vega=None,
            market_data_type=3,
        ),
    )

    result = project_positioning(
        rows,
        underlying_spot=Decimal("625"),
        now=NOW,
        expected_contract_count=4,
    )

    assert result.status is PositioningProjectionStatus.DEGRADED
    assert {
        "DELAYED_MARKET_DATA",
        "MISSING_GREEKS",
        "MISSING_OPEN_INTEREST",
        "PARTIAL_OPTION_CHAIN_COVERAGE",
        "STALE_DATA",
    } <= set(result.reasons)
    assert result.data_asof == NOW - timedelta(seconds=8)
    assert result.data_age_seconds == Decimal("8")
    assert result.option_chain_coverage_rate == Decimal("0.5")
    assert result.missing_open_interest_count == 1
    assert result.missing_open_interest_rate == Decimal("0.5")
    assert result.missing_greeks_count == 1
    assert result.missing_greeks_rate == Decimal("0.5")
    assert result.missing_gamma_rate == Decimal("0.5")
    assert result.gex_usable_contract_count == 1
    assert result.gex_usable_contract_rate == Decimal("0.5")

    # Partial descriptive values remain visible with explicit degradation.
    assert result.call_wall == Decimal("625")
    assert result.put_wall is None
    assert result.estimated_net_gex_usd_per_one_percent is not None
    assert result.decision_authority == "SUPPORTING_ONLY"
    assert result.order_allowed is False


def test_missing_coverage_denominator_is_never_assumed_complete() -> None:
    result = project_positioning(
        _complete_chain(),
        underlying_spot=Decimal("625"),
        now=NOW,
    )

    assert result.status is PositioningProjectionStatus.DEGRADED
    assert result.expected_contract_count is None
    assert result.option_chain_coverage_rate is None
    assert "OPTION_CHAIN_COVERAGE_UNKNOWN" in result.reasons
    assert any("denominator" in item for item in result.limitations)


def test_empty_chain_returns_unavailable_projection_instead_of_raising() -> None:
    result = project_positioning(
        (),
        underlying_spot=Decimal("625"),
        now=NOW,
        expected_contract_count=10,
    )

    assert result.status is PositioningProjectionStatus.UNAVAILABLE
    assert result.reasons == ("NO_OPTION_QUOTES",)
    assert result.data_asof is None
    assert result.data_age_seconds is None
    assert result.observed_contract_count == 0
    assert result.unique_contract_count == 0
    assert result.option_chain_coverage_rate == Decimal("0")
    assert result.missing_open_interest_rate is None
    assert result.missing_greeks_rate is None
    assert result.max_pain is None
    assert result.call_wall is None
    assert result.put_wall is None
    assert result.put_call_open_interest_ratio is None
    assert result.estimated_net_gex_usd_per_one_percent is None
    assert result.decision_authority == "SUPPORTING_ONLY"


def test_mixed_chain_is_unavailable_and_never_blends_contracts() -> None:
    result = project_positioning(
        (
            _quote("625", "C"),
            _quote("625", "P", symbol="QQQ"),
        ),
        underlying_spot=Decimal("625"),
        now=NOW,
        expected_contract_count=2,
    )

    assert result.status is PositioningProjectionStatus.UNAVAILABLE
    assert "MIXED_UNDERLYING_OR_EXPIRATION" in result.reasons
    assert result.underlying is None
    assert result.expiration is None
    assert result.max_pain is None
    assert result.decision_authority == "SUPPORTING_ONLY"


def test_duplicate_contract_is_deduplicated_and_disclosed() -> None:
    older = _quote(
        "625",
        "C",
        observed_at=NOW - timedelta(seconds=4),
        open_interest=100,
        contract_id=6251,
    )
    newer = _quote(
        "625",
        "C",
        observed_at=NOW - timedelta(seconds=2),
        open_interest=900,
        contract_id=6251,
    )
    result = project_positioning(
        (older, newer, _quote("625", "P", contract_id=6252)),
        underlying_spot=Decimal("625"),
        now=NOW,
        expected_contract_count=2,
    )

    assert result.status is PositioningProjectionStatus.DEGRADED
    assert "DUPLICATE_CONTRACT_QUOTES" in result.reasons
    assert result.observed_contract_count == 3
    assert result.unique_contract_count == 2
    assert result.call_open_interest == 900
    assert result.option_chain_coverage_rate == Decimal("1")


def test_future_timestamp_is_explicitly_degraded() -> None:
    result = project_positioning(
        (_quote("625", "C", observed_at=NOW + timedelta(seconds=1)),),
        underlying_spot=Decimal("625"),
        now=NOW,
        expected_contract_count=1,
    )

    assert result.status is PositioningProjectionStatus.DEGRADED
    assert "FUTURE_DATA" in result.reasons
    assert result.data_age_seconds == Decimal("-1")
    assert result.future_contract_count == 1
    assert result.decision_authority == "SUPPORTING_ONLY"


def test_projection_authority_flags_are_immutable() -> None:
    result = project_positioning(
        _complete_chain(),
        underlying_spot=Decimal("625"),
        now=NOW,
        expected_contract_count=6,
    )

    with pytest.raises(FrozenInstanceError):
        result.order_allowed = True  # type: ignore[misc]


@pytest.mark.parametrize("value", [True, -1, Decimal("6")])
def test_expected_contract_count_requires_nonnegative_integer(value: object) -> None:
    with pytest.raises((TypeError, ValueError)):
        project_positioning(
            _complete_chain(),
            underlying_spot=Decimal("625"),
            now=NOW,
            expected_contract_count=value,  # type: ignore[arg-type]
        )
