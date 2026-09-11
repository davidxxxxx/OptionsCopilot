"""Transparent Max Pain, option-wall, and estimated GEX calculations.

Open interest does not reveal dealer/customer direction and is commonly a
prior-session value.  The GEX number here is therefore an explicitly-labelled
proxy and may only support, never trigger, a trade.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Iterable

from options_copilot.gateway.ibkr_readonly import OptionQuoteSnapshot


ZERO = Decimal("0")


@dataclass(frozen=True, slots=True)
class OptionPositioningSnapshot:
    underlying: str
    expiration: date
    observed_at: datetime
    max_pain: Decimal | None
    call_wall: Decimal | None
    put_wall: Decimal | None
    call_open_interest: int
    put_open_interest: int
    put_call_open_interest_ratio: Decimal | None
    estimated_net_gex_usd_per_one_percent: Decimal | None
    supporting_only: bool = True
    gex_method: str = "OI_PROXY_CALL_POSITIVE_PUT_NEGATIVE"
    warning: str = (
        "Open interest may be delayed and does not identify dealer direction; "
        "Max Pain, walls, and estimated GEX are supporting signals only."
    )

def calculate_positioning(
    quotes: Iterable[OptionQuoteSnapshot],
    *,
    underlying_spot: Decimal,
) -> OptionPositioningSnapshot:
    rows = tuple(quotes)
    if not rows:
        raise ValueError("at least one option quote is required")
    if not isinstance(underlying_spot, Decimal) or not underlying_spot.is_finite():
        raise TypeError("underlying_spot must be a finite Decimal")
    if underlying_spot <= 0:
        raise ValueError("underlying_spot must be positive")
    underlying = rows[0].contract.symbol
    expiration = rows[0].contract.expiration
    if any(
        row.contract.symbol != underlying or row.contract.expiration != expiration
        for row in rows
    ):
        raise ValueError("positioning must be calculated per underlying and expiration")
    observed_at = max(row.observed_at for row in rows)
    call_oi_by_strike: dict[Decimal, int] = {}
    put_oi_by_strike: dict[Decimal, int] = {}
    estimated_gex = ZERO
    has_gamma_oi = False
    for row in rows:
        strike = row.contract.strike
        oi = row.open_interest or 0
        target = call_oi_by_strike if row.contract.right == "C" else put_oi_by_strike
        target[strike] = target.get(strike, 0) + oi
        if row.gamma is not None and row.open_interest is not None:
            sign = Decimal("1") if row.contract.right == "C" else Decimal("-1")
            estimated_gex += (
                sign
                * row.gamma
                * Decimal(row.open_interest)
                * Decimal(row.contract.multiplier)
                * underlying_spot
                * underlying_spot
                * Decimal("0.01")
            )
            has_gamma_oi = True

    call_oi = sum(call_oi_by_strike.values())
    put_oi = sum(put_oi_by_strike.values())
    strikes = sorted(set(call_oi_by_strike) | set(put_oi_by_strike))
    max_pain = _max_pain(strikes, call_oi_by_strike, put_oi_by_strike, rows)
    call_wall = _wall(call_oi_by_strike, underlying_spot)
    put_wall = _wall(put_oi_by_strike, underlying_spot)
    ratio = Decimal(put_oi) / Decimal(call_oi) if call_oi > 0 else None
    return OptionPositioningSnapshot(
        underlying=underlying,
        expiration=expiration,
        observed_at=observed_at,
        max_pain=max_pain,
        call_wall=call_wall,
        put_wall=put_wall,
        call_open_interest=call_oi,
        put_open_interest=put_oi,
        put_call_open_interest_ratio=ratio,
        estimated_net_gex_usd_per_one_percent=estimated_gex if has_gamma_oi else None,
    )


def _max_pain(
    strikes: list[Decimal],
    calls: dict[Decimal, int],
    puts: dict[Decimal, int],
    quotes: tuple[OptionQuoteSnapshot, ...],
) -> Decimal | None:
    if not strikes or not any(calls.values()) and not any(puts.values()):
        return None
    multiplier_by_strike: dict[Decimal, Decimal] = {}
    for row in quotes:
        multiplier_by_strike[row.contract.strike] = Decimal(row.contract.multiplier)
    payouts: dict[Decimal, Decimal] = {}
    for settlement in strikes:
        call_payout = sum(
            (
                max(settlement - strike, ZERO)
                * Decimal(oi)
                * multiplier_by_strike.get(strike, Decimal("100"))
                for strike, oi in calls.items()
            ),
            ZERO,
        )
        put_payout = sum(
            (
                max(strike - settlement, ZERO)
                * Decimal(oi)
                * multiplier_by_strike.get(strike, Decimal("100"))
                for strike, oi in puts.items()
            ),
            ZERO,
        )
        payouts[settlement] = call_payout + put_payout
    return min(payouts, key=lambda strike: (payouts[strike], strike))


def _wall(open_interest: dict[Decimal, int], spot: Decimal) -> Decimal | None:
    if not open_interest or max(open_interest.values(), default=0) <= 0:
        return None
    return min(
        open_interest,
        key=lambda strike: (-open_interest[strike], abs(strike - spot), strike),
    )
