"""Turn a view plus an option chain into scored, defined-risk candidate structures."""
from __future__ import annotations

import math
from collections.abc import Callable, Sequence
from dataclasses import dataclass, replace
from datetime import date

from copilot.analysis.models import View
from copilot.config import StrategySettings
from copilot.market.models import OptionChain, OptionQuote
from .payoff import Leg, PayoffProfile, analyze_payoff, intrinsic, pnl
from .pricing import (
    RISK_FREE_RATE,
    PriceGrid,
    bs_delta,
    bs_price_array,
    implied_vol,
    terminal_distribution,
)

import numpy as np

STRUCTURE_NAMES = {
    "bull_call_spread": "牛市看涨价差（借方）",
    "bull_put_spread": "牛市看跌价差（贷方）",
    "long_call": "买入看涨",
    "bear_put_spread": "熊市看跌价差（借方）",
    "bear_call_spread": "熊市看涨价差（贷方）",
    "long_put": "买入看跌",
    "iron_condor": "铁鹰（贷方）",
    "iron_butterfly": "铁蝶（贷方）",
    "call_butterfly": "看涨蝶式（借方）",
    "long_straddle": "买入跨式",
    "long_strangle": "买入宽跨式",
    "call_calendar": "看涨日历价差",
    "call_diagonal": "看涨对角价差（PMCC）",
    "put_diagonal": "看跌对角价差",
}
# Structures that are short premium in the front month; the earnings gate applies to them.
SHORT_PREMIUM = frozenset({
    "bull_put_spread", "bear_call_spread", "iron_condor", "iron_butterfly",
    "call_calendar", "call_diagonal", "put_diagonal",
})
MULTI_EXPIRY = frozenset({"call_calendar", "call_diagonal", "put_diagonal"})
VOL_VIEW_MULTIPLIER = {"expand": 1.15, "contract": 0.85, "neutral": 1.0}
KEEP_PER_STRUCTURE = 3


@dataclass(frozen=True)
class Candidate:
    symbol: str
    structure: str
    expiry: date
    dte: int
    legs: tuple[Leg, ...]
    multiplier: int
    profile: PayoffProfile
    view_ev: float
    view_pop: float
    market_ev: float
    market_pop: float
    edge: float
    slippage: float
    spread_cost: float
    atm_iv: float
    view_sigma: float
    spot: float
    drift: float = 0.0  # expected move actually used (after the sigma cap)
    origin: str = "news"  # "news" (view ledger) or "ladder" (index drawdown rule)

    @property
    def return_on_risk(self) -> float:
        return self.edge / self.profile.max_loss

    @property
    def reward_risk(self) -> float | None:
        if self.profile.max_profit is None:
            return None
        return self.profile.max_profit / self.profile.max_loss


def structures_for(view: View) -> list[str]:
    if view.structures:
        return list(view.structures)
    if view.direction == "bullish":
        names = ["bull_call_spread", "bull_put_spread"]
        if view.conviction >= 3 or view.volatility == "expand":
            names.append("long_call")
        if view.conviction >= 3:
            names.append("call_diagonal")
        return names
    if view.direction == "bearish":
        names = ["bear_put_spread", "bear_call_spread"]
        if view.conviction >= 3 or view.volatility == "expand":
            names.append("long_put")
        if view.conviction >= 3:
            names.append("put_diagonal")
        return names
    if view.volatility == "expand":
        return ["long_straddle", "long_strangle"]
    return ["iron_condor", "iron_butterfly", "call_butterfly", "call_calendar"]


def dte_bounds(view: View, settings: StrategySettings) -> tuple[int, int]:
    return view.dte_min or settings.min_dte, view.dte_max or settings.max_dte


def back_expiries(available: Sequence[date], front: date) -> list[date]:
    """Back months for calendars/diagonals: first listed expiry ~4 and ~8 weeks after the front."""

    later = [expiry for expiry in sorted(available) if 21 <= (expiry - front).days <= 120]
    chosen: list[date] = []
    for gap in (28, 56):
        pick = next((expiry for expiry in later if (expiry - front).days >= gap), None)
        if pick and pick not in chosen:
            chosen.append(pick)
    return chosen


def select_expiries(
    available: Sequence[date], today: date, view: View, settings: StrategySettings
) -> list[date]:
    """Nearest listed expiry at or after the view's target date, plus the next one."""

    low, high = dte_bounds(view, settings)
    window = [expiry for expiry in sorted(available) if low <= (expiry - today).days <= high]
    if not window:
        return []
    target_days = max(low, min(high, view.horizon_days + 5))
    if view.catalyst_date and view.catalyst_date >= today:
        target_days = max(target_days, (view.catalyst_date - today).days + 1)
    after = [expiry for expiry in window if (expiry - today).days >= target_days]
    start = window.index(after[0]) if after else len(window) - 1
    chosen = [window[start]]
    for expiry in window[start + 1:]:
        if (expiry - chosen[0]).days >= 5:
            chosen.append(expiry)
            break
    return chosen[: settings.expiries_per_symbol]


def strike_window(spot: float, sigma: float, t_years: float, view: View) -> tuple[float, float]:
    width = max(2.5 * sigma * math.sqrt(t_years), 1.5 * view.expected_move_pct, 0.04)
    return spot * math.exp(-width), spot * math.exp(width)


def build_candidates(
    view: View,
    chain: OptionChain,
    settings: StrategySettings,
    *,
    today: date,
    event_dates: Sequence[date] = (),
    catalyst_dates: Sequence[date] = (),
    indicative_prices: bool = False,
    expiries: Sequence[date] | None = None,
) -> list[Candidate]:
    """Candidates on ``expiries`` (default: every chain expiry in the DTE window).

    Calendars/diagonals use those expiries as the front month and later chain expiries
    as the back month. ``indicative_prices`` prices legs without bid/ask from IB's model
    price (market closed).
    """

    view = _effective_view(
        view, chain, settings, today=today, event_dates=event_dates, catalyst_dates=catalyst_dates
    )
    names = structures_for(view)
    candidates: list[Candidate] = []
    setups: list[_ExpirySetup] = []
    for expiry in expiries if expiries is not None else chain.expiries():
        setup = _ExpirySetup.create(
            view, chain, expiry, settings,
            today=today, event_dates=event_dates, catalyst_dates=catalyst_dates,
            indicative_prices=indicative_prices,
        )
        if setup is None:
            continue
        setups.append(setup)
        for name in names:
            if name in MULTI_EXPIRY or setup.blocks(name):
                continue
            built = [
                candidate
                for legs in _GENERATORS[name](setup.context, setup.target)
                if (candidate := setup.evaluate(name, legs)) is not None
            ]
            built.sort(key=lambda item: item.return_on_risk, reverse=True)
            candidates.extend(built[:KEEP_PER_STRUCTURE])
    for name in (n for n in names if n in MULTI_EXPIRY):
        for setup in setups:
            if setup.dte > 60 or setup.blocks(name):
                continue
            for back in back_expiries(chain.expiries(), setup.expiry):
                back_ctx = _back_context(chain, back, setup.spot, today, settings, indicative_prices)
                if back_ctx is None:
                    continue
                built = [
                    candidate
                    for legs in _MULTI_GENERATORS[name](setup.context, back_ctx, setup.target)
                    if (candidate := setup.evaluate_multi(name, legs, back_ctx)) is not None
                ]
                built.sort(key=lambda item: item.return_on_risk, reverse=True)
                candidates.extend(built[:KEEP_PER_STRUCTURE])
    return candidates


def rebuild_candidate(
    view: View,
    chain: OptionChain,
    settings: StrategySettings,
    *,
    today: date,
    structure: str,
    legs: Sequence[tuple[date, float, str, int]],
    event_dates: Sequence[date] = (),
    catalyst_dates: Sequence[date] = (),
    indicative_prices: bool = False,
) -> Candidate | None:
    """Re-score an exact earlier structure ((expiry, strike, right, signed qty) legs) on new quotes."""

    expiries = sorted({expiry for expiry, *_ in legs})
    if len(expiries) > 2 or (len(expiries) == 2) != (structure in MULTI_EXPIRY):
        return None
    view = _effective_view(
        view, chain, settings, today=today, event_dates=event_dates, catalyst_dates=catalyst_dates
    )
    setup = _ExpirySetup.create(
        view, chain, expiries[0], settings,
        today=today, event_dates=event_dates, catalyst_dates=catalyst_dates,
        indicative_prices=indicative_prices, any_dte=True,
    )
    if setup is None or setup.blocks(structure):
        return None  # e.g. an earnings date now falls before the short leg expires
    back_ctx = None
    if len(expiries) == 2:
        back_ctx = _back_context(chain, expiries[1], setup.spot, today, settings, indicative_prices)
        if back_ctx is None:
            return None
    rebuilt = []
    for expiry, strike, right, quantity in legs:
        ctx = setup.context if expiry == expiries[0] else back_ctx
        side = ctx.calls if right == "C" else ctx.puts
        quote = next((q for q, _ in side if abs(q.strike - strike) < 1e-6), None)
        if quote is None:
            return None  # leg no longer quoted or no longer liquid
        rebuilt.append(ctx.leg(quote, quantity))
    if back_ctx is not None:
        return setup.evaluate_multi(structure, rebuilt, back_ctx)
    return setup.evaluate(structure, rebuilt)


def _effective_view(
    view: View,
    chain: OptionChain,
    settings: StrategySettings,
    *,
    today: date,
    event_dates: Sequence[date],
    catalyst_dates: Sequence[date] = (),
) -> View:
    low, high = dte_bounds(view, settings)
    expiries = [e for e in chain.expiries() if low <= (e - today).days <= high]
    if view.volatility == "expand" and not _has_catalyst(
        view, event_dates, catalyst_dates, today, max(expiries, default=today)
    ):
        # "Volatility will expand" without a dated catalyst is a vague claim; price it neutral
        # (and pick neutral structures) instead of paying up for long premium.
        return replace(view, volatility="neutral")
    return view


@dataclass
class _ExpirySetup:
    """Everything needed to price structures on one expiry under the market and the view."""

    symbol: str
    expiry: date
    dte: int
    spot: float
    context: "_ExpiryContext"
    market_grid: PriceGrid
    view_grid: PriceGrid
    view_sigma: float
    drift: float
    target: float
    settings: StrategySettings
    has_event: bool = False
    origin: str = "news"

    def blocks(self, structure: str) -> bool:
        """Earnings gate: no short premium whose short leg spans an earnings date."""

        return (
            self.settings.no_short_premium_through_earnings
            and self.has_event
            and structure in SHORT_PREMIUM
        )

    @classmethod
    def create(
        cls,
        view: View,
        chain: OptionChain,
        expiry: date,
        settings: StrategySettings,
        *,
        today: date,
        event_dates: Sequence[date],
        catalyst_dates: Sequence[date],
        indicative_prices: bool,
        any_dte: bool = False,
    ) -> "_ExpirySetup | None":
        spot = chain.underlying.price
        dte = (expiry - today).days
        low, high = dte_bounds(view, settings)
        if not spot or spot <= 0 or dte < 1 or (not any_dte and not low <= dte <= high):
            return None
        quotes = chain.for_expiry(expiry)
        if indicative_prices:
            quotes = [with_reference_price(quote) for quote in quotes]
        context = _ExpiryContext.create(quotes, spot=spot, dte=dte, settings=settings)
        if context is None:
            return None
        has_event = any(today <= event <= expiry for event in event_dates)
        vol_view = view.volatility
        if vol_view == "expand" and not _has_catalyst(
            view, event_dates, catalyst_dates, today, expiry
        ):
            vol_view = "neutral"  # the catalyst falls after this expiry
        view_sigma = context.atm_iv * VOL_VIEW_MULTIPLIER[vol_view]
        hv20 = chain.underlying.hv20
        if hv20 and not has_event:
            # Implied vol usually exceeds realised vol (variance risk premium);
            # lean on realised vol a little when no scheduled event is inside.
            view_sigma *= min(1.1, max(0.85, 0.5 + 0.5 * hv20 / context.atm_iv))
        growth = math.exp(RISK_FREE_RATE * context.t_years)
        move_sd = context.atm_iv * math.sqrt(max(view.horizon_days, 1) / 365)
        cap = settings.max_drift_sigma * move_sd
        # Ledger views state the move in standard deviations; scale by this expiry's IV.
        raw = view.sign * view.drift_sigma * move_sd if view.drift_sigma is not None else view.signed_drift
        drift = max(-cap, min(cap, raw))
        return cls(
            symbol=chain.underlying.symbol,
            expiry=expiry,
            dte=dte,
            spot=spot,
            context=context,
            market_grid=_grid(spot * growth, context.atm_iv, context.t_years, settings),
            view_grid=_grid(spot * (1 + drift) * growth, view_sigma, context.t_years, settings),
            view_sigma=view_sigma,
            drift=drift,
            target=spot * (1 + drift),
            settings=settings,
            has_event=has_event,
            origin=view.source if view.source in {"ladder", "macro"} else "news",
        )

    def evaluate(self, structure: str, legs: list[Leg]) -> "Candidate | None":
        return _evaluate(
            self.symbol, structure, self.expiry, self.dte, legs, self.context,
            market_grid=self.market_grid, view_grid=self.view_grid,
            view_sigma=self.view_sigma, spot=self.spot, settings=self.settings, drift=self.drift,
            origin=self.origin,
        )

    def evaluate_multi(self, structure: str, legs: list[Leg], back: "_ExpiryContext") -> "Candidate | None":
        return _evaluate_multi(self, structure, legs, back)


def _has_catalyst(
    view: View,
    event_dates: Sequence[date],
    catalyst_dates: Sequence[date],
    today: date,
    expiry: date,
) -> bool:
    dates = [*event_dates, *catalyst_dates, *([view.catalyst_date] if view.catalyst_date else [])]
    return any(today <= moment <= expiry for moment in dates)


def _grid(forward: float, sigma: float, t_years: float, settings: StrategySettings) -> PriceGrid:
    return terminal_distribution(
        forward, sigma, t_years,
        tail_weight=settings.tail_weight,
        tail_multiple=settings.tail_vol_multiple,
        core_multiple=settings.core_vol_multiple,
    )


@dataclass
class _ExpiryContext:
    calls: list[tuple[OptionQuote, float]]  # (quote, delta) sorted by strike
    puts: list[tuple[OptionQuote, float]]
    atm_iv: float
    t_years: float
    spot: float
    fill_ratio: float

    @classmethod
    def create(
        cls, quotes: Sequence[OptionQuote], *, spot: float, dte: int, settings: StrategySettings
    ) -> "_ExpiryContext | None":
        t_years = max(dte, 0.5) / 365
        usable = [quote for quote in quotes if _usable(quote, settings)]
        atm_iv = _atm_iv(usable, spot, t_years)
        if atm_iv is None:
            return None
        with_delta: list[tuple[OptionQuote, float]] = []
        for quote in usable:
            delta = quote.delta
            if delta is None or not math.isfinite(delta):
                sigma = quote.iv or implied_vol(quote.mid, spot, quote.strike, t_years, quote.right) or atm_iv
                delta = bs_delta(spot, quote.strike, t_years, sigma, quote.right)
            with_delta.append((quote, delta))
        calls = sorted((item for item in with_delta if item[0].right == "C"), key=lambda item: item[0].strike)
        puts = sorted((item for item in with_delta if item[0].right == "P"), key=lambda item: item[0].strike)
        if not calls or not puts:
            return None
        return cls(calls, puts, atm_iv, t_years, spot, settings.fill_ratio)

    def fill(self, quote: OptionQuote, quantity: int) -> float:
        mid = quote.mid
        if quantity > 0:
            return mid + self.fill_ratio * (quote.ask - mid)
        return mid - self.fill_ratio * (mid - quote.bid)

    def leg(self, quote: OptionQuote, quantity: int) -> Leg:
        return Leg(quote=quote, quantity=quantity, price=self.fill(quote, quantity))


def with_reference_price(quote: OptionQuote) -> OptionQuote:
    """Synthesize a bid/ask around IB's model price (or last) when no live market exists."""

    if quote.mid is not None:
        return quote
    reference = quote.model_price or quote.last
    if not reference or reference <= 0:
        return quote
    half_spread = max(0.01, 0.02 * reference)
    return replace(
        quote,
        bid=round(max(0.01, reference - half_spread), 2),
        ask=round(reference + half_spread, 2),
        indicative=True,
    )


def _usable(quote: OptionQuote, settings: StrategySettings) -> bool:
    mid = quote.mid
    if mid is None or mid <= 0:
        return False
    if quote.spread > 0.05 and quote.spread / mid > settings.max_leg_spread_pct:
        return False
    if quote.open_interest is not None and quote.open_interest < settings.min_open_interest:
        return False
    return True


def _atm_iv(quotes: Sequence[OptionQuote], spot: float, t_years: float) -> float | None:
    values: list[float] = []
    for right in ("C", "P"):
        side = sorted((q for q in quotes if q.right == right), key=lambda q: abs(q.strike - spot))[:2]
        for quote in side:
            iv = quote.iv or implied_vol(quote.mid, spot, quote.strike, t_years, right)
            if iv and 0.01 < iv < 5:
                values.append(iv)
    return sum(values) / len(values) if values else None


def _nearest(items: list[tuple[OptionQuote, float]], target: float, low: float, high: float, count: int):
    eligible = [item for item in items if low <= item[1] <= high]
    return sorted(eligible, key=lambda item: abs(item[1] - target))[:count]


def _index(items: list[tuple[OptionQuote, float]], quote: OptionQuote) -> int:
    return next(i for i, item in enumerate(items) if item[0] is quote)


def _gen_bull_call(ctx: _ExpiryContext, target: float):
    for long_q, _ in _nearest(ctx.calls, 0.55, 0.30, 0.78, 4):
        start = _index(ctx.calls, long_q)
        for short_q, short_delta in ctx.calls[start + 1:start + 7]:
            if short_delta >= 0.04:
                yield [ctx.leg(long_q, 1), ctx.leg(short_q, -1)]


def _gen_bear_put(ctx: _ExpiryContext, target: float):
    for long_q, _ in _nearest(ctx.puts, -0.55, -0.78, -0.30, 4):
        start = _index(ctx.puts, long_q)
        for short_q, short_delta in reversed(ctx.puts[max(0, start - 6):start]):
            if short_delta <= -0.04:
                yield [ctx.leg(long_q, 1), ctx.leg(short_q, -1)]


def _gen_bull_put(ctx: _ExpiryContext, target: float):
    for short_q, _ in _nearest(ctx.puts, -0.28, -0.45, -0.10, 5):
        start = _index(ctx.puts, short_q)
        for long_q, _ in reversed(ctx.puts[max(0, start - 5):start]):
            yield [ctx.leg(short_q, -1), ctx.leg(long_q, 1)]


def _gen_bear_call(ctx: _ExpiryContext, target: float):
    for short_q, _ in _nearest(ctx.calls, 0.28, 0.10, 0.45, 5):
        start = _index(ctx.calls, short_q)
        for long_q, _ in ctx.calls[start + 1:start + 6]:
            yield [ctx.leg(short_q, -1), ctx.leg(long_q, 1)]


def _gen_long_call(ctx: _ExpiryContext, target: float):
    for quote, _ in _nearest(ctx.calls, 0.5, 0.25, 0.75, 5):
        yield [ctx.leg(quote, 1)]


def _gen_long_put(ctx: _ExpiryContext, target: float):
    for quote, _ in _nearest(ctx.puts, -0.5, -0.75, -0.25, 5):
        yield [ctx.leg(quote, 1)]


def _gen_iron_condor(ctx: _ExpiryContext, target: float):
    short_puts = _nearest(ctx.puts, -0.18, -0.32, -0.08, 4)
    short_calls = _nearest(ctx.calls, 0.18, 0.08, 0.32, 4)
    for sp, _ in short_puts:
        sp_index = _index(ctx.puts, sp)
        for sc, _ in short_calls:
            if sc.strike <= sp.strike:
                continue
            sc_index = _index(ctx.calls, sc)
            for wing in (1, 2, 3):
                if sp_index - wing < 0 or sc_index + wing >= len(ctx.calls):
                    continue
                lp = ctx.puts[sp_index - wing][0]
                lc = ctx.calls[sc_index + wing][0]
                yield [ctx.leg(sp, -1), ctx.leg(lp, 1), ctx.leg(sc, -1), ctx.leg(lc, 1)]


def _gen_iron_butterfly(ctx: _ExpiryContext, target: float):
    put_strikes = {item[0].strike: item[0] for item in ctx.puts}
    centers = sorted((item[0] for item in ctx.calls if item[0].strike in put_strikes),
                     key=lambda q: abs(q.strike - ctx.spot))[:2]
    for center_call in centers:
        center_put = put_strikes[center_call.strike]
        c_index = _index(ctx.calls, center_call)
        p_index = _index(ctx.puts, center_put)
        for wing in range(2, 7):
            if p_index - wing < 0 or c_index + wing >= len(ctx.calls):
                continue
            yield [
                ctx.leg(center_put, -1), ctx.leg(center_call, -1),
                ctx.leg(ctx.puts[p_index - wing][0], 1), ctx.leg(ctx.calls[c_index + wing][0], 1),
            ]


def _gen_call_butterfly(ctx: _ExpiryContext, target: float):
    centers = sorted(ctx.calls, key=lambda item: abs(item[0].strike - target))[:2]
    for center, _ in centers:
        index = _index(ctx.calls, center)
        for wing in range(1, 5):
            if index - wing < 0 or index + wing >= len(ctx.calls):
                continue
            yield [
                ctx.leg(ctx.calls[index - wing][0], 1),
                ctx.leg(center, -2),
                ctx.leg(ctx.calls[index + wing][0], 1),
            ]


def _gen_long_straddle(ctx: _ExpiryContext, target: float):
    put_strikes = {item[0].strike: item[0] for item in ctx.puts}
    centers = sorted((item[0] for item in ctx.calls if item[0].strike in put_strikes),
                     key=lambda q: abs(q.strike - ctx.spot))[:2]
    for call in centers:
        yield [ctx.leg(call, 1), ctx.leg(put_strikes[call.strike], 1)]


def _gen_long_strangle(ctx: _ExpiryContext, target: float):
    calls = _nearest(ctx.calls, 0.28, 0.15, 0.40, 4)
    puts = _nearest(ctx.puts, -0.28, -0.40, -0.15, 4)
    for call, call_delta in calls:
        for put, put_delta in puts:
            if put.strike < call.strike and abs(call_delta + put_delta) <= 0.08:
                yield [ctx.leg(call, 1), ctx.leg(put, 1)]


def _gen_call_calendar(front: _ExpiryContext, back: _ExpiryContext, target: float):
    back_calls = {q.strike: q for q, _ in back.calls}
    centers = sorted((q for q, _ in front.calls if q.strike in back_calls), key=lambda q: abs(q.strike - target))[:2]
    for short in centers:
        yield [front.leg(short, -1), back.leg(back_calls[short.strike], 1)]


def _gen_call_diagonal(front: _ExpiryContext, back: _ExpiryContext, target: float):
    for long_q, _ in _nearest(back.calls, 0.75, 0.60, 0.90, 2):
        for short_q, _ in _nearest(front.calls, 0.30, 0.15, 0.45, 3):
            if short_q.strike > long_q.strike:
                yield [back.leg(long_q, 1), front.leg(short_q, -1)]


def _gen_put_diagonal(front: _ExpiryContext, back: _ExpiryContext, target: float):
    for long_q, _ in _nearest(back.puts, -0.75, -0.90, -0.60, 2):
        for short_q, _ in _nearest(front.puts, -0.30, -0.45, -0.15, 3):
            if short_q.strike < long_q.strike:
                yield [back.leg(long_q, 1), front.leg(short_q, -1)]


_MULTI_GENERATORS: dict[str, Callable[[_ExpiryContext, _ExpiryContext, float], object]] = {
    "call_calendar": _gen_call_calendar,
    "call_diagonal": _gen_call_diagonal,
    "put_diagonal": _gen_put_diagonal,
}


def _back_context(
    chain: OptionChain, expiry: date, spot: float, today: date,
    settings: StrategySettings, indicative_prices: bool,
) -> "_ExpiryContext | None":
    quotes = chain.for_expiry(expiry)
    if indicative_prices:
        quotes = [with_reference_price(quote) for quote in quotes]
    return _ExpiryContext.create(quotes, spot=spot, dte=(expiry - today).days, settings=settings)


def _evaluate_multi(
    setup: "_ExpirySetup", structure: str, legs: list[Leg], back: _ExpiryContext
) -> Candidate | None:
    """Two-expiry structures, valued at the front expiry.

    Front legs settle at intrinsic; back legs are revalued with Black-Scholes at their
    own implied vol for the time left. Max loss/profit and breakevens come from a dense
    price axis (including far tails) because the payoff is no longer piecewise linear.
    """

    settings = setup.settings
    multiplier = legs[0].quote.multiplier
    tau = back.t_years - setup.context.t_years
    if tau <= 0:
        return None
    fees = settings.commission_per_contract * sum(abs(leg.quantity) for leg in legs) * 2
    net_debit = sum(leg.quantity * leg.price for leg in legs)
    back_vols = {}
    for leg in legs:
        if leg.quote.expiry != setup.expiry:
            back_vols[id(leg)] = (
                leg.quote.iv
                or implied_vol(leg.quote.mid, setup.spot, leg.strike, back.t_years, leg.right)
                or back.atm_iv
            )

    def value(prices: np.ndarray) -> np.ndarray:
        total = np.zeros_like(prices, dtype=float)
        for leg in legs:
            if leg.quote.expiry == setup.expiry:
                total += intrinsic([leg], prices)  # already signed by the leg quantity
            else:
                total += leg.quantity * bs_price_array(prices, leg.strike, tau, back_vols[id(leg)], leg.right)
        return total

    axis = setup.spot * np.exp(np.linspace(-2.5, 2.5, 2001))
    axis = np.concatenate(([setup.spot * 1e-4], axis, [setup.spot * 20]))
    pnl_axis = (value(axis) - net_debit) * multiplier - fees
    max_loss = -float(pnl_axis.min())
    if max_loss <= 0.005 or pnl_axis[-1] < pnl_axis[-2] - 1.0:
        return None  # riskless (bad quotes) or losing without bound to the upside
    breakevens = []
    inner = (axis > setup.spot * 0.3) & (axis < setup.spot * 3)
    xs, ys = axis[inner], pnl_axis[inner]
    for x0, y0, x1, y1 in zip(xs, ys, xs[1:], ys[1:]):
        if (y0 < 0 <= y1) or (y0 >= 0 > y1):
            breakevens.append(round(float(x0 + (x1 - x0) * (-y0) / (y1 - y0)), 2))
    profile = PayoffProfile(
        net_debit=net_debit, max_loss=max_loss, max_profit=float(pnl_axis.max()),
        breakevens=tuple(breakevens), fees=fees,
    )
    market_values = value(setup.market_grid.prices)
    view_values = value(setup.view_grid.prices)
    market_pnl = (market_values - net_debit) * multiplier - fees
    view_pnl = (view_values - net_debit) * multiplier - fees
    slippage = sum(abs(leg.quantity) * abs(leg.price - leg.quote.mid) for leg in legs) * multiplier
    edge = (
        setup.view_grid.expectation(view_values) - setup.market_grid.expectation(market_values)
    ) * multiplier - slippage - fees
    return Candidate(
        symbol=setup.symbol,
        structure=structure,
        expiry=setup.expiry,
        dte=setup.dte,
        legs=tuple(legs),
        multiplier=multiplier,
        profile=profile,
        view_ev=setup.view_grid.expectation(view_pnl),
        view_pop=setup.view_grid.probability(view_pnl > 0),
        market_ev=setup.market_grid.expectation(market_pnl),
        market_pop=setup.market_grid.probability(market_pnl > 0),
        edge=edge,
        slippage=slippage,
        spread_cost=sum(abs(leg.quantity) * (leg.quote.spread or 0) / 2 for leg in legs) * multiplier,
        atm_iv=setup.context.atm_iv,
        view_sigma=setup.view_sigma,
        spot=setup.spot,
        drift=setup.drift,
        origin=setup.origin,
    )


_GENERATORS: dict[str, Callable[[_ExpiryContext, float], object]] = {
    "bull_call_spread": _gen_bull_call,
    "bull_put_spread": _gen_bull_put,
    "long_call": _gen_long_call,
    "bear_put_spread": _gen_bear_put,
    "bear_call_spread": _gen_bear_call,
    "long_put": _gen_long_put,
    "iron_condor": _gen_iron_condor,
    "iron_butterfly": _gen_iron_butterfly,
    "call_butterfly": _gen_call_butterfly,
    "long_straddle": _gen_long_straddle,
    "long_strangle": _gen_long_strangle,
}


def _evaluate(
    symbol: str,
    structure: str,
    expiry: date,
    dte: int,
    legs: list[Leg],
    ctx: _ExpiryContext,
    *,
    market_grid: PriceGrid,
    view_grid: PriceGrid,
    view_sigma: float,
    spot: float,
    settings: StrategySettings,
    drift: float = 0.0,
    origin: str = "news",
) -> Candidate | None:
    multiplier = legs[0].quote.multiplier
    profile = analyze_payoff(
        legs, multiplier=multiplier, commission_per_contract=settings.commission_per_contract
    )
    if profile is None:
        return None
    market_pnl = pnl(legs, market_grid.prices, multiplier=multiplier, fees=profile.fees)
    view_pnl = pnl(legs, view_grid.prices, multiplier=multiplier, fees=profile.fees)
    spread_cost = sum(abs(leg.quantity) * (leg.quote.spread or 0) / 2 for leg in legs) * multiplier
    slippage = sum(abs(leg.quantity) * abs(leg.price - leg.quote.mid) for leg in legs) * multiplier
    # Edge = what the view adds over the market's own distribution, under the same
    # model, minus execution costs. Model-vs-market smile differences cancel out.
    view_value = view_grid.expectation(intrinsic(legs, view_grid.prices)) * multiplier
    market_value = market_grid.expectation(intrinsic(legs, market_grid.prices)) * multiplier
    edge = view_value - market_value - slippage - profile.fees
    return Candidate(
        symbol=symbol,
        structure=structure,
        expiry=expiry,
        dte=dte,
        legs=tuple(legs),
        multiplier=multiplier,
        profile=profile,
        view_ev=view_grid.expectation(view_pnl),
        view_pop=view_grid.probability(view_pnl > 0),
        market_ev=market_grid.expectation(market_pnl),
        market_pop=market_grid.probability(market_pnl > 0),
        edge=edge,
        slippage=slippage,
        spread_cost=spread_cost,
        atm_iv=ctx.atm_iv,
        view_sigma=view_sigma,
        spot=spot,
        drift=drift,
        origin=origin,
    )


__all__ = [
    "MULTI_EXPIRY",
    "SHORT_PREMIUM",
    "back_expiries",
    "dte_bounds",
    "Candidate",
    "STRUCTURE_NAMES",
    "build_candidates",
    "rebuild_candidate",
    "select_expiries",
    "strike_window",
    "structures_for",
    "with_reference_price",
]
