"""Black-Scholes helpers and the terminal price distribution used to score structures.

The distribution is a two-component lognormal mixture with a common forward: a
core component slightly narrower than the input volatility and a wide tail
component. It keeps the mean where the view (or the market) puts it while
giving short-premium structures realistic tail losses.
"""
from __future__ import annotations

import math
from dataclasses import dataclass

import numpy as np

RISK_FREE_RATE = 0.04


def _norm_cdf(value: float) -> float:
    return 0.5 * (1.0 + math.erf(value / math.sqrt(2.0)))


def bs_price(spot: float, strike: float, t_years: float, sigma: float, right: str) -> float:
    if t_years <= 0 or sigma <= 0:
        return max(0.0, spot - strike) if right == "C" else max(0.0, strike - spot)
    sqrt_t = math.sqrt(t_years)
    d1 = (math.log(spot / strike) + (RISK_FREE_RATE + 0.5 * sigma * sigma) * t_years) / (sigma * sqrt_t)
    d2 = d1 - sigma * sqrt_t
    discount = math.exp(-RISK_FREE_RATE * t_years)
    if right == "C":
        return spot * _norm_cdf(d1) - strike * discount * _norm_cdf(d2)
    return strike * discount * _norm_cdf(-d2) - spot * _norm_cdf(-d1)


def bs_delta(spot: float, strike: float, t_years: float, sigma: float, right: str) -> float:
    if t_years <= 0 or sigma <= 0:
        itm = spot > strike if right == "C" else spot < strike
        return (1.0 if right == "C" else -1.0) if itm else 0.0
    d1 = (math.log(spot / strike) + (RISK_FREE_RATE + 0.5 * sigma * sigma) * t_years) / (
        sigma * math.sqrt(t_years)
    )
    return _norm_cdf(d1) if right == "C" else _norm_cdf(d1) - 1.0


def implied_vol(price: float, spot: float, strike: float, t_years: float, right: str) -> float | None:
    """Bisection IV solver; None when the price is outside no-arbitrage bounds."""

    if price <= 0 or spot <= 0 or strike <= 0 or t_years <= 0:
        return None
    intrinsic = bs_price(spot, strike, 0.0, 0.0, right)
    if price < intrinsic * 0.999:
        return None
    low, high = 1e-4, 5.0
    if bs_price(spot, strike, t_years, high, right) < price:
        return None
    for _ in range(80):
        middle = 0.5 * (low + high)
        if bs_price(spot, strike, t_years, middle, right) > price:
            high = middle
        else:
            low = middle
        if high - low < 1e-6:
            break
    return 0.5 * (low + high)


_ERF = np.frompyfunc(math.erf, 1, 1)


def bs_price_array(prices: np.ndarray, strike: float, t_years: float, sigma: float, right: str) -> np.ndarray:
    """Black-Scholes value for many underlying prices at once (used for calendar back legs)."""

    prices = np.maximum(np.asarray(prices, dtype=float), 1e-9)
    if t_years <= 0 or sigma <= 0:
        return np.maximum(prices - strike, 0.0) if right == "C" else np.maximum(strike - prices, 0.0)
    sqrt_t = math.sqrt(t_years)
    d1 = (np.log(prices / strike) + (RISK_FREE_RATE + 0.5 * sigma * sigma) * t_years) / (sigma * sqrt_t)
    d2 = d1 - sigma * sqrt_t
    discount = math.exp(-RISK_FREE_RATE * t_years)

    def cdf(x: np.ndarray) -> np.ndarray:
        return 0.5 * (1.0 + _ERF(x / math.sqrt(2.0)).astype(float))

    if right == "C":
        return prices * cdf(d1) - strike * discount * cdf(d2)
    return strike * discount * cdf(-d2) - prices * cdf(-d1)


@dataclass(frozen=True)
class PriceGrid:
    prices: np.ndarray
    weights: np.ndarray  # sums to 1

    def expectation(self, values: np.ndarray) -> float:
        return float(np.dot(values, self.weights))

    def probability(self, mask: np.ndarray) -> float:
        return float(self.weights[mask].sum())

    @property
    def mean(self) -> float:
        return self.expectation(self.prices)


def terminal_distribution(
    forward: float,
    sigma: float,
    t_years: float,
    *,
    tail_weight: float = 0.15,
    tail_multiple: float = 1.6,
    core_multiple: float = 0.88,
    points: int = 1201,
) -> PriceGrid:
    if forward <= 0 or sigma <= 0 or t_years <= 0:
        raise ValueError("forward, sigma and t_years must be positive")
    if tail_weight > 0:
        # Scale both components so an at-the-money option is worth exactly what a
        # lognormal with ``sigma`` says. The mixture then agrees with the market's
        # ATM implied vol and only adds weight to the wings (like a real smile)
        # instead of making ATM premium look rich or cheap.
        scale = _atm_matched_scale(sigma, t_years, tail_weight, tail_multiple, core_multiple)
        components = [
            (1.0 - tail_weight, sigma * core_multiple * scale),
            (tail_weight, sigma * tail_multiple * scale),
        ]
    else:
        components = [(1.0, sigma)]
    widest = max(vol for _, vol in components) * math.sqrt(t_years)
    x = np.linspace(-7.0 * widest, 7.0 * widest, points)
    density = np.zeros_like(x)
    for weight, vol in components:
        sd = vol * math.sqrt(t_years)
        mean = -0.5 * sd * sd  # keeps E[S_T] equal to the forward for every component
        density += weight * np.exp(-0.5 * ((x - mean) / sd) ** 2) / (sd * math.sqrt(2 * math.pi))
    weights = density / density.sum()
    return PriceGrid(prices=forward * np.exp(x), weights=weights)


def _atm_matched_scale(
    sigma: float, t_years: float, tail_weight: float, tail_multiple: float, core_multiple: float
) -> float:
    """Solve sum_i w_i * atm(m_i * s) = atm(sigma), atm(v) = 2N(v*sqrt(T)/2) - 1."""

    root_t = math.sqrt(t_years)

    def atm(vol: float) -> float:
        return 2.0 * _norm_cdf(vol * root_t / 2.0) - 1.0

    target = atm(sigma)
    low, high = 0.3, 3.0
    for _ in range(60):
        middle = 0.5 * (low + high)
        value = (1.0 - tail_weight) * atm(sigma * core_multiple * middle) + tail_weight * atm(
            sigma * tail_multiple * middle
        )
        if value > target:
            high = middle
        else:
            low = middle
    return 0.5 * (low + high)


__all__ = [
    "PriceGrid",
    "RISK_FREE_RATE",
    "bs_delta",
    "bs_price_array",
    "bs_price",
    "implied_vol",
    "terminal_distribution",
]
