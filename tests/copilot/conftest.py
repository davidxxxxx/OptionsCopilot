"""Shared fixtures: isolated settings, a Black-Scholes priced chain, fake broker and model."""
from __future__ import annotations

from collections.abc import Sequence
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from copilot.config import Settings, load_settings
from copilot.market.ibkr import IbkrUnavailable
from copilot.market.models import AccountSnapshot, ChainInfo, OptionChain, OptionQuote, Position, Underlying
from copilot.store import Store
from copilot.strategies.pricing import bs_delta, bs_price

TODAY = date(2026, 10, 9)


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return load_settings(
        local_path=tmp_path / "absent.toml",
        overrides={
            "paths": {"data_dir": str(tmp_path / "data"), "secrets_file": str(tmp_path / "keys.json")},
            "universe": {"core": ["SPY", "QQQ"], "watchlist": ["SPY", "QQQ", "NVDA", "JPM"]},
        },
    )


@pytest.fixture
def store(settings: Settings):
    db = Store(settings.database_path)
    yield db
    db.close()


def make_quotes(
    symbol: str,
    spot: float,
    iv: float,
    expiries: Sequence[date],
    *,
    today: date = TODAY,
    step: float = 5.0,
    count: int = 16,
    half_spread: float = 0.03,
    open_interest: int = 1000,
) -> list[OptionQuote]:
    quotes: list[OptionQuote] = []
    center = round(spot / step) * step
    for expiry in expiries:
        t_years = max((expiry - today).days, 0.5) / 365
        for index in range(-count, count + 1):
            strike = center + index * step
            if strike <= 0:
                continue
            for right in ("C", "P"):
                fair = bs_price(spot, strike, t_years, iv, right)
                if fair < 0.05:
                    continue
                quotes.append(OptionQuote(
                    symbol=symbol, expiry=expiry, strike=strike, right=right,
                    bid=round(max(0.01, fair - half_spread), 2), ask=round(fair + half_spread, 2),
                    iv=iv, delta=bs_delta(spot, strike, t_years, iv, right),
                    open_interest=open_interest, volume=100, market_data_type=1,
                    quote_time=datetime(2026, 10, 9, 14, 0, tzinfo=timezone.utc),
                ))
    return quotes


def make_chain(symbol: str = "SPY", spot: float = 600.0, iv: float = 0.18, **kwargs: Any) -> OptionChain:
    expiries = kwargs.pop("expiries", (TODAY + timedelta(days=21),))
    underlying = Underlying(symbol=symbol, price=spot, close=spot, hv20=kwargs.pop("hv20", None), iv30=iv)
    return OptionChain(underlying=underlying, quotes=tuple(make_quotes(symbol, spot, iv, expiries, **kwargs)))


class FakeMarket:
    """In-memory stand-in for IbkrMarket."""

    def __init__(
        self, spots: dict[str, float], *, iv: float = 0.25, unavailable: bool = False,
        hang_chains: bool = False, positions: tuple = (), net_liquidation: float = 50_000.0,
    ) -> None:
        self.spots = spots
        self.positions = positions
        self.net_liquidation = net_liquidation
        self.iv = iv
        self.unavailable = unavailable
        self.hang_chains = hang_chains
        self.chain_calls = 0
        self.warnings: list[str] = []
        self.closed = False

    def connect(self) -> None:
        if self.unavailable:
            raise IbkrUnavailable("无法连接 127.0.0.1:4001")

    def close(self) -> None:
        self.closed = True

    def account(self) -> AccountSnapshot:
        return AccountSnapshot(
            observed_at=datetime.now(timezone.utc), net_liquidation=self.net_liquidation,
            positions=self.positions or (Position(symbol="JPM", sec_type="STK", quantity=10, avg_cost=300.0),),
        )

    def underlyings(self, symbols: Sequence[str]) -> dict[str, Underlying]:
        return {
            symbol: Underlying(symbol=symbol, price=self.spots[symbol], close=self.spots[symbol], change_pct=0.0)
            for symbol in symbols if symbol in self.spots
        }

    def daily_history(self, symbol: str, what: str) -> list[tuple[str, float]]:
        start = TODAY - timedelta(days=260)
        if what == "TRADES":
            spot = self.spots[symbol]
            return [((start + timedelta(days=i)).isoformat(), spot * (1 + 0.004 * ((-1) ** i))) for i in range(200)]
        return [((start + timedelta(days=i)).isoformat(), self.iv * (0.8 + 0.002 * i)) for i in range(200)]

    def chain_info(self, symbol: str) -> ChainInfo | None:
        self.chain_calls += 1
        if self.hang_chains:
            raise TimeoutError()
        expiries = tuple(TODAY + timedelta(days=d) for d in (7, 14, 21, 28, 35, 49))
        return ChainInfo(symbol, symbol, 100, "SMART", expiries, ())

    def option_quotes(self, info: ChainInfo, expiries, *, spot, strike_low, strike_high,
                      max_strikes_per_side=20, require_bbo=True):
        step = 5.0 if spot > 200 else 1.0
        quotes = make_quotes(info.symbol, spot, self.iv, list(expiries), step=step)
        return [q for q in quotes if strike_low <= q.strike <= strike_high]


class FakeLlm:
    """Answers the tagging prompt by keyword rules and the brief prompt with fixed text."""

    def __init__(self, rules: dict[str, dict[str, Any]] | None = None, *,
                 brief: str = "降息预期支撑风险资产", fail: Exception | None = None) -> None:
        self.rules = rules or {}
        self.brief = brief
        self.fail = fail
        self.calls = {"tag": 0, "brief": 0}
        self.tagged_lines: list[str] = []

    def complete_json(self, system: str, user: str, *, max_tokens: int):
        import re as _re

        usage = {"model": "fake", "prompt_tokens": 10, "completion_tokens": 5, "cost_cny": 0.001}
        if self.fail is not None:
            raise self.fail
        if "逐条分类" in system:
            self.calls["tag"] += 1
            items = []
            for line in user.splitlines():
                match = _re.match(r"^\[(\d+)\] (.*)$", line)
                if not match:
                    continue
                self.tagged_lines.append(match[2])
                rule = next((r for keyword, r in self.rules.items() if keyword in match[2]), None)
                items.append({"id": int(match[1]), **(rule or {
                    "category": "other", "importance": 0, "summary": "", "impacts": []})})
            return {"items": items}, usage
        self.calls["brief"] += 1
        return {"macro_summary": self.brief}, usage


def _fake_live_quotes(self, specs):
    from copilot.strategies.pricing import bs_price

    result = {}
    for symbol, expiry, strike, right in specs:
        fair = bs_price(self.spots[symbol], strike, max((expiry - TODAY).days, 0.5) / 365, self.iv, right)
        if fair >= 0.05:
            result[(symbol, expiry, strike, right)] = (round(fair - 0.02, 2), round(fair + 0.02, 2))
    self.live_requests.append(list(specs))
    return result


FakeMarket.live_quotes = _fake_live_quotes
FakeMarket.live_requests = []


class FakeCboe:
    """Stand-in for CboeClient: delayed BS-priced chains; ``failing`` symbols raise."""

    def __init__(self, spots: dict[str, float], *, iv: float = 0.25, failing: set[str] = frozenset()) -> None:
        self.spots = spots
        self.steps = {symbol: 5.0 if spot > 200 else 1.0 for symbol, spot in spots.items()}
        self.iv = iv
        self.failing = set(failing)
        self.calls: list[str] = []

    def chain(self, symbol: str):
        from dataclasses import replace as _replace

        from copilot.market.cboe import CboeChain, CboeError

        self.calls.append(symbol)
        if symbol in self.failing or symbol not in self.spots:
            raise CboeError("CBOE 请求失败: HTTP_ERROR (HTTP 503)")
        spot = self.spots[symbol]
        expiries = [TODAY + timedelta(days=d) for d in (7, 14, 21, 28, 35, 49)]
        quotes = [
            _replace(q, market_data_type=3, source="cboe")
            for q in make_quotes(symbol, spot, self.iv, expiries, step=self.steps[symbol], count=40)
        ]
        underlying = Underlying(symbol=symbol, price=spot, close=spot, iv30=self.iv, market_data_type=3)
        return CboeChain(underlying=underlying, quotes=tuple(quotes),
                         as_of=datetime(2026, 10, 9, 13, 45, tzinfo=timezone.utc))
