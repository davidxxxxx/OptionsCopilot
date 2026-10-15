"""CBOE delayed option chains: free, no login, one request per underlying.

Data is about 15 minutes delayed during the session and holds the closing snapshot
(real bid/ask) outside it. It is the public feed behind cboe.com's delayed quote
pages, not a contracted API, so callers keep IBKR as a fallback.
"""
from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

from copilot.http import HttpError, request_json
from .models import OptionQuote, Underlying

# cdn.cboe.com now answers with a 307 to cdn-api.cboe.com; accept both.
URL = "https://cdn-api.cboe.com/api/global/delayed_quotes/options/{symbol}.json"
HOSTS = ("cdn-api.cboe.com", "cdn.cboe.com")
MAX_BYTES = 40 * 1024 * 1024  # decompressed; SPY is ~6 MB
DELAYED_DATA_TYPE = 3  # same code IBKR uses for delayed market data
NEW_YORK = ZoneInfo("America/New_York")
_OCC = re.compile(r"^(?P<root>[A-Z0-9]{1,6})(?P<date>\d{6})(?P<right>[CP])(?P<strike>\d{8})$")
_SYMBOL = re.compile(r"^[A-Z]{1,5}$")


class CboeError(RuntimeError):
    """CBOE chain unavailable; the message never carries response text."""


@dataclass(frozen=True)
class CboeChain:
    underlying: Underlying
    quotes: tuple[OptionQuote, ...]
    as_of: datetime | None


class CboeClient:
    def __init__(
        self, *, timeout: float = 30.0, fetch: Callable[[str], Any] | None = None
    ) -> None:
        self._timeout = timeout
        self._fetch = fetch or self._http_fetch

    def _http_fetch(self, url: str) -> Any:
        return request_json(
            "GET", url, allowed_hosts=HOSTS, timeout=self._timeout, max_bytes=MAX_BYTES,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) OptionsCopilot/2.0"},
        )

    def chain(self, symbol: str) -> CboeChain:
        if not _SYMBOL.match(symbol):
            raise CboeError(f"不支持的代码 {symbol}")
        try:
            payload = self._fetch(URL.format(symbol=symbol))
        except HttpError as exc:
            raise CboeError(f"CBOE 请求失败: {exc}") from None
        return parse_chain(payload, symbol)


def parse_occ(code: str) -> tuple[str, date, str, float] | None:
    """``GLD261120C00385000`` -> ("GLD", 2026-11-20, "C", 385.0)."""

    match = _OCC.match(code or "")
    if not match:
        return None
    raw = match["date"]
    try:
        expiry = date(2000 + int(raw[:2]), int(raw[2:4]), int(raw[4:6]))
    except ValueError:
        return None
    return match["root"], expiry, match["right"], int(match["strike"]) / 1000


def parse_chain(payload: Any, symbol: str) -> CboeChain:
    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data, dict) or not isinstance(data.get("options"), list):
        raise CboeError("CBOE 返回格式异常")
    as_of = _utc(payload.get("timestamp"))
    underlying = Underlying(
        symbol=symbol,
        price=_positive(data.get("current_price")),
        bid=_positive(data.get("bid")),
        ask=_positive(data.get("ask")),
        close=_positive(data.get("prev_day_close")),
        change_pct=_percent(data.get("price_change_percent")),
        market_data_type=DELAYED_DATA_TYPE,
        quote_time=_new_york(data.get("last_trade_time")),
        iv30=_percent(data.get("iv30")),
    )
    quotes: list[OptionQuote] = []
    for row in data["options"]:
        if not isinstance(row, dict):
            continue
        parsed = parse_occ(str(row.get("option") or ""))
        # Adjusted contracts (after splits/specials) use another root and multiplier.
        if parsed is None or parsed[0] != symbol:
            continue
        _root, expiry, right, strike = parsed
        quotes.append(OptionQuote(
            symbol=symbol,
            expiry=expiry,
            strike=strike,
            right=right,
            multiplier=100,
            trading_class=symbol,
            bid=_positive(row.get("bid")),
            ask=_positive(row.get("ask")),
            last=_positive(row.get("last_trade_price")),
            iv=_positive(row.get("iv")),
            delta=_number(row.get("delta")),
            gamma=_number(row.get("gamma")),
            theta=_number(row.get("theta")),
            vega=_number(row.get("vega")),
            volume=_count(row.get("volume")),
            open_interest=_count(row.get("open_interest")),
            quote_time=as_of,
            market_data_type=DELAYED_DATA_TYPE,
            model_price=_positive(row.get("theo")),
            source="cboe",
        ))
    if underlying.price is None or not quotes:
        raise CboeError("CBOE 没有该标的的期权数据")
    return CboeChain(underlying=underlying, quotes=tuple(quotes), as_of=as_of)


def _number(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value) if value == value and abs(value) != float("inf") else None


def _positive(value: Any) -> float | None:
    number = _number(value)
    return number if number is not None and number > 0 else None


def _count(value: Any) -> int | None:
    number = _number(value)
    return int(number) if number is not None and number >= 0 else None


def _percent(value: Any) -> float | None:
    number = _number(value)
    return number / 100 if number is not None else None


def _utc(value: Any) -> datetime | None:
    try:
        return datetime.fromisoformat(str(value)).replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _new_york(value: Any) -> datetime | None:
    try:
        return datetime.fromisoformat(str(value)).replace(tzinfo=NEW_YORK)
    except ValueError:
        return None


__all__ = ["CboeChain", "CboeClient", "CboeError", "parse_chain", "parse_occ"]
