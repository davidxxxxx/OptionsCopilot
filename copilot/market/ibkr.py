"""Read-only IBKR adapter on ib_insync: account, underlyings, history and option quotes.

Connections are opened with ``readonly=True`` and this module never calls an order
API; tests/copilot/test_readonly_guard.py enforces both. One connection lives for
one pipeline run, so there is no long-lived reconnect state to go stale.
"""
from __future__ import annotations

import asyncio
import logging
import math
import time
from collections.abc import Iterable, Sequence
from datetime import date, datetime, timezone
from typing import Any

from copilot.config import IbkrSettings
from .models import AccountSnapshot, ChainInfo, OptionQuote, Position, Underlying, valid_price

log = logging.getLogger(__name__)

# Data-farm status chatter and "delayed data displayed" notices are not problems.
_QUIET_CODES = {2100, 2104, 2106, 2107, 2108, 2119, 2150, 2158, 10167, 10197}
_BATCH_PAUSE_SECONDS = 0.5
_QUALIFY_BATCH = 15
# 200 = no security definition: expected for sampled strikes a weekly expiry does not list.
_MISSING_CONTRACT = 200


class IbkrUnavailable(RuntimeError):
    """TWS / IB Gateway could not be reached or refused the session."""


class IbkrMarket:
    def __init__(self, settings: IbkrSettings) -> None:
        self.settings = settings
        self.warnings: list[str] = []
        self.upstream_lost = False
        self.missing_contracts = 0
        self._ib: Any = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._stocks: dict[str, Any] = {}

    # ---- lifecycle --------------------------------------------------------
    def __enter__(self) -> "IbkrMarket":
        self.connect()
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def connect(self) -> None:
        # Each run owns a fresh event loop on its own thread; uvicorn keeps its own.
        # It must exist before ib_insync is imported: eventkit grabs the current
        # thread's loop at import time and raises on a GUI worker thread without one.
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        from ib_insync import IB  # lazy: news-only runs and tests need no event loop

        logging.getLogger("ib_insync").setLevel(logging.CRITICAL)
        ib = IB()
        ib.RequestTimeout = 30
        ib.errorEvent += self._on_error
        failure = "unknown"
        for offset in range(3):
            try:
                ib.connect(
                    self.settings.host,
                    self.settings.port,
                    clientId=self.settings.client_id + offset,
                    timeout=self.settings.connect_timeout_seconds,
                    readonly=True,
                )
            except ConnectionRefusedError:
                failure = f"无法连接 {self.settings.host}:{self.settings.port}（TWS/IB Gateway 未运行或 API 未开启）"
                break
            except Exception as exc:  # client id in use, handshake timeout, ...
                failure = f"{type(exc).__name__}: {exc}"[:200]
                if ib.isConnected():
                    ib.disconnect()
                continue
            if ib.isConnected():
                break
        if not ib.isConnected():
            self.close()
            raise IbkrUnavailable(failure)
        ib.reqMarketDataType(self.settings.market_data_type)
        self._ib = ib

    def close(self) -> None:
        if self._ib is not None:
            try:
                self._ib.disconnect()
            except Exception:
                pass
            self._ib = None
        if self._loop is not None:
            try:
                self._loop.close()
            finally:
                asyncio.set_event_loop(None)
                self._loop = None

    @property
    def connected(self) -> bool:
        return self._ib is not None and self._ib.isConnected()

    def _require(self) -> Any:
        if not self.connected:
            raise IbkrUnavailable("IBKR 连接已断开")
        if self.upstream_lost:
            # Error 1100: the gateway is up but has lost IBKR's servers; every request
            # would just hang until timeout, so fail fast and let the scheduler retry.
            raise IbkrUnavailable("IB Gateway 与 IBKR 服务器断开（错误 1100），请检查网关登录/网络")
        return self._ib

    def _on_error(self, req_id: int, code: int, message: str, contract: Any = None) -> None:
        if code == 1100:
            self.upstream_lost = True
        elif code in (1101, 1102):
            self.upstream_lost = False
        if code == _MISSING_CONTRACT:
            self.missing_contracts += 1
            return
        if code in _QUIET_CODES:
            return
        if len(self.warnings) < 60:
            subject = getattr(contract, "localSymbol", "") or getattr(contract, "symbol", "")
            self.warnings.append(f"{code} {subject} {message}"[:200].strip())

    # ---- account ----------------------------------------------------------
    def account(self) -> AccountSnapshot:
        ib = self._require()
        net_liquidation: float | None = None
        currency = "USD"
        for value in ib.accountSummary():
            if value.tag != "NetLiquidation":
                continue
            amount = _float(value.value)
            if amount is not None:
                net_liquidation = (net_liquidation or 0.0) + amount
                currency = value.currency or currency
        positions = tuple(
            position
            for item in ib.reqPositions()
            if (position := _position(item)) is not None
        )
        return AccountSnapshot(
            observed_at=datetime.now(timezone.utc),
            net_liquidation=net_liquidation,
            currency=currency,
            positions=positions,
        )

    # ---- underlyings ------------------------------------------------------
    def underlyings(self, symbols: Sequence[str]) -> dict[str, Underlying]:
        from ib_insync import Stock

        ib = self._require()
        wanted = [symbol for symbol in symbols if symbol not in self._stocks]
        for batch in _chunks(wanted, _QUALIFY_BATCH):
            for contract in ib.qualifyContracts(*(Stock(symbol, "SMART", "USD") for symbol in batch)):
                if contract.conId:
                    self._stocks[contract.symbol] = contract
            ib.sleep(_BATCH_PAUSE_SECONDS)
        contracts = [self._stocks[symbol] for symbol in symbols if symbol in self._stocks]
        result: dict[str, Underlying] = {}
        for batch in _chunks(contracts, self.settings.max_concurrent_quotes):
            tickers = self._stream(batch, "", lambda t: valid_price(_float(t.marketPrice())) or valid_price(_float(t.close)))
            for contract, ticker in zip(batch, tickers):
                result[contract.symbol] = _underlying(contract, ticker)
        return result

    def daily_history(self, symbol: str, what: str) -> list[tuple[str, float]]:
        """One year of daily bars; ``what`` is TRADES or OPTION_IMPLIED_VOLATILITY."""

        ib = self._require()
        contract = self._stocks.get(symbol)
        if contract is None:
            return []
        bars = ib.reqHistoricalData(
            contract,
            endDateTime="",
            durationStr="1 Y",
            barSizeSetting="1 day",
            whatToShow=what,
            useRTH=True,
            formatDate=1,
            timeout=self.settings.history_timeout_seconds,
        )
        return [
            (str(bar.date), float(bar.close))
            for bar in bars or ()
            if _float(bar.close) is not None
        ]

    # ---- options ----------------------------------------------------------
    def chain_info(self, symbol: str) -> ChainInfo | None:
        ib = self._require()
        contract = self._stocks.get(symbol)
        if contract is None:
            return None
        params = ib.reqSecDefOptParams(contract.symbol, "", contract.secType, contract.conId)
        smart = [item for item in params if item.exchange == "SMART"] or list(params)
        if not smart:
            return None
        own = [item for item in smart if item.tradingClass == symbol]
        chosen = max(own or smart, key=lambda item: len(item.expirations))
        expiries = sorted(
            date(int(raw[:4]), int(raw[4:6]), int(raw[6:8])) for raw in chosen.expirations
        )
        return ChainInfo(
            symbol=symbol,
            trading_class=chosen.tradingClass,
            multiplier=int(float(chosen.multiplier or 100)),
            exchange="SMART",
            expiries=tuple(expiries),
            strikes=tuple(sorted(float(value) for value in chosen.strikes)),
        )

    def option_quotes(
        self,
        info: ChainInfo,
        expiries: Iterable[date],
        *,
        spot: float,
        strike_low: float,
        strike_high: float,
        max_strikes_per_side: int = 20,
        require_bbo: bool = True,
    ) -> list[OptionQuote]:
        """Quotes for strikes sampled across the window; ``require_bbo`` waits for bid/ask.

        Outside regular hours IBKR sends no option bid/ask, so waiting for them would
        only burn the timeout; the caller then prices from IB's model price instead.
        """
        from ib_insync import Option

        ib = self._require()
        # Fully specified contracts go straight to reqMktData. Asking for the whole chain
        # per expiry (reqContractDetails without a strike) returns hundreds of contracts
        # for SPY/QQQ and is suspected of stalling IB Gateway's API thread.
        strikes = sample_strikes(
            [strike for strike in info.strikes if strike_low <= strike <= strike_high],
            spot,
            max_strikes_per_side,
        )
        selected = [
            Option(
                info.symbol,
                expiry.strftime("%Y%m%d"),
                strike,
                right,
                "SMART",
                multiplier=str(info.multiplier),
                currency="USD",
                tradingClass=info.trading_class,
            )
            for expiry in expiries
            for strike in strikes
            for right in ("C", "P")
        ]
        if require_bbo:
            def ready(t: Any) -> bool:
                return (
                    valid_price(_float(t.bid)) and valid_price(_float(t.ask))
                    and t.modelGreeks is not None
                )
        else:
            def ready(t: Any) -> bool:
                return t.modelGreeks is not None or valid_price(_float(t.last))
        quotes: list[OptionQuote] = []
        batch_size = min(self.settings.option_batch_size, self.settings.max_concurrent_quotes)
        for batch in _chunks(selected, batch_size):
            tickers = self._stream(batch, "101", ready)
            for contract, ticker in zip(batch, tickers):
                if ticker.modelGreeks is None and not any(
                    valid_price(_float(value)) for value in (ticker.bid, ticker.ask, ticker.last)
                ):
                    continue  # strike not listed for this expiry, or no data at all
                quotes.append(_option_quote(info, contract, ticker))
            ib.sleep(_BATCH_PAUSE_SECONDS)
        return quotes

    def live_quotes(
        self, specs: Sequence[tuple[str, date, float, str]]
    ) -> dict[tuple[str, date, float, str], tuple[float, float]]:
        """Real-time bid/ask for specific legs: (symbol, expiry, strike, right) -> (bid, ask)."""

        from ib_insync import Option

        self._require()
        result: dict[tuple[str, date, float, str], tuple[float, float]] = {}
        batch_size = min(self.settings.option_batch_size, self.settings.max_concurrent_quotes)
        for batch in _chunks(list(specs), batch_size):
            contracts = [
                Option(symbol, expiry.strftime("%Y%m%d"), strike, right, "SMART",
                       multiplier="100", currency="USD", tradingClass=symbol)
                for symbol, expiry, strike, right in batch
            ]
            tickers = self._stream(
                contracts, "",
                lambda t: valid_price(_float(t.bid)) and valid_price(_float(t.ask)),
            )
            for spec, ticker in zip(batch, tickers):
                bid, ask = _positive(ticker.bid), _positive(ticker.ask)
                if bid and ask and ask >= bid:
                    result[spec] = (bid, ask)
        return result

    def pause(self, seconds: float) -> None:
        if self.connected and seconds > 0:
            self._ib.sleep(seconds)

    def _stream(self, contracts: Sequence[Any], ticks: str, ready: Any) -> list[Any]:
        ib = self._require()
        tickers = [ib.reqMktData(contract, ticks, False, False) for contract in contracts]
        deadline = time.monotonic() + self.settings.quote_wait_seconds
        while time.monotonic() < deadline:
            ib.sleep(0.25)
            if all(ready(ticker) for ticker in tickers):
                break
        for contract in contracts:
            try:
                ib.cancelMktData(contract)
            except Exception:
                pass
        return tickers


def _underlying(contract: Any, ticker: Any) -> Underlying:
    close = _float(ticker.close)
    price = _float(ticker.marketPrice())
    if not valid_price(price):
        price = _float(ticker.last) if valid_price(_float(ticker.last)) else close
    change = price / close - 1 if valid_price(price) and valid_price(close) else None
    return Underlying(
        symbol=contract.symbol,
        con_id=contract.conId,
        price=price if valid_price(price) else None,
        bid=_float(ticker.bid),
        ask=_float(ticker.ask),
        close=close,
        change_pct=change,
        market_data_type=_int(ticker.marketDataType),
        quote_time=ticker.time,
    )


def _option_quote(info: ChainInfo, contract: Any, ticker: Any) -> OptionQuote:
    greeks = ticker.modelGreeks
    raw_expiry = str(contract.lastTradeDateOrContractMonth)[:8]
    open_interest = ticker.callOpenInterest if contract.right == "C" else ticker.putOpenInterest
    return OptionQuote(
        symbol=info.symbol,
        expiry=date(int(raw_expiry[:4]), int(raw_expiry[4:6]), int(raw_expiry[6:8])),
        strike=float(contract.strike),
        right=contract.right,
        con_id=contract.conId,
        multiplier=int(float(contract.multiplier or info.multiplier)),
        trading_class=contract.tradingClass or info.trading_class,
        bid=_positive(ticker.bid),
        ask=_positive(ticker.ask),
        last=_positive(ticker.last),
        iv=_positive(getattr(greeks, "impliedVol", None)),
        delta=_float(getattr(greeks, "delta", None)),
        gamma=_float(getattr(greeks, "gamma", None)),
        theta=_float(getattr(greeks, "theta", None)),
        vega=_float(getattr(greeks, "vega", None)),
        volume=_int(ticker.volume),
        open_interest=_int(open_interest),
        quote_time=ticker.time,
        market_data_type=_int(ticker.marketDataType),
        model_price=_positive(getattr(greeks, "optPrice", None)),
    )


def sample_strikes(strikes: Sequence[float], spot: float, count: int) -> list[float]:
    """The strikes nearest the money plus an even spread across the rest of the window."""

    if len(strikes) <= count:
        return list(strikes)
    nearest = sorted(strikes, key=lambda strike: abs(strike - spot))[: max(4, count // 4)]
    remaining = [strike for strike in strikes if strike not in nearest]
    slots = count - len(nearest)
    # Liquidity concentrates on round strikes; use the coarsest grid that still fills the slots.
    for unit in (10.0, 5.0, 2.5, 1.0):
        rounded = [strike for strike in remaining if abs(strike / unit - round(strike / unit)) < 1e-6]
        if len(rounded) >= slots:
            remaining = rounded
            break
    if slots > 0 and remaining:
        step = len(remaining) / slots
        picked = {remaining[min(len(remaining) - 1, int(i * step + step / 2))] for i in range(slots)}
    else:
        picked = set()
    return sorted(set(nearest) | picked)


def _position(item: Any) -> Position | None:
    contract = item.contract
    quantity = _float(item.position)
    if not quantity:
        return None
    expiry = None
    raw = str(getattr(contract, "lastTradeDateOrContractMonth", "") or "")[:8]
    if len(raw) == 8 and raw.isdigit():
        expiry = date(int(raw[:4]), int(raw[4:6]), int(raw[6:8]))
    return Position(
        symbol=contract.symbol,
        sec_type=contract.secType,
        quantity=quantity,
        avg_cost=_float(item.avgCost) or 0.0,
        multiplier=int(float(contract.multiplier or 1)),
        expiry=expiry,
        strike=_float(getattr(contract, "strike", None)) or None,
        right=getattr(contract, "right", None) or None,
        con_id=contract.conId,
    )


def _float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _positive(value: Any) -> float | None:
    number = _float(value)
    return number if number is not None and number > 0 else None


def _int(value: Any) -> int | None:
    number = _float(value)
    return int(number) if number is not None and number >= 0 else None


def _chunks(items: Sequence[Any], size: int) -> list[Sequence[Any]]:
    return [items[index:index + size] for index in range(0, len(items), max(1, size))]


__all__ = ["IbkrMarket", "IbkrUnavailable", "sample_strikes"]
