from __future__ import annotations

import asyncio
import inspect
import json
from contextlib import AbstractContextManager, nullcontext
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from options_copilot.config import OptionsCopilotConfig
from options_copilot.feature_source_diagnostic import validate_feature_source_observation
from options_copilot.storage.canonical import canonical_hash
from options_copilot.gateway.ibkr_readonly import (
    BrokerConnectionError,
    GatewayReadinessStatus,
    IBKRReadOnlyGateway,
    MarketDataPacingError,
    OptionContractRef,
    OptionQualificationError,
    QuoteBatchStatus,
    SessionCalendarReadError,
    UnderlyingIvHistory,
    probe_ibkr_gateway_readiness,
)


NOW = datetime(2026, 8, 3, 13, 30, tzinfo=timezone.utc)


def _historical_lease(
    *,
    allowed: bool = True,
    reason: str | None = None,
) -> AbstractContextManager[object]:
    return nullcontext(SimpleNamespace(allowed=allowed, reason=reason))


def _config(tmp_path: Path) -> OptionsCopilotConfig:
    return OptionsCopilotConfig(data_dir=tmp_path / "data", log_dir=tmp_path / "logs")


class FakeControlEvent:
    def __init__(self):
        self.handlers = []

    def __iadd__(self, handler):
        self.handlers.append(handler)
        return self

    def __isub__(self, handler):
        self.handlers.remove(handler)
        return self

    def emit(self, *args):
        for handler in tuple(self.handlers):
            handler(*args)


class FakeControlClient:
    """Emit real response-end protocol callbacks for the fake owner only."""

    def __init__(self, ib):
        self.ib = ib
        self.request_id = 0

    def getReqId(self):
        self.request_id += 1
        return self.request_id

    def reqAccountSummary(self, request_id, _group, _tags):
        for row in self.ib.accountSummary():
            self.ib.wrapper.accountSummary(
                request_id, "DU_TEST", row.tag, row.value, row.currency,
            )
        self.ib.wrapper.accountSummaryEnd(request_id)

    def cancelAccountSummary(self, _request_id):
        pass

    def reqPositions(self):
        for row in self.ib.portfolio():
            self.ib.wrapper.position(
                "DU_TEST", row.contract, row.position, row.averageCost,
            )
        self.ib.wrapper.positionEnd()

    def cancelPositions(self):
        pass

    def reqAllOpenOrders(self):
        for trade in self.ib.openTrades():
            # This fixture models only currently working raw broker rows.
            if trade.orderStatus.status.upper() in {
                "FILLED", "CANCELLED", "APICANCELLED", "INACTIVE",
            }:
                continue
            trade.order.account = "DU_TEST"
            self.ib.wrapper.openOrder(
                trade.order.orderId, trade.contract, trade.order, trade.orderStatus,
            )
        self.ib.wrapper.openOrderEnd()

    def reqOpenOrders(self):
        self.ib.wrapper.openOrderEnd()


class FakeIB:
    def __init__(self) -> None:
        self.connected = False
        self.connect_kwargs = None
        self.open_trades = []
        self.next_option_contract_id = 9000
        self.quote_mode = "complete"
        self.req_tickers_calls = 0
        self.contract_adjusted = False
        self.contract_multiplier = "100"
        self.quote_overrides = {}
        self.greek_overrides = {}
        self.exchange_time = NOW
        self.market_data_type = 1
        self.market_data_type_requests = []
        self.historical_ticks_by_con_id = {}
        self.req_historical_ticks_calls = []
        self.historical_data = ()
        self.req_historical_data_calls = []
        self.errorEvent = FakeControlEvent()
        self.connectedEvent = FakeControlEvent()
        self.disconnectedEvent = FakeControlEvent()
        self.wrapper = SimpleNamespace(**{
            name: lambda *_args: None
            for name in (
                "accountSummary", "accountSummaryEnd", "position", "positionEnd",
                "openOrder", "openOrderEnd",
            )
        })
        self.client = FakeControlClient(self)

    def connect(self, host, port, **kwargs):
        self.connected = True
        self.connect_kwargs = {"host": host, "port": port, **kwargs}
        self.client.reqPositions()
        self.connectedEvent.emit()

    def isConnected(self):
        return self.connected

    def disconnect(self):
        self.connected = False
        self.disconnectedEvent.emit()

    def managedAccounts(self):
        return ["DU_TEST"]

    def run(self, awaitable):
        return asyncio.get_event_loop().run_until_complete(awaitable)

    def reqMarketDataType(self, market_data_type):
        self.market_data_type_requests.append(market_data_type)

    def accountSummary(self):
        values = {
            "NetLiquidation": "2012.44",
            "EquityWithLoanValue": "1786.63",
            "AvailableFunds": "1786.63",
            "BuyingPower": "1786.63",
            "InitMarginReq": "0",
            "MaintMarginReq": "0",
            "ExcessLiquidity": "1786.63",
            "DayTradesRemaining": "3",
        }
        return [
            SimpleNamespace(tag=k, value=v, currency="USD") for k, v in values.items()
        ]

    def portfolio(self):
        contract = SimpleNamespace(
            conId=838413533,
            symbol="GLD",
            localSymbol="GLD   260821C00375000",
            secType="OPT",
            currency="USD",
            primaryExchange="AMEX",
            exchange="SMART",
            lastTradeDateOrContractMonth="20260821",
            strike=375.0,
            right="C",
            tradingClass="GLD",
            multiplier="100",
        )
        return [
            SimpleNamespace(
                contract=contract,
                position=1,
                averageCost=1004.6895,
                marketPrice=6.08,
                marketValue=608.0,
                unrealizedPNL=-396.0,
                realizedPNL=0,
            )
        ]

    def openTrades(self):
        return self.open_trades

    def qualifyContracts(self, *contracts):
        if contracts and all(getattr(item, "secType", "") == "STK" for item in contracts):
            for index, contract in enumerate(contracts, start=8488):
                contract.conId = index
                contract.primaryExchange = "NASDAQ"
            return list(contracts)
        result = []
        for contract in contracts:
            self.next_option_contract_id += 1
            contract.conId = self.next_option_contract_id
            contract.localSymbol = (
                f"{contract.symbol}-{contract.right}-{contract.strike}"
            )
            contract.multiplier = "100"
            result.append(contract)
        return result

    def reqSecDefOptParams(self, *_args):
        return [
            SimpleNamespace(
                exchange="SMART",
                tradingClass="SPY",
                multiplier="100",
                expirations={"20260821", "20260828", "20260918"},
                strikes={620.0, 625.0, 630.0},
            )
        ]

    def reqContractDetails(self, contract):
        if getattr(contract, "secType", "") == "STK":
            if not int(getattr(contract, "conId", 0) or 0):
                contract.conId = 8488
                contract.primaryExchange = "NASDAQ"
            return [
                SimpleNamespace(
                    contract=contract,
                    liquidHours="20260803:0930-20260803:1600",
                    tradingHours="20260803:0930-20260803:1600",
                    timeZoneId="US/Eastern",
                )
            ]
        resolved = SimpleNamespace(
            conId=contract.conId,
            localSymbol=contract.localSymbol,
            tradingClass=contract.tradingClass,
            multiplier=self.contract_multiplier,
            exchange=contract.exchange,
            lastTradeDateOrContractMonth=contract.lastTradeDateOrContractMonth,
            strike=contract.strike,
            right=contract.right,
            secType="OPT",
            currency=contract.currency,
        )
        return [
            SimpleNamespace(
                contract=resolved,
                adjusted=self.contract_adjusted,
            )
        ]

    def reqTickers(self, *contracts):
        self.req_tickers_calls += 1
        if self.quote_mode == "timeout":
            raise TimeoutError("fixture timeout")
        if self.quote_mode == "cancelled":
            raise asyncio.CancelledError()
        rows = []
        for contract in contracts:
            is_put = str(getattr(contract, "right", "")).upper() == "P"
            quote_values = {
                "bid": 1.0,
                "ask": 1.2,
                "last": 1.1,
                "close": 1.05,
                "volume": 123,
                "callVolume": 80,
                "putVolume": 43,
                "callOpenInterest": 500,
                "putOpenInterest": 600,
            }
            quote_values.update(self.quote_overrides)
            greek_values = {
                "impliedVol": 0.22,
                "delta": -0.40 if is_put else 0.40,
                "gamma": 0.03,
                "theta": -0.05,
                "vega": 0.12,
            }
            greek_values.update(self.greek_overrides)
            rows.append(
                SimpleNamespace(
                    contract=contract,
                    time=NOW,
                    exchangeTime=self.exchange_time,
                    marketDataType=self.market_data_type,
                    modelGreeks=SimpleNamespace(**greek_values),
                    **quote_values,
                )
            )
        if self.quote_mode == "partial":
            return rows[:-1]
        if self.quote_mode == "duplicate":
            return [rows[0], rows[0]]
        if self.quote_mode == "extra":
            extra = SimpleNamespace(**vars(rows[0]))
            extra.contract = SimpleNamespace(conId=999999)
            rows.append(extra)
        return rows

    def reqHistoricalTicks(
        self,
        contract,
        start_date_time,
        end_date_time,
        number_of_ticks,
        what_to_show,
        use_rth,
        ignore_size,
    ):
        self.req_historical_ticks_calls.append(
            {
                "con_id": contract.conId,
                "start_date_time": start_date_time,
                "end_date_time": end_date_time,
                "number_of_ticks": number_of_ticks,
                "what_to_show": what_to_show,
                "use_rth": use_rth,
                "ignore_size": ignore_size,
            }
        )
        value = self.historical_ticks_by_con_id.get(contract.conId, ())
        if isinstance(value, BaseException):
            raise value
        return value

    def reqHistoricalData(self, contract, **kwargs):
        self.req_historical_data_calls.append(
            {"contract": contract, **kwargs}
        )
        if isinstance(self.historical_data, BaseException):
            raise self.historical_data
        return self.historical_data


def _daily_iv_bars(*, count: int = 12) -> tuple[SimpleNamespace, ...]:
    return tuple(
        SimpleNamespace(
            date=(NOW.date() - timedelta(days=count - index + 1)).strftime(
                "%Y%m%d"
            ),
            close=0.18 + index / 1000,
        )
        for index in range(count)
    )


def test_feature_histories_are_bounded_observations_with_exact_requests(tmp_path):
    fake = FakeIB()
    fake.historical_data = _daily_iv_bars(count=260)
    leases: list[str] = []

    def lease(kind):
        leases.append(kind)
        return _historical_lease()

    gateway = IBKRReadOnlyGateway(
        _config(tmp_path), ib_factory=lambda: fake,
        historical_request_lease_factory=lambda: lease("historical"),
        market_data_request_lease_factory=lease, now=lambda: NOW,
    )
    gateway.connect()
    try:
        prices = gateway.feature_price_history("SPY", end_at=NOW)
        ivs = gateway.feature_iv_history("SPY", end_at=NOW)
        for result in (prices, ivs):
            assert validate_feature_source_observation(
                result, kind=result["kind"], symbol="SPY", cutoff=NOW,
            ) == result
            assert result["status"] == "DELIVERED"
            assert result["prior_completed_bar_count"] == 260
            assert result["enough_prior_bars"] is True
            assert result["calendar_coverage_verified"] is False
            assert result["point_in_time_verified"] is False
            assert result["model_input_complete"] is False
            assert result["production_eligible"] is False
            assert result["decision_authority"] == "OBSERVATION_ONLY"
            assert result["basis_status"] == "PROVIDER_NATIVE_UNRESOLVED"
            assert result["request_sent"] is True
            assert result["available_at"] == NOW.isoformat()
            assert result["content_hash"] == canonical_hash({
                key: value for key, value in result.items() if key != "content_hash"
            })
            json.dumps(result)
        assert prices["request_parameters"]["durationStr"] == "1 Y"
        assert prices["request_parameters"]["endDateTime"] == ""
        assert prices["request_parameters"]["whatToShow"] == "ADJUSTED_LAST"
        assert ivs["request_parameters"]["durationStr"] == "2 Y"
        assert ivs["request_parameters"]["whatToShow"] == "OPTION_IMPLIED_VOLATILITY"
        assert leases == ["secdef", "historical", "historical"]
        assert fake.RequestTimeout == 7.0
        assert all(0 < row["timeout"] <= 4 for row in fake.req_historical_data_calls)
    finally:
        gateway.disconnect()


def test_feature_history_retains_invalid_dates_exclusions_and_empty_uncertainty(tmp_path):
    fake = FakeIB()
    fake.historical_data = (
        SimpleNamespace(date=NOW.date(), close=0.2),
        SimpleNamespace(date="invalid-date", close=0.2),
        SimpleNamespace(date=NOW.date() - timedelta(days=1), close=0.2),
        SimpleNamespace(date=NOW.date() - timedelta(days=1), close=0.2),
    )
    gateway = IBKRReadOnlyGateway(
        _config(tmp_path), ib_factory=lambda: fake,
        historical_request_lease_factory=_historical_lease,
        market_data_request_lease_factory=lambda _kind: _historical_lease(),
        now=lambda: NOW,
    )
    gateway.connect()
    try:
        observed = gateway.feature_iv_history("SPY", end_at=NOW)
        assert observed["prior_completed_bar_count"] == 1
        assert validate_feature_source_observation(
            observed, kind=observed["kind"], symbol="SPY", cutoff=NOW,
        ) == observed
        assert observed["excluded_current_or_future_bar_count"] == 1
        assert observed["invalid_bar_count"] == 1
        assert observed["duplicate_prior_date_count"] == 1
        assert observed["bars"][1]["raw_date"] == "invalid-date"
        assert observed["status"] == "PARTIAL"
        fake.historical_data = ()
        empty = gateway.feature_iv_history("SPY", end_at=NOW)
        assert validate_feature_source_observation(
            empty, kind=empty["kind"], symbol="SPY", cutoff=NOW,
        ) == empty
        assert empty["status"] == "UNAVAILABLE"
        assert "FEATURE_HISTORY_EMPTY_OR_TIMEOUT" in empty["reason_codes"]
        assert not any("NOT_SUBSCRIBED" in item for item in empty["reason_codes"])
        fake.historical_data = _daily_iv_bars(count=805)
        bounded = gateway.feature_iv_history("SPY", end_at=NOW)
        assert validate_feature_source_observation(
            bounded, kind=bounded["kind"], symbol="SPY", cutoff=NOW,
        ) == bounded
        assert len(bounded["bars"]) == 800
        assert bounded["received_bar_count"] == 805
        assert "FEATURE_SOURCE_ROW_LIMIT_EXCEEDED" in bounded["reason_codes"]
    finally:
        gateway.disconnect()


def test_feature_history_pacing_denial_sends_no_historical_request(tmp_path):
    fake = FakeIB()
    gateway = IBKRReadOnlyGateway(
        _config(tmp_path), ib_factory=lambda: fake,
        historical_request_lease_factory=lambda: _historical_lease(
            allowed=False, reason="PACING_COOLDOWN_ACTIVE",
        ),
        market_data_request_lease_factory=lambda _kind: _historical_lease(),
        now=lambda: NOW,
    )
    gateway.connect()
    try:
        observed = gateway.feature_iv_history("SPY", end_at=NOW)
        assert observed["status"] == "UNAVAILABLE"
        assert validate_feature_source_observation(
            observed, kind=observed["kind"], symbol="SPY", cutoff=NOW,
        ) == observed
        assert observed["request_sent"] is False
        assert "FEATURE_PACING_PACING_COOLDOWN_ACTIVE" in observed["reason_codes"]
        assert fake.req_historical_data_calls == []
    finally:
        gateway.disconnect()


def test_feature_history_numeric_api_errors_bind_only_own_request(tmp_path):
    class Event:
        def __init__(self):
            self.handlers = []

        def __iadd__(self, handler):
            self.handlers.append(handler)
            return self

        def __isub__(self, handler):
            self.handlers.remove(handler)
            return self

    class Bars(list):
        reqId = 442

    class ErrorIB(FakeIB):
        def __init__(self):
            super().__init__()
            self.errorEvent = Event()

        def reqHistoricalData(self, contract, **kwargs):
            for handler in self.errorEvent.handlers:
                handler(441, 354, "unrelated request details", contract)
                handler(442, 162, "private details can include a credential", contract)
            return Bars()

    fake = ErrorIB()
    gateway = IBKRReadOnlyGateway(
        _config(tmp_path), ib_factory=lambda: fake,
        historical_request_lease_factory=_historical_lease,
        market_data_request_lease_factory=lambda _kind: _historical_lease(),
        now=lambda: NOW,
    )
    gateway.connect()
    try:
        observed = gateway.feature_iv_history("SPY", end_at=NOW)
        assert observed["broker_request_id"] == 442
        assert validate_feature_source_observation(
            observed, kind=observed["kind"], symbol="SPY", cutoff=NOW,
        ) == observed
        assert observed["broker_error_codes"] == [162]
        assert "FEATURE_SOURCE_BROKER_ERROR:162" in observed["reason_codes"]
        assert not any("NOT_SUBSCRIBED" in item for item in observed["reason_codes"])
        assert "private details" not in json.dumps(observed)
        assert len(fake.errorEvent.handlers) == 1  # Owned upstream guard remains.
    finally:
        gateway.disconnect()


@pytest.mark.parametrize("deliver_new_tick", [True, False])
def test_feature_current_iv_requires_own_new_tick24_and_cancels_only_own_stream(tmp_path, deliver_new_tick):
    class Event:
        def __init__(self):
            self.handlers = []

        def __iadd__(self, handler):
            self.handlers.append(handler)
            return self

        def __isub__(self, handler):
            self.handlers.remove(handler)
            return self

        def emit(self, *args):
            for handler in tuple(self.handlers):
                handler(*args)

    class FeatureIB(FakeIB):
        def __init__(self):
            super().__init__()
            self.pendingTickersEvent = Event()
            self.errorEvent = Event()
            self.wrapper.reqId2Ticker = {}
            self.elapsed = 0.0
            self.cancelled = []
            self.requested = None

        def reqMktData(self, contract, generic_tick_list, snapshot, regulatory_snapshot):
            assert generic_tick_list == "106"
            assert snapshot is regulatory_snapshot is False
            self.requested = contract
            self.ticker = SimpleNamespace(
                contract=contract, impliedVolatility=0.99, ticks=[], marketDataType=1,
            )
            self.wrapper.reqId2Ticker[771] = self.ticker
            return self.ticker

        def sleep(self, seconds):
            self.elapsed += seconds
            if deliver_new_tick:
                self.ticker.ticks = [SimpleNamespace(tickType=24, time=NOW, price=0.21)]
                self.pendingTickersEvent.emit((self.ticker,))

        def cancelMktData(self, contract):
            self.cancelled.append(contract)

    fake = FeatureIB()
    gateway = IBKRReadOnlyGateway(
        _config(tmp_path), ib_factory=lambda: fake,
        market_data_request_lease_factory=lambda _kind: _historical_lease(),
        now=lambda: NOW, monotonic=lambda: fake.elapsed,
    )
    gateway.connect()
    try:
        observed = gateway.feature_current_iv("SPY", end_at=NOW)
        assert validate_feature_source_observation(
            observed, kind=observed["kind"], symbol="SPY", cutoff=NOW,
        ) == observed
        assert observed["value"] == ("0.21" if deliver_new_tick else None)
        assert observed["status"] == ("DELIVERED" if deliver_new_tick else "UNAVAILABLE")
        assert observed["received_at"] == (NOW.isoformat() if deliver_new_tick else None)
        assert observed["source_event_timestamp"] is None
        assert observed["model_input_complete"] is False
        assert observed["broker_request_id"] == 771
        assert fake.cancelled == [fake.requested]
        assert fake.requested is not gateway._underlying_identity_cache["SPY"]
        assert fake.pendingTickersEvent.handlers == []
        assert len(fake.errorEvent.handlers) == 1  # Owned upstream guard remains.
        assert fake.market_data_type_requests == []
    finally:
        gateway.disconnect()


def test_underlying_iv_history_uses_fixed_stock_basis_and_exact_request(
    tmp_path: Path,
) -> None:
    fake = FakeIB()
    fake.historical_data = _daily_iv_bars()
    gateway = IBKRReadOnlyGateway(
        _config(tmp_path),
        ib_factory=lambda: fake,
        historical_request_lease_factory=_historical_lease,
        now=lambda: NOW,
    )

    with gateway:
        history = gateway.underlying_iv_history("spy", end_at=NOW)

    assert isinstance(history, UnderlyingIvHistory)
    assert history.verify_hash()
    assert history.symbol == "SPY"
    assert len(history.points) == 12
    assert len(fake.req_historical_data_calls) == 1
    request = fake.req_historical_data_calls[0]
    assert request["contract"].secType == "STK"
    assert request["contract"].symbol == "SPY"
    assert request["contract"].exchange == "SMART"
    assert request["contract"].currency == "USD"
    assert request["endDateTime"] == NOW.astimezone(
        ZoneInfo("America/New_York")
    ).replace(hour=0, minute=0, second=0, microsecond=0)
    assert request["durationStr"] == "30 D"
    assert request["barSizeSetting"] == "1 day"
    assert request["whatToShow"] == "OPTION_IMPLIED_VOLATILITY"
    assert request["useRTH"] is True
    assert request["formatDate"] == 1
    assert request["keepUpToDate"] is False
    assert request["timeout"] == 8.0


def test_underlying_iv_history_lease_covers_actual_broker_call(
    tmp_path: Path,
) -> None:
    fake = FakeIB()
    fake.historical_data = _daily_iv_bars()
    lease_active = False

    class Lease:
        def __enter__(self):
            nonlocal lease_active
            lease_active = True
            return SimpleNamespace(allowed=True, reason=None)

        def __exit__(self, *_args):
            nonlocal lease_active
            lease_active = False

    original = fake.reqHistoricalData

    def request_while_leased(contract, **kwargs):
        assert lease_active is True
        return original(contract, **kwargs)

    fake.reqHistoricalData = request_while_leased  # type: ignore[method-assign]
    gateway = IBKRReadOnlyGateway(
        _config(tmp_path),
        ib_factory=lambda: fake,
        historical_request_lease_factory=Lease,
        now=lambda: NOW,
    )

    with gateway:
        gateway.underlying_iv_history("SPY", end_at=NOW)

    assert lease_active is False


@pytest.mark.parametrize(
    "bars",
    (
        _daily_iv_bars(count=9),
        _daily_iv_bars()[:-1] + (_daily_iv_bars()[-2],),
        _daily_iv_bars()[:-1]
        + (SimpleNamespace(date=NOW.strftime("%Y%m%d"), close=0.2),),
        _daily_iv_bars()[:-1]
        + (SimpleNamespace(date="20260801", close=float("nan")),),
    ),
)
def test_underlying_iv_history_rejects_non_authoritative_series(
    tmp_path: Path,
    bars: tuple[SimpleNamespace, ...],
) -> None:
    fake = FakeIB()
    fake.historical_data = bars
    gateway = IBKRReadOnlyGateway(
        _config(tmp_path),
        ib_factory=lambda: fake,
        historical_request_lease_factory=_historical_lease,
        now=lambda: NOW,
    )

    with gateway, pytest.raises(BrokerConnectionError):
        gateway.underlying_iv_history("SPY", end_at=NOW)


def test_underlying_iv_history_requires_approved_historical_pacing(
    tmp_path: Path,
) -> None:
    fake = FakeIB()
    fake.historical_data = _daily_iv_bars()
    gateway = IBKRReadOnlyGateway(
        _config(tmp_path),
        ib_factory=lambda: fake,
        now=lambda: NOW,
    )

    with gateway, pytest.raises(MarketDataPacingError) as raised:
        gateway.underlying_iv_history("SPY", end_at=NOW)

    assert raised.value.reason_code == "PACING_CAPABILITY_MISSING"
    assert fake.req_historical_data_calls == []


class _FakeSocket:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


def test_live_gateway_defaults_are_port_4001_and_permanently_readonly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("OPTIONS_COPILOT_IBKR_PORT", raising=False)
    monkeypatch.setenv("OPTIONS_COPILOT_IBKR_READONLY", "false")

    config = OptionsCopilotConfig.from_env()

    assert config.ibkr_port == 4001
    assert config.ibkr_readonly is True


def test_gateway_refuses_non_readonly_config_before_constructing_client(
    tmp_path: Path,
) -> None:
    factory_calls = 0

    def factory() -> FakeIB:
        nonlocal factory_calls
        factory_calls += 1
        return FakeIB()

    gateway = IBKRReadOnlyGateway(
        OptionsCopilotConfig(
            data_dir=tmp_path / "data",
            log_dir=tmp_path / "logs",
            ibkr_readonly=False,
        ),
        ib_factory=factory,
    )

    with pytest.raises(BrokerConnectionError, match="read-only policy"):
        gateway.connect()
    assert factory_calls == 0


def test_listener_probe_never_authenticates_and_closes_bounded_tcp_probe(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    opened = _FakeSocket()
    calls: list[tuple[tuple[str, int], float]] = []

    def connector(address: tuple[str, int], timeout: float) -> _FakeSocket:
        calls.append((address, timeout))
        return opened

    result = probe_ibkr_gateway_readiness(
        config,
        socket_connector=connector,
        dependency_check=lambda: True,
        now=lambda: NOW,
        timeout_seconds=99,
    )

    assert calls == [(("127.0.0.1", 4001), 5.0)]
    assert opened.closed is True
    assert result.status is GatewayReadinessStatus.LISTENER_READY
    assert result.readonly is True
    assert result.listener_reachable is True
    assert result.authenticated is False
    assert result.market_data_verified is False
    assert result.scope == "TCP_LISTENER_AND_CLIENT_LIBRARY_ONLY"


def test_listener_probe_fails_closed_without_listener_or_client_library(
    tmp_path: Path,
) -> None:
    def unavailable(_address: tuple[str, int], _timeout: float) -> object:
        raise ConnectionRefusedError("fixture refused")

    result = probe_ibkr_gateway_readiness(
        _config(tmp_path),
        socket_connector=unavailable,
        dependency_check=lambda: False,
        now=lambda: NOW,
    )

    assert result.status is GatewayReadinessStatus.UNAVAILABLE
    assert result.listener_reachable is False
    assert result.client_library_available is False
    assert result.blockers == (
        "IB_INSYNC_CLIENT_UNAVAILABLE",
        "IBKR_GATEWAY_LISTENER_UNREACHABLE",
    )


def test_connection_is_forced_readonly_and_account_position_snapshots_are_decimal(
    tmp_path: Path,
) -> None:
    fake = FakeIB()
    gateway = IBKRReadOnlyGateway(
        _config(tmp_path), ib_factory=lambda: fake, now=lambda: NOW
    )
    gateway.connect()

    assert fake.connect_kwargs["readonly"] is True
    assert fake.RequestTimeout == 7.0
    account = gateway.account_snapshot()
    positions = gateway.positions()
    assert account.net_liquidation == Decimal("2012.44")
    assert account.day_trades_remaining == 3
    assert positions[0].symbol == "GLD"
    assert positions[0].average_cost == Decimal("1004.6895")
    assert positions[0].expiration == date(2026, 8, 21)
    assert positions[0].strike == Decimal("375.0")
    assert positions[0].right == "C"
    assert positions[0].trading_class == "GLD"
    assert positions[0].multiplier == 100
    gateway.disconnect()
    assert gateway.connected is False


def test_gateway_refuses_reads_without_connection(tmp_path: Path) -> None:
    gateway = IBKRReadOnlyGateway(_config(tmp_path), ib_factory=FakeIB, now=lambda: NOW)
    with pytest.raises(BrokerConnectionError):
        gateway.account_snapshot()


def test_option_chain_filters_prohibited_dte_and_quotes_every_leg(
    tmp_path: Path,
) -> None:
    fake = FakeIB()
    gateway = IBKRReadOnlyGateway(
        _config(tmp_path), ib_factory=lambda: fake, now=lambda: NOW
    )
    gateway.connect()
    expirations = gateway.option_expirations("SPY", min_dte=14, max_dte=35)
    assert [item.expiration for item in expirations] == [
        date(2026, 8, 21),
        date(2026, 8, 28),
    ]
    assert expirations[0].strikes == (
        Decimal("620.0"),
        Decimal("625.0"),
        Decimal("630.0"),
    )

    contracts = gateway.qualify_option_contracts(
        "SPY", date(2026, 8, 21), [625], rights=("C", "P")
    )
    quotes = gateway.option_quotes(contracts)
    assert len(quotes) == 2
    assert quotes[0].midpoint == Decimal("1.1")
    assert quotes[0].open_interest == 500
    assert quotes[1].open_interest == 600
    assert all(item.has_executable_market for item in quotes)
    assert all(not item.is_delayed for item in quotes)


def test_gateway_holds_one_pacing_lease_per_wire_request(
    tmp_path: Path,
) -> None:
    active = {"secdef": 0, "snapshot_quote": 0}
    lease_calls: list[str] = []

    class Lease:
        def __init__(self, request_class: str) -> None:
            self.request_class = request_class

        def __enter__(self) -> object:
            active[self.request_class] += 1
            lease_calls.append(self.request_class)
            return SimpleNamespace(allowed=True, reason=None)

        def __exit__(self, *_: object) -> None:
            active[self.request_class] -= 1

    class PacingAwareIB(FakeIB):
        def __init__(self) -> None:
            super().__init__()
            self.next_contract_id = 9000

        def qualifyContracts(self, *contracts):
            # Qualification is serialized because the signed secdef
            # concurrency limit may be lower than the bounded strike window.
            assert active["secdef"] == 1
            results = []
            for contract in contracts:
                self.next_contract_id += 1
                contract.conId = self.next_contract_id
                contract.primaryExchange = "NASDAQ"
                if getattr(contract, "secType", "") != "STK":
                    contract.localSymbol = (
                        f"{contract.symbol}-{contract.right}-{contract.strike}"
                    )
                    contract.multiplier = "100"
                results.append(contract)
            return results

        def reqSecDefOptParams(self, *_args):
            assert active["secdef"] == 1
            return super().reqSecDefOptParams(*_args)

        def reqContractDetails(self, contract):
            assert active["secdef"] == 1
            return super().reqContractDetails(contract)

        def reqTickers(self, *contracts):
            assert active["snapshot_quote"] == len(contracts)
            return super().reqTickers(*contracts)

    fake = PacingAwareIB()
    gateway = IBKRReadOnlyGateway(
        _config(tmp_path),
        ib_factory=lambda: fake,
        market_data_request_lease_factory=lambda request_class: Lease(
            request_class
        ),
        now=lambda: NOW,
    )
    gateway.connect()

    expirations = gateway.option_expirations("SPY", min_dte=14, max_dte=35)
    contracts = gateway.qualify_option_contracts(
        "SPY",
        expirations[0].expiration,
        [Decimal("625")],
        rights=("C", "P"),
    )
    definitions = gateway.option_contract_definitions(contracts)
    quote_batch = gateway.option_quote_batch(contracts)
    underlying = gateway.underlying_quotes(("SPY",))
    calendar = gateway.options_session_hours("SPY")

    assert len(contracts) == len(definitions) == len(quote_batch.quotes) == 2
    assert len(underlying) == 1
    assert calendar.contract_id > 0
    assert lease_calls.count("secdef") == 7
    assert lease_calls.count("snapshot_quote") == 3
    assert active == {"secdef": 0, "snapshot_quote": 0}
    gateway.disconnect()


def test_underlying_identity_cache_reuses_chain_quote_and_iv_basis_until_rollover(
    tmp_path: Path,
) -> None:
    current = [NOW]
    lease_calls: list[str] = []

    class CountingIB(FakeIB):
        def __init__(self) -> None:
            super().__init__()
            self.stock_qualifications = 0
            self.historical_data = _daily_iv_bars()

        def qualifyContracts(self, *contracts):
            if contracts and all(
                getattr(contract, "secType", "") == "STK"
                for contract in contracts
            ):
                self.stock_qualifications += len(contracts)
            return super().qualifyContracts(*contracts)

    def market_lease(request_class: str) -> AbstractContextManager[object]:
        lease_calls.append(request_class)
        return nullcontext(SimpleNamespace(allowed=True, reason=None))

    fake = CountingIB()
    gateway = IBKRReadOnlyGateway(
        _config(tmp_path),
        ib_factory=lambda: fake,
        historical_request_lease_factory=_historical_lease,
        market_data_request_lease_factory=market_lease,
        now=lambda: current[0],
    )
    gateway.connect()

    gateway.option_expirations("SPY", min_dte=14, max_dte=35)
    gateway.underlying_quotes(("SPY",))
    gateway.underlying_iv_history("SPY", end_at=NOW)

    assert fake.stock_qualifications == 1
    assert lease_calls.count("secdef") == 2

    current[0] = NOW + timedelta(days=1)
    gateway.underlying_quotes(("SPY",))

    assert fake.stock_qualifications == 2
    assert lease_calls.count("secdef") == 3
    gateway.disconnect()


def test_calendar_seeds_underlying_identity_cache_and_reconnect_clears_it(
    tmp_path: Path,
) -> None:
    class CountingIB(FakeIB):
        def __init__(self) -> None:
            super().__init__()
            self.stock_qualifications = 0

        def qualifyContracts(self, *contracts):
            if contracts and all(
                getattr(contract, "secType", "") == "STK"
                for contract in contracts
            ):
                self.stock_qualifications += len(contracts)
            return super().qualifyContracts(*contracts)

    fake = CountingIB()
    gateway = IBKRReadOnlyGateway(
        _config(tmp_path),
        ib_factory=lambda: fake,
        now=lambda: NOW,
    )
    gateway.connect()

    gateway.options_session_hours("SPY")
    gateway.underlying_quotes(("SPY",))
    assert fake.stock_qualifications == 0

    gateway.disconnect()
    gateway.connect()
    gateway.underlying_quotes(("SPY",))
    assert fake.stock_qualifications == 1
    gateway.disconnect()


def test_option_qualification_serializes_a_batch_larger_than_pacing_concurrency(
    tmp_path: Path,
) -> None:
    active = 0
    peak_active = 0
    lease_calls = 0
    wire_batch_sizes: list[int] = []

    class Lease:
        def __init__(self) -> None:
            self.reserved = False

        def __enter__(self) -> object:
            nonlocal active, peak_active, lease_calls
            lease_calls += 1
            if active >= 2:
                return SimpleNamespace(
                    allowed=False,
                    reason="PACING_CONCURRENCY_LIMIT",
                )
            active += 1
            peak_active = max(peak_active, active)
            self.reserved = True
            return SimpleNamespace(allowed=True, reason=None)

        def __exit__(self, *_: object) -> None:
            nonlocal active
            if self.reserved:
                active -= 1

    class CountingIB(FakeIB):
        def __init__(self) -> None:
            super().__init__()
            self.next_contract_id = 9500

        def qualifyContracts(self, *contracts):
            wire_batch_sizes.append(len(contracts))
            assert active == 1
            for contract in contracts:
                self.next_contract_id += 1
                contract.conId = self.next_contract_id
                contract.localSymbol = (
                    f"{contract.symbol}-{contract.right}-{contract.strike}"
                )
                contract.multiplier = "100"
            return list(contracts)

    fake = CountingIB()
    gateway = IBKRReadOnlyGateway(
        _config(tmp_path),
        ib_factory=lambda: fake,
        market_data_request_lease_factory=lambda request_class: (
            Lease()
            if request_class == "secdef"
            else nullcontext(SimpleNamespace(allowed=True, reason=None))
        ),
        now=lambda: NOW,
    )
    gateway.connect()

    contracts = gateway.qualify_option_contracts(
        "SPY",
        date(2026, 8, 21),
        [Decimal("620"), Decimal("625"), Decimal("630")],
        rights=("C", "P"),
    )

    assert len(contracts) == 6
    assert wire_batch_sizes == [1, 1, 1, 1, 1, 1]
    assert lease_calls == 6
    assert peak_active == 1
    assert active == 0
    gateway.disconnect()


def test_paced_option_qualification_expands_owner_deadline_for_wire_batch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed_timeout: list[float | None] = []
    gateway = IBKRReadOnlyGateway(
        _config(tmp_path),
        ib_factory=FakeIB,
        market_data_request_lease_factory=lambda _request_class: nullcontext(
            SimpleNamespace(allowed=True, reason=None)
        ),
        now=lambda: NOW,
    )

    def capture_owner_call(
        _operation: object,
        *,
        create: bool = False,
        timeout_seconds: float | None = None,
    ) -> tuple[object, ...]:
        assert create is False
        observed_timeout.append(timeout_seconds)
        return ()

    monkeypatch.setattr(gateway, "_call_on_owner", capture_owner_call)

    gateway.qualify_option_contracts(
        "SPY",
        date(2026, 8, 21),
        [Decimal("620"), Decimal("625"), Decimal("630")],
        rights=("C", "P"),
    )

    assert observed_timeout[0] is not None
    assert observed_timeout[0] > gateway.config.ibkr_timeout_seconds
    assert observed_timeout[0] <= 60.0


def test_paced_option_qualification_reports_exact_wire_timeout(
    tmp_path: Path,
) -> None:
    class TimeoutIB(FakeIB):
        def qualifyContracts(self, *contracts):
            if contracts and all(
                getattr(item, "secType", "") == "STK" for item in contracts
            ):
                return super().qualifyContracts(*contracts)
            raise TimeoutError("fixture secdef timeout")

    gateway = IBKRReadOnlyGateway(
        _config(tmp_path),
        ib_factory=TimeoutIB,
        market_data_request_lease_factory=lambda _request_class: nullcontext(
            SimpleNamespace(allowed=True, reason=None)
        ),
        now=lambda: NOW,
    )
    gateway.connect()

    with pytest.raises(OptionQualificationError) as raised:
        gateway.qualify_option_contracts(
            "SPY",
            date(2026, 8, 21),
            [Decimal("620"), Decimal("625")],
            rights=("C",),
        )

    assert raised.value.reason_code == "OPTION_QUALIFICATION_TIMEOUT"
    assert raised.value.symbol == "SPY"
    assert raised.value.requested_count == 2
    assert raised.value.completed_count == 0
    assert raised.value.failed_right == "C"
    assert raised.value.failed_strike == Decimal("620")
    gateway.disconnect()


def test_paced_option_qualification_keeps_valid_contract_after_missing_strike(
    tmp_path: Path,
) -> None:
    class PartialIB(FakeIB):
        def qualifyContracts(self, *contracts):
            if contracts and all(
                getattr(item, "secType", "") == "STK" for item in contracts
            ):
                return super().qualifyContracts(*contracts)
            if float(contracts[0].strike) == 620.0:
                return []
            return super().qualifyContracts(*contracts)

    gateway = IBKRReadOnlyGateway(
        _config(tmp_path),
        ib_factory=PartialIB,
        market_data_request_lease_factory=lambda _request_class: nullcontext(
            SimpleNamespace(allowed=True, reason=None)
        ),
        now=lambda: NOW,
    )
    gateway.connect()

    contracts = gateway.qualify_option_contracts(
        "SPY",
        date(2026, 8, 21),
        [Decimal("620"), Decimal("625")],
        rights=("C",),
    )

    assert tuple(item.strike for item in contracts) == (Decimal("625.0"),)
    gateway.disconnect()


def test_option_qualification_mid_batch_denial_discards_partial_results(
    tmp_path: Path,
) -> None:
    active = 0
    lease_calls = 0
    wire_calls = 0

    class Lease:
        def __init__(self) -> None:
            self.reserved = False

        def __enter__(self) -> object:
            nonlocal active, lease_calls
            lease_calls += 1
            if lease_calls == 3:
                return SimpleNamespace(
                    allowed=False,
                    reason="PACING_REQUEST_WINDOW_EXHAUSTED",
                )
            active += 1
            self.reserved = True
            return SimpleNamespace(allowed=True, reason=None)

        def __exit__(self, *_: object) -> None:
            nonlocal active
            if self.reserved:
                active -= 1

    class CountingIB(FakeIB):
        def qualifyContracts(self, *contracts):
            nonlocal wire_calls
            wire_calls += 1
            assert active == 1
            for contract in contracts:
                contract.conId = 9700 + wire_calls
                contract.localSymbol = (
                    f"{contract.symbol}-{contract.right}-{contract.strike}"
                )
                contract.multiplier = "100"
            return list(contracts)

    gateway = IBKRReadOnlyGateway(
        _config(tmp_path),
        ib_factory=CountingIB,
        market_data_request_lease_factory=lambda request_class: (
            Lease()
            if request_class == "secdef"
            else nullcontext(SimpleNamespace(allowed=True, reason=None))
        ),
        now=lambda: NOW,
    )
    gateway.connect()

    with pytest.raises(MarketDataPacingError) as raised:
        gateway.qualify_option_contracts(
            "SPY",
            date(2026, 8, 21),
            [Decimal("620"), Decimal("625")],
            rights=("C", "P"),
        )

    assert raised.value.request_class == "secdef"
    assert raised.value.reason_code == "PACING_REQUEST_WINDOW_EXHAUSTED"
    assert lease_calls == 3
    assert wire_calls == 2
    assert active == 0
    gateway.disconnect()


def test_paced_option_quotes_use_short_lived_streaming_batch(
    tmp_path: Path,
) -> None:
    active = {
        "historical": 0,
        "scanner": 0,
        "secdef": 0,
        "snapshot_quote": 0,
        "streaming_quote": 0,
    }
    lease_calls: list[str] = []

    class Lease:
        def __init__(self, request_class: str) -> None:
            self.request_class = request_class

        def __enter__(self) -> object:
            active[self.request_class] += 1
            lease_calls.append(self.request_class)
            return SimpleNamespace(allowed=True, reason=None)

        def __exit__(self, *_: object) -> None:
            active[self.request_class] -= 1

    class StreamingIB(FakeIB):
        def __init__(self) -> None:
            super().__init__()
            self.streaming_requests: list[object] = []
            self.streaming_cancellations: list[object] = []
            self.tick_by_tick_attempts = 0
            self.wrapper.reqId2Ticker = {}

        def reqMktData(
            self,
            contract,
            generic_tick_list,
            snapshot,
            regulatory_snapshot,
        ):
            assert generic_tick_list == "100,101,106"
            assert snapshot is regulatory_snapshot is False
            self.streaming_requests.append(contract)
            ticker = super().reqTickers(contract)[0]
            self.wrapper.reqId2Ticker[7000 + contract.conId] = ticker
            return ticker

        def cancelMktData(self, contract) -> None:
            self.streaming_cancellations.append(contract)

        def reqTickByTickData(self, *_args: object) -> object:
            self.tick_by_tick_attempts += 1
            raise AssertionError(
                "option BidAsk timestamps must use the paced historical path"
            )

        def cancelTickByTickData(self, *_args: object) -> None:
            raise AssertionError("no option tick-by-tick request was opened")

        def sleep(self, _seconds: float) -> None:
            raise AssertionError("complete fixture must not wait for more ticks")

    fake = StreamingIB()
    gateway = IBKRReadOnlyGateway(
        _config(tmp_path),
        ib_factory=lambda: fake,
        market_data_request_lease_factory=lambda request_class: Lease(
            request_class
        ),
        now=lambda: NOW,
    )
    gateway.connect()
    contracts = gateway.qualify_option_contracts(
        "SPY",
        date(2026, 8, 21),
        [Decimal("625")],
        rights=("C", "P"),
    )

    batch = gateway.option_quote_batch(contracts)

    assert batch.status is QuoteBatchStatus.COMPLETE
    assert batch.source == "IBKR_REQ_MKT_DATA_READONLY"
    assert fake.market_data_type_requests == [1]
    assert lease_calls.count("streaming_quote") == 2
    assert lease_calls.count("snapshot_quote") == 0
    assert fake.streaming_requests == fake.streaming_cancellations
    assert len(fake.streaming_requests) == 2
    assert fake.tick_by_tick_attempts == 0
    assert [
        diagnostic.broker_request_id for diagnostic in batch.request_diagnostics
    ] == [7000 + contract.contract_id for contract in contracts]
    assert all(
        diagnostic.received_fields
        == (
            "bid",
            "ask",
            "exchange_time",
            "market_data_type",
            "implied_volatility",
            "delta",
            "gamma",
            "theta",
            "vega",
            "volume",
            "open_interest",
        )
        for diagnostic in batch.request_diagnostics
    )
    assert all(not diagnostic.deadline_expired for diagnostic in batch.request_diagnostics)
    assert all(diagnostic.timeout_reason is None for diagnostic in batch.request_diagnostics)
    assert all(value == 0 for value in active.values())
    gateway.disconnect()


def test_streaming_option_batch_allows_bounded_time_for_staggered_open_interest(
    tmp_path: Path,
) -> None:
    elapsed = 0.0

    class StaggeredOpenInterestIB(FakeIB):
        def __init__(self) -> None:
            super().__init__()
            self.tickers: list[object] = []
            self.wrapper.reqId2Ticker = {}

        def reqMktData(
            self,
            contract,
            generic_tick_list,
            snapshot,
            regulatory_snapshot,
        ):
            assert generic_tick_list == "100,101,106"
            assert snapshot is regulatory_snapshot is False
            ticker = super().reqTickers(contract)[0]
            ticker.callOpenInterest = None
            ticker.putOpenInterest = None
            self.tickers.append(ticker)
            self.wrapper.reqId2Ticker[7000 + contract.conId] = ticker
            return ticker

        def cancelMktData(self, _contract) -> None:
            return None

        def sleep(self, seconds: float) -> None:
            nonlocal elapsed
            elapsed += seconds
            if elapsed >= 6.0:
                for ticker in self.tickers:
                    ticker.callOpenInterest = 500
                    ticker.putOpenInterest = 600

    fake = StaggeredOpenInterestIB()
    gateway = IBKRReadOnlyGateway(
        _config(tmp_path),
        ib_factory=lambda: fake,
        market_data_request_lease_factory=lambda request_class: nullcontext(
            SimpleNamespace(allowed=True, reason=None, request_class=request_class)
        ),
        historical_request_lease_factory=_historical_lease,
        now=lambda: NOW,
        monotonic=lambda: elapsed,
    )
    gateway.connect()
    contracts = gateway.qualify_option_contracts(
        "SPY",
        date(2026, 8, 21),
        [Decimal("620"), Decimal("625"), Decimal("630")],
        rights=("C", "P"),
    )

    batch = gateway.option_quote_batch(contracts)

    assert elapsed >= 6.0
    assert batch.status is QuoteBatchStatus.COMPLETE
    assert all(quote.open_interest is not None for quote in batch.quotes)
    assert all(
        not diagnostic.deadline_expired
        for diagnostic in batch.request_diagnostics
    )
    gateway.disconnect()


def test_streaming_option_diagnostic_binds_api_error_and_timeout_to_reqid_and_conid(
    tmp_path: Path,
) -> None:
    class ErrorEvent:
        def __init__(self) -> None:
            self.handlers: list[object] = []

        def __iadd__(self, handler):
            self.handlers.append(handler)
            return self

        def __isub__(self, handler):
            self.handlers.remove(handler)
            return self

        def emit(self, *args: object) -> None:
            for handler in tuple(self.handlers):
                handler(*args)

    class EmptyStreamingIB(FakeIB):
        def __init__(self) -> None:
            super().__init__()
            self.errorEvent = ErrorEvent()
            self.wrapper.reqId2Ticker = {}

        def reqMktData(
            self,
            contract,
            _generic_tick_list,
            _snapshot,
            _regulatory_snapshot,
        ):
            self.exchange_time = None
            self.market_data_type = None
            self.quote_overrides.update(
                bid=None,
                ask=None,
                last=None,
                close=None,
                volume=None,
                callVolume=None,
                callOpenInterest=None,
            )
            self.greek_overrides.update(
                impliedVol=None,
                delta=None,
                gamma=None,
                theta=None,
                vega=None,
            )
            ticker = super().reqTickers(contract)[0]
            self.wrapper.reqId2Ticker[881] = ticker
            self.errorEvent.emit(
                881,
                10090,
                "Part of requested market data is not subscribed",
                contract,
            )
            return ticker

        def cancelMktData(self, _contract) -> None:
            return None

        def sleep(self, _seconds: float) -> None:
            return None

    monotonic_values = iter((0.0, 1.0, 2.0, 3.0, 4.0, 5.0))
    fake = EmptyStreamingIB()
    gateway = IBKRReadOnlyGateway(
        _config(tmp_path),
        ib_factory=lambda: fake,
        market_data_request_lease_factory=lambda request_class: nullcontext(
            SimpleNamespace(allowed=True, reason=None, request_class=request_class)
        ),
        now=lambda: NOW,
        monotonic=lambda: next(monotonic_values),
    )
    gateway.connect()
    contract = gateway.qualify_option_contracts(
        "SPY",
        date(2026, 8, 21),
        [Decimal("625")],
        rights=("C",),
    )[0]

    batch = gateway.option_quote_batch((contract,))

    assert batch.status is QuoteBatchStatus.PARTIAL
    assert (
        f"IBKR_OPTION_MARKET_DATA_NOT_SUBSCRIBED:10090:{contract.contract_id}"
        in batch.blockers
    )
    assert len(batch.request_diagnostics) == 1
    diagnostic = batch.request_diagnostics[0]
    assert diagnostic.broker_request_id == 881
    assert diagnostic.contract_id == contract.contract_id
    assert diagnostic.generic_ticks == (100, 101, 106)
    assert diagnostic.received_fields == ()
    assert diagnostic.missing_fields == (
        "bid",
        "ask",
        "exchange_time",
        "market_data_type",
        "implied_volatility",
        "delta",
        "gamma",
        "theta",
        "vega",
        "volume",
        "open_interest",
    )
    assert diagnostic.error_codes == (10090,)
    assert diagnostic.deadline_expired is True
    assert diagnostic.timeout_reason == (
        "STREAMING_REQUIRED_FIELDS_DEADLINE_EXPIRED_AFTER_API_ERROR"
    )
    assert len(fake.errorEvent.handlers) == 1  # Owned upstream guard remains.
    gateway.disconnect()


def test_option_volume_falls_back_from_unset_nan_to_contract_volume(
    tmp_path: Path,
) -> None:
    fake = FakeIB()
    fake.quote_overrides.update(callVolume=float("nan"), volume=321)
    gateway = IBKRReadOnlyGateway(
        _config(tmp_path),
        ib_factory=lambda: fake,
        now=lambda: NOW,
    )
    gateway.connect()
    contract = gateway.qualify_option_contracts(
        "SPY",
        date(2026, 8, 21),
        [Decimal("625")],
        rights=("C",),
    )[0]

    quote = gateway.option_quote_batch((contract,)).quotes[0]

    assert quote.volume == 321
    gateway.disconnect()


def test_after_hours_indicative_batch_requests_frozen_and_restores_live(
    tmp_path: Path,
) -> None:
    class IndicativeIB(FakeIB):
        def __init__(self) -> None:
            super().__init__()
            self.market_data_type = 2
            self.streaming_requests = []
            self.streaming_cancellations = []

        def reqMarketDataType(self, market_data_type):
            super().reqMarketDataType(market_data_type)
            self.market_data_type = market_data_type

        def reqMktData(
            self,
            contract,
            generic_tick_list,
            snapshot,
            regulatory_snapshot,
        ):
            assert generic_tick_list == ""
            assert snapshot is regulatory_snapshot is False
            self.streaming_requests.append(contract)
            ticker = super().reqTickers(contract)[0]
            ticker.marketDataType = 2
            return ticker

        def cancelMktData(self, contract) -> None:
            self.streaming_cancellations.append(contract)

        def sleep(self, _seconds: float) -> None:
            raise AssertionError("indicative fixture is immediately ready")

    fake = IndicativeIB()
    gateway = IBKRReadOnlyGateway(
        _config(tmp_path),
        ib_factory=lambda: fake,
        now=lambda: NOW,
    )
    gateway.connect()
    contract = gateway.qualify_option_contracts(
        "SPY",
        date(2026, 8, 21),
        [Decimal("625")],
        rights=("C",),
    )[0]

    batch = gateway.option_indicative_quote_batch((contract,))

    assert batch.status is QuoteBatchStatus.COMPLETE
    assert batch.source.endswith("REQUESTED_TYPE_2")
    assert fake.market_data_type_requests == [2, 1]
    assert fake.streaming_requests == fake.streaming_cancellations
    assert batch.quotes[0].market_data_type == 2
    gateway.disconnect()


def test_after_hours_indicative_falls_back_to_completed_option_daily_close(
    tmp_path: Path,
) -> None:
    class HistoricalCloseIB(FakeIB):
        def __init__(self) -> None:
            super().__init__()
            self.market_data_type = 4
            self.streaming_requests = []
            self.streaming_cancellations = []
            self.historical_async_calls: list[dict[str, object]] = []

        def reqMarketDataType(self, market_data_type):
            super().reqMarketDataType(market_data_type)
            self.market_data_type = market_data_type

        def reqMktData(
            self,
            contract,
            generic_tick_list,
            snapshot,
            regulatory_snapshot,
        ):
            assert generic_tick_list == ""
            assert snapshot is regulatory_snapshot is False
            self.streaming_requests.append(contract)
            ticker = super().reqTickers(contract)[0]
            ticker.bid = float("nan")
            ticker.ask = float("nan")
            ticker.last = float("nan")
            ticker.close = float("nan")
            ticker.marketDataType = 4
            return ticker

        def cancelMktData(self, contract) -> None:
            self.streaming_cancellations.append(contract)

        def sleep(self, _seconds: float) -> None:
            return None

        async def reqHistoricalTicksAsync(self, *_args, **_kwargs):
            return ()

        async def reqHistoricalDataAsync(self, contract, **kwargs):
            self.historical_async_calls.append(
                {"contract_id": contract.conId, **kwargs}
            )
            return (
                SimpleNamespace(date="20260731", close=1.25),
                # Today's unfinished daily bar must never be selected.
                SimpleNamespace(date="20260803", close=9.99),
            )

    ticks = iter(index / 4 for index in range(1000))
    active = 0
    peak_active = 0
    lease_calls = 0

    class HistoricalLease:
        def __enter__(self):
            nonlocal active, peak_active, lease_calls
            active += 1
            lease_calls += 1
            peak_active = max(peak_active, active)
            return SimpleNamespace(allowed=True, reason=None)

        def __exit__(self, *_args):
            nonlocal active
            active -= 1

    fake = HistoricalCloseIB()
    gateway = IBKRReadOnlyGateway(
        _config(tmp_path),
        ib_factory=lambda: fake,
        historical_request_lease_factory=HistoricalLease,
        market_data_request_lease_factory=lambda request_class: nullcontext(
            SimpleNamespace(allowed=True, reason=None, request_class=request_class)
        ),
        now=lambda: NOW,
        monotonic=lambda: next(ticks),
    )
    gateway.connect()
    contracts = gateway.qualify_option_contracts(
        "SPY",
        date(2026, 8, 21),
        [Decimal("625")],
        rights=("C", "P"),
    )

    batch = gateway.option_indicative_quote_batch(contracts)

    assert batch.status is QuoteBatchStatus.COMPLETE
    assert batch.source.endswith("+HISTORICAL_OPTION_MARK_READONLY")
    assert len(fake.historical_async_calls) == 2
    assert all(call["durationStr"] == "2 D" for call in fake.historical_async_calls)
    assert all(call["barSizeSetting"] == "1 day" for call in fake.historical_async_calls)
    assert all(call["whatToShow"] == "TRADES" for call in fake.historical_async_calls)
    assert all(call["useRTH"] is True for call in fake.historical_async_calls)
    # Each leg independently consumes one trade request and, when that is
    # empty, one daily-close fallback request.
    assert lease_calls == 4
    assert peak_active == 2
    assert active == 0
    assert all(quote.close == Decimal("1.25") for quote in batch.quotes)
    assert all(
        quote.exchange_time == datetime(2026, 7, 31, 20, 0, tzinfo=timezone.utc)
        for quote in batch.quotes
    )
    assert fake.streaming_requests == fake.streaming_cancellations
    gateway.disconnect()


def test_after_hours_historical_close_requests_only_legs_without_any_mark(
    tmp_path: Path,
) -> None:
    class PartialMarksIB(FakeIB):
        def __init__(self) -> None:
            super().__init__()
            self.market_data_type = 4
            self.historical_ids: list[int] = []

        def reqMktData(self, contract, *_args):
            ticker = super().reqTickers(contract)[0]
            ticker.marketDataType = 4
            if contract.right == "P":
                ticker.bid = ticker.ask = ticker.last = ticker.close = float("nan")
            return ticker

        def cancelMktData(self, _contract) -> None:
            return None

        def sleep(self, _seconds: float) -> None:
            return None

        async def reqHistoricalTicksAsync(self, *_args, **_kwargs):
            return ()

        async def reqHistoricalDataAsync(self, contract, **_kwargs):
            self.historical_ids.append(contract.conId)
            return (SimpleNamespace(date="20260731", close=0.75),)

    ticks = iter(index / 4 for index in range(1000))
    fake = PartialMarksIB()
    gateway = IBKRReadOnlyGateway(
        _config(tmp_path),
        ib_factory=lambda: fake,
        historical_request_lease_factory=_historical_lease,
        market_data_request_lease_factory=lambda request_class: nullcontext(
            SimpleNamespace(allowed=True, reason=None, request_class=request_class)
        ),
        now=lambda: NOW,
        monotonic=lambda: next(ticks),
    )
    gateway.connect()
    contracts = gateway.qualify_option_contracts(
        "SPY",
        date(2026, 8, 21),
        [Decimal("625")],
        rights=("C", "P"),
    )

    batch = gateway.option_indicative_quote_batch(contracts)

    assert fake.historical_ids == [contracts[1].contract_id]
    assert batch.quotes[0].bid == Decimal("1.0")
    assert batch.quotes[1].close == Decimal("0.75")
    gateway.disconnect()


def test_after_hours_indicative_prefers_previous_session_last_trade(
    tmp_path: Path,
) -> None:
    class HistoricalTradeIB(FakeIB):
        def __init__(self) -> None:
            super().__init__()
            self.market_data_type = 4
            self.trade_ids: list[int] = []
            self.daily_calls = 0

        def reqMktData(self, contract, *_args):
            ticker = super().reqTickers(contract)[0]
            ticker.bid = ticker.ask = ticker.last = ticker.close = float("nan")
            ticker.marketDataType = 4
            return ticker

        def cancelMktData(self, _contract) -> None:
            return None

        def sleep(self, _seconds: float) -> None:
            return None

        async def reqHistoricalTicksAsync(self, contract, *_args):
            self.trade_ids.append(contract.conId)
            return (
                SimpleNamespace(
                    time=datetime(2026, 7, 31, 19, 59, tzinfo=timezone.utc),
                    price=1.35,
                ),
            )

        async def reqHistoricalDataAsync(self, *_args, **_kwargs):
            self.daily_calls += 1
            return ()

    ticks = iter(index / 4 for index in range(1000))
    fake = HistoricalTradeIB()
    gateway = IBKRReadOnlyGateway(
        _config(tmp_path),
        ib_factory=lambda: fake,
        historical_request_lease_factory=_historical_lease,
        market_data_request_lease_factory=lambda request_class: nullcontext(
            SimpleNamespace(allowed=True, reason=None, request_class=request_class)
        ),
        now=lambda: NOW,
        monotonic=lambda: next(ticks),
    )
    gateway.connect()
    contracts = gateway.qualify_option_contracts(
        "SPY",
        date(2026, 8, 21),
        [Decimal("625")],
        rights=("C", "P"),
    )

    batch = gateway.option_indicative_quote_batch(contracts)

    assert batch.status is QuoteBatchStatus.COMPLETE
    assert fake.trade_ids == [item.contract_id for item in contracts]
    assert fake.daily_calls == 0
    assert all(quote.last == Decimal("1.35") for quote in batch.quotes)
    assert all(
        quote.research_price_basis == "PREVIOUS_SESSION_LAST_TRADE"
        for quote in batch.quotes
    )
    gateway.disconnect()


def test_streaming_option_batch_resolves_leg_times_concurrently(
    tmp_path: Path,
) -> None:
    class StreamingIB(FakeIB):
        def __init__(self) -> None:
            super().__init__()
            self.exchange_time = None
            self.historical_async_calls: list[int] = []

        def reqMktData(
            self,
            contract,
            _generic_tick_list,
            _snapshot,
            _regulatory_snapshot,
        ):
            return super().reqTickers(contract)[0]

        def cancelMktData(self, _contract) -> None:
            return None

        def sleep(self, _seconds: float) -> None:
            raise AssertionError("complete fixture must not wait for more ticks")

        async def reqHistoricalTicksAsync(
            self,
            contract,
            _start_date_time,
            _end_date_time,
            _number_of_ticks,
            _what_to_show,
            _use_rth,
            _ignore_size,
            _misc_options,
        ):
            self.historical_async_calls.append(contract.conId)
            return (
                SimpleNamespace(
                    time=NOW - timedelta(seconds=1),
                    priceBid=1.01,
                    priceAsk=1.19,
                ),
            )

    fake = StreamingIB()
    gateway = IBKRReadOnlyGateway(
        _config(tmp_path),
        ib_factory=lambda: fake,
        historical_request_lease_factory=_historical_lease,
        market_data_request_lease_factory=lambda request_class: nullcontext(
            SimpleNamespace(allowed=True, reason=None, request_class=request_class)
        ),
        now=lambda: NOW,
    )
    gateway.connect()
    contracts = gateway.qualify_option_contracts(
        "SPY",
        date(2026, 8, 21),
        [Decimal("625")],
        rights=("C", "P"),
    )

    batch = gateway.option_quote_batch(contracts)

    assert batch.status is QuoteBatchStatus.COMPLETE
    assert batch.source == "IBKR_REQ_MKT_DATA_PLUS_HISTORICAL_TICKS_READONLY"
    assert set(fake.historical_async_calls) == {
        contract.contract_id for contract in contracts
    }
    assert all(
        quote.exchange_time == NOW - timedelta(seconds=1)
        for quote in batch.quotes
    )
    assert all(
        quote.bid == Decimal("1.01") and quote.ask == Decimal("1.19")
        for quote in batch.quotes
    )
    gateway.disconnect()


def test_streaming_option_batch_accepts_locked_executable_historical_bbo(
    tmp_path: Path,
) -> None:
    class LockedStreamingIB(FakeIB):
        def __init__(self) -> None:
            super().__init__()
            self.exchange_time = None
            self.quote_overrides = {"bid": 1.1, "ask": 1.1}

        def reqMktData(
            self,
            contract,
            _generic_tick_list,
            _snapshot,
            _regulatory_snapshot,
        ):
            return super().reqTickers(contract)[0]

        def cancelMktData(self, _contract) -> None:
            return None

        def sleep(self, _seconds: float) -> None:
            raise AssertionError("locked executable BBO is already complete")

        async def reqHistoricalTicksAsync(
            self,
            _contract,
            _start_date_time,
            _end_date_time,
            _number_of_ticks,
            _what_to_show,
            _use_rth,
            _ignore_size,
            _misc_options,
        ):
            return (
                SimpleNamespace(
                    time=NOW - timedelta(seconds=1),
                    priceBid=1.1,
                    priceAsk=1.1,
                ),
            )

    fake = LockedStreamingIB()
    gateway = IBKRReadOnlyGateway(
        _config(tmp_path),
        ib_factory=lambda: fake,
        historical_request_lease_factory=_historical_lease,
        market_data_request_lease_factory=lambda request_class: nullcontext(
            SimpleNamespace(allowed=True, reason=None, request_class=request_class)
        ),
        now=lambda: NOW,
    )
    gateway.connect()
    contracts = gateway.qualify_option_contracts(
        "SPY",
        date(2026, 8, 21),
        [Decimal("625")],
        rights=("C", "P"),
    )

    batch = gateway.option_quote_batch(contracts)

    assert batch.status is QuoteBatchStatus.COMPLETE
    assert all(quote.bid == quote.ask == Decimal("1.1") for quote in batch.quotes)
    assert all(
        quote.exchange_time == NOW - timedelta(seconds=1)
        for quote in batch.quotes
    )
    assert all(not diagnostic.deadline_expired for diagnostic in batch.request_diagnostics)
    gateway.disconnect()


def test_streaming_option_batch_skips_tick_by_tick_and_uses_historical_bbo(
    tmp_path: Path,
) -> None:
    class TickByTickStreamingIB(FakeIB):
        def __init__(self) -> None:
            super().__init__()
            self.exchange_time = None
            self.streaming_tickers: dict[int, object] = {}
            self.tick_by_tick_requests: list[int] = []
            self.tick_by_tick_cancellations: list[int] = []
            self.historical_async_calls: list[int] = []
            self.wrapper.reqId2Ticker = {}

        def reqMktData(
            self,
            contract,
            _generic_tick_list,
            _snapshot,
            _regulatory_snapshot,
        ):
            ticker = super().reqTickers(contract)[0]
            self.streaming_tickers[contract.conId] = ticker
            self.wrapper.reqId2Ticker[7000 + contract.conId] = ticker
            return ticker

        def cancelMktData(self, _contract) -> None:
            return None

        def reqTickByTickData(
            self,
            contract,
            _tick_type,
            _number_of_ticks,
            _ignore_size,
        ):
            self.tick_by_tick_requests.append(contract.conId)
            ticker = self.streaming_tickers[contract.conId]
            ticker.tickByTicks = [
                SimpleNamespace(
                    time=NOW - timedelta(seconds=1),
                    bidPrice=1.01,
                    askPrice=1.19,
                )
            ]
            self.wrapper.reqId2Ticker[8000 + contract.conId] = ticker
            return ticker

        def cancelTickByTickData(self, contract, _tick_type) -> None:
            self.tick_by_tick_cancellations.append(contract.conId)

        def sleep(self, _seconds: float) -> None:
            raise AssertionError("tick-by-tick BBO and option fields are complete")

        async def reqHistoricalTicksAsync(self, contract, *_args, **_kwargs):
            self.historical_async_calls.append(contract.conId)
            return (
                SimpleNamespace(
                    time=NOW - timedelta(seconds=1),
                    priceBid=1.01,
                    priceAsk=1.19,
                ),
            )

    fake = TickByTickStreamingIB()
    gateway = IBKRReadOnlyGateway(
        _config(tmp_path),
        ib_factory=lambda: fake,
        historical_request_lease_factory=_historical_lease,
        market_data_request_lease_factory=lambda request_class: nullcontext(
            SimpleNamespace(allowed=True, reason=None, request_class=request_class)
        ),
        now=lambda: NOW,
    )
    gateway.connect()
    contracts = gateway.qualify_option_contracts(
        "SPY",
        date(2026, 8, 21),
        [Decimal("625")],
        rights=("C", "P"),
    )

    batch = gateway.option_quote_batch(contracts)

    assert batch.status is QuoteBatchStatus.COMPLETE
    assert batch.source == "IBKR_REQ_MKT_DATA_PLUS_HISTORICAL_TICKS_READONLY"
    assert fake.tick_by_tick_requests == []
    assert fake.tick_by_tick_cancellations == []
    assert set(fake.historical_async_calls) == {
        contract.contract_id for contract in contracts
    }
    assert all(quote.bid == Decimal("1.01") for quote in batch.quotes)
    assert all(quote.ask == Decimal("1.19") for quote in batch.quotes)
    assert all(
        quote.exchange_time == NOW - timedelta(seconds=1)
        for quote in batch.quotes
    )
    assert all(
        "exchange_time" in diagnostic.received_fields
        for diagnostic in batch.request_diagnostics
    )
    assert [
        diagnostic.broker_request_id for diagnostic in batch.request_diagnostics
    ] == [7000 + contract.contract_id for contract in contracts]
    assert all(
        diagnostic.broker_timed_bbo_request_id is None
        for diagnostic in batch.request_diagnostics
    )
    gateway.disconnect()


def test_tick_by_tick_unavailable_falls_back_to_paced_historical_bbo(
    tmp_path: Path,
) -> None:
    class HistoricalFallbackIB(FakeIB):
        def __init__(self) -> None:
            super().__init__()
            self.exchange_time = None
            self.historical_async_calls: list[int] = []

        def reqMktData(self, contract, *_args):
            return super().reqTickers(contract)[0]

        def cancelMktData(self, _contract) -> None:
            return None

        def reqTickByTickData(self, *_args):
            raise RuntimeError("tick-by-tick unavailable")

        def cancelTickByTickData(self, *_args) -> None:
            return None

        def sleep(self, _seconds: float) -> None:
            raise AssertionError("all primary option fields are complete")

        async def reqHistoricalTicksAsync(
            self,
            contract,
            *_args,
        ):
            self.historical_async_calls.append(contract.conId)
            return (
                SimpleNamespace(
                    time=NOW - timedelta(seconds=1),
                    priceBid=1.01,
                    priceAsk=1.19,
                ),
            )

    fake = HistoricalFallbackIB()
    gateway = IBKRReadOnlyGateway(
        _config(tmp_path),
        ib_factory=lambda: fake,
        historical_request_lease_factory=_historical_lease,
        market_data_request_lease_factory=lambda request_class: nullcontext(
            SimpleNamespace(allowed=True, reason=None, request_class=request_class)
        ),
        now=lambda: NOW,
    )
    gateway.connect()
    contract = gateway.qualify_option_contracts(
        "SPY",
        date(2026, 8, 21),
        [Decimal("625")],
        rights=("P",),
    )[0]

    batch = gateway.option_quote_batch((contract,))

    assert batch.status is QuoteBatchStatus.COMPLETE
    assert batch.source == "IBKR_REQ_MKT_DATA_PLUS_HISTORICAL_TICKS_READONLY"
    assert fake.historical_async_calls == [contract.contract_id]
    gateway.disconnect()


def test_option_quote_path_avoids_tick_by_tick_10189_entitlement_error(
    tmp_path: Path,
) -> None:
    class ErrorEvent:
        def __init__(self) -> None:
            self.handlers: list[object] = []

        def __iadd__(self, handler):
            self.handlers.append(handler)
            return self

        def __isub__(self, handler):
            self.handlers.remove(handler)
            return self

        def emit(self, *args: object) -> None:
            for handler in tuple(self.handlers):
                handler(*args)

    class UnsupportedTickByTickIB(FakeIB):
        def __init__(self) -> None:
            super().__init__()
            self.errorEvent = ErrorEvent()
            self.exchange_time = None
            self.wrapper.reqId2Ticker = {}
            self.historical_async_calls: list[int] = []
            self.tick_by_tick_attempts = 0

        def reqMktData(self, contract, *_args):
            ticker = super().reqTickers(contract)[0]
            self.wrapper.reqId2Ticker[7000 + contract.conId] = ticker
            return ticker

        def cancelMktData(self, _contract) -> None:
            return None

        def reqTickByTickData(self, contract, *_args):
            self.tick_by_tick_attempts += 1
            ticker = SimpleNamespace(contract=contract, tickByTicks=[])
            request_id = 8000 + contract.conId
            self.wrapper.reqId2Ticker[request_id] = ticker
            self.errorEvent.emit(
                request_id,
                10189,
                "BidAsk tick-by-tick requests are not supported",
                contract,
            )
            return ticker

        def cancelTickByTickData(self, *_args) -> None:
            raise AssertionError("rejected tick-by-tick request must not be cancelled")

        def sleep(self, _seconds: float) -> None:
            raise AssertionError("10189 must immediately select historical fallback")

        async def reqHistoricalTicksAsync(self, contract, *_args):
            self.historical_async_calls.append(contract.conId)
            return (
                SimpleNamespace(
                    time=NOW - timedelta(seconds=1),
                    priceBid=1.01,
                    priceAsk=1.19,
                ),
            )

    fake = UnsupportedTickByTickIB()
    gateway = IBKRReadOnlyGateway(
        _config(tmp_path),
        ib_factory=lambda: fake,
        historical_request_lease_factory=_historical_lease,
        market_data_request_lease_factory=lambda request_class: nullcontext(
            SimpleNamespace(allowed=True, reason=None, request_class=request_class)
        ),
        now=lambda: NOW,
    )
    gateway.connect()
    contract = gateway.qualify_option_contracts(
        "SPY",
        date(2026, 8, 21),
        [Decimal("625")],
        rights=("P",),
    )[0]

    batch = gateway.option_quote_batch((contract,))

    assert batch.status is QuoteBatchStatus.COMPLETE
    assert batch.source == "IBKR_REQ_MKT_DATA_PLUS_HISTORICAL_TICKS_READONLY"
    assert fake.historical_async_calls == [contract.contract_id]
    assert fake.tick_by_tick_attempts == 0
    assert batch.request_diagnostics[0].error_codes == ()
    gateway.disconnect()


def test_streaming_option_batch_chunks_historical_reads_to_approved_concurrency(
    tmp_path: Path,
) -> None:
    class StreamingIB(FakeIB):
        def __init__(self) -> None:
            super().__init__()
            self.exchange_time = None
            self.historical_async_calls: list[int] = []

        async def reqHistoricalTicksAsync(
            self,
            contract,
            _start_date_time,
            _end_date_time,
            _number_of_ticks,
            _what_to_show,
            _use_rth,
            _ignore_size,
            _misc_options,
        ):
            self.historical_async_calls.append(contract.conId)
            return (
                SimpleNamespace(
                    time=NOW - timedelta(seconds=1),
                    priceBid=1.01,
                    priceAsk=1.19,
                ),
            )

    active = 0
    peak_active = 0

    class HistoricalLease:
        def __init__(self) -> None:
            self.reserved = False

        def __enter__(self):
            nonlocal active, peak_active
            if active >= 2:
                return SimpleNamespace(
                    allowed=False,
                    reason="PACING_CONCURRENCY_LIMIT",
                )
            active += 1
            peak_active = max(peak_active, active)
            self.reserved = True
            return SimpleNamespace(allowed=True, reason=None)

        def __exit__(self, *_args):
            nonlocal active
            if self.reserved:
                active -= 1

    fake = StreamingIB()
    gateway = IBKRReadOnlyGateway(
        _config(tmp_path),
        ib_factory=lambda: fake,
        historical_request_lease_factory=HistoricalLease,
        now=lambda: NOW,
    )
    gateway.connect()
    contracts = gateway.qualify_option_contracts(
        "SPY",
        date(2026, 8, 21),
        [Decimal("625"), Decimal("630")],
        rights=("C", "P"),
    )

    batch = gateway.option_quote_batch(contracts)

    assert batch.status is QuoteBatchStatus.COMPLETE
    assert set(fake.historical_async_calls) == {
        contract.contract_id for contract in contracts
    }
    assert peak_active == 2
    assert active == 0
    assert not any(
        "HISTORICAL_PACING" in blocker for blocker in batch.blockers
    )
    gateway.disconnect()


def test_option_quote_batch_expands_owner_deadline_for_four_historical_chunks(
    tmp_path: Path,
) -> None:
    class HistoricalIB(FakeIB):
        def __init__(self) -> None:
            super().__init__()
            self.exchange_time = None
            self.historical_async_calls: list[int] = []

        async def reqHistoricalTicksAsync(self, contract, *_args):
            self.historical_async_calls.append(contract.conId)
            return (
                SimpleNamespace(
                    time=NOW - timedelta(seconds=1),
                    priceBid=1.0,
                    priceAsk=1.2,
                ),
            )

    active = 0

    class HistoricalLease:
        def __init__(self) -> None:
            self.reserved = False

        def __enter__(self):
            nonlocal active
            if active >= 2:
                return SimpleNamespace(
                    allowed=False,
                    reason="PACING_CONCURRENCY_LIMIT",
                )
            active += 1
            self.reserved = True
            return SimpleNamespace(allowed=True, reason=None)

        def __exit__(self, *_args):
            nonlocal active
            if self.reserved:
                active -= 1

    monotonic_value = -1.0

    def advancing_monotonic() -> float:
        nonlocal monotonic_value
        monotonic_value += 1.0
        return monotonic_value

    fake = HistoricalIB()
    gateway = IBKRReadOnlyGateway(
        _config(tmp_path),
        ib_factory=lambda: fake,
        historical_request_lease_factory=HistoricalLease,
        historical_request_max_concurrency=2,
        now=lambda: NOW,
        monotonic=advancing_monotonic,
    )
    gateway.connect()
    contracts = gateway.qualify_option_contracts(
        "SPY",
        date(2026, 8, 21),
        [Decimal("620"), Decimal("625"), Decimal("630"), Decimal("635")],
        rights=("C", "P"),
    )

    batch = gateway.option_quote_batch(contracts)

    assert batch.status is QuoteBatchStatus.COMPLETE
    assert len(fake.historical_async_calls) == 8
    assert set(fake.historical_async_calls) == {
        contract.contract_id for contract in contracts
    }
    assert monotonic_value > gateway.config.ibkr_timeout_seconds - 1.0
    assert active == 0
    gateway.disconnect()


def test_gateway_stops_before_denied_per_contract_quote_wire_request(
    tmp_path: Path,
) -> None:
    quote_leases = 0
    wire_calls = 0

    class Lease:
        def __init__(self, request_class: str) -> None:
            self.request_class = request_class

        def __enter__(self) -> object:
            nonlocal quote_leases
            if self.request_class != "snapshot_quote":
                return SimpleNamespace(allowed=True, reason=None)
            quote_leases += 1
            return SimpleNamespace(
                allowed=quote_leases == 1,
                reason=(
                    None
                    if quote_leases == 1
                    else "PACING_REQUEST_WINDOW_EXHAUSTED"
                ),
            )

        def __exit__(self, *_: object) -> None:
            return None

    class CountingIB(FakeIB):
        def __init__(self) -> None:
            super().__init__()
            self.next_contract_id = 9500

        def qualifyContracts(self, *contracts):
            for contract in contracts:
                self.next_contract_id += 1
                contract.conId = self.next_contract_id
                contract.localSymbol = (
                    f"{contract.symbol}-{contract.right}-{contract.strike}"
                )
                contract.multiplier = "100"
            return list(contracts)

        def reqTickers(self, *contracts):
            nonlocal wire_calls
            wire_calls += len(contracts)
            return super().reqTickers(*contracts)

    fake = CountingIB()
    gateway = IBKRReadOnlyGateway(
        _config(tmp_path),
        ib_factory=lambda: fake,
        market_data_request_lease_factory=lambda request_class: Lease(
            request_class
        ),
        now=lambda: NOW,
    )
    gateway.connect()
    contracts = gateway.qualify_option_contracts(
        "SPY",
        date(2026, 8, 21),
        [Decimal("625")],
        rights=("C", "P"),
    )

    with pytest.raises(MarketDataPacingError) as raised:
        gateway.option_quote_batch(contracts)

    assert raised.value.request_class == "snapshot_quote"
    assert raised.value.reason_code == "PACING_REQUEST_WINDOW_EXHAUSTED"
    assert quote_leases == 2
    assert wire_calls == 0
    gateway.disconnect()


def test_working_orders_and_known_empty_vs_unknown_instructions(tmp_path: Path) -> None:
    fake = FakeIB()
    contract = SimpleNamespace(conId=101, localSymbol="SPY CALL")
    order = SimpleNamespace(
        orderId=7,
        permId=70,
        clientId=17,
        action="buy",
        orderType="LMT",
        totalQuantity=1,
        lmtPrice=1.25,
        auxPrice=None,
        tif="DAY",
        transmit=False,
    )
    fake.open_trades = [
        SimpleNamespace(
            contract=contract,
            order=order,
            orderStatus=SimpleNamespace(status="Submitted"),
        ),
        SimpleNamespace(
            contract=contract,
            order=order,
            orderStatus=SimpleNamespace(status="Filled"),
        ),
    ]
    gateway = IBKRReadOnlyGateway(
        _config(tmp_path),
        ib_factory=lambda: fake,
        instruction_reader=lambda: [],
        now=lambda: NOW,
    )
    gateway.connect()

    orders = gateway.working_orders()

    assert len(orders) == 1
    assert orders[0]["order_id"] == 7
    assert orders[0]["total_quantity"] == Decimal("1")
    assert orders[0]["limit_price"] == Decimal("1.25")
    assert gateway.unsubmitted_instructions() == ()
    unknown = IBKRReadOnlyGateway(
        _config(tmp_path),
        ib_factory=FakeIB,
        now=lambda: NOW,
    )
    assert unknown.unsubmitted_instructions() is None


def test_authoritative_secdefs_expose_exact_identity_and_standard_status(
    tmp_path: Path,
) -> None:
    fake = FakeIB()
    gateway = IBKRReadOnlyGateway(
        _config(tmp_path), ib_factory=lambda: fake, now=lambda: NOW
    )
    gateway.connect()
    contracts = gateway.qualify_option_contracts(
        "SPY", date(2026, 8, 21), [625], rights=("C", "P")
    )

    definitions = gateway.option_contract_definitions(contracts)

    assert len(definitions) == 2
    assert set(definitions[0].identity_dict()) == {
        "conId",
        "localSymbol",
        "tradingClass",
        "multiplier",
        "exchange",
        "expiry",
        "strike",
        "right",
    }
    assert all(item.standard_contract for item in definitions)
    assert all(not item.adjusted for item in definitions)
    assert all(
        item.source == "IBKR_REQ_CONTRACT_DETAILS_READONLY" for item in definitions
    )


def test_missing_secdef_fails_closed_for_the_entire_batch(tmp_path: Path) -> None:
    fake = FakeIB()
    gateway = IBKRReadOnlyGateway(
        _config(tmp_path), ib_factory=lambda: fake, now=lambda: NOW
    )
    gateway.connect()
    contracts = gateway.qualify_option_contracts(
        "SPY", date(2026, 8, 21), [625], rights=("C", "P")
    )
    fake.reqContractDetails = lambda _contract: []  # type: ignore[method-assign]

    with pytest.raises(BrokerConnectionError, match="secdef batch"):
        gateway.option_contract_definitions(contracts)


@pytest.mark.parametrize(
    ("attribute", "value"),
    [("contract_adjusted", True), ("contract_multiplier", "10")],
)
def test_adjusted_or_nonstandard_multiplier_secdef_is_marked_nonstandard(
    tmp_path: Path,
    attribute: str,
    value,
) -> None:
    fake = FakeIB()
    setattr(fake, attribute, value)
    gateway = IBKRReadOnlyGateway(
        _config(tmp_path), ib_factory=lambda: fake, now=lambda: NOW
    )
    gateway.connect()
    contracts = gateway.qualify_option_contracts(
        "SPY", date(2026, 8, 21), [625], rights=("C",)
    )

    definition = gateway.option_contract_definitions(contracts)[0]

    assert definition.standard_contract is False


def test_quote_batch_binds_one_request_and_complete_metadata_per_leg(
    tmp_path: Path,
) -> None:
    fake = FakeIB()
    gateway = IBKRReadOnlyGateway(
        _config(tmp_path),
        ib_factory=lambda: fake,
        batch_id_factory=lambda: "batch-fixed",
        now=lambda: NOW,
    )
    gateway.connect()
    contracts = gateway.qualify_option_contracts(
        "SPY", date(2026, 8, 21), [625], rights=("C", "P")
    )

    batch = gateway.option_quote_batch(contracts)

    assert fake.req_tickers_calls == 1
    assert batch.batch_id == "batch-fixed"
    assert batch.status is QuoteBatchStatus.COMPLETE
    assert batch.requested_at == NOW
    assert batch.observed_at == NOW
    assert batch.completed_at == NOW
    assert len({item.request_id for item in batch.quotes}) == 2
    assert all(item.batch_id == batch.batch_id for item in batch.quotes)
    assert all(item.requested_at == batch.requested_at for item in batch.quotes)
    assert all(item.observed_at == batch.observed_at for item in batch.quotes)
    assert all(item.completed_at == batch.completed_at for item in batch.quotes)
    assert all(item.source == batch.source for item in batch.quotes)


def test_option_greeks_preserve_finite_signed_values_and_nonnegative_metrics(
    tmp_path: Path,
) -> None:
    fake = FakeIB()
    gateway = IBKRReadOnlyGateway(
        _config(tmp_path), ib_factory=lambda: fake, now=lambda: NOW
    )
    gateway.connect()
    contracts = gateway.qualify_option_contracts(
        "SPY", date(2026, 8, 21), [625], rights=("C", "P")
    )

    call, put = gateway.option_quote_batch(contracts).quotes

    assert call.delta == Decimal("0.4")
    assert put.delta == Decimal("-0.4")
    assert call.theta == put.theta == Decimal("-0.05")
    assert call.gamma == put.gamma == Decimal("0.03")
    assert call.vega == put.vega == Decimal("0.12")
    assert call.implied_volatility == put.implied_volatility == Decimal("0.22")


def test_missing_quote_greeks_oi_volume_and_provenance_make_batch_partial(
    tmp_path: Path,
) -> None:
    fake = FakeIB()
    fake.quote_overrides.update(
        bid=None,
        volume=None,
        callVolume=None,
        callOpenInterest=None,
    )
    fake.greek_overrides["gamma"] = None
    fake.exchange_time = None
    fake.market_data_type = None
    gateway = IBKRReadOnlyGateway(
        _config(tmp_path), ib_factory=lambda: fake, now=lambda: NOW
    )
    gateway.connect()
    contract = gateway.qualify_option_contracts(
        "SPY", date(2026, 8, 21), [625], rights=("C",)
    )[0]

    batch = gateway.option_quote_batch((contract,))

    assert batch.status is QuoteBatchStatus.PARTIAL
    assert set(batch.blockers) == {
        f"QUOTE_BID_UNAVAILABLE:{contract.contract_id}",
        f"QUOTE_EXCHANGE_TIMESTAMP_UNAVAILABLE:{contract.contract_id}",
        f"QUOTE_LIVE_MARKET_DATA_TYPE_UNAVAILABLE:{contract.contract_id}",
        f"QUOTE_GAMMA_UNAVAILABLE:{contract.contract_id}",
        f"QUOTE_VOLUME_UNAVAILABLE:{contract.contract_id}",
        f"QUOTE_OPEN_INTEREST_UNAVAILABLE:{contract.contract_id}",
    }
    projected = gateway.option_quotes((contract,))[0]
    assert projected.bid is None
    assert projected.ask is None
    assert projected.implied_volatility is None


def test_empty_live_option_batch_has_one_actionable_aggregate_blocker(
    tmp_path: Path,
) -> None:
    fake = FakeIB()
    fake.exchange_time = None
    fake.quote_overrides.update(
        bid=None,
        ask=None,
        last=None,
        close=None,
        volume=None,
        callVolume=None,
        callOpenInterest=None,
    )
    fake.greek_overrides.update(
        impliedVol=None,
        delta=None,
        gamma=None,
        theta=None,
        vega=None,
    )
    gateway = IBKRReadOnlyGateway(
        _config(tmp_path), ib_factory=lambda: fake, now=lambda: NOW
    )
    gateway.connect()
    contract = gateway.qualify_option_contracts(
        "SPY", date(2026, 8, 21), [625], rights=("C",)
    )[0]

    batch = gateway.option_quote_batch((contract,))

    assert batch.status is QuoteBatchStatus.PARTIAL
    assert "IBKR_OPTION_EXECUTABLE_TICKS_UNAVAILABLE" in batch.blockers
    assert fake.market_data_type_requests == [1]


def test_missing_wrapper_exchange_time_uses_matching_fresh_historical_bbo(
    tmp_path: Path,
) -> None:
    fake = FakeIB()
    fake.exchange_time = None
    gateway = IBKRReadOnlyGateway(
        _config(tmp_path),
        ib_factory=lambda: fake,
        historical_request_lease_factory=_historical_lease,
        now=lambda: NOW,
    )
    gateway.connect()
    contract = gateway.qualify_option_contracts(
        "SPY", date(2026, 8, 21), [625], rights=("C",)
    )[0]
    fake.historical_ticks_by_con_id[contract.contract_id] = (
        SimpleNamespace(
            time=NOW - timedelta(seconds=1),
            priceBid=1.0,
            priceAsk=1.2,
        ),
    )

    batch = gateway.option_quote_batch((contract,))

    assert batch.status is QuoteBatchStatus.COMPLETE
    assert batch.source == "IBKR_REQ_TICKERS_PLUS_HISTORICAL_TICKS_READONLY"
    assert batch.quotes[0].exchange_time == NOW - timedelta(seconds=1)
    assert len(fake.req_historical_ticks_calls) == 1
    request = fake.req_historical_ticks_calls[0]
    assert request["what_to_show"] == "Bid_Ask"
    assert request["number_of_ticks"] == 1
    assert request["use_rth"] is False


def test_historical_pacing_lease_covers_the_actual_broker_request(
    tmp_path: Path,
) -> None:
    fake = FakeIB()
    fake.exchange_time = None
    state = {"leased": False}

    class Lease:
        def __enter__(self):
            state["leased"] = True
            return SimpleNamespace(allowed=True, reason=None)

        def __exit__(self, *_args):
            state["leased"] = False

    original_request = fake.reqHistoricalTicks

    def request_while_leased(*args, **kwargs):
        assert state["leased"] is True
        return original_request(*args, **kwargs)

    fake.reqHistoricalTicks = request_while_leased  # type: ignore[method-assign]
    gateway = IBKRReadOnlyGateway(
        _config(tmp_path),
        ib_factory=lambda: fake,
        historical_request_lease_factory=Lease,
        now=lambda: NOW,
    )
    gateway.connect()
    contract = gateway.qualify_option_contracts(
        "SPY", date(2026, 8, 21), [625], rights=("C",)
    )[0]
    fake.historical_ticks_by_con_id[contract.contract_id] = (
        SimpleNamespace(
            time=NOW - timedelta(seconds=1),
            priceBid=1.0,
            priceAsk=1.2,
        ),
    )

    batch = gateway.option_quote_batch((contract,))

    assert batch.status is QuoteBatchStatus.COMPLETE
    assert state["leased"] is False


def test_historical_enrichment_stops_at_the_aggregate_owner_deadline(
    tmp_path: Path,
) -> None:
    fake = FakeIB()
    fake.exchange_time = None
    monotonic_values = iter((0.0, 1.0, 2.0, 8.0))
    gateway = IBKRReadOnlyGateway(
        _config(tmp_path),
        ib_factory=lambda: fake,
        historical_request_lease_factory=_historical_lease,
        historical_request_max_concurrency=2,
        now=lambda: NOW,
        monotonic=lambda: next(monotonic_values, 8.0),
    )
    gateway.connect()
    contracts = gateway.qualify_option_contracts(
        "SPY", date(2026, 8, 21), [625], rights=("C", "P")
    )
    for contract in contracts:
        fake.historical_ticks_by_con_id[contract.contract_id] = (
            SimpleNamespace(
                time=NOW - timedelta(seconds=1),
                priceBid=1.0,
                priceAsk=1.2,
            ),
        )

    batch = gateway.option_quote_batch(contracts)

    assert batch.status is QuoteBatchStatus.PARTIAL
    assert len(fake.req_historical_ticks_calls) == 1
    assert (
        f"HISTORICAL_BBO_DEADLINE_EXCEEDED:{contracts[1].contract_id}"
        in batch.blockers
    )
    assert gateway.account_snapshot().net_liquidation == Decimal("2012.44")


def test_deadline_expiry_before_dispatch_does_not_claim_historical_source(
    tmp_path: Path,
) -> None:
    fake = FakeIB()
    fake.exchange_time = None
    monotonic_values = iter((0.0, 1.0, 8.0))
    gateway = IBKRReadOnlyGateway(
        _config(tmp_path),
        ib_factory=lambda: fake,
        historical_request_lease_factory=_historical_lease,
        now=lambda: NOW,
        monotonic=lambda: next(monotonic_values),
    )
    gateway.connect()
    contract = gateway.qualify_option_contracts(
        "SPY", date(2026, 8, 21), [625], rights=("C",)
    )[0]

    batch = gateway.option_quote_batch((contract,))

    assert batch.status is QuoteBatchStatus.PARTIAL
    assert batch.source == "IBKR_REQ_TICKERS_READONLY"
    assert fake.req_historical_ticks_calls == []
    assert (
        f"HISTORICAL_BBO_DEADLINE_EXCEEDED:{contract.contract_id}"
        in batch.blockers
    )


def test_naive_wrapper_exchange_time_is_not_promoted_to_utc_authority(
    tmp_path: Path,
) -> None:
    fake = FakeIB()
    fake.exchange_time = NOW.replace(tzinfo=None)
    gateway = IBKRReadOnlyGateway(
        _config(tmp_path),
        ib_factory=lambda: fake,
        now=lambda: NOW,
    )
    gateway.connect()
    contract = gateway.qualify_option_contracts(
        "SPY", date(2026, 8, 21), [625], rights=("C",)
    )[0]

    batch = gateway.option_quote_batch((contract,))

    assert batch.status is QuoteBatchStatus.PARTIAL
    assert batch.quotes[0].exchange_time is None
    assert (
        f"QUOTE_EXCHANGE_TIMESTAMP_UNAVAILABLE:{contract.contract_id}"
        in batch.blockers
    )


@pytest.mark.parametrize(
    "historical_tick",
    [
        SimpleNamespace(
            time=NOW - timedelta(seconds=1),
            priceBid=0.95,
            priceAsk=1.2,
        ),
        SimpleNamespace(
            time=NOW - timedelta(seconds=6),
            priceBid=1.0,
            priceAsk=1.2,
        ),
        SimpleNamespace(
            time=NOW + timedelta(seconds=1),
            priceBid=1.0,
            priceAsk=1.2,
        ),
        SimpleNamespace(
            time=(NOW - timedelta(seconds=1)).replace(tzinfo=None),
            priceBid=1.0,
            priceAsk=1.2,
        ),
    ],
)
def test_historical_bbo_mismatch_stale_or_future_time_remains_partial(
    tmp_path: Path,
    historical_tick,
) -> None:
    fake = FakeIB()
    fake.exchange_time = None
    gateway = IBKRReadOnlyGateway(
        _config(tmp_path),
        ib_factory=lambda: fake,
        historical_request_lease_factory=_historical_lease,
        now=lambda: NOW,
    )
    gateway.connect()
    contract = gateway.qualify_option_contracts(
        "SPY", date(2026, 8, 21), [625], rights=("C",)
    )[0]
    fake.historical_ticks_by_con_id[contract.contract_id] = (historical_tick,)

    batch = gateway.option_quote_batch((contract,))

    assert batch.status is QuoteBatchStatus.PARTIAL
    assert batch.quotes[0].exchange_time is None
    assert (
        f"QUOTE_EXCHANGE_TIMESTAMP_UNAVAILABLE:{contract.contract_id}"
        in batch.blockers
    )


def test_historical_bbo_request_requires_explicit_pacing_authority(
    tmp_path: Path,
) -> None:
    fake = FakeIB()
    fake.exchange_time = None
    gateway = IBKRReadOnlyGateway(
        _config(tmp_path),
        ib_factory=lambda: fake,
        historical_request_lease_factory=lambda: _historical_lease(
            allowed=False,
            reason="PACING_REQUEST_WINDOW_EXHAUSTED",
        ),
        now=lambda: NOW,
    )
    gateway.connect()
    contract = gateway.qualify_option_contracts(
        "SPY", date(2026, 8, 21), [625], rights=("C",)
    )[0]

    batch = gateway.option_quote_batch((contract,))

    assert batch.status is QuoteBatchStatus.PARTIAL
    assert fake.req_historical_ticks_calls == []
    assert f"HISTORICAL_PACING_DENIED:{contract.contract_id}" in batch.blockers


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (-1, Decimal("-1")),
        (1, Decimal("1")),
        ("-1.0001", None),
        ("1.0001", None),
    ],
)
def test_option_delta_enforces_closed_unit_interval(
    tmp_path: Path,
    value,
    expected: Decimal | None,
) -> None:
    fake = FakeIB()
    fake.greek_overrides["delta"] = value
    gateway = IBKRReadOnlyGateway(
        _config(tmp_path), ib_factory=lambda: fake, now=lambda: NOW
    )
    gateway.connect()
    contract = gateway.qualify_option_contracts(
        "SPY", date(2026, 8, 21), [625], rights=("P",)
    )[0]

    quote = gateway.option_quote_batch((contract,)).quotes[0]

    assert quote.delta == expected


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("impliedVol", "NaN"),
        ("delta", "Infinity"),
        ("gamma", "-Infinity"),
        ("theta", float("nan")),
        ("vega", float("inf")),
    ],
)
def test_nonfinite_option_greeks_are_rejected(
    tmp_path: Path,
    field: str,
    value,
) -> None:
    fake = FakeIB()
    fake.greek_overrides[field] = value
    gateway = IBKRReadOnlyGateway(
        _config(tmp_path), ib_factory=lambda: fake, now=lambda: NOW
    )
    gateway.connect()
    contract = gateway.qualify_option_contracts(
        "SPY", date(2026, 8, 21), [625], rights=("P",)
    )[0]

    quote = gateway.option_quote_batch((contract,)).quotes[0]

    attribute = "implied_volatility" if field == "impliedVol" else field
    assert getattr(quote, attribute) is None


def test_negative_market_values_volume_and_open_interest_remain_rejected(
    tmp_path: Path,
) -> None:
    fake = FakeIB()
    fake.quote_overrides.update(
        bid=-1,
        ask=-2,
        putVolume=-3,
        putOpenInterest=-4,
    )
    fake.greek_overrides["impliedVol"] = -0.25
    gateway = IBKRReadOnlyGateway(
        _config(tmp_path), ib_factory=lambda: fake, now=lambda: NOW
    )
    gateway.connect()
    contract = gateway.qualify_option_contracts(
        "SPY", date(2026, 8, 21), [625], rights=("P",)
    )[0]

    quote = gateway.option_quote_batch((contract,)).quotes[0]

    assert quote.bid is None
    assert quote.ask is None
    assert quote.volume is None
    assert quote.open_interest is None
    assert quote.implied_volatility is None


@pytest.mark.parametrize("value", [-0.5, 0.5, 1.5])
def test_fractional_volume_and_open_interest_are_rejected_without_truncation(
    tmp_path: Path,
    value: float,
) -> None:
    fake = FakeIB()
    fake.quote_overrides.update(putVolume=value, putOpenInterest=value)
    gateway = IBKRReadOnlyGateway(
        _config(tmp_path), ib_factory=lambda: fake, now=lambda: NOW
    )
    gateway.connect()
    contract = gateway.qualify_option_contracts(
        "SPY", date(2026, 8, 21), [625], rights=("P",)
    )[0]

    quote = gateway.option_quote_batch((contract,)).quotes[0]

    assert quote.volume is None
    assert quote.open_interest is None


def test_quote_batch_samples_one_observation_time_for_every_leg(
    tmp_path: Path,
) -> None:
    times = iter(
        (
            NOW,
            NOW + timedelta(milliseconds=10),
            NOW + timedelta(milliseconds=20),
        )
    )
    fake = FakeIB()
    gateway = IBKRReadOnlyGateway(
        _config(tmp_path),
        ib_factory=lambda: fake,
        batch_id_factory=lambda: "batch-clock",
        now=lambda: next(times),
    )
    gateway.connect()
    contracts = gateway.qualify_option_contracts(
        "SPY", date(2026, 8, 21), [625], rights=("C", "P")
    )

    batch = gateway.option_quote_batch(contracts)

    assert batch.requested_at == NOW
    assert batch.observed_at == NOW + timedelta(milliseconds=10)
    assert batch.completed_at == NOW + timedelta(milliseconds=20)
    assert {quote.observed_at for quote in batch.quotes} == {batch.observed_at}
    assert {quote.exchange_time for quote in batch.quotes} == {NOW}


@pytest.mark.parametrize(
    ("mode", "expected_status", "expected_quotes"),
    [
        ("timeout", QuoteBatchStatus.TIMEOUT, 0),
        ("cancelled", QuoteBatchStatus.CANCELLED, 0),
        ("partial", QuoteBatchStatus.PARTIAL, 1),
        ("duplicate", QuoteBatchStatus.PARTIAL, 1),
        ("extra", QuoteBatchStatus.PARTIAL, 2),
    ],
)
def test_timeout_cancel_partial_and_quote_identity_anomalies_fail_closed(
    tmp_path: Path,
    mode: str,
    expected_status: QuoteBatchStatus,
    expected_quotes: int,
) -> None:
    fake = FakeIB()
    fake.quote_mode = mode
    gateway = IBKRReadOnlyGateway(
        _config(tmp_path),
        ib_factory=lambda: fake,
        batch_id_factory=lambda: "batch-fixed",
        now=lambda: NOW,
    )
    gateway.connect()
    contracts = gateway.qualify_option_contracts(
        "SPY", date(2026, 8, 21), [625], rights=("C", "P")
    )

    batch = gateway.option_quote_batch(contracts)

    assert fake.req_tickers_calls == 1
    assert batch.status is expected_status
    assert len(batch.quotes) == expected_quotes


def test_repeated_conid_is_rejected_and_gateway_has_no_write_callable(
    tmp_path: Path,
) -> None:
    fake = FakeIB()
    gateway = IBKRReadOnlyGateway(
        _config(tmp_path), ib_factory=lambda: fake, now=lambda: NOW
    )
    gateway.connect()
    contract = gateway.qualify_option_contracts(
        "SPY", date(2026, 8, 21), [625], rights=("C",)
    )[0]

    with pytest.raises(ValueError, match="duplicate conId"):
        gateway.option_contract_definitions((contract, contract))
    with pytest.raises(ValueError, match="duplicate conId"):
        gateway.option_quote_batch((contract, contract))

    public_callables = {
        name
        for name, value in inspect.getmembers(IBKRReadOnlyGateway)
        if callable(value) and not name.startswith("_")
    }
    assert public_callables.isdisjoint(
        {
            "place_order",
            "cancel_order",
            "modify_order",
            "submit_order",
            "transmit_order",
            "create_review_instruction",
        }
    )


def test_gateway_package_exports_atomic_snapshot_contract() -> None:
    from options_copilot import gateway

    assert gateway.BrokerSnapshotBuilder is not None
    assert gateway.AtomicBrokerSnapshot is not None
    assert gateway.OptionQuoteBatch is not None
    assert gateway.OptionSecDefSnapshot is not None


def test_sub_seven_dte_is_permanently_rejected(tmp_path: Path) -> None:
    fake = FakeIB()
    gateway = IBKRReadOnlyGateway(
        _config(tmp_path), ib_factory=lambda: fake, now=lambda: NOW
    )
    gateway.connect()
    with pytest.raises(ValueError):
        gateway.option_expirations("SPY", min_dte=3, max_dte=14)


def test_underlying_quotes_are_batched_and_bound_to_ibkr_identity(
    tmp_path: Path,
) -> None:
    fake = FakeIB()
    gateway = IBKRReadOnlyGateway(
        _config(tmp_path), ib_factory=lambda: fake, now=lambda: NOW
    )
    gateway.connect()

    quotes = gateway.underlying_quotes(("SPY", "QQQ"))

    assert fake.req_tickers_calls == 1
    assert [item.symbol for item in quotes] == ["SPY", "QQQ"]
    assert [item.contract_id for item in quotes] == [8488, 8489]
    assert all(item.source == "IBKR_REQ_TICKERS_READONLY" for item in quotes)
    assert all(item.observed_at == NOW for item in quotes)
    assert all(item.bid == Decimal("1.0") for item in quotes)
    assert all(item.ask == Decimal("1.2") for item in quotes)
    assert all(item.market_price == Decimal("1.1") for item in quotes)


def test_indicative_underlying_quotes_wait_for_prior_session_close(
    tmp_path: Path,
) -> None:
    class StagedFrozenIB(FakeIB):
        def __init__(self) -> None:
            super().__init__()
            self.pending_tickers: list[object] = []
            self.streaming_cancellations: list[object] = []
            self.sleep_calls = 0

        def reqMarketDataType(self, market_data_type):
            super().reqMarketDataType(market_data_type)
            self.market_data_type = market_data_type

        def reqMktData(self, contract, *_args):
            ticker = super().reqTickers(contract)[0]
            ticker.close = float("nan")
            ticker.marketDataType = 2
            self.pending_tickers.append(ticker)
            return ticker

        def cancelMktData(self, contract) -> None:
            self.streaming_cancellations.append(contract)

        def sleep(self, _seconds: float) -> None:
            self.sleep_calls += 1
            for ticker in self.pending_tickers:
                ticker.close = 1.05

    fake = StagedFrozenIB()
    gateway = IBKRReadOnlyGateway(
        _config(tmp_path),
        ib_factory=lambda: fake,
        now=lambda: NOW,
    )
    gateway.connect()

    quotes = gateway.underlying_indicative_quotes(("SPY", "XLF"))

    assert fake.sleep_calls == 1
    assert fake.market_data_type_requests == [2, 1]
    assert len(fake.streaming_cancellations) == 2
    assert [item.close for item in quotes] == [Decimal("1.05"), Decimal("1.05")]
    assert all(
        item.source == "IBKR_AFTER_HOURS_UNDERLYING_READONLY" for item in quotes
    )
    gateway.disconnect()


def test_indicative_underlying_quotes_fall_back_to_completed_daily_close(
    tmp_path: Path,
) -> None:
    class HistoricalUnderlyingIB(FakeIB):
        def __init__(self) -> None:
            super().__init__()
            self.pending_tickers: list[object] = []
            self.historical_async_calls: list[dict[str, object]] = []

        def reqMarketDataType(self, market_data_type):
            super().reqMarketDataType(market_data_type)
            self.market_data_type = market_data_type

        def reqMktData(self, contract, *_args):
            ticker = super().reqTickers(contract)[0]
            ticker.close = float("nan")
            ticker.marketDataType = 2
            self.pending_tickers.append(ticker)
            return ticker

        def cancelMktData(self, _contract) -> None:
            return None

        def sleep(self, _seconds: float) -> None:
            return None

        async def reqHistoricalDataAsync(self, contract, **kwargs):
            self.historical_async_calls.append(
                {"contract_id": contract.conId, **kwargs}
            )
            return (
                SimpleNamespace(date="20260730", close=0.90),
                SimpleNamespace(date="20260731", close=0.95),
                SimpleNamespace(date="20260803", close=9.99),
            )

    ticks = iter(index / 10 for index in range(1000))
    fake = HistoricalUnderlyingIB()
    gateway = IBKRReadOnlyGateway(
        _config(tmp_path),
        ib_factory=lambda: fake,
        historical_request_lease_factory=lambda: _historical_lease(),
        now=lambda: NOW,
        monotonic=lambda: next(ticks),
    )
    gateway.connect()

    quotes = gateway.underlying_indicative_quotes(("SPY", "XLF"))

    assert len(fake.historical_async_calls) == 2
    assert all(call["whatToShow"] == "TRADES" for call in fake.historical_async_calls)
    assert [item.last for item in quotes] == [Decimal("0.95"), Decimal("0.95")]
    assert [item.close for item in quotes] == [Decimal("0.90"), Decimal("0.90")]
    assert all(
        item.source.endswith("HISTORICAL_TWO_CLOSES") for item in quotes
    )
    gateway.disconnect()


def test_underlying_quote_identity_mismatch_fails_closed(tmp_path: Path) -> None:
    fake = FakeIB()
    fake.quote_mode = "partial"
    gateway = IBKRReadOnlyGateway(
        _config(tmp_path), ib_factory=lambda: fake, now=lambda: NOW
    )
    gateway.connect()

    with pytest.raises(BrokerConnectionError, match="underlying quote batch"):
        gateway.underlying_quotes(("SPY", "QQQ"))


def test_broker_published_options_session_hours_are_read_only_and_complete(
    tmp_path: Path,
) -> None:
    fake = FakeIB()
    gateway = IBKRReadOnlyGateway(
        _config(tmp_path), ib_factory=lambda: fake, now=lambda: NOW
    )
    gateway.connect()

    hours = gateway.options_session_hours("SPY")

    assert hours.symbol == "SPY"
    assert hours.contract_id == 8488
    assert hours.observed_at == NOW
    assert hours.source == "IBKR_REQ_CONTRACT_DETAILS_READONLY"
    assert hours.liquid_hours == "20260803:0930-20260803:1600"
    assert hours.trading_hours == hours.liquid_hours
    assert hours.timezone_id == "US/Eastern"


@pytest.mark.parametrize(
    ("mode", "reason"),
    (
        ("empty", "CALENDAR_CONTRACT_DETAILS_UNAVAILABLE"),
        ("ambiguous", "CALENDAR_CONTRACT_DETAILS_AMBIGUOUS"),
        ("incomplete", "CALENDAR_CONTRACT_DETAILS_INCOMPLETE"),
        ("identity", "CALENDAR_CONTRACT_IDENTITY_MISMATCH"),
        ("timeout", "CALENDAR_BROKER_REQUEST_TIMEOUT"),
        ("failed", "CALENDAR_BROKER_REQUEST_FAILED"),
    ),
)
def test_calendar_broker_failures_keep_safe_specific_reason(tmp_path, mode, reason):
    class CalendarIB(FakeIB):
        def reqContractDetails(self, contract):
            if mode == "timeout":
                raise TimeoutError("private broker detail")
            if mode == "failed":
                raise RuntimeError("private broker detail")
            rows = super().reqContractDetails(contract)
            if mode == "empty":
                return []
            if mode == "ambiguous":
                return [*rows, *rows]
            if mode == "incomplete":
                rows[0].liquidHours = ""
            if mode == "identity":
                rows[0].contract.symbol = "QQQ"
            return rows

    fake = CalendarIB()
    gateway = IBKRReadOnlyGateway(
        _config(tmp_path), ib_factory=lambda: fake, now=lambda: NOW
    )
    gateway.connect()
    try:
        with pytest.raises(SessionCalendarReadError) as raised:
            gateway.options_session_hours("SPY")
        assert raised.value.reason_code == reason
        assert str(raised.value) == reason
        assert "private broker detail" not in str(raised.value)
    finally:
        gateway.disconnect()
