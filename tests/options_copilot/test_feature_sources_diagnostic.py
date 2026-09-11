"""Bounded raw-source diagnostics retain broker ownership and observation scope."""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import datetime, timezone
import threading
import time
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import httpx
import pytest

from options_copilot.api import OptionsCopilotServices, create_app
from options_copilot.gateway.ibkr_readonly import MarketDataPacingError
from options_copilot.runtime import OptionsCopilotRuntime
from options_copilot.storage.canonical import canonical_hash


REQUEST = {
    "confirmation_token": "READ_FEATURE_SOURCE_DIAGNOSTIC",
    "scope": "SINGLE_UNDERLYING",
    "symbol": "SPY",
}


def _source(kind: str, symbol: str, cutoff: datetime) -> dict[str, object]:
    observed = datetime.now(timezone.utc).isoformat()
    payload = {
        "schema": "options_copilot.feature_source_diagnostic.v1",
        "kind": kind,
        "status": "DELIVERED",
        "symbol": symbol,
        "source": "IBKR",
        "contract": {
            "con_id": 756733,
            "symbol": symbol,
            "sec_type": "STK",
            "currency": "USD",
            "exchange": "SMART",
            "primary_exchange": "ARCA",
        },
        "requested_at": observed,
        "cutoff_at": cutoff.isoformat(),
        "available_at": observed,
        "request_sent": True,
        "broker_request_id": 32,
        "request_parameters": {
            "endDateTime": "" if kind == "PRICE_HISTORY" else cutoff.astimezone(ZoneInfo("America/New_York")).replace(hour=0, minute=0, second=0, microsecond=0).isoformat(),
            "durationStr": "1 Y" if kind == "PRICE_HISTORY" else "2 Y",
            "barSizeSetting": "1 day",
            "whatToShow": "ADJUSTED_LAST" if kind == "PRICE_HISTORY" else "OPTION_IMPLIED_VOLATILITY",
            "useRTH": True, "formatDate": 1, "keepUpToDate": False,
        },
        "basis_status": "PROVIDER_NATIVE_UNRESOLVED",
        "decision_authority": "OBSERVATION_ONLY",
        "model_input_complete": False,
        "production_eligible": False,
        "point_in_time_verified": False,
        "reason_codes": [],
        "broker_error_codes": [],
    }
    if kind == "CURRENT_IV":
        payload.update({
            "value": "0.2125", "received_at": observed,
            "tick_type": 24, "generic_tick": 106, "market_data_type": 1,
            "source_event_timestamp": None,
            "request_parameters": {"genericTickList": "106", "snapshot": False, "regulatorySnapshot": False},
        })
    else:
        payload.update({
            "bars": [{
                "raw_date": "2026-09-04", "trading_date": "2026-09-04",
                "open": "0.2", "high": "0.3", "low": "0.1",
                "close": "0.21", "volume": None,
                "prior_date_row": True, "valid_close": True,
            }],
            "received_bar_count": 1, "prior_completed_bar_count": 1,
            "excluded_current_or_future_bar_count": 0,
            "invalid_bar_count": 0, "duplicate_prior_date_count": 0,
            "required_prior_bar_count": 60 if kind == "PRICE_HISTORY" else 252,
            "enough_prior_bars": False, "calendar_coverage_verified": False,
        })
    return {**payload, "content_hash": canonical_hash(payload)}


class _Gateway:
    connected = True

    def __init__(self) -> None:
        self.calls: list[tuple[str, str, datetime]] = []

    def _read(self, kind, symbol, end_at):
        self.calls.append((kind, symbol, end_at))
        return _source(kind, symbol, end_at)

    def feature_price_history(self, symbol, *, end_at):
        return self._read("PRICE_HISTORY", symbol, end_at)

    def feature_iv_history(self, symbol, *, end_at):
        return self._read("IV_HISTORY", symbol, end_at)

    def feature_current_iv(self, symbol, *, end_at):
        return self._read("CURRENT_IV", symbol, end_at)


def _runtime(gateway=None) -> OptionsCopilotRuntime:
    runtime = object.__new__(OptionsCopilotRuntime)
    runtime.config = SimpleNamespace(news_core_symbols=("SPY", "AAPL"))
    runtime.production_composition = SimpleNamespace(gateway=gateway or _Gateway())
    runtime._closed = False
    runtime._feature_source_diagnostic_lock = threading.Lock()
    runtime._feature_source_diagnostic_last_attempt = None
    runtime._feature_source_diagnostic_cooldown_until = None
    return runtime


def _app(handler):
    return create_app(OptionsCopilotServices(
        health_provider=lambda: {}, bootstrap_provider=lambda: {},
        candidates_provider=lambda: [], positions_provider=lambda: [],
        learning_provider=lambda: {}, feature_sources_diagnostic_handler=handler,
    ))


def _request(app, method="POST", body=None):
    async def send():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver",
        ) as client:
            return await client.request(method, "/api/diagnostics/feature-sources", json=body)
    return asyncio.run(send())


def test_feature_sources_use_one_gateway_preserve_raw_fields_and_cool_down() -> None:
    runtime = _runtime()
    gateway = runtime.production_composition.gateway
    payload = runtime.feature_sources_diagnostic(REQUEST)

    assert [row[0] for row in gateway.calls] == ["PRICE_HISTORY", "IV_HISTORY", "CURRENT_IV"]
    assert {row[1] for row in gateway.calls} == {"SPY"}
    assert len({row[2] for row in gateway.calls}) == 1
    assert payload["status"] == "OBSERVED"
    assert payload["basis_status"] == "PROVIDER_NATIVE_UNRESOLVED"
    assert payload["model_input_complete"] is False
    assert payload["decision_authority"] == "OBSERVATION_ONLY"
    assert payload["sources"]["PRICE_HISTORY"]["bars"][0]["close"] == "0.21"
    assert payload["sources"]["CURRENT_IV"]["value"] == "0.2125"
    assert payload["sources"]["CURRENT_IV"]["source_event_timestamp"] is None
    body = dict(payload)
    assert body.pop("content_hash") == canonical_hash(body)
    for raw in payload["sources"].values():
        source_body = dict(raw)
        assert source_body.pop("content_hash") == canonical_hash(source_body)
    blocked = runtime.feature_sources_diagnostic({**REQUEST, "symbol": "AAPL"})
    assert blocked["status"] == "WAIT"
    assert blocked["reason_codes"] == ("FEATURE_SOURCE_DIAGNOSTIC_COOLDOWN",)
    assert len(gateway.calls) == 3
    assert all(source["request_sent"] is False for source in blocked["sources"].values())

    runtime._feature_source_diagnostic_last_attempt = time.monotonic() - 901
    runtime.feature_sources_diagnostic(REQUEST)
    assert len(gateway.calls) == 6


@pytest.mark.parametrize("request_body", [
    {**REQUEST, "symbol": "spy"}, {**REQUEST, "symbol": "UNLISTED"},
    {**REQUEST, "confirmation_token": "YES"}, {**REQUEST, "scope": "ALL"},
    {**REQUEST, "duration": "100 Y"},
])
def test_invalid_feature_source_request_never_calls_gateway(request_body) -> None:
    runtime = _runtime()
    with pytest.raises(ValueError):
        runtime.feature_sources_diagnostic(request_body)
    assert runtime.production_composition.gateway.calls == []
    assert runtime._feature_source_diagnostic_last_attempt is None


def test_independent_source_failures_remain_unknown_and_no_retry_occurs() -> None:
    class Gateway(_Gateway):
        def feature_price_history(self, symbol, *, end_at):
            self.calls.append(("PRICE_HISTORY", symbol, end_at))
            raise TimeoutError("private-provider-error-should-not-appear")

        def feature_iv_history(self, symbol, *, end_at):
            self.calls.append(("IV_HISTORY", symbol, end_at))
            raise MarketDataPacingError("historical", "PACING_REQUEST_WINDOW_EXHAUSTED")

    runtime = _runtime(Gateway())
    payload = runtime.feature_sources_diagnostic(REQUEST)
    assert payload["status"] == "PARTIAL"
    assert len(runtime.production_composition.gateway.calls) == 3
    assert payload["sources"]["PRICE_HISTORY"]["bars"] is None
    assert payload["sources"]["PRICE_HISTORY"]["received_bar_count"] is None
    assert payload["sources"]["PRICE_HISTORY"]["reason_codes"] == ("FEATURE_SOURCE_TIMEOUT",)
    assert payload["sources"]["IV_HISTORY"]["reason_codes"] == ("FEATURE_SOURCE_PACING_DENIED",)
    assert payload["sources"]["CURRENT_IV"]["status"] == "DELIVERED"
    assert "private-provider-error" not in repr(payload)
    assert "NOT_SUBSCRIBED" not in repr(payload)
    assert runtime.feature_sources_diagnostic(REQUEST)["status"] == "WAIT"
    assert len(runtime.production_composition.gateway.calls) == 3


def test_disconnected_gateway_is_not_connected_or_retried() -> None:
    runtime = _runtime()
    runtime.production_composition.gateway.connected = False
    payload = runtime.feature_sources_diagnostic(REQUEST)
    assert payload["status"] == "UNAVAILABLE"
    assert payload["reason_codes"] == ("IBKR_READONLY_GATEWAY_NOT_CONNECTED",)
    assert runtime.production_composition.gateway.calls == []
    assert runtime.feature_sources_diagnostic(REQUEST)["status"] == "WAIT"


def test_empty_source_stays_distinct_from_unknown_or_unsubscribed() -> None:
    class Gateway(_Gateway):
        def feature_price_history(self, symbol, *, end_at):
            payload = super().feature_price_history(symbol, end_at=end_at)
            payload.update({
                "status": "UNAVAILABLE", "bars": [], "received_bar_count": 0,
                "prior_completed_bar_count": 0,
                "reason_codes": ["FEATURE_HISTORY_EMPTY_OR_TIMEOUT"],
            })
            payload.pop("content_hash")
            return {**payload, "content_hash": canonical_hash(payload)}

    result = _runtime(Gateway()).feature_sources_diagnostic(REQUEST)
    assert result["sources"]["PRICE_HISTORY"]["bars"] == []
    assert result["sources"]["PRICE_HISTORY"]["received_bar_count"] == 0
    assert "NOT_SUBSCRIBED" not in repr(result)


def test_feature_diagnostic_is_single_flight() -> None:
    entered, release = threading.Event(), threading.Event()

    class Gateway(_Gateway):
        def feature_price_history(self, symbol, *, end_at):
            entered.set()
            assert release.wait(timeout=5)
            return super().feature_price_history(symbol, end_at=end_at)

    runtime = _runtime(Gateway())
    with ThreadPoolExecutor(max_workers=1) as executor:
        first = executor.submit(runtime.feature_sources_diagnostic, REQUEST)
        try:
            assert entered.wait(timeout=5)
            second = runtime.feature_sources_diagnostic(REQUEST)
            assert second["reason_codes"] == ("FEATURE_SOURCE_DIAGNOSTIC_RUNNING",)
            assert all(row["request_sent"] is False for row in second["sources"].values())
        finally:
            release.set()
        assert first.result(timeout=5)["status"] == "OBSERVED"
    assert len(runtime.production_composition.gateway.calls) == 3


@pytest.mark.parametrize("change", [
    {"symbol": "AAPL"}, {"content_hash": "0" * 64},
    {"model_input_complete": True}, {"market_score": "0"},
    {"bars": [{}] * 801}, {"authorization": "private-test-value"},
])
def test_hostile_raw_source_is_rejected_without_losing_other_sources(change) -> None:
    class Gateway(_Gateway):
        def feature_price_history(self, symbol, *, end_at):
            payload = super().feature_price_history(symbol, end_at=end_at)
            payload.update(deepcopy(change))
            return payload

    result = _runtime(Gateway()).feature_sources_diagnostic(REQUEST)
    source = result["sources"]["PRICE_HISTORY"]
    assert source["status"] == "UNAVAILABLE"
    assert source["reason_codes"] == ("FEATURE_SOURCE_RESPONSE_INVALID",)
    assert result["sources"]["CURRENT_IV"]["status"] == "DELIVERED"
    assert "private-test-value" not in repr(result)


def test_api_feature_sources_retains_observation_scope_and_get_does_no_work() -> None:
    runtime = _runtime()
    app = _app(runtime.feature_sources_diagnostic)
    assert _request(app, method="GET").status_code == 405
    assert runtime.production_composition.gateway.calls == []
    response = _request(app, body=REQUEST)
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "OBSERVED"
    assert payload["model_input_complete"] is False
    assert payload["production_eligible"] is False
    assert payload["instruction_creation_allowed"] is False
    assert payload["broker_write_authority"] is False
    assert len(runtime.production_composition.gateway.calls) == 3


@pytest.mark.parametrize("body", [
    {**REQUEST, "symbol": "spy"}, {**REQUEST, "scope": "ALL"},
    {**REQUEST, "confirmation_token": "YES"}, {**REQUEST, "new_source": "external"},
    {**REQUEST, "symbol": "UNLISTED"},
])
def test_api_invalid_requests_do_not_acquire(body) -> None:
    runtime = _runtime()
    response = _request(_app(runtime.feature_sources_diagnostic), body=body)
    assert response.status_code == 422
    assert runtime.production_composition.gateway.calls == []


def test_api_provider_failure_is_redacted() -> None:
    def fail(_request):
        raise RuntimeError("private-provider-exception")

    response = _request(_app(fail), body=REQUEST)
    assert response.status_code == 503
    assert response.json()["detail"] == "FEATURE_SOURCE_DIAGNOSTIC_FAILED"
    assert "private-provider" not in response.text


def test_api_rejects_forged_action_authority() -> None:
    runtime = _runtime()
    payload = dict(runtime.feature_sources_diagnostic(REQUEST))
    payload["model_input_complete"] = True
    payload.pop("content_hash")
    payload["content_hash"] = canonical_hash(payload)
    assert _request(_app(lambda _request: payload), body=REQUEST).status_code == 502


@pytest.mark.parametrize("path,value", [
    (("order_allowed",), True),
    (("sources", "CURRENT_IV", "order_allowed"), True),
    (("sources", "CURRENT_IV", "request_sent"), False),
    (("sources", "CURRENT_IV", "market_data_type"), True),
    (("sources", "CURRENT_IV", "production_eligible"), True),
    (("sources", "CURRENT_IV", "received_at"), {}),
    (("sources", "CURRENT_IV", "source_event_timestamp"), "2026-09-08T00:00:00+00:00"),
    (("sources", "CURRENT_IV", "request_parameters", "order_allowed"), True),
    (("sources", "PRICE_HISTORY", "contract", "symbol"), "AAPL"),
    (("sources", "PRICE_HISTORY", "contract", "currency"), {}),
    (("sources", "PRICE_HISTORY", "contract", "con_id"), True),
    (("sources", "PRICE_HISTORY", "prior_completed_bar_count"), True),
    (("sources", "PRICE_HISTORY", "received_bar_count"), "1"),
    (("sources", "PRICE_HISTORY", "available_at"), {}),
    (("sources", "IV_HISTORY", "request_parameters", "formatDate"), True),
    (("sources", "IV_HISTORY", "request_parameters", "whatToShow"), "TRADES"),
    (("sources", "IV_HISTORY", "bars", 0, "volume"), {"raw": "100"}),
    (("sources", "IV_HISTORY", "bars", 0, "order_allowed"), True),
])
def test_api_rejects_self_rehashed_nested_authority_and_shape_forgery(path, value) -> None:
    payload = dict(_runtime().feature_sources_diagnostic(REQUEST))
    target = payload
    for name in path[:-1]:
        target = target[name]
    target[path[-1]] = deepcopy(value)
    if path[0] == "sources":
        source = payload["sources"][path[1]]
        source.pop("content_hash")
        source["content_hash"] = canonical_hash(source)
    payload.pop("content_hash")
    payload["content_hash"] = canonical_hash(payload)
    response = _request(_app(lambda _request: payload), body=REQUEST)
    assert response.status_code == 502
    assert response.json()["detail"] == "FEATURE_SOURCE_DIAGNOSTIC_RESPONSE_INVALID"
