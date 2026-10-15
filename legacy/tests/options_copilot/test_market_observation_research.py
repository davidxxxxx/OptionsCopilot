"""Tests for the supporting-only market-observation research adapter."""

from __future__ import annotations

import asyncio
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json

import pytest
from fastapi import HTTPException
from starlette.requests import Request

from options_copilot.api import OptionsCopilotServices, create_app
from options_copilot.gateway.market_observation import OBSERVATION_SCHEMA
from options_copilot.market_observation_research import (
    FRESH_EXECUTABLE_EVIDENCE_REASON,
    MAX_RESEARCH_JSON_DEPTH,
    MAX_RESEARCH_REQUEST_BYTES,
    MarketObservationResearchAdapter,
    READ_MODEL_SCHEMA,
)
from options_copilot.storage.canonical import canonical_hash


NOW = datetime(2026, 10, 5, 17, 22, 22, tzinfo=timezone.utc)


def _price(name: str, value: str, at: datetime, *, data_type: int = 1) -> dict[str, object]:
    return {
        "tick_type": {"bid": 1, "ask": 2, "last": 4}[name],
        "tick_variant": "LIVE_FAMILY",
        "value": value,
        "valid": True,
        "callback_received_at": at.isoformat(),
        "market_data_type": data_type,
        "source_event_time": None,
        "source_event_time_status": "UNAVAILABLE",
        "update_count": 1,
    }


def _computation(name: str, at: datetime, *, data_type: int = 1) -> dict[str, object]:
    values = {
        "implied_volatility": "0.1713361",
        "delta": "0.5",
        "gamma": "0.01",
        "vega": "0.2",
        "theta": "-0.1",
        "option_price": "10.9",
        "underlying_price": "754.56",
    }
    return {
        "tick_type": {"bid": 10, "ask": 11, "last": 12, "model": 13}[name],
        "tick_variant": "LIVE_FAMILY",
        "values": values,
        "validity": {field: True for field in values},
        "valid": True,
        "callback_received_at": at.isoformat(),
        "market_data_type": data_type,
        "source_event_time": None,
        "source_event_time_status": "UNAVAILABLE",
        "update_count": 1,
    }


def _row(
    contract_id: int,
    security_type: str,
    right: str | None,
    *,
    data_type: int = 1,
) -> dict[str, object]:
    at = NOW - timedelta(seconds=1) + timedelta(
        microseconds=contract_id % 1_000
    )
    prices = {
        name: _price(name, value, at, data_type=data_type)
        for name, value in (("bid", "10.88"), ("ask", "10.97"), ("last", "10.99"))
    }
    computations = (
        {
            name: _computation(name, at, data_type=data_type)
            for name in ("bid", "ask", "last", "model")
        }
        if security_type == "OPT"
        else {}
    )
    return {
        "contract": {
            "contract_id": contract_id,
            "symbol": "QQQ",
            "security_type": security_type,
            "currency": "USD",
            "exchange": "SMART",
            "primary_exchange": None,
            "expiration": "20261016" if security_type == "OPT" else None,
            "strike": "752" if security_type == "OPT" else None,
            "right": right,
            "trading_class": "QQQ",
            "multiplier": "100" if security_type == "OPT" else None,
        },
        "market_data_type": {
            "value": data_type,
            "callback_received_at": at.isoformat(),
        },
        "prices": prices,
        "option_computations": computations,
        "price_tick_history": [dict(item) for item in prices.values()],
        "option_computation_tick_history": [
            dict(item) for item in computations.values()
        ],
        "history_limits": {"price": 256, "option_computation": 512},
        "history_truncated": {"price": 0, "option_computation": 0},
        "asynchronous_callbacks": True,
        "atomic_source_snapshot": False,
    }


def _observation() -> dict[str, object]:
    body = {
        "schema": OBSERVATION_SCHEMA,
        "status": "OBSERVED",
        "scope": "BOUNDED_MARKET_DATA_ONLY",
        "source": "IBKR",
        "requested_at": (NOW - timedelta(seconds=8)).isoformat(),
        "completed_at": NOW.isoformat(),
        "request": {
            "symbol": "QQQ",
            "expiration": "20261016",
            "strike": "752",
            "rights": ["C", "P"],
            "collection_seconds": 8.0,
        },
        "contracts": [
            _row(320227571, "STK", None),
            _row(926245270, "OPT", "C"),
            _row(926246839, "OPT", "P"),
        ],
        "decision_authority": "OBSERVATION_ONLY",
        "production_eligible": False,
        "approval_eligible": False,
        "instruction_creation_allowed": False,
        "source_event_time_available": False,
        "atomic_source_snapshot": False,
        "account_snapshot": {"private": "must not escape"},
        "working_orders": [{"private": "must not escape"}],
    }
    return {**body, "content_hash": canonical_hash(body)}


def _rehash(value: dict[str, object]) -> None:
    value.pop("content_hash", None)
    value["content_hash"] = canonical_hash(value)


def _services(**overrides: object) -> OptionsCopilotServices:
    values: dict[str, object] = {
        "health_provider": lambda: {},
        "bootstrap_provider": lambda: {},
        "candidates_provider": lambda: [],
        "positions_provider": lambda: [],
        "learning_provider": lambda: {},
    }
    values.update(overrides)
    return OptionsCopilotServices(**values)  # type: ignore[arg-type]


def _route(app, path: str):
    return next(route.endpoint for route in app.routes if route.path == path)


def _request(
    value: object,
    *,
    include_content_length: bool = True,
    chunk_size: int | None = None,
    content_type: str | None = "application/json",
) -> Request:
    payload = json.dumps(value).encode("utf-8")
    return _raw_request(
        payload,
        include_content_length=include_content_length,
        chunk_size=chunk_size,
        content_type=content_type,
    )


def _raw_request(
    payload: bytes,
    *,
    include_content_length: bool = True,
    chunk_size: int | None = None,
    content_type: str | None = "application/json",
) -> Request:
    chunks = (
        [payload]
        if chunk_size is None
        else [
            payload[offset:offset + chunk_size]
            for offset in range(0, len(payload), chunk_size)
        ]
    )
    messages = [
        {
            "type": "http.request",
            "body": chunk,
            "more_body": index < len(chunks) - 1,
        }
        for index, chunk in enumerate(chunks)
    ]

    async def receive() -> dict[str, object]:
        if messages:
            return messages.pop(0)
        return {"type": "http.disconnect"}

    headers = []
    if content_type is not None:
        headers.append((b"content-type", content_type.encode("ascii")))
    if include_content_length:
        headers.append((b"content-length", str(len(payload)).encode("ascii")))
    return Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/api/research/market-observation",
            "headers": headers,
        },
        receive,
    )


def test_adapter_projects_only_allowlisted_research_fields() -> None:
    result = MarketObservationResearchAdapter(clock=lambda: NOW).project(_observation())

    assert result["schema"] == READ_MODEL_SCHEMA
    assert result["status"] == "RESEARCH_READY"
    assert result["decision_authority"] == "OBSERVATION_ONLY"
    assert result["model_input_complete"] is False
    assert result["production_eligible"] is False
    assert result["entry_authority"] is False
    assert result["approval_eligible"] is False
    assert result["instruction_creation_allowed"] is False
    assert result["order_allowed"] is False
    assert result["requires_fresh_executable_evidence"] is True
    assert FRESH_EXECUTABLE_EVIDENCE_REASON in result["reason_codes"]
    assert "account_snapshot" not in repr(result)
    assert "working_orders" not in repr(result)
    assert (
        "update_count"
        not in result["contracts"][0]["market_fields"]["prices"]["bid"]
    )


def test_adapter_uses_current_time_and_reports_old_evidence_as_stale() -> None:
    result = MarketObservationResearchAdapter(
        clock=lambda: NOW + timedelta(days=1)
    ).project(_observation())

    assert result["status"] == "RESEARCH_DEGRADED"
    assert "RESEARCH_MARKET_FIELDS_STALE" in result["reason_codes"]
    assert all(row["stale_fields"] for row in result["contracts"])


def test_adapter_rejects_tamper_topology_and_authority_upgrade() -> None:
    tampered = _observation()
    tampered["status"] = "UNAVAILABLE"
    with pytest.raises(ValueError, match="MARKET_OBSERVATION_HASH_MISMATCH"):
        MarketObservationResearchAdapter(clock=lambda: NOW).project(tampered)

    topology = _observation()
    topology["contracts"][2]["contract"]["right"] = "C"  # type: ignore[index]
    _rehash(topology)
    with pytest.raises(
        ValueError,
        match="MARKET_OBSERVATION_CONTRACT_IDENTITY_INVALID",
    ):
        MarketObservationResearchAdapter(clock=lambda: NOW).project(topology)

    upgraded = _observation()
    upgraded["production_eligible"] = True
    _rehash(upgraded)
    with pytest.raises(ValueError, match="MARKET_OBSERVATION_AUTHORITY_INVALID"):
        MarketObservationResearchAdapter(clock=lambda: NOW).project(upgraded)


@pytest.mark.parametrize(
    ("mutation", "expected_reason"),
    (
        ("mixed", "RESEARCH_MARKET_DATA_MIXED"),
        ("delayed", "RESEARCH_MARKET_DATA_DELAYED_HISTORY"),
        ("truncated", "RESEARCH_MARKET_HISTORY_TRUNCATED"),
    ),
)
def test_adapter_degrades_mixed_delayed_and_truncated_history(
    mutation: str,
    expected_reason: str,
) -> None:
    observation = _observation()
    option = observation["contracts"][1]  # type: ignore[index]
    if mutation == "mixed":
        option["prices"]["bid"]["market_data_type"] = 3
    elif mutation == "delayed":
        option["price_tick_history"][0]["tick_type"] = 66
    else:
        option["history_truncated"]["price"] = 1
    _rehash(observation)

    result = MarketObservationResearchAdapter(clock=lambda: NOW).project(observation)

    assert result["status"] == "RESEARCH_DEGRADED"
    assert expected_reason in result["reason_codes"]
    assert result["production_eligible"] is False


def test_runtime_api_entry_projects_without_candidate_or_ranking_services() -> None:
    calls = []
    adapter = MarketObservationResearchAdapter(clock=lambda: NOW + timedelta(days=1))
    app = create_app(
        _services(
            market_observation_research_handler=lambda value: (
                calls.append("research_projection"),
                adapter.project(value),
            )[1],
        )
    )

    result = asyncio.run(
        _route(app, "/api/research/market-observation")(
            _request(_observation())
        )
    )

    assert calls == ["research_projection"]
    assert result["status"] == "RESEARCH_DEGRADED"
    assert result["production_eligible"] is False


def test_api_rejects_handler_output_with_private_or_authoritative_injection() -> None:
    result = MarketObservationResearchAdapter(clock=lambda: NOW).project(_observation())
    top_level = {**result, "account_snapshot": {"private": True}}
    nested = deepcopy(result)
    nested["contracts"][0]["working_orders"] = [{"private": True}]
    _rehash(nested)

    for injected in (top_level, nested):
        app = create_app(
            _services(market_observation_research_handler=lambda _value: injected)
        )
        with pytest.raises(HTTPException) as raised:
            asyncio.run(
                _route(app, "/api/research/market-observation")(
                    _request(_observation())
                )
            )
        assert raised.value.status_code == 422
        assert raised.value.detail == "MARKET_OBSERVATION_RESEARCH_INPUT_INVALID"


@pytest.mark.parametrize("include_content_length", (True, False))
def test_api_rejects_oversized_body_before_handler(
    include_content_length: bool,
) -> None:
    assert MAX_RESEARCH_REQUEST_BYTES == 2_065_536
    calls = []
    oversized = {"padding": "x" * 3_000_000}
    app = create_app(
        _services(
            market_observation_research_handler=lambda value: (
                calls.append(value),
                value,
            )[1]
        )
    )

    with pytest.raises(HTTPException) as raised:
        asyncio.run(
            _route(app, "/api/research/market-observation")(
                _request(
                    oversized,
                    include_content_length=include_content_length,
                    chunk_size=None if include_content_length else 32_768,
                )
            )
        )

    assert raised.value.status_code == 413
    assert raised.value.detail == "MARKET_OBSERVATION_RESEARCH_REQUEST_TOO_LARGE"
    assert calls == []


@pytest.mark.parametrize(
    "content_type",
    (
        "text/plain",
        "application/x-www-form-urlencoded",
        "application/problem+json",
        "application/json; charset=latin-1",
        None,
    ),
)
def test_api_rejects_unsupported_content_type_before_reading_body(
    content_type: str | None,
) -> None:
    calls = []
    reads = []

    async def receive() -> dict[str, object]:
        reads.append("read")
        return {"type": "http.request", "body": b"{}", "more_body": False}

    headers = [] if content_type is None else [
        (b"content-type", content_type.encode("ascii"))
    ]
    request = Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/api/research/market-observation",
            "headers": headers,
        },
        receive,
    )
    app = create_app(
        _services(
            market_observation_research_handler=lambda value: calls.append(value)
        )
    )

    with pytest.raises(HTTPException) as raised:
        asyncio.run(_route(app, "/api/research/market-observation")(request))

    assert raised.value.status_code == 415
    assert raised.value.detail == (
        "MARKET_OBSERVATION_RESEARCH_CONTENT_TYPE_UNSUPPORTED"
    )
    assert reads == []
    assert calls == []


def test_api_accepts_application_json_with_utf8_charset() -> None:
    calls = []
    result = MarketObservationResearchAdapter(clock=lambda: NOW).project(_observation())
    app = create_app(
        _services(
            market_observation_research_handler=lambda value: (
                calls.append(value),
                result,
            )[1]
        )
    )

    response = asyncio.run(
        _route(app, "/api/research/market-observation")(
            _request(
                _observation(),
                content_type="application/json; charset=UTF-8",
            )
        )
    )

    assert response == result
    assert len(calls) == 1


@pytest.mark.parametrize(
    "encoding",
    ("utf-16", "utf-16-le", "utf-32", "utf-32-le"),
)
def test_api_rejects_utf16_and_utf32_bodies_before_handler(
    encoding: str,
) -> None:
    calls = []
    app = create_app(
        _services(
            market_observation_research_handler=lambda value: calls.append(value)
        )
    )
    payload = '{"value":"中文"}'.encode(encoding)

    with pytest.raises(HTTPException) as raised:
        asyncio.run(
            _route(app, "/api/research/market-observation")(
                _raw_request(payload)
            )
        )

    assert raised.value.status_code == 422
    assert raised.value.detail == "MARKET_OBSERVATION_RESEARCH_INPUT_INVALID"
    assert calls == []


@pytest.mark.parametrize(
    "payload",
    (
        '{"value":"中文"}'.encode("utf-8"),
        b'{"value":"\\u4e2d\\u6587"}',
    ),
)
def test_api_accepts_utf8_chinese_literal_and_escape(payload: bytes) -> None:
    calls = []
    result = MarketObservationResearchAdapter(clock=lambda: NOW).project(_observation())
    app = create_app(
        _services(
            market_observation_research_handler=lambda value: (
                calls.append(value),
                result,
            )[1]
        )
    )

    response = asyncio.run(
        _route(app, "/api/research/market-observation")(
            _raw_request(payload)
        )
    )

    assert response == result
    assert calls == [{"value": "中文"}]


def test_api_rejects_invalid_utf8_before_handler() -> None:
    calls = []
    app = create_app(
        _services(
            market_observation_research_handler=lambda value: calls.append(value)
        )
    )

    with pytest.raises(HTTPException) as raised:
        asyncio.run(
            _route(app, "/api/research/market-observation")(
                _raw_request(b'{"value":"\xff"}')
            )
        )

    assert raised.value.status_code == 422
    assert raised.value.detail == "MARKET_OBSERVATION_RESEARCH_INPUT_INVALID"
    assert calls == []


def test_api_converges_excessive_integer_to_422_before_handler() -> None:
    calls = []
    app = create_app(
        _services(
            market_observation_research_handler=lambda value: calls.append(value)
        )
    )
    payload = b'{"value":' + (b"9" * 5_000) + b"}"

    with pytest.raises(HTTPException) as raised:
        asyncio.run(
            _route(app, "/api/research/market-observation")(
                _raw_request(payload)
            )
        )

    assert raised.value.status_code == 422
    assert raised.value.detail == "MARKET_OBSERVATION_RESEARCH_INPUT_INVALID"
    assert calls == []


@pytest.mark.parametrize(
    ("depth", "accepted"),
    ((64, True), (65, False), (10_000, False)),
)
def test_api_enforces_exact_json_depth_boundary(
    depth: int,
    accepted: bool,
) -> None:
    assert MAX_RESEARCH_JSON_DEPTH == 64
    calls = []
    result = MarketObservationResearchAdapter(clock=lambda: NOW).project(_observation())
    app = create_app(
        _services(
            market_observation_research_handler=lambda value: (
                calls.append(value),
                result,
            )[1]
        )
    )
    payload = (
        b'{"value":'
        + (b"[" * (depth - 1))
        + b"0"
        + (b"]" * (depth - 1))
        + b"}"
    )

    if accepted:
        response = asyncio.run(
            _route(app, "/api/research/market-observation")(
                _raw_request(payload)
            )
        )
        assert response == result
        assert len(calls) == 1
    else:
        with pytest.raises(HTTPException) as raised:
            asyncio.run(
                _route(app, "/api/research/market-observation")(
                    _raw_request(payload)
                )
            )
        assert raised.value.status_code == 422
        assert raised.value.detail == "MARKET_OBSERVATION_RESEARCH_INPUT_INVALID"
        assert calls == []


@pytest.mark.parametrize("include_content_length", (True, False))
@pytest.mark.parametrize(("extra_bytes", "accepted"), ((0, True), (1, False)))
def test_api_enforces_exact_request_size_boundary(
    include_content_length: bool,
    extra_bytes: int,
    accepted: bool,
) -> None:
    calls = []
    result = MarketObservationResearchAdapter(clock=lambda: NOW).project(_observation())
    app = create_app(
        _services(
            market_observation_research_handler=lambda value: (
                calls.append(value),
                result,
            )[1]
        )
    )
    prefix = b'{"padding":"'
    suffix = b'"}'
    payload = (
        prefix
        + b"x" * (
            MAX_RESEARCH_REQUEST_BYTES - len(prefix) - len(suffix) + extra_bytes
        )
        + suffix
    )

    if accepted:
        response = asyncio.run(
            _route(app, "/api/research/market-observation")(
                _raw_request(
                    payload,
                    include_content_length=include_content_length,
                    chunk_size=32_768,
                )
            )
        )
        assert response == result
        assert len(calls) == 1
    else:
        with pytest.raises(HTTPException) as raised:
            asyncio.run(
                _route(app, "/api/research/market-observation")(
                    _raw_request(
                        payload,
                        include_content_length=include_content_length,
                        chunk_size=32_768,
                    )
                )
            )
        assert raised.value.status_code == 413
        assert raised.value.detail == (
            "MARKET_OBSERVATION_RESEARCH_REQUEST_TOO_LARGE"
        )
        assert calls == []
