from __future__ import annotations

from datetime import datetime, timezone
from io import BytesIO
import json
from pathlib import Path
from types import SimpleNamespace
import traceback
from urllib.error import HTTPError

import pytest

from options_copilot.providers.jin10 import Jin10EventProvider
from options_copilot.providers.jin10_mcp import (
    JIN10_MCP_URL,
    Jin10McpError,
    Jin10McpHttpClient,
    Jin10McpNewsBatch,
)
from options_copilot.security.jin10_credentials import (
    activate_jin10_credential,
    jin10_rotation_evidence_dir,
)
from options_copilot.security.revocation import (
    JIN10_SECRET_NAME,
    RevocationAttestation,
    reserve_rotation,
    write_revocation_attestation,
)
import options_copilot.runtime as runtime_module


NOW = datetime(2026, 8, 6, 6, 30, tzinfo=timezone.utc)


class _Headers(dict):
    def get(self, key, default=None):
        wanted = str(key).lower()
        for name, value in self.items():
            if str(name).lower() == wanted:
                return value
        return default


class _Response:
    def __init__(
        self,
        body: bytes = b"",
        *,
        status: int = 200,
        content_type: str = "application/json; charset=utf-8",
        session_id: str | None = None,
        url: str = JIN10_MCP_URL,
    ) -> None:
        self.status = status
        self.headers = _Headers(
            {
                "Content-Type": content_type,
                "Content-Length": str(len(body)),
                "Content-Encoding": "identity",
                **({"Mcp-Session-Id": session_id} if session_id else {}),
            }
        )
        self._body = BytesIO(body)
        self._url = url

    def geturl(self) -> str:
        return self._url

    def read(self, size: int = -1) -> bytes:
        return self._body.read(size)

    def readline(self, size: int = -1) -> bytes:
        return self._body.readline(size)

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None


class _McpOpener:
    def __init__(self) -> None:
        self.requests = []

    def open(self, request, *, timeout):
        self.requests.append((request, timeout))
        assert request.full_url == JIN10_MCP_URL
        if request.get_method() == "DELETE":
            return _Response(status=204, content_type="application/json")
        payload = json.loads(request.data.decode("utf-8"))
        method = payload["method"]
        if method == "initialize":
            body = {
                "jsonrpc": "2.0",
                "id": payload["id"],
                "result": {
                    "protocolVersion": "2025-03-26",
                    "capabilities": {"tools": {}},
                    "serverInfo": {"name": "jin10", "version": "fixture"},
                },
            }
            return _Response(
                json.dumps(body).encode(), session_id="fixture-session"
            )
        if method == "notifications/initialized":
            return _Response(status=202, content_type="application/json")
        if method == "tools/list":
            body = {
                "jsonrpc": "2.0",
                "id": payload["id"],
                "result": {
                    "tools": [
                        {
                            "name": "list_flash",
                            "inputSchema": {
                                "type": "object",
                                "properties": {"limit": {"type": "integer"}},
                            },
                        },
                        {
                            "name": "list_news",
                            "inputSchema": {
                                "type": "object",
                                "properties": {},
                            },
                        },
                        {
                            "name": "list_calendar",
                            "inputSchema": {
                                "type": "object",
                                "additionalProperties": False,
                            },
                        },
                    ]
                },
            }
            return _Response(json.dumps(body).encode())
        assert method == "tools/call"
        tool = payload["params"]["name"]
        if tool == "list_flash":
            data = {
                "status": 200,
                "message": "success",
                "data": {
                    "items": [
                        {
                            "id": "flash-1",
                            "title": "",
                            "content": "美联储官员发表讲话",
                            "time": "2026-08-06T14:29:00+08:00",
                            "url": "https://flash.jin10.com/detail/flash-1",
                        }
                    ]
                },
            }
            assert payload["params"]["arguments"] == {"limit": 12}
        elif tool == "list_news":
            data = {
                "status": 200,
                "message": "success",
                "data": {
                    "items": [
                        {
                            "id": "news-1",
                            "title": "科技股财报前瞻",
                            "introduction": "关注隐含波动率与预期差。",
                            "time": "2026-08-06T13:00:00+08:00",
                            "url": "https://xnews.jin10.com/details/news-1",
                        }
                    ]
                },
            }
        else:
            assert tool == "list_calendar"
            assert payload["params"]["arguments"] == {}
            data = {
                "status": 200,
                "message": "OK",
                "data": [
                    {
                        "title": "美国7月未季调CPI年率",
                        "pub_time": "2026-08-12 20:30",
                        "consensus": "3.4",
                        "actual": "3.4",
                        "previous": "3.50",
                        "revised": None,
                        "star": 5,
                        "affect_txt": "影响较小",
                    }
                ],
            }
        body = {
            "jsonrpc": "2.0",
            "id": payload["id"],
            "result": {
                "content": [{"type": "text", "text": json.dumps(data)}],
                "isError": False,
            },
        }
        return _Response(json.dumps(body).encode())


class _Secrets:
    def __init__(self, token: str) -> None:
        self.token = token

    def get(self, name: str) -> str:
        assert name == "JIN10_MCP_TOKEN"
        return self.token


def test_streamable_http_client_negotiates_tools_without_leaking_token() -> None:
    opener = _McpOpener()
    client = Jin10McpHttpClient(opener=opener, timeout_seconds=3)

    batch = client.fetch_news("fixture-token-must-stay-secret", limit=12)

    assert set(batch.payloads) == {"list_flash", "list_news"}
    assert batch.failures == ()
    assert [request.get_method() for request, _timeout in opener.requests] == [
        "POST",
        "POST",
        "POST",
        "POST",
        "POST",
        "DELETE",
    ]
    for request, timeout in opener.requests:
        assert 0 < timeout <= 3.0
        assert request.full_url == JIN10_MCP_URL
        assert "fixture-token-must-stay-secret" not in request.full_url
        assert request.get_header("Authorization") == (
            "Bearer fixture-token-must-stay-secret"
        )
    session_requests = opener.requests[1:]
    assert all(
        request.get_header("Mcp-session-id") == "fixture-session"
        for request, _timeout in session_requests
    )


def test_streamable_http_client_default_budget_covers_complete_session() -> None:
    client = Jin10McpHttpClient(opener=_McpOpener())

    assert client.timeout_seconds == 8.0
    assert client.overall_timeout_seconds == 40.0


def test_streamable_http_client_fetches_bounded_calendar_tool() -> None:
    opener = _McpOpener()
    client = Jin10McpHttpClient(opener=opener, timeout_seconds=3)

    batch = client.fetch_calendar("fixture-token-must-stay-secret")

    assert batch.payload["status"] == 200
    assert batch.payload["data"][0]["consensus"] == "3.4"  # type: ignore[index]
    calls = [
        json.loads(request.data.decode("utf-8"))
        for request, _timeout in opener.requests
        if request.get_method() == "POST" and request.data
    ]
    assert calls[-1]["params"] == {
        "name": "list_calendar",
        "arguments": {},
    }


def test_provider_projects_flash_and_news_as_supporting_only() -> None:
    batch = Jin10McpNewsBatch(
        payloads={
            "list_flash": {
                "status": 200,
                "data": {
                    "items": [
                        {
                            "id": "flash-1",
                            "title": "",
                            "content": "黄金快速上涨",
                            "time": "2026-08-06T14:29:00+08:00",
                            "url": "https://flash.jin10.com/detail/flash-1",
                        }
                    ]
                },
            },
            "list_news": {
                "status": 200,
                "data": {
                    "items": [
                        {
                            "id": "news-1",
                            "title": "美股期权观察",
                            "introduction": "关注成交成本。",
                            "time": "2026-08-06T13:00:00+08:00",
                            "url": "https://xnews.jin10.com/details/news-1",
                        }
                    ]
                },
            },
        }
    )

    class Client:
        transport_verified = True

        @staticmethod
        def fetch_news(token: str, *, limit: int) -> Jin10McpNewsBatch:
            assert token == "fixture-token"
            assert limit == 50
            return batch

    provider = Jin10EventProvider(
        _Secrets("fixture-token"), mcp_client=Client(), now=lambda: NOW
    )
    events = provider.news(["SPY", "QQQ"], limit=50)

    assert len(events) == 2
    assert provider.transport_verified is True
    assert provider.health == "READY"
    assert {event.source for event in events} == {"Jin10"}
    assert {event.symbol for event in events} == {None}
    assert {event.decision_authority for event in events} == {"SUPPORTING_ONLY"}
    assert {event.source_id for event in events} == {"flash-1", "news-1"}


def test_auth_failure_is_redacted_and_fails_closed() -> None:
    token = "sentinel-jin10-token-must-not-leak"

    class Opener:
        @staticmethod
        def open(request, *, timeout):
            raise HTTPError(request.full_url, 401, token, {}, None)

    client = Jin10McpHttpClient(opener=Opener())
    with pytest.raises(Jin10McpError) as captured:
        client.fetch_news(token, limit=10)

    rendered = "".join(
        traceback.format_exception(
            captured.type, captured.value, captured.tb
        )
    )
    assert token not in rendered
    assert captured.value.reason == "AUTHENTICATION_FAILED"
    assert captured.value.__cause__ is None
    assert captured.value.__context__ is None


def test_rate_limit_is_redacted_and_never_retried_by_transport() -> None:
    token = "sentinel-jin10-rate-limit-token"

    class Opener:
        calls = 0

        @classmethod
        def open(cls, request, *, timeout):
            cls.calls += 1
            raise HTTPError(request.full_url, 429, token, {}, None)

    client = Jin10McpHttpClient(opener=Opener())
    with pytest.raises(Jin10McpError) as captured:
        client.fetch_news(token, limit=10)

    assert Opener.calls == 1
    assert captured.value.reason == "RATE_LIMITED"
    assert captured.value.__cause__ is None
    assert captured.value.__context__ is None
    assert token not in "".join(
        traceback.format_exception(
            captured.type, captured.value, captured.tb
        )
    )


def test_bad_json_drops_sensitive_parser_context() -> None:
    token = "sentinel-jin10-bad-json-token"

    class Opener:
        @staticmethod
        def open(_request, *, timeout):
            return _Response(f"not-json:{token}".encode())

    client = Jin10McpHttpClient(opener=Opener())
    with pytest.raises(Jin10McpError) as captured:
        client.fetch_news("fixture-token", limit=10)

    assert captured.value.reason == "BAD_JSON"
    assert captured.value.__context__ is None
    assert token not in "".join(
        traceback.format_exception(
            captured.type, captured.value, captured.tb
        )
    )


def test_unexpected_internal_failure_drops_sensitive_exception_context() -> None:
    token = "sentinel-jin10-internal-token"

    class Client(Jin10McpHttpClient):
        def _list_tools(self, *args, **kwargs):
            raise RuntimeError(token)

    client = Client(opener=_McpOpener())
    with pytest.raises(Jin10McpError) as captured:
        client.fetch_news("fixture-token", limit=10)

    assert captured.value.reason == "PROTOCOL_ERROR"
    assert captured.value.__context__ is None
    assert token not in "".join(
        traceback.format_exception(
            captured.type, captured.value, captured.tb
        )
    )


def test_overall_deadline_bounds_the_complete_short_session() -> None:
    ticks = iter((0.0, 0.0, 4.0, 4.0))
    opener = _McpOpener()
    client = Jin10McpHttpClient(
        opener=opener,
        timeout_seconds=3,
        overall_timeout_seconds=3,
        monotonic_clock=lambda: next(ticks),
    )

    with pytest.raises(Jin10McpError) as captured:
        client.fetch_news("fixture-token", limit=10)

    assert captured.value.reason == "REQUEST_TIMEOUT"
    assert len(opener.requests) == 1


def test_unexpected_redirect_never_leaves_exact_mcp_endpoint() -> None:
    class Opener:
        @staticmethod
        def open(_request, *, timeout):
            return _Response(
                b"{}", url="https://example.invalid/not-jin10"
            )

    client = Jin10McpHttpClient(opener=Opener())
    with pytest.raises(Jin10McpError) as captured:
        client.fetch_news("fixture-token", limit=10)
    assert captured.value.reason == "PROTOCOL_ERROR"


def test_initialize_requires_declared_tools_capability() -> None:
    class Opener(_McpOpener):
        def open(self, request, *, timeout):
            if request.get_method() == "POST":
                payload = json.loads(request.data.decode("utf-8"))
                if payload["method"] == "initialize":
                    self.requests.append((request, timeout))
                    body = {
                        "jsonrpc": "2.0",
                        "id": payload["id"],
                        "result": {
                            "protocolVersion": "2025-03-26",
                            "capabilities": {},
                        },
                    }
                    return _Response(json.dumps(body).encode())
            return super().open(request, timeout=timeout)

    client = Jin10McpHttpClient(opener=Opener())
    with pytest.raises(Jin10McpError) as captured:
        client.fetch_news("fixture-token", limit=10)

    assert captured.value.reason == "TOOLS_CAPABILITY_UNAVAILABLE"


def test_streaming_initialize_stops_after_matching_event_without_waiting_for_eof() -> None:
    class StreamingResponse(_Response):
        def read(self, size: int = -1) -> bytes:
            raise AssertionError("SSE response must not be read through EOF")

    class Opener(_McpOpener):
        def open(self, request, *, timeout):
            if request.get_method() == "POST":
                payload = json.loads(request.data.decode("utf-8"))
                if payload["method"] == "initialize":
                    notification = json.dumps(
                        {"jsonrpc": "2.0", "method": "notifications/message"}
                    )
                    response = json.dumps(
                        {
                            "jsonrpc": "2.0",
                            "id": payload["id"],
                            "result": {
                                "protocolVersion": "2025-03-26",
                                "capabilities": {"tools": {}},
                            },
                        }
                    )
                    body = (
                        f"data: {notification}\n\n"
                        f"data: {response}\n\n"
                        "data: this connection may remain open"
                    ).encode()
                    self.requests.append((request, timeout))
                    return StreamingResponse(
                        body,
                        content_type="text/event-stream; charset=utf-8",
                        session_id="fixture-session",
                    )
            return super().open(request, timeout=timeout)

    client = Jin10McpHttpClient(opener=Opener())
    batch = client.fetch_news("fixture-token", limit=12)
    assert set(batch.payloads) == {"list_flash", "list_news"}


def test_repeated_tools_cursor_fails_closed_within_bounded_pages() -> None:
    class Opener(_McpOpener):
        tool_list_calls = 0

        def open(self, request, *, timeout):
            if request.get_method() == "POST":
                payload = json.loads(request.data.decode("utf-8"))
                if payload["method"] == "tools/list":
                    self.requests.append((request, timeout))
                    self.tool_list_calls += 1
                    body = {
                        "jsonrpc": "2.0",
                        "id": payload["id"],
                        "result": {"tools": [], "nextCursor": "same-cursor"},
                    }
                    return _Response(json.dumps(body).encode())
            return super().open(request, timeout=timeout)

    opener = Opener()
    client = Jin10McpHttpClient(opener=opener)
    with pytest.raises(Jin10McpError) as captured:
        client.fetch_news("fixture-token", limit=10)

    assert captured.value.reason == "PAGINATION_CURSOR_LOOP"
    assert opener.tool_list_calls == 2


def test_session_id_may_not_change_after_initialize() -> None:
    class Opener(_McpOpener):
        def open(self, request, *, timeout):
            if request.get_method() == "POST":
                payload = json.loads(request.data.decode("utf-8"))
                if payload["method"] == "tools/list":
                    self.requests.append((request, timeout))
                    body = {
                        "jsonrpc": "2.0",
                        "id": payload["id"],
                        "result": {"tools": []},
                    }
                    return _Response(
                        json.dumps(body).encode(),
                        session_id="rotated-session",
                    )
            return super().open(request, timeout=timeout)

    client = Jin10McpHttpClient(opener=Opener())
    with pytest.raises(Jin10McpError) as captured:
        client.fetch_news("fixture-token", limit=10)

    assert captured.value.reason == "PROTOCOL_ERROR"


def test_runtime_composes_verified_mcp_only_for_activated_dpapi_generation(
    monkeypatch,
    tmp_path: Path,
) -> None:
    class Store:
        value: str | None = None

        def names(self) -> tuple[str, ...]:
            return (JIN10_SECRET_NAME,) if self.value is not None else ()

        def get(self, name: str) -> str | None:
            assert name == JIN10_SECRET_NAME
            return self.value

    store = Store()
    evidence_dir = jin10_rotation_evidence_dir(tmp_path)
    attestation = RevocationAttestation.create(
        name=JIN10_SECRET_NAME,
        actor="human:test",
        signed_at=NOW,
    )
    write_revocation_attestation(attestation, evidence_dir)
    from options_copilot.security.local_api_keys import LocalJin10EnvelopeReader

    store.value = "fixture-token"
    class BindingStore:
        value: str | None = None

        def get(self, _name: str) -> str | None:
            return self.value

        def set(self, _name: str, value: str) -> None:
            self.value = value

    binding_store = BindingStore()
    reader = LocalJin10EnvelopeReader(store, binding_store)
    generation = reader.create_opaque_binding()
    assert generation is not None
    with reserve_rotation(attestation, evidence_dir) as reservation:
        activate_jin10_credential(
            evidence_dir,
            attestation_hash=attestation.canonical_hash,
            credential_generation=generation,
            activated_at=NOW,
        )
        reservation.commit(rotated_at=NOW)

    monkeypatch.setattr(
        runtime_module,
        "LocalApiKeyStore",
        lambda _path: store,
    )
    news, calendars = runtime_module._configured_event_providers(
        SimpleNamespace(
            secrets_path=Path("fixture-only"),
            data_dir=tmp_path,
        ),
        jin10_binding_store=binding_store,
    )

    jin10 = [item for item in news if isinstance(item, Jin10EventProvider)]
    assert len(jin10) == 1
    assert jin10[0].transport_verified is True
    assert len(calendars) == 1
