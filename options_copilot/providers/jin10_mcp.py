"""Bounded Streamable HTTP client for the official Jin10 MCP endpoint.

The client is deliberately small and read-only.  It implements only the MCP
initialize, tools/list, and tools/call messages needed for supplemental news
discovery.  Tokens are supplied in an Authorization header, never in a URL,
and every transport error is converted to a fixed redacted reason code.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from http.client import HTTPException
import json
import math
import re
import socket
import ssl
from threading import Lock
from time import monotonic
from types import MappingProxyType
from typing import Callable
from urllib.error import HTTPError, URLError
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener


JIN10_MCP_URL = "https://mcp.jin10.com/mcp"
JIN10_MCP_PROTOCOL_VERSION = "2025-03-26"
JIN10_MCP_USER_AGENT = "OptionsCopilot/0.1 read-only-jin10-mcp"
MAXIMUM_JIN10_RESPONSE_BYTES = 2 * 1024 * 1024
_SUPPORTED_PROTOCOL_VERSIONS = frozenset(
    {"2024-11-05", "2025-03-26", "2025-06-18"}
)
_SESSION_ID = re.compile(r"^[\x21-\x7e]{1,512}$")
_DISCOVERY_TOOLS = ("list_flash", "list_news")
_CALENDAR_TOOL = "list_calendar"
_METADATA_TOOLS = frozenset((*_DISCOVERY_TOOLS, _CALENDAR_TOOL))


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: ANN001
        raise HTTPError(
            req.full_url,
            code,
            "Jin10 MCP redirects are disabled",
            headers,
            fp,
        )


class Jin10McpError(RuntimeError):
    """A redacted fixed-code MCP transport or protocol failure."""

    def __init__(self, reason: str) -> None:
        checked = str(reason or "").strip().upper()
        if not checked or not re.fullmatch(r"[A-Z0-9_]+", checked):
            checked = "PROTOCOL_ERROR"
        self.reason = checked
        super().__init__(checked)


@dataclass(frozen=True, slots=True)
class Jin10McpNewsBatch:
    """Tool payloads plus fixed, non-sensitive per-tool failure codes."""

    payloads: Mapping[str, Mapping[str, object]]
    failures: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        checked_payloads: dict[str, Mapping[str, object]] = {}
        for name, payload in self.payloads.items():
            tool = str(name).strip()
            if tool not in _DISCOVERY_TOOLS or not isinstance(payload, Mapping):
                raise ValueError("Jin10 MCP batch payload is invalid")
            checked_payloads[tool] = MappingProxyType(dict(payload))
        checked_failures = tuple(str(item).strip().upper() for item in self.failures)
        if any(
            not item or not re.fullmatch(r"[A-Z0-9_:]+", item)
            for item in checked_failures
        ):
            raise ValueError("Jin10 MCP batch failure code is invalid")
        object.__setattr__(self, "payloads", MappingProxyType(checked_payloads))
        object.__setattr__(self, "failures", checked_failures)


@dataclass(frozen=True, slots=True)
class Jin10McpCalendarBatch:
    """One bounded point-in-time calendar payload from the official MCP."""

    payload: Mapping[str, object]

    def __post_init__(self) -> None:
        if not isinstance(self.payload, Mapping):
            raise TypeError("Jin10 MCP calendar payload must be a mapping")
        object.__setattr__(self, "payload", MappingProxyType(dict(self.payload)))


@dataclass(slots=True)
class Jin10McpHttpClient:
    """Strict read-only client for ``https://mcp.jin10.com/mcp``."""

    opener: object | None = None
    timeout_seconds: float = 8.0
    overall_timeout_seconds: float = 40.0
    monotonic_clock: Callable[[], float] = monotonic
    _request_number: int = field(default=0, init=False, repr=False)
    _request_lock: Lock = field(default_factory=Lock, init=False, repr=False)

    transport_verified = True

    def __post_init__(self) -> None:
        if isinstance(self.timeout_seconds, bool) or not isinstance(
            self.timeout_seconds, (int, float)
        ):
            raise TypeError("timeout_seconds must be numeric")
        self.timeout_seconds = float(self.timeout_seconds)
        if not 0 < self.timeout_seconds <= 30:
            raise ValueError("timeout_seconds must be between 0 and 30")
        if isinstance(self.overall_timeout_seconds, bool) or not isinstance(
            self.overall_timeout_seconds, (int, float)
        ):
            raise TypeError("overall_timeout_seconds must be numeric")
        self.overall_timeout_seconds = float(self.overall_timeout_seconds)
        if not 0 < self.overall_timeout_seconds <= 60:
            raise ValueError("overall_timeout_seconds must be between 0 and 60")
        if not callable(self.monotonic_clock):
            raise TypeError("monotonic_clock must be callable")
        if self.opener is None:
            self.opener = build_opener(ProxyHandler(), _NoRedirect())

    def fetch_news(self, token: str, *, limit: int = 50) -> Jin10McpNewsBatch:
        """Fetch one bounded flash/news page in a single short MCP session."""

        checked_token = _token(token)
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 200:
            raise ValueError("limit must be an integer between 1 and 200")

        session_id: str | None = None
        protocol_version = JIN10_MCP_PROTOCOL_VERSION
        deadline_at: float | None = None
        try:
            deadline_at = (
                float(self.monotonic_clock()) + self.overall_timeout_seconds
            )
            initialize = self._request(
                checked_token,
                method="initialize",
                params={
                    "protocolVersion": JIN10_MCP_PROTOCOL_VERSION,
                    "capabilities": {},
                    "clientInfo": {
                        "name": "options-copilot",
                        "version": "0.1",
                    },
                },
                session_id=None,
                protocol_version=protocol_version,
                deadline_at=deadline_at,
            )
            protocol_version = str(initialize.result.get("protocolVersion") or "")
            if protocol_version not in _SUPPORTED_PROTOCOL_VERSIONS:
                raise Jin10McpError("UNSUPPORTED_PROTOCOL")
            capabilities = initialize.result.get("capabilities")
            if (
                not isinstance(capabilities, Mapping)
                or not isinstance(capabilities.get("tools"), Mapping)
            ):
                raise Jin10McpError("TOOLS_CAPABILITY_UNAVAILABLE")
            session_id = initialize.session_id
            self._notification(
                checked_token,
                method="notifications/initialized",
                params={},
                session_id=session_id,
                protocol_version=protocol_version,
                deadline_at=deadline_at,
            )
            tools = self._list_tools(
                checked_token,
                session_id=session_id,
                protocol_version=protocol_version,
                deadline_at=deadline_at,
            )
            payloads: dict[str, Mapping[str, object]] = {}
            failures: list[str] = []
            for tool_name in _DISCOVERY_TOOLS:
                descriptor = tools.get(tool_name)
                if descriptor is None:
                    failures.append(f"{tool_name}:TOOL_UNAVAILABLE")
                    continue
                arguments = _tool_arguments(descriptor, limit=limit)
                if arguments is None:
                    failures.append(f"{tool_name}:TOOL_SCHEMA_UNSUPPORTED")
                    continue
                try:
                    called = self._request(
                        checked_token,
                        method="tools/call",
                        params={"name": tool_name, "arguments": arguments},
                        session_id=session_id,
                        protocol_version=protocol_version,
                        deadline_at=deadline_at,
                    )
                    payloads[tool_name] = _tool_payload(called.result)
                except Jin10McpError as exc:
                    failures.append(f"{tool_name}:{exc.reason}")
            return Jin10McpNewsBatch(
                payloads=payloads,
                failures=tuple(failures),
            )
        except Jin10McpError:
            raise
        except Exception:
            # Leave the exception handler before constructing the public
            # failure.  ``raise ... from None`` suppresses display but still
            # retains the original object in ``__context__``.
            pass
        finally:
            if session_id is not None and deadline_at is not None:
                self._delete_session(
                    checked_token,
                    session_id=session_id,
                    protocol_version=protocol_version,
                    deadline_at=deadline_at,
                )
        raise Jin10McpError("PROTOCOL_ERROR") from None

    def fetch_calendar(self, token: str) -> Jin10McpCalendarBatch:
        """Fetch the current natural-week calendar in one bounded session."""

        checked_token = _token(token)
        result = self._call_fixed_tool(
            checked_token,
            tool_name=_CALENDAR_TOOL,
            arguments={},
        )
        return Jin10McpCalendarBatch(payload=_tool_payload(result))

    def _call_fixed_tool(
        self,
        token: str,
        *,
        tool_name: str,
        arguments: Mapping[str, object],
    ) -> Mapping[str, object]:
        """Negotiate and invoke one exact allowlisted read-only tool."""

        if tool_name not in _METADATA_TOOLS:
            raise Jin10McpError("TOOL_UNAVAILABLE")
        session_id: str | None = None
        protocol_version = JIN10_MCP_PROTOCOL_VERSION
        deadline_at: float | None = None
        try:
            deadline_at = (
                float(self.monotonic_clock()) + self.overall_timeout_seconds
            )
            initialize = self._request(
                token,
                method="initialize",
                params={
                    "protocolVersion": JIN10_MCP_PROTOCOL_VERSION,
                    "capabilities": {},
                    "clientInfo": {"name": "options-copilot", "version": "0.1"},
                },
                session_id=None,
                protocol_version=protocol_version,
                deadline_at=deadline_at,
            )
            protocol_version = str(initialize.result.get("protocolVersion") or "")
            if protocol_version not in _SUPPORTED_PROTOCOL_VERSIONS:
                raise Jin10McpError("UNSUPPORTED_PROTOCOL")
            capabilities = initialize.result.get("capabilities")
            if (
                not isinstance(capabilities, Mapping)
                or not isinstance(capabilities.get("tools"), Mapping)
            ):
                raise Jin10McpError("TOOLS_CAPABILITY_UNAVAILABLE")
            session_id = initialize.session_id
            self._notification(
                token,
                method="notifications/initialized",
                params={},
                session_id=session_id,
                protocol_version=protocol_version,
                deadline_at=deadline_at,
            )
            tools = self._list_tools(
                token,
                session_id=session_id,
                protocol_version=protocol_version,
                deadline_at=deadline_at,
            )
            descriptor = tools.get(tool_name)
            if descriptor is None:
                raise Jin10McpError("TOOL_UNAVAILABLE")
            if not _empty_object_arguments(descriptor, arguments=arguments):
                raise Jin10McpError("TOOL_SCHEMA_UNSUPPORTED")
            called = self._request(
                token,
                method="tools/call",
                params={"name": tool_name, "arguments": dict(arguments)},
                session_id=session_id,
                protocol_version=protocol_version,
                deadline_at=deadline_at,
            )
            return called.result
        except Jin10McpError:
            raise
        except Exception:
            pass
        finally:
            if session_id is not None and deadline_at is not None:
                self._delete_session(
                    token,
                    session_id=session_id,
                    protocol_version=protocol_version,
                    deadline_at=deadline_at,
                )
        raise Jin10McpError("PROTOCOL_ERROR") from None

    def _next_request_id(self) -> int:
        with self._request_lock:
            self._request_number += 1
            return self._request_number

    def _request(
        self,
        token: str,
        *,
        method: str,
        params: Mapping[str, object],
        session_id: str | None,
        protocol_version: str,
        deadline_at: float,
    ) -> "_McpReply":
        request_id = self._next_request_id()
        payload = {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
            "params": dict(params),
        }
        response, returned_session = self._post(
            token,
            payload=payload,
            session_id=session_id,
            protocol_version=protocol_version,
            allow_empty=False,
            deadline_at=deadline_at,
        )
        if not isinstance(response, Mapping):
            raise Jin10McpError("PROTOCOL_ERROR")
        if response.get("jsonrpc") != "2.0" or response.get("id") != request_id:
            raise Jin10McpError("PROTOCOL_ERROR")
        if response.get("error") is not None:
            raise Jin10McpError("REMOTE_ERROR")
        result = response.get("result")
        if not isinstance(result, Mapping):
            raise Jin10McpError("PROTOCOL_ERROR")
        if (
            session_id is not None
            and returned_session is not None
            and returned_session != session_id
        ):
            raise Jin10McpError("PROTOCOL_ERROR")
        effective_session = returned_session or session_id
        return _McpReply(dict(result), effective_session)

    def _notification(
        self,
        token: str,
        *,
        method: str,
        params: Mapping[str, object],
        session_id: str | None,
        protocol_version: str,
        deadline_at: float,
    ) -> None:
        self._post(
            token,
            payload={"jsonrpc": "2.0", "method": method, "params": dict(params)},
            session_id=session_id,
            protocol_version=protocol_version,
            allow_empty=True,
            deadline_at=deadline_at,
        )

    def _list_tools(
        self,
        token: str,
        *,
        session_id: str | None,
        protocol_version: str,
        deadline_at: float,
    ) -> dict[str, Mapping[str, object]]:
        tools: dict[str, Mapping[str, object]] = {}
        cursor: str | None = None
        seen_cursors: set[str] = set()
        for _page in range(3):
            params: dict[str, object] = {}
            if cursor is not None:
                params["cursor"] = cursor
            reply = self._request(
                token,
                method="tools/list",
                params=params,
                session_id=session_id,
                protocol_version=protocol_version,
                deadline_at=deadline_at,
            )
            rows = reply.result.get("tools")
            if not isinstance(rows, Sequence) or isinstance(
                rows, (str, bytes, bytearray)
            ):
                raise Jin10McpError("PROTOCOL_ERROR")
            for row in rows:
                if not isinstance(row, Mapping):
                    raise Jin10McpError("PROTOCOL_ERROR")
                name = str(row.get("name") or "").strip()
                if name in _METADATA_TOOLS:
                    tools[name] = dict(row)
            raw_cursor = reply.result.get("nextCursor")
            if raw_cursor in (None, ""):
                return tools
            if not isinstance(raw_cursor, str) or len(raw_cursor) > 1024:
                raise Jin10McpError("PROTOCOL_ERROR")
            if raw_cursor in seen_cursors:
                raise Jin10McpError("PAGINATION_CURSOR_LOOP")
            seen_cursors.add(raw_cursor)
            cursor = raw_cursor
        raise Jin10McpError("PAGINATION_LIMIT_EXCEEDED")

    def _post(
        self,
        token: str,
        *,
        payload: Mapping[str, object],
        session_id: str | None,
        protocol_version: str,
        allow_empty: bool,
        deadline_at: float,
    ) -> tuple[Mapping[str, object] | None, str | None]:
        body = json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        request = Request(
            JIN10_MCP_URL,
            data=body,
            headers=_headers(
                token,
                session_id=session_id,
                protocol_version=protocol_version,
                content_type=True,
            ),
            method="POST",
        )
        failure_reason: str | None = None
        try:
            response = self.opener.open(  # type: ignore[union-attr]
                request,
                timeout=self._remaining_timeout(deadline_at),
            )
            expected_id = payload.get("id")
            return _read_response(
                response,
                allow_empty=allow_empty,
                expected_id=(expected_id if isinstance(expected_id, int) else None),
            )
        except Jin10McpError:
            raise
        except HTTPError as exc:
            failure_reason = _http_reason(exc.code)
        except (TimeoutError, socket.timeout):
            failure_reason = "REQUEST_TIMEOUT"
        except ssl.SSLError:
            failure_reason = "TLS_ERROR"
        except (URLError, HTTPException, OSError):
            failure_reason = "CONNECT_ERROR"
        except Exception:
            failure_reason = "TRANSPORT_ERROR"
        assert failure_reason is not None
        raise Jin10McpError(failure_reason) from None

    def _remaining_timeout(self, deadline_at: float) -> float:
        remaining = float(deadline_at) - float(self.monotonic_clock())
        if not math.isfinite(remaining) or remaining <= 0:
            raise Jin10McpError("REQUEST_TIMEOUT")
        return min(self.timeout_seconds, remaining)

    def _delete_session(
        self,
        token: str,
        *,
        session_id: str,
        protocol_version: str,
        deadline_at: float,
    ) -> None:
        request = Request(
            JIN10_MCP_URL,
            data=None,
            headers=_headers(
                token,
                session_id=session_id,
                protocol_version=protocol_version,
                content_type=False,
            ),
            method="DELETE",
        )
        try:
            response = self.opener.open(  # type: ignore[union-attr]
                request,
                timeout=self._remaining_timeout(deadline_at),
            )
            with response:
                if str(response.geturl()) != JIN10_MCP_URL:
                    return
                if int(getattr(response, "status", 200)) not in {200, 202, 204}:
                    return
                response.read(MAXIMUM_JIN10_RESPONSE_BYTES + 1)
        except Exception:
            return


@dataclass(frozen=True, slots=True)
class _McpReply:
    result: Mapping[str, object]
    session_id: str | None


def _token(value: object) -> str:
    if not isinstance(value, str):
        raise Jin10McpError("AUTHENTICATION_UNAVAILABLE")
    token = value.strip()
    if not token or len(token) > 4096 or any(ord(char) < 33 for char in token):
        raise Jin10McpError("AUTHENTICATION_UNAVAILABLE")
    return token


def _headers(
    token: str,
    *,
    session_id: str | None,
    protocol_version: str,
    content_type: bool,
) -> dict[str, str]:
    headers = {
        "Accept": "application/json, text/event-stream",
        "Accept-Encoding": "identity",
        "Authorization": f"Bearer {token}",
        "MCP-Protocol-Version": protocol_version,
        "User-Agent": JIN10_MCP_USER_AGENT,
    }
    if content_type:
        headers["Content-Type"] = "application/json"
    if session_id is not None:
        if not _SESSION_ID.fullmatch(session_id):
            raise Jin10McpError("PROTOCOL_ERROR")
        headers["Mcp-Session-Id"] = session_id
    return headers


def _read_response(
    response: object,
    *,
    allow_empty: bool,
    expected_id: int | None,
) -> tuple[Mapping[str, object] | None, str | None]:
    with response:
        if str(response.geturl()) != JIN10_MCP_URL:
            raise Jin10McpError("PROTOCOL_ERROR")
        status = int(getattr(response, "status", 200))
        if status not in {200, 202, 204}:
            raise Jin10McpError(_http_reason(status))
        headers = response.headers
        content_encoding = str(_header(headers, "Content-Encoding") or "identity")
        if content_encoding.lower() not in {"", "identity"}:
            raise Jin10McpError("PROTOCOL_ERROR")
        raw_length = _header(headers, "Content-Length")
        if raw_length not in (None, ""):
            try:
                declared_length = int(raw_length)
            except (TypeError, ValueError):
                raise Jin10McpError("PROTOCOL_ERROR") from None
            if declared_length < 0 or declared_length > MAXIMUM_JIN10_RESPONSE_BYTES:
                raise Jin10McpError("RESPONSE_TOO_LARGE")
        raw_session = _header(headers, "Mcp-Session-Id")
        session_id = None if raw_session in (None, "") else str(raw_session)
        if session_id is not None and not _SESSION_ID.fullmatch(session_id):
            raise Jin10McpError("PROTOCOL_ERROR")
        content_type = str(_header(headers, "Content-Type") or "").lower()
        media_type = content_type.split(";", 1)[0].strip()
        if media_type == "text/event-stream":
            payload = _read_sse_response(response, expected_id=expected_id)
            return payload, session_id
        body = response.read(MAXIMUM_JIN10_RESPONSE_BYTES + 1)
        if not isinstance(body, bytes):
            raise Jin10McpError("PROTOCOL_ERROR")
        if len(body) > MAXIMUM_JIN10_RESPONSE_BYTES:
            raise Jin10McpError("RESPONSE_TOO_LARGE")
        if not body:
            if allow_empty or status in {202, 204}:
                return None, session_id
            raise Jin10McpError("PROTOCOL_ERROR")
        if media_type == "application/json":
            payload = _decode_json(body)
        else:
            raise Jin10McpError("PROTOCOL_ERROR")
        return payload, session_id


def _decode_json(body: bytes) -> Mapping[str, object]:
    payload: object | None = None
    invalid = False
    try:
        payload = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        invalid = True
    if invalid:
        # Raise after leaving the parser exception handler so response text is
        # not retained through JSONDecodeError.__context__.
        raise Jin10McpError("BAD_JSON") from None
    if not isinstance(payload, Mapping):
        raise Jin10McpError("BAD_JSON")
    return dict(payload)


def _read_sse_response(
    response: object,
    *,
    expected_id: int | None,
) -> Mapping[str, object]:
    """Read only through the first matching SSE response event.

    Streamable HTTP responses may stay open after the JSON-RPC reply.  Reading
    to EOF would therefore hang a provider poll.  Notifications are ignored;
    the first response carrying the request id is returned under strict byte
    and event budgets.
    """

    readline = getattr(response, "readline", None)
    if not callable(readline):
        raise Jin10McpError("PROTOCOL_ERROR")
    total = 0
    event_count = 0
    data_lines: list[str] = []
    while total <= MAXIMUM_JIN10_RESPONSE_BYTES and event_count < 100:
        raw_line = readline(MAXIMUM_JIN10_RESPONSE_BYTES - total + 1)
        if not isinstance(raw_line, bytes):
            raise Jin10McpError("PROTOCOL_ERROR")
        total += len(raw_line)
        if total > MAXIMUM_JIN10_RESPONSE_BYTES:
            raise Jin10McpError("RESPONSE_TOO_LARGE")
        if not raw_line:
            break
        try:
            line = raw_line.decode("utf-8").rstrip("\r\n")
        except UnicodeDecodeError:
            raise Jin10McpError("BAD_JSON") from None
        if line.startswith("data:"):
            data_lines.append(line[5:].lstrip())
            continue
        if line != "" or not data_lines:
            continue
        event_count += 1
        data = "\n".join(data_lines)
        data_lines.clear()
        parsed = _decode_json(data.encode("utf-8"))
        message_id = parsed.get("id")
        if expected_id is None or message_id == expected_id:
            return dict(parsed)
    raise Jin10McpError("STREAM_RESPONSE_UNAVAILABLE")


def _header(headers: object, name: str) -> object | None:
    getter = getattr(headers, "get", None)
    if callable(getter):
        value = getter(name)
        if value is not None:
            return value
    wanted = name.lower()
    if isinstance(headers, Mapping):
        for key, value in headers.items():
            if str(key).lower() == wanted:
                return value
    return None


def _http_reason(status: object) -> str:
    try:
        code = int(status)
    except (TypeError, ValueError):
        return "TRANSPORT_ERROR"
    if code in {401, 403}:
        return "AUTHENTICATION_FAILED"
    if code == 429:
        return "RATE_LIMITED"
    if 500 <= code <= 599:
        return "REMOTE_UNAVAILABLE"
    return "HTTP_ERROR"


def _tool_arguments(
    descriptor: Mapping[str, object],
    *,
    limit: int,
) -> dict[str, object] | None:
    schema = descriptor.get("inputSchema")
    if not isinstance(schema, Mapping):
        return None
    if schema.get("type") not in (None, "object"):
        return None
    raw_required = schema.get("required", ())
    if not isinstance(raw_required, Sequence) or isinstance(
        raw_required, (str, bytes, bytearray)
    ):
        return None
    required = {str(item) for item in raw_required}
    if required - {"limit"}:
        return None
    properties = schema.get("properties", {})
    if not isinstance(properties, Mapping):
        return None
    arguments: dict[str, object] = {}
    if "limit" in properties:
        limit_schema = properties["limit"]
        if not isinstance(limit_schema, Mapping) or limit_schema.get("type") not in {
            None,
            "integer",
            "number",
        }:
            return None
        arguments["limit"] = min(limit, 50)
    elif "limit" in required:
        return None
    return arguments


def _empty_object_arguments(
    descriptor: Mapping[str, object],
    *,
    arguments: Mapping[str, object],
) -> bool:
    schema = descriptor.get("inputSchema")
    if not isinstance(schema, Mapping) or schema.get("type") not in (None, "object"):
        return False
    raw_required = schema.get("required", ())
    if not isinstance(raw_required, Sequence) or isinstance(
        raw_required, (str, bytes, bytearray)
    ):
        return False
    if tuple(raw_required):
        return False
    properties = schema.get("properties", {})
    if properties is not None and not isinstance(properties, Mapping):
        return False
    return not arguments


def _tool_payload(result: Mapping[str, object]) -> Mapping[str, object]:
    if result.get("isError") is True:
        raise Jin10McpError("REMOTE_TOOL_ERROR")
    structured = result.get("structuredContent")
    if isinstance(structured, Mapping):
        return dict(structured)
    content = result.get("content")
    if not isinstance(content, Sequence) or isinstance(
        content, (str, bytes, bytearray)
    ):
        raise Jin10McpError("PROTOCOL_ERROR")
    parsed_rows: list[Mapping[str, object]] = []
    for item in content:
        if not isinstance(item, Mapping) or item.get("type") != "text":
            continue
        text = item.get("text")
        if not isinstance(text, str):
            continue
        parsed = _decode_json(text.encode("utf-8"))
        parsed_rows.append(dict(parsed))
    if len(parsed_rows) != 1:
        raise Jin10McpError("PROTOCOL_ERROR")
    return parsed_rows[0]


__all__ = [
    "JIN10_MCP_PROTOCOL_VERSION",
    "JIN10_MCP_URL",
    "Jin10McpError",
    "Jin10McpCalendarBatch",
    "Jin10McpHttpClient",
    "Jin10McpNewsBatch",
]
