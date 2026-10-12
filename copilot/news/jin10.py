"""Jin10 flash news over the MCP streamable-HTTP transport (read-only)."""
from __future__ import annotations

import json
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, build_opener
from zoneinfo import ZoneInfo

from copilot.http import USER_AGENT
from .models import NewsItem, stable_id

JIN10_MCP_URL = "https://mcp.jin10.com/mcp"
PROTOCOL_VERSION = "2025-03-26"
MAX_RESPONSE_BYTES = 4 * 1024 * 1024
MAX_PAGES = 60
BEIJING = ZoneInfo("Asia/Shanghai")


class Jin10Error(RuntimeError):
    """Fixed-code failure; never carries the token or response text."""


class Jin10Client:
    def __init__(
        self,
        token: str,
        *,
        timeout: float = 20.0,
        opener: Callable[..., Any] | None = None,
    ) -> None:
        if not token or not token.strip():
            raise Jin10Error("TOKEN_MISSING")
        self._token = token.strip()
        self._timeout = timeout
        self._open = opener or build_opener().open
        self._next_id = 0

    def fetch_flash(
        self, max_items: int = 200, *, stop_before: datetime | None = None
    ) -> list[NewsItem]:
        """Newest-first flash items; paging stops once items reach ``stop_before``."""

        session_id: str | None = None
        try:
            result, session_id = self._call("initialize", {
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": {"name": "options-copilot", "version": "2.0"},
            }, None)
            self._notify("notifications/initialized", session_id)
            items: list[dict[str, Any]] = []
            cursor: str | None = None
            seen: set[str] = set()
            for _page in range(MAX_PAGES):
                arguments = {} if cursor is None else {"cursor": cursor}
                result, _ = self._call(
                    "tools/call", {"name": "list_flash", "arguments": arguments}, session_id
                )
                page = _tool_payload(result)
                data = page.get("data") if isinstance(page, dict) else None
                if not isinstance(data, dict) or not isinstance(data.get("items"), list):
                    raise Jin10Error("UNEXPECTED_PAYLOAD")
                rows = [row for row in data["items"] if isinstance(row, dict)]
                items.extend(rows)
                cursor = data.get("next_cursor") if data.get("has_more") else None
                if len(items) >= max_items or not cursor or cursor in seen:
                    break
                if stop_before is not None and _reached(rows, stop_before):
                    break
                seen.add(cursor)
            return [news for row in items[:max_items] if (news := flash_to_news(row))]
        finally:
            if session_id:
                self._delete(session_id)

    def _call(
        self, method: str, params: dict[str, Any], session_id: str | None
    ) -> tuple[dict[str, Any], str | None]:
        self._next_id += 1
        request_id = self._next_id
        reply, returned_session = self._post(
            {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params},
            session_id,
            request_id,
        )
        if not isinstance(reply, dict) or reply.get("id") != request_id:
            raise Jin10Error("PROTOCOL_ERROR")
        if reply.get("error") is not None:
            raise Jin10Error("REMOTE_ERROR")
        result = reply.get("result")
        if not isinstance(result, dict):
            raise Jin10Error("PROTOCOL_ERROR")
        return result, returned_session or session_id

    def _notify(self, method: str, session_id: str | None) -> None:
        self._post({"jsonrpc": "2.0", "method": method, "params": {}}, session_id, None)

    def _headers(self, session_id: str | None) -> dict[str, str]:
        headers = {
            "Accept": "application/json, text/event-stream",
            "Accept-Encoding": "identity",
            "Authorization": f"Bearer {self._token}",
            "MCP-Protocol-Version": PROTOCOL_VERSION,
            "User-Agent": USER_AGENT,
        }
        if session_id:
            headers["Mcp-Session-Id"] = session_id
        return headers

    def _post(
        self, payload: dict[str, Any], session_id: str | None, expected_id: int | None
    ) -> tuple[Any, str | None]:
        headers = self._headers(session_id)
        headers["Content-Type"] = "application/json"
        request = Request(
            JIN10_MCP_URL,
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers=headers,
            method="POST",
        )
        try:
            response = self._open(request, timeout=self._timeout)
        except HTTPError as exc:
            code = "AUTH_FAILED" if exc.code in (401, 403) else (
                "RATE_LIMITED" if exc.code == 429 else f"HTTP_{exc.code}"
            )
            raise Jin10Error(code) from None
        except (URLError, OSError, TimeoutError):
            raise Jin10Error("CONNECT_ERROR") from None
        with response:
            returned = response.headers.get("Mcp-Session-Id") or None
            content_type = (response.headers.get("Content-Type") or "").split(";")[0].strip()
            if content_type == "text/event-stream":
                return _read_sse(response, expected_id), returned
            raw = response.read(MAX_RESPONSE_BYTES + 1)
        if len(raw) > MAX_RESPONSE_BYTES:
            raise Jin10Error("RESPONSE_TOO_LARGE")
        if not raw:
            if expected_id is None:
                return None, returned
            raise Jin10Error("EMPTY_RESPONSE")
        return _decode(raw), returned

    def _delete(self, session_id: str) -> None:
        request = Request(JIN10_MCP_URL, headers=self._headers(session_id), method="DELETE")
        try:
            with self._open(request, timeout=min(self._timeout, 5.0)) as response:
                response.read(1024)
        except Exception:  # Session cleanup is best effort; the server expires sessions.
            pass


def flash_to_news(row: dict[str, Any]) -> NewsItem | None:
    content = str(row.get("content") or "").strip()
    title = str(row.get("title") or "").strip()
    published = parse_jin10_time(row.get("time"))
    if not (content or title) or published is None:
        return None
    source_id = str(row.get("id") or "").strip() or stable_id(
        title or content, published.isoformat()
    )
    important = row.get("important")
    return NewsItem(
        source="jin10",
        source_id=source_id,
        published_at=published,
        title=(title or content)[:300],
        body=content[:2000] if title else "",
        url=str(row.get("url") or "")[:500],
        importance=1 if important in (1, True, "1") else 0,
    )


def _reached(rows: list[dict[str, Any]], stop_before: datetime) -> bool:
    times = [t for row in rows if (t := parse_jin10_time(row.get("time"))) is not None]
    return bool(times) and min(times) <= stop_before


def parse_jin10_time(value: Any) -> datetime | None:
    """Jin10 timestamps without an offset are Beijing time."""

    if isinstance(value, (int, float)) and not isinstance(value, bool):
        seconds = value / 1000 if value > 1e11 else value
        return datetime.fromtimestamp(seconds, tz=timezone.utc)
    if isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError:
            return None
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=BEIJING)
    return None


def _tool_payload(result: dict[str, Any]) -> Any:
    if result.get("isError") is True:
        raise Jin10Error("TOOL_ERROR")
    structured = result.get("structuredContent")
    if isinstance(structured, dict):
        return structured
    for block in result.get("content") or ():
        if isinstance(block, dict) and block.get("type") == "text":
            return _decode(str(block.get("text") or "").encode("utf-8"))
    raise Jin10Error("UNEXPECTED_PAYLOAD")


def _read_sse(response: Any, expected_id: int | None) -> Any:
    total = 0
    data_lines: list[str] = []
    while total <= MAX_RESPONSE_BYTES:
        raw_line = response.readline(MAX_RESPONSE_BYTES)
        if not raw_line:
            break
        total += len(raw_line)
        line = raw_line.decode("utf-8", "replace").rstrip("\r\n")
        if line.startswith("data:"):
            data_lines.append(line[5:].lstrip())
            continue
        if line or not data_lines:
            continue
        message = _decode("\n".join(data_lines).encode("utf-8"))
        data_lines.clear()
        if expected_id is None or (isinstance(message, dict) and message.get("id") == expected_id):
            return message
    if data_lines:
        return _decode("\n".join(data_lines).encode("utf-8"))
    raise Jin10Error("STREAM_ENDED")


def _decode(raw: bytes) -> Any:
    try:
        return json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise Jin10Error("BAD_JSON") from None


__all__ = ["JIN10_MCP_URL", "Jin10Client", "Jin10Error", "flash_to_news", "parse_jin10_time"]
