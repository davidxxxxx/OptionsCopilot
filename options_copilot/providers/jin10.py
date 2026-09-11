"""Supplemental Jin10 adapter with a narrow DPAPI and failure boundary."""
from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from typing import Callable, Sequence
from urllib.parse import urlsplit

from options_copilot.security.dpapi import DPAPISecretStore

from .entity_linking import link_news_entity
from .events import JsonTransport, NewsEvent, _event_id, _symbol
from .jin10_mcp import Jin10McpError, Jin10McpNewsBatch


class Jin10EventProvider:
    """Read-only supplemental discovery; events can never carry trade authority."""

    SECRET_NAME = "JIN10_MCP_TOKEN"

    def __init__(
        self,
        secrets: DPAPISecretStore | None,
        *,
        transport: JsonTransport | None = None,
        mcp_client: object | None = None,
        now: Callable[[], datetime] | None = None,
        timeout_seconds: float = 8.0,
    ) -> None:
        if transport is not None and mcp_client is not None:
            raise ValueError("transport and mcp_client are mutually exclusive")
        self._secrets = secrets
        self._transport = transport
        self._mcp_client = mcp_client
        self._now = now or (lambda: datetime.now(timezone.utc))
        self._timeout = timeout_seconds
        self.transport_verified = (
            mcp_client is not None
            and getattr(mcp_client, "transport_verified", False) is True
        )
        self._retry_not_before: datetime | None = None
        self.health = "DISABLED"
        self.health_reason: str | None = "not_fetched"

    def news(self, symbols: Sequence[str], *, limit: int = 50) -> tuple[NewsEvent, ...]:
        if not symbols or not 1 <= limit <= 200:
            raise ValueError("symbols and a limit between 1 and 200 are required")
        tickers = tuple(_symbol(symbol) for symbol in symbols)
        token = (
            self._secrets.get(self.SECRET_NAME)
            if self._secrets is not None
            else None
        )
        if not token and self._mcp_client is not None:
            self.health, self.health_reason = "DOWN", "credential_not_activated"
            return ()
        if not token or (self._transport is None and self._mcp_client is None):
            self.health, self.health_reason = "DISABLED", "secret_or_transport_unavailable"
            return ()
        if self._mcp_client is not None:
            return self._mcp_news(token, symbols=tickers, limit=limit)
        try:
            payload = self._transport(
                "https://api.jin10.com/config/global",
                headers={"Accept": "application/json", "Authorization": f"Bearer {token}"},
                timeout_seconds=self._timeout,
            )
        except TimeoutError:
            self.health, self.health_reason = "TIMEOUT", "request_timeout"
            return ()
        except Exception as exc:
            text = str(exc).lower()
            self.health, self.health_reason = ("RATE_LIMITED", "rate_limited") if "429" in text else ("DEGRADED", "request_failed")
            return ()
        rows = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(rows, list):
            self.health, self.health_reason = "BAD_JSON", "data_not_list"
            return ()
        observed = self._now()
        events: list[NewsEvent] = []
        malformed = False
        for item in rows[:limit]:
            if not isinstance(item, dict):
                malformed = True
                continue
            title = str(item.get("title") or item.get("content") or "").strip()
            raw_time = item.get("published_at") or item.get("time") or item.get("datetime")
            published = _parse_jin10_time(raw_time)
            if not title or published is None:
                malformed = True
                continue
            summary = str(item.get("summary") or item.get("content") or "")
            link = link_news_entity(
                title,
                summary,
                allowed_symbols=tickers,
                source_symbols=(
                    item.get("symbol"),
                    item.get("ticker"),
                    item.get("symbols"),
                    item.get("tickers"),
                ),
            )
            source_id = str(item.get("id") or _event_id("jin10", title, published.isoformat()))
            events.append(NewsEvent(
                event_id=_event_id("jin10", source_id), symbol=link.symbol, source="Jin10", headline=title[:500],
                summary=summary[:2000], url=str(item.get("url") or "")[:2000],
                published_at=published, first_seen_at=observed, ingested_at=observed, observed_at=observed,
                source_rank=9, source_id=source_id,
                provenance=("Jin10", *link.provenance),
            ))
        if malformed and not events:
            self.health, self.health_reason = "MISSING_FIELDS", "required_fields_absent"
            return ()
        self.health, self.health_reason = "READY", None
        return tuple(sorted(events, key=lambda event: event.published_at, reverse=True))

    def _mcp_news(
        self,
        token: str,
        *,
        symbols: Sequence[str],
        limit: int,
    ) -> tuple[NewsEvent, ...]:
        observed = self._now()
        if (
            self._retry_not_before is not None
            and observed < self._retry_not_before
        ):
            self.health, self.health_reason = "DEGRADED", "cooldown_active"
            return ()
        if not self.transport_verified:
            self.health, self.health_reason = "DISABLED", "transport_unverified"
            return ()
        reader = getattr(self._mcp_client, "fetch_news", None)
        if not callable(reader):
            self.health, self.health_reason = "DISABLED", "transport_unavailable"
            return ()
        try:
            batch = reader(token, limit=limit)
        except Jin10McpError as exc:
            self.health, self.health_reason = _mcp_failure_health(exc.reason)
            if exc.reason == "RATE_LIMITED":
                self._retry_not_before = observed + timedelta(minutes=5)
            return ()
        except Exception:
            self.health, self.health_reason = "DOWN", "request_failed"
            return ()
        if not isinstance(batch, Jin10McpNewsBatch):
            self.health, self.health_reason = "DOWN", "bad_json"
            return ()

        self._retry_not_before = None
        events: list[NewsEvent] = []
        malformed = False
        trusted_tool_result = False
        seen: set[tuple[str, str]] = set()
        for tool_name in ("list_flash", "list_news"):
            payload = batch.payloads.get(tool_name)
            if payload is None:
                continue
            rows = _mcp_items(payload)
            if rows is None:
                malformed = True
                continue
            if not rows:
                trusted_tool_result = True
            for item in rows:
                event = _mcp_event(
                    tool_name,
                    item,
                    observed=observed,
                    allowed_symbols=symbols,
                )
                if event is None:
                    malformed = True
                    continue
                trusted_tool_result = True
                identity = (tool_name, event.source_id or event.event_id)
                if identity in seen:
                    continue
                seen.add(identity)
                events.append(event)

        if batch.failures:
            if trusted_tool_result:
                self.health, self.health_reason = "DEGRADED", "partial_tool_failure"
            else:
                self.health, self.health_reason = "DOWN", "no_usable_records"
            if any(item.endswith(":RATE_LIMITED") for item in batch.failures):
                self._retry_not_before = observed + timedelta(minutes=5)
        elif malformed:
            if trusted_tool_result:
                self.health, self.health_reason = "DEGRADED", "partial_parse"
            else:
                self.health, self.health_reason = "DOWN", "invalid_records"
        elif not trusted_tool_result:
            self.health, self.health_reason = "DOWN", "no_usable_records"
        else:
            self.health, self.health_reason = "READY", None
        return tuple(
            sorted(events, key=lambda event: event.published_at, reverse=True)[:limit]
        )


def _parse_jin10_time(value: object) -> datetime | None:
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(value, tz=timezone.utc)
        except (ValueError, OverflowError, OSError):
            return None
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            return None
    return None


def _mcp_failure_health(reason: str) -> tuple[str, str]:
    normalized = str(reason or "").strip().upper()
    if normalized == "REQUEST_TIMEOUT":
        return "DEGRADED", "request_timeout"
    if normalized == "RATE_LIMITED":
        return "DEGRADED", "rate_limited"
    if normalized in {"AUTHENTICATION_FAILED", "AUTHENTICATION_UNAVAILABLE"}:
        return "DOWN", "authentication_failed"
    if normalized == "BAD_JSON":
        return "DOWN", "bad_json"
    return "DOWN", "request_failed"


def _mcp_items(payload: Mapping[str, object]) -> list[Mapping[str, object]] | None:
    status = payload.get("status")
    if status not in (200, "200"):
        return None
    data = payload.get("data")
    if not isinstance(data, Mapping):
        return None
    raw_items = data.get("items")
    if not isinstance(raw_items, list):
        return None
    if any(not isinstance(item, Mapping) for item in raw_items):
        return None
    return [dict(item) for item in raw_items]


def _mcp_event(
    tool_name: str,
    item: Mapping[str, object],
    *,
    observed: datetime,
    allowed_symbols: Sequence[str],
) -> NewsEvent | None:
    source_id = str(item.get("id") or "").strip()
    published = _parse_jin10_time(item.get("time"))
    if tool_name == "list_flash":
        content = str(item.get("content") or "").strip()
        title = str(item.get("title") or "").strip() or content
        summary = content
        # Jin10's live ``list_flash`` records do not carry an ``id``.  Bind
        # their durable identity to immutable source content and publication
        # time instead of discarding the complete flash feed as malformed.
        if not source_id and title and published is not None:
            source_id = _event_id(
                "jin10-flash",
                title,
                published.isoformat(),
            )
    elif tool_name == "list_news":
        title = str(item.get("title") or "").strip()
        summary = str(item.get("introduction") or "").strip()
    else:
        return None
    if not source_id or not title or published is None:
        return None
    url = _jin10_url(item.get("url"))
    link = link_news_entity(
        title,
        summary,
        allowed_symbols=allowed_symbols,
        source_symbols=(
            item.get("symbol"),
            item.get("ticker"),
            item.get("symbols"),
            item.get("tickers"),
        ),
    )
    return NewsEvent(
        event_id=_event_id("jin10-mcp", tool_name, source_id),
        symbol=link.symbol,
        source="Jin10",
        headline=title[:500],
        summary=summary[:2000],
        url=url,
        published_at=published,
        first_seen_at=observed,
        ingested_at=observed,
        observed_at=observed,
        source_rank=9,
        source_id=source_id,
        provenance=("Jin10 MCP", tool_name, *link.provenance),
    )


def _jin10_url(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    parsed = urlsplit(text)
    host = (parsed.hostname or "").lower()
    if parsed.scheme != "https" or not (
        host == "jin10.com" or host.endswith(".jin10.com")
    ):
        return ""
    return text[:2000]


__all__ = ["Jin10EventProvider"]
