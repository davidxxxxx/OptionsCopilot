"""Small bounded HTTPS client: fixed hosts, timeouts, size caps, no cross-host redirects."""
from __future__ import annotations

import json
import zlib
from collections.abc import Iterable, Mapping
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

DEFAULT_MAX_BYTES = 4 * 1024 * 1024
USER_AGENT = "OptionsCopilot/2.0 (local research tool)"


class HttpError(RuntimeError):
    """A transport failure whose message never includes request headers or keys."""

    def __init__(self, reason: str, status: int | None = None, message: str = "") -> None:
        text = reason if status is None else f"{reason} (HTTP {status})"
        super().__init__(f"{text}: {message}" if message else text)
        self.reason = reason
        self.status = status
        self.message = message


class _AllowlistRedirect(HTTPRedirectHandler):
    """Follow redirects only to HTTPS hosts on the request's allowlist."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: ANN001
        target = urlsplit(newurl)
        allowed = getattr(req, "allowed_hosts", {urlsplit(req.full_url).hostname})
        if target.scheme != "https" or target.hostname not in allowed:
            raise HTTPError(req.full_url, code, "redirect to a non-allowlisted host refused", headers, fp)
        redirected = super().redirect_request(req, fp, code, msg, headers, newurl)
        if redirected is not None:
            redirected.allowed_hosts = allowed
        return redirected


_OPENER = build_opener(_AllowlistRedirect)


def request_json(
    method: str,
    url: str,
    *,
    allowed_hosts: Iterable[str],
    params: Mapping[str, Any] | None = None,
    headers: Mapping[str, str] | None = None,
    body: Any = None,
    timeout: float = 15.0,
    max_bytes: int = DEFAULT_MAX_BYTES,
) -> Any:
    """Send one HTTPS request and decode a JSON response."""

    parts = urlsplit(url)
    if parts.scheme != "https" or parts.hostname not in set(allowed_hosts):
        raise HttpError("URL_NOT_ALLOWED")
    if params:
        url = f"{url}?{urlencode({k: v for k, v in params.items() if v is not None})}"
    data = None
    merged = {"User-Agent": USER_AGENT, "Accept": "application/json", "Accept-Encoding": "gzip"}
    merged.update(headers or {})
    if body is not None:
        data = json.dumps(body, ensure_ascii=False).encode("utf-8")
        merged["Content-Type"] = "application/json"
    request = Request(url, data=data, headers=merged, method=method)
    request.allowed_hosts = set(allowed_hosts)
    try:
        with _OPENER.open(request, timeout=timeout) as response:
            encoding = (response.headers.get("Content-Encoding") or "").lower()
            raw = response.read(max_bytes + 1)
    except HTTPError as exc:
        raise HttpError("HTTP_ERROR", exc.code, _error_message(exc, [
            value for name, value in merged.items()
            if name.lower() in ("authorization", "x-finnhub-token")
        ])) from None
    except TimeoutError:
        raise HttpError("TIMEOUT") from None
    except (URLError, OSError) as exc:
        reason = getattr(exc, "reason", None)
        if isinstance(reason, TimeoutError) or "timed out" in str(reason or exc):
            raise HttpError("TIMEOUT") from None
        raise HttpError("CONNECT_ERROR") from None
    if len(raw) > max_bytes:
        raise HttpError("RESPONSE_TOO_LARGE")
    if encoding == "gzip":
        try:
            # Cap the decompressed size too, not just the bytes on the wire.
            raw = zlib.decompressobj(16 + zlib.MAX_WBITS).decompress(raw, max_bytes + 1)
        except zlib.error:
            raise HttpError("BAD_GZIP") from None
        if len(raw) > max_bytes:
            raise HttpError("RESPONSE_TOO_LARGE")
    try:
        return json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise HttpError("BAD_JSON") from None


def _error_message(exc: HTTPError, secrets: Iterable[str]) -> str:
    """Short provider error text (e.g. "Insufficient Balance") with header values scrubbed."""

    try:
        raw = exc.read(4096) if exc.fp is not None else b""
        payload = json.loads(raw.decode("utf-8", "replace")) if raw else None
    except Exception:
        return ""
    message: Any = payload
    if isinstance(payload, dict):
        error = payload.get("error")
        message = (
            error.get("message") if isinstance(error, dict) else error
        ) or payload.get("message") or ""
    text = " ".join(str(message or "").split())[:160]
    for secret in secrets:
        if secret and len(secret) >= 8:
            text = text.replace(secret, "***")
            text = text.replace(secret.split()[-1], "***")
    return text


__all__ = ["HttpError", "USER_AGENT", "request_json"]
