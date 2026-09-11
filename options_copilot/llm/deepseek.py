"""Minimal direct DeepSeek client with injected transport and strict telemetry."""
from __future__ import annotations

import hashlib
import json
import math
import re
import socket
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Protocol

from options_copilot.llm.cost_meter import (
    PRO,
    CostMeter,
    CostPolicy,
    CostRejection,
    MODELS,
    calculate_cost_usd,
)
from options_copilot.llm.model_snapshot import (
    ModelSnapshotPrivacyError,
    assert_model_snapshot_safe,
)
from options_copilot.llm.redaction import redact_for_model
from options_copilot.providers.transport import (
    BoundedTransportError,
    decode_strict_json_object,
    read_bounded_response,
    remaining_timeout,
)


DEEPSEEK_URL = "https://api.deepseek.com/chat/completions"
DEEPSEEK_WHOLE_CYCLE_SECONDS = 6.5
MAXIMUM_DEEPSEEK_RESPONSE_BYTES = 65_536
MAXIMUM_DEEPSEEK_REQUEST_BYTES = 65_536
MAXIMUM_DEEPSEEK_HTTP_ATTEMPTS = 2
DEFAULT_MAX_COMPLETION_TOKENS = 600
DEFAULT_MAX_REQUEST_BYTES = 65_536
_DEEPSEEK_BASE_URL = "https://api.deepseek.com"
_RETRY_BACKOFF_SECONDS = 0.1
_ALLOWED_RESPONSE_CHARSETS = frozenset({"ascii", "us-ascii", "utf-8", "utf8"})
_RETRYABLE_FAILURES = frozenset(
    {
        "BAD_JSON",
        "CONNECT_ERROR",
        "EMPTY_RESPONSE",
        "INCOMPLETE_FINISH",
        "RATE_LIMITED",
        "REMOTE_UNAVAILABLE",
        "REQUEST_TIMEOUT",
        "TLS_ERROR",
    }
)
_REASON_CODE = re.compile(r"^[A-Z][A-Z0-9_]{0,63}$")
_FORBIDDEN_DEEPSEEK_KEY_PREFIXES = (
    "account",
    "approval",
    "broker",
    "creator",
    "credential",
    "instruction",
    "order",
    "secret",
    "token",
)
_FORBIDDEN_DEEPSEEK_KEYS = frozenset(
    {
        "file",
        "filepath",
        "function",
        "functions",
        "path",
        "position",
        "positions",
        "tool",
        "toolchoice",
        "tools",
    }
)


@dataclass(frozen=True, slots=True)
class HttpResponse:
    status: int
    body: bytes = b""
    body_chunks: tuple[bytes, ...] = ()


class HttpTransport(Protocol):
    def post(
        self,
        *,
        url: str,
        headers: Mapping[str, str],
        body: str,
        timeout_seconds: float,
        stream: bool,
    ) -> HttpResponse: ...


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: ANN001
        return None


class UrllibTransport:
    """Exact, bounded POST transport for the direct DeepSeek endpoint."""

    def __init__(self, *, opener: object | None = None) -> None:
        self._opener = opener or urllib.request.build_opener(
            urllib.request.ProxyHandler({}),
            _NoRedirect(),
        )

    def post(
        self,
        *,
        url: str,
        headers: Mapping[str, str],
        body: str,
        timeout_seconds: float,
        stream: bool,
    ) -> HttpResponse:
        checked_url = _deepseek_url(url)
        checked_headers = _deepseek_headers(headers)
        timeout = _attempt_timeout(timeout_seconds)
        if stream is not False:
            raise DeepSeekTransportError("STREAMING_FORBIDDEN")
        if not isinstance(body, str):
            raise DeepSeekTransportError("REQUEST_BODY_INVALID")
        body_invalid = False
        try:
            encoded_body = body.encode("utf-8", errors="strict")
        except UnicodeError:
            body_invalid = True
            encoded_body = b""
        if body_invalid:
            raise DeepSeekTransportError("REQUEST_BODY_INVALID")
        if len(encoded_body) > MAXIMUM_DEEPSEEK_REQUEST_BYTES:
            raise DeepSeekTransportError("REQUEST_TOO_LARGE")
        request = urllib.request.Request(
            checked_url,
            data=encoded_body,
            headers=checked_headers,
            method="POST",
        )
        response, failure_reason = _open_deepseek_response(
            self._opener,
            request,
            timeout=timeout,
        )
        if failure_reason is not None:
            raise DeepSeekTransportError(failure_reason)
        if response is None:
            raise DeepSeekTransportError("CONNECT_ERROR")
        return HttpResponse(status=200, body=_read_deepseek_response(response))


def _deepseek_url(value: object) -> str:
    if not isinstance(value, str):
        raise DeepSeekTransportError("URL_NOT_ALLOWED")
    invalid = False
    try:
        parsed = urllib.parse.urlsplit(value)
        port = parsed.port
    except (TypeError, ValueError):
        invalid = True
        parsed = None
        port = None
    if not invalid and parsed is not None:
        invalid = (
            value != DEEPSEEK_URL
            or parsed.scheme != "https"
            or parsed.hostname != "api.deepseek.com"
            or parsed.netloc != "api.deepseek.com"
            or parsed.username is not None
            or parsed.password is not None
            or port is not None
            or parsed.path != "/chat/completions"
            or bool(parsed.query)
            or bool(parsed.fragment)
        )
    if invalid:
        raise DeepSeekTransportError("URL_NOT_ALLOWED")
    return DEEPSEEK_URL


def _deepseek_headers(headers: Mapping[str, str]) -> dict[str, str]:
    if not isinstance(headers, Mapping):
        raise DeepSeekTransportError("REQUEST_HEADERS_INVALID")
    normalized: dict[str, str] = {}
    for raw_name, raw_value in headers.items():
        name = str(raw_name).strip().lower()
        if name in normalized:
            raise DeepSeekTransportError("REQUEST_HEADERS_INVALID")
        if not isinstance(raw_value, str):
            raise DeepSeekTransportError("REQUEST_HEADERS_INVALID")
        normalized[name] = raw_value
    required = {
        "accept",
        "accept-encoding",
        "authorization",
        "content-type",
    }
    if set(normalized) != required:
        raise DeepSeekTransportError("REQUEST_HEADERS_INVALID")
    authorization = normalized["authorization"]
    if (
        not authorization.startswith("Bearer ")
        or not authorization[7:]
        or any(not 33 <= ord(character) < 127 for character in authorization[7:])
    ):
        raise DeepSeekTransportError("REQUEST_HEADERS_INVALID")
    if (
        normalized["accept"] != "application/json"
        or normalized["accept-encoding"] != "identity"
        or normalized["content-type"] != "application/json"
    ):
        raise DeepSeekTransportError("REQUEST_HEADERS_INVALID")
    return {
        "Authorization": authorization,
        "Accept": "application/json",
        "Accept-Encoding": "identity",
        "Content-Type": "application/json",
    }


def _attempt_timeout(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("timeout_seconds must be numeric")
    timeout = float(value)
    if not math.isfinite(timeout) or not 0 < timeout <= DEEPSEEK_WHOLE_CYCLE_SECONDS:
        raise ValueError(
            f"timeout_seconds must be between 0 and {DEEPSEEK_WHOLE_CYCLE_SECONDS}"
        )
    return timeout


def _open_deepseek_response(
    opener: object,
    request: urllib.request.Request,
    *,
    timeout: float,
) -> tuple[object | None, str | None]:
    try:
        response = opener.open(request, timeout=timeout)  # type: ignore[attr-defined]
    except urllib.error.HTTPError as error:
        reason = _http_failure_reason(getattr(error, "code", None))
        _close_response(error)
        return None, reason
    except urllib.error.URLError as error:
        reason_value = getattr(error, "reason", None)
        if isinstance(reason_value, (TimeoutError, socket.timeout)):
            return None, "REQUEST_TIMEOUT"
        if isinstance(reason_value, ssl.SSLError):
            return None, "TLS_ERROR"
        return None, "CONNECT_ERROR"
    except (TimeoutError, socket.timeout):
        return None, "REQUEST_TIMEOUT"
    except ssl.SSLError:
        return None, "TLS_ERROR"
    except OSError:
        return None, "CONNECT_ERROR"
    except Exception:
        return None, "TRANSPORT_ERROR"
    return response, None


def _read_deepseek_response(response: object) -> bytes:
    failure_reason: str | None = None
    try:
        raw_status = getattr(response, "status", 200)
        if isinstance(raw_status, bool) or not isinstance(raw_status, int):
            failure_reason = "HTTP_ERROR"
        elif raw_status != 200:
            failure_reason = _http_failure_reason(raw_status)
        elif _deepseek_url(response.geturl()) != DEEPSEEK_URL:  # type: ignore[attr-defined]
            failure_reason = "REDIRECT_FORBIDDEN"
        else:
            headers = getattr(response, "headers", None)
            encoding = str(_header(headers, "Content-Encoding") or "identity")
            if encoding.strip().lower() not in {"", "identity"}:
                failure_reason = "ENCODING_INVALID"
            elif not _valid_json_content_type(_header(headers, "Content-Type")):
                failure_reason = "CONTENT_TYPE_INVALID"
    except DeepSeekTransportError:
        failure_reason = "REDIRECT_FORBIDDEN"
    except Exception:
        failure_reason = "RESPONSE_INVALID"
    if failure_reason is not None:
        _close_response(response)
        raise DeepSeekTransportError(failure_reason)

    bounded_reason: str | None = None
    body: bytes | None = None
    try:
        body = read_bounded_response(response, MAXIMUM_DEEPSEEK_RESPONSE_BYTES)
    except BoundedTransportError as error:
        bounded_reason = error.reason
    if bounded_reason is not None:
        raise DeepSeekTransportError(bounded_reason)
    assert body is not None
    return body


def _valid_json_content_type(value: object) -> bool:
    if not isinstance(value, str) or not value.strip():
        return False
    parts = [part.strip().lower() for part in value.split(";")]
    if not parts or parts[0] != "application/json":
        return False
    seen_charset = False
    for parameter in parts[1:]:
        if not parameter or "=" not in parameter:
            return False
        name, raw_value = (item.strip() for item in parameter.split("=", 1))
        charset = raw_value.strip("\"'")
        if name != "charset" or seen_charset or charset not in _ALLOWED_RESPONSE_CHARSETS:
            return False
        seen_charset = True
    return True


def _header(headers: object, name: str) -> object | None:
    getter = getattr(headers, "get", None)
    if callable(getter):
        value = getter(name)
        if value is not None:
            return value
    if isinstance(headers, Mapping):
        wanted = name.lower()
        for key, value in headers.items():
            if str(key).lower() == wanted:
                return value
    return None


def _close_response(response: object) -> None:
    try:
        closer = getattr(response, "close", None)
        if callable(closer):
            closer()
    except Exception:
        return


def _http_failure_reason(status: object) -> str:
    if isinstance(status, bool):
        return "HTTP_ERROR"
    try:
        code = int(status)
    except (TypeError, ValueError):
        return "HTTP_ERROR"
    if 300 <= code <= 399:
        return "REDIRECT_FORBIDDEN"
    if code == 429:
        return "RATE_LIMITED"
    if 500 <= code <= 599:
        return "REMOTE_UNAVAILABLE"
    return "HTTP_ERROR"


@dataclass(frozen=True, slots=True)
class CompletionMetadata:
    model: str
    latency_ms: float
    prompt_tokens: int
    completion_tokens: int
    cache_hit_tokens: int
    cache_miss_tokens: int
    cost_usd: float
    prompt_hash: str
    static_prefix_hash: str
    dynamic_field_count: int
    attempts: int


@dataclass(frozen=True, slots=True)
class DeepSeekResult:
    model_json: dict[str, Any] | None
    metadata: CompletionMetadata
    fallback_reason: str | None = None
    cost_rejection: CostRejection | None = None


class DeepSeekError(RuntimeError):
    """A fixed-code DeepSeek failure with no untrusted detail."""

    def __init__(self, reason: str) -> None:
        checked = str(reason or "").strip().upper()
        if _REASON_CODE.fullmatch(checked) is None:
            checked = "DEEPSEEK_ERROR"
        self.reason = checked
        super().__init__(checked)


class DeepSeekTransportError(DeepSeekError):
    """A fixed-code failure owned by the exact DeepSeek transport."""


class _RetryableError(DeepSeekError):
    """An internal fixed-code failure eligible for one bounded retry."""


class DeepSeekClient:
    def __init__(
        self,
        *,
        api_key: str,
        transport: HttpTransport | None = None,
        cost_meter: CostMeter | None = None,
        audit_logger: Callable[[dict[str, object]], None] | None = None,
        clock: Callable[[], datetime] | None = None,
        monotonic_clock: Callable[[], float] | None = None,
        sleeper: Callable[[float], None] | None = None,
        base_url: str = _DEEPSEEK_BASE_URL,
        timeout_seconds: float = DEEPSEEK_WHOLE_CYCLE_SECONDS,
        max_completion_tokens: int = DEFAULT_MAX_COMPLETION_TOKENS,
        max_request_bytes: int = DEFAULT_MAX_REQUEST_BYTES,
    ) -> None:
        if (
            not isinstance(api_key, str)
            or api_key != api_key.strip()
            or not api_key
            or len(api_key) > 4_096
            or any(not 33 <= ord(character) < 127 for character in api_key)
        ):
            raise ValueError("DeepSeek API key is required")
        if base_url != _DEEPSEEK_BASE_URL:
            raise ValueError("DeepSeek base URL is fixed")
        _deepseek_url(DEEPSEEK_URL)
        self._api_key = api_key
        self._transport = transport or UrllibTransport()
        self._cost_meter = cost_meter or CostMeter(CostPolicy())
        self._audit_logger = audit_logger
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._monotonic_clock = monotonic_clock or time.monotonic
        self._sleeper = sleeper or time.sleep
        self._url = DEEPSEEK_URL
        self._timeout_seconds = _attempt_timeout(timeout_seconds)
        if not callable(self._clock):
            raise TypeError("clock must be callable")
        if not callable(self._monotonic_clock):
            raise TypeError("monotonic_clock must be callable")
        if not callable(self._sleeper):
            raise TypeError("sleeper must be callable")
        if not 1 <= max_completion_tokens <= DEFAULT_MAX_COMPLETION_TOKENS:
            raise ValueError(
                f"max_completion_tokens must be between 1 and {DEFAULT_MAX_COMPLETION_TOKENS}"
            )
        if (
            isinstance(max_request_bytes, bool)
            or not isinstance(max_request_bytes, int)
            or not 1_024 <= max_request_bytes <= MAXIMUM_DEEPSEEK_REQUEST_BYTES
        ):
            raise ValueError(
                f"max_request_bytes must be between 1024 and {MAXIMUM_DEEPSEEK_REQUEST_BYTES}"
            )
        self._max_completion_tokens = max_completion_tokens
        self._max_request_bytes = max_request_bytes

    def complete(
        self,
        *,
        model: str,
        static_prefix: str,
        dynamic_snapshot: Mapping[str, Any],
        stream: bool = False,
        estimated_cost_usd: float = 0.0,
        thinking: bool = False,
    ) -> DeepSeekResult:
        started_monotonic = _monotonic_value(self._monotonic_clock)
        deadline_at = started_monotonic + DEEPSEEK_WHOLE_CYCLE_SECONDS
        if model not in MODELS:
            raise ValueError(f"unsupported DeepSeek model: {model}")
        if not static_prefix.strip() or "json" not in static_prefix.lower():
            raise ValueError("static prefix must explicitly request JSON")
        if stream is not False:
            raise ValueError("DeepSeek completions must be non-streaming")
        if thinking and model != PRO:
            raise ValueError("thinking mode is available only for explicit Pro escalation")
        caller_estimate = float(estimated_cost_usd)
        if not math.isfinite(caller_estimate):
            raise ValueError("estimated cost must be finite")
        if caller_estimate < 0:
            raise ValueError("estimated cost cannot be negative")
        # The direct client rejects private structure before the redactor can
        # remove it. Redaction remains defense in depth, never permission to
        # continue after an unsafe caller snapshot.
        try:
            _assert_deepseek_snapshot_keys(dynamic_snapshot)
            assert_model_snapshot_safe(dynamic_snapshot)
            redacted = redact_for_model(dynamic_snapshot)
            assert_model_snapshot_safe(redacted)
            original_text = json.dumps(
                dynamic_snapshot,
                allow_nan=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            dynamic_text = json.dumps(
                redacted,
                allow_nan=False,
                sort_keys=True,
                separators=(",", ":"),
            )
        except (ModelSnapshotPrivacyError, TypeError, ValueError):
            raise ValueError("DeepSeek snapshot violates privacy boundary") from None
        if dynamic_text != original_text:
            raise ValueError("DeepSeek snapshot violates privacy boundary")
        request_payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": static_prefix},
                {"role": "user", "content": dynamic_text},
            ],
            "response_format": {"type": "json_object"},
            "thinking": {"type": "enabled" if thinking else "disabled"},
            "max_tokens": self._max_completion_tokens,
            "stream": False,
        }
        if not thinking:
            request_payload["temperature"] = 0
        body = json.dumps(
            request_payload,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        body_bytes = body.encode("utf-8")
        if len(body_bytes) > self._max_request_bytes:
            raise ValueError("DeepSeek request byte limit exceeded")
        prompt_hash = _sha256(body)
        static_hash = _sha256(static_prefix)
        field_count = _field_count(redacted)
        prompt_token_upper_bound = max(1, len(body_bytes))
        worst_case_cost = calculate_cost_usd(
            model,
            {
                "prompt_tokens": prompt_token_upper_bound,
                "prompt_cache_miss_tokens": prompt_token_upper_bound,
                "completion_tokens": self._max_completion_tokens,
            },
        )
        estimate = max(caller_estimate, worst_case_cost)
        last_reason = "REQUEST_FAILED"
        attempts_made = 0
        for attempt in range(1, MAXIMUM_DEEPSEEK_HTTP_ATTEMPTS + 1):
            attempt_timeout = _remaining_or_none(
                deadline_at,
                self._monotonic_clock,
                self._timeout_seconds,
            )
            if attempt_timeout is None:
                last_reason = "REQUEST_TIMEOUT"
                break
            reservation, rejection, reservation_failed = _reserve_cost(
                self._cost_meter,
                model,
                estimated_cost_usd=estimate,
            )
            if reservation_failed:
                raise DeepSeekError("COST_ACCOUNTING_FAILED")
            if rejection:
                metadata = CompletionMetadata(
                    model=model,
                    latency_ms=_elapsed_milliseconds(
                        started_monotonic,
                        self._monotonic_clock,
                    ),
                    prompt_tokens=0,
                    completion_tokens=0,
                    cache_hit_tokens=0,
                    cache_miss_tokens=0,
                    cost_usd=0.0,
                    prompt_hash=prompt_hash,
                    static_prefix_hash=static_hash,
                    dynamic_field_count=field_count,
                    attempts=attempts_made,
                )
                self._log(
                    metadata,
                    outcome="fallback",
                    fallback_reason=rejection.reason,
                )
                return DeepSeekResult(
                    None,
                    metadata,
                    rejection.reason,
                    rejection,
                )

            assert reservation is not None
            attempts_made = attempt
            response, failure_reason = _post_once(
                self._transport,
                url=self._url,
                headers={
                    "Authorization": f"Bearer {self._api_key}",
                    "Accept": "application/json",
                    "Accept-Encoding": "identity",
                    "Content-Type": "application/json",
                },
                body=body,
                timeout_seconds=attempt_timeout,
            )
            retryable = failure_reason in _RETRYABLE_FAILURES
            model_json: dict[str, Any] | None = None
            usage: dict[str, int] | None = None
            if failure_reason is None:
                status_reason = _response_status_reason(response)
                if status_reason is not None:
                    failure_reason = status_reason
                    retryable = status_reason in _RETRYABLE_FAILURES
                else:
                    try:
                        assert response is not None
                        model_json, usage = _parse_response(response.body)
                    except _RetryableError as error:
                        failure_reason = error.reason
                        retryable = True
                    except DeepSeekError as error:
                        failure_reason = error.reason
                        retryable = False

            if failure_reason is None:
                if (
                    _remaining_or_none(
                        deadline_at,
                        self._monotonic_clock,
                        self._timeout_seconds,
                    )
                    is None
                ):
                    failure_reason = "REQUEST_TIMEOUT"
                    retryable = False
                else:
                    assert model_json is not None and usage is not None
                    cost, settlement_failed = _settle_cost(
                        self._cost_meter,
                        reservation,
                        usage,
                        now=self._clock(),
                    )
                    if settlement_failed:
                        raise DeepSeekError("COST_ACCOUNTING_FAILED")
                    assert cost is not None
                    metadata = CompletionMetadata(
                        model=model,
                        latency_ms=_elapsed_milliseconds(
                            started_monotonic,
                            self._monotonic_clock,
                        ),
                        prompt_tokens=usage["prompt_tokens"],
                        completion_tokens=usage["completion_tokens"],
                        cache_hit_tokens=usage["prompt_cache_hit_tokens"],
                        cache_miss_tokens=usage["prompt_cache_miss_tokens"],
                        cost_usd=cost,
                        prompt_hash=prompt_hash,
                        static_prefix_hash=static_hash,
                        dynamic_field_count=field_count,
                        attempts=attempt,
                    )
                    self._log(metadata, outcome="success")
                    return DeepSeekResult(model_json, metadata)

            if not _commit_cost_failure(
                self._cost_meter,
                reservation,
                now=self._clock(),
            ):
                raise DeepSeekError("COST_ACCOUNTING_FAILED")
            assert failure_reason is not None
            last_reason = failure_reason
            if not retryable:
                raise DeepSeekError(failure_reason)
            if attempt >= MAXIMUM_DEEPSEEK_HTTP_ATTEMPTS:
                break
            if not _sleep_before_retry(
                deadline_at=deadline_at,
                monotonic_clock=self._monotonic_clock,
                sleeper=self._sleeper,
            ):
                last_reason = "REQUEST_TIMEOUT"
                break
        raise DeepSeekError(last_reason)

    def _log(self, metadata: CompletionMetadata, **extra: object) -> None:
        if self._audit_logger is None:
            return
        self._audit_logger({**asdict(metadata), **extra})


def _assert_deepseek_snapshot_keys(value: object) -> None:
    if isinstance(value, Mapping):
        for raw_key, item in value.items():
            compact = re.sub(r"[^a-z0-9]", "", str(raw_key).casefold())
            if compact in _FORBIDDEN_DEEPSEEK_KEYS or compact.startswith(
                _FORBIDDEN_DEEPSEEK_KEY_PREFIXES
            ):
                raise ValueError("DeepSeek snapshot violates privacy boundary")
            _assert_deepseek_snapshot_keys(item)
        return
    if isinstance(value, Sequence) and not isinstance(
        value,
        (str, bytes, bytearray),
    ):
        for item in value:
            _assert_deepseek_snapshot_keys(item)


def _reserve_cost(
    meter: object,
    model: str,
    *,
    estimated_cost_usd: float,
) -> tuple[object | None, CostRejection | None, bool]:
    try:
        reservation, rejection = meter.reserve(  # type: ignore[attr-defined]
            model,
            estimated_cost_usd=estimated_cost_usd,
        )
    except Exception:
        return None, None, True
    if rejection is not None and not isinstance(rejection, CostRejection):
        return None, None, True
    if (reservation is None) == (rejection is None):
        return None, None, True
    return reservation, rejection, False


def _settle_cost(
    meter: object,
    reservation: object,
    usage: Mapping[str, int],
    *,
    now: datetime,
) -> tuple[float | None, bool]:
    try:
        cost = float(  # type: ignore[attr-defined]
            meter.settle(reservation, usage, now=now)
        )
    except Exception:
        return None, True
    if not math.isfinite(cost) or cost < 0:
        return None, True
    return cost, False


def _commit_cost_failure(
    meter: object,
    reservation: object,
    *,
    now: datetime,
) -> bool:
    try:
        meter.commit_failure(reservation, now=now)  # type: ignore[attr-defined]
    except Exception:
        return False
    return True


def _post_once(
    transport: HttpTransport,
    *,
    url: str,
    headers: Mapping[str, str],
    body: str,
    timeout_seconds: float,
) -> tuple[HttpResponse | None, str | None]:
    try:
        response = transport.post(
            url=url,
            headers=headers,
            body=body,
            timeout_seconds=timeout_seconds,
            stream=False,
        )
    except DeepSeekTransportError as error:
        return None, error.reason
    except BoundedTransportError as error:
        return None, error.reason
    except (TimeoutError, socket.timeout):
        return None, "REQUEST_TIMEOUT"
    except ssl.SSLError:
        return None, "TLS_ERROR"
    except (urllib.error.URLError, OSError):
        return None, "CONNECT_ERROR"
    except Exception:
        return None, "TRANSPORT_ERROR"
    if not isinstance(response, HttpResponse):
        return None, "TRANSPORT_PROTOCOL_ERROR"
    return response, None


def _response_status_reason(response: HttpResponse | None) -> str | None:
    if response is None:
        return "TRANSPORT_PROTOCOL_ERROR"
    status = response.status
    if isinstance(status, bool) or not isinstance(status, int):
        return "TRANSPORT_PROTOCOL_ERROR"
    if status == 200:
        return None
    return _http_failure_reason(status)


def _remaining_or_none(
    deadline_at: float,
    monotonic_clock: Callable[[], float],
    maximum_attempt_timeout: float,
) -> float | None:
    try:
        return remaining_timeout(
            deadline_at,
            monotonic_clock,
            maximum_attempt_timeout,
        )
    except BoundedTransportError:
        return None


def _sleep_before_retry(
    *,
    deadline_at: float,
    monotonic_clock: Callable[[], float],
    sleeper: Callable[[float], None],
) -> bool:
    sleep_seconds = _remaining_or_none(
        deadline_at,
        monotonic_clock,
        _RETRY_BACKOFF_SECONDS,
    )
    if sleep_seconds is None:
        return False
    failed = False
    try:
        sleeper(sleep_seconds)
    except Exception:
        failed = True
    return not failed


def _monotonic_value(clock: Callable[[], float]) -> float:
    invalid = False
    try:
        value = float(clock())
        invalid = not math.isfinite(value)
    except Exception:
        invalid = True
        value = 0.0
    if invalid:
        raise DeepSeekError("REQUEST_TIMEOUT")
    return value


def _elapsed_milliseconds(
    started_at: float,
    monotonic_clock: Callable[[], float],
) -> float:
    invalid = False
    try:
        elapsed = (float(monotonic_clock()) - started_at) * 1_000.0
        invalid = not math.isfinite(elapsed)
    except Exception:
        invalid = True
        elapsed = 0.0
    if invalid:
        return 0.0
    return max(0.0, elapsed)


def _parse_response(body: bytes) -> tuple[dict[str, Any], dict[str, int]]:
    if not isinstance(body, bytes) or len(body) > MAXIMUM_DEEPSEEK_RESPONSE_BYTES:
        raise DeepSeekError("RESPONSE_TOO_LARGE")
    try:
        payload = decode_strict_json_object(body)
    except BoundedTransportError:
        raise _RetryableError("BAD_JSON")
    choices = payload.get("choices")
    if not isinstance(choices, list) or not choices:
        raise _RetryableError("EMPTY_RESPONSE")
    choice = choices[0]
    if not isinstance(choice, Mapping):
        raise DeepSeekError("RESPONSE_SCHEMA_INVALID")
    _require_stop_finish_reason(choice.get("finish_reason"))
    message = choice.get("message")
    if not isinstance(message, Mapping):
        raise DeepSeekError("RESPONSE_SCHEMA_INVALID")
    content = message.get("content")
    if not isinstance(content, str) or not content.strip():
        raise _RetryableError("EMPTY_RESPONSE")
    return _parse_model_json(content), _usage(payload.get("usage"))


def _parse_model_json(content: str) -> dict[str, Any]:
    try:
        value = decode_strict_json_object(content.encode("utf-8", errors="strict"))
    except (BoundedTransportError, UnicodeError):
        raise _RetryableError("BAD_JSON")
    return dict(value)


def _usage(value: object) -> dict[str, int]:
    if not isinstance(value, Mapping):
        raise DeepSeekError("RESPONSE_SCHEMA_INVALID")
    keys = (
        "prompt_tokens",
        "completion_tokens",
        "prompt_cache_hit_tokens",
        "prompt_cache_miss_tokens",
    )
    if any(key not in value for key in keys):
        raise DeepSeekError("RESPONSE_SCHEMA_INVALID")
    if any(
        isinstance(value[key], bool) or not isinstance(value[key], int)
        for key in keys
    ):
        raise DeepSeekError("RESPONSE_SCHEMA_INVALID")
    usage = {key: value[key] for key in keys}
    if any(count < 0 for count in usage.values()):
        raise DeepSeekError("RESPONSE_SCHEMA_INVALID")
    if (
        usage["prompt_tokens"]
        != usage["prompt_cache_hit_tokens"] + usage["prompt_cache_miss_tokens"]
    ):
        raise DeepSeekError("RESPONSE_SCHEMA_INVALID")
    return usage


def _require_stop_finish_reason(value: object) -> None:
    finish_reason = value if isinstance(value, str) else None
    if finish_reason == "stop":
        return
    if finish_reason in {None, "length", "insufficient_system_resource"}:
        raise _RetryableError("INCOMPLETE_FINISH")
    raise DeepSeekError("UNSAFE_FINISH")


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _field_count(value: object) -> int:
    if isinstance(value, Mapping):
        return len(value) + sum(_field_count(item) for item in value.values())
    if isinstance(value, list):
        return sum(_field_count(item) for item in value)
    return 0


__all__ = [
    "DEEPSEEK_URL",
    "DEEPSEEK_WHOLE_CYCLE_SECONDS",
    "MAXIMUM_DEEPSEEK_HTTP_ATTEMPTS",
    "MAXIMUM_DEEPSEEK_REQUEST_BYTES",
    "MAXIMUM_DEEPSEEK_RESPONSE_BYTES",
    "CompletionMetadata",
    "DeepSeekClient",
    "DeepSeekError",
    "DeepSeekResult",
    "DeepSeekTransportError",
    "HttpResponse",
    "HttpTransport",
    "UrllibTransport",
]
