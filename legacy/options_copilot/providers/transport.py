"""Source-neutral bounded response and strict JSON mechanics."""
from __future__ import annotations

from collections.abc import Callable, Mapping
import json
import math
import re


_REASON_CODE = re.compile(r"^[A-Z][A-Z0-9_]{0,63}$")


class BoundedTransportError(RuntimeError):
    """A fixed-code failure that retains no untrusted transport value."""

    def __init__(self, reason: str) -> None:
        checked = str(reason or "").strip().upper()
        if _REASON_CODE.fullmatch(checked) is None:
            checked = "TRANSPORT_ERROR"
        self.reason = checked
        super().__init__(checked)


def remaining_timeout(
    deadline_at: float,
    monotonic_clock: Callable[[], float],
    maximum_attempt_timeout: float,
) -> float:
    """Return one attempt timeout bounded by an absolute monotonic deadline."""

    if not callable(monotonic_clock):
        raise TypeError("monotonic_clock must be callable")
    if isinstance(maximum_attempt_timeout, bool) or not isinstance(
        maximum_attempt_timeout,
        (int, float),
    ):
        raise TypeError("maximum_attempt_timeout must be numeric")
    attempt_timeout = float(maximum_attempt_timeout)
    if not math.isfinite(attempt_timeout) or attempt_timeout <= 0:
        raise BoundedTransportError("REQUEST_TIMEOUT")

    invalid_budget = False
    try:
        deadline = float(deadline_at)
        current = float(monotonic_clock())
        remaining = deadline - current
        invalid_budget = not math.isfinite(remaining) or remaining <= 0
    except Exception:
        invalid_budget = True
        remaining = 0.0
    if invalid_budget:
        raise BoundedTransportError("REQUEST_TIMEOUT")
    return min(attempt_timeout, remaining)


def read_bounded_response(response: object, maximum_bytes: int) -> bytes:
    """Read at most ``maximum_bytes + 1`` and close the response exactly once."""

    if (
        isinstance(maximum_bytes, bool)
        or not isinstance(maximum_bytes, int)
        or maximum_bytes <= 0
    ):
        raise ValueError("maximum_bytes must be a positive integer")

    failure_reason: str | None = None
    body: bytes | None = None
    try:
        headers = getattr(response, "headers", None)
        raw_length = _header(headers, "Content-Length")
        if raw_length not in (None, ""):
            try:
                declared_length = int(str(raw_length))
            except (TypeError, ValueError):
                failure_reason = "RESPONSE_LENGTH_INVALID"
            else:
                if declared_length < 0:
                    failure_reason = "RESPONSE_LENGTH_INVALID"
                elif declared_length > maximum_bytes:
                    failure_reason = "RESPONSE_TOO_LARGE"
        if failure_reason is None:
            reader = getattr(response, "read", None)
            if not callable(reader):
                failure_reason = "RESPONSE_READ_FAILED"
            else:
                try:
                    candidate = reader(maximum_bytes + 1)
                except Exception:
                    failure_reason = "RESPONSE_READ_FAILED"
                else:
                    if not isinstance(candidate, bytes):
                        failure_reason = "RESPONSE_BODY_INVALID"
                    elif len(candidate) > maximum_bytes:
                        failure_reason = "RESPONSE_TOO_LARGE"
                    else:
                        body = candidate
    except Exception:
        failure_reason = "RESPONSE_INVALID"
    finally:
        _close_response(response)

    if failure_reason is not None:
        raise BoundedTransportError(failure_reason)
    assert body is not None
    return body


def decode_strict_json_object(body: bytes) -> Mapping[str, object]:
    """Decode one complete finite JSON object with unique object keys."""

    payload: object | None = None
    invalid = False
    if not isinstance(body, bytes) or not body:
        invalid = True
    else:
        try:
            payload = json.loads(
                body.decode("utf-8", errors="strict"),
                object_pairs_hook=_object_without_duplicate_keys,
                parse_constant=_reject_json_constant,
                parse_float=_finite_float,
            )
        except Exception:
            invalid = True
    if invalid or not isinstance(payload, Mapping):
        raise BoundedTransportError("BAD_JSON")
    return dict(payload)


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


def _object_without_duplicate_keys(
    pairs: list[tuple[str, object]],
) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate object key")
        result[key] = value
    return result


def _reject_json_constant(_value: str) -> object:
    raise ValueError("non-finite numeric constant")


def _finite_float(value: str) -> float:
    parsed = float(value)
    if not math.isfinite(parsed):
        raise ValueError("non-finite numeric value")
    return parsed


__all__ = [
    "BoundedTransportError",
    "decode_strict_json_object",
    "read_bounded_response",
    "remaining_timeout",
]
