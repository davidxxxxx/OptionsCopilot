"""Privacy boundary applied before any external model request."""
from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import Any


REDACTED = "[REDACTED]"
_SENSITIVE_KEYS = {
    "account",
    "account_id",
    "account_number",
    "order_id",
    "basket_id",
    "fcm_id",
    "user_tag",
    "api_key",
    "authorization",
    "credential",
    "password",
    "token",
    "path",
    "file_path",
    "tools",
    "tool_choice",
    "functions",
    "function_call",
    "command",
    "commands",
    "command_schema",
    "command_gateway",
    "raw_bars",
    "bars",
    "candles",
    "k_lines",
    "klines",
    "ohlcv",
    "raw_history",
    "raw_mbo",
    "mbo_rows",
    "raw_ticks",
}
_KEY_MARKERS = ("account_id", "order_id", "basket_id", "fcm_id", "api_key", "user_tag")
_COMPACT_SENSITIVE_KEYS = {
    re.sub(r"[^a-z0-9]", "", key)
    for key in (*_SENSITIVE_KEYS, *_KEY_MARKERS)
}
_SECRET_PATTERNS = (
    re.compile(r"\bsk-[A-Za-z0-9_-]{12,}\b", re.IGNORECASE),
    re.compile(r"\bfcm[-_ ]?id\s+(?:is\s+)?[A-Za-z0-9_-]{4,}\b", re.IGNORECASE),
    # Require an explicit assignment delimiter.  The former fully optional
    # separators treated benign prose such as ``accountsThe`` as an account
    # identifier and changed an otherwise-safe snapshot before transport.
    re.compile(
        r"\b(?:account|broker|order|basket|fcm)[-_ ]?(?:id|number)?\s*[:=]\s*"
        r"[A-Za-z0-9_-]{4,}\b",
        re.IGNORECASE,
    ),
    # IBKR account identifiers are often written as ``account number
    # DU123456`` or ``broker account U1234567`` without an assignment
    # delimiter.  Match only an explicit account label followed by an
    # account-shaped token so ordinary prose such as ``accountsThe`` remains
    # untouched.
    re.compile(
        r"\b(?:account(?:[_ -]+(?:id|number))?|broker[_ -]+account)\s*"
        r"(?::|=)?\s*[A-Z]{0,3}\d{5,}\b",
        re.IGNORECASE,
    ),
    re.compile(r"(?<![A-Za-z0-9])(?:[A-Za-z]:\\[^\s\"']+)", re.IGNORECASE),
    re.compile(r"(?<![A-Za-z0-9])/(?:home|users?|var|tmp|opt)/[^\s\"']+", re.IGNORECASE),
)


def redact_for_model(value: Any) -> Any:
    if isinstance(value, Mapping):
        redacted: dict[str, Any] = {}
        for raw_key, item in value.items():
            key = str(raw_key)
            normalized = key.strip().lower()
            compact = re.sub(r"[^a-z0-9]", "", normalized)
            if (
                normalized in _SENSITIVE_KEYS
                or compact in _COMPACT_SENSITIVE_KEYS
                or compact.startswith(("account", "broker", "fcm"))
                or any(marker in normalized for marker in _KEY_MARKERS)
            ):
                continue
            else:
                redacted[key] = redact_for_model(item)
        return redacted
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [redact_for_model(item) for item in value]
    if isinstance(value, str):
        result = value
        for pattern in _SECRET_PATTERNS:
            result = pattern.sub(REDACTED, result)
        return result
    if value is None or isinstance(value, (bool, int, float)):
        return value
    return redact_for_model(str(value))
