"""Small Windows DPAPI-backed key/value store.

Only encrypted blobs and non-sensitive key names are persisted.  Plaintext is
never accepted through command-line arguments or environment variables by this
component.
"""
from __future__ import annotations

import base64
import json
import os
import re
import tempfile
import threading
from pathlib import Path


_NAME_RE = re.compile(r"^[A-Z][A-Z0-9_]{1,63}$")
_DPAPI_UI_FORBIDDEN = 0x1


class SecretStoreError(RuntimeError):
    pass


class DPAPISecretStore:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._lock = threading.RLock()

    def names(self) -> tuple[str, ...]:
        with self._lock:
            return tuple(sorted(self._read_payload()))

    def contains(self, name: str) -> bool:
        key = _validate_name(name)
        with self._lock:
            return key in self._read_payload()

    def set(self, name: str, value: str) -> None:
        key = _validate_name(name)
        if not isinstance(value, str) or not value:
            raise ValueError("secret value must be a non-empty string")
        protected = _protect(value.encode("utf-8"), description=f"OptionsCopilot:{key}")
        with self._lock:
            payload = self._read_payload()
            payload[key] = base64.b64encode(protected).decode("ascii")
            self._write_payload(payload)

    def get(self, name: str) -> str | None:
        key = _validate_name(name)
        with self._lock:
            encoded = self._read_payload().get(key)
        if encoded is None:
            return None
        try:
            blob = base64.b64decode(encoded, validate=True)
            return _unprotect(blob).decode("utf-8")
        except Exception as exc:
            raise SecretStoreError(f"unable to decrypt secret {key}") from exc

    def delete(self, name: str) -> bool:
        key = _validate_name(name)
        with self._lock:
            payload = self._read_payload()
            if key not in payload:
                return False
            del payload[key]
            self._write_payload(payload)
        return True

    def _read_payload(self) -> dict[str, str]:
        if not self.path.exists():
            return {}
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise SecretStoreError("secret store is unreadable") from exc
        if not isinstance(raw, dict) or raw.get("version") != 1:
            raise SecretStoreError("unsupported secret store format")
        values = raw.get("values")
        if not isinstance(values, dict):
            raise SecretStoreError("invalid secret store values")
        result: dict[str, str] = {}
        for key, value in values.items():
            _validate_name(key)
            if not isinstance(value, str):
                raise SecretStoreError("invalid encrypted secret value")
            result[key] = value
        return result

    def _write_payload(self, values: dict[str, str]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        body = json.dumps(
            {"version": 1, "values": dict(sorted(values.items()))},
            sort_keys=True,
            separators=(",", ":"),
        )
        handle, temporary = tempfile.mkstemp(
            prefix=f".{self.path.name}.", suffix=".tmp", dir=self.path.parent
        )
        try:
            with os.fdopen(handle, "w", encoding="utf-8", newline="\n") as stream:
                stream.write(body)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.path)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)


def _validate_name(name: str) -> str:
    if not isinstance(name, str) or not _NAME_RE.fullmatch(name):
        raise ValueError("secret name must match [A-Z][A-Z0-9_]{1,63}")
    return name


def _protect(data: bytes, *, description: str) -> bytes:
    if os.name != "nt":
        raise SecretStoreError("Windows DPAPI is required")
    try:
        import win32crypt

        return bytes(
            win32crypt.CryptProtectData(
                data,
                description,
                None,
                None,
                None,
                _DPAPI_UI_FORBIDDEN,
            )
        )
    except Exception as exc:
        raise SecretStoreError("DPAPI encryption failed") from exc


def _unprotect(blob: bytes) -> bytes:
    if os.name != "nt":
        raise SecretStoreError("Windows DPAPI is required")
    try:
        import win32crypt

        _description, value = win32crypt.CryptUnprotectData(
            blob,
            None,
            None,
            None,
            _DPAPI_UI_FORBIDDEN,
        )
        return bytes(value)
    except Exception as exc:
        raise SecretStoreError("DPAPI decryption failed") from exc
