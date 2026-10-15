"""Strict local JSON credentials for optional Options Copilot providers.

The file is intentionally local-only and gitignored.  This reader never logs,
serializes, or includes credential values in exceptions.  Empty strings are an
explicit provider disable switch.
"""
from __future__ import annotations

import hashlib
import hmac
import json
from pathlib import Path
import re
import secrets
from typing import Final, Mapping, Protocol


LOCAL_API_KEY_FILENAME: Final = "api_keys.local.json"
API_KEY_FIELDS: Final = (
    "jin10_mcp_token",
    "finnhub_api_key",
    "alpha_vantage_api_key",
    "deepseek_api_key",
)
_SECRET_NAME_TO_FIELD: Final = {
    "JIN10_MCP_TOKEN": "jin10_mcp_token",
    "FINNHUB_API_KEY": "finnhub_api_key",
    "ALPHA_VANTAGE_API_KEY": "alpha_vantage_api_key",
    "DEEPSEEK_API_KEY": "deepseek_api_key",
}
_MAX_FILE_BYTES = 64 * 1024
_MAX_VALUE_LENGTH = 8192
JIN10_BINDING_SECRET_NAME: Final = "JIN10_LOCAL_ACTIVATION_BINDING"
_JIN10_BINDING_SCHEMA: Final = "options_copilot.security.local_jin10_binding.v1"


class LocalApiKeyFileError(RuntimeError):
    """The local credential file could not be trusted.

    Messages deliberately contain no input material.  In particular, JSON
    decoder exceptions are not retained as ``__cause__``/``__context__``
    because those objects retain the complete source document.
    """


class LocalJin10BindingError(RuntimeError):
    """The encrypted opaque Jin10 activation binding is absent or invalid."""


class Jin10BindingStore(Protocol):
    """Structural protocol implemented by the Windows DPAPI key/value store."""

    def get(self, name: str) -> str | None: ...

    def set(self, name: str, value: str) -> None: ...


class LocalApiKeyStore:
    """Read a fixed-schema, local plaintext JSON credential file on demand."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def get(self, name: str) -> str | None:
        field = _field_for_name(name)
        value = self._read_payload()[field]
        return value or None

    def contains(self, name: str) -> bool:
        return self.get(name) is not None

    def names(self) -> tuple[str, ...]:
        payload = self._read_payload()
        return tuple(
            sorted(
                secret_name
                for secret_name, field in _SECRET_NAME_TO_FIELD.items()
                if payload[field]
            )
        )

    def configuration_status(self) -> dict[str, str]:
        """Return non-secret statuses for the fixed GUI/API contract."""

        try:
            payload = self._read_payload()
        except LocalApiKeyFileError:
            return {field: "ERROR" for field in API_KEY_FIELDS}
        return {
            field: "CONFIGURED" if payload[field] else "DISABLED"
            for field in API_KEY_FIELDS
        }

    def _read_payload(self) -> dict[str, str]:
        if not self.path.exists():
            return {field: "" for field in API_KEY_FIELDS}
        try:
            if self.path.stat().st_size > _MAX_FILE_BYTES:
                raise LocalApiKeyFileError("local API key file is invalid")
            text = self.path.read_text(encoding="utf-8")
        except LocalApiKeyFileError:
            raise
        except UnicodeError:
            text = None
        except OSError:
            raise LocalApiKeyFileError("local API key file is unreadable") from None
        if text is None:
            # Raise outside the decoder exception handler so the exception does
            # not retain the source bytes through ``__context__``.
            raise LocalApiKeyFileError("local API key file encoding is invalid")
        parse_failed = False
        try:
            payload = json.loads(text, object_pairs_hook=_strict_object)
        except (json.JSONDecodeError, _DuplicateKeyError):
            payload = None
            parse_failed = True
        if parse_failed:
            # JSONDecodeError retains the complete input as ``doc``.  Raising
            # after the handler keeps it out of the public exception chain.
            raise LocalApiKeyFileError("local API key file is invalid JSON")
        if not isinstance(payload, dict):
            raise LocalApiKeyFileError("local API key file must be a JSON object")
        if set(payload) != set(API_KEY_FIELDS):
            raise LocalApiKeyFileError("local API key file schema is invalid")
        result: dict[str, str] = {}
        for field in API_KEY_FIELDS:
            value = payload[field]
            if (
                not isinstance(value, str)
                or len(value) > _MAX_VALUE_LENGTH
                or value != value.strip()
                or "\x00" in value
            ):
                raise LocalApiKeyFileError("local API key file value is invalid")
            result[field] = value
        return result


class LocalJin10EnvelopeReader:
    """Adapt a local raw token to the existing revocation/activation gate.

    The raw token is never handed directly to ``Jin10EventProvider``.  A stable
    opaque random generation lets the existing activation manifest bind the
    exact local replacement token without storing the token or a reproducible
    token fingerprint in evidence.
    """

    def __init__(
        self,
        source: LocalApiKeyStore,
        binding_store: Jin10BindingStore | None = None,
    ) -> None:
        self._source = source
        self._binding_store = binding_store

    def get(self, name: str) -> str | None:
        if name != "JIN10_MCP_TOKEN":
            return None
        material = self._bound_material()
        if material is None:
            return None
        token, generation = material
        from options_copilot.security.jin10_credentials import (
            encode_jin10_credential,
        )

        encoded, _generation = encode_jin10_credential(
            token,
            credential_generation=generation,
        )
        return encoded

    def credential_generation(self) -> str | None:
        material = self._bound_material()
        return None if material is None else material[1]

    def create_opaque_binding(self) -> str:
        """Create a DPAPI-protected random generation for the current token."""

        if self._binding_store is None:
            raise LocalJin10BindingError("Jin10 binding store is unavailable")
        token = self._source.get("JIN10_MCP_TOKEN")
        if token is None:
            raise LocalJin10BindingError(
                "jin10_mcp_token is disabled in api_keys.local.json"
            )
        generation = secrets.token_hex(16)
        binding = {
            "schema": _JIN10_BINDING_SCHEMA,
            "credential_generation": generation,
            # This digest is persisted only inside the DPAPI-encrypted value.
            # Public evidence receives only the unrelated random generation.
            "token_digest": _token_digest(token),
        }
        self._binding_store.set(
            JIN10_BINDING_SECRET_NAME,
            json.dumps(binding, sort_keys=True, separators=(",", ":")),
        )
        return generation

    def _bound_material(self) -> tuple[str, str] | None:
        token = self._source.get("JIN10_MCP_TOKEN")
        if token is None or self._binding_store is None:
            return None
        try:
            encoded = self._binding_store.get(JIN10_BINDING_SECRET_NAME)
        except Exception as exc:
            raise LocalJin10BindingError(
                "Jin10 activation binding is unreadable"
            ) from exc
        if encoded is None:
            return None
        try:
            binding = json.loads(encoded)
        except (json.JSONDecodeError, TypeError):
            raise LocalJin10BindingError(
                "Jin10 activation binding is invalid"
            ) from None
        if not isinstance(binding, dict) or set(binding) != {
            "schema",
            "credential_generation",
            "token_digest",
        }:
            raise LocalJin10BindingError("Jin10 activation binding is invalid")
        generation = binding.get("credential_generation")
        digest = binding.get("token_digest")
        if (
            binding.get("schema") != _JIN10_BINDING_SCHEMA
            or not isinstance(generation, str)
            or re.fullmatch(r"[0-9a-f]{32}", generation) is None
            or not isinstance(digest, str)
            or re.fullmatch(r"[0-9a-f]{64}", digest) is None
        ):
            raise LocalJin10BindingError("Jin10 activation binding is invalid")
        if not hmac.compare_digest(digest, _token_digest(token)):
            return None
        return token, generation


def local_api_key_path(data_dir: str | Path) -> Path:
    return Path(data_dir).resolve() / LOCAL_API_KEY_FILENAME


def provider_configuration_status(store: LocalApiKeyStore) -> Mapping[str, str]:
    """Return only CONFIGURED/DISABLED/ERROR, never credential material."""

    return store.configuration_status()


def _field_for_name(name: str) -> str:
    if not isinstance(name, str) or name not in _SECRET_NAME_TO_FIELD:
        raise ValueError("unsupported local API key name")
    return _SECRET_NAME_TO_FIELD[name]


class _DuplicateKeyError(ValueError):
    pass


def _strict_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise _DuplicateKeyError
        result[key] = value
    return result


def _token_digest(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


__all__ = [
    "API_KEY_FIELDS",
    "JIN10_BINDING_SECRET_NAME",
    "LOCAL_API_KEY_FILENAME",
    "LocalApiKeyFileError",
    "LocalApiKeyStore",
    "LocalJin10EnvelopeReader",
    "LocalJin10BindingError",
    "local_api_key_path",
    "provider_configuration_status",
]
