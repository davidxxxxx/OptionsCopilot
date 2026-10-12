from __future__ import annotations

import json
from pathlib import Path
import traceback

import pytest

from options_copilot.security.jin10_credentials import resolve_jin10_credential
from options_copilot.security.local_api_keys import (
    API_KEY_FIELDS,
    LocalApiKeyFileError,
    LocalApiKeyStore,
    LocalJin10EnvelopeReader,
    local_api_key_path,
    provider_configuration_status,
)


def _write(path: Path, **overrides: object) -> None:
    payload: dict[str, object] = {field: "" for field in API_KEY_FIELDS}
    payload.update(overrides)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_fixed_local_file_maps_only_configured_provider_names(tmp_path: Path) -> None:
    path = local_api_key_path(tmp_path)
    _write(
        path,
        finnhub_api_key="finnhub-fixture",
        deepseek_api_key="deepseek-fixture",
    )
    store = LocalApiKeyStore(path)

    assert store.get("FINNHUB_API_KEY") == "finnhub-fixture"
    assert store.get("ALPHA_VANTAGE_API_KEY") is None
    assert store.names() == ("DEEPSEEK_API_KEY", "FINNHUB_API_KEY")
    assert provider_configuration_status(store) == {
        "jin10_mcp_token": "DISABLED",
        "finnhub_api_key": "CONFIGURED",
        "alpha_vantage_api_key": "DISABLED",
        "deepseek_api_key": "CONFIGURED",
    }


def test_missing_file_means_all_optional_providers_are_disabled(tmp_path: Path) -> None:
    store = LocalApiKeyStore(local_api_key_path(tmp_path))

    assert store.names() == ()
    assert set(provider_configuration_status(store).values()) == {"DISABLED"}


@pytest.mark.parametrize(
    "body",
    [
        "[]",
        "not-json",
        '{"jin10_mcp_token":"","finnhub_api_key":"","alpha_vantage_api_key":"","deepseek_api_key":"","unknown":""}',
        '{"jin10_mcp_token":"","finnhub_api_key":"","alpha_vantage_api_key":""}',
        '{"jin10_mcp_token":"","finnhub_api_key":null,"alpha_vantage_api_key":"","deepseek_api_key":""}',
        '{"jin10_mcp_token":"","finnhub_api_key":" first","alpha_vantage_api_key":"","deepseek_api_key":""}',
        '{"jin10_mcp_token":"","finnhub_api_key":"one","finnhub_api_key":"two","alpha_vantage_api_key":"","deepseek_api_key":""}',
    ],
)
def test_invalid_or_non_exact_schema_fails_closed(body: str, tmp_path: Path) -> None:
    path = local_api_key_path(tmp_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")
    store = LocalApiKeyStore(path)

    with pytest.raises(LocalApiKeyFileError):
        store.names()
    assert set(provider_configuration_status(store).values()) == {"ERROR"}


def test_parse_error_and_repr_do_not_expose_secret_material(tmp_path: Path) -> None:
    secret = "sentinel-local-secret-must-not-leak"
    path = local_api_key_path(tmp_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f'{{"finnhub_api_key":"{secret}"', encoding="utf-8")
    store = LocalApiKeyStore(path)

    with pytest.raises(LocalApiKeyFileError) as captured:
        store.names()

    rendered = "".join(
        traceback.format_exception(captured.type, captured.value, captured.tb)
    )
    assert secret not in rendered
    assert secret not in repr(store)
    assert captured.value.__cause__ is None
    assert captured.value.__context__ is None


def test_unknown_secret_name_is_rejected_without_reading_file(tmp_path: Path) -> None:
    store = LocalApiKeyStore(local_api_key_path(tmp_path))

    with pytest.raises(ValueError, match="unsupported local API key name"):
        store.get("UNSUPPORTED_API_KEY")


def test_plaintext_jin10_never_bypasses_existing_activation_gate(tmp_path: Path) -> None:
    path = local_api_key_path(tmp_path)
    _write(path, jin10_mcp_token="replacement-fixture-token")
    source = LocalApiKeyStore(path)
    class BindingStore:
        value: str | None = None

        def get(self, _name: str) -> str | None:
            return self.value

        def set(self, _name: str, value: str) -> None:
            self.value = value

    binding_store = BindingStore()
    envelope_reader = LocalJin10EnvelopeReader(source, binding_store)

    assert envelope_reader.credential_generation() is None
    bound_generation = envelope_reader.create_opaque_binding()

    first_generation = envelope_reader.credential_generation()
    second_generation = envelope_reader.credential_generation()
    resolution = resolve_jin10_credential(envelope_reader, tmp_path / "evidence")

    assert first_generation == second_generation == bound_generation
    assert isinstance(first_generation, str) and len(first_generation) == 32
    assert resolution.secret_store is None
    assert resolution.status == "ROTATION_NOT_ATTESTED"
    assert "replacement-fixture-token" not in repr(envelope_reader)
    assert "replacement-fixture-token" not in str(binding_store.value)
