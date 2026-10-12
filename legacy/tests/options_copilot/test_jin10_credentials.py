from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path

from options_copilot.security.jin10_credentials import (
    JIN10_CREDENTIAL_SCHEMA,
    activate_jin10_credential,
    encode_jin10_credential,
    resolve_jin10_credential,
)
from options_copilot.security.revocation import (
    JIN10_SECRET_NAME,
    RevocationAttestation,
    reserve_rotation,
    write_revocation_attestation,
)


NOW = datetime(2026, 8, 6, 2, 30, tzinfo=timezone.utc)


class FakeSecrets:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}

    def contains(self, name: str) -> bool:
        return name in self.values

    def get(self, name: str) -> str | None:
        return self.values.get(name)

    def set(self, name: str, value: str) -> None:
        self.values[name] = value


def _attestation(evidence_dir: Path, actor: str = "human:test"):
    value = RevocationAttestation.create(
        name=JIN10_SECRET_NAME,
        actor=actor,
        signed_at=NOW,
    )
    write_revocation_attestation(value, evidence_dir)
    return value


def _complete_activation(
    secrets: FakeSecrets,
    evidence_dir: Path,
    *,
    token: str = "fixture-only-value",
    actor: str = "human:test",
):
    attestation = _attestation(evidence_dir, actor)
    encoded, generation = encode_jin10_credential(
        token,
        credential_generation="0123456789abcdef0123456789abcdef",
    )
    with reserve_rotation(attestation, evidence_dir) as reservation:
        secrets.set(JIN10_SECRET_NAME, encoded)
        activate_jin10_credential(
            evidence_dir,
            attestation_hash=attestation.canonical_hash,
            credential_generation=generation,
            activated_at=NOW,
        )
        reservation.commit(rotated_at=NOW)
    return attestation, generation


def test_complete_activation_resolves_only_the_dpapi_enclosed_value(
    tmp_path: Path,
) -> None:
    secret_value = "fixture-only-value"
    secrets = FakeSecrets()
    attestation, generation = _complete_activation(
        secrets,
        tmp_path,
        token=secret_value,
    )

    resolution = resolve_jin10_credential(secrets, tmp_path)

    assert resolution.status == "ACTIVATED"
    assert resolution.secret_store is not None
    assert resolution.secret_store.get(JIN10_SECRET_NAME) == secret_value
    encrypted_plaintext = json.loads(secrets.values[JIN10_SECRET_NAME])
    assert encrypted_plaintext == {
        "credential_generation": generation,
        "name": JIN10_SECRET_NAME,
        "schema": JIN10_CREDENTIAL_SCHEMA,
        "token": secret_value,
    }
    activation_text = next(tmp_path.glob("activation-*.json")).read_text(
        encoding="utf-8"
    )
    assert secret_value not in activation_text
    assert attestation.canonical_hash in activation_text
    assert generation in activation_text


def test_new_dpapi_generation_without_activation_fails_closed(tmp_path: Path) -> None:
    secrets = FakeSecrets()
    _complete_activation(secrets, tmp_path)
    active = resolve_jin10_credential(secrets, tmp_path)
    assert active.secret_store is not None
    replacement, _generation = encode_jin10_credential(
        "replacement-value",
        credential_generation="abcdef0123456789abcdef0123456789",
    )
    secrets.set(JIN10_SECRET_NAME, replacement)

    resolution = resolve_jin10_credential(secrets, tmp_path)

    assert resolution.status == "GENERATION_NOT_ACTIVATED"
    assert resolution.secret_store is None
    assert active.secret_store.get(JIN10_SECRET_NAME) is None


def test_activation_without_committed_rotation_fails_closed(tmp_path: Path) -> None:
    secrets = FakeSecrets()
    attestation = _attestation(tmp_path)
    encoded, generation = encode_jin10_credential(
        "fixture-only-value",
        credential_generation="0123456789abcdef0123456789abcdef",
    )
    secrets.set(JIN10_SECRET_NAME, encoded)
    activate_jin10_credential(
        tmp_path,
        attestation_hash=attestation.canonical_hash,
        credential_generation=generation,
        activated_at=NOW,
    )

    resolution = resolve_jin10_credential(secrets, tmp_path)

    assert resolution.status == "ROTATION_NOT_ATTESTED"
    assert resolution.secret_store is None


def test_latest_rotation_must_bind_current_credential_generation(
    tmp_path: Path,
) -> None:
    secrets = FakeSecrets()
    _complete_activation(secrets, tmp_path, actor="human:first")
    second = _attestation(tmp_path, actor="human:second")
    with reserve_rotation(second, tmp_path) as reservation:
        reservation.commit(rotated_at=NOW.replace(microsecond=1))

    resolution = resolve_jin10_credential(secrets, tmp_path)

    assert resolution.status == "GENERATION_NOT_ACTIVATED"
    assert resolution.secret_store is None


def test_legacy_bare_token_and_malformed_activation_never_resolve(
    tmp_path: Path,
) -> None:
    secrets = FakeSecrets()
    secrets.set(JIN10_SECRET_NAME, "legacy-bare-token")
    assert resolve_jin10_credential(secrets, tmp_path).status == "INVALID_ENVELOPE"

    _complete_activation(secrets, tmp_path)
    active = resolve_jin10_credential(secrets, tmp_path)
    assert active.secret_store is not None
    activation = next(tmp_path.glob("activation-*.json"))
    activation.write_text('{"token":"must-never-be-read"}\n', encoding="utf-8")
    assert active.secret_store.get(JIN10_SECRET_NAME) is None
    resolution = resolve_jin10_credential(secrets, tmp_path)
    assert resolution.status == "ACTIVATION_INVALID"
    assert resolution.secret_store is None


def test_resolution_errors_and_repr_never_include_secret_material(
    tmp_path: Path,
) -> None:
    secret_value = "do-not-render-this-value"
    secrets = FakeSecrets()
    _complete_activation(secrets, tmp_path, token=secret_value)
    resolution = resolve_jin10_credential(secrets, tmp_path)

    assert secret_value not in repr(resolution)
    assert secret_value not in repr(resolution.secret_store)
