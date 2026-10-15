"""Crash-consistent activation for the rotated Jin10 DPAPI credential.

The credential generation is encrypted inside the same DPAPI value as the
token.  A zero-material activation document binds that opaque generation to
one consumed revocation attestation.  Readers expose the token only when the
current envelope, activation, and latest rotation manifest agree exactly.
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import re
import secrets
from typing import Protocol

from options_copilot.security.revocation import (
    JIN10_SECRET_NAME,
    RevocationError,
    latest_rotation_manifest,
)
from options_copilot.storage.canonical import canonical_json, datetime_text


JIN10_CREDENTIAL_SCHEMA = "options_copilot.security.jin10_credential.v1"
JIN10_ACTIVATION_SCHEMA = "options_copilot.security.jin10_activation.v1"
ACTIVATION_MANIFEST_GLOB = "activation-*.json"
JIN10_ROTATION_EVIDENCE_PARTS = (
    "evidence",
    "checkpoints",
    "P3",
    "jin10-rotation",
)

_ENVELOPE_FIELDS = frozenset(
    {"schema", "name", "credential_generation", "token"}
)
_ACTIVATION_FIELDS = frozenset(
    {
        "schema",
        "name",
        "attestation_hash",
        "credential_generation",
        "activated_at",
    }
)
_GENERATION_RE = re.compile(r"[0-9a-f]{32}\Z")
_HASH_RE = re.compile(r"[0-9a-f]{64}\Z")


class Jin10CredentialError(RuntimeError):
    """A credential envelope or activation document failed closed."""


class SecretReader(Protocol):
    def get(self, name: str) -> str | None: ...


@dataclass(frozen=True, slots=True)
class Jin10ActivationManifest:
    schema: str
    name: str
    attestation_hash: str
    credential_generation: str
    activated_at: datetime

    def __post_init__(self) -> None:
        if self.schema != JIN10_ACTIVATION_SCHEMA:
            raise Jin10CredentialError("Jin10 activation schema is invalid")
        if self.name != JIN10_SECRET_NAME:
            raise Jin10CredentialError("Jin10 activation name is invalid")
        object.__setattr__(
            self,
            "attestation_hash",
            _digest(self.attestation_hash),
        )
        object.__setattr__(
            self,
            "credential_generation",
            _generation(self.credential_generation),
        )
        object.__setattr__(
            self,
            "activated_at",
            _utc_datetime(self.activated_at),
        )

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> "Jin10ActivationManifest":
        document = _strict_document(
            value,
            expected_fields=_ACTIVATION_FIELDS,
            kind="Jin10 activation",
        )
        return cls(
            schema=document["schema"],
            name=document["name"],
            attestation_hash=document["attestation_hash"],
            credential_generation=document["credential_generation"],
            activated_at=document["activated_at"],
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": self.schema,
            "name": self.name,
            "attestation_hash": self.attestation_hash,
            "credential_generation": self.credential_generation,
            "activated_at": datetime_text(self.activated_at),
        }


@dataclass(frozen=True, slots=True)
class ActivatedJin10SecretStore:
    """Read-only view that revalidates activation on every credential read."""

    credential_generation: str
    _source: SecretReader = field(repr=False)
    _evidence_dir: Path = field(repr=False)

    def get(self, name: str) -> str | None:
        if name != JIN10_SECRET_NAME:
            return None
        status, token, _generation_value = _resolve_material(
            self._source,
            self._evidence_dir,
        )
        return token if status == "ACTIVATED" else None


@dataclass(frozen=True, slots=True)
class Jin10CredentialResolution:
    status: str
    secret_store: ActivatedJin10SecretStore | None = field(default=None, repr=False)


def encode_jin10_credential(
    token: str,
    *,
    credential_generation: str | None = None,
) -> tuple[str, str]:
    """Create plaintext that must be encrypted as one DPAPI value."""

    if (
        not isinstance(token, str)
        or not token
        or len(token) > 8192
        or token != token.strip()
        or "\x00" in token
    ):
        raise Jin10CredentialError("Jin10 credential value is invalid")
    generation = _generation(
        credential_generation
        if credential_generation is not None
        else secrets.token_hex(16)
    )
    return (
        canonical_json(
            {
                "schema": JIN10_CREDENTIAL_SCHEMA,
                "name": JIN10_SECRET_NAME,
                "credential_generation": generation,
                "token": token,
            }
        ),
        generation,
    )


def activate_jin10_credential(
    evidence_dir: str | Path,
    *,
    attestation_hash: str,
    credential_generation: str,
    activated_at: datetime | None = None,
) -> Path:
    """Append a zero-material binding after DPAPI storage and before rotation commit."""

    manifest = Jin10ActivationManifest(
        schema=JIN10_ACTIVATION_SCHEMA,
        name=JIN10_SECRET_NAME,
        attestation_hash=attestation_hash,
        credential_generation=credential_generation,
        activated_at=activated_at or datetime.now(timezone.utc),
    )
    directory = Path(evidence_dir).resolve()
    destination = directory / (
        f"activation-{manifest.attestation_hash}-"
        f"{manifest.credential_generation}.json"
    )
    _write_exclusive_json(destination, manifest.to_dict())
    return destination


def resolve_jin10_credential(
    store: SecretReader,
    evidence_dir: str | Path,
) -> Jin10CredentialResolution:
    """Return an in-memory reader only for an exactly activated generation."""

    directory = Path(evidence_dir).resolve()
    status, _token, generation = _resolve_material(store, directory)
    if status != "ACTIVATED" or generation is None:
        return Jin10CredentialResolution(status)
    return Jin10CredentialResolution(
        "ACTIVATED",
        ActivatedJin10SecretStore(
            credential_generation=generation,
            _source=store,
            _evidence_dir=directory,
        ),
    )


def _resolve_material(
    store: SecretReader,
    evidence_dir: Path,
) -> tuple[str, str | None, str | None]:
    try:
        encoded = store.get(JIN10_SECRET_NAME)
    except Exception:
        return "CREDENTIAL_UNREADABLE", None, None
    if encoded is None:
        return "NOT_CONFIGURED", None, None
    try:
        token, generation = _decode_envelope(encoded)
    except Jin10CredentialError:
        return "INVALID_ENVELOPE", None, None
    try:
        rotation = latest_rotation_manifest(
            evidence_dir,
            expected_name=JIN10_SECRET_NAME,
        )
    except RevocationError:
        return "ROTATION_INVALID", None, generation
    if rotation is None:
        return "ROTATION_NOT_ATTESTED", None, generation
    try:
        activations = _load_activations(evidence_dir)
    except Jin10CredentialError:
        return "ACTIVATION_INVALID", None, generation
    matches = tuple(
        item
        for item in activations
        if item.attestation_hash == rotation.attestation_hash
        and item.credential_generation == generation
        and item.activated_at <= rotation.rotated_at
    )
    if len(matches) != 1:
        return "GENERATION_NOT_ACTIVATED", None, generation
    return "ACTIVATED", token, generation


def jin10_rotation_evidence_dir(data_dir: str | Path) -> Path:
    return Path(data_dir).resolve().joinpath(*JIN10_ROTATION_EVIDENCE_PARTS)


def _decode_envelope(value: object) -> tuple[str, str]:
    if not isinstance(value, str) or len(value) > 16384:
        raise Jin10CredentialError("Jin10 credential envelope is invalid")
    document = _load_json_text(value, kind="Jin10 credential envelope")
    document = _strict_document(
        document,
        expected_fields=_ENVELOPE_FIELDS,
        kind="Jin10 credential envelope",
    )
    if document["schema"] != JIN10_CREDENTIAL_SCHEMA:
        raise Jin10CredentialError("Jin10 credential envelope is invalid")
    if document["name"] != JIN10_SECRET_NAME:
        raise Jin10CredentialError("Jin10 credential envelope is invalid")
    token = document["token"]
    if (
        not isinstance(token, str)
        or not token
        or len(token) > 8192
        or token != token.strip()
        or "\x00" in token
    ):
        raise Jin10CredentialError("Jin10 credential envelope is invalid")
    return token, _generation(document["credential_generation"])


def _load_activations(evidence_dir: str | Path) -> tuple[Jin10ActivationManifest, ...]:
    directory = Path(evidence_dir).resolve()
    if not directory.exists():
        return ()
    result: list[Jin10ActivationManifest] = []
    identities: set[tuple[str, str]] = set()
    try:
        paths = sorted(directory.glob(ACTIVATION_MANIFEST_GLOB))
    except OSError as exc:
        raise Jin10CredentialError("unable to enumerate Jin10 activations") from exc
    for path in paths:
        try:
            raw = path.read_text(encoding="utf-8")
        except OSError as exc:
            raise Jin10CredentialError("unable to read Jin10 activation") from exc
        manifest = Jin10ActivationManifest.from_dict(
            _load_json_text(raw, kind="Jin10 activation")
        )
        expected_name = (
            f"activation-{manifest.attestation_hash}-"
            f"{manifest.credential_generation}.json"
        )
        if path.name != expected_name:
            raise Jin10CredentialError("Jin10 activation filename is invalid")
        identity = (manifest.attestation_hash, manifest.credential_generation)
        if identity in identities:
            raise Jin10CredentialError("duplicate Jin10 activation binding")
        identities.add(identity)
        result.append(manifest)
    return tuple(result)


def _strict_document(
    value: Mapping[str, object],
    *,
    expected_fields: frozenset[str],
    kind: str,
) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise Jin10CredentialError(f"{kind} must be an object")
    missing = expected_fields.difference(value)
    unknown = set(value).difference(expected_fields)
    if missing or unknown:
        raise Jin10CredentialError(f"{kind} fields are invalid")
    if any(not isinstance(key, str) for key in value):
        raise Jin10CredentialError(f"{kind} fields are invalid")
    return dict(value)


def _load_json_text(value: str, *, kind: str) -> Mapping[str, object]:
    try:
        document = json.loads(
            value,
            object_pairs_hook=_unique_object,
            parse_constant=_reject_constant,
        )
    except (json.JSONDecodeError, ValueError) as exc:
        raise Jin10CredentialError(f"{kind} JSON is invalid") from exc
    if not isinstance(document, Mapping):
        raise Jin10CredentialError(f"{kind} must be an object")
    return document


def _write_exclusive_json(path: Path, document: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rendered = canonical_json(document) + "\n"
    try:
        with path.open("x", encoding="utf-8", newline="\n") as stream:
            stream.write(rendered)
            stream.flush()
            os.fsync(stream.fileno())
    except FileExistsError as exc:
        raise Jin10CredentialError("Jin10 activation already exists") from exc
    except OSError as exc:
        raise Jin10CredentialError("unable to persist Jin10 activation") from exc


def _generation(value: object) -> str:
    if not isinstance(value, str) or _GENERATION_RE.fullmatch(value) is None:
        raise Jin10CredentialError("Jin10 credential generation is invalid")
    return value


def _digest(value: object) -> str:
    if not isinstance(value, str) or _HASH_RE.fullmatch(value) is None:
        raise Jin10CredentialError("Jin10 attestation binding is invalid")
    return value


def _utc_datetime(value: object) -> datetime:
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError as exc:
            raise Jin10CredentialError("Jin10 activation time is invalid") from exc
    elif isinstance(value, datetime):
        parsed = value
    else:
        raise Jin10CredentialError("Jin10 activation time is invalid")
    if (
        parsed.tzinfo is None
        or parsed.utcoffset() is None
        or parsed.utcoffset() != timedelta(0)
    ):
        raise Jin10CredentialError("Jin10 activation time is invalid")
    return parsed.astimezone(timezone.utc)


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate field")
        result[key] = value
    return result


def _reject_constant(value: str) -> object:
    raise ValueError(f"non-finite JSON constant is forbidden: {value}")


__all__ = [
    "ActivatedJin10SecretStore",
    "JIN10_ACTIVATION_SCHEMA",
    "JIN10_CREDENTIAL_SCHEMA",
    "JIN10_ROTATION_EVIDENCE_PARTS",
    "Jin10ActivationManifest",
    "Jin10CredentialError",
    "Jin10CredentialResolution",
    "activate_jin10_credential",
    "encode_jin10_credential",
    "jin10_rotation_evidence_dir",
    "resolve_jin10_credential",
]
