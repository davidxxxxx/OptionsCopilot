"""Canonical, zero-credential evidence for human secret revocation.

The exposed Jin10 credential is never read by this module.  A human may attest
that it was revoked in the provider UI; the replacement setter then consumes
that exact, hash-verified artifact once.  Both attestation and rotation files
are created exclusively so existing evidence is never overwritten.
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import re
from typing import IO

from options_copilot.storage.canonical import (
    canonical_hash as _canonical_hash,
    canonical_json,
    datetime_text,
)


REVOCATION_SCHEMA = "options_copilot.security.revocation_attestation.v1"
REVOCATION_VERSION = 1
JIN10_SECRET_NAME = "JIN10_MCP_TOKEN"
ROTATION_MANIFEST_GLOB = "rotation-*.json"

_ATTESTATION_FIELDS = frozenset(
    {
        "schema",
        "version",
        "name",
        "old_token_revoked",
        "actor",
        "signer",
        "signed_at",
        "canonical_hash",
    }
)
_ROTATION_FIELDS = frozenset({"name", "attestation_hash", "rotated_at"})
_HUMAN_ACTOR_RE = re.compile(r"human:[A-Za-z0-9][A-Za-z0-9._@-]{0,79}\Z")
_HASH_RE = re.compile(r"[0-9a-f]{64}\Z")
_FORBIDDEN_FIELD_RE = re.compile(
    r"(?:cipher(?:text)?|fingerprint|authorization|headers?|secret[_-]?(?:value|fragment)|token[_-]?(?:value|fragment))",
    re.IGNORECASE,
)


class RevocationError(RuntimeError):
    """Base class for invalid, unsafe, or unwritable revocation evidence."""


class RevocationValidationError(RevocationError):
    """An attestation or rotation manifest failed closed validation."""


class RevocationWriteError(RevocationError):
    """Append-only evidence could not be created without overwriting data."""


class RevocationReplayError(RevocationError):
    """An attestation has already been consumed or is being consumed."""


@dataclass(frozen=True, slots=True)
class RevocationAttestation:
    """One explicit human statement that the previously exposed token is revoked."""

    schema: str
    version: int
    name: str
    old_token_revoked: bool
    actor: str
    signer: str
    signed_at: datetime
    canonical_hash: str

    def __post_init__(self) -> None:
        if self.schema != REVOCATION_SCHEMA:
            raise RevocationValidationError(
                f"schema must be {REVOCATION_SCHEMA!r}"
            )
        if self.version != REVOCATION_VERSION or isinstance(self.version, bool):
            raise RevocationValidationError(
                f"version must be {REVOCATION_VERSION}"
            )
        if self.name != JIN10_SECRET_NAME:
            raise RevocationValidationError(
                f"revocation attestation name must be {JIN10_SECRET_NAME}"
            )
        if self.old_token_revoked is not True:
            raise RevocationValidationError("old_token_revoked must be true")
        actor = _human_actor("actor", self.actor)
        signer = _human_actor("signer", self.signer)
        if signer != actor:
            raise RevocationValidationError("signer must equal the human actor")
        signed_at = _utc_timestamp("signed_at", self.signed_at)
        digest = _digest("canonical_hash", self.canonical_hash)

        object.__setattr__(self, "actor", actor)
        object.__setattr__(self, "signer", signer)
        object.__setattr__(self, "signed_at", signed_at)
        object.__setattr__(self, "canonical_hash", digest)

        if _canonical_hash(self.signable_dict()) != digest:
            raise RevocationValidationError(
                "canonical hash mismatch: revocation attestation was tampered with"
            )

    @classmethod
    def create(
        cls,
        *,
        name: str,
        actor: str,
        signed_at: datetime | None = None,
    ) -> "RevocationAttestation":
        timestamp = _utc_timestamp(
            "signed_at", signed_at or datetime.now(timezone.utc)
        )
        human = _human_actor("actor", actor)
        signable: dict[str, object] = {
            "schema": REVOCATION_SCHEMA,
            "version": REVOCATION_VERSION,
            "name": name,
            "old_token_revoked": True,
            "actor": human,
            "signer": human,
            "signed_at": datetime_text(timestamp),
        }
        return cls.from_dict(
            {**signable, "canonical_hash": _canonical_hash(signable)}
        )

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> "RevocationAttestation":
        document = _strict_document(
            value, expected_fields=_ATTESTATION_FIELDS, kind="revocation attestation"
        )
        return cls(
            schema=document["schema"],
            version=document["version"],
            name=document["name"],
            old_token_revoked=document["old_token_revoked"],
            actor=document["actor"],
            signer=document["signer"],
            signed_at=document["signed_at"],
            canonical_hash=document["canonical_hash"],
        )

    def signable_dict(self) -> dict[str, object]:
        return {
            "schema": self.schema,
            "version": self.version,
            "name": self.name,
            "old_token_revoked": self.old_token_revoked,
            "actor": self.actor,
            "signer": self.signer,
            "signed_at": datetime_text(self.signed_at),
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.signable_dict(), "canonical_hash": self.canonical_hash}


@dataclass(frozen=True, slots=True)
class RotationManifest:
    """Zero-material record that one exact attestation gated a rotation."""

    name: str
    attestation_hash: str
    rotated_at: datetime

    def __post_init__(self) -> None:
        if self.name != JIN10_SECRET_NAME:
            raise RevocationValidationError(
                f"rotation manifest name must be {JIN10_SECRET_NAME}"
            )
        object.__setattr__(
            self, "attestation_hash", _digest("attestation_hash", self.attestation_hash)
        )
        object.__setattr__(
            self, "rotated_at", _utc_timestamp("rotated_at", self.rotated_at)
        )

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> "RotationManifest":
        document = _strict_document(
            value, expected_fields=_ROTATION_FIELDS, kind="rotation manifest"
        )
        return cls(
            name=document["name"],
            attestation_hash=document["attestation_hash"],
            rotated_at=document["rotated_at"],
        )

    def to_dict(self) -> dict[str, object]:
        # Deliberately exactly three fields: no credential value or derivative.
        return {
            "name": self.name,
            "attestation_hash": self.attestation_hash,
            "rotated_at": datetime_text(self.rotated_at),
        }


class RotationReservation:
    """Cross-process one-use reservation for one exact attestation hash."""

    def __init__(
        self,
        *,
        attestation: RevocationAttestation,
        directory: Path,
        pending_path: Path,
        pending_stream: IO[str],
    ) -> None:
        self.attestation = attestation
        self.directory = directory
        self.pending_path = pending_path
        self._pending_stream = pending_stream
        self._committed = False

    @property
    def manifest_path(self) -> Path:
        return self.directory / f"rotation-{self.attestation.canonical_hash}.json"

    def commit(self, *, rotated_at: datetime | None = None) -> Path:
        if self._committed:
            raise RevocationReplayError("rotation reservation was already committed")
        manifest = RotationManifest(
            name=self.attestation.name,
            attestation_hash=self.attestation.canonical_hash,
            rotated_at=rotated_at or datetime.now(timezone.utc),
        )
        try:
            _write_exclusive_json(self.manifest_path, manifest.to_dict())
        except RevocationWriteError as exc:
            raise RevocationReplayError(
                "revocation attestation was already consumed"
            ) from exc
        self._committed = True
        self._close_pending(remove=True)
        return self.manifest_path

    def abort(self) -> None:
        if not self._committed:
            self._close_pending(remove=True)

    def _close_pending(self, *, remove: bool) -> None:
        if not self._pending_stream.closed:
            self._pending_stream.close()
        if remove:
            try:
                self.pending_path.unlink(missing_ok=True)
            except OSError as exc:
                raise RevocationWriteError(
                    "unable to release the revocation rotation reservation"
                ) from exc

    def __enter__(self) -> "RotationReservation":
        return self

    def __exit__(self, *_: object) -> None:
        self.abort()


def load_revocation_attestation(
    path: str | Path,
    *,
    expected_name: str = JIN10_SECRET_NAME,
) -> RevocationAttestation:
    document = _load_json(Path(path), kind="revocation attestation")
    attestation = RevocationAttestation.from_dict(document)
    if attestation.name != expected_name:
        raise RevocationValidationError(
            f"revocation attestation name mismatch: expected {expected_name}"
        )
    return attestation


def write_revocation_attestation(
    attestation: RevocationAttestation,
    evidence_dir: str | Path,
) -> Path:
    value = RevocationAttestation.from_dict(attestation.to_dict())
    stamp = value.signed_at.strftime("%Y%m%dT%H%M%S%fZ")
    path = Path(evidence_dir).resolve() / (
        f"revocation-{stamp}-{value.canonical_hash}.json"
    )
    _write_exclusive_json(path, value.to_dict())
    return path


def reserve_rotation(
    attestation: RevocationAttestation,
    evidence_dir: str | Path,
) -> RotationReservation:
    value = RevocationAttestation.from_dict(attestation.to_dict())
    directory = Path(evidence_dir).resolve()
    directory.mkdir(parents=True, exist_ok=True)
    manifest_path = directory / f"rotation-{value.canonical_hash}.json"
    pending_path = directory / f".rotation-{value.canonical_hash}.pending"
    if manifest_path.exists():
        raise RevocationReplayError("revocation attestation was already consumed")
    try:
        pending_stream = pending_path.open("x", encoding="utf-8", newline="\n")
    except FileExistsError as exc:
        raise RevocationReplayError(
            "revocation attestation is already being consumed"
        ) from exc
    except OSError as exc:
        raise RevocationWriteError(
            "unable to reserve revocation attestation for rotation"
        ) from exc
    try:
        pending_stream.write(value.canonical_hash + "\n")
        pending_stream.flush()
        os.fsync(pending_stream.fileno())
    except OSError:
        pending_stream.close()
        pending_path.unlink(missing_ok=True)
        raise
    return RotationReservation(
        attestation=value,
        directory=directory,
        pending_path=pending_path,
        pending_stream=pending_stream,
    )


def load_rotation_manifests(
    evidence_dir: str | Path,
    *,
    expected_name: str = JIN10_SECRET_NAME,
) -> tuple[RotationManifest, ...]:
    directory = Path(evidence_dir).resolve()
    if not directory.exists():
        return ()
    manifests: list[RotationManifest] = []
    attestations = _attestations_by_hash(directory)
    for path in sorted(directory.glob(ROTATION_MANIFEST_GLOB)):
        manifest = RotationManifest.from_dict(
            _load_json(path, kind="rotation manifest")
        )
        if manifest.name != expected_name:
            continue
        if manifest.attestation_hash not in attestations:
            raise RevocationValidationError(
                "rotation manifest has no matching verified revocation attestation"
            )
        manifests.append(manifest)
    manifests.sort(key=lambda value: (value.rotated_at, value.attestation_hash))
    return tuple(manifests)


def latest_rotation_manifest(
    evidence_dir: str | Path,
    *,
    expected_name: str = JIN10_SECRET_NAME,
) -> RotationManifest | None:
    manifests = load_rotation_manifests(evidence_dir, expected_name=expected_name)
    return manifests[-1] if manifests else None


def _attestations_by_hash(directory: Path) -> dict[str, RevocationAttestation]:
    result: dict[str, RevocationAttestation] = {}
    for path in sorted(directory.glob("revocation-*.json")):
        attestation = load_revocation_attestation(path)
        if attestation.canonical_hash in result:
            raise RevocationValidationError(
                "duplicate revocation attestation hash in evidence directory"
            )
        result[attestation.canonical_hash] = attestation
    return result


def _write_exclusive_json(path: Path, document: Mapping[str, object]) -> None:
    rendered = canonical_json(document) + "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("x", encoding="utf-8", newline="\n") as stream:
            stream.write(rendered)
            stream.flush()
            os.fsync(stream.fileno())
    except FileExistsError as exc:
        raise RevocationWriteError(
            "append-only evidence path already exists and cannot be overwritten"
        ) from exc
    except OSError as exc:
        raise RevocationWriteError("unable to create append-only evidence") from exc


def _load_json(path: Path, *, kind: str) -> Mapping[str, object]:
    try:
        raw = path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise RevocationValidationError(f"{kind} file does not exist") from exc
    except OSError as exc:
        raise RevocationValidationError(f"unable to read {kind} file") from exc
    try:
        document = json.loads(
            raw,
            object_pairs_hook=_unique_object,
            parse_constant=_reject_json_constant,
        )
    except (json.JSONDecodeError, ValueError) as exc:
        raise RevocationValidationError(f"invalid {kind} JSON") from exc
    if not isinstance(document, Mapping):
        raise RevocationValidationError(f"{kind} must be a JSON object")
    return document


def _strict_document(
    value: Mapping[str, object],
    *,
    expected_fields: frozenset[str],
    kind: str,
) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise RevocationValidationError(f"{kind} must be a JSON object")
    for field in value:
        if not isinstance(field, str):
            raise RevocationValidationError(f"{kind} field names must be strings")
        if field != "old_token_revoked" and _FORBIDDEN_FIELD_RE.search(field):
            raise RevocationValidationError(
                f"{kind} contains forbidden credential-material field"
            )
    missing = sorted(expected_fields.difference(value))
    if missing:
        raise RevocationValidationError(
            f"{kind} is missing required field {missing[0]}"
        )
    unknown = sorted(set(value).difference(expected_fields))
    if unknown:
        raise RevocationValidationError(f"{kind} contains unknown field {unknown[0]}")
    return dict(value)


def _human_actor(field: str, value: object) -> str:
    if not isinstance(value, str) or _HUMAN_ACTOR_RE.fullmatch(value) is None:
        raise RevocationValidationError(
            f"{field} must identify an explicit human as human:<id>"
        )
    return value


def _utc_timestamp(field: str, value: object) -> datetime:
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError as exc:
            raise RevocationValidationError(f"{field} must be an ISO-8601 UTC time") from exc
    elif isinstance(value, datetime):
        parsed = value
    else:
        raise RevocationValidationError(f"{field} must be an ISO-8601 UTC time")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise RevocationValidationError(f"{field} must be timezone-aware UTC")
    if parsed.utcoffset() != timedelta(0):
        raise RevocationValidationError(f"{field} must be UTC")
    return parsed.astimezone(timezone.utc)


def _digest(field: str, value: object) -> str:
    if not isinstance(value, str) or _HASH_RE.fullmatch(value) is None:
        raise RevocationValidationError(
            f"{field} must be a lowercase 64-character SHA-256 hash"
        )
    return value


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON object field")
        result[key] = value
    return result


def _reject_json_constant(value: str) -> object:
    raise ValueError(f"non-finite JSON constant is forbidden: {value}")


__all__ = [
    "JIN10_SECRET_NAME",
    "REVOCATION_SCHEMA",
    "REVOCATION_VERSION",
    "RevocationAttestation",
    "RevocationError",
    "RevocationReplayError",
    "RevocationValidationError",
    "RevocationWriteError",
    "RotationManifest",
    "RotationReservation",
    "latest_rotation_manifest",
    "load_revocation_attestation",
    "load_rotation_manifests",
    "reserve_rotation",
    "write_revocation_attestation",
]
