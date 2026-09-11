"""Immutable, human-signed governance contracts for Options Copilot.

The signature used here is an explicit human attestation (actor and UTC time)
bound into a SHA-256 content hash.  It deliberately does not use, request, or
store private keys or credentials.  Contract files are written with exclusive
creation and corrections form a hash-linked append-only version chain.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import json
import os
from pathlib import Path
import re
from types import MappingProxyType

from options_copilot.storage.canonical import (
    canonical_hash,
    canonical_json,
    datetime_text,
    utc_datetime,
)


CONTRACT_SCHEMA = "options_copilot.governance.signed_contract.v1"
_HASH_RE = re.compile(r"[0-9a-f]{64}\Z")
_VERSION_RE = re.compile(r"v([1-9][0-9]*)(?:\.([0-9]+))*\Z")
_TOP_LEVEL_FIELDS = frozenset(
    {
        "schema",
        "contract_kind",
        "version",
        "effective_at",
        "provenance",
        "payload",
        "actor",
        "signed_at",
        "supersedes_version",
        "supersedes_hash",
        "contract_hash",
    }
)
_PROVENANCE_FIELDS = frozenset({"source", "source_hash", "observed_at"})
_SECRET_FIELD_NAMES = frozenset(
    {
        "access_key",
        "api_key",
        "api_token",
        "authorization",
        "credential",
        "credentials",
        "password",
        "private_key",
        "secret",
        "secret_key",
        "token",
    }
)


class ContractKind(str, Enum):
    STRATEGY_NAV = "STRATEGY_NAV"
    EXECUTION_COST = "EXECUTION_COST"
    INITIAL_CHAMPION_SCENARIO_POLICY = "INITIAL_CHAMPION_SCENARIO_POLICY"

    @classmethod
    def parse(cls, value: "ContractKind | str") -> "ContractKind":
        if isinstance(value, cls):
            return value
        if not isinstance(value, str) or not value.strip():
            raise ContractValidationError("contract_kind must be a nonblank string")
        normalized = re.sub(r"[^A-Za-z0-9]+", "_", value.strip()).strip("_").upper()
        aliases = {
            "INITIAL_POLICY": cls.INITIAL_CHAMPION_SCENARIO_POLICY.value,
            "CHAMPION_SCENARIO_POLICY": cls.INITIAL_CHAMPION_SCENARIO_POLICY.value,
        }
        normalized = aliases.get(normalized, normalized)
        try:
            return cls(normalized)
        except ValueError as exc:
            allowed = ", ".join(item.value for item in cls)
            raise ContractValidationError(
                f"unsupported contract kind {value!r}; expected one of {allowed}"
            ) from exc


class ContractError(RuntimeError):
    """Base error for governance contract operations."""


class ContractValidationError(ContractError):
    """A contract is incomplete, malformed, tampered with, or mismatched."""


class ContractWriteError(ContractError):
    """An append-only contract artifact could not be created safely."""


@dataclass(frozen=True, slots=True)
class SignedContract:
    """One immutable, canonically hashed human governance attestation."""

    schema: str
    contract_kind: ContractKind
    version: str
    effective_at: datetime
    provenance: Mapping[str, object]
    payload: Mapping[str, object]
    actor: str
    signed_at: datetime
    supersedes_version: str | None
    supersedes_hash: str | None
    contract_hash: str

    def __post_init__(self) -> None:
        if self.schema != CONTRACT_SCHEMA:
            raise ContractValidationError(
                f"schema must be {CONTRACT_SCHEMA!r}"
            )
        kind = ContractKind.parse(self.contract_kind)
        version = _version("version", self.version)
        effective_at = _timestamp("effective_at", self.effective_at)
        provenance = _provenance(self.provenance)
        payload = _payload(self.payload)
        actor = _actor(self.actor)
        signed_at = _timestamp("signed_at", self.signed_at)
        supersedes_version, supersedes_hash = _supersession(
            version,
            self.supersedes_version,
            self.supersedes_hash,
        )
        contract_hash = _digest("contract_hash", self.contract_hash)

        object.__setattr__(self, "contract_kind", kind)
        object.__setattr__(self, "version", version)
        object.__setattr__(self, "effective_at", effective_at)
        object.__setattr__(self, "provenance", _freeze(provenance))
        object.__setattr__(self, "payload", _freeze(payload))
        object.__setattr__(self, "actor", actor)
        object.__setattr__(self, "signed_at", signed_at)
        object.__setattr__(self, "supersedes_version", supersedes_version)
        object.__setattr__(self, "supersedes_hash", supersedes_hash)
        object.__setattr__(self, "contract_hash", contract_hash)

        actual_hash = canonical_hash(self.signable_dict())
        if actual_hash != contract_hash:
            raise ContractValidationError(
                "contract hash mismatch: artifact was mutated or signed incorrectly"
            )

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> "SignedContract":
        if not isinstance(value, Mapping):
            raise ContractValidationError("contract document must be a JSON object")
        missing = sorted(_TOP_LEVEL_FIELDS.difference(value))
        if missing:
            raise ContractValidationError(
                f"contract is missing required field {missing[0]}"
            )
        unknown = sorted(set(value).difference(_TOP_LEVEL_FIELDS))
        if unknown:
            raise ContractValidationError(
                f"contract contains unknown field {unknown[0]}"
            )
        return cls(
            schema=value["schema"],
            contract_kind=value["contract_kind"],
            version=value["version"],
            effective_at=value["effective_at"],
            provenance=value["provenance"],
            payload=value["payload"],
            actor=value["actor"],
            signed_at=value["signed_at"],
            supersedes_version=value["supersedes_version"],
            supersedes_hash=value["supersedes_hash"],
            contract_hash=value["contract_hash"],
        )

    @property
    def content_hash(self) -> str:
        """Compatibility name for consumers that call the binding content_hash."""

        return self.contract_hash

    @property
    def kind(self) -> ContractKind:
        return self.contract_kind

    def signable_dict(self) -> dict[str, object]:
        """Return every signed field except the hash that attests to them."""

        return {
            "schema": self.schema,
            "contract_kind": self.contract_kind.value,
            "version": self.version,
            "effective_at": datetime_text(self.effective_at),
            "provenance": _thaw(self.provenance),
            "payload": _thaw(self.payload),
            "actor": self.actor,
            "signed_at": datetime_text(self.signed_at),
            "supersedes_version": self.supersedes_version,
            "supersedes_hash": self.supersedes_hash,
        }

    def to_dict(self) -> dict[str, object]:
        document = self.signable_dict()
        document["contract_hash"] = self.contract_hash
        return document

    as_dict = to_dict

    def verify(self, **expectations: object) -> "SignedContract":
        return verify_contract(self, **expectations)


def sign_contract(
    *,
    kind: ContractKind | str,
    version: str,
    effective_at: datetime | str,
    provenance: Mapping[str, object],
    payload: Mapping[str, object],
    actor: str,
    signed_at: datetime | str,
    supersedes_version: str | None = None,
    supersedes_hash: str | None = None,
) -> SignedContract:
    """Create an in-memory contract without accepting any credential material."""

    parsed_kind = ContractKind.parse(kind)
    parsed_version = _version("version", version)
    parsed_effective_at = _timestamp("effective_at", effective_at)
    parsed_provenance = _provenance(provenance)
    parsed_payload = _payload(payload)
    parsed_actor = _actor(actor)
    parsed_signed_at = _timestamp("signed_at", signed_at)
    prior_version, prior_hash = _supersession(
        parsed_version,
        supersedes_version,
        supersedes_hash,
    )
    signable: dict[str, object] = {
        "schema": CONTRACT_SCHEMA,
        "contract_kind": parsed_kind.value,
        "version": parsed_version,
        "effective_at": datetime_text(parsed_effective_at),
        "provenance": parsed_provenance,
        "payload": parsed_payload,
        "actor": parsed_actor,
        "signed_at": datetime_text(parsed_signed_at),
        "supersedes_version": prior_version,
        "supersedes_hash": prior_hash,
    }
    return SignedContract.from_dict(
        {**signable, "contract_hash": canonical_hash(signable)}
    )


def create_correction(
    prior: SignedContract | Mapping[str, object],
    *,
    version: str,
    effective_at: datetime | str,
    provenance: Mapping[str, object],
    payload: Mapping[str, object],
    actor: str,
    signed_at: datetime | str,
) -> SignedContract:
    """Create a later hash-linked version without changing the prior object."""

    previous = verify_contract(prior)
    correction_signed_at = _timestamp("signed_at", signed_at)
    if correction_signed_at < previous.signed_at:
        raise ContractValidationError(
            "correction signed_at cannot predate the prior contract signature"
        )
    correction = sign_contract(
        kind=previous.contract_kind,
        version=version,
        effective_at=effective_at,
        provenance=provenance,
        payload=payload,
        actor=actor,
        signed_at=correction_signed_at,
        supersedes_version=previous.version,
        supersedes_hash=previous.contract_hash,
    )
    return verify_correction(previous, correction)


def verify_contract(
    contract: SignedContract | Mapping[str, object],
    *,
    expected_kind: ContractKind | str | None = None,
    expected_version: str | None = None,
    expected_hash: str | None = None,
    expected_signer: str | None = None,
    expected_effective_at: datetime | str | None = None,
    as_of: datetime | str | None = None,
) -> SignedContract:
    """Validate the signature plus any identity expected by a consumer."""

    value = (
        contract
        if isinstance(contract, SignedContract)
        else SignedContract.from_dict(contract)
    )
    # Reconstructing rechecks the hash even when an existing object is supplied.
    value = SignedContract.from_dict(value.to_dict())
    if expected_kind is not None and value.contract_kind is not ContractKind.parse(
        expected_kind
    ):
        raise ContractValidationError(
            f"contract kind mismatch: expected {ContractKind.parse(expected_kind).value}, "
            f"got {value.contract_kind.value}"
        )
    if expected_version is not None:
        expected = _version("expected_version", expected_version)
        if value.version != expected:
            raise ContractValidationError(
                f"contract version mismatch: expected {expected}, got {value.version}"
            )
    if expected_hash is not None:
        expected = _digest("expected_hash", expected_hash)
        if value.contract_hash != expected:
            raise ContractValidationError(
                "contract hash mismatch for consumer expectation"
            )
    if expected_signer is not None:
        expected = _actor(expected_signer, field="expected_signer")
        if value.actor != expected:
            raise ContractValidationError(
                f"contract signer mismatch: expected {expected!r}, got {value.actor!r}"
            )
    if expected_effective_at is not None:
        expected = _timestamp("expected_effective_at", expected_effective_at)
        if value.effective_at != expected:
            raise ContractValidationError(
                "contract effective time mismatch for consumer expectation"
            )
    if as_of is not None:
        observation_time = _timestamp("as_of", as_of)
        if value.effective_at > observation_time:
            raise ContractValidationError("contract is not effective at requested time")
        if value.signed_at > observation_time:
            raise ContractValidationError("contract signature is later than requested time")
    return value


def require_contract(
    contract: SignedContract | Mapping[str, object],
    *,
    expected_kind: ContractKind | str,
    expected_version: str,
    expected_hash: str,
    expected_signer: str,
    expected_effective_at: datetime | str,
    as_of: datetime | str | None = None,
) -> SignedContract:
    """Consumer-facing helper that makes every governance binding explicit."""

    return verify_contract(
        contract,
        expected_kind=expected_kind,
        expected_version=expected_version,
        expected_hash=expected_hash,
        expected_signer=expected_signer,
        expected_effective_at=expected_effective_at,
        as_of=as_of,
    )


def verify_correction(
    prior: SignedContract | Mapping[str, object],
    correction: SignedContract | Mapping[str, object],
) -> SignedContract:
    previous = verify_contract(prior)
    current = verify_contract(correction)
    if current.contract_kind is not previous.contract_kind:
        raise ContractValidationError("correction contract kind differs from prior kind")
    if current.supersedes_version != previous.version:
        raise ContractValidationError(
            "correction supersedes_version does not identify the prior version"
        )
    if current.supersedes_hash != previous.contract_hash:
        raise ContractValidationError(
            "correction supersedes_hash does not identify the prior artifact"
        )
    if _version_parts(current.version) <= _version_parts(previous.version):
        raise ContractValidationError("correction version must advance the prior version")
    if current.signed_at < previous.signed_at:
        raise ContractValidationError("correction signature predates prior contract")
    return current


def load_contract(
    path: str | Path,
    **expectations: object,
) -> SignedContract:
    contract_path = Path(path)
    try:
        raw = contract_path.read_text(encoding="utf-8")
    except FileNotFoundError:
        raise
    except OSError as exc:
        raise ContractWriteError(f"cannot read contract {contract_path}: {exc}") from exc
    try:
        document = json.loads(raw, parse_constant=_reject_json_constant)
    except (json.JSONDecodeError, ValueError) as exc:
        raise ContractValidationError(
            f"invalid contract JSON at {contract_path}: {exc}"
        ) from exc
    if not isinstance(document, Mapping):
        raise ContractValidationError("contract document must be a JSON object")
    return verify_contract(document, **expectations)


def write_contract(contract: SignedContract, path: str | Path) -> Path:
    """Persist a contract once; an existing path is never replaced."""

    value = verify_contract(contract)
    contract_path = Path(path)
    rendered = json.dumps(
        value.to_dict(),
        ensure_ascii=False,
        allow_nan=False,
        indent=2,
        sort_keys=True,
    ) + "\n"
    contract_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with contract_path.open("x", encoding="utf-8", newline="\n") as stream:
            stream.write(rendered)
            stream.flush()
            os.fsync(stream.fileno())
    except FileExistsError as exc:
        raise ContractWriteError(
            f"contract path already exists and cannot be overwritten: {contract_path}"
        ) from exc
    except OSError as exc:
        raise ContractWriteError(
            f"cannot create contract artifact {contract_path}: {exc}"
        ) from exc
    return contract_path


def write_correction(
    prior_path: str | Path,
    output_path: str | Path,
    *,
    version: str,
    effective_at: datetime | str,
    provenance: Mapping[str, object],
    payload: Mapping[str, object],
    actor: str,
    signed_at: datetime | str,
) -> SignedContract:
    """Load a prior artifact and exclusively create its next version elsewhere."""

    prior_location = Path(prior_path)
    output_location = Path(output_path)
    if prior_location.resolve() == output_location.resolve():
        raise ContractWriteError("a correction cannot overwrite its prior contract path")
    prior = load_contract(prior_location)
    correction = create_correction(
        prior,
        version=version,
        effective_at=effective_at,
        provenance=provenance,
        payload=payload,
        actor=actor,
        signed_at=signed_at,
    )
    write_contract(correction, output_location)
    return correction


def canonical_contract_hash(value: Mapping[str, object]) -> str:
    """Hash a signable contract mapping, ignoring only ``contract_hash``."""

    if not isinstance(value, Mapping):
        raise ContractValidationError("contract hash input must be a mapping")
    body = dict(value)
    body.pop("contract_hash", None)
    try:
        return canonical_hash(body)
    except (TypeError, ValueError) as exc:
        raise ContractValidationError(f"contract cannot be canonically hashed: {exc}") from exc


def _version(field: str, value: object) -> str:
    if not isinstance(value, str) or _VERSION_RE.fullmatch(value) is None:
        raise ContractValidationError(
            f"{field} must be an immutable version identifier such as v1 or v2.1"
        )
    return value


def _version_parts(value: str) -> tuple[int, ...]:
    _version("version", value)
    return tuple(int(item) for item in value[1:].split("."))


def _digest(field: str, value: object) -> str:
    if not isinstance(value, str) or _HASH_RE.fullmatch(value) is None:
        raise ContractValidationError(
            f"{field} must be a lowercase 64-character SHA-256 hash"
        )
    return value


def _actor(value: object, *, field: str = "actor") -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value) > 160
        or any(ord(character) < 32 for character in value)
    ):
        raise ContractValidationError(
            f"{field} must identify a nonblank human actor without control characters"
        )
    return value


def _timestamp(field: str, value: object) -> datetime:
    if isinstance(value, str):
        text = value.strip()
        if not text:
            raise ContractValidationError(f"{field} must be a timezone-aware timestamp")
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            value = datetime.fromisoformat(text)
        except ValueError as exc:
            raise ContractValidationError(
                f"{field} must be an ISO-8601 timestamp"
            ) from exc
    try:
        return utc_datetime(value, field=field)
    except (TypeError, ValueError) as exc:
        raise ContractValidationError(str(exc)) from exc


def _provenance(value: object) -> dict[str, object]:
    normalized = _mapping("provenance", value)
    missing = sorted(_PROVENANCE_FIELDS.difference(normalized))
    if missing:
        raise ContractValidationError(
            f"provenance is missing required field {missing[0]}"
        )
    source = normalized["source"]
    if (
        not isinstance(source, str)
        or not source.strip()
        or source != source.strip()
    ):
        raise ContractValidationError("provenance source must be a nonblank string")
    normalized["source_hash"] = _digest(
        "provenance source_hash", normalized["source_hash"]
    )
    normalized["observed_at"] = datetime_text(
        _timestamp("provenance observed_at", normalized["observed_at"])
    )
    _reject_secrets(normalized, path="provenance")
    return normalized


def _payload(value: object) -> dict[str, object]:
    normalized = _mapping("payload", value)
    _reject_secrets(normalized, path="payload")
    return normalized


def _mapping(field: str, value: object) -> dict[str, object]:
    if not isinstance(value, Mapping) or not value:
        raise ContractValidationError(f"{field} must be a nonempty JSON object")
    try:
        normalized = json.loads(canonical_json(value))
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ContractValidationError(f"{field} is not canonical JSON: {exc}") from exc
    if not isinstance(normalized, dict):  # pragma: no cover - Mapping guarantees this
        raise ContractValidationError(f"{field} must be a JSON object")
    return normalized


def _supersession(
    version: str,
    supersedes_version: object,
    supersedes_hash: object,
) -> tuple[str | None, str | None]:
    if supersedes_version is None and supersedes_hash is None:
        if version != "v1":
            raise ContractValidationError(
                "a contract after v1 requires supersedes_version and supersedes_hash"
            )
        return None, None
    if supersedes_version is None:
        raise ContractValidationError(
            "supersedes_version is required when supersedes_hash is present"
        )
    if supersedes_hash is None:
        raise ContractValidationError(
            "supersedes_hash is required when supersedes_version is present"
        )
    prior_version = _version("supersedes_version", supersedes_version)
    prior_hash = _digest("supersedes_hash", supersedes_hash)
    if _version_parts(version) <= _version_parts(prior_version):
        raise ContractValidationError(
            "correction version must advance supersedes_version"
        )
    return prior_version, prior_hash


def _reject_secrets(value: object, *, path: str) -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            normalized = re.sub(r"[^a-z0-9]+", "_", str(key).lower()).strip("_")
            if normalized in _SECRET_FIELD_NAMES or any(
                normalized.endswith(f"_{suffix}")
                for suffix in ("password", "secret", "token", "private_key")
            ):
                raise ContractValidationError(
                    f"secret-like field {path}.{key} cannot be signed"
                )
            _reject_secrets(item, path=f"{path}.{key}")
    elif isinstance(value, Sequence) and not isinstance(
        value, (str, bytes, bytearray, memoryview)
    ):
        for index, item in enumerate(value):
            _reject_secrets(item, path=f"{path}[{index}]")


def _freeze(value: object) -> object:
    if isinstance(value, dict):
        return MappingProxyType({key: _freeze(item) for key, item in value.items()})
    if isinstance(value, list):
        return tuple(_freeze(item) for item in value)
    return value


def _thaw(value: object) -> object:
    if isinstance(value, Mapping):
        return {str(key): _thaw(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw(item) for item in value]
    return value


def _reject_json_constant(value: str) -> object:
    raise ValueError(f"non-finite JSON number {value!r} is prohibited")


__all__ = [
    "CONTRACT_SCHEMA",
    "ContractError",
    "ContractKind",
    "ContractValidationError",
    "ContractWriteError",
    "SignedContract",
    "canonical_contract_hash",
    "create_correction",
    "load_contract",
    "require_contract",
    "sign_contract",
    "verify_contract",
    "verify_correction",
    "write_contract",
    "write_correction",
]
