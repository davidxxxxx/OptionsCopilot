"""Load a human-approved market-data pacing capability, fail closed.

The P0 checkpoint consists of two independently hashed JSON artifacts:
``capability.json`` contains the request budgets and ``approval.json`` binds
that exact capability to an expected human actor and an ``APPROVED`` decision.
This module is read-only and deliberately contains no broker operations.
"""
from __future__ import annotations

import base64
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
import json
from pathlib import Path
import re
from types import MappingProxyType
from typing import Any, Protocol

from options_copilot.operations.capabilities import (
    CapabilityRecord,
    CapabilityStatus,
    DEFAULT_PACING_MAX_AGE,
    MarketDataPacingCapability,
)
from options_copilot.storage.canonical import (
    canonical_hash,
    canonical_json,
    datetime_text,
    thaw_json,
    utc_datetime,
)


_HASH_RE = re.compile(r"[0-9a-f]{64}\Z")
_KEY_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,159}\Z")
_SIGNATURE_RE = re.compile(r"[A-Za-z0-9_+/=-]{32,2048}\Z")
PACING_PRODUCTION_HUMAN = "PACING_PRODUCTION_HUMAN"
PACING_TEST_ONLY = "TEST_ONLY"
PACING_KEYRING_SCHEMA = "options_copilot.pacing_authority_keyring.v1"
PACING_EXPECTED_ACTOR = "human:xujie"
PACING_POLICY_AUTHORITY_KIND = "market_data_pacing_policy_authority"
PACING_POLICY_AUTHORITY_MODE = "LONG_LIVED_UNTIL_REVOKED"
PACING_POLICY_REVOCATION_KIND = "market_data_pacing_policy_revocation"
_KEYRING_FIELDS = frozenset(
    {
        "schema",
        "trust_domain",
        "signer_key_id",
        "signature_algorithm",
        "public_key_base64",
        "content_hash",
    }
)
_APPROVAL_REQUIRED_FIELDS_V1 = frozenset(
    {
        "schema_version",
        "kind",
        "actor",
        "signer",
        "decision",
        "signed_at",
        "observed_at",
        "version",
        "source",
        "capability",
        "capability_content_hash",
        "content_hash",
        "review_only",
        "direct_order_submission",
        "signer_key_id",
        "signature_algorithm",
        "approval_signature",
    }
)
_APPROVAL_REQUIRED_FIELDS_V2 = frozenset(
    {
        *_APPROVAL_REQUIRED_FIELDS_V1,
        "authority_mode",
        "policy_content_hash",
    }
)
_REVOCATION_REQUIRED_FIELDS = frozenset(
    {
        "schema_version",
        "kind",
        "actor",
        "signer",
        "decision",
        "revoked_at",
        "reason",
        "policy_content_hash",
        "approval_content_hash",
        "review_only",
        "direct_order_submission",
        "signer_key_id",
        "signature_algorithm",
        "revocation_signature",
        "content_hash",
    }
)


class PacingAuthoritySignatureVerifier(Protocol):
    """Verify an approval with independently installed public-key material."""

    trust_domain: str
    keyring_hash: str

    def verify(
        self,
        *,
        signer_key_id: str,
        signature_algorithm: str,
        message: bytes,
        signature: str,
    ) -> bool: ...


class Ed25519PacingAuthorityVerifier:
    """Immutable production verifier built only from a separate keyring file."""

    __slots__ = ("_public_keys", "keyring_hash")
    trust_domain = PACING_PRODUCTION_HUMAN

    def __init__(self, public_keys: Mapping[str, bytes], *, keyring_hash: str) -> None:
        checked_hash = str(keyring_hash or "").strip().lower()
        if _HASH_RE.fullmatch(checked_hash) is None:
            raise ValueError("pacing keyring hash is invalid")
        normalized: dict[str, bytes] = {}
        for key_id, public_key in public_keys.items():
            if (
                not isinstance(key_id, str)
                or _KEY_ID_RE.fullmatch(key_id) is None
                or key_id.lower().startswith("test-only:")
                or not isinstance(public_key, bytes)
                or len(public_key) != 32
            ):
                raise ValueError("pacing keyring entry is invalid")
            normalized[key_id] = bytes(public_key)
        if not normalized:
            raise ValueError("pacing keyring cannot be empty")
        self._public_keys = MappingProxyType(dict(sorted(normalized.items())))
        self.keyring_hash = checked_hash

    def verify(
        self,
        *,
        signer_key_id: str,
        signature_algorithm: str,
        message: bytes,
        signature: str,
    ) -> bool:
        if signature_algorithm != "ED25519":
            return False
        public_key = self._public_keys.get(signer_key_id)
        if public_key is None:
            return False
        try:
            encoded = base64.b64decode(signature, validate=True)
            from cryptography.exceptions import InvalidSignature
            from cryptography.hazmat.primitives.asymmetric.ed25519 import (
                Ed25519PublicKey,
            )

            Ed25519PublicKey.from_public_bytes(public_key).verify(encoded, message)
        except (ImportError, InvalidSignature, TypeError, ValueError):
            return False
        return True


def load_pacing_authority_verifier(
    keyring_path: str | Path | None,
) -> Ed25519PacingAuthorityVerifier | None:
    """Load a separate, operator-installed public keyring; never private keys."""

    if keyring_path is None:
        return None
    value = _load_json_mapping(Path(keyring_path))
    if set(value) != _KEYRING_FIELDS:
        raise ValueError("pacing keyring fields are invalid")
    if (
        value.get("schema") != PACING_KEYRING_SCHEMA
        or value.get("trust_domain") != PACING_PRODUCTION_HUMAN
        or value.get("signature_algorithm") != "ED25519"
    ):
        raise ValueError("pacing keyring contract is invalid")
    key_id = value.get("signer_key_id")
    public_key_text = value.get("public_key_base64")
    supplied_hash = value.get("content_hash")
    if (
        not isinstance(key_id, str)
        or _KEY_ID_RE.fullmatch(key_id) is None
        or key_id.lower().startswith("test-only:")
        or not isinstance(public_key_text, str)
        or not isinstance(supplied_hash, str)
    ):
        raise ValueError("pacing keyring identity is invalid")
    signable = dict(value)
    signable.pop("content_hash")
    if canonical_hash(signable) != supplied_hash:
        raise ValueError("pacing keyring content hash mismatch")
    try:
        public_key = base64.b64decode(public_key_text, validate=True)
    except (TypeError, ValueError) as exc:
        raise ValueError("pacing keyring public key is invalid") from exc
    return Ed25519PacingAuthorityVerifier(
        {key_id: public_key},
        keyring_hash=supplied_hash,
    )


@dataclass(frozen=True, slots=True)
class PacingPolicyAuthority:
    """Long-lived human authority over one exact immutable five-class policy."""

    capability: MarketDataPacingCapability
    policy_content_hash: str
    approval_hash: str
    actor: str
    signer_key_id: str
    signed_at: datetime
    authority_mode: str = PACING_POLICY_AUTHORITY_MODE

    def __post_init__(self) -> None:
        if not isinstance(self.capability, MarketDataPacingCapability):
            raise TypeError("policy capability must be MarketDataPacingCapability")
        for field_name in ("policy_content_hash", "approval_hash"):
            value = str(getattr(self, field_name) or "").strip().lower()
            if _HASH_RE.fullmatch(value) is None:
                raise ValueError(f"{field_name} is invalid")
            object.__setattr__(self, field_name, value)
        actor = _expected_actor(self.actor)
        signer_key_id = str(self.signer_key_id or "").strip()
        if _KEY_ID_RE.fullmatch(signer_key_id) is None:
            raise ValueError("policy signer_key_id is invalid")
        if self.authority_mode != PACING_POLICY_AUTHORITY_MODE:
            raise ValueError("policy authority mode is invalid")
        object.__setattr__(self, "actor", actor)
        object.__setattr__(self, "signer_key_id", signer_key_id)
        object.__setattr__(
            self,
            "signed_at",
            utc_datetime(self.signed_at, field="signed_at"),
        )


@dataclass(frozen=True, slots=True)
class ApprovedPacingCapabilityResolution:
    """One immutable loader outcome suitable for production composition."""

    capability: MarketDataPacingCapability | None
    record: CapabilityRecord
    approval_hash: str | None = None
    actor: str | None = None
    signer_key_id: str | None = None
    policy_authority: PacingPolicyAuthority | None = None

    @property
    def ready(self) -> bool:
        return (
            self.capability is not None
            and self.record.status is CapabilityStatus.READY_FOR_REVIEW
            and not self.record.reason_codes
        )


def load_approved_pacing_capability(
    run_directory: str | Path,
    *,
    now: datetime,
    expected_actor: str,
    max_age: timedelta = DEFAULT_PACING_MAX_AGE,
    signature_verifier: PacingAuthoritySignatureVerifier | None = None,
    allow_test_authority: bool = False,
) -> ApprovedPacingCapabilityResolution:
    """Load and cross-check the P0 capability and human approval artifacts.

    No self-declared actor is trusted.  The caller must inject the expected
    human identity.  Any missing, stale, malformed, tampered, rejected, or
    mismatched artifact returns no capability and includes the fixed
    ``PACING_CAPABILITY_MISSING`` reason used by the request budget gate.
    """

    checked_at = utc_datetime(now, field="now")
    actor = _expected_actor(expected_actor)
    directory = Path(run_directory)

    try:
        capability_payload = _load_json_mapping(directory / "capability.json")
    except FileNotFoundError:
        return _failure(
            checked_at,
            CapabilityStatus.MISSING,
            "PACING_CAPABILITY_FILE_MISSING",
        )
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        return _failure(
            checked_at,
            CapabilityStatus.FORBIDDEN,
            "PACING_CAPABILITY_FILE_INVALID",
        )

    try:
        approval = _load_json_mapping(directory / "approval.json")
    except FileNotFoundError:
        return _failure(
            checked_at,
            CapabilityStatus.MISSING,
            "PACING_APPROVAL_FILE_MISSING",
        )
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        return _failure(
            checked_at,
            CapabilityStatus.FORBIDDEN,
            "PACING_APPROVAL_FILE_INVALID",
        )

    schema_version = approval.get("schema_version")
    if schema_version == 1:
        inspection_max_age = max_age
    elif schema_version == 2:
        # A v2 policy is approved once and remains valid until an authenticated
        # revocation is installed.  Its immutable limit contract does not age.
        inspection_max_age = timedelta.max
    else:
        return _failure(
            checked_at,
            CapabilityStatus.FORBIDDEN,
            "PACING_APPROVAL_SCHEMA_INVALID",
        )

    inspected = MarketDataPacingCapability.inspect(
        capability_payload,
        now=checked_at,
        max_age=inspection_max_age,
    )
    if inspected.status is not CapabilityStatus.READY_FOR_REVIEW:
        return ApprovedPacingCapabilityResolution(None, inspected)

    capability = MarketDataPacingCapability(
        version=capability_payload["version"],  # type: ignore[arg-type]
        observed_at=_timestamp(capability_payload["observed_at"], "observed_at"),
        source=capability_payload["source"],  # type: ignore[arg-type]
        request_classes=capability_payload["request_classes"],  # type: ignore[arg-type]
        content_hash=capability_payload["content_hash"],  # type: ignore[arg-type]
        signer=capability_payload["signer"],  # type: ignore[arg-type]
    )

    approval_error = _validate_approval(
        approval,
        capability_payload=capability_payload,
        capability=capability,
        expected_actor=actor,
        now=checked_at,
        signature_verifier=signature_verifier,
        allow_test_authority=allow_test_authority,
    )
    if approval_error is not None:
        return _failure(
            capability.observed_at,
            CapabilityStatus.FORBIDDEN,
            approval_error,
        )

    approval_hash = str(approval["content_hash"])
    policy_authority: PacingPolicyAuthority | None = None
    if schema_version == 2:
        policy_authority = PacingPolicyAuthority(
            capability=capability,
            policy_content_hash=str(approval["policy_content_hash"]),
            approval_hash=approval_hash,
            actor=actor,
            signer_key_id=str(approval["signer_key_id"]),
            signed_at=_timestamp(approval["signed_at"], "approval.signed_at"),
        )
        revocation_error = _revocation_status(
            directory / "revocation.json",
            policy_authority=policy_authority,
            now=checked_at,
            signature_verifier=signature_verifier,
            allow_test_authority=allow_test_authority,
        )
        if revocation_error is not None:
            return _failure(
                capability.observed_at,
                CapabilityStatus.FORBIDDEN,
                revocation_error,
            )
    elif (directory / "revocation.json").exists():
        return _failure(
            capability.observed_at,
            CapabilityStatus.FORBIDDEN,
            "PACING_POLICY_REVOCATION_INVALID",
        )

    record = CapabilityRecord(
        name="market_data_pacing",
        status=CapabilityStatus.READY_FOR_REVIEW,
        observed_at=capability.observed_at,
        reason_codes=(),
        details={
            **dict(thaw_json(inspected.details)),
            "approval_hash": approval_hash,
            "approval_actor": actor,
            "approval_decision": "APPROVED",
            "approval_signature_authenticated": True,
            "approval_signer_key_id": approval["signer_key_id"],
            "approval_signature_algorithm": approval["signature_algorithm"],
            "approval_keyring_hash": getattr(
                signature_verifier,
                "keyring_hash",
                None,
            ),
            "approval_schema_version": schema_version,
            "authority_mode": (
                PACING_POLICY_AUTHORITY_MODE
                if policy_authority is not None
                else "LEGACY_24_HOUR_CAPABILITY"
            ),
            "policy_content_hash": (
                None
                if policy_authority is None
                else policy_authority.policy_content_hash
            ),
            "valid_until_revoked": policy_authority is not None,
            "review_only": True,
            "direct_order_submission": False,
        },
    )
    return ApprovedPacingCapabilityResolution(
        capability=capability,
        record=record,
        approval_hash=approval_hash,
        actor=actor,
        signer_key_id=str(approval["signer_key_id"]),
        policy_authority=policy_authority,
    )


def _validate_approval(
    approval: Mapping[str, object],
    *,
    capability_payload: Mapping[str, object],
    capability: MarketDataPacingCapability,
    expected_actor: str,
    now: datetime,
    signature_verifier: PacingAuthoritySignatureVerifier | None,
    allow_test_authority: bool,
) -> str | None:
    schema_version = approval.get("schema_version")
    required_fields = (
        _APPROVAL_REQUIRED_FIELDS_V2
        if schema_version == 2
        else _APPROVAL_REQUIRED_FIELDS_V1
    )
    if required_fields.difference(approval):
        return "PACING_APPROVAL_FIELD_MISSING"

    supplied_hash = approval.get("content_hash")
    if not isinstance(supplied_hash, str) or _HASH_RE.fullmatch(supplied_hash) is None:
        return "PACING_APPROVAL_HASH_MISMATCH"
    signable = {key: value for key, value in approval.items() if key != "content_hash"}
    try:
        expected_hash = canonical_hash(signable)
    except (TypeError, ValueError):
        return "PACING_APPROVAL_HASH_MISMATCH"
    if supplied_hash != expected_hash:
        return "PACING_APPROVAL_HASH_MISMATCH"

    signer_key_id = approval.get("signer_key_id")
    signature_algorithm = approval.get("signature_algorithm")
    approval_signature = approval.get("approval_signature")
    if (
        not isinstance(signer_key_id, str)
        or _KEY_ID_RE.fullmatch(signer_key_id) is None
        or not isinstance(signature_algorithm, str)
        or not isinstance(approval_signature, str)
        or _SIGNATURE_RE.fullmatch(approval_signature) is None
    ):
        return "PACING_APPROVAL_SIGNATURE_INVALID"
    if signature_verifier is None:
        return "PACING_APPROVAL_SIGNATURE_VERIFIER_MISSING"
    verifier_domain = getattr(signature_verifier, "trust_domain", None)
    if allow_test_authority:
        if (
            verifier_domain != PACING_TEST_ONLY
            or not signer_key_id.lower().startswith("test-only:")
            or not signature_algorithm.startswith("TEST_ONLY_")
        ):
            return "PACING_APPROVAL_SIGNATURE_VERIFIER_UNTRUSTED"
    elif (
        not isinstance(signature_verifier, Ed25519PacingAuthorityVerifier)
        or verifier_domain != PACING_PRODUCTION_HUMAN
        or signer_key_id.lower().startswith("test-only:")
        or signature_algorithm != "ED25519"
    ):
        return "PACING_APPROVAL_SIGNATURE_VERIFIER_UNTRUSTED"
    signature_message = dict(approval)
    signature_message.pop("approval_signature")
    signature_message.pop("content_hash")
    try:
        authenticated = signature_verifier.verify(
            signer_key_id=signer_key_id,
            signature_algorithm=signature_algorithm,
            message=canonical_json(signature_message).encode("utf-8"),
            signature=approval_signature,
        )
    except Exception:
        return "PACING_APPROVAL_SIGNATURE_INVALID"
    if authenticated is not True:
        return "PACING_APPROVAL_SIGNATURE_INVALID"

    if schema_version == 1:
        if approval.get("kind") != "market_data_pacing_approval":
            return "PACING_APPROVAL_KIND_INVALID"
    elif schema_version == 2:
        if approval.get("kind") != PACING_POLICY_AUTHORITY_KIND:
            return "PACING_APPROVAL_KIND_INVALID"
        if approval.get("authority_mode") != PACING_POLICY_AUTHORITY_MODE:
            return "PACING_APPROVAL_AUTHORITY_INVALID"
        supplied_policy_hash = approval.get("policy_content_hash")
        if (
            not isinstance(supplied_policy_hash, str)
            or _HASH_RE.fullmatch(supplied_policy_hash) is None
            or supplied_policy_hash
            != pacing_policy_content_hash(capability_payload, actor=expected_actor)
        ):
            return "PACING_POLICY_HASH_MISMATCH"
    else:
        return "PACING_APPROVAL_SCHEMA_INVALID"
    if approval.get("decision") != "APPROVED":
        return "PACING_APPROVAL_DECISION_INVALID"
    if approval.get("review_only") is not True:
        return "PACING_APPROVAL_AUTHORITY_INVALID"
    if approval.get("direct_order_submission") is not False:
        return "PACING_APPROVAL_AUTHORITY_INVALID"

    if (
        approval.get("actor") != expected_actor
        or approval.get("signer") != expected_actor
        or capability.signer != expected_actor
    ):
        return "PACING_APPROVAL_ACTOR_MISMATCH"

    nested_capability = approval.get("capability")
    if not isinstance(nested_capability, Mapping):
        return "PACING_APPROVAL_CAPABILITY_MISMATCH"
    try:
        nested_hash = canonical_hash(nested_capability)
        file_hash = canonical_hash(capability_payload)
    except (TypeError, ValueError):
        return "PACING_APPROVAL_CAPABILITY_MISMATCH"
    if nested_hash != file_hash:
        return "PACING_APPROVAL_CAPABILITY_MISMATCH"
    if approval.get("capability_content_hash") != capability.content_hash:
        return "PACING_APPROVAL_CAPABILITY_MISMATCH"
    if nested_capability.get("content_hash") != capability.content_hash:
        return "PACING_APPROVAL_CAPABILITY_MISMATCH"
    if approval.get("version") != capability.version:
        return "PACING_APPROVAL_CAPABILITY_MISMATCH"
    if approval.get("source") != capability.source:
        return "PACING_APPROVAL_CAPABILITY_MISMATCH"

    try:
        approved_observed_at = _timestamp(
            approval.get("observed_at"), "approval.observed_at"
        )
        signed_at = _timestamp(approval.get("signed_at"), "approval.signed_at")
    except (TypeError, ValueError):
        return "PACING_APPROVAL_TIMESTAMP_INVALID"
    if approved_observed_at != capability.observed_at:
        return "PACING_APPROVAL_CAPABILITY_MISMATCH"
    if signed_at < capability.observed_at or signed_at > now:
        return "PACING_APPROVAL_TIMESTAMP_INVALID"
    return None


def pacing_policy_content_hash(
    capability_payload: Mapping[str, object],
    *,
    actor: str,
) -> str:
    """Hash the exact long-lived policy without granting any broker authority."""

    return canonical_hash(
        {
            "schema_version": 2,
            "kind": PACING_POLICY_AUTHORITY_KIND,
            "authority_mode": PACING_POLICY_AUTHORITY_MODE,
            "actor": _expected_actor(actor),
            "capability": dict(capability_payload),
            "review_only": True,
            "direct_order_submission": False,
        }
    )


def _revocation_status(
    path: Path,
    *,
    policy_authority: PacingPolicyAuthority,
    now: datetime,
    signature_verifier: PacingAuthoritySignatureVerifier | None,
    allow_test_authority: bool,
) -> str | None:
    if not path.exists():
        return None
    try:
        revocation = _load_json_mapping(path)
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        return "PACING_POLICY_REVOCATION_INVALID"
    if set(revocation) != _REVOCATION_REQUIRED_FIELDS:
        return "PACING_POLICY_REVOCATION_INVALID"

    supplied_hash = revocation.get("content_hash")
    signable = dict(revocation)
    signable.pop("content_hash", None)
    try:
        expected_hash = canonical_hash(signable)
    except (TypeError, ValueError):
        return "PACING_POLICY_REVOCATION_INVALID"
    if (
        not isinstance(supplied_hash, str)
        or _HASH_RE.fullmatch(supplied_hash) is None
        or supplied_hash != expected_hash
    ):
        return "PACING_POLICY_REVOCATION_INVALID"

    signer_key_id = revocation.get("signer_key_id")
    signature_algorithm = revocation.get("signature_algorithm")
    signature = revocation.get("revocation_signature")
    if (
        signer_key_id != policy_authority.signer_key_id
        or not isinstance(signature_algorithm, str)
        or not isinstance(signature, str)
        or _SIGNATURE_RE.fullmatch(signature) is None
        or signature_verifier is None
    ):
        return "PACING_POLICY_REVOCATION_INVALID"
    verifier_domain = getattr(signature_verifier, "trust_domain", None)
    if allow_test_authority:
        trusted = (
            verifier_domain == PACING_TEST_ONLY
            and str(signer_key_id).lower().startswith("test-only:")
            and signature_algorithm.startswith("TEST_ONLY_")
        )
    else:
        trusted = (
            isinstance(signature_verifier, Ed25519PacingAuthorityVerifier)
            and verifier_domain == PACING_PRODUCTION_HUMAN
            and not str(signer_key_id).lower().startswith("test-only:")
            and signature_algorithm == "ED25519"
        )
    if not trusted:
        return "PACING_POLICY_REVOCATION_INVALID"
    signature_message = dict(revocation)
    signature_message.pop("revocation_signature")
    signature_message.pop("content_hash")
    try:
        authenticated = signature_verifier.verify(
            signer_key_id=str(signer_key_id),
            signature_algorithm=signature_algorithm,
            message=canonical_json(signature_message).encode("utf-8"),
            signature=signature,
        )
    except Exception:
        return "PACING_POLICY_REVOCATION_INVALID"
    if authenticated is not True:
        return "PACING_POLICY_REVOCATION_INVALID"

    reason = revocation.get("reason")
    if (
        revocation.get("schema_version") != 1
        or revocation.get("kind") != PACING_POLICY_REVOCATION_KIND
        or revocation.get("decision") != "REVOKED"
        or revocation.get("actor") != policy_authority.actor
        or revocation.get("signer") != policy_authority.actor
        or revocation.get("policy_content_hash")
        != policy_authority.policy_content_hash
        or revocation.get("approval_content_hash") != policy_authority.approval_hash
        or revocation.get("review_only") is not True
        or revocation.get("direct_order_submission") is not False
        or not isinstance(reason, str)
        or not reason.strip()
    ):
        return "PACING_POLICY_REVOCATION_INVALID"
    try:
        revoked_at = _timestamp(revocation.get("revoked_at"), "revoked_at")
    except (TypeError, ValueError):
        return "PACING_POLICY_REVOCATION_INVALID"
    if revoked_at < policy_authority.signed_at or revoked_at > now:
        return "PACING_POLICY_REVOCATION_INVALID"
    return "PACING_POLICY_REVOKED"


def _failure(
    observed_at: datetime,
    status: CapabilityStatus,
    reason: str,
) -> ApprovedPacingCapabilityResolution:
    record = CapabilityRecord(
        name="market_data_pacing",
        status=status,
        observed_at=observed_at,
        reason_codes=(reason, "PACING_CAPABILITY_MISSING"),
        details={
            "review_only": True,
            "direct_order_submission": False,
        },
    )
    return ApprovedPacingCapabilityResolution(None, record)


def _expected_actor(value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("expected_actor must be a nonblank human identity")
    actor = value.strip()
    if not actor.startswith("human:") or len(actor) <= len("human:"):
        raise ValueError("expected_actor must use the human:<identity> form")
    return actor


def _timestamp(value: object, field: str) -> datetime:
    if isinstance(value, datetime):
        return utc_datetime(value, field=field)
    if not isinstance(value, str) or not value.strip():
        raise TypeError(f"{field} must be an ISO 8601 timestamp")
    raw = value.strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    return utc_datetime(datetime.fromisoformat(raw), field=field)


def _load_json_mapping(path: Path) -> dict[str, object]:
    raw = path.read_text(encoding="utf-8")
    value: Any = json.loads(
        raw,
        object_pairs_hook=_reject_duplicate_keys,
        parse_constant=_reject_json_constant,
    )
    if not isinstance(value, Mapping):
        raise TypeError(f"{path.name} must contain a JSON object")
    return {str(key): item for key, item in value.items()}


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON field {key!r}")
        result[key] = value
    return result


def _reject_json_constant(value: str) -> object:
    raise ValueError(f"non-finite JSON number {value!r} is prohibited")


__all__ = [
    "ApprovedPacingCapabilityResolution",
    "Ed25519PacingAuthorityVerifier",
    "PACING_EXPECTED_ACTOR",
    "PACING_KEYRING_SCHEMA",
    "PACING_POLICY_AUTHORITY_KIND",
    "PACING_POLICY_AUTHORITY_MODE",
    "PACING_POLICY_REVOCATION_KIND",
    "PACING_PRODUCTION_HUMAN",
    "PACING_TEST_ONLY",
    "PacingPolicyAuthority",
    "PacingAuthoritySignatureVerifier",
    "load_approved_pacing_capability",
    "load_pacing_authority_verifier",
    "pacing_policy_content_hash",
]
