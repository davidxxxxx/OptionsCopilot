"""Immutable P9 governance authority schemas and fail-closed verifiers.

This module deliberately contains no signing function.  Production attestation
is available only through the interactive governance CLI; consumers receive
already-attested documents and can only verify them.
"""
from __future__ import annotations

import base64
from collections.abc import Mapping
from dataclasses import dataclass, fields
from datetime import datetime
import re
from types import MappingProxyType
from typing import Protocol

from options_copilot.storage.canonical import canonical_hash, canonical_json, utc_datetime


PROMOTION_AUTHORITY_SCHEMA = "options_copilot.learning.promotion_authority.v1"
ROLLBACK_AUTHORITY_SCHEMA = "options_copilot.learning.rollback_authority.v1"
A_GRADE_AUTHORITY_SCHEMA = "options_copilot.learning.a_grade_authority.v1"
_HASH_RE = re.compile(r"[0-9a-f]{64}\Z")
_VERSION_RE = re.compile(r"v[1-9][0-9]*(?:\.[0-9]+)*\Z")
_KEY_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,159}\Z")
_ALGORITHM_RE = re.compile(r"[A-Z][A-Z0-9_-]{2,63}\Z")
_SIGNATURE_RE = re.compile(r"[A-Za-z0-9_+/=-]{32,2048}\Z")
NO_TRUSTED_HUMAN_SIGNER = "NO_TRUSTED_HUMAN_SIGNER"
PRODUCTION_HUMAN = "PRODUCTION_HUMAN"
TEST_ONLY_AUTHORITY = "TEST_ONLY"
_KEYRING_FACTORY_SEAL = object()
_VERIFIER_FACTORY_SEAL = object()


class AuthorityValidationError(ValueError):
    """An authority artifact is incomplete, mismatched, or tampered."""


class AuthoritySignatureVerifier(Protocol):
    """Verify a detached signature against an independently approved key."""

    trust_domain: str

    def verify(
        self,
        *,
        signer_key_id: str,
        signature_algorithm: str,
        message: bytes,
        signature: str,
    ) -> bool: ...


class ApprovedHumanAuthorityKeyring:
    """Immutable approved Ed25519 public keys for human governance."""

    __slots__ = ("_public_keys", "keyring_hash")

    def __init__(
        self,
        public_keys: Mapping[str, bytes],
        *,
        _factory_seal: object,
    ) -> None:
        if _factory_seal is not _KEYRING_FACTORY_SEAL:
            raise AuthorityValidationError(
                "approved human keyrings must be created by the keyring factory"
            )
        normalized: dict[str, bytes] = {}
        for key_id, public_key in public_keys.items():
            if (
                not isinstance(key_id, str)
                or _KEY_ID_RE.fullmatch(key_id) is None
                or key_id.lower().startswith("test-only:")
            ):
                raise AuthorityValidationError("approved signer_key_id is invalid")
            if not isinstance(public_key, bytes) or len(public_key) != 32:
                raise AuthorityValidationError(
                    "approved Ed25519 public keys must contain exactly 32 bytes"
                )
            normalized[key_id] = bytes(public_key)
        if not normalized:
            raise AuthorityValidationError("approved human keyring cannot be empty")
        self._public_keys = MappingProxyType(dict(sorted(normalized.items())))
        self.keyring_hash = canonical_hash(
            {
                "schema": "options_copilot.learning.approved_human_keyring.v1",
                "trust_domain": PRODUCTION_HUMAN,
                "keys": {
                    key_id: base64.b64encode(value).decode("ascii")
                    for key_id, value in self._public_keys.items()
                },
            }
        )

    def verifier(self) -> "ProductionHumanAuthorityVerifier":
        return ProductionHumanAuthorityVerifier(
            self,
            _factory_seal=_VERIFIER_FACTORY_SEAL,
        )


class ProductionHumanAuthorityVerifier:
    """Production verifier capability sealed to one immutable approved keyring."""

    __slots__ = ("_keyring",)
    trust_domain = PRODUCTION_HUMAN

    def __init__(
        self,
        keyring: ApprovedHumanAuthorityKeyring,
        *,
        _factory_seal: object,
    ) -> None:
        if (
            _factory_seal is not _VERIFIER_FACTORY_SEAL
            or not isinstance(keyring, ApprovedHumanAuthorityKeyring)
        ):
            raise AuthorityValidationError(
                "production human verifier must be created by an approved keyring"
            )
        self._keyring = keyring

    @property
    def keyring_hash(self) -> str:
        return self._keyring.keyring_hash

    def verify(
        self,
        *,
        signer_key_id: str,
        signature_algorithm: str,
        message: bytes,
        signature: str,
    ) -> bool:
        if (
            signer_key_id.lower().startswith("test-only:")
            or signature_algorithm.startswith("TEST_ONLY_")
            or signature_algorithm != "ED25519"
        ):
            return False
        public_key = self._keyring._public_keys.get(signer_key_id)
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


def validate_authority_verifier(
    signature_verifier: AuthoritySignatureVerifier | None,
    *,
    allow_test_authority: bool = False,
) -> None:
    """Reject verifier capabilities outside the explicit trust domain."""

    if not isinstance(allow_test_authority, bool):
        raise TypeError("allow_test_authority must be a boolean")
    if signature_verifier is None:
        return
    verifier_domain = getattr(signature_verifier, "trust_domain", None)
    if allow_test_authority:
        if verifier_domain == TEST_ONLY_AUTHORITY:
            return
        if verifier_domain == PRODUCTION_HUMAN:
            raise AuthorityValidationError(NO_TRUSTED_HUMAN_SIGNER)
        raise AuthorityValidationError("test authority verifier trust domain is invalid")
    if verifier_domain == TEST_ONLY_AUTHORITY:
        raise AuthorityValidationError(
            "unapproved authority verifier: approved production human verifier required"
        )
    # No independently pinned and approved production keyring is configured.
    # A caller-supplied object must never be able to declare itself production.
    raise AuthorityValidationError(NO_TRUSTED_HUMAN_SIGNER)


def _digest(value: object, *, field: str) -> str:
    if not isinstance(value, str) or _HASH_RE.fullmatch(value) is None:
        raise AuthorityValidationError(f"{field} must be a lowercase 64-character hash")
    return value


def _optional_digest(value: object, *, field: str) -> str | None:
    if value is None:
        return None
    return _digest(value, field=field)


def _version(value: object, *, field: str) -> str:
    if not isinstance(value, str) or _VERSION_RE.fullmatch(value) is None:
        raise AuthorityValidationError(f"{field} must be a v1-style version")
    return value


def _human(value: object) -> str:
    if not isinstance(value, str) or not value.startswith("human:") or not value[6:].strip():
        raise AuthorityValidationError("actor must be an explicit human:* identity")
    if value != value.strip() or len(value) > 160:
        raise AuthorityValidationError("actor is invalid")
    return value


def _time(value: object, *, field: str) -> datetime:
    if isinstance(value, str):
        text = value[:-1] + "+00:00" if value.endswith("Z") else value
        try:
            value = datetime.fromisoformat(text)
        except ValueError as exc:
            raise AuthorityValidationError(f"{field} must be ISO-8601") from exc
    try:
        return utc_datetime(value, field=field)  # type: ignore[arg-type]
    except (TypeError, ValueError) as exc:
        raise AuthorityValidationError(str(exc)) from exc


def _sequence(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise AuthorityValidationError("sequence must be a positive integer")
    return value


def _reason(value: object) -> str:
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise AuthorityValidationError("reason must be nonblank and trimmed")
    return value


def _verify_document(
    value: Mapping[str, object],
    *,
    required: frozenset[str],
    schema: str,
    decision: str,
    signature_verifier: AuthoritySignatureVerifier | None,
    allow_test_authority: bool,
) -> None:
    if not isinstance(value, Mapping):
        raise AuthorityValidationError("authority document must be a mapping")
    missing = sorted(required.difference(value))
    if missing:
        raise AuthorityValidationError(f"authority is missing required field {missing[0]}")
    unknown = sorted(set(value).difference(required))
    if unknown:
        raise AuthorityValidationError(f"authority contains unknown field {unknown[0]}")
    if value.get("schema") != schema:
        raise AuthorityValidationError("authority schema mismatch")
    if value.get("phase") != "P9":
        raise AuthorityValidationError("authority phase must be P9")
    if value.get("decision") != decision:
        raise AuthorityValidationError("authority decision mismatch")
    signer_key_id = value.get("signer_key_id")
    signature_algorithm = value.get("signature_algorithm")
    signature = value.get("governance_signature")
    if not isinstance(signer_key_id, str) or _KEY_ID_RE.fullmatch(signer_key_id) is None:
        raise AuthorityValidationError("signer_key_id is invalid")
    if not isinstance(signature_algorithm, str) or _ALGORITHM_RE.fullmatch(signature_algorithm) is None:
        raise AuthorityValidationError("signature_algorithm is invalid")
    if not isinstance(signature, str) or _SIGNATURE_RE.fullmatch(signature) is None:
        raise AuthorityValidationError("governance_signature encoding is invalid")
    validate_authority_verifier(
        signature_verifier,
        allow_test_authority=allow_test_authority,
    )
    if signature_verifier is None:
        raise AuthorityValidationError(NO_TRUSTED_HUMAN_SIGNER)
    verifier_domain = getattr(signature_verifier, "trust_domain", None)
    test_identity = signer_key_id.lower().startswith("test-only:")
    test_algorithm = signature_algorithm.startswith("TEST_ONLY_")
    if verifier_domain == TEST_ONLY_AUTHORITY:
        if not allow_test_authority or not test_identity or not test_algorithm:
            raise AuthorityValidationError("test-only authority identity is invalid")
    elif test_identity or test_algorithm:
        raise AuthorityValidationError(
            "test-only authority identity is prohibited in production"
        )
    content_hash = _digest(value.get("content_hash"), field="content_hash")
    signable = dict(value)
    signable.pop("governance_signature")
    signable.pop("content_hash")
    try:
        authenticated = signature_verifier.verify(
            signer_key_id=signer_key_id,
            signature_algorithm=signature_algorithm,
            message=canonical_json(signable).encode("utf-8"),
            signature=signature,
        )
    except Exception as exc:
        raise AuthorityValidationError("trusted signer verification failed") from exc
    if authenticated is not True:
        raise AuthorityValidationError("governance signature mismatch")
    signed = dict(value)
    signed.pop("content_hash")
    if canonical_hash(signed) != content_hash:
        raise AuthorityValidationError("authority content hash mismatch")


_POLICY_FIELDS = frozenset({
    "schema", "phase", "decision", "sequence", "actor", "signed_at",
    "prior_policy_version", "prior_policy_hash", "current_policy_version",
    "current_policy_hash", "initial_policy_source_hash", "evaluation_report_hash",
    "reference_dataset_hash", "independence_hash", "execution_cost_hash",
    "risk_contract_hash", "reason", "previous_authority_hash",
    "signer_key_id", "signature_algorithm",
    "governance_signature", "content_hash",
})


@dataclass(frozen=True, slots=True)
class PromotionAuthority:
    schema: str
    phase: str
    decision: str
    sequence: int
    actor: str
    signed_at: datetime
    prior_policy_version: str
    prior_policy_hash: str
    current_policy_version: str
    current_policy_hash: str
    initial_policy_source_hash: str
    evaluation_report_hash: str
    reference_dataset_hash: str
    independence_hash: str
    execution_cost_hash: str
    risk_contract_hash: str
    reason: str
    previous_authority_hash: str
    signer_key_id: str
    signature_algorithm: str
    governance_signature: str
    content_hash: str

    @classmethod
    def from_dict(
        cls,
        value: Mapping[str, object],
        *,
        signature_verifier: AuthoritySignatureVerifier | None = None,
        allow_test_authority: bool = False,
    ) -> "PromotionAuthority":
        _verify_document(
            value,
            required=_POLICY_FIELDS,
            schema=PROMOTION_AUTHORITY_SCHEMA,
            decision="PROMOTE_CHALLENGER",
            signature_verifier=signature_verifier,
            allow_test_authority=allow_test_authority,
        )
        parsed = cls(
            schema=str(value["schema"]), phase=str(value["phase"]), decision=str(value["decision"]),
            sequence=_sequence(value["sequence"]), actor=_human(value["actor"]),
            signed_at=_time(value["signed_at"], field="signed_at"),
            prior_policy_version=_version(value["prior_policy_version"], field="prior_policy_version"),
            prior_policy_hash=_digest(value["prior_policy_hash"], field="prior_policy_hash"),
            current_policy_version=_version(value["current_policy_version"], field="current_policy_version"),
            current_policy_hash=_digest(value["current_policy_hash"], field="current_policy_hash"),
            initial_policy_source_hash=_digest(value["initial_policy_source_hash"], field="initial_policy_source_hash"),
            evaluation_report_hash=_digest(value["evaluation_report_hash"], field="evaluation_report_hash"),
            reference_dataset_hash=_digest(value["reference_dataset_hash"], field="reference_dataset_hash"),
            independence_hash=_digest(value["independence_hash"], field="independence_hash"),
            execution_cost_hash=_digest(value["execution_cost_hash"], field="execution_cost_hash"),
            risk_contract_hash=_digest(value["risk_contract_hash"], field="risk_contract_hash"),
            reason=_reason(value["reason"]),
            previous_authority_hash=_digest(value["previous_authority_hash"], field="previous_authority_hash"),
            signer_key_id=str(value["signer_key_id"]),
            signature_algorithm=str(value["signature_algorithm"]),
            governance_signature=str(value["governance_signature"]),
            content_hash=_digest(value["content_hash"], field="content_hash"),
        )
        if _version_parts(parsed.current_policy_version) <= _version_parts(parsed.prior_policy_version):
            raise AuthorityValidationError("promotion policy version must advance")
        if parsed.current_policy_hash == parsed.prior_policy_hash:
            raise AuthorityValidationError("promotion must identify a different policy hash")
        return parsed

    def to_dict(self) -> dict[str, object]:
        return _authority_dict(self)


@dataclass(frozen=True, slots=True)
class RollbackAuthority:
    schema: str
    phase: str
    decision: str
    sequence: int
    actor: str
    signed_at: datetime
    prior_policy_version: str
    prior_policy_hash: str
    current_policy_version: str
    current_policy_hash: str
    initial_policy_source_hash: str
    evaluation_report_hash: str
    reference_dataset_hash: str
    independence_hash: str
    execution_cost_hash: str
    risk_contract_hash: str
    reason: str
    previous_authority_hash: str
    signer_key_id: str
    signature_algorithm: str
    governance_signature: str
    content_hash: str

    @classmethod
    def from_dict(
        cls,
        value: Mapping[str, object],
        *,
        signature_verifier: AuthoritySignatureVerifier | None = None,
        allow_test_authority: bool = False,
    ) -> "RollbackAuthority":
        _verify_document(
            value,
            required=_POLICY_FIELDS,
            schema=ROLLBACK_AUTHORITY_SCHEMA,
            decision="ROLLBACK_POLICY",
            signature_verifier=signature_verifier,
            allow_test_authority=allow_test_authority,
        )
        parsed = cls(
            schema=str(value["schema"]), phase=str(value["phase"]), decision=str(value["decision"]),
            sequence=_sequence(value["sequence"]), actor=_human(value["actor"]),
            signed_at=_time(value["signed_at"], field="signed_at"),
            prior_policy_version=_version(value["prior_policy_version"], field="prior_policy_version"),
            prior_policy_hash=_digest(value["prior_policy_hash"], field="prior_policy_hash"),
            current_policy_version=_version(value["current_policy_version"], field="current_policy_version"),
            current_policy_hash=_digest(value["current_policy_hash"], field="current_policy_hash"),
            initial_policy_source_hash=_digest(value["initial_policy_source_hash"], field="initial_policy_source_hash"),
            evaluation_report_hash=_digest(value["evaluation_report_hash"], field="evaluation_report_hash"),
            reference_dataset_hash=_digest(value["reference_dataset_hash"], field="reference_dataset_hash"),
            independence_hash=_digest(value["independence_hash"], field="independence_hash"),
            execution_cost_hash=_digest(value["execution_cost_hash"], field="execution_cost_hash"),
            risk_contract_hash=_digest(value["risk_contract_hash"], field="risk_contract_hash"),
            reason=_reason(value["reason"]),
            previous_authority_hash=_digest(value["previous_authority_hash"], field="previous_authority_hash"),
            signer_key_id=str(value["signer_key_id"]),
            signature_algorithm=str(value["signature_algorithm"]),
            governance_signature=str(value["governance_signature"]),
            content_hash=_digest(value["content_hash"], field="content_hash"),
        )
        if parsed.current_policy_version == parsed.prior_policy_version and parsed.current_policy_hash == parsed.prior_policy_hash:
            raise AuthorityValidationError("rollback must change the current policy")
        return parsed

    def to_dict(self) -> dict[str, object]:
        return _authority_dict(self)


_A_GRADE_FIELDS = frozenset({
    "schema", "phase", "decision", "version", "sequence", "append_only",
    "proposal_hash", "candidate_hash", "current_policy_version", "current_policy_hash",
    "policy_authority_marker_hash", "execution_cost_version", "execution_cost_hash",
    "ranking_basis_hash", "evaluation_report_hash", "reference_dataset_hash",
    "independence_hash", "risk_contract_hash", "actor", "signed_at", "expires_at",
    "previous_authority_hash", "revoked", "rolled_back", "signer_key_id",
    "signature_algorithm", "governance_signature", "content_hash",
})


@dataclass(frozen=True, slots=True)
class AGradeAuthority:
    schema: str
    phase: str
    decision: str
    version: str
    sequence: int
    append_only: bool
    proposal_hash: str
    candidate_hash: str
    current_policy_version: str
    current_policy_hash: str
    policy_authority_marker_hash: str
    execution_cost_version: str
    execution_cost_hash: str
    ranking_basis_hash: str
    evaluation_report_hash: str
    reference_dataset_hash: str
    independence_hash: str
    risk_contract_hash: str
    actor: str
    signed_at: datetime
    expires_at: datetime
    previous_authority_hash: str | None
    revoked: bool
    rolled_back: bool
    signer_key_id: str
    signature_algorithm: str
    governance_signature: str
    content_hash: str

    @classmethod
    def from_dict(
        cls,
        value: Mapping[str, object],
        *,
        signature_verifier: AuthoritySignatureVerifier | None = None,
        allow_test_authority: bool = False,
    ) -> "AGradeAuthority":
        _verify_document(
            value,
            required=_A_GRADE_FIELDS,
            schema=A_GRADE_AUTHORITY_SCHEMA,
            decision="APPROVE_A_GRADE",
            signature_verifier=signature_verifier,
            allow_test_authority=allow_test_authority,
        )
        if value["append_only"] is not True:
            raise AuthorityValidationError("A-grade authority must be append-only")
        if not isinstance(value["revoked"], bool) or not isinstance(value["rolled_back"], bool):
            raise AuthorityValidationError("revocation flags must be booleans")
        parsed = cls(
            schema=str(value["schema"]), phase=str(value["phase"]), decision=str(value["decision"]),
            version=_version(value["version"], field="version"), sequence=_sequence(value["sequence"]),
            append_only=True, proposal_hash=_digest(value["proposal_hash"], field="proposal_hash"),
            candidate_hash=_digest(value["candidate_hash"], field="candidate_hash"),
            current_policy_version=_version(value["current_policy_version"], field="current_policy_version"),
            current_policy_hash=_digest(value["current_policy_hash"], field="current_policy_hash"),
            policy_authority_marker_hash=_digest(value["policy_authority_marker_hash"], field="policy_authority_marker_hash"),
            execution_cost_version=_version(value["execution_cost_version"], field="execution_cost_version"),
            execution_cost_hash=_digest(value["execution_cost_hash"], field="execution_cost_hash"),
            ranking_basis_hash=_digest(value["ranking_basis_hash"], field="ranking_basis_hash"),
            evaluation_report_hash=_digest(value["evaluation_report_hash"], field="evaluation_report_hash"),
            reference_dataset_hash=_digest(value["reference_dataset_hash"], field="reference_dataset_hash"),
            independence_hash=_digest(value["independence_hash"], field="independence_hash"),
            risk_contract_hash=_digest(value["risk_contract_hash"], field="risk_contract_hash"),
            actor=_human(value["actor"]), signed_at=_time(value["signed_at"], field="signed_at"),
            expires_at=_time(value["expires_at"], field="expires_at"),
            previous_authority_hash=_optional_digest(value["previous_authority_hash"], field="previous_authority_hash"),
            revoked=bool(value["revoked"]), rolled_back=bool(value["rolled_back"]),
            signer_key_id=str(value["signer_key_id"]),
            signature_algorithm=str(value["signature_algorithm"]),
            governance_signature=str(value["governance_signature"]),
            content_hash=_digest(value["content_hash"], field="content_hash"),
        )
        if parsed.expires_at <= parsed.signed_at:
            raise AuthorityValidationError("A-grade authority expiry must follow signed_at")
        return parsed

    @property
    def policy_hash(self) -> str:
        return self.current_policy_hash

    @property
    def previous_marker_hash(self) -> str | None:
        return self.previous_authority_hash

    def to_dict(self) -> dict[str, object]:
        return _authority_dict(self)


def verify_promotion_authority(
    value: PromotionAuthority | Mapping[str, object],
    *,
    signature_verifier: AuthoritySignatureVerifier | None = None,
    allow_test_authority: bool = False,
) -> PromotionAuthority:
    return PromotionAuthority.from_dict(
        value.to_dict() if isinstance(value, PromotionAuthority) else value,
        signature_verifier=signature_verifier,
        allow_test_authority=allow_test_authority,
    )


def verify_rollback_authority(
    value: RollbackAuthority | Mapping[str, object],
    *,
    signature_verifier: AuthoritySignatureVerifier | None = None,
    allow_test_authority: bool = False,
) -> RollbackAuthority:
    return RollbackAuthority.from_dict(
        value.to_dict() if isinstance(value, RollbackAuthority) else value,
        signature_verifier=signature_verifier,
        allow_test_authority=allow_test_authority,
    )


def verify_a_grade_authority(
    value: AGradeAuthority | Mapping[str, object],
    *,
    signature_verifier: AuthoritySignatureVerifier | None = None,
    allow_test_authority: bool = False,
) -> AGradeAuthority:
    return AGradeAuthority.from_dict(
        value.to_dict() if isinstance(value, AGradeAuthority) else value,
        signature_verifier=signature_verifier,
        allow_test_authority=allow_test_authority,
    )


def _authority_dict(value: object) -> dict[str, object]:
    output: dict[str, object] = {}
    for field in fields(value):  # type: ignore[arg-type]
        item = getattr(value, field.name)
        output[field.name] = item.isoformat() if isinstance(item, datetime) else item
    return output


def _version_parts(value: str) -> tuple[int, ...]:
    return tuple(int(part) for part in value[1:].split("."))


__all__ = [
    "A_GRADE_AUTHORITY_SCHEMA", "AGradeAuthority", "ApprovedHumanAuthorityKeyring",
    "AuthoritySignatureVerifier", "AuthorityValidationError", "NO_TRUSTED_HUMAN_SIGNER",
    "PRODUCTION_HUMAN", "ProductionHumanAuthorityVerifier", "TEST_ONLY_AUTHORITY",
    "PROMOTION_AUTHORITY_SCHEMA", "PromotionAuthority", "ROLLBACK_AUTHORITY_SCHEMA",
    "RollbackAuthority", "validate_authority_verifier", "verify_a_grade_authority",
    "verify_promotion_authority", "verify_rollback_authority",
]
