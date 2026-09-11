"""Fail-closed contract boundary for a review-instruction creator.

This module validates metadata and unsigned contract-candidate JSON only.  It has no
connector invocation, browser, broker, authentication, or order primitive.
Capability probing below means validating an installed operation descriptor;
it never creates an instruction.
"""
from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import hmac
import ipaddress
import json
from pathlib import Path
import re
from urllib.parse import unquote, urlsplit


AUTH_ISOLATION = "no_secret_in_app_snapshot_or_log"
CONTRACT_VERSION = 1
EVIDENCE_VERSION = 1
CAPABILITY_EVIDENCE_MAX_AGE = timedelta(hours=24)
TRUSTED_NO_REDIRECT = "TRUSTED_NO_REDIRECT"
JSON_SCHEMA_DRAFT = "https://json-schema.org/draft/2020-12/schema"
INSTALLED_CAPABILITY_EVIDENCE_VERSION = 2
INSTALLED_CONNECTOR_ID = "asdk_app_69bc11db874881918718abaca20b68ce"
INSTALLED_PLUGIN_RELEASE = "4.0.0"
INSTALLED_CATALOG_SHA256 = (
    "369bdc7105d2363c32416bfbd024b0f02aece5c89af948fc95e53cb49b02c1f7"
)
INSTALLED_CATALOG_CANONICAL_PATH = (
    "G:/Codex/home/cache/remote_plugin_catalog/4314424b515ce642.json"
)
INSTALLED_REGISTRY_SHA256 = (
    "5f17f6fd1345b6fa9e51a5bfe52668de4811d5123b94d7bf763ebe53c3c7c084"
)
INSTALLED_REGISTRY_CANONICAL_PATH = (
    "G:/Codex/home/cache/codex_apps_tools/6c292d6e0aa93feae1c80dd3f5c201560615b3f1.json"
)
INSTALLED_CREATOR_CALLABLE = "interactive_brokers_ibkr_create_order_instruction"
INSTALLED_CREATOR_RESOURCE = "interactive_brokers_ibkr.create_order_instruction"
INSTALLED_CREATOR_INPUT_SCHEMA_SHA256 = (
    "6c5d401f7f7119cd12afd7aaf6a17a0f79ff1a99e886c6fde2132a9132a50ad2"
)
INSTALLED_CREATOR_OUTPUT_SCHEMA_SHA256 = (
    "4f73ce4ce74fedb0a60541987d960693600487cafe82cd281e1705c2618fff66"
)
INSTALLED_CREATOR_DESCRIPTION_SHA256 = (
    "ab905191469d63a40b5a767e85d5627c422021ca97c700ea21904839b6235e91"
)
INSTALLED_DELETE_INSTRUCTION_CALLABLE = (
    "interactive_brokers_ibkr_delete_order_instruction"
)
INSTALLED_DELETE_INSTRUCTION_RESOURCE = (
    "interactive_brokers_ibkr.delete_order_instruction"
)
INSTALLED_DELETE_INSTRUCTION_DESCRIPTION_SHA256 = (
    "964241f0a8b82cca7307c630996ae6f39c606955f8a8622724784420a34ae876"
)
INSTALLED_TOOL_INVENTORY_SHA256 = (
    "a85a7ccc9f31f8585a87eb518839c30fcb901bfebabf2c88f77b5efeb7e90a35"
)
INSTALLED_TOOL_COUNT = 27


class CreatorTransportValidationError(ValueError):
    """Creator metadata or unsigned contract structure failed closed."""


_IDENTIFIER_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,159}\Z")
_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_CLAIMED_ACTOR_RE = re.compile(r"human:[A-Za-z0-9][A-Za-z0-9._@+-]{0,127}\Z")
_HOST_LABEL_RE = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\Z")
_AUTH_KEY_MARKERS = (
    "password",
    "credential",
    "apikey",
    "accesskey",
    "privatekey",
    "authtoken",
    "bearertoken",
    "accesstoken",
    "refreshtoken",
    "clientsecret",
    "authorizationheader",
    "sessiontoken",
    "connectorauth",
    "oauth",
    "cookie",
)
_AUTH_VALUE_RE = re.compile(
    r"(?i)(?:bearer\s+\S+|"
    r"(?:token|password|secret|credential|api[_-]?key)\s*[:=]\s*\S+|"
    r"-----BEGIN\s+(?:RSA\s+)?PRIVATE\s+KEY-----|"
    r"(?:sk|ghp|github_pat)_[A-Za-z0-9_-]{12,})"
)

_OPERATION_FIELDS = frozenset(
    {
        "metadata_version",
        "connector_id",
        "tool_name",
        "transport",
        "operation_class",
        "review_only",
        "order_capable",
        "browser_automation",
        "auth_isolation",
        "input_schema",
        "output_schema",
        "deep_link_rules",
        "reconciliation",
        "idempotency",
    }
)
_CONTRACT_FIELDS = frozenset(
    (_OPERATION_FIELDS - {"metadata_version"})
    | {
        "contract_version",
        "capability_evidence_sha256",
        "signed_by",
        "signed_at",
        "content_sha256",
    }
)
_DEEP_LINK_RULE_FIELDS = frozenset(
    {
        "scheme",
        "allowed_hosts",
        "allowed_path_prefixes",
        "allow_query",
        "allow_fragment",
        "allow_userinfo",
    }
)
_RECONCILIATION_FIELDS = frozenset(
    {
        "instruction_id_field",
        "deep_link_field",
        "review_only_field",
        "order_submitted_field",
        "transmitted_to_broker_field",
        "uncertain_outcome",
    }
)
_IDEMPOTENCY_FIELDS = frozenset(
    {"strategy", "request_field", "retry_policy", "uncertain_outcome"}
)
_SCHEMA_KEYWORDS = frozenset(
    {
        "$schema",
        "type",
        "properties",
        "required",
        "additionalProperties",
        "items",
        "const",
        "enum",
        "format",
        "pattern",
        "minimum",
        "maximum",
        "exclusiveMinimum",
        "exclusiveMaximum",
        "minLength",
        "maxLength",
        "minItems",
        "maxItems",
        "uniqueItems",
        "title",
        "description",
    }
)
_AMBIGUOUS_SCHEMA_KEYWORDS = frozenset(
    {
        "$ref",
        "$dynamicRef",
        "$defs",
        "definitions",
        "patternProperties",
        "unevaluatedProperties",
        "propertyNames",
        "allOf",
        "anyOf",
        "oneOf",
        "not",
        "if",
        "then",
        "else",
        "dependentSchemas",
    }
)
_INSTALLED_EVIDENCE_FIELDS = frozenset(
    {
        "evidence_version",
        "evidence_kind",
        "observed_at",
        "sources",
        "provider_capability",
        "local_safety_policy",
        "activity",
        "combined_sha256",
    }
)
_EXPECTED_INSTALLED_TOOLS = (
    "interactive_brokers_ibkr_create_order_instruction",
    "interactive_brokers_ibkr_create_watchlist",
    "interactive_brokers_ibkr_delete_order_instruction",
    "interactive_brokers_ibkr_delete_watchlist",
    "interactive_brokers_ibkr_edit_watchlist",
    "interactive_brokers_ibkr_get_account_balances",
    "interactive_brokers_ibkr_get_account_orders",
    "interactive_brokers_ibkr_get_account_positions",
    "interactive_brokers_ibkr_get_account_summary",
    "interactive_brokers_ibkr_get_account_trades",
    "interactive_brokers_ibkr_get_combo_identifier",
    "interactive_brokers_ibkr_get_company_connections",
    "interactive_brokers_ibkr_get_company_themes",
    "interactive_brokers_ibkr_get_option_data",
    "interactive_brokers_ibkr_get_option_parameters",
    "interactive_brokers_ibkr_get_order_instructions",
    "interactive_brokers_ibkr_get_pa_allocation",
    "interactive_brokers_ibkr_get_pa_performance_all_periods",
    "interactive_brokers_ibkr_get_price_history",
    "interactive_brokers_ibkr_get_price_snapshot",
    "interactive_brokers_ibkr_get_theme_details",
    "interactive_brokers_ibkr_get_watchlist",
    "interactive_brokers_ibkr_get_watchlists",
    "interactive_brokers_ibkr_provide_customer_feedback",
    "interactive_brokers_ibkr_search_contracts",
    "interactive_brokers_ibkr_search_futures",
    "interactive_brokers_ibkr_search_investment_topics",
)
_FORBIDDEN_ORDER_OPERATION_SUFFIXES = (
    "cancel_order",
    "modify_order",
    "place_order",
    "submit_order",
    "transmit_order",
)
@dataclass(frozen=True, slots=True)
class _InstalledSourceSnapshot:
    source_bytes: bytes
    last_write_at: datetime
    captured_at: datetime
    canonical_path: str


@dataclass(frozen=True, slots=True)
class _CurrentInstalledSources:
    catalog: _InstalledSourceSnapshot
    registry: _InstalledSourceSnapshot


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _read_current_installed_sources() -> _CurrentInstalledSources:
    """Read both fixed canonical sources without accepting caller material."""

    return _CurrentInstalledSources(
        catalog=_read_fixed_canonical_source(
            INSTALLED_CATALOG_CANONICAL_PATH,
            label="authoritative plugin catalog",
        ),
        registry=_read_fixed_canonical_source(
            INSTALLED_REGISTRY_CANONICAL_PATH,
            label="installed runtime registry",
        ),
    )


def _read_fixed_canonical_source(
    canonical_path: str,
    *,
    label: str,
) -> _InstalledSourceSnapshot:
    source = Path(canonical_path)
    try:
        resolved = source.resolve(strict=True)
    except OSError as exc:
        raise CreatorTransportValidationError(
            f"{label} canonical source file is unavailable"
        ) from exc
    if resolved.as_posix().casefold() != source.absolute().as_posix().casefold():
        raise CreatorTransportValidationError(
            f"{label} path is not the authoritative canonical source"
        )
    try:
        before = resolved.stat()
        source_bytes = resolved.read_bytes()
        after = resolved.stat()
    except OSError as exc:
        raise CreatorTransportValidationError(
            f"{label} canonical source file is unavailable"
        ) from exc
    if (
        before.st_mtime_ns != after.st_mtime_ns
        or before.st_size != after.st_size
        or after.st_size != len(source_bytes)
    ):
        raise CreatorTransportValidationError(f"{label} changed during source capture")
    return _InstalledSourceSnapshot(
        source_bytes=source_bytes,
        last_write_at=datetime.fromtimestamp(
            after.st_mtime_ns / 1_000_000_000,
            tz=timezone.utc,
        ),
        captured_at=_utc_now(),
        canonical_path=canonical_path,
    )


def _reconcile_current_canonical_sources(
    sources: Mapping[str, object],
    current: _CurrentInstalledSources,
) -> None:
    bindings = (
        (
            "authoritative_catalog",
            current.catalog,
        ),
        (
            "installed_runtime_registry",
            current.registry,
        ),
    )
    for label, snapshot in bindings:
        current_age = snapshot.captured_at - snapshot.last_write_at
        if current_age < timedelta(0):
            raise CreatorTransportValidationError(
                f"{label} current canonical last-write time is in the future"
            )
        if current_age > CAPABILITY_EVIDENCE_MAX_AGE:
            raise CreatorTransportValidationError(
                f"{label} current canonical source is stale"
            )
        source = _json_object(label, sources[label])
        if source.get("canonical_path") != snapshot.canonical_path:
            raise CreatorTransportValidationError(
                f"{label}.canonical_path does not reconcile to the current canonical source"
            )
        if source.get("sha256") != hashlib.sha256(snapshot.source_bytes).hexdigest():
            raise CreatorTransportValidationError(
                f"{label}.sha256 does not reconcile to the current canonical source"
            )
        evidence_last_write = _parse_timestamp(
            source.get("last_write_at"),
            f"{label}.last_write_at",
        )
        if evidence_last_write > snapshot.last_write_at:
            raise CreatorTransportValidationError(
                f"{label}.last_write_at is newer than the current canonical source"
            )


def contract_content_sha256(contract: Mapping[str, object]) -> str:
    """Hash contract structure; this digest is never a human signature."""

    document = _json_object("contract", contract)
    document.pop("content_sha256", None)
    return _canonical_sha256(document)


def _build_installed_capability_evidence(
) -> dict[str, object]:
    """Bind the exact installed provider sources into redacted capability evidence.

    This is metadata inspection only.  It cannot invoke a Connector operation and
    deliberately does not manufacture the deep-link or reconciliation terms that
    belong in a future unsigned transport dossier.
    """

    acquisition = _read_current_installed_sources()
    catalog = _decode_source_document(
        acquisition.catalog.source_bytes,
        label="authoritative plugin catalog",
        expected_sha256=INSTALLED_CATALOG_SHA256,
    )
    registry = _decode_source_document(
        acquisition.registry.source_bytes,
        label="installed runtime registry",
        expected_sha256=INSTALLED_REGISTRY_SHA256,
    )
    plugin = _installed_plugin_release(catalog)
    inventory, creator, delete_instruction = _installed_tool_inventory(registry)
    input_schema = _json_object(
        "installed creator input schema", creator.get("inputSchema")
    )
    output_schema = _json_object(
        "installed creator output schema", creator.get("outputSchema")
    )
    description = creator.get("description")
    if not isinstance(description, str):
        raise CreatorTransportValidationError(
            "installed creator description must be a string"
        )
    observed = _utc_now()
    evidence: dict[str, object] = {
        "evidence_version": INSTALLED_CAPABILITY_EVIDENCE_VERSION,
        "evidence_kind": "installed_capability_evidence",
        "observed_at": _utc_timestamp_precise(observed),
        "sources": {
            "authoritative_catalog": {
                "source_identity": "authoritative_plugin_catalog",
                "canonical_path": acquisition.catalog.canonical_path,
                "schema_version": catalog.get("schema_version"),
                "sha256": INSTALLED_CATALOG_SHA256,
                "last_write_at": _utc_timestamp_precise(acquisition.catalog.last_write_at),
                "captured_at": _utc_timestamp_precise(acquisition.catalog.captured_at),
            },
            "installed_runtime_registry": {
                "source_identity": "installed_runtime_tool_registry",
                "canonical_path": acquisition.registry.canonical_path,
                "schema_version": registry.get("schema_version"),
                "sha256": INSTALLED_REGISTRY_SHA256,
                "last_write_at": _utc_timestamp_precise(acquisition.registry.last_write_at),
                "captured_at": _utc_timestamp_precise(acquisition.registry.captured_at),
            },
        },
        "provider_capability": {
            "connector_id": INSTALLED_CONNECTOR_ID,
            "plugin_release": plugin["version"],
            "runtime_operation": {
                "callable_name": INSTALLED_CREATOR_CALLABLE,
                "resource_name": INSTALLED_CREATOR_RESOURCE,
                "description_sha256": _canonical_sha256(description),
                "input_schema": input_schema,
                "input_schema_sha256": _canonical_sha256(input_schema),
                "output_schema": output_schema,
                "output_schema_sha256": _canonical_sha256(output_schema),
            },
            "provider_semantics": {
                "instruction_is_draft": True,
                "instruction_is_live_order": False,
                "user_must_review": True,
                "user_must_submit": True,
                "live_order_only_after_user_submission": True,
            },
            "tool_inventory": inventory,
            "tool_count": len(inventory),
            "tool_inventory_sha256": _canonical_sha256(inventory),
            "inventory_scope": "exact_observed_registry_snapshot",
            "forbidden_order_operations_present": _forbidden_order_operations(
                inventory
            ),
            "draft_instruction_delete_operation": {
                "callable_name": INSTALLED_DELETE_INSTRUCTION_CALLABLE,
                "resource_name": INSTALLED_DELETE_INSTRUCTION_RESOURCE,
                "description_sha256": _canonical_sha256(
                    delete_instruction["description"]
                ),
                "destructive_hint": delete_instruction["annotations"][
                    "destructiveHint"
                ],
            },
        },
        "local_safety_policy": {
            "evidence_scope": "metadata_only_non_invoking",
            "future_unsigned_transport_dossier_present": False,
            "deep_link_destination_contract_present": False,
            "creator_transport_status": "CREATOR_TRANSPORT_UNAVAILABLE",
            "crt_01": "PASS",
            "crt_02": "PASS",
            "crt_03": "BLOCKED_EXTERNAL",
            "order_authority": False,
            "delete_instruction_selected": False,
            "delete_instruction_invoked": False,
            "delete_instruction_scope": "draft_instruction_only",
            "live_order_cancel_authority": False,
        },
        "activity": {
            "connector_calls": 0,
            "instruction_calls": 0,
            "order_calls": 0,
            "network_calls": 0,
        },
    }
    _reject_source_private_material(evidence)
    evidence["combined_sha256"] = _canonical_sha256(evidence)
    return validate_installed_capability_evidence(evidence)


def validate_installed_capability_evidence(
    evidence: Mapping[str, object],
) -> dict[str, object]:
    """Strictly validate one source-bound installed capability observation."""

    checked_at = _utc_now()
    acquisition = _read_current_installed_sources()
    document = _json_object("installed capability evidence", evidence)
    _require_exact_fields(document, _INSTALLED_EVIDENCE_FIELDS, "installed evidence")
    _reject_auth_material(document)
    _reject_source_private_material(document)
    if (
        isinstance(document["evidence_version"], bool)
        or not isinstance(document["evidence_version"], int)
        or document["evidence_version"] != INSTALLED_CAPABILITY_EVIDENCE_VERSION
    ):
        raise CreatorTransportValidationError(
            "installed evidence_version must be 2"
        )
    if document["evidence_kind"] != "installed_capability_evidence":
        raise CreatorTransportValidationError(
            "evidence_kind must be installed_capability_evidence"
        )
    observed_at = _parse_canonical_fresh_observation(
        document["observed_at"], checked_at=checked_at
    )
    sources = _json_object("installed evidence sources", document["sources"])
    _require_exact_fields(
        sources,
        frozenset({"authoritative_catalog", "installed_runtime_registry"}),
        "installed evidence sources",
    )
    _validate_source_binding(
        sources["authoritative_catalog"],
        label="authoritative_catalog",
        source_identity="authoritative_plugin_catalog",
        canonical_path=INSTALLED_CATALOG_CANONICAL_PATH,
        schema_version=1,
        expected_sha256=INSTALLED_CATALOG_SHA256,
        observed_at=observed_at,
        checked_at=checked_at,
    )
    _validate_source_binding(
        sources["installed_runtime_registry"],
        label="installed_runtime_registry",
        source_identity="installed_runtime_tool_registry",
        canonical_path=INSTALLED_REGISTRY_CANONICAL_PATH,
        schema_version=4,
        expected_sha256=INSTALLED_REGISTRY_SHA256,
        observed_at=observed_at,
        checked_at=checked_at,
    )
    _reconcile_current_canonical_sources(sources, acquisition)
    provider = _json_object(
        "installed provider capability", document["provider_capability"]
    )
    _require_exact_fields(
        provider,
        frozenset(
            {
                "connector_id",
                "plugin_release",
                "runtime_operation",
                "provider_semantics",
                "tool_inventory",
                "tool_count",
                "tool_inventory_sha256",
                "inventory_scope",
                "forbidden_order_operations_present",
                "draft_instruction_delete_operation",
            }
        ),
        "installed provider capability",
    )
    if provider["connector_id"] != INSTALLED_CONNECTOR_ID:
        raise CreatorTransportValidationError("installed connector_id mismatch")
    if provider["plugin_release"] != INSTALLED_PLUGIN_RELEASE:
        raise CreatorTransportValidationError("installed plugin release mismatch")
    _validate_installed_runtime_operation(provider["runtime_operation"])
    _validate_provider_semantics(provider["provider_semantics"])
    _validate_delete_instruction_operation(
        provider["draft_instruction_delete_operation"]
    )
    if provider["inventory_scope"] != "exact_observed_registry_snapshot":
        raise CreatorTransportValidationError("installed inventory scope mismatch")
    inventory = _validate_installed_inventory(provider)
    if _forbidden_order_operations(inventory):
        raise CreatorTransportValidationError(
            "installed inventory contains an order submission operation"
        )
    policy = _json_object(
        "installed capability local safety policy", document["local_safety_policy"]
    )
    expected_policy = {
        "evidence_scope": "metadata_only_non_invoking",
        "future_unsigned_transport_dossier_present": False,
        "deep_link_destination_contract_present": False,
        "creator_transport_status": "CREATOR_TRANSPORT_UNAVAILABLE",
        "crt_01": "PASS",
        "crt_02": "PASS",
        "crt_03": "BLOCKED_EXTERNAL",
        "order_authority": False,
        "delete_instruction_selected": False,
        "delete_instruction_invoked": False,
        "delete_instruction_scope": "draft_instruction_only",
        "live_order_cancel_authority": False,
    }
    if not hmac.compare_digest(
        _canonical_json_bytes(policy), _canonical_json_bytes(expected_policy)
    ):
        raise CreatorTransportValidationError(
            "installed capability local safety policy mismatch"
        )
    activity = _json_object("installed capability activity", document["activity"])
    expected_activity = {
        "connector_calls": 0,
        "instruction_calls": 0,
        "order_calls": 0,
        "network_calls": 0,
    }
    if not hmac.compare_digest(
        _canonical_json_bytes(activity), _canonical_json_bytes(expected_activity)
    ):
        raise CreatorTransportValidationError(
            "installed capability activity must contain exact zero call counts"
        )
    combined_hash = _sha256(document["combined_sha256"], "combined_sha256")
    unhashed = dict(document)
    unhashed.pop("combined_sha256")
    if not hmac.compare_digest(combined_hash, _canonical_sha256(unhashed)):
        raise CreatorTransportValidationError(
            "combined_sha256 does not match installed capability evidence"
        )
    document["observed_at"] = _utc_timestamp_precise(observed_at)
    return document


def validate_creator_transport_contract(
    contract: Mapping[str, object],
    *,
    capability_evidence: Mapping[str, object] | None = None,
    checked_at: datetime | None = None,
) -> dict[str, object]:
    """Validate unsigned structure without granting transport authority."""

    document = _json_object("creator transport contract", contract)
    _require_exact_fields(document, _CONTRACT_FIELDS, "contract")
    _reject_auth_material(document)
    if (
        isinstance(document["contract_version"], bool)
        or not isinstance(document["contract_version"], int)
        or document["contract_version"] != CONTRACT_VERSION
    ):
        raise CreatorTransportValidationError("contract_version must be 1")
    _validate_operation_core(document)
    _sha256(
        document["capability_evidence_sha256"],
        "capability_evidence_sha256",
    )
    signed_by = document["signed_by"]
    if (
        not isinstance(signed_by, str)
        or _CLAIMED_ACTOR_RE.fullmatch(signed_by) is None
    ):
        raise CreatorTransportValidationError(
            "signed_by claim must use the structural form human:<actor>"
        )
    _claimed_signature_timestamp(document["signed_at"])
    content_hash = _sha256(document["content_sha256"], "content_sha256")
    expected_hash = contract_content_sha256(document)
    if not hmac.compare_digest(content_hash, expected_hash):
        raise CreatorTransportValidationError(
            "content_sha256 does not match the signed contract content"
        )
    if capability_evidence is None:
        raise CreatorTransportValidationError(
            "capability evidence is required; a contract cannot validate in isolation"
        )
    _bind_contract_to_evidence(
        document,
        capability_evidence,
        checked_at=checked_at,
    )
    return {
        "status": "CREATOR_TRANSPORT_STRUCTURE_VERIFIED_UNSIGNED",
        "reason": "WAITING_GENUINE_HUMAN_SIGNATURE",
        "creator_transport_status": "CREATOR_TRANSPORT_UNAVAILABLE",
        "contract_activatable": False,
        "human_signature_verified": False,
        "claimed_signed_by": signed_by,
        "claimed_signed_at": document["signed_at"],
        "connector_id": document["connector_id"],
        "tool_name": document["tool_name"],
        "contract_version": document["contract_version"],
        "content_sha256": content_hash,
        "capability_evidence_sha256": document["capability_evidence_sha256"],
        "review_only": True,
        "order_capable": False,
        "browser_automation": False,
    }


def discover_supported_operations(
    operations: Iterable[Mapping[str, object]],
) -> list[dict[str, object]]:
    """Return only exact review-only descriptors, without invoking any tool."""

    if isinstance(operations, (str, bytes, bytearray, Mapping)):
        raise CreatorTransportValidationError(
            "managed connector operation inventory must be an iterable of objects"
        )
    supported: list[dict[str, object]] = []
    for operation in operations:
        try:
            supported.append(_validate_operation_descriptor(operation))
        except (CreatorTransportValidationError, TypeError, ValueError):
            # Discovery is intentionally non-reflective.  A rejected descriptor
            # may itself contain sensitive text, so no value or exception is
            # copied into evidence or logs.
            continue
    supported.sort(
        key=lambda item: (str(item["connector_id"]), str(item["tool_name"]))
    )
    return supported


def build_capability_probe_evidence(
    operations: Iterable[Mapping[str, object]],
    *,
    observed_at: datetime | None = None,
) -> dict[str, object]:
    """Build redacted metadata-only evidence; no connector call is possible."""

    inventory = list(operations)
    supported = discover_supported_operations(inventory)
    timestamp = _utc_timestamp(observed_at or datetime.now(timezone.utc))
    evidence: dict[str, object] = {
        "evidence_version": EVIDENCE_VERSION,
        "probe_kind": "local_metadata_only_non_order",
        "status": (
            "SUPPORTED_OPERATIONS_FOUND"
            if supported
            else "CREATOR_TRANSPORT_UNAVAILABLE"
        ),
        "observed_at": timestamp,
        "supported_operations": supported,
        "rejected_operation_count": len(inventory) - len(supported),
        "instruction_created": False,
        "connector_invoked": False,
        "order_operation_invoked": False,
        "browser_automation_used": False,
        "auth_isolation": AUTH_ISOLATION,
    }
    _reject_auth_material(evidence)
    evidence["evidence_sha256"] = _canonical_sha256(evidence)
    return evidence


def validate_capability_probe_evidence(
    evidence: Mapping[str, object],
    *,
    checked_at: datetime | None = None,
) -> dict[str, object]:
    """Verify probe evidence before it is bound into a signed contract."""

    document = _json_object("capability probe evidence", evidence)
    expected_fields = {
        "evidence_version",
        "probe_kind",
        "status",
        "observed_at",
        "supported_operations",
        "rejected_operation_count",
        "instruction_created",
        "connector_invoked",
        "order_operation_invoked",
        "browser_automation_used",
        "auth_isolation",
        "evidence_sha256",
    }
    _require_exact_fields(document, frozenset(expected_fields), "evidence")
    _reject_auth_material(document)
    if (
        isinstance(document["evidence_version"], bool)
        or not isinstance(document["evidence_version"], int)
        or document["evidence_version"] != EVIDENCE_VERSION
    ):
        raise CreatorTransportValidationError("evidence_version must be 1")
    if document["probe_kind"] != "local_metadata_only_non_order":
        raise CreatorTransportValidationError(
            "probe_kind must be local_metadata_only_non_order"
        )
    if document["auth_isolation"] != AUTH_ISOLATION:
        raise CreatorTransportValidationError("evidence auth_isolation is invalid")
    for field in (
        "instruction_created",
        "connector_invoked",
        "order_operation_invoked",
        "browser_automation_used",
    ):
        if document[field] is not False:
            raise CreatorTransportValidationError(f"evidence.{field} must be false")
    rejected = document["rejected_operation_count"]
    if isinstance(rejected, bool) or not isinstance(rejected, int) or rejected < 0:
        raise CreatorTransportValidationError(
            "evidence.rejected_operation_count must be a nonnegative integer"
        )
    observed_at = _parse_timestamp(
        document["observed_at"], "evidence.observed_at"
    )
    if (
        not isinstance(document["observed_at"], str)
        or _utc_timestamp(observed_at) != document["observed_at"]
    ):
        raise CreatorTransportValidationError(
            "evidence.observed_at must be a canonical UTC timestamp ending in Z"
        )
    raw_supported = document["supported_operations"]
    if not isinstance(raw_supported, list):
        raise CreatorTransportValidationError(
            "evidence.supported_operations must be an array"
        )
    supported = discover_supported_operations(raw_supported)
    if not hmac.compare_digest(
        _canonical_json_bytes(supported),
        _canonical_json_bytes(raw_supported),
    ):
        raise CreatorTransportValidationError(
            "evidence contains an unsupported operation descriptor"
        )
    expected_status = (
        "SUPPORTED_OPERATIONS_FOUND"
        if supported
        else "CREATOR_TRANSPORT_UNAVAILABLE"
    )
    if document["status"] != expected_status:
        raise CreatorTransportValidationError(
            "evidence status does not match supported operations"
        )
    actual_hash = _sha256(document["evidence_sha256"], "evidence_sha256")
    unhashed = dict(document)
    unhashed.pop("evidence_sha256")
    expected_hash = _canonical_sha256(unhashed)
    if not hmac.compare_digest(actual_hash, expected_hash):
        raise CreatorTransportValidationError(
            "evidence_sha256 does not match probe evidence"
        )
    checked = checked_at or datetime.now(timezone.utc)
    if (
        not isinstance(checked, datetime)
        or checked.tzinfo is None
        or checked.utcoffset() is None
    ):
        raise CreatorTransportValidationError(
            "capability evidence check time must be timezone-aware"
        )
    age = checked.astimezone(timezone.utc) - observed_at
    if age < timedelta(0) or age > CAPABILITY_EVIDENCE_MAX_AGE:
        raise CreatorTransportValidationError(
            "capability evidence is not fresh within the 24-hour bound"
        )
    return document


def validate_deep_link(
    deep_link: str,
    rules: Mapping[str, object],
    *,
    redirect_provenance: object = None,
) -> str:
    """Validate one returned URL against the exact signed host/path rules."""

    normalized_rules = _validate_deep_link_rules(rules)
    if redirect_provenance != TRUSTED_NO_REDIRECT:
        raise CreatorTransportValidationError(
            "deep link redirect provenance must explicitly prove no redirect"
        )
    if not isinstance(deep_link, str) or not deep_link or deep_link != deep_link.strip():
        raise CreatorTransportValidationError(
            "deep link must be a nonblank canonical HTTPS URL"
        )
    decoded = unquote(deep_link)
    if decoded != deep_link or "\\" in deep_link or any(
        ord(character) < 32 or character.isspace() for character in deep_link
    ):
        raise CreatorTransportValidationError(
            "deep link contains encoded, control, or ambiguous characters"
        )
    try:
        parsed = urlsplit(deep_link)
        port = parsed.port
    except ValueError as exc:
        raise CreatorTransportValidationError("deep link is malformed") from exc
    if parsed.scheme != "https" or not parsed.netloc or parsed.hostname is None:
        raise CreatorTransportValidationError(
            "deep link must use HTTPS with an explicit host"
        )
    if parsed.username is not None or parsed.password is not None:
        raise CreatorTransportValidationError(
            "deep link cannot contain URL credentials"
        )
    if port is not None:
        raise CreatorTransportValidationError(
            "deep link cannot contain an explicit port"
        )
    if parsed.query:
        raise CreatorTransportValidationError("deep link query is forbidden")
    if parsed.fragment:
        raise CreatorTransportValidationError("deep link fragment is forbidden")
    host = parsed.hostname
    if (
        not deep_link.startswith("https://")
        or parsed.netloc != host
        or host != host.casefold()
        or host.endswith(".")
    ):
        raise CreatorTransportValidationError(
            "deep link host or scheme is not canonical"
        )
    allowed_hosts = normalized_rules["allowed_hosts"]
    assert isinstance(allowed_hosts, list)
    if host not in allowed_hosts:
        raise CreatorTransportValidationError("deep link host is not contracted")
    prefixes = normalized_rules["allowed_path_prefixes"]
    assert isinstance(prefixes, list)
    path_segments = parsed.path.split("/")
    if (
        any(segment in {".", ".."} for segment in path_segments)
        or any(not segment for segment in path_segments[1:-1])
    ):
        raise CreatorTransportValidationError(
            "deep link path contains a non-canonical segment"
        )
    if not any(parsed.path.startswith(prefix) for prefix in prefixes):
        raise CreatorTransportValidationError("deep link path is not contracted")
    return deep_link


def _validate_operation_descriptor(
    operation: Mapping[str, object],
) -> dict[str, object]:
    document = _json_object("managed connector operation", operation)
    _require_exact_fields(document, _OPERATION_FIELDS, "operation")
    _reject_auth_material(document)
    if (
        isinstance(document["metadata_version"], bool)
        or not isinstance(document["metadata_version"], int)
        or document["metadata_version"] != 1
    ):
        raise CreatorTransportValidationError("operation.metadata_version must be 1")
    _validate_operation_core(document)
    return document


def _validate_operation_core(document: Mapping[str, object]) -> None:
    _identifier(document.get("connector_id"), "connector_id")
    _identifier(document.get("tool_name"), "tool_name")
    if document.get("transport") != "managed_connector":
        raise CreatorTransportValidationError(
            "transport must be managed_connector"
        )
    if document.get("operation_class") != "review_instruction_creator":
        raise CreatorTransportValidationError(
            "operation_class must be review_instruction_creator"
        )
    if document.get("review_only") is not True:
        raise CreatorTransportValidationError("review_only must be true")
    if document.get("order_capable") is not False:
        raise CreatorTransportValidationError("order-capable operations are forbidden")
    if document.get("browser_automation") is not False:
        raise CreatorTransportValidationError("browser automation is forbidden")
    if document.get("auth_isolation") != AUTH_ISOLATION:
        raise CreatorTransportValidationError(
            f"auth_isolation must be {AUTH_ISOLATION}"
        )
    input_schema = _validate_exact_schema(
        document.get("input_schema"), "input_schema", root=True
    )
    output_schema = _validate_exact_schema(
        document.get("output_schema"), "output_schema", root=True
    )
    _validate_review_schema(input_schema, output_schema)
    _validate_deep_link_rules(document.get("deep_link_rules"))
    _validate_reconciliation(document.get("reconciliation"), output_schema)
    _validate_idempotency(document.get("idempotency"), input_schema)


def _validate_review_schema(
    input_schema: Mapping[str, object],
    output_schema: Mapping[str, object],
) -> None:
    input_properties = input_schema.get("properties")
    output_properties = output_schema.get("properties")
    assert isinstance(input_properties, Mapping)
    assert isinstance(output_properties, Mapping)
    required_inputs = {
        "idempotency_key",
        "proposal",
        "instruction_intent",
        "review_only",
    }
    required_outputs = {
        "review_only",
        "order_submitted",
        "transmitted_to_broker",
        "instruction_id",
        "deep_link",
    }
    if not required_inputs.issubset(input_properties):
        raise CreatorTransportValidationError(
            "input_schema is missing required review-only fields"
        )
    if not required_outputs.issubset(output_properties):
        raise CreatorTransportValidationError(
            "output_schema is missing reconciliation fields"
        )
    if _property_const(input_properties, "review_only") is not True:
        raise CreatorTransportValidationError(
            "input_schema.review_only must be const true"
        )
    if _property_const(output_properties, "review_only") is not True:
        raise CreatorTransportValidationError(
            "output_schema.review_only must be const true"
        )
    for field in ("order_submitted", "transmitted_to_broker"):
        if _property_const(output_properties, field) is not False:
            raise CreatorTransportValidationError(
                f"output_schema.{field} must be const false"
            )


def _validate_exact_schema(
    value: object,
    label: str,
    *,
    root: bool = False,
) -> dict[str, object]:
    schema = _json_object(label, value)
    if not schema:
        raise CreatorTransportValidationError(f"{label} schema cannot be empty")
    ambiguous = sorted(set(schema).intersection(_AMBIGUOUS_SCHEMA_KEYWORDS))
    unknown = sorted(set(schema).difference(_SCHEMA_KEYWORDS))
    if ambiguous or unknown:
        fields = ambiguous + unknown
        raise CreatorTransportValidationError(
            f"{label} schema contains wildcard or unsupported keywords: "
            + ", ".join(fields)
        )
    _validate_schema_keyword_shapes(schema, label)
    if root and schema.get("$schema") != JSON_SCHEMA_DRAFT:
        raise CreatorTransportValidationError(
            f"{label} schema must declare JSON Schema 2020-12"
        )
    if not root and "$schema" in schema:
        raise CreatorTransportValidationError(
            f"{label} nested schema cannot change the schema dialect"
        )
    schema_type = schema.get("type")
    if isinstance(schema_type, list):
        raise CreatorTransportValidationError(
            f"{label} schema union types are not exact"
        )
    if schema_type is not None and schema_type not in {
        "object",
        "array",
        "string",
        "integer",
        "number",
        "boolean",
        "null",
    }:
        raise CreatorTransportValidationError(f"{label} schema type is invalid")
    if schema_type == "object" or "properties" in schema:
        if schema_type != "object":
            raise CreatorTransportValidationError(
                f"{label} schema with properties must have type object"
            )
        properties = schema.get("properties")
        if not isinstance(properties, dict) or not properties:
            raise CreatorTransportValidationError(
                f"{label} object schema requires explicit properties"
            )
        if schema.get("additionalProperties") is not False:
            raise CreatorTransportValidationError(
                f"{label} object schema must set additionalProperties false"
            )
        required = schema.get("required")
        if (
            not isinstance(required, list)
            or any(not isinstance(item, str) for item in required)
            or len(required) != len(set(required))
            or set(required) != set(properties)
        ):
            raise CreatorTransportValidationError(
                f"{label} schema required fields must exactly match properties"
            )
        for name, child in properties.items():
            if (
                not isinstance(name, str)
                or not name
                or any(character in name for character in "*?[]{}")
            ):
                raise CreatorTransportValidationError(
                    f"{label} schema contains a wildcard property"
                )
            _validate_exact_schema(child, f"{label}.properties.{name}")
    elif any(key in schema for key in ("properties", "required", "additionalProperties")):
        raise CreatorTransportValidationError(
            f"{label} non-object schema contains object keywords"
        )
    if schema_type == "array" or "items" in schema:
        if schema_type != "array" or not isinstance(schema.get("items"), Mapping):
            raise CreatorTransportValidationError(
                f"{label} array schema requires one exact items schema"
            )
        _validate_exact_schema(schema["items"], f"{label}.items")
    return schema


def _validate_schema_keyword_shapes(
    schema: Mapping[str, object],
    label: str,
) -> None:
    for key in ("$schema", "type", "format", "pattern", "title", "description"):
        if key in schema and not isinstance(schema[key], str):
            raise CreatorTransportValidationError(
                f"{label} schema keyword {key} must be a string"
            )
    for key in ("minLength", "maxLength", "minItems", "maxItems"):
        value = schema.get(key)
        if key in schema and (
            isinstance(value, bool)
            or not isinstance(value, int)
            or value < 0
        ):
            raise CreatorTransportValidationError(
                f"{label} schema keyword {key} must be a nonnegative integer"
            )
    for key in ("minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum"):
        value = schema.get(key)
        if key in schema and (
            isinstance(value, bool) or not isinstance(value, (int, float))
        ):
            raise CreatorTransportValidationError(
                f"{label} schema keyword {key} must be a JSON number"
            )
    if "uniqueItems" in schema and not isinstance(schema["uniqueItems"], bool):
        raise CreatorTransportValidationError(
            f"{label} schema keyword uniqueItems must be a boolean"
        )
    if "enum" in schema:
        enum = schema["enum"]
        if not isinstance(enum, list) or not enum:
            raise CreatorTransportValidationError(
                f"{label} schema keyword enum must be a nonempty array"
            )
        encoded = [_canonical_json_bytes(item) for item in enum]
        if len(encoded) != len(set(encoded)):
            raise CreatorTransportValidationError(
                f"{label} schema keyword enum contains duplicate values"
            )
    pattern = schema.get("pattern")
    if isinstance(pattern, str):
        try:
            re.compile(pattern)
        except re.error as exc:
            raise CreatorTransportValidationError(
                f"{label} schema keyword pattern is invalid"
            ) from exc
    for lower, upper in (
        ("minLength", "maxLength"),
        ("minItems", "maxItems"),
        ("minimum", "maximum"),
    ):
        if lower in schema and upper in schema and schema[lower] > schema[upper]:
            raise CreatorTransportValidationError(
                f"{label} schema keyword {lower} exceeds {upper}"
            )


def _validate_deep_link_rules(value: object) -> dict[str, object]:
    rules = _json_object("deep_link_rules", value)
    _require_exact_fields(rules, _DEEP_LINK_RULE_FIELDS, "deep_link_rules")
    if rules["scheme"] != "https":
        raise CreatorTransportValidationError(
            "deep_link_rules.scheme must be https"
        )
    for flag in ("allow_query", "allow_fragment", "allow_userinfo"):
        if rules[flag] is not False:
            raise CreatorTransportValidationError(
                f"deep_link_rules.{flag} must be false"
            )
    raw_hosts = rules["allowed_hosts"]
    if not isinstance(raw_hosts, list) or not raw_hosts:
        raise CreatorTransportValidationError(
            "deep_link_rules.allowed_hosts must be a nonempty exact array"
        )
    hosts: list[str] = []
    for raw_host in raw_hosts:
        if not isinstance(raw_host, str) or raw_host != raw_host.casefold():
            raise CreatorTransportValidationError(
                "deep_link_rules allowed host must be lowercase"
            )
        host = raw_host.rstrip(".")
        if (
            host != raw_host
            or "*" in host
            or "/" in host
            or "@" in host
            or ":" in host
            or host == "localhost"
            or host.endswith((".localhost", ".local"))
        ):
            raise CreatorTransportValidationError(
                "deep_link_rules contains an unknown or wildcard host"
            )
        try:
            ipaddress.ip_address(host)
        except ValueError:
            labels = host.split(".")
            if len(labels) < 2 or any(
                _HOST_LABEL_RE.fullmatch(label) is None for label in labels
            ):
                raise CreatorTransportValidationError(
                    "deep_link_rules contains an invalid host"
                )
        else:
            raise CreatorTransportValidationError(
                "deep_link_rules hosts must be exact DNS names, not IP addresses"
            )
        hosts.append(host)
    if len(hosts) != len(set(hosts)):
        raise CreatorTransportValidationError(
            "deep_link_rules.allowed_hosts contains duplicates"
        )
    raw_prefixes = rules["allowed_path_prefixes"]
    if not isinstance(raw_prefixes, list) or not raw_prefixes:
        raise CreatorTransportValidationError(
            "deep_link_rules.allowed_path_prefixes must be a nonempty exact array"
        )
    prefixes: list[str] = []
    for prefix in raw_prefixes:
        if (
            not isinstance(prefix, str)
            or not prefix.startswith("/")
            or not prefix.endswith("/")
            or prefix == "/"
            or unquote(prefix) != prefix
            or "\\" in prefix
            or ".." in prefix.split("/")
            or any(character in prefix for character in "*?[]{}#")
            or any(ord(character) < 32 or character.isspace() for character in prefix)
        ):
            raise CreatorTransportValidationError(
                "deep_link_rules contains an unknown or wildcard path"
            )
        prefixes.append(prefix)
    if len(prefixes) != len(set(prefixes)):
        raise CreatorTransportValidationError(
            "deep_link_rules.allowed_path_prefixes contains duplicates"
        )
    return rules


def _validate_reconciliation(
    value: object,
    output_schema: Mapping[str, object],
) -> dict[str, object]:
    reconciliation = _json_object("reconciliation", value)
    _require_exact_fields(
        reconciliation, _RECONCILIATION_FIELDS, "reconciliation"
    )
    if reconciliation["uncertain_outcome"] != "manual_reconciliation_required":
        raise CreatorTransportValidationError(
            "reconciliation.uncertain_outcome must require manual reconciliation"
        )
    properties = output_schema.get("properties")
    assert isinstance(properties, Mapping)
    expected = {
        "instruction_id_field": "instruction_id",
        "deep_link_field": "deep_link",
        "review_only_field": "review_only",
        "order_submitted_field": "order_submitted",
        "transmitted_to_broker_field": "transmitted_to_broker",
    }
    for setting, field in expected.items():
        if reconciliation[setting] != field or field not in properties:
            raise CreatorTransportValidationError(
                f"reconciliation.{setting} must bind output field {field}"
            )
    return reconciliation


def _validate_idempotency(
    value: object,
    input_schema: Mapping[str, object],
) -> dict[str, object]:
    idempotency = _json_object("idempotency", value)
    _require_exact_fields(idempotency, _IDEMPOTENCY_FIELDS, "idempotency")
    expected = {
        "strategy": "approval_id",
        "request_field": "idempotency_key",
        "retry_policy": "never_after_attempt",
        "uncertain_outcome": "manual_reconciliation_required",
    }
    for field, expected_value in expected.items():
        if idempotency[field] != expected_value:
            raise CreatorTransportValidationError(
                f"idempotency.{field} must be {expected_value}"
            )
    properties = input_schema.get("properties")
    assert isinstance(properties, Mapping)
    if idempotency["request_field"] not in properties:
        raise CreatorTransportValidationError(
            "idempotency request field is absent from input_schema"
        )
    return idempotency


def _bind_contract_to_evidence(
    contract: Mapping[str, object],
    evidence: Mapping[str, object],
    *,
    checked_at: datetime | None,
) -> None:
    validated = validate_capability_probe_evidence(
        evidence,
        checked_at=checked_at,
    )
    evidence_hash = str(validated["evidence_sha256"])
    if not hmac.compare_digest(
        str(contract["capability_evidence_sha256"]), evidence_hash
    ):
        raise CreatorTransportValidationError(
            "capability_evidence_sha256 does not bind supplied evidence"
        )
    supported = validated["supported_operations"]
    assert isinstance(supported, list)
    core = {
        key: contract[key]
        for key in _OPERATION_FIELDS
        if key != "metadata_version"
    }
    expected_core = _canonical_json_bytes(core)
    matches = []
    for operation in supported:
        operation_core = {
            key: operation[key]
            for key in _OPERATION_FIELDS
            if key != "metadata_version"
        }
        if hmac.compare_digest(
            _canonical_json_bytes(operation_core),
            expected_core,
        ):
            matches.append(operation)
    if len(matches) != 1:
        raise CreatorTransportValidationError(
            "signed contract does not bind exactly one supported operation"
        )


def _property_const(properties: Mapping[str, object], field: str) -> object:
    schema = properties.get(field)
    return schema.get("const") if isinstance(schema, Mapping) else None


def _decode_source_document(
    raw: bytes,
    *,
    label: str,
    expected_sha256: str,
) -> dict[str, object]:
    if not isinstance(raw, bytes):
        raise CreatorTransportValidationError(f"{label} must be exact source bytes")
    if not hmac.compare_digest(hashlib.sha256(raw).hexdigest(), expected_sha256):
        raise CreatorTransportValidationError(f"{label} SHA-256 mismatch")

    def reject_duplicate_keys(
        pairs: list[tuple[str, object]],
    ) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise CreatorTransportValidationError(
                    f"{label} contains duplicate JSON field {key!r}"
                )
            result[key] = value
        return result

    def reject_constant(value: str) -> object:
        raise CreatorTransportValidationError(
            f"{label} contains non-finite JSON constant {value!r}"
        )

    try:
        decoded = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=reject_duplicate_keys,
            parse_constant=reject_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CreatorTransportValidationError(
            f"{label} is not valid UTF-8 JSON"
        ) from exc
    if not isinstance(decoded, dict):
        raise CreatorTransportValidationError(f"{label} must be a JSON object")
    return decoded


def _installed_plugin_release(catalog: Mapping[str, object]) -> dict[str, object]:
    if catalog.get("schema_version") != 1 or isinstance(
        catalog.get("schema_version"), bool
    ):
        raise CreatorTransportValidationError(
            "authoritative plugin catalog schema version mismatch"
        )
    plugins = catalog.get("plugins")
    if not isinstance(plugins, list):
        raise CreatorTransportValidationError(
            "authoritative plugin catalog plugins must be an array"
        )
    matches: list[dict[str, object]] = []
    for raw_plugin in plugins:
        if not isinstance(raw_plugin, Mapping):
            continue
        release = raw_plugin.get("release")
        if not isinstance(release, Mapping):
            continue
        app_ids = release.get("app_ids")
        if isinstance(app_ids, list) and INSTALLED_CONNECTOR_ID in app_ids:
            matches.append(_json_object("installed plugin release", release))
    if len(matches) != 1:
        raise CreatorTransportValidationError(
            "installed connector must bind exactly one authoritative plugin release"
        )
    release = matches[0]
    if release.get("version") != INSTALLED_PLUGIN_RELEASE:
        raise CreatorTransportValidationError("installed plugin release mismatch")
    return release


def _installed_tool_inventory(
    registry: Mapping[str, object],
) -> tuple[list[dict[str, str]], dict[str, object], dict[str, object]]:
    if registry.get("schema_version") != 4 or isinstance(
        registry.get("schema_version"), bool
    ):
        raise CreatorTransportValidationError(
            "installed runtime registry schema version mismatch"
        )
    raw_tools = registry.get("tools")
    if not isinstance(raw_tools, list):
        raise CreatorTransportValidationError(
            "installed runtime registry tools must be an array"
        )
    inventory: list[dict[str, str]] = []
    creator_matches: list[dict[str, object]] = []
    delete_instruction_matches: list[dict[str, object]] = []
    for raw_wrapper in raw_tools:
        if not isinstance(raw_wrapper, Mapping):
            continue
        if raw_wrapper.get("connector_id") != INSTALLED_CONNECTOR_ID:
            continue
        callable_name = raw_wrapper.get("tool_name")
        tool = raw_wrapper.get("tool")
        if not isinstance(callable_name, str) or not isinstance(tool, Mapping):
            raise CreatorTransportValidationError(
                "installed runtime operation has invalid callable metadata"
            )
        metadata = tool.get("_meta")
        if not isinstance(metadata, Mapping):
            raise CreatorTransportValidationError(
                "installed runtime operation has invalid resource metadata"
            )
        resource_name = metadata.get("resource_name")
        if not isinstance(resource_name, str):
            raise CreatorTransportValidationError(
                "installed runtime operation resource_name must be a string"
            )
        if tool.get("name") != resource_name:
            raise CreatorTransportValidationError(
                "installed runtime callable and resource metadata mismatch"
            )
        inventory.append(
            {"callable_name": callable_name, "resource_name": resource_name}
        )
        if callable_name == INSTALLED_CREATOR_CALLABLE:
            creator_matches.append(_json_object("installed creator operation", tool))
        if callable_name == INSTALLED_DELETE_INSTRUCTION_CALLABLE:
            delete_instruction_matches.append(
                _json_object("installed delete-instruction operation", tool)
            )
    inventory.sort(key=lambda item: item["callable_name"])
    if len(creator_matches) != 1:
        raise CreatorTransportValidationError(
            "installed registry must contain exactly one creator callable"
        )
    if len(delete_instruction_matches) != 1:
        raise CreatorTransportValidationError(
            "installed registry must contain exactly one delete-instruction callable"
        )
    if len(inventory) != INSTALLED_TOOL_COUNT:
        raise CreatorTransportValidationError("installed tool count mismatch")
    if _canonical_sha256(inventory) != INSTALLED_TOOL_INVENTORY_SHA256:
        raise CreatorTransportValidationError("installed tool inventory mismatch")
    creator = creator_matches[0]
    if creator.get("name") != INSTALLED_CREATOR_RESOURCE:
        raise CreatorTransportValidationError("installed creator resource mismatch")
    metadata = creator.get("_meta")
    if (
        not isinstance(metadata, Mapping)
        or metadata.get("resource_name") != INSTALLED_CREATOR_RESOURCE
        or metadata.get("connector_id") != INSTALLED_CONNECTOR_ID
    ):
        raise CreatorTransportValidationError(
            "installed creator source identity mismatch"
        )
    delete_instruction = delete_instruction_matches[0]
    annotations = delete_instruction.get("annotations")
    if (
        delete_instruction.get("name") != INSTALLED_DELETE_INSTRUCTION_RESOURCE
        or not isinstance(delete_instruction.get("description"), str)
        or not isinstance(annotations, Mapping)
        or annotations.get("destructiveHint") is not True
    ):
        raise CreatorTransportValidationError(
            "installed delete-instruction source semantics mismatch"
        )
    return inventory, creator, delete_instruction


def _forbidden_order_operations(
    inventory: Sequence[Mapping[str, object]],
) -> list[str]:
    forbidden: list[str] = []
    for item in inventory:
        for field in ("callable_name", "resource_name"):
            value = item.get(field)
            if isinstance(value, str) and any(
                value.casefold().endswith(suffix)
                for suffix in _FORBIDDEN_ORDER_OPERATION_SUFFIXES
            ):
                forbidden.append(value)
    return sorted(set(forbidden))


def _parse_canonical_fresh_observation(
    value: object,
    *,
    checked_at: datetime,
) -> datetime:
    observed_at = _parse_timestamp(value, "installed evidence.observed_at")
    if not isinstance(value, str) or _utc_timestamp_precise(observed_at) != value:
        raise CreatorTransportValidationError(
            "installed evidence observed_at must be canonical UTC"
        )
    checked = checked_at
    if (
        not isinstance(checked, datetime)
        or checked.tzinfo is None
        or checked.utcoffset() is None
    ):
        raise CreatorTransportValidationError(
            "installed evidence check time must be timezone-aware"
        )
    age = checked.astimezone(timezone.utc) - observed_at
    if age < timedelta(0) or age > CAPABILITY_EVIDENCE_MAX_AGE:
        raise CreatorTransportValidationError(
            "installed capability evidence is stale or future-dated"
        )
    return observed_at


def _validate_source_binding(
    value: object,
    *,
    label: str,
    source_identity: str,
    canonical_path: str,
    schema_version: int,
    expected_sha256: str,
    observed_at: datetime,
    checked_at: datetime,
) -> None:
    source = _json_object(label, value)
    _require_exact_fields(
        source,
        frozenset(
            {
                "source_identity",
                "canonical_path",
                "schema_version",
                "sha256",
                "last_write_at",
                "captured_at",
            }
        ),
        label,
    )
    if source["source_identity"] != source_identity:
        raise CreatorTransportValidationError(f"{label} source identity mismatch")
    if source["canonical_path"] != canonical_path:
        raise CreatorTransportValidationError(f"{label} canonical path mismatch")
    version = source["schema_version"]
    if isinstance(version, bool) or not isinstance(version, int) or version != schema_version:
        raise CreatorTransportValidationError(f"{label} schema version mismatch")
    if not hmac.compare_digest(_sha256(source["sha256"], f"{label}.sha256"), expected_sha256):
        raise CreatorTransportValidationError(f"{label} source SHA-256 mismatch")
    last_write_at = _parse_timestamp(source["last_write_at"], f"{label}.last_write_at")
    captured_at = _parse_timestamp(source["captured_at"], f"{label}.captured_at")
    for field, raw_value, parsed in (
        ("last_write_at", source["last_write_at"], last_write_at),
        ("captured_at", source["captured_at"], captured_at),
    ):
        if not isinstance(raw_value, str) or _utc_timestamp_precise(parsed) != raw_value:
            raise CreatorTransportValidationError(
                f"{label}.{field} must be a canonical UTC timestamp"
            )
    if last_write_at > captured_at:
        raise CreatorTransportValidationError(
            f"{label} source last-write timestamp is after capture"
        )
    checked = checked_at
    if (
        not isinstance(checked, datetime)
        or checked.tzinfo is None
        or checked.utcoffset() is None
    ):
        raise CreatorTransportValidationError(
            "installed evidence check time must be timezone-aware"
        )
    checked = checked.astimezone(timezone.utc)
    for field, timestamp in (
        ("last_write_at", last_write_at),
        ("captured_at", captured_at),
    ):
        for boundary_name, boundary in (
            ("observation", observed_at),
            ("check", checked),
        ):
            age = boundary - timestamp
            if age < timedelta(0):
                raise CreatorTransportValidationError(
                    f"{label}.{field} is after {boundary_name} time"
                )
            if age > CAPABILITY_EVIDENCE_MAX_AGE:
                raise CreatorTransportValidationError(
                    f"{label}.{field} is stale at {boundary_name} time"
                )


def _validate_installed_runtime_operation(value: object) -> None:
    operation = _json_object("installed runtime operation", value)
    _require_exact_fields(
        operation,
        frozenset(
            {
                "callable_name",
                "resource_name",
                "description_sha256",
                "input_schema",
                "input_schema_sha256",
                "output_schema",
                "output_schema_sha256",
            }
        ),
        "installed runtime operation",
    )
    if operation["callable_name"] != INSTALLED_CREATOR_CALLABLE:
        raise CreatorTransportValidationError("installed creator callable mismatch")
    if operation["resource_name"] != INSTALLED_CREATOR_RESOURCE:
        raise CreatorTransportValidationError("installed creator resource mismatch")
    expected_hashes = {
        "description_sha256": INSTALLED_CREATOR_DESCRIPTION_SHA256,
        "input_schema_sha256": INSTALLED_CREATOR_INPUT_SCHEMA_SHA256,
        "output_schema_sha256": INSTALLED_CREATOR_OUTPUT_SCHEMA_SHA256,
    }
    for field, expected in expected_hashes.items():
        if not hmac.compare_digest(_sha256(operation[field], field), expected):
            raise CreatorTransportValidationError(f"installed {field} mismatch")
    for schema_field, hash_field in (
        ("input_schema", "input_schema_sha256"),
        ("output_schema", "output_schema_sha256"),
    ):
        schema = _json_object(f"installed {schema_field}", operation[schema_field])
        if not hmac.compare_digest(
            _canonical_sha256(schema), str(operation[hash_field])
        ):
            raise CreatorTransportValidationError(
                f"installed {schema_field} content mismatch"
            )


def _validate_provider_semantics(value: object) -> None:
    semantics = _json_object("installed provider semantics", value)
    expected = {
        "instruction_is_draft": True,
        "instruction_is_live_order": False,
        "user_must_review": True,
        "user_must_submit": True,
        "live_order_only_after_user_submission": True,
    }
    if not hmac.compare_digest(
        _canonical_json_bytes(semantics), _canonical_json_bytes(expected)
    ):
        raise CreatorTransportValidationError("installed provider semantics mismatch")


def _validate_delete_instruction_operation(value: object) -> None:
    operation = _json_object("installed delete-instruction operation", value)
    expected = {
        "callable_name": INSTALLED_DELETE_INSTRUCTION_CALLABLE,
        "resource_name": INSTALLED_DELETE_INSTRUCTION_RESOURCE,
        "description_sha256": INSTALLED_DELETE_INSTRUCTION_DESCRIPTION_SHA256,
        "destructive_hint": True,
    }
    if not hmac.compare_digest(
        _canonical_json_bytes(operation), _canonical_json_bytes(expected)
    ):
        raise CreatorTransportValidationError(
            "installed destructive draft-instruction delete operation mismatch"
        )


def _validate_installed_inventory(
    provider: Mapping[str, object],
) -> list[dict[str, object]]:
    raw_inventory = provider["tool_inventory"]
    if not isinstance(raw_inventory, list):
        raise CreatorTransportValidationError("installed tool_inventory must be an array")
    inventory: list[dict[str, object]] = []
    for raw_item in raw_inventory:
        item = _json_object("installed tool inventory item", raw_item)
        _require_exact_fields(
            item,
            frozenset({"callable_name", "resource_name"}),
            "installed tool inventory item",
        )
        _identifier(item["callable_name"], "inventory.callable_name")
        _identifier(item["resource_name"], "inventory.resource_name")
        inventory.append(item)
    expected_names = [item["callable_name"] for item in inventory]
    if tuple(expected_names) != _EXPECTED_INSTALLED_TOOLS:
        raise CreatorTransportValidationError("installed tool inventory names mismatch")
    count = provider["tool_count"]
    if isinstance(count, bool) or not isinstance(count, int) or count != INSTALLED_TOOL_COUNT:
        raise CreatorTransportValidationError("installed tool_count mismatch")
    inventory_hash = _sha256(
        provider["tool_inventory_sha256"], "tool_inventory_sha256"
    )
    if not hmac.compare_digest(
        inventory_hash, INSTALLED_TOOL_INVENTORY_SHA256
    ) or not hmac.compare_digest(inventory_hash, _canonical_sha256(inventory)):
        raise CreatorTransportValidationError("installed tool inventory hash mismatch")
    forbidden = provider["forbidden_order_operations_present"]
    if not isinstance(forbidden, list) or forbidden:
        raise CreatorTransportValidationError(
            "forbidden_order_operations_present must be an empty array"
        )
    return inventory


def _reject_source_private_material(value: object) -> None:
    if isinstance(value, Mapping):
        for raw_key, item in value.items():
            compact = re.sub(r"[^a-z0-9]", "", str(raw_key).casefold())
            if compact in {"linkid", "linkownerprofile", "resourceuri"}:
                raise CreatorTransportValidationError(
                    "raw provider link or owner metadata is forbidden in evidence"
                )
            _reject_source_private_material(item)
    elif isinstance(value, Sequence) and not isinstance(
        value, (str, bytes, bytearray, memoryview)
    ):
        for item in value:
            _reject_source_private_material(item)
    elif isinstance(value, str) and re.search(r"(?:^|/)link_[a-z0-9]+(?:/|$)", value):
        raise CreatorTransportValidationError(
            "raw provider link identifiers are forbidden in evidence"
        )


def _require_exact_fields(
    document: Mapping[str, object],
    expected: frozenset[str],
    label: str,
) -> None:
    missing = sorted(expected.difference(document))
    unknown = sorted(set(document).difference(expected))
    details: list[str] = []
    if missing:
        details.append("missing " + ", ".join(missing))
    if unknown:
        details.append("unsupported " + ", ".join(unknown))
    if details:
        raise CreatorTransportValidationError(
            f"{label} fields are not exact: " + "; ".join(details)
        )


def _reject_auth_material(value: object, *, path: str = "$ ") -> None:
    if isinstance(value, Mapping):
        for raw_key, item in value.items():
            key = str(raw_key)
            compact = re.sub(r"[^a-z0-9]", "", key.casefold())
            if any(marker in compact for marker in _AUTH_KEY_MARKERS):
                raise CreatorTransportValidationError(
                    "authentication material is forbidden in creator metadata"
                )
            _reject_auth_material(item, path=f"{path}.{key}")
    elif isinstance(value, Sequence) and not isinstance(
        value, (str, bytes, bytearray, memoryview)
    ):
        for item in value:
            _reject_auth_material(item, path=path)
    elif isinstance(value, str) and value != AUTH_ISOLATION:
        if _AUTH_VALUE_RE.search(value):
            raise CreatorTransportValidationError(
                "authentication material is forbidden in creator metadata"
            )


def _identifier(value: object, field: str) -> str:
    if not isinstance(value, str) or _IDENTIFIER_RE.fullmatch(value) is None:
        raise CreatorTransportValidationError(f"{field} is not a valid identifier")
    return value


def _sha256(value: object, field: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise CreatorTransportValidationError(
            f"{field} must be lowercase SHA-256 hex"
        )
    return value


def _claimed_signature_timestamp(value: object) -> datetime:
    parsed = _parse_timestamp(value, "signed_at")
    if not isinstance(value, str) or not value.endswith("Z"):
        raise CreatorTransportValidationError(
            "signed_at must be a canonical UTC timestamp ending in Z"
        )
    return parsed


def _parse_timestamp(value: object, field: str) -> datetime:
    if not isinstance(value, str) or not value:
        raise CreatorTransportValidationError(
            f"{field} must be a timezone-aware ISO timestamp"
        )
    try:
        parsed = datetime.fromisoformat(
            value[:-1] + "+00:00" if value.endswith("Z") else value
        )
    except ValueError as exc:
        raise CreatorTransportValidationError(
            f"{field} must be a timezone-aware ISO timestamp"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise CreatorTransportValidationError(
            f"{field} must be timezone-aware"
        )
    return parsed.astimezone(timezone.utc)


def _utc_timestamp(value: datetime) -> str:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise CreatorTransportValidationError(
            "probe clock must return a timezone-aware datetime"
        )
    normalized = value.astimezone(timezone.utc)
    return normalized.isoformat(timespec="seconds").replace("+00:00", "Z")


def _utc_timestamp_precise(value: datetime) -> str:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise CreatorTransportValidationError(
            "source timestamp must be a timezone-aware datetime"
        )
    normalized = value.astimezone(timezone.utc)
    return normalized.isoformat(timespec="microseconds").replace("+00:00", "Z")


def _json_object(label: str, value: object) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise CreatorTransportValidationError(f"{label} must be a JSON object")
    try:
        encoded = json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        decoded = json.loads(encoded)
    except (TypeError, ValueError) as exc:
        raise CreatorTransportValidationError(
            f"{label} must contain only finite JSON values"
        ) from exc
    if not isinstance(decoded, dict):
        raise CreatorTransportValidationError(f"{label} must be a JSON object")
    return decoded


def _canonical_sha256(value: object) -> str:
    return hashlib.sha256(_canonical_json_bytes(value)).hexdigest()


def _canonical_json_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


__all__ = [
    "AUTH_ISOLATION",
    "CAPABILITY_EVIDENCE_MAX_AGE",
    "CONTRACT_VERSION",
    "CreatorTransportValidationError",
    "INSTALLED_CAPABILITY_EVIDENCE_VERSION",
    "INSTALLED_CATALOG_SHA256",
    "INSTALLED_CONNECTOR_ID",
    "INSTALLED_CREATOR_CALLABLE",
    "INSTALLED_CREATOR_RESOURCE",
    "INSTALLED_PLUGIN_RELEASE",
    "INSTALLED_REGISTRY_SHA256",
    "TRUSTED_NO_REDIRECT",
    "build_capability_probe_evidence",
    "contract_content_sha256",
    "discover_supported_operations",
    "validate_capability_probe_evidence",
    "validate_creator_transport_contract",
    "validate_deep_link",
    "validate_installed_capability_evidence",
]
