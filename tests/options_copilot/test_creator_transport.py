from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import hashlib
import inspect
import json
import os
from pathlib import Path
import re
from uuid import UUID

import pytest

import options_copilot.bridge.creator_transport as creator_transport
from options_copilot.bridge.creator_transport import (
    AUTH_ISOLATION,
    TRUSTED_NO_REDIRECT,
    CreatorTransportValidationError,
    build_capability_probe_evidence,
    contract_content_sha256,
    discover_supported_operations,
    validate_capability_probe_evidence,
    validate_creator_transport_contract,
    validate_deep_link,
    validate_installed_capability_evidence,
)
from options_copilot.operations.creator_probe import (
    INSTALLED_CAPABILITY_FILENAME,
    main,
    run_discovery,
    run_installed_capability_capture,
    verify_contract,
)


JSON_SCHEMA_DRAFT = "https://json-schema.org/draft/2020-12/schema"


def _input_schema() -> dict[str, object]:
    return {
        "$schema": JSON_SCHEMA_DRAFT,
        "type": "object",
        "properties": {
            "idempotency_key": {
                "type": "string",
                "minLength": 1,
                "maxLength": 160,
            },
            "proposal": {
                "type": "object",
                "properties": {
                    "proposal_hash": {
                        "type": "string",
                        "pattern": "^[0-9a-f]{64}$",
                    }
                },
                "required": ["proposal_hash"],
                "additionalProperties": False,
            },
            "instruction_intent": {
                "type": "object",
                "properties": {
                    "order_type": {"const": "LMT"},
                    "time_in_force": {"const": "DAY"},
                },
                "required": ["order_type", "time_in_force"],
                "additionalProperties": False,
            },
            "review_only": {"const": True},
        },
        "required": [
            "idempotency_key",
            "proposal",
            "instruction_intent",
            "review_only",
        ],
        "additionalProperties": False,
    }


def _output_schema() -> dict[str, object]:
    return {
        "$schema": JSON_SCHEMA_DRAFT,
        "type": "object",
        "properties": {
            "review_only": {"const": True},
            "order_submitted": {"const": False},
            "transmitted_to_broker": {"const": False},
            "instruction_id": {"type": "string", "minLength": 1},
            "deep_link": {"type": "string", "format": "uri"},
        },
        "required": [
            "review_only",
            "order_submitted",
            "transmitted_to_broker",
            "instruction_id",
            "deep_link",
        ],
        "additionalProperties": False,
    }


def _operation() -> dict[str, object]:
    return {
        "metadata_version": 1,
        "connector_id": "managed-ibkr",
        "tool_name": "create_review_instruction",
        "transport": "managed_connector",
        "operation_class": "review_instruction_creator",
        "review_only": True,
        "order_capable": False,
        "browser_automation": False,
        "auth_isolation": AUTH_ISOLATION,
        "input_schema": _input_schema(),
        "output_schema": _output_schema(),
        "deep_link_rules": {
            "scheme": "https",
            "allowed_hosts": ["chatgpt.com"],
            "allowed_path_prefixes": ["/connector/ibkr/"],
            "allow_query": False,
            "allow_fragment": False,
            "allow_userinfo": False,
        },
        "reconciliation": {
            "instruction_id_field": "instruction_id",
            "deep_link_field": "deep_link",
            "review_only_field": "review_only",
            "order_submitted_field": "order_submitted",
            "transmitted_to_broker_field": "transmitted_to_broker",
            "uncertain_outcome": "manual_reconciliation_required",
        },
        "idempotency": {
            "strategy": "approval_id",
            "request_field": "idempotency_key",
            "retry_policy": "never_after_attempt",
            "uncertain_outcome": "manual_reconciliation_required",
        },
    }


def _capability_evidence(
    *, observed_at: datetime | None = None
) -> dict[str, object]:
    return build_capability_probe_evidence(
        [_operation()],
        observed_at=observed_at
        or datetime(2026, 8, 3, 6, 0, tzinfo=timezone.utc),
    )


def _contract(
    *, capability_evidence: dict[str, object] | None = None
) -> dict[str, object]:
    evidence = capability_evidence or _capability_evidence()
    operation = _operation()
    operation.pop("metadata_version")
    contract = {
        "contract_version": 1,
        **operation,
        "capability_evidence_sha256": evidence["evidence_sha256"],
        "signed_by": "human:xujie",
        "signed_at": "2026-08-03T06:00:00Z",
    }
    contract["content_sha256"] = contract_content_sha256(contract)
    return contract


def _validate_contract(
    contract: dict[str, object],
    *,
    capability_evidence: dict[str, object] | None = None,
    checked_at: datetime | None = None,
) -> dict[str, object]:
    return validate_creator_transport_contract(
        contract,
        capability_evidence=capability_evidence or _capability_evidence(),
        checked_at=checked_at
        or datetime(2026, 8, 3, 6, 1, tzinfo=timezone.utc),
    )


def _assert_no_exposed_auth_material(value: object) -> None:
    forbidden_keys = (
        "password",
        "credential",
        "apikey",
        "accesskey",
        "bearertoken",
        "accesstoken",
        "refreshtoken",
        "clientsecret",
        "sessiontoken",
        "cookie",
    )
    if isinstance(value, dict):
        for key, item in value.items():
            compact = re.sub(r"[^a-z0-9]", "", str(key).casefold())
            assert not any(marker in compact for marker in forbidden_keys)
            _assert_no_exposed_auth_material(item)
    elif isinstance(value, list):
        for item in value:
            _assert_no_exposed_auth_material(item)
    elif isinstance(value, str) and value != AUTH_ISOLATION:
        assert re.search(r"(?i)(bearer\s+\S+|(?:token|password|secret|api[_-]?key)=\S+)", value) is None


def test_structural_contract_review_cannot_claim_human_signature_or_activation() -> None:
    validated = _validate_contract(_contract())

    assert validated["status"] == "CREATOR_TRANSPORT_STRUCTURE_VERIFIED_UNSIGNED"
    assert validated["reason"] == "WAITING_GENUINE_HUMAN_SIGNATURE"
    assert validated["creator_transport_status"] == "CREATOR_TRANSPORT_UNAVAILABLE"
    assert validated["contract_activatable"] is False
    assert validated["human_signature_verified"] is False
    assert validated["claimed_signed_by"] == "human:xujie"
    assert validated["connector_id"] == "managed-ibkr"
    assert validated["tool_name"] == "create_review_instruction"
    assert validated["review_only"] is True
    assert validated["order_capable"] is False
    assert validated["browser_automation"] is False


@pytest.mark.parametrize(
    "required_field",
    [
        "contract_version",
        "connector_id",
        "tool_name",
        "input_schema",
        "output_schema",
        "auth_isolation",
        "deep_link_rules",
        "reconciliation",
        "idempotency",
        "capability_evidence_sha256",
        "signed_by",
        "signed_at",
        "content_sha256",
    ],
)
def test_contract_requires_every_governance_field(required_field: str) -> None:
    contract = _contract()
    contract.pop(required_field)

    with pytest.raises(CreatorTransportValidationError, match=required_field):
        _validate_contract(contract)


@pytest.mark.parametrize("version", [True, 1.0])
def test_contract_rejects_non_integer_contract_version(version: object) -> None:
    contract = _contract()
    contract["contract_version"] = version
    contract["content_sha256"] = contract_content_sha256(contract)

    with pytest.raises(CreatorTransportValidationError, match="contract_version"):
        _validate_contract(contract)


@pytest.mark.parametrize("version", [True, 1.0])
def test_discovery_rejects_non_integer_metadata_version(version: object) -> None:
    operation = _operation()
    operation["metadata_version"] = version

    assert discover_supported_operations([operation]) == []


@pytest.mark.parametrize("version", [True, 1.0])
def test_evidence_rejects_non_integer_evidence_version(version: object) -> None:
    evidence = _capability_evidence()
    evidence["evidence_version"] = version

    with pytest.raises(CreatorTransportValidationError, match="evidence_version"):
        validate_capability_probe_evidence(
            evidence,
            checked_at=datetime(2026, 8, 3, 6, 1, tzinfo=timezone.utc),
        )


@pytest.mark.parametrize(
    "mutation",
    [
        lambda schema: schema.__setitem__("additionalProperties", True),
        lambda schema: schema["properties"].__setitem__("*", {}),
        lambda schema: schema.__setitem__("patternProperties", {".*": {}}),
        lambda schema: schema.__setitem__("required", ["review_only"]),
    ],
)
def test_contract_rejects_wildcard_or_inexact_input_schema(mutation) -> None:
    contract = _contract()
    schema = contract["input_schema"]
    assert isinstance(schema, dict)
    mutation(schema)
    contract["content_sha256"] = contract_content_sha256(contract)

    with pytest.raises(CreatorTransportValidationError, match="schema"):
        _validate_contract(contract)


@pytest.mark.parametrize(
    ("keyword", "value"),
    [
        ("minLength", True),
        ("maxLength", 1.0),
        ("minimum", False),
        ("maximum", "10"),
        ("uniqueItems", 1),
        ("format", True),
        ("pattern", 7),
        ("enum", "BUY"),
    ],
)
def test_contract_rejects_invalid_json_schema_keyword_shapes(
    keyword: str,
    value: object,
) -> None:
    evidence_operation = _operation()
    contract_operation = deepcopy(evidence_operation)
    evidence_schema = evidence_operation["input_schema"]
    contract_schema = contract_operation["input_schema"]
    assert isinstance(evidence_schema, dict)
    assert isinstance(contract_schema, dict)
    evidence_property = evidence_schema["properties"]["idempotency_key"]
    contract_property = contract_schema["properties"]["idempotency_key"]
    assert isinstance(evidence_property, dict)
    assert isinstance(contract_property, dict)
    evidence_property[keyword] = value
    contract_property[keyword] = value
    evidence = build_capability_probe_evidence(
        [evidence_operation],
        observed_at=datetime(2026, 8, 3, 6, 0, tzinfo=timezone.utc),
    )
    contract_operation.pop("metadata_version")
    contract = {
        "contract_version": 1,
        **contract_operation,
        "capability_evidence_sha256": evidence["evidence_sha256"],
        "signed_by": "human:xujie",
        "signed_at": "2026-08-03T06:00:00Z",
    }
    contract["content_sha256"] = contract_content_sha256(contract)

    with pytest.raises(CreatorTransportValidationError, match="schema"):
        _validate_contract(contract, capability_evidence=evidence)


@pytest.mark.parametrize(
    ("field", "value"),
    [("order_capable", True), ("browser_automation", True), ("review_only", False)],
)
def test_discovery_rejects_order_browser_and_non_review_operations(
    field: str, value: object
) -> None:
    operation = _operation()
    operation[field] = value

    assert discover_supported_operations([operation]) == []


def test_contract_rejects_application_visible_auth_material() -> None:
    contract = _contract()
    schema = contract["input_schema"]
    assert isinstance(schema, dict)
    properties = schema["properties"]
    assert isinstance(properties, dict)
    properties["api_key"] = {"type": "string"}
    required = schema["required"]
    assert isinstance(required, list)
    required.append("api_key")
    contract["content_sha256"] = contract_content_sha256(contract)

    with pytest.raises(CreatorTransportValidationError, match="authentication material"):
        _validate_contract(contract)


@pytest.mark.parametrize(
    "url",
    [
        "https://evil.example/connector/ibkr/review/123",
        "https://chatgpt.com/unknown/123",
        "https://user:pass@chatgpt.com/connector/ibkr/review/123",
        "https://chatgpt.com/connector/ibkr/review/123?token=abc",
        "https://chatgpt.com/connector/ibkr/review/123#credential=abc",
        "http://chatgpt.com/connector/ibkr/review/123",
    ],
)
def test_deep_link_rejects_unknown_rules_and_url_credentials(url: str) -> None:
    with pytest.raises(CreatorTransportValidationError, match="deep link"):
        validate_deep_link(
            url,
            _contract()["deep_link_rules"],
            redirect_provenance=TRUSTED_NO_REDIRECT,
        )


def test_deep_link_accepts_only_exact_contracted_https_host_and_path() -> None:
    url = "https://chatgpt.com/connector/ibkr/review/abc-123"

    assert (
        validate_deep_link(
            url,
            _contract()["deep_link_rules"],
            redirect_provenance=TRUSTED_NO_REDIRECT,
        )
        == url
    )


@pytest.mark.parametrize(
    "url",
    [
        "https://chatgpt.com/connector/ibkr/../admin",
        "https://chatgpt.com/connector/ibkr/%2e%2e/admin",
        "https://chatgpt.com/connector\\ibkr/review/123",
        "https://CHATGPT.com/connector/ibkr/review/123",
        "https://chatgpt.com./connector/ibkr/review/123",
        "https://chatgpt.com:443/connector/ibkr/review/123",
        "https://chatgpt.com/connector/ibkr//review/123",
    ],
)
def test_deep_link_rejects_dot_segments_backslashes_and_noncanonical_urls(
    url: str,
) -> None:
    with pytest.raises(CreatorTransportValidationError, match="deep link"):
        validate_deep_link(
            url,
            _contract()["deep_link_rules"],
            redirect_provenance=TRUSTED_NO_REDIRECT,
        )


@pytest.mark.parametrize("provenance", [None, "UNKNOWN", "REDIRECTED", ""])
def test_deep_link_requires_explicit_trusted_no_redirect_provenance(
    provenance: object,
) -> None:
    with pytest.raises(CreatorTransportValidationError, match="provenance"):
        validate_deep_link(
            "https://chatgpt.com/connector/ibkr/review/abc-123",
            _contract()["deep_link_rules"],
            redirect_provenance=provenance,
        )


def test_probe_evidence_is_local_non_order_and_redacted() -> None:
    evidence = build_capability_probe_evidence(
        [_operation()], observed_at=datetime(2026, 8, 3, 6, 0, tzinfo=timezone.utc)
    )

    assert evidence["status"] == "SUPPORTED_OPERATIONS_FOUND"
    assert evidence["instruction_created"] is False
    assert evidence["connector_invoked"] is False
    assert evidence["order_operation_invoked"] is False
    assert evidence["browser_automation_used"] is False
    assert re.fullmatch(r"[0-9a-f]{64}", str(evidence["evidence_sha256"]))
    _assert_no_exposed_auth_material(evidence)


def test_contract_rejects_absent_capability_evidence() -> None:
    with pytest.raises(CreatorTransportValidationError, match="capability evidence"):
        validate_creator_transport_contract(_contract())


def test_contract_rejects_mismatched_operation_capability_evidence() -> None:
    evidence = _capability_evidence()
    contract = _contract(capability_evidence=evidence)
    contract["tool_name"] = "different_review_creator"
    contract["content_sha256"] = contract_content_sha256(contract)

    with pytest.raises(CreatorTransportValidationError, match="exactly one"):
        _validate_contract(contract, capability_evidence=evidence)


@pytest.mark.parametrize("contract_const", [1.0, True])
def test_contract_binding_distinguishes_integer_float_and_boolean_json(
    contract_const: object,
) -> None:
    evidence_operation = _operation()
    evidence_schema = evidence_operation["input_schema"]
    assert isinstance(evidence_schema, dict)
    evidence_intent = evidence_schema["properties"]["instruction_intent"]
    assert isinstance(evidence_intent, dict)
    evidence_order_type = evidence_intent["properties"]["order_type"]
    assert isinstance(evidence_order_type, dict)
    evidence_order_type["const"] = 1
    evidence = build_capability_probe_evidence(
        [evidence_operation],
        observed_at=datetime(2026, 8, 3, 6, 0, tzinfo=timezone.utc),
    )

    contract_operation = deepcopy(evidence_operation)
    contract_schema = contract_operation["input_schema"]
    assert isinstance(contract_schema, dict)
    contract_intent = contract_schema["properties"]["instruction_intent"]
    assert isinstance(contract_intent, dict)
    contract_order_type = contract_intent["properties"]["order_type"]
    assert isinstance(contract_order_type, dict)
    contract_order_type["const"] = contract_const
    contract_operation.pop("metadata_version")
    contract = {
        "contract_version": 1,
        **contract_operation,
        "capability_evidence_sha256": evidence["evidence_sha256"],
        "signed_by": "human:xujie",
        "signed_at": "2026-08-03T06:00:00Z",
    }
    contract["content_sha256"] = contract_content_sha256(contract)

    with pytest.raises(CreatorTransportValidationError, match="exactly one"):
        _validate_contract(contract, capability_evidence=evidence)


def test_capability_evidence_rejects_malformed_and_hash_inconsistent_documents() -> None:
    malformed = _capability_evidence()
    malformed.pop("probe_kind")
    with pytest.raises(CreatorTransportValidationError, match="probe_kind"):
        validate_capability_probe_evidence(
            malformed,
            checked_at=datetime(2026, 8, 3, 6, 1, tzinfo=timezone.utc),
        )

    tampered = _capability_evidence()
    tampered["rejected_operation_count"] = 1
    with pytest.raises(CreatorTransportValidationError, match="evidence_sha256"):
        validate_capability_probe_evidence(
            tampered,
            checked_at=datetime(2026, 8, 3, 6, 1, tzinfo=timezone.utc),
        )


def test_capability_evidence_rejects_stale_or_future_observation() -> None:
    checked_at = datetime(2026, 8, 4, 6, 0, 1, tzinfo=timezone.utc)
    stale = _capability_evidence(observed_at=checked_at - timedelta(hours=24, seconds=1))
    future = _capability_evidence(observed_at=checked_at + timedelta(seconds=1))

    for evidence in (stale, future):
        with pytest.raises(CreatorTransportValidationError, match="fresh"):
            validate_capability_probe_evidence(evidence, checked_at=checked_at)


def test_discovery_writes_probe_evidence_but_never_a_contract(tmp_path) -> None:
    result = run_discovery(
        evidence_dir=tmp_path,
        operations=[_operation()],
        clock=lambda: datetime(2026, 8, 3, 6, 0, tzinfo=timezone.utc),
    )

    evidence_paths = list(tmp_path.glob("*/creator_transport_discovery.v1.json"))
    assert result["status"] == "SUPPORTED_OPERATIONS_FOUND"
    assert len(evidence_paths) == 1
    evidence_path = evidence_paths[0]
    assert evidence_path.is_file()
    assert not list(tmp_path.rglob("creator_transport_contract.v1.json"))
    persisted = json.loads(evidence_path.read_text(encoding="utf-8"))
    assert persisted == result
    _assert_no_exposed_auth_material(persisted)


def test_discovery_uses_unique_exclusive_attempt_paths_without_overwrite(
    tmp_path,
) -> None:
    clock = lambda: datetime(2026, 8, 3, 6, 0, tzinfo=timezone.utc)
    first = run_discovery(
        evidence_dir=tmp_path,
        operations=[_operation()],
        clock=clock,
    )
    first_path = next(tmp_path.glob("*/creator_transport_discovery.v1.json"))
    first_bytes = first_path.read_bytes()

    second = run_discovery(
        evidence_dir=tmp_path,
        operations=[_operation()],
        clock=clock,
    )

    evidence_paths = sorted(tmp_path.glob("*/creator_transport_discovery.v1.json"))
    assert len(evidence_paths) == 2
    assert evidence_paths[0].parent != evidence_paths[1].parent
    assert first_path.read_bytes() == first_bytes
    assert json.loads(first_bytes) == first
    assert json.loads(evidence_paths[1].read_text(encoding="utf-8")) == second


def test_verify_contract_requires_fresh_exact_capability_file(tmp_path) -> None:
    evidence = _capability_evidence()
    contract = _contract(capability_evidence=evidence)
    contract_path = tmp_path / "contract.json"
    evidence_path = tmp_path / "evidence.json"
    contract_path.write_text(json.dumps(contract), encoding="utf-8")
    evidence_path.write_text(json.dumps(evidence), encoding="utf-8")

    result = verify_contract(
        contract_path,
        capability_evidence_path=evidence_path,
        clock=lambda: datetime(2026, 8, 3, 6, 1, tzinfo=timezone.utc),
    )

    assert result["status"] == "CREATOR_TRANSPORT_STRUCTURE_VERIFIED_UNSIGNED"
    assert result["reason"] == "WAITING_GENUINE_HUMAN_SIGNATURE"
    assert result["creator_transport_status"] == "CREATOR_TRANSPORT_UNAVAILABLE"
    assert result["contract_activatable"] is False
    assert result["human_signature_verified"] is False
    assert result["review_only"] is True
    assert result["order_capable"] is False
    assert result["connector_invoked"] is False


def test_verify_contract_cli_returns_blocked_unsigned_result(
    tmp_path,
    capsys,
) -> None:
    evidence = _capability_evidence(
        observed_at=datetime.now(timezone.utc) - timedelta(seconds=1)
    )
    contract = _contract(capability_evidence=evidence)
    contract_path = tmp_path / "contract.json"
    evidence_path = tmp_path / "evidence.json"
    contract_path.write_text(json.dumps(contract), encoding="utf-8")
    evidence_path.write_text(json.dumps(evidence), encoding="utf-8")

    exit_code = main(
        [
            "--verify-contract",
            str(contract_path),
            "--capability-evidence",
            str(evidence_path),
            "--json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 2
    assert payload["status"] == "CREATOR_TRANSPORT_STRUCTURE_VERIFIED_UNSIGNED"
    assert payload["reason"] == "WAITING_GENUINE_HUMAN_SIGNATURE"
    assert payload["contract_activatable"] is False


def test_verify_contract_rejects_duplicate_json_members(tmp_path) -> None:
    evidence = _capability_evidence()
    contract_path = tmp_path / "contract.json"
    evidence_path = tmp_path / "evidence.json"
    contract_path.write_text(
        '{"contract_version":1,"contract_version":1}',
        encoding="utf-8",
    )
    evidence_path.write_text(json.dumps(evidence), encoding="utf-8")

    with pytest.raises(CreatorTransportValidationError, match="valid JSON"):
        verify_contract(
            contract_path,
            capability_evidence_path=evidence_path,
            clock=lambda: datetime(2026, 8, 3, 6, 1, tzinfo=timezone.utc),
        )


def test_discovery_fails_closed_when_inventory_has_no_supported_operation(
    tmp_path,
) -> None:
    order_operation = _operation()
    order_operation["order_capable"] = True

    result = run_discovery(
        evidence_dir=tmp_path,
        operations=[order_operation],
        clock=lambda: datetime(2026, 8, 3, 6, 0, tzinfo=timezone.utc),
    )

    assert result["status"] == "CREATOR_TRANSPORT_UNAVAILABLE"
    assert result["supported_operations"] == []
    assert result["rejected_operation_count"] == 1
    assert result["instruction_created"] is False
    _assert_no_exposed_auth_material(result)


def test_contract_hash_covers_signer_time_and_capability_evidence() -> None:
    contract = _contract()
    original_hash = contract["content_sha256"]

    for field, changed in (
        ("signed_by", "human:someone-else"),
        ("signed_at", "2026-08-03T06:01:00Z"),
        ("capability_evidence_sha256", "b" * 64),
    ):
        tampered = deepcopy(contract)
        tampered[field] = changed
        assert contract_content_sha256(tampered) != original_hash
        with pytest.raises(CreatorTransportValidationError, match="content_sha256"):
            _validate_contract(tampered)


def _canonical_hash(value: object) -> str:
    encoded = json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _installed_source_fixture(monkeypatch, tmp_path) -> tuple[bytes, bytes]:
    connector_id = creator_transport.INSTALLED_CONNECTOR_ID
    input_schema = {
        "$schema": JSON_SCHEMA_DRAFT,
        "type": "object",
        "properties": {
            "side": {"type": "string", "enum": ["BUY", "SELL"]},
            "quantity": {"type": "number"},
        },
        "required": ["side"],
    }
    output_schema = {
        "$schema": JSON_SCHEMA_DRAFT,
        "type": "object",
        "properties": {"id": {"type": "string"}, "url": {"type": "string"}},
    }
    description = (
        "Creates a new instruction. An instruction is not a live order. "
        "A user reviews and submits it before it becomes a live order."
    )
    catalog = {
        "schema_version": 1,
        "plugins": [
            {
                "release": {
                    "version": creator_transport.INSTALLED_PLUGIN_RELEASE,
                    "app_ids": [connector_id],
                }
            }
        ],
    }
    tools = []
    for callable_name in creator_transport._EXPECTED_INSTALLED_TOOLS:
        resource_name = callable_name.replace(
            "interactive_brokers_ibkr_", "interactive_brokers_ibkr.", 1
        )
        tool: dict[str, object] = {
            "name": resource_name,
            "_meta": {
                "resource_name": resource_name,
                "connector_id": connector_id,
                "link_id": "link_redacted_at_source_boundary",
            },
        }
        if callable_name == creator_transport.INSTALLED_CREATOR_CALLABLE:
            tool.update(
                {
                    "description": description,
                    "inputSchema": input_schema,
                    "outputSchema": output_schema,
                }
            )
        if callable_name == creator_transport.INSTALLED_DELETE_INSTRUCTION_CALLABLE:
            tool.update(
                {
                    "description": "Deletes an existing instruction by its ID.",
                    "annotations": {"destructiveHint": True},
                }
            )
        tools.append(
            {
                "connector_id": connector_id,
                "tool_name": callable_name,
                "tool": tool,
            }
        )
    registry = {"schema_version": 4, "tools": tools}
    catalog_bytes = json.dumps(catalog, sort_keys=True).encode("utf-8")
    registry_bytes = json.dumps(registry, sort_keys=True).encode("utf-8")
    inventory = [
        {
            "callable_name": item["tool_name"],
            "resource_name": item["tool"]["name"],
        }
        for item in tools
    ]
    inventory.sort(key=lambda item: item["callable_name"])
    monkeypatch.setattr(
        creator_transport,
        "INSTALLED_CATALOG_SHA256",
        hashlib.sha256(catalog_bytes).hexdigest(),
    )
    monkeypatch.setattr(
        creator_transport,
        "INSTALLED_REGISTRY_SHA256",
        hashlib.sha256(registry_bytes).hexdigest(),
    )
    monkeypatch.setattr(
        creator_transport,
        "INSTALLED_CREATOR_INPUT_SCHEMA_SHA256",
        _canonical_hash(input_schema),
    )
    monkeypatch.setattr(
        creator_transport,
        "INSTALLED_CREATOR_OUTPUT_SCHEMA_SHA256",
        _canonical_hash(output_schema),
    )
    monkeypatch.setattr(
        creator_transport,
        "INSTALLED_CREATOR_DESCRIPTION_SHA256",
        _canonical_hash(description),
    )
    monkeypatch.setattr(
        creator_transport,
        "INSTALLED_TOOL_INVENTORY_SHA256",
        _canonical_hash(inventory),
    )
    catalog_path = tmp_path / "installed-catalog.json"
    registry_path = tmp_path / "installed-registry.json"
    catalog_path.write_bytes(catalog_bytes)
    registry_path.write_bytes(registry_bytes)
    observed_at = datetime(2026, 8, 9, 12, 45, tzinfo=timezone.utc)
    source_timestamp = (observed_at - timedelta(minutes=1)).timestamp()
    os.utime(catalog_path, (source_timestamp, source_timestamp))
    os.utime(registry_path, (source_timestamp, source_timestamp))
    monkeypatch.setattr(
        creator_transport,
        "INSTALLED_CATALOG_CANONICAL_PATH",
        catalog_path.resolve().as_posix(),
    )
    monkeypatch.setattr(
        creator_transport,
        "INSTALLED_REGISTRY_CANONICAL_PATH",
        registry_path.resolve().as_posix(),
    )
    monkeypatch.setattr(creator_transport, "_utc_now", lambda: observed_at)
    return catalog_bytes, registry_bytes


def _build_installed_fixture_evidence(
) -> dict[str, object]:
    return creator_transport._build_installed_capability_evidence()


def _rehash_installed_evidence(evidence: dict[str, object]) -> None:
    evidence.pop("combined_sha256", None)
    evidence["combined_sha256"] = _canonical_hash(evidence)


def _rehash_installed_inventory(evidence: dict[str, object]) -> None:
    provider = evidence["provider_capability"]
    assert isinstance(provider, dict)
    inventory = provider["tool_inventory"]
    assert isinstance(inventory, list)
    provider["tool_count"] = len(inventory)
    provider["tool_inventory_sha256"] = _canonical_hash(inventory)
    _rehash_installed_evidence(evidence)


def test_installed_capability_has_no_public_builder_and_requires_reconciliation(
    monkeypatch,
    tmp_path,
) -> None:
    catalog_bytes, registry_bytes = _installed_source_fixture(monkeypatch, tmp_path)
    observed_at = datetime(2026, 8, 9, 12, 45, tzinfo=timezone.utc)
    evidence = _build_installed_fixture_evidence()

    assert not hasattr(creator_transport, "build_installed_capability_evidence")
    assert not hasattr(creator_transport, "_trusted_installed_source_acquisition")
    assert "trusted_acquisition" not in inspect.signature(
        validate_installed_capability_evidence
    ).parameters
    assert "checked_at" not in inspect.signature(
        validate_installed_capability_evidence
    ).parameters
    assert not inspect.signature(
        creator_transport._build_installed_capability_evidence
    ).parameters
    with pytest.raises(TypeError):
        creator_transport._build_installed_capability_evidence(
            catalog_bytes,
            registry_bytes,
        )
    with pytest.raises(TypeError):
        validate_installed_capability_evidence(
            evidence,
            checked_at=observed_at,
        )

    tampered = deepcopy(evidence)
    sources = tampered["sources"]
    assert isinstance(sources, dict)
    catalog = sources["authoritative_catalog"]
    assert isinstance(catalog, dict)
    catalog["sha256"] = hashlib.sha256(b"caller-fabricated-bytes").hexdigest()
    catalog["last_write_at"] = "2026-08-09T12:44:59.000000Z"
    _rehash_installed_evidence(tampered)
    with pytest.raises(
        CreatorTransportValidationError,
        match="source SHA-256 mismatch|current canonical source",
    ):
        validate_installed_capability_evidence(
            tampered,
        )


def test_installed_capability_evidence_binds_exact_sources_and_stays_unavailable(
    monkeypatch,
    tmp_path,
) -> None:
    _installed_source_fixture(monkeypatch, tmp_path)
    observed_at = datetime(2026, 8, 9, 12, 45, tzinfo=timezone.utc)

    evidence = _build_installed_fixture_evidence()
    checked_at = observed_at + timedelta(minutes=1)
    monkeypatch.setattr(creator_transport, "_utc_now", lambda: checked_at)
    validated = validate_installed_capability_evidence(evidence)

    sources = validated["sources"]
    assert isinstance(sources, dict)
    assert sources["authoritative_catalog"] == {
        "source_identity": "authoritative_plugin_catalog",
        "canonical_path": creator_transport.INSTALLED_CATALOG_CANONICAL_PATH,
        "schema_version": 1,
        "sha256": creator_transport.INSTALLED_CATALOG_SHA256,
        "last_write_at": "2026-08-09T12:44:00.000000Z",
        "captured_at": "2026-08-09T12:45:00.000000Z",
    }
    assert sources["installed_runtime_registry"] == {
        "source_identity": "installed_runtime_tool_registry",
        "canonical_path": creator_transport.INSTALLED_REGISTRY_CANONICAL_PATH,
        "schema_version": 4,
        "sha256": creator_transport.INSTALLED_REGISTRY_SHA256,
        "last_write_at": "2026-08-09T12:44:00.000000Z",
        "captured_at": "2026-08-09T12:45:00.000000Z",
    }
    provider = validated["provider_capability"]
    assert isinstance(provider, dict)
    operation = provider["runtime_operation"]
    assert isinstance(operation, dict)
    assert provider["connector_id"] == creator_transport.INSTALLED_CONNECTOR_ID
    assert provider["plugin_release"] == "4.0.0"
    assert provider["tool_count"] == 27
    assert provider["inventory_scope"] == "exact_observed_registry_snapshot"
    assert provider["forbidden_order_operations_present"] == []
    assert provider["draft_instruction_delete_operation"] == {
        "callable_name": creator_transport.INSTALLED_DELETE_INSTRUCTION_CALLABLE,
        "resource_name": creator_transport.INSTALLED_DELETE_INSTRUCTION_RESOURCE,
        "description_sha256": creator_transport.INSTALLED_DELETE_INSTRUCTION_DESCRIPTION_SHA256,
        "destructive_hint": True,
    }
    assert operation["callable_name"] == creator_transport.INSTALLED_CREATOR_CALLABLE
    assert operation["resource_name"] == creator_transport.INSTALLED_CREATOR_RESOURCE
    assert provider["provider_semantics"] == {
        "instruction_is_draft": True,
        "instruction_is_live_order": False,
        "user_must_review": True,
        "user_must_submit": True,
        "live_order_only_after_user_submission": True,
    }
    assert validated["local_safety_policy"] == {
        "creator_transport_status": "CREATOR_TRANSPORT_UNAVAILABLE",
        "crt_01": "PASS",
        "crt_02": "PASS",
        "crt_03": "BLOCKED_EXTERNAL",
        "deep_link_destination_contract_present": False,
        "evidence_scope": "metadata_only_non_invoking",
        "future_unsigned_transport_dossier_present": False,
        "order_authority": False,
        "delete_instruction_selected": False,
        "delete_instruction_invoked": False,
        "delete_instruction_scope": "draft_instruction_only",
        "live_order_cancel_authority": False,
    }
    assert validated["activity"] == {
        "connector_calls": 0,
        "instruction_calls": 0,
        "network_calls": 0,
        "order_calls": 0,
    }
    assert "link_id" not in json.dumps(validated, sort_keys=True)
    assert _canonical_hash(
        {key: value for key, value in validated.items() if key != "combined_sha256"}
    ) == validated["combined_sha256"]


def test_installed_capability_accepts_newer_same_byte_canonical_cache_rewrite(
    monkeypatch,
    tmp_path,
) -> None:
    catalog_bytes, registry_bytes = _installed_source_fixture(monkeypatch, tmp_path)
    observed_at = datetime(2026, 8, 9, 12, 45, tzinfo=timezone.utc)
    evidence = _build_installed_fixture_evidence()
    rewrite_at = observed_at + timedelta(seconds=30)
    for raw_path, source_bytes in (
        (creator_transport.INSTALLED_CATALOG_CANONICAL_PATH, catalog_bytes),
        (creator_transport.INSTALLED_REGISTRY_CANONICAL_PATH, registry_bytes),
    ):
        path = Path(raw_path)
        path.write_bytes(source_bytes)
        timestamp = rewrite_at.timestamp()
        os.utime(path, (timestamp, timestamp))
    checked_at = observed_at + timedelta(minutes=1)
    monkeypatch.setattr(creator_transport, "_utc_now", lambda: checked_at)

    validated = validate_installed_capability_evidence(evidence)

    assert validated == evidence


def test_installed_capability_rejects_rehashed_seven_day_old_evidence_today(
    monkeypatch,
    tmp_path,
) -> None:
    _installed_source_fixture(monkeypatch, tmp_path)
    today = datetime(2026, 8, 9, 12, 45, tzinfo=timezone.utc)
    evidence = _build_installed_fixture_evidence()
    stale_observation = today - timedelta(days=7)
    evidence["observed_at"] = creator_transport._utc_timestamp_precise(
        stale_observation
    )
    sources = evidence["sources"]
    assert isinstance(sources, dict)
    for source in sources.values():
        assert isinstance(source, dict)
        source["last_write_at"] = creator_transport._utc_timestamp_precise(
            stale_observation - timedelta(minutes=1)
        )
        source["captured_at"] = creator_transport._utc_timestamp_precise(
            stale_observation
        )
    _rehash_installed_evidence(evidence)
    monkeypatch.setattr(creator_transport, "_utc_now", lambda: today)

    with pytest.raises(CreatorTransportValidationError, match="stale"):
        validate_installed_capability_evidence(evidence)


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (
            lambda evidence: evidence["provider_capability"].__setitem__(
                "connector_id", "asdk_app_wrong"
            ),
            "connector_id",
        ),
        (
            lambda evidence: evidence["provider_capability"].__setitem__(
                "plugin_release", "4.0.1"
            ),
            "plugin release",
        ),
        (
            lambda evidence: evidence["provider_capability"][
                "runtime_operation"
            ]["input_schema"].__setitem__("type", True),
            "input_schema content",
        ),
        (
            lambda evidence: evidence["provider_capability"].__setitem__(
                "tool_count", True
            ),
            "tool_count",
        ),
        (
            lambda evidence: evidence["activity"].__setitem__(
                "connector_calls", True
            ),
            "activity",
        ),
        (
            lambda evidence: evidence["local_safety_policy"].__setitem__(
                "delete_instruction_invoked", True
            ),
            "local safety policy",
        ),
        (
            lambda evidence: evidence["provider_capability"].__setitem__(
                "link_id", "link_must_not_escape"
            ),
            "link",
        ),
    ],
)
def test_installed_capability_rejects_mismatch_scalar_confusion_and_raw_link(
    monkeypatch,
    tmp_path,
    mutation,
    message: str,
) -> None:
    _installed_source_fixture(monkeypatch, tmp_path)
    observed_at = datetime(2026, 8, 9, 12, 45, tzinfo=timezone.utc)
    evidence = _build_installed_fixture_evidence()
    mutation(evidence)
    _rehash_installed_evidence(evidence)

    with pytest.raises(CreatorTransportValidationError, match=message):
        validate_installed_capability_evidence(
            evidence,
        )


@pytest.mark.parametrize(
    "mutation",
    [
        lambda inventory: inventory.pop(0),
        lambda inventory: inventory.append(
            {
                "callable_name": "interactive_brokers_ibkr_z_extra_metadata_tool",
                "resource_name": "interactive_brokers_ibkr.z_extra_metadata_tool",
            }
        ),
        lambda inventory: inventory.insert(1, deepcopy(inventory[0])),
        lambda inventory: inventory.__setitem__(
            slice(0, 2), [inventory[1], inventory[0]]
        ),
        lambda inventory: inventory[0].__setitem__(
            "resource_name", "interactive_brokers_ibkr.renamed_resource"
        ),
    ],
)
def test_installed_capability_rejects_inventory_drop_extra_duplicate_reorder_or_rename(
    monkeypatch,
    tmp_path,
    mutation,
) -> None:
    _installed_source_fixture(monkeypatch, tmp_path)
    observed_at = datetime(2026, 8, 9, 12, 45, tzinfo=timezone.utc)
    evidence = _build_installed_fixture_evidence()
    provider = evidence["provider_capability"]
    assert isinstance(provider, dict)
    inventory = provider["tool_inventory"]
    assert isinstance(inventory, list)
    mutation(inventory)
    _rehash_installed_inventory(evidence)

    with pytest.raises(CreatorTransportValidationError, match="inventory|tool_count"):
        validate_installed_capability_evidence(
            evidence,
        )


def test_installed_capability_rejects_source_hash_stale_future_and_combined_hash(
    monkeypatch,
    tmp_path,
) -> None:
    _installed_source_fixture(monkeypatch, tmp_path)
    observed_at = datetime(2026, 8, 9, 12, 45, tzinfo=timezone.utc)
    evidence = _build_installed_fixture_evidence()
    for checked_at in (
        observed_at - timedelta(seconds=1),
        observed_at + timedelta(hours=24, seconds=1),
    ):
        monkeypatch.setattr(creator_transport, "_utc_now", lambda: checked_at)
        with pytest.raises(CreatorTransportValidationError, match="stale|future"):
            validate_installed_capability_evidence(evidence)
    monkeypatch.setattr(creator_transport, "_utc_now", lambda: observed_at)
    tampered = deepcopy(evidence)
    tampered["combined_sha256"] = "0" * 64
    with pytest.raises(CreatorTransportValidationError, match="combined_sha256"):
        validate_installed_capability_evidence(
            tampered,
        )


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("canonical_path", "G:/copied/catalog.json", "canonical path"),
        ("last_write_at", "2026-08-08T12:44:59.000000Z", "stale"),
        ("captured_at", "2026-08-09T12:45:01.000000Z", "after observation"),
        ("captured_at", "2026-08-09T12:45:00Z", "canonical UTC"),
    ],
)
def test_installed_capability_rejects_unbound_or_invalid_source_provenance(
    monkeypatch,
    tmp_path,
    field: str,
    value: str,
    message: str,
) -> None:
    _installed_source_fixture(monkeypatch, tmp_path)
    observed_at = datetime(2026, 8, 9, 12, 45, tzinfo=timezone.utc)
    evidence = _build_installed_fixture_evidence()
    sources = evidence["sources"]
    assert isinstance(sources, dict)
    catalog = sources["authoritative_catalog"]
    assert isinstance(catalog, dict)
    catalog[field] = value
    _rehash_installed_evidence(evidence)

    with pytest.raises(CreatorTransportValidationError, match=message):
        validate_installed_capability_evidence(
            evidence,
        )


def test_installed_capture_rejects_copy_path_and_cannot_refresh_expired_sources(
    monkeypatch,
    tmp_path,
) -> None:
    catalog_bytes, registry_bytes = _installed_source_fixture(monkeypatch, tmp_path)
    canonical_catalog = tmp_path / "canonical-catalog.json"
    canonical_registry = tmp_path / "canonical-registry.json"
    copied_catalog = tmp_path / "copied-catalog.json"
    copied_registry = tmp_path / "copied-registry.json"
    for path, payload in (
        (canonical_catalog, catalog_bytes),
        (canonical_registry, registry_bytes),
        (copied_catalog, catalog_bytes),
        (copied_registry, registry_bytes),
    ):
        path.write_bytes(payload)
    source_time = datetime(2026, 8, 8, 12, 0, tzinfo=timezone.utc)
    for path in (canonical_catalog, canonical_registry, copied_catalog, copied_registry):
        timestamp = source_time.timestamp()
        os.utime(path, (timestamp, timestamp))
    monkeypatch.setattr(
        creator_transport,
        "INSTALLED_CATALOG_CANONICAL_PATH",
        canonical_catalog.resolve().as_posix(),
    )
    monkeypatch.setattr(
        creator_transport,
        "INSTALLED_REGISTRY_CANONICAL_PATH",
        canonical_registry.resolve().as_posix(),
    )

    with pytest.raises(TypeError):
        run_installed_capability_capture(
            catalog_path=copied_catalog,
            registry_path=copied_registry,
            evidence_dir=tmp_path / "copy-evidence",
            clock=lambda: source_time + timedelta(hours=1),
        )

    first_observation = source_time + timedelta(hours=23)
    monkeypatch.setattr(creator_transport, "_utc_now", lambda: first_observation)
    first = run_installed_capability_capture(
        evidence_dir=tmp_path / "fresh-evidence",
        clock=lambda: first_observation,
    )
    assert first["observed_at"] == "2026-08-09T11:00:00.000000Z"

    renewed_observation = source_time + timedelta(hours=25)
    monkeypatch.setattr(creator_transport, "_utc_now", lambda: renewed_observation)
    with pytest.raises(CreatorTransportValidationError, match="last_write_at is stale"):
        run_installed_capability_capture(
            evidence_dir=tmp_path / "expired-evidence",
            clock=lambda: renewed_observation,
        )
    assert not (tmp_path / "expired-evidence").exists()


def test_installed_capability_capture_is_timestamp_uuid_exclusive_and_collision_safe(
    monkeypatch,
    tmp_path,
) -> None:
    catalog_bytes, registry_bytes = _installed_source_fixture(monkeypatch, tmp_path)
    catalog_path = tmp_path / "catalog.json"
    registry_path = tmp_path / "registry.json"
    evidence_dir = tmp_path / "evidence"
    catalog_path.write_bytes(catalog_bytes)
    registry_path.write_bytes(registry_bytes)
    observed_at = datetime(2026, 8, 9, 12, 45, tzinfo=timezone.utc)
    source_timestamp = (observed_at - timedelta(minutes=1)).timestamp()
    os.utime(catalog_path, (source_timestamp, source_timestamp))
    os.utime(registry_path, (source_timestamp, source_timestamp))
    monkeypatch.setattr(
        creator_transport,
        "INSTALLED_CATALOG_CANONICAL_PATH",
        catalog_path.resolve().as_posix(),
    )
    monkeypatch.setattr(
        creator_transport,
        "INSTALLED_REGISTRY_CANONICAL_PATH",
        registry_path.resolve().as_posix(),
    )
    monkeypatch.setattr(creator_transport, "_utc_now", lambda: observed_at)
    fixed_uuid = UUID("12345678-1234-5678-1234-567812345678")

    first = run_installed_capability_capture(
        evidence_dir=evidence_dir,
        clock=lambda: observed_at,
        uuid_factory=lambda: fixed_uuid,
    )
    path = next(evidence_dir.glob(f"*/{INSTALLED_CAPABILITY_FILENAME}"))
    original = path.read_bytes()

    with pytest.raises(CreatorTransportValidationError, match="collision"):
        run_installed_capability_capture(
            evidence_dir=evidence_dir,
            clock=lambda: observed_at,
            uuid_factory=lambda: fixed_uuid,
        )

    assert path.read_bytes() == original
    assert json.loads(original) == first
    assert len(list(evidence_dir.glob(f"*/{INSTALLED_CAPABILITY_FILENAME}"))) == 1
