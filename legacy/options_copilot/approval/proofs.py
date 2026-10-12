"""Strict, canonical proof contracts for approval-time broker and NAV truth."""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from decimal import Decimal, InvalidOperation
import re

from options_copilot.storage.canonical import canonical_hash, datetime_text, utc_datetime


BROKER_PROOF_SCHEMA = "options_copilot.approval.broker_proof.v1"
STRATEGY_NAV_PROOF_SCHEMA = "options_copilot.approval.strategy_nav_proof.v3"
STRATEGY_NAV_AUTHORITY_SCHEMA = "options_copilot.strategy_nav_authority.v1"
BROKER_PROOF_MAX_AGE_SECONDS = 5

_BROKER_PROOF_KEYS = frozenset(
    {
        "schema",
        "ranking_snapshot_id",
        "candidate_id",
        "proposal_hash",
        "snapshot_hash",
        "built_at",
        "quote_batch_id",
        "oldest_quote_observed_at",
        "state_hashes",
        "contract_definitions_hash",
        "quotes_hash",
        "contract_ids",
        "account_nlv_usd",
        "open_option_position_count",
        "working_order_count",
        "unsubmitted_instruction_count",
        "status",
    }
)
_STATE_HASH_KEYS = frozenset(
    {"account", "positions", "working_orders", "unsubmitted_instructions"}
)
_STRATEGY_NAV_PROOF_KEYS = frozenset(
    {
        "schema",
        "content_hash",
        "authority_hash",
        "contract_hash",
        "ledger_head_hash",
        "strategy_nav_usd",
        "observed_account_nlv",
        "reconciliation_difference",
        "asof",
        "snapshot_payload",
    }
)
_STRATEGY_NAV_SNAPSHOT_KEYS = frozenset(
    {
        "asof",
        "strategy_nav",
        "strategy_deposits",
        "strategy_withdrawals",
        "realized_pnl",
        "open_position_unrealized_pnl",
        "fees",
        "signed_corrections",
        "non_strategy_contribution",
        "fill_principal_contribution",
        "observed_account_nlv",
        "reconciliation_difference",
        "contract_version",
        "contract_hash",
        "ledger_head_hash",
        "valid",
        "no_trade_reasons",
    }
)
_STRATEGY_NAV_CANDIDATE_FIELDS = {
    "content_hash": "strategy_nav_content_hash",
    "authority_hash": "strategy_nav_hash",
    "contract_hash": "strategy_nav_contract_hash",
    "ledger_head_hash": "strategy_nav_ledger_head_hash",
    "strategy_nav_usd": "strategy_nav_usd",
    "observed_account_nlv": "strategy_nav_observed_account_nlv",
    "reconciliation_difference": "strategy_nav_reconciliation_difference",
    "asof": "strategy_nav_asof",
}
_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,159}\Z")
_HASH_RE = re.compile(r"[0-9a-f]{64}\Z")


class ApprovalProofError(ValueError):
    """A supplied approval proof is incomplete, stale, or authority-conflicting."""


def normalize_broker_proof(
    proof: Mapping[str, object],
    *,
    ranking_snapshot_id: str,
    candidate_id: str,
    proposal_hash: str,
    candidate_body: Mapping[str, object],
    checked_at: datetime,
) -> dict[str, object]:
    """Validate and canonicalize one current, component-atomic broker proof."""

    source = _exact_mapping("broker_proof", proof, _BROKER_PROOF_KEYS)
    if source["schema"] != BROKER_PROOF_SCHEMA:
        raise ApprovalProofError("broker_proof schema is unsupported")
    for field, expected in (
        ("ranking_snapshot_id", ranking_snapshot_id),
        ("candidate_id", candidate_id),
    ):
        value = _identifier(f"broker_proof.{field}", source[field])
        if value != expected:
            raise ApprovalProofError(f"broker_proof {field} does not match frozen authority")
    supplied_proposal_hash = _digest(
        "broker_proof.proposal_hash", source["proposal_hash"]
    )
    if supplied_proposal_hash != proposal_hash:
        raise ApprovalProofError(
            "broker_proof proposal_hash does not match frozen authority"
        )

    at = utc_datetime(checked_at, field="broker_proof checked_at")
    built_at = _fresh_timestamp("broker_proof.built_at", source["built_at"], at)
    oldest_quote = _fresh_timestamp(
        "broker_proof.oldest_quote_observed_at",
        source["oldest_quote_observed_at"],
        at,
    )

    state_source = _exact_mapping(
        "broker_proof.state_hashes", source["state_hashes"], _STATE_HASH_KEYS
    )
    state_hashes = {
        name: _digest(f"broker_proof.state_hashes.{name}", state_source[name])
        for name in sorted(_STATE_HASH_KEYS)
    }
    contract_ids = _contract_ids(source["contract_ids"])
    expected_contract_ids = _candidate_contract_ids(candidate_body)
    if contract_ids != expected_contract_ids:
        raise ApprovalProofError(
            "broker_proof contract_ids do not match frozen candidate legs"
        )
    for field in (
        "open_option_position_count",
        "working_order_count",
        "unsubmitted_instruction_count",
    ):
        if type(source[field]) is not int or source[field] != 0:
            raise ApprovalProofError(f"broker_proof {field} must be integer zero")
    if source["status"] != "COMPLETE":
        raise ApprovalProofError("broker_proof status must be COMPLETE")

    return {
        "schema": BROKER_PROOF_SCHEMA,
        "ranking_snapshot_id": ranking_snapshot_id,
        "candidate_id": candidate_id,
        "proposal_hash": supplied_proposal_hash,
        "snapshot_hash": _digest(
            "broker_proof.snapshot_hash", source["snapshot_hash"]
        ),
        "built_at": datetime_text(built_at),
        "quote_batch_id": _identifier(
            "broker_proof.quote_batch_id", source["quote_batch_id"]
        ),
        "oldest_quote_observed_at": datetime_text(oldest_quote),
        "state_hashes": state_hashes,
        "contract_definitions_hash": _digest(
            "broker_proof.contract_definitions_hash",
            source["contract_definitions_hash"],
        ),
        "quotes_hash": _digest("broker_proof.quotes_hash", source["quotes_hash"]),
        "contract_ids": list(contract_ids),
        "account_nlv_usd": _decimal_text(
            "broker_proof.account_nlv_usd",
            source["account_nlv_usd"],
            positive=True,
        ),
        "open_option_position_count": 0,
        "working_order_count": 0,
        "unsubmitted_instruction_count": 0,
        "status": "COMPLETE",
    }


def normalize_strategy_nav_proof(
    proof: Mapping[str, object],
    *,
    candidate_body: Mapping[str, object],
    broker_proof: Mapping[str, object],
    checked_at: datetime,
) -> dict[str, object]:
    """Validate and canonicalize the NAV proof bound into a frozen candidate."""

    source = _exact_mapping(
        "strategy_nav_proof", proof, _STRATEGY_NAV_PROOF_KEYS
    )
    if source["schema"] != STRATEGY_NAV_PROOF_SCHEMA:
        raise ApprovalProofError("strategy_nav_proof schema is unsupported")
    content_hash = _digest(
        "strategy_nav_proof.content_hash", source["content_hash"]
    )
    authority_hash = _digest(
        "strategy_nav_proof.authority_hash", source["authority_hash"]
    )
    contract_hash = _digest(
        "strategy_nav_proof.contract_hash", source["contract_hash"]
    )
    ledger_head_hash = _digest(
        "strategy_nav_proof.ledger_head_hash", source["ledger_head_hash"]
    )
    strategy_nav_usd = _decimal_text(
        "strategy_nav_proof.strategy_nav_usd",
        source["strategy_nav_usd"],
        positive=True,
    )
    expected_authority_payload = {
        "schema": STRATEGY_NAV_AUTHORITY_SCHEMA,
        "strategy_nav_usd": Decimal(strategy_nav_usd),
        "contract_hash": contract_hash,
        "ledger_head_hash": ledger_head_hash,
    }
    if canonical_hash(expected_authority_payload) != authority_hash:
        raise ApprovalProofError("strategy_nav_proof authority_hash is invalid")

    observed_nlv = _decimal_text(
        "strategy_nav_proof.observed_account_nlv",
        source["observed_account_nlv"],
        positive=True,
    )
    broker_nlv = _decimal_text(
        "broker_proof.account_nlv_usd",
        broker_proof.get("account_nlv_usd"),
        positive=True,
    )
    if Decimal(observed_nlv) != Decimal(broker_nlv):
        raise ApprovalProofError(
            "strategy_nav_proof observed_account_nlv does not match broker proof"
        )
    reconciliation_difference = _decimal_text(
        "strategy_nav_proof.reconciliation_difference",
        source["reconciliation_difference"],
        positive=False,
    )
    if Decimal(reconciliation_difference) != (
        Decimal(observed_nlv) - Decimal(strategy_nav_usd)
    ):
        raise ApprovalProofError(
            "strategy_nav_proof reconciliation_difference is invalid"
        )
    at = utc_datetime(checked_at, field="strategy_nav_proof checked_at")
    asof = _timestamp("strategy_nav_proof.asof", source["asof"])
    if asof > at:
        raise ApprovalProofError("strategy_nav_proof asof cannot be in the future")

    snapshot_payload = _normalize_strategy_nav_snapshot_payload(
        source["snapshot_payload"]
    )
    if canonical_hash(snapshot_payload) != content_hash:
        raise ApprovalProofError(
            "strategy_nav_proof content_hash does not match snapshot_payload"
        )
    snapshot_binding = {
        "content_hash": content_hash,
        "authority_hash": authority_hash,
        "contract_hash": contract_hash,
        "ledger_head_hash": ledger_head_hash,
        "strategy_nav_usd": strategy_nav_usd,
        "observed_account_nlv": observed_nlv,
        "reconciliation_difference": reconciliation_difference,
        "asof": datetime_text(asof),
    }
    _require_strategy_nav_snapshot_matches_binding(
        snapshot_payload,
        snapshot_binding,
    )
    require_candidate_strategy_nav_binding(candidate_body, snapshot_binding)

    return {
        "schema": STRATEGY_NAV_PROOF_SCHEMA,
        "content_hash": content_hash,
        "authority_hash": authority_hash,
        "contract_hash": contract_hash,
        "ledger_head_hash": ledger_head_hash,
        "strategy_nav_usd": strategy_nav_usd,
        "observed_account_nlv": observed_nlv,
        "reconciliation_difference": reconciliation_difference,
        "asof": datetime_text(asof),
        "snapshot_payload": snapshot_payload,
    }


def require_candidate_strategy_nav_binding(
    candidate_body: Mapping[str, object],
    nav_binding: Mapping[str, object],
) -> None:
    """Require one complete Strategy NAV identity across ranking and approval."""

    if not isinstance(candidate_body, Mapping) or not isinstance(nav_binding, Mapping):
        raise ApprovalProofError("Strategy NAV binding must be a mapping")
    candidate = {
        "content_hash": _digest(
            "candidate_body.strategy_nav_content_hash",
            candidate_body.get("strategy_nav_content_hash"),
        ),
        "authority_hash": _digest(
            "candidate_body.strategy_nav_hash",
            candidate_body.get("strategy_nav_hash"),
        ),
        "contract_hash": _digest(
            "candidate_body.strategy_nav_contract_hash",
            candidate_body.get("strategy_nav_contract_hash"),
        ),
        "ledger_head_hash": _digest(
            "candidate_body.strategy_nav_ledger_head_hash",
            candidate_body.get("strategy_nav_ledger_head_hash"),
        ),
        "strategy_nav_usd": _decimal_text(
            "candidate_body.strategy_nav_usd",
            candidate_body.get("strategy_nav_usd"),
            positive=True,
        ),
        "observed_account_nlv": _decimal_text(
            "candidate_body.strategy_nav_observed_account_nlv",
            candidate_body.get("strategy_nav_observed_account_nlv"),
            positive=True,
        ),
        "reconciliation_difference": _decimal_text(
            "candidate_body.strategy_nav_reconciliation_difference",
            candidate_body.get("strategy_nav_reconciliation_difference"),
            positive=False,
        ),
        "asof": datetime_text(
            _timestamp(
                "candidate_body.strategy_nav_asof",
                candidate_body.get("strategy_nav_asof"),
            )
        ),
    }
    current = {
        "content_hash": _digest(
            "strategy_nav_binding.content_hash", nav_binding.get("content_hash")
        ),
        "authority_hash": _digest(
            "strategy_nav_binding.authority_hash", nav_binding.get("authority_hash")
        ),
        "contract_hash": _digest(
            "strategy_nav_binding.contract_hash", nav_binding.get("contract_hash")
        ),
        "ledger_head_hash": _digest(
            "strategy_nav_binding.ledger_head_hash",
            nav_binding.get("ledger_head_hash"),
        ),
        "strategy_nav_usd": _decimal_text(
            "strategy_nav_binding.strategy_nav_usd",
            nav_binding.get("strategy_nav_usd"),
            positive=True,
        ),
        "observed_account_nlv": _decimal_text(
            "strategy_nav_binding.observed_account_nlv",
            nav_binding.get("observed_account_nlv"),
            positive=True,
        ),
        "reconciliation_difference": _decimal_text(
            "strategy_nav_binding.reconciliation_difference",
            nav_binding.get("reconciliation_difference"),
            positive=False,
        ),
        "asof": datetime_text(
            _timestamp("strategy_nav_binding.asof", nav_binding.get("asof"))
        ),
    }
    for binding_name, candidate_name in _STRATEGY_NAV_CANDIDATE_FIELDS.items():
        if current[binding_name] != candidate[binding_name]:
            raise ApprovalProofError(
                f"Strategy NAV {candidate_name} does not match frozen candidate"
            )


def _normalize_strategy_nav_snapshot_payload(value: object) -> dict[str, object]:
    source = _exact_mapping(
        "strategy_nav_proof.snapshot_payload",
        value,
        _STRATEGY_NAV_SNAPSHOT_KEYS,
    )
    if source["valid"] is not True:
        raise ApprovalProofError("strategy_nav_proof snapshot_payload must be valid")
    reasons = source["no_trade_reasons"]
    if (
        not isinstance(reasons, Sequence)
        or isinstance(reasons, (str, bytes, bytearray))
        or tuple(reasons)
    ):
        raise ApprovalProofError(
            "strategy_nav_proof snapshot_payload no_trade_reasons must be empty"
        )
    contract_version = _identifier(
        "strategy_nav_proof.snapshot_payload.contract_version",
        source["contract_version"],
    )
    payload: dict[str, object] = {
        "asof": _timestamp(
            "strategy_nav_proof.snapshot_payload.asof", source["asof"]
        ),
        "strategy_nav": Decimal(
            _decimal_text(
                "strategy_nav_proof.snapshot_payload.strategy_nav",
                source["strategy_nav"],
                positive=True,
            )
        ),
        "observed_account_nlv": Decimal(
            _decimal_text(
                "strategy_nav_proof.snapshot_payload.observed_account_nlv",
                source["observed_account_nlv"],
                positive=True,
            )
        ),
        "reconciliation_difference": Decimal(
            _decimal_text(
                "strategy_nav_proof.snapshot_payload.reconciliation_difference",
                source["reconciliation_difference"],
                positive=False,
            )
        ),
        "contract_version": contract_version,
        "contract_hash": _digest(
            "strategy_nav_proof.snapshot_payload.contract_hash",
            source["contract_hash"],
        ),
        "ledger_head_hash": _digest(
            "strategy_nav_proof.snapshot_payload.ledger_head_hash",
            source["ledger_head_hash"],
        ),
        "valid": True,
        "no_trade_reasons": (),
    }
    for field in (
        "strategy_deposits",
        "strategy_withdrawals",
        "realized_pnl",
        "open_position_unrealized_pnl",
        "fees",
        "signed_corrections",
        "non_strategy_contribution",
        "fill_principal_contribution",
    ):
        payload[field] = Decimal(
            _decimal_text(
                f"strategy_nav_proof.snapshot_payload.{field}",
                source[field],
                positive=False,
            )
        )
    return payload


def _require_strategy_nav_snapshot_matches_binding(
    snapshot: Mapping[str, object],
    binding: Mapping[str, object],
) -> None:
    pairs = (
        ("strategy_nav", "strategy_nav_usd", True),
        ("observed_account_nlv", "observed_account_nlv", True),
        ("reconciliation_difference", "reconciliation_difference", False),
    )
    for snapshot_field, binding_field, positive in pairs:
        if _decimal_text(
            f"strategy_nav_proof.snapshot_payload.{snapshot_field}",
            snapshot.get(snapshot_field),
            positive=positive,
        ) != _decimal_text(
            f"strategy_nav_proof.{binding_field}",
            binding.get(binding_field),
            positive=positive,
        ):
            raise ApprovalProofError(
                f"strategy_nav_proof {binding_field} does not match snapshot_payload"
            )
    if snapshot.get("contract_hash") != binding.get("contract_hash"):
        raise ApprovalProofError(
            "strategy_nav_proof contract_hash does not match snapshot_payload"
        )
    if snapshot.get("ledger_head_hash") != binding.get("ledger_head_hash"):
        raise ApprovalProofError(
            "strategy_nav_proof ledger_head_hash does not match snapshot_payload"
        )
    if datetime_text(
        _timestamp("strategy_nav_proof.snapshot_payload.asof", snapshot.get("asof"))
    ) != binding.get("asof"):
        raise ApprovalProofError(
            "strategy_nav_proof asof does not match snapshot_payload"
        )


def _exact_mapping(
    field: str, value: object, expected_keys: frozenset[str]
) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise ApprovalProofError(f"{field} must be a mapping")
    keys = set(value)
    if any(not isinstance(key, str) for key in keys):
        raise ApprovalProofError(f"{field} keys must be strings")
    if keys != expected_keys:
        missing = sorted(expected_keys - keys)
        extra = sorted(keys - expected_keys)
        details = []
        if missing:
            details.append(f"missing={','.join(missing)}")
        if extra:
            details.append(f"extra={','.join(extra)}")
        raise ApprovalProofError(f"{field} fields are invalid ({'; '.join(details)})")
    return value


def _digest(field: str, value: object) -> str:
    if not isinstance(value, str) or _HASH_RE.fullmatch(value) is None:
        raise ApprovalProofError(f"{field} must be lowercase SHA-256 hex")
    return value


def _identifier(field: str, value: object) -> str:
    if not isinstance(value, str) or _ID_RE.fullmatch(value) is None:
        raise ApprovalProofError(f"{field} is not a valid identifier")
    return value


def _timestamp(field: str, value: object) -> datetime:
    if isinstance(value, datetime):
        return utc_datetime(value, field=field)
    if not isinstance(value, str) or not value or value != value.strip():
        raise ApprovalProofError(f"{field} must be a timezone-aware datetime")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return utc_datetime(parsed, field=field)
    except (TypeError, ValueError) as exc:
        raise ApprovalProofError(
            f"{field} must be a timezone-aware datetime"
        ) from exc


def _fresh_timestamp(field: str, value: object, checked_at: datetime) -> datetime:
    parsed = _timestamp(field, value)
    age = (checked_at - parsed).total_seconds()
    if age < 0:
        raise ApprovalProofError(f"{field} cannot be in the future")
    if age > BROKER_PROOF_MAX_AGE_SECONDS:
        raise ApprovalProofError(f"{field} is stale")
    return parsed


def _contract_ids(value: object) -> tuple[int, ...]:
    if not isinstance(value, Sequence) or isinstance(
        value, (str, bytes, bytearray, memoryview)
    ):
        raise ApprovalProofError("broker_proof.contract_ids must be a sequence")
    ids = tuple(value)
    if (
        not ids
        or any(type(item) is not int or item <= 0 for item in ids)
        or len(ids) != len(set(ids))
    ):
        raise ApprovalProofError(
            "broker_proof.contract_ids must be unique positive integers"
        )
    return tuple(sorted(ids))


def _candidate_contract_ids(candidate_body: Mapping[str, object]) -> tuple[int, ...]:
    legs = candidate_body.get("legs")
    if not isinstance(legs, Sequence) or isinstance(
        legs, (str, bytes, bytearray, memoryview)
    ):
        raise ApprovalProofError("frozen candidate legs are invalid")
    ids: list[int] = []
    for leg in legs:
        if not isinstance(leg, Mapping):
            raise ApprovalProofError("frozen candidate legs are invalid")
        contract_id = leg.get("con_id")
        if type(contract_id) is not int or contract_id <= 0:
            raise ApprovalProofError("frozen candidate contract_ids are invalid")
        ids.append(contract_id)
    if not ids or len(ids) != len(set(ids)):
        raise ApprovalProofError("frozen candidate contract_ids are invalid")
    return tuple(sorted(ids))


def _decimal_text(field: str, value: object, *, positive: bool) -> str:
    if isinstance(value, bool):
        raise ApprovalProofError(f"{field} must be finite decimal")
    try:
        decimal = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ApprovalProofError(f"{field} must be finite decimal") from exc
    if not decimal.is_finite() or (positive and decimal <= 0):
        qualifier = "positive finite" if positive else "finite"
        raise ApprovalProofError(f"{field} must be {qualifier} decimal")
    normalized = decimal.normalize()
    return "0" if not normalized else format(normalized, "f")


__all__ = [
    "ApprovalProofError",
    "BROKER_PROOF_MAX_AGE_SECONDS",
    "BROKER_PROOF_SCHEMA",
    "STRATEGY_NAV_PROOF_SCHEMA",
    "normalize_broker_proof",
    "normalize_strategy_nav_proof",
    "require_candidate_strategy_nav_binding",
]
