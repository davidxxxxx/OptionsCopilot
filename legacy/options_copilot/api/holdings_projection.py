"""Time-bounded, non-authoritative display of full-inventory close observations."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
import re


HOLDINGS_CLOSE_SCHEMA = "options_copilot.holdings_close_preview.v1"
_MAX_AGE_SECONDS = 5
_META_FIELDS = frozenset({
    "schema", "candidate_id", "symbol", "expiration", "scope",
    "grouping_status", "review_state", "management_kind", "generated_at",
    "oldest_quote_at", "proof_scope", "after_risk_condition",
    "atomic_basket_fill_status", "legging_risk_status", "assignment_risk_status",
    "calendar_evidence_status", "extrinsic_value_path_status", "broker_margin_status", "exposure_basis",
    "capital_usage_basis", "candidate_hash", "transition_proof_hash",
    "exit_contract_hash", "assignment_assumption_hash", "broker_snapshot_hash",
    "quote_batch_id", "quote_batch_hash", "positions_state_hash",
    "execution_cost_contract_version", "execution_cost_contract_hash",
})
_FINANCIAL_FIELDS = frozenset({
    "gross_component_liquidation_cashflow_usd", "estimated_commission_usd",
    "normal_slippage_usd", "stress_slippage_usd", "estimated_execution_cost_usd",
    "all_in_close_cashflow_usd",
})
_LEG_FIELDS = frozenset({
    "contract_id", "local_symbol", "expiration", "strike", "right",
    "current_signed_quantity", "signed_quantity_delta", "action",
    "action_quantity", "multiplier", "executable_price", "bid", "ask",
    "quote_observed_at", "secdef_identity_hash", "trading_class", "exchange",
})
_RISK_FIELDS = frozenset({"max_loss_usd", "exposure_usd", "capital_usage_usd"})
_PAYOFF_FIELDS = frozenset({
    "schema", "valuation_basis", "model", "gross_liquidation_cashflow_usd",
    "estimated_future_exit_cost_usd", "future_exit_cost_included_in_max_loss",
    "min_intrinsic_usd", "max_intrinsic_usd", "risk_scope",
    "historical_entry_cost_included", "reopening_premium_included",
    "cost_adjusted_holding_max_loss_usd",
    "gross_holding_max_loss_usd", "max_loss_usd", "max_profit_usd",
    "unbounded_profit", "geometry_hash", "payoff_hash",
})


def _timestamp(value: object) -> datetime | None:
    if not isinstance(value, str) or len(value) > 64:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(timezone.utc)


def _pick(raw: object, fields: frozenset[str]) -> dict[str, object]:
    if not isinstance(raw, Mapping):
        return {}
    return {
        name: value for name in fields
        if (value := raw.get(name)) is None
        or isinstance(value, (str, bool, int, float))
    }


def project_holdings_close_preview(
    raw: Mapping[str, object], *, now: datetime | None = None,
) -> dict[str, object]:
    """Expire current financial figures without erasing source evidence identity.

    The source checksum is a consistency check, not a signature or broker
    authority. This projection never becomes the source document it references.
    """

    from options_copilot.positions.holdings_close import (
        verify_holdings_close_preview_payload,
    )

    instant = now or datetime.now(timezone.utc)
    if instant.tzinfo is None or instant.utcoffset() is None:
        raise ValueError("holdings projection clock must be timezone-aware")
    instant = instant.astimezone(timezone.utc)
    try:
        source_valid = verify_holdings_close_preview_payload(raw)
    except (TypeError, ValueError, ArithmeticError):
        source_valid = False
    generated = _timestamp(raw.get("generated_at"))
    oldest = _timestamp(raw.get("oldest_quote_at"))
    raw_legs = raw.get("execution_legs")
    legs = (
        list(raw_legs)
        if isinstance(raw_legs, Sequence)
        and not isinstance(raw_legs, (str, bytes, bytearray))
        and 1 <= len(raw_legs) <= 8
        else []
    )
    quote_times = [
        _timestamp(leg.get("quote_observed_at"))
        if isinstance(leg, Mapping) else None
        for leg in legs
    ]
    current = bool(
        source_valid and generated is not None and oldest is not None
        and generated <= instant and legs
        and all(moment is not None for moment in quote_times)
    )
    if current:
        times = [moment for moment in quote_times if moment is not None]
        current = bool(
            oldest == min(times) and max(times) <= generated
            and all(0 <= (instant - moment).total_seconds() <= _MAX_AGE_SECONDS for moment in times)
        )
    projected = _pick(raw, _META_FIELDS)
    projected.update({name: raw.get(name) if current else None for name in _FINANCIAL_FIELDS})
    projected["before_payoff"] = _pick(raw.get("before_payoff"), _PAYOFF_FIELDS) if current else {}
    projected["before_risk"] = _pick(raw.get("before_risk"), _RISK_FIELDS) if current else {}
    projected["after_risk"] = _pick(raw.get("after_risk"), _RISK_FIELDS) if current else {}
    projected_legs = [_pick(leg, _LEG_FIELDS) for leg in legs if isinstance(leg, Mapping)]
    if not current:
        for leg in projected_legs:
            for name in ("bid", "ask", "executable_price"):
                leg[name] = None
    projected["execution_legs"] = projected_legs
    raw_reasons = raw.get("reason_codes")
    reasons = [
        reason for reason in raw_reasons[:32]
        if isinstance(reason, str) and re.fullmatch(r"[A-Z][A-Z0-9_]{0,95}", reason)
    ] if isinstance(raw_reasons, (list, tuple)) else []
    if not source_valid:
        reasons.append("HOLDINGS_PREVIEW_SOURCE_INVALID")
    if not current:
        reasons.append("HOLDINGS_PREVIEW_QUOTE_REFERENCE_STALE_OR_UNAVAILABLE")
    projected.update({
        "scope": "ALL_OBSERVED_OPTION_HOLDINGS",
        "grouping_status": "NOT_INFERRED",
        "review_state": "PREVIEW_UNVERIFIED",
        "management_kind": "CLOSE_ALL",
        "quote_reference_status": "CURRENT_COMPONENT_REFERENCE" if current else "STALE_OR_UNAVAILABLE",
        "projection_checked_at": instant.isoformat(),
        "projection_is_source_payload": False,
        "source_payload_valid": source_valid,
        "reason_codes": sorted(set(reasons)),
        "review_only": True,
        "dry_run_only": True,
        "approval_enabled": False,
        "instruction_enabled": False,
        "instruction_creation_allowed": False,
        "direct_order_submission": False,
        "order_allowed": False,
        "affects_eligibility": False,
    })
    return projected


__all__ = ["HOLDINGS_CLOSE_SCHEMA", "project_holdings_close_preview"]
