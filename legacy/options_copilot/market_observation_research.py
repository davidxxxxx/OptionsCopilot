"""Supporting-only adapter for bounded IBKR market observations."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timezone

from options_copilot.gateway.market_observation import (
    MAX_OBSERVATION_BYTES,
    OBSERVATION_SCHEMA,
    RESEARCH_INPUT_SCHEMA,
    build_research_market_input,
)
from options_copilot.storage.canonical import canonical_hash


READ_MODEL_SCHEMA = "options_copilot.market_observation_research_read_model.v1"
FRESH_EXECUTABLE_EVIDENCE_REASON = "FRESH_EXECUTABLE_OPTION_EVIDENCE_REQUIRED"
MAX_RESEARCH_REQUEST_BYTES = MAX_OBSERVATION_BYTES + 65_536
MAX_RESEARCH_JSON_DEPTH = 64

_CONTRACT_FIELDS = (
    "contract_id",
    "symbol",
    "security_type",
    "currency",
    "exchange",
    "primary_exchange",
    "expiration",
    "strike",
    "right",
    "trading_class",
    "multiplier",
)
_PRICE_FIELDS = (
    "tick_type",
    "tick_variant",
    "value",
    "valid",
    "callback_received_at",
    "market_data_type",
    "source_event_time",
    "source_event_time_status",
)
_COMPUTATION_VALUE_FIELDS = (
    "implied_volatility",
    "delta",
    "gamma",
    "vega",
    "theta",
    "option_price",
    "underlying_price",
)


class MarketObservationResearchAdapter:
    """Convert one bounded observation into a non-authoritative read model."""

    def __init__(
        self,
        *,
        clock: Callable[[], datetime] | None = None,
        max_age_seconds: float = 15.0,
    ) -> None:
        if (
            isinstance(max_age_seconds, bool)
            or not isinstance(max_age_seconds, (int, float))
            or not 0 < float(max_age_seconds) <= 15.0
        ):
            raise ValueError("MARKET_OBSERVATION_RESEARCH_MAX_AGE_INVALID")
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._max_age_seconds = float(max_age_seconds)

    def project(self, observation: Mapping[str, object]) -> dict[str, object]:
        """Validate and project an observation without acquiring new data."""

        if not isinstance(observation, Mapping):
            raise TypeError("market observation must be a mapping")
        if observation.get("schema") != OBSERVATION_SCHEMA:
            raise ValueError("MARKET_OBSERVATION_SCHEMA_INVALID")
        research_input = build_research_market_input(
            observation,
            now=self._clock(),
            max_age_seconds=self._max_age_seconds,
        )
        if research_input.get("schema") != RESEARCH_INPUT_SCHEMA:
            raise ValueError("MARKET_OBSERVATION_RESEARCH_INPUT_INVALID")
        reasons = _text_sequence(research_input.get("reason_codes"))
        if FRESH_EXECUTABLE_EVIDENCE_REASON not in reasons:
            reasons.append(FRESH_EXECUTABLE_EVIDENCE_REASON)
        body = {
            "schema": READ_MODEL_SCHEMA,
            "status": research_input.get("status"),
            "checked_at": research_input.get("checked_at"),
            "observation_hash": research_input.get("observation_hash"),
            "source_projection_hash": research_input.get("content_hash"),
            "contracts": [
                _project_contract_row(row)
                for row in _mapping_sequence(research_input.get("contracts"))
            ],
            "reason_codes": reasons,
            "decision_authority": "OBSERVATION_ONLY",
            "research_only": True,
            "model_input_complete": False,
            "production_eligible": False,
            "entry_authority": False,
            "approval_eligible": False,
            "instruction_creation_allowed": False,
            "order_allowed": False,
            "requires_fresh_executable_evidence": True,
        }
        return validate_market_observation_research_read_model(
            {**body, "content_hash": canonical_hash(body)}
        )


def validate_market_observation_research_read_model(
    value: Mapping[str, object],
) -> dict[str, object]:
    """Require the exact public read-model shape and its fail-closed authority."""

    if not isinstance(value, Mapping):
        raise TypeError("market observation research read model must be a mapping")
    required = {
        "schema",
        "status",
        "checked_at",
        "observation_hash",
        "source_projection_hash",
        "contracts",
        "reason_codes",
        "decision_authority",
        "research_only",
        "model_input_complete",
        "production_eligible",
        "entry_authority",
        "approval_eligible",
        "instruction_creation_allowed",
        "order_allowed",
        "requires_fresh_executable_evidence",
        "content_hash",
    }
    if set(value) != required or value.get("schema") != READ_MODEL_SCHEMA:
        raise ValueError("MARKET_OBSERVATION_RESEARCH_READ_MODEL_INVALID")
    if value.get("status") not in {"RESEARCH_READY", "RESEARCH_DEGRADED"}:
        raise ValueError("MARKET_OBSERVATION_RESEARCH_READ_MODEL_INVALID")
    if (
        value.get("decision_authority") != "OBSERVATION_ONLY"
        or value.get("research_only") is not True
        or value.get("model_input_complete") is not False
        or value.get("production_eligible") is not False
        or value.get("entry_authority") is not False
        or value.get("approval_eligible") is not False
        or value.get("instruction_creation_allowed") is not False
        or value.get("order_allowed") is not False
        or value.get("requires_fresh_executable_evidence") is not True
    ):
        raise ValueError("MARKET_OBSERVATION_RESEARCH_AUTHORITY_INVALID")
    reasons = _text_sequence(value.get("reason_codes"))
    if FRESH_EXECUTABLE_EVIDENCE_REASON not in reasons:
        raise ValueError("MARKET_OBSERVATION_RESEARCH_READ_MODEL_INVALID")
    contracts = _mapping_sequence(value.get("contracts"))
    if len(contracts) > 3:
        raise ValueError("MARKET_OBSERVATION_RESEARCH_READ_MODEL_INVALID")
    if any(_project_contract_row(row) != dict(row) for row in contracts):
        raise ValueError("MARKET_OBSERVATION_RESEARCH_READ_MODEL_INVALID")
    body = dict(value)
    supplied_hash = body.pop("content_hash", None)
    if not isinstance(supplied_hash, str) or canonical_hash(body) != supplied_hash:
        raise ValueError("MARKET_OBSERVATION_RESEARCH_HASH_INVALID")
    return dict(value)


def _project_contract_row(value: Mapping[str, object]) -> dict[str, object]:
    contract = value.get("contract")
    market_fields = value.get("market_fields")
    if not isinstance(contract, Mapping) or not isinstance(market_fields, Mapping):
        raise ValueError("MARKET_OBSERVATION_RESEARCH_INPUT_INVALID")
    prices = market_fields.get("prices")
    computations = market_fields.get("option_computations")
    if not isinstance(prices, Mapping) or not isinstance(computations, Mapping):
        raise ValueError("MARKET_OBSERVATION_RESEARCH_INPUT_INVALID")
    return {
        "contract": {name: contract.get(name) for name in _CONTRACT_FIELDS},
        "market_fields": {
            "prices": {
                name: _project_price_field(row)
                for name, row in prices.items()
                if name in {"bid", "ask", "last"} and isinstance(row, Mapping)
            },
            "option_computations": {
                name: _project_computation_field(row)
                for name, row in computations.items()
                if name in {"bid", "ask", "last", "model"}
                and isinstance(row, Mapping)
            },
            "price_tick_history": [
                _project_price_field(row)
                for row in _mapping_sequence(market_fields.get("price_tick_history"))
            ],
            "option_computation_tick_history": [
                _project_computation_field(row)
                for row in _mapping_sequence(
                    market_fields.get("option_computation_tick_history")
                )
            ],
        },
        "observed_fields": _text_sequence(value.get("observed_fields")),
        "missing_or_invalid_fields": _text_sequence(
            value.get("missing_or_invalid_fields")
        ),
        "stale_fields": _text_sequence(value.get("stale_fields")),
        "non_live_fields": _text_sequence(value.get("non_live_fields")),
        "distinct_callback_receipt_times": _text_sequence(
            value.get("distinct_callback_receipt_times")
        ),
        "market_data_types_observed": list(
            value.get("market_data_types_observed", ())
        ),
        "mixed_market_data_types": value.get("mixed_market_data_types") is True,
        "history_truncated": dict(value.get("history_truncated", {})),
        "source_event_time": None,
        "source_event_time_status": "UNAVAILABLE",
        "atomic_source_snapshot": False,
    }


def _project_price_field(value: Mapping[str, object]) -> dict[str, object]:
    return {name: value.get(name) for name in _PRICE_FIELDS}


def _project_computation_field(value: Mapping[str, object]) -> dict[str, object]:
    projected = _project_price_field(value)
    raw_values = value.get("values")
    validity = value.get("validity")
    projected["values"] = {
        name: raw_values.get(name)
        for name in _COMPUTATION_VALUE_FIELDS
    } if isinstance(raw_values, Mapping) else {}
    projected["validity"] = {
        name: validity.get(name)
        for name in _COMPUTATION_VALUE_FIELDS
    } if isinstance(validity, Mapping) else {}
    return projected


def _mapping_sequence(value: object) -> list[Mapping[str, object]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise ValueError("MARKET_OBSERVATION_RESEARCH_INPUT_INVALID")
    if not all(isinstance(item, Mapping) for item in value):
        raise ValueError("MARKET_OBSERVATION_RESEARCH_INPUT_INVALID")
    return list(value)  # type: ignore[arg-type]


def _text_sequence(value: object) -> list[str]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise ValueError("MARKET_OBSERVATION_RESEARCH_INPUT_INVALID")
    if not all(isinstance(item, str) for item in value):
        raise ValueError("MARKET_OBSERVATION_RESEARCH_INPUT_INVALID")
    return list(dict.fromkeys(value))


__all__ = [
    "FRESH_EXECUTABLE_EVIDENCE_REASON",
    "MAX_RESEARCH_REQUEST_BYTES",
    "MAX_RESEARCH_JSON_DEPTH",
    "MarketObservationResearchAdapter",
    "READ_MODEL_SCHEMA",
    "validate_market_observation_research_read_model",
]
