"""Strict supporting-only Phase 2 advisory boundary contracts."""
from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Annotated, Literal, Sequence

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from options_copilot.storage.canonical import utc_datetime


ADVISORY_SCHEMA_VERSION = "options_copilot.phase2_advisory.v1"
MAXIMUM_DISPLAY_TEXT_LENGTH = 320
MAXIMUM_EVIDENCE_ID_LENGTH = 128
MAXIMUM_SLICE_ITEMS = 16
MAXIMUM_PROVENANCE_IDS = 64

ShortText = Annotated[
    str,
    Field(min_length=1, max_length=MAXIMUM_DISPLAY_TEXT_LENGTH),
]
EvidenceId = Annotated[
    str,
    Field(min_length=1, max_length=MAXIMUM_EVIDENCE_ID_LENGTH),
]
Symbol = Annotated[str, Field(pattern=r"^[A-Z][A-Z0-9.\-]{0,14}$")]


class AdvisoryFallbackReason(str, Enum):
    """Fixed reasons that may distinguish deterministic fallback envelopes."""

    MODEL_DISABLED = "MODEL_DISABLED"
    MODEL_BUDGET_EXHAUSTED = "MODEL_BUDGET_EXHAUSTED"
    MODEL_CONTEXT_LIMIT = "MODEL_CONTEXT_LIMIT"
    MODEL_TRANSPORT_UNAVAILABLE = "MODEL_TRANSPORT_UNAVAILABLE"
    MODEL_OUTPUT_INVALID = "MODEL_OUTPUT_INVALID"
    MODEL_BINDING_INVALID = "MODEL_BINDING_INVALID"


class ObservedFact(BaseModel):
    """One supplied fact and the exact evidence identities that support it."""

    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    statement: ShortText
    status: Literal["OBSERVED", "UNCERTAIN", "CONFLICTED"]
    evidence_ids: Annotated[
        tuple[EvidenceId, ...],
        Field(min_length=1, max_length=MAXIMUM_SLICE_ITEMS),
    ]

    @field_validator("statement")
    @classmethod
    def _statement_is_display_safe(cls, value: str) -> str:
        if not value.strip() or any(ord(character) < 32 for character in value):
            raise ValueError("statement must contain safe display text")
        return value

    @field_validator("evidence_ids")
    @classmethod
    def _evidence_ids_are_unique(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(value) != len(set(value)):
            raise ValueError("evidence_ids must be unique")
        return value


class AdvisorySlice(BaseModel):
    """One descriptive advisory slice with no production authority fields."""

    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    status: Literal[
        "SUPPORTS",
        "CONTRADICTS",
        "NEUTRAL",
        "UNCERTAIN",
        "UNAVAILABLE",
    ]
    direction: Literal["BULLISH", "BEARISH", "NEUTRAL", "MIXED", "UNCERTAIN"]
    summary: ShortText
    evidence_ids: Annotated[
        tuple[EvidenceId, ...],
        Field(max_length=MAXIMUM_SLICE_ITEMS),
    ] = ()

    @field_validator("summary")
    @classmethod
    def _summary_is_display_safe(cls, value: str) -> str:
        if not value.strip() or any(ord(character) < 32 for character in value):
            raise ValueError("summary must contain safe display text")
        return value

    @field_validator("evidence_ids")
    @classmethod
    def _evidence_ids_are_unique(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(value) != len(set(value)):
            raise ValueError("evidence_ids must be unique")
        return value


class ModelAdvisoryPayload(BaseModel):
    """Provider-owned descriptive fields; authority switches are impossible."""

    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    schema_version: Literal["options_copilot.phase2_advisory.v1"]
    symbol: Symbol
    consensus_state: Literal[
        "BEAT",
        "MISS",
        "IN_LINE",
        "UNCERTAIN",
        "NOT_APPLICABLE",
    ]
    event_news_facts: Annotated[
        tuple[ObservedFact, ...],
        Field(min_length=1, max_length=MAXIMUM_SLICE_ITEMS),
    ]
    fundamental_support: AdvisorySlice
    expected_price_impact: AdvisorySlice
    options_volatility_impact: AdvisorySlice
    counter_evidence: Annotated[
        tuple[ShortText, ...],
        Field(max_length=MAXIMUM_SLICE_ITEMS),
    ]

    @field_validator("counter_evidence")
    @classmethod
    def _counter_evidence_is_display_safe(
        cls,
        value: tuple[str, ...],
    ) -> tuple[str, ...]:
        if len(value) != len(set(value)):
            raise ValueError("counter_evidence must be unique")
        if any(
            not item.strip() or any(ord(character) < 32 for character in item)
            for item in value
        ):
            raise ValueError("counter_evidence must contain safe display text")
        return value


class NormalizedAdvisory(BaseModel):
    """Server-owned public envelope for MODEL and FALLBACK projections."""

    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    model_state: Literal["MODEL", "FALLBACK"]
    fallback_reason: AdvisoryFallbackReason | None
    as_of: datetime
    provenance_ids: Annotated[
        tuple[EvidenceId, ...],
        Field(min_length=1, max_length=MAXIMUM_PROVENANCE_IDS),
    ]
    decision_authority: Literal["SUPPORTING_ONLY"] = "SUPPORTING_ONLY"
    approval_eligible: Literal[False] = False
    instruction_creation_allowed: Literal[False] = False
    order_allowed: Literal[False] = False
    payload: ModelAdvisoryPayload

    @field_validator("as_of")
    @classmethod
    def _as_of_is_aware(cls, value: datetime) -> datetime:
        return utc_datetime(value, field="as_of")

    @field_validator("provenance_ids")
    @classmethod
    def _provenance_is_canonical(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if value != tuple(sorted(set(value))):
            raise ValueError("provenance_ids must be unique and canonically sorted")
        return value

    @model_validator(mode="after")
    def _state_matches_reason(self) -> NormalizedAdvisory:
        if self.model_state == "MODEL" and self.fallback_reason is not None:
            raise ValueError("MODEL advisory cannot carry a fallback reason")
        if self.model_state == "FALLBACK" and self.fallback_reason is None:
            raise ValueError("FALLBACK advisory requires a fallback reason")
        return self


def build_fallback_advisory(
    *,
    symbol: str,
    reason: AdvisoryFallbackReason | str,
    as_of: datetime,
    provenance_ids: Sequence[str],
    observations: Sequence[ObservedFact],
) -> NormalizedAdvisory:
    """Build one canonical fallback entirely from supplied observations."""

    fallback_reason = AdvisoryFallbackReason(reason)
    canonical_provenance = tuple(sorted(set(provenance_ids)))
    supplied_observations = tuple(observations)
    slice_evidence_ids = canonical_provenance[:MAXIMUM_SLICE_ITEMS]
    unavailable_summary = (
        "Model advisory is unavailable; supplied observations remain "
        "supporting-only and uncertain."
    )
    unavailable_slice = AdvisorySlice(
        status="UNAVAILABLE",
        direction="UNCERTAIN",
        summary=unavailable_summary,
        evidence_ids=slice_evidence_ids,
    )
    payload = ModelAdvisoryPayload(
        schema_version=ADVISORY_SCHEMA_VERSION,
        symbol=symbol,
        consensus_state="UNCERTAIN",
        event_news_facts=supplied_observations,
        fundamental_support=unavailable_slice,
        expected_price_impact=unavailable_slice,
        options_volatility_impact=unavailable_slice,
        counter_evidence=tuple(
            observation.statement for observation in supplied_observations
        )[:MAXIMUM_SLICE_ITEMS],
    )
    return NormalizedAdvisory(
        model_state="FALLBACK",
        fallback_reason=fallback_reason,
        as_of=as_of,
        provenance_ids=canonical_provenance,
        payload=payload,
    )


__all__ = [
    "ADVISORY_SCHEMA_VERSION",
    "AdvisoryFallbackReason",
    "AdvisorySlice",
    "ModelAdvisoryPayload",
    "NormalizedAdvisory",
    "ObservedFact",
    "build_fallback_advisory",
]
