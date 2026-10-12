"""Immutable contracts for the multi-strategy option research pool."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Mapping

from options_copilot.equity_pool.reference import normalize_equity_pool_reference
from options_copilot.storage.canonical import canonical_hash, freeze_json, thaw_json, utc_datetime
from options_copilot.strategies import StrategyKind


V1_SCHEMA = "options_copilot.option_structure_pool.v1"
V2_SCHEMA = "options_copilot.option_structure_pool.v2"
MAX_QUOTE_CLOCK_SKEW_SECONDS = Decimal("0.5")


class ThesisClass(str, Enum):
    DIRECTIONAL_BULLISH = "DIRECTIONAL_BULLISH"
    DIRECTIONAL_BEARISH = "DIRECTIONAL_BEARISH"
    RANGE_BOUND = "RANGE_BOUND"
    EVENT_LONG_VOLATILITY = "EVENT_LONG_VOLATILITY"
    UNCERTAIN = "UNCERTAIN"


class StructureDisposition(str, Enum):
    EXACT_EVIDENCE_CAPTURED = "EXACT_EVIDENCE_CAPTURED"
    RESEARCH_ONLY = "RESEARCH_ONLY"
    EXCLUDED = "EXCLUDED"


@dataclass(frozen=True, slots=True)
class OptionStructureDecision:
    underlying: str
    thesis_class: ThesisClass
    structure: StrategyKind
    disposition: StructureDisposition
    reason_codes: tuple[str, ...]
    thesis_observed_at: datetime | None = None
    equity_pool_reference: Mapping[str, object] | None = None
    equity_thesis_evidence: Mapping[str, object] | None = None
    candidate_identity: str | None = None
    candidate_id: str | None = None
    candidate_hash: str | None = None
    exact_economics: Mapping[str, object] | None = None
    schema: str = V2_SCHEMA

    def __post_init__(self) -> None:
        symbol = self.underlying.strip().upper() if isinstance(self.underlying, str) else ""
        if not symbol:
            raise ValueError("underlying is required")
        object.__setattr__(self, "underlying", symbol)
        if not isinstance(self.thesis_class, ThesisClass):
            object.__setattr__(self, "thesis_class", ThesisClass(str(self.thesis_class)))
        if not isinstance(self.structure, StrategyKind):
            object.__setattr__(self, "structure", StrategyKind(str(self.structure)))
        if not isinstance(self.disposition, StructureDisposition):
            object.__setattr__(self, "disposition", StructureDisposition(str(self.disposition)))
        if self.schema not in {V1_SCHEMA, V2_SCHEMA}:
            raise ValueError("option structure decision schema is invalid")
        reasons = tuple(dict.fromkeys(str(item).strip().upper() for item in self.reason_codes if str(item).strip()))
        if not reasons:
            raise ValueError("reason_codes cannot be empty")
        object.__setattr__(self, "reason_codes", reasons)

        if self.schema == V1_SCHEMA:
            self._validate_legacy_evidence()
            return

        thesis_at = utc_datetime(self.thesis_observed_at, field="thesis_observed_at")
        reference = normalize_equity_pool_reference(self.equity_pool_reference)
        if reference is None:
            raise ValueError("v2 option decision requires equity_pool_reference")
        selected = tuple(str(item).strip().upper() for item in reference["selected_symbols"])
        discovered = tuple(
            str(item).strip().upper()
            for item in reference.get("discovered_symbols", selected)
        )
        allowed = (
            selected
            if self.disposition is StructureDisposition.EXACT_EVIDENCE_CAPTURED
            else discovered
        )
        if symbol not in allowed:
            raise ValueError("option decision underlying is absent from equity pool")
        object.__setattr__(self, "thesis_observed_at", thesis_at)
        object.__setattr__(self, "equity_pool_reference", reference)
        thesis_evidence = normalize_equity_thesis_row(
            self.equity_thesis_evidence,
            expected_symbol=symbol,
        )
        if thesis_evidence is None:
            if self.thesis_class is not ThesisClass.UNCERTAIN:
                raise ValueError("missing equity thesis requires UNCERTAIN thesis_class")
            if self.disposition is StructureDisposition.EXACT_EVIDENCE_CAPTURED:
                raise ValueError("exact structure requires equity thesis evidence")
        else:
            expected_class = thesis_class_for_direction(
                str(thesis_evidence["direction_label"])
            )
            if self.thesis_class is not expected_class:
                raise ValueError("equity thesis direction does not match thesis_class")
            evidence_at = datetime.fromisoformat(str(thesis_evidence["observed_at"]))
            if thesis_at != evidence_at:
                raise ValueError("thesis_observed_at does not match equity thesis")
        object.__setattr__(self, "equity_thesis_evidence", thesis_evidence)

        carries_candidate = any(
            value is not None
            for value in (
                self.candidate_identity,
                self.candidate_id,
                self.candidate_hash,
                self.exact_economics,
            )
        )
        if carries_candidate:
            if self.disposition is StructureDisposition.EXCLUDED:
                raise ValueError("excluded structure cannot carry candidate evidence")
            if not isinstance(self.candidate_id, str) or not self.candidate_id.strip():
                raise ValueError("candidate evidence requires candidate_id")
            _hash(self.candidate_hash, "candidate_hash")
            _hash(self.candidate_identity, "candidate_identity")
            if not isinstance(self.exact_economics, Mapping):
                raise ValueError("candidate evidence requires exact_economics")
            frozen = freeze_json(self.exact_economics)
            assert isinstance(frozen, Mapping)
            if canonical_hash(frozen) != self.candidate_hash:
                raise ValueError("captured economics do not match candidate_hash")
            expected_identity = option_candidate_identity(frozen)
            if expected_identity != self.candidate_identity:
                raise ValueError("candidate identity does not match exact contracts")
            object.__setattr__(self, "candidate_id", self.candidate_id.strip())
            object.__setattr__(self, "exact_economics", frozen)
        elif self.disposition is StructureDisposition.EXACT_EVIDENCE_CAPTURED:
            raise ValueError("captured structure requires candidate evidence")

    def _validate_legacy_evidence(self) -> None:
        if self.disposition is StructureDisposition.EXACT_EVIDENCE_CAPTURED:
            if not isinstance(self.candidate_id, str) or not self.candidate_id.strip():
                raise ValueError("captured structure requires candidate_id")
            _hash(self.candidate_hash, "candidate_hash")
            if not isinstance(self.exact_economics, Mapping):
                raise ValueError("captured structure requires exact_economics")
            frozen = freeze_json(self.exact_economics)
            assert isinstance(frozen, Mapping)
            if canonical_hash(frozen) != self.candidate_hash:
                raise ValueError("captured economics do not match candidate_hash")
            object.__setattr__(self, "exact_economics", frozen)
        elif any(value is not None for value in (self.candidate_id, self.candidate_hash, self.exact_economics)):
            raise ValueError("legacy non-captured structure cannot carry candidate evidence")

    @property
    def decision_hash(self) -> str:
        return canonical_hash(self.as_dict())

    @property
    def identity_key(self) -> tuple[str, str, str]:
        identity = self.candidate_identity or "TEMPLATE_DISPOSITION"
        return self.underlying, self.structure.value, identity

    def as_dict(self) -> dict[str, object]:
        payload = {
            "underlying": self.underlying,
            "thesis_class": self.thesis_class.value,
            "structure": self.structure.value,
            "disposition": self.disposition.value,
            "reason_codes": self.reason_codes,
            "candidate_id": self.candidate_id,
            "candidate_hash": self.candidate_hash,
            "exact_economics": self.exact_economics,
            "entry_eligible": False,
            "approval_eligible": False,
            "instruction_creation_allowed": False,
            "order_allowed": False,
        }
        if self.schema == V2_SCHEMA:
            payload.update({
                "thesis_observed_at": self.thesis_observed_at,
                "equity_pool_reference": self.equity_pool_reference,
                "equity_pool_reference_hash": canonical_hash(self.equity_pool_reference),
                "equity_thesis_evidence": self.equity_thesis_evidence,
                "equity_thesis_hash": (
                    None
                    if self.equity_thesis_evidence is None
                    else canonical_hash(self.equity_thesis_evidence)
                ),
                "candidate_identity": self.candidate_identity,
            })
        return payload

    def read_projection(self, *, now: datetime, quote_freshness_seconds: int) -> dict[str, object]:
        payload = thaw_json(freeze_json(self.as_dict()))
        assert isinstance(payload, dict)
        if self.schema != V2_SCHEMA or self.candidate_identity is None:
            return payload
        economics = self.exact_economics
        assert isinstance(economics, Mapping)
        age = candidate_quote_age_seconds(economics, now=now)
        payload["quote_age_seconds"] = None if age is None else str(age)
        if age is None or age < 0 or age > Decimal(quote_freshness_seconds):
            if self.disposition is StructureDisposition.EXACT_EVIDENCE_CAPTURED:
                payload["disposition"] = StructureDisposition.RESEARCH_ONLY.value
            payload["reason_codes"] = tuple(dict.fromkeys((*self.reason_codes, "EXECUTABLE_QUOTES_STALE")))
            payload["freshness_degraded"] = True
        else:
            payload["freshness_degraded"] = False
        return payload


@dataclass(frozen=True, slots=True)
class OptionStructurePoolSnapshot:
    scan_run_id: str
    observed_at: datetime
    decisions: tuple[OptionStructureDecision, ...]
    generation_reason_codes: tuple[str, ...] = ()
    schema: str = V2_SCHEMA

    def __post_init__(self) -> None:
        run_id = self.scan_run_id.strip() if isinstance(self.scan_run_id, str) else ""
        if not run_id:
            raise ValueError("scan_run_id is required")
        if self.schema not in {V1_SCHEMA, V2_SCHEMA}:
            raise ValueError("option structure pool schema is invalid")
        object.__setattr__(self, "scan_run_id", run_id)
        object.__setattr__(self, "observed_at", utc_datetime(self.observed_at, field="option pool observed_at"))
        decisions = tuple(self.decisions)
        if not all(isinstance(item, OptionStructureDecision) for item in decisions):
            raise TypeError("decisions must contain OptionStructureDecision values")
        if any(item.schema != self.schema for item in decisions):
            raise ValueError("decision schema does not match snapshot schema")
        identities = tuple(item.identity_key for item in decisions)
        if len(set(identities)) != len(identities):
            raise ValueError("duplicate option pool decision")
        object.__setattr__(self, "decisions", decisions)
        object.__setattr__(self, "generation_reason_codes", tuple(dict.fromkeys(str(item).strip().upper() for item in self.generation_reason_codes if str(item).strip())))

    @property
    def snapshot_hash(self) -> str:
        return canonical_hash({
            "schema": self.schema,
            "scan_run_id": self.scan_run_id,
            "observed_at": self.observed_at,
            "decision_hashes": tuple(item.decision_hash for item in self.decisions),
            "generation_reason_codes": self.generation_reason_codes,
        })

    def as_dict(self) -> dict[str, object]:
        exact = tuple(item for item in self.decisions if item.disposition is StructureDisposition.EXACT_EVIDENCE_CAPTURED)
        research = tuple(item for item in self.decisions if item.disposition is StructureDisposition.RESEARCH_ONLY)
        excluded = tuple(item for item in self.decisions if item.disposition is StructureDisposition.EXCLUDED)
        return {
            "schema": self.schema,
            "scan_run_id": self.scan_run_id,
            "observed_at": self.observed_at,
            "snapshot_hash": self.snapshot_hash,
            "decisions": tuple(item.as_dict() for item in self.decisions),
            "exact_count": len(exact),
            "research_only_count": len(research),
            "excluded_count": len(excluded),
            "generation_reason_codes": self.generation_reason_codes,
            "decision_authority": "SUPPORTING_ONLY",
            "entry_authority": False,
            "approval_eligible": False,
            "instruction_creation_allowed": False,
            "order_allowed": False,
        }

    def read_projection(self, *, now: datetime, quote_freshness_seconds: int) -> dict[str, object]:
        decisions = tuple(
            item.read_projection(now=now, quote_freshness_seconds=quote_freshness_seconds)
            for item in self.decisions
        )
        payload = thaw_json(freeze_json(self.as_dict()))
        assert isinstance(payload, dict)
        payload["decisions"] = decisions
        payload["exact_count"] = sum(item["disposition"] == StructureDisposition.EXACT_EVIDENCE_CAPTURED.value for item in decisions)
        payload["research_only_count"] = sum(item["disposition"] == StructureDisposition.RESEARCH_ONLY.value for item in decisions)
        payload["excluded_count"] = sum(item["disposition"] == StructureDisposition.EXCLUDED.value for item in decisions)
        return payload


def option_candidate_identity(payload: Mapping[str, object]) -> str:
    legs = payload.get("legs")
    if not isinstance(legs, (tuple, list)) or not legs:
        raise ValueError("candidate identity requires exact legs")
    identity_legs: list[dict[str, object]] = []
    for leg in legs:
        if not isinstance(leg, Mapping):
            raise ValueError("candidate identity leg is invalid")
        identity_legs.append({
            "con_id": leg.get("con_id"),
            "contract_id_ex": leg.get("contract_id_ex"),
            "expiration": leg.get("expiration"),
            "strike": leg.get("strike"),
            "right": leg.get("right"),
            "side": leg.get("side"),
            "ratio": leg.get("ratio"),
            "multiplier": leg.get("multiplier"),
            "exchange": leg.get("exchange"),
        })
    return canonical_hash({
        "symbol": str(payload.get("symbol", "")).strip().upper(),
        "structure": str(payload.get("structure", "")),
        "legs": identity_legs,
    })


def normalize_equity_thesis_row(
    value: object,
    *,
    expected_symbol: str,
) -> Mapping[str, object] | None:
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise TypeError("equity thesis evidence must be a mapping")
    frozen = freeze_json(value)
    if not isinstance(frozen, Mapping):
        raise TypeError("equity thesis evidence must be canonical")
    symbol = str(frozen.get("symbol", "")).strip().upper()
    if symbol != expected_symbol.strip().upper():
        raise ValueError("equity thesis symbol mismatch")
    label = str(frozen.get("direction_label", "")).strip().upper()
    if label not in {"BULLISH", "BEARISH", "NEUTRAL", "MIXED", "UNCERTAIN"}:
        raise ValueError("equity thesis direction_label is invalid")
    direction = _bounded_decimal(
        frozen.get("direction_score"),
        field="equity thesis direction_score",
        minimum=Decimal("-100"),
        maximum=Decimal("100"),
    )
    uncertainty = _bounded_decimal(
        frozen.get("uncertainty"),
        field="equity thesis uncertainty",
        minimum=Decimal("0"),
        maximum=Decimal("1"),
    )
    observed = datetime.fromisoformat(str(frozen.get("observed_at", "")))
    observed = utc_datetime(observed, field="equity thesis observed_at")
    source_hashes = frozen.get("source_hashes")
    if not isinstance(source_hashes, tuple) or not source_hashes:
        raise ValueError("equity thesis source_hashes are invalid")
    for digest in source_hashes:
        _hash(digest, "equity thesis source hash")
    canonical_input_hash = _hash(
        frozen.get("canonical_input_hash"),
        "equity thesis canonical_input_hash",
    )
    selected_rank = frozen.get("selected_rank")
    if isinstance(selected_rank, bool) or not isinstance(selected_rank, int) or selected_rank <= 0:
        raise ValueError("equity thesis selected_rank is invalid")
    normalized = freeze_json({
        "schema": "options_copilot.equity_thesis_evidence.v1",
        "symbol": symbol,
        "direction_label": label,
        "direction_score": direction,
        "uncertainty": uncertainty,
        "observed_at": observed,
        "source_hashes": source_hashes,
        "canonical_input_hash": canonical_input_hash,
        "selected_rank": selected_rank,
    })
    assert isinstance(normalized, Mapping)
    supplied_hash = frozen.get("thesis_hash")
    if supplied_hash is not None and _hash(supplied_hash, "equity thesis hash") != canonical_hash(normalized):
        raise ValueError("equity thesis hash mismatch")
    return normalized


def thesis_class_for_direction(direction_label: str) -> ThesisClass:
    return {
        "BULLISH": ThesisClass.DIRECTIONAL_BULLISH,
        "BEARISH": ThesisClass.DIRECTIONAL_BEARISH,
        "NEUTRAL": ThesisClass.RANGE_BOUND,
    }.get(str(direction_label).strip().upper(), ThesisClass.UNCERTAIN)


def normalize_equity_theses(
    value: object,
    *,
    equity_pool_reference: Mapping[str, object],
) -> Mapping[str, Mapping[str, object]]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise TypeError("equity_theses must be a mapping")
    rows = value.get("rows")
    if value.get("schema") != "options_copilot.equity_theses.v1" or not isinstance(rows, (tuple, list)):
        raise ValueError("equity_theses schema is invalid")
    selected_symbols = tuple(
        str(item).strip().upper()
        for item in equity_pool_reference["selected_symbols"]
    )
    if value.get("equity_pool_reference_hash") != canonical_hash(equity_pool_reference):
        raise ValueError("equity_theses reference hash mismatch")
    normalized: dict[str, Mapping[str, object]] = {}
    for raw in rows:
        if not isinstance(raw, Mapping):
            raise ValueError("equity_theses row is invalid")
        symbol = str(raw.get("symbol", "")).strip().upper()
        row = normalize_equity_thesis_row(raw, expected_symbol=symbol)
        assert row is not None
        if symbol in normalized:
            raise ValueError("duplicate equity thesis symbol")
        normalized[symbol] = row
    if any(symbol not in selected_symbols for symbol in normalized):
        raise ValueError("equity thesis is absent from selected equity pool")
    supplied_hash = value.get("rows_hash")
    expected_hash = canonical_hash(tuple(normalized[symbol] for symbol in sorted(normalized)))
    if supplied_hash != expected_hash:
        raise ValueError("equity_theses rows_hash mismatch")
    return freeze_json(normalized)


def _bounded_decimal(
    value: object,
    *,
    field: str,
    minimum: Decimal,
    maximum: Decimal,
) -> Decimal:
    if value is None or isinstance(value, bool):
        raise ValueError(f"{field} is invalid")
    try:
        parsed = value if isinstance(value, Decimal) else Decimal(str(value))
    except (ArithmeticError, ValueError) as exc:
        raise ValueError(f"{field} is invalid") from exc
    if not parsed.is_finite() or parsed < minimum or parsed > maximum:
        raise ValueError(f"{field} is invalid")
    return parsed


def candidate_quote_age_seconds(payload: Mapping[str, object], *, now: datetime) -> Decimal | None:
    current = utc_datetime(now, field="quote freshness now")
    observed: list[datetime] = []
    legs = payload.get("legs")
    if not isinstance(legs, (tuple, list)) or not legs:
        return None
    for leg in legs:
        if not isinstance(leg, Mapping):
            return None
        value = leg.get(
            "exchange_timestamp",
            leg.get("exchange_time", leg.get("observed_at")),
        )
        if not isinstance(value, str):
            return None
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            return None
        observed.append(parsed.astimezone(timezone.utc))
    newest = max(observed)
    newest_age = Decimal(str((current - newest).total_seconds()))
    if newest_age < -MAX_QUOTE_CLOCK_SKEW_SECONDS:
        return None
    oldest = min(observed)
    # Preserve a signed age. A tolerated small negative clock skew must never
    # be clamped to zero and misrepresented as a fresh executable quote.
    return Decimal(str((current - oldest).total_seconds()))


def _hash(value: object, field: str) -> str:
    if not isinstance(value, str) or len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
        raise ValueError(f"{field} is invalid")
    return value


__all__ = [
    "OptionStructureDecision",
    "OptionStructurePoolSnapshot",
    "StructureDisposition",
    "ThesisClass",
    "V1_SCHEMA",
    "V2_SCHEMA",
    "MAX_QUOTE_CLOCK_SKEW_SECONDS",
    "candidate_quote_age_seconds",
    "normalize_equity_thesis_row",
    "normalize_equity_theses",
    "option_candidate_identity",
    "thesis_class_for_direction",
]
