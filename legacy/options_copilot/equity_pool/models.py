"""Immutable contracts for the durable sector-balanced equity pool."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum
from types import MappingProxyType
from typing import Mapping

from options_copilot.storage.canonical import (
    canonical_hash,
    freeze_json,
    thaw_json,
    utc_datetime,
)

from .taxonomy import DEFAULT_TAXONOMY_HASH, TAXONOMY_VERSION
from .taxonomy import MEGA_CAP_TECH_SYMBOL_VALUES


ZERO = Decimal("0")
ONE = Decimal("1")
HASH_LENGTH = 64


class EquityCategory(str, Enum):
    COMMUNICATION_SERVICES = "COMMUNICATION_SERVICES"
    CONSUMER_DISCRETIONARY = "CONSUMER_DISCRETIONARY"
    CONSUMER_STAPLES = "CONSUMER_STAPLES"
    ENERGY = "ENERGY"
    FINANCIALS = "FINANCIALS"
    HEALTH_CARE = "HEALTH_CARE"
    INDUSTRIALS = "INDUSTRIALS"
    INFORMATION_TECHNOLOGY = "INFORMATION_TECHNOLOGY"
    MATERIALS = "MATERIALS"
    REAL_ESTATE = "REAL_ESTATE"
    UTILITIES = "UTILITIES"
    BROAD_EQUITY_ETF = "BROAD_EQUITY_ETF"
    SECTOR_ETF = "SECTOR_ETF"
    RATES_BOND_ETF = "RATES_BOND_ETF"
    COMMODITY_ETF = "COMMODITY_ETF"
    OTHER_ETF = "OTHER_ETF"
    UNCLASSIFIED = "UNCLASSIFIED"

    @property
    def is_etf_bucket(self) -> bool:
        return self in ETF_BUCKETS


COMPANY_SECTORS = frozenset(
    {
        EquityCategory.COMMUNICATION_SERVICES,
        EquityCategory.CONSUMER_DISCRETIONARY,
        EquityCategory.CONSUMER_STAPLES,
        EquityCategory.ENERGY,
        EquityCategory.FINANCIALS,
        EquityCategory.HEALTH_CARE,
        EquityCategory.INDUSTRIALS,
        EquityCategory.INFORMATION_TECHNOLOGY,
        EquityCategory.MATERIALS,
        EquityCategory.REAL_ESTATE,
        EquityCategory.UTILITIES,
    }
)
ETF_BUCKETS = frozenset(
    {
        EquityCategory.BROAD_EQUITY_ETF,
        EquityCategory.SECTOR_ETF,
        EquityCategory.RATES_BOND_ETF,
        EquityCategory.COMMODITY_ETF,
        EquityCategory.OTHER_ETF,
    }
)


class ClassificationSource(str, Enum):
    IBKR_METADATA = "IBKR_METADATA"
    LOCAL_EXACT = "LOCAL_EXACT"
    UNCLASSIFIED = "UNCLASSIFIED"


class FactorKind(str, Enum):
    NEWS = "NEWS"
    FUNDAMENTALS = "FUNDAMENTALS"
    REGIME = "REGIME"
    TREND_VOLATILITY = "TREND_VOLATILITY"
    POSITIONING = "POSITIONING"


class FactorStatus(str, Enum):
    AVAILABLE = "AVAILABLE"
    MISSING = "MISSING"
    STALE = "STALE"
    CONFLICTED = "CONFLICTED"
    NOT_APPLICABLE = "NOT_APPLICABLE"


class DirectionLabel(str, Enum):
    BULLISH = "BULLISH"
    BEARISH = "BEARISH"
    NEUTRAL = "NEUTRAL"
    MIXED = "MIXED"
    UNCERTAIN = "UNCERTAIN"


class PositionMode(str, Enum):
    CLEAR = "CLEAR"
    BLOCKED_OPEN_POSITION = "BLOCKED_OPEN_POSITION"


class PoolDisposition(str, Enum):
    SELECTED = "SELECTED"
    EXCLUDED = "EXCLUDED"


TAXONOMY_HASH = DEFAULT_TAXONOMY_HASH
POLICY_VERSION = "equity-pool-policy.v1"


def _nonblank(value: object, field: str, *, uppercase: bool = False) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field} must be a string")
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field} cannot be blank")
    return normalized.upper() if uppercase else normalized


def _decimal(
    value: object,
    field: str,
    *,
    minimum: Decimal | None = None,
    maximum: Decimal | None = None,
) -> Decimal:
    if not isinstance(value, Decimal):
        raise TypeError(f"{field} must be a Decimal")
    if not value.is_finite():
        raise ValueError(f"{field} must be finite")
    if minimum is not None and value < minimum:
        raise ValueError(f"{field} must be at least {minimum}")
    if maximum is not None and value > maximum:
        raise ValueError(f"{field} must be at most {maximum}")
    return value


def _hash(value: object, field: str) -> str:
    normalized = _nonblank(value, field).lower()
    if len(normalized) != HASH_LENGTH or any(
        character not in "0123456789abcdef" for character in normalized
    ):
        raise ValueError(f"{field} must be a SHA-256 hex digest")
    return normalized


def _enum(value: object, enum_type: type[Enum], field: str) -> Enum:
    if isinstance(value, enum_type):
        return value
    if isinstance(value, str):
        try:
            return enum_type(value.strip().upper())
        except ValueError as exc:
            raise ValueError(f"invalid {field}: {value!r}") from exc
    raise TypeError(f"{field} must be a {enum_type.__name__}")


def _integer(value: object, field: str, *, minimum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field} must be an integer")
    if minimum is not None and value < minimum:
        raise ValueError(f"{field} must be at least {minimum}")
    return value


@dataclass(frozen=True, slots=True)
class CanonicalClassification:
    symbol: str
    category: EquityCategory
    source: ClassificationSource
    mega_cap_tech: bool = False
    taxonomy_version: str = TAXONOMY_VERSION
    taxonomy_hash: str = TAXONOMY_HASH

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", _nonblank(self.symbol, "symbol", uppercase=True))
        category = _enum(self.category, EquityCategory, "category")
        source = _enum(self.source, ClassificationSource, "source")
        object.__setattr__(self, "category", category)
        object.__setattr__(self, "source", source)
        if not isinstance(self.mega_cap_tech, bool):
            raise TypeError("mega_cap_tech must be a bool")
        object.__setattr__(
            self,
            "mega_cap_tech",
            self.symbol in MEGA_CAP_TECH_SYMBOL_VALUES,
        )
        object.__setattr__(
            self,
            "taxonomy_version",
            _nonblank(self.taxonomy_version, "taxonomy_version"),
        )
        object.__setattr__(self, "taxonomy_hash", _hash(self.taxonomy_hash, "taxonomy_hash"))
        if source is ClassificationSource.UNCLASSIFIED and category is not EquityCategory.UNCLASSIFIED:
            raise ValueError("UNCLASSIFIED source requires UNCLASSIFIED category")
        if category is EquityCategory.UNCLASSIFIED and source is not ClassificationSource.UNCLASSIFIED:
            raise ValueError("UNCLASSIFIED category requires UNCLASSIFIED source")

    @property
    def concentration_group(self) -> str:
        if self.category in COMPANY_SECTORS:
            return f"SECTOR:{self.category.value}"
        if self.category in ETF_BUCKETS:
            return f"ETF:{self.category.value}"
        return EquityCategory.UNCLASSIFIED.value

    def as_dict(self) -> dict[str, object]:
        return {
            "symbol": self.symbol,
            "category": self.category.value,
            "source": self.source.value,
            "mega_cap_tech": self.mega_cap_tech,
            "concentration_group": self.concentration_group,
            "taxonomy_version": self.taxonomy_version,
            "taxonomy_hash": self.taxonomy_hash,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> "CanonicalClassification":
        return cls(
            symbol=str(value["symbol"]),
            category=EquityCategory(str(value["category"])),
            source=ClassificationSource(str(value["source"])),
            mega_cap_tech=value.get("mega_cap_tech", False),  # type: ignore[arg-type]
            taxonomy_version=str(value["taxonomy_version"]),
            taxonomy_hash=str(value["taxonomy_hash"]),
        )


@dataclass(frozen=True, slots=True)
class FactorEvidence:
    factor: FactorKind
    status: FactorStatus
    signed_signal: Decimal | None
    confidence: Decimal | None
    horizon: str
    observed_at: datetime
    effective_at: datetime
    valid_until: datetime | None
    source_hashes: tuple[str, ...]
    reasons: tuple[str, ...]
    payload_hash: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "factor", _enum(self.factor, FactorKind, "factor"))
        status = _enum(self.status, FactorStatus, "status")
        object.__setattr__(self, "status", status)
        if status is FactorStatus.AVAILABLE:
            if self.signed_signal is None or self.confidence is None:
                raise ValueError("AVAILABLE evidence requires signal and confidence")
            _decimal(self.signed_signal, "signed_signal", minimum=-ONE, maximum=ONE)
            _decimal(self.confidence, "confidence", minimum=ZERO, maximum=ONE)
        elif self.signed_signal is not None or self.confidence is not None:
            raise ValueError("unavailable evidence must keep signal and confidence null")
        object.__setattr__(self, "horizon", _nonblank(self.horizon, "horizon"))
        observed = utc_datetime(self.observed_at, field="observed_at")
        effective = utc_datetime(self.effective_at, field="effective_at")
        object.__setattr__(self, "observed_at", observed)
        object.__setattr__(self, "effective_at", effective)
        if effective > observed:
            raise ValueError("effective_at cannot be after observed_at")
        if self.valid_until is not None:
            valid_until = utc_datetime(self.valid_until, field="valid_until")
            if valid_until < observed:
                raise ValueError("valid_until cannot be before observed_at")
            object.__setattr__(self, "valid_until", valid_until)
        hashes = tuple(_hash(item, "source_hash") for item in self.source_hashes)
        if status is FactorStatus.AVAILABLE and not hashes:
            raise ValueError("AVAILABLE evidence requires at least one source hash")
        if len(hashes) != len(set(hashes)):
            raise ValueError("source_hashes must be unique")
        object.__setattr__(self, "source_hashes", hashes)
        reasons = tuple(_nonblank(item, "reason") for item in self.reasons)
        if status is not FactorStatus.AVAILABLE and not reasons:
            raise ValueError("unavailable evidence requires at least one reason")
        object.__setattr__(self, "reasons", reasons)
        object.__setattr__(self, "payload_hash", _hash(self.payload_hash, "payload_hash"))

    def as_dict(self) -> dict[str, object]:
        return {
            "factor": self.factor.value,
            "status": self.status.value,
            "signed_signal": self.signed_signal,
            "confidence": self.confidence,
            "horizon": self.horizon,
            "observed_at": self.observed_at,
            "effective_at": self.effective_at,
            "valid_until": self.valid_until,
            "source_hashes": self.source_hashes,
            "reasons": self.reasons,
            "payload_hash": self.payload_hash,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> "FactorEvidence":
        return cls(
            factor=FactorKind(str(value["factor"])),
            status=FactorStatus(str(value["status"])),
            signed_signal=value.get("signed_signal"),  # type: ignore[arg-type]
            confidence=value.get("confidence"),  # type: ignore[arg-type]
            horizon=str(value["horizon"]),
            observed_at=datetime.fromisoformat(str(value["observed_at"])),
            effective_at=datetime.fromisoformat(str(value["effective_at"])),
            valid_until=(
                datetime.fromisoformat(str(value["valid_until"]))
                if value.get("valid_until") is not None
                else None
            ),
            source_hashes=tuple(str(item) for item in value["source_hashes"]),  # type: ignore[union-attr]
            reasons=tuple(str(item) for item in value["reasons"]),  # type: ignore[union-attr]
            payload_hash=str(value["payload_hash"]),
        )


@dataclass(frozen=True, slots=True)
class LiquidityEvidence:
    status: FactorStatus
    score: Decimal | None
    observed_at: datetime
    source_hashes: tuple[str, ...]
    reasons: tuple[str, ...]
    payload_hash: str

    def __post_init__(self) -> None:
        status = _enum(self.status, FactorStatus, "status")
        object.__setattr__(self, "status", status)
        if status is FactorStatus.AVAILABLE:
            if self.score is None:
                raise ValueError("AVAILABLE liquidity requires a score")
            _decimal(self.score, "score", minimum=ZERO, maximum=Decimal("100"))
        elif self.score is not None:
            raise ValueError("unavailable liquidity must keep score null")
        object.__setattr__(self, "observed_at", utc_datetime(self.observed_at, field="observed_at"))
        hashes = tuple(_hash(item, "source_hash") for item in self.source_hashes)
        if status is FactorStatus.AVAILABLE and not hashes:
            raise ValueError("AVAILABLE liquidity requires at least one source hash")
        object.__setattr__(self, "source_hashes", hashes)
        reasons = tuple(_nonblank(item, "reason") for item in self.reasons)
        if status is not FactorStatus.AVAILABLE and not reasons:
            raise ValueError("unavailable liquidity requires at least one reason")
        object.__setattr__(self, "reasons", reasons)
        object.__setattr__(self, "payload_hash", _hash(self.payload_hash, "payload_hash"))

    def as_dict(self) -> dict[str, object]:
        return {
            "status": self.status.value,
            "score": self.score,
            "observed_at": self.observed_at,
            "source_hashes": self.source_hashes,
            "reasons": self.reasons,
            "payload_hash": self.payload_hash,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> "LiquidityEvidence":
        return cls(
            status=FactorStatus(str(value["status"])),
            score=value.get("score"),  # type: ignore[arg-type]
            observed_at=datetime.fromisoformat(str(value["observed_at"])),
            source_hashes=tuple(str(item) for item in value["source_hashes"]),  # type: ignore[union-attr]
            reasons=tuple(str(item) for item in value["reasons"]),  # type: ignore[union-attr]
            payload_hash=str(value["payload_hash"]),
        )


@dataclass(frozen=True, slots=True)
class EquityPoolInput:
    classification: CanonicalClassification
    factors: tuple[FactorEvidence, ...]
    liquidity: LiquidityEvidence
    discovery_rank: int
    discovery_source: str
    captured_at: datetime
    deepseek_overlay: Mapping[str, object] | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.classification, CanonicalClassification):
            raise TypeError("classification must be CanonicalClassification")
        factors = tuple(self.factors)
        if any(not isinstance(item, FactorEvidence) for item in factors):
            raise TypeError("factors must contain FactorEvidence values")
        kinds = tuple(item.factor for item in factors)
        if len(kinds) != len(set(kinds)):
            raise ValueError("factor kinds must be unique")
        if set(kinds) != set(FactorKind):
            raise ValueError("factors must explicitly cover all canonical factor kinds")
        object.__setattr__(self, "factors", tuple(sorted(factors, key=lambda item: item.factor.value)))
        if not isinstance(self.liquidity, LiquidityEvidence):
            raise TypeError("liquidity must be LiquidityEvidence")
        if isinstance(self.discovery_rank, bool) or not isinstance(self.discovery_rank, int):
            raise TypeError("discovery_rank must be an integer")
        if self.discovery_rank <= 0:
            raise ValueError("discovery_rank must be positive")
        object.__setattr__(self, "discovery_source", _nonblank(self.discovery_source, "discovery_source"))
        object.__setattr__(self, "captured_at", utc_datetime(self.captured_at, field="captured_at"))
        if self.deepseek_overlay is not None:
            frozen = freeze_json(self.deepseek_overlay)
            if not isinstance(frozen, Mapping):
                raise TypeError("deepseek_overlay must be a mapping")
            object.__setattr__(self, "deepseek_overlay", frozen)

    @property
    def symbol(self) -> str:
        return self.classification.symbol

    @property
    def canonical_hash(self) -> str:
        return canonical_hash(self.canonical_body())

    def canonical_body(self) -> dict[str, object]:
        """Return authority-bearing inputs, deliberately excluding DeepSeek shadow."""

        return {
            "classification": self.classification.as_dict(),
            "factors": tuple(item.as_dict() for item in self.factors),
            "liquidity": self.liquidity.as_dict(),
            "discovery_rank": self.discovery_rank,
            "discovery_source": self.discovery_source,
            "captured_at": self.captured_at,
        }

    @classmethod
    def from_canonical_body(cls, value: Mapping[str, object]) -> "EquityPoolInput":
        classification = value["classification"]
        factors = value["factors"]
        liquidity = value["liquidity"]
        if not isinstance(classification, Mapping) or not isinstance(liquidity, Mapping):
            raise TypeError("classification and liquidity must be mappings")
        if not isinstance(factors, (list, tuple)):
            raise TypeError("factors must be a sequence")
        return cls(
            classification=CanonicalClassification.from_dict(classification),
            factors=tuple(
                FactorEvidence.from_dict(item)
                for item in factors
                if isinstance(item, Mapping)
            ),
            liquidity=LiquidityEvidence.from_dict(liquidity),
            discovery_rank=_integer(value["discovery_rank"], "discovery_rank", minimum=1),
            discovery_source=str(value["discovery_source"]),
            captured_at=datetime.fromisoformat(str(value["captured_at"])),
        )


@dataclass(frozen=True, slots=True)
class EquityScore:
    symbol: str
    direction_score: Decimal
    coverage_confidence: Decimal
    positive_evidence_mass: Decimal
    negative_evidence_mass: Decimal
    conflict_penalty: Decimal
    uncertainty: Decimal
    liquidity_score: Decimal | None
    opportunity_score: Decimal | None
    direction_label: DirectionLabel

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", _nonblank(self.symbol, "symbol", uppercase=True))
        for field, minimum, maximum in (
            ("direction_score", Decimal("-100"), Decimal("100")),
            ("coverage_confidence", ZERO, ONE),
            ("positive_evidence_mass", ZERO, ONE),
            ("negative_evidence_mass", ZERO, ONE),
            ("conflict_penalty", ZERO, ONE),
            ("uncertainty", ZERO, ONE),
        ):
            _decimal(getattr(self, field), field, minimum=minimum, maximum=maximum)
        if self.liquidity_score is not None:
            _decimal(self.liquidity_score, "liquidity_score", minimum=ZERO, maximum=Decimal("100"))
        if self.opportunity_score is not None:
            _decimal(self.opportunity_score, "opportunity_score", minimum=ZERO, maximum=Decimal("100"))
        if (self.liquidity_score is None) != (self.opportunity_score is None):
            raise ValueError("liquidity_score and opportunity_score availability must match")
        object.__setattr__(self, "direction_label", _enum(self.direction_label, DirectionLabel, "direction_label"))

    def as_dict(self) -> dict[str, object]:
        return {
            "symbol": self.symbol,
            "direction_score": self.direction_score,
            "coverage_confidence": self.coverage_confidence,
            "positive_evidence_mass": self.positive_evidence_mass,
            "negative_evidence_mass": self.negative_evidence_mass,
            "conflict_penalty": self.conflict_penalty,
            "uncertainty": self.uncertainty,
            "liquidity_score": self.liquidity_score,
            "opportunity_score": self.opportunity_score,
            "direction_label": self.direction_label.value,
        }


@dataclass(frozen=True, slots=True)
class PoolDecision:
    symbol: str
    disposition: PoolDisposition
    score: EquityScore
    classification: CanonicalClassification
    reasons: tuple[str, ...]
    canonical_input_hash: str
    selected_rank: int | None = None
    decision_authority: str = "SUPPORTING_ONLY"
    instruction_creation_allowed: bool = False
    order_allowed: bool = False
    entry_eligible: bool = False

    def __post_init__(self) -> None:
        symbol = _nonblank(self.symbol, "symbol", uppercase=True)
        object.__setattr__(self, "symbol", symbol)
        disposition = _enum(self.disposition, PoolDisposition, "disposition")
        object.__setattr__(self, "disposition", disposition)
        if not isinstance(self.score, EquityScore):
            raise TypeError("score must be EquityScore")
        if not isinstance(self.classification, CanonicalClassification):
            raise TypeError("classification must be CanonicalClassification")
        if symbol != self.score.symbol or symbol != self.classification.symbol:
            raise ValueError("decision symbol must match score and classification")
        reasons = tuple(_nonblank(item, "reason", uppercase=True) for item in self.reasons)
        if not reasons or len(reasons) != len(set(reasons)):
            raise ValueError("decision reasons must be non-empty and unique")
        object.__setattr__(self, "reasons", reasons)
        object.__setattr__(self, "canonical_input_hash", _hash(self.canonical_input_hash, "canonical_input_hash"))
        if disposition is PoolDisposition.SELECTED:
            if isinstance(self.selected_rank, bool) or not isinstance(self.selected_rank, int):
                raise TypeError("SELECTED decision requires an integer selected_rank")
            if self.selected_rank <= 0:
                raise ValueError("selected_rank must be positive")
        elif self.selected_rank is not None:
            raise ValueError("EXCLUDED decision cannot have selected_rank")
        if self.decision_authority != "SUPPORTING_ONLY":
            raise ValueError("decision_authority must be SUPPORTING_ONLY")
        for field in ("instruction_creation_allowed", "order_allowed", "entry_eligible"):
            if not isinstance(getattr(self, field), bool):
                raise TypeError(f"{field} must be a bool")
            if getattr(self, field):
                raise ValueError(f"{field} must remain false")

    def as_dict(self) -> dict[str, object]:
        return {
            "symbol": self.symbol,
            "disposition": self.disposition.value,
            "score": self.score.as_dict(),
            "classification": self.classification.as_dict(),
            "reasons": self.reasons,
            "canonical_input_hash": self.canonical_input_hash,
            "selected_rank": self.selected_rank,
            "decision_authority": self.decision_authority,
            "instruction_creation_allowed": self.instruction_creation_allowed,
            "order_allowed": self.order_allowed,
            "entry_eligible": self.entry_eligible,
        }


@dataclass(frozen=True, slots=True)
class EquityPoolSnapshot:
    pool_id: str
    slot: datetime
    generated_at: datetime
    policy_version: str
    policy_hash: str
    taxonomy_version: str
    taxonomy_hash: str
    position_mode: PositionMode
    discovery_count: int
    considered_count: int
    selected: tuple[PoolDecision, ...]
    excluded: tuple[PoolDecision, ...]
    concentration_counts: Mapping[str, int]
    research_only: bool = True
    entry_authority: bool = False
    approval_eligible: bool = False
    decision_authority: str = "SUPPORTING_ONLY"
    instruction_creation_allowed: bool = False
    order_allowed: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "slot", utc_datetime(self.slot, field="slot"))
        object.__setattr__(self, "generated_at", utc_datetime(self.generated_at, field="generated_at"))
        if self.generated_at != self.slot:
            raise ValueError("generated_at must equal the deterministic pool slot")
        object.__setattr__(self, "policy_hash", _hash(self.policy_hash, "policy_hash"))
        object.__setattr__(self, "taxonomy_hash", _hash(self.taxonomy_hash, "taxonomy_hash"))
        object.__setattr__(self, "pool_id", _hash(self.pool_id, "pool_id"))
        if self.policy_version != POLICY_VERSION:
            raise ValueError(f"policy_version must be {POLICY_VERSION}")
        if self.taxonomy_version != TAXONOMY_VERSION or self.taxonomy_hash != TAXONOMY_HASH:
            raise ValueError("snapshot taxonomy is not current")
        object.__setattr__(self, "position_mode", _enum(self.position_mode, PositionMode, "position_mode"))
        for field in (
            "research_only",
            "entry_authority",
            "approval_eligible",
            "instruction_creation_allowed",
            "order_allowed",
        ):
            if not isinstance(getattr(self, field), bool):
                raise TypeError(f"{field} must be a bool")
        if (
            not self.research_only
            or self.entry_authority
            or self.approval_eligible
            or self.instruction_creation_allowed
            or self.order_allowed
        ):
            raise ValueError("equity pool is permanently research-only")
        if self.decision_authority != "SUPPORTING_ONLY":
            raise ValueError("decision_authority must be SUPPORTING_ONLY")
        for field in ("discovery_count", "considered_count"):
            value = getattr(self, field)
            if isinstance(value, bool) or not isinstance(value, int):
                raise TypeError(f"{field} must be an integer")
            if value < 0:
                raise ValueError(f"{field} cannot be negative")
        if self.considered_count > min(self.discovery_count, 150):
            raise ValueError("considered_count exceeds bounded discovery coverage")
        selected = tuple(self.selected)
        excluded = tuple(self.excluded)
        if any(not isinstance(item, PoolDecision) for item in selected + excluded):
            raise TypeError("snapshot rows must be PoolDecision values")
        if len(selected) > 30:
            raise ValueError("equity pool permits at most 30 selected rows")
        if any(item.disposition is not PoolDisposition.SELECTED for item in selected):
            raise ValueError("selected rows must have SELECTED disposition")
        if any(item.disposition is not PoolDisposition.EXCLUDED for item in excluded):
            raise ValueError("excluded rows must have EXCLUDED disposition")
        ranks = tuple(item.selected_rank for item in selected)
        if ranks != tuple(range(1, len(selected) + 1)):
            raise ValueError("selected ranks must be contiguous and ordered")
        symbols = tuple(item.symbol for item in selected + excluded)
        if len(symbols) != len(set(symbols)):
            raise ValueError("snapshot decisions must cover unique symbols")
        if len(symbols) != self.discovery_count:
            raise ValueError("snapshot row coverage must equal discovery_count")
        object.__setattr__(self, "selected", selected)
        object.__setattr__(self, "excluded", excluded)
        counts = dict(self.concentration_counts)
        if any(isinstance(value, bool) or not isinstance(value, int) or value < 0 for value in counts.values()):
            raise ValueError("concentration counts must be non-negative integers")
        recomputed: dict[str, int] = {}
        for decision in selected:
            group = decision.classification.concentration_group
            recomputed[group] = recomputed.get(group, 0) + 1
        recomputed["THEME:MEGA_CAP_TECH"] = sum(
            decision.classification.mega_cap_tech for decision in selected
        )
        recomputed["UNCLASSIFIED"] = sum(
            decision.classification.category is EquityCategory.UNCLASSIFIED
            for decision in selected
        )
        if counts != dict(sorted(recomputed.items())):
            raise ValueError("concentration_counts do not match selected rows")
        object.__setattr__(self, "concentration_counts", MappingProxyType(counts))

    @property
    def snapshot_hash(self) -> str:
        return canonical_hash(self.as_dict())

    def as_dict(self) -> dict[str, object]:
        return {
            "pool_id": self.pool_id,
            "slot": self.slot,
            "generated_at": self.generated_at,
            "policy_version": self.policy_version,
            "policy_hash": self.policy_hash,
            "taxonomy_version": self.taxonomy_version,
            "taxonomy_hash": self.taxonomy_hash,
            "position_mode": self.position_mode.value,
            "discovery_count": self.discovery_count,
            "considered_count": self.considered_count,
            "selected": tuple(item.as_dict() for item in self.selected),
            "excluded": tuple(item.as_dict() for item in self.excluded),
            "concentration_counts": dict(self.concentration_counts),
            "research_only": self.research_only,
            "entry_authority": self.entry_authority,
            "approval_eligible": self.approval_eligible,
            "decision_authority": self.decision_authority,
            "instruction_creation_allowed": self.instruction_creation_allowed,
            "order_allowed": self.order_allowed,
        }


__all__ = [
    "COMPANY_SECTORS",
    "ETF_BUCKETS",
    "TAXONOMY_HASH",
    "TAXONOMY_VERSION",
    "POLICY_VERSION",
    "CanonicalClassification",
    "ClassificationSource",
    "DirectionLabel",
    "EquityCategory",
    "EquityPoolInput",
    "EquityPoolSnapshot",
    "EquityScore",
    "FactorEvidence",
    "FactorKind",
    "FactorStatus",
    "LiquidityEvidence",
    "PoolDecision",
    "PoolDisposition",
    "PositionMode",
]
