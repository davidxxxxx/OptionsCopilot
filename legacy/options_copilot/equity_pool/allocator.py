"""Bounded, no-filler allocator for the G035 research equity pool."""

from __future__ import annotations

from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from options_copilot.storage.canonical import canonical_hash, utc_datetime

from .models import (
    POLICY_VERSION,
    TAXONOMY_HASH,
    TAXONOMY_VERSION,
    DirectionLabel,
    EquityCategory,
    EquityPoolInput,
    EquityPoolSnapshot,
    EquityScore,
    FactorStatus,
    PoolDecision,
    PoolDisposition,
    PositionMode,
)
from .scoring import SCORING_HASH, SCORING_VERSION, score_equity


@dataclass(frozen=True, slots=True)
class EquityPoolPolicy:
    version: str = POLICY_VERSION
    discovery_limit: int = 150
    deep_scan_limit: int = 30
    concentration_group_cap: int = 5
    mega_cap_tech_cap: int = 3
    unclassified_cap: int = 2

    def __post_init__(self) -> None:
        for field in (
            "discovery_limit",
            "deep_scan_limit",
            "concentration_group_cap",
            "mega_cap_tech_cap",
            "unclassified_cap",
        ):
            value = getattr(self, field)
            if isinstance(value, bool) or not isinstance(value, int):
                raise TypeError(f"{field} must be an integer")
            if value <= 0:
                raise ValueError(f"{field} must be positive")
        if self.discovery_limit > 150:
            raise ValueError("discovery_limit cannot exceed 150")
        if self.deep_scan_limit > 30 or self.deep_scan_limit > self.discovery_limit:
            raise ValueError("deep_scan_limit cannot exceed 30 or discovery_limit")

    @property
    def policy_hash(self) -> str:
        return canonical_hash(
            {
                "version": self.version,
                "discovery_limit": self.discovery_limit,
                "deep_scan_limit": self.deep_scan_limit,
                "concentration_group_cap": self.concentration_group_cap,
                "mega_cap_tech_cap": self.mega_cap_tech_cap,
                "unclassified_cap": self.unclassified_cap,
                "scoring_version": SCORING_VERSION,
                "scoring_hash": SCORING_HASH,
                "taxonomy_version": TAXONOMY_VERSION,
                "taxonomy_hash": TAXONOMY_HASH,
                "authority": "RESEARCH_ONLY",
            }
        )


DEFAULT_POLICY = EquityPoolPolicy()


class EquityPoolAllocator:
    def __init__(self, policy: EquityPoolPolicy = DEFAULT_POLICY) -> None:
        if not isinstance(policy, EquityPoolPolicy):
            raise TypeError("policy must be EquityPoolPolicy")
        self.policy = policy

    def allocate(
        self,
        inputs: Sequence[EquityPoolInput],
        *,
        slot: datetime,
        position_mode: PositionMode = PositionMode.CLEAR,
    ) -> EquityPoolSnapshot:
        frozen_slot = utc_datetime(slot, field="slot")
        if not isinstance(position_mode, PositionMode):
            position_mode = PositionMode(str(position_mode))
        raw_inputs = tuple(inputs)
        if any(not isinstance(item, EquityPoolInput) for item in raw_inputs):
            raise TypeError("inputs must contain EquityPoolInput values")

        ordered_discovery = sorted(
            raw_inputs,
            key=lambda item: (
                item.discovery_rank,
                item.symbol,
                item.canonical_hash,
            ),
        )
        unique: list[EquityPoolInput] = []
        excluded: list[PoolDecision] = []
        seen: set[str] = set()
        for item in ordered_discovery:
            score = score_equity(item, as_of=frozen_slot)
            if item.symbol in seen:
                continue
            seen.add(item.symbol)
            if len(unique) >= self.policy.discovery_limit:
                excluded.append(
                    _decision(item, score, "DISCOVERY_LIMIT")
                )
                continue
            unique.append(item)

        eligible: list[tuple[EquityPoolInput, EquityScore]] = []
        for item in unique:
            score = score_equity(item, as_of=frozen_slot)
            reason = _ineligibility_reason(item, score, slot=frozen_slot)
            if reason is None:
                eligible.append((item, score))
            else:
                excluded.append(_decision(item, score, reason))

        eligible.sort(
            key=lambda pair: (
                0 if pair[1].opportunity_score is not None else 1,
                -(pair[1].opportunity_score or Decimal("0")),
                -abs(pair[1].direction_score),
                -(pair[1].liquidity_score or Decimal("0")),
                pair[0].discovery_rank,
                pair[0].symbol,
                pair[0].canonical_hash,
            )
        )

        selected: list[PoolDecision] = []
        group_counts: Counter[str] = Counter()
        mega_cap_tech_count = 0
        unclassified_count = 0
        for item, score in eligible:
            group = item.classification.concentration_group
            if len(selected) >= self.policy.deep_scan_limit:
                excluded.append(_decision(item, score, "DEEP_SCAN_LIMIT"))
                continue
            if (
                item.classification.category is EquityCategory.UNCLASSIFIED
                and unclassified_count >= self.policy.unclassified_cap
            ):
                excluded.append(_decision(item, score, "UNCLASSIFIED_CAP"))
                continue
            if (
                item.classification.mega_cap_tech
                and mega_cap_tech_count >= self.policy.mega_cap_tech_cap
            ):
                excluded.append(_decision(item, score, "MEGA_CAP_TECH_CAP"))
                continue
            if group_counts[group] >= self.policy.concentration_group_cap:
                excluded.append(_decision(item, score, "CONCENTRATION_GROUP_CAP"))
                continue
            rank = len(selected) + 1
            selected.append(
                PoolDecision(
                    symbol=item.symbol,
                    disposition=PoolDisposition.SELECTED,
                    score=score,
                    classification=item.classification,
                    reasons=(
                        "QUALIFIED_FOR_DEEP_SCAN"
                        if score.opportunity_score is not None
                        else "RETAINED_UNCERTAIN_RESEARCH"
                    ,),
                    canonical_input_hash=item.canonical_hash,
                    selected_rank=rank,
                )
            )
            group_counts[group] += 1
            if item.classification.mega_cap_tech:
                mega_cap_tech_count += 1
            if item.classification.category is EquityCategory.UNCLASSIFIED:
                unclassified_count += 1

        excluded.sort(
            key=lambda item: (
                item.symbol,
                item.reasons,
                item.canonical_input_hash,
            )
        )
        normalized_hashes = tuple(
            sorted(item.canonical_hash for item in raw_inputs)
        )
        pool_id = canonical_hash(
            {
                "slot": frozen_slot,
                "input_hashes": normalized_hashes,
                "policy_hash": self.policy.policy_hash,
                "position_mode": position_mode.value,
            }
        )
        concentration_counts = dict(sorted(group_counts.items()))
        concentration_counts["THEME:MEGA_CAP_TECH"] = mega_cap_tech_count
        concentration_counts["UNCLASSIFIED"] = unclassified_count
        return EquityPoolSnapshot(
            pool_id=pool_id,
            slot=frozen_slot,
            generated_at=frozen_slot,
            policy_version=self.policy.version,
            policy_hash=self.policy.policy_hash,
            taxonomy_version=TAXONOMY_VERSION,
            taxonomy_hash=TAXONOMY_HASH,
            position_mode=position_mode,
            discovery_count=len(seen),
            considered_count=len(unique),
            selected=tuple(selected),
            excluded=tuple(excluded),
            concentration_counts=concentration_counts,
        )


def _ineligibility_reason(
    item: EquityPoolInput,
    score: EquityScore,
    *,
    slot: datetime,
) -> str | None:
    if item.discovery_source == "MISSING_PROVENANCE":
        return "DISCOVERY_PROVENANCE_UNAVAILABLE"
    if item.classification.taxonomy_version != TAXONOMY_VERSION or item.classification.taxonomy_hash != TAXONOMY_HASH:
        return "TAXONOMY_NOT_CURRENT"
    if item.captured_at > slot:
        return "CAPTURED_AT_FUTURE"
    if item.liquidity.status is not FactorStatus.AVAILABLE:
        return f"LIQUIDITY_{item.liquidity.status.value}"
    if item.liquidity.observed_at > slot:
        return "LIQUIDITY_FUTURE"
    if score.liquidity_score is None:
        return "LIQUIDITY_STALE"
    if score.direction_label is DirectionLabel.MIXED:
        return "MIXED_DIRECTION_EVIDENCE"
    if score.direction_label is DirectionLabel.UNCERTAIN:
        return "INSUFFICIENT_DIRECTION_EVIDENCE"
    if score.opportunity_score is None:
        return "OPPORTUNITY_UNAVAILABLE"
    if score.opportunity_score <= Decimal("0"):
        return "ZERO_OPPORTUNITY"
    return None


def _decision(
    item: EquityPoolInput,
    score: EquityScore,
    reason: str,
) -> PoolDecision:
    return PoolDecision(
        symbol=item.symbol,
        disposition=PoolDisposition.EXCLUDED,
        score=score,
        classification=item.classification,
        reasons=(reason,),
        canonical_input_hash=item.canonical_hash,
    )


__all__ = [
    "DEFAULT_POLICY",
    "EquityPoolAllocator",
    "EquityPoolPolicy",
]
