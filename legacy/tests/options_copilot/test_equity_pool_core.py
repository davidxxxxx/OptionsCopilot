"""Focused G035 model, scoring, classification, and allocator tests."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from options_copilot.equity_pool import (
    COMPANY_SECTORS,
    ETF_BUCKETS,
    CanonicalClassification,
    ClassificationSource,
    DirectionLabel,
    EquityCategory,
    EquityPoolAllocator,
    EquityPoolInput,
    FactorEvidence,
    FactorKind,
    FactorStatus,
    LiquidityEvidence,
    PositionMode,
    classify_security,
    score_equity,
)
from options_copilot.equity_pool.taxonomy import (
    COMPANY_ALIAS_VALUES,
    DEFAULT_TAXONOMY_HASH,
    ETF_ALIAS_VALUES,
)
from options_copilot.equity_pool.scoring import SCORING_HASH, SCORING_POLICY_BODY
from options_copilot.storage.canonical import canonical_hash


NOW = datetime(2026, 8, 21, 13, 30, tzinfo=timezone.utc)
H = canonical_hash({"fixture": "equity-pool"})


def _factor(
    factor: FactorKind,
    *,
    signal: Decimal | None = Decimal("1"),
    confidence: Decimal | None = Decimal("1"),
    status: FactorStatus = FactorStatus.AVAILABLE,
) -> FactorEvidence:
    return FactorEvidence(
        factor=factor,
        status=status,
        signed_signal=signal,
        confidence=confidence,
        horizon="5D",
        observed_at=NOW,
        effective_at=NOW - timedelta(minutes=1),
        valid_until=NOW + timedelta(days=1),
        source_hashes=(canonical_hash({"factor": factor.value}),) if status is FactorStatus.AVAILABLE else (),
        reasons=() if status is FactorStatus.AVAILABLE else (f"{status.value}_FIXTURE",),
        payload_hash=canonical_hash({"payload": factor.value, "status": status.value}),
    )


def _input(
    symbol: str,
    *,
    category: EquityCategory = EquityCategory.ENERGY,
    rank: int = 1,
    liquidity: Decimal | None = Decimal("80"),
    mega: bool = False,
    overlay: dict[str, object] | None = None,
    factors: tuple[FactorEvidence, ...] | None = None,
) -> EquityPoolInput:
    if factors is None:
        factors = tuple(_factor(kind) for kind in FactorKind)
    liquidity_status = FactorStatus.AVAILABLE if liquidity is not None else FactorStatus.MISSING
    return EquityPoolInput(
        classification=CanonicalClassification(
            symbol=symbol,
            category=category,
            source=(
                ClassificationSource.UNCLASSIFIED
                if category is EquityCategory.UNCLASSIFIED
                else ClassificationSource.LOCAL_EXACT
            ),
            mega_cap_tech=mega,
        ),
        factors=factors,
        liquidity=LiquidityEvidence(
            status=liquidity_status,
            score=liquidity,
            observed_at=NOW,
            source_hashes=(H,) if liquidity is not None else (),
            reasons=() if liquidity is not None else ("LIQUIDITY_NOT_CAPTURED",),
            payload_hash=canonical_hash({"liquidity": symbol}),
        ),
        discovery_rank=rank,
        discovery_source="IBKR_MOST_ACTIVE",
        captured_at=NOW,
        deepseek_overlay=overlay,
    )


def test_taxonomy_has_exact_canonical_categories_and_versioned_hash() -> None:
    assert len(COMPANY_SECTORS) == 11
    assert len(ETF_BUCKETS) == 5
    assert EquityCategory.UNCLASSIFIED not in COMPANY_SECTORS | ETF_BUCKETS
    classification = classify_security(
        "xom",
        security_type="STK",
        ibkr_sector="Energy",
    )
    assert classification.category is EquityCategory.ENERGY
    assert classification.source is ClassificationSource.IBKR_METADATA
    assert classification.concentration_group == "SECTOR:ENERGY"
    assert len(classification.taxonomy_hash) == 64


def test_classification_uses_ibkr_then_local_exact_then_unclassified() -> None:
    override = classify_security(
        "AAPL",
        security_type="STK",
        ibkr_sector="Energy",
    )
    local = classify_security("AAPL", security_type="STK")
    etf = classify_security(
        "NEWETF",
        security_type="ETF",
        ibkr_category="Fixed Income",
    )
    no_fuzzy = classify_security(
        "UNKNOWN",
        security_type="STK",
        ibkr_sector="Information Tech",
    )
    assert override.category is EquityCategory.ENERGY
    assert override.source is ClassificationSource.IBKR_METADATA
    assert override.mega_cap_tech is True
    assert local.category is EquityCategory.INFORMATION_TECHNOLOGY
    assert local.source is ClassificationSource.LOCAL_EXACT
    assert etf.category is EquityCategory.RATES_BOND_ETF
    assert etf.concentration_group == "ETF:RATES_BOND_ETF"
    assert no_fuzzy.category is EquityCategory.UNCLASSIFIED
    assert no_fuzzy.source is ClassificationSource.UNCLASSIFIED


def test_classifier_consumes_every_alias_bound_into_taxonomy_hash() -> None:
    for alias, category in COMPANY_ALIAS_VALUES.items():
        classified = classify_security("ZZZ", security_type="STK", ibkr_sector=alias)
        assert classified.category is EquityCategory(category)
    for alias, category in ETF_ALIAS_VALUES.items():
        classified = classify_security("ZZZ", security_type="ETF", ibkr_category=alias)
        assert classified.category is EquityCategory(category)


def test_factor_validation_keeps_missing_null_and_requires_time_and_hashes() -> None:
    missing = _factor(
        FactorKind.NEWS,
        signal=None,
        confidence=None,
        status=FactorStatus.MISSING,
    )
    assert missing.signed_signal is None
    assert missing.confidence is None
    with pytest.raises(ValueError, match="must keep signal"):
        _factor(FactorKind.NEWS, status=FactorStatus.STALE)
    with pytest.raises(ValueError, match="requires signal"):
        _factor(FactorKind.NEWS, signal=None, status=FactorStatus.AVAILABLE)
    with pytest.raises(ValueError, match="timezone-aware"):
        replace(missing, observed_at=NOW.replace(tzinfo=None))
    with pytest.raises(ValueError, match="SHA-256"):
        replace(missing, payload_hash="bad")


def test_input_requires_every_factor_once() -> None:
    item = _input("XOM")
    with pytest.raises(ValueError, match="all canonical"):
        replace(item, factors=item.factors[:-1])
    with pytest.raises(ValueError, match="unique"):
        replace(item, factors=item.factors[:-1] + (item.factors[0],))


def test_architect_scoring_formula_is_exact_and_liquidity_is_separate() -> None:
    factors = (
        _factor(FactorKind.NEWS, signal=Decimal("1"), confidence=Decimal("1")),
        _factor(FactorKind.FUNDAMENTALS, signal=Decimal("-0.5"), confidence=Decimal("0.8")),
        _factor(FactorKind.REGIME, signal=None, confidence=None, status=FactorStatus.MISSING),
        _factor(FactorKind.TREND_VOLATILITY, signal=Decimal("0.5"), confidence=Decimal("0.8")),
        _factor(FactorKind.POSITIONING, signal=Decimal("0.5"), confidence=Decimal("0.6")),
    )
    score = score_equity(_input("XOM", factors=factors))
    assert score.direction_score == Decimal("35.000")
    assert score.coverage_confidence == Decimal("0.720")
    assert score.positive_evidence_mass == Decimal("0.430")
    assert score.negative_evidence_mass == Decimal("0.080")
    assert score.conflict_penalty == Decimal("0.160")
    assert score.uncertainty == Decimal("0.440")
    assert score.opportunity_score == Decimal("35.25000")
    assert score.direction_label is DirectionLabel.BULLISH

    unavailable = score_equity(_input("XOM", liquidity=None, factors=factors))
    assert unavailable.liquidity_score is None
    assert unavailable.opportunity_score is None
    assert unavailable.direction_score == score.direction_score


def test_conflict_and_uncertainty_labels_follow_fixed_thresholds() -> None:
    conflict = tuple(
        _factor(
            kind,
            signal=Decimal("1") if kind is FactorKind.NEWS else Decimal("-1"),
            confidence=Decimal("1"),
        )
        for kind in FactorKind
    )
    assert score_equity(_input("MIX", factors=conflict)).direction_label is DirectionLabel.MIXED

    sparse = tuple(
        _factor(
            kind,
            signal=Decimal("1") if kind is FactorKind.POSITIONING else None,
            confidence=Decimal("1") if kind is FactorKind.POSITIONING else None,
            status=FactorStatus.AVAILABLE if kind is FactorKind.POSITIONING else FactorStatus.MISSING,
        )
        for kind in FactorKind
    )
    sparse_score = score_equity(_input("SPARSE", factors=sparse))
    assert sparse_score.coverage_confidence == Decimal("0.10")
    assert sparse_score.direction_label is DirectionLabel.UNCERTAIN


def test_deepseek_overlay_cannot_change_hash_score_or_order() -> None:
    first = _input("XOM", overlay={"verdict": "BULLISH", "score": Decimal("1")})
    second = _input("XOM", overlay={"verdict": "BEARISH", "score": Decimal("0")})
    assert first.canonical_hash == second.canonical_hash
    assert score_equity(first) == score_equity(second)
    allocator = EquityPoolAllocator()
    left = allocator.allocate((first,), slot=NOW)
    right = allocator.allocate((second,), slot=NOW)
    assert left.pool_id == right.pool_id
    assert left.snapshot_hash == right.snapshot_hash

    tied = allocator.allocate(
        (_input("ZZZ", rank=1), _input("AAA", rank=1)),
        slot=NOW,
    )
    assert tuple(row.symbol for row in tied.selected) == ("AAA", "ZZZ")


def test_allocator_applies_group_theme_and_unclassified_caps_without_fillers() -> None:
    inputs: list[EquityPoolInput] = []
    for rank, symbol in enumerate(("AAPL", "MSFT", "NVDA", "AVGO"), start=1):
        inputs.append(
            _input(
                symbol,
                category=EquityCategory.INFORMATION_TECHNOLOGY,
                rank=rank,
                liquidity=Decimal(100 - rank),
                mega=True,
            )
        )
    for rank, symbol in enumerate(("ORCL", "CRM", "ADBE", "AMD"), start=5):
        inputs.append(
            _input(
                symbol,
                category=EquityCategory.INFORMATION_TECHNOLOGY,
                rank=rank,
                liquidity=Decimal(100 - rank),
            )
        )
    for rank, symbol in enumerate(("U1", "U2", "U3"), start=20):
        inputs.append(
            _input(
                symbol,
                category=EquityCategory.UNCLASSIFIED,
                rank=rank,
                liquidity=Decimal(80 - rank),
            )
        )
    inputs.append(_input("NOQUOTE", rank=40, liquidity=None))

    snapshot = EquityPoolAllocator().allocate(inputs, slot=NOW)
    tech = [
        row for row in snapshot.selected
        if row.classification.category is EquityCategory.INFORMATION_TECHNOLOGY
    ]
    unclassified = [
        row for row in snapshot.selected
        if row.classification.category is EquityCategory.UNCLASSIFIED
    ]
    reasons = {reason for row in snapshot.excluded for reason in row.reasons}
    assert len(tech) == 5
    assert sum(row.classification.mega_cap_tech for row in tech) == 3
    assert len(unclassified) == 2
    assert "MEGA_CAP_TECH_CAP" in reasons
    assert "CONCENTRATION_GROUP_CAP" in reasons
    assert "UNCLASSIFIED_CAP" in reasons
    assert "LIQUIDITY_MISSING" in reasons
    assert snapshot.concentration_counts["THEME:MEGA_CAP_TECH"] == 3


def test_allocator_is_bounded_deterministic_and_position_mode_never_grants_authority() -> None:
    sectors = sorted(COMPANY_SECTORS, key=lambda item: item.value)
    inputs = tuple(
        _input(
            f"S{index:03d}",
            category=sectors[index % len(sectors)],
            rank=index + 1,
            liquidity=Decimal(100 - (index % 50)),
        )
        for index in range(160)
    )
    allocator = EquityPoolAllocator()
    snapshot = allocator.allocate(
        tuple(reversed(inputs)),
        slot=NOW,
        position_mode=PositionMode.BLOCKED_OPEN_POSITION,
    )
    repeat = allocator.allocate(
        inputs,
        slot=NOW,
        position_mode=PositionMode.BLOCKED_OPEN_POSITION,
    )
    assert snapshot.discovery_count == 160
    assert snapshot.considered_count == 150
    assert len(snapshot.selected) == 30
    assert snapshot.snapshot_hash == repeat.snapshot_hash
    assert snapshot.position_mode is PositionMode.BLOCKED_OPEN_POSITION
    assert snapshot.research_only is True
    assert snapshot.entry_authority is False
    assert snapshot.approval_eligible is False
    assert any("DISCOVERY_LIMIT" in row.reasons for row in snapshot.excluded)
    assert any("DEEP_SCAN_LIMIT" in row.reasons for row in snapshot.excluded)


def test_uncertain_candidates_are_explicitly_excluded_not_filled() -> None:
    sparse = tuple(
        _factor(
            kind,
            signal=None,
            confidence=None,
            status=FactorStatus.MISSING,
        )
        for kind in FactorKind
    )
    snapshot = EquityPoolAllocator().allocate(
        (_input("EMPTY", factors=sparse),),
        slot=NOW,
    )
    assert snapshot.selected == ()
    assert snapshot.excluded[0].reasons == ("INSUFFICIENT_DIRECTION_EVIDENCE",)


def test_slot_gate_excludes_future_expired_and_stale_evidence() -> None:
    future_factor = replace(
        _factor(FactorKind.NEWS),
        observed_at=NOW + timedelta(minutes=1),
        effective_at=NOW + timedelta(minutes=1),
        valid_until=NOW + timedelta(days=1),
    )
    expired_factor = replace(
        _factor(FactorKind.FUNDAMENTALS),
        observed_at=NOW - timedelta(days=2),
        effective_at=NOW - timedelta(days=2),
        valid_until=NOW - timedelta(minutes=1),
    )
    factors = tuple(
        future_factor if item.factor is FactorKind.NEWS else
        expired_factor if item.factor is FactorKind.FUNDAMENTALS else item
        for item in _input("XOM").factors
    )
    score = score_equity(_input("XOM", factors=factors), as_of=NOW)
    assert score.coverage_confidence == Decimal("0.50")

    stale = replace(
        _input("STALE"),
        liquidity=replace(
            _input("STALE").liquidity,
            observed_at=NOW - timedelta(minutes=16),
        ),
    )
    future_capture = replace(_input("FUTURE"), captured_at=NOW + timedelta(seconds=1))
    snapshot = EquityPoolAllocator().allocate((stale, future_capture), slot=NOW)
    reasons = {row.symbol: row.reasons for row in snapshot.excluded}
    assert reasons["STALE"] == ("LIQUIDITY_STALE",)
    assert reasons["FUTURE"] == ("CAPTURED_AT_FUTURE",)


def test_taxonomy_hash_binds_injected_mapping_and_allocator_rejects_drift() -> None:
    custom = classify_security(
        "XOM",
        security_type="STK",
        local_mapping={"XOM": EquityCategory.FINANCIALS},
    )
    assert custom.taxonomy_hash != DEFAULT_TAXONOMY_HASH
    item = replace(_input("XOM"), classification=custom)
    snapshot = EquityPoolAllocator().allocate((item,), slot=NOW)
    assert snapshot.selected == ()
    assert snapshot.excluded[0].reasons == ("TAXONOMY_NOT_CURRENT",)


def test_score_decision_and_snapshot_contracts_are_strict() -> None:
    score = score_equity(_input("XOM"), as_of=NOW)
    with pytest.raises(TypeError, match="Decimal"):
        replace(score, direction_score=1.0)
    with pytest.raises(ValueError, match="at most"):
        replace(score, uncertainty=Decimal("1.01"))
    snapshot = EquityPoolAllocator().allocate((_input("XOM"),), slot=NOW)
    with pytest.raises(ValueError, match="contiguous"):
        replace(
            snapshot,
            selected=(replace(snapshot.selected[0], selected_rank=2),),
        )
    with pytest.raises(ValueError, match="concentration_counts"):
        replace(snapshot, concentration_counts={"SECTOR:ENERGY": 99})
    with pytest.raises(ValueError, match="SUPPORTING_ONLY"):
        replace(snapshot.selected[0], decision_authority="EXECUTION")


def test_mega_cap_identity_is_derived_and_cannot_bypass_cap() -> None:
    direct = CanonicalClassification(
        symbol="AAPL",
        category=EquityCategory.INFORMATION_TECHNOLOGY,
        source=ClassificationSource.LOCAL_EXACT,
        mega_cap_tech=False,
    )
    assert direct.mega_cap_tech is True
    inputs = tuple(
        replace(
            _input(symbol, rank=rank),
            classification=CanonicalClassification(
                symbol=symbol,
                category=EquityCategory.INFORMATION_TECHNOLOGY,
                source=ClassificationSource.LOCAL_EXACT,
                mega_cap_tech=False,
            ),
        )
        for rank, symbol in enumerate(("AAPL", "MSFT", "NVDA", "AVGO"), start=1)
    )
    snapshot = EquityPoolAllocator().allocate(inputs, slot=NOW)
    assert sum(row.classification.mega_cap_tech for row in snapshot.selected) == 3
    assert any(row.reasons == ("MEGA_CAP_TECH_CAP",) for row in snapshot.excluded)


def test_scoring_hash_binds_every_timing_formula_and_threshold_field() -> None:
    required = {
        "weights", "direction", "coverage", "conflict", "uncertainty",
        "opportunity", "factor_timing", "liquidity_max_age_seconds",
        "conflict_threshold", "minimum_coverage", "maximum_uncertainty",
        "direction_threshold", "algorithm", "version",
    }
    assert required <= set(SCORING_POLICY_BODY)
    assert SCORING_POLICY_BODY["liquidity_max_age_seconds"] == 900
    for field in required:
        changed = dict(SCORING_POLICY_BODY)
        changed[field] = f"changed:{field}"
        assert canonical_hash(changed) != SCORING_HASH
