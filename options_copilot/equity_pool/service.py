"""Bounded production adapter for scanner-derived G035 equity pools."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass, is_dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import re
import threading

from options_copilot.fundamentals import fundamental_supporting_source_hash
from options_copilot.storage.canonical import canonical_hash, utc_datetime

from .allocator import EquityPoolAllocator
from .classification import LOCAL_EXACT_MAPPING, classify_security
from .evidence_cache import _quote_trend_measurement
from .models import (
    ETF_BUCKETS,
    EquityPoolInput,
    FactorEvidence,
    FactorKind,
    FactorStatus,
    LiquidityEvidence,
    PositionMode,
)
from .store import (
    EquityPoolStore,
    EquityPoolStoreConflict,
    EquityPoolStoreCorruption,
    StoredEquityPoolSnapshot,
)
from .scoring import SCORING_HASH


_SCANNER_SOURCES = (
    "NEWS_EVENT_POOL",
    "MOST_ACTIVE",
    "TOP_PERC_GAIN",
    "TOP_PERC_LOSE",
    "CORE_UNIVERSE",
    "LEGACY_AFTER_HOURS_CACHE",
    "MISSING_PROVENANCE",
)
_SCANNER_PRIORITY = {value: index for index, value in enumerate(_SCANNER_SOURCES)}
_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_DIRECTION = {
    "BULLISH": Decimal("1"),
    "BEARISH": Decimal("-1"),
    "NEUTRAL": Decimal("0"),
}
_NEGATIVE_FUNDAMENTAL_METRICS = frozenset(
    {"DEBT_CURRENT", "DEBT_NONCURRENT", "PE_TTM", "PB_ANNUAL", "PS_TTM"}
)


@dataclass(frozen=True, slots=True)
class EquityPoolBuildResult:
    stored: StoredEquityPoolSnapshot
    scanner_input_hashes: tuple[str, ...]
    pacing_usage_hash: str

    @property
    def selected_symbols(self) -> tuple[str, ...]:
        return tuple(item.symbol for item in self.stored.snapshot.selected)

    @property
    def acquisition_targets(self) -> tuple[str, ...]:
        """Sector-balanced missing-evidence targets; never selected or eligible."""

        group_counts: dict[str, int] = {}
        mega = 0
        unclassified = 0
        targets: list[str] = []
        for item in sorted(self.stored.normalized_inputs, key=lambda row: (row.discovery_rank, row.symbol)):
            # A fresh measured liquidity row proves this symbol already crossed
            # the acquisition boundary for the current pool snapshot.  It may
            # still be research-only because its thesis is uncertain; do not
            # spend another quote request merely because it was not selected.
            if item.liquidity.status is FactorStatus.AVAILABLE:
                continue
            classification = item.classification
            group = classification.concentration_group
            if len(targets) >= 30 or group_counts.get(group, 0) >= 5:
                continue
            if classification.mega_cap_tech and mega >= 3:
                continue
            if classification.category.value == "UNCLASSIFIED" and unclassified >= 2:
                continue
            targets.append(item.symbol)
            group_counts[group] = group_counts.get(group, 0) + 1
            mega += int(classification.mega_cap_tech)
            unclassified += int(classification.category.value == "UNCLASSIFIED")
        return tuple(targets)

    @property
    def equity_pool_reference(self) -> dict[str, object]:
        decisions = self.stored.snapshot.selected + self.stored.snapshot.excluded
        exclusion_stats: dict[str, int] = {}
        for decision in self.stored.snapshot.excluded:
            for reason in decision.reasons:
                exclusion_stats[reason] = exclusion_stats.get(reason, 0) + 1
        return {
            "schema": "options_copilot.equity_pool_reference.v1",
            "snapshot_id": self.stored.snapshot.pool_id,
            "snapshot_hash": self.stored.snapshot_hash,
            "input_manifest_hash": self.stored.normalized_inputs_hash,
            "rows_hash": canonical_hash(tuple(item.as_dict() for item in decisions)),
            "policy_hash": self.stored.snapshot.policy_hash,
            "taxonomy_hash": self.stored.snapshot.taxonomy_hash,
            "scoring_hash": SCORING_HASH,
            "selected_symbols": self.selected_symbols,
            "discovered_symbols": tuple(
                item.symbol
                for item in sorted(
                    self.stored.normalized_inputs,
                    key=lambda row: (row.discovery_rank, row.symbol),
                )
            ),
            "acquisition_targets": self.acquisition_targets,
            "acquisition_targets_authority": "RESEARCH_SCHEDULING_ONLY",
            "discovery_count": self.stored.snapshot.discovery_count,
            "selected_count": len(self.stored.snapshot.selected),
            "excluded_count": len(self.stored.snapshot.excluded),
            "exclusion_stats": dict(sorted(exclusion_stats.items())),
        }

    def as_dict(self) -> dict[str, object]:
        return {
            "pool_id": self.stored.snapshot.pool_id,
            "snapshot_hash": self.stored.snapshot_hash,
            "chain_hash": self.stored.chain_hash,
            "normalized_inputs_hash": self.stored.normalized_inputs_hash,
            "scanner_input_hashes": self.scanner_input_hashes,
            "pacing_usage_hash": self.pacing_usage_hash,
            "discovery_count": self.stored.snapshot.discovery_count,
            "considered_count": self.stored.snapshot.considered_count,
            "selected_count": len(self.stored.snapshot.selected),
            "selected_symbols": self.selected_symbols,
            "position_mode": self.stored.snapshot.position_mode.value,
            "research_only": True,
            "entry_authority": False,
            "approval_eligible": False,
            "decision_authority": "SUPPORTING_ONLY",
            "instruction_creation_allowed": False,
            "order_allowed": False,
            "equity_pool_reference": self.equity_pool_reference,
        }


class EquityPoolService:
    """Factorize captured scanner rows and persist one replayable research pool."""

    def __init__(
        self,
        store: EquityPoolStore,
        *,
        allocator: EquityPoolAllocator | None = None,
        news_reader: Callable[[], object] | None = None,
        fundamentals_reader: Callable[[str, datetime], object] | None = None,
        factor_readers: Mapping[
            FactorKind,
            Callable[[str, datetime], object],
        ] | None = None,
        liquidity_reader: Callable[[str, datetime], object] | None = None,
        evidence_batch_reader: Callable[[tuple[str, ...], datetime], object] | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if not isinstance(store, EquityPoolStore):
            raise TypeError("store must be EquityPoolStore")
        self.store = store
        self.allocator = allocator or EquityPoolAllocator()
        self.news_reader = news_reader
        self.fundamentals_reader = fundamentals_reader
        self.factor_readers = dict(factor_readers or {})
        if FactorKind.NEWS in self.factor_readers or FactorKind.FUNDAMENTALS in self.factor_readers:
            raise ValueError("news and fundamentals use their dedicated deterministic adapters")
        if any(not callable(reader) for reader in self.factor_readers.values()):
            raise TypeError("factor readers must be callable")
        if news_reader is not None and not callable(news_reader):
            raise TypeError("news_reader must be callable or null")
        if fundamentals_reader is not None and not callable(fundamentals_reader):
            raise TypeError("fundamentals_reader must be callable or null")
        if liquidity_reader is not None and not callable(liquidity_reader):
            raise TypeError("liquidity_reader must be callable or null")
        self.liquidity_reader = liquidity_reader
        if evidence_batch_reader is not None and not callable(evidence_batch_reader):
            raise TypeError("evidence_batch_reader must be callable or null")
        self.evidence_batch_reader = evidence_batch_reader
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._lock = threading.RLock()
        self._last_build: EquityPoolBuildResult | None = None

    def build(
        self,
        *,
        scanner_rows: Sequence[object],
        slot: datetime,
        pacing_usage: Mapping[str, object],
        position_mode: PositionMode = PositionMode.CLEAR,
        commit_guard: Callable[[], bool] | None = None,
    ) -> EquityPoolBuildResult:
        frozen_slot = utc_datetime(slot, field="equity pool slot")
        normalized_usage = _mapping(pacing_usage)
        pacing_usage_hash = canonical_hash(
            {
                "schema": "options_copilot.equity_pool_pacing_usage.v1",
                "slot": frozen_slot,
                "usage": normalized_usage,
            }
        )
        rows = _bounded_scanner_rows(scanner_rows)
        evidence_view = (
            None
            if self.evidence_batch_reader is None
            else self.evidence_batch_reader(
                tuple(str(row["symbol"]) for row in rows),
                frozen_slot,
            )
        )
        news_snapshot = self._news_snapshot()
        inputs: list[EquityPoolInput] = []
        scanner_hashes: list[str] = []
        for discovery_rank, row in enumerate(rows, start=1):
            row_hash = canonical_hash(
                {
                    "schema": "options_copilot.ibkr_scanner_discovery.v1",
                    "slot": frozen_slot,
                    "row": row,
                }
            )
            scanner_hashes.append(row_hash)
            inputs.append(
                self._factorize(
                    row,
                    discovery_rank=discovery_rank,
                    slot=frozen_slot,
                    scanner_hash=row_hash,
                    pacing_usage_hash=pacing_usage_hash,
                    news_snapshot=news_snapshot,
                    evidence_view=evidence_view,
                )
            )
        stored = self.store.append(
            slot=frozen_slot,
            inputs=tuple(inputs),
            allocator=self.allocator,
            position_mode=position_mode,
            recorded_at=utc_datetime(self._clock(), field="equity pool clock"),
            commit_guard=commit_guard,
        )
        result = EquityPoolBuildResult(
            stored=stored,
            scanner_input_hashes=tuple(scanner_hashes),
            pacing_usage_hash=pacing_usage_hash,
        )
        with self._lock:
            self._last_build = result
        return result

    def selected_symbols(self) -> tuple[str, ...]:
        """Return research symbols only; this method grants no option authority."""

        try:
            # ``latest`` verifies the full predecessor chain through the
            # returned row.  A separate full-ledger pass here duplicated the
            # same authority work on every startup/read.
            latest = self.store.latest()
        except Exception:
            return ()
        return () if latest is None else tuple(
            item.symbol for item in latest.snapshot.selected
        )

    def latest_payload(self) -> dict[str, object]:
        """Return a fail-closed API-ready projection without provider work."""

        try:
            # ``latest`` already performs the complete chain verification.
            latest = self.store.latest()
        except EquityPoolStoreCorruption:
            return _unavailable_payload("EQUITY_POOL_LEDGER_CORRUPT")
        except Exception:
            return _unavailable_payload("EQUITY_POOL_LEDGER_UNAVAILABLE")
        if latest is None:
            return _unavailable_payload("EQUITY_POOL_NOT_RUN", status="NOT_RUN")
        scanner_hashes, pacing_hash = _discovery_hashes(latest)
        with self._lock:
            current = self._last_build
        if current is not None and current.stored.snapshot.pool_id == latest.snapshot.pool_id:
            scanner_hashes = current.scanner_input_hashes
            pacing_hash = current.pacing_usage_hash
            reference = current.equity_pool_reference
        else:
            reference = EquityPoolBuildResult(
                stored=latest,
                scanner_input_hashes=scanner_hashes,
                pacing_usage_hash=pacing_hash or "",
            ).equity_pool_reference
        snapshot = latest.snapshot
        return {
            "schema": "options_copilot.equity_pool_read_model.v1",
            "status": "READY",
            "decision": "RESEARCH_ONLY",
            "reason_codes": (
                ()
                if snapshot.selected
                else ("NO_QUALIFIED_EQUITY_OPPORTUNITIES",)
            ),
            "pool_id": snapshot.pool_id,
            "slot": snapshot.slot.isoformat(),
            "snapshot_hash": latest.snapshot_hash,
            "chain_hash": latest.chain_hash,
            "normalized_inputs_hash": latest.normalized_inputs_hash,
            "policy_version": snapshot.policy_version,
            "policy_hash": snapshot.policy_hash,
            "taxonomy_version": snapshot.taxonomy_version,
            "taxonomy_hash": snapshot.taxonomy_hash,
            "position_mode": snapshot.position_mode.value,
            "discovery_count": snapshot.discovery_count,
            "considered_count": snapshot.considered_count,
            "selected_count": len(snapshot.selected),
            "excluded_count": len(snapshot.excluded),
            "selected_symbols": tuple(item.symbol for item in snapshot.selected),
            "selected": tuple(item.as_dict() for item in snapshot.selected),
            "excluded": tuple(item.as_dict() for item in snapshot.excluded),
            "concentration_counts": dict(snapshot.concentration_counts),
            "scanner_sources": _SCANNER_SOURCES,
            "scanner_input_hashes": scanner_hashes,
            "pacing_usage_hash": pacing_hash,
            "equity_pool_reference": reference,
            "research_only": True,
            "entry_authority": False,
            "approval_eligible": False,
            "decision_authority": "SUPPORTING_ONLY",
            "instruction_creation_allowed": False,
            "order_allowed": False,
            "review_only": True,
            "direct_order_submission": False,
        }

    def _factorize(
        self,
        row: Mapping[str, object],
        *,
        discovery_rank: int,
        slot: datetime,
        scanner_hash: str,
        pacing_usage_hash: str,
        news_snapshot: Mapping[str, object],
        evidence_view: object | None,
    ) -> EquityPoolInput:
        symbol = str(row["symbol"])
        local_category = LOCAL_EXACT_MAPPING.get(symbol)
        security_type = str(row.get("security_type") or "").strip().upper()
        if not security_type:
            security_type = "ETF" if local_category in ETF_BUCKETS else "STK"
        classification = classify_security(
            symbol,
            security_type=security_type,
            ibkr_sector=_optional_text(row.get("industry")),
            ibkr_category=_optional_text(row.get("category")),
        )
        factors = {
            FactorKind.NEWS: _news_factor(news_snapshot, symbol=symbol, slot=slot),
            FactorKind.FUNDAMENTALS: self._fundamentals_factor(symbol, slot),
            FactorKind.REGIME: self._optional_factor(
                FactorKind.REGIME,
                symbol,
                slot,
                "REGIME_EVIDENCE_UNAVAILABLE",
                evidence_view=evidence_view,
            ),
            FactorKind.TREND_VOLATILITY: self._optional_factor(
                FactorKind.TREND_VOLATILITY,
                symbol,
                slot,
                "TREND_VOLATILITY_EVIDENCE_UNAVAILABLE",
                evidence_view=evidence_view,
            ),
            FactorKind.POSITIONING: self._optional_factor(
                FactorKind.POSITIONING,
                symbol,
                slot,
                "POSITIONING_EVIDENCE_UNAVAILABLE",
                evidence_view=evidence_view,
            ),
        }
        liquidity = self._liquidity(
            symbol,
            slot,
            fallback=_scanner_liquidity(
                row,
                slot=slot,
                source_hashes=(scanner_hash, pacing_usage_hash),
            ),
            evidence_view=evidence_view,
        )
        return EquityPoolInput(
            classification=classification,
            factors=tuple(factors[kind] for kind in FactorKind),
            liquidity=liquidity,
            discovery_rank=discovery_rank,
            discovery_source=str(row["source_scan"]),
            captured_at=slot,
        )

    def _news_snapshot(self) -> Mapping[str, object]:
        if self.news_reader is None:
            return {}
        try:
            return _mapping(self.news_reader())
        except Exception:
            return {}

    def _fundamentals_factor(self, symbol: str, slot: datetime) -> FactorEvidence:
        if self.fundamentals_reader is None:
            return _missing_factor(
                FactorKind.FUNDAMENTALS,
                slot,
                "FUNDAMENTALS_READER_UNAVAILABLE",
            )
        try:
            raw = _mapping(self.fundamentals_reader(symbol, slot))
        except Exception:
            return _missing_factor(
                FactorKind.FUNDAMENTALS,
                slot,
                "FUNDAMENTALS_READ_FAILED",
            )
        return _fundamentals_factor(raw, symbol=symbol, slot=slot)

    def _optional_factor(
        self,
        kind: FactorKind,
        symbol: str,
        slot: datetime,
        unavailable_reason: str,
        *,
        fallback: FactorEvidence | None = None,
        evidence_view: object | None = None,
    ) -> FactorEvidence:
        if evidence_view is not None:
            read_factor = getattr(evidence_view, "read_factor", None)
            if not callable(read_factor):
                return fallback or _missing_factor(kind, slot, f"{kind.value}_EVIDENCE_INVALID")
            try:
                value = read_factor(symbol, kind)
            except Exception:
                return fallback or _missing_factor(kind, slot, f"{kind.value}_READ_FAILED")
            if isinstance(value, FactorEvidence) and value.factor is kind:
                return value
            return fallback or _missing_factor(kind, slot, unavailable_reason)
        reader = self.factor_readers.get(kind)
        if reader is None:
            return fallback or _missing_factor(kind, slot, unavailable_reason)
        try:
            value = reader(symbol, slot)
        except Exception:
            return fallback or _missing_factor(kind, slot, f"{kind.value}_READ_FAILED")
        if isinstance(value, FactorEvidence) and value.factor is kind:
            return value
        return fallback or _missing_factor(kind, slot, f"{kind.value}_EVIDENCE_INVALID")

    def _liquidity(
        self,
        symbol: str,
        slot: datetime,
        *,
        fallback: LiquidityEvidence,
        evidence_view: object | None = None,
    ) -> LiquidityEvidence:
        if evidence_view is not None:
            read_liquidity = getattr(evidence_view, "read_liquidity", None)
            if not callable(read_liquidity):
                return _missing_liquidity(slot, "LIQUIDITY_EVIDENCE_INVALID", fallback.source_hashes)
            try:
                value = read_liquidity(symbol)
            except Exception:
                return _missing_liquidity(slot, "LIQUIDITY_READ_FAILED", fallback.source_hashes)
            if isinstance(value, LiquidityEvidence):
                return value
            return fallback
        if self.liquidity_reader is None:
            return fallback
        try:
            value = self.liquidity_reader(symbol, slot)
        except Exception:
            return _missing_liquidity(slot, "LIQUIDITY_READ_FAILED", fallback.source_hashes)
        if not isinstance(value, LiquidityEvidence):
            return _missing_liquidity(slot, "LIQUIDITY_EVIDENCE_INVALID", fallback.source_hashes)
        return value


def _bounded_scanner_rows(values: Sequence[object]) -> tuple[Mapping[str, object], ...]:
    normalized: list[dict[str, object]] = []
    for value in values:
        raw = _mapping(value)
        symbol = str(raw.get("symbol") or "").strip().upper()
        source = str(raw.get("source_scan") or "").strip().upper()
        rank = raw.get("rank")
        if (
            not symbol
            or source not in _SCANNER_PRIORITY
            or isinstance(rank, bool)
            or not isinstance(rank, int)
            or not 0 <= rank < 50
        ):
            continue
        normalized.append(
            {
                "symbol": symbol,
                "rank": rank,
                "source_scan": source,
                "contract_id": _positive_int(raw.get("contract_id")),
                "exchange": _optional_text(raw.get("exchange")),
                "industry": _optional_text(raw.get("industry")),
                "category": _optional_text(raw.get("category")),
                "subcategory": _optional_text(raw.get("subcategory")),
                "security_type": _optional_text(raw.get("security_type")),
            }
        )
    normalized.sort(
        key=lambda row: (
            _SCANNER_PRIORITY[str(row["source_scan"])],
            int(row["rank"]),
            str(row["symbol"]),
        )
    )
    unique: list[Mapping[str, object]] = []
    seen: set[str] = set()
    for row in normalized:
        symbol = str(row["symbol"])
        if symbol in seen:
            continue
        seen.add(symbol)
        unique.append(row)
        if len(unique) >= 150:
            break
    return tuple(unique)


def _news_factor(
    snapshot: Mapping[str, object],
    *,
    symbol: str,
    slot: datetime,
) -> FactorEvidence:
    rows = snapshot.get("news")
    if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes, bytearray)):
        return _missing_factor(FactorKind.NEWS, slot, "NEWS_EVIDENCE_UNAVAILABLE")
    positive = Decimal("0")
    negative = Decimal("0")
    signed = Decimal("0")
    total_weight = Decimal("0")
    confidence_mass = Decimal("0")
    hashes: list[str] = []
    observed_values: list[datetime] = []
    binding_reasons: set[str] = set()
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        binding_reason = _research_symbol_binding_reason(row, symbol)
        if binding_reason is None:
            continue
        observed = _timestamp(row.get("observed_at")) or _timestamp(row.get("published_at"))
        if observed is None or observed > slot:
            continue
        classification = _mapping(row.get("classification"))
        direction = str(
            classification.get("direction", row.get("direction", ""))
        ).strip().upper()
        signal = _DIRECTION.get(direction)
        confidence = _normalized_confidence(
            classification.get("confidence", row.get("confidence"))
        )
        impact = _bounded_decimal(
            row.get("event_impact_score"),
            minimum=Decimal("0"),
            maximum=Decimal("100"),
        )
        if signal is None or confidence is None or impact is None:
            continue
        weight = impact / Decimal("100")
        signed_value = signal * weight * confidence
        signed += signed_value
        positive += max(Decimal("0"), signed_value)
        negative += max(Decimal("0"), -signed_value)
        total_weight += weight
        confidence_mass += weight * confidence
        hashes.append(canonical_hash(dict(row)))
        observed_values.append(observed)
        binding_reasons.add(binding_reason)
    if not hashes or total_weight <= 0:
        return _missing_factor(FactorKind.NEWS, slot, "NEWS_DIRECTION_EVIDENCE_UNAVAILABLE")
    conflict_ratio = (
        Decimal("2") * min(positive, negative) / (positive + negative)
        if positive + negative > 0
        else Decimal("0")
    )
    if conflict_ratio >= Decimal("0.5"):
        return FactorEvidence(
            factor=FactorKind.NEWS,
            status=FactorStatus.CONFLICTED,
            signed_signal=None,
            confidence=None,
            horizon="5D",
            observed_at=max(observed_values),
            effective_at=max(observed_values),
            valid_until=None,
            source_hashes=tuple(dict.fromkeys(hashes)),
            reasons=("NEWS_DIRECTION_CONFLICTED",),
            payload_hash=canonical_hash(
                {"symbol": symbol, "source_hashes": tuple(hashes), "conflict": conflict_ratio}
            ),
        )
    return FactorEvidence(
        factor=FactorKind.NEWS,
        status=FactorStatus.AVAILABLE,
        signed_signal=max(Decimal("-1"), min(Decimal("1"), signed / total_weight)),
        confidence=max(Decimal("0"), min(Decimal("1"), confidence_mass / total_weight)),
        horizon="5D",
        observed_at=max(observed_values),
        effective_at=max(observed_values),
        valid_until=slot + timedelta(days=1),
        source_hashes=tuple(dict.fromkeys(hashes)),
        reasons=tuple(sorted(binding_reasons)),
        payload_hash=canonical_hash(
            {"symbol": symbol, "source_hashes": tuple(hashes), "signed": signed}
        ),
    )


def _fundamentals_factor(
    value: Mapping[str, object],
    *,
    symbol: str,
    slot: datetime,
) -> FactorEvidence:
    payload = _mapping(value.get("payload"))
    if str(payload.get("symbol") or "").strip().upper() != symbol:
        return _missing_factor(FactorKind.FUNDAMENTALS, slot, "FUNDAMENTALS_SYMBOL_MISMATCH")
    revisions = payload.get("revisions")
    source_hash = _digest_text(value.get("source_hash"))
    authentic_source_hash = fundamental_supporting_source_hash(payload)
    payload_as_of = _timestamp(payload.get("as_of"))
    if (
        source_hash is None
        or authentic_source_hash != source_hash
        or payload_as_of != slot
    ):
        return _missing_factor(
            FactorKind.FUNDAMENTALS,
            slot,
            "FUNDAMENTALS_SOURCE_HASH_INVALID",
        )
    if not isinstance(revisions, Sequence) or isinstance(revisions, (str, bytes, bytearray)):
        return _missing_factor(
            FactorKind.FUNDAMENTALS,
            slot,
            "FUNDAMENTAL_REVISION_EVIDENCE_UNAVAILABLE",
            source_hashes=(source_hash,),
        )
    signed_mass = Decimal("0")
    confidence_mass = Decimal("0")
    observed_values: list[datetime] = []
    row_hashes: list[str] = []
    for row in revisions:
        if not isinstance(row, Mapping):
            continue
        observed = _timestamp(row.get("observed_at"))
        delta = _finite_decimal(row.get("delta"))
        previous = _finite_decimal(row.get("previous_value"))
        metric = str(row.get("metric") or "").strip().upper()
        if observed is None or observed > slot or delta is None or previous is None or not metric:
            continue
        magnitude = min(Decimal("1"), abs(delta) / (abs(previous) + Decimal("1")))
        direction = Decimal("-1") if metric in _NEGATIVE_FUNDAMENTAL_METRICS else Decimal("1")
        signed_mass += direction * (Decimal("1") if delta > 0 else Decimal("-1") if delta < 0 else Decimal("0")) * magnitude
        confidence_mass += max(Decimal("0.25"), magnitude)
        observed_values.append(observed)
        row_hashes.append(canonical_hash(dict(row)))
    if not row_hashes:
        return _missing_factor(
            FactorKind.FUNDAMENTALS,
            slot,
            "FUNDAMENTAL_DIRECTION_EVIDENCE_UNAVAILABLE",
            source_hashes=(source_hash,),
        )
    count = Decimal(len(row_hashes))
    return FactorEvidence(
        factor=FactorKind.FUNDAMENTALS,
        status=FactorStatus.AVAILABLE,
        signed_signal=max(Decimal("-1"), min(Decimal("1"), signed_mass / count)),
        confidence=max(Decimal("0"), min(Decimal("1"), confidence_mass / count)),
        horizon="WEEKS_2_4",
        observed_at=max(observed_values),
        effective_at=max(observed_values),
        valid_until=slot + timedelta(days=7),
        source_hashes=tuple(dict.fromkeys((source_hash, *row_hashes))),
        reasons=("POINT_IN_TIME_FUNDAMENTAL_REVISIONS",),
        payload_hash=canonical_hash(
            {"symbol": symbol, "source_hash": source_hash, "revisions": tuple(row_hashes)}
        ),
    )


def _scanner_liquidity(
    row: Mapping[str, object],
    *,
    slot: datetime,
    source_hashes: tuple[str, ...],
) -> LiquidityEvidence:
    return LiquidityEvidence(
        status=FactorStatus.MISSING,
        score=None,
        observed_at=slot,
        source_hashes=source_hashes,
        reasons=("MEASURED_LIQUIDITY_EVIDENCE_UNAVAILABLE",),
        payload_hash=canonical_hash(
            {"symbol": row["symbol"], "source": row["source_scan"], "rank": row["rank"]}
        ),
    )


def _missing_liquidity(
    slot: datetime,
    reason: str,
    source_hashes: tuple[str, ...],
) -> LiquidityEvidence:
    return LiquidityEvidence(
        status=FactorStatus.MISSING,
        score=None,
        observed_at=slot,
        source_hashes=source_hashes,
        reasons=(reason,),
        payload_hash=canonical_hash({"reason": reason, "slot": slot, "source_hashes": source_hashes}),
    )


def _missing_factor(
    kind: FactorKind,
    slot: datetime,
    reason: str,
    *,
    source_hashes: tuple[str, ...] = (),
) -> FactorEvidence:
    return FactorEvidence(
        factor=kind,
        status=FactorStatus.MISSING,
        signed_signal=None,
        confidence=None,
        horizon="5D",
        observed_at=slot,
        effective_at=slot,
        valid_until=None,
        source_hashes=source_hashes,
        reasons=(reason,),
        payload_hash=canonical_hash(
            {"factor": kind.value, "slot": slot, "reason": reason, "sources": source_hashes}
        ),
    )


def _discovery_hashes(
    stored: StoredEquityPoolSnapshot,
) -> tuple[tuple[str, ...], str | None]:
    scanner: list[str] = []
    pacing: set[str] = set()
    for item in stored.normalized_inputs:
        hashes = item.liquidity.source_hashes
        if hashes:
            scanner.append(hashes[0])
        if len(hashes) >= 2:
            pacing.add(hashes[1])
    return tuple(scanner), next(iter(pacing)) if len(pacing) == 1 else None


def _unavailable_payload(reason: str, *, status: str = "UNAVAILABLE") -> dict[str, object]:
    return {
        "schema": "options_copilot.equity_pool_read_model.v1",
        "status": status,
        "decision": "NO_TRADE",
        "reason_codes": (reason,),
        "pool_id": None,
        "slot": None,
        "snapshot_hash": None,
        "chain_hash": None,
        "normalized_inputs_hash": None,
        "position_mode": "UNKNOWN",
        "discovery_count": 0,
        "considered_count": 0,
        "selected_count": 0,
        "excluded_count": 0,
        "selected_symbols": (),
        "selected": (),
        "excluded": (),
        "concentration_counts": {},
        "scanner_sources": _SCANNER_SOURCES,
        "scanner_input_hashes": (),
        "pacing_usage_hash": None,
        "research_only": True,
        "entry_authority": False,
        "approval_eligible": False,
        "decision_authority": "SUPPORTING_ONLY",
        "instruction_creation_allowed": False,
        "order_allowed": False,
        "review_only": True,
        "direct_order_submission": False,
    }


def _mapping(value: object) -> dict[str, object]:
    if isinstance(value, Mapping):
        return {str(key): item for key, item in value.items()}
    if is_dataclass(value):
        return asdict(value)
    return {}


def _verified_symbol(row: Mapping[str, object], symbol: str) -> bool:
    values = row.get("symbols")
    if not isinstance(values, Sequence) or isinstance(values, (str, bytes, bytearray)):
        return False
    if symbol not in {str(item).strip().upper() for item in values}:
        return False
    binding = _mapping(row.get("symbol_binding"))
    status = str(binding.get("status") or "").strip().upper()
    return status.startswith("VERIFIED") or status in {"PROVIDER_VERIFIED", "IBKR_VERIFIED"}


def _research_symbol_binding_reason(
    row: Mapping[str, object],
    symbol: str,
) -> str | None:
    if _verified_symbol(row, symbol):
        return "G034_DETERMINISTIC_NEWS"
    values = row.get("symbols")
    if (
        not isinstance(values, Sequence)
        or isinstance(values, (str, bytes, bytearray))
        or len(values) != 0
    ):
        return None
    # Equity-pool package initialization is reachable from ranking and option
    # pool imports. Keep the news validation boundary lazy so importing a
    # ranking store cannot recurse through news -> decision -> ranking.
    from options_copilot.news.macro_proxy import (
        require_current_research_proxy_binding,
    )

    try:
        binding = require_current_research_proxy_binding(
            row.get("research_proxy_binding"),
            symbol=symbol,
        )
    except (TypeError, ValueError):
        return None
    if binding is None:
        return None
    return "G034_DETERMINISTIC_MACRO_RESEARCH_PROXY"


def _timestamp(value: object) -> datetime | None:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc) if value.tzinfo is not None else None
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(timezone.utc) if parsed.tzinfo is not None else None


def _finite_decimal(value: object) -> Decimal | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        parsed = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def _bounded_decimal(
    value: object,
    *,
    minimum: Decimal,
    maximum: Decimal,
) -> Decimal | None:
    parsed = _finite_decimal(value)
    return parsed if parsed is not None and minimum <= parsed <= maximum else None


def _normalized_confidence(value: object) -> Decimal | None:
    """Accept the domain ratio and the news read model's percentage scale."""

    parsed = _finite_decimal(value)
    if parsed is None or parsed < 0:
        return None
    if parsed <= 1:
        return parsed
    if parsed <= 100:
        return parsed / Decimal("100")
    return None


def _digest_text(value: object) -> str | None:
    text = str(value or "").strip().lower()
    return text if _DIGEST.fullmatch(text) is not None else None


def _optional_text(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = " ".join(value.strip().split())
    return normalized[:160] if normalized else None


def _positive_int(value: object) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) and value > 0 else None


def read_hash_bound_factor(
    payload: object,
    *,
    symbol: str,
    kind: FactorKind,
    as_of: datetime,
) -> FactorEvidence | None:
    """Read only an exact hash-bound cached equity factor; never infer neutral."""

    raw = _mapping(payload)
    rows = raw.get("equity_pool_factors")
    if not isinstance(rows, (list, tuple)):
        return None
    cutoff = utc_datetime(as_of, field="factor cutoff")
    maximum_age = {
        FactorKind.REGIME: timedelta(hours=24),
        FactorKind.TREND_VOLATILITY: timedelta(minutes=15),
        FactorKind.POSITIONING: timedelta(minutes=15),
    }.get(kind, timedelta(days=7))
    candidates: list[FactorEvidence] = []
    for item in rows:
        row = _mapping(item)
        if str(row.get("symbol") or "").strip().upper() != symbol.strip().upper():
            continue
        body = row.get("factor")
        if not isinstance(body, Mapping):
            continue
        if row.get("record_hash") != canonical_hash({"symbol": symbol.strip().upper(), "factor": body}):
            continue
        try:
            factor = FactorEvidence.from_dict(body)
        except (KeyError, TypeError, ValueError):
            continue
        if factor.factor is not kind:
            continue
        if factor.observed_at > cutoff or factor.effective_at > cutoff:
            continue
        if cutoff - factor.observed_at > maximum_age:
            continue
        if factor.valid_until is not None and factor.valid_until < cutoff:
            continue
        candidates.append(factor)
    return max(
        candidates,
        key=lambda item: (item.observed_at, item.effective_at, item.payload_hash),
        default=None,
    )


def read_hash_bound_liquidity(
    payload: object,
    *,
    symbol: str,
    as_of: datetime,
) -> LiquidityEvidence | None:
    """Read measured cached underlying liquidity only from an exact hash binding."""

    raw = _mapping(payload)
    rows = raw.get("equity_pool_liquidity")
    if not isinstance(rows, (list, tuple)):
        return None
    cutoff = utc_datetime(as_of, field="liquidity cutoff")
    candidates: list[LiquidityEvidence] = []
    for item in rows:
        row = _mapping(item)
        normalized = symbol.strip().upper()
        if str(row.get("symbol") or "").strip().upper() != normalized:
            continue
        body = row.get("liquidity")
        if not isinstance(body, Mapping):
            continue
        if row.get("record_hash") != canonical_hash({"symbol": normalized, "liquidity": body}):
            continue
        try:
            evidence = LiquidityEvidence.from_dict(body)
        except (KeyError, TypeError, ValueError):
            continue
        if evidence.observed_at > cutoff or cutoff - evidence.observed_at > timedelta(minutes=15):
            continue
        candidates.append(evidence)
    return max(candidates, key=lambda item: (item.observed_at, item.payload_hash), default=None)


def read_runtime_factor(
    payload: object,
    *,
    symbol: str,
    kind: FactorKind,
    as_of: datetime,
) -> FactorEvidence | None:
    exact = read_hash_bound_factor(payload, symbol=symbol, kind=kind, as_of=as_of)
    if exact is not None:
        return exact
    if kind is FactorKind.POSITIONING:
        return _runtime_positioning_factor(payload, symbol=symbol, as_of=as_of)
    if kind is FactorKind.TREND_VOLATILITY:
        return _runtime_price_trend_factor(payload, symbol=symbol, as_of=as_of)
    if kind is FactorKind.REGIME:
        return _runtime_regime_factor(payload, symbol=symbol, as_of=as_of)
    return None


def read_runtime_liquidity(payload: object, *, symbol: str, as_of: datetime) -> LiquidityEvidence | None:
    exact = read_hash_bound_liquidity(payload, symbol=symbol, as_of=as_of)
    if exact is not None:
        return exact
    cutoff = utc_datetime(as_of, field="liquidity cutoff")
    candidates: list[LiquidityEvidence] = []
    for row in _candidate_rows(payload):
        if _row_symbol(row) != symbol.strip().upper():
            continue
        basis = row.get("underlying_quote_basis")
        basis_hash = row.get("underlying_quote_basis_hash")
        if not isinstance(basis, Mapping) or basis_hash != canonical_hash(basis):
            continue
        observed = _aware_time(basis.get("observed_at"))
        bid = _decimal_or_none(basis.get("bid"))
        ask = _decimal_or_none(basis.get("ask"))
        if observed is None or observed > cutoff or cutoff - observed > timedelta(minutes=15):
            continue
        if bid is None or ask is None or bid <= 0 or ask < bid:
            continue
        mid = (bid + ask) / Decimal("2")
        spread_rate = (ask - bid) / mid if mid > 0 else Decimal("1")
        score = max(Decimal("0"), min(Decimal("100"), Decimal("100") * (Decimal("1") - spread_rate)))
        candidates.append(LiquidityEvidence(
            status=FactorStatus.AVAILABLE, score=score, observed_at=observed,
            source_hashes=(str(basis_hash),), reasons=("MEASURED_UNDERLYING_BID_ASK_SPREAD",),
            payload_hash=canonical_hash({"symbol": symbol.strip().upper(), "basis_hash": basis_hash, "score": score}),
        ))
    return max(candidates, key=lambda item: (item.observed_at, item.payload_hash), default=None)


def _runtime_positioning_factor(payload: object, *, symbol: str, as_of: datetime) -> FactorEvidence | None:
    cutoff = utc_datetime(as_of, field="positioning cutoff")
    rows = _mapping(payload).get("positioning")
    if not isinstance(rows, (list, tuple)):
        return None
    candidates: list[FactorEvidence] = []
    for item in rows:
        row = _mapping(item)
        if _row_symbol(row) != symbol.strip().upper() or row.get("decision_authority") != "SUPPORTING_ONLY":
            continue
        observed = _aware_time(row.get("data_asof"))
        source_hash = row.get("broker_snapshot_hash")
        gex = _decimal_or_none(row.get("estimated_net_gex_usd_per_one_percent"))
        confidence = _decimal_or_none(row.get("gex_usable_contract_rate"))
        if not _is_digest(source_hash) or observed is None or observed > cutoff or cutoff - observed > timedelta(minutes=15):
            continue
        if gex is None or confidence is None or not Decimal("0") <= confidence <= Decimal("1"):
            continue
        signal = Decimal("0") if gex == 0 else Decimal("1") if gex > 0 else Decimal("-1")
        candidates.append(FactorEvidence(
            factor=FactorKind.POSITIONING, status=FactorStatus.AVAILABLE,
            signed_signal=signal, confidence=confidence, horizon="INTRADAY",
            observed_at=observed, effective_at=observed,
            valid_until=observed + timedelta(minutes=15), source_hashes=(str(source_hash),),
            reasons=("ACTUAL_POSITIONING_READ_MODEL_GEX_PROXY",),
            payload_hash=canonical_hash(row),
        ))
    return max(candidates, key=lambda item: (item.observed_at, item.payload_hash), default=None)


def _runtime_price_trend_factor(payload: object, *, symbol: str, as_of: datetime) -> FactorEvidence | None:
    cutoff = utc_datetime(as_of, field="trend cutoff")
    candidates: list[FactorEvidence] = []
    for row in _candidate_rows(payload):
        if _row_symbol(row) != symbol.strip().upper():
            continue
        basis = row.get("underlying_quote_basis")
        basis_hash = row.get("underlying_quote_basis_hash")
        if not isinstance(basis, Mapping) or basis_hash != canonical_hash(basis):
            continue
        observed = _aware_time(basis.get("observed_at"))
        bid = _decimal_or_none(basis.get("bid"))
        ask = _decimal_or_none(basis.get("ask"))
        last = _decimal_or_none(basis.get("last"))
        close = _decimal_or_none(basis.get("close"))
        if observed is None or observed > cutoff or cutoff - observed > timedelta(minutes=15):
            continue
        measurement = _quote_trend_measurement(
            bid=bid,
            ask=ask,
            last=last,
            close=close,
        )
        if measurement is None:
            continue
        signal, confidence = measurement
        candidates.append(FactorEvidence(
            factor=FactorKind.TREND_VOLATILITY, status=FactorStatus.AVAILABLE,
            signed_signal=signal, confidence=confidence, horizon="INTRADAY",
            observed_at=observed, effective_at=observed,
            valid_until=observed + timedelta(minutes=15), source_hashes=(str(basis_hash),),
            reasons=(
                "HASH_BOUND_UNDERLYING_PRICE_TREND_VOLATILITY_NOT_INFERRED",
                "CONFIDENCE_FROM_MEASURED_BID_ASK_QUALITY",
            ),
            payload_hash=canonical_hash(
                {
                    "symbol": symbol.strip().upper(),
                    "basis_hash": basis_hash,
                    "signal": signal,
                    "confidence": confidence,
                }
            ),
        ))
    return max(candidates, key=lambda item: (item.observed_at, item.payload_hash), default=None)


def _runtime_regime_factor(payload: object, *, symbol: str, as_of: datetime) -> FactorEvidence | None:
    cutoff = utc_datetime(as_of, field="regime cutoff")
    candidates: list[FactorEvidence] = []
    for row in _candidate_rows(payload):
        regime = row.get("market_credit_regime")
        regime_hash = row.get("regime_hash")
        if _row_symbol(row) != symbol.strip().upper() or not isinstance(regime, Mapping):
            continue
        if regime_hash != canonical_hash(regime):
            continue
        observed = _aware_time(regime.get("observed_at"))
        signal = _decimal_or_none(regime.get("signed_signal"))
        confidence = _decimal_or_none(regime.get("confidence"))
        if observed is None or observed > cutoff or cutoff - observed > timedelta(hours=24):
            continue
        if signal is None or confidence is None or not Decimal("-1") <= signal <= Decimal("1") or not Decimal("0") <= confidence <= Decimal("1"):
            continue
        candidates.append(FactorEvidence(
            factor=FactorKind.REGIME, status=FactorStatus.AVAILABLE,
            signed_signal=signal, confidence=confidence, horizon="1D",
            observed_at=observed, effective_at=observed,
            valid_until=observed + timedelta(hours=24), source_hashes=(str(regime_hash),),
            reasons=("HASH_BOUND_MARKET_CREDIT_REGIME",), payload_hash=canonical_hash(regime),
        ))
    return max(candidates, key=lambda item: (item.observed_at, item.payload_hash), default=None)


def _candidate_rows(payload: object) -> tuple[Mapping[str, object], ...]:
    root = _mapping(payload)
    values: list[Mapping[str, object]] = []
    for key in ("candidates", "rows", "coarse_contracts"):
        rows = root.get(key)
        if isinstance(rows, (list, tuple)):
            values.extend(_mapping(item) for item in rows)
    immutable = root.get("immutable_inputs")
    if isinstance(immutable, Mapping):
        universe = immutable.get("universe")
        if isinstance(universe, Mapping):
            rows = universe.get("coarse_contracts")
            if isinstance(rows, (list, tuple)):
                values.extend(_mapping(item) for item in rows)
    return tuple(values)


def _row_symbol(row: Mapping[str, object]) -> str:
    return str(row.get("symbol") or row.get("underlying") or "").strip().upper()


def _aware_time(value: object) -> datetime | None:
    try:
        parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
        return utc_datetime(parsed, field="cached evidence time")
    except (TypeError, ValueError):
        return None


def _decimal_or_none(value: object) -> Decimal | None:
    try:
        parsed = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def _is_digest(value: object) -> bool:
    return isinstance(value, str) and bool(_DIGEST.fullmatch(value))


__all__ = [
    "EquityPoolBuildResult",
    "EquityPoolService",
    "read_hash_bound_factor",
    "read_hash_bound_liquidity",
    "read_runtime_factor",
    "read_runtime_liquidity",
]
