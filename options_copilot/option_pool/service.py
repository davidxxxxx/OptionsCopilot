"""Deterministic capture and template-disposition policy for G036."""

from __future__ import annotations

from options_copilot.option_pool.leg_identity import normalise_option_right, normalise_option_side

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Callable, Iterable, Mapping

from options_copilot.equity_pool.reference import normalize_equity_pool_reference
from options_copilot.domain import OptionContract, OptionLeg, OptionRight, PositionSide
from options_copilot.storage.canonical import canonical_hash, freeze_json, utc_datetime
from options_copilot.strategies import (
    GeneratedStrategyCandidate,
    StrategyKind,
    StrategyTemplateRegistry,
    TemplateValidationError,
)

from .models import (
    OptionStructureDecision,
    OptionStructurePoolSnapshot,
    StructureDisposition,
    ThesisClass,
    normalize_equity_thesis_row,
    normalize_equity_theses,
    option_candidate_identity,
    candidate_quote_age_seconds,
    thesis_class_for_direction,
)
from .store import OptionStructurePoolStore


_DIRECTIONAL = frozenset({
    StrategyKind.LONG_OPTION,
    StrategyKind.DEBIT_VERTICAL,
    StrategyKind.CREDIT_VERTICAL,
})
_RANGE = frozenset({StrategyKind.BUTTERFLY, StrategyKind.IRON_CONDOR})
_TERM = frozenset({StrategyKind.CALENDAR, StrategyKind.DIAGONAL})
_LEG_REQUIRED = (
    "con_id", "contract_id_ex", "expiration", "strike", "right", "side",
    "ratio", "multiplier", "exchange", "bid", "ask",
    "market_data_type", "quote_age_seconds", "delta", "gamma", "theta",
    "vega", "volume", "open_interest", "liquidity",
)
_ECONOMICS_REQUIRED = (
    "max_loss_usd", "breakevens", "scenario_pnl",
    "estimated_commissions_usd", "estimated_slippage_usd", "all_in_cost_usd",
    "after_cost_ev_usd", "final_costs", "invalidation_evidence", "assignment_evidence",
    "ex_dividend_evidence",
)


@dataclass(frozen=True, slots=True)
class FinalizedOptionPoolCandidate:
    """Typed final-pipeline authority for exact option-pool capture."""

    payload: Mapping[str, object]
    candidate_hash: str
    source_candidate_hash: str
    broker_snapshot_hash: str
    quote_batch_id: str
    strategy_nav_hash: str
    secdef_hash: str
    signed_cost_hash: str
    scenario_hash: str

    def __post_init__(self) -> None:
        frozen = freeze_json(self.payload)
        if not isinstance(frozen, Mapping):
            raise TypeError("finalized option-pool payload must be canonical")
        for field in (
            "candidate_hash",
            "source_candidate_hash",
            "broker_snapshot_hash",
            "strategy_nav_hash",
            "secdef_hash",
            "signed_cost_hash",
            "scenario_hash",
        ):
            _required_hash(getattr(self, field), field)
        quote_batch_id = (
            self.quote_batch_id.strip()
            if isinstance(self.quote_batch_id, str)
            else ""
        )
        if not quote_batch_id:
            raise ValueError("quote_batch_id is invalid")
        if canonical_hash(frozen) != self.candidate_hash:
            raise ValueError("finalized candidate hash mismatch")
        final_costs = frozen.get("final_costs")
        if (
            frozen.get("source_candidate_hash") != self.source_candidate_hash
            or frozen.get("broker_snapshot_hash") != self.broker_snapshot_hash
            or frozen.get("quote_batch_id") != quote_batch_id
            or frozen.get("strategy_nav_hash") != self.strategy_nav_hash
            or frozen.get("secdef_hash") != self.secdef_hash
            or not isinstance(final_costs, Mapping)
            or final_costs.get("cost_hash") != self.signed_cost_hash
            or canonical_hash(frozen.get("scenario_pnl")) != self.scenario_hash
        ):
            raise ValueError("finalized option-pool authority binding mismatch")
        object.__setattr__(self, "payload", frozen)
        object.__setattr__(self, "quote_batch_id", quote_batch_id)


class OptionStructurePoolService:
    """Persist exact-contract structures plus every template disposition."""

    def __init__(
        self,
        store: OptionStructurePoolStore,
        *,
        clock: Callable[[], datetime] | None = None,
        quote_freshness_seconds: int = 5,
    ) -> None:
        if not isinstance(store, OptionStructurePoolStore):
            raise TypeError("store must be OptionStructurePoolStore")
        if isinstance(quote_freshness_seconds, bool) or quote_freshness_seconds <= 0:
            raise ValueError("quote_freshness_seconds must be positive")
        self.store = store
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self.quote_freshness_seconds = quote_freshness_seconds

    def capture_generation(
        self,
        *,
        scan_run_id: str,
        candidates: Iterable[object],
        observed_at: datetime,
        equity_pool_reference: Mapping[str, object],
        equity_theses: Mapping[str, object] | None = None,
        thesis_observed_at: datetime | None = None,
        reason_codes: Iterable[object] = (),
    ) -> OptionStructurePoolSnapshot:
        reference = normalize_equity_pool_reference(equity_pool_reference)
        if reference is None:
            raise ValueError("equity_pool_reference is required")
        thesis_at = utc_datetime(
            thesis_observed_at or observed_at,
            field="option pool thesis_observed_at",
        )
        selected_symbols = tuple(
            str(item).strip().upper() for item in reference["selected_symbols"]
        )
        thesis_by_symbol = normalize_equity_theses(
            equity_theses,
            equity_pool_reference=reference,
        )
        captured_by_symbol: dict[str, list[_CapturedCandidate]] = defaultdict(list)
        for value in candidates:
            captured = _capture_candidate(value)
            if captured.symbol not in selected_symbols:
                raise ValueError("candidate underlying is absent from equity pool")
            captured_by_symbol[captured.symbol].append(captured)

        decisions: list[OptionStructureDecision] = []
        for symbol in sorted(selected_symbols):
            actual = sorted(
                captured_by_symbol.get(symbol, ()),
                key=lambda item: (item.structure.value, item.identity, item.candidate_id),
            )
            thesis_evidence = thesis_by_symbol.get(symbol)
            thesis = (
                ThesisClass.UNCERTAIN
                if thesis_evidence is None
                else thesis_class_for_direction(
                    str(thesis_evidence["direction_label"])
                )
            )
            symbol_thesis_at = (
                thesis_at
                if thesis_evidence is None
                else datetime.fromisoformat(str(thesis_evidence["observed_at"]))
            )
            seen_structures: set[StrategyKind] = set()
            for candidate in actual:
                seen_structures.add(candidate.structure)
                completeness_reasons = _candidate_completeness_reasons(
                    candidate.payload,
                    structure=candidate.structure,
                    observed_at=observed_at,
                )
                if not candidate.exact_authority:
                    completeness_reasons = tuple(dict.fromkeys((
                        "UNTRUSTED_OPTION_POOL_CANDIDATE_SOURCE",
                        *completeness_reasons,
                    )))
                if thesis_evidence is None:
                    completeness_reasons = tuple(dict.fromkeys((
                        "EQUITY_THESIS_EVIDENCE_UNAVAILABLE",
                        *completeness_reasons,
                    )))
                elif thesis is ThesisClass.UNCERTAIN:
                    completeness_reasons = tuple(dict.fromkeys((
                        "EQUITY_THESIS_UNCERTAIN",
                        *completeness_reasons,
                    )))
                disposition = (
                    StructureDisposition.EXACT_EVIDENCE_CAPTURED
                    if not completeness_reasons
                    else StructureDisposition.RESEARCH_ONLY
                )
                decisions.append(OptionStructureDecision(
                    underlying=symbol,
                    thesis_class=thesis,
                    thesis_observed_at=symbol_thesis_at,
                    equity_pool_reference=reference,
                    equity_thesis_evidence=thesis_evidence,
                    structure=candidate.structure,
                    disposition=disposition,
                    reason_codes=(
                        ("FRESH_EXACT_OPTION_EVIDENCE_CAPTURED",)
                        if disposition is StructureDisposition.EXACT_EVIDENCE_CAPTURED
                        else completeness_reasons
                    ),
                    candidate_identity=candidate.identity,
                    candidate_id=candidate.candidate_id,
                    candidate_hash=candidate.candidate_hash,
                    exact_economics=candidate.payload,
                ))
            for structure in StrategyKind:
                if structure in seen_structures:
                    continue
                disposition, reasons = _missing_disposition(thesis, structure)
                if thesis_evidence is None:
                    disposition = StructureDisposition.RESEARCH_ONLY
                    reasons = tuple(dict.fromkeys((
                        "EQUITY_THESIS_EVIDENCE_UNAVAILABLE",
                        *reasons,
                    )))
                decisions.append(OptionStructureDecision(
                    underlying=symbol,
                    thesis_class=thesis,
                    thesis_observed_at=symbol_thesis_at,
                    equity_pool_reference=reference,
                    equity_thesis_evidence=thesis_evidence,
                    structure=structure,
                    disposition=disposition,
                    reason_codes=reasons,
                ))

        snapshot = OptionStructurePoolSnapshot(
            scan_run_id=scan_run_id,
            observed_at=observed_at,
            decisions=tuple(decisions),
            generation_reason_codes=tuple(reason_codes),
        )
        return self.store.append(snapshot)

    def capture_dispositions(
        self,
        *,
        scan_run_id: str,
        observed_at: datetime,
        equity_pool_reference: Mapping[str, object],
        equity_theses: Mapping[str, object] | None = None,
        thesis_observed_at: datetime | None = None,
        reason_codes: Iterable[object] = (),
    ) -> OptionStructurePoolSnapshot:
        """Persist zero-candidate template reasons on a terminal research failure."""

        return self.capture_generation(
            scan_run_id=scan_run_id,
            candidates=(),
            observed_at=observed_at,
            equity_pool_reference=equity_pool_reference,
            equity_theses=equity_theses,
            thesis_observed_at=thesis_observed_at,
            reason_codes=reason_codes,
        )

    def capture_research_candidates(
        self,
        *,
        scan_run_id: str,
        candidates: Iterable[object],
        observed_at: datetime,
        equity_pool_reference: Mapping[str, object],
        equity_theses: Mapping[str, object] | None = None,
        reason_codes: Iterable[object] = (),
        research_reason_codes: Iterable[object] = (
            "AFTER_HOURS_EXACT_IDENTITY_RESEARCH_ONLY",
        ),
        commit_guard: Callable[[], bool] | None = None,
    ) -> OptionStructurePoolSnapshot:
        """Persist only real hash-bound candidates as non-executable research."""

        reference = normalize_equity_pool_reference(equity_pool_reference)
        if reference is None:
            raise ValueError("equity_pool_reference is required")
        selected = tuple(
            str(item).strip().upper() for item in reference["selected_symbols"]
        )
        discovered = tuple(
            str(item).strip().upper()
            for item in reference.get("discovered_symbols", selected)
        )
        thesis_by_symbol = normalize_equity_theses(
            equity_theses,
            equity_pool_reference=reference,
        )
        research_reasons = tuple(
            dict.fromkeys(
                str(item).strip().upper()
                for item in research_reason_codes
                if str(item).strip()
            )
        )
        if not research_reasons:
            raise ValueError("research_reason_codes cannot be empty")
        decisions: list[OptionStructureDecision] = []
        for value in candidates:
            candidate = _capture_candidate(value)
            if candidate.symbol not in discovered:
                raise ValueError("candidate underlying is absent from equity discovery")
            completeness = _candidate_completeness_reasons(
                candidate.payload,
                structure=candidate.structure,
                observed_at=observed_at,
            )
            thesis_evidence = thesis_by_symbol.get(candidate.symbol)
            thesis = (
                ThesisClass.UNCERTAIN
                if thesis_evidence is None
                else thesis_class_for_direction(
                    str(thesis_evidence["direction_label"])
                )
            )
            thesis_reasons = (
                ("EQUITY_THESIS_EVIDENCE_UNAVAILABLE",)
                if thesis_evidence is None
                else ("EQUITY_THESIS_UNCERTAIN",)
                if thesis is ThesisClass.UNCERTAIN
                else ()
            )
            decisions.append(
                OptionStructureDecision(
                    underlying=candidate.symbol,
                    thesis_class=thesis,
                    thesis_observed_at=(
                        observed_at
                        if thesis_evidence is None
                        else datetime.fromisoformat(
                            str(thesis_evidence["observed_at"])
                        )
                    ),
                    equity_pool_reference=reference,
                    equity_thesis_evidence=thesis_evidence,
                    structure=candidate.structure,
                    disposition=StructureDisposition.RESEARCH_ONLY,
                    reason_codes=tuple(
                        dict.fromkeys(
                            (
                                *research_reasons,
                                *thesis_reasons,
                                *completeness,
                            )
                        )
                    ),
                    candidate_identity=candidate.identity,
                    candidate_id=candidate.candidate_id,
                    candidate_hash=candidate.candidate_hash,
                    exact_economics=candidate.payload,
                )
            )
        snapshot = OptionStructurePoolSnapshot(
            scan_run_id=scan_run_id,
            observed_at=observed_at,
            decisions=tuple(decisions),
            generation_reason_codes=tuple(reason_codes),
        )
        return self.store.append(snapshot, commit_guard=commit_guard)

    def latest_payload(self) -> dict[str, object]:
        snapshot = self.store.latest()
        if snapshot is None:
            return {
                "status": "UNAVAILABLE",
                "decision": "NO_TRADE",
                "reason_codes": ("OPTION_STRUCTURE_POOL_UNAVAILABLE",),
                "decisions": (),
                "exact_count": 0,
                "research_only_count": 0,
                "excluded_count": 0,
                "decision_authority": "SUPPORTING_ONLY",
                "entry_authority": False,
                "approval_eligible": False,
                "instruction_creation_allowed": False,
                "order_allowed": False,
            }
        payload = snapshot.read_projection(
            now=self._clock(),
            quote_freshness_seconds=self.quote_freshness_seconds,
        )
        return {"status": "READY", "decision": "RESEARCH_ONLY", **payload}


class _CapturedCandidate:
    __slots__ = (
        "candidate_hash", "candidate_id", "identity", "payload", "structure",
        "symbol", "underlying_quote_basis", "exact_authority",
    )

    def __init__(
        self,
        *,
        payload: Mapping[str, object],
        candidate_hash: str,
        exact_authority: bool,
    ) -> None:
        frozen = freeze_json(payload)
        if not isinstance(frozen, Mapping):
            raise TypeError("candidate payload must be canonical")
        if canonical_hash(frozen) != candidate_hash:
            raise ValueError("candidate hash does not bind exact economics")
        self.payload = frozen
        self.candidate_hash = candidate_hash
        self.exact_authority = exact_authority
        self.candidate_id = str(frozen.get("candidate_id", "")).strip()
        if not self.candidate_id:
            raise ValueError("candidate_id is required")
        self.symbol = str(frozen.get("symbol", "")).strip().upper()
        if not self.symbol:
            raise ValueError("candidate symbol is required")
        self.structure = StrategyKind(str(frozen.get("structure", "")))
        self.identity = option_candidate_identity(frozen)
        basis = frozen.get("underlying_quote_basis")
        self.underlying_quote_basis = basis if isinstance(basis, Mapping) else None


def _capture_candidate(value: object) -> _CapturedCandidate:
    if isinstance(value, FinalizedOptionPoolCandidate):
        return _CapturedCandidate(
            payload=value.payload,
            candidate_hash=value.candidate_hash,
            exact_authority=True,
        )
    if isinstance(value, GeneratedStrategyCandidate):
        return _CapturedCandidate(
            payload=value.hash_payload(),
            candidate_hash=value.candidate_hash,
            exact_authority=False,
        )
    if isinstance(value, Mapping):
        candidate_hash = value.get("candidate_hash")
        payload = value.get("payload")
        if not isinstance(candidate_hash, str) or not isinstance(payload, Mapping):
            raise TypeError("mapping candidate requires candidate_hash and payload")
        return _CapturedCandidate(
            payload=payload,
            candidate_hash=candidate_hash,
            exact_authority=False,
        )
    raise TypeError("option pool accepts generated or hash-bound mapping candidates only")


def _candidate_completeness_reasons(
    payload: Mapping[str, object],
    *,
    structure: StrategyKind,
    observed_at: datetime,
) -> tuple[str, ...]:
    reasons: list[str] = []
    legs = payload.get("legs")
    if not isinstance(legs, tuple) or not legs:
        return ("EXACT_CONTRACT_LEGS_UNAVAILABLE",)
    for leg in legs:
        if not isinstance(leg, Mapping):
            reasons.append("EXACT_CONTRACT_LEGS_INVALID")
            continue
        missing = {field for field in _LEG_REQUIRED if leg.get(field) is None}
        if (
            {"con_id", "contract_id_ex", "expiration", "strike", "right", "side"} & missing
            or not _valid_contract_identity(
                leg,
                expected_symbol=str(payload.get("symbol", "")),
            )
        ):
            reasons.append("EXACT_CONTRACT_IDENTITY_INCOMPLETE")
        if (
            {"bid", "ask", "market_data_type", "quote_age_seconds"} & missing
            or not _valid_quote(leg, now=observed_at)
        ):
            reasons.append("EXECUTABLE_LEG_QUOTE_INCOMPLETE")
        if {"delta", "gamma", "theta", "vega"} & missing or not _valid_greeks(leg):
            reasons.append("OPTION_GREEKS_INCOMPLETE")
        if (
            {"volume", "open_interest", "liquidity"} & missing
            or not _valid_liquidity(leg)
        ):
            reasons.append("OPTION_LIQUIDITY_EVIDENCE_INCOMPLETE")
    if not _valid_structure_semantics(
        payload,
        structure=structure,
        observed_at=observed_at,
    ):
        reasons.append("STRUCTURE_TEMPLATE_SEMANTICS_INVALID")
    economics_missing = {field for field in _ECONOMICS_REQUIRED if payload.get(field) is None}
    max_profit = payload.get("max_profit_usd")
    max_profit_valid = (
        _finite_decimal(max_profit) is not None
        or (max_profit is None and payload.get("max_profit_type") == "UNBOUNDED")
    )
    dte = payload.get("dte")
    if (
        {"max_loss_usd", "breakevens", "scenario_pnl"} & economics_missing
        or _finite_decimal(payload.get("max_loss_usd"), positive=True) is None
        or not max_profit_valid
        or not isinstance(payload.get("breakevens"), tuple)
        or not payload.get("breakevens")
        or not isinstance(payload.get("scenario_pnl"), tuple)
        or not payload.get("scenario_pnl")
    ):
        reasons.append("STRUCTURE_PAYOFF_EVIDENCE_INCOMPLETE")
    elif not _valid_payoff_evidence(payload):
        reasons.append("STRUCTURE_PAYOFF_EVIDENCE_INCOMPLETE")
    if (
        isinstance(dte, bool)
        or not isinstance(dte, int)
        or dte < 0
        or not _valid_dte_binding(payload, observed_at=observed_at)
    ):
        reasons.append("DTE_EVIDENCE_INCOMPLETE")
    if (
        {"estimated_commissions_usd", "estimated_slippage_usd", "all_in_cost_usd", "after_cost_ev_usd", "final_costs"} & economics_missing
        or not isinstance(payload.get("final_costs"), Mapping)
    ):
        reasons.append("AFTER_COST_ECONOMICS_INCOMPLETE")
    elif not _valid_after_cost_economics(payload, require_positive=False):
        reasons.append("AFTER_COST_ECONOMICS_INCOMPLETE")
    elif (
        (after_cost_ev := _finite_decimal(payload.get("after_cost_ev_usd")))
        is not None
        and after_cost_ev <= 0
    ):
        reasons.append("CANDIDATE_AFTER_COST_EV_NONPOSITIVE")
    invalidation = payload.get("invalidation_evidence")
    if (
        "invalidation_evidence" in economics_missing
        or not isinstance(invalidation, Mapping)
        or invalidation.get("status") != "BOUND"
    ):
        reasons.append("THESIS_INVALIDATION_EVIDENCE_INCOMPLETE")
    assignment = payload.get("assignment_evidence")
    ex_dividend = payload.get("ex_dividend_evidence")
    short_count = sum(
        str(leg.get("side", "")).upper() in {"SELL", "SHORT"}
        for leg in legs
        if isinstance(leg, Mapping)
    )
    expected_status = "SUPPORTED" if short_count else "NOT_APPLICABLE"
    top_level_short_risk = (
        isinstance(assignment, Mapping)
        and isinstance(ex_dividend, Mapping)
        and assignment.get("status") == expected_status
        and ex_dividend.get("status") == expected_status
    )
    per_leg_short_risk = all(
        str(leg.get("side", "")).upper() not in {"SELL", "SHORT"}
        or (
            isinstance(leg.get("short_leg_risk_evidence"), Mapping)
            and leg["short_leg_risk_evidence"].get("status") == "SUPPORTED"
        )
        for leg in legs
        if isinstance(leg, Mapping)
    )
    if not top_level_short_risk or (short_count > 0 and not per_leg_short_risk):
        reasons.append("ASSIGNMENT_AND_EX_DIVIDEND_EVIDENCE_INCOMPLETE")
    if structure in _TERM:
        reasons.append("CROSS_EXPIRY_ECONOMICS_UNSUPPORTED")
    return tuple(dict.fromkeys(reasons))


def _missing_disposition(
    thesis: ThesisClass,
    structure: StrategyKind,
) -> tuple[StructureDisposition, tuple[str, ...]]:
    if structure in _TERM:
        return StructureDisposition.RESEARCH_ONLY, (
            "CROSS_EXPIRY_ECONOMICS_UNSUPPORTED",
            "ASSIGNMENT_AND_EX_DIVIDEND_EVIDENCE_INCOMPLETE",
        )
    if thesis in {ThesisClass.DIRECTIONAL_BULLISH, ThesisClass.DIRECTIONAL_BEARISH}:
        if structure in _DIRECTIONAL:
            return StructureDisposition.RESEARCH_ONLY, ("EXACT_CONTRACT_ECONOMICS_NOT_CAPTURED",)
        return StructureDisposition.EXCLUDED, ("TEMPLATE_NOT_APPROPRIATE_FOR_DIRECTIONAL_THESIS",)
    if thesis is ThesisClass.RANGE_BOUND:
        if structure in _RANGE:
            return StructureDisposition.RESEARCH_ONLY, (
                "SKEW_AND_WING_LIQUIDITY_EVIDENCE_INCOMPLETE",
                "ASSIGNMENT_AND_EX_DIVIDEND_EVIDENCE_INCOMPLETE",
            )
        return StructureDisposition.EXCLUDED, ("TEMPLATE_NOT_APPROPRIATE_FOR_RANGE_THESIS",)
    return StructureDisposition.RESEARCH_ONLY, (
        "THESIS_CLASS_UNCERTAIN",
        "EXACT_CONTRACT_ECONOMICS_NOT_CAPTURED",
    )


def _decimal(value: object) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return parsed if parsed.is_finite() and parsed > 0 else None


def _finite_decimal(value: object, *, positive: bool = False) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    if not parsed.is_finite() or (positive and parsed <= 0):
        return None
    return parsed


def _valid_contract_identity(
    leg: Mapping[str, object],
    *,
    expected_symbol: str,
) -> bool:
    con_id = leg.get("con_id")
    if isinstance(con_id, bool) or not isinstance(con_id, int) or con_id <= 0:
        return False
    expiration = leg.get("expiration")
    strike = _finite_decimal(leg.get("strike"), positive=True)
    try:
        right = normalise_option_right(leg.get("right"))
        side = normalise_option_side(leg.get("side"))
    except ValueError:
        return False
    exchange = str(leg.get("exchange", "")).strip().upper()
    contract_id_ex = str(leg.get("contract_id_ex", "")).strip()
    underlying = str(leg.get("underlying", expected_symbol)).strip().upper()
    ratio = leg.get("ratio")
    multiplier = _finite_decimal(leg.get("multiplier"), positive=True)
    if not isinstance(expiration, str):
        return False
    try:
        parsed_expiration = date.fromisoformat(expiration)
    except ValueError:
        return False
    return (
        parsed_expiration.isoformat() == expiration
        and strike is not None
        and right in {"CALL", "PUT"}
        and side in {"LONG", "SHORT"}
        and bool(exchange)
        and contract_id_ex == f"{con_id}@{exchange}"
        and underlying == expected_symbol.strip().upper()
        and not isinstance(ratio, bool)
        and isinstance(ratio, int)
        and ratio > 0
        and multiplier is not None
    )


def _valid_quote(leg: Mapping[str, object], *, now: datetime) -> bool:
    bid = _finite_decimal(leg.get("bid"))
    ask = _finite_decimal(leg.get("ask"))
    market_data_type = leg.get("market_data_type")
    timestamp = leg.get("exchange_timestamp", leg.get("exchange_time"))
    if bid is None or ask is None or bid < 0 or ask < bid:
        return False
    if market_data_type not in {1, "1", "LIVE", "REALTIME"}:
        return False
    if not isinstance(timestamp, str):
        return False
    try:
        parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    except ValueError:
        return False
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return False
    age = candidate_quote_age_seconds({"legs": (leg,)}, now=now)
    return age is not None and Decimal("0") <= age <= Decimal("5")


def _valid_greeks(leg: Mapping[str, object]) -> bool:
    delta = _finite_decimal(leg.get("delta"))
    gamma = _finite_decimal(leg.get("gamma"))
    theta = _finite_decimal(leg.get("theta"))
    vega = _finite_decimal(leg.get("vega"))
    return (
        delta is not None
        and Decimal("-1") <= delta <= Decimal("1")
        and gamma is not None
        and gamma >= 0
        and theta is not None
        and vega is not None
        and vega >= 0
    )


def _valid_liquidity(leg: Mapping[str, object]) -> bool:
    for field in ("volume", "open_interest"):
        value = leg.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            return False
    liquidity = leg.get("liquidity")
    if not isinstance(liquidity, Mapping) or liquidity.get("status") != "MEASURED":
        return False
    bid = _finite_decimal(leg.get("bid"))
    ask = _finite_decimal(leg.get("ask"))
    spread = _finite_decimal(liquidity.get("bid_ask_spread"))
    return (
        bid is not None
        and ask is not None
        and spread is not None
        and spread == ask - bid
        and liquidity.get("volume") == leg.get("volume")
        and liquidity.get("open_interest") == leg.get("open_interest")
    )


def _valid_payoff_evidence(payload: Mapping[str, object]) -> bool:
    breakevens = payload.get("breakevens")
    scenarios = payload.get("scenario_pnl")
    if not isinstance(breakevens, tuple) or not all(
        _finite_decimal(value, positive=True) is not None for value in breakevens
    ):
        return False
    if not isinstance(scenarios, tuple) or not scenarios:
        return False
    max_loss = _finite_decimal(payload.get("max_loss_usd"), positive=True)
    raw_max_profit = payload.get("max_profit_usd")
    max_profit = _finite_decimal(raw_max_profit)
    if max_loss is None or (
        raw_max_profit is not None and (max_profit is None or max_profit < 0)
    ):
        return False
    probability_total = Decimal("0")
    for raw in scenarios:
        if not isinstance(raw, Mapping):
            return False
        terminal = _finite_decimal(
            raw.get("terminal_underlying_price", raw.get("underlying_price")),
            positive=True,
        )
        probability = _finite_decimal(raw.get("probability"), positive=True)
        pnl = _finite_decimal(raw.get("pnl_usd"))
        if terminal is None or probability is None or pnl is None:
            return False
        if pnl < -max_loss or (max_profit is not None and pnl > max_profit):
            return False
        probability_total += probability
    return probability_total == Decimal("1")


def _valid_structure_semantics(
    payload: Mapping[str, object],
    *,
    structure: StrategyKind,
    observed_at: datetime,
) -> bool:
    raw_legs = payload.get("legs")
    if not isinstance(raw_legs, tuple) or not raw_legs:
        return False
    resolved: list[OptionLeg] = []
    try:
        for raw in raw_legs:
            if not isinstance(raw, Mapping):
                return False
            side = normalise_option_side(raw.get("side"))
            resolved.append(OptionLeg(
                contract=OptionContract(
                    contract_id=str(raw["contract_id_ex"]),
                    underlying=str(raw.get("underlying", payload.get("symbol", ""))),
                    expiration=date.fromisoformat(str(raw["expiration"])),
                    strike=Decimal(str(raw["strike"])),
                    right=OptionRight(normalise_option_right(raw["right"])),
                    multiplier=Decimal(str(raw["multiplier"])),
                    currency=str(raw.get("currency", "USD")),
                    exchange=str(raw["exchange"]),
                    broker_contract_id=int(raw["con_id"]),
                ),
                side=PositionSide(side),
                quantity=int(raw["ratio"]),
            ))
        StrategyTemplateRegistry().get(structure).validate(tuple(resolved))
    except (ArithmeticError, KeyError, TypeError, ValueError, TemplateValidationError):
        return False
    # Research rows may bind exact leg geometry before executable economics.
    # Dedicated completeness reasons below own missing payoff and cost evidence.
    economics_fields = ("debit_usd", "credit_usd", "max_loss_usd")
    economics_present = tuple(field in payload for field in economics_fields)
    if any(economics_present):
        if not all(economics_present):
            return False
        debit = _finite_decimal(payload.get("debit_usd"))
        credit = _finite_decimal(payload.get("credit_usd"))
        max_loss = _finite_decimal(payload.get("max_loss_usd"), positive=True)
        if debit is None or credit is None or debit < 0 or credit < 0 or max_loss is None:
            return False
        if structure in {
            StrategyKind.LONG_OPTION,
            StrategyKind.DEBIT_VERTICAL,
            StrategyKind.BUTTERFLY,
            StrategyKind.CALENDAR,
            StrategyKind.DIAGONAL,
        } and debit <= credit:
            return False
        if (
            structure in {StrategyKind.CREDIT_VERTICAL, StrategyKind.IRON_CONDOR}
            and credit <= debit
        ):
            return False
    return True


def _valid_dte_binding(
    payload: Mapping[str, object],
    *,
    observed_at: datetime,
) -> bool:
    legs = payload.get("legs")
    declared = payload.get("dte")
    if (
        not isinstance(legs, tuple)
        or not legs
        or isinstance(declared, bool)
        or not isinstance(declared, int)
    ):
        return False
    expirations: list[date] = []
    try:
        for leg in legs:
            if not isinstance(leg, Mapping):
                return False
            expiration = date.fromisoformat(str(leg["expiration"]))
            expirations.append(expiration)
    except (KeyError, ValueError):
        return False
    cutoff = utc_datetime(observed_at, field="option pool observed_at").date()
    derived = (min(expirations) - cutoff).days
    return derived >= 0 and declared == derived


def _required_hash(value: object, field: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(char not in "0123456789abcdef" for char in value)
    ):
        raise ValueError(f"{field} is invalid")
    return value


def _valid_after_cost_economics(
    payload: Mapping[str, object],
    *,
    require_positive: bool = True,
) -> bool:
    commission = _finite_decimal(payload.get("estimated_commissions_usd"))
    slippage = _finite_decimal(payload.get("estimated_slippage_usd"))
    all_in = _finite_decimal(payload.get("all_in_cost_usd"))
    after_cost_ev = _finite_decimal(payload.get("after_cost_ev_usd"))
    final_costs = payload.get("final_costs")
    scenarios = payload.get("scenario_pnl")
    if (
        commission is None
        or commission < 0
        or slippage is None
        or slippage < 0
        or all_in is None
        or after_cost_ev is None
        or (require_positive and after_cost_ev <= 0)
        or not isinstance(final_costs, Mapping)
        or not isinstance(scenarios, tuple)
    ):
        return False
    # all_in is a signed net debit: a credit strategy can receive cash after
    # costs. Completeness depends on reconciliation, not on the cashflow sign.
    debit = _finite_decimal(payload.get("debit_usd"))
    credit = _finite_decimal(payload.get("credit_usd"))
    if (
        debit is None or credit is None or debit < 0 or credit < 0
        or all_in != debit - credit + commission + slippage
    ):
        return False
    final_commission = _finite_decimal(final_costs.get("commission_usd"))
    final_slippage = _finite_decimal(final_costs.get("slippage_usd"))
    execution_cost = _finite_decimal(final_costs.get("execution_cost_usd"))
    if (
        final_commission != commission
        or final_slippage != slippage
        or execution_cost != commission + slippage
        or not isinstance(final_costs.get("cost_version"), str)
        or not str(final_costs["cost_version"]).strip()
    ):
        return False
    cost_hash = final_costs.get("cost_hash")
    if (
        not isinstance(cost_hash, str)
        or len(cost_hash) != 64
        or any(char not in "0123456789abcdef" for char in cost_hash)
    ):
        return False
    expected = Decimal("0")
    for row in scenarios:
        if not isinstance(row, Mapping):
            return False
        probability = _finite_decimal(row.get("probability"), positive=True)
        pnl = _finite_decimal(row.get("pnl_usd"))
        if probability is None or pnl is None:
            return False
        expected += probability * pnl
    return expected == after_cost_ev


__all__ = ["FinalizedOptionPoolCandidate", "OptionStructurePoolService"]
