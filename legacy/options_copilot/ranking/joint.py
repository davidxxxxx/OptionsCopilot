"""Immutable joint equity-and-option ranking for review-only suggestions."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from enum import Enum

from options_copilot.option_pool.service import FinalizedOptionPoolCandidate
from options_copilot.storage.canonical import (
    canonical_hash,
    datetime_text,
    freeze_json,
    thaw_json,
    utc_datetime,
)


SCHEMA = "options_copilot.joint_ranking.v1"
ZERO = Decimal("0")
ONE = Decimal("1")
FIVE_SECONDS = Decimal("5")
MAX_CANDIDATE_RISK = Decimal("0.15")
MAX_AGGREGATE_RISK = Decimal("0.20")
INPUT_SCHEMA = "options_copilot.joint_ranking_input.v1"


class JointDisposition(str, Enum):
    EXECUTABLE_REVIEW = "EXECUTABLE_REVIEW"
    RESEARCH_ONLY = "RESEARCH_ONLY"


@dataclass(frozen=True, slots=True)
class JointRankingRow:
    candidate_id: str
    underlying: str
    candidate_hash: str
    disposition: JointDisposition
    rank: int | None
    score: Decimal
    score_components: Mapping[str, object]
    reason_codes: tuple[str, ...]
    row_hash: str

    def __post_init__(self) -> None:
        candidate_id = _text(self.candidate_id, "candidate_id")
        underlying = _text(self.underlying, "underlying").upper()
        candidate_hash = _hash(self.candidate_hash, "candidate_hash")
        disposition = JointDisposition(self.disposition)
        score = _decimal(self.score, "score", minimum=ZERO, maximum=Decimal("100"))
        components = freeze_json(self.score_components)
        if not isinstance(components, Mapping):
            raise TypeError("score_components must be a mapping")
        reasons = _codes(self.reason_codes)
        if disposition is JointDisposition.EXECUTABLE_REVIEW:
            if not isinstance(self.rank, int) or isinstance(self.rank, bool) or self.rank <= 0:
                raise ValueError("executable rows require a positive rank")
            if reasons:
                raise ValueError("executable rows cannot carry rejection reasons")
        elif self.rank is not None or not reasons:
            raise ValueError("research rows require reasons and cannot carry a rank")
        values = {
            "candidate_id": candidate_id,
            "underlying": underlying,
            "candidate_hash": candidate_hash,
            "disposition": disposition.value,
            "rank": self.rank,
            "score": score,
            "score_components": components,
            "reason_codes": reasons,
        }
        if canonical_hash(values) != _hash(self.row_hash, "row_hash"):
            raise ValueError("joint ranking row hash mismatch")
        object.__setattr__(self, "candidate_id", candidate_id)
        object.__setattr__(self, "underlying", underlying)
        object.__setattr__(self, "candidate_hash", candidate_hash)
        object.__setattr__(self, "disposition", disposition)
        object.__setattr__(self, "score", score)
        object.__setattr__(self, "score_components", components)
        object.__setattr__(self, "reason_codes", reasons)

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> "JointRankingRow":
        if not isinstance(value, Mapping):
            raise TypeError("joint ranking row must be a mapping")
        return cls(
            candidate_id=value.get("candidate_id"),
            underlying=value.get("underlying"),
            candidate_hash=value.get("candidate_hash"),
            disposition=value.get("disposition"),
            rank=value.get("rank"),
            score=value.get("score"),
            score_components=value.get("score_components", {}),
            reason_codes=value.get("reason_codes", ()),
            row_hash=value.get("row_hash"),
        )  # type: ignore[arg-type]

    @classmethod
    def build(
        cls,
        *,
        candidate_id: str,
        underlying: str,
        candidate_hash: str,
        disposition: JointDisposition,
        rank: int | None,
        score: Decimal,
        score_components: Mapping[str, object],
        reason_codes: Sequence[str] = (),
    ) -> "JointRankingRow":
        values = {
            "candidate_id": _text(candidate_id, "candidate_id"),
            "underlying": _text(underlying, "underlying").upper(),
            "candidate_hash": _hash(candidate_hash, "candidate_hash"),
            "disposition": JointDisposition(disposition).value,
            "rank": rank,
            "score": score,
            "score_components": freeze_json(score_components),
            "reason_codes": _codes(reason_codes),
        }
        return cls(**values, row_hash=canonical_hash(values))  # type: ignore[arg-type]

    def as_dict(self) -> dict[str, object]:
        return {
            "candidate_id": self.candidate_id,
            "underlying": self.underlying,
            "candidate_hash": self.candidate_hash,
            "disposition": self.disposition.value,
            "rank": self.rank,
            "score": self.score,
            "score_components": thaw_json(self.score_components),
            "reason_codes": self.reason_codes,
            "row_hash": self.row_hash,
        }


@dataclass(frozen=True, slots=True)
class JointRankingSnapshot:
    scan_run_id: str
    generated_at: datetime
    broker_snapshot_hash: str
    strategy_nav_hash: str
    executable: tuple[JointRankingRow, ...]
    research_watchlist: tuple[JointRankingRow, ...]
    input_hash: str
    snapshot_hash: str
    schema: str = SCHEMA
    review_only: bool = True
    direct_order_submission: bool = False

    def __post_init__(self) -> None:
        if self.schema != SCHEMA:
            raise ValueError("unsupported joint ranking schema")
        generated_at = utc_datetime(self.generated_at, field="generated_at")
        executable = tuple(self.executable)
        research = tuple(self.research_watchlist)
        if len(executable) > 10:
            raise ValueError("joint ranking permits at most ten executable rows")
        if tuple(row.rank for row in executable) != tuple(range(1, len(executable) + 1)):
            raise ValueError("executable ranks must be contiguous")
        if len({row.underlying for row in executable}) != len(executable):
            raise ValueError("executable rows must have unique underlyings")
        if any(row.disposition is not JointDisposition.EXECUTABLE_REVIEW for row in executable):
            raise ValueError("executable collection contains a research row")
        if any(row.disposition is not JointDisposition.RESEARCH_ONLY for row in research):
            raise ValueError("research watchlist contains an executable row")
        candidate_ids = tuple(row.candidate_id for row in executable + research)
        if len(candidate_ids) != len(set(candidate_ids)):
            raise ValueError("joint ranking candidate ids must be unique")
        body = {
            "schema": self.schema,
            "scan_run_id": _text(self.scan_run_id, "scan_run_id"),
            "generated_at": datetime_text(generated_at),
            "broker_snapshot_hash": _hash(self.broker_snapshot_hash, "broker_snapshot_hash"),
            "strategy_nav_hash": _hash(self.strategy_nav_hash, "strategy_nav_hash"),
            "executable": tuple(row.as_dict() for row in executable),
            "research_watchlist": tuple(row.as_dict() for row in research),
            "input_hash": _hash(self.input_hash, "input_hash"),
            "review_only": True,
            "direct_order_submission": False,
        }
        if not self.review_only or self.direct_order_submission:
            raise ValueError("joint ranking must remain review-only")
        if canonical_hash(body) != _hash(self.snapshot_hash, "snapshot_hash"):
            raise ValueError("joint ranking snapshot hash mismatch")
        object.__setattr__(self, "generated_at", generated_at)
        object.__setattr__(self, "executable", executable)
        object.__setattr__(self, "research_watchlist", research)

    def as_dict(self) -> dict[str, object]:
        return {
            "schema": self.schema,
            "scan_run_id": self.scan_run_id,
            "generated_at": self.generated_at,
            "broker_snapshot_hash": self.broker_snapshot_hash,
            "strategy_nav_hash": self.strategy_nav_hash,
            "executable": tuple(row.as_dict() for row in self.executable),
            "research_watchlist": tuple(row.as_dict() for row in self.research_watchlist),
            "input_hash": self.input_hash,
            "review_only": self.review_only,
            "direct_order_submission": self.direct_order_submission,
            "snapshot_hash": self.snapshot_hash,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> "JointRankingSnapshot":
        if not isinstance(value, Mapping):
            raise TypeError("joint ranking snapshot must be a mapping")
        executable = value.get("executable")
        research = value.get("research_watchlist")
        if not isinstance(executable, Sequence) or isinstance(
            executable, (str, bytes, bytearray)
        ) or not isinstance(research, Sequence) or isinstance(
            research, (str, bytes, bytearray)
        ):
            raise TypeError("joint ranking row collections are invalid")
        return cls(
            scan_run_id=value.get("scan_run_id"),
            generated_at=_optional_datetime(value.get("generated_at")),
            broker_snapshot_hash=value.get("broker_snapshot_hash"),
            strategy_nav_hash=value.get("strategy_nav_hash"),
            executable=tuple(JointRankingRow.from_dict(row) for row in executable),
            research_watchlist=tuple(
                JointRankingRow.from_dict(row) for row in research
            ),
            input_hash=value.get("input_hash"),
            snapshot_hash=value.get("snapshot_hash"),
            schema=value.get("schema", SCHEMA),
            review_only=value.get("review_only") is True,
            direct_order_submission=value.get("direct_order_submission") is True,
        )  # type: ignore[arg-type]


class JointRankingEngine:
    """Score complete exact candidates and keep hard admission independent."""

    def rank(
        self,
        candidates: Iterable[FinalizedOptionPoolCandidate],
        *,
        scan_run_id: str,
        now: datetime,
        equity_theses: Mapping[str, object],
        broker_snapshot_hash: str,
        strategy_nav_hash: str,
        gate_reasons_by_candidate: Mapping[str, Sequence[str]],
        open_position_underlyings: Iterable[str] = (),
        aggregate_open_risk_usd: Decimal = ZERO,
        concentration_by_underlying: Mapping[str, Decimal],
        limit: int = 10,
    ) -> JointRankingSnapshot:
        if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 10:
            raise ValueError("limit must be between 1 and 10")
        cutoff = utc_datetime(now, field="now")
        expected_broker = _hash(broker_snapshot_hash, "broker_snapshot_hash")
        expected_nav = _hash(strategy_nav_hash, "strategy_nav_hash")
        candidate_values = tuple(candidates)
        if any(
            not isinstance(candidate, FinalizedOptionPoolCandidate)
            for candidate in candidate_values
        ):
            raise TypeError("joint ranking accepts only FinalizedOptionPoolCandidate values")
        candidate_ids = {
            _text(candidate.payload.get("candidate_id"), "candidate_id")
            for candidate in candidate_values
        }
        candidate_symbols = {
            _text(candidate.payload.get("symbol"), "symbol").upper()
            for candidate in candidate_values
        }
        gates = gate_reasons_by_candidate
        if set(gates) != candidate_ids:
            raise ValueError("gate evidence must cover every finalized candidate exactly")
        blocked_underlyings = {_text(item, "open_position_underlying").upper() for item in open_position_underlyings}
        concentrations: dict[str, Decimal] = {}
        for raw_key, raw_value in concentration_by_underlying.items():
            normalized_key = _text(raw_key, "concentration_underlying").upper()
            if normalized_key in concentrations:
                raise ValueError("concentration evidence contains normalized duplicates")
            normalized_value = _optional_decimal(raw_value)
            if normalized_value is None or not ZERO <= normalized_value <= ONE:
                raise ValueError("concentration evidence value is invalid")
            concentrations[normalized_key] = normalized_value
        if set(concentrations) != candidate_symbols:
            raise ValueError("concentration evidence must cover every underlying exactly")
        thesis_by_symbol, duplicate_thesis_symbols = _theses(equity_theses)
        prepared: list[tuple[FinalizedOptionPoolCandidate, Decimal, Mapping[str, object], tuple[str, ...]]] = []
        seen: set[str] = set()
        for candidate in candidate_values:
            payload = candidate.payload
            candidate_id = _text(payload.get("candidate_id"), "candidate_id")
            symbol = _text(payload.get("symbol"), "symbol").upper()
            reasons: list[str] = []
            if candidate_id in seen:
                reasons.append("DUPLICATE_CANDIDATE_ID")
            seen.add(candidate_id)
            if candidate.broker_snapshot_hash != expected_broker:
                reasons.append("BROKER_SNAPSHOT_BINDING_MISMATCH")
            if candidate.strategy_nav_hash != expected_nav:
                reasons.append("STRATEGY_NAV_AUTHORITY_MISMATCH")
            reasons.extend(_codes(gates.get(candidate_id, ())))
            if symbol in blocked_underlyings:
                reasons.append("OPEN_POSITION_UNDERLYING_BLOCKED")
            thesis = thesis_by_symbol.get(symbol)
            if thesis is None:
                reasons.append("EQUITY_THESIS_UNAVAILABLE")
            elif symbol in duplicate_thesis_symbols:
                reasons.append("EQUITY_THESIS_DUPLICATE")
            else:
                supplied_thesis_hash = payload.get("equity_thesis_hash")
                expected_thesis_hash = canonical_hash(
                    {key: value for key, value in thesis.items() if key != "thesis_hash"}
                )
                if supplied_thesis_hash != expected_thesis_hash:
                    reasons.append("EQUITY_THESIS_HASH_MISMATCH")
                reasons.extend(_thesis_direction_reasons(payload, thesis))
            components, evidence_reasons = _score_components(
                payload,
                thesis=thesis,
                now=cutoff,
                aggregate_open_risk_usd=aggregate_open_risk_usd,
                concentration=concentrations.get(symbol, ZERO),
            )
            reasons.extend(evidence_reasons)
            score = _weighted_score(components)
            prepared.append((candidate, score, components, _codes(reasons)))

        ordered = sorted(
            prepared,
            key=lambda item: (
                bool(item[3]),
                -item[1],
                _text(item[0].payload.get("symbol"), "symbol").upper(),
                _text(item[0].payload.get("candidate_id"), "candidate_id"),
                item[0].candidate_hash,
            ),
        )
        executable: list[JointRankingRow] = []
        research: list[JointRankingRow] = []
        selected_underlyings: set[str] = set()
        for candidate, score, components, initial_reasons in ordered:
            payload = candidate.payload
            candidate_id = str(payload["candidate_id"])
            symbol = str(payload["symbol"]).upper()
            reasons = list(initial_reasons)
            if not reasons and symbol in selected_underlyings:
                reasons.append("ALTERNATIVE_SAME_UNDERLYING")
            if not reasons and len(executable) >= limit:
                reasons.append("TOP10_LIMIT")
            if reasons:
                research.append(JointRankingRow.build(
                    candidate_id=candidate_id,
                    underlying=symbol,
                    candidate_hash=candidate.candidate_hash,
                    disposition=JointDisposition.RESEARCH_ONLY,
                    rank=None,
                    score=score,
                    score_components=components,
                    reason_codes=reasons,
                ))
                continue
            selected_underlyings.add(symbol)
            executable.append(JointRankingRow.build(
                candidate_id=candidate_id,
                underlying=symbol,
                candidate_hash=candidate.candidate_hash,
                disposition=JointDisposition.EXECUTABLE_REVIEW,
                rank=len(executable) + 1,
                score=score,
                score_components=components,
            ))
        input_document = build_joint_ranking_input_document(
            candidate_values,
            scan_run_id=scan_run_id,
            now=cutoff,
            equity_theses=equity_theses,
            broker_snapshot_hash=expected_broker,
            strategy_nav_hash=expected_nav,
            gate_reasons_by_candidate=gates,
            open_position_underlyings=blocked_underlyings,
            aggregate_open_risk_usd=aggregate_open_risk_usd,
            concentration_by_underlying=concentrations,
            limit=limit,
        )
        input_hash = str(input_document["input_hash"])
        body = {
            "schema": SCHEMA,
            "scan_run_id": str(scan_run_id).strip(),
            "generated_at": datetime_text(cutoff),
            "broker_snapshot_hash": expected_broker,
            "strategy_nav_hash": expected_nav,
            "executable": tuple(row.as_dict() for row in executable),
            "research_watchlist": tuple(row.as_dict() for row in research),
            "input_hash": input_hash,
            "review_only": True,
            "direct_order_submission": False,
        }
        return JointRankingSnapshot(
            scan_run_id=str(scan_run_id).strip(),
            generated_at=cutoff,
            broker_snapshot_hash=expected_broker,
            strategy_nav_hash=expected_nav,
            executable=tuple(executable),
            research_watchlist=tuple(research),
            input_hash=input_hash,
            snapshot_hash=canonical_hash(body),
        )


def build_joint_ranking_input_document(
    candidates: Iterable[FinalizedOptionPoolCandidate],
    *,
    scan_run_id: str,
    now: datetime,
    equity_theses: Mapping[str, object],
    broker_snapshot_hash: str,
    strategy_nav_hash: str,
    gate_reasons_by_candidate: Mapping[str, Sequence[str]],
    open_position_underlyings: Iterable[str] = (),
    aggregate_open_risk_usd: Decimal = ZERO,
    concentration_by_underlying: Mapping[str, Decimal],
    limit: int = 10,
) -> Mapping[str, object]:
    """Build the hash-bound inputs consumed by deterministic joint ranking."""

    if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 10:
        raise ValueError("limit must be between 1 and 10")
    cutoff = utc_datetime(now, field="now")
    expected_broker = _hash(broker_snapshot_hash, "broker_snapshot_hash")
    expected_nav = _hash(strategy_nav_hash, "strategy_nav_hash")
    candidate_values = tuple(candidates)
    if any(
        not isinstance(candidate, FinalizedOptionPoolCandidate)
        for candidate in candidate_values
    ):
        raise TypeError("joint ranking accepts only FinalizedOptionPoolCandidate values")
    candidate_ids = {
        _text(candidate.payload.get("candidate_id"), "candidate_id")
        for candidate in candidate_values
    }
    if set(gate_reasons_by_candidate) != candidate_ids:
        raise ValueError("gate evidence must cover every finalized candidate exactly")
    thesis_by_symbol, _ = _theses(equity_theses)
    blocked = {
        _text(item, "open_position_underlying").upper()
        for item in open_position_underlyings
    }
    concentrations: dict[str, Decimal] = {}
    for raw_key, raw_value in concentration_by_underlying.items():
        key = _text(raw_key, "concentration_underlying").upper()
        if key in concentrations:
            raise ValueError("concentration evidence contains normalized duplicates")
        value = _optional_decimal(raw_value)
        if value is None or not ZERO <= value <= ONE:
            raise ValueError("concentration evidence value is invalid")
        concentrations[key] = value
    input_body = {
            "scan_run_id": _text(scan_run_id, "scan_run_id"),
            "generated_at": datetime_text(cutoff),
            "broker_snapshot_hash": expected_broker,
            "strategy_nav_hash": expected_nav,
            "candidate_hashes": tuple(
                sorted(candidate.candidate_hash for candidate in candidate_values)
            ),
            "equity_theses": tuple(
                thesis_by_symbol[symbol] for symbol in sorted(thesis_by_symbol)
            ),
            "gate_reasons_by_candidate": {
                str(key): _codes(value)
                for key, value in sorted(gate_reasons_by_candidate.items())
            },
            "open_position_underlyings": tuple(sorted(blocked)),
            "aggregate_open_risk_usd": aggregate_open_risk_usd,
            "concentration_by_underlying": dict(sorted(concentrations.items())),
            "limit": limit,
        }
    return freeze_json(
        {
            "schema": INPUT_SCHEMA,
            **input_body,
            "input_hash": canonical_hash(input_body),
        }
    )


def _score_components(
    payload: Mapping[str, object],
    *,
    thesis: Mapping[str, object] | None,
    now: datetime,
    aggregate_open_risk_usd: Decimal,
    concentration: Decimal,
) -> tuple[Mapping[str, object], tuple[str, ...]]:
    reasons: list[str] = []
    direction = _optional_decimal(None if thesis is None else thesis.get("direction_score"))
    uncertainty = _optional_decimal(None if thesis is None else thesis.get("uncertainty"))
    if direction is None or uncertainty is None or not ZERO <= uncertainty <= ONE:
        reasons.append("EQUITY_THESIS_INCOMPLETE")
        thesis_strength = uncertainty_quality = ZERO
    else:
        thesis_strength = min(ONE, abs(direction) / Decimal("100"))
        uncertainty_quality = ONE - uncertainty
    max_loss = _positive_decimal(payload.get("max_loss_usd"))
    after_cost_ev = _positive_decimal(payload.get("after_cost_ev_usd"))
    nav = _positive_decimal(payload.get("strategy_nav_usd"))
    if max_loss is None:
        reasons.append("MAXIMUM_LOSS_UNAVAILABLE")
    if after_cost_ev is None:
        reasons.append("AFTER_COST_EV_NOT_POSITIVE")
    if nav is None:
        reasons.append("STRATEGY_NAV_UNAVAILABLE")
    ev_quality = ZERO if max_loss is None or after_cost_ev is None else min(ONE, after_cost_ev / max_loss)
    liquidity = _optional_decimal(payload.get("liquidity_score"))
    if liquidity is None or not ZERO <= liquidity <= ONE:
        reasons.append("LIQUIDITY_INCOMPLETE")
        liquidity_quality = ZERO
    else:
        liquidity_quality = liquidity
    legs = payload.get("legs")
    greeks_quality = ONE
    if not isinstance(legs, Sequence) or isinstance(legs, (str, bytes, bytearray)) or not legs:
        reasons.append("OPTION_LEGS_INCOMPLETE")
        greeks_quality = ZERO
    else:
        for leg in legs:
            if not isinstance(leg, Mapping):
                reasons.append("OPTION_LEGS_INCOMPLETE")
                greeks_quality = ZERO
                break
            age = _optional_decimal(leg.get("quote_age_seconds"))
            observed = _optional_datetime(leg.get("exchange_time"))
            if age is None or age < ZERO or age > FIVE_SECONDS:
                reasons.append("EXECUTABLE_QUOTE_STALE")
            if observed is None or observed > now:
                reasons.append("EXECUTABLE_QUOTE_FUTURE_OR_MISSING")
            else:
                computed_age = Decimal(str((now - observed).total_seconds()))
                if computed_age > FIVE_SECONDS:
                    reasons.append("EXECUTABLE_QUOTE_STALE")
                if age is not None and abs(age - computed_age) > Decimal("0.001"):
                    reasons.append("EXECUTABLE_QUOTE_AGE_BINDING_MISMATCH")
            if leg.get("market_data_type") != 1:
                reasons.append("EXECUTABLE_MARKET_DATA_NOT_LIVE")
            bid = _optional_decimal(leg.get("bid"))
            ask = _optional_decimal(leg.get("ask"))
            if bid is None or ask is None or bid < ZERO or ask < bid:
                reasons.append("EXECUTABLE_BID_ASK_INVALID")
            if any(_optional_decimal(leg.get(name)) is None for name in ("delta", "gamma", "theta", "vega")):
                reasons.append("GREEKS_INCOMPLETE")
                greeks_quality = ZERO
            if _optional_decimal(leg.get("volume")) is None or _optional_decimal(leg.get("open_interest")) is None:
                reasons.append("OPTION_ACTIVITY_INCOMPLETE")
    scenarios = payload.get("scenario_pnl")
    scenario_quality = ZERO
    if not isinstance(scenarios, Sequence) or isinstance(scenarios, (str, bytes, bytearray)) or not scenarios:
        reasons.append("SCENARIO_PAYOFF_INCOMPLETE")
    else:
        probability_sum = ZERO
        profitable_probability = ZERO
        valid = True
        for row in scenarios:
            if not isinstance(row, Mapping):
                valid = False
                break
            probability = _optional_decimal(row.get("probability"))
            pnl = _optional_decimal(row.get("pnl_usd"))
            if probability is None or pnl is None or probability < ZERO or probability > ONE:
                valid = False
                break
            probability_sum += probability
            if pnl > ZERO:
                profitable_probability += probability
        if not valid or probability_sum != ONE:
            reasons.append("SCENARIO_PROBABILITIES_INVALID")
        else:
            scenario_quality = profitable_probability
    event_status = str(payload.get("event_evidence_status", "")).upper()
    event_overlap = payload.get("earnings_overlap")
    event_defined = payload.get("event_defined") is True
    if event_status != "AVAILABLE" or not isinstance(event_overlap, bool):
        reasons.append("EVENT_EVIDENCE_UNAVAILABLE")
        event_quality = ZERO
    else:
        event_quality = ONE if not event_overlap or event_defined else Decimal("0.25")
    risk_fraction = None if max_loss is None or nav is None else max_loss / nav
    if risk_fraction is None or risk_fraction > MAX_CANDIDATE_RISK:
        reasons.append("CANDIDATE_RISK_CAPACITY_EXCEEDED")
        nav_quality = ZERO
    else:
        nav_quality = max(ZERO, ONE - (risk_fraction / MAX_CANDIDATE_RISK))
    if nav is None or max_loss is None or aggregate_open_risk_usd < ZERO or (
        aggregate_open_risk_usd + max_loss > nav * MAX_AGGREGATE_RISK
    ):
        reasons.append("AGGREGATE_RISK_CAPACITY_EXCEEDED")
    concentration_value = _optional_decimal(concentration)
    if concentration_value is None or not ZERO <= concentration_value <= ONE:
        reasons.append("CONCENTRATION_EVIDENCE_INVALID")
        concentration_quality = ZERO
    else:
        concentration_quality = ONE - concentration_value
    components = freeze_json({
        "thesis_strength": thesis_strength,
        "uncertainty_quality": uncertainty_quality,
        "after_cost_ev_quality": ev_quality,
        "liquidity_quality": liquidity_quality,
        "greeks_quality": greeks_quality,
        "scenario_quality": scenario_quality,
        "event_quality": event_quality,
        "nav_capacity_quality": nav_quality,
        "concentration_quality": concentration_quality,
        "max_loss_usd": max_loss,
        "after_cost_ev_usd": after_cost_ev,
        "strategy_nav_usd": nav,
        "risk_fraction": risk_fraction,
    })
    assert isinstance(components, Mapping)
    return components, _codes(reasons)


def _weighted_score(components: Mapping[str, object]) -> Decimal:
    weights = {
        "thesis_strength": Decimal("0.16"),
        "uncertainty_quality": Decimal("0.10"),
        "after_cost_ev_quality": Decimal("0.20"),
        "liquidity_quality": Decimal("0.13"),
        "greeks_quality": Decimal("0.08"),
        "scenario_quality": Decimal("0.13"),
        "event_quality": Decimal("0.07"),
        "nav_capacity_quality": Decimal("0.08"),
        "concentration_quality": Decimal("0.05"),
    }
    return sum(
        (_optional_decimal(components.get(name)) or ZERO) * weight
        for name, weight in weights.items()
    ) * Decimal("100")


def _theses(
    value: Mapping[str, object],
) -> tuple[dict[str, Mapping[str, object]], frozenset[str]]:
    rows = value.get("rows", ())
    if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes, bytearray)):
        return {}, frozenset()
    normalized: dict[str, Mapping[str, object]] = {}
    duplicates: set[str] = set()
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        symbol = str(row.get("symbol", "")).strip().upper()
        if not symbol:
            continue
        if symbol in normalized:
            duplicates.add(symbol)
        else:
            normalized[symbol] = row
    return normalized, frozenset(duplicates)


def _thesis_direction_reasons(
    payload: Mapping[str, object],
    thesis: Mapping[str, object],
) -> tuple[str, ...]:
    label = str(thesis.get("direction_label", "")).strip().upper()
    legs = payload.get("legs")
    if not isinstance(legs, Sequence) or isinstance(legs, (str, bytes, bytearray)):
        return ("THESIS_PAYOFF_DIRECTION_UNAVAILABLE",)
    net_delta = ZERO
    for leg in legs:
        if not isinstance(leg, Mapping):
            return ("THESIS_PAYOFF_DIRECTION_UNAVAILABLE",)
        delta = _optional_decimal(leg.get("delta"))
        ratio = _optional_decimal(leg.get("ratio"))
        side = str(leg.get("side", "")).strip().upper()
        if delta is None or ratio is None or ratio <= ZERO or side not in {
            "BUY", "LONG", "SELL", "SHORT",
        }:
            return ("THESIS_PAYOFF_DIRECTION_UNAVAILABLE",)
        net_delta += delta * ratio * (ONE if side in {"BUY", "LONG"} else -ONE)
    tolerance = Decimal("0.05")
    aligned = (
        (label == "BULLISH" and net_delta > tolerance)
        or (label == "BEARISH" and net_delta < -tolerance)
        or (label in {"NEUTRAL", "MIXED"} and abs(net_delta) <= tolerance)
    )
    if label == "UNCERTAIN":
        return ("THESIS_DIRECTION_UNCERTAIN",)
    return () if aligned else ("THESIS_PAYOFF_DIRECTION_MISMATCH",)


def _optional_datetime(value: object) -> datetime | None:
    try:
        parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
        return utc_datetime(parsed, field="timestamp")
    except (TypeError, ValueError):
        return None


def _positive_decimal(value: object) -> Decimal | None:
    parsed = _optional_decimal(value)
    return parsed if parsed is not None and parsed > ZERO else None


def _optional_decimal(value: object) -> Decimal | None:
    if isinstance(value, bool):
        return None
    try:
        parsed = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def _decimal(value: object, name: str, *, minimum: Decimal, maximum: Decimal) -> Decimal:
    parsed = _optional_decimal(value)
    if parsed is None or not minimum <= parsed <= maximum:
        raise ValueError(f"{name} is invalid")
    return parsed


def _text(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} cannot be blank")
    return value.strip()


def _hash(value: object, name: str) -> str:
    text = _text(value, name)
    if len(text) != 64 or any(char not in "0123456789abcdef" for char in text):
        raise ValueError(f"{name} must be a lowercase SHA-256 hash")
    return text


def _codes(values: Iterable[object]) -> tuple[str, ...]:
    if isinstance(values, (str, bytes, bytearray)):
        raise TypeError("reason codes must be an iterable, not text")
    return tuple(sorted({_text(value, "reason_code").upper() for value in values}))


__all__ = [
    "build_joint_ranking_input_document",
    "JointDisposition",
    "JointRankingEngine",
    "JointRankingRow",
    "JointRankingSnapshot",
    "SCHEMA",
]
