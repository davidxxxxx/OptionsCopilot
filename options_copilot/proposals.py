"""Fail-closed validation for GUI and bridge option proposals.

The GUI payload is untrusted even when it was produced by another local
component.  This adapter rebuilds the immutable domain objects from explicit
contract and quote fields, runs the locked :class:`RiskEngine`, and emits a
small canonical payload containing only values that were checked or recomputed.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, DecimalException, InvalidOperation
from types import MappingProxyType
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from options_copilot.domain import (
    CandidateRiskTier,
    OptionContract,
    OptionLeg,
    OptionLegQuote,
    OptionRight,
    PositionSide,
    StrategyCandidate,
    TerminalScenario,
)
from options_copilot.risk import (
    DteEntryExceptionAuthority,
    OptionTimePolicy,
    RiskAssessment,
    RiskEngine,
)


ZERO = Decimal("0")
ONE_CENT = Decimal("0.01")
HARD_QUOTE_FRESH_SECONDS = Decimal("5")
MINIMUM_DTE = 7
_MISSING = object()

try:
    _US_EQUITY_MARKET_TIMEZONE = ZoneInfo("America/New_York")
except ZoneInfoNotFoundError:  # pragma: no cover - Windows fallback without tzdata
    # UTC remains deterministic and conservative around the US evening date
    # boundary if a stripped Python runtime lacks the IANA timezone database.
    _US_EQUITY_MARKET_TIMEZONE = timezone.utc


@dataclass(frozen=True, slots=True)
class ValidatedProposal:
    """A proposal whose quotes, economics, and locked risk gates were checked.

    ``executable_cost_usd`` is the signed executable option premium before
    commissions and slippage: a debit is positive and a credit is negative.
    ``all_in_executable_cost_usd`` includes those explicit execution frictions.
    The maximum loss always comes from the risk engine, never from the payload.
    """

    candidate: StrategyCandidate
    risk_assessment: RiskAssessment
    maximum_loss_usd: Decimal
    executable_cost_usd: Decimal
    all_in_executable_cost_usd: Decimal
    expected_value_usd: Decimal
    expected_value_before_costs_usd: Decimal
    canonical_proposal: Mapping[str, object]
    canonical_json: str
    proposal_hash: str

    @property
    def strategy_candidate(self) -> StrategyCandidate:
        return self.candidate

    @property
    def risk(self) -> RiskAssessment:
        return self.risk_assessment

    @property
    def computed_maximum_loss_usd(self) -> Decimal:
        return self.maximum_loss_usd

    @property
    def computed_max_loss_usd(self) -> Decimal:
        return self.maximum_loss_usd

    @property
    def computed_executable_cost_usd(self) -> Decimal:
        return self.executable_cost_usd

    @property
    def estimated_execution_costs_usd(self) -> Decimal:
        return self.candidate.estimated_execution_costs

    @property
    def normalized_proposal(self) -> Mapping[str, object]:
        return self.canonical_proposal

    @property
    def normalized_candidate(self) -> Mapping[str, object]:
        return self.canonical_proposal

    @property
    def normalized_legs(self) -> tuple[Mapping[str, object], ...]:
        legs = self.canonical_proposal["legs"]
        assert isinstance(legs, tuple)
        return legs

    @property
    def canonical_hash(self) -> str:
        return self.proposal_hash

    def to_dict(self) -> dict[str, object]:
        """Return a detached JSON-safe copy of the canonical proposal."""

        value = json.loads(self.canonical_json)
        assert isinstance(value, dict)
        return value


@dataclass(frozen=True, slots=True)
class _ParsedLeg:
    quote: OptionLegQuote
    snapshot_id: str


def validate_proposal(
    proposal: Mapping[str, object],
    account_equity: Decimal,
    open_combinations: int,
    now: datetime,
    quote_fresh_seconds: Decimal | int | float | str,
    expected_quote_snapshot_id: str,
    a_grade_unlocked: bool = False,
    risk_engine: RiskEngine | None = None,
    authoritative_repricing: bool = False,
    time_policy: OptionTimePolicy | None = None,
    dte_exception_authority: DteEntryExceptionAuthority | None = None,
) -> ValidatedProposal:
    """Validate one canonical rank-1 proposal and return recomputed economics.

    All ordinary validation failures are reported as :class:`ValueError` so a
    caller can uniformly fail closed at a GUI, approval-store, or bridge
    boundary.  ``account_equity`` deliberately remains Decimal-only because it
    controls the locked account-risk ceilings.
    """

    if not isinstance(proposal, Mapping):
        raise ValueError("proposal must be a mapping")
    equity = _input_decimal(account_equity, "account_equity", positive=True)
    if isinstance(open_combinations, bool) or not isinstance(open_combinations, int):
        raise ValueError("open_combinations must be an integer")
    if open_combinations < 0:
        raise ValueError("open_combinations cannot be negative")
    checked_now = _aware_datetime(now, "now")
    freshness = _decimal(
        quote_fresh_seconds,
        "quote_fresh_seconds",
        nonnegative=True,
    )
    freshness_limit = min(freshness, HARD_QUOTE_FRESH_SECONDS)
    expected_snapshot = _nonblank(
        expected_quote_snapshot_id,
        "expected_quote_snapshot_id",
    )
    if not isinstance(a_grade_unlocked, bool):
        raise ValueError("a_grade_unlocked must be a boolean")
    if risk_engine is not None and not isinstance(risk_engine, RiskEngine):
        raise ValueError("risk_engine must be a RiskEngine")
    if not isinstance(authoritative_repricing, bool):
        raise ValueError("authoritative_repricing must be a boolean")
    if time_policy is not None and not isinstance(time_policy, OptionTimePolicy):
        raise ValueError("time_policy must be an OptionTimePolicy")
    if dte_exception_authority is not None and not isinstance(
        dte_exception_authority,
        DteEntryExceptionAuthority,
    ):
        raise ValueError(
            "dte_exception_authority must be a DteEntryExceptionAuthority"
        )

    rank = proposal.get("rank", _MISSING)
    if isinstance(rank, bool) or not isinstance(rank, int) or rank != 1:
        raise ValueError("proposal.rank must be the integer 1")
    if proposal.get("eligible_to_send", _MISSING) is not True:
        raise ValueError("proposal.eligible_to_send must be exactly true")

    proposal_snapshot = _required_string(
        proposal,
        "quote_snapshot_id",
        "proposal.quote_snapshot_id",
    )
    if proposal_snapshot != expected_snapshot:
        raise ValueError(
            "quote snapshot replacement detected: proposal.quote_snapshot_id "
            "does not match expected_quote_snapshot_id"
        )

    candidate_id = _candidate_id(proposal)
    risk = _required_mapping(proposal, "risk", "proposal.risk")
    risk_tier = _candidate_risk_tier(
        proposal,
        risk,
        authority_allows_a_grade=bool(
            risk_engine is not None and risk_engine.a_grade_approved
        ),
    )

    commissions = _required_decimal_alias(
        proposal,
        ("estimated_commissions", "estimated_commissions_usd", "commission_usd"),
        "estimated_commissions",
        nested_key="pricing",
        nonnegative=True,
    )
    slippage = _required_decimal_alias(
        proposal,
        ("estimated_slippage", "estimated_slippage_usd", "slippage_usd"),
        "estimated_slippage",
        nested_key="pricing",
        nonnegative=True,
    )
    expected_value = _decimal(
        _required(proposal, "expected_value_usd", "proposal.expected_value_usd"),
        "proposal.expected_value_usd",
    )
    if expected_value <= ZERO:
        raise ValueError(
            "proposal.expected_value_usd must be positive after commissions and slippage"
        )
    declared_maximum_loss = _decimal(
        _required(risk, "maximum_loss_usd", "proposal.risk.maximum_loss_usd"),
        "proposal.risk.maximum_loss_usd",
        nonnegative=True,
    )

    raw_legs = _required_sequence(proposal, "legs", "proposal.legs")
    if not raw_legs:
        raise ValueError("proposal.legs must contain at least one option leg")
    parsed_legs = tuple(
        _parse_leg(
            raw_leg,
            index=index,
            now=checked_now,
            freshness_limit=freshness_limit,
            expected_snapshot=expected_snapshot,
        )
        for index, raw_leg in enumerate(raw_legs)
    )
    expiration = _validate_same_contract_family(proposal, parsed_legs)
    selected_time_policy = time_policy or OptionTimePolicy()
    time_decision = selected_time_policy.evaluate_dte(
        proposal_id=candidate_id,
        expiration=expiration,
        now=checked_now,
        exception_authority=dte_exception_authority,
    )
    if not time_decision.allowed:
        raise ValueError(
            "proposal expiration has " + time_decision.rejection_message
        )

    terminal_scenarios = _terminal_scenarios(proposal)
    try:
        candidate = StrategyCandidate(
            candidate_id=candidate_id,
            leg_quotes=tuple(item.quote for item in parsed_legs),
            terminal_scenarios=terminal_scenarios,
            estimated_commissions=commissions,
            estimated_slippage=slippage,
            risk_tier=risk_tier,
        )
    except (TypeError, ValueError) as exc:
        raise ValueError(f"proposal cannot form a valid StrategyCandidate: {exc}") from exc

    selected_risk_engine = risk_engine or RiskEngine()
    if selected_risk_engine.production_bound:
        assessment = selected_risk_engine.assess(
            candidate,
            open_combinations=open_combinations,
        )
    else:
        assessment = selected_risk_engine.assess(
            candidate,
            account_equity=equity,
            open_combinations=open_combinations,
        )
    if not assessment.approved:
        codes = ", ".join(item.value for item in assessment.rejections)
        raise ValueError(f"proposal risk rejected: {codes}")
    computed_maximum_loss = assessment.payoff.max_loss
    if computed_maximum_loss is None or not computed_maximum_loss.is_finite():
        raise ValueError("proposal risk rejected: maximum loss is unknown")
    if (
        not authoritative_repricing
        and abs(declared_maximum_loss - computed_maximum_loss) > ONE_CENT
    ):
        raise ValueError(
            "proposal.risk.maximum_loss_usd does not match recomputed maximum "
            f"loss within $0.01 (declared={declared_maximum_loss}, "
            f"computed={computed_maximum_loss})"
        )

    executable_cost = _executable_premium(candidate)
    all_in_cost = executable_cost + candidate.estimated_execution_costs
    scenario_expected_value = _scenario_expected_value(candidate, assessment)
    if authoritative_repricing:
        expected_value = scenario_expected_value
    else:
        _validate_declared_reference_cost(proposal, executable_cost)
        _validate_declared_expected_value_before_costs(
            proposal,
            expected_value=expected_value,
            execution_costs=candidate.estimated_execution_costs,
        )
        if abs(scenario_expected_value - expected_value) > ONE_CENT:
            raise ValueError(
                "proposal.expected_value_usd does not match the recomputed "
                "cost-after scenario EV within $0.01 "
                f"(declared={expected_value}, computed={scenario_expected_value})"
            )
    expected_before_costs = expected_value + candidate.estimated_execution_costs

    canonical_plain = _canonical_payload(
        candidate=candidate,
        assessment=assessment,
        expected_value=expected_value,
        expected_before_costs=expected_before_costs,
        executable_cost=executable_cost,
        all_in_cost=all_in_cost,
        quote_snapshot_id=expected_snapshot,
        now=checked_now,
    )
    canonical_json = json.dumps(
        canonical_plain,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    proposal_hash = hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()
    return ValidatedProposal(
        candidate=candidate,
        risk_assessment=assessment,
        maximum_loss_usd=computed_maximum_loss,
        executable_cost_usd=executable_cost,
        all_in_executable_cost_usd=all_in_cost,
        expected_value_usd=expected_value,
        expected_value_before_costs_usd=expected_before_costs,
        canonical_proposal=_freeze_json(canonical_plain),
        canonical_json=canonical_json,
        proposal_hash=proposal_hash,
    )


def _input_decimal(value: object, field: str, *, positive: bool = False) -> Decimal:
    if not isinstance(value, Decimal):
        raise ValueError(f"{field} must be a Decimal")
    if not value.is_finite() or (positive and value <= ZERO):
        qualifier = "finite and positive" if positive else "finite"
        raise ValueError(f"{field} must be {qualifier}")
    return value


def _decimal(
    value: object,
    field: str,
    *,
    nonnegative: bool = False,
    positive: bool = False,
) -> Decimal:
    if isinstance(value, bool):
        raise ValueError(f"{field} must be a finite decimal number, not boolean")
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError(f"{field} must be a finite decimal number")
    if not isinstance(value, (Decimal, int, float, str)):
        raise ValueError(f"{field} must be a JSON number or decimal string")
    if isinstance(value, str) and not value.strip():
        raise ValueError(f"{field} cannot be blank")
    try:
        result = Decimal(str(value).strip())
    except (InvalidOperation, DecimalException, ValueError) as exc:
        raise ValueError(f"{field} must be a finite decimal number") from exc
    if not result.is_finite():
        raise ValueError(f"{field} must be a finite decimal number")
    if positive and result <= ZERO:
        raise ValueError(f"{field} must be positive")
    if nonnegative and result < ZERO:
        raise ValueError(f"{field} cannot be negative")
    return result


def _required(mapping: Mapping[str, object], key: str, field: str) -> object:
    value = mapping.get(key, _MISSING)
    if value is _MISSING or value is None:
        raise ValueError(f"{field} is required")
    return value


def _nonblank(value: object, field: str, *, uppercase: bool = False) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a string")
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field} cannot be blank")
    return normalized.upper() if uppercase else normalized


def _required_string(
    mapping: Mapping[str, object],
    key: str,
    field: str,
    *,
    uppercase: bool = False,
) -> str:
    return _nonblank(_required(mapping, key, field), field, uppercase=uppercase)


def _required_mapping(
    mapping: Mapping[str, object], key: str, field: str
) -> Mapping[str, object]:
    value = _required(mapping, key, field)
    if not isinstance(value, Mapping):
        raise ValueError(f"{field} must be a mapping")
    return value


def _required_sequence(
    mapping: Mapping[str, object], key: str, field: str
) -> Sequence[object]:
    value = _required(mapping, key, field)
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise ValueError(f"{field} must be a sequence")
    return value


def _candidate_id(proposal: Mapping[str, object]) -> str:
    values: list[tuple[str, str]] = []
    for key in ("proposal_id", "candidate_id"):
        if key in proposal and proposal[key] is not None:
            values.append((key, _nonblank(proposal[key], f"proposal.{key}")))
    if not values:
        raise ValueError("proposal.proposal_id or proposal.candidate_id is required")
    if any(value != values[0][1] for _, value in values[1:]):
        raise ValueError("proposal_id and candidate_id must match when both are present")
    return values[0][1]


def _required_decimal_alias(
    proposal: Mapping[str, object],
    aliases: tuple[str, ...],
    field: str,
    *,
    nested_key: str,
    nonnegative: bool,
) -> Decimal:
    locations: list[tuple[str, object]] = []
    for key in aliases:
        if key in proposal:
            locations.append((f"proposal.{key}", proposal[key]))
    nested = proposal.get(nested_key)
    if nested is not None:
        if not isinstance(nested, Mapping):
            raise ValueError(f"proposal.{nested_key} must be a mapping")
        for key in aliases:
            if key in nested:
                locations.append((f"proposal.{nested_key}.{key}", nested[key]))
    if not locations:
        raise ValueError(f"proposal.{field} is required")
    parsed = tuple(
        _decimal(value, location, nonnegative=nonnegative)
        for location, value in locations
    )
    if any(value != parsed[0] for value in parsed[1:]):
        names = ", ".join(location for location, _ in locations)
        raise ValueError(f"conflicting {field} values at {names}")
    return parsed[0]


def _candidate_risk_tier(
    proposal: Mapping[str, object],
    risk: Mapping[str, object],
    *,
    authority_allows_a_grade: bool,
) -> CandidateRiskTier:
    declared: list[tuple[str, CandidateRiskTier]] = []
    for location, value in (
        ("proposal.risk_tier", proposal.get("risk_tier", _MISSING)),
        ("proposal.risk.risk_tier", risk.get("risk_tier", _MISSING)),
        ("proposal.risk.tier", risk.get("tier", _MISSING)),
    ):
        if value is _MISSING or value is None:
            continue
        text = _nonblank(value, location, uppercase=True).replace("-", "_").replace(" ", "_")
        if text in {"NORMAL", "STANDARD"}:
            tier = CandidateRiskTier.NORMAL
        elif text in {"A", "A+", "A_GRADE", "VALIDATED_A_GRADE"}:
            tier = CandidateRiskTier.VALIDATED_A_GRADE
        else:
            raise ValueError(f"{location} is not a recognized risk tier")
        declared.append((location, tier))

    grade = proposal.get("grade", _MISSING)
    if grade is not _MISSING and grade is not None:
        grade_text = (
            _nonblank(grade, "proposal.grade", uppercase=True)
            .replace("-", "_")
            .replace(" ", "_")
        )
        if grade_text in {"A", "A+", "A_GRADE", "VALIDATED_A_GRADE"}:
            declared.append(("proposal.grade", CandidateRiskTier.VALIDATED_A_GRADE))
    if declared and any(item[1] is not declared[0][1] for item in declared[1:]):
        names = ", ".join(item[0] for item in declared)
        raise ValueError(f"conflicting A-grade risk declarations at {names}")
    tier = declared[0][1] if declared else CandidateRiskTier.NORMAL
    if tier is CandidateRiskTier.VALIDATED_A_GRADE and not authority_allows_a_grade:
        raise ValueError("validated A-grade risk tier is locked")
    return tier


def _leg_parts(
    raw_leg: object,
    index: int,
) -> tuple[Mapping[str, object], Mapping[str, object], Mapping[str, object]]:
    field = f"proposal.legs[{index}]"
    if not isinstance(raw_leg, Mapping):
        raise ValueError(f"{field} must be a mapping")
    leg = raw_leg.get("leg", raw_leg)
    if not isinstance(leg, Mapping):
        raise ValueError(f"{field}.leg must be a mapping")
    contract = leg.get("contract", leg)
    if not isinstance(contract, Mapping):
        raise ValueError(f"{field}.contract must be a mapping")
    quote = raw_leg.get("quote", raw_leg)
    if not isinstance(quote, Mapping):
        raise ValueError(f"{field}.quote must be a mapping")
    return leg, contract, quote


def _parse_leg(
    raw_leg: object,
    *,
    index: int,
    now: datetime,
    freshness_limit: Decimal,
    expected_snapshot: str,
) -> _ParsedLeg:
    prefix = f"proposal.legs[{index}]"
    leg, contract, quote = _leg_parts(raw_leg, index)
    contract_id = _required_string(
        contract,
        "contract_id_ex",
        f"{prefix}.contract_id_ex",
    )
    security_type = _required_string(
        contract,
        "security_type",
        f"{prefix}.security_type",
        uppercase=True,
    )
    if security_type != "OPT":
        raise ValueError(f"{prefix}.security_type must be OPT")
    underlying = _required_string(
        contract,
        "underlying" if "underlying" in contract else "symbol",
        f"{prefix}.underlying",
        uppercase=True,
    )
    expiration = _expiration(
        _required(contract, "expiration", f"{prefix}.expiration"),
        f"{prefix}.expiration",
    )
    strike = _decimal(
        _required(contract, "strike", f"{prefix}.strike"),
        f"{prefix}.strike",
        positive=True,
    )
    right_text = _required_string(
        contract,
        "right",
        f"{prefix}.right",
        uppercase=True,
    )
    if right_text not in {"CALL", "PUT"}:
        raise ValueError(f"{prefix}.right must be CALL or PUT")
    side_text = _required_string(leg, "side", f"{prefix}.side", uppercase=True)
    if side_text not in {"BUY", "SELL"}:
        raise ValueError(f"{prefix}.side must be BUY or SELL")
    quantity = _positive_integer(
        _required(leg, "quantity", f"{prefix}.quantity"),
        f"{prefix}.quantity",
    )
    multiplier = _decimal(
        _required(contract, "multiplier", f"{prefix}.multiplier"),
        f"{prefix}.multiplier",
        positive=True,
    )
    currency = _required_string(
        contract,
        "currency",
        f"{prefix}.currency",
        uppercase=True,
    )
    if currency != "USD":
        raise ValueError(f"{prefix}.currency must be USD for a US equity option")
    exchange = _required_string(
        contract,
        "exchange",
        f"{prefix}.exchange",
        uppercase=True,
    )
    bid = _decimal(
        _required(quote, "bid", f"{prefix}.bid"),
        f"{prefix}.bid",
        nonnegative=True,
    )
    ask = _decimal(
        _required(quote, "ask", f"{prefix}.ask"),
        f"{prefix}.ask",
        nonnegative=True,
    )
    if bid > ask:
        raise ValueError(f"{prefix}.bid cannot exceed ask")
    quote_time = _parse_datetime(
        _required(quote, "quote_time", f"{prefix}.quote_time"),
        f"{prefix}.quote_time",
    )
    age_seconds = _duration_seconds(now - quote_time)
    if age_seconds < ZERO:
        raise ValueError(f"{prefix}.quote_time cannot be in the future")
    if age_seconds > freshness_limit:
        raise ValueError(
            f"{prefix}.quote_time is stale ({age_seconds} seconds; "
            f"limit is {freshness_limit})"
        )
    snapshot_id = _required_string(
        quote,
        "quote_snapshot_id",
        f"{prefix}.quote_snapshot_id",
    )
    if snapshot_id != expected_snapshot:
        raise ValueError(
            f"quote snapshot replacement detected at {prefix}.quote_snapshot_id"
        )

    last = _optional_decimal(quote, "last", f"{prefix}.last", nonnegative=True)
    implied_volatility = _optional_decimal_alias(
        quote,
        ("implied_volatility", "iv"),
        f"{prefix}.implied_volatility",
        nonnegative=True,
    )
    volume = _optional_nonnegative_integer(quote, "volume", f"{prefix}.volume")
    open_interest = _optional_integer_alias(
        quote,
        ("open_interest", "oi"),
        f"{prefix}.open_interest",
    )
    try:
        option_contract = OptionContract(
            contract_id=contract_id,
            underlying=underlying,
            expiration=expiration,
            strike=strike,
            right=OptionRight(right_text),
            multiplier=multiplier,
            currency=currency,
            exchange=exchange,
        )
        option_leg = OptionLeg(
            contract=option_contract,
            side=PositionSide.LONG if side_text == "BUY" else PositionSide.SHORT,
            quantity=quantity,
        )
        leg_quote = OptionLegQuote(
            leg=option_leg,
            bid=bid,
            ask=ask,
            last=last,
            implied_volatility=implied_volatility,
            volume=volume,
            open_interest=open_interest,
            observed_at=quote_time,
        )
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{prefix} is not a valid option leg: {exc}") from exc
    return _ParsedLeg(quote=leg_quote, snapshot_id=snapshot_id)


def _positive_integer(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field} must be a positive integer")
    if value <= 0:
        raise ValueError(f"{field} must be a positive integer")
    return value


def _optional_nonnegative_integer(
    mapping: Mapping[str, object], key: str, field: str
) -> int | None:
    value = mapping.get(key)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field} must be a nonnegative integer")
    return value


def _optional_integer_alias(
    mapping: Mapping[str, object], aliases: tuple[str, ...], field: str
) -> int | None:
    values = [(key, mapping[key]) for key in aliases if key in mapping]
    if not values or all(value is None for _, value in values):
        return None
    parsed = tuple(
        _optional_nonnegative_integer({key: value}, key, f"{field} ({key})")
        for key, value in values
    )
    present = tuple(value for value in parsed if value is not None)
    if not present:
        return None
    if any(value != present[0] for value in present[1:]):
        raise ValueError(f"conflicting {field} aliases")
    return present[0]


def _optional_decimal(
    mapping: Mapping[str, object],
    key: str,
    field: str,
    *,
    nonnegative: bool,
) -> Decimal | None:
    value = mapping.get(key)
    if value is None:
        return None
    return _decimal(value, field, nonnegative=nonnegative)


def _optional_decimal_alias(
    mapping: Mapping[str, object],
    aliases: tuple[str, ...],
    field: str,
    *,
    nonnegative: bool,
) -> Decimal | None:
    values = [(key, mapping[key]) for key in aliases if key in mapping]
    if not values or all(value is None for _, value in values):
        return None
    parsed = tuple(
        None
        if value is None
        else _decimal(value, f"{field} ({key})", nonnegative=nonnegative)
        for key, value in values
    )
    present = tuple(value for value in parsed if value is not None)
    if any(value != present[0] for value in present[1:]):
        raise ValueError(f"conflicting {field} aliases")
    return present[0]


def _expiration(value: object, field: str) -> date:
    if isinstance(value, datetime):
        raise ValueError(f"{field} must be a date, not a datetime")
    if isinstance(value, date):
        return value
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be an ISO date")
    text = value.strip()
    try:
        if len(text) == 8 and text.isdigit():
            return date(int(text[:4]), int(text[4:6]), int(text[6:]))
        return date.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"{field} must be an ISO date") from exc


def _parse_datetime(value: object, field: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value.strip():
        text = value.strip()
        try:
            parsed = datetime.fromisoformat(
                text[:-1] + "+00:00" if text.endswith(("Z", "z")) else text
            )
        except ValueError as exc:
            raise ValueError(f"{field} must be an ISO timezone-aware datetime") from exc
    else:
        raise ValueError(f"{field} must be an ISO timezone-aware datetime")
    return _aware_datetime(parsed, field)


def _aware_datetime(value: object, field: str) -> datetime:
    if not isinstance(value, datetime):
        raise ValueError(f"{field} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value


def _duration_seconds(value: Any) -> Decimal:
    return Decimal(str(value.total_seconds()))


def _validate_same_contract_family(
    proposal: Mapping[str, object],
    parsed_legs: tuple[_ParsedLeg, ...],
) -> date:
    contracts = tuple(item.quote.contract for item in parsed_legs)
    underlyings = {item.underlying for item in contracts}
    expirations = {item.expiration for item in contracts}
    if len(underlyings) != 1:
        raise ValueError("all proposal legs must have the same underlying")
    if len(expirations) != 1:
        raise ValueError("all proposal legs must have the same expiration")
    underlying = contracts[0].underlying
    expiration = contracts[0].expiration

    for key in ("underlying", "symbol"):
        if key in proposal and proposal[key] is not None:
            declared = _nonblank(
                proposal[key],
                f"proposal.{key}",
                uppercase=True,
            )
            if declared != underlying:
                raise ValueError(f"proposal.{key} does not match the option legs")
    for key in ("expiration", "expiry"):
        if key in proposal and proposal[key] is not None:
            declared_expiration = _expiration(proposal[key], f"proposal.{key}")
            if declared_expiration != expiration:
                raise ValueError(f"proposal.{key} does not match the option legs")

    return expiration


def _terminal_scenarios(
    proposal: Mapping[str, object],
) -> tuple[TerminalScenario, ...]:
    raw = proposal.get("terminal_scenarios", _MISSING)
    if raw is _MISSING or raw is None:
        raise ValueError(
            "proposal.terminal_scenarios is required to recompute cost-after EV"
        )
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes, bytearray)):
        raise ValueError("proposal.terminal_scenarios must be a sequence")
    if not raw:
        raise ValueError(
            "proposal.terminal_scenarios must contain at least one scenario"
        )
    scenarios: list[TerminalScenario] = []
    for index, item in enumerate(raw):
        prefix = f"proposal.terminal_scenarios[{index}]"
        if not isinstance(item, Mapping):
            raise ValueError(f"{prefix} must be a mapping")
        terminal_price = _decimal(
            _required(
                item,
                "terminal_underlying_price",
                f"{prefix}.terminal_underlying_price",
            ),
            f"{prefix}.terminal_underlying_price",
            nonnegative=True,
        )
        probability = _decimal(
            _required(item, "probability", f"{prefix}.probability"),
            f"{prefix}.probability",
            positive=True,
        )
        try:
            scenarios.append(
                TerminalScenario(
                    terminal_underlying_price=terminal_price,
                    probability=probability,
                )
            )
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{prefix} is invalid: {exc}") from exc
    return tuple(scenarios)


def _executable_premium(candidate: StrategyCandidate) -> Decimal:
    total = ZERO
    for item in candidate.leg_quotes:
        price = item.executable_price
        if price is None:
            raise ValueError("proposal risk rejected: missing executable quote")
        exposure = Decimal(item.leg.quantity) * item.contract.multiplier
        if item.leg.side is PositionSide.LONG:
            total += price * exposure
        else:
            total -= price * exposure
    return total


def _optional_alias_decimal(
    proposal: Mapping[str, object],
    aliases: tuple[str, ...],
    *,
    nested_key: str,
    field: str,
) -> Decimal | None:
    locations: list[tuple[str, object]] = []
    for key in aliases:
        if key in proposal:
            locations.append((f"proposal.{key}", proposal[key]))
    nested = proposal.get(nested_key)
    if nested is not None:
        if not isinstance(nested, Mapping):
            raise ValueError(f"proposal.{nested_key} must be a mapping")
        for key in aliases:
            if key in nested:
                locations.append((f"proposal.{nested_key}.{key}", nested[key]))
    if not locations:
        return None
    parsed = tuple(_decimal(value, name) for name, value in locations)
    if any(value != parsed[0] for value in parsed[1:]):
        raise ValueError(f"conflicting {field} declarations")
    return parsed[0]


def _validate_declared_reference_cost(
    proposal: Mapping[str, object], computed: Decimal
) -> None:
    declared = _optional_alias_decimal(
        proposal,
        ("reference_cost_usd", "executable_cost_usd"),
        nested_key="pricing",
        field="reference executable cost",
    )
    if declared is not None and abs(declared - computed) > ONE_CENT:
        raise ValueError(
            "declared reference_cost_usd does not match recomputed executable "
            f"cost within $0.01 (declared={declared}, computed={computed})"
        )


def _validate_declared_expected_value_before_costs(
    proposal: Mapping[str, object],
    *,
    expected_value: Decimal,
    execution_costs: Decimal,
) -> None:
    declared = _optional_alias_decimal(
        proposal,
        ("expected_value_before_costs_usd",),
        nested_key="pricing",
        field="expected value before costs",
    )
    if declared is None:
        return
    recomputed_after_costs = declared - execution_costs
    if abs(recomputed_after_costs - expected_value) > ONE_CENT:
        raise ValueError(
            "proposal.expected_value_usd does not include the declared "
            "commissions and slippage"
        )


def _scenario_expected_value(
    candidate: StrategyCandidate,
    assessment: RiskAssessment,
) -> Decimal:
    if not candidate.terminal_scenarios:
        raise ValueError(
            "proposal.terminal_scenarios is required to recompute cost-after EV"
        )
    scenario_ev = sum(
        (
            item.probability
            * assessment.payoff.pnl_at(item.terminal_underlying_price)
            for item in candidate.terminal_scenarios
        ),
        ZERO,
    )
    if scenario_ev <= ZERO:
        raise ValueError("recomputed expected_value_usd is not positive after costs")
    return scenario_ev


def _canonical_payload(
    *,
    candidate: StrategyCandidate,
    assessment: RiskAssessment,
    expected_value: Decimal,
    expected_before_costs: Decimal,
    executable_cost: Decimal,
    all_in_cost: Decimal,
    quote_snapshot_id: str,
    now: datetime,
) -> dict[str, object]:
    contracts = tuple(item.contract for item in candidate.leg_quotes)
    expiration = contracts[0].expiration
    legs: list[dict[str, object]] = []
    for quoted_leg in candidate.leg_quotes:
        contract = quoted_leg.contract
        leg = quoted_leg.leg
        row: dict[str, object] = {
            "contract_id_ex": contract.contract_id,
            "underlying": contract.underlying,
            "security_type": "OPT",
            "expiration": contract.expiration.isoformat(),
            "strike": _decimal_text(contract.strike),
            "right": contract.right.value,
            "side": "BUY" if leg.side is PositionSide.LONG else "SELL",
            "quantity": leg.quantity,
            "multiplier": _decimal_text(contract.multiplier),
            "currency": contract.currency,
            "exchange": contract.exchange,
            "bid": _decimal_text(quoted_leg.bid),
            "ask": _decimal_text(quoted_leg.ask),
            "quote_time": _datetime_text(quoted_leg.observed_at),
            "quote_snapshot_id": quote_snapshot_id,
        }
        if quoted_leg.last is not None:
            row["last"] = _decimal_text(quoted_leg.last)
        if quoted_leg.implied_volatility is not None:
            row["implied_volatility"] = _decimal_text(
                quoted_leg.implied_volatility
            )
        if quoted_leg.volume is not None:
            row["volume"] = quoted_leg.volume
        if quoted_leg.open_interest is not None:
            row["open_interest"] = quoted_leg.open_interest
        legs.append(row)

    risk_fraction = assessment.risk_fraction
    if risk_fraction is None:
        raise ValueError("proposal risk rejected: risk fraction is unknown")
    return {
        "proposal_id": candidate.candidate_id,
        "candidate_id": candidate.candidate_id,
        "rank": 1,
        "eligible_to_send": True,
        "underlying": contracts[0].underlying,
        "expiration": expiration.isoformat(),
        "dte": (expiration - _valuation_date(now)).days,
        "quote_snapshot_id": quote_snapshot_id,
        "risk_tier": candidate.risk_tier.value,
        "expected_value_usd": _decimal_text(expected_value),
        "expected_value_before_costs_usd": _decimal_text(expected_before_costs),
        "estimated_commissions": _decimal_text(candidate.estimated_commissions),
        "estimated_slippage": _decimal_text(candidate.estimated_slippage),
        "estimated_execution_costs_usd": _decimal_text(
            candidate.estimated_execution_costs
        ),
        "reference_cost_usd": _decimal_text(executable_cost),
        "all_in_executable_cost_usd": _decimal_text(all_in_cost),
        "terminal_scenarios": [
            {
                "terminal_underlying_price": _decimal_text(
                    scenario.terminal_underlying_price
                ),
                "probability": _decimal_text(scenario.probability),
            }
            for scenario in candidate.terminal_scenarios
        ],
        "legs": legs,
        "risk": {
            "maximum_loss_usd": _decimal_text(assessment.payoff.max_loss),
            "risk_fraction": _decimal_text(risk_fraction),
            "allowed_risk_fraction": _decimal_text(
                assessment.allowed_risk_fraction
            ),
            "defined_risk": True,
        },
    }


def _decimal_text(value: Decimal | None) -> str:
    if value is None or not isinstance(value, Decimal) or not value.is_finite():
        raise ValueError("canonical proposal contains an invalid Decimal")
    if value == ZERO:
        return "0"
    return format(value.normalize(), "f")


def _datetime_text(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _valuation_date(value: datetime) -> date:
    """Use the US equity-market date, independent of the caller's display TZ."""

    return value.astimezone(_US_EQUITY_MARKET_TIMEZONE).date()


def _freeze_json(value: object) -> object:
    if isinstance(value, Mapping):
        return MappingProxyType(
            {str(key): _freeze_json(item) for key, item in value.items()}
        )
    if isinstance(value, list):
        return tuple(_freeze_json(item) for item in value)
    return value


__all__ = ["ValidatedProposal", "validate_proposal"]
