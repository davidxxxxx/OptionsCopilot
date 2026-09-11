"""Atomic, hash-bound, supporting-only external 09:20 Top-10 structures.

The file is a research input only.  It can resolve exact option identities into
``ResolvedStructure`` values, but it has no approval, instruction, or order
authority and deliberately contains no dynamic quote data.
"""
from __future__ import annotations

import json
import os
import re
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Callable
from zoneinfo import ZoneInfo

from options_copilot.analytics.scenarios import INITIAL_POLICY_HASH, INITIAL_POLICY_VERSION
from options_copilot.execution_cost import EXECUTION_COST_HASH, EXECUTION_COST_VERSION
from options_copilot.storage.canonical import canonical_hash, canonical_json

from .models import (
    ConditionalOptionLeg,
    ConditionalOptionPreselection,
    OptionLegSide,
    OptionRight,
    PreselectionPhase,
    PreselectionTerminalScenario,
)
from .preselection import strategy_structure_hash
from .preselection_producer import ResolvedStructure


EXTERNAL_TOP10_SCHEMA = "options_copilot.external_top10_structures"
EXTERNAL_TOP10_VERSION = 1
NEW_YORK = ZoneInfo("America/New_York")
PREMARKET_SLOT = time(9, 20)
TOP10_COUNT = 10
MAXIMUM_OBSERVATION_AGE_SECONDS = Decimal("300")
NORMAL_RISK_FRACTION = Decimal("0.10")
HARD_RISK_FRACTION = Decimal("0.20")
MAXIMUM_DOCUMENT_BYTES = 16 * 1024 * 1024

_BATCH_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_IDENTITY_FIELDS = {
    "conId",
    "localSymbol",
    "tradingClass",
    "multiplier",
    "exchange",
    "expiry",
    "strike",
    "right",
}
_DYNAMIC_FIELDS = {
    "bid",
    "ask",
    "quote_asof",
    "quote_batch_id",
    "implied_volatility",
    "delta",
    "gamma",
    "theta",
    "vega",
    "volume",
    "open_interest",
    "dte",
}
_LEG_FIELDS = {"underlying", "identity", "side", "ratio", "quantity", *_DYNAMIC_FIELDS}
_STRUCTURE_FIELDS = {
    "preselection_id",
    "underlying",
    "strategy_type",
    "legs",
    "risk_defined",
    "maximum_loss_usd",
    "estimated_cost_usd",
    "cost_after_ev_usd",
    "entry_condition",
    "invalidation_condition",
    "profit_target_condition",
    "stop_loss_condition",
    "evidence_ids",
    "evidence_hashes",
    "strategy_hash",
    "research_summary",
    "scenario_asof",
    "terminal_scenarios",
    "scenario_hash",
    "execution_cost_contract_version",
    "execution_cost_contract_hash",
    "decision_authority",
    "approval_eligible",
    "instruction_creation_allowed",
    "order_allowed",
}
_PAYLOAD_FIELDS = {
    "batch_id",
    "scheduled_for",
    "observed_at",
    "strategy_nav_usd",
    "current_policy_version",
    "current_policy_hash",
    "normal_risk_fraction",
    "hard_risk_fraction",
    "a_grade_enabled",
    "evidence_hashes",
    "decision_authority",
    "approval_eligible",
    "instruction_creation_allowed",
    "order_allowed",
    "structures",
}
_DOCUMENT_FIELDS = {
    "schema",
    "version",
    "written_at",
    "content_hash",
    *_PAYLOAD_FIELDS,
}
_ALLOWED_STRATEGIES = {
    "LONG_CALL",
    "LONG_PUT",
    "BULL_CALL_VERTICAL",
    "BEAR_CALL_VERTICAL",
    "BULL_PUT_VERTICAL",
    "BEAR_PUT_VERTICAL",
}


class ExternalTop10ValidationError(ValueError):
    """The external Top-10 file is not one trusted supporting-only batch."""


@dataclass(frozen=True, slots=True)
class TrustedTerminalScenarioSet:
    """Complete point-in-time scenario contract for one external candidate."""

    candidate_id: str
    strategy_hash: str
    scenario_asof: datetime
    scenarios: tuple[PreselectionTerminalScenario, ...]
    current_policy_version: str
    current_policy_hash: str
    scenario_hash: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "scenarios", tuple(self.scenarios))
        if isinstance(self.scenario_asof, datetime) and self.scenario_asof.tzinfo is not None:
            object.__setattr__(
                self,
                "scenario_asof",
                self.scenario_asof.astimezone(timezone.utc),
            )

    @classmethod
    def create(
        cls,
        *,
        candidate_id: str,
        strategy_hash: str,
        scenario_asof: datetime,
        scenarios: Sequence[PreselectionTerminalScenario],
        current_policy_version: str,
        current_policy_hash: str,
    ) -> "TrustedTerminalScenarioSet":
        frozen = tuple(scenarios)
        normalized_asof = _aware(scenario_asof, "scenario_asof")
        scenario_hash = trusted_terminal_scenario_set_hash(
            candidate_id=candidate_id,
            strategy_hash=strategy_hash,
            scenario_asof=normalized_asof,
            scenarios=frozen,
            current_policy_version=current_policy_version,
            current_policy_hash=current_policy_hash,
        )
        return cls(
            candidate_id=candidate_id,
            strategy_hash=strategy_hash,
            scenario_asof=normalized_asof,
            scenarios=frozen,
            current_policy_version=current_policy_version,
            current_policy_hash=current_policy_hash,
            scenario_hash=scenario_hash,
        )

    def hash_payload(self) -> dict[str, object]:
        return _trusted_scenario_payload(
            candidate_id=self.candidate_id,
            strategy_hash=self.strategy_hash,
            scenario_asof=self.scenario_asof,
            scenarios=self.scenarios,
            current_policy_version=self.current_policy_version,
            current_policy_hash=self.current_policy_hash,
        )

    def as_dict(self) -> dict[str, object]:
        return {**self.hash_payload(), "scenario_hash": self.scenario_hash}

    def verify_hash(self) -> bool:
        try:
            return (
                isinstance(self.scenario_hash, str)
                and _SHA256.fullmatch(self.scenario_hash) is not None
                and canonical_hash(self.hash_payload()) == self.scenario_hash
            )
        except (ArithmeticError, TypeError, ValueError):
            return False


@dataclass(frozen=True, slots=True)
class ExternalResolvedStructure(ResolvedStructure):
    """Backward-compatible structure carrying its complete scenario contract."""

    scenario_set: TrustedTerminalScenarioSet

    def __post_init__(self) -> None:
        ResolvedStructure.__post_init__(self)
        if not isinstance(self.scenario_set, TrustedTerminalScenarioSet):
            raise TypeError("scenario_set must be a TrustedTerminalScenarioSet")
        candidate = self.candidate
        if (
            self.scenario_set.candidate_id != candidate.preselection_id
            or self.scenario_set.strategy_hash != candidate.strategy_hash
            or self.scenario_set.scenarios != candidate.terminal_scenarios
            or self.scenario_set.scenario_hash != candidate.scenario_hash
        ):
            raise ValueError("scenario_set does not bind the resolved candidate")


def trusted_terminal_scenario_set_hash(
    *,
    candidate_id: str,
    strategy_hash: str,
    scenario_asof: datetime,
    scenarios: Sequence[PreselectionTerminalScenario],
    current_policy_version: str,
    current_policy_hash: str,
) -> str:
    """Hash every scenario input and its candidate, time, and policy lineage."""

    return canonical_hash(
        _trusted_scenario_payload(
            candidate_id=candidate_id,
            strategy_hash=strategy_hash,
            scenario_asof=scenario_asof,
            scenarios=tuple(scenarios),
            current_policy_version=current_policy_version,
            current_policy_hash=current_policy_hash,
        )
    )


def _trusted_scenario_payload(
    *,
    candidate_id: str,
    strategy_hash: str,
    scenario_asof: datetime,
    scenarios: Sequence[PreselectionTerminalScenario],
    current_policy_version: str,
    current_policy_hash: str,
) -> dict[str, object]:
    return {
        "schema": "options_copilot.trusted_terminal_scenario_set.v1",
        "candidate_id": candidate_id,
        "strategy_hash": strategy_hash,
        "scenario_asof": scenario_asof,
        "scenarios": tuple(
            {
                "terminal_underlying_price": item.terminal_underlying_price,
                "probability": item.probability,
            }
            for item in scenarios
        ),
        "current_policy_version": current_policy_version,
        "current_policy_hash": current_policy_hash,
    }


class ExternalTop10Publisher:
    """Validate and atomically publish exactly ten pre-market structures."""

    def __init__(
        self,
        path: Path | str,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.path = Path(path)
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    def publish(self, payload: Mapping[str, object]) -> tuple[ResolvedStructure, ...]:
        if not isinstance(payload, Mapping) or set(payload) != _PAYLOAD_FIELDS:
            raise ExternalTop10ValidationError("external Top-10 payload fields are incomplete")
        written_at = _aware(self._clock(), "publisher clock")
        try:
            normalized = json.loads(canonical_json(payload), object_pairs_hook=_unique_object)
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ExternalTop10ValidationError("external Top-10 payload is not canonical") from exc
        document: dict[str, object] = {
            "schema": EXTERNAL_TOP10_SCHEMA,
            "version": EXTERNAL_TOP10_VERSION,
            **normalized,
            "written_at": written_at.isoformat(timespec="microseconds"),
        }
        document["content_hash"] = canonical_hash(document)
        structures, _ = _parse_document(document, now=written_at)
        rendered = (canonical_json(document) + "\n").encode("utf-8")
        if len(rendered) > MAXIMUM_DOCUMENT_BYTES:
            raise ExternalTop10ValidationError("external Top-10 document is too large")
        _atomic_replace(self.path, rendered)
        return structures


class ExternalTop10StructureSource:
    """Read-only ``resolve_top10`` adapter for the independent producer."""

    def __init__(
        self,
        path: Path | str,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.path = Path(path)
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    def resolve_top10(
        self,
        *,
        scheduled_for: datetime,
    ) -> tuple[ResolvedStructure, ...]:
        requested = _aware(scheduled_for, "requested scheduled_for")
        requested_et = requested.astimezone(NEW_YORK)
        if requested_et.time().replace(tzinfo=None) != PREMARKET_SLOT:
            raise ExternalTop10ValidationError(
                "requested scheduled_for must be the exact 09:20 ET slot"
            )
        document = _read_json(self.path)
        structures, document_slot = _parse_document(
            document,
            now=_aware(self._clock(), "reader clock"),
        )
        if requested != document_slot:
            raise ExternalTop10ValidationError(
                "requested scheduled_for does not match the external batch"
            )
        return structures


def _parse_document(
    document: object,
    *,
    now: datetime,
) -> tuple[tuple[ResolvedStructure, ...], datetime]:
    if not isinstance(document, Mapping) or set(document) != _DOCUMENT_FIELDS:
        raise ExternalTop10ValidationError("external Top-10 top-level fields are incomplete")
    if document["schema"] != EXTERNAL_TOP10_SCHEMA:
        raise ExternalTop10ValidationError("external Top-10 schema is unsupported")
    version = document["version"]
    if isinstance(version, bool) or version != EXTERNAL_TOP10_VERSION:
        raise ExternalTop10ValidationError("external Top-10 version is unsupported")
    content_hash = _digest(document["content_hash"], "content hash")
    unsigned = dict(document)
    unsigned.pop("content_hash")
    if canonical_hash(unsigned) != content_hash:
        raise ExternalTop10ValidationError("external Top-10 content hash mismatch")

    batch_id = document["batch_id"]
    if not isinstance(batch_id, str) or _BATCH_ID.fullmatch(batch_id) is None:
        raise ExternalTop10ValidationError("external Top-10 batch_id is invalid")
    scheduled_for = _timestamp(document["scheduled_for"], "scheduled_for")
    scheduled_et = scheduled_for.astimezone(NEW_YORK)
    if scheduled_et.time().replace(tzinfo=None) != PREMARKET_SLOT:
        raise ExternalTop10ValidationError("scheduled_for must be the exact 09:20 ET slot")
    observed_at = _timestamp(document["observed_at"], "observed_at")
    written_at = _timestamp(document["written_at"], "written_at")
    now = _aware(now, "reader clock")
    if not observed_at <= written_at <= now:
        raise ExternalTop10ValidationError("external Top-10 timestamps are invalid")
    age = Decimal(str((now - observed_at).total_seconds()))
    if age > MAXIMUM_OBSERVATION_AGE_SECONDS:
        raise ExternalTop10ValidationError(
            "external Top-10 observation is older than five minutes"
        )
    if observed_at.astimezone(NEW_YORK).date() != scheduled_et.date():
        raise ExternalTop10ValidationError("observation and 09:20 ET slot dates differ")

    nav = _positive_decimal(document["strategy_nav_usd"], "strategy_nav_usd")
    current_policy_version = _text(
        document["current_policy_version"],
        "current_policy_version",
    )
    if current_policy_version != INITIAL_POLICY_VERSION:
        raise ExternalTop10ValidationError(
            "external Top-10 risk policy version is not current"
        )
    current_policy_hash = _digest(
        document["current_policy_hash"],
        "current_policy_hash",
    )
    if current_policy_hash != INITIAL_POLICY_HASH:
        raise ExternalTop10ValidationError(
            "external Top-10 risk policy hash is not current"
        )
    if _decimal(document["normal_risk_fraction"], "normal_risk_fraction") != NORMAL_RISK_FRACTION:
        raise ExternalTop10ValidationError("normal risk fraction must remain 10%")
    if _decimal(document["hard_risk_fraction"], "hard_risk_fraction") != HARD_RISK_FRACTION:
        raise ExternalTop10ValidationError("hard risk fraction must remain 20%")
    if document["a_grade_enabled"] is not False:
        raise ExternalTop10ValidationError("A-grade is not supported by this source")
    _supporting_only(document, "batch")

    rows = _array(document["structures"], "structures")
    if len(rows) != TOP10_COUNT:
        raise ExternalTop10ValidationError(
            "external Top-10 must contain exactly ten structures"
        )
    preselection_ids = [row.get("preselection_id") for row in rows]
    if len(set(_text(item, "preselection_id") for item in preselection_ids)) != TOP10_COUNT:
        raise ExternalTop10ValidationError("duplicate preselection id")
    strategy_hashes = [_digest(row.get("strategy_hash"), "strategy hash") for row in rows]
    if len(set(strategy_hashes)) != TOP10_COUNT:
        raise ExternalTop10ValidationError("duplicate strategy hash")

    seen_contracts: set[int] = set()
    structures: list[ExternalResolvedStructure] = []
    batch_evidence: list[str] = []
    for index, row in enumerate(rows):
        resolved = _structure(
            row,
            index=index,
            scheduled_for=scheduled_for,
            observed_at=observed_at,
            strategy_nav=nav,
            current_policy_version=current_policy_version,
            current_policy_hash=current_policy_hash,
            seen_contracts=seen_contracts,
        )
        batch_evidence.extend(resolved.candidate.evidence_hashes)
        structures.append(resolved)

    declared_evidence = _digest_array(document["evidence_hashes"], "evidence_hashes")
    if tuple(dict.fromkeys(batch_evidence)) != declared_evidence:
        raise ExternalTop10ValidationError("batch evidence hashes do not match structures")
    return tuple(structures), scheduled_for


def _structure(
    row: Mapping[str, object],
    *,
    index: int,
    scheduled_for: datetime,
    observed_at: datetime,
    strategy_nav: Decimal,
    current_policy_version: str,
    current_policy_hash: str,
    seen_contracts: set[int],
) -> ExternalResolvedStructure:
    name = f"structures[{index}]"
    if set(row) != _STRUCTURE_FIELDS:
        raise ExternalTop10ValidationError(f"{name} fields are incomplete")
    _supporting_only(row, name)
    preselection_id = _text(row["preselection_id"], f"{name}.preselection_id")
    underlying = _symbol(row["underlying"], f"{name}.underlying")
    strategy_type = _text(row["strategy_type"], f"{name}.strategy_type").upper()
    if strategy_type not in _ALLOWED_STRATEGIES:
        raise ExternalTop10ValidationError(f"{name} strategy is not allowed")
    if row["risk_defined"] is not True:
        raise ExternalTop10ValidationError(f"{name} must be explicitly defined-risk")
    maximum_loss = _positive_decimal(row["maximum_loss_usd"], f"{name}.maximum_loss_usd")
    hard_cap = strategy_nav * HARD_RISK_FRACTION
    normal_cap = strategy_nav * NORMAL_RISK_FRACTION
    if maximum_loss >= hard_cap:
        raise ExternalTop10ValidationError(f"{name} breaches the permanent 20% reject line")
    if maximum_loss > normal_cap:
        raise ExternalTop10ValidationError(f"{name} breaches the normal 10% max-loss cap")
    estimated_cost = _nonnegative_decimal(row["estimated_cost_usd"], f"{name}.estimated_cost_usd")
    cost_after_ev = _positive_decimal(row["cost_after_ev_usd"], f"{name}.cost_after_ev_usd")
    if row["execution_cost_contract_version"] != EXECUTION_COST_VERSION:
        raise ExternalTop10ValidationError(
            f"{name} execution-cost contract version is not current"
        )
    if _digest(
        row["execution_cost_contract_hash"],
        f"{name}.execution_cost_contract_hash",
    ) != EXECUTION_COST_HASH:
        raise ExternalTop10ValidationError(
            f"{name} execution-cost contract hash is not current"
        )
    scenario_asof = _timestamp(row["scenario_asof"], f"{name}.scenario_asof")
    if scenario_asof > observed_at:
        raise ExternalTop10ValidationError(
            f"{name} scenario_asof contains future information"
        )
    scenario_age = Decimal(str((observed_at - scenario_asof).total_seconds()))
    if scenario_age > MAXIMUM_OBSERVATION_AGE_SECONDS:
        raise ExternalTop10ValidationError(f"{name} scenario_asof is stale")
    if scenario_asof.astimezone(NEW_YORK).date() != scheduled_for.astimezone(NEW_YORK).date():
        raise ExternalTop10ValidationError(
            f"{name} scenario_asof is outside the trading date"
        )
    scenarios = _terminal_scenarios(row["terminal_scenarios"], name=name)
    declared_scenario_hash = _digest(row["scenario_hash"], f"{name}.scenario_hash")
    evidence_ids = _text_array(row["evidence_ids"], f"{name}.evidence_ids")
    evidence_hashes = _digest_array(row["evidence_hashes"], f"{name}.evidence_hashes")
    if not evidence_ids or not evidence_hashes:
        raise ExternalTop10ValidationError(f"{name} evidence is incomplete")

    raw_legs = _array(row["legs"], f"{name}.legs")
    typed_legs = tuple(
        _leg(
            leg,
            name=f"{name}.legs[{leg_index}]",
            underlying=underlying,
            scheduled_for=scheduled_for,
            seen_contracts=seen_contracts,
        )
        for leg_index, leg in enumerate(raw_legs)
    )
    _validate_strategy(strategy_type, typed_legs, name)
    declared_hash = _digest(row["strategy_hash"], f"{name}.strategy_hash")
    expected_hash = strategy_structure_hash(underlying, strategy_type, typed_legs)
    if declared_hash != expected_hash:
        raise ExternalTop10ValidationError(f"{name} strategy hash mismatch")
    candidate = ConditionalOptionPreselection(
        preselection_id=preselection_id,
        underlying=underlying,
        strategy_type=strategy_type,
        phase=PreselectionPhase.PRE_MARKET,
        legs=typed_legs,
        risk_defined=True,
        maximum_loss_usd=maximum_loss,
        estimated_cost_usd=estimated_cost,
        cost_after_ev_usd=cost_after_ev,
        entry_condition=_text(row["entry_condition"], f"{name}.entry_condition"),
        invalidation_condition=_text(
            row["invalidation_condition"], f"{name}.invalidation_condition"
        ),
        profit_target_condition=_text(
            row["profit_target_condition"], f"{name}.profit_target_condition"
        ),
        stop_loss_condition=_text(
            row["stop_loss_condition"], f"{name}.stop_loss_condition"
        ),
        evidence_ids=evidence_ids,
        evidence_hashes=evidence_hashes,
        strategy_hash=declared_hash,
        research_summary=_text(row["research_summary"], f"{name}.research_summary"),
        terminal_scenarios=scenarios,
        scenario_asof=scenario_asof,
        scenario_hash=declared_scenario_hash,
        execution_cost_contract_version=EXECUTION_COST_VERSION,
        execution_cost_contract_hash=EXECUTION_COST_HASH,
        risk_policy_version=current_policy_version,
        risk_policy_hash=current_policy_hash,
    )
    scenario_set = TrustedTerminalScenarioSet(
        candidate_id=preselection_id,
        strategy_hash=declared_hash,
        scenario_asof=scenario_asof,
        scenarios=scenarios,
        current_policy_version=current_policy_version,
        current_policy_hash=current_policy_hash,
        scenario_hash=declared_scenario_hash,
    )
    if not scenario_set.verify_hash():
        raise ExternalTop10ValidationError(f"{name} scenario-set hash mismatch")
    return ExternalResolvedStructure(candidate, scenario_set)


def _leg(
    row: Mapping[str, object],
    *,
    name: str,
    underlying: str,
    scheduled_for: datetime,
    seen_contracts: set[int],
) -> ConditionalOptionLeg:
    if set(row) != _LEG_FIELDS:
        raise ExternalTop10ValidationError(f"{name} fields are incomplete")
    if row["underlying"] != underlying:
        raise ExternalTop10ValidationError(f"{name} underlying differs within structure")
    if any(row[field] is not None for field in _DYNAMIC_FIELDS):
        raise ExternalTop10ValidationError(f"{name} dynamic quote fields must be empty")
    identity = _identity(row["identity"], f"{name}.identity")
    contract_id = int(identity["conId"])
    if contract_id in seen_contracts:
        raise ExternalTop10ValidationError("duplicate conId across external Top-10")
    seen_contracts.add(contract_id)
    expiry = date.fromisoformat(str(identity["expiry"]))
    dte = (expiry - scheduled_for.astimezone(NEW_YORK).date()).days
    if dte < 14 or dte > 35:
        raise ExternalTop10ValidationError(f"{name} expiry is outside normal 14-35 DTE")
    side_text = _text(row["side"], f"{name}.side")
    if side_text not in {"BUY", "SELL"}:
        raise ExternalTop10ValidationError(f"{name}.side is invalid")
    ratio = _positive_integer(row["ratio"], f"{name}.ratio")
    quantity = _positive_integer(row["quantity"], f"{name}.quantity")
    if ratio != 1:
        raise ExternalTop10ValidationError(f"{name} ratio must be 1")
    return ConditionalOptionLeg(
        underlying=underlying,
        con_id=contract_id,
        expiry=expiry,
        strike=_positive_decimal(identity["strike"], f"{name}.identity.strike"),
        right=OptionRight(str(identity["right"])),
        side=OptionLegSide(side_text),
        ratio=ratio,
        quantity=quantity,
        bid=None,
        ask=None,
        quote_asof=None,
        quote_batch_id=None,
        implied_volatility=None,
        delta=None,
        gamma=None,
        theta=None,
        vega=None,
        volume=None,
        open_interest=None,
        dte=None,
        local_symbol=str(identity["localSymbol"]),
        trading_class=str(identity["tradingClass"]),
        multiplier=100,
        exchange=str(identity["exchange"]),
    )


def _validate_strategy(
    strategy: str,
    legs: tuple[ConditionalOptionLeg, ...],
    name: str,
) -> None:
    if strategy in {"LONG_CALL", "LONG_PUT"}:
        wanted_right = OptionRight.CALL if strategy == "LONG_CALL" else OptionRight.PUT
        if (
            len(legs) != 1
            or legs[0].right is not wanted_right
            or legs[0].side is not OptionLegSide.BUY
            or legs[0].ratio != 1
        ):
            raise ExternalTop10ValidationError(f"{name} long-option direction is invalid")
        return
    if len(legs) != 2 or legs[0].quantity != legs[1].quantity:
        raise ExternalTop10ValidationError(f"{name} vertical must be a 1:1 quantity pair")
    if legs[0].expiry != legs[1].expiry:
        raise ExternalTop10ValidationError(f"{name} vertical expiry differs within structure")
    ordered = sorted(legs, key=lambda item: item.strike or Decimal("0"))
    lower, higher = ordered
    if lower.strike == higher.strike:
        raise ExternalTop10ValidationError(f"{name} vertical strikes must differ")
    patterns = {
        "BULL_CALL_VERTICAL": (OptionRight.CALL, OptionLegSide.BUY, OptionLegSide.SELL),
        "BEAR_CALL_VERTICAL": (OptionRight.CALL, OptionLegSide.SELL, OptionLegSide.BUY),
        "BULL_PUT_VERTICAL": (OptionRight.PUT, OptionLegSide.BUY, OptionLegSide.SELL),
        "BEAR_PUT_VERTICAL": (OptionRight.PUT, OptionLegSide.SELL, OptionLegSide.BUY),
    }
    right, lower_side, higher_side = patterns[strategy]
    if (
        lower.right is not right
        or higher.right is not right
        or lower.side is not lower_side
        or higher.side is not higher_side
        or lower.ratio != 1
        or higher.ratio != 1
    ):
        raise ExternalTop10ValidationError(f"{name} vertical direction/strike relationship is invalid")


def _terminal_scenarios(
    value: object,
    *,
    name: str,
) -> tuple[PreselectionTerminalScenario, ...]:
    rows = _array(value, f"{name}.terminal_scenarios")
    if not rows:
        raise ExternalTop10ValidationError(f"{name} terminal scenarios are missing")
    scenarios: list[PreselectionTerminalScenario] = []
    try:
        for index, row in enumerate(rows):
            if set(row) != {"terminal_underlying_price", "probability"}:
                raise ExternalTop10ValidationError(
                    f"{name}.terminal_scenarios[{index}] fields are incomplete"
                )
            scenarios.append(
                PreselectionTerminalScenario(
                    terminal_underlying_price=_nonnegative_decimal(
                        row["terminal_underlying_price"],
                        f"{name}.terminal_scenarios[{index}].terminal_underlying_price",
                    ),
                    probability=_positive_decimal(
                        row["probability"],
                        f"{name}.terminal_scenarios[{index}].probability",
                    ),
                )
            )
    except (TypeError, ValueError) as exc:
        if isinstance(exc, ExternalTop10ValidationError):
            raise
        raise ExternalTop10ValidationError(
            f"{name} terminal scenarios are invalid"
        ) from exc
    result = tuple(scenarios)
    if sum((item.probability for item in result), Decimal("0")) != Decimal("1"):
        raise ExternalTop10ValidationError(
            f"{name} terminal scenario probabilities must sum to one"
        )
    prices = tuple(item.terminal_underlying_price for item in result)
    if len(set(prices)) != len(prices):
        raise ExternalTop10ValidationError(
            f"{name} terminal scenario prices must be unique"
        )
    return result


def _identity(value: object, name: str) -> dict[str, object]:
    row = _mapping(value, name)
    if set(row) != _IDENTITY_FIELDS:
        raise ExternalTop10ValidationError(f"{name} identity fields are incomplete")
    contract_id = _positive_integer(row["conId"], f"{name}.conId")
    for field in ("localSymbol", "tradingClass", "exchange"):
        _text(row[field], f"{name}.{field}")
    multiplier = row["multiplier"]
    if isinstance(multiplier, bool) or multiplier != 100:
        raise ExternalTop10ValidationError(f"{name}.multiplier must be integer 100")
    expiry = _date(row["expiry"], f"{name}.expiry")
    strike = _positive_decimal(row["strike"], f"{name}.strike")
    right = row["right"]
    if right not in {"CALL", "PUT"}:
        raise ExternalTop10ValidationError(f"{name}.right is invalid")
    return {
        "conId": contract_id,
        "localSymbol": str(row["localSymbol"]),
        "tradingClass": str(row["tradingClass"]),
        "multiplier": 100,
        "exchange": str(row["exchange"]).upper(),
        "expiry": expiry.isoformat(),
        "strike": strike,
        "right": str(right),
    }


def _supporting_only(row: Mapping[str, object], name: str) -> None:
    if (
        row.get("decision_authority") != "SUPPORTING_ONLY"
        or row.get("approval_eligible") is not False
        or row.get("instruction_creation_allowed") is not False
        or row.get("order_allowed") is not False
    ):
        raise ExternalTop10ValidationError(f"{name} must remain permanently SUPPORTING_ONLY")


def _read_json(path: Path) -> Mapping[str, object]:
    try:
        size = path.stat().st_size
        if size <= 0 or size > MAXIMUM_DOCUMENT_BYTES:
            raise ExternalTop10ValidationError("external Top-10 document size is invalid")
        payload = path.read_bytes()
    except ExternalTop10ValidationError:
        raise
    except OSError as exc:
        raise ExternalTop10ValidationError("external Top-10 file is unavailable") from exc
    try:
        document = json.loads(payload.decode("utf-8"), object_pairs_hook=_unique_object)
    except (UnicodeDecodeError, json.JSONDecodeError, ExternalTop10ValidationError) as exc:
        raise ExternalTop10ValidationError("external Top-10 JSON is invalid") from exc
    return _mapping(document, "document")


def _mapping(value: object, name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or not all(isinstance(key, str) for key in value):
        raise ExternalTop10ValidationError(f"{name} must be a known object")
    return value


def _array(value: object, name: str) -> tuple[Mapping[str, object], ...]:
    if isinstance(value, (str, bytes, bytearray, memoryview)) or not isinstance(value, Sequence):
        raise ExternalTop10ValidationError(f"{name} must be a known array")
    return tuple(_mapping(item, f"{name}[{index}]") for index, item in enumerate(value))


def _text_array(value: object, name: str) -> tuple[str, ...]:
    if isinstance(value, (str, bytes, bytearray, memoryview)) or not isinstance(value, Sequence):
        raise ExternalTop10ValidationError(f"{name} must be an array")
    values = tuple(_text(item, name) for item in value)
    if len(values) != len(set(values)):
        raise ExternalTop10ValidationError(f"{name} contains duplicates")
    return values


def _digest_array(value: object, name: str) -> tuple[str, ...]:
    if isinstance(value, (str, bytes, bytearray, memoryview)) or not isinstance(value, Sequence):
        raise ExternalTop10ValidationError(f"{name} must be an array")
    values = tuple(_digest(item, name) for item in value)
    if not values or len(values) != len(set(values)):
        raise ExternalTop10ValidationError(f"{name} is empty or contains duplicates")
    return values


def _text(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ExternalTop10ValidationError(f"{name} cannot be blank")
    return value.strip()


def _symbol(value: object, name: str) -> str:
    symbol = _text(value, name).upper()
    if len(symbol) > 12 or not symbol.replace(".", "").isalnum():
        raise ExternalTop10ValidationError(f"{name} is invalid")
    return symbol


def _digest(value: object, name: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise ExternalTop10ValidationError(f"{name} must be a lowercase SHA-256 digest")
    return value


def _timestamp(value: object, name: str) -> datetime:
    if isinstance(value, datetime):
        return _aware(value, name)
    if not isinstance(value, str):
        raise ExternalTop10ValidationError(f"{name} must be a timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ExternalTop10ValidationError(f"{name} must be a timestamp") from exc
    return _aware(parsed, name)


def _aware(value: datetime, name: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ExternalTop10ValidationError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _date(value: object, name: str) -> date:
    if not isinstance(value, str):
        raise ExternalTop10ValidationError(f"{name} must be an ISO date")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ExternalTop10ValidationError(f"{name} must be an ISO date") from exc


def _decimal(value: object, name: str) -> Decimal:
    raw = value
    if isinstance(value, Mapping) and set(value) == {"$decimal"}:
        raw = value["$decimal"]
    if raw is None or isinstance(raw, (bool, float)):
        raise ExternalTop10ValidationError(f"{name} must be an exact decimal")
    try:
        result = Decimal(str(raw))
    except (InvalidOperation, ValueError) as exc:
        raise ExternalTop10ValidationError(f"{name} must be an exact decimal") from exc
    if not result.is_finite():
        raise ExternalTop10ValidationError(f"{name} must be finite")
    return result


def _positive_decimal(value: object, name: str) -> Decimal:
    result = _decimal(value, name)
    if result <= 0:
        raise ExternalTop10ValidationError(f"{name} must be finite and positive")
    return result


def _nonnegative_decimal(value: object, name: str) -> Decimal:
    result = _decimal(value, name)
    if result < 0:
        raise ExternalTop10ValidationError(f"{name} must be nonnegative")
    return result


def _positive_integer(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ExternalTop10ValidationError(f"{name} must be a positive integer")
    return value


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ExternalTop10ValidationError(f"external Top-10 JSON repeats key {key!r}")
        result[key] = value
    return result


def _atomic_replace(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = -1
    temporary: Path | None = None
    try:
        descriptor, raw_path = tempfile.mkstemp(
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
        )
        temporary = Path(raw_path)
        with os.fdopen(descriptor, "wb") as handle:
            descriptor = -1
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        temporary = None
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if temporary is not None:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass


__all__ = [
    "EXTERNAL_TOP10_SCHEMA",
    "EXTERNAL_TOP10_VERSION",
    "ExternalResolvedStructure",
    "ExternalTop10Publisher",
    "ExternalTop10StructureSource",
    "ExternalTop10ValidationError",
    "TrustedTerminalScenarioSet",
    "trusted_terminal_scenario_set_hash",
]
