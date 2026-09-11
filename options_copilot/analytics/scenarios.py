"""Policy-bound scenario probabilities and fail-closed eligibility."""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, fields, is_dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_EVEN, localcontext
from enum import Enum
from pathlib import Path
import threading
from typing import Any, Callable, Protocol

from options_copilot.governance.contracts import (
    ContractKind,
    SignedContract,
    load_contract,
    verify_contract,
)
from options_copilot.storage.canonical import canonical_hash, freeze_json, utc_datetime
from options_copilot.analytics.economic_gates import signed_economics_reasons


ZERO, ONE, QUANTUM = Decimal("0"), Decimal("1"), Decimal("0.000001")
INITIAL_POLICY_VERSION = "v1"
INITIAL_POLICY_HASH = "b5d969d13fc624cdb2c49b3a78ce0fa34119bedd95f51db7b25ba9c977f55a3c"
INITIAL_POLICY_EFFECTIVE_AT = "2026-08-03T16:11:43.941811+00:00"

class ScenarioAction(str, Enum): TRADE = "TRADE"; NO_TRADE = "NO_TRADE"

@dataclass(frozen=True, slots=True)
class ResolvedPolicy:
    current_policy_version: str
    current_policy_hash: str
    policy_authority_marker_hash: str
    effective_at: datetime
    payload: object
    calibration_provenance: object

class PolicyResolver(Protocol):
    def resolve(self, *, now: datetime) -> ResolvedPolicy: ...
    def is_current(self, resolution: ResolvedPolicy) -> bool: ...

class InitialPolicyResolver:
    """The immutable Initial Champion fallback used until signed P9 promotion."""
    def __init__(
        self,
        path: str | Path | None = None,
        *,
        maximum_age: timedelta | None = None,
    ) -> None:
        self.path = Path(path) if path else Path(__file__).parents[1] / "governance" / "initial_champion_scenario_policy.v1.json"
        if maximum_age is not None and maximum_age <= timedelta(0):
            raise ValueError("maximum_age must be positive or None")
        self.maximum_age = maximum_age
        self._guard_lock = threading.RLock()
    def resolve(self, *, now: datetime) -> ResolvedPolicy:
        now = utc_datetime(now, field="now")
        contract = self._load_contract(as_of=now)
        effective = contract.effective_at
        if effective > now or (
            self.maximum_age is not None
            and now - effective > self.maximum_age
        ):
            raise ValueError("initial policy is future or stale")
        payload = contract.payload
        provenance = contract.provenance
        if provenance.get("execution_cost_contract_hash") != payload.get("hard_no_trade_thresholds", {}).get("cost_and_expectancy", {}).get("execution_cost_contract_hash"): raise ValueError("initial policy bindings invalid")
        return self._resolution(contract)

    def is_current(self, resolution: ResolvedPolicy) -> bool:
        """Re-verify the immutable checked-in head before a ranking commit."""

        if not isinstance(resolution, ResolvedPolicy):
            return False
        try:
            contract = self._load_contract()
        except Exception:
            return False
        return resolution == self._resolution(contract)

    def guard_current(
        self,
        resolution: ResolvedPolicy,
        *,
        callback: Callable[[], object],
    ) -> object | None:
        if not callable(callback):
            raise TypeError("callback must be callable")
        with self._guard_lock:
            if not self.is_current(resolution):
                return None
            return callback()

    def _load_contract(self, *, as_of: datetime | None = None) -> SignedContract:
        expectations: dict[str, object] = {
            "expected_kind": ContractKind.INITIAL_CHAMPION_SCENARIO_POLICY,
            "expected_version": INITIAL_POLICY_VERSION,
            "expected_hash": INITIAL_POLICY_HASH,
            "expected_effective_at": INITIAL_POLICY_EFFECTIVE_AT,
        }
        if as_of is not None:
            expectations["as_of"] = as_of
        loaded = load_contract(self.path, **expectations)
        # Keep the resolver's consumer contract explicit even though load_contract
        # already parses the artifact: an existing SignedContract is reconstructed
        # here so the canonical hash is checked again at the decision boundary.
        return verify_contract(loaded, **expectations)

    @staticmethod
    def _resolution(contract: SignedContract) -> ResolvedPolicy:
        document = contract.to_dict()
        return ResolvedPolicy(
            contract.version,
            contract.contract_hash,
            canonical_hash(document),
            contract.effective_at,
            freeze_json(contract.payload),
            freeze_json(contract.payload.get("calibration", {})),
        )

@dataclass(frozen=True, slots=True)
class Scenario:
    name: str
    probability: Decimal
    terminal_price: Decimal

@dataclass(frozen=True, slots=True)
class ScenarioDecision:
    action: ScenarioAction
    reasons: tuple[str, ...]
    scenarios: tuple[Scenario, ...]
    after_cost_expected_value: Decimal | None
    current_policy_version: str | None
    current_policy_hash: str | None
    policy_authority_marker_hash: str | None
    risk_authority_version: str | None
    risk_authority_marker_hash: str | None
    risk_contract_hash: str | None
    cost_version: str | None
    cost_hash: str | None
    result_hash: str

class ScenarioEngine:
    def __init__(self, resolver: PolicyResolver | None = None) -> None: self.resolver = resolver or InitialPolicyResolver()
    def evaluate(
        self,
        raw: Mapping[str, Any] | object,
        *,
        now: datetime | None = None,
        resolved_policy: ResolvedPolicy | None = None,
        risk_authority: object | None = None,
    ) -> ScenarioDecision:
        now = utc_datetime(now or datetime.now(timezone.utc), field="now")
        doc = _mapping(raw)
        try: policy = resolved_policy or self.resolver.resolve(now=now)
        except Exception: return self._no_trade(("POLICY_UNAVAILABLE",), None, doc)
        reasons = _hard_gate_reasons(doc, now, policy)
        reasons.extend(_authority_disagreements(doc, policy, risk_authority))
        if reasons: return self._no_trade(tuple(sorted(set(reasons))), policy, doc, risk_authority)
        spot, iv, dte = _dec(doc["spot"]), _dec(doc["atm_iv"]), int(doc["dte"])
        assert spot is not None and iv is not None
        expected = max(Decimal("0.005"), iv * (Decimal(dte) / Decimal("365")).sqrt())
        direction, volatility = _dec(doc.get("market_score")), _dec(doc.get("volatility_score"))
        assert direction is not None and volatility is not None
        scenarios = _probabilities(spot, expected, direction, volatility)
        if sum(s.probability for s in scenarios) != ONE: return self._no_trade(("PROBABILITY_SUM_INVALID",), policy, doc, risk_authority)
        ev = _dec(doc.get("after_cost_expected_value"))
        if ev is None or ev <= ZERO: return self._no_trade(("NON_POSITIVE_AFTER_COST_EV",), policy, doc, risk_authority)
        thresholds = _mapping(_mapping(policy.payload).get("hard_no_trade_thresholds", {})).get("cost_and_expectancy", {})
        economics = signed_economics_reasons(doc, _mapping(thresholds))
        if economics:
            return self._no_trade(economics, policy, doc, risk_authority)
        return self._decision(ScenarioAction.TRADE, (), scenarios, ev, policy, doc, risk_authority)
    def evaluate_pre_cost(
        self,
        raw: Mapping[str, Any] | object,
        *,
        now: datetime | None = None,
        resolved_policy: ResolvedPolicy | None = None,
        risk_authority: object | None = None,
    ) -> ScenarioDecision:
        """Resolve only broker-bound probabilities before signed costs are applied.

        Consumers must call their signed cost port before treating this as a
        tradable outcome; its ``after_cost_expected_value`` is intentionally
        ``None``.  This keeps externally supplied EV from becoming authority.
        """
        now = utc_datetime(now or datetime.now(timezone.utc), field="now")
        doc = _mapping(raw)
        try: policy = resolved_policy or self.resolver.resolve(now=now)
        except Exception: return self._no_trade(("POLICY_UNAVAILABLE",), None, doc)
        reasons = _hard_gate_reasons(doc, now, policy, require_cost=False)
        reasons.extend(_authority_disagreements(doc, policy, risk_authority))
        if reasons: return self._no_trade(tuple(sorted(set(reasons))), policy, doc, risk_authority)
        spot, iv, dte = _dec(doc["spot"]), _dec(doc["atm_iv"]), int(doc["dte"])
        assert spot is not None and iv is not None
        direction, volatility = _dec(doc.get("market_score")), _dec(doc.get("volatility_score"))
        assert direction is not None and volatility is not None
        scenarios = _probabilities(spot, max(Decimal("0.005"), iv * (Decimal(dte) / Decimal("365")).sqrt()), direction, volatility)
        if sum(item.probability for item in scenarios) != ONE:
            return self._no_trade(("PROBABILITY_SUM_INVALID",), policy, doc, risk_authority)
        return self._decision(ScenarioAction.TRADE, (), scenarios, None, policy, doc, risk_authority)
    assess = evaluate
    def _no_trade(self, reasons: tuple[str, ...], policy: ResolvedPolicy | None, doc: Mapping[str, Any], risk_authority: object | None = None) -> ScenarioDecision: return self._decision(ScenarioAction.NO_TRADE, reasons, (), None, policy, doc, risk_authority)
    def _decision(self, action: ScenarioAction, reasons: tuple[str, ...], scenarios: tuple[Scenario, ...], ev: Decimal | None, policy: ResolvedPolicy | None, doc: Mapping[str, Any], risk_authority: object | None = None) -> ScenarioDecision:
        version, policy_hash, marker = (None, None, None) if policy is None else (policy.current_policy_version, policy.current_policy_hash, policy.policy_authority_marker_hash)
        risk = _mapping(risk_authority)
        risk_version = str(risk.get("version")) if risk.get("version") else None
        risk_marker = risk.get("risk_authority_marker_hash", risk.get("marker_hash"))
        risk_contract = risk.get("risk_contract_hash")
        cost_version, cost_hash = doc.get("cost_version"), doc.get("cost_hash")
        identity = {"action": action.value, "reasons": reasons, "scenarios": [{"name": item.name, "probability": item.probability, "terminal_price": item.terminal_price} for item in scenarios], "ev": ev, "policy": policy_hash, "marker": marker, "risk_version": risk_version, "risk_marker": risk_marker, "risk_contract": risk_contract, "cost": cost_hash, "input": doc.get("input_hash")}
        return ScenarioDecision(action, reasons, scenarios, ev, version, policy_hash, marker, risk_version, str(risk_marker) if risk_marker else None, str(risk_contract) if risk_contract else None, str(cost_version) if cost_version else None, str(cost_hash) if cost_hash else None, canonical_hash(identity))

def _hard_gate_reasons(doc: Mapping[str, Any], now: datetime, policy: ResolvedPolicy, *, require_cost: bool = True) -> list[str]:
    reasons: list[str] = []
    # Signed normalization forbids imputing missing point-in-time inputs.
    # An observed Decimal zero is valid; absent, untyped or nonfinite is not.
    for field, reason in (
        ("market_score", "MISSING_MARKET_DIRECTION_INPUT"),
        ("volatility_score", "MISSING_VOLATILITY_STATE_INPUT"),
    ):
        if _dec(doc.get(field)) is None:
            reasons.append(reason)
    evidence = _mapping(doc.get("hard_evidence", {}))
    for name in ("MARKET", "VOLATILITY", "LIQUIDITY"):
        row = _mapping(evidence.get(name, {}))
        if not bool(row.get("eligible")): reasons.append(f"MISSING_OR_INELIGIBLE_{name}_EVIDENCE")
        if not _hash_maybe(row.get("hash", row.get("evidence_hash"))): reasons.append(f"UNBOUND_{name}_EVIDENCE")
    if bool(doc.get("conflicted", False)) or bool(doc.get("stale", False)): reasons.append("STALE_OR_CONFLICTED_INPUT")
    if _dec(doc.get("max_loss")) is None or (_dec(doc.get("max_loss")) or ZERO) <= ZERO: reasons.append("UNKNOWN_MAX_LOSS")
    if _dec(doc.get("spot")) is None or (_dec(doc.get("spot")) or ZERO) <= ZERO or _dec(doc.get("atm_iv")) is None or (_dec(doc.get("atm_iv")) or ZERO) <= ZERO: reasons.append("MISSING_VOLATILITY_INPUT")
    try: dte = int(doc.get("dte"));
    except (TypeError, ValueError): dte = -1
    if dte < 7: reasons.append("DTE_OUT_OF_POLICY")
    if require_cost and (not _hash_maybe(doc.get("cost_hash")) or not doc.get("cost_version")): reasons.append("UNBOUND_EXECUTION_COST")
    required_cost = _mapping(policy.payload).get("hard_no_trade_thresholds", {})
    expected = _mapping(_mapping(required_cost).get("cost_and_expectancy", {})).get("execution_cost_contract_hash")
    if require_cost and expected != doc.get("cost_hash"): reasons.append("COST_CONTRACT_MISMATCH")
    return reasons

def _probabilities(spot: Decimal, move: Decimal, direction: Decimal, volatility: Decimal) -> tuple[Scenario, ...]:
    direction, volatility = max(Decimal("-1"), min(ONE, direction)), max(Decimal("-1"), min(ONE, volatility))
    logits = (("STRONG_DOWN", -Decimal("1.25")*direction+Decimal(".60")*volatility, Decimal("-1.25")), ("DOWN", -Decimal(".65")*direction+Decimal(".15")*volatility, Decimal("-.60")), ("RANGE", Decimal(".75")*(ONE-abs(direction))-Decimal(".75")*volatility, ZERO), ("UP", Decimal(".65")*direction+Decimal(".15")*volatility, Decimal(".60")), ("STRONG_UP", Decimal("1.25")*direction+Decimal(".60")*volatility, Decimal("1.25")))
    with localcontext() as ctx:
        ctx.prec = 50
        exps = [((value / Decimal("1.25")).exp()) for _, value, _ in logits]; total = sum(exps, ZERO)
        values = [(Decimal(".80") * item / total + Decimal(".04")).quantize(QUANTUM, rounding=ROUND_HALF_EVEN) for item in exps]
    values[2] += ONE - sum(values, ZERO)
    return tuple(Scenario(name, probability, max(ZERO, spot * (ONE + multiplier * move))) for (name, _, multiplier), probability in zip(logits, values))

def _mapping(value: object) -> Mapping[str, Any]:
    if isinstance(value, Mapping): return value
    if is_dataclass(value): return {field.name: getattr(value, field.name) for field in fields(value)}
    return vars(value) if hasattr(value, "__dict__") else {}

def _authority_disagreements(doc: Mapping[str, Any], policy: ResolvedPolicy, risk_authority: object | None) -> list[str]:
    reasons: list[str] = []
    expected_policy = {
        "current_policy_version": policy.current_policy_version,
        "current_policy_hash": policy.current_policy_hash,
        "policy_authority_marker_hash": policy.policy_authority_marker_hash,
    }
    for key, expected in expected_policy.items():
        if key in doc and doc.get(key) != expected:
            reasons.append("POLICY_RESOLUTION_DISAGREEMENT")
            break
    if risk_authority is not None:
        risk = _mapping(risk_authority)
        expected_risk = {
            "risk_authority_version": risk.get("version"),
            "risk_authority_marker_hash": risk.get("risk_authority_marker_hash", risk.get("marker_hash")),
            "risk_contract_hash": risk.get("risk_contract_hash"),
        }
        for key, expected in expected_risk.items():
            if key in doc and doc.get(key) != expected:
                reasons.append("RISK_AUTHORITY_RESOLUTION_DISAGREEMENT")
                break
    return reasons
def _dec(value: object) -> Decimal | None: return value if isinstance(value, Decimal) and value.is_finite() else None
def _hash(value: object, name: str) -> str:
    if not _hash_maybe(value): raise ValueError(f"invalid {name}")
    return str(value)
def _hash_maybe(value: object) -> bool: return isinstance(value, str) and len(value) == 64 and all(c in "0123456789abcdef" for c in value)

__all__ = ["InitialPolicyResolver", "PolicyResolver", "ResolvedPolicy", "Scenario", "ScenarioAction", "ScenarioDecision", "ScenarioEngine"]
