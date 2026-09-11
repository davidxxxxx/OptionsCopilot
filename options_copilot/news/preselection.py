"""Fail-closed evaluation for display-only conditional option preselections."""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Iterable, Sequence

from options_copilot.analytics.scenarios import (
    INITIAL_POLICY_HASH,
    INITIAL_POLICY_VERSION,
)
from options_copilot.execution_cost import (
    EXECUTION_COST_HASH,
    EXECUTION_COST_VERSION,
)
from options_copilot.storage.canonical import canonical_hash

from .models import (
    ConditionalOptionLeg,
    ConditionalOptionPreselection,
    OptionLegSide,
    PreselectionPhase,
)
from .open_reprice_economics import (
    OpenRepriceEconomics,
    TrustedTerminalScenario,
    TrustedTerminalScenarioSet,
    strategy_nav_post_hash,
)


MAXIMUM_QUOTE_AGE = timedelta(seconds=5)
PRE_MARKET_LIMIT = 10
OPEN_REPRICED_LIMIT = 10
ACTION_POOL_LIMIT = 3
NORMAL_RISK_FRACTION = Decimal("0.10")
_DIGEST_CHARS = frozenset("0123456789abcdef")
_OPEN_ECONOMICS_REQUIRED_FIELDS = (
    "scenario_asof",
    "scenario_hash",
    "execution_cost_contract_version",
    "execution_cost_contract_hash",
    "risk_policy_version",
    "risk_policy_hash",
    "broker_snapshot_hash",
    "strategy_nav_usd",
    "strategy_nav_post_hash",
    "economics_quote_batch_id",
    "economics_quote_asof",
    "payoff_hash",
    "economics_calculation_hash",
    "debit_usd",
    "credit_usd",
    "net_entry_cost_usd",
    "estimated_commission_usd",
    "estimated_entry_slippage_usd",
    "estimated_exit_slippage_usd",
    "estimated_slippage_usd",
    "expected_value_before_costs_usd",
    "risk_fraction",
)


@dataclass(frozen=True, slots=True)
class EvaluatedPreselection:
    candidate: ConditionalOptionPreselection
    blockers: tuple[str, ...]
    quote_batch_id: str | None
    oldest_quote_asof: datetime | None
    maximum_quote_age_seconds: Decimal | None
    risk_adjusted_ev: Decimal | None

    @property
    def action_pool_eligible(self) -> bool:
        return not self.blockers

    def as_dict(
        self,
        *,
        research_rank: int | None = None,
        repriced_rank: int | None = None,
        action_rank: int | None = None,
    ) -> dict[str, object]:
        payload = self.candidate.as_dict()
        payload.update(
            {
                "quote_batch_id": self.quote_batch_id,
                "oldest_quote_asof": (
                    None
                    if self.oldest_quote_asof is None
                    else self.oldest_quote_asof.isoformat()
                ),
                "maximum_quote_age_seconds": (
                    None
                    if self.maximum_quote_age_seconds is None
                    else float(self.maximum_quote_age_seconds)
                ),
                "risk_adjusted_ev": (
                    None if self.risk_adjusted_ev is None else str(self.risk_adjusted_ev)
                ),
                "blockers": list(self.blockers),
                "research_only": not self.action_pool_eligible,
                "action_pool_eligible": self.action_pool_eligible,
                "research_rank": research_rank,
                "repriced_rank": repriced_rank,
                "action_rank": action_rank,
                # These constants intentionally duplicate the frozen model
                # fields at the final projection boundary.
                "decision_authority": "SUPPORTING_ONLY",
                "approval_eligible": False,
                "instruction_creation_allowed": False,
            }
        )
        return payload


@dataclass(frozen=True, slots=True)
class EvaluatedPreselectionBatch:
    """One fail-closed decision over the complete market-open candidate set."""

    evaluations: tuple[EvaluatedPreselection, ...]
    blockers: tuple[str, ...]
    quote_batch_id: str | None
    quote_asof: datetime | None
    expected_preselection_ids: tuple[str, ...]

    @property
    def action_pool_eligible(self) -> bool:
        return not self.blockers

    @property
    def outcome(self) -> str:
        return "SUPPORTING_ONLY" if self.action_pool_eligible else "NO_TRADE"

    @property
    def action_pool(self) -> tuple[EvaluatedPreselection, ...]:
        if not self.action_pool_eligible:
            return ()
        return tuple(sorted(self.evaluations, key=_rank_key))[:ACTION_POOL_LIMIT]

    def as_dict(self) -> dict[str, object]:
        return {
            "outcome": self.outcome,
            "blockers": list(self.blockers),
            "quote_batch_id": self.quote_batch_id,
            "quote_asof": None if self.quote_asof is None else self.quote_asof.isoformat(),
            "expected_preselection_ids": list(self.expected_preselection_ids),
            "candidate_count": len(self.evaluations),
            "action_pool_eligible": self.action_pool_eligible,
            "action_pool": [item.as_dict() for item in self.action_pool],
            "decision_authority": "SUPPORTING_ONLY",
            "approval_eligible": False,
            "instruction_creation_allowed": False,
            "order_allowed": False,
        }


def evaluate_preselection(
    candidate: ConditionalOptionPreselection,
    *,
    now: datetime,
) -> EvaluatedPreselection:
    """Evaluate hard-data completeness without creating execution authority."""

    checked_now = _aware(now)
    blockers: list[str] = []
    if candidate.phase is PreselectionPhase.PRE_MARKET:
        blockers.append("PREMARKET_RESEARCH_ONLY")
    if not candidate.legs:
        blockers.append("OPTION_LEGS_MISSING")

    quote_times: list[datetime] = []
    quote_batches: set[str] = set()
    con_ids: set[int] = set()
    for index, leg in enumerate(candidate.legs, start=1):
        if leg.underlying != candidate.underlying:
            blockers.append(f"LEG_UNDERLYING_MISMATCH:{index}")
        blockers.extend(
            f"LEG_EXECUTION_FIELD_MISSING:{index}:{name}"
            for name in leg.missing_execution_fields
        )
        if leg.con_id is None:
            blockers.append(f"CON_ID_MISSING:{index}")
        elif leg.con_id in con_ids:
            blockers.append(f"CON_ID_DUPLICATE:{leg.con_id}")
        else:
            con_ids.add(leg.con_id)
        if leg.bid is not None and leg.ask is not None and leg.bid > leg.ask:
            blockers.append(f"QUOTE_CROSSED:{index}")
        if leg.quote_asof is not None:
            quote_time = _aware(leg.quote_asof)
            quote_times.append(quote_time)
            if quote_time > checked_now:
                blockers.append("QUOTE_FROM_FUTURE")
            elif checked_now - quote_time > MAXIMUM_QUOTE_AGE:
                blockers.append("QUOTE_STALE")
        if leg.quote_batch_id is not None:
            quote_batches.add(leg.quote_batch_id)
        if leg.dte is None:
            blockers.append(f"LEG_EXECUTION_FIELD_MISSING:{index}:dte")
        elif leg.dte < 7:
            blockers.append("DTE_BELOW_PERMANENT_FLOOR")

    if len(quote_batches) > 1:
        blockers.append("QUOTE_BATCH_MISMATCH")
    if len(set(quote_times)) > 1:
        blockers.append("QUOTE_ASOF_MISMATCH")
    if not candidate.risk_defined or _contains_naked_or_unbounded_short(candidate.legs):
        blockers.append("UNLIMITED_OR_NAKED_RISK")
    if candidate.maximum_loss_usd is None:
        blockers.append("MAX_LOSS_UNKNOWN")
    elif candidate.maximum_loss_usd <= 0:
        blockers.append("MAX_LOSS_INVALID")
    if candidate.estimated_cost_usd is None:
        blockers.append("ESTIMATED_COST_UNKNOWN")
    if candidate.cost_after_ev_usd is None:
        blockers.append("COST_AFTER_EV_UNKNOWN")
    elif candidate.cost_after_ev_usd <= 0:
        blockers.append("COST_AFTER_EV_NOT_POSITIVE")
    if candidate.phase is PreselectionPhase.OPEN_REPRICED:
        blockers.extend(
            _open_economics_lineage_blockers(
                candidate,
                quote_batches=quote_batches,
                quote_times=set(quote_times),
            )
        )
    for name in (
        "entry_condition",
        "invalidation_condition",
        "profit_target_condition",
        "stop_loss_condition",
    ):
        if getattr(candidate, name) is None:
            blockers.append(f"CONDITION_MISSING:{name}")
    if not candidate.evidence_ids:
        blockers.append("EVIDENCE_IDS_MISSING")
    if not candidate.evidence_hashes:
        blockers.append("EVIDENCE_HASHES_MISSING")
    if len(candidate.evidence_ids) != len(candidate.evidence_hashes):
        blockers.append("EVIDENCE_BINDING_MISMATCH")
    if candidate.strategy_hash != strategy_structure_hash(
        candidate.underlying,
        candidate.strategy_type,
        candidate.legs,
    ):
        blockers.append("STRATEGY_HASH_MISMATCH")

    oldest_quote = min(quote_times) if quote_times else None
    maximum_age = (
        None
        if not quote_times or any(item > checked_now for item in quote_times)
        else Decimal(str(max((checked_now - item).total_seconds() for item in quote_times)))
    )
    risk_adjusted_ev = (
        None
        if candidate.maximum_loss_usd is None
        or candidate.maximum_loss_usd <= 0
        or candidate.cost_after_ev_usd is None
        else (candidate.cost_after_ev_usd / candidate.maximum_loss_usd).quantize(
            Decimal("0.00000001")
        )
    )
    return EvaluatedPreselection(
        candidate=candidate,
        blockers=tuple(dict.fromkeys(blockers)),
        quote_batch_id=next(iter(quote_batches)) if len(quote_batches) == 1 else None,
        oldest_quote_asof=oldest_quote,
        maximum_quote_age_seconds=maximum_age,
        risk_adjusted_ev=risk_adjusted_ev,
    )


def evaluate_preselection_batch(
    candidates: Sequence[ConditionalOptionPreselection] | Iterable[ConditionalOptionPreselection],
    *,
    now: datetime,
    expected_preselection_ids: Iterable[str] | None = None,
    blockers: Iterable[str] = (),
) -> EvaluatedPreselectionBatch:
    """Evaluate the complete open batch; one bad member blocks every member."""

    checked_now = _aware(now)
    candidate_rows = tuple(candidates)
    batch_blockers = [str(item).strip() for item in blockers if str(item).strip()]
    expected = (
        tuple(str(item).strip() for item in expected_preselection_ids)
        if expected_preselection_ids is not None
        else tuple(item.preselection_id for item in candidate_rows)
    )
    identifiers = [item.preselection_id for item in candidate_rows]
    if len(candidate_rows) > OPEN_REPRICED_LIMIT:
        batch_blockers.append("BATCH_LIMIT_EXCEEDED")
    if len(set(identifiers)) != len(identifiers):
        batch_blockers.append("CANDIDATE_ID_DUPLICATE")
    if len(set(expected)) != len(expected):
        batch_blockers.append("PARENT_ID_DUPLICATE")
    missing = sorted(set(expected) - set(identifiers))
    unexpected = sorted(set(identifiers) - set(expected))
    if missing:
        batch_blockers.append(f"PARENT_SET_INCOMPLETE:{','.join(missing)}")
    if unexpected:
        batch_blockers.append(f"PARENT_SET_UNEXPECTED:{','.join(unexpected)}")
    if len(identifiers) != len(expected):
        batch_blockers.append("PARENT_SET_COUNT_MISMATCH")

    evaluations: list[EvaluatedPreselection] = []
    quote_batches: set[str] = set()
    quote_times: set[datetime] = set()
    for candidate in candidate_rows:
        if not isinstance(candidate, ConditionalOptionPreselection):
            raise TypeError("candidates must contain ConditionalOptionPreselection values")
        if candidate.phase is not PreselectionPhase.OPEN_REPRICED:
            batch_blockers.append(f"CANDIDATE_PHASE_INVALID:{candidate.preselection_id}")
        evaluated = evaluate_preselection(candidate, now=checked_now)
        evaluations.append(evaluated)
        batch_blockers.extend(
            f"CANDIDATE:{candidate.preselection_id}:{blocker}"
            for blocker in evaluated.blockers
        )
        if evaluated.quote_batch_id is not None:
            quote_batches.add(evaluated.quote_batch_id)
        quote_times.update(
            leg.quote_asof for leg in candidate.legs if leg.quote_asof is not None
        )

    if len(quote_batches) != 1:
        batch_blockers.append("BATCH_QUOTE_BATCH_NOT_UNIFIED")
    if len(quote_times) != 1:
        batch_blockers.append("BATCH_QUOTE_ASOF_NOT_UNIFIED")
    return EvaluatedPreselectionBatch(
        evaluations=tuple(evaluations),
        blockers=tuple(dict.fromkeys(batch_blockers)),
        quote_batch_id=next(iter(quote_batches)) if len(quote_batches) == 1 else None,
        quote_asof=next(iter(quote_times)) if len(quote_times) == 1 else None,
        expected_preselection_ids=expected,
    )


def _open_economics_lineage_blockers(
    candidate: ConditionalOptionPreselection,
    *,
    quote_batches: set[str],
    quote_times: set[datetime],
) -> tuple[str, ...]:
    """Revalidate durable 09:35 economics without trusting positive claims.

    The immutable ledger may continue to display legacy rows, but an open row
    cannot enter the display action pool unless every economics input and hash
    still binds to the same strategy, quote batch, scenario set, cost policy,
    risk policy, and NAV snapshot.
    """

    if not candidate.terminal_scenarios or any(
        getattr(candidate, name, None) is None
        for name in _OPEN_ECONOMICS_REQUIRED_FIELDS
    ):
        return ("OPEN_REPRICE_ECONOMICS_LINEAGE_MISSING",)

    if not _is_digest(candidate.broker_snapshot_hash):
        return ("ECONOMICS_SNAPSHOT_HASH_MISMATCH",)
    if len(quote_batches) != 1 or (
        candidate.economics_quote_batch_id != next(iter(quote_batches), None)
    ):
        return ("ECONOMICS_QUOTE_BATCH_MISMATCH",)
    if len(quote_times) != 1 or (
        candidate.economics_quote_asof != next(iter(quote_times), None)
    ):
        return ("ECONOMICS_QUOTE_ASOF_MISMATCH",)
    if (
        candidate.execution_cost_contract_version != EXECUTION_COST_VERSION
        or candidate.execution_cost_contract_hash != EXECUTION_COST_HASH
    ):
        return ("EXECUTION_COST_CONTRACT_HASH_MISMATCH",)
    if (
        candidate.risk_policy_version != INITIAL_POLICY_VERSION
        or candidate.risk_policy_hash != INITIAL_POLICY_HASH
    ):
        return ("RISK_POLICY_CONTRACT_HASH_MISMATCH",)

    try:
        scenario_set = TrustedTerminalScenarioSet.create(
            candidate_id=candidate.preselection_id,
            strategy_hash=candidate.strategy_hash,
            scenario_asof=candidate.scenario_asof,  # type: ignore[arg-type]
            scenarios=tuple(
                TrustedTerminalScenario(
                    item.terminal_underlying_price,
                    item.probability,
                )
                for item in candidate.terminal_scenarios
            ),
            current_policy_version=candidate.risk_policy_version,  # type: ignore[arg-type]
            current_policy_hash=candidate.risk_policy_hash,  # type: ignore[arg-type]
        )
    except (TypeError, ValueError):
        return ("TERMINAL_SCENARIOS_INVALID",)
    if scenario_set.scenario_hash != candidate.scenario_hash:
        return ("TERMINAL_SCENARIO_HASH_MISMATCH",)
    if candidate.scenario_asof > candidate.economics_quote_asof:  # type: ignore[operator]
        return ("TERMINAL_SCENARIO_FROM_FUTURE",)

    try:
        expected_nav_hash = strategy_nav_post_hash(
            candidate_id=candidate.preselection_id,
            strategy_hash=candidate.strategy_hash,
            snapshot_hash=candidate.broker_snapshot_hash,  # type: ignore[arg-type]
            strategy_nav_usd=candidate.strategy_nav_usd,  # type: ignore[arg-type]
        )
    except (TypeError, ValueError):
        return ("STRATEGY_NAV_LINEAGE_INVALID",)
    if candidate.strategy_nav_post_hash != expected_nav_hash:
        return ("STRATEGY_NAV_LINEAGE_INVALID",)

    economics_values = (
        candidate.maximum_loss_usd,
        candidate.estimated_cost_usd,
        candidate.cost_after_ev_usd,
        candidate.strategy_nav_usd,
        candidate.debit_usd,
        candidate.credit_usd,
        candidate.net_entry_cost_usd,
        candidate.estimated_commission_usd,
        candidate.estimated_entry_slippage_usd,
        candidate.estimated_exit_slippage_usd,
        candidate.estimated_slippage_usd,
        candidate.expected_value_before_costs_usd,
        candidate.risk_fraction,
    )
    if any(
        not isinstance(value, Decimal) or not value.is_finite()
        for value in economics_values
    ):
        return ("OPEN_REPRICE_ECONOMICS_VALUE_INVALID",)
    assert all(isinstance(value, Decimal) for value in economics_values)
    if (
        candidate.debit_usd < 0  # type: ignore[operator]
        or candidate.credit_usd < 0  # type: ignore[operator]
        or candidate.estimated_commission_usd < 0  # type: ignore[operator]
        or candidate.estimated_entry_slippage_usd < 0  # type: ignore[operator]
        or candidate.estimated_exit_slippage_usd < 0  # type: ignore[operator]
        or candidate.estimated_slippage_usd < 0  # type: ignore[operator]
    ):
        return ("OPEN_REPRICE_ECONOMICS_VALUE_INVALID",)
    if candidate.net_entry_cost_usd < 0:  # type: ignore[operator]
        return ("NET_CREDIT_DISPLAY_CONTRACT_UNSUPPORTED",)
    if candidate.estimated_slippage_usd != (
        candidate.estimated_entry_slippage_usd
        + candidate.estimated_exit_slippage_usd  # type: ignore[operator]
    ):
        return ("OPEN_REPRICE_ECONOMICS_VALUE_MISMATCH",)
    if candidate.net_entry_cost_usd != (
        candidate.debit_usd
        - candidate.credit_usd  # type: ignore[operator]
        + candidate.estimated_commission_usd  # type: ignore[operator]
        + candidate.estimated_slippage_usd  # type: ignore[operator]
    ) or candidate.estimated_cost_usd != candidate.net_entry_cost_usd:
        return ("OPEN_REPRICE_ECONOMICS_VALUE_MISMATCH",)
    if candidate.expected_value_before_costs_usd != (
        candidate.cost_after_ev_usd
        + candidate.estimated_commission_usd  # type: ignore[operator]
        + candidate.estimated_slippage_usd  # type: ignore[operator]
    ):
        return ("OPEN_REPRICE_ECONOMICS_VALUE_MISMATCH",)
    expected_risk_fraction = (
        candidate.maximum_loss_usd / candidate.strategy_nav_usd  # type: ignore[operator]
    )
    if candidate.risk_fraction != expected_risk_fraction:
        return ("OPEN_REPRICE_ECONOMICS_VALUE_MISMATCH",)
    if expected_risk_fraction > NORMAL_RISK_FRACTION:
        return ("NORMAL_RISK_LIMIT_EXCEEDED",)

    if not _is_digest(candidate.payoff_hash) or not _is_digest(
        candidate.economics_calculation_hash
    ):
        return ("OPEN_REPRICE_ECONOMICS_LINEAGE_INVALID",)
    try:
        economics = OpenRepriceEconomics(
            candidate_id=candidate.preselection_id,
            strategy_hash=candidate.strategy_hash,
            broker_snapshot_hash=candidate.broker_snapshot_hash,  # type: ignore[arg-type]
            quote_batch_id=candidate.economics_quote_batch_id,  # type: ignore[arg-type]
            quote_asof=candidate.economics_quote_asof,  # type: ignore[arg-type]
            scenario_hash=candidate.scenario_hash,  # type: ignore[arg-type]
            scenario_asof=candidate.scenario_asof,  # type: ignore[arg-type]
            cost_contract_version=candidate.execution_cost_contract_version,  # type: ignore[arg-type]
            cost_contract_hash=candidate.execution_cost_contract_hash,  # type: ignore[arg-type]
            policy_version=candidate.risk_policy_version,  # type: ignore[arg-type]
            policy_hash=candidate.risk_policy_hash,  # type: ignore[arg-type]
            strategy_nav_usd=candidate.strategy_nav_usd,  # type: ignore[arg-type]
            strategy_nav_post_hash=candidate.strategy_nav_post_hash,  # type: ignore[arg-type]
            debit_usd=candidate.debit_usd,  # type: ignore[arg-type]
            credit_usd=candidate.credit_usd,  # type: ignore[arg-type]
            commission_usd=candidate.estimated_commission_usd,  # type: ignore[arg-type]
            entry_slippage_usd=candidate.estimated_entry_slippage_usd,  # type: ignore[arg-type]
            exit_slippage_usd=candidate.estimated_exit_slippage_usd,  # type: ignore[arg-type]
            total_slippage_usd=candidate.estimated_slippage_usd,  # type: ignore[arg-type]
            all_in_cost_usd=candidate.net_entry_cost_usd,  # type: ignore[arg-type]
            maximum_loss_usd=candidate.maximum_loss_usd,  # type: ignore[arg-type]
            before_cost_expected_value_usd=(
                candidate.expected_value_before_costs_usd  # type: ignore[arg-type]
            ),
            after_cost_expected_value_usd=candidate.cost_after_ev_usd,  # type: ignore[arg-type]
            payoff_hash=candidate.payoff_hash,  # type: ignore[arg-type]
            risk_fraction=candidate.risk_fraction,  # type: ignore[arg-type]
            economics_hash=candidate.economics_calculation_hash,  # type: ignore[arg-type]
        )
    except (TypeError, ValueError):
        return ("OPEN_REPRICE_ECONOMICS_LINEAGE_INVALID",)
    if not economics.verify_hash():
        return ("OPEN_REPRICE_ECONOMICS_HASH_MISMATCH",)
    return ()


def _is_digest(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in _DIGEST_CHARS for character in value)
    )


def build_preselection_pools(
    candidates: Iterable[ConditionalOptionPreselection],
    *,
    now: datetime,
) -> tuple[
    tuple[EvaluatedPreselection, ...],
    tuple[EvaluatedPreselection, ...],
    tuple[EvaluatedPreselection, ...],
]:
    """Return capped pre-market, open-repriced, and safe display action pools."""

    evaluated = tuple(evaluate_preselection(item, now=now) for item in candidates)
    pre_market = tuple(
        sorted(
            (
                item
                for item in evaluated
                if item.candidate.phase is PreselectionPhase.PRE_MARKET
            ),
            key=_rank_key,
        )[:PRE_MARKET_LIMIT]
    )
    repriced = tuple(
        sorted(
            (
                item
                for item in evaluated
                if item.candidate.phase is PreselectionPhase.OPEN_REPRICED
            ),
            key=_rank_key,
        )[:OPEN_REPRICED_LIMIT]
    )
    open_batch = tuple(
        item.candidate
        for item in evaluated
        if item.candidate.phase is PreselectionPhase.OPEN_REPRICED
    )
    batch_evaluation = evaluate_preselection_batch(open_batch, now=now)
    action_pool = batch_evaluation.action_pool
    return pre_market, repriced, action_pool


def strategy_structure_hash(
    underlying: str,
    strategy_type: str,
    legs: Iterable[ConditionalOptionLeg],
) -> str:
    """Hash immutable contract identities and ratios, excluding live prices."""

    return canonical_hash(
        {
            "schema": "options_copilot.conditional_option_strategy.v2",
            "underlying": str(underlying).strip().upper(),
            "strategy_type": str(strategy_type).strip().upper(),
            "legs": [
                {
                    "con_id": leg.con_id,
                    "local_symbol": leg.local_symbol,
                    "trading_class": leg.trading_class,
                    "multiplier": leg.multiplier,
                    "exchange": leg.exchange,
                    "expiry": leg.expiry,
                    "strike": leg.strike,
                    "right": leg.right,
                    "side": leg.side,
                    "ratio": leg.ratio,
                    "quantity": leg.quantity,
                }
                for leg in legs
            ],
        }
    )


def _contains_naked_or_unbounded_short(
    legs: tuple[ConditionalOptionLeg, ...],
) -> bool:
    """Require every short option unit to be covered in the same expiry/right bucket."""

    exposure: dict[tuple[object, object], dict[OptionLegSide, int]] = defaultdict(
        lambda: {OptionLegSide.BUY: 0, OptionLegSide.SELL: 0}
    )
    for leg in legs:
        if (
            leg.expiry is None
            or leg.right is None
            or leg.side is None
            or leg.ratio is None
            or leg.quantity is None
        ):
            return True
        exposure[(leg.expiry, leg.right)][leg.side] += leg.ratio * leg.quantity
    return any(
        totals[OptionLegSide.SELL] > totals[OptionLegSide.BUY]
        for totals in exposure.values()
    )


def _rank_key(item: EvaluatedPreselection) -> tuple[Decimal, Decimal, str]:
    risk_adjusted = item.risk_adjusted_ev or Decimal("-Infinity")
    absolute_ev = item.candidate.cost_after_ev_usd or Decimal("-Infinity")
    return (-risk_adjusted, -absolute_ev, item.candidate.preselection_id)


def _aware(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("preselection clock must be timezone-aware")
    return value


__all__ = [
    "ACTION_POOL_LIMIT",
    "EvaluatedPreselectionBatch",
    "EvaluatedPreselection",
    "MAXIMUM_QUOTE_AGE",
    "OPEN_REPRICED_LIMIT",
    "PRE_MARKET_LIMIT",
    "build_preselection_pools",
    "evaluate_preselection",
    "evaluate_preselection_batch",
    "strategy_structure_hash",
]
