"""Signed, deterministic execution-cost and expiry-EV resolution.

The resolver is deliberately a pure analytical boundary.  It reads one
checked-in, human-signed governance artifact and combines it only with frozen
candidate bodies and scenario results supplied by the decision pipeline.  It
has no broker, network, model, environment-variable, approval, instruction, or
order-placement dependency.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, fields, is_dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_CEILING
from enum import Enum
from pathlib import Path
from typing import Any

from options_copilot.domain import (
    OptionContract,
    OptionLeg,
    OptionLegQuote,
    OptionRight,
    PositionSide,
    StrategyCandidate,
)
from options_copilot.governance.contracts import (
    ContractKind,
    SignedContract,
    load_contract,
)
from options_copilot.risk import PayoffStatus, analyze_expiration_payoff
from options_copilot.storage.canonical import canonical_hash, utc_datetime


EXECUTION_COST_VERSION = "v1"
EXECUTION_COST_HASH = (
    "d5922fa9808c4f1e7150362026037d17fb550d6f64daf8f86985042053e5428b"
)
EXECUTION_COST_SIGNER = "human:xujie"
EXECUTION_COST_EFFECTIVE_AT = "2026-08-03T16:05:00.316359+00:00"

ZERO = Decimal("0")
ONE = Decimal("1")
CENT = Decimal("0.01")
FALLBACK_PER_CONTRACT_SIDE = Decimal("1.25")
MINIMUM_PER_ORDER = Decimal("1.00")
ENTRY_SPREAD_FACTOR = Decimal("0.25")
EXIT_SPREAD_FACTOR = Decimal("0.50")
MINIMUM_ENTRY_SLIPPAGE = Decimal("0.01")
MINIMUM_EXIT_SLIPPAGE = Decimal("0.02")
MAXIMUM_QUOTE_AGE_SECONDS = Decimal("5")
STANDARD_OPTION_MULTIPLIER = Decimal("100")


class ExecutionCostResolutionError(ValueError):
    """A signed cost or candidate input could not be resolved safely."""


class ExecutionCostCurrentnessError(RuntimeError):
    """A prior execution-cost resolution cannot be proven current."""


@dataclass(frozen=True, slots=True)
class CandidateCostResolution:
    """One candidate's independently recomputed cost and scenario EV."""

    candidate_id: str
    cost_version: str
    cost_hash: str
    quote_batch_id: str
    commission_usd: Decimal
    slippage_usd: Decimal
    execution_cost_usd: Decimal
    expected_value_before_costs_usd: Decimal
    after_cost_expected_value: Decimal
    scenario_count: int
    calculation_hash: str
    stress_execution_cost_usd: Decimal | None = None
    stress_after_cost_expected_value: Decimal | None = None

    @property
    def execution_cost_contract_version(self) -> str:
        return self.cost_version

    @property
    def execution_cost_contract_hash(self) -> str:
        return self.cost_hash


@dataclass(frozen=True, slots=True)
class ExecutionCostResolution:
    """The current signed cost identity plus optional per-candidate results."""

    cost_version: str
    cost_hash: str
    contract_effective_at: datetime
    contract_signed_at: datetime
    contract_marker_hash: str
    resolved_at: datetime
    scan_run_id: str | None
    candidates: tuple[CandidateCostResolution, ...]
    resolution_hash: str

    @property
    def version(self) -> str:
        return self.cost_version

    @property
    def contract_hash(self) -> str:
        return self.cost_hash

    @property
    def execution_cost_contract_version(self) -> str:
        return self.cost_version

    @property
    def execution_cost_contract_hash(self) -> str:
        return self.cost_hash


@dataclass(frozen=True, slots=True)
class _ParsedCandidate:
    candidate_id: str
    quote_batch_id: str
    candidate: StrategyCandidate
    commission_usd: Decimal
    slippage_usd: Decimal


class SignedExecutionCostResolver:
    """Resolve the sole checked-in execution-cost authority.

    ``contract_path`` is a composition-time dependency intended for controlled
    packaging and tests.  Calls to :meth:`resolve` never inspect candidate,
    context, environment, or policy fields for an alternate path.
    """

    def __init__(
        self,
        contract_path: str | Path | None = None,
        *,
        clock: Callable[[], datetime] | None = None,
        authority_read_lease: object | None = None,
        allow_test_authority_lease: bool = False,
    ) -> None:
        self.contract_path = (
            Path(contract_path)
            if contract_path is not None
            else Path(__file__).with_name("governance")
            / "execution_cost_contract.v1.json"
        )
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._authority_read_lease = _test_authority_read_lease(
            authority_read_lease,
            allow_test_authority_lease=allow_test_authority_lease,
        )

    def resolve(
        self,
        *,
        now: datetime,
        scan_run_id: str | None = None,
        candidates: Sequence[object] = (),
        scenarios: Sequence[object] = (),
        context: object | None = None,
        current_policy: object | None = None,
        resolved_policy: object | None = None,
    ) -> ExecutionCostResolution:
        """Recompute costs and EV, or raise before returning partial output.

        With no candidates this is an identity-only resolution used by the
        approval currentness boundary.  ``context`` is intentionally unused;
        it cannot override the contract or any frozen candidate input.
        """

        del context
        checked_at = utc_datetime(now, field="now")
        contract = self._load_expected(as_of=checked_at)
        self._validate_policy_bindings(
            current_policy=current_policy,
            resolved_policy=resolved_policy,
            expected_hash=contract.contract_hash,
        )
        candidate_rows = _sequence(candidates, "candidates")
        scenario_rows = _sequence(scenarios, "scenarios")
        if len(candidate_rows) != len(scenario_rows):
            raise ExecutionCostResolutionError(
                "SCENARIO_CANDIDATE_COUNT_MISMATCH"
            )

        normalized_scan_run_id = _optional_nonblank(scan_run_id, "scan_run_id")
        if candidate_rows and normalized_scan_run_id is None:
            raise ExecutionCostResolutionError("SCAN_RUN_ID_REQUIRED")

        resolved_candidates: list[CandidateCostResolution] = []
        seen_ids: set[str] = set()
        for raw_candidate, raw_scenario in zip(candidate_rows, scenario_rows):
            parsed = _parse_candidate(
                raw_candidate,
                now=checked_at,
                cost_version=contract.version,
                cost_hash=contract.contract_hash,
            )
            if parsed.candidate_id in seen_ids:
                raise ExecutionCostResolutionError("DUPLICATE_CANDIDATE_ID")
            seen_ids.add(parsed.candidate_id)
            terms = _parse_scenarios(
                raw_scenario,
                candidate_id=parsed.candidate_id,
                cost_version=contract.version,
                cost_hash=contract.contract_hash,
            )
            resolved_candidates.append(
                _resolve_candidate(
                    parsed,
                    terms=terms,
                    scan_run_id=normalized_scan_run_id,
                    cost_version=contract.version,
                    cost_hash=contract.contract_hash,
                )
            )

        marker_hash = canonical_hash(contract.to_dict())
        candidate_documents = [
            _candidate_resolution_document(item) for item in resolved_candidates
        ]
        resolution_body = {
            "schema": "options_copilot.execution_cost_resolution.v1",
            "cost_version": contract.version,
            "cost_hash": contract.contract_hash,
            "contract_effective_at": contract.effective_at,
            "contract_signed_at": contract.signed_at,
            "contract_marker_hash": marker_hash,
            "resolved_at": checked_at,
            "scan_run_id": normalized_scan_run_id,
            "candidates": candidate_documents,
        }
        return ExecutionCostResolution(
            cost_version=contract.version,
            cost_hash=contract.contract_hash,
            contract_effective_at=contract.effective_at,
            contract_signed_at=contract.signed_at,
            contract_marker_hash=marker_hash,
            resolved_at=checked_at,
            scan_run_id=normalized_scan_run_id,
            candidates=tuple(resolved_candidates),
            resolution_hash=canonical_hash(resolution_body),
        )

    apply = resolve
    run = resolve

    def is_current(self, resolution: ExecutionCostResolution) -> bool:
        """Return false on type errors, unreadable artifacts, or head drift."""

        try:
            self._assert_current(resolution)
        except (ExecutionCostCurrentnessError, TypeError, ValueError, OSError):
            return False
        return True

    def assert_current(
        self,
        resolution: ExecutionCostResolution,
    ) -> ExecutionCostResolution:
        """Return ``resolution`` only while its complete signed head matches."""

        self._assert_current(resolution)
        return resolution

    def guard_current(
        self,
        resolution: ExecutionCostResolution,
        *,
        callback: Callable[[], object],
    ) -> object | None:
        """Run ``callback`` only while an authority-side read lease is held.

        The checked-in JSON contract has no writer protocol capable of holding
        a cross-process read lease across an approval commit.  Consequently a
        normal production resolver deliberately returns unavailable here.  A
        controllable lease may be injected only behind the explicit TEST_ONLY
        constructor gate; this keeps unit tests able to exercise the approval
        transaction without misrepresenting an in-process lock as production
        authority.

        Lease/acquisition failures before callback entry fail closed.  Once the
        callback starts, every exception is propagated so its caller can roll
        back the restricted transaction.
        """

        if not callable(callback):
            raise TypeError("callback must be callable")
        lease = self._authority_read_lease
        if lease is None:
            return None

        callback_started = False

        def guarded() -> object | None:
            nonlocal callback_started
            if callback_started:
                raise ExecutionCostCurrentnessError(
                    "authority read lease invoked callback more than once"
                )
            if not self.is_current(resolution):
                return None
            callback_started = True
            return callback()

        try:
            result = lease.guard_read(guarded)
        except Exception:
            if callback_started:
                raise
            return None
        if not callback_started:
            return None
        return result

    def _assert_current(self, resolution: ExecutionCostResolution) -> None:
        if not isinstance(resolution, ExecutionCostResolution):
            raise ExecutionCostCurrentnessError(
                "resolution must be an ExecutionCostResolution"
            )
        try:
            checked_at = utc_datetime(self._clock(), field="clock result")
            current = load_contract(self.contract_path, as_of=checked_at)
        except Exception as exc:
            raise ExecutionCostCurrentnessError(
                "current execution cost contract is unavailable or invalid"
            ) from exc
        marker_hash = canonical_hash(current.to_dict())
        if (
            current.contract_kind is not ContractKind.EXECUTION_COST
            or current.version != resolution.cost_version
            or current.contract_hash != resolution.cost_hash
            or current.effective_at != resolution.contract_effective_at
            or current.signed_at != resolution.contract_signed_at
            or marker_hash != resolution.contract_marker_hash
        ):
            raise ExecutionCostCurrentnessError(
                "execution cost contract head changed"
            )
        try:
            _validate_contract_semantics(current)
        except ExecutionCostResolutionError as exc:
            raise ExecutionCostCurrentnessError(
                "current execution cost contract semantics are invalid"
            ) from exc

    def _load_expected(self, *, as_of: datetime) -> SignedContract:
        try:
            contract = load_contract(
                self.contract_path,
                expected_kind=ContractKind.EXECUTION_COST,
                expected_version=EXECUTION_COST_VERSION,
                expected_hash=EXECUTION_COST_HASH,
                expected_signer=EXECUTION_COST_SIGNER,
                expected_effective_at=EXECUTION_COST_EFFECTIVE_AT,
                as_of=as_of,
            )
            _validate_contract_semantics(contract)
            return contract
        except Exception as exc:
            raise ExecutionCostResolutionError(
                "SIGNED_EXECUTION_COST_CONTRACT_INVALID"
            ) from exc

    @staticmethod
    def _validate_policy_bindings(
        *,
        current_policy: object | None,
        resolved_policy: object | None,
        expected_hash: str,
    ) -> None:
        policy_hashes: list[str] = []
        for policy in (current_policy, resolved_policy):
            if policy is None:
                continue
            document = _document(policy)
            payload = _document(document.get("payload"))
            thresholds = _document(payload.get("hard_no_trade_thresholds"))
            cost_rule = _document(thresholds.get("cost_and_expectancy"))
            bound_hash = cost_rule.get("execution_cost_contract_hash")
            if bound_hash != expected_hash:
                raise ExecutionCostResolutionError(
                    "POLICY_EXECUTION_COST_BINDING_MISMATCH"
                )
            authority_hash = document.get(
                "current_policy_hash",
                document.get("policy_hash", document.get("contract_hash")),
            )
            if not _is_hash(authority_hash):
                raise ExecutionCostResolutionError("POLICY_IDENTITY_INVALID")
            policy_hashes.append(str(authority_hash))
        if len(set(policy_hashes)) > 1:
            raise ExecutionCostResolutionError("POLICY_RESOLUTION_DISAGREEMENT")


def _test_authority_read_lease(
    lease: object | None,
    *,
    allow_test_authority_lease: bool,
) -> object | None:
    """Admit only an explicitly enabled, structurally TEST_ONLY lease."""

    if lease is None:
        return None
    if (
        not allow_test_authority_lease
        or getattr(lease, "test_only", False) is not True
        or not callable(getattr(lease, "guard_read", None))
    ):
        raise ValueError(
            "execution-cost authority leases are TEST_ONLY and require "
            "allow_test_authority_lease=True, test_only=True, and guard_read"
        )
    return lease


def _validate_contract_semantics(contract: SignedContract) -> None:
    payload = contract.payload
    commission = _document(payload.get("commission_and_fees"))
    quote_policy = _document(payload.get("quote_spread_and_slippage"))
    quote_gates = _document(quote_policy.get("quote_hard_gates"))
    slippage = _document(quote_policy.get("adverse_slippage"))
    precision = _document(payload.get("precision_and_aggregation"))
    expected = (
        (payload.get("contract_id"), "execution-cost.v1"),
        (payload.get("currency"), "USD"),
        (commission.get("fallback_usd_per_contract_side"), "1.25"),
        (commission.get("minimum_usd_per_order"), "1.00"),
        (commission.get("round_trip_reserved_at_candidate_creation"), True),
        (quote_gates.get("maximum_age_seconds"), "5"),
        (quote_gates.get("locked_bid_equals_ask"), "NO_TRADE"),
        (quote_gates.get("crossed_bid_greater_than_ask"), "NO_TRADE"),
        (
            slippage.get("entry_per_option_share"),
            "max(0.01 USD, 0.25 * displayed_spread)",
        ),
        (
            slippage.get("planned_exit_per_option_share"),
            "max(0.02 USD, 0.50 * displayed_spread)",
        ),
        (
            slippage.get("stress_formula"),
            "entry_adverse plus 1.50 times planned_exit_adverse for every leg",
        ),
        (precision.get("binary_float_allowed"), False),
        (precision.get("cost_quantum_usd"), "0.01"),
        (precision.get("rounding"), "ROUND_CEILING"),
    )
    if any(actual != required for actual, required in expected):
        raise ExecutionCostResolutionError(
            "SIGNED_EXECUTION_COST_SEMANTICS_MISMATCH"
        )


def _parse_candidate(
    value: object,
    *,
    now: datetime,
    cost_version: str,
    cost_hash: str,
) -> _ParsedCandidate:
    document = _document(value)
    candidate_id = _nonblank(document.get("candidate_id"), "candidate_id")
    if document.get("execution_cost_contract_version") != cost_version:
        raise ExecutionCostResolutionError("CANDIDATE_COST_VERSION_MISMATCH")
    if document.get("execution_cost_contract_hash") != cost_hash:
        raise ExecutionCostResolutionError("CANDIDATE_COST_HASH_MISMATCH")
    quote_batch_id = _nonblank(document.get("quote_batch_id"), "quote_batch_id")
    symbol = _nonblank(
        document.get("symbol", document.get("underlying")), "symbol"
    ).upper()
    legs = _sequence(document.get("legs"), "candidate legs")
    if not legs:
        raise ExecutionCostResolutionError("CANDIDATE_LEGS_MISSING")

    quoted_legs: list[OptionLegQuote] = []
    seen_contract_ids: set[int] = set()
    total_ratios = 0
    for raw_leg in legs:
        row = _document(raw_leg)
        con_id = row.get("con_id", row.get("conId"))
        if isinstance(con_id, bool) or not isinstance(con_id, int) or con_id <= 0:
            raise ExecutionCostResolutionError("LEG_CONID_INVALID")
        if con_id in seen_contract_ids:
            raise ExecutionCostResolutionError("DUPLICATE_LEG_CONID")
        seen_contract_ids.add(con_id)
        ratio = row.get("ratio", row.get("quantity"))
        if isinstance(ratio, bool) or not isinstance(ratio, int) or ratio <= 0:
            raise ExecutionCostResolutionError("LEG_RATIO_INVALID")
        total_ratios += ratio

        leg_batch = next(
            (
                row.get(field)
                for field in ("quote_batch_id", "quote_snapshot_id", "batch_id")
                if field in row
            ),
            None,
        )
        if leg_batch is not None and leg_batch != quote_batch_id:
            raise ExecutionCostResolutionError("QUOTE_BATCH_MISMATCH")

        bid = _decimal(row.get("bid"), "bid", positive=True)
        ask = _decimal(row.get("ask"), "ask", positive=True)
        if bid >= ask:
            raise ExecutionCostResolutionError("QUOTE_LOCKED_OR_CROSSED")
        observed_at = _timestamp(
            row.get("observed_at", row.get("quote_time")), "observed_at"
        )
        quote_age = _seconds(now - observed_at)
        if quote_age < ZERO or quote_age > MAXIMUM_QUOTE_AGE_SECONDS:
            raise ExecutionCostResolutionError("QUOTE_STALE_OR_FUTURE")

        multiplier = _decimal(row.get("multiplier"), "multiplier", positive=True)
        if multiplier != STANDARD_OPTION_MULTIPLIER:
            raise ExecutionCostResolutionError("NONSTANDARD_OPTION_MULTIPLIER")
        strike = _decimal(row.get("strike"), "strike", positive=True)
        expiration = _date(row.get("expiration"), "expiration")
        underlying = _nonblank(row.get("underlying"), "leg underlying").upper()
        if underlying != symbol:
            raise ExecutionCostResolutionError("LEG_UNDERLYING_MISMATCH")
        if _nonblank(row.get("security_type"), "security_type").upper() != "OPT":
            raise ExecutionCostResolutionError("LEG_SECURITY_TYPE_INVALID")
        currency = _nonblank(row.get("currency"), "currency").upper()
        if currency != "USD":
            raise ExecutionCostResolutionError("LEG_CURRENCY_INVALID")
        exchange = _nonblank(row.get("exchange"), "exchange").upper()
        right = _option_right(row.get("right"))
        side = _position_side(row.get("side"))
        contract = OptionContract(
            contract_id=f"{con_id}@{exchange}",
            underlying=underlying,
            expiration=expiration,
            strike=strike,
            right=right,
            multiplier=multiplier,
            currency=currency,
            exchange=exchange,
            broker_contract_id=con_id,
        )
        leg = OptionLeg(contract=contract, side=side, quantity=ratio)
        quoted_legs.append(
            OptionLegQuote(
                leg=leg,
                bid=bid,
                ask=ask,
                last=None,
                implied_volatility=None,
                volume=None,
                open_interest=None,
                observed_at=observed_at,
            )
        )

    commission = (
        Decimal("2")
        * max(MINIMUM_PER_ORDER, FALLBACK_PER_CONTRACT_SIDE * total_ratios)
    ).quantize(CENT, rounding=ROUND_CEILING)
    slippage = sum(
        (
            max(
                MINIMUM_ENTRY_SLIPPAGE,
                ENTRY_SPREAD_FACTOR * (item.ask - item.bid),
            )
            + max(
                MINIMUM_EXIT_SLIPPAGE,
                EXIT_SPREAD_FACTOR * (item.ask - item.bid),
            )
        )
        * item.contract.multiplier
        * item.leg.quantity
        for item in quoted_legs
        if item.ask is not None and item.bid is not None
    ).quantize(CENT, rounding=ROUND_CEILING)
    total_cost = (commission + slippage).quantize(CENT, rounding=ROUND_CEILING)

    debit = sum(
        (
            item.ask * item.contract.multiplier * item.leg.quantity
            for item in quoted_legs
            if item.leg.side is PositionSide.LONG and item.ask is not None
        ),
        ZERO,
    )
    credit = sum(
        (
            item.bid * item.contract.multiplier * item.leg.quantity
            for item in quoted_legs
            if item.leg.side is PositionSide.SHORT and item.bid is not None
        ),
        ZERO,
    )
    _require_claim(document, "estimated_commissions_usd", commission)
    _require_claim(document, "estimated_slippage_usd", slippage)
    _require_claim(document, "debit_usd", debit)
    _require_claim(document, "credit_usd", credit)
    # A net-credit structure legitimately has a negative all-in *cost*.
    # Commission, slippage, debit, and credit remain nonnegative amounts.
    _require_claim(
        document,
        "all_in_cost_usd",
        debit - credit + total_cost,
        allow_negative=True,
    )

    candidate = StrategyCandidate(
        candidate_id=candidate_id,
        leg_quotes=tuple(quoted_legs),
        estimated_commissions=commission,
        estimated_slippage=slippage,
    )
    payoff = analyze_expiration_payoff(candidate)
    if payoff.status is not PayoffStatus.CALCULATED or payoff.max_loss is None:
        raise ExecutionCostResolutionError("CANDIDATE_PAYOFF_NOT_EXACT")
    claimed_max_loss = _decimal(
        document.get("max_loss_usd"), "max_loss_usd", positive=True
    )
    if claimed_max_loss != payoff.max_loss:
        raise ExecutionCostResolutionError("CANDIDATE_MAX_LOSS_MISMATCH")
    return _ParsedCandidate(
        candidate_id=candidate_id,
        quote_batch_id=quote_batch_id,
        candidate=candidate,
        commission_usd=commission,
        slippage_usd=slippage,
    )


def _parse_scenarios(
    value: object,
    *,
    candidate_id: str,
    cost_version: str,
    cost_hash: str,
) -> tuple[tuple[Decimal, Decimal], ...]:
    document = _document(value)
    action = document.get("action")
    action_text = str(action.value if isinstance(action, Enum) else action)
    if action_text != "TRADE":
        raise ExecutionCostResolutionError("SCENARIO_NOT_TRADABLE")
    supplied_candidate_id = document.get("candidate_id")
    if supplied_candidate_id is not None and supplied_candidate_id != candidate_id:
        raise ExecutionCostResolutionError("SCENARIO_CANDIDATE_ID_MISMATCH")
    if document.get("cost_version") != cost_version:
        raise ExecutionCostResolutionError("SCENARIO_COST_VERSION_MISMATCH")
    if document.get("cost_hash") != cost_hash:
        raise ExecutionCostResolutionError("SCENARIO_COST_HASH_MISMATCH")
    rows = _sequence(document.get("scenarios"), "scenario terms")
    if not rows:
        raise ExecutionCostResolutionError("SCENARIO_TERMS_MISSING")

    terms: list[tuple[Decimal, Decimal]] = []
    prices: set[Decimal] = set()
    for raw_term in rows:
        row = _document(raw_term)
        term_candidate_id = row.get("candidate_id")
        if term_candidate_id is not None and term_candidate_id != candidate_id:
            raise ExecutionCostResolutionError("SCENARIO_CANDIDATE_ID_MISMATCH")
        price_value = row.get(
            "terminal_price", row.get("terminal_underlying_price")
        )
        price = _decimal(price_value, "terminal_price", nonnegative=True)
        probability = _decimal(row.get("probability"), "probability", positive=True)
        if probability > ONE:
            raise ExecutionCostResolutionError("SCENARIO_PROBABILITY_INVALID")
        if price in prices:
            raise ExecutionCostResolutionError("DUPLICATE_SCENARIO_PRICE")
        prices.add(price)
        terms.append((price, probability))
    if sum((probability for _, probability in terms), ZERO) != ONE:
        raise ExecutionCostResolutionError("SCENARIO_PROBABILITY_SUM_INVALID")
    return tuple(terms)


def _resolve_candidate(
    parsed: _ParsedCandidate,
    *,
    terms: tuple[tuple[Decimal, Decimal], ...],
    scan_run_id: str | None,
    cost_version: str,
    cost_hash: str,
) -> CandidateCostResolution:
    payoff = analyze_expiration_payoff(parsed.candidate)
    if payoff.status is not PayoffStatus.CALCULATED:
        raise ExecutionCostResolutionError("CANDIDATE_PAYOFF_NOT_EXACT")
    after_cost_ev = sum(
        (probability * payoff.pnl_at(price) for price, probability in terms),
        ZERO,
    )
    if not after_cost_ev.is_finite():
        raise ExecutionCostResolutionError("AFTER_COST_EV_NONFINITE")
    execution_cost = (
        parsed.commission_usd + parsed.slippage_usd
    ).quantize(CENT, rounding=ROUND_CEILING)
    before_cost_ev = after_cost_ev + execution_cost
    stress_slippage = sum((
        (
            max(MINIMUM_ENTRY_SLIPPAGE, ENTRY_SPREAD_FACTOR * (leg.ask - leg.bid))
            + Decimal("1.50") * max(
                MINIMUM_EXIT_SLIPPAGE, EXIT_SPREAD_FACTOR * (leg.ask - leg.bid)
            )
        ) * leg.contract.multiplier * leg.leg.quantity
        for leg in parsed.candidate.leg_quotes
        if leg.ask is not None and leg.bid is not None
    ), ZERO).quantize(CENT, rounding=ROUND_CEILING)
    stress_cost = (parsed.commission_usd + stress_slippage).quantize(
        CENT, rounding=ROUND_CEILING,
    )
    stress_ev = before_cost_ev - stress_cost
    calculation_body = {
        "schema": "options_copilot.candidate_cost_resolution.v1",
        "scan_run_id": scan_run_id,
        "candidate_id": parsed.candidate_id,
        "cost_version": cost_version,
        "cost_hash": cost_hash,
        "quote_batch_id": parsed.quote_batch_id,
        "commission_usd": parsed.commission_usd,
        "slippage_usd": parsed.slippage_usd,
        "execution_cost_usd": execution_cost,
        "expected_value_before_costs_usd": before_cost_ev,
        "after_cost_expected_value": after_cost_ev,
        "stress_execution_cost_usd": stress_cost,
        "stress_after_cost_expected_value": stress_ev,
        "scenarios": [
            {"terminal_price": price, "probability": probability}
            for price, probability in terms
        ],
    }
    return CandidateCostResolution(
        candidate_id=parsed.candidate_id,
        cost_version=cost_version,
        cost_hash=cost_hash,
        quote_batch_id=parsed.quote_batch_id,
        commission_usd=parsed.commission_usd,
        slippage_usd=parsed.slippage_usd,
        execution_cost_usd=execution_cost,
        expected_value_before_costs_usd=before_cost_ev,
        after_cost_expected_value=after_cost_ev,
        scenario_count=len(terms),
        calculation_hash=canonical_hash(calculation_body),
        stress_execution_cost_usd=stress_cost,
        stress_after_cost_expected_value=stress_ev,
    )


def _candidate_resolution_document(
    value: CandidateCostResolution,
) -> dict[str, object]:
    return {
        "candidate_id": value.candidate_id,
        "cost_version": value.cost_version,
        "cost_hash": value.cost_hash,
        "quote_batch_id": value.quote_batch_id,
        "commission_usd": value.commission_usd,
        "slippage_usd": value.slippage_usd,
        "execution_cost_usd": value.execution_cost_usd,
        "expected_value_before_costs_usd": value.expected_value_before_costs_usd,
        "after_cost_expected_value": value.after_cost_expected_value,
        "stress_execution_cost_usd": value.stress_execution_cost_usd,
        "stress_after_cost_expected_value": value.stress_after_cost_expected_value,
        "scenario_count": value.scenario_count,
        "calculation_hash": value.calculation_hash,
    }


def _require_claim(
    document: Mapping[str, Any],
    field: str,
    expected: Decimal,
    *,
    allow_negative: bool = False,
) -> None:
    supplied = _decimal(document.get(field), field)
    if not allow_negative and supplied < ZERO:
        raise ExecutionCostResolutionError(f"{field.upper()}_NEGATIVE")
    if supplied != expected:
        raise ExecutionCostResolutionError(f"{field.upper()}_MISMATCH")


def _decimal(
    value: object,
    field: str,
    *,
    positive: bool = False,
    nonnegative: bool = False,
) -> Decimal:
    if isinstance(value, bool) or isinstance(value, float) or not isinstance(
        value, (Decimal, int, str)
    ):
        raise ExecutionCostResolutionError(
            f"{field} must be Decimal-compatible without binary float"
        )
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value).strip())
    except (InvalidOperation, ValueError) as exc:
        raise ExecutionCostResolutionError(f"{field} must be a finite Decimal") from exc
    if not result.is_finite():
        raise ExecutionCostResolutionError(f"{field} must be a finite Decimal")
    if positive and result <= ZERO:
        raise ExecutionCostResolutionError(f"{field} must be positive")
    if nonnegative and result < ZERO:
        raise ExecutionCostResolutionError(f"{field} cannot be negative")
    return result


def _timestamp(value: object, field: str) -> datetime:
    if isinstance(value, str):
        text = value.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            value = datetime.fromisoformat(text)
        except ValueError as exc:
            raise ExecutionCostResolutionError(f"{field} is invalid") from exc
    try:
        return utc_datetime(value, field=field)
    except (TypeError, ValueError) as exc:
        raise ExecutionCostResolutionError(str(exc)) from exc


def _date(value: object, field: str) -> date:
    if isinstance(value, datetime):
        raise ExecutionCostResolutionError(f"{field} must be a date")
    if isinstance(value, str):
        try:
            value = date.fromisoformat(value.strip())
        except ValueError as exc:
            raise ExecutionCostResolutionError(f"{field} is invalid") from exc
    if not isinstance(value, date):
        raise ExecutionCostResolutionError(f"{field} must be a date")
    return value


def _seconds(value: object) -> Decimal:
    days = getattr(value, "days", None)
    seconds = getattr(value, "seconds", None)
    microseconds = getattr(value, "microseconds", None)
    if not all(isinstance(item, int) for item in (days, seconds, microseconds)):
        raise ExecutionCostResolutionError("QUOTE_AGE_INVALID")
    total_microseconds = (
        days * 86_400 * 1_000_000 + seconds * 1_000_000 + microseconds
    )
    return Decimal(total_microseconds) / Decimal("1000000")


def _option_right(value: object) -> OptionRight:
    normalized = str(getattr(value, "value", value)).strip().upper()
    aliases = {"C": OptionRight.CALL, "CALL": OptionRight.CALL,
               "P": OptionRight.PUT, "PUT": OptionRight.PUT}
    try:
        return aliases[normalized]
    except KeyError as exc:
        raise ExecutionCostResolutionError("LEG_OPTION_RIGHT_INVALID") from exc


def _position_side(value: object) -> PositionSide:
    normalized = str(getattr(value, "value", value)).strip().upper()
    aliases = {"LONG": PositionSide.LONG, "BUY": PositionSide.LONG,
               "SHORT": PositionSide.SHORT, "SELL": PositionSide.SHORT}
    try:
        return aliases[normalized]
    except KeyError as exc:
        raise ExecutionCostResolutionError("LEG_SIDE_INVALID") from exc


def _document(value: object) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    if is_dataclass(value):
        return {field.name: getattr(value, field.name) for field in fields(value)}
    if hasattr(value, "__dict__"):
        return vars(value)
    raise ExecutionCostResolutionError("DOCUMENT_REQUIRED")


def _sequence(value: object, field: str) -> tuple[object, ...]:
    if value is None:
        return ()
    if not isinstance(value, Sequence) or isinstance(
        value, (str, bytes, bytearray, memoryview)
    ):
        raise ExecutionCostResolutionError(f"{field} must be a sequence")
    return tuple(value)


def _nonblank(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ExecutionCostResolutionError(f"{field} must be nonblank")
    return value.strip()


def _optional_nonblank(value: object, field: str) -> str | None:
    if value is None:
        return None
    return _nonblank(value, field)


def _is_hash(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


__all__ = [
    "CandidateCostResolution",
    "EXECUTION_COST_EFFECTIVE_AT",
    "EXECUTION_COST_HASH",
    "EXECUTION_COST_SIGNER",
    "EXECUTION_COST_VERSION",
    "ExecutionCostCurrentnessError",
    "ExecutionCostResolution",
    "ExecutionCostResolutionError",
    "SignedExecutionCostResolver",
]
