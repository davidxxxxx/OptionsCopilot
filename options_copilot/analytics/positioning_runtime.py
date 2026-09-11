"""Read-only runtime projection for option-positioning analytics.

The projection deliberately has no approval, eligibility, instruction, order,
or broker-acquisition behavior.  It wraps the existing deterministic Max Pain,
wall, PCR, and estimated-GEX calculation with the data-quality facts needed to
interpret those values without promoting them beyond supporting evidence.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum
from typing import Any

from options_copilot.analytics.positioning import calculate_positioning
from options_copilot.gateway.ibkr_readonly import OptionQuoteSnapshot


ZERO = Decimal("0")
ONE = Decimal("1")
SUPPORTING_ONLY = "SUPPORTING_ONLY"
DEFAULT_MAXIMUM_DATA_AGE = timedelta(seconds=5)
SCHEMA_VERSION = "options_copilot.positioning_projection.v1"

_GREEK_FIELDS = ("delta", "gamma", "theta", "vega")
_BASE_LIMITATIONS = (
    "SUPPORTING_ONLY: these values cannot affect eligibility, approval, "
    "instruction creation, or orders.",
    "Open interest may be delayed or prior-session data and does not identify "
    "dealer direction or customer direction.",
    "Estimated GEX is an OI proxy that assumes calls are positive and puts are "
    "negative; actual dealer positioning and hedging direction are unknown.",
    "Max Pain, walls, and PCR are descriptive concentrations, not executable "
    "prices, payoff, liquidity, or risk evidence.",
)
_PROHIBITED_USES = ("ELIGIBILITY", "APPROVAL", "INSTRUCTION", "ORDER")


class PositioningProjectionStatus(str, Enum):
    """Data-quality state; never a trading or eligibility state."""

    READY = "READY"
    DEGRADED = "DEGRADED"
    UNAVAILABLE = "UNAVAILABLE"


@dataclass(frozen=True, slots=True)
class PositioningProjection:
    """Immutable supporting-only view of one underlying/expiration chain.

    Rates are fractions in the inclusive range ``0`` through ``1``.  The
    conservative ``data_asof`` is the oldest quote timestamp, so
    ``data_age_seconds`` is the maximum age of any quote used in the view.
    """

    status: PositioningProjectionStatus
    reasons: tuple[str, ...]
    underlying: str | None
    expiration: date | None
    generated_at: datetime
    data_asof: datetime | None
    data_age_seconds: Decimal | None
    newest_data_asof: datetime | None
    newest_data_age_seconds: Decimal | None
    maximum_data_age_seconds: Decimal
    observed_contract_count: int
    unique_contract_count: int
    expected_contract_count: int | None
    option_chain_coverage_rate: Decimal | None
    stale_contract_count: int
    future_contract_count: int
    delayed_contract_count: int
    missing_open_interest_count: int
    missing_open_interest_rate: Decimal | None
    missing_greeks_count: int
    missing_greeks_rate: Decimal | None
    missing_gamma_count: int
    missing_gamma_rate: Decimal | None
    gex_usable_contract_count: int
    gex_usable_contract_rate: Decimal | None
    max_pain: Decimal | None
    call_wall: Decimal | None
    put_wall: Decimal | None
    call_open_interest: int
    put_open_interest: int
    put_call_open_interest_ratio: Decimal | None
    estimated_net_gex_usd_per_one_percent: Decimal | None
    limitations: tuple[str, ...]
    schema_version: str = field(default=SCHEMA_VERSION, init=False)
    decision_authority: str = field(default=SUPPORTING_ONLY, init=False)
    supporting_only: bool = field(default=True, init=False)
    affects_eligibility: bool = field(default=False, init=False)
    approval_allowed: bool = field(default=False, init=False)
    instruction_allowed: bool = field(default=False, init=False)
    order_allowed: bool = field(default=False, init=False)
    prohibited_uses: tuple[str, ...] = field(default=_PROHIBITED_USES, init=False)

    @property
    def pcr(self) -> Decimal | None:
        """Concise display alias for the put/call open-interest ratio."""

        return self.put_call_open_interest_ratio

    @property
    def gex_usd_per_one_percent(self) -> Decimal | None:
        """Concise display alias for the explicitly estimated GEX proxy."""

        return self.estimated_net_gex_usd_per_one_percent

    def to_dict(self) -> dict[str, Any]:
        """Return a serialization-friendly, flat read-model document."""

        return {
            "schema_version": self.schema_version,
            "decision_authority": self.decision_authority,
            "supporting_only": self.supporting_only,
            "status": self.status.value,
            "reasons": self.reasons,
            "underlying": self.underlying,
            "expiration": self.expiration,
            "generated_at": self.generated_at,
            "data_asof": self.data_asof,
            "data_age_seconds": self.data_age_seconds,
            "newest_data_asof": self.newest_data_asof,
            "newest_data_age_seconds": self.newest_data_age_seconds,
            "maximum_data_age_seconds": self.maximum_data_age_seconds,
            "observed_contract_count": self.observed_contract_count,
            "unique_contract_count": self.unique_contract_count,
            "expected_contract_count": self.expected_contract_count,
            "option_chain_coverage_rate": self.option_chain_coverage_rate,
            "stale_contract_count": self.stale_contract_count,
            "future_contract_count": self.future_contract_count,
            "delayed_contract_count": self.delayed_contract_count,
            "missing_open_interest_count": self.missing_open_interest_count,
            "missing_open_interest_rate": self.missing_open_interest_rate,
            "missing_greeks_count": self.missing_greeks_count,
            "missing_greeks_rate": self.missing_greeks_rate,
            "missing_gamma_count": self.missing_gamma_count,
            "missing_gamma_rate": self.missing_gamma_rate,
            "gex_usable_contract_count": self.gex_usable_contract_count,
            "gex_usable_contract_rate": self.gex_usable_contract_rate,
            "max_pain": self.max_pain,
            "call_wall": self.call_wall,
            "put_wall": self.put_wall,
            "call_open_interest": self.call_open_interest,
            "put_open_interest": self.put_open_interest,
            "put_call_open_interest_ratio": self.put_call_open_interest_ratio,
            "estimated_net_gex_usd_per_one_percent": (
                self.estimated_net_gex_usd_per_one_percent
            ),
            "limitations": self.limitations,
            "affects_eligibility": self.affects_eligibility,
            "approval_allowed": self.approval_allowed,
            "instruction_allowed": self.instruction_allowed,
            "order_allowed": self.order_allowed,
            "prohibited_uses": self.prohibited_uses,
        }

    as_dict = to_dict


def project_positioning(
    quotes: Iterable[OptionQuoteSnapshot],
    *,
    underlying_spot: Decimal,
    now: datetime | None = None,
    expected_contract_count: int | None = None,
    maximum_data_age: timedelta = DEFAULT_MAXIMUM_DATA_AGE,
) -> PositioningProjection:
    """Project one read-only chain into transparent supporting evidence.

    Missing market fields, stale/future timestamps, partial coverage, delayed
    feeds, and duplicate rows are returned as quality reasons.  They never
    become exceptions merely because the external data is incomplete.  Type
    errors in the caller-owned configuration still raise immediately.
    """

    checked_spot = _positive_decimal(underlying_spot, "underlying_spot")
    generated_at = _aware_utc(now or datetime.now(timezone.utc), "now")
    maximum_age_seconds = _maximum_age_seconds(maximum_data_age)
    _validate_expected_count(expected_contract_count)
    try:
        source_rows = tuple(quotes)
    except TypeError as exc:
        raise TypeError("quotes must be an iterable of OptionQuoteSnapshot") from exc
    if not all(isinstance(item, OptionQuoteSnapshot) for item in source_rows):
        raise TypeError("quotes must contain only OptionQuoteSnapshot values")

    if not source_rows:
        coverage = (
            ZERO
            if expected_contract_count is not None and expected_contract_count > 0
            else None
        )
        return PositioningProjection(
            status=PositioningProjectionStatus.UNAVAILABLE,
            reasons=("NO_OPTION_QUOTES",),
            underlying=None,
            expiration=None,
            generated_at=generated_at,
            data_asof=None,
            data_age_seconds=None,
            newest_data_asof=None,
            newest_data_age_seconds=None,
            maximum_data_age_seconds=maximum_age_seconds,
            observed_contract_count=0,
            unique_contract_count=0,
            expected_contract_count=expected_contract_count,
            option_chain_coverage_rate=coverage,
            stale_contract_count=0,
            future_contract_count=0,
            delayed_contract_count=0,
            missing_open_interest_count=0,
            missing_open_interest_rate=None,
            missing_greeks_count=0,
            missing_greeks_rate=None,
            missing_gamma_count=0,
            missing_gamma_rate=None,
            gex_usable_contract_count=0,
            gex_usable_contract_rate=None,
            max_pain=None,
            call_wall=None,
            put_wall=None,
            call_open_interest=0,
            put_open_interest=0,
            put_call_open_interest_ratio=None,
            estimated_net_gex_usd_per_one_percent=None,
            limitations=_limitations(("NO_OPTION_QUOTES",)),
        )

    reasons: set[str] = set()
    rows, duplicate, conflicting_identity = _deduplicate(source_rows)
    if duplicate:
        reasons.add("DUPLICATE_CONTRACT_QUOTES")
    if conflicting_identity:
        reasons.add("CONFLICTING_CONTRACT_IDENTITY")

    invalid_contract = any(not _valid_contract(item) for item in rows)
    if invalid_contract:
        reasons.add("INVALID_CONTRACT_IDENTITY")
    chain_keys = {
        (item.contract.symbol.strip().upper(), item.contract.expiration)
        for item in rows
        if _valid_contract(item)
    }
    homogeneous = (
        len(chain_keys) == 1 and not invalid_contract and not conflicting_identity
    )
    if not homogeneous:
        reasons.add("MIXED_UNDERLYING_OR_EXPIRATION")
        underlying = None
        expiration = None
    else:
        underlying, expiration = next(iter(chain_keys))

    timestamps = tuple(item.observed_at for item in rows if _aware(item.observed_at))
    timestamps_valid = len(timestamps) == len(rows)
    if not timestamps_valid:
        reasons.add("INVALID_OBSERVED_AT")
    data_asof = min(timestamps) if timestamps else None
    newest_data_asof = max(timestamps) if timestamps else None
    data_age = (
        _timedelta_decimal(generated_at - data_asof) if data_asof is not None else None
    )
    newest_age = (
        _timedelta_decimal(generated_at - newest_data_asof)
        if newest_data_asof is not None
        else None
    )
    stale_count = sum(
        _timedelta_decimal(generated_at - item.observed_at) > maximum_age_seconds
        for item in rows
        if _aware(item.observed_at) and item.observed_at <= generated_at
    )
    future_count = sum(
        item.observed_at > generated_at for item in rows if _aware(item.observed_at)
    )
    if stale_count:
        reasons.add("STALE_DATA")
    if future_count:
        reasons.add("FUTURE_DATA")

    delayed_count = sum(item.is_delayed for item in rows)
    if delayed_count:
        reasons.add("DELAYED_MARKET_DATA")

    unique_count = len(rows)
    coverage = _coverage(unique_count, expected_contract_count, reasons)
    normalized_rows = tuple(_normalize_market_fields(item) for item in rows)
    missing_oi_count = sum(item.open_interest is None for item in normalized_rows)
    missing_greek_count = sum(
        any(getattr(item, name) is None for name in _GREEK_FIELDS)
        for item in normalized_rows
    )
    missing_gamma_count = sum(item.gamma is None for item in normalized_rows)
    gex_usable_count = sum(
        item.open_interest is not None and item.gamma is not None
        for item in normalized_rows
    )
    if missing_oi_count:
        reasons.add("MISSING_OPEN_INTEREST")
    if missing_greek_count:
        reasons.add("MISSING_GREEKS")

    max_pain = call_wall = put_wall = ratio = estimated_gex = None
    call_oi = put_oi = 0
    fatal_quality = not homogeneous or not timestamps_valid
    if not fatal_quality:
        try:
            calculation = calculate_positioning(
                normalized_rows,
                underlying_spot=checked_spot,
            )
        except (ArithmeticError, TypeError, ValueError):
            reasons.add("POSITIONING_CALCULATION_UNAVAILABLE")
            fatal_quality = True
        else:
            max_pain = calculation.max_pain
            call_wall = calculation.call_wall
            put_wall = calculation.put_wall
            call_oi = calculation.call_open_interest
            put_oi = calculation.put_open_interest
            ratio = calculation.put_call_open_interest_ratio
            estimated_gex = calculation.estimated_net_gex_usd_per_one_percent

    if not fatal_quality:
        unavailable = []
        if max_pain is None:
            unavailable.append("MAX_PAIN")
        if call_wall is None:
            unavailable.append("CALL_WALL")
        if put_wall is None:
            unavailable.append("PUT_WALL")
        if ratio is None:
            unavailable.append("PCR")
        if estimated_gex is None:
            unavailable.append("GEX")
        if unavailable:
            reasons.add("POSITIONING_SIGNALS_PARTIAL")

    has_metric = any(
        value is not None
        for value in (max_pain, call_wall, put_wall, ratio, estimated_gex)
    )
    status = (
        PositioningProjectionStatus.UNAVAILABLE
        if fatal_quality or not has_metric
        else PositioningProjectionStatus.DEGRADED
        if reasons
        else PositioningProjectionStatus.READY
    )
    ordered_reasons = tuple(sorted(reasons))
    denominator = Decimal(unique_count)
    return PositioningProjection(
        status=status,
        reasons=ordered_reasons,
        underlying=underlying,
        expiration=expiration,
        generated_at=generated_at,
        data_asof=data_asof,
        data_age_seconds=data_age,
        newest_data_asof=newest_data_asof,
        newest_data_age_seconds=newest_age,
        maximum_data_age_seconds=maximum_age_seconds,
        observed_contract_count=len(source_rows),
        unique_contract_count=unique_count,
        expected_contract_count=expected_contract_count,
        option_chain_coverage_rate=coverage,
        stale_contract_count=stale_count,
        future_contract_count=future_count,
        delayed_contract_count=delayed_count,
        missing_open_interest_count=missing_oi_count,
        missing_open_interest_rate=(
            Decimal(missing_oi_count) / denominator if unique_count else None
        ),
        missing_greeks_count=missing_greek_count,
        missing_greeks_rate=(
            Decimal(missing_greek_count) / denominator if unique_count else None
        ),
        missing_gamma_count=missing_gamma_count,
        missing_gamma_rate=(
            Decimal(missing_gamma_count) / denominator if unique_count else None
        ),
        gex_usable_contract_count=gex_usable_count,
        gex_usable_contract_rate=(
            Decimal(gex_usable_count) / denominator if unique_count else None
        ),
        max_pain=max_pain,
        call_wall=call_wall,
        put_wall=put_wall,
        call_open_interest=call_oi,
        put_open_interest=put_oi,
        put_call_open_interest_ratio=ratio,
        estimated_net_gex_usd_per_one_percent=estimated_gex,
        limitations=_limitations(ordered_reasons),
    )


def _deduplicate(
    rows: tuple[OptionQuoteSnapshot, ...],
) -> tuple[tuple[OptionQuoteSnapshot, ...], bool, bool]:
    selected: dict[int, OptionQuoteSnapshot] = {}
    duplicate = False
    conflicting_identity = False
    for item in rows:
        contract_id = item.contract.contract_id
        previous = selected.get(contract_id)
        if previous is None:
            selected[contract_id] = item
            continue
        duplicate = True
        if previous.contract != item.contract:
            conflicting_identity = True
            continue
        if _aware(item.observed_at) and (
            not _aware(previous.observed_at) or item.observed_at > previous.observed_at
        ):
            selected[contract_id] = item
    return tuple(selected.values()), duplicate, conflicting_identity


def _normalize_market_fields(item: OptionQuoteSnapshot) -> OptionQuoteSnapshot:
    open_interest = (
        item.open_interest if _nonnegative_integer(item.open_interest) else None
    )
    greek_values = {
        name: value if _finite_decimal(value) else None
        for name in _GREEK_FIELDS
        if (value := getattr(item, name)) is not None
    }
    for name in _GREEK_FIELDS:
        greek_values.setdefault(name, None)
    return replace(item, open_interest=open_interest, **greek_values)


def _coverage(
    observed: int,
    expected: int | None,
    reasons: set[str],
) -> Decimal | None:
    if expected is None or expected == 0:
        reasons.add("OPTION_CHAIN_COVERAGE_UNKNOWN")
        return None
    if observed < expected:
        reasons.add("PARTIAL_OPTION_CHAIN_COVERAGE")
    elif observed > expected:
        reasons.add("OBSERVED_COUNT_EXCEEDS_EXPECTED")
    return min(ONE, Decimal(observed) / Decimal(expected))


def _limitations(reasons: tuple[str, ...]) -> tuple[str, ...]:
    result = list(_BASE_LIMITATIONS)
    reason_set = set(reasons)
    if "OPTION_CHAIN_COVERAGE_UNKNOWN" in reason_set:
        result.append(
            "Option-chain coverage is unknown because no independent expected-contract "
            "denominator was supplied."
        )
    if "PARTIAL_OPTION_CHAIN_COVERAGE" in reason_set:
        result.append(
            "The observed option chain is partial; structural levels and ratios use only "
            "the disclosed subset."
        )
    if "MISSING_OPEN_INTEREST" in reason_set:
        result.append(
            "Contracts with missing or invalid open interest are excluded from OI-based "
            "contributions."
        )
    if "MISSING_GREEKS" in reason_set:
        result.append(
            "Contracts with missing or invalid gamma cannot contribute to estimated GEX; "
            "other missing Greeks are reported for chain-quality review."
        )
    if "STALE_DATA" in reason_set or "FUTURE_DATA" in reason_set:
        result.append(
            "Stale or future-dated rows remain observation-only and are explicitly "
            "degraded; their values are not freshness authority."
        )
    if "DELAYED_MARKET_DATA" in reason_set:
        result.append("At least one row is explicitly marked as delayed market data.")
    if "DUPLICATE_CONTRACT_QUOTES" in reason_set:
        result.append(
            "Duplicate contract rows are disclosed and only the newest valid-timestamp "
            "row is counted."
        )
    if "NO_OPTION_QUOTES" in reason_set:
        result.append(
            "No option quotes were available, so every positioning value is null."
        )
    return tuple(result)


def _valid_contract(item: OptionQuoteSnapshot) -> bool:
    contract = item.contract
    return (
        isinstance(contract.contract_id, int)
        and not isinstance(contract.contract_id, bool)
        and contract.contract_id > 0
        and isinstance(contract.symbol, str)
        and bool(contract.symbol.strip())
        and isinstance(contract.expiration, date)
        and not isinstance(contract.expiration, datetime)
        and _finite_decimal(contract.strike)
        and contract.strike > ZERO
        and contract.right in {"C", "P"}
        and isinstance(contract.multiplier, int)
        and not isinstance(contract.multiplier, bool)
        and contract.multiplier > 0
    )


def _validate_expected_count(value: int | None) -> None:
    if value is None:
        return
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError("expected_contract_count must be an integer or None")
    if value < 0:
        raise ValueError("expected_contract_count cannot be negative")


def _maximum_age_seconds(value: timedelta) -> Decimal:
    if not isinstance(value, timedelta):
        raise TypeError("maximum_data_age must be a timedelta")
    result = _timedelta_decimal(value)
    if result < ZERO:
        raise ValueError("maximum_data_age cannot be negative")
    return result


def _positive_decimal(value: object, field_name: str) -> Decimal:
    if not _finite_decimal(value):
        raise TypeError(f"{field_name} must be a finite Decimal")
    assert isinstance(value, Decimal)
    if value <= ZERO:
        raise ValueError(f"{field_name} must be positive")
    return value


def _finite_decimal(value: object) -> bool:
    return isinstance(value, Decimal) and value.is_finite()


def _nonnegative_integer(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _aware(value: object) -> bool:
    return (
        isinstance(value, datetime)
        and value.tzinfo is not None
        and value.utcoffset() is not None
    )


def _aware_utc(value: object, field_name: str) -> datetime:
    if not _aware(value):
        raise ValueError(f"{field_name} must be timezone-aware")
    assert isinstance(value, datetime)
    return value.astimezone(timezone.utc)


def _timedelta_decimal(value: timedelta) -> Decimal:
    whole_seconds = value.days * 86_400 + value.seconds
    return Decimal(whole_seconds) + Decimal(value.microseconds) / Decimal(1_000_000)


build_positioning_projection = project_positioning
SupportingPositioningProjection = PositioningProjection


__all__ = [
    "DEFAULT_MAXIMUM_DATA_AGE",
    "PositioningProjection",
    "PositioningProjectionStatus",
    "SCHEMA_VERSION",
    "SUPPORTING_ONLY",
    "SupportingPositioningProjection",
    "build_positioning_projection",
    "project_positioning",
]
