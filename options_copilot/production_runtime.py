"""Fail-closed production adapters for the Options Copilot composition root.

The objects in this module own read-only research only.  They deliberately do
not import approval, bridge, creator, order, or broker-write primitives.
"""
from __future__ import annotations

import hashlib
import re
import threading
from collections.abc import Callable, Iterable, Mapping, Sequence
from contextlib import contextmanager, nullcontext
from copy import deepcopy
from dataclasses import asdict, dataclass, field, is_dataclass, replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_HALF_EVEN
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from options_copilot.analytics.iv_percentile import IV_PERCENTILE_CONFIRMATION_OBSERVED_AT
from options_copilot.analytics.positioning_runtime import project_positioning
from options_copilot.feature_source_resolution import validate_feature_source_binding
from options_copilot.gateway import (
    AtomicBrokerSnapshot,
    atomic_account_nlv,
    BatchedOptionQuote,
    BrokerConnectionError,
    BrokerSnapshotBuilder,
    BrokerSnapshotStatus,
    IBKRReadOnlyGateway,
    MarketDataPacingError,
    OptionContractRef,
    OptionQualificationError,
    OptionQuoteSnapshot,
    OptionQuoteBatch,
    OptionSecDefSnapshot,
    QuoteBatchStatus,
    UnderlyingQuoteSnapshot,
    UnderlyingIvHistory,
)
from options_copilot.performance.nav_ledger import StrategyNavSnapshot
from options_copilot.domain import (
    OptionContract,
    OptionLeg,
    OptionLegQuote,
    OptionRight as DomainOptionRight,
    PositionSide,
    StrategyCandidate,
    TerminalScenario,
)
from options_copilot.execution_cost import (
    CENT,
    ENTRY_SPREAD_FACTOR,
    EXECUTION_COST_HASH,
    EXECUTION_COST_VERSION,
    EXIT_SPREAD_FACTOR,
    FALLBACK_PER_CONTRACT_SIDE,
    MINIMUM_ENTRY_SLIPPAGE,
    MINIMUM_EXIT_SLIPPAGE,
    MINIMUM_PER_ORDER,
)
from options_copilot.governance.contracts import (
    ContractKind,
    ContractValidationError,
    verify_contract,
)
from options_copilot.news.models import (
    ConditionalOptionLeg,
    ConditionalOptionPreselection,
    ImpactDirection,
    MarketConfirmation,
    OptionLegSide,
    OptionRight,
    OptionTradabilityInput,
    PreselectionPhase,
    UnderlyingQuoteBasis,
)
from options_copilot.news.preselection import strategy_structure_hash
from options_copilot.news.open_reprice_economics import strategy_nav_post_hash
from options_copilot.news.macro_proxy import require_current_research_proxy_binding
from options_copilot.news.preselection_producer import (
    ResolvedStructure,
    Top10StructureResolution,
    validate_entry_positions,
)
from options_copilot.option_pool.models import (
    normalize_equity_thesis_row,
    option_candidate_identity,
)
from options_copilot.option_pool.leg_identity import normalise_option_right, normalise_option_side
from options_copilot.scanner.coverage import OrdinaryScanCoverage
from options_copilot.news_runtime import IbkrNewsBinding
from options_copilot.operations.capabilities import (
    CapabilityStatus,
    DEFAULT_PACING_MAX_AGE,
)
from options_copilot.operations.pacing_authority import (
    ApprovedPacingCapabilityResolution,
    PacingAuthoritySignatureVerifier,
    load_approved_pacing_capability,
)
from options_copilot.scanner.pacing import (
    PACING_CAPABILITY_MISSING,
    BudgetDecision,
    RequestBudgetByClass,
    RequestBudgetLease,
)
from options_copilot.risk import PayoffStatus, analyze_expiration_payoff
from options_copilot.research_allocation import (
    RESEARCH_ALLOCATION_INPUT_INVALID,
    ResearchAllocationInputError,
    balanced_research_symbols,
    build_research_allocation_evidence,
    canonical_research_score,
    canonical_research_symbol,
)
from options_copilot.storage.canonical import (
    canonical_hash,
    freeze_json,
    utc_datetime,
)
from options_copilot.storage.evidence import EvidenceRecord, EvidenceStore
from options_copilot.strategies import option_quote_liquidity_assessment


_DIGEST_CHARS = frozenset("0123456789abcdef")
_CREATOR_UNAVAILABLE = "CREATOR_TRANSPORT_UNAVAILABLE"
_SCANNER_PACING_USAGE_UNAVAILABLE = "SCANNER_PACING_USAGE_UNAVAILABLE"
_SCANNER_PACING_USAGE_INVALID = "SCANNER_PACING_USAGE_INVALID"
_OPTION_PIPELINE_PACING_USAGE_UNAVAILABLE = (
    "OPTION_PIPELINE_PACING_USAGE_UNAVAILABLE"
)
_OPTION_PIPELINE_PACING_USAGE_INVALID = "OPTION_PIPELINE_PACING_USAGE_INVALID"
_DERIVATIVE_SECURITY_TYPES = frozenset({"OPT", "BAG", "COMBO"})
_CONNECT_TIMEOUT_SECONDS = 8.0
_CONTROL_REFRESH_SECONDS = 5.0
_CONTROL_STALE_SECONDS = 15.0
_EXECUTABLE_QUOTE_MAX_AGE_SECONDS = 5.0
_SUPERVISOR_POLL_SECONDS = 1.0
_RECONNECT_BACKOFF_SECONDS = (1.0, 2.0, 4.0, 8.0, 15.0, 30.0, 60.0)
_UNDERLYING_SCAN_CODES = ("MOST_ACTIVE", "TOP_PERC_GAIN", "TOP_PERC_LOSE")


class PacingAuthorityGuard:
    """Re-read one exact human-approved pacing head before every request.

    A renewed or replaced capability requires a process restart.  This keeps a
    single cumulative ``RequestBudgetByClass`` alive for the process lifetime
    and prevents a file change from silently resetting its counters.
    """

    def __init__(
        self,
        run_directory: str | Path | None,
        *,
        expected_actor: str,
        clock: Callable[[], datetime] | None = None,
        signature_verifier: PacingAuthoritySignatureVerifier | None = None,
    ) -> None:
        self.run_directory = None if run_directory is None else Path(run_directory)
        self.expected_actor = expected_actor
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self.signature_verifier = signature_verifier
        self._lock = threading.RLock()
        self._initial = self._load()

    @property
    def initial_resolution(self) -> ApprovedPacingCapabilityResolution:
        return self._initial

    @property
    def capability(self):
        return self._initial.capability

    @property
    def ready(self) -> bool:
        return not self.reasons()

    def now(self) -> datetime:
        return utc_datetime(self._clock(), field="pacing clock")

    def reasons(self) -> tuple[str, ...]:
        with self._lock:
            current = self._load()
            reasons = list(current.record.reason_codes)
            initial = self._initial
            if not initial.ready:
                reasons.extend(initial.record.reason_codes)
            elif (
                not current.ready
                or current.capability is None
                or initial.capability is None
                or current.capability.content_hash != initial.capability.content_hash
                or current.approval_hash != initial.approval_hash
                or current.actor != initial.actor
            ):
                reasons.extend(("PACING_CAPABILITY_CHANGED_OR_STALE", PACING_CAPABILITY_MISSING))
            return tuple(dict.fromkeys(reasons))

    def _load(self) -> ApprovedPacingCapabilityResolution:
        directory = self.run_directory
        if directory is None:
            # The loader remains the sole parser.  A deliberately absent path
            # yields its canonical PACING_CAPABILITY_MISSING record.
            directory = Path("__options_copilot_pacing_authority_not_selected__")
        return load_approved_pacing_capability(
            directory,
            now=self.now(),
            expected_actor=self.expected_actor,
            signature_verifier=self.signature_verifier,
        )


class GuardedRequestBudget:
    """One non-resetting budget whose every operation rechecks its authority."""

    def __init__(
        self,
        guard: PacingAuthorityGuard,
        *,
        now: datetime,
    ) -> None:
        self.guard = guard
        validation_max_age = (
            timedelta.max
            if guard.initial_resolution.policy_authority is not None
            else DEFAULT_PACING_MAX_AGE
        )
        self._budget = RequestBudgetByClass(
            guard.capability,
            now=now,
            clock=guard.now,
            validation_max_age=validation_max_age,
        )

    @property
    def ready(self) -> bool:
        return self._budget.ready and self.guard.ready

    @property
    def reason(self) -> str | None:
        return None if self.ready else PACING_CAPABILITY_MISSING

    @property
    def capability_hash(self) -> str | None:
        return self._budget.capability_hash if self.ready else None

    def decision(self, request_class: str) -> BudgetDecision:
        if not self.guard.ready:
            usage = self._budget.usage()
            if request_class not in usage:
                # Preserve RequestBudgetByClass's closed request-class domain.
                return self._budget.decision(request_class)
            row = usage[request_class]
            return BudgetDecision(
                False,
                request_class,
                PACING_CAPABILITY_MISSING,
                row["used"],
                row["limit"],
            )
        return self._budget.decision(request_class)

    consume = decision

    def lease(self, request_class: str) -> RequestBudgetLease:
        if not self.guard.ready:
            return self._budget.denied_lease(
                request_class,
                PACING_CAPABILITY_MISSING,
            )
        return self._budget.lease(request_class)

    def usage(self) -> dict[str, dict[str, int]]:
        return self._budget.usage()

    def approved_max_concurrency(self, request_class: str) -> int:
        """Return one signed class limit, or the fail-closed serial default."""

        capability = self.guard.capability
        if not self.ready or capability is None:
            return 1
        row = capability.request_classes.get(request_class)
        if not isinstance(row, Mapping):
            return 1
        value = row.get("max_concurrency")
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            return 1
        return value

    def cache_record(self, **kwargs: object):
        if not self.guard.ready:
            raise RuntimeError(PACING_CAPABILITY_MISSING)
        return self._budget.cache_record(**kwargs)  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class _UnderlyingDiscoveryRead:
    rows: tuple[object, ...]
    completed_scan_codes: tuple[str, ...]
    failed_scan_codes: tuple[str, ...]
    reason_codes: tuple[str, ...]
    source_row_counts: tuple[tuple[str, int], ...] = ()
    pacing_usage: Mapping[str, object] | None = None
    confirmed_charged_scanner_requests: int = 0


@dataclass(frozen=True, slots=True)
class _PositionModeEquityResearchRead:
    reason_codes: tuple[str, ...]
    discovery: _UnderlyingDiscoveryRead


class _PacingUsageSnapshotError(ValueError):
    """Raised before publishing research that lacks its pacing witness."""

    def __init__(self, reason_code: str) -> None:
        super().__init__(reason_code)
        self.reason_code = reason_code


def _empty_pacing_usage_snapshot() -> Mapping[str, object]:
    frozen = freeze_json({})
    if not isinstance(frozen, Mapping):
        raise RuntimeError("canonical empty pacing usage is not a mapping")
    return frozen


def _pacing_usage_snapshot(
    pacing: object,
    *,
    required_request_class: str | None = None,
    minimum_required_usage: int = 0,
    discovered_underlyings: int = 0,
) -> Mapping[str, object]:
    """Freeze request usage at the acquisition boundary that produced it."""

    usage_reader = getattr(pacing, "usage", None)
    if not callable(usage_reader):
        if required_request_class is not None:
            raise _PacingUsageSnapshotError(_SCANNER_PACING_USAGE_UNAVAILABLE)
        return _empty_pacing_usage_snapshot()
    try:
        usage = usage_reader()
    except Exception:
        if required_request_class is not None:
            raise _PacingUsageSnapshotError(
                _SCANNER_PACING_USAGE_UNAVAILABLE
            ) from None
        return _empty_pacing_usage_snapshot()
    try:
        frozen = freeze_json(usage)
    except Exception:
        if required_request_class is not None:
            raise _PacingUsageSnapshotError(
                _SCANNER_PACING_USAGE_INVALID
            ) from None
        return _empty_pacing_usage_snapshot()
    if not isinstance(frozen, Mapping):
        if required_request_class is not None:
            raise _PacingUsageSnapshotError(_SCANNER_PACING_USAGE_INVALID)
        return _empty_pacing_usage_snapshot()
    for request_class, raw_usage in frozen.items():
        if (
            not isinstance(request_class, str)
            or not request_class.strip()
            or not isinstance(raw_usage, Mapping)
            or set(raw_usage) != {"used", "limit"}
        ):
            if required_request_class is not None:
                raise _PacingUsageSnapshotError(_SCANNER_PACING_USAGE_INVALID)
            return _empty_pacing_usage_snapshot()
        used = raw_usage.get("used")
        limit = raw_usage.get("limit")
        if (
            isinstance(used, bool)
            or not isinstance(used, int)
            or isinstance(limit, bool)
            or not isinstance(limit, int)
            or used < 0
            or limit < 0
            or used > limit
        ):
            if required_request_class is not None:
                raise _PacingUsageSnapshotError(_SCANNER_PACING_USAGE_INVALID)
            return _empty_pacing_usage_snapshot()
    if required_request_class is not None:
        if (
            isinstance(minimum_required_usage, bool)
            or not isinstance(minimum_required_usage, int)
            or not 0 <= minimum_required_usage <= len(_UNDERLYING_SCAN_CODES)
        ):
            raise _PacingUsageSnapshotError(_SCANNER_PACING_USAGE_INVALID)
        required_usage = frozen.get(required_request_class)
        if not isinstance(required_usage, Mapping):
            raise _PacingUsageSnapshotError(_SCANNER_PACING_USAGE_INVALID)
        required_used = required_usage.get("used")
        if (
            isinstance(required_used, bool)
            or not isinstance(required_used, int)
            or required_used < minimum_required_usage
            or discovered_underlyings > minimum_required_usage * 50
        ):
            raise _PacingUsageSnapshotError(_SCANNER_PACING_USAGE_INVALID)
    return frozen


def _discover_underlyings(
    gateway: object,
    pacing: object,
) -> _UnderlyingDiscoveryRead:
    """Charge and execute each IBKR scanner subscription independently."""

    discovered: list[object] = []
    seen: set[object] = set()
    completed: list[str] = []
    failed: list[str] = []
    reasons: list[str] = []
    source_row_counts: list[tuple[str, int]] = []
    confirmed_charged_scanner_requests = 0
    gateway_paced = bool(
        getattr(gateway, "market_data_pacing_enabled", False)
    )
    for scan_code in _UNDERLYING_SCAN_CODES:
        lease = _gateway_request_lease(gateway, pacing, "scanner")
        with lease as decision:
            if not bool(getattr(decision, "allowed", False)):
                denied_scan_codes = tuple(
                    code for code in _UNDERLYING_SCAN_CODES
                    if code not in completed and code not in failed
                )
                failed.extend(denied_scan_codes)
                source_row_counts.extend(
                    (code, 0) for code in denied_scan_codes
                )
                reasons.extend(
                    _pacing_denial_reasons("IBKR_SCANNER", decision)
                )
                break
            if not gateway_paced:
                confirmed_charged_scanner_requests += 1
            try:
                rows = tuple(
                    gateway.scan_underlyings(  # type: ignore[attr-defined]
                        scan_codes=(scan_code,),
                        rows_per_scan=50,
                    )
                )
            except MarketDataPacingError as exc:
                # The gateway can exhaust its wire-level window after an
                # earlier scanner source succeeded. Preserve those rows as
                # research evidence, but mark this and every unattempted
                # source failed so the action path remains NO_TRADE.
                paced_scan_codes = tuple(
                    code for code in _UNDERLYING_SCAN_CODES
                    if code not in completed and code not in failed
                )
                failed.extend(paced_scan_codes)
                source_row_counts.extend(
                    (code, 0) for code in paced_scan_codes
                )
                reasons.extend(
                    _pacing_error_reasons("IBKR_SCANNER", exc)
                )
                break
            except Exception:
                # A gateway-owned failure can occur before or after its
                # internal lease.  Without an attestation, do not claim that
                # this request consumed pacing authority.
                failed.append(scan_code)
                source_row_counts.append((scan_code, 0))
                reasons.append(f"IBKR_SCANNER_{scan_code}_UNAVAILABLE")
                continue
            if gateway_paced:
                confirmed_charged_scanner_requests += 1
        completed.append(scan_code)
        source_row_counts.append((scan_code, len(rows)))
        for row in rows:
            contract_id = getattr(row, "contract_id", None)
            symbol = str(getattr(row, "symbol", "")).strip().upper()
            identity: object = (
                ("CONTRACT", contract_id)
                if isinstance(contract_id, int) and contract_id > 0
                else ("SYMBOL", symbol)
            )
            if not symbol or identity in seen:
                continue
            seen.add(identity)
            discovered.append(row)
    return _UnderlyingDiscoveryRead(
        rows=tuple(discovered),
        completed_scan_codes=tuple(completed),
        failed_scan_codes=tuple(dict.fromkeys(failed)),
        reason_codes=tuple(dict.fromkeys(reasons)),
        source_row_counts=tuple(source_row_counts),
        pacing_usage=_pacing_usage_snapshot(
            pacing,
            required_request_class="scanner",
            minimum_required_usage=confirmed_charged_scanner_requests,
            discovered_underlyings=len(discovered),
        ),
        confirmed_charged_scanner_requests=(
            confirmed_charged_scanner_requests
        ),
    )


def _scanner_result_row(item: object) -> dict[str, object]:
    rank = getattr(item, "rank")
    return {
        "symbol": getattr(item, "symbol"),
        "rank": rank,
        "score": max(Decimal("0"), Decimal("100") - Decimal(rank)),
        "source_scan": getattr(item, "source_scan"),
        "contract_id": getattr(item, "contract_id"),
        "exchange": getattr(item, "exchange", None),
        "industry": getattr(item, "industry", None),
        "category": getattr(item, "category", None),
        "subcategory": getattr(item, "subcategory", None),
    }


@dataclass(frozen=True, slots=True)
class _UnderlyingQuoteRead:
    rows: tuple[object, ...]
    requested_symbols: tuple[str, ...]
    observed_symbols: tuple[str, ...]
    missing_symbols: tuple[str, ...]
    reason_codes: tuple[str, ...]
    failed_symbols: tuple[str, ...] = ()

    @property
    def complete(self) -> bool:
        return not self.reason_codes


@dataclass(frozen=True, slots=True)
class _IndicativeUnderlyingQuoteBasis:
    """Closing stock mark that can select research direction, never action."""

    symbol: str
    contract_id: int
    exchange: str
    source: str
    observed_at: datetime
    bid: Decimal | None
    ask: Decimal | None
    last: Decimal | None
    close: Decimal
    market_data_type: int

    @property
    def market_price(self) -> Decimal | None:
        if self.bid is not None and self.ask is not None and self.ask >= self.bid:
            return (self.bid + self.ask) / Decimal("2")
        return self.last if self.last is not None else self.close

    def as_dict(self) -> dict[str, object]:
        return {
            "schema": "options_copilot.indicative_underlying_quote_basis.v1",
            "symbol": self.symbol,
            "contract_id": self.contract_id,
            "exchange": self.exchange,
            "source": self.source,
            "observed_at": self.observed_at,
            "bid": self.bid,
            "ask": self.ask,
            "last": self.last,
            "close": self.close,
            "market_data_type": self.market_data_type,
            "decision_authority": "SUPPORTING_ONLY",
        }

    @property
    def basis_hash(self) -> str:
        return canonical_hash(self.as_dict())


@dataclass(frozen=True, slots=True)
class _OptionabilityRead:
    expirations: tuple[tuple[str, tuple[object, ...]], ...]
    excluded_symbols: tuple[str, ...] = ()
    unresolved_symbols: tuple[str, ...] = ()
    reason_codes: tuple[str, ...] = ()
    exclusion_reasons: tuple[tuple[str, str], ...] = ()
    attempted_symbols: tuple[str, ...] = ()
    deferred_symbols: tuple[str, ...] = ()

    @property
    def complete(self) -> bool:
        return not self.reason_codes and not self.unresolved_symbols

    def as_mapping(self) -> dict[str, tuple[object, ...]]:
        return dict(self.expirations)


@dataclass(frozen=True, slots=True)
class _CoarseCandidateRead:
    candidates: tuple[Mapping[str, object], ...]
    missing_symbols: tuple[str, ...] = ()
    reason_codes: tuple[str, ...] = ()
    excluded_symbols: tuple[str, ...] = ()
    quote_excluded_symbols: tuple[str, ...] = ()
    quote_exclusion_reasons: tuple[tuple[str, str], ...] = ()
    completed_symbols: tuple[str, ...] = ()
    optionability_exclusion_reasons: tuple[tuple[str, str], ...] = ()
    attempted_symbols: tuple[str, ...] = ()
    deferred_symbols: tuple[str, ...] = ()
    coverage_evidence: Mapping[str, object] | None = None


@dataclass(frozen=True, slots=True)
class _StructurePlan:
    structure: str
    legs: tuple[tuple[str, Decimal, str, int], ...]
    thesis_label: str
    uncertainty: Decimal

    @property
    def rights(self) -> tuple[str, ...]:
        return tuple(dict.fromkeys(item[0] for item in self.legs))

    @property
    def strikes(self) -> tuple[Decimal, ...]:
        return tuple(dict.fromkeys(item[1] for item in self.legs))


def _pacing_lease(pacing: object, request_class: str) -> object:
    """Keep a pacing concurrency reservation open for the broker call."""

    lease_factory = getattr(pacing, "lease", None)
    if callable(lease_factory):
        return lease_factory(request_class)
    return nullcontext(pacing.decision(request_class))  # type: ignore[attr-defined]


def _gateway_request_lease(
    gateway: object,
    pacing: object,
    request_class: str,
) -> object:
    """Avoid double charging when the gateway leases each wire request."""

    if bool(getattr(gateway, "market_data_pacing_enabled", False)):
        return nullcontext(
            BudgetDecision(True, request_class, None, 0, 0)
        )
    return _pacing_lease(pacing, request_class)


def _pacing_denial_reasons(prefix: str, decision: object) -> tuple[str, ...]:
    reasons = [f"{prefix}_PACING_DENIED"]
    pacing_reason = str(getattr(decision, "reason", "") or "").strip().upper()
    if pacing_reason:
        reasons.append(f"{prefix}_{pacing_reason}")
    return tuple(reasons)


def _pacing_error_reasons(
    prefix: str,
    error: MarketDataPacingError,
) -> tuple[str, ...]:
    return tuple(
        dict.fromkeys(
            (
                f"{prefix}_PACING_DENIED",
                f"{prefix}_{error.reason_code}",
            )
        )
    )


def _option_qualification_failure_reason(error: BaseException) -> str:
    if isinstance(error, OptionQualificationError):
        return error.reason_code
    if isinstance(error, TimeoutError):
        return "OPTION_QUALIFICATION_OWNER_TIMEOUT"
    if isinstance(error, BrokerConnectionError):
        return "OPTION_QUALIFICATION_BROKER_FAILED"
    return "OPTION_QUALIFICATION_FAILED"


def _preflight_optionable_underlyings(
    gateway: object,
    pacing: object,
    symbols: Sequence[str],
    *,
    asof: date,
    max_attempts: int | None = None,
    max_optionable: int | None = None,
) -> _OptionabilityRead:
    """Resolve a bounded pool while preserving real wire-request headroom."""

    gateway_paced = bool(
        getattr(gateway, "market_data_pacing_enabled", False)
    )
    if max_attempts is None:
        max_attempts = 4 if gateway_paced else 18
    if max_optionable is None:
        # Each production candidate consumes two optionability requests, one
        # underlying qualification, two leg qualifications, one IV-history
        # basis request, and four pre/post atomic-snapshot secdef reads.  Keep
        # the live batch at two and bound discovery to four attempts so the
        # session calendar plus the complete evidence path remains below the
        # approved 30-request rolling minute.
        max_optionable = 2 if gateway_paced else 10

    if max_attempts <= 0 or max_optionable <= 0 or max_optionable > max_attempts:
        raise ValueError("invalid optionability preflight bounds")
    if not isinstance(asof, date) or isinstance(asof, datetime):
        raise TypeError("asof must be a date")

    requested = tuple(
        dict.fromkeys(
            str(symbol).strip().upper()
            for symbol in symbols
            if str(symbol).strip()
        )
    )
    resolved: list[tuple[str, tuple[object, ...]]] = []
    excluded: list[str] = []
    exclusion_reasons: list[tuple[str, str]] = []
    unresolved: list[str] = []
    reasons: list[str] = []
    attempted: list[str] = []
    for index, symbol in enumerate(requested[:max_attempts]):
        if len(resolved) >= max_optionable:
            break
        attempted.append(symbol)
        with _gateway_request_lease(gateway, pacing, "secdef") as decision:
            if not bool(getattr(decision, "allowed", False)):
                unresolved.extend(requested[index:])
                reasons.extend(_pacing_denial_reasons("OPTIONABILITY", decision))
                break
            try:
                expirations = tuple(
                    gateway.option_expirations(  # type: ignore[attr-defined]
                        symbol,
                        min_dte=14,
                        max_dte=35,
                    )
                )
            except MarketDataPacingError as exc:
                unresolved.extend(requested[index:])
                reasons.extend(_pacing_error_reasons("OPTIONABILITY", exc))
                break
            except Exception:
                excluded.append(symbol)
                exclusion_reasons.append((symbol, "OPTIONABILITY_READ_FAILED"))
                continue
        malformed = any(
            not isinstance(getattr(item, "expiration", None), date)
            or isinstance(getattr(item, "expiration", None), datetime)
            for item in expirations
        )
        if malformed:
            excluded.append(symbol)
            exclusion_reasons.append((symbol, "OPTIONABILITY_RESPONSE_INVALID"))
            continue
        eligible = tuple(
            item
            for item in expirations
            if 14 <= (item.expiration - asof).days <= 35
            and int(getattr(item, "multiplier", 0) or 0) == 100
            and bool(tuple(getattr(item, "strikes", ()) or ()))
        )
        if not eligible:
            excluded.append(symbol)
            exclusion_reasons.append(
                (symbol, "OPTIONABILITY_NO_ELIGIBLE_EXPIRATION")
            )
            continue
        resolved.append((symbol, eligible))
    unresolved_set = set(unresolved)
    attempted_set = set(attempted)
    deferred = tuple(
        symbol
        for symbol in requested
        if symbol not in attempted_set and symbol not in unresolved_set
    )
    return _OptionabilityRead(
        expirations=tuple(resolved),
        excluded_symbols=tuple(excluded),
        unresolved_symbols=tuple(dict.fromkeys(unresolved)),
        reason_codes=tuple(dict.fromkeys(reasons)),
        exclusion_reasons=tuple(exclusion_reasons),
        attempted_symbols=tuple(attempted),
        deferred_symbols=deferred,
    )


def _read_underlying_quotes(
    gateway: object,
    pacing: object,
    symbols: Sequence[str],
    *,
    batch_size: int = 4,
    indicative: bool = False,
) -> _UnderlyingQuoteRead:
    """Read bounded stock snapshots without one slow symbol losing the set."""

    rows: list[object] = []
    failed: list[str] = []
    reasons: list[str] = []
    requested = tuple(
        dict.fromkeys(
            str(symbol).strip().upper()
            for symbol in symbols
            if str(symbol).strip()
        )
    )
    def read_batch(batch: tuple[str, ...]) -> None:
        if not batch or reasons:
            return
        batch_failed = False
        returned: tuple[object, ...] = ()
        with _gateway_request_lease(
            gateway,
            pacing,
            "snapshot_quote",
        ) as decision:
            if not bool(getattr(decision, "allowed", False)):
                reasons.append("UNDERLYING_QUOTE_PACING_DENIED")
                pacing_reason = str(getattr(decision, "reason", "") or "").strip()
                if pacing_reason:
                    reasons.append(f"UNDERLYING_QUOTE_{pacing_reason.upper()}")
                return
            try:
                reader = getattr(
                    gateway,
                    (
                        "underlying_indicative_quotes"
                        if indicative
                        else "underlying_quotes"
                    ),
                )
                returned = tuple(
                    reader(batch)
                )
            except MarketDataPacingError as exc:
                reasons.extend(_pacing_error_reasons("UNDERLYING_QUOTE", exc))
                return
            except Exception:
                batch_failed = True
        if batch_failed:
            if len(batch) == 1:
                failed.append(batch[0])
                return
            midpoint = len(batch) // 2
            read_batch(batch[:midpoint])
            read_batch(batch[midpoint:])
            return
        returned_symbols = tuple(
            dict.fromkeys(
                str(getattr(row, "symbol", "")).strip().upper()
                for row in returned
                if str(getattr(row, "symbol", "")).strip()
            )
        )
        if any(symbol not in batch for symbol in returned_symbols):
            reasons.append("UNDERLYING_QUOTE_INVALID_RESPONSE")
            return
        rows.extend(returned)
        unseen = tuple(symbol for symbol in batch if symbol not in returned_symbols)
        if not unseen:
            return
        if len(batch) == 1:
            failed.append(batch[0])
        elif len(unseen) == len(batch):
            midpoint = len(batch) // 2
            read_batch(batch[:midpoint])
            read_batch(batch[midpoint:])
        else:
            read_batch(unseen)

    for offset in range(0, len(requested), batch_size):
        read_batch(requested[offset : offset + batch_size])
        if reasons:
            break
    observed = tuple(
        dict.fromkeys(
            str(getattr(row, "symbol", "")).strip().upper()
            for row in rows
            if str(getattr(row, "symbol", "")).strip()
        )
    )
    missing = tuple(symbol for symbol in requested if symbol not in set(observed))
    if missing and not reasons and set(missing) != set(failed):
        reasons.append("UNDERLYING_QUOTE_BATCH_INCOMPLETE")
    return _UnderlyingQuoteRead(
        rows=tuple(rows),
        requested_symbols=requested,
        observed_symbols=observed,
        missing_symbols=missing,
        reason_codes=tuple(dict.fromkeys(reasons)),
        failed_symbols=tuple(dict.fromkeys(failed)),
    )


def _underlying_capture_completed_at(
    rows: Sequence[object],
    *,
    clock: Callable[[], datetime],
) -> datetime:
    """Bind cache capture to the completed quote read, never the scan slot."""

    completed_at = utc_datetime(
        clock(),
        field="underlying evidence capture completion time",
    )
    observed_values = tuple(
        utc_datetime(
            getattr(row, "observed_at"),
            field="underlying quote observed_at",
        )
        for row in rows
    )
    if not observed_values:
        raise ValueError("underlying evidence capture rows are empty")
    if any(observed_at > completed_at for observed_at in observed_values):
        raise ValueError(
            "underlying quote observed_at is after trusted capture completion"
        )
    # The logical scan slot remains separately bound in the equity-pool row.
    # Broker timestamps are evidence values and can never advance this trusted
    # local post-read boundary.
    return completed_at


def _funnel_trace(
    *,
    scan_run_id: str,
    discovered_underlyings: int,
    deep_scan_requested: int,
    deep_scan_attempted: int,
    deep_scan_completed: int,
    deep_scan_deferred_symbols: Sequence[str] = (),
    pacing: object,
    pacing_usage_snapshot: Mapping[str, object] | None = None,
    underlying_quote_reason_codes: Sequence[str] = (),
    underlying_quote_missing_symbols: Sequence[str] = (),
    optionability_excluded_symbols: Sequence[str] = (),
    optionability_exclusion_reasons: Sequence[tuple[str, str]] = (),
    underlying_quote_excluded_symbols: Sequence[str] = (),
    underlying_quote_exclusion_reasons: Sequence[tuple[str, str]] = (),
    research_allocation: Mapping[str, object] | None = None,
    equity_pool_reference: Mapping[str, object] | None = None,
    equity_theses: Mapping[str, object] | None = None,
    scanner_completed_scan_codes: Sequence[str] = (),
    scanner_failed_scan_codes: Sequence[str] = (),
    scanner_source_row_counts: Sequence[tuple[str, int]] = (),
    scanner_confirmed_charged_requests: int = 0,
) -> dict[str, object]:
    usage = (
        _pacing_usage_snapshot(pacing)
        if pacing_usage_snapshot is None
        else pacing_usage_snapshot
    )
    deferred_symbols = tuple(
        dict.fromkeys(
            str(item).strip().upper()
            for item in deep_scan_deferred_symbols
            if str(item).strip()
        )
    )
    trace: dict[str, object] = {
        "schema": "options_copilot.discovery_funnel_trace.v1",
        "scan_run_id": scan_run_id,
        "discovered_underlyings": discovered_underlyings,
        "deep_scan_requested": deep_scan_requested,
        "deep_scan_attempted": deep_scan_attempted,
        "deep_scan_completed": deep_scan_completed,
        "deep_scan_deferred": len(deferred_symbols),
        "ranked_limit": 10,
        "ranked_count": 0,
        "filler_candidates": 0,
        "pacing_capability_hash": getattr(pacing, "capability_hash", None),
        "pacing_usage": usage,
    }
    if deferred_symbols:
        trace["deep_scan_deferred_symbols"] = deferred_symbols
    if (
        scanner_completed_scan_codes
        or scanner_failed_scan_codes
        or scanner_source_row_counts
    ):
        trace["scanner_confirmed_charged_requests"] = (
            scanner_confirmed_charged_requests
        )
        trace["scanner_completed_scan_codes"] = tuple(
            dict.fromkeys(
                str(item).strip().upper()
                for item in scanner_completed_scan_codes
            )
        )
        trace["scanner_failed_scan_codes"] = tuple(
            dict.fromkeys(
                str(item).strip().upper()
                for item in scanner_failed_scan_codes
            )
        )
        trace["scanner_source_row_counts"] = tuple(
            {
                "scan_code": str(scan_code).strip().upper(),
                "row_count": row_count,
            }
            for scan_code, row_count in scanner_source_row_counts
        )
    if underlying_quote_reason_codes:
        trace["underlying_quote_reason_codes"] = tuple(
            dict.fromkeys(str(item) for item in underlying_quote_reason_codes)
        )
    if underlying_quote_missing_symbols:
        trace["underlying_quote_missing_symbols"] = tuple(
            dict.fromkeys(str(item) for item in underlying_quote_missing_symbols)
        )
    if optionability_excluded_symbols:
        trace["optionability_excluded_symbols"] = tuple(
            dict.fromkeys(str(item) for item in optionability_excluded_symbols)
        )
    if optionability_exclusion_reasons:
        trace["optionability_exclusion_reasons"] = tuple(
            {
                "symbol": str(symbol).strip().upper(),
                "reason_code": str(reason).strip().upper(),
            }
            for symbol, reason in dict.fromkeys(
                (
                    str(symbol).strip().upper(),
                    str(reason).strip().upper(),
                )
                for symbol, reason in optionability_exclusion_reasons
                if str(symbol).strip() and str(reason).strip()
            )
        )
    if underlying_quote_excluded_symbols:
        trace["underlying_quote_excluded_symbols"] = tuple(
            dict.fromkeys(str(item) for item in underlying_quote_excluded_symbols)
        )
    if underlying_quote_exclusion_reasons:
        trace["underlying_quote_exclusion_reasons"] = tuple(
            {
                "symbol": str(symbol).strip().upper(),
                "reason_code": str(reason).strip().upper(),
            }
            for symbol, reason in dict.fromkeys(
                (
                    str(symbol).strip().upper(),
                    str(reason).strip().upper(),
                )
                for symbol, reason in underlying_quote_exclusion_reasons
                if str(symbol).strip() and str(reason).strip()
            )
        )
    if research_allocation is not None:
        trace["research_allocation"] = dict(research_allocation)
    if equity_pool_reference is not None:
        trace["equity_pool_reference"] = dict(equity_pool_reference)
    if equity_theses is not None:
        trace["equity_theses"] = dict(equity_theses)
    return trace


def equity_theses_from_pool_result(
    pool_result: object,
    *,
    equity_pool_reference: Mapping[str, object],
) -> dict[str, object]:
    stored = getattr(pool_result, "stored", None)
    snapshot = getattr(stored, "snapshot", None)
    selected = tuple(getattr(snapshot, "selected", ()))
    rows: list[Mapping[str, object]] = []
    for decision in selected:
        score = getattr(decision, "score", None)
        symbol = str(getattr(decision, "symbol", "")).strip().upper()
        observed_at = getattr(snapshot, "slot", None)
        canonical_input_hash = getattr(decision, "canonical_input_hash", None)
        source_hashes = (str(canonical_input_hash),)
        if (
            not symbol
            or score is None
            or not isinstance(observed_at, datetime)
            or not isinstance(canonical_input_hash, str)
            or len(canonical_input_hash) != 64
        ):
            raise ValueError("equity thesis projection is incomplete")
        body = {
            "schema": "options_copilot.equity_thesis_evidence.v1",
            "symbol": symbol,
            "direction_label": str(getattr(getattr(score, "direction_label", None), "value", "")),
            "direction_score": getattr(score, "direction_score", None),
            "uncertainty": getattr(score, "uncertainty", None),
            "observed_at": observed_at,
            "source_hashes": source_hashes,
            "canonical_input_hash": canonical_input_hash,
            "selected_rank": getattr(decision, "selected_rank", None),
        }
        rows.append({**body, "thesis_hash": canonical_hash(body)})
    ordered = tuple(sorted(rows, key=lambda item: str(item["symbol"])))
    return {
        "schema": "options_copilot.equity_theses.v1",
        "equity_pool_reference_hash": canonical_hash(equity_pool_reference),
        "rows": ordered,
        "rows_hash": canonical_hash(
            tuple({key: value for key, value in row.items() if key != "thesis_hash"} for row in ordered)
        ),
    }


def _equity_pool_projection(
    pool_result: object,
) -> tuple[
    tuple[str, ...],
    tuple[str, ...],
    Mapping[str, object] | None,
    Mapping[str, object] | None,
    Mapping[str, object] | None,
]:
    """Project one pool result consistently before and after evidence capture."""

    selected_symbols = tuple(
        str(item).strip().upper()
        for item in getattr(pool_result, "selected_symbols", ())
        if str(item).strip()
    )
    acquisition_targets = tuple(
        str(item).strip().upper()
        for item in getattr(pool_result, "acquisition_targets", ())
        if str(item).strip()
    )
    evidence: Mapping[str, object] | None = None
    reference: Mapping[str, object] | None = None
    theses: Mapping[str, object] | None = None
    result_body = getattr(pool_result, "as_dict", None)
    if callable(result_body):
        raw_evidence = result_body()
        if isinstance(raw_evidence, Mapping):
            evidence = dict(raw_evidence)
            raw_reference = raw_evidence.get("equity_pool_reference")
            if isinstance(raw_reference, Mapping):
                reference = dict(raw_reference)
                theses = equity_theses_from_pool_result(
                    pool_result,
                    equity_pool_reference=reference,
                )
                selected_symbols, deep_scan_exclusions = (
                    _structure_eligible_thesis_symbols(
                        selected_symbols,
                        theses,
                    )
                )
                evidence["deep_scan_symbols"] = selected_symbols
                evidence["deep_scan_count"] = len(selected_symbols)
                evidence["deep_scan_exclusions"] = tuple(
                    {
                        "symbol": symbol,
                        "reason_code": reason,
                    }
                    for symbol, reason in deep_scan_exclusions
                )
    return selected_symbols, acquisition_targets, evidence, reference, theses


def _secondary_equity_evidence_targets(
    deep_scan_symbols: Sequence[str],
    acquisition_targets: Sequence[str],
) -> tuple[str, ...]:
    """Keep unrelated research enrichment off the executable scan path."""

    if deep_scan_symbols:
        return ()
    return tuple(
        dict.fromkeys(
            str(symbol).strip().upper()
            for symbol in acquisition_targets
            if str(symbol).strip()
        )
    )


def _equity_pool_deep_scan_reason_codes(
    evidence: Mapping[str, object] | None,
) -> tuple[str, ...]:
    """Expose formal thesis-routing exclusions as scan decision reasons."""

    if not isinstance(evidence, Mapping):
        return ()
    raw_exclusions = evidence.get("deep_scan_exclusions", ())
    if not isinstance(raw_exclusions, Sequence) or isinstance(
        raw_exclusions,
        (str, bytes, bytearray),
    ):
        return ()
    reasons = (
        str(item.get("reason_code", "")).strip()
        for item in raw_exclusions
        if isinstance(item, Mapping)
    )
    return tuple(dict.fromkeys(reason for reason in reasons if reason))


def _structure_eligible_thesis_symbols(
    selected_symbols: Sequence[str],
    equity_theses: Mapping[str, object],
) -> tuple[tuple[str, ...], tuple[tuple[str, str], ...]]:
    """Route only theses that can reach the closed structure registry."""

    rows = equity_theses.get("rows", ())
    thesis_by_symbol = {
        str(row.get("symbol", "")).strip().upper(): row
        for row in rows
        if isinstance(row, Mapping)
        and str(row.get("symbol", "")).strip()
    } if isinstance(rows, Sequence) and not isinstance(
        rows,
        (str, bytes, bytearray),
    ) else {}
    eligible: list[str] = []
    excluded: list[tuple[str, str]] = []
    for raw_symbol in selected_symbols:
        symbol = str(raw_symbol).strip().upper()
        thesis = thesis_by_symbol.get(symbol)
        if thesis is None:
            excluded.append((symbol, "EQUITY_THESIS_EVIDENCE_UNAVAILABLE"))
            continue
        try:
            uncertainty = Decimal(str(thesis["uncertainty"]))
        except (KeyError, InvalidOperation, TypeError, ValueError):
            excluded.append((symbol, "EQUITY_THESIS_EVIDENCE_INVALID"))
            continue
        if (
            not uncertainty.is_finite()
            or uncertainty < 0
            or uncertainty > 1
        ):
            excluded.append((symbol, "EQUITY_THESIS_EVIDENCE_INVALID"))
            continue
        if uncertainty > _STRUCTURE_UNCERTAINTY_LIMIT:
            excluded.append(
                (symbol, "EQUITY_THESIS_UNCERTAINTY_ABOVE_STRUCTURE_LIMIT")
            )
            continue
        direction = str(thesis.get("direction_label", "")).strip().upper()
        if direction not in _SUPPORTED_STRUCTURE_DIRECTIONS:
            excluded.append((symbol, "EQUITY_THESIS_DIRECTION_UNSUPPORTED"))
            continue
        eligible.append(symbol)
    return tuple(eligible), tuple(excluded)


def _research_allocation_evidence(
    *,
    event_rows: Sequence[Mapping[str, object]],
    scanner_rows: Sequence[Mapping[str, object]],
    core_rows: Sequence[Mapping[str, object]],
    selected_symbols: Sequence[str],
    limit: int,
) -> dict[str, object]:
    """Build replayable evidence and reject any caller-selected drift."""

    evidence = build_research_allocation_evidence(
        event_rows=event_rows,
        scanner_rows=scanner_rows,
        core_rows=core_rows,
        limit=limit,
    )
    selected = tuple(evidence["selected_symbols"])
    supplied = tuple(selected_symbols)
    if supplied != selected:
        raise ValueError(
            "research allocation selected_symbols must match replayed producer output"
        )
    return evidence


class ProductionPipelineInputs:
    """Acquire bounded research seeds without manufacturing missing evidence."""

    def __init__(
        self,
        gateway: IBKRReadOnlyGateway,
        pacing: GuardedRequestBudget,
        position_manager: object,
        *,
        core_symbols: Sequence[str],
        event_pool_reader: Callable[[], object] | None = None,
        fundamentals_reader: Callable[[str, datetime], object] | None = None,
        equity_pool_builder: Callable[..., object] | None = None,
        equity_evidence_capture: Callable[[Sequence[object], datetime], object] | None = None,
        execution_cost_contract: Mapping[str, object] | None = None,
        ordinary_coverage: OrdinaryScanCoverage | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.gateway = gateway
        self.pacing = pacing
        self.position_manager = position_manager
        self.core_symbols = tuple(dict.fromkeys(str(item).strip().upper() for item in core_symbols))
        self._event_pool_reader = event_pool_reader
        self._fundamentals_reader = fundamentals_reader
        self._equity_pool_builder = equity_pool_builder
        self._equity_evidence_capture = equity_evidence_capture
        frozen_cost_contract = (
            None
            if execution_cost_contract is None
            else freeze_json(execution_cost_contract)
        )
        if frozen_cost_contract is not None and not isinstance(
            frozen_cost_contract,
            Mapping,
        ):
            raise TypeError("execution_cost_contract must be a mapping")
        self._execution_cost_contract = frozen_cost_contract
        self._ordinary_coverage = ordinary_coverage
        self._management_refresher: Callable[[], object] | None = None
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._reader_lock = threading.RLock()
        self._manual_state = threading.local()
        self._manual_core_cursor = 0
        self._manual_trading_date: date | None = None

    @contextmanager
    def manual_core_only(self):
        """Suppress new scanner subscriptions for one operator-requested run."""

        previous = bool(getattr(self._manual_state, "core_only", False))
        previous_offset = getattr(self._manual_state, "core_offset", None)
        manual_trading_date = utc_datetime(
            self._clock(),
            field="manual scan clock",
        ).astimezone(ZoneInfo("America/New_York")).date()
        with self._reader_lock:
            if self._manual_trading_date != manual_trading_date:
                self._manual_core_cursor = 0
                self._manual_trading_date = manual_trading_date
            core_count = len(self.core_symbols)
            # IBKR option subscriptions commonly need one bounded request to
            # warm a newly selected contract pair.  Keep two consecutive
            # manual attempts on the same core starting point: warm, then
            # evaluate, before rotating to the next symbol.
            attempt_index = self._manual_core_cursor
            offset = (
                (attempt_index // 2) % core_count
                if core_count
                else 0
            )
            self._manual_core_cursor += 1
            target_symbol = self.core_symbols[offset] if core_count else None
            next_offset = (
                ((attempt_index + 1) // 2) % core_count
                if core_count
                else 0
            )
            scope = {
                "target_symbol": target_symbol,
                "attempt_number": (attempt_index % 2) + 1,
                "attempt_kind": (
                    "WARMUP" if attempt_index % 2 == 0 else "REEVALUATION"
                ),
                "core_index": offset,
                "core_count": core_count,
                "cycle": (attempt_index // max(1, core_count * 2)) + 1,
                "next_symbol": (
                    self.core_symbols[next_offset] if core_count else None
                ),
            }
        self._manual_state.core_only = True
        self._manual_state.core_offset = offset
        try:
            yield scope
        finally:
            self._manual_state.core_only = previous
            if previous_offset is None:
                try:
                    del self._manual_state.core_offset
                except AttributeError:
                    pass
            else:
                self._manual_state.core_offset = previous_offset

    def retry_manual_core_attempt(self, scope: Mapping[str, object]) -> bool:
        """Retain the current manual symbol when no durable scan was produced."""

        def integer(name: str) -> int | None:
            value = scope.get(name)
            if isinstance(value, bool) or not isinstance(value, int):
                return None
            return value

        core_count = integer("core_count")
        core_index = integer("core_index")
        attempt_number = integer("attempt_number")
        cycle = integer("cycle")
        if (
            core_count != len(self.core_symbols)
            or core_count is None
            or core_count <= 0
            or core_index is None
            or not 0 <= core_index < core_count
            or attempt_number not in {1, 2}
            or cycle is None
            or cycle <= 0
        ):
            return False
        expected_symbol = self.core_symbols[core_index]
        if str(scope.get("target_symbol") or "").strip().upper() != expected_symbol:
            return False
        attempt_index = (
            (cycle - 1) * core_count * 2
            + core_index * 2
            + attempt_number
            - 1
        )
        manual_trading_date = utc_datetime(
            self._clock(),
            field="manual scan retry clock",
        ).astimezone(ZoneInfo("America/New_York")).date()
        with self._reader_lock:
            if (
                self._manual_trading_date != manual_trading_date
                or self._manual_core_cursor != attempt_index + 1
            ):
                return False
            self._manual_core_cursor = attempt_index
            return True

    def bind_event_pool_reader(self, reader: Callable[[], object]) -> None:
        if not callable(reader):
            raise TypeError("event_pool_reader must be callable")
        with self._reader_lock:
            self._event_pool_reader = reader

    def bind_fundamentals_reader(
        self,
        reader: Callable[[str, datetime], object],
    ) -> None:
        """Bind optional point-in-time evidence with supporting-only authority."""

        if not callable(reader):
            raise TypeError("fundamentals_reader must be callable")
        with self._reader_lock:
            self._fundamentals_reader = reader

    def bind_management_refresher(self, refresher: Callable[[], object]) -> None:
        """Bind the review-only position preview refreshed at research slots."""

        if not callable(refresher):
            raise TypeError("management_refresher must be callable")
        with self._reader_lock:
            self._management_refresher = refresher

    def bind_equity_pool_builder(self, builder: Callable[..., object]) -> None:
        if not callable(builder):
            raise TypeError("equity_pool_builder must be callable")
        with self._reader_lock:
            self._equity_pool_builder = builder

    def bind_equity_evidence_capture(
        self,
        capture: Callable[[Sequence[object], datetime], object],
    ) -> None:
        if not callable(capture):
            raise TypeError("equity_evidence_capture must be callable")
        with self._reader_lock:
            self._equity_evidence_capture = capture

    def run(self, *, scan_run_id: str, slot_at: datetime) -> Mapping[str, object]:
        checked_at = utc_datetime(self._clock(), field="pipeline input clock")
        slot = utc_datetime(slot_at, field="slot_at")
        base_hash = canonical_hash(
            {
                "scan_run_id": scan_run_id,
                "slot_at": slot,
                "observed_at": checked_at,
                "pacing_capability_hash": self.pacing.capability_hash,
            }
        )
        if not self.pacing.ready:
            return self._empty(
                base_hash,
                (PACING_CAPABILITY_MISSING,),
                scan_run_id=scan_run_id,
            )
        # This is deliberately the first broker read on every entry-research
        # path.  A non-flat derivative account returns before orders,
        # instructions, chain discovery, qualification, secdef, or quotes.
        try:
            raw_positions = self.gateway.positions()
        except Exception:
            return self._empty(
                base_hash,
                ("BROKER_POSITIONS_UNAVAILABLE",),
                scan_run_id=scan_run_id,
            )
        validated_positions, positions_invalid = validate_entry_positions(
            raw_positions
        )
        if positions_invalid or validated_positions is None:
            return self._empty(
                base_hash,
                ("POSITIONS_UNKNOWN_OR_INVALID",),
                scan_run_id=scan_run_id,
            )
        positions = tuple(_mapping(item) for item in raw_positions)  # type: ignore[arg-type]
        if any(
            security_type in _DERIVATIVE_SECURITY_TYPES and quantity != 0
            for _, security_type, quantity in validated_positions
        ):
            # An open derivative must stop entry research, but it is exactly
            # when the read-only management coordinator needs to refresh the
            # current combination.  Refresh before returning so the GUI does
            # not remain on its startup placeholder until the account is flat.
            equity_research = self._refresh_position_mode_equity_research(slot)
            management_reasons = self._refresh_management()
            return self._empty(
                base_hash,
                tuple(
                    dict.fromkeys(
                        (
                            "POSITION_MANAGEMENT_ONLY",
                            *equity_research.reason_codes,
                            *management_reasons,
                        )
                    )
                ),
                positions=positions,
                status="POSITION_MANAGEMENT_ONLY",
                scan_run_id=scan_run_id,
                discovery=equity_research.discovery,
            )
        execution_state_reasons = self._execution_state_reasons()
        if execution_state_reasons:
            return self._empty(
                base_hash,
                execution_state_reasons,
                scan_run_id=scan_run_id,
            )
        management_reasons = self._refresh_management()
        if management_reasons:
            return self._empty(
                base_hash,
                management_reasons,
                scan_run_id=scan_run_id,
            )

        if bool(getattr(self._manual_state, "core_only", False)):
            discovery = _UnderlyingDiscoveryRead((), (), (), ())
            pacing_usage_snapshot = _pacing_usage_snapshot(self.pacing)
        else:
            try:
                discovery = _discover_underlyings(self.gateway, self.pacing)
            except _PacingUsageSnapshotError as exc:
                return self._empty(
                    base_hash,
                    (exc.reason_code,),
                    scan_run_id=scan_run_id,
                    pacing_usage_snapshot=_empty_pacing_usage_snapshot(),
                )
            except MarketDataPacingError as exc:
                return self._empty(
                    base_hash,
                    _pacing_error_reasons("IBKR_SCANNER", exc),
                    scan_run_id=scan_run_id,
                )
            pacing_usage_snapshot = discovery.pacing_usage
            if pacing_usage_snapshot is None:
                pacing_usage_snapshot = _pacing_usage_snapshot(self.pacing)
        scanner_rows = tuple(
            _scanner_result_row(item) for item in discovery.rows
        )
        event_snapshot = self._event_snapshot()
        try:
            event_rows = self._event_rows(event_snapshot)
        except ResearchAllocationInputError:
            return self._empty(
                base_hash,
                (RESEARCH_ALLOCATION_INPUT_INVALID,),
                positions=positions,
                scan_run_id=scan_run_id,
                discovery=discovery,
                pacing_usage_snapshot=pacing_usage_snapshot,
            )
        event_discovery_rows = (
            ()
            if bool(getattr(self._manual_state, "core_only", False))
            else _verified_news_discovery_rows(event_snapshot)
        )
        core_symbols = self.core_symbols
        manual_core_only = bool(getattr(self._manual_state, "core_only", False))
        if manual_core_only and core_symbols:
            core_offset = int(getattr(self._manual_state, "core_offset", 0))
            core_symbols = (
                core_symbols[core_offset:] + core_symbols[:core_offset]
            )
        core_rows = tuple(
            {
                "symbol": symbol,
                "score": Decimal(60 - index),
                "source": "CORE_UNIVERSE",
            }
            for index, symbol in enumerate(core_symbols[:40])
        )
        core_pool_rows = tuple(
            {
                "symbol": symbol,
                "rank": index,
                "score": Decimal(60 - index),
                "source_scan": "CORE_UNIVERSE",
                "contract_id": None,
                "exchange": None,
                "industry": None,
                "category": None,
                "subcategory": None,
            }
            for index, symbol in enumerate(core_symbols[:40])
        )
        # A manual campaign deliberately researches one rotating core symbol.
        # Ordinary scans retain the stable core beside event and scanner rows;
        # the equity-pool allocator still owns all eligibility decisions.
        equity_pool_scanner_rows = (
            core_pool_rows[:1]
            if manual_core_only
            else (*event_discovery_rows, *scanner_rows, *core_pool_rows)
        )
        equity_pool_builder = self._equity_pool_builder
        proposed_deep_scan_symbols: tuple[str, ...] = ()
        equity_pool_evidence: Mapping[str, object] | None = None
        equity_pool_reference: Mapping[str, object] | None = None
        equity_theses: Mapping[str, object] | None = None
        acquisition_targets: tuple[str, ...] = ()
        if equity_pool_builder is not None:
            try:
                pool_result = equity_pool_builder(
                    scanner_rows=equity_pool_scanner_rows,
                    slot=slot,
                    pacing_usage=pacing_usage_snapshot,
                )
                (
                    proposed_deep_scan_symbols,
                    acquisition_targets,
                    equity_pool_evidence,
                    equity_pool_reference,
                    equity_theses,
                ) = _equity_pool_projection(pool_result)
            except Exception:
                return self._empty(
                    base_hash,
                    ("EQUITY_POOL_UNAVAILABLE",),
                    positions=positions,
                    scan_run_id=scan_run_id,
                    discovery=discovery,
                    pacing_usage_snapshot=pacing_usage_snapshot,
                )
        evidence_capture_targets = _secondary_equity_evidence_targets(
            proposed_deep_scan_symbols,
            acquisition_targets,
        )
        if (
            equity_pool_builder is not None
            and evidence_capture_targets
            and self._equity_evidence_capture is not None
        ):
            captured = _read_underlying_quotes(
                self.gateway,
                self.pacing,
                evidence_capture_targets,
                batch_size=4,
            )
            if captured.rows:
                try:
                    capture_completed_at = _underlying_capture_completed_at(
                        captured.rows,
                        clock=self._clock,
                    )
                    self._equity_evidence_capture(
                        captured.rows,
                        capture_completed_at,
                    )
                except Exception:
                    return self._empty(
                        base_hash,
                        ("EQUITY_EVIDENCE_CACHE_UNAVAILABLE",),
                        positions=positions,
                        scan_run_id=scan_run_id,
                        discovery=discovery,
                        pacing_usage_snapshot=pacing_usage_snapshot,
                    )
                if captured.complete:
                    try:
                        refreshed_pool_result = equity_pool_builder(
                            scanner_rows=equity_pool_scanner_rows,
                            slot=capture_completed_at,
                            pacing_usage=pacing_usage_snapshot,
                        )
                        (
                            proposed_deep_scan_symbols,
                            acquisition_targets,
                            equity_pool_evidence,
                            equity_pool_reference,
                            equity_theses,
                        ) = _equity_pool_projection(refreshed_pool_result)
                    except Exception:
                        return self._empty(
                            base_hash,
                            ("EQUITY_POOL_UNAVAILABLE",),
                            positions=positions,
                            scan_run_id=scan_run_id,
                            discovery=discovery,
                            pacing_usage_snapshot=pacing_usage_snapshot,
                        )

        try:
            if equity_pool_builder is None:
                proposed_deep_scan_symbols = _balanced_discovery_symbols(
                    (*event_rows, *scanner_rows), core_rows, limit=30,
                )
                research_allocation = _research_allocation_evidence(
                    event_rows=event_rows,
                    scanner_rows=scanner_rows,
                    core_rows=core_rows,
                    selected_symbols=proposed_deep_scan_symbols,
                    limit=30,
                )
            else:
                if equity_pool_evidence is None or equity_pool_reference is None:
                    raise ResearchAllocationInputError("equity pool evidence unavailable")
                research_allocation = None
        except ResearchAllocationInputError:
            return self._empty(
                base_hash,
                (RESEARCH_ALLOCATION_INPUT_INVALID,),
                positions=positions,
                scan_run_id=scan_run_id,
                discovery=discovery,
                pacing_usage_snapshot=pacing_usage_snapshot,
            )
        deep_scan_symbols = (
            proposed_deep_scan_symbols
            if research_allocation is None
            else tuple(research_allocation["selected_symbols"])
        )
        coarse_read = self._coarse_candidate_outcome(
            scan_run_id=scan_run_id,
            slot_at=slot,
            symbols=deep_scan_symbols,
            event_snapshot=event_snapshot,
            equity_theses=equity_theses,
        )
        coarse = coarse_read.candidates
        recorded_completed_symbols = tuple(
            getattr(coarse_read, "completed_symbols", ()) or ()
        )
        recorded_attempted_symbols = tuple(
            getattr(coarse_read, "attempted_symbols", ()) or ()
        )
        recorded_deferred_symbols = tuple(
            getattr(coarse_read, "deferred_symbols", ()) or ()
        )
        coarse_completed_symbols = tuple(
            dict.fromkeys(
                (
                    *recorded_completed_symbols,
                    *(
                        str(row.get("symbol", "")).strip().upper()
                        for row in coarse
                        if isinstance(row, Mapping)
                        and str(row.get("symbol", "")).strip()
                    ),
                )
            )
        )
        funnel_trace = _funnel_trace(
            scan_run_id=scan_run_id,
            discovered_underlyings=len(scanner_rows),
            deep_scan_requested=len(deep_scan_symbols),
            deep_scan_attempted=max(
                len(recorded_attempted_symbols),
                len(coarse_completed_symbols),
            ),
            deep_scan_completed=len(coarse_completed_symbols),
            deep_scan_deferred_symbols=recorded_deferred_symbols,
            pacing=self.pacing,
            pacing_usage_snapshot=pacing_usage_snapshot,
            underlying_quote_reason_codes=coarse_read.reason_codes,
            underlying_quote_missing_symbols=coarse_read.missing_symbols,
            optionability_excluded_symbols=coarse_read.excluded_symbols,
            optionability_exclusion_reasons=(
                tuple(
                    getattr(
                        coarse_read,
                        "optionability_exclusion_reasons",
                        (),
                    )
                    or ()
                )
            ),
            underlying_quote_excluded_symbols=coarse_read.quote_excluded_symbols,
            underlying_quote_exclusion_reasons=(
                coarse_read.quote_exclusion_reasons
            ),
            research_allocation=research_allocation,
            equity_pool_reference=equity_pool_reference,
            equity_theses=equity_theses,
            scanner_completed_scan_codes=discovery.completed_scan_codes,
            scanner_failed_scan_codes=discovery.failed_scan_codes,
            scanner_source_row_counts=discovery.source_row_counts,
            scanner_confirmed_charged_requests=(
                discovery.confirmed_charged_scanner_requests
            ),
        )
        equity_routing_reasons = _equity_pool_deep_scan_reason_codes(
            equity_pool_evidence
        )
        coverage_evidence = getattr(coarse_read, "coverage_evidence", None)
        if coverage_evidence is not None:
            funnel_trace["ordinary_coverage"] = coverage_evidence
        pipeline_reasons = tuple(
            dict.fromkeys(
                (
                    *discovery.reason_codes,
                    *equity_routing_reasons,
                    *coarse_read.reason_codes,
                )
            )
        )
        payload = {
            "input_hash": base_hash,
            "positions": positions,
            "candidate_evidence_references": {},
            "funnel_trace": funnel_trace,
            "universe": {
                "positions": positions,
                "core_etfs": core_rows,
                "event_pool": event_rows,
                "scanner": scanner_rows,
                "equity_acquisition_targets": acquisition_targets,
                "coarse_contracts": coarse,
                "signed_dte_exception": False,
                "funnel_trace": funnel_trace,
            },
            "reasons": (
                pipeline_reasons
                if pipeline_reasons
                else (() if coarse else ("NO_COARSE_OPTION_CANDIDATES",))
            ),
        }
        if pipeline_reasons:
            payload["status"] = "NO_TRADE"
            payload["decision"] = "NO_TRADE"
        return payload

    def _refresh_position_mode_equity_research(
        self,
        slot: datetime,
    ) -> _PositionModeEquityResearchRead:
        """Refresh stock-only research while option entry remains blocked."""

        discovery = _UnderlyingDiscoveryRead((), (), (), ())
        if self._equity_pool_builder is None:
            return _PositionModeEquityResearchRead((), discovery)
        try:
            discovery = _discover_underlyings(self.gateway, self.pacing)
            scanner_rows = tuple(
                _scanner_result_row(item) for item in discovery.rows
            )
            result = self._equity_pool_builder(
                scanner_rows=scanner_rows,
                slot=slot,
                pacing_usage=(
                    discovery.pacing_usage
                    if discovery.pacing_usage is not None
                    else _pacing_usage_snapshot(self.pacing)
                ),
                position_mode="BLOCKED_OPEN_POSITION",
            )
            targets = tuple(getattr(result, "acquisition_targets", ()))
            if targets and self._equity_evidence_capture is not None:
                captured = _read_underlying_quotes(
                    self.gateway, self.pacing, targets, batch_size=4,
                )
                if captured.rows:
                    self._equity_evidence_capture(
                        captured.rows,
                        _underlying_capture_completed_at(
                            captured.rows,
                            clock=self._clock,
                        ),
                    )
            return _PositionModeEquityResearchRead(
                discovery.reason_codes,
                discovery,
            )
        except _PacingUsageSnapshotError as exc:
            return _PositionModeEquityResearchRead(
                (exc.reason_code,),
                _UnderlyingDiscoveryRead(
                    (),
                    (),
                    (),
                    (),
                    pacing_usage=_empty_pacing_usage_snapshot(),
                ),
            )
        except Exception:
            return _PositionModeEquityResearchRead(
                ("EQUITY_RESEARCH_REFRESH_UNAVAILABLE",),
                discovery,
            )

    load = run
    acquire = run

    def _empty(
        self,
        input_hash: str,
        reasons: tuple[str, ...],
        *,
        positions: tuple[Mapping[str, object], ...] = (),
        status: str = "NO_TRADE",
        scan_run_id: str | None = None,
        discovery: _UnderlyingDiscoveryRead | None = None,
        pacing_usage_snapshot: Mapping[str, object] | None = None,
    ) -> Mapping[str, object]:
        discovery_read = discovery or _UnderlyingDiscoveryRead((), (), (), ())
        funnel_trace = _funnel_trace(
            scan_run_id=scan_run_id or "UNAVAILABLE",
            discovered_underlyings=len(discovery_read.rows),
            deep_scan_requested=0,
            deep_scan_attempted=0,
            deep_scan_completed=0,
            pacing=self.pacing,
            pacing_usage_snapshot=(
                discovery_read.pacing_usage
                if pacing_usage_snapshot is None
                else pacing_usage_snapshot
            ),
            scanner_completed_scan_codes=(
                discovery_read.completed_scan_codes
            ),
            scanner_failed_scan_codes=discovery_read.failed_scan_codes,
            scanner_source_row_counts=discovery_read.source_row_counts,
            scanner_confirmed_charged_requests=(
                discovery_read.confirmed_charged_scanner_requests
            ),
        )
        return {
            "status": status,
            "decision": "NO_TRADE",
            "input_hash": input_hash,
            "positions": positions,
            "candidate_evidence_references": {},
            "funnel_trace": funnel_trace,
            "universe": {
                "positions": positions,
                "core_etfs": (),
                "event_pool": (),
                "scanner": (),
                "coarse_contracts": (),
                "signed_dte_exception": False,
                "funnel_trace": funnel_trace,
            },
            "reasons": reasons,
        }

    def _event_snapshot(self) -> Mapping[str, object]:
        with self._reader_lock:
            reader = self._event_pool_reader
        if reader is None:
            return {}
        try:
            payload = reader()
        except Exception:
            return {}
        if not isinstance(payload, Mapping):
            return {}
        return dict(payload)

    def _event_rows(
        self,
        payload: Mapping[str, object],
    ) -> tuple[Mapping[str, object], ...]:
        raw = payload.get("news", ())
        if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes, bytearray)):
            return ()
        rows: list[Mapping[str, object]] = []
        for item in raw:
            if not isinstance(item, Mapping):
                continue
            symbols = item.get("symbols", ())
            if isinstance(symbols, str):
                symbols = (symbols,)
            if not isinstance(symbols, Sequence):
                continue
            canonical_symbols = tuple(
                text
                for symbol in symbols
                if (text := canonical_research_symbol(symbol)) is not None
            )
            research_proxy = None
            if not canonical_symbols:
                raw_proxy = item.get("research_proxy_binding")
                if isinstance(raw_proxy, Mapping):
                    proxy_symbol = canonical_research_symbol(
                        raw_proxy.get("proxy_symbol")
                    )
                    if proxy_symbol is not None:
                        try:
                            research_proxy = require_current_research_proxy_binding(
                                raw_proxy,
                                symbol=proxy_symbol,
                            )
                        except (TypeError, ValueError):
                            research_proxy = None
                if research_proxy is not None:
                    canonical_symbols = (research_proxy.proxy_symbol,)
            # Ordinary unbound news remains chronology-only. A macro row may
            # participate solely through a current hash-bound research proxy;
            # it never becomes an issuer binding or changes downstream Gates.
            # Scoreless rows still cannot poison the entire equity pool.
            if not canonical_symbols:
                continue
            deterministic_score = canonical_research_score(
                item.get("event_impact_score")
            )
            if deterministic_score is None:
                raise ResearchAllocationInputError(
                    f"{RESEARCH_ALLOCATION_INPUT_INVALID}: "
                    "invalid event_impact_score"
                )
            advisory_score: Decimal | None = None
            selected_score = deterministic_score
            selected_source = (
                "MACRO_RESEARCH_PROXY_SUPPORTING_ONLY"
                if research_proxy is not None
                else "NEWS_SUPPORTING_ONLY"
            )
            advisory = item.get("research_advisory")
            if (
                isinstance(advisory, Mapping)
                and advisory.get("decision_authority") == "SUPPORTING_ONLY"
                and advisory.get("approval_eligible") is False
                and advisory.get("instruction_creation_allowed") is False
                and advisory.get("order_allowed") is False
            ):
                candidate_advisory_score = canonical_research_score(
                    advisory.get("research_priority_score")
                )
                if (
                    "research_priority_score" in advisory
                    and candidate_advisory_score is None
                ):
                    raise ResearchAllocationInputError(
                        f"{RESEARCH_ALLOCATION_INPUT_INVALID}: "
                        "invalid research_priority_score"
                    )
                if candidate_advisory_score is not None:
                    # This changes only which symbols receive scarce deep-scan
                    # research first. Contract identity, quotes, payoff, risk,
                    # eligibility, approvals, and instructions remain locally
                    # deterministic and broker-bound downstream.
                    advisory_score = candidate_advisory_score
                    if candidate_advisory_score > deterministic_score:
                        selected_score = candidate_advisory_score
                        selected_source = (
                            "NEWS_SHADOW_PRIORITY_SUPPORTING_ONLY"
                        )
            for text in canonical_symbols:
                rows.append(
                    {
                        "symbol": text,
                        "score": selected_score,
                        "source": selected_source,
                        "deterministic_score": deterministic_score,
                        "advisory_score": advisory_score,
                        "selected_research_priority_score": selected_score,
                        "selected_research_priority_source": selected_source,
                        "influence_scope": "RESEARCH_SCHEDULING_HINT_ONLY",
                        "decision_authority": "SUPPORTING_ONLY",
                        "eligibility_effect": "NONE",
                        "risk_effect": "NONE",
                        "approval_eligible": False,
                        "instruction_creation_allowed": False,
                        "order_allowed": False,
                    }
                )
        return tuple(rows[:50])

    def _refresh_management(self) -> tuple[str, ...]:
        with self._reader_lock:
            refresher = self._management_refresher
        if refresher is None:
            return ()
        try:
            result = refresher()
        except Exception:
            return ("MANAGEMENT_REFRESH_FAILED",)
        status = str(getattr(result, "status", "")).strip().upper()
        raw_reasons = getattr(result, "reason_codes", ())
        reasons = (
            tuple(
                dict.fromkeys(
                    str(item).strip().upper()
                    for item in raw_reasons
                    if str(item).strip()
                )
            )
            if isinstance(raw_reasons, Sequence)
            and not isinstance(raw_reasons, (str, bytes, bytearray))
            else ()
        )
        if status == "CANDIDATES":
            return ("OPEN_POSITION_MANAGEMENT_ONLY",)
        if status == "NO_TRADE" and reasons not in {
            ("NO_OPEN_OPTION_POSITION",),
            # Preserve compatibility with durable/read models produced before
            # the generic defined-risk manager migration.
            ("NO_OPEN_GLD_POSITION",),
        }:
            return reasons or ("MANAGEMENT_STATE_UNAVAILABLE",)
        return ()

    def _execution_state_reasons(self) -> tuple[str, ...]:
        try:
            working_orders = self.gateway.working_orders()
        except Exception:
            return ("WORKING_ORDERS_READ_FAILED",)
        if not _sequence_value(working_orders):
            return ("WORKING_ORDERS_UNKNOWN",)
        if working_orders:
            return ("WORKING_ORDERS_PRESENT",)

        try:
            instructions = self.gateway.unsubmitted_instructions()
        except Exception:
            return ("UNSUBMITTED_INSTRUCTIONS_READ_FAILED",)
        if not _sequence_value(instructions):
            return ("UNSUBMITTED_INSTRUCTIONS_UNKNOWN",)
        if instructions:
            return ("UNSUBMITTED_INSTRUCTIONS_PRESENT",)
        return ()

    def _coarse_candidate_outcome(
        self,
        *,
        scan_run_id: str,
        slot_at: datetime,
        symbols: Sequence[str],
        event_snapshot: Mapping[str, object] | None = None,
        equity_theses: Mapping[str, object] | None = None,
    ) -> _CoarseCandidateRead:
        cursor = self._ordinary_coverage
        manual = bool(getattr(self._manual_state, "core_only", False))
        arguments = {
            "scan_run_id": scan_run_id,
            "slot_at": slot_at,
            "symbols": symbols,
            "event_snapshot": event_snapshot,
            "equity_theses": equity_theses,
        }
        if cursor is None or manual:
            return self._acquire_coarse_candidate_outcome(**arguments)
        with cursor.lock:
            try:
                ordered = cursor.arrange(symbols)
            except Exception:
                return _CoarseCandidateRead((), reason_codes=("ORDINARY_COVERAGE_EVIDENCE_INVALID",))
            arguments["symbols"] = ordered
            outcome = self._acquire_coarse_candidate_outcome(**arguments)
            visited = tuple(symbol for symbol in dict.fromkeys((
                *outcome.attempted_symbols,
                *outcome.completed_symbols,
                *outcome.excluded_symbols,
                *outcome.missing_symbols,
                *outcome.quote_excluded_symbols,
            )) if symbol in ordered and symbol not in outcome.deferred_symbols)
            try:
                evidence = cursor.record(
                    scan_run_id=scan_run_id,
                    ordered_symbols=ordered,
                    visited_symbols=visited,
                    observed_at=self._clock(),
                )
            except Exception:
                return _CoarseCandidateRead((), reason_codes=("ORDINARY_COVERAGE_APPEND_FAILED",))
            return replace(outcome, coverage_evidence=evidence)

    def _acquire_coarse_candidate_outcome(
        self,
        *,
        scan_run_id: str,
        slot_at: datetime,
        symbols: Sequence[str],
        event_snapshot: Mapping[str, object] | None = None,
        equity_theses: Mapping[str, object] | None = None,
    ) -> _CoarseCandidateRead:
        if not symbols:
            return _CoarseCandidateRead(())
        ordered_symbols = tuple(
            dict.fromkeys(
                str(symbol).strip().upper()
                for symbol in symbols
                if str(symbol).strip()
            )
        )
        thesis_by_symbol = {
            str(row.get("symbol", "")).strip().upper(): row
            for row in (
                equity_theses.get("rows", ())
                if isinstance(equity_theses, Mapping)
                else ()
            )
            if isinstance(row, Mapping)
            and str(row.get("symbol", "")).strip()
        }
        if not thesis_by_symbol:
            return _CoarseCandidateRead(
                (),
                reason_codes=("EQUITY_THESIS_EVIDENCE_UNAVAILABLE",),
            )
        gateway_paced = bool(
            getattr(self.gateway, "market_data_pacing_enabled", False)
        )
        manual_core_only = bool(getattr(self._manual_state, "core_only", False))
        # The 30-request rolling SECDEF authority can evidence two low-
        # uncertainty directional names because their long option and debit
        # vertical share the same two contracts.  Neutral or moderate-
        # uncertainty plans can require four or five distinct contracts per
        # name, including two definition reads in the later atomic snapshot.
        # A complex plan may preflight one bounded fallback, but qualification
        # stops as soon as one underlying produces complete coarse candidates.
        # This prevents first-symbol failure from starving the scan while the
        # human-approved rolling request limits remain unchanged.
        complex_paced_plan = gateway_paced and (
            _pacing_requires_single_optionable_underlying(
                ordered_symbols,
                thesis_by_symbol,
            )
        )
        asof = slot_at.astimezone(ZoneInfo("America/New_York")).date()
        optionability = _preflight_optionable_underlyings(
            self.gateway,
            self.pacing,
            ordered_symbols,
            asof=asof,
            max_attempts=(
                2
                if complex_paced_plan
                else None
            ),
            max_optionable=(
                1
                if manual_core_only
                else 2
                if complex_paced_plan
                else None
            ),
        )
        qualification_attempted: list[str] = []
        processed_symbols: set[str] = set()

        def qualification_scope() -> dict[str, tuple[str, ...]]:
            terminal = {*optionability.excluded_symbols, *processed_symbols}
            attempted = tuple(dict.fromkeys(qualification_attempted))
            attempted_set = set(attempted)
            return {
                "attempted_symbols": attempted,
                "deferred_symbols": tuple(
                    symbol
                    for symbol in ordered_symbols
                    if symbol not in terminal and symbol not in attempted_set
                ),
            }
        if not optionability.complete:
            return _CoarseCandidateRead(
                (),
                missing_symbols=optionability.unresolved_symbols,
                reason_codes=optionability.reason_codes,
                excluded_symbols=optionability.excluded_symbols,
                completed_symbols=(),
                optionability_exclusion_reasons=(
                    optionability.exclusion_reasons
                ),
                attempted_symbols=optionability.attempted_symbols,
                deferred_symbols=tuple(
                    symbol for symbol in ordered_symbols
                    if symbol not in optionability.attempted_symbols
                ),
            )
        expirations_by_symbol = optionability.as_mapping()
        optionable_symbols = tuple(expirations_by_symbol)
        if not optionable_symbols:
            return _CoarseCandidateRead(
                (),
                excluded_symbols=optionability.excluded_symbols,
                reason_codes=tuple(
                    dict.fromkeys(
                        reason
                        for _symbol, reason in optionability.exclusion_reasons
                    )
                ),
                completed_symbols=(),
                optionability_exclusion_reasons=(
                    optionability.exclusion_reasons
                ),
                **qualification_scope(),
            )
        requested_symbols = set(optionable_symbols)
        quote_symbols = tuple(dict.fromkeys((*optionable_symbols, "SPY")))
        quote_read = _read_underlying_quotes(
            self.gateway,
            self.pacing,
            quote_symbols,
            batch_size=4,
        )
        if not quote_read.complete:
            return _CoarseCandidateRead(
                (),
                missing_symbols=quote_read.missing_symbols,
                reason_codes=quote_read.reason_codes,
                excluded_symbols=optionability.excluded_symbols,
                quote_excluded_symbols=quote_read.failed_symbols,
                completed_symbols=(),
                optionability_exclusion_reasons=(
                    optionability.exclusion_reasons
                ),
                **qualification_scope(),
            )
        underlyings = quote_read.rows
        if not underlyings:
            return _CoarseCandidateRead(
                (),
                excluded_symbols=optionability.excluded_symbols,
                quote_excluded_symbols=quote_read.failed_symbols,
                completed_symbols=(),
                optionability_exclusion_reasons=(
                    optionability.exclusion_reasons
                ),
                **qualification_scope(),
            )
        try:
            quote_verified_at = utc_datetime(
                self._clock(),
                field="underlying quote verification time",
            )
        except (TypeError, ValueError):
            return _CoarseCandidateRead(
                (),
                reason_codes=("UNDERLYING_QUOTE_CLOCK_INVALID",),
                excluded_symbols=optionability.excluded_symbols,
                quote_excluded_symbols=quote_read.failed_symbols,
                completed_symbols=(),
                optionability_exclusion_reasons=(
                    optionability.exclusion_reasons
                ),
                **qualification_scope(),
            )
        short_leg_research_evidence = _execution_cost_short_leg_research_evidence(
            self._execution_cost_contract,
            as_of=quote_verified_at,
        )
        validated_underlyings: list[tuple[object, UnderlyingQuoteBasis]] = []
        basis_rejections: list[tuple[str, str]] = []
        seen_underlying_contracts: set[int] = set()
        for row in underlyings:
            symbol = str(getattr(row, "symbol", "")).strip().upper()
            basis, reason = _direct_underlying_quote_basis(
                row,
                expected_symbol=symbol,
                verified_at=quote_verified_at,
            )
            if basis is not None and basis.contract_id in seen_underlying_contracts:
                basis = None
                reason = "UNDERLYING_QUOTE_IDENTITY_MISMATCH"
            if basis is None:
                basis_rejections.append(
                    (
                        symbol or "UNKNOWN",
                        reason or "UNDERLYING_QUOTE_BASIS_INVALID",
                    )
                )
                continue
            seen_underlying_contracts.add(basis.contract_id)
            validated_underlyings.append((row, basis))
        basis_by_symbol = {
            basis.symbol: basis for _row, basis in validated_underlyings
        }
        benchmark = basis_by_symbol.get("SPY")
        results: list[Mapping[str, object]] = []
        qualification_rejections: list[tuple[str, str]] = []
        for underlying, underlying_basis in validated_underlyings:
            underlying_symbol = underlying_basis.symbol
            if underlying_symbol not in requested_symbols:
                continue
            qualification_attempted.append(underlying_symbol)
            spot = underlying_basis.market_price
            if spot is None or spot <= 0:
                qualification_rejections.append(
                    (underlying_symbol, "UNDERLYING_MARKET_PRICE_UNAVAILABLE")
                )
                processed_symbols.add(underlying_symbol)
                continue
            expirations = expirations_by_symbol.get(
                str(underlying.symbol).strip().upper(),
                (),
            )
            # SecDef option parameters expose expirations and strikes as
            # independent aggregate sets.  Prefer the standard monthly series
            # (third Friday), whose near-ATM strike grid is the least sparse;
            # a weekly Friday is only the bounded fallback.
            chain = _preferred_option_expiration(expirations)
            if chain is None:
                qualification_rejections.append(
                    (underlying_symbol, "OPTION_EXPIRATION_SELECTION_FAILED")
                )
                processed_symbols.add(underlying_symbol)
                continue
            thesis_row = thesis_by_symbol.get(underlying_symbol)
            if thesis_row is None:
                qualification_rejections.append(
                    (underlying_symbol, "EQUITY_THESIS_EVIDENCE_UNAVAILABLE")
                )
                processed_symbols.add(underlying_symbol)
                continue
            try:
                thesis_uncertainty = Decimal(str(thesis_row["uncertainty"]))
            except (KeyError, InvalidOperation, ValueError):
                thesis_uncertainty = Decimal("2")
            plans = _planned_structure_requests(
                chain.strikes,
                spot,
                direction_label=str(thesis_row.get("direction_label", "")),
                uncertainty=thesis_uncertainty,
            )
            if not plans:
                qualification_rejections.append(
                    (
                        underlying_symbol,
                        _structure_plan_rejection_reason(
                            chain.strikes,
                            direction_label=str(
                                thesis_row.get("direction_label", "")
                            ),
                            uncertainty=thesis_uncertainty,
                        ),
                    )
                )
                processed_symbols.add(underlying_symbol)
                continue
            requested_rights = tuple(dict.fromkeys(
                right for plan in plans for right in plan.rights
            ))
            planned_strikes_by_right = {
                right: tuple(dict.fromkeys(
                    strike
                    for plan in plans
                    for leg_right, strike, _side, _ratio in plan.legs
                    if leg_right == right
                ))
                for right in requested_rights
            }
            planned_contract_count = len({
                (right, strike)
                for plan in plans
                for right, strike, _side, _ratio in plan.legs
            })
            reserved_contract_ids = {
                int(leg["con_id"])
                for result in results
                for leg in result.get("legs", ())
                if isinstance(leg, Mapping)
            }
            if gateway_paced:
                try:
                    remaining_secdef = _remaining_pacing_capacity(
                        self.pacing,
                        "secdef",
                    )
                    remaining_streaming = _remaining_pacing_capacity(
                        self.pacing,
                        "streaming_quote",
                    )
                except _PacingUsageSnapshotError as exc:
                    qualification_rejections.append(
                        (underlying_symbol, exc.reason_code)
                    )
                    processed_symbols.add(underlying_symbol)
                    continue
            else:
                remaining_secdef = None
                remaining_streaming = None
            future_contract_count = (
                len(reserved_contract_ids) + planned_contract_count
            )
            planned_secdef_required = sum(
                len(items) for items in planned_strikes_by_right.values()
            ) + (2 * future_contract_count)
            # One standard option subscription is required per exact conId.
            # Exchange timestamps use the independently paced historical
            # Bid_Ask path; unsupported option tick-by-tick requests must not
            # consume the signed streaming budget.
            streaming_required = future_contract_count
            if gateway_paced and (
                remaining_secdef < planned_secdef_required
                or remaining_streaming < streaming_required
            ):
                qualification_rejections.append(
                    (
                        underlying_symbol,
                        "OPTION_PIPELINE_PACING_HEADROOM_INSUFFICIENT",
                    )
                )
                processed_symbols.add(underlying_symbol)
                continue
            expanded_strikes_by_right = {
                right: _qualification_strike_window(
                    planned_strikes_by_right[right],
                    chain.strikes,
                    spot=spot,
                    right=right,
                )
                for right in requested_rights
            }
            expanded_secdef_required = sum(
                len(items) for items in expanded_strikes_by_right.values()
            ) + (
                2
                * (
                    len(reserved_contract_ids)
                    + sum(
                        len(items)
                        for items in expanded_strikes_by_right.values()
                    )
                )
            )
            expanded_streaming_required = (
                len(reserved_contract_ids)
                + sum(
                    len(items) for items in expanded_strikes_by_right.values()
                )
            )
            paced_fallback_allowed = gateway_paced and (
                remaining_secdef >= expanded_secdef_required
                and remaining_streaming >= expanded_streaming_required
            )
            strikes_by_right = {
                right: _qualification_strike_request(
                    planned_strikes_by_right[right],
                    chain.strikes,
                    spot=spot,
                    right=right,
                    paced=gateway_paced,
                    fallback_allowed=paced_fallback_allowed,
                )
                for right in requested_rights
            }
            planned_structures = tuple(plan.structure for plan in plans)
            qualified_contracts: list[OptionContractRef] = []
            qualification_failed = False
            for right in requested_rights:
                with _gateway_request_lease(
                    self.gateway,
                    self.pacing,
                    "secdef",
                ) as decision:
                    if not bool(getattr(decision, "allowed", False)):
                        missing = tuple(
                            symbol
                            for symbol in optionable_symbols
                            if symbol not in processed_symbols
                        )
                        return _CoarseCandidateRead(
                            (),
                            missing_symbols=missing,
                            reason_codes=_pacing_denial_reasons(
                                "OPTION_QUALIFICATION",
                                decision,
                            ),
                            excluded_symbols=optionability.excluded_symbols,
                            quote_excluded_symbols=quote_read.failed_symbols,
                            completed_symbols=tuple(
                                symbol
                                for symbol in ordered_symbols
                                if symbol in processed_symbols
                            ),
                            optionability_exclusion_reasons=(
                                optionability.exclusion_reasons
                            ),
                            **qualification_scope(),
                        )
                    try:
                        qualified_contracts.extend(
                            self.gateway.qualify_option_contracts(
                                underlying.symbol,
                                chain.expiration,
                                strikes_by_right[right],
                                exchange=chain.exchange,
                                trading_class=chain.trading_class,
                                rights=(right,),
                            )
                        )
                    except MarketDataPacingError as exc:
                        missing = tuple(
                            symbol
                            for symbol in optionable_symbols
                            if symbol not in processed_symbols
                        )
                        return _CoarseCandidateRead(
                            (),
                            missing_symbols=missing,
                            reason_codes=_pacing_error_reasons(
                                "OPTION_QUALIFICATION",
                                exc,
                            ),
                            excluded_symbols=optionability.excluded_symbols,
                            quote_excluded_symbols=quote_read.failed_symbols,
                            completed_symbols=tuple(
                                symbol
                                for symbol in ordered_symbols
                                if symbol in processed_symbols
                            ),
                            optionability_exclusion_reasons=(
                                optionability.exclusion_reasons
                            ),
                            **qualification_scope(),
                        )
                    except Exception as exc:
                        qualification_rejections.append(
                            (
                                underlying_symbol,
                                _option_qualification_failure_reason(exc),
                            )
                        )
                        processed_symbols.add(underlying_symbol)
                        qualification_failed = True
                        break
            if qualification_failed:
                continue
            contracts = tuple(qualified_contracts)
            if any(
                item.symbol.strip().upper() != underlying_symbol
                or item.expiration != chain.expiration
                or item.right not in requested_rights
                or item.multiplier != 100
                or item.currency.upper() != "USD"
                for item in contracts
            ):
                qualification_rejections.append(
                    (underlying_symbol, "OPTION_QUALIFICATION_INVALID_RESPONSE")
                )
                processed_symbols.add(underlying_symbol)
                continue
            # IBKR SecDef option parameters publish expiration and strike sets
            # independently.  A near-ATM aggregate strike may therefore be
            # absent from the selected monthly expiration.  Rebuild the same
            # thesis-approved strategy set from the contracts IBKR actually
            # qualified; never fabricate the missing identity or change the
            # direction/uncertainty decision.
            plans = _qualified_structure_requests(
                contracts,
                spot,
                direction_label=str(thesis_row.get("direction_label", "")),
                uncertainty=thesis_uncertainty,
            )
            if not plans:
                qualification_rejections.extend(
                    (underlying_symbol, f"{structure}_QUALIFICATION_INCOMPLETE")
                    for structure in planned_structures
                )
                processed_symbols.add(underlying_symbol)
                continue
            qualified_structures = {plan.structure for plan in plans}
            qualification_rejections.extend(
                (underlying_symbol, f"{structure}_QUALIFICATION_INCOMPLETE")
                for structure in planned_structures
                if structure not in qualified_structures
            )
            resolved_plan_contracts: list[
                tuple[_StructurePlan, tuple[OptionContractRef, ...]]
            ] = []
            for plan in plans:
                selected_contracts = _planned_contracts(contracts, plan)
                if len(selected_contracts) != len(plan.legs):
                    qualification_rejections.append(
                        (underlying_symbol, f"{plan.structure}_QUALIFICATION_INCOMPLETE")
                    )
                    continue
                resolved_plan_contracts.append((plan, selected_contracts))
            if not resolved_plan_contracts:
                processed_symbols.add(underlying_symbol)
                continue
            if gateway_paced:
                actual_contract_ids = {
                    contract.contract_id
                    for _plan, selected_contracts in resolved_plan_contracts
                    for contract in selected_contracts
                }
                snapshot_contract_count = len(
                    reserved_contract_ids | actual_contract_ids
                )
                try:
                    remaining_secdef = _remaining_pacing_capacity(
                        self.pacing,
                        "secdef",
                    )
                    remaining_streaming = _remaining_pacing_capacity(
                        self.pacing,
                        "streaming_quote",
                    )
                except _PacingUsageSnapshotError as exc:
                    qualification_rejections.append(
                        (underlying_symbol, exc.reason_code)
                    )
                    processed_symbols.add(underlying_symbol)
                    continue
                if (
                    remaining_secdef < 2 * snapshot_contract_count
                    or remaining_streaming < snapshot_contract_count
                ):
                    qualification_rejections.append(
                        (
                            underlying_symbol,
                            "OPTION_PIPELINE_PACING_HEADROOM_INSUFFICIENT",
                        )
                    )
                    processed_symbols.add(underlying_symbol)
                    continue
            for plan, selected_contracts in resolved_plan_contracts:
                if len(results) >= 10:
                    break
                contract_ids = "|".join(str(item.contract_id) for item in selected_contracts)
                candidate_id = "candidate." + hashlib.sha256(
                    (
                        f"{scan_run_id}|{underlying.symbol}|{chain.expiration.isoformat()}|"
                        f"{plan.structure}|{contract_ids}"
                    ).encode("utf-8")
                ).hexdigest()[:24]
                hold_until = min(chain.expiration, asof + timedelta(days=5))
                event_facts = _event_gate_fields(
                    event_snapshot or {},
                    symbol=str(underlying.symbol),
                    slot_at=slot_at,
                    holding_end=hold_until,
                )
                fundamental_facts = self._fundamental_gate_fields(
                    symbol=str(underlying.symbol),
                    slot_at=slot_at,
                )
                thesis_hash = canonical_hash(
                    {
                        key: value
                        for key, value in thesis_row.items()
                        if key != "thesis_hash"
                    }
                )
                leg_rows = tuple(
                    _contract_leg(
                        contract,
                        side,
                        ratio=ratio,
                        short_leg_risk_evidence=(
                            short_leg_research_evidence
                            if str(side).strip().upper() in {"SHORT", "SELL"}
                            else {
                                "status": "NOT_APPLICABLE",
                                "reason_codes": (),
                                "evidence_hash": None,
                            }
                        ),
                    )
                    for contract, (_right, _strike, side, ratio) in zip(
                        selected_contracts,
                        plan.legs,
                        strict=True,
                    )
                )
                short_leg_proofs = tuple(
                    leg["short_leg_risk_evidence"]
                    for leg in leg_rows
                    if str(leg.get("side", "")).strip().upper()
                    in {"SHORT", "SELL"}
                )
                short_risk_status = (
                    "NOT_APPLICABLE"
                    if not short_leg_proofs
                    else "SUPPORTED"
                    if all(
                        isinstance(proof, Mapping)
                        and proof.get("status") == "SUPPORTED"
                        for proof in short_leg_proofs
                    )
                    else "UNSUPPORTED"
                )
                results.append({
                    "candidate_id": candidate_id,
                    "symbol": underlying.symbol,
                    "score": Decimal("70") - Decimal(len(results)) / Decimal("100"),
                    "dte": (chain.expiration - asof).days,
                    "structure": plan.structure,
                    "legs": leg_rows,
                    "exit_plan": {
                        "thesis_invalidation": "The hash-bound G035 equity thesis direction or evidence coverage is no longer valid.",
                        "risk_stop": "Human review is required before maximum-loss tolerance is reached.",
                        "profit_take": "Human review is required when the bounded profit objective is reached.",
                        "time_stop": "Human close or reduce review no later than the configured short holding window.",
                        "maximum_holding_date": hold_until.isoformat(),
                        "bad_quote_action": "NO_TRADE",
                    },
                    "terminal_scenarios": (),
                    "thesis": "Hash-bound G035 equity thesis; every option and account Gate remains mandatory.",
                    "equity_thesis_evidence": thesis_row,
                    "equity_thesis_hash": thesis_hash,
                    "invalidation_evidence": {
                        "status": "BOUND",
                        "thesis_invalidation": (
                            "The hash-bound G035 equity thesis direction or "
                            "evidence coverage is no longer valid."
                        ),
                        "maximum_holding_date": hold_until.isoformat(),
                        "equity_thesis_hash": thesis_hash,
                    },
                    "assignment_evidence": {
                        "status": short_risk_status,
                        "short_leg_evidence": short_leg_proofs,
                    },
                    "ex_dividend_evidence": {
                        "status": short_risk_status,
                        "short_leg_evidence": short_leg_proofs,
                    },
                    "spot": spot,
                    "underlying_quote_basis": underlying_basis.as_dict(),
                    "underlying_quote_basis_hash": underlying_basis.basis_hash,
                    "outcome_capture_baseline": _outcome_capture_baseline(
                        underlying_basis,
                        benchmark,
                    ),
                    **event_facts,
                    **fundamental_facts,
                })
            processed_symbols.add(underlying_symbol)
            if complex_paced_plan and results:
                break
        quote_exclusion_reasons = tuple(
            dict.fromkeys(
                (
                    *((symbol, "UNDERLYING_QUOTE_FAILED") for symbol in quote_read.failed_symbols),
                    *basis_rejections,
                    *qualification_rejections,
                )
            )
        )
        terminal_rejections = tuple(
            dict.fromkeys((*basis_rejections, *qualification_rejections))
        )
        candidate_symbols = {
            str(row.get("symbol", "")).strip().upper()
            for row in results
            if str(row.get("symbol", "")).strip()
        }
        reason_codes = (
            tuple(
                dict.fromkeys(
                    (
                        *(
                            reason
                            for _symbol, reason in optionability.exclusion_reasons
                        ),
                        *(reason for _symbol, reason in terminal_rejections),
                    )
                )
            )
            if not results
            else ()
        )
        completed_symbol_set = set(processed_symbols)
        completed_symbols = tuple(
            symbol for symbol in ordered_symbols if symbol in completed_symbol_set
        )
        return _CoarseCandidateRead(
            tuple(results[:10]),
            reason_codes=reason_codes,
            excluded_symbols=optionability.excluded_symbols,
            quote_excluded_symbols=tuple(
                dict.fromkeys(
                    (
                        *quote_read.failed_symbols,
                        *(
                            symbol
                            for symbol, _reason in terminal_rejections
                            if symbol not in candidate_symbols
                        ),
                    )
                )
            ),
            quote_exclusion_reasons=quote_exclusion_reasons,
            completed_symbols=completed_symbols,
            optionability_exclusion_reasons=(
                optionability.exclusion_reasons
            ),
            **qualification_scope(),
        )

    def _fundamental_gate_fields(
        self,
        *,
        symbol: str,
        slot_at: datetime,
    ) -> dict[str, object]:
        with self._reader_lock:
            reader = self._fundamentals_reader
        if reader is None:
            return {
                "fundamental_supporting_status": "DEGRADED",
                "fundamental_supporting_hash": None,
                "fundamental_supporting_payload": {},
                "fundamental_supporting_reason_codes": (
                    "FUNDAMENTALS_UNAVAILABLE",
                ),
            }
        try:
            value = reader(str(symbol).strip().upper(), slot_at)
        except Exception:
            value = None
        if not isinstance(value, Mapping):
            return {
                "fundamental_supporting_status": "DEGRADED",
                "fundamental_supporting_hash": None,
                "fundamental_supporting_payload": {},
                "fundamental_supporting_reason_codes": (
                    "FUNDAMENTALS_UNAVAILABLE",
                ),
            }
        status = str(value.get("status") or "DEGRADED").strip().upper()
        source_hash = value.get("source_hash")
        payload = value.get("payload")
        reason_codes = value.get("reason_codes")
        if (
            status not in {"AVAILABLE", "DEGRADED"}
            or (status == "AVAILABLE" and not _digest(source_hash))
            or not isinstance(payload, Mapping)
            or payload.get("decision_authority") != "SUPPORTING_ONLY"
            or not _sequence_value(reason_codes)
        ):
            status = "DEGRADED"
            source_hash = None
            payload = {}
            reason_codes = ("FUNDAMENTALS_UNAVAILABLE",)
        return {
            "fundamental_supporting_status": status,
            "fundamental_supporting_hash": source_hash,
            "fundamental_supporting_payload": dict(payload),
            "fundamental_supporting_reason_codes": tuple(
                str(item).strip().upper() for item in reason_codes
            ),
        }


class DurableOptionPoolTop10StructureSource:
    """Reuse recent verified research identities at 09:20 ET.

    The option-pool store verifies its append-only hash chain before returning
    a bounded newest-first window.  A later thesis-unbound closing research
    pool must not hide an earlier, still-current thesis-bound pool produced by
    the ordinary scan lineage.  This adapter copies only static contract
    geometry into the premarket ledger; all dynamic quotes and economics
    remain empty until the independent 09:35 AtomicBrokerSnapshot.  Only
    dynamic market evidence may be refreshed there: missing thesis,
    invalidation, assignment, dividend, or other static evidence excludes the
    row.  A direct identity fallback is explicitly thesis-unbound and therefore
    cannot create a 09:20 parent.
    """

    _MAXIMUM_RESEARCH_AGE = timedelta(days=5)
    _RECENT_SNAPSHOT_LIMIT = 64
    _DEFINED_RISK_STRUCTURES = frozenset(
        {
            "LONG_OPTION",
            "DEBIT_VERTICAL",
            "CREDIT_VERTICAL",
            "BUTTERFLY",
            "IRON_CONDOR",
            "CALENDAR",
            "DIAGONAL",
        }
    )
    _PREMARKET_REFRESHABLE_REASON_CODES = frozenset(
        {
            "FRESH_EXACT_OPTION_EVIDENCE_CAPTURED",
            "AFTER_HOURS_RESEARCH_ONLY",
            "AFTER_HOURS_EXACT_IDENTITY_RESEARCH_ONLY",
            "REGULAR_SESSION_EXACT_IDENTITY_RESEARCH_ONLY",
            "CANDIDATE_AFTER_COST_EV_NONPOSITIVE",
            "FRESH_EXECUTABLE_OPTION_EVIDENCE_REQUIRED",
            "EXECUTABLE_LEG_QUOTE_INCOMPLETE",
            "EXECUTABLE_QUOTES_STALE",
            "OPTION_GREEKS_INCOMPLETE",
            "OPTION_LIQUIDITY_EVIDENCE_INCOMPLETE",
            "STRUCTURE_PAYOFF_EVIDENCE_INCOMPLETE",
            "AFTER_COST_ECONOMICS_INCOMPLETE",
        }
    )

    def __init__(
        self,
        store: object,
        *,
        fallback: object,
        maximum_snapshot_contracts: int | None = None,
    ) -> None:
        if not callable(getattr(store, "latest", None)):
            raise TypeError("durable option-pool store must expose latest")
        if not callable(getattr(fallback, "resolve_top10", None)):
            raise TypeError("durable option-pool fallback must expose resolve_top10")
        if (
            maximum_snapshot_contracts is not None
            and (
                isinstance(maximum_snapshot_contracts, bool)
                or not isinstance(maximum_snapshot_contracts, int)
                or not 1 <= maximum_snapshot_contracts <= 50
            )
        ):
            raise ValueError("maximum_snapshot_contracts must be 1-50 or null")
        self._store = store
        self._fallback = fallback
        self._maximum_snapshot_contracts = maximum_snapshot_contracts

    def resolve_top10(
        self,
        *,
        scheduled_for: datetime,
    ) -> Top10StructureResolution:
        slot = utc_datetime(scheduled_for, field="scheduled_for")
        try:
            snapshots = self._recent_snapshots()
        except Exception:
            return Top10StructureResolution(
                (),
                reason_codes=("OPTION_POOL_LEDGER_INVALID",),
            )
        if not snapshots:
            return self._fallback_resolution(slot)
        snapshot = self._select_snapshot(snapshots, slot=slot)
        if snapshot is None:
            return self._fallback_resolution(slot)

        scan_run_id = str(getattr(snapshot, "scan_run_id", "")).strip()
        snapshot_hash = getattr(snapshot, "snapshot_hash", None)
        decisions = getattr(snapshot, "decisions", None)
        try:
            observed_at = utc_datetime(
                getattr(snapshot, "observed_at", None),
                field="option pool observed_at",
            )
        except (TypeError, ValueError):
            return Top10StructureResolution(
                (),
                reason_codes=("OPTION_POOL_SNAPSHOT_INVALID",),
            )
        age = slot - observed_at
        if (
            not self._supported_lineage(scan_run_id)
            or age < timedelta(0)
            or age > self._MAXIMUM_RESEARCH_AGE
        ):
            return self._fallback_resolution(slot)
        if (
            not _digest(snapshot_hash)
            or not isinstance(decisions, Sequence)
            or isinstance(decisions, (str, bytes, bytearray, memoryview))
        ):
            return Top10StructureResolution(
                (),
                reason_codes=("OPTION_POOL_SNAPSHOT_INVALID",),
            )

        structures: list[ResolvedStructure] = []
        invalid_symbols: list[str] = []
        invalid_count = 0
        static_blocked_symbols: list[str] = []
        static_blockers: list[str] = []
        seen_identities: set[str] = set()
        snapshot_contract_ids: set[int] = set()
        for decision in decisions:
            disposition = str(
                getattr(getattr(decision, "disposition", None), "value", None)
                or getattr(decision, "disposition", "")
            ).strip().upper()
            if disposition == "EXCLUDED":
                continue
            symbol = str(getattr(decision, "underlying", "")).strip().upper()
            try:
                row_blockers, thesis_hash = self._static_evidence_binding(
                    decision,
                )
            except (ArithmeticError, TypeError, ValueError):
                invalid_count += 1
                if symbol:
                    invalid_symbols.append(symbol)
                continue
            if row_blockers:
                if symbol:
                    static_blocked_symbols.append(symbol)
                static_blockers.extend(row_blockers)
                continue
            try:
                candidate, candidate_identity = self._candidate_from_decision(
                    decision,
                    scheduled_for=slot,
                    snapshot_hash=str(snapshot_hash),
                    scan_run_id=scan_run_id,
                    thesis_hash=thesis_hash,
                )
            except (ArithmeticError, TypeError, ValueError):
                invalid_count += 1
                if symbol:
                    invalid_symbols.append(symbol)
                continue
            if candidate is None or candidate_identity in seen_identities:
                continue
            candidate_contract_ids = {
                leg.con_id
                for leg in candidate.legs
                if isinstance(leg.con_id, int) and not isinstance(leg.con_id, bool)
            }
            if (
                self._maximum_snapshot_contracts is not None
                and len(snapshot_contract_ids | candidate_contract_ids)
                > self._maximum_snapshot_contracts
            ):
                continue
            seen_identities.add(candidate_identity)
            snapshot_contract_ids.update(candidate_contract_ids)
            structures.append(ResolvedStructure(candidate))
            if len(structures) >= 10:
                break

        if structures and invalid_count and len(structures) < 10:
            # A previously valid research pool can become partial as contracts
            # roll below the DTE floor.  Preserve every verified durable row,
            # then opportunistically replace only those discarded slots through
            # the same bounded read-only fallback.  An incomplete or failed
            # fallback must not erase the durable rows that remain valid.
            try:
                supplemental = self._fallback_resolution(slot)
            except Exception:
                supplemental = None
            if (
                supplemental is not None
                and not supplemental.reason_codes
                and not supplemental.missing_symbols
            ):
                target_count = min(10, len(structures) + invalid_count)
                seen_strategy_hashes = {
                    item.candidate.strategy_hash for item in structures
                }
                seen_underlyings = {
                    item.candidate.underlying for item in structures
                }
                for item in supplemental.structures:
                    strategy_hash = item.candidate.strategy_hash
                    underlying = item.candidate.underlying
                    candidate_contract_ids = {
                        leg.con_id
                        for leg in item.candidate.legs
                        if isinstance(leg.con_id, int)
                        and not isinstance(leg.con_id, bool)
                    }
                    if (
                        strategy_hash in seen_strategy_hashes
                        or underlying in seen_underlyings
                        or (
                            self._maximum_snapshot_contracts is not None
                            and len(snapshot_contract_ids | candidate_contract_ids)
                            > self._maximum_snapshot_contracts
                        )
                    ):
                        continue
                    seen_strategy_hashes.add(strategy_hash)
                    seen_underlyings.add(underlying)
                    snapshot_contract_ids.update(candidate_contract_ids)
                    structures.append(item)
                    if len(structures) >= target_count:
                        break

        if structures:
            return Top10StructureResolution(tuple(structures))
        if static_blocked_symbols:
            return Top10StructureResolution(
                (),
                reason_codes=tuple(
                    dict.fromkeys(
                        (
                            "OPTION_POOL_STATIC_EVIDENCE_INCOMPLETE",
                            *static_blockers,
                        )
                    )
                ),
                missing_symbols=tuple(dict.fromkeys(static_blocked_symbols)),
            )
        if invalid_symbols:
            return Top10StructureResolution(
                (),
                reason_codes=("OPTION_POOL_CANDIDATES_INVALID",),
                missing_symbols=tuple(invalid_symbols),
            )
        return Top10StructureResolution(
            (),
            reason_codes=("OPTION_POOL_NO_PREMARKET_ELIGIBLE_STRUCTURES",),
        )

    def _recent_snapshots(self) -> tuple[object, ...]:
        recent = getattr(self._store, "recent", None)
        if callable(recent):
            value = recent(limit=self._RECENT_SNAPSHOT_LIMIT)
            if (
                not isinstance(value, Sequence)
                or isinstance(value, (str, bytes, bytearray, memoryview))
            ):
                raise TypeError("durable option-pool recent result is invalid")
            return tuple(value)
        latest = self._store.latest()
        return () if latest is None else (latest,)

    def _select_snapshot(
        self,
        snapshots: Sequence[object],
        *,
        slot: datetime,
    ) -> object | None:
        first_eligible: object | None = None
        for snapshot in snapshots:
            scan_run_id = str(getattr(snapshot, "scan_run_id", "")).strip()
            try:
                observed_at = utc_datetime(
                    getattr(snapshot, "observed_at", None),
                    field="option pool observed_at",
                )
            except (TypeError, ValueError):
                return snapshot
            age = slot - observed_at
            if (
                not self._supported_lineage(scan_run_id)
                or age < timedelta(0)
                or age > self._MAXIMUM_RESEARCH_AGE
            ):
                continue
            if first_eligible is None:
                first_eligible = snapshot
            if self._has_static_eligible_decision(snapshot):
                return snapshot
        return first_eligible

    @staticmethod
    def _supported_lineage(scan_run_id: str) -> bool:
        return scan_run_id.startswith(("after-hours-formal.", "scan."))

    def _has_static_eligible_decision(self, snapshot: object) -> bool:
        snapshot_hash = getattr(snapshot, "snapshot_hash", None)
        decisions = getattr(snapshot, "decisions", None)
        if (
            not _digest(snapshot_hash)
            or not isinstance(decisions, Sequence)
            or isinstance(decisions, (str, bytes, bytearray, memoryview))
        ):
            return False
        for decision in decisions:
            disposition = str(
                getattr(getattr(decision, "disposition", None), "value", None)
                or getattr(decision, "disposition", "")
            ).strip().upper()
            if disposition == "EXCLUDED":
                continue
            try:
                blockers, _ = self._static_evidence_binding(decision)
            except (ArithmeticError, TypeError, ValueError):
                continue
            if not blockers:
                return True
        return False

    def _fallback_resolution(self, slot: datetime) -> Top10StructureResolution:
        value = self._fallback.resolve_top10(scheduled_for=slot)
        resolution = (
            value
            if isinstance(value, Top10StructureResolution)
            else Top10StructureResolution(tuple(value))
        )
        return Top10StructureResolution(
            resolution.structures,
            reason_codes=tuple(
                dict.fromkeys(
                    (
                        *resolution.reason_codes,
                        "PREMARKET_EQUITY_THESIS_SOURCE_UNAVAILABLE",
                    )
                )
            ),
            missing_symbols=resolution.missing_symbols,
        )

    def _static_evidence_binding(
        self,
        decision: object,
    ) -> tuple[tuple[str, ...], str]:
        """Validate evidence that a 09:35 market refresh cannot create."""

        symbol = str(getattr(decision, "underlying", "")).strip().upper()
        if not symbol:
            raise ValueError("durable option-pool underlying is invalid")
        raw_reasons = getattr(decision, "reason_codes", None)
        if (
            not isinstance(raw_reasons, Sequence)
            or isinstance(raw_reasons, (str, bytes, bytearray, memoryview))
            or not raw_reasons
        ):
            return ("OPTION_POOL_REASON_CODES_INVALID",), ""
        reasons = tuple(
            dict.fromkeys(
                str(item).strip().upper()
                for item in raw_reasons
                if str(item).strip()
            )
        )
        if not reasons:
            return ("OPTION_POOL_REASON_CODES_INVALID",), ""
        blockers = [
            reason
            for reason in reasons
            if reason not in self._PREMARKET_REFRESHABLE_REASON_CODES
        ]

        thesis_value = getattr(decision, "equity_thesis_evidence", None)
        try:
            thesis = normalize_equity_thesis_row(
                thesis_value,
                expected_symbol=symbol,
            )
        except (ArithmeticError, TypeError, ValueError):
            thesis = None
            blockers.append("EQUITY_THESIS_EVIDENCE_INVALID")
        if thesis is None:
            if "EQUITY_THESIS_EVIDENCE_UNAVAILABLE" not in blockers:
                blockers.append("EQUITY_THESIS_EVIDENCE_UNAVAILABLE")
            return tuple(dict.fromkeys(blockers)), ""

        direction = str(thesis.get("direction_label", "")).strip().upper()
        if direction not in {"BULLISH", "BEARISH", "NEUTRAL"}:
            blockers.append("EQUITY_THESIS_DIRECTION_UNACTIONABLE")
        thesis_hash = canonical_hash(thesis)
        payload = getattr(decision, "exact_economics", None)
        if not isinstance(payload, Mapping):
            raise TypeError("durable option-pool economics are invalid")
        supplied_hash = payload.get("equity_thesis_hash")
        if not _digest(supplied_hash) or supplied_hash != thesis_hash:
            blockers.append("EQUITY_THESIS_HASH_INVALID")
        try:
            payload_thesis = normalize_equity_thesis_row(
                payload.get("equity_thesis_evidence"),
                expected_symbol=symbol,
            )
        except (ArithmeticError, TypeError, ValueError):
            payload_thesis = None
        if payload_thesis is None or canonical_hash(payload_thesis) != thesis_hash:
            blockers.append("EQUITY_THESIS_BINDING_INVALID")
        return tuple(dict.fromkeys(blockers)), thesis_hash

    def _candidate_from_decision(
        self,
        decision: object,
        *,
        scheduled_for: datetime,
        snapshot_hash: str,
        scan_run_id: str,
        thesis_hash: str,
    ) -> tuple[ConditionalOptionPreselection | None, str]:
        payload = getattr(decision, "exact_economics", None)
        candidate_hash = getattr(decision, "candidate_hash", None)
        candidate_identity = getattr(decision, "candidate_identity", None)
        candidate_id = str(getattr(decision, "candidate_id", "")).strip()
        if (
            not isinstance(payload, Mapping)
            or not _digest(candidate_hash)
            or canonical_hash(payload) != candidate_hash
            or not _digest(candidate_identity)
            or option_candidate_identity(payload) != candidate_identity
            or not candidate_id
            or str(payload.get("candidate_id", "")).strip() != candidate_id
        ):
            raise ValueError("durable option-pool candidate binding is invalid")
        symbol = str(payload.get("symbol", "")).strip().upper()
        structure = str(payload.get("structure", "")).strip().upper()
        raw_legs = payload.get("legs")
        if (
            not symbol
            or structure not in self._DEFINED_RISK_STRUCTURES
            or not isinstance(raw_legs, Sequence)
            or isinstance(raw_legs, (str, bytes, bytearray, memoryview))
            or not raw_legs
            or len(raw_legs) > 8
        ):
            raise ValueError("durable option-pool structure is invalid")

        trading_date = scheduled_for.astimezone(
            ZoneInfo("America/New_York")
        ).date()
        legs = tuple(
            self._leg_from_mapping(
                value,
                expected_symbol=symbol,
                trading_date=trading_date,
            )
            for value in raw_legs
        )
        strategy_hash = strategy_structure_hash(symbol, structure, legs)
        preselection_id = "durable-top10." + hashlib.sha256(
            (
                f"{scheduled_for.date().isoformat()}|{candidate_identity}"
            ).encode("utf-8")
        ).hexdigest()[:24]
        return (
            ConditionalOptionPreselection(
                preselection_id=preselection_id,
                underlying=symbol,
                strategy_type=structure,
                phase=PreselectionPhase.PRE_MARKET,
                legs=legs,
                risk_defined=True,
                maximum_loss_usd=None,
                estimated_cost_usd=None,
                cost_after_ev_usd=None,
                entry_condition=(
                    "Review only after the 09:35 ET AtomicBrokerSnapshot "
                    "reprice and every hard Gate passes."
                ),
                invalidation_condition=(
                    "Discard if the prior research thesis, exact identity, "
                    "or current market evidence no longer agrees."
                ),
                profit_target_condition=(
                    "No profit target exists before executable economics."
                ),
                stop_loss_condition=(
                    "No order exists; the human retains exclusive control."
                ),
                evidence_ids=(
                    f"DURABLE_OPTION_POOL:{scan_run_id}",
                    f"DURABLE_OPTION_IDENTITY:{candidate_identity}",
                    f"DURABLE_EQUITY_THESIS:{thesis_hash}",
                ),
                evidence_hashes=(
                    snapshot_hash,
                    str(candidate_hash),
                    str(candidate_identity),
                    thesis_hash,
                ),
                strategy_hash=strategy_hash,
                research_summary=(
                    "Hash-verified prior-session exact-contract research; "
                    "all quotes, Greeks, liquidity, risk, and cost-after-EV "
                    "are intentionally refreshed at 09:35 ET."
                ),
            ),
            str(candidate_identity),
        )

    @staticmethod
    def _leg_from_mapping(
        value: object,
        *,
        expected_symbol: str,
        trading_date: date,
    ) -> ConditionalOptionLeg:
        if not isinstance(value, Mapping):
            raise TypeError("durable option-pool leg must be a mapping")
        con_id = value.get("con_id")
        ratio = value.get("ratio")
        multiplier = _decimal(value.get("multiplier"))
        if (
            isinstance(con_id, bool)
            or not isinstance(con_id, int)
            or con_id <= 0
            or isinstance(ratio, bool)
            or not isinstance(ratio, int)
            or ratio <= 0
            or multiplier != 100
        ):
            raise ValueError("durable option-pool leg quantities are invalid")
        expiration = date.fromisoformat(str(value.get("expiration", "")))
        dte = (expiration - trading_date).days
        if dte < 14 or dte > 35:
            raise ValueError("durable option-pool leg DTE is outside policy")
        strike = _decimal(value.get("strike"))
        if strike is None or strike <= 0:
            raise ValueError("durable option-pool strike is invalid")
        right = OptionRight(normalise_option_right(value.get("right")))
        side = (
            OptionLegSide.BUY
            if normalise_option_side(value.get("side")) == "LONG"
            else OptionLegSide.SELL
        )
        exchange = str(value.get("exchange", "")).strip().upper()
        local_symbol = str(value.get("local_symbol", "")).strip()
        trading_class = str(value.get("trading_class", "")).strip()
        contract_id_ex = str(value.get("contract_id_ex", "")).strip()
        if (
            right is None
            or side is None
            or not exchange
            or not local_symbol
            or not trading_class
            or contract_id_ex != f"{con_id}@{exchange}"
            or str(value.get("symbol", expected_symbol)).strip().upper()
            != expected_symbol
        ):
            raise ValueError("durable option-pool leg identity is invalid")
        return ConditionalOptionLeg(
            underlying=expected_symbol,
            con_id=con_id,
            expiry=expiration,
            strike=strike,
            right=right,
            side=side,
            ratio=ratio,
            quantity=ratio,
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
            dte=dte,
            local_symbol=local_symbol,
            trading_class=trading_class,
            multiplier=int(multiplier),
            exchange=exchange,
        )


class DirectTop10StructureSource:
    """Discover at most ten exact, defined-risk structures through IBKR.

    This source owns identity discovery only.  It deliberately leaves every
    dynamic option quote and all economics empty; ``Top10PreselectionProducer``
    fills those fields from one later ``AtomicBrokerSnapshot``.  Discovery is
    bounded to thirty underlyings and one vertical per underlying so a broad
    scanner result cannot turn into an unbounded option-chain crawl.
    """

    def __init__(
        self,
        gateway: IBKRReadOnlyGateway,
        pacing: GuardedRequestBudget,
        *,
        clock: Callable[[], datetime],
        core_symbols: Sequence[str] = (),
        include_scanner: bool = True,
        excluded_symbols: Sequence[str] = (),
        maximum_optionability_attempts: int = 14,
        maximum_optionable: int = 14,
        maximum_structures: int = 10,
        indicative_underlyings: bool = False,
    ) -> None:
        if not isinstance(include_scanner, bool):
            raise TypeError("include_scanner must be a bool")
        if not callable(clock):
            raise TypeError("clock must be callable")
        self.gateway = gateway
        self.pacing = pacing
        self._clock = clock
        self.include_scanner = include_scanner
        self.excluded_symbols = frozenset(
            str(item).strip().upper()
            for item in excluded_symbols
            if str(item).strip()
        )
        if (
            maximum_optionability_attempts < 1
            or maximum_optionability_attempts > 14
            or maximum_optionable < 1
            or maximum_optionable > maximum_optionability_attempts
            or maximum_structures < 1
            or maximum_structures > 10
        ):
            raise ValueError("direct Top-10 discovery bounds are invalid")
        self.maximum_optionability_attempts = maximum_optionability_attempts
        self.maximum_optionable = maximum_optionable
        self.maximum_structures = maximum_structures
        if not isinstance(indicative_underlyings, bool):
            raise TypeError("indicative_underlyings must be a bool")
        self.indicative_underlyings = indicative_underlyings
        self._last_discovery_metadata: tuple[Mapping[str, object], ...] = ()
        self.core_symbols = tuple(
            dict.fromkeys(
                str(item).strip().upper()
                for item in core_symbols
                if str(item).strip()
            )
        )

    @property
    def discovery_metadata(self) -> tuple[Mapping[str, object], ...]:
        """Return detached source/industry evidence for the latest resolution."""

        return tuple(dict(item) for item in self._last_discovery_metadata)

    def resolve_top10(
        self,
        *,
        scheduled_for: datetime,
    ) -> Top10StructureResolution:
        self._last_discovery_metadata = ()
        slot = utc_datetime(scheduled_for, field="scheduled_for")
        if not self.pacing.ready:
            return Top10StructureResolution(
                (),
                reason_codes=(
                    str(
                        getattr(self.pacing, "reason", None)
                        or PACING_CAPABILITY_MISSING
                    ),
                ),
            )

        try:
            discovery = (
                _discover_underlyings(self.gateway, self.pacing)
                if self.include_scanner
                else _UnderlyingDiscoveryRead((), (), (), ())
            )
        except MarketDataPacingError as exc:
            return Top10StructureResolution(
                (),
                reason_codes=_pacing_error_reasons("IBKR_SCANNER", exc),
            )
        scanner_rows = discovery.rows

        scanner_docs = tuple(
            {
                "symbol": str(getattr(item, "symbol", "")).strip().upper(),
                "score": max(
                    Decimal("0"),
                    Decimal("100")
                    - Decimal(str(getattr(item, "rank", 100))),
                ),
                "source_scan": str(
                    getattr(item, "source_scan", "")
                ).strip().upper(),
                "industry": str(getattr(item, "industry", "") or "").strip(),
                "category": str(getattr(item, "category", "") or "").strip(),
                "subcategory": str(
                    getattr(item, "subcategory", "") or ""
                ).strip(),
            }
            for item in scanner_rows
            if str(getattr(item, "symbol", "")).strip()
            and str(getattr(item, "symbol", "")).strip().upper()
            not in self.excluded_symbols
        )
        core_docs = tuple(
            {
                "symbol": symbol,
                "score": Decimal(max(1, 60 - index)),
                "source_scan": "CORE_UNIVERSE",
            }
            for index, symbol in enumerate(self.core_symbols[:40])
            if symbol not in self.excluded_symbols
        )
        scanner_docs = _sector_diverse_scanner_rows(scanner_docs)
        symbols = _balanced_discovery_symbols(
            scanner_docs,
            core_docs,
            limit=30,
            preserve_scanner_order=True,
        )
        if not symbols:
            return Top10StructureResolution(
                (),
                reason_codes=discovery.reason_codes,
            )
        asof = slot.astimezone(ZoneInfo("America/New_York")).date()
        optionability = _preflight_optionable_underlyings(
            self.gateway,
            self.pacing,
            symbols,
            asof=asof,
            # This path discovers identity-only research structures.  It does
            # not perform the action pipeline's repeated AtomicBrokerSnapshot
            # reads, so inheriting that pipeline's 4-attempt/2-candidate cap
            # silently collapses a requested Top-10 into a Top-2.  Fourteen
            # attempts leave room for exclusions while the ten-result cap and
            # guarded 30-request secdef budget remain authoritative.  Retain
            # all fourteen optionable names so qualification failures can be
            # backfilled from the bounded reserve instead of shrinking Top-10.
            max_attempts=self.maximum_optionability_attempts,
            max_optionable=self.maximum_optionable,
        )
        if not optionability.complete:
            return Top10StructureResolution(
                (),
                reason_codes=optionability.reason_codes,
                missing_symbols=optionability.unresolved_symbols,
            )
        expirations_by_symbol = optionability.as_mapping()
        optionable_symbols = tuple(expirations_by_symbol)
        if not optionable_symbols:
            return Top10StructureResolution(
                (),
                reason_codes=("NO_OPTIONABLE_UNDERLYINGS",),
            )
        underlying_read = _read_underlying_quotes(
            self.gateway,
            self.pacing,
            optionable_symbols,
            indicative=self.indicative_underlyings,
        )
        if not underlying_read.complete:
            return Top10StructureResolution(
                (),
                reason_codes=underlying_read.reason_codes,
                missing_symbols=underlying_read.missing_symbols,
            )
        underlying_rows = underlying_read.rows
        if not underlying_rows:
            return Top10StructureResolution(
                (),
                reason_codes=("UNDERLYING_QUOTE_EMPTY",),
                missing_symbols=underlying_read.requested_symbols,
            )
        try:
            quote_verified_at = utc_datetime(
                self._clock(),
                field="underlying quote verification time",
            )
        except (TypeError, ValueError):
            return Top10StructureResolution(
                (),
                reason_codes=("UNDERLYING_QUOTE_CLOCK_INVALID",),
                missing_symbols=underlying_read.requested_symbols,
            )
        validated_underlyings: list[tuple[object, UnderlyingQuoteBasis]] = []
        basis_rejections: list[tuple[str, str]] = []
        seen_underlying_contracts: set[int] = set()
        for row in underlying_rows:
            symbol = str(getattr(row, "symbol", "")).strip().upper()
            basis, reason = _direct_underlying_quote_basis(
                row,
                expected_symbol=symbol,
                verified_at=quote_verified_at,
                allow_indicative=self.indicative_underlyings,
            )
            if basis is not None and basis.contract_id in seen_underlying_contracts:
                basis = None
                reason = "UNDERLYING_QUOTE_IDENTITY_MISMATCH"
            if basis is None:
                basis_rejections.append(
                    (
                        symbol or "UNKNOWN",
                        reason or "UNDERLYING_QUOTE_BASIS_INVALID",
                    )
                )
                continue
            seen_underlying_contracts.add(basis.contract_id)
            validated_underlyings.append((row, basis))
        if not validated_underlyings:
            return Top10StructureResolution(
                (),
                reason_codes=tuple(
                    dict.fromkeys(
                        reason for _symbol_value, reason in basis_rejections
                    )
                )
                or ("UNDERLYING_QUOTE_BASIS_UNAVAILABLE",),
                missing_symbols=tuple(
                    dict.fromkeys(
                        (
                            *underlying_read.failed_symbols,
                            *(symbol for symbol, _reason in basis_rejections),
                        )
                    )
                ),
            )
        excluded_quote_reasons = [
            *((symbol, "UNDERLYING_QUOTE_FAILED") for symbol in underlying_read.failed_symbols),
            *basis_rejections,
            *((symbol, "OPTIONABILITY_UNAVAILABLE") for symbol in optionability.excluded_symbols),
        ]
        source_scan_by_symbol: dict[str, str] = {}
        metadata_by_symbol: dict[str, Mapping[str, object]] = {}
        for row in (*scanner_docs, *core_docs):
            symbol = str(row["symbol"])
            source_scan_by_symbol.setdefault(symbol, str(row["source_scan"]))
            metadata_by_symbol.setdefault(
                symbol,
                {
                    "symbol": symbol,
                    "source_scan": str(row["source_scan"]),
                    "industry": str(row.get("industry", "") or ""),
                    "category": str(row.get("category", "") or ""),
                    "subcategory": str(row.get("subcategory", "") or ""),
                },
            )

        structures: list[ResolvedStructure] = []
        seen_underlyings: set[str] = set()
        for underlying, underlying_basis in validated_underlyings:
            if len(structures) >= self.maximum_structures:
                break
            symbol = str(getattr(underlying, "symbol", "")).strip().upper()
            source_scan = source_scan_by_symbol.get(symbol)
            if source_scan not in {
                "MOST_ACTIVE",
                "TOP_PERC_GAIN",
                "TOP_PERC_LOSE",
                "CORE_UNIVERSE",
            }:
                excluded_quote_reasons.append(
                    (symbol, "DISCOVERY_PROVENANCE_UNAVAILABLE")
                )
                continue
            spot = underlying_basis.market_price
            if not symbol or symbol in seen_underlyings or spot is None or spot <= 0:
                continue
            expirations = expirations_by_symbol.get(symbol, ())
            # Use the same standard-monthly preference as the ordinary funnel
            # so research and executable qualification do not select different
            # sparse weekly strike grids from the same aggregate SecDef sets.
            chain = _preferred_option_expiration(expirations)
            if chain is None or int(getattr(chain, "multiplier", 0)) != 100:
                excluded_quote_reasons.append(
                    (symbol, "OPTION_CHAIN_IDENTITY_INVALID")
                )
                continue

            selected = _preferred_directional_vertical(
                chain.strikes,
                spot,
                close=underlying_basis.close,
            )
            if selected is None:
                excluded_quote_reasons.append(
                    (symbol, "DIRECTIONAL_VERTICAL_UNAVAILABLE")
                )
                continue
            right, strikes = selected
            with _gateway_request_lease(
                self.gateway,
                self.pacing,
                "secdef",
            ) as decision:
                if not bool(getattr(decision, "allowed", False)):
                    missing = tuple(
                        item
                        for item in optionable_symbols
                        if item not in seen_underlyings
                    )
                    return Top10StructureResolution(
                        (),
                        reason_codes=_pacing_denial_reasons(
                            "OPTION_QUALIFICATION",
                            decision,
                        ),
                        missing_symbols=missing,
                    )
                try:
                    contracts = tuple(
                        self.gateway.qualify_option_contracts(
                            symbol,
                            chain.expiration,
                            strikes,
                            exchange=chain.exchange,
                            trading_class=chain.trading_class,
                            rights=(right,),
                        )
                    )
                except MarketDataPacingError as exc:
                    missing = tuple(
                        item
                        for item in optionable_symbols
                        if item not in seen_underlyings
                    )
                    return Top10StructureResolution(
                        (),
                        reason_codes=_pacing_error_reasons(
                            "OPTION_QUALIFICATION",
                            exc,
                        ),
                        missing_symbols=missing,
                    )
                except Exception as exc:
                    excluded_quote_reasons.append(
                        (symbol, _option_qualification_failure_reason(exc))
                    )
                    continue
            ordered = _ordered_vertical_contracts(contracts, right)
            if len(ordered) != 2:
                excluded_quote_reasons.append(
                    (symbol, "OPTION_QUALIFICATION_INCOMPLETE")
                )
                continue
            if any(
                item.symbol.strip().upper() != symbol
                or item.expiration != chain.expiration
                or item.right != right
                or item.multiplier != 100
                or item.currency.upper() != "USD"
                for item in ordered
            ):
                excluded_quote_reasons.append(
                    (symbol, "OPTION_QUALIFICATION_INVALID_RESPONSE")
                )
                continue

            dte = (chain.expiration - asof).days
            if dte < 14 or dte > 35:
                continue
            legs = tuple(
                _direct_top10_leg(
                    contract,
                    side=(
                        OptionLegSide.BUY if index == 0 else OptionLegSide.SELL
                    ),
                    dte=dte,
                )
                for index, contract in enumerate(ordered)
            )
            strategy_hash = strategy_structure_hash(
                symbol,
                "DEBIT_VERTICAL",
                legs,
            )
            exclusion_evidence = tuple(
                (
                    f"IBKR_DIRECT_SYMBOL_EXCLUDED:{excluded_symbol}",
                    canonical_hash(
                        {
                            "schema": "options_copilot.direct_top10_symbol_exclusion.v1",
                            "scheduled_for": slot,
                            "excluded_symbol": excluded_symbol,
                            "reason_code": reason,
                        }
                    ),
                )
                for excluded_symbol, reason in dict.fromkeys(
                    excluded_quote_reasons
                )
            )
            source_evidence = {
                "schema": "options_copilot.direct_top10_discovery.v1",
                "scheduled_for": slot,
                "underlying": symbol,
                "source_scan": source_scan,
                "underlying_quote_basis": underlying_basis.as_dict(),
                "underlying_quote_basis_hash": underlying_basis.basis_hash,
                "strategy_hash": strategy_hash,
                "underlying_quote_excluded_symbols": tuple(
                    symbol for symbol, _reason in excluded_quote_reasons
                ),
            }
            metadata = dict(metadata_by_symbol.get(symbol, {}))
            metadata.update(
                {
                    "underlying_quote_basis": underlying_basis.as_dict(),
                    "underlying_quote_basis_hash": underlying_basis.basis_hash,
                }
            )
            metadata_by_symbol[symbol] = metadata
            evidence_hash = canonical_hash(source_evidence)
            preselection_id = "direct-top10." + hashlib.sha256(
                f"{slot.isoformat()}|{symbol}|{strategy_hash}".encode("utf-8")
            ).hexdigest()[:24]
            candidate = ConditionalOptionPreselection(
                preselection_id=preselection_id,
                underlying=symbol,
                strategy_type="DEBIT_VERTICAL",
                phase=PreselectionPhase.PRE_MARKET,
                legs=legs,
                risk_defined=True,
                maximum_loss_usd=None,
                estimated_cost_usd=None,
                cost_after_ev_usd=None,
                entry_condition=(
                    "Review only after the 09:35 ET atomic IBKR reprice and "
                    "all hard eligibility gates remain valid."
                ),
                invalidation_condition=(
                    "The directional thesis or fresh IBKR market evidence no "
                    "longer supports this bounded vertical."
                ),
                profit_target_condition=(
                    "Human review is required before any profit-taking action."
                ),
                stop_loss_condition=(
                    "Human review is required before the defined maximum loss."
                ),
                evidence_ids=(
                    f"IBKR_DIRECT_DISCOVERY:{symbol}:{chain.expiration.isoformat()}",
                    (
                        "IBKR_UNDERLYING_QUOTE_BASIS:"
                        f"{symbol}:{underlying_basis.contract_id}"
                    ),
                    *(item[0] for item in exclusion_evidence),
                ),
                evidence_hashes=(
                    evidence_hash,
                    underlying_basis.basis_hash,
                    *(item[1] for item in exclusion_evidence),
                ),
                strategy_hash=strategy_hash,
                research_summary=(
                    "Bounded IBKR read-only discovery with exact contract "
                    "identities; dynamic quotes and economics are intentionally "
                    "deferred to one atomic broker snapshot."
                ),
                underlying_quote_basis=(
                    None if self.indicative_underlyings else underlying_basis
                ),
                underlying_quote_basis_hash=(
                    None
                    if self.indicative_underlyings
                    else underlying_basis.basis_hash
                ),
            )
            structures.append(ResolvedStructure(candidate))
            seen_underlyings.add(symbol)
        self._last_discovery_metadata = tuple(
            dict(metadata_by_symbol.get(item.candidate.underlying, {}))
            for item in structures
        )
        if not structures and excluded_quote_reasons:
            normalized_rejections = tuple(dict.fromkeys(excluded_quote_reasons))
            return Top10StructureResolution(
                (),
                reason_codes=tuple(
                    dict.fromkeys(reason for _symbol, reason in normalized_rejections)
                ),
                missing_symbols=tuple(
                    dict.fromkeys(symbol for symbol, _reason in normalized_rejections)
                ),
            )
        return Top10StructureResolution(
            tuple(structures),
            reason_codes=discovery.reason_codes,
        )


class ProductionOptionsEvidenceAcquisition:
    """Resolve only exact leg identities already frozen by the bounded funnel."""

    def resolve_contracts(self, *, universe: object = None, finalists: object = None, **_: object) -> tuple[OptionContractRef, ...]:
        source = finalists
        if source is None and isinstance(universe, Mapping):
            source = universe.get("finalists", ())
        if not isinstance(source, Sequence) or isinstance(source, (str, bytes, bytearray)):
            return ()
        contracts: dict[int, OptionContractRef] = {}
        try:
            for finalist in source:
                if not isinstance(finalist, Mapping):
                    return ()
                legs = finalist.get("legs", ())
                if not isinstance(legs, Sequence) or isinstance(legs, (str, bytes, bytearray)):
                    return ()
                for leg in legs:
                    contract = _contract_from_leg(leg)
                    prior = contracts.get(contract.contract_id)
                    if prior is not None and prior != contract:
                        return ()
                    contracts[contract.contract_id] = contract
        except (KeyError, TypeError, ValueError):
            return ()
        if not contracts or len(contracts) > 50:
            return ()
        return tuple(contracts[key] for key in sorted(contracts))

    def acquire(self, **kwargs: object) -> Mapping[str, object]:
        contracts = self.resolve_contracts(**kwargs)
        return {"contracts": contracts, "reasons": () if contracts else ("OPTION_CONTRACTS_UNRESOLVED",)}

    run = acquire
    build = acquire


class ProductionBrokerEvidenceAcquisition:
    """Build one atomic broker snapshot and expose only hash-bound evidence."""

    def __init__(
        self,
        broker_snapshot_builder: BrokerSnapshotBuilder,
        options_evidence_acquisition: ProductionOptionsEvidenceAcquisition,
        evidence_store: EvidenceStore,
        pacing: GuardedRequestBudget,
        strategy_nav_source: object,
        *,
        execution_cost_contract: Mapping[str, object],
        policy_resolver: object,
        feature_source_resolver: object | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.broker_snapshot_builder = broker_snapshot_builder
        self.options_evidence_acquisition = options_evidence_acquisition
        self.evidence_store = evidence_store
        self.pacing = pacing
        self.strategy_nav_source = strategy_nav_source
        self.execution_cost_contract = execution_cost_contract
        self.policy_resolver = policy_resolver
        self.feature_source_resolver = feature_source_resolver
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._feature_source_lock = threading.RLock()
        self._feature_source_read_model = self._publish_feature_source_bindings(
            scan_run_id=None,
            cutoff=None,
        )
        self._positioning_lock = threading.RLock()
        self._positioning_read_model = _positioning_unavailable(
            ("NO_POSITIONING_SAMPLE",)
        )

    def positioning(self) -> Mapping[str, object]:
        """Return the latest finalist-leg positioning sample, display only."""

        with self._positioning_lock:
            payload = dict(self._positioning_read_model)
            rows = payload.get("positioning", ())
            payload["positioning"] = tuple(
                dict(item) for item in rows if isinstance(item, Mapping)
            )
            return payload

    read_positioning = positioning

    def feature_source_bindings(self) -> Mapping[str, object]:
        """Read a detached cache-only projection of the last acquisition lane."""

        with self._feature_source_lock:
            return deepcopy(self._feature_source_read_model)

    def _publish_feature_source_bindings(
        self,
        *,
        scan_run_id: str | None,
        cutoff: datetime | None,
        bindings: Sequence[Mapping[str, object]] = (),
        reasons: Sequence[str] | None = None,
    ) -> dict[str, object]:
        not_run = reasons is None
        if reasons is None:
            reasons = (
                "FEATURE_SOURCE_RESOLVER_UNWIRED"
                if self.feature_source_resolver is None
                else "FEATURE_SOURCE_CONSUMER_NOT_RUN",
            )
        payload: dict[str, object] = {
            "schema": "options_copilot.production_feature_source_bindings.v1",
            "scan_run_id": scan_run_id,
            "cutoff": None if cutoff is None else cutoff.isoformat(),
            "status": "WIRED_NOT_RUN" if not_run else "INCOMPLETE",
            "bindings": tuple(deepcopy(dict(row)) for row in bindings),
            "reason_codes": tuple(dict.fromkeys(reasons)),
            "decision_authority": "OBSERVATION_ONLY",
            "model_input_complete": False,
            "production_eligible": False,
            "affects_eligibility": False,
            "approval_eligible": False,
            "instruction_creation_allowed": False,
            "order_allowed": False,
        }
        with self._feature_source_lock:
            self._feature_source_read_model = deepcopy(payload)
        return payload

    def _resolve_feature_source_bindings(
        self,
        *,
        scan_run_id: str,
        contracts: Sequence[OptionContractRef],
        histories: Mapping[str, UnderlyingIvHistory],
    ) -> dict[str, object]:
        cutoff = utc_datetime(self._clock(), field="feature source binding clock")
        if self.feature_source_resolver is None:
            return self._publish_feature_source_bindings(
                scan_run_id=scan_run_id, cutoff=cutoff,
            )
        bindings: list[Mapping[str, object]] = []
        reasons: list[str] = ["FEATURE_PRODUCTION_AUTHORITY_UNRESOLVED"]
        try:
            resolver = getattr(self.feature_source_resolver, "resolve", None)
        except Exception:
            resolver = None
        if not callable(resolver):
            reasons.append("FEATURE_SOURCE_RESOLVER_INVALID")
        else:
            requested = sorted({(item.symbol, item.expiration) for item in contracts})
            # Bound diagnostic work independently of the option leg count.
            if len(requested) > 32:
                reasons.append("FEATURE_SOURCE_BINDING_LIMIT_EXCEEDED")
            for symbol, expiration in requested[:32]:
                history = histories[symbol]
                if not _feature_source_history_identity_valid(
                    history, symbol=symbol, cutoff=cutoff,
                ):
                    reasons.append("FEATURE_SOURCE_HISTORY_IDENTITY_INVALID")
                    continue
                try:
                    raw = resolver(
                        symbol=symbol, con_id=history.contract_id,
                        expiration=expiration, cutoff=cutoff,
                    )
                except Exception:
                    reasons.append("FEATURE_SOURCE_RESOLVER_FAILED")
                    continue
                try:
                    binding = validate_feature_source_binding(
                        raw, symbol=symbol, con_id=history.contract_id,
                        expiration=expiration, cutoff=cutoff,
                        require_current_convention=True,
                    )
                except Exception:
                    reasons.append("FEATURE_SOURCE_BINDING_INVALID")
                    continue
                bindings.append(binding)
                reasons.extend(binding["reason_codes"])
        return self._publish_feature_source_bindings(
            scan_run_id=scan_run_id, cutoff=cutoff,
            bindings=bindings, reasons=reasons,
        )

    def acquire(
        self,
        *,
        scan_run_id: str,
        universe: object,
        context: Mapping[str, object],
        resolved_policy: object | None = None,
        **_: object,
    ) -> Mapping[str, object]:
        feature_bindings = self._publish_feature_source_bindings(
            scan_run_id=scan_run_id, cutoff=None,
        )
        contract_reader = getattr(
            self.policy_resolver,
            "policy_contract_document",
            None,
        )
        if not callable(contract_reader):
            self._publish_positioning_unavailable(
                ("CURRENT_POLICY_CONTRACT_UNAVAILABLE",)
            )
            return {
                "reasons": ("CURRENT_POLICY_CONTRACT_UNAVAILABLE",),
                "feature_source_bindings": feature_bindings,
            }
        try:
            policy_contract = contract_reader(resolved_policy)
        except Exception:
            policy_contract = None
        if not isinstance(policy_contract, Mapping):
            self._publish_positioning_unavailable(
                ("CURRENT_POLICY_CONTRACT_UNAVAILABLE",)
            )
            return {
                "reasons": ("CURRENT_POLICY_CONTRACT_UNAVAILABLE",),
                "feature_source_bindings": feature_bindings,
            }
        if not self.pacing.ready:
            self._publish_positioning_unavailable((PACING_CAPABILITY_MISSING,))
            return {
                "reasons": (PACING_CAPABILITY_MISSING,),
                "feature_source_bindings": feature_bindings,
            }
        contracts = self.options_evidence_acquisition.resolve_contracts(universe=universe)
        if not contracts:
            self._publish_positioning_unavailable(("OPTION_CONTRACTS_UNRESOLVED",))
            return {
                "reasons": ("OPTION_CONTRACTS_UNRESOLVED",),
                "feature_source_bindings": feature_bindings,
            }
        symbols = tuple(sorted({item.symbol for item in contracts}))
        history_reader = getattr(
            getattr(self.broker_snapshot_builder, "source", None),
            "underlying_iv_history",
            None,
        )
        if not callable(history_reader):
            reasons = ("IV_HISTORY_SOURCE_UNAVAILABLE",)
            self._publish_positioning_unavailable(reasons)
            return {"reasons": reasons, "feature_source_bindings": feature_bindings}
        # Daily IV history is independent of the executable option BBO.  Read
        # it first so the subsequent atomic broker snapshot remains inside the
        # five-second quote gate when strategy generation and ranking begin.
        history_end_at = utc_datetime(self._clock(), field="broker evidence clock")
        iv_histories: dict[str, UnderlyingIvHistory] = {}
        for symbol in symbols:
            try:
                value = history_reader(symbol, end_at=history_end_at)
            except MarketDataPacingError as exc:
                reasons = (f"HISTORICAL_{exc.reason_code}",)
                self._publish_positioning_unavailable(reasons)
                return {"reasons": reasons, "feature_source_bindings": feature_bindings}
            except (ArithmeticError, OSError, RuntimeError, TypeError, ValueError):
                reasons = ("IV_HISTORY_UNAVAILABLE",)
                self._publish_positioning_unavailable(reasons)
                return {"reasons": reasons, "feature_source_bindings": feature_bindings}
            if not isinstance(value, UnderlyingIvHistory):
                reasons = ("IV_HISTORY_UNAVAILABLE",)
                self._publish_positioning_unavailable(reasons)
                return {"reasons": reasons, "feature_source_bindings": feature_bindings}
            iv_histories[symbol] = value
        # Local source references are resolved before the executable snapshot;
        # they neither replace this legacy history nor supply model D/V inputs.
        feature_bindings = self._resolve_feature_source_bindings(
            scan_run_id=scan_run_id, contracts=contracts, histories=iv_histories,
        )
        snapshot = self.broker_snapshot_builder.build(contracts)
        if (
            not isinstance(snapshot, AtomicBrokerSnapshot)
            or not snapshot.verify_hash()
            or snapshot.status is not BrokerSnapshotStatus.COMPLETE
        ):
            reasons = tuple(getattr(snapshot, "reason_codes", ())) or ("BROKER_SNAPSHOT_INCOMPLETE",)
            self._publish_positioning_unavailable(reasons)
            return {
                "broker_snapshot": snapshot,
                "broker_snapshot_hash": getattr(snapshot, "snapshot_hash", None),
                "feature_source_bindings": feature_bindings,
                "reasons": reasons,
            }
        secdefs = _secdefs_from_snapshot(snapshot)
        quote_batch = _quote_batch_from_snapshot(snapshot)
        if len(secdefs) != len(contracts) or quote_batch is None:
            self._publish_positioning_unavailable(
                ("BROKER_EVIDENCE_RECONSTRUCTION_FAILED",)
            )
            return {
                "broker_snapshot": snapshot,
                "broker_snapshot_hash": snapshot.snapshot_hash,
                "reasons": ("BROKER_EVIDENCE_RECONSTRUCTION_FAILED",),
                "feature_source_bindings": feature_bindings,
            }
        # Selected-leg IVs are neither an ATM surface nor the native current
        # comparator confirmed for the 252-session percentile. Until those
        # sources are resolved, retain the real snapshot but do not derive or
        # persist hard volatility evidence from the legacy aggregate. The
        # server clock, not a caller's cutoff or self-attested source flags,
        # owns this boundary; pre-confirmation fixtures remain reproducible.
        if utc_datetime(
            self._clock(), field="production IV source cutoff",
        ) >= IV_PERCENTILE_CONFIRMATION_OBSERVED_AT:
            reasons = (
                "ATM_SURFACE_SOURCE_UNVERIFIED",
                "IV_BASIS_UNRESOLVED",
                "FEATURE_PRODUCTION_AUTHORITY_UNRESOLVED",
            )
            self._publish_positioning_sample(
                scan_run_id=scan_run_id,
                universe=universe,
                contracts=contracts,
                snapshot=snapshot,
            )
            return {
                "broker_snapshot": snapshot,
                "broker_snapshot_hash": snapshot.snapshot_hash,
                "feature_source_bindings": feature_bindings,
                "reasons": reasons,
            }
        secdefs_by_id = {item.contract_id: item for item in secdefs}
        quotes_by_id = {item.contract_id: item for item in snapshot.quotes}
        volatility_by_underlying: dict[str, Mapping[str, object]] = {}
        atm_iv_by_underlying: dict[str, Decimal] = {}
        for symbol in symbols:
            contract_ids = tuple(
                item.contract_id for item in contracts if item.symbol == symbol
            )
            symbol_secdefs = tuple(
                secdefs_by_id[contract_id]
                for contract_id in contract_ids
                if contract_id in secdefs_by_id
            )
            symbol_quotes = tuple(
                quotes_by_id[contract_id]
                for contract_id in contract_ids
                if contract_id in quotes_by_id
            )
            ivs = tuple(
                item.implied_volatility
                for item in symbol_quotes
                if item.implied_volatility is not None
            )
            atm_iv = (
                None
                if not ivs
                else sum(ivs, Decimal("0")) / Decimal(len(ivs))
            )
            history = iv_histories[symbol]
            history_reasons = _validate_underlying_iv_history(
                history,
                symbol=symbol,
                snapshot=snapshot,
                atm_iv=atm_iv,
            )
            if history_reasons:
                self._publish_positioning_unavailable(history_reasons)
                return {
                    "broker_snapshot": snapshot,
                    "broker_snapshot_hash": snapshot.snapshot_hash,
                    "reasons": history_reasons,
                    "feature_source_bindings": feature_bindings,
                }
            assert atm_iv is not None
            atm_iv_by_underlying[symbol] = atm_iv
            quote_documents = tuple(
                _quote_document(item) for item in symbol_quotes
            )
            volatility_by_underlying[symbol] = {
                "source": "IBKR",
                "observed_at": snapshot.built_at,
                "secdef_hash": canonical_hash(
                    tuple(item.identity_dict() for item in symbol_secdefs)
                ),
                "quote_hash": canonical_hash(quote_documents),
                "quotes": quote_documents,
                "atm_iv": atm_iv,
                "iv_history": tuple(item.close for item in history.points),
                "iv_history_basis_hash": history.basis_hash,
                "iv_history_content_hash": history.content_hash,
            }
        nav, nav_reasons = self._strategy_nav_for_snapshot(snapshot)
        if nav is None:
            self._publish_positioning_unavailable(nav_reasons)
            return {
                "broker_snapshot": snapshot,
                "broker_snapshot_hash": snapshot.snapshot_hash,
                "reasons": nav_reasons,
                "feature_source_bindings": feature_bindings,
            }
        guard = getattr(self.strategy_nav_source, "guard_current", None)
        if not callable(guard):
            reasons = ("STRATEGY_NAV_GUARD_UNAVAILABLE",)
            self._publish_positioning_unavailable(reasons)
            return {
                "broker_snapshot": snapshot,
                "broker_snapshot_hash": snapshot.snapshot_hash,
                "reasons": reasons,
                "feature_source_bindings": feature_bindings,
            }
        single_history = iv_histories[symbols[0]] if len(symbols) == 1 else None
        iv_history_basis_hash = (
            single_history.basis_hash
            if single_history is not None
            else canonical_hash(
                tuple(
                    (symbol, iv_histories[symbol].basis_hash)
                    for symbol in symbols
                )
            )
        )
        iv_history_content_hash = (
            single_history.content_hash
            if single_history is not None
            else canonical_hash(
                tuple(
                    (symbol, iv_histories[symbol].content_hash)
                    for symbol in symbols
                )
            )
        )

        def persist_broker_evidence() -> object:
            return self.evidence_store.append(
                EvidenceRecord(
                    identity=f"broker-snapshot:{scan_run_id}:{snapshot.snapshot_hash}",
                    kind="BROKER_SNAPSHOT",
                    symbol=_single_symbol(contracts),
                    provider="IBKR",
                    source_id=snapshot.snapshot_hash,
                    published_at=snapshot.built_at,
                    first_seen_at=snapshot.built_at,
                    ingested_at=snapshot.built_at,
                    observed_at=snapshot.built_at,
                    payload={
                        "snapshot_hash": snapshot.snapshot_hash,
                        "feature_source_bindings": feature_bindings,
                        "status": snapshot.status.value,
                        "quote_batch_id": snapshot.quote_batch_id,
                        "contract_ids": tuple(
                            item.contract_id for item in contracts
                        ),
                        "contract_quote_evidence": tuple(
                            _broker_contract_quote_evidence(
                                secdefs_by_id[item.contract_id],
                                quotes_by_id[item.contract_id],
                            )
                            for item in sorted(
                                contracts,
                                key=lambda value: value.contract_id,
                            )
                        ),
                        "oldest_quote_age_seconds": (
                            snapshot.oldest_quote_age_seconds
                        ),
                        "maximum_leg_skew_seconds": (
                            snapshot.maximum_leg_skew_seconds
                        ),
                        "strategy_nav_content_hash": nav.content_hash,
                        "strategy_nav_authority_hash": nav.authority_hash,
                        "strategy_nav_ledger_head_hash": nav.ledger_head_hash,
                        "iv_history_basis_hash": iv_history_basis_hash,
                        "iv_history_content_hash": iv_history_content_hash,
                        "iv_history_end_at": (
                            single_history.end_at
                            if single_history is not None
                            else None
                        ),
                        "iv_history_points": tuple(
                            {
                                "trading_date": item.trading_date,
                                "close": item.close,
                            }
                            for item in (
                                single_history.points
                                if single_history is not None
                                else ()
                            )
                        ),
                        "iv_histories": tuple(
                            {
                                "symbol": symbol,
                                "basis_hash": iv_histories[symbol].basis_hash,
                                "content_hash": iv_histories[symbol].content_hash,
                                "end_at": iv_histories[symbol].end_at,
                                "points": tuple(
                                    {
                                        "trading_date": item.trading_date,
                                        "close": item.close,
                                    }
                                    for item in iv_histories[symbol].points
                                ),
                            }
                            for symbol in symbols
                        ),
                    },
                )
            )

        guarded = guard(nav, callback=persist_broker_evidence)
        stored = getattr(guarded, "evidence", None)
        if stored is None:
            reasons = ("STRATEGY_NAV_HEAD_CHANGED",)
            self._publish_positioning_unavailable(reasons)
            return {
                "broker_snapshot": snapshot,
                "broker_snapshot_hash": snapshot.snapshot_hash,
                "reasons": reasons,
                "feature_source_bindings": feature_bindings,
            }
        liquidity_hash = canonical_hash(
            tuple(
                {
                    "con_id": item.contract_id,
                    "bid": item.bid,
                    "ask": item.ask,
                    "volume": item.volume,
                    "open_interest": item.open_interest,
                }
                for item in snapshot.quotes
            )
        )
        volatility_hash = canonical_hash(
            {
                "option_surface": tuple(
                    {
                        "con_id": item.contract_id,
                        "iv": item.implied_volatility,
                        "delta": item.delta,
                        "gamma": item.gamma,
                        "theta": item.theta,
                        "vega": item.vega,
                    }
                    for item in snapshot.quotes
                ),
                "iv_history_content_hash": iv_history_content_hash,
                "iv_history_basis_hash": iv_history_basis_hash,
                "volatility_by_underlying": volatility_by_underlying,
            }
        )
        evidence_hashes = {
            "MARKET": stored.content_hash,
            "VOLATILITY": volatility_hash,
            "LIQUIDITY": liquidity_hash,
        }
        spot_by_underlying: dict[str, Decimal] = {}
        for item in (
            universe.get("finalists", ()) if isinstance(universe, Mapping) else ()
        ):
            if not isinstance(item, Mapping):
                continue
            symbol = str(
                item.get("symbol") or item.get("underlying") or ""
            ).strip().upper()
            spot_value = _decimal(item.get("spot"))
            if symbol and spot_value is not None and spot_value > 0:
                spot_by_underlying[symbol] = spot_value
        spot = (
            spot_by_underlying.get(symbols[0])
            if len(symbols) == 1
            else None
        )
        self._publish_positioning_sample(
            scan_run_id=scan_run_id,
            universe=universe,
            contracts=contracts,
            snapshot=snapshot,
        )
        return {
            "snapshot_hash": snapshot.snapshot_hash,
            "broker_snapshot_hash": snapshot.snapshot_hash,
            "feature_source_bindings": feature_bindings,
            "evidence_hash": canonical_hash(
                {
                    "broker_snapshot_hash": snapshot.snapshot_hash,
                    "stored_evidence_hash": stored.content_hash,
                    "evidence_hashes": evidence_hashes,
                }
            ),
            "contracts": contracts,
            "secdefs": secdefs,
            "quote_batch": quote_batch,
            "nav_snapshot": nav,
            "broker_snapshot": snapshot,
            "execution_cost_contract": self.execution_cost_contract,
            "policy_contract": policy_contract,
            "evidence_hashes": evidence_hashes,
            "spot": spot,
            "spot_by_underlying": spot_by_underlying,
            "atm_iv": (
                atm_iv_by_underlying.get(symbols[0])
                if len(symbols) == 1
                else None
            ),
            "atm_iv_by_underlying": atm_iv_by_underlying,
            "iv_history": tuple(
                item.close for item in single_history.points
            ) if single_history is not None else (),
            "iv_history_basis_hash": iv_history_basis_hash,
            "iv_history_content_hash": iv_history_content_hash,
            "volatility_by_underlying": volatility_by_underlying,
            "source": "IBKR",
            "observed_at": snapshot.built_at,
            "secdef_hash": canonical_hash(tuple(item.identity_dict() for item in secdefs)),
            "quote_hash": canonical_hash(tuple(_quote_document(item) for item in snapshot.quotes)),
            "quotes": tuple(_quote_document(item) for item in snapshot.quotes),
            "reasons": (),
        }

    def _strategy_nav_for_snapshot(
        self,
        snapshot: AtomicBrokerSnapshot,
    ) -> tuple[StrategyNavSnapshot | None, tuple[str, ...]]:
        reader = getattr(self.strategy_nav_source, "snapshot", None)
        if not callable(reader):
            return None, ("STRATEGY_NAV_SOURCE_UNAVAILABLE",)
        try:
            broker_nlv = atomic_account_nlv(snapshot)
            nav = reader(
                asof=snapshot.built_at,
                observed_account_nlv=broker_nlv,
            )
        except (ArithmeticError, OSError, TypeError, ValueError):
            return None, ("STRATEGY_NAV_RECONCILIATION_UNAVAILABLE",)
        if (
            not isinstance(nav, StrategyNavSnapshot)
            or not nav.valid
            or nav.strategy_nav is None
            or not nav.strategy_nav.is_finite()
            or nav.strategy_nav <= 0
            or nav.asof != snapshot.built_at
            or nav.observed_account_nlv != broker_nlv
            or nav.reconciliation_difference is None
            or not nav.reconciliation_difference.is_finite()
            or not _digest(nav.content_hash)
            or canonical_hash(nav.hash_payload()) != nav.content_hash
            or not _digest(nav.authority_hash)
            or not _digest(nav.contract_hash)
            or not _digest(nav.ledger_head_hash)
        ):
            return None, ("STRATEGY_NAV_RECONCILIATION_INVALID",)
        expected = (broker_nlv - nav.strategy_nav).quantize(
            Decimal("0.01"),
            rounding=ROUND_HALF_EVEN,
        )
        if nav.reconciliation_difference != expected:
            return None, ("STRATEGY_NAV_RECONCILIATION_INVALID",)
        return nav, ()

    def _publish_positioning_unavailable(
        self,
        reasons: Sequence[object],
    ) -> None:
        with self._positioning_lock:
            self._positioning_read_model = _positioning_unavailable(reasons)

    def _publish_positioning_sample(
        self,
        *,
        scan_run_id: str,
        universe: object,
        contracts: Sequence[OptionContractRef],
        snapshot: AtomicBrokerSnapshot,
    ) -> None:
        spots = _finalist_spots(universe)
        contract_by_id = {item.contract_id: item for item in contracts}
        grouped: dict[tuple[str, date], list[OptionQuoteSnapshot]] = {}
        for quote in snapshot.quotes:
            contract = contract_by_id.get(quote.contract_id)
            if contract is None:
                continue
            grouped.setdefault((contract.symbol, contract.expiration), []).append(
                OptionQuoteSnapshot(
                    contract=contract,
                    observed_at=quote.observed_at,
                    exchange_time=quote.exchange_time,
                    bid=quote.bid,
                    ask=quote.ask,
                    last=quote.last,
                    close=quote.close,
                    volume=quote.volume,
                    open_interest=quote.open_interest,
                    implied_volatility=quote.implied_volatility,
                    delta=quote.delta,
                    gamma=quote.gamma,
                    theta=quote.theta,
                    vega=quote.vega,
                    market_data_type=quote.market_data_type,
                )
            )

        projections: list[dict[str, object]] = []
        for (symbol, expiration), rows in sorted(grouped.items()):
            spot = spots.get(symbol)
            if spot is None or spot <= 0:
                continue
            try:
                projection = project_positioning(
                    rows,
                    underlying_spot=spot,
                    now=snapshot.built_at,
                    expected_contract_count=None,
                )
            except (ArithmeticError, TypeError, ValueError):
                continue
            projections.append(
                {
                    **projection.to_dict(),
                    "expiration": expiration,
                    "scan_run_id": scan_run_id,
                    "source": "IBKR_ATOMIC_BROKER_SNAPSHOT",
                    "broker_snapshot_hash": snapshot.snapshot_hash,
                    "chain_scope": "FROZEN_FINALIST_LEGS_ONLY",
                    "coverage_limitation": (
                        "Candidate legs are not a complete option chain; Max Pain, "
                        "walls, PCR, and GEX remain partial supporting evidence."
                    ),
                }
            )

        reasons = () if projections else ("POSITIONING_SAMPLE_UNAVAILABLE",)
        aggregate = {
            "schema_version": "options_copilot.positioning_feed.v1",
            "status": "DEGRADED" if projections else "UNAVAILABLE",
            "reasons": reasons,
            "positioning": tuple(projections),
            "count": len(projections),
            "decision_authority": "SUPPORTING_ONLY",
            "supporting_only": True,
            "affects_eligibility": False,
            "approval_allowed": False,
            "instruction_allowed": False,
            "order_allowed": False,
        }
        with self._positioning_lock:
            self._positioning_read_model = aggregate

    run = acquire
    build = acquire


class ProductionRiskGate:
    """Compute exact per-candidate loss fractions; never relabel A-grade."""

    def evaluate(self, *, candidates: Sequence[Mapping[str, object]], **_: object) -> Mapping[str, object]:
        fractions: dict[str, Decimal] = {}
        reasons: list[str] = []
        for item in candidates:
            candidate_id = str(item.get("candidate_id", "")).strip()
            loss = _decimal(item.get("max_loss_usd"))
            nav = _decimal(item.get("strategy_nav_usd"))
            if not candidate_id or loss is None or loss < 0 or nav is None or nav <= 0:
                reasons.append("RISK_INPUT_INVALID")
                continue
            fraction = loss / nav
            fractions[candidate_id] = fraction
            if fraction > Decimal("0.15"):
                reasons.append("RISK_ABOVE_15_PERCENT_REJECT_LINE")
            if fraction >= Decimal("0.20"):
                reasons.append("HARD_20_PERCENT_REJECT_LINE")
        eligible = len(fractions) == len(candidates) and bool(candidates) and not reasons
        return {
            "eligible": eligible,
            "risk_fractions": fractions,
            "risk_fraction": next(iter(fractions.values())) if len(fractions) == 1 else None,
            "reasons": tuple(dict.fromkeys(reasons)),
        }

    assess = evaluate
    run = evaluate


class ProductionTop10SnapshotProvider:
    """Apply the shared production pacing budget to one Top-10 snapshot batch."""

    def __init__(
        self,
        broker_snapshot_builder: BrokerSnapshotBuilder,
        pacing: GuardedRequestBudget,
        *,
        batch_lock: threading.RLock | None = None,
    ) -> None:
        if not callable(getattr(broker_snapshot_builder, "build", None)):
            raise TypeError("broker_snapshot_builder must expose build()")
        self.broker_snapshot_builder = broker_snapshot_builder
        self.pacing = pacing
        self._batch_lock = batch_lock or threading.RLock()

    def build(self, contracts: Sequence[OptionContractRef]) -> object:
        # The underlying BrokerSnapshotBuilder leases each actual SECDEF and
        # quote request.  This lock prevents scheduler paths from interleaving
        # the pre/post identity reads of one atomic broker snapshot.
        with self._batch_lock:
            if not self.pacing.ready:
                raise RuntimeError(PACING_CAPABILITY_MISSING)
            return self.broker_snapshot_builder.build(contracts)


class SerializedBrokerSnapshotProvider:
    """Serialize all production users of one atomic broker snapshot builder."""

    def __init__(
        self,
        broker_snapshot_builder: BrokerSnapshotBuilder,
        *,
        batch_lock: threading.RLock,
    ) -> None:
        if not callable(getattr(broker_snapshot_builder, "build", None)):
            raise TypeError("broker_snapshot_builder must expose build()")
        self.broker_snapshot_builder = broker_snapshot_builder
        # Downstream evidence acquisition also needs the read-only IV history
        # port owned by the exact source used for this atomic snapshot.
        self.source = broker_snapshot_builder.source
        self._batch_lock = batch_lock

    def build(self, contracts: Sequence[OptionContractRef]) -> object:
        with self._batch_lock:
            return self.broker_snapshot_builder.build(contracts)


class ProductionOutcomeMarketAdapter:
    """Acquire one exact-horizon batch through the shared read-only broker owner."""

    def __init__(
        self,
        gateway: IBKRReadOnlyGateway,
        broker_snapshot_builder: object,
        pacing: GuardedRequestBudget,
        *,
        batch_lock: threading.RLock,
    ) -> None:
        if not callable(getattr(gateway, "underlying_quotes", None)):
            raise TypeError("gateway must expose underlying_quotes")
        if not callable(getattr(broker_snapshot_builder, "build", None)):
            raise TypeError("broker_snapshot_builder must expose build")
        self.gateway = gateway
        self.broker_snapshot_builder = broker_snapshot_builder
        self.pacing = pacing
        self._batch_lock = batch_lock

    def observe(
        self,
        specs: tuple[Mapping[str, object], ...],
        *,
        expected_at: datetime,
    ) -> Mapping[str, object]:
        utc_datetime(expected_at, field="expected_at")
        if not specs:
            raise ValueError("outcome capture specs cannot be empty")
        symbols: list[str] = []
        contracts: dict[int, OptionContractRef] = {}
        for spec in specs:
            plan = spec.get("capture_plan")
            if not isinstance(plan, Mapping):
                raise ValueError("outcome capture plan is unavailable")
            baseline_request = spec.get("prediction_baseline_request")
            benchmark_symbol = (
                baseline_request.get("benchmark_symbol")
                if isinstance(baseline_request, Mapping)
                else plan.get("benchmark_symbol")
            )
            for raw_symbol in (spec.get("symbol"), benchmark_symbol):
                symbol = str(raw_symbol or "").strip().upper()
                if not symbol:
                    raise ValueError("outcome capture symbol is unavailable")
                if symbol not in symbols:
                    symbols.append(symbol)
            legs = plan.get("legs", ())
            if not isinstance(legs, Sequence) or isinstance(
                legs, (str, bytes, bytearray, memoryview)
            ):
                raise ValueError("outcome capture legs are unavailable")
            if not legs and not isinstance(baseline_request, Mapping):
                raise ValueError("outcome capture legs are unavailable")
            for leg in legs:
                if not isinstance(leg, Mapping):
                    raise ValueError("outcome capture leg is invalid")
                contract = _outcome_contract_ref(leg.get("contract"))
                prior = contracts.get(contract.contract_id)
                if prior is not None and prior != contract:
                    raise ValueError("outcome capture contract identity conflicted")
                contracts[contract.contract_id] = contract
        if len(symbols) > 50 or len(contracts) > 50:
            raise ValueError("outcome capture batch exceeds read-only bounds")

        snapshot: AtomicBrokerSnapshot | None = None
        with self._batch_lock:
            if not self.pacing.ready:
                raise RuntimeError(PACING_CAPABILITY_MISSING)
            with _gateway_request_lease(
                self.gateway,
                self.pacing,
                "snapshot_quote",
            ) as decision:
                if not bool(getattr(decision, "allowed", False)):
                    raise RuntimeError(
                        "PACING_BUDGET_EXHAUSTED:snapshot_quote"
                    )
                underlying_rows = tuple(
                    self.gateway.underlying_quotes(tuple(symbols))
                )
            if contracts:
                snapshot = self.broker_snapshot_builder.build(
                    tuple(contracts[key] for key in sorted(contracts))
                )
        if contracts and (
            not isinstance(snapshot, AtomicBrokerSnapshot)
            or not snapshot.complete
            or not snapshot.verify_hash()
            or len(snapshot.quotes) != len(contracts)
        ):
            raise RuntimeError("OUTCOME_CAPTURE_BROKER_SNAPSHOT_INCOMPLETE")

        underlyings: list[dict[str, object]] = []
        for row in underlying_rows:
            price = _decimal(getattr(row, "market_price", None))
            symbol = str(getattr(row, "symbol", "")).strip().upper()
            observed_at = getattr(row, "observed_at", None)
            if (
                symbol not in symbols
                or price is None
                or price <= 0
                or not isinstance(observed_at, datetime)
            ):
                raise RuntimeError("OUTCOME_CAPTURE_UNDERLYING_INVALID")
            source_payload = {
                "symbol": symbol,
                "contract_id": getattr(row, "contract_id", None),
                "price": price,
                "observed_at": observed_at,
                "source": getattr(row, "source", None),
            }
            underlyings.append(
                {
                    "symbol": symbol,
                    "price": price,
                    "observed_at": observed_at,
                    "source_id": (
                        f"underlying:{getattr(row, 'contract_id', 0)}:{symbol}"
                    ),
                    "source_hash": canonical_hash(source_payload),
                }
            )
        if {item["symbol"] for item in underlyings} != set(symbols):
            raise RuntimeError("OUTCOME_CAPTURE_UNDERLYING_INCOMPLETE")

        quotes = tuple(
            {
                "con_id": item.contract_id,
                "bid": item.bid,
                "ask": item.ask,
                "implied_volatility": item.implied_volatility,
                "volume": item.volume,
                "observed_at": item.observed_at,
            }
            for item in (() if snapshot is None else snapshot.quotes)
        )
        observed_at = max(
            *(item["observed_at"] for item in underlyings),
            *(item["observed_at"] for item in quotes),
        )
        if snapshot is None:
            source_hash = canonical_hash(
                {
                    "schema": "options_copilot.prediction_baseline_batch.v1",
                    "expected_at": expected_at,
                    "underlyings": tuple(underlyings),
                }
            )
            source_id = f"outcome-underlyings:{source_hash}"
        else:
            source_hash = snapshot.snapshot_hash
            source_id = f"outcome:{snapshot.quote_batch_id or snapshot.snapshot_hash}"
        return {
            "schema": "options_copilot.outcome_market_batch.v1",
            "observed_at": observed_at,
            "source": "IBKR_READ_ONLY_ATOMIC_CAPTURE",
            "source_id": source_id,
            "source_hash": source_hash,
            "underlyings": tuple(underlyings),
            "quotes": quotes,
        }

class ProductionDteGate:
    def evaluate(self, *, candidates: Sequence[Mapping[str, object]], **_: object) -> Mapping[str, object]:
        reasons: list[str] = []
        for item in candidates:
            dte = item.get("dte")
            if isinstance(dte, bool) or not isinstance(dte, int):
                reasons.append("DTE_EVIDENCE_MISSING")
            elif dte < 7:
                reasons.append("DTE_BELOW_PERMANENT_FLOOR")
            elif dte < 14 and not _digest(item.get("dte_exception_hash")):
                reasons.append("DTE_EXCEPTION_AUTHORITY_REQUIRED")
            elif dte > 35:
                reasons.append("DTE_ABOVE_NORMAL_MAXIMUM")
        return {"eligible": bool(candidates) and not reasons, "reasons": tuple(dict.fromkeys(reasons))}

    assess = evaluate
    run = evaluate
    resolve = evaluate


class ProductionSingleCombinationGate:
    def evaluate(self, *, evidence: Mapping[str, object], **_: object) -> Mapping[str, object]:
        snapshot = evidence.get("broker_snapshot")
        reasons: list[str] = []
        open_combinations = 0
        if not isinstance(snapshot, AtomicBrokerSnapshot) or not snapshot.complete:
            reasons.append("BROKER_SNAPSHOT_INCOMPLETE")
        else:
            for name in ("positions", "working_orders", "unsubmitted_instructions"):
                component = snapshot.state_evidence.get(name)
                if component is None or not component.known or not component.stable or component.count is None:
                    reasons.append(f"{name.upper()}_UNKNOWN")
                    continue
                if name == "positions":
                    open_combinations = _open_option_combination_count(component.state)
                    if open_combinations:
                        reasons.append("MAX_OPEN_COMBINATIONS")
                elif component.count != 0:
                    reasons.append(f"{name.upper()}_NONZERO")
        return {
            "eligible": not reasons,
            "open_combinations": open_combinations,
            "reasons": tuple(dict.fromkeys(reasons)),
        }

    assess = evaluate
    run = evaluate


class ProductionEligibilityGate:
    def __init__(
        self,
        risk_gate: ProductionRiskGate,
        dte_gate: ProductionDteGate,
        single_combination_gate: ProductionSingleCombinationGate,
    ) -> None:
        self.risk_gate = risk_gate
        self.dte_gate = dte_gate
        self.single_combination_gate = single_combination_gate

    def evaluate(
        self,
        *,
        candidates: Sequence[Mapping[str, object]],
        scenarios: Sequence[Mapping[str, object]],
        costs: Mapping[str, object],
        evidence: Mapping[str, object],
        **kwargs: object,
    ) -> Mapping[str, object]:
        reasons: list[str] = []
        evidence_hashes = evidence.get("evidence_hashes")
        if not isinstance(evidence_hashes, Mapping) or set(evidence_hashes) < {
            "MARKET",
            "VOLATILITY",
            "LIQUIDITY",
        } or any(not _digest(value) for value in evidence_hashes.values()):
            reasons.append("HARD_EVIDENCE_INCOMPLETE")
        if len(scenarios) != len(candidates) or any(
            str(item.get("action", "")) != "TRADE" for item in scenarios
        ):
            reasons.append("SCENARIO_INELIGIBLE")
        if not _digest(costs.get("cost_hash", costs.get("contract_hash"))):
            reasons.append("SIGNED_COST_BINDING_MISSING")
        risk = self.risk_gate.evaluate(candidates=candidates, evidence=evidence, **kwargs)
        dte = self.dte_gate.evaluate(candidates=candidates, evidence=evidence, **kwargs)
        combination = self.single_combination_gate.evaluate(evidence=evidence, **kwargs)
        for result in (risk, dte, combination):
            reasons.extend(str(item) for item in result.get("reasons", ()))
        eligible = bool(candidates) and not reasons and all(
            result.get("eligible") is True for result in (risk, dte, combination)
        )
        return {
            "eligible": eligible,
            "risk_fractions": risk.get("risk_fractions", {}),
            "risk_fraction": risk.get("risk_fraction"),
            "open_combinations": combination.get("open_combinations", 0),
            "reasons": tuple(dict.fromkeys(reasons)),
        }

    assess = evaluate
    run = evaluate


class IBKRNewsResearchAdapter:
    """Reprice frozen ranking legs for the news display action pool only."""

    def __init__(
        self,
        gateway: IBKRReadOnlyGateway,
        ranking_store: object,
        pacing: GuardedRequestBudget,
        *,
        clock: Callable[[], datetime] | None = None,
        batch_lock: threading.RLock | None = None,
    ) -> None:
        self.gateway = gateway
        self.ranking_store = ranking_store
        self.pacing = pacing
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._lock = threading.RLock()
        self._cache_lock = threading.RLock()
        self._batch_lock = batch_lock or threading.RLock()
        self._preselections: tuple[ConditionalOptionPreselection, ...] = ()
        self._cached_bindings: tuple[IbkrNewsBinding, ...] = ()
        self._cached_reaction_underlyings: tuple[UnderlyingQuoteSnapshot, ...] = ()
        self.health = "DEGRADED"
        self.health_reason: str | None = PACING_CAPABILITY_MISSING
        self.available_count = 0
        self.requested_count = 10
        self.coverage_reason = "INDEPENDENT_TOP10_RESEARCH_LEDGER_UNAVAILABLE"

    def bindings(self, symbols: tuple[str, ...]) -> Iterable[IbkrNewsBinding]:
        with self._lock:
            self._preselections = ()
            self.available_count = 0
            checked_at = utc_datetime(self._clock(), field="news adapter clock")
            if not _news_research_window(checked_at):
                self.health = "READY"
                self.health_reason = "OUTSIDE_US_EQUITY_RESEARCH_WINDOW"
                return ()
            if not self.pacing.ready:
                self.health = "DEGRADED"
                self.health_reason = PACING_CAPABILITY_MISSING
                return ()
            payload = self._latest_ranking()
            rows = payload.get("candidates", ()) if isinstance(payload, Mapping) else ()
            if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes, bytearray)):
                self.health = "DEGRADED"
                self.health_reason = "RANKING_READ_MODEL_INVALID"
                return ()
            selected = []
            for item in rows:
                if not isinstance(item, Mapping):
                    continue
                candidate_body = item.get("candidate_body", item)
                if not isinstance(candidate_body, Mapping):
                    continue
                symbol = str(
                    candidate_body.get(
                        "underlying",
                        candidate_body.get("symbol", ""),
                    )
                ).upper()
                if symbol in symbols:
                    selected.append(item)
                if len(selected) == 10:
                    break
            self.available_count = len(selected)
            contracts = _ranking_contracts(selected)
            if not selected or not contracts:
                self.health = "READY"
                self.health_reason = None
                return ()
            with self._batch_lock:
                with _gateway_request_lease(
                    self.gateway,
                    self.pacing,
                    "snapshot_quote",
                ) as decision:
                    if not bool(getattr(decision, "allowed", False)):
                        self.health = "DEGRADED"
                        self.health_reason = "PACING_BUDGET_EXHAUSTED"
                        return ()
                    try:
                        batch = self.gateway.option_quote_batch(contracts)
                    except MarketDataPacingError as exc:
                        self.health = "DEGRADED"
                        self.health_reason = _pacing_error_reasons(
                            "IBKR_OPTION_REPRICE",
                            exc,
                        )[-1]
                        return ()
                    except Exception:
                        self.health = "DEGRADED"
                        self.health_reason = "IBKR_OPTION_REPRICE_UNAVAILABLE"
                        return ()
            if batch.status is not QuoteBatchStatus.COMPLETE:
                self.health = "DEGRADED"
                self.health_reason = "IBKR_OPTION_REPRICE_INCOMPLETE"
                return ()
            quote_by_id = {item.contract_id: item for item in batch.quotes}
            bindings: list[IbkrNewsBinding] = []
            preselections: list[ConditionalOptionPreselection] = []
            for row in selected:
                built = _news_candidate(
                    row,
                    quote_by_id,
                    batch,
                    now=self._clock(),
                    ranking_snapshot=payload,
                )
                if built is None:
                    continue
                binding, preselection = built
                bindings.append(binding)
                preselections.append(preselection)
            self._preselections = tuple(preselections)
            with self._cache_lock:
                self._cached_bindings = tuple(bindings)
            self.health = "READY" if len(bindings) == len(selected) else "DEGRADED"
            self.health_reason = None if self.health == "READY" else "IBKR_OPTION_REPRICE_PARTIAL"
            return tuple(bindings)

    def cached_bindings(self, symbols: tuple[str, ...]) -> Iterable[IbkrNewsBinding]:
        """Return the last completed local quote batch without broker acquisition."""

        requested = {str(symbol).strip().upper() for symbol in symbols}
        with self._cache_lock:
            return tuple(
                item
                for item in self._cached_bindings
                if str(item.symbol).strip().upper() in requested
            )

    def reaction_underlying_quotes(
        self,
        symbols: tuple[str, ...],
    ) -> tuple[UnderlyingQuoteSnapshot, ...]:
        """Acquire one complete atomic underlying basket for macro reactions."""

        requested = tuple(dict.fromkeys(str(item).strip().upper() for item in symbols))
        if not requested or len(requested) != len(symbols):
            return ()
        if not self.pacing.ready:
            return ()
        with self._batch_lock:
            with _gateway_request_lease(
                self.gateway,
                self.pacing,
                "snapshot_quote",
            ) as decision:
                if not bool(getattr(decision, "allowed", False)):
                    return ()
                try:
                    rows = tuple(self.gateway.underlying_quotes(requested))
                except Exception:
                    return ()
        observed_at = {item.observed_at for item in rows}
        returned = tuple(item.symbol.upper() for item in rows)
        if (
            len(rows) != len(requested)
            or len(set(returned)) != len(returned)
            or set(returned) != set(requested)
            or len(observed_at) != 1
        ):
            return ()
        with self._cache_lock:
            self._cached_reaction_underlyings = rows
        return rows

    def cached_reaction_underlying_quotes(
        self,
        symbols: tuple[str, ...],
    ) -> tuple[UnderlyingQuoteSnapshot, ...]:
        """Read the last complete underlying basket without broker acquisition."""

        requested = tuple(dict.fromkeys(str(item).strip().upper() for item in symbols))
        with self._cache_lock:
            rows = self._cached_reaction_underlyings
        if (
            len(requested) != len(symbols)
            or len(rows) != len(requested)
            or {item.symbol.upper() for item in rows} != set(requested)
        ):
            return ()
        return rows

    def preselections(self) -> Iterable[ConditionalOptionPreselection]:
        with self._lock:
            return tuple(self._preselections)

    def coverage(self) -> Mapping[str, object]:
        with self._lock:
            return {
                "requested_count": self.requested_count,
                "available_count": self.available_count,
                "source": "FROZEN_RANKING_TOP10",
                "status": "PARTIAL",
                "reason": self.coverage_reason,
                "decision_authority": "SUPPORTING_ONLY",
            }

    def _latest_ranking(self) -> Mapping[str, object]:
        latest = getattr(self.ranking_store, "latest", None)
        reader = getattr(self.ranking_store, "read_snapshot", None)
        if not callable(latest) or not callable(reader):
            return {}
        try:
            head = latest()
            snapshot_id = getattr(head, "ranking_snapshot_id", None)
            if not isinstance(snapshot_id, str):
                return {}
            payload = reader(snapshot_id)
            return payload if isinstance(payload, Mapping) else {}
        except Exception:
            return {}


@dataclass(slots=True)
class ProductionLifecycle:
    """Supervise one shared read-only gateway and its scanner lifecycle.

    The supervisor is the sole connection owner.  Scanner and control-state
    reads reuse that exact gateway, while health remains a cached read model and
    never opens a second IBKR session.  Control state is operational evidence
    only; executable option quotes retain their separate five-second gate.
    """

    gateway: IBKRReadOnlyGateway
    scanner_loop: object
    stores: tuple[object, ...]
    pacing_guard: PacingAuthorityGuard
    clock: Callable[[], datetime] = field(
        default=lambda: datetime.now(timezone.utc),
        repr=False,
    )
    supervisor_wait: Callable[[threading.Event, float], bool] | None = field(
        default=None,
        repr=False,
    )
    management_refresher: Callable[[], object] | None = field(
        default=None,
        repr=False,
    )
    _started: bool = False
    _closed: bool = False
    _supervisor_started: bool = False
    _start_reason: str | None = None
    _lock: threading.RLock = field(default_factory=threading.RLock, repr=False)
    _connect_lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    _stop: threading.Event = field(default_factory=threading.Event, repr=False)
    _initial_attempt_complete: threading.Event = field(
        default_factory=threading.Event,
        repr=False,
    )
    _supervisor_thread: threading.Thread | None = field(default=None, repr=False)
    _api_connected: bool = False
    _ever_connected: bool = False
    _backoff_index: int = 0
    _connect_attempts: int = 0
    _next_retry_seconds: float | None = None
    _last_connect_attempt_at: datetime | None = None
    _last_connect_success_at: datetime | None = None
    _last_control_attempt_at: datetime | None = None
    _last_control_observed_at: datetime | None = None
    _last_control_success_at: datetime | None = None
    _last_control_reason: str | None = None
    _control_authority_identity: tuple[int, str] | None = None
    _control_counts: dict[str, int] = field(default_factory=dict, repr=False)
    _control_account: dict[str, object] = field(default_factory=dict, repr=False)
    _control_positions: tuple[dict[str, object], ...] = field(
        default=(),
        repr=False,
    )
    _pending_management_fingerprint: str | None = field(default=None, repr=False)
    _last_management_fingerprint: str | None = field(default=None, repr=False)
    _closed_store_ids: set[int] = field(default_factory=set, repr=False)

    def __post_init__(self) -> None:
        if self.management_refresher is not None and not callable(
            self.management_refresher
        ):
            raise TypeError("management_refresher must be callable or null")
        config = getattr(self.gateway, "config", None)
        timeout = float(
            getattr(config, "ibkr_timeout_seconds", _CONNECT_TIMEOUT_SECONDS)
        )
        refresh = float(
            getattr(
                config,
                "control_snapshot_refresh_seconds",
                _CONTROL_REFRESH_SECONDS,
            )
        )
        stale = float(
            getattr(
                config,
                "control_snapshot_stale_seconds",
                _CONTROL_STALE_SECONDS,
            )
        )
        if timeout != _CONNECT_TIMEOUT_SECONDS:
            raise ValueError("IBKR connection timeout is locked to eight seconds")
        if refresh != _CONTROL_REFRESH_SECONDS:
            raise ValueError("control snapshot refresh is locked to five seconds")
        if stale != _CONTROL_STALE_SECONDS:
            raise ValueError("control snapshot staleness is locked to fifteen seconds")

    def start(self) -> None:
        with self._lock:
            if self._closed:
                self._start_reason = "PRODUCTION_LIFECYCLE_CLOSED"
                return
            if self._supervisor_started:
                return
            if not self.pacing_guard.ready:
                return
            # Daily evidence work must keep its schedule while IBKR is
            # unavailable. Broker-dependent callbacks remain fail-closed at
            # their own account, quote, NAV, and execution gates.
            if not self._start_scanner_once():
                return
            self._stop.clear()
            self._initial_attempt_complete.clear()
            self._supervisor_started = True
            self._start_reason = "PRODUCTION_LIFECYCLE_STARTING"
            thread = threading.Thread(
                target=self._run_supervisor,
                name="options-copilot-production-supervisor",
                daemon=True,
            )
            self._supervisor_thread = thread
            thread.start()
        # Preserve startup's bounded first-attempt semantics while keeping all
        # broker connection work on the single supervisor thread.
        if not self._initial_attempt_complete.wait(
            timeout=_CONNECT_TIMEOUT_SECONDS + 0.25
        ):
            with self._lock:
                self._start_reason = "IBKR_READONLY_CONNECTION_TIMEOUT"

    def close(self) -> bool:
        with self._lock:
            if self._closed:
                return True
            self._start_reason = "PRODUCTION_LIFECYCLE_CLOSING"
            self._stop.set()
            supervisor = self._supervisor_thread
        if supervisor is not None and supervisor is not threading.current_thread():
            supervisor.join(timeout=_CONNECT_TIMEOUT_SECONDS + 0.25)
            if supervisor.is_alive():
                with self._lock:
                    self._start_reason = "PRODUCTION_SUPERVISOR_CLOSE_TIMEOUT"
                return False

        close = getattr(self.scanner_loop, "close", None)
        if callable(close) and close() is False:
            with self._lock:
                self._start_reason = "SCANNER_CLOSE_TIMEOUT"
            return False
        try:
            self.gateway.disconnect()
        except Exception:
            with self._lock:
                self._start_reason = "IBKR_READONLY_DISCONNECT_FAILED"
            return False
        with self._lock:
            self._api_connected = False
            self._started = False
            for store in reversed(self.stores):
                store_id = id(store)
                if store_id in self._closed_store_ids:
                    continue
                close = getattr(store, "close", None)
                if callable(close):
                    try:
                        close()
                    except Exception:
                        self._start_reason = "STORE_CLOSE_FAILED"
                        return False
                self._closed_store_ids.add(store_id)
            self._closed = True
            self._supervisor_started = False
            self._supervisor_thread = None
            self._start_reason = "PRODUCTION_LIFECYCLE_CLOSED"
            return True

    def health(self) -> Mapping[str, object]:
        return self._health("health")

    def summary(self) -> Mapping[str, object]:
        return self._health("summary")

    def _health(self, scanner_method: str) -> Mapping[str, object]:
        scanner_health = getattr(self.scanner_loop, scanner_method, None)
        try:
            scanner = (
                scanner_health()
                if callable(scanner_health)
                else {"status": "UNAVAILABLE"}
            )
        except Exception:
            scanner = {"status": "UNAVAILABLE"}
        with self._lock:
            connected = self._api_connected
            upstream = self._gateway_upstream_health()
            # Diagnostic work may outlive or precede a newly published batch.
            # Age the locked observation now, not when that work began.
            control = self._control_health(self._now_or_none(), upstream=upstream)
            control_status = str(control["status"])
            broker_status = (
                "DOWN"
                if not connected
                else "UP"
                if control_status == "CURRENT" and self._last_control_reason is None
                else "DEGRADED"
            )
            reasons = list(self.pacing_guard.reasons())
            if upstream is not None and control_status != "CURRENT":
                reasons.extend(upstream["reason_codes"])
                if upstream["status"] != "READY":
                    reasons.append("IBKR_UPSTREAM_CONTROL_UNVERIFIED")
            if self._start_reason is not None:
                reasons.append(self._start_reason)
            if self._supervisor_started and not connected:
                reasons.append(
                    "IBKR_READONLY_CONNECTION_LOST"
                    if self._ever_connected
                    else "IBKR_READONLY_CONNECTION_UNAVAILABLE"
                )
            scanner_status = str(scanner.get("status", "UNAVAILABLE")).upper()
            if self._started and scanner_status != "READY":
                reasons.append("SCANNER_NOT_READY")
            if self._supervisor_started:
                if self._last_control_reason is not None:
                    reasons.append(self._last_control_reason)
                if control_status == "STALE":
                    reasons.append("CONTROL_SNAPSHOT_STALE")
                elif control_status == "UNAVAILABLE":
                    reasons.append("CONTROL_SNAPSHOT_UNAVAILABLE")
            normalized_reasons = tuple(dict.fromkeys(reasons))
            ready = (
                self._started
                and self._supervisor_started
                and connected
                and scanner_status == "READY"
                and broker_status == "UP"
                and control_status == "CURRENT"
                and not normalized_reasons
            )
            return {
                "status": "READY" if ready else "DEGRADED",
                "decision": "READY" if ready else "NO_TRADE",
                "connected": connected,
                "scanner": scanner,
                "scheduler": scanner,
                "api_session": {
                    "status": "CONNECTED" if connected else "DISCONNECTED",
                    "connected": connected,
                    "read_only": True,
                    "connect_timeout_seconds": _CONNECT_TIMEOUT_SECONDS,
                },
                "broker_upstream": {
                    "status": broker_status,
                    "last_success_at": (
                        None
                        if self._last_control_success_at is None
                        else self._last_control_success_at.isoformat()
                    ),
                    "last_error": self._last_control_reason,
                    "connect_attempts": self._connect_attempts,
                    "next_retry_seconds": self._next_retry_seconds,
                    **({"authority_state": upstream["status"],
                        "generation": upstream["generation"],
                        "verified_at": upstream["verified_at"],
                        "reason_codes": upstream["reason_codes"]}
                       if upstream is not None else {}),
                },
                "control_snapshot": control,
                "executable_quote": {
                    "status": "ON_DEMAND",
                    "stale": None,
                    "maximum_age_seconds": _EXECUTABLE_QUOTE_MAX_AGE_SECONDS,
                    "decision_authority": False,
                },
                "reasons": normalized_reasons,
                "review_only": True,
                "direct_order_submission": False,
            }

    def control_snapshot(self) -> Mapping[str, object]:
        """Return a detached operator read model with zero decision authority."""

        with self._lock:
            upstream = self._gateway_upstream_health()
            health = self._control_health(self._now_or_none(), upstream=upstream)
            return {
                **health,
                "account": dict(self._control_account),
                "positions": tuple(dict(item) for item in self._control_positions),
                "reason": self._last_control_reason,
                "decision_authority": "LAST_KNOWN_ONLY",
                "review_only": True,
                "direct_order_submission": False,
            }

    def _run_supervisor(self) -> None:
        initial_reported = False
        try:
            while not self._stop.is_set():
                connected = self._gateway_connected()
                with self._lock:
                    previously_connected = self._api_connected or self._ever_connected
                    self._api_connected = connected

                if not connected:
                    if previously_connected and self._backoff_index == 0:
                        with self._lock:
                            self._start_reason = "IBKR_READONLY_CONNECTION_LOST"
                        if self._wait_for_backoff():
                            break
                    connected = self._connect_once()
                    if not connected and not initial_reported:
                        self._initial_attempt_complete.set()
                        initial_reported = True
                    if not connected:
                        if self._wait_for_backoff():
                            break
                        continue

                if not self._start_scanner_once():
                    if not initial_reported:
                        self._initial_attempt_complete.set()
                        initial_reported = True
                    if self._wait_for_backoff():
                        break
                    continue

                if self._control_refresh_due():
                    self._refresh_control_snapshot()
                if not initial_reported:
                    self._initial_attempt_complete.set()
                    initial_reported = True
                self._refresh_management_if_pending()
                if self._wait(_SUPERVISOR_POLL_SECONDS):
                    break
        finally:
            if not initial_reported:
                self._initial_attempt_complete.set()

    def _connect_once(self) -> bool:
        with self._connect_lock:
            attempted_at = self._now_or_none()
            with self._lock:
                self._last_connect_attempt_at = attempted_at
                self._connect_attempts += 1
                self._next_retry_seconds = None
            try:
                self.gateway.connect()
                connected = bool(self.gateway.connected)
                if not connected:
                    raise RuntimeError("IBKR API session remained disconnected")
            except TimeoutError:
                with self._lock:
                    self._api_connected = False
                    self._start_reason = "IBKR_READONLY_CONNECTION_TIMEOUT"
                return False
            except Exception:
                with self._lock:
                    self._api_connected = False
                    self._start_reason = "IBKR_READONLY_CONNECTION_UNAVAILABLE"
                return False
            connected_at = self._now_or_none()
            with self._lock:
                self._api_connected = True
                self._ever_connected = True
                self._backoff_index = 0
                self._next_retry_seconds = None
                self._last_connect_success_at = connected_at
                self._start_reason = None
                # One successful control observation after every reconnect may
                # refresh management, even when the position fingerprint is
                # unchanged from the prior API session.
                self._last_management_fingerprint = None
            return True

    def _start_scanner_once(self) -> bool:
        with self._lock:
            if self._started:
                return True
        try:
            self.scanner_loop.start()
        except Exception:
            with self._lock:
                self._start_reason = "SCANNER_START_FAILED"
            try:
                self.gateway.disconnect()
            except Exception:
                pass
            with self._lock:
                self._api_connected = False
            return False
        with self._lock:
            self._started = True
            self._start_reason = None
        return True

    def _control_refresh_due(self) -> bool:
        now = self._now_or_none()
        with self._lock:
            previous = self._last_control_attempt_at
        if now is None or previous is None:
            return True
        return (now - previous).total_seconds() >= _CONTROL_REFRESH_SECONDS

    def _refresh_control_snapshot(self) -> None:
        attempted_at = self._now_or_none()
        with self._lock:
            self._last_control_attempt_at = attempted_at

        try:
            account = self.gateway.account_snapshot()
        except Exception:
            self._record_control_failure("CONTROL_ACCOUNT_READ_FAILED")
            return
        if account is None:
            self._record_control_failure("CONTROL_ACCOUNT_STATE_UNKNOWN")
            return
        # The real gateway supplies one response-end verified batch.  Its
        # identity must survive every component read and the final publish.
        # Injected non-IB adapters keep their existing explicit contracts.
        upstream = self._gateway_upstream_health()
        authority_identity = self._upstream_identity(upstream)
        if upstream is not None and authority_identity is None:
            self._record_control_failure("IBKR_UPSTREAM_CONTROL_UNVERIFIED")
            return
        try:
            positions = self.gateway.positions()
        except Exception:
            self._record_control_failure("CONTROL_POSITIONS_READ_FAILED")
            return
        try:
            working_orders = self.gateway.working_orders()
        except Exception:
            self._record_control_failure("CONTROL_WORKING_ORDERS_READ_FAILED")
            return
        try:
            instructions = self.gateway.unsubmitted_instructions()
        except Exception:
            self._record_control_failure("CONTROL_INSTRUCTIONS_READ_FAILED")
            return

        positions_count = self._sequence_count(positions)
        working_order_count = self._sequence_count(working_orders)
        instruction_count = self._sequence_count(instructions)
        observed_at = self._now_or_none()
        if upstream is not None:
            current_upstream = self._gateway_upstream_health()
            if authority_identity != self._upstream_identity(current_upstream):
                self._record_control_failure("CONTROL_AUTHORITY_CHANGED_DURING_READ")
                return
            try:
                observed_at = utc_datetime(
                    datetime.fromisoformat(authority_identity[1]),
                    field="control response batch time",
                )
                account_time = (account.get("asof") if isinstance(account, Mapping)
                                else getattr(account, "asof", None))
                if isinstance(account_time, str):
                    account_time = datetime.fromisoformat(account_time)
                if utc_datetime(account_time, field="control account time") != observed_at:
                    raise ValueError("CONTROL_ACCOUNT_BATCH_TIME_MISMATCH")
                for position in positions:
                    position_time = (position.get("asof") if isinstance(position, Mapping)
                                     else getattr(position, "asof", None))
                    if isinstance(position_time, str):
                        position_time = datetime.fromisoformat(position_time)
                    if utc_datetime(position_time, field="control position time") != observed_at:
                        raise ValueError("CONTROL_POSITION_BATCH_TIME_MISMATCH")
            except (ValueError, TypeError, KeyError):
                self._record_control_failure("CONTROL_AUTHORITY_BATCH_INVALID")
                return
        if positions_count is None:
            self._record_control_failure("CONTROL_POSITIONS_STATE_UNKNOWN")
            return
        if working_order_count is None:
            self._record_control_failure("CONTROL_WORKING_ORDERS_STATE_UNKNOWN")
            return
        if instruction_count is None:
            self._record_control_observation(
                account,
                positions,
                working_order_count=working_order_count,
                instruction_count=None,
                reason="UNSUBMITTED_INSTRUCTIONS_UNKNOWN",
                observed_at=observed_at,
                authority_identity=authority_identity,
            )
            return

        if observed_at is None:
            self._record_control_failure("CONTROL_SNAPSHOT_CLOCK_INVALID")
            return
        self._record_control_observation(
            account,
            positions,
            working_order_count=working_order_count,
            instruction_count=instruction_count,
            reason=None,
            observed_at=observed_at,
            authority_identity=authority_identity,
        )
        fingerprint = self._management_fingerprint(
            positions,
            working_order_count=working_order_count,
            instruction_count=instruction_count,
        )
        if fingerprint is not None:
            with self._lock:
                if fingerprint != self._last_management_fingerprint:
                    self._pending_management_fingerprint = fingerprint

    def _refresh_management_if_pending(self) -> None:
        refresher = self.management_refresher
        if refresher is None:
            return
        with self._lock:
            if self._gateway_upstream_health() is not None and self._control_health(
                self._now_or_none()
            )["status"] != "CURRENT":
                return
            fingerprint = self._pending_management_fingerprint
            if fingerprint is None:
                return
            # Claim before invoking the heavier read-only refresh.  A failure
            # is not retried every five seconds and therefore cannot create an
            # unbounded SECDEF/quote loop.  Reconnect or a real position/order
            # fingerprint change authorizes one later attempt.
            self._pending_management_fingerprint = None
            self._last_management_fingerprint = fingerprint
        try:
            refresher()
        except Exception:
            pass

    @classmethod
    def _management_fingerprint(
        cls,
        positions: object,
        *,
        working_order_count: int,
        instruction_count: int,
    ) -> str | None:
        detached = cls._detached_mapping_sequence(positions)
        if detached is None:
            return None
        volatile = {
            "asof",
            "market_price",
            "marketprice",
            "market_value",
            "marketvalue",
            "unrealized_pnl",
            "unrealizedpnl",
            "realized_pnl",
            "realizedpnl",
        }
        stable_rows = []
        for row in detached:
            stable_rows.append(
                {
                    key: value
                    for key, value in row.items()
                    if key.replace("-", "_").lower() not in volatile
                }
            )
        stable_rows.sort(key=canonical_hash)
        return canonical_hash(
            {
                "positions": stable_rows,
                "working_order_count": working_order_count,
                "unsubmitted_instruction_count": instruction_count,
            }
        )

    def _record_control_observation(
        self,
        account: object,
        positions: object,
        *,
        working_order_count: int,
        instruction_count: int | None,
        reason: str | None,
        observed_at: datetime | None = None,
        authority_identity: tuple[int, str] | None = None,
    ) -> None:
        at = observed_at or self._now_or_none()
        detached_account = self._detached_mapping(account)
        detached_positions = self._detached_mapping_sequence(positions)
        if at is None or detached_account is None or detached_positions is None:
            self._record_control_failure("CONTROL_SNAPSHOT_PROJECTION_INVALID")
            return
        with self._lock:
            if authority_identity is not None and authority_identity != self._upstream_identity(
                self._gateway_upstream_health()
            ):
                self._last_control_reason = "CONTROL_AUTHORITY_CHANGED_DURING_READ"
                return
            self._control_authority_identity = authority_identity
            self._last_control_observed_at = at
            if reason is None:
                self._last_control_success_at = at
            self._last_control_reason = reason
            self._control_account = detached_account
            self._control_positions = detached_positions
            self._control_counts = {
                "positions_count": len(detached_positions),
                "working_order_count": working_order_count,
            }
            if instruction_count is not None:
                self._control_counts["unsubmitted_instruction_count"] = (
                    instruction_count
                )

    def _record_control_failure(self, reason: str) -> None:
        with self._lock:
            self._last_control_reason = reason
        connected = self._gateway_connected()
        with self._lock:
            self._api_connected = connected

    def _control_health(
        self, now: datetime | None, *, upstream: Mapping[str, object] | None = None,
    ) -> dict[str, object]:
        observed_at = self._last_control_observed_at
        if observed_at is None or now is None:
            return {
                "status": "UNAVAILABLE",
                "stale": True,
                "observed_at": None,
                "age_ms": None,
                "refresh_seconds": _CONTROL_REFRESH_SECONDS,
                "stale_after_seconds": _CONTROL_STALE_SECONDS,
                "positions_count": None,
                "working_order_count": None,
                "unsubmitted_instruction_count": None,
            }
        age_seconds = (now - observed_at).total_seconds()
        disconnected = not self._api_connected
        if upstream is None:
            upstream = self._gateway_upstream_health()
        authority_invalid = upstream is not None and (
            self._upstream_identity(upstream) is None
            or self._upstream_identity(upstream) != self._control_authority_identity
        )
        stale = (
            disconnected
            or authority_invalid
            or age_seconds < 0
            or age_seconds > _CONTROL_STALE_SECONDS
        )
        partial = not stale and self._last_control_reason is not None
        return {
            "status": "STALE" if stale else "PARTIAL" if partial else "CURRENT",
            "stale": stale,
            "observed_at": observed_at.isoformat(),
            "age_ms": int(abs(age_seconds) * 1000),
            "refresh_seconds": _CONTROL_REFRESH_SECONDS,
            "stale_after_seconds": _CONTROL_STALE_SECONDS,
            "positions_count": self._control_counts.get("positions_count"),
            "working_order_count": self._control_counts.get("working_order_count"),
            "unsubmitted_instruction_count": self._control_counts.get(
                "unsubmitted_instruction_count"
            ),
        }

    def _gateway_upstream_health(self) -> dict[str, object] | None:
        """Observe the owned gateway's event state without a broker request."""

        try:
            reader = getattr(self.gateway, "upstream_health", None)
            if reader is None:
                return None
            raw = reader()
            if not isinstance(raw, Mapping):
                raise ValueError("invalid upstream state")
            status = raw.get("status")
            generation = raw.get("generation")
            if status not in {"READY", "LOST", "RECOVERY_PENDING", "RECONNECT_REQUIRED", "DISCONNECTED"}:
                raise ValueError("invalid upstream state")
            if type(generation) is not int or generation < 0:
                raise ValueError("invalid upstream generation")
            verified = raw.get("verified_at")
            if verified is not None:
                verified = utc_datetime(datetime.fromisoformat(verified), field="upstream batch time").isoformat()
            if status == "READY" and verified is None:
                raise ValueError("missing upstream batch time")
            reasons = raw.get("reason_codes", ())
            if not isinstance(reasons, (tuple, list)):
                raise ValueError("invalid upstream reasons")
            return {
                "status": status, "generation": generation, "verified_at": verified,
                "reason_codes": tuple(reason for reason in reasons[:16]
                                      if isinstance(reason, str) and re.fullmatch(r"[A-Z0-9_]{1,128}", reason)),
            }
        except Exception:
            return {"status": "RECOVERY_PENDING", "generation": None, "verified_at": None,
                    "reason_codes": ("IBKR_UPSTREAM_STATE_UNAVAILABLE",)}

    @staticmethod
    def _upstream_identity(upstream: Mapping[str, object] | None) -> tuple[int, str] | None:
        if upstream is None or upstream.get("status") != "READY":
            return None
        generation, verified = upstream.get("generation"), upstream.get("verified_at")
        return (generation, verified) if type(generation) is int and isinstance(verified, str) else None

    def _wait_for_backoff(self) -> bool:
        with self._lock:
            index = min(self._backoff_index, len(_RECONNECT_BACKOFF_SECONDS) - 1)
            delay = _RECONNECT_BACKOFF_SECONDS[index]
            self._backoff_index = min(
                self._backoff_index + 1,
                len(_RECONNECT_BACKOFF_SECONDS) - 1,
            )
            self._next_retry_seconds = delay
        stopped = self._wait(delay)
        with self._lock:
            self._next_retry_seconds = None
        return stopped

    def _wait(self, seconds: float) -> bool:
        waiter = self.supervisor_wait
        if waiter is None:
            return self._stop.wait(seconds)
        try:
            return bool(waiter(self._stop, seconds))
        except Exception:
            with self._lock:
                self._start_reason = "PRODUCTION_SUPERVISOR_WAIT_FAILED"
            return True

    def _gateway_connected(self) -> bool:
        try:
            return bool(self.gateway.connected)
        except Exception:
            return False

    def _now_or_none(self) -> datetime | None:
        try:
            return utc_datetime(self.clock(), field="production supervisor clock")
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _sequence_count(value: object) -> int | None:
        if isinstance(value, (str, bytes, bytearray, memoryview)) or not isinstance(
            value,
            Sequence,
        ):
            return None
        return len(value)

    @staticmethod
    def _detached_mapping(value: object) -> dict[str, object] | None:
        if is_dataclass(value) and not isinstance(value, type):
            raw = asdict(value)
        elif isinstance(value, Mapping):
            raw = dict(value)
        elif hasattr(value, "__dict__"):
            raw = {
                str(key): item
                for key, item in vars(value).items()
                if isinstance(key, str) and not key.startswith("_")
            }
        else:
            return None
        return raw if all(isinstance(key, str) for key in raw) else None

    @classmethod
    def _detached_mapping_sequence(
        cls,
        value: object,
    ) -> tuple[dict[str, object], ...] | None:
        if isinstance(value, (str, bytes, bytearray, memoryview)) or not isinstance(
            value,
            Sequence,
        ):
            return None
        rows: list[dict[str, object]] = []
        for item in value:
            detached = cls._detached_mapping(item)
            if detached is None:
                return None
            rows.append(detached)
        return tuple(rows)


def creator_unavailable_reason() -> str:
    return _CREATOR_UNAVAILABLE


def _positioning_unavailable(reasons: Sequence[object]) -> dict[str, object]:
    normalized = tuple(
        dict.fromkeys(
            str(item).strip().upper()
            for item in reasons
            if str(item).strip()
        )
    ) or ("POSITIONING_UNAVAILABLE",)
    return {
        "schema_version": "options_copilot.positioning_feed.v1",
        "status": "UNAVAILABLE",
        "reasons": normalized,
        "positioning": (),
        "count": 0,
        "decision_authority": "SUPPORTING_ONLY",
        "supporting_only": True,
        "affects_eligibility": False,
        "approval_allowed": False,
        "instruction_allowed": False,
        "order_allowed": False,
    }


def _finalist_spots(universe: object) -> dict[str, Decimal]:
    if not isinstance(universe, Mapping):
        return {}
    raw = universe.get("finalists", ())
    if not _sequence_value(raw):
        return {}
    results: dict[str, Decimal] = {}
    for item in raw:
        if not isinstance(item, Mapping):
            continue
        symbol = str(item.get("symbol", item.get("underlying", ""))).strip().upper()
        spot = _decimal(item.get("spot"))
        if symbol and spot is not None and spot > 0:
            results[symbol] = spot
    return results


def _mapping(value: object) -> Mapping[str, object]:
    if isinstance(value, Mapping):
        return dict(value)
    if is_dataclass(value):
        return asdict(value)
    data = getattr(value, "__dict__", None)
    return dict(data) if isinstance(data, Mapping) else {"value": str(value)}


def _sequence_value(value: object) -> bool:
    return isinstance(value, Sequence) and not isinstance(
        value,
        (str, bytes, bytearray, memoryview),
    )


def _ranked_symbols(*groups: Sequence[Mapping[str, object]]) -> tuple[str, ...]:
    scores: dict[str, Decimal] = {}
    for group in groups:
        for item in group:
            symbol = str(item.get("symbol", item.get("underlying", ""))).strip().upper()
            if not symbol:
                continue
            score = _decimal(item.get("score")) or Decimal("0")
            scores[symbol] = max(scores.get(symbol, Decimal("-Infinity")), score)
    return tuple(item[0] for item in sorted(scores.items(), key=lambda row: (-row[1], row[0])))


def _sector_diverse_scanner_rows(
    rows: Sequence[Mapping[str, object]],
) -> tuple[Mapping[str, object], ...]:
    """Round-robin IBKR industry groups before bounded option-chain work."""

    ordered = sorted(
        (dict(item) for item in rows),
        key=lambda item: (
            -(_decimal(item.get("score")) or Decimal("0")),
            str(item.get("symbol", "")),
        ),
    )
    groups: dict[str, list[Mapping[str, object]]] = {}
    for item in ordered:
        sector = next(
            (
                str(item.get(name, "")).strip().upper()
                for name in ("industry", "category", "subcategory", "source_scan")
                if str(item.get(name, "")).strip()
            ),
            "UNCLASSIFIED",
        )
        groups.setdefault(sector, []).append(item)
    diversified: list[Mapping[str, object]] = []
    while groups:
        for sector in tuple(groups):
            bucket = groups[sector]
            diversified.append(bucket.pop(0))
            if not bucket:
                del groups[sector]
    return tuple(diversified)


def _balanced_discovery_symbols(
    scanner_rows: Sequence[Mapping[str, object]],
    core_rows: Sequence[Mapping[str, object]],
    *,
    limit: int,
    preserve_scanner_order: bool = False,
) -> tuple[str, ...]:
    """Interleave broad discovery with liquid fallbacks under one hard cap."""

    return balanced_research_symbols(
        scanner_rows,
        core_rows,
        limit=limit,
        preserve_scanner_order=preserve_scanner_order,
    )


def _verified_news_discovery_rows(
    payload: Mapping[str, object],
    *,
    limit: int = 30,
) -> tuple[Mapping[str, object], ...]:
    """Project deterministic, symbol-verified news into bounded pool discovery."""

    raw = payload.get("news", ())
    if (
        isinstance(limit, bool)
        or not isinstance(limit, int)
        or not 0 <= limit <= 30
        or not isinstance(raw, Sequence)
        or isinstance(raw, (str, bytes, bytearray))
    ):
        return ()
    candidates: list[tuple[Decimal, int, str]] = []
    for ordinal, item in enumerate(raw):
        if not isinstance(item, Mapping):
            continue
        binding = item.get("symbol_binding")
        status = (
            str(binding.get("status") or "").strip().upper()
            if isinstance(binding, Mapping)
            else ""
        )
        verified_issuer_binding = (
            status.startswith("VERIFIED")
            or status in {"PROVIDER_VERIFIED", "IBKR_VERIFIED"}
        )
        score = canonical_research_score(item.get("event_impact_score"))
        if score is None:
            continue
        symbols = item.get("symbols", ())
        if isinstance(symbols, str):
            symbols = (symbols,)
        if not isinstance(symbols, Sequence):
            continue
        research_proxy = None
        if not verified_issuer_binding:
            if len(symbols) != 0:
                continue
            raw_proxy = item.get("research_proxy_binding")
            if isinstance(raw_proxy, Mapping):
                proxy_symbol = canonical_research_symbol(
                    raw_proxy.get("proxy_symbol")
                )
                if proxy_symbol is not None:
                    try:
                        research_proxy = require_current_research_proxy_binding(
                            raw_proxy,
                            symbol=proxy_symbol,
                        )
                    except (TypeError, ValueError):
                        research_proxy = None
            if research_proxy is None:
                continue
            symbols = (research_proxy.proxy_symbol,)
        research_rank = item.get("research_rank")
        rank = (
            research_rank
            if isinstance(research_rank, int)
            and not isinstance(research_rank, bool)
            and research_rank > 0
            else ordinal + 1
        )
        for value in symbols:
            symbol = canonical_research_symbol(value)
            if symbol is not None:
                candidates.append((score, rank, symbol))
    candidates.sort(key=lambda item: (-item[0], item[1], item[2]))
    rows: list[Mapping[str, object]] = []
    seen: set[str] = set()
    for score, _rank, symbol in candidates:
        if symbol in seen:
            continue
        seen.add(symbol)
        rows.append(
            {
                "symbol": symbol,
                "rank": len(rows),
                "source_scan": "NEWS_EVENT_POOL",
                "contract_id": None,
                "exchange": None,
                "industry": None,
                "category": None,
                "subcategory": None,
                "security_type": None,
                "event_impact_score": score,
            }
        )
        if len(rows) >= limit:
            break
    return tuple(rows)


def _directional_strikes(
    strikes: Sequence[Decimal], spot: Decimal
) -> tuple[tuple[str, tuple[Decimal, Decimal]], ...]:
    values = tuple(sorted({item for item in strikes if isinstance(item, Decimal) and item > 0}))
    calls = tuple(item for item in values if item >= spot)
    puts = tuple(item for item in values if item <= spot)
    result: list[tuple[str, tuple[Decimal, Decimal]]] = []
    if len(calls) >= 2:
        result.append(("C", (calls[0], calls[1])))
    if len(puts) >= 2:
        result.append(("P", (puts[-1], puts[-2])))
    return tuple(result)


def _preferred_directional_vertical(
    strikes: Sequence[Decimal],
    spot: Decimal,
    *,
    close: Decimal | None,
) -> tuple[str, tuple[Decimal, Decimal]] | None:
    """Choose one bounded research direction from current price versus close."""

    if close is None or close <= 0 or spot == close:
        return None
    available = _directional_strikes(strikes, spot)
    if not available:
        return None
    preferred_right = "P" if spot < close else "C"
    return next(
        (item for item in available if item[0] == preferred_right),
        None,
    )


def _preferred_option_expiration(expirations: Sequence[object]) -> object | None:
    """Prefer a SMART standard monthly series before sparse weekly fallbacks."""

    smart = tuple(
        item
        for item in expirations
        if str(getattr(item, "exchange", "")).strip().upper() == "SMART"
    )
    standard_monthly = next(
        (
            item
            for item in smart
            if isinstance(getattr(item, "expiration", None), date)
            and item.expiration.weekday() == 4
            and 15 <= item.expiration.day <= 21
        ),
        None,
    )
    if standard_monthly is not None:
        return standard_monthly
    friday = next(
        (
            item
            for item in smart
            if isinstance(getattr(item, "expiration", None), date)
            and item.expiration.weekday() == 4
        ),
        None,
    )
    if friday is not None:
        return friday
    if smart:
        return smart[0]
    return expirations[0] if expirations else None


_STRUCTURE_UNCERTAINTY_LIMIT = Decimal("0.55")
_SUPPORTED_STRUCTURE_DIRECTIONS = frozenset({"BULLISH", "BEARISH", "NEUTRAL"})


def _pacing_requires_single_optionable_underlying(
    symbols: Sequence[str],
    thesis_by_symbol: Mapping[str, Mapping[str, object]],
) -> bool:
    """Reserve enough SECDEF headroom for complex structures and snapshots."""

    for symbol in symbols[:4]:
        row = thesis_by_symbol.get(str(symbol).strip().upper())
        if not isinstance(row, Mapping):
            continue
        label = str(row.get("direction_label", "")).strip().upper()
        try:
            uncertainty = Decimal(str(row.get("uncertainty")))
        except (InvalidOperation, TypeError, ValueError):
            return True
        if (
            label == "NEUTRAL"
            or label not in {"BULLISH", "BEARISH"}
            or uncertainty > Decimal("0.25")
        ):
            return True
    return False


def _planned_structure_requests(
    strikes: Sequence[Decimal],
    spot: Decimal,
    *,
    direction_label: str,
    uncertainty: Decimal,
) -> tuple[_StructurePlan, ...]:
    """Plan bounded structures from the G035 thesis, never from price drift."""

    values = tuple(sorted({
        item for item in strikes
        if isinstance(item, Decimal) and item.is_finite() and item > 0
    }))
    if (
        not values
        or not isinstance(uncertainty, Decimal)
        or not uncertainty.is_finite()
        or uncertainty < 0
        or uncertainty > 1
    ):
        return ()
    label = direction_label.strip().upper()
    if (
        label not in _SUPPORTED_STRUCTURE_DIRECTIONS
        or uncertainty > _STRUCTURE_UNCERTAINTY_LIMIT
    ):
        return ()
    calls = tuple(item for item in values if item >= spot)
    puts = tuple(item for item in values if item <= spot)
    plans: list[_StructurePlan] = []
    if label == "BULLISH":
        if calls:
            plans.append(_StructurePlan(
                "LONG_OPTION", (("C", calls[0], "LONG", 1),), label, uncertainty,
            ))
        if len(calls) >= 2:
            plans.append(_StructurePlan(
                "DEBIT_VERTICAL",
                (("C", calls[0], "LONG", 1), ("C", calls[1], "SHORT", 1)),
                label,
                uncertainty,
            ))
        if len(puts) >= 2:
            plans.append(_StructurePlan(
                "CREDIT_VERTICAL",
                (("P", puts[-2], "LONG", 1), ("P", puts[-1], "SHORT", 1)),
                label,
                uncertainty,
            ))
    elif label == "BEARISH":
        if puts:
            plans.append(_StructurePlan(
                "LONG_OPTION", (("P", puts[-1], "LONG", 1),), label, uncertainty,
            ))
        if len(puts) >= 2:
            plans.append(_StructurePlan(
                "DEBIT_VERTICAL",
                (("P", puts[-1], "LONG", 1), ("P", puts[-2], "SHORT", 1)),
                label,
                uncertainty,
            ))
        if len(calls) >= 2:
            plans.append(_StructurePlan(
                "CREDIT_VERTICAL",
                (("C", calls[1], "LONG", 1), ("C", calls[0], "SHORT", 1)),
                label,
                uncertainty,
            ))
    elif label == "NEUTRAL":
        butterflies = tuple(
            (values[index - 1], values[index], values[index + 1])
            for index in range(1, len(values) - 1)
            if values[index] - values[index - 1] == values[index + 1] - values[index]
        )
        if butterflies:
            wings = min(butterflies, key=lambda row: abs(row[1] - spot))
            plans.append(_StructurePlan(
                "BUTTERFLY",
                (
                    ("C", wings[0], "LONG", 1),
                    ("C", wings[1], "SHORT", 2),
                    ("C", wings[2], "LONG", 1),
                ),
                label,
                uncertainty,
            ))
        if len(puts) >= 2 and len(calls) >= 2:
            plans.append(_StructurePlan(
                "IRON_CONDOR",
                (
                    ("P", puts[-2], "LONG", 1),
                    ("P", puts[-1], "SHORT", 1),
                    ("C", calls[0], "SHORT", 1),
                    ("C", calls[1], "LONG", 1),
                ),
                label,
                uncertainty,
            ))
    # Reserve enough SECDEF capacity for both pre/post definition reads in the
    # later AtomicBrokerSnapshot.  Two structures per thesis preserve a real
    # strategy choice without consuming the evidence budget before Gate 1.
    # Low-uncertainty directional theses retain the convex long-option choice;
    # moderate uncertainty substitutes the bounded credit vertical so that the
    # closed strategy registry is reachable without increasing request count.
    if label in {"BULLISH", "BEARISH"} and len(plans) > 2:
        return (
            tuple(plans[:2])
            if uncertainty <= Decimal("0.25")
            else tuple(plans[1:3])
        )
    return tuple(plans[:2])


def _structure_plan_rejection_reason(
    strikes: Sequence[Decimal],
    *,
    direction_label: str,
    uncertainty: Decimal,
) -> str:
    """Explain an empty structure plan without weakening its safety limit."""

    values = tuple(
        item
        for item in strikes
        if isinstance(item, Decimal) and item.is_finite() and item > 0
    )
    if (
        not values
        or not isinstance(uncertainty, Decimal)
        or not uncertainty.is_finite()
        or uncertainty < 0
        or uncertainty > 1
    ):
        return "EQUITY_THESIS_HAS_NO_SUPPORTED_TEMPLATE"
    if uncertainty > _STRUCTURE_UNCERTAINTY_LIMIT:
        return "EQUITY_THESIS_UNCERTAINTY_ABOVE_STRUCTURE_LIMIT"
    if direction_label.strip().upper() not in _SUPPORTED_STRUCTURE_DIRECTIONS:
        return "EQUITY_THESIS_DIRECTION_UNSUPPORTED"
    return "EQUITY_THESIS_HAS_NO_SUPPORTED_TEMPLATE"


def _planned_contracts(
    contracts: Sequence[OptionContractRef],
    plan: _StructurePlan,
) -> tuple[OptionContractRef, ...]:
    by_key: dict[tuple[str, Decimal], OptionContractRef] = {}
    for contract in contracts:
        key = (contract.right, contract.strike)
        if key in by_key and by_key[key] != contract:
            return ()
        by_key[key] = contract
    return tuple(
        by_key.get((right, strike))
        for right, strike, _side, _ratio in plan.legs
    ) if all((right, strike) in by_key for right, strike, _side, _ratio in plan.legs) else ()


def _qualified_structure_requests(
    contracts: Sequence[OptionContractRef],
    spot: Decimal,
    *,
    direction_label: str,
    uncertainty: Decimal,
) -> tuple[_StructurePlan, ...]:
    """Build bounded adjacent-strike alternatives from qualified identities."""

    base_plans = _planned_structure_requests(
        tuple(item.strike for item in contracts),
        spot,
        direction_label=direction_label,
        uncertainty=uncertainty,
    )
    if not base_plans:
        return ()
    strikes_by_right = {
        right: tuple(sorted({
            item.strike
            for item in contracts
            if item.right == right
            and item.strike.is_finite()
            and item.strike > 0
        }))
        for right in ("C", "P")
    }
    expanded: list[_StructurePlan] = []
    for plan in base_plans:
        if plan.structure == "LONG_OPTION":
            right = plan.legs[0][0]
            directional = (
                tuple(item for item in strikes_by_right[right] if item >= spot)
                if right == "C"
                else tuple(
                    sorted(
                        (item for item in strikes_by_right[right] if item <= spot),
                        reverse=True,
                    )
                )
            )
            if directional:
                _old_right, _old_strike, side, ratio = plan.legs[0]
                expanded.append(
                    _StructurePlan(
                        plan.structure,
                        ((right, directional[0], side, ratio),),
                        plan.thesis_label,
                        plan.uncertainty,
                    )
                )
            continue
        if plan.structure not in {"DEBIT_VERTICAL", "CREDIT_VERTICAL"}:
            expanded.append(plan)
            continue
        right = plan.legs[0][0]
        directional = (
            tuple(item for item in strikes_by_right[right] if item >= spot)
            if right == "C"
            else tuple(
                sorted(
                    (item for item in strikes_by_right[right] if item <= spot),
                    reverse=True,
                )
            )
        )
        if len(directional) < 2:
            continue
        first_before_second = plan.legs[0][1] < plan.legs[1][1]
        for near, far in zip(directional, directional[1:]):
            lower, higher = sorted((near, far))
            first_strike, second_strike = (
                (lower, higher) if first_before_second else (higher, lower)
            )
            expanded.append(
                _StructurePlan(
                    plan.structure,
                    (
                        (right, first_strike, plan.legs[0][2], plan.legs[0][3]),
                        (right, second_strike, plan.legs[1][2], plan.legs[1][3]),
                    ),
                    plan.thesis_label,
                    plan.uncertainty,
                )
            )
    return tuple(expanded[:10])


def _qualification_strike_window(
    planned: Sequence[Decimal],
    available: Sequence[Decimal],
    *,
    spot: Decimal,
    right: str,
) -> tuple[Decimal, ...]:
    """Add only enough bounded expiration-grid fallbacks to rebuild one plan."""

    normalized_planned = tuple(dict.fromkeys(
        item
        for item in planned
        if isinstance(item, Decimal) and item.is_finite() and item > 0
    ))
    if right == "C":
        directional = tuple(sorted({
            item
            for item in available
            if isinstance(item, Decimal)
            and item.is_finite()
            and item > 0
            and item >= spot
        }))
    elif right == "P":
        directional = tuple(sorted({
            item
            for item in available
            if isinstance(item, Decimal)
            and item.is_finite()
            and item > 0
            and item <= spot
        }, reverse=True))
    else:
        raise ValueError("option right must be C or P")
    fallbacks = tuple(
        item for item in directional if item not in normalized_planned
    )[: len(normalized_planned)]
    return (
        *normalized_planned,
        *fallbacks,
    )


def _qualification_strike_request(
    planned: Sequence[Decimal],
    available: Sequence[Decimal],
    *,
    spot: Decimal,
    right: str,
    paced: bool,
    fallback_allowed: bool = False,
) -> tuple[Decimal, ...]:
    """Expand the strike grid only when downstream evidence has headroom."""

    normalized_planned = tuple(dict.fromkeys(
        item
        for item in planned
        if isinstance(item, Decimal) and item.is_finite() and item > 0
    ))
    if paced and not fallback_allowed:
        return normalized_planned
    return _qualification_strike_window(
        normalized_planned,
        available,
        spot=spot,
        right=right,
    )


def _remaining_pacing_capacity(
    pacing: object,
    request_class: str,
) -> int:
    """Return proved request headroom or fail closed with an exact reason."""

    usage_reader = getattr(pacing, "usage", None)
    if not callable(usage_reader):
        raise _PacingUsageSnapshotError(
            _OPTION_PIPELINE_PACING_USAGE_UNAVAILABLE
        )
    try:
        usage = usage_reader()
    except Exception:
        raise _PacingUsageSnapshotError(
            _OPTION_PIPELINE_PACING_USAGE_UNAVAILABLE
        ) from None
    if not isinstance(usage, Mapping):
        raise _PacingUsageSnapshotError(_OPTION_PIPELINE_PACING_USAGE_INVALID)
    row = usage.get(request_class)
    if row is None:
        raise _PacingUsageSnapshotError(
            _OPTION_PIPELINE_PACING_USAGE_UNAVAILABLE
        )
    if not isinstance(row, Mapping):
        raise _PacingUsageSnapshotError(_OPTION_PIPELINE_PACING_USAGE_INVALID)
    used = row.get("used")
    limit = row.get("limit")
    if (
        isinstance(used, bool)
        or not isinstance(used, int)
        or isinstance(limit, bool)
        or not isinstance(limit, int)
        or used < 0
        or limit < used
    ):
        raise _PacingUsageSnapshotError(_OPTION_PIPELINE_PACING_USAGE_INVALID)
    return limit - used


def _direct_underlying_quote_basis(
    row: object,
    *,
    expected_symbol: str,
    verified_at: datetime,
    allow_indicative: bool = False,
) -> tuple[UnderlyingQuoteBasis | _IndicativeUnderlyingQuoteBasis | None, str | None]:
    """Validate and freeze the live stock quote used to select CALL or PUT."""

    symbol = str(getattr(row, "symbol", "")).strip().upper()
    if not symbol or symbol != expected_symbol:
        return None, "UNDERLYING_QUOTE_IDENTITY_MISMATCH"
    contract_id = getattr(row, "contract_id", None)
    if (
        isinstance(contract_id, bool)
        or not isinstance(contract_id, int)
        or contract_id <= 0
    ):
        return None, "UNDERLYING_QUOTE_IDENTITY_INCOMPLETE"
    exchange = str(getattr(row, "exchange", "")).strip().upper()
    source = str(getattr(row, "source", "")).strip().upper()
    allowed_sources = (
        {
            "IBKR_REQ_TICKERS_READONLY",
            "IBKR_AFTER_HOURS_UNDERLYING_READONLY",
            "IBKR_AFTER_HOURS_UNDERLYING_READONLY+HISTORICAL_PREVIOUS_CLOSE",
            "IBKR_AFTER_HOURS_UNDERLYING_READONLY+HISTORICAL_TWO_CLOSES",
        }
        if allow_indicative
        else {"IBKR_REQ_TICKERS_READONLY"}
    )
    if not exchange or source not in allowed_sources:
        return None, "UNDERLYING_QUOTE_IDENTITY_INCOMPLETE"
    try:
        observed_at = utc_datetime(
            getattr(row, "observed_at", None),
            field="underlying quote observed_at",
        )
    except (TypeError, ValueError):
        return None, "UNDERLYING_QUOTE_TIMESTAMP_INVALID"
    age_seconds = (verified_at - observed_at).total_seconds()
    if (
        age_seconds < 0
        or (not allow_indicative and age_seconds > _EXECUTABLE_QUOTE_MAX_AGE_SECONDS)
        or (allow_indicative and age_seconds > _CONTROL_STALE_SECONDS)
    ):
        return None, "UNDERLYING_QUOTE_STALE_OR_FUTURE"
    market_data_type = getattr(row, "market_data_type", None)
    if not allow_indicative and market_data_type in {3, 4}:
        return None, "UNDERLYING_QUOTE_MARKET_DATA_DELAYED"
    if (
        isinstance(market_data_type, bool)
        or not isinstance(market_data_type, int)
        or market_data_type not in ({1, 2, 3, 4} if allow_indicative else {1})
    ):
        return None, "UNDERLYING_QUOTE_MARKET_DATA_NOT_LIVE"
    close = _decimal(getattr(row, "close", None))
    if close is None:
        return None, "UNDERLYING_QUOTE_FIELDS_INCOMPLETE"
    try:
        basis = _IndicativeUnderlyingQuoteBasis(
            symbol=symbol,
            contract_id=contract_id,
            exchange=exchange,
            source=source,
            observed_at=observed_at,
            bid=_decimal(getattr(row, "bid", None)),
            ask=_decimal(getattr(row, "ask", None)),
            last=_decimal(getattr(row, "last", None)),
            close=close,
            market_data_type=market_data_type,
        ) if allow_indicative else UnderlyingQuoteBasis(
            symbol=symbol,
            contract_id=contract_id,
            exchange=exchange,
            source=source,
            observed_at=observed_at,
            bid=_decimal(getattr(row, "bid", None)),
            ask=_decimal(getattr(row, "ask", None)),
            last=_decimal(getattr(row, "last", None)),
            close=close,
            market_data_type=market_data_type,
        )
    except (TypeError, ValueError):
        return None, "UNDERLYING_QUOTE_FIELDS_INVALID"
    return basis, None


def _ordered_vertical_contracts(
    contracts: Sequence[OptionContractRef], right: str
) -> tuple[OptionContractRef, ...]:
    rows = tuple(item for item in contracts if item.right == right)
    if right == "C":
        return tuple(sorted(rows, key=lambda item: item.strike))
    return tuple(sorted(rows, key=lambda item: item.strike, reverse=True))


def _execution_cost_short_leg_research_evidence(
    execution_cost_contract: Mapping[str, object] | None,
    *,
    as_of: datetime,
) -> Mapping[str, object]:
    """Resolve signed static short-leg policy without claiming market evidence."""

    unavailable = {
        "status": "UNSUPPORTED",
        "reason_codes": (
            "ASSIGNMENT_EXERCISE_EX_DIVIDEND_EVIDENCE_UNAVAILABLE",
        ),
        "evidence_hash": None,
    }
    if execution_cost_contract is None:
        frozen = freeze_json(unavailable)
        assert isinstance(frozen, Mapping)
        return frozen
    try:
        verified = verify_contract(
            execution_cost_contract,
            expected_kind=ContractKind.EXECUTION_COST,
            expected_version=EXECUTION_COST_VERSION,
            expected_hash=EXECUTION_COST_HASH,
            as_of=as_of,
        )
        assignment = verified.payload.get("assignment_exercise_and_dividend")
        if not isinstance(assignment, Mapping):
            raise ValueError("assignment policy is unavailable")
        complete = all(
            isinstance(assignment.get(key), Mapping) and bool(assignment.get(key))
            for key in ("assignment", "exercise", "early_exercise", "ex_dividend")
        ) and isinstance(assignment.get("short_leg_exit_deadline"), str)
        declared_status = str(assignment.get("status", "")).strip().upper()
        declared_hash = assignment.get("evidence_hash")
        declared_supported = (
            declared_status == "SUPPORTED"
            and isinstance(declared_hash, str)
            and len(declared_hash) == 64
            and all(char in _DIGEST_CHARS for char in declared_hash.lower())
        )
        if not complete or not (declared_supported or declared_status == ""):
            raise ValueError("assignment policy is incomplete")
        resolved = {
            "status": "SUPPORTED",
            "reason_codes": (),
            "evidence_hash": canonical_hash(
                {
                    "execution_cost_contract_hash": verified.contract_hash,
                    "assignment_exercise_and_dividend": assignment,
                }
            ),
        }
    except (ContractValidationError, TypeError, ValueError):
        resolved = unavailable
    frozen = freeze_json(resolved)
    assert isinstance(frozen, Mapping)
    return frozen


def _contract_leg(
    contract: OptionContractRef,
    side: str,
    *,
    ratio: int = 1,
    short_leg_risk_evidence: Mapping[str, object] | None = None,
) -> Mapping[str, object]:
    row = {
        "con_id": contract.contract_id,
        "contract_id_ex": contract.contract_id_ex,
        "symbol": contract.symbol,
        "local_symbol": contract.local_symbol,
        "expiration": contract.expiration.isoformat(),
        "strike": contract.strike,
        "right": contract.right,
        "exchange": contract.exchange,
        "trading_class": contract.trading_class,
        "multiplier": contract.multiplier,
        "currency": contract.currency,
        "side": side,
        "ratio": ratio,
    }
    if short_leg_risk_evidence is not None:
        row["short_leg_risk_evidence"] = short_leg_risk_evidence
    return row


def _outcome_contract_ref(value: object) -> OptionContractRef:
    if not isinstance(value, Mapping):
        raise ValueError("outcome capture contract is unavailable")
    try:
        contract_id = int(value["contract_id"])
        multiplier = int(value["multiplier"])
        expiration_value = value["expiration"]
        expiration = (
            expiration_value
            if isinstance(expiration_value, date)
            and not isinstance(expiration_value, datetime)
            else date.fromisoformat(str(expiration_value))
        )
        right = str(value["right"]).upper()
        if right not in {"C", "P"}:
            raise ValueError("invalid right")
        return OptionContractRef(
            contract_id=contract_id,
            contract_id_ex=str(value["contract_id_ex"]),
            symbol=str(value["symbol"]).strip().upper(),
            local_symbol=str(value["local_symbol"]),
            expiration=expiration,
            strike=Decimal(str(value["strike"])),
            right=right,  # type: ignore[arg-type]
            exchange=str(value["exchange"]),
            trading_class=str(value["trading_class"]),
            multiplier=multiplier,
            currency=str(value.get("currency", "USD")),
        )
    except (KeyError, TypeError, ValueError, InvalidOperation) as exc:
        raise ValueError("outcome capture contract is invalid") from exc


def _outcome_capture_baseline(
    underlying: object,
    benchmark: object | None,
) -> Mapping[str, object] | None:
    def binding(value: object) -> Mapping[str, object] | None:
        symbol = str(getattr(value, "symbol", "")).strip().upper()
        price = _decimal(getattr(value, "market_price", None))
        observed_at = getattr(value, "observed_at", None)
        source = str(getattr(value, "source", "")).strip()
        contract_id = getattr(value, "contract_id", None)
        if (
            not symbol
            or price is None
            or price <= 0
            or not isinstance(observed_at, datetime)
            or observed_at.tzinfo is None
            or not source
            or not isinstance(contract_id, int)
            or isinstance(contract_id, bool)
            or contract_id <= 0
        ):
            return None
        checked_at = utc_datetime(observed_at, field="underlying observed_at")
        payload = {
            "symbol": symbol,
            "price": price,
            "observed_at": checked_at,
            "source": source,
            "source_id": f"underlying:{contract_id}:{symbol}",
        }
        return {
            **payload,
            "source_hash": canonical_hash(payload),
        }

    underlying_binding = binding(underlying)
    benchmark_binding = None if benchmark is None else binding(benchmark)
    if underlying_binding is None or benchmark_binding is None:
        return None
    return {
        "schema": "options_copilot.outcome_capture_baseline.v1",
        "benchmark_symbol": "SPY",
        "underlying": underlying_binding,
        "benchmark": benchmark_binding,
    }


def _direct_top10_leg(
    contract: OptionContractRef,
    *,
    side: OptionLegSide,
    dte: int,
) -> ConditionalOptionLeg:
    return ConditionalOptionLeg(
        underlying=contract.symbol,
        con_id=contract.contract_id,
        expiry=contract.expiration,
        strike=contract.strike,
        right=OptionRight.CALL if contract.right == "C" else OptionRight.PUT,
        side=side,
        ratio=1,
        quantity=1,
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
        dte=dte,
        local_symbol=contract.local_symbol,
        trading_class=contract.trading_class,
        multiplier=contract.multiplier,
        exchange=contract.exchange,
    )


def _contract_from_leg(value: object) -> OptionContractRef:
    if not isinstance(value, Mapping):
        raise TypeError("leg must be a mapping")
    expiration = value["expiration"]
    if isinstance(expiration, str):
        expiration = date.fromisoformat(expiration)
    return OptionContractRef(
        contract_id=int(value["con_id"]),
        contract_id_ex=str(value["contract_id_ex"]),
        symbol=str(value["symbol"]),
        local_symbol=str(value["local_symbol"]),
        expiration=expiration,
        strike=Decimal(str(value["strike"])),
        right=str(value["right"]),  # type: ignore[arg-type]
        exchange=str(value["exchange"]),
        trading_class=str(value["trading_class"]),
        multiplier=int(value["multiplier"]),
        currency=str(value.get("currency", "USD")),
    )


def _secdefs_from_snapshot(snapshot: AtomicBrokerSnapshot) -> tuple[OptionSecDefSnapshot, ...]:
    rows: list[OptionSecDefSnapshot] = []
    try:
        for item in snapshot.secdef_evidence:
            identity = item.post_identity
            if not item.stable or not item.standard_contract or item.adjusted or identity is None:
                return ()
            expiry = identity["expiry"]
            if isinstance(expiry, str):
                expiry = date.fromisoformat(expiry)
            rows.append(
                OptionSecDefSnapshot(
                    contract_id=int(identity["conId"]),
                    local_symbol=str(identity["localSymbol"]),
                    trading_class=str(identity["tradingClass"]),
                    multiplier=int(identity["multiplier"]),
                    exchange=str(identity["exchange"]),
                    expiration=expiry,
                    strike=Decimal(str(identity["strike"])),
                    right=str(identity["right"]),  # type: ignore[arg-type]
                    security_type="OPT",
                    currency="USD",
                    standard_contract=True,
                    adjusted=False,
                    source=str(item.post_source or "IBKR_REQ_CONTRACT_DETAILS_READONLY"),
                )
            )
    except (KeyError, TypeError, ValueError, InvalidOperation):
        return ()
    return tuple(rows)


def _quote_batch_from_snapshot(snapshot: AtomicBrokerSnapshot) -> OptionQuoteBatch | None:
    if (
        snapshot.quote_batch_id is None
        or snapshot.quote_batch_status is not QuoteBatchStatus.COMPLETE
        or snapshot.quote_batch_requested_at is None
        or snapshot.quote_batch_completed_at is None
        or snapshot.quote_batch_source is None
    ):
        return None
    return OptionQuoteBatch(
        batch_id=snapshot.quote_batch_id,
        status=snapshot.quote_batch_status,
        requested_at=snapshot.quote_batch_requested_at,
        completed_at=snapshot.quote_batch_completed_at,
        source=snapshot.quote_batch_source,
        quotes=snapshot.quotes,
        observed_at=snapshot.quote_batch_observed_at,
        blockers=(),
    )


def _single_symbol(contracts: Sequence[OptionContractRef]) -> str | None:
    values = {item.symbol for item in contracts}
    return next(iter(values)) if len(values) == 1 else None


def _feature_source_history_identity_valid(
    value: object,
    *,
    symbol: str,
    cutoff: datetime,
) -> bool:
    """Validate broker history identity before addressing the local source cache.

    The complete legacy history/ATM gate still runs against the later atomic
    snapshot.  This preflight grants only permission to read observation rows.
    """

    try:
        if (
            not isinstance(value, UnderlyingIvHistory)
            or not value.verify_hash()
            or value.symbol != symbol
            or type(value.contract_id) is not int
            or value.contract_id <= 0
            or value.request_exchange != "SMART"
            or value.currency != "USD"
            or value.duration != "30 D"
            or value.bar_size != "1 day"
            or value.what_to_show != "OPTION_IMPLIED_VOLATILITY"
            or value.use_rth is not True
            or not value.end_at <= value.observed_at <= cutoff
            or (cutoff - value.observed_at).total_seconds() > 60
        ):
            return False
        expected = canonical_hash({
            "symbol": symbol, "contract_id": value.contract_id,
            "security_type": "STK", "request_exchange": "SMART", "currency": "USD",
        })
        days = tuple(row.trading_date for row in value.points)
        return (
            value.basis_hash == expected
            and 10 <= len(days) <= 800
            and days == tuple(sorted(set(days)))
            and all(
                type(day) is date
                and day < cutoff.astimezone(ZoneInfo("America/New_York")).date()
                for day in days
            )
            and all(
                isinstance(row.close, Decimal) and row.close.is_finite()
                and Decimal("0") < row.close <= Decimal("5")
                for row in value.points
            )
        )
    except Exception:
        return False


def _validate_underlying_iv_history(
    value: object,
    *,
    symbol: str,
    snapshot: AtomicBrokerSnapshot,
    atm_iv: Decimal | None,
) -> tuple[str, ...]:
    """Verify one fixed-basis historical series before it becomes hard evidence."""

    if not isinstance(value, UnderlyingIvHistory) or not value.verify_hash():
        return ("IV_HISTORY_HASH_INVALID",)
    if (
        value.symbol != symbol
        or value.contract_id <= 0
        or value.request_exchange != "SMART"
        or value.currency != "USD"
        or value.duration != "30 D"
        or value.bar_size != "1 day"
        or value.what_to_show != "OPTION_IMPLIED_VOLATILITY"
        or value.use_rth is not True
        or value.end_at > snapshot.built_at
        or value.observed_at < value.end_at
        or value.observed_at > snapshot.built_at
        or (snapshot.built_at - value.observed_at).total_seconds() > 60
    ):
        return ("IV_HISTORY_BASIS_INVALID",)
    expected_basis_hash = canonical_hash(
        {
            "symbol": symbol,
            "contract_id": value.contract_id,
            "security_type": "STK",
            "request_exchange": "SMART",
            "currency": "USD",
        }
    )
    if value.basis_hash != expected_basis_hash:
        return ("IV_HISTORY_BASIS_HASH_INVALID",)
    dates = tuple(item.trading_date for item in value.points)
    closes = tuple(item.close for item in value.points)
    cutoff_date = snapshot.built_at.astimezone(
        ZoneInfo("America/New_York")
    ).date()
    if (
        len(closes) < 10
        or len(set(dates)) != len(dates)
        or dates != tuple(sorted(dates))
        or any(item >= cutoff_date for item in dates)
        or any(
            not item.is_finite() or item <= 0 or item > Decimal("5")
            for item in closes
        )
    ):
        return ("IV_HISTORY_POINTS_INVALID",)
    if atm_iv is None or not atm_iv.is_finite() or atm_iv <= 0:
        return ("IV_HISTORY_CURRENT_IV_UNAVAILABLE",)
    scale_ratio = closes[-1] / atm_iv
    if scale_ratio < Decimal("0.1") or scale_ratio > Decimal("10"):
        return ("IV_HISTORY_SCALE_MISMATCH",)
    return ()


def _quote_document(item: BatchedOptionQuote) -> Mapping[str, object]:
    return {
        "con_id": item.contract_id,
        "request_id": item.request_id,
        "bid": item.bid,
        "ask": item.ask,
        "last": item.last,
        "volume": item.volume,
        "open_interest": item.open_interest,
        "implied_volatility": item.implied_volatility,
        "delta": item.delta,
        "gamma": item.gamma,
        "theta": item.theta,
        "vega": item.vega,
        "exchange_time": item.exchange_time,
        "requested_at": item.requested_at,
        "observed_at": item.observed_at,
        "completed_at": item.completed_at,
        "batch_id": item.batch_id,
        "source": item.source,
        "market_data_type": item.market_data_type,
    }


def _broker_contract_quote_evidence(
    secdef: OptionSecDefSnapshot,
    quote: BatchedOptionQuote,
) -> Mapping[str, object]:
    """Persist the exact identity, quote fields, and unchanged liquidity Gate."""

    return {
        "con_id": secdef.contract_id,
        "contract": secdef.identity_dict(),
        "quote": _quote_document(quote),
        "liquidity": option_quote_liquidity_assessment(quote),
    }


def _open_option_combination_count(value: object) -> int:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return 1
    found = False
    for item in value:
        row = _mapping(item)
        quantity = _decimal(row.get("quantity", row.get("position")))
        if quantity is None:
            return 1
        if quantity == 0:
            continue
        security_type = str(row.get("security_type", row.get("secType", ""))).upper()
        if security_type in {"OPT", "OPTION", "BAG", "COMBO"}:
            found = True
    return 1 if found else 0


def _ranking_contracts(rows: Sequence[Mapping[str, object]]) -> tuple[OptionContractRef, ...]:
    contracts: dict[int, OptionContractRef] = {}
    try:
        for row in rows:
            body = row.get("candidate_body", row)
            if not isinstance(body, Mapping):
                return ()
            legs = body.get("legs", ())
            if not isinstance(legs, Sequence) or isinstance(legs, (str, bytes, bytearray)):
                return ()
            for leg in legs:
                if not isinstance(leg, Mapping):
                    return ()
                expiration = leg["expiration"]
                if isinstance(expiration, str):
                    expiration = date.fromisoformat(expiration)
                con_id = int(leg.get("con_id", str(leg["contract_id_ex"]).split("@", 1)[0]))
                contract = OptionContractRef(
                    contract_id=con_id,
                    contract_id_ex=str(leg["contract_id_ex"]),
                    symbol=str(leg.get("underlying", body.get("symbol"))),
                    local_symbol=str(leg.get("local_symbol", leg["contract_id_ex"])),
                    expiration=expiration,
                    strike=Decimal(str(leg["strike"])),
                    right="C" if str(leg["right"]).upper() in {"C", "CALL"} else "P",
                    exchange=str(leg["exchange"]),
                    trading_class=str(leg.get("trading_class", body.get("symbol"))),
                    multiplier=int(leg["multiplier"]),
                    currency=str(leg.get("currency", "USD")),
                )
                prior = contracts.get(con_id)
                if prior is not None and prior != contract:
                    return ()
                contracts[con_id] = contract
    except (KeyError, TypeError, ValueError, InvalidOperation):
        return ()
    return tuple(contracts[key] for key in sorted(contracts))


def _news_candidate(
    row: Mapping[str, object],
    quotes: Mapping[int, BatchedOptionQuote],
    batch: OptionQuoteBatch,
    *,
    now: datetime,
    ranking_snapshot: Mapping[str, object] | None = None,
) -> tuple[IbkrNewsBinding, ConditionalOptionPreselection] | None:
    body = row.get("candidate_body", row)
    if not isinstance(body, Mapping):
        return None
    candidate_id = str(body.get("candidate_id", row.get("candidate_id", ""))).strip()
    candidate_hash = str(row.get("candidate_hash", "")).strip()
    if (
        not candidate_id
        or not _digest(candidate_hash)
        or canonical_hash(body) != candidate_hash
        or str(row.get("candidate_id", candidate_id)).strip() != candidate_id
    ):
        return None
    ranking_snapshot_id: str | None = None
    ranking_candidate_hash: str | None = None
    broker_snapshot_hash: str | None = None
    risk_policy_version: str | None = None
    risk_policy_hash: str | None = None
    execution_cost_contract_version: str | None = None
    execution_cost_contract_hash: str | None = None
    if ranking_snapshot is not None:
        raw_snapshot_id = ranking_snapshot.get("ranking_snapshot_id")
        if not isinstance(raw_snapshot_id, str) or not raw_snapshot_id.strip():
            return None
        snapshot_candidates = ranking_snapshot.get("candidates")
        if not isinstance(snapshot_candidates, Sequence) or isinstance(
            snapshot_candidates,
            (str, bytes, bytearray, memoryview),
        ):
            return None
        matching_snapshot_rows = [
            item
            for item in snapshot_candidates
            if isinstance(item, Mapping)
            and item.get("candidate_id") == candidate_id
            and item.get("candidate_hash") == candidate_hash
            and item.get("candidate_body") == body
        ]
        if len(matching_snapshot_rows) != 1:
            return None
        ranking_snapshot_id = raw_snapshot_id.strip()
        ranking_candidate_hash = candidate_hash
        raw_broker_hash = ranking_snapshot.get("broker_snapshot_hash")
        raw_policy_version = ranking_snapshot.get("current_policy_version")
        raw_policy_hash = ranking_snapshot.get("current_policy_hash")
        raw_cost_version = ranking_snapshot.get("cost_version")
        raw_cost_hash = ranking_snapshot.get("cost_hash")
        broker_snapshot_hash = (
            str(raw_broker_hash) if _digest(raw_broker_hash) else None
        )
        if (
            isinstance(raw_policy_version, str)
            and raw_policy_version.strip()
            and _digest(raw_policy_hash)
        ):
            risk_policy_version = raw_policy_version.strip()
            risk_policy_hash = str(raw_policy_hash)
        if (
            isinstance(raw_cost_version, str)
            and raw_cost_version.strip()
            and _digest(raw_cost_hash)
        ):
            execution_cost_contract_version = raw_cost_version.strip()
            execution_cost_contract_hash = str(raw_cost_hash)
    raw_legs = body.get("legs", ())
    if not isinstance(raw_legs, Sequence) or isinstance(raw_legs, (str, bytes, bytearray)):
        return None
    legs: list[ConditionalOptionLeg] = []
    selected_quotes: list[BatchedOptionQuote] = []
    quoted_legs: list[OptionLegQuote] = []
    try:
        for raw in raw_legs:
            if not isinstance(raw, Mapping):
                return None
            con_id = int(raw.get("con_id", str(raw["contract_id_ex"]).split("@", 1)[0]))
            quote = quotes[con_id]
            if quote.contract_id != con_id or quote.batch_id != batch.batch_id:
                return None
            selected_quotes.append(quote)
            expiry = raw["expiration"]
            if isinstance(expiry, str):
                expiry = date.fromisoformat(expiry)
            right_text = str(raw["right"]).upper()
            side_text = str(raw["side"]).upper()
            if right_text not in {"C", "CALL", "P", "PUT"}:
                return None
            if side_text not in {"BUY", "LONG", "SELL", "SHORT"}:
                return None
            ratio = _positive_integer(raw.get("ratio", 1))
            quantity = _positive_integer(raw.get("quantity", 1))
            multiplier = _decimal(raw.get("multiplier"))
            if (
                ratio is None
                or quantity is None
                or multiplier != Decimal("100")
                or str(raw.get("security_type", "OPT")).upper() != "OPT"
                or str(raw.get("currency", "USD")).upper() != "USD"
            ):
                return None
            right = (
                OptionRight.CALL
                if right_text in {"C", "CALL"}
                else OptionRight.PUT
            )
            side = (
                OptionLegSide.BUY
                if side_text in {"BUY", "LONG"}
                else OptionLegSide.SELL
            )
            underlying = str(raw.get("underlying", body["symbol"])).upper()
            legs.append(
                ConditionalOptionLeg(
                    underlying=underlying,
                    con_id=con_id,
                    expiry=expiry,
                    strike=Decimal(str(raw["strike"])),
                    right=right,
                    side=side,
                    ratio=ratio,
                    quantity=quantity,
                    bid=quote.bid,
                    ask=quote.ask,
                    quote_asof=quote.observed_at,
                    quote_batch_id=batch.batch_id,
                    implied_volatility=quote.implied_volatility,
                    delta=quote.delta,
                    gamma=quote.gamma,
                    theta=quote.theta,
                    vega=quote.vega,
                    volume=quote.volume,
                    open_interest=quote.open_interest,
                    dte=(expiry - utc_datetime(now, field="news adapter clock").date()).days,
                    local_symbol=str(raw["local_symbol"]),
                    trading_class=str(raw["trading_class"]),
                    multiplier=int(multiplier),
                    exchange=str(raw["exchange"]),
                )
            )
            domain_contract = OptionContract(
                contract_id=str(raw["contract_id_ex"]),
                underlying=underlying,
                expiration=expiry,
                strike=Decimal(str(raw["strike"])),
                right=(
                    DomainOptionRight.CALL
                    if right is OptionRight.CALL
                    else DomainOptionRight.PUT
                ),
                multiplier=multiplier,
                currency="USD",
                exchange=str(raw.get("exchange", "SMART")),
                broker_contract_id=con_id,
            )
            domain_leg = OptionLeg(
                contract=domain_contract,
                side=(
                    PositionSide.LONG
                    if side is OptionLegSide.BUY
                    else PositionSide.SHORT
                ),
                quantity=ratio * quantity,
            )
            quoted_legs.append(
                OptionLegQuote(
                    leg=domain_leg,
                    bid=quote.bid,
                    ask=quote.ask,
                    last=quote.last,
                    implied_volatility=quote.implied_volatility,
                    volume=quote.volume,
                    open_interest=quote.open_interest,
                    observed_at=quote.observed_at,
                )
            )
    except (KeyError, TypeError, ValueError, InvalidOperation):
        return None
    if not selected_quotes:
        return None
    observed_at = min(item.observed_at for item in selected_quotes)
    bid, ask = _news_combo_market(quoted_legs)
    volume = (
        min(item.volume for item in selected_quotes if item.volume is not None)
        if all(item.volume is not None for item in selected_quotes)
        else None
    )
    open_interest = (
        min(
            item.open_interest
            for item in selected_quotes
            if item.open_interest is not None
        )
        if all(item.open_interest is not None for item in selected_quotes)
        else None
    )
    symbol = str(body["symbol"]).upper()
    if any(item.underlying != symbol for item in legs):
        return None
    direction = _direction_for_legs(legs)
    binding = IbkrNewsBinding(
        symbol=symbol,
        quote_snapshot_id=batch.batch_id,
        tradability=OptionTradabilityInput(symbol, "IBKR", observed_at, bid, ask, volume, open_interest),
        confirmation=MarketConfirmation("IBKR", observed_at, direction, (batch.batch_id,)),
    )
    exit_plan = body.get("exit_plan", {})
    if not isinstance(exit_plan, Mapping):
        exit_plan = {}
    strategy_hash = strategy_structure_hash(
        symbol,
        str(body.get("structure", row.get("structure", "UNKNOWN"))),
        legs,
    )
    economics = _news_repriced_economics(
        body,
        candidate_id=candidate_id,
        quoted_legs=tuple(quoted_legs),
    )
    evidence_id = f"{batch.batch_id}:{candidate_id}"
    repricing_hash = canonical_hash(
        {
            "schema": "options_copilot.news_reprice_evidence.v1",
            "candidate_hash": candidate_hash,
            "strategy_hash": strategy_hash,
            "quote_batch_id": batch.batch_id,
            "quotes": tuple(_quote_document(item) for item in selected_quotes),
            "economics": (
                None
                if economics is None
                else {
                    "maximum_loss_usd": economics.maximum_loss_usd,
                    "estimated_cost_usd": economics.estimated_cost_usd,
                    "cost_after_ev_usd": economics.cost_after_ev_usd,
                    "commission_usd": economics.commission_usd,
                    "slippage_usd": economics.slippage_usd,
                }
            ),
        }
    )
    strategy_nav_usd = _decimal(body.get("strategy_nav_usd"))
    raw_account_hash = body.get(
        "account_snapshot_hash",
        body.get("strategy_nav_hash"),
    )
    account_snapshot_hash = (
        str(raw_account_hash) if _digest(raw_account_hash) else None
    )
    raw_nav_post_hash = body.get("strategy_nav_post_hash")
    nav_post_hash = (
        str(raw_nav_post_hash) if _digest(raw_nav_post_hash) else None
    )
    if (
        nav_post_hash is None
        and broker_snapshot_hash is not None
        and strategy_nav_usd is not None
        and strategy_nav_usd > 0
    ):
        try:
            nav_post_hash = strategy_nav_post_hash(
                candidate_id=candidate_id,
                strategy_hash=strategy_hash,
                snapshot_hash=broker_snapshot_hash,
                strategy_nav_usd=strategy_nav_usd,
            )
        except (TypeError, ValueError):
            nav_post_hash = None
    preselection = ConditionalOptionPreselection(
        preselection_id=candidate_id,
        underlying=symbol,
        strategy_type=str(body.get("structure", row.get("structure", "UNKNOWN"))),
        phase=_phase(now),
        legs=tuple(legs),
        risk_defined=economics is not None and economics.maximum_loss_usd is not None,
        maximum_loss_usd=(
            None if economics is None else economics.maximum_loss_usd
        ),
        estimated_cost_usd=(
            None if economics is None else economics.estimated_cost_usd
        ),
        cost_after_ev_usd=(
            None if economics is None else economics.cost_after_ev_usd
        ),
        entry_condition="Only after current IBKR quotes and all hard gates remain valid.",
        invalidation_condition=str(exit_plan.get("thesis_invalidation")) if exit_plan.get("thesis_invalidation") else None,
        profit_target_condition=str(exit_plan.get("profit_take")) if exit_plan.get("profit_take") else None,
        stop_loss_condition=str(exit_plan.get("risk_stop")) if exit_plan.get("risk_stop") else None,
        evidence_ids=(evidence_id,),
        evidence_hashes=(repricing_hash,),
        strategy_hash=strategy_hash,
        execution_cost_contract_version=execution_cost_contract_version,
        execution_cost_contract_hash=execution_cost_contract_hash,
        risk_policy_version=risk_policy_version,
        risk_policy_hash=risk_policy_hash,
        broker_snapshot_hash=broker_snapshot_hash,
        account_snapshot_hash=account_snapshot_hash,
        strategy_nav_usd=strategy_nav_usd,
        strategy_nav_post_hash=nav_post_hash,
        economics_quote_batch_id=batch.batch_id,
        economics_quote_asof=observed_at,
        ranking_snapshot_id=ranking_snapshot_id,
        ranking_candidate_hash=ranking_candidate_hash,
        economics_calculation_hash=(
            repricing_hash if ranking_snapshot_id is not None else None
        ),
        research_summary=(
            "Fresh IBKR repricing with exact bounded payoff and scenario EV "
            "recomputed from the frozen, review-only ranking candidate."
        ),
    )
    return binding, preselection


@dataclass(frozen=True, slots=True)
class _NewsRepricedEconomics:
    maximum_loss_usd: Decimal | None
    estimated_cost_usd: Decimal | None
    cost_after_ev_usd: Decimal | None
    commission_usd: Decimal
    slippage_usd: Decimal


def _news_repriced_economics(
    body: Mapping[str, object],
    *,
    candidate_id: str,
    quoted_legs: tuple[OptionLegQuote, ...],
) -> _NewsRepricedEconomics | None:
    """Recompute current executable economics; never reuse frozen prices or EV."""

    if (
        body.get("execution_cost_contract_version") != EXECUTION_COST_VERSION
        or body.get("execution_cost_contract_hash") != EXECUTION_COST_HASH
        or not quoted_legs
        or any(
            item.bid is None
            or item.ask is None
            or item.bid <= 0
            or item.ask <= item.bid
            for item in quoted_legs
        )
    ):
        return None
    total_sides = sum(item.leg.quantity for item in quoted_legs)
    commission = (
        Decimal("2")
        * max(
            MINIMUM_PER_ORDER,
            FALLBACK_PER_CONTRACT_SIDE * Decimal(total_sides),
        )
    ).quantize(CENT, rounding=ROUND_CEILING)
    slippage = sum(
        (
            max(MINIMUM_ENTRY_SLIPPAGE, ENTRY_SPREAD_FACTOR * (item.ask - item.bid))
            + max(MINIMUM_EXIT_SLIPPAGE, EXIT_SPREAD_FACTOR * (item.ask - item.bid))
        )
        * item.contract.multiplier
        * item.leg.quantity
        for item in quoted_legs
        if item.ask is not None and item.bid is not None
    ).quantize(CENT, rounding=ROUND_CEILING)
    scenarios = _news_terminal_scenarios(body.get("terminal_scenarios"))
    try:
        candidate = StrategyCandidate(
            candidate_id=candidate_id,
            leg_quotes=quoted_legs,
            terminal_scenarios=scenarios or (),
            estimated_commissions=commission,
            estimated_slippage=slippage,
        )
        payoff = analyze_expiration_payoff(candidate)
    except (ArithmeticError, TypeError, ValueError, InvalidOperation):
        return None
    if (
        payoff.status is not PayoffStatus.CALCULATED
        or payoff.max_loss is None
        or payoff.max_loss <= 0
    ):
        return None
    debit = sum(
        item.ask * item.contract.multiplier * item.leg.quantity
        for item in quoted_legs
        if item.leg.side is PositionSide.LONG and item.ask is not None
    )
    credit = sum(
        item.bid * item.contract.multiplier * item.leg.quantity
        for item in quoted_legs
        if item.leg.side is PositionSide.SHORT and item.bid is not None
    )
    all_in_cost = (debit - credit + commission + slippage).quantize(
        CENT,
        rounding=ROUND_CEILING,
    )
    after_cost_ev = (
        None
        if not scenarios
        else sum(
            (
                item.probability * payoff.pnl_at(item.terminal_underlying_price)
                for item in scenarios
            ),
            Decimal("0"),
        )
    )
    return _NewsRepricedEconomics(
        maximum_loss_usd=payoff.max_loss,
        # The display contract currently models a non-negative cash outlay.
        # Net-credit structures remain research-only until that contract gains
        # an explicit credit field; inventing an absolute "cost" is forbidden.
        estimated_cost_usd=all_in_cost if all_in_cost >= 0 else None,
        cost_after_ev_usd=after_cost_ev,
        commission_usd=commission,
        slippage_usd=slippage,
    )


def _news_terminal_scenarios(value: object) -> tuple[TerminalScenario, ...] | None:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return None
    try:
        scenarios = tuple(
            TerminalScenario(
                terminal_underlying_price=Decimal(
                    str(
                        item.get(
                            "terminal_underlying_price",
                            item.get("terminal_price"),
                        )
                    )
                ),
                probability=Decimal(str(item.get("probability"))),
            )
            for item in value
            if isinstance(item, Mapping)
        )
    except (InvalidOperation, TypeError, ValueError):
        return None
    if len(scenarios) != len(value) or not scenarios:
        return None
    return scenarios if sum((item.probability for item in scenarios), Decimal("0")) == 1 else None


def _news_combo_market(
    quoted_legs: Sequence[OptionLegQuote],
) -> tuple[Decimal | None, Decimal | None]:
    if not quoted_legs or any(item.bid is None or item.ask is None for item in quoted_legs):
        return None, None
    bid = sum(
        (
            item.bid * item.leg.quantity
            if item.leg.side is PositionSide.LONG
            else -item.ask * item.leg.quantity
        )
        for item in quoted_legs
        if item.bid is not None and item.ask is not None
    )
    ask = sum(
        (
            item.ask * item.leg.quantity
            if item.leg.side is PositionSide.LONG
            else -item.bid * item.leg.quantity
        )
        for item in quoted_legs
        if item.bid is not None and item.ask is not None
    )
    return (bid, ask) if Decimal("0") <= bid <= ask else (None, None)


def _positive_integer(value: object) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) and value > 0 else None


def _direction_for_legs(legs: Sequence[ConditionalOptionLeg]) -> ImpactDirection:
    bought = [item for item in legs if item.side is OptionLegSide.BUY]
    rights = {item.right for item in bought}
    if rights == {OptionRight.CALL}:
        return ImpactDirection.BULLISH
    if rights == {OptionRight.PUT}:
        return ImpactDirection.BEARISH
    return ImpactDirection.NEUTRAL


def _phase(now: datetime) -> PreselectionPhase:
    checked = utc_datetime(now, field="news adapter clock")
    try:
        from zoneinfo import ZoneInfo

        eastern = checked.astimezone(ZoneInfo("America/New_York"))
    except Exception:
        eastern = checked
    return (
        PreselectionPhase.PRE_MARKET
        if eastern.hour < 9 or (eastern.hour == 9 and eastern.minute < 30)
        else PreselectionPhase.OPEN_REPRICED
    )


def _news_research_window(now: datetime) -> bool:
    """Permit IBKR repricing only in weekday US pre/open/post-market windows."""

    checked = utc_datetime(now, field="news adapter clock")
    try:
        from zoneinfo import ZoneInfo

        eastern = checked.astimezone(ZoneInfo("America/New_York"))
    except Exception:
        return False
    minute = eastern.hour * 60 + eastern.minute
    return eastern.weekday() < 5 and 4 * 60 <= minute < 20 * 60


def _event_gate_fields(
    snapshot: Mapping[str, object],
    *,
    symbol: str,
    slot_at: datetime,
    holding_end: date,
) -> dict[str, object]:
    """Separate hard calendar coverage from optional event/news context."""

    checked_symbol = str(symbol).strip().upper()
    slot = utc_datetime(slot_at, field="event gate slot")
    eastern_date = slot.astimezone(ZoneInfo("America/New_York")).date()
    rows = snapshot.get("calendar", ())
    if not _sequence_value(rows):
        rows = ()
    supporting_hashes: list[str] = []
    hard_matching_hashes: list[str] = []
    conflicting_match = False
    envelope = _validated_calendar_generation_envelope(
        snapshot.get("calendar_envelope"),
        slot=slot,
    )
    for row in rows:
        if not isinstance(row, Mapping) or str(row.get("category", "")).upper() != "EARNINGS":
            continue
        symbols = row.get("symbols", ())
        if isinstance(symbols, str):
            symbols = (symbols,)
        if not _sequence_value(symbols) or checked_symbol not in {
            str(item).strip().upper() for item in symbols
        }:
            continue
        event_date = _date_value(row.get("event_date"))
        if event_date is None or not eastern_date <= event_date <= holding_end:
            continue
        digest = row.get("record_hash")
        if not _digest(digest):
            digest = row.get("content_hash")
        if _digest(digest):
            supporting_hashes.append(str(digest))
        if envelope is None or not _calendar_row_in_generation(
            row,
            envelope_hash=envelope["envelope_hash"],
            member_hashes=envelope["member_hashes"],
        ):
            continue
        if str(row.get("status", "")).strip().upper() == "CONFLICTED":
            conflicting_match = True
        if _digest(digest):
            hard_matching_hashes.append(str(digest))

    calendar_hash = snapshot.get("calendar_snapshot_hash")
    generation_hash = snapshot.get("event_generation_hash")
    ready_sources = _ready_earnings_calendar_sources(
        snapshot.get("source_health"),
        slot=slot,
    )
    window_start = _datetime_value(snapshot.get("calendar_window_start"))
    window_end = _datetime_value(snapshot.get("calendar_window_end"))
    coverage_ready = (
        _digest(calendar_hash)
        and _digest(generation_hash)
        and _event_generation_hash_matches(snapshot)
        and envelope is not None
        and envelope["ready_sources"] == {"NASDAQ", "FINNHUB"}
        and ready_sources == {"NASDAQ", "FINNHUB"}
        and not conflicting_match
        and window_start is not None
        and window_end is not None
        and window_start.astimezone(ZoneInfo("America/New_York")).date() <= eastern_date
        and window_end.astimezone(ZoneInfo("America/New_York")).date() >= holding_end
        and window_start == envelope["window_start"]
        and window_end == envelope["window_end"]
    )
    supporting_hash = (
        canonical_hash(
            {
                "schema": "options_copilot.event_supporting_context.v1",
                "symbol": checked_symbol,
                "holding_start": eastern_date.isoformat(),
                "holding_end": holding_end.isoformat(),
                "matching_event_hashes": tuple(sorted(set(supporting_hashes))),
                "event_generation_hash": (
                    generation_hash if _digest(generation_hash) else None
                ),
                "decision_authority": "SUPPORTING_ONLY",
            }
        )
        if supporting_hashes
        else None
    )
    if not coverage_ready:
        return {
            "event_evidence_status": "UNAVAILABLE",
            "earnings_overlap": None,
            "event_defined": False,
            "event_evidence_hash": None,
            "event_supporting_overlap": bool(supporting_hashes),
            "event_supporting_hash": supporting_hash,
        }
    overlap = bool(hard_matching_hashes)
    return {
        "event_evidence_status": "AVAILABLE",
        "earnings_overlap": overlap,
        "event_defined": False,
        "event_evidence_hash": canonical_hash(
            {
                "schema": "options_copilot.event_gate_evidence.v1",
                "symbol": checked_symbol,
                "holding_start": eastern_date.isoformat(),
                "holding_end": holding_end.isoformat(),
                "earnings_overlap": overlap,
                "calendar_snapshot_hash": (
                    calendar_hash if _digest(calendar_hash) else None
                ),
                "event_generation_hash": generation_hash,
                "calendar_envelope_hash": envelope["envelope_hash"],
                "ready_calendar_sources": tuple(sorted(ready_sources)),
                "matching_event_hashes": tuple(
                    sorted(set(hard_matching_hashes))
                ),
                "conflicting_match": conflicting_match,
                "decision_authority": "HARD_DERIVED_CALENDAR_ENVELOPE",
            }
        ),
        "event_supporting_overlap": bool(supporting_hashes),
        "event_supporting_hash": supporting_hash,
    }


def _validated_calendar_generation_envelope(
    value: object,
    *,
    slot: datetime,
) -> dict[str, object] | None:
    if not isinstance(value, Mapping):
        return None
    required = {
        "schema",
        "observed_at",
        "window_start",
        "window_end",
        "source_batches",
        "current_members",
        "current_member_count",
        "envelope_hash",
    }
    if set(value) != required:
        return None
    envelope_hash = value.get("envelope_hash")
    payload = {key: item for key, item in value.items() if key != "envelope_hash"}
    if (
        value.get("schema")
        != "options_copilot.calendar_generation_envelope.v1"
        or not _digest(envelope_hash)
        or canonical_hash(payload) != envelope_hash
    ):
        return None
    observed_at = _datetime_value(value.get("observed_at"))
    window_start = _datetime_value(value.get("window_start"))
    window_end = _datetime_value(value.get("window_end"))
    if (
        observed_at is None
        or window_start is None
        or window_end is None
        or window_start >= window_end
        or abs((observed_at - slot).total_seconds()) > 15 * 60
    ):
        return None
    batches = value.get("source_batches")
    members = value.get("current_members")
    member_count = value.get("current_member_count")
    if (
        not _sequence_value(batches)
        or not _sequence_value(members)
        or not isinstance(member_count, int)
        or isinstance(member_count, bool)
        or member_count != len(members)
    ):
        return None
    flattened_hashes: list[str] = []
    ready_sources: set[str] = set()
    seen_sources: set[str] = set()
    for batch in batches:
        if not isinstance(batch, Mapping) or set(batch) != {
            "schema",
            "source",
            "status",
            "reason",
            "success_count",
            "failure_date_count",
            "observed_at",
            "window_start",
            "window_end",
            "members",
            "batch_hash",
        }:
            return None
        batch_hash = batch.get("batch_hash")
        batch_payload = {
            key: item for key, item in batch.items() if key != "batch_hash"
        }
        source = str(batch.get("source") or "").strip().upper()
        batch_observed = _datetime_value(batch.get("observed_at"))
        batch_members = batch.get("members")
        if (
            batch.get("schema") != "options_copilot.calendar_source_batch.v1"
            or not source
            or source in seen_sources
            or not _digest(batch_hash)
            or canonical_hash(batch_payload) != batch_hash
            or batch_observed is None
            or abs((batch_observed - slot).total_seconds()) > 15 * 60
            or _datetime_value(batch.get("window_start")) != window_start
            or _datetime_value(batch.get("window_end")) != window_end
            or not _sequence_value(batch_members)
        ):
            return None
        seen_sources.add(source)
        batch_member_hashes = []
        for member in batch_members:
            member_hash = _calendar_generation_member_hash(member, source=source)
            if member_hash is None:
                return None
            batch_member_hashes.append(member_hash)
        flattened_hashes.extend(batch_member_hashes)
        successes = batch.get("success_count")
        failures = batch.get("failure_date_count")
        if (
            str(batch.get("status") or "").strip().upper() == "READY"
            and isinstance(successes, int)
            and not isinstance(successes, bool)
            and successes > 0
            and isinstance(failures, int)
            and not isinstance(failures, bool)
            and failures == 0
            and batch_member_hashes
        ):
            ready_sources.add(source)
    member_hashes: list[str] = []
    for member in members:
        member_hash = _calendar_generation_member_hash(member)
        if member_hash is None:
            return None
        member_hashes.append(member_hash)
    if sorted(member_hashes) != sorted(flattened_hashes):
        return None
    return {
        "envelope_hash": str(envelope_hash),
        "window_start": window_start,
        "window_end": window_end,
        "ready_sources": ready_sources,
        "member_hashes": frozenset(member_hashes),
    }


def _calendar_generation_member_hash(
    value: object,
    *,
    source: str | None = None,
) -> str | None:
    if not isinstance(value, Mapping) or set(value) != {
        "identity",
        "source",
        "batch_source",
        "source_id",
        "source_content_hash",
        "record_hash",
        "row_hash",
        "observed_at",
    }:
        return None
    member_source = str(value.get("source") or "").strip().upper()
    batch_source = str(value.get("batch_source") or "").strip().upper()
    if (
        not str(value.get("identity") or "").strip()
        or not member_source
        or not batch_source
        or (source is not None and batch_source != source)
        or not str(value.get("source_id") or "").strip()
        or not _digest(value.get("source_content_hash"))
        or not _digest(value.get("record_hash"))
        or not _digest(value.get("row_hash"))
        or _datetime_value(value.get("observed_at")) is None
    ):
        return None
    return canonical_hash(dict(value))


def _calendar_row_in_generation(
    row: Mapping[str, object],
    *,
    envelope_hash: object,
    member_hashes: object,
) -> bool:
    if (
        row.get("current_generation") is not True
        or row.get("calendar_envelope_hash") != envelope_hash
        or not isinstance(member_hashes, frozenset)
    ):
        return False
    member = {
        "identity": row.get("evidence_identity"),
        "source": str(row.get("source") or "").strip().upper(),
        "batch_source": row.get("calendar_generation_source"),
        "source_id": row.get("source_id"),
        "source_content_hash": row.get("content_hash"),
        "record_hash": row.get("record_hash"),
        "row_hash": row.get("evidence_row_hash"),
        "observed_at": row.get("observed_at"),
    }
    member_hash = _calendar_generation_member_hash(member)
    return (
        member_hash is not None
        and member_hash == row.get("calendar_generation_member_hash")
        and member_hash in member_hashes
    )


def _event_generation_hash_matches(snapshot: Mapping[str, object]) -> bool:
    generation_hash = snapshot.get("event_generation_hash")
    if not _digest(generation_hash):
        return False
    return generation_hash == canonical_hash(
        {
            "news_asof": snapshot.get("news_asof"),
            "source_health": snapshot.get("source_health", []),
            "calendar_asof": snapshot.get("calendar_asof"),
            "calendar_snapshot_hash": snapshot.get("calendar_snapshot_hash"),
            "calendar_window_start": snapshot.get("calendar_window_start"),
            "calendar_window_end": snapshot.get("calendar_window_end"),
            "calendar_envelope": snapshot.get("calendar_envelope"),
            "calendar": snapshot.get("calendar", []),
        }
    )


def _ready_earnings_calendar_sources(
    rows: object,
    *,
    slot: datetime,
) -> set[str]:
    if not _sequence_value(rows):
        return set()
    ready: set[str] = set()
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        source = str(row.get("source", "")).strip().upper()
        if source not in {"NASDAQ", "FINNHUB"}:
            continue
        observed = _datetime_value(row.get("asof"))
        failures = row.get("failure_date_count")
        successes = row.get("success_count")
        if (
            str(row.get("source_kind", "")).strip().upper() == "CALENDAR"
            and str(row.get("status", "")).strip().upper() == "READY"
            and failures == 0
            and isinstance(successes, int)
            and not isinstance(successes, bool)
            and successes > 0
            and observed is not None
            and abs((observed - slot).total_seconds()) <= 15 * 60
        ):
            ready.add(source)
    return ready


def _datetime_value(value: object) -> datetime | None:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc) if value.tzinfo is not None else None
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed.astimezone(timezone.utc) if parsed.tzinfo is not None else None


def _date_value(value: object) -> date | None:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if not isinstance(value, str):
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _decimal(value: object) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return result if result.is_finite() else None


def _digest(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and set(value) <= _DIGEST_CHARS
    )


__all__ = [
    "DirectTop10StructureSource",
    "DurableOptionPoolTop10StructureSource",
    "equity_theses_from_pool_result",
    "GuardedRequestBudget",
    "IBKRNewsResearchAdapter",
    "PacingAuthorityGuard",
    "ProductionBrokerEvidenceAcquisition",
    "ProductionDteGate",
    "ProductionEligibilityGate",
    "ProductionLifecycle",
    "ProductionOptionsEvidenceAcquisition",
    "ProductionOutcomeMarketAdapter",
    "ProductionPipelineInputs",
    "ProductionRiskGate",
    "SerializedBrokerSnapshotProvider",
    "ProductionSingleCombinationGate",
    "ProductionTop10SnapshotProvider",
    "creator_unavailable_reason",
]
