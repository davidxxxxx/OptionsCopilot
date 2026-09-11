"""Read-only after-hours option marks and indicative vertical economics."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from hashlib import sha256
import json
import os
from pathlib import Path
import tempfile
import threading
from typing import Protocol
from zoneinfo import ZoneInfo

from options_copilot.gateway.ibkr_readonly import (
    BatchedOptionQuote,
    BrokerConnectionError,
    MarketDataPacingError,
    OptionContractRef,
    OptionQuoteBatch,
)
from options_copilot.option_pool.models import option_candidate_identity
from options_copilot.storage.canonical import canonical_hash
from options_copilot.strategies import StrategyKind


AFTER_HOURS_INDICATIVE_SCHEMA = "options_copilot.after_hours_indicative.v1"
AFTER_HOURS_STORE_SCHEMA = "options_copilot.after_hours_indicative_store.v1"
_CENT = Decimal("0.01")
_CACHED_CANDIDATE_MAX_AGE = timedelta(minutes=15)
_NEW_YORK_TIME_ZONE = ZoneInfo("America/New_York")
_AFTER_HOURS_STRATEGY_ALIASES = {
    "BULL_CALL_VERTICAL": StrategyKind.DEBIT_VERTICAL.value,
    "BEAR_PUT_VERTICAL": StrategyKind.DEBIT_VERTICAL.value,
    "BULL_CALL_DEBIT_VERTICAL": StrategyKind.DEBIT_VERTICAL.value,
    "BEAR_PUT_DEBIT_VERTICAL": StrategyKind.DEBIT_VERTICAL.value,
    "BULL_PUT_CREDIT_VERTICAL": StrategyKind.CREDIT_VERTICAL.value,
    "BEAR_CALL_CREDIT_VERTICAL": StrategyKind.CREDIT_VERTICAL.value,
    "LONG_CALL": StrategyKind.LONG_OPTION.value,
    "LONG_PUT": StrategyKind.LONG_OPTION.value,
}
_AFTER_HOURS_RESEARCH_BOUNDARY_BLOCKERS = (
    "AFTER_HOURS_RESEARCH_ONLY",
    "FRESH_EXECUTABLE_OPTION_EVIDENCE_REQUIRED",
    "EXECUTABLE_LEG_QUOTE_INCOMPLETE",
    "OPTION_GREEKS_INCOMPLETE",
    "OPTION_LIQUIDITY_EVIDENCE_INCOMPLETE",
    "STRUCTURE_PAYOFF_EVIDENCE_INCOMPLETE",
    "AFTER_COST_ECONOMICS_INCOMPLETE",
)


class AfterHoursResearchLegRatioError(ValueError):
    """A research leg ratio is not an exact positive integer."""


def after_hours_leg_ratio(leg: Mapping[str, object]) -> int:
    """Return one exact ratio, defaulting only a genuinely legacy omission."""

    if "ratio" not in leg:
        return 1
    value = leg.get("ratio")
    if isinstance(value, bool):
        raise AfterHoursResearchLegRatioError("RESEARCH_LEG_RATIO_INVALID")
    if isinstance(value, int):
        result = value
    elif isinstance(value, Decimal):
        if not value.is_finite() or value != value.to_integral_value():
            raise AfterHoursResearchLegRatioError("RESEARCH_LEG_RATIO_INVALID")
        result = int(value)
    elif isinstance(value, str):
        text = value.strip()
        if not text.isascii() or not text.isdigit():
            raise AfterHoursResearchLegRatioError("RESEARCH_LEG_RATIO_INVALID")
        result = int(text)
    else:
        raise AfterHoursResearchLegRatioError("RESEARCH_LEG_RATIO_INVALID")
    if result <= 0:
        raise AfterHoursResearchLegRatioError("RESEARCH_LEG_RATIO_INVALID")
    return result


def after_hours_campaign_lineage_hash(payload: Mapping[str, object]) -> str:
    """Hash the immutable research identities shared by runtime and scheduler."""

    rows = payload.get("candidates")
    candidates = rows if isinstance(rows, Sequence) and not isinstance(
        rows,
        (str, bytes, bytearray),
    ) else ()
    return canonical_hash(
        {
            "schema": "options_copilot.after_hours_formal_pool_lineage.v2",
            "campaign": payload.get("campaign"),
            "candidates": tuple(
                {
                    "research_id": row.get("research_id"),
                    "underlying": row.get("underlying"),
                    "strategy_type": row.get("strategy_type"),
                    "source_scan": row.get("source_scan"),
                    "underlying_quote_basis_hash": row.get(
                        "underlying_quote_basis_hash"
                    ),
                    "legs": tuple(
                        (
                            leg.get("contract_id"),
                            leg.get("expiration"),
                            leg.get("strike"),
                            leg.get("right"),
                            leg.get("side"),
                            leg.get("ratio", 1),
                        )
                        for leg in row.get("legs", ())
                        if isinstance(leg, Mapping)
                    ),
                }
                for row in candidates
                if isinstance(row, Mapping)
            ),
        }
    )


def after_hours_campaign_progress_status(
    payload: Mapping[str, object],
    *,
    maximum_underlyings: int = 10,
) -> str | None:
    """Validate bounded campaign progress for a research-only handoff."""

    if (
        isinstance(maximum_underlyings, bool)
        or not isinstance(maximum_underlyings, int)
        or maximum_underlyings < 1
        or maximum_underlyings > 10
    ):
        raise ValueError("maximum_underlyings must be between one and ten")
    campaign = payload.get("campaign")
    if not isinstance(campaign, Mapping):
        return None
    completed = campaign.get("completed_underlyings")
    target = campaign.get("target_underlyings")
    remaining = campaign.get("remaining_underlyings")
    if any(
        isinstance(value, bool) or not isinstance(value, int)
        for value in (completed, target, remaining)
    ):
        return None
    assert isinstance(completed, int)
    assert isinstance(target, int)
    assert isinstance(remaining, int)
    if (
        target < 1
        or target > maximum_underlyings
        or completed < 1
        or completed > target
        or remaining != target - completed
    ):
        return None
    return "COMPLETE" if remaining == 0 else "PARTIAL"


def after_hours_option_identity_payload(
    row: Mapping[str, object],
) -> dict[str, object] | None:
    """Normalize one research row exactly as the formal option pool does."""

    symbol = str(row.get("underlying", "")).strip().upper()
    raw_legs = row.get("legs")
    if (
        not symbol
        or not isinstance(raw_legs, Sequence)
        or isinstance(raw_legs, (str, bytes, bytearray))
        or not raw_legs
    ):
        return None
    legs: list[dict[str, object]] = []
    for raw in raw_legs:
        if not isinstance(raw, Mapping):
            return None
        try:
            expiration = date.fromisoformat(str(raw.get("expiration", "")))
            contract_id = int(raw.get("contract_id"))
            multiplier = int(raw.get("multiplier"))
            ratio = after_hours_leg_ratio(raw)
        except (TypeError, ValueError):
            return None
        strike = str(raw.get("strike", "")).strip()
        if contract_id <= 0 or multiplier <= 0 or ratio <= 0 or not strike:
            return None
        raw_right = str(raw.get("right", "")).strip().upper()
        right = (
            "CALL"
            if raw_right in {"C", "CALL"}
            else "PUT"
            if raw_right in {"P", "PUT"}
            else raw_right
        )
        legs.append(
            {
                "con_id": contract_id,
                "contract_id_ex": raw.get("contract_id_ex"),
                "expiration": expiration.isoformat(),
                "strike": strike,
                "right": right,
                "side": str(raw.get("side", "")).strip().upper(),
                "ratio": ratio,
                "multiplier": multiplier,
                "exchange": str(raw.get("exchange", "")).strip().upper(),
                "local_symbol": raw.get("local_symbol"),
                "trading_class": raw.get("trading_class"),
            }
        )
    raw_strategy = str(row.get("strategy_type", "")).strip().upper()
    if not raw_strategy:
        rights = {str(leg.get("right", "")).strip().upper() for leg in legs}
        if len(legs) == 1:
            raw_strategy = StrategyKind.LONG_OPTION.value
        elif len(legs) == 2 and len(rights) == 1:
            raw_strategy = StrategyKind.DEBIT_VERTICAL.value
    structure = _AFTER_HOURS_STRATEGY_ALIASES.get(raw_strategy, raw_strategy)
    if structure not in {item.value for item in StrategyKind}:
        return None
    return {
        "symbol": symbol,
        "structure": structure,
        "legs": tuple(legs),
    }


def after_hours_candidate_cache_identity(row: Mapping[str, object]) -> str:
    """Bind cached marks to one normalized research and option identity."""

    raw_legs = row.get("legs")
    if not isinstance(raw_legs, Sequence) or isinstance(
        raw_legs,
        (str, bytes, bytearray),
    ):
        raise ValueError("candidate identity legs must be a sequence")
    for raw in raw_legs:
        if not isinstance(raw, Mapping):
            raise ValueError("candidate identity leg is invalid")
        after_hours_leg_ratio(raw)
    option_payload = after_hours_option_identity_payload(row)
    if option_payload is None:
        raise ValueError("candidate option identity is invalid")
    return canonical_hash(
        {
            "schema": "options_copilot.after_hours_candidate_cache_identity.v1",
            "research_id": _text(
                row.get("research_id", row.get("candidate_id"))
            ),
            "strategy_type": _text(
                row.get("strategy_type", row.get("strategy", option_payload["structure"]))
            ).upper(),
            "option_candidate_identity": option_candidate_identity(option_payload),
        }
    )


def after_hours_candidate_identity_manifest(
    payload: Mapping[str, object],
    *,
    maximum_candidates: int = 10,
) -> tuple[str, ...] | None:
    """Return the fail-closed exact identity manifest for one campaign."""

    rows = payload.get("candidates")
    if (
        isinstance(maximum_candidates, bool)
        or not isinstance(maximum_candidates, int)
        or maximum_candidates < 1
        or maximum_candidates > 10
    ):
        raise ValueError("maximum_candidates must be between one and ten")
    if not isinstance(rows, Sequence) or isinstance(
        rows,
        (str, bytes, bytearray),
    ):
        return None
    identities: list[str] = []
    for row in rows[:maximum_candidates]:
        if not isinstance(row, Mapping):
            return None
        identity_payload = after_hours_option_identity_payload(row)
        if identity_payload is None:
            return None
        identities.append(option_candidate_identity(identity_payload))
    return tuple(identities)


class AfterHoursQuoteProvider(Protocol):
    def option_indicative_quote_batch(
        self,
        contracts: Sequence[OptionContractRef],
    ) -> OptionQuoteBatch:
        ...


class AfterHoursIndicativeStore:
    """Atomic, non-authoritative cache for the latest closing campaign."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._lock = threading.RLock()

    def read(self) -> dict[str, object] | None:
        with self._lock:
            if not self.path.exists():
                return None
            try:
                document = json.loads(self.path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                raise RuntimeError("after-hours cache is unreadable") from exc
        if not isinstance(document, Mapping) or document.get("schema") != AFTER_HOURS_STORE_SCHEMA:
            raise RuntimeError("after-hours cache schema is invalid")
        payload = document.get("payload")
        if not isinstance(payload, Mapping):
            raise RuntimeError("after-hours cache payload is invalid")
        if payload.get("schema") != AFTER_HOURS_INDICATIVE_SCHEMA:
            raise RuntimeError("after-hours cached read model is invalid")
        if document.get("payload_hash") != canonical_hash(payload):
            raise RuntimeError("after-hours cache hash mismatch")
        return dict(payload)

    def write(
        self,
        payload: Mapping[str, object],
        *,
        commit_guard: Callable[[], bool] | None = None,
    ) -> None:
        if payload.get("schema") != AFTER_HOURS_INDICATIVE_SCHEMA:
            raise ValueError("after-hours read model schema is invalid")
        safe_payload = dict(payload)
        document = {
            "schema": AFTER_HOURS_STORE_SCHEMA,
            "written_at": datetime.now(timezone.utc).isoformat(),
            "payload": safe_payload,
            "payload_hash": canonical_hash(safe_payload),
        }
        body = json.dumps(
            document,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
        with self._lock:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            handle, temporary = tempfile.mkstemp(
                prefix=f".{self.path.name}.",
                suffix=".tmp",
                dir=self.path.parent,
            )
            try:
                with os.fdopen(handle, "w", encoding="utf-8", newline="\n") as stream:
                    stream.write(body)
                    stream.flush()
                    os.fsync(stream.fileno())
                if commit_guard is not None and not commit_guard():
                    raise TimeoutError("after-hours cache commit cancelled")
                os.replace(temporary, self.path)
            finally:
                if os.path.exists(temporary):
                    os.unlink(temporary)


def build_after_hours_indicative_read_model(
    research: Mapping[str, object],
    *,
    quote_provider: AfterHoursQuoteProvider,
    strategy_nav_usd: Decimal | None,
    normal_risk_fraction: Decimal = Decimal("0.10"),
    maximum_candidates: int = 10,
    maximum_quote_attempts: int | None = None,
    previous_read_model: Mapping[str, object] | None = None,
    clock: Callable[[], datetime] | None = None,
) -> dict[str, object]:
    """Progressively reprice exact structures without creating authority.

    Each two-leg structure is one bounded broker batch.  This prevents a slow
    historical fallback near the front of a ten-candidate list from consuming
    the single owner deadline that the remaining structures need.  Previously
    priced identities may be reused by a later pacing-window heartbeat.
    """

    observed_at = _clock_time(clock)
    base = _base_model(observed_at)
    if maximum_candidates < 1 or maximum_candidates > 10:
        raise ValueError("maximum_candidates must be between one and ten")
    if maximum_quote_attempts is not None and (
        maximum_quote_attempts < 1 or maximum_quote_attempts > maximum_candidates
    ):
        raise ValueError("maximum_quote_attempts is outside the candidate limit")
    if normal_risk_fraction <= 0 or normal_risk_fraction > Decimal("0.20"):
        raise ValueError("normal_risk_fraction is outside the locked ceiling")
    rows = _research_rows(research)[:maximum_candidates]
    if not rows:
        return {
            **base,
            "reason_codes": ["RESEARCH_STRUCTURES_UNAVAILABLE"],
        }
    try:
        candidate_identities = tuple(
            after_hours_candidate_cache_identity(row) for row in rows
        )
    except AfterHoursResearchLegRatioError:
        return {
            **base,
            "reason_codes": ["RESEARCH_LEG_RATIO_INVALID"],
        }
    except (KeyError, TypeError, ValueError):
        return {
            **base,
            "reason_codes": ["RESEARCH_CONTRACT_IDENTITY_INVALID"],
        }
    try:
        contracts_by_candidate = tuple(_candidate_contracts(row) for row in rows)
    except (KeyError, TypeError, ValueError):
        return {
            **base,
            "reason_codes": ["RESEARCH_CONTRACT_IDENTITY_INVALID"],
        }
    all_contracts = tuple(
        contract for candidate_contracts in contracts_by_candidate for contract in candidate_contracts
    )
    if len({contract.contract_id for contract in all_contracts}) != len(all_contracts):
        return {
            **base,
            "reason_codes": ["RESEARCH_CONTRACT_IDENTITY_DUPLICATE"],
        }
    previous_by_identity, stale_previous_by_identity = (
        _previous_candidates_by_freshness(
            previous_read_model,
            now=observed_at,
        )
    )
    candidates: list[dict[str, object]] = []
    batch_ids: list[str] = []
    batch_sources: list[str] = []
    batch_statuses: list[QuoteBatchStatus] = []
    batch_observations: list[datetime] = []
    reused_observations: list[datetime] = []
    stale_observations: list[datetime] = []
    reason_codes: list[str] = []
    attempted_count = 0
    reused_count = 0
    reused_mark_evidence_count = 0
    stale_reused_count = 0
    stopped = False
    for row, candidate_contracts, identity in zip(
        rows,
        contracts_by_candidate,
        candidate_identities,
        strict=True,
    ):
        prior = previous_by_identity.get(identity)
        if prior is not None:
            cached_quotes = _cached_quote_evidence(
                prior[0],
                observed_at=prior[1],
            )
            projected = _candidate_projection(
                row,
                candidate_contracts,
                cached_quotes,
                observed_at=observed_at,
                strategy_nav_usd=strategy_nav_usd,
                normal_risk_fraction=normal_risk_fraction,
            )
            candidates.append(projected)
            reused_observations.append(prior[1])
            reused_mark_evidence_count += 1
            if projected["pricing_status"] == "AVAILABLE":
                reused_count += 1
            continue
        stale_prior = stale_previous_by_identity.get(identity)
        quote_by_id: dict[int, BatchedOptionQuote] = {}
        if maximum_quote_attempts is not None and attempted_count >= maximum_quote_attempts:
            reason_codes.append("AFTER_HOURS_REPRICE_CONTINUES_NEXT_WINDOW")
        elif not stopped:
            attempted_count += 1
            try:
                batch = quote_provider.option_indicative_quote_batch(
                    candidate_contracts
                )
            except MarketDataPacingError as exc:
                reason_codes.append(exc.reason_code)
                stopped = True
            except BrokerConnectionError:
                reason_codes.append("IBKR_READONLY_GATEWAY_DISCONNECTED")
                stopped = True
            except Exception:
                # One candidate may time out independently.  Continue so a slow
                # contract cannot starve every later structure.
                reason_codes.append("AFTER_HOURS_OPTION_QUOTE_UNAVAILABLE")
            else:
                batch_ids.append(batch.batch_id)
                batch_sources.append(batch.source)
                batch_statuses.append(batch.status)
                batch_observations.append(batch.observed_at or batch.completed_at)
                reason_codes.extend(batch.blockers)
                quote_by_id = {quote.contract_id: quote for quote in batch.quotes}
        projected = _candidate_projection(
            row,
            candidate_contracts,
            quote_by_id,
            observed_at=observed_at,
            strategy_nav_usd=strategy_nav_usd,
            normal_risk_fraction=normal_risk_fraction,
        )
        if (
            projected["mark_evidence_status"] != "AVAILABLE"
            and stale_prior is not None
        ):
            projected = _candidate_projection(
                row,
                candidate_contracts,
                _cached_quote_evidence(
                    stale_prior[0],
                    observed_at=stale_prior[1],
                ),
                observed_at=observed_at,
                strategy_nav_usd=strategy_nav_usd,
                normal_risk_fraction=normal_risk_fraction,
            )
            projected = _mark_candidate_stale(projected)
            stale_observations.append(stale_prior[1])
            stale_reused_count += 1
            reason_codes.append("AFTER_HOURS_CACHED_MARKS_STALE")
        candidates.append(projected)
    priced_count = sum(item["pricing_status"] == "AVAILABLE" for item in candidates)
    mark_evidence_count = sum(
        item["mark_evidence_status"] in {"AVAILABLE", "STALE"}
        for item in candidates
    )
    if priced_count < len(candidates) and (priced_count > 0 or not stopped):
        reason_codes.append("AFTER_HOURS_INDICATIVE_PARTIAL")
    reason_codes = list(dict.fromkeys(reason_codes))
    complete_without_blockers = priced_count == len(candidates) and not reason_codes
    observed = (
        min(stale_observations)
        if stale_observations
        else max((*batch_observations, *reused_observations), default=observed_at)
    )
    quote_batch_id = _campaign_batch_id(batch_ids)
    if quote_batch_id is None and (
        reused_mark_evidence_count or stale_reused_count
    ):
        quote_batch_id = _optional_text(
            None if previous_read_model is None else previous_read_model.get("quote_batch_id")
        )
    sources = list(dict.fromkeys(batch_sources))
    if (
        not sources
        and (reused_mark_evidence_count or stale_reused_count)
        and previous_read_model is not None
    ):
        prior_source = _optional_text(previous_read_model.get("quote_source"))
        if prior_source is not None:
            sources.append(prior_source)
    return {
        **base,
        "status": "AVAILABLE" if complete_without_blockers else "DEGRADED",
        "freshness_status": (
            "STALE"
            if stale_reused_count
            else "CURRENT"
            if mark_evidence_count
            else "UNAVAILABLE"
        ),
        "observed_at": observed.astimezone(timezone.utc).isoformat(),
        "quote_batch_id": quote_batch_id,
        "quote_batch_status": (
            "COMPLETE"
            if complete_without_blockers
            else "PARTIAL"
            if batch_ids or reused_mark_evidence_count or stale_reused_count
            else "UNAVAILABLE"
        ),
        "quote_source": "+".join(sources) or None,
        "quote_batch_count": len(batch_ids),
        "attempted_candidate_count": attempted_count,
        "reused_priced_count": reused_count,
        "reused_mark_evidence_count": reused_mark_evidence_count,
        "stale_reused_count": stale_reused_count,
        "requested_count": len(candidates),
        "priced_count": priced_count,
        "mark_evidence_count": mark_evidence_count,
        "reason_codes": reason_codes,
        "strategy_nav_usd": _decimal_text(strategy_nav_usd),
        "normal_risk_fraction": format(normal_risk_fraction, "f"),
        "candidates": candidates,
    }


def _campaign_batch_id(batch_ids: Sequence[str]) -> str | None:
    unique = tuple(dict.fromkeys(item.strip() for item in batch_ids if item.strip()))
    if not unique:
        return None
    if len(unique) == 1:
        return unique[0]
    digest = sha256("\x00".join(unique).encode("utf-8")).hexdigest()
    return f"after-hours-campaign.{digest}"


def _previous_candidates_by_freshness(
    value: Mapping[str, object] | None,
    *,
    now: datetime,
) -> tuple[
    dict[str, tuple[Mapping[str, object], datetime]],
    dict[str, tuple[Mapping[str, object], datetime]],
]:
    if value is None:
        return {}, {}
    previous_observed_at = _aware_time(value.get("observed_at"))
    raw_rows = value.get("candidates")
    if not isinstance(raw_rows, Sequence) or isinstance(
        raw_rows,
        (str, bytes, bytearray),
    ):
        return {}, {}
    fresh: dict[str, tuple[Mapping[str, object], datetime]] = {}
    stale: dict[str, tuple[Mapping[str, object], datetime]] = {}
    for row in raw_rows:
        if not isinstance(row, Mapping):
            continue
        raw_legs = row.get("legs")
        if not isinstance(raw_legs, Sequence) or isinstance(
            raw_legs,
            (str, bytes, bytearray),
        ):
            continue
        try:
            identity = after_hours_candidate_cache_identity(row)
        except (TypeError, ValueError):
            continue
        if len(raw_legs) > 0:
            candidate_observed_at = _candidate_observed_at(
                row,
                fallback=previous_observed_at,
            )
            if candidate_observed_at is None:
                continue
            if not _cached_quote_evidence(
                row,
                observed_at=candidate_observed_at,
            ):
                continue
            age = now - candidate_observed_at
            target = (
                fresh
                if timedelta(0) <= age <= _CACHED_CANDIDATE_MAX_AGE
                else stale
            )
            target[identity] = (row, candidate_observed_at)
    return fresh, stale


def _cached_quote_evidence(
    candidate: Mapping[str, object],
    *,
    observed_at: datetime,
) -> dict[int, BatchedOptionQuote]:
    """Recover only immutable leg marks; current economics are never cached."""

    raw_legs = candidate.get("legs")
    if not isinstance(raw_legs, Sequence) or isinstance(
        raw_legs,
        (str, bytes, bytearray),
    ):
        return {}
    quotes: list[BatchedOptionQuote] = []
    sides: list[str] = []
    for raw in raw_legs:
        if not isinstance(raw, Mapping):
            return {}
        try:
            contract_id = _positive_int(raw.get("contract_id"))
        except (TypeError, ValueError):
            return {}
        side = _optional_text(raw.get("side"))
        if side is None or side.upper() not in {"BUY", "SELL"}:
            return {}
        quote_asof = _aware_time(raw.get("quote_asof")) or observed_at
        raw_market_data_type = raw.get("market_data_type")
        market_data_type = (
            raw_market_data_type
            if isinstance(raw_market_data_type, int)
            and not isinstance(raw_market_data_type, bool)
            else None
        )
        quotes.append(
            BatchedOptionQuote(
                contract_id=contract_id,
                batch_id="AFTER_HOURS_CACHED_MARK_EVIDENCE",
                request_id=f"cached.{contract_id}",
                requested_at=quote_asof,
                observed_at=quote_asof,
                completed_at=quote_asof,
                source="AFTER_HOURS_CACHED_MARK_EVIDENCE",
                bid=_decimal(raw.get("bid")),
                ask=_decimal(raw.get("ask")),
                last=_decimal(raw.get("last")),
                close=_decimal(raw.get("close")),
                exchange_time=quote_asof,
                market_data_type=market_data_type,
                research_price_basis=_optional_text(raw.get("price_basis")),
            )
        )
        sides.append(side.upper())
    marks, bases = _coherent_indicative_marks(quotes, sides=sides)
    if any(mark is None for mark in marks) or any(basis is None for basis in bases):
        return {}
    return {quote.contract_id: quote for quote in quotes}


def _mark_candidate_stale(value: Mapping[str, object]) -> dict[str, object]:
    result = dict(value)
    if result.get("pricing_status") == "AVAILABLE":
        result["pricing_status"] = "STALE"
    result["freshness_status"] = "STALE"
    result["mark_evidence_status"] = "STALE"
    blockers = result.get("blockers")
    result["blockers"] = list(
        dict.fromkeys(
            (
                *(
                    tuple(str(item) for item in blockers)
                    if isinstance(blockers, Sequence)
                    and not isinstance(blockers, (str, bytes, bytearray))
                    else ()
                ),
                "AFTER_HOURS_CACHED_MARKS_STALE",
            )
        )
    )
    return result


def unavailable_after_hours_indicative_read_model(reason: str) -> dict[str, object]:
    """Return one explicit fail-closed after-hours research projection."""

    return {
        **_base_model(datetime.now(timezone.utc)),
        "reason_codes": [_text(reason).upper()],
    }


def _base_model(observed_at: datetime) -> dict[str, object]:
    return {
        "schema": AFTER_HOURS_INDICATIVE_SCHEMA,
        "status": "UNAVAILABLE",
        "freshness_status": "UNAVAILABLE",
        "decision": "NO_TRADE",
        "mode": "AFTER_HOURS_INDICATIVE",
        "observed_at": observed_at.astimezone(timezone.utc).isoformat(),
        "quote_batch_id": None,
        "quote_batch_status": "UNAVAILABLE",
        "quote_source": None,
        "requested_count": 0,
        "priced_count": 0,
        "reason_codes": [],
        "strategy_nav_usd": None,
        "normal_risk_fraction": "0.10",
        "candidates": [],
        "decision_authority": "SUPPORTING_ONLY",
        "approval_eligible": False,
        "instruction_creation_allowed": False,
        "order_allowed": False,
        "review_only": True,
        "direct_order_submission": False,
    }


def _research_rows(research: Mapping[str, object]) -> tuple[Mapping[str, object], ...]:
    for key in ("open_repriced", "candidates", "premarket"):
        value = research.get(key)
        if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
            rows = tuple(item for item in value if isinstance(item, Mapping))
            if rows:
                return rows
    return ()


def _candidate_contracts(row: Mapping[str, object]) -> tuple[OptionContractRef, ...]:
    underlying = _text(row.get("underlying", row.get("symbol"))).upper()
    raw_legs = row["legs"]
    if not isinstance(raw_legs, Sequence) or isinstance(raw_legs, (str, bytes, bytearray)):
        raise TypeError("legs must be a sequence")
    if len(raw_legs) != 2 or not all(isinstance(item, Mapping) for item in raw_legs):
        raise ValueError("an indicative structure must contain exactly two legs")
    contracts: list[OptionContractRef] = []
    for raw in raw_legs:
        assert isinstance(raw, Mapping)
        expiration = date.fromisoformat(_text(raw.get("expiration", raw.get("expiry"))))
        right = _text(raw.get("right")).upper()
        if right not in {"C", "P"}:
            raise ValueError("option right is invalid")
        contracts.append(
            OptionContractRef(
                contract_id=_positive_int(raw.get("contract_id")),
                contract_id_ex=_text(raw.get("contract_id_ex")),
                symbol=underlying,
                local_symbol=_text(raw.get("local_symbol")),
                expiration=expiration,
                strike=_positive_decimal(raw.get("strike")),
                right=right,  # type: ignore[arg-type]
                exchange=_text(raw.get("exchange")),
                trading_class=_text(raw.get("trading_class")),
                multiplier=_positive_int(raw.get("multiplier")),
            )
        )
    return tuple(contracts)


def _candidate_projection(
    row: Mapping[str, object],
    contracts: Sequence[OptionContractRef],
    quote_by_id: Mapping[int, BatchedOptionQuote],
    *,
    observed_at: datetime,
    strategy_nav_usd: Decimal | None,
    normal_risk_fraction: Decimal,
) -> dict[str, object]:
    raw_legs = row["legs"]
    assert isinstance(raw_legs, Sequence)
    sides: list[str] = []
    ratios: list[int] = []
    quotes: list[BatchedOptionQuote | None] = []
    for raw, contract in zip(raw_legs, contracts, strict=True):
        assert isinstance(raw, Mapping)
        side = _text(raw.get("side")).upper()
        if side not in {"BUY", "SELL"}:
            side = "BUY" if not sides else "SELL"
        sides.append(side)
        ratios.append(after_hours_leg_ratio(raw))
        quotes.append(quote_by_id.get(contract.contract_id))
    marks, bases = _coherent_indicative_marks(quotes, sides=sides)
    new_york_date = observed_at.astimezone(_NEW_YORK_TIME_ZONE).date()
    raw_dtes = tuple(
        (contract.expiration - new_york_date).days for contract in contracts
    )
    leg_dtes = tuple(None if value < 0 else value for value in raw_dtes)
    same_expiration = len({contract.expiration for contract in contracts}) == 1
    candidate_dte = (
        leg_dtes[0]
        if same_expiration and all(value is not None for value in leg_dtes)
        else None
    )
    leg_rows: list[dict[str, object]] = []
    for raw, contract, quote, side, mark, basis, dte in zip(
        raw_legs,
        contracts,
        quotes,
        sides,
        marks,
        bases,
        leg_dtes,
        strict=True,
    ):
        assert isinstance(raw, Mapping)
        leg_rows.append(
            {
                "side": side,
                "ratio": ratios[len(leg_rows)],
                "contract_id": contract.contract_id,
                "contract_id_ex": contract.contract_id_ex,
                "local_symbol": contract.local_symbol,
                "strike": format(contract.strike, "f"),
                "right": contract.right,
                "expiration": contract.expiration.isoformat(),
                "dte": dte,
                "exchange": contract.exchange,
                "trading_class": contract.trading_class,
                "multiplier": contract.multiplier,
                "bid": _decimal_text(None if quote is None else quote.bid),
                "ask": _decimal_text(None if quote is None else quote.ask),
                "last": _decimal_text(None if quote is None else quote.last),
                "close": _decimal_text(None if quote is None else quote.close),
                "indicative_mark": _decimal_text(mark),
                "price_basis": basis,
                "market_data_type": None if quote is None else quote.market_data_type,
                "quote_asof": None if quote is None or quote.exchange_time is None else quote.exchange_time.isoformat(),
            }
        )
    blockers: list[str] = []
    if any(value < 0 for value in raw_dtes):
        blockers.append("OPTION_CONTRACT_EXPIRED")
    if not same_expiration:
        blockers.append("OPTION_EXPIRATION_MISMATCH")
    if any(mark is None for mark in marks):
        blockers.append("INDICATIVE_LEG_PRICE_UNAVAILABLE")
    if any(basis is None for basis in bases):
        blockers.append("INDICATIVE_PRICE_BASIS_UNAVAILABLE")
    if len({basis for basis in bases if basis is not None}) > 1:
        blockers.append("INDICATIVE_PRICE_BASIS_MIXED")
    mark_evidence_available = not any(mark is None for mark in marks) and not any(
        basis is None for basis in bases
    )
    strategy_type = _text(
        row.get("strategy_type", row.get("strategy", "DEBIT_VERTICAL"))
    ).upper()
    if any(ratio != 1 for ratio in ratios):
        blockers.append("INDICATIVE_VERTICAL_RATIO_UNSUPPORTED")
    if strategy_type not in {
        "BULL_CALL_VERTICAL",
        "BEAR_PUT_VERTICAL",
        "BULL_CALL_DEBIT_VERTICAL",
        "BEAR_PUT_DEBIT_VERTICAL",
        "DEBIT_VERTICAL",
    }:
        blockers.append("INDICATIVE_STRATEGY_ECONOMICS_UNSUPPORTED")
    direction = _strategy_direction(
        strategy_type,
        contracts=contracts,
        sides=sides,
    )
    if (
        strategy_type
        in {
            "BULL_CALL_VERTICAL",
            "BEAR_PUT_VERTICAL",
            "BULL_CALL_DEBIT_VERTICAL",
            "BEAR_PUT_DEBIT_VERTICAL",
            "DEBIT_VERTICAL",
        }
        and direction == "NEUTRAL_OR_UNSPECIFIED"
    ):
        blockers.append("INDICATIVE_VERTICAL_GEOMETRY_INVALID")
    debit: Decimal | None = None
    maximum_loss: Decimal | None = None
    maximum_profit: Decimal | None = None
    breakeven_price: Decimal | None = None
    risk_fraction: Decimal | None = None
    execution_cost = _nonnegative_decimal(row.get("execution_cost_cap_usd")) or Decimal("0")
    quantity = _positive_int(row.get("quantity", 1))
    multiplier = contracts[0].multiplier
    if not blockers:
        buy = marks[0] if str(leg_rows[0]["side"]) == "BUY" else marks[1]
        sell = marks[1] if str(leg_rows[1]["side"]) == "SELL" else marks[0]
        assert buy is not None and sell is not None
        debit = ((buy - sell) * Decimal(multiplier * quantity)).quantize(_CENT, rounding=ROUND_HALF_UP)
        width = abs(contracts[0].strike - contracts[1].strike) * Decimal(multiplier * quantity)
        if debit <= 0 or debit > width:
            blockers.append("INDICATIVE_VERTICAL_ECONOMICS_INVALID")
            debit = None
        else:
            maximum_loss = (debit + execution_cost).quantize(_CENT, rounding=ROUND_HALF_UP)
            maximum_profit = (width - maximum_loss).quantize(
                _CENT,
                rounding=ROUND_HALF_UP,
            )
            geometry = _debit_vertical_geometry(contracts=contracts, sides=sides)
            assert geometry is not None
            _, long_contract = geometry
            all_in_cost_per_share = maximum_loss / Decimal(
                multiplier * quantity
            )
            breakeven_price = (
                long_contract.strike + all_in_cost_per_share
                if direction == "BULLISH"
                else long_contract.strike - all_in_cost_per_share
            ).quantize(_CENT, rounding=ROUND_HALF_UP)
            if maximum_profit <= 0:
                blockers.append("INDICATIVE_AFTER_COST_UPSIDE_NONPOSITIVE")
            if strategy_nav_usd is None or strategy_nav_usd <= 0:
                blockers.append("STRATEGY_NAV_UNAVAILABLE")
            else:
                risk_fraction = maximum_loss / strategy_nav_usd
                if maximum_loss > strategy_nav_usd * normal_risk_fraction:
                    blockers.append("INDICATIVE_RISK_CAP_EXCEEDED")
    # Closing marks may support display-only debit and risk geometry, but they
    # cannot satisfy any executable quote, volatility, liquidity, or EV Gate.
    blockers.extend(_AFTER_HOURS_RESEARCH_BOUNDARY_BLOCKERS)
    rank = _positive_int(row.get("rank", row.get("repriced_rank", 1)))
    return {
        "research_id": _text(row.get("research_id", row.get("candidate_id", f"research-{rank}"))),
        "rank": rank,
        "underlying": _text(row.get("underlying", row.get("symbol"))).upper(),
        "sector": _optional_text(row.get("sector")) or "UNCLASSIFIED",
        "source_scan": _optional_text(row.get("source_scan")) or "UNKNOWN",
        "strategy_type": strategy_type,
        "direction": direction,
        "research_summary": _optional_text(row.get("research_summary")),
        "entry_condition": _optional_text(row.get("entry_condition")),
        "invalidation_condition": _optional_text(
            row.get("invalidation_condition")
        ),
        "profit_target_condition": _optional_text(
            row.get("profit_target_condition")
        ),
        "stop_loss_condition": _optional_text(row.get("stop_loss_condition")),
        "expiration": contracts[0].expiration.isoformat(),
        "dte": candidate_dte,
        "quantity": quantity,
        "mark_evidence_status": (
            "AVAILABLE" if mark_evidence_available else "UNAVAILABLE"
        ),
        "pricing_status": "AVAILABLE" if debit is not None else "UNAVAILABLE",
        "freshness_status": (
            "CURRENT" if mark_evidence_available else "UNAVAILABLE"
        ),
        "quote_status": "UNAVAILABLE",
        "greeks_status": "UNAVAILABLE",
        "liquidity_status": "UNAVAILABLE",
        "indicative_entry_debit_usd": _decimal_text(debit),
        "execution_cost_cap_usd": _decimal_text(execution_cost),
        "indicative_maximum_loss_usd": _decimal_text(maximum_loss),
        "indicative_maximum_profit_usd": _decimal_text(maximum_profit),
        "breakeven_price": _decimal_text(breakeven_price),
        "indicative_cost_after_ev_usd": None,
        "strategy_nav_fraction": _decimal_text(risk_fraction),
        "indicative_price_basis": (
            bases[0]
            if bases[0] is not None and all(basis == bases[0] for basis in bases)
            else None
        ),
        "blockers": list(dict.fromkeys(blockers)),
        "legs": leg_rows,
        "trade_status": "NO_TRADE",
        "decision_authority": "SUPPORTING_ONLY",
        "approval_eligible": False,
        "instruction_creation_allowed": False,
        "order_allowed": False,
    }


def _coherent_indicative_marks(
    quotes: Sequence[BatchedOptionQuote | None],
    *,
    sides: Sequence[str],
) -> tuple[list[Decimal | None], list[str | None]]:
    if len(quotes) != len(sides):
        raise ValueError("quote and side cardinality mismatch")
    if quotes and all(
        quote is not None
        and quote.bid is not None
        and quote.ask is not None
        and quote.ask >= quote.bid
        for quote in quotes
    ):
        return (
            [
                quote.ask if side == "BUY" else quote.bid
                for quote, side in zip(quotes, sides, strict=True)
                if quote is not None
            ],
            ["FROZEN_BBO"] * len(quotes),
        )
    if quotes and all(
        quote is not None
        and quote.research_price_basis == "PREVIOUS_SESSION_LAST_TRADE"
        and quote.last is not None
        and quote.last > 0
        for quote in quotes
    ):
        return (
            [quote.last for quote in quotes if quote is not None],
            ["PREVIOUS_SESSION_LAST_TRADE"] * len(quotes),
        )
    if quotes and all(
        quote is not None and quote.last is not None and quote.last > 0
        for quote in quotes
    ):
        return (
            [quote.last for quote in quotes if quote is not None],
            ["LAST"] * len(quotes),
        )
    if quotes and all(
        quote is not None and quote.close is not None and quote.close > 0
        for quote in quotes
    ):
        return (
            [quote.close for quote in quotes if quote is not None],
            ["PREVIOUS_CLOSE"] * len(quotes),
        )
    fallback = [
        _indicative_mark(quote, side=side)
        for quote, side in zip(quotes, sides, strict=True)
    ]
    return (
        [item[0] for item in fallback],
        [item[1] for item in fallback],
    )


def _indicative_mark(
    quote: BatchedOptionQuote | None,
    *,
    side: str,
) -> tuple[Decimal | None, str | None]:
    if quote is None:
        return None, None
    if (
        quote.research_price_basis == "PREVIOUS_SESSION_LAST_TRADE"
        and quote.last is not None
        and quote.last > 0
    ):
        return quote.last, "PREVIOUS_SESSION_LAST_TRADE"
    if (
        quote.research_price_basis == "PREVIOUS_CLOSE"
        and quote.close is not None
        and quote.close > 0
    ):
        return quote.close, "PREVIOUS_CLOSE"
    if quote.bid is not None and quote.ask is not None and quote.ask >= quote.bid:
        return (quote.ask if side == "BUY" else quote.bid), "FROZEN_BBO"
    if quote.last is not None and quote.last > 0:
        return quote.last, "LAST"
    if quote.close is not None and quote.close > 0:
        return quote.close, "PREVIOUS_CLOSE"
    return None, None


def _text(value: object) -> str:
    result = str(value or "").strip()
    if not result:
        raise ValueError("nonblank text is required")
    return result


def _optional_text(value: object) -> str | None:
    result = str(value or "").strip()
    return result or None


def _clock_time(clock: Callable[[], datetime] | None) -> datetime:
    value = datetime.now(timezone.utc) if clock is None else clock()
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise ValueError("after-hours clock must return an aware datetime")
    return value.astimezone(timezone.utc)


def _aware_time(value: object) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.strip())
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(timezone.utc)


def _candidate_observed_at(
    value: Mapping[str, object],
    *,
    fallback: datetime | None,
) -> datetime | None:
    raw_legs = value.get("legs")
    if isinstance(raw_legs, Sequence) and not isinstance(
        raw_legs,
        (str, bytes, bytearray),
    ):
        observations = tuple(
            parsed
            for leg in raw_legs
            if isinstance(leg, Mapping)
            for parsed in (_aware_time(leg.get("quote_asof")),)
            if parsed is not None
        )
        if observations and len(observations) == len(raw_legs):
            return min(observations)
    return fallback


def _strategy_direction(
    strategy_type: str,
    *,
    contracts: Sequence[OptionContractRef],
    sides: Sequence[str],
) -> str:
    value = strategy_type.strip().upper()
    if value not in {
        "BULL_CALL_VERTICAL",
        "BEAR_PUT_VERTICAL",
        "BULL_CALL_DEBIT_VERTICAL",
        "BEAR_PUT_DEBIT_VERTICAL",
        "DEBIT_VERTICAL",
    }:
        return "NEUTRAL_OR_UNSPECIFIED"
    geometry = _debit_vertical_geometry(contracts=contracts, sides=sides)
    return "NEUTRAL_OR_UNSPECIFIED" if geometry is None else geometry[0]


def _debit_vertical_geometry(
    *,
    contracts: Sequence[OptionContractRef],
    sides: Sequence[str],
) -> tuple[str, OptionContractRef] | None:
    if len(contracts) != 2 or len(sides) != 2:
        return None
    buys = [
        contract
        for contract, side in zip(contracts, sides, strict=True)
        if side == "BUY"
    ]
    sells = [
        contract
        for contract, side in zip(contracts, sides, strict=True)
        if side == "SELL"
    ]
    if len(buys) != 1 or len(sells) != 1:
        return None
    buy, sell = buys[0], sells[0]
    if (
        buy.right != sell.right
        or buy.expiration != sell.expiration
        or buy.multiplier != sell.multiplier
    ):
        return None
    if buy.right == "C" and buy.strike < sell.strike:
        return "BULLISH", buy
    if buy.right == "P" and buy.strike > sell.strike:
        return "BEARISH", buy
    return None


def _positive_int(value: object) -> int:
    if isinstance(value, bool):
        raise ValueError("positive integer required")
    result = int(value)
    if result <= 0:
        raise ValueError("positive integer required")
    return result


def _positive_decimal(value: object) -> Decimal:
    result = _decimal(value)
    if result is None or result <= 0:
        raise ValueError("positive decimal required")
    return result


def _nonnegative_decimal(value: object) -> Decimal | None:
    result = _decimal(value)
    return result if result is not None and result >= 0 else None


def _decimal(value: object) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return result if result.is_finite() else None


def _decimal_text(value: Decimal | None) -> str | None:
    return None if value is None else format(value, "f")


__all__ = [
    "AFTER_HOURS_INDICATIVE_SCHEMA",
    "AFTER_HOURS_STORE_SCHEMA",
    "AfterHoursIndicativeStore",
    "AfterHoursQuoteProvider",
    "AfterHoursResearchLegRatioError",
    "after_hours_candidate_cache_identity",
    "after_hours_candidate_identity_manifest",
    "after_hours_campaign_lineage_hash",
    "after_hours_campaign_progress_status",
    "after_hours_leg_ratio",
    "after_hours_option_identity_payload",
    "build_after_hours_indicative_read_model",
    "unavailable_after_hours_indicative_read_model",
]
