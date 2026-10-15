"""Fail-closed 09:20/09:35 runner for external read-only Top-10 inputs.

This module performs only observation-time validation and lineage binding.  It
has no approval, instruction, bridge, or order authority, and deliberately
does not mutate the shared runtime.  A process restart loses the in-memory
09:20 parent binding, so a standalone 09:35 tick safely returns ``NO_TRADE``.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation
from enum import Enum
import re
from typing import Protocol
from zoneinfo import ZoneInfo

from options_copilot.analytics.scenarios import INITIAL_POLICY_HASH, INITIAL_POLICY_VERSION
from options_copilot.gateway.external_readonly_feed import (
    OPEN_REPRICE_PURPOSE,
    PREMARKET_ACCOUNT_PURPOSE,
)
from options_copilot.storage.canonical import canonical_hash, datetime_text, utc_datetime

from .models import NewsAuthority, OptionRight, PreselectionPhase
from .external_top10_source import ExternalResolvedStructure
from .preselection_producer import ResolvedStructure


NEW_YORK = ZoneInfo("America/New_York")
PREMARKET_SLOT = time(9, 20)
OPEN_REPRICE_SLOT = time(9, 35)
TOP10_COUNT = 10
_SLOT_WINDOW = timedelta(minutes=1)
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_IDENTITY_FIELDS = frozenset(
    {
        "conId",
        "localSymbol",
        "tradingClass",
        "multiplier",
        "exchange",
        "expiry",
        "strike",
        "right",
    }
)


class ExternalTickStatus(str, Enum):
    READY = "READY"
    NO_TRADE = "NO_TRADE"


class TradingSessionGate(Protocol):
    def is_trading_session(self, *, scheduled_for: datetime) -> bool | None:
        raise NotImplementedError


class ExternalReadonlyFeedReader(Protocol):
    def read(self) -> object:
        raise NotImplementedError


class ExternalTop10StructureSource(Protocol):
    def resolve_top10(
        self,
        *,
        scheduled_for: datetime,
    ) -> Sequence[ResolvedStructure]:
        raise NotImplementedError


@dataclass(frozen=True, slots=True, order=True)
class ParentContractIdentity:
    con_id: int
    local_symbol: str
    trading_class: str
    multiplier: int
    exchange: str
    expiry: date
    strike: Decimal
    right: str

    def as_dict(self) -> dict[str, object]:
        return {
            "conId": self.con_id,
            "localSymbol": self.local_symbol,
            "tradingClass": self.trading_class,
            "multiplier": self.multiplier,
            "exchange": self.exchange,
            "expiry": self.expiry,
            "strike": self.strike,
            "right": self.right,
        }


@dataclass(frozen=True, slots=True)
class ParentStructureBinding:
    trading_date: date
    structure_hash: str
    preselection_ids: tuple[str, ...]
    strategy_hashes: tuple[str, ...]
    contracts: tuple[ParentContractIdentity, ...]
    scenario_contract_hash: str
    scenario_hashes: tuple[str, ...]
    current_policy_version: str
    current_policy_hash: str


@dataclass(frozen=True, slots=True)
class ExternalTickResult:
    status: ExternalTickStatus
    slot: str | None
    reason_codes: tuple[str, ...]
    scheduled_for: datetime | None
    external_batch_id: str | None = None
    external_batch_hash: str | None = None
    parent_structure_hash: str | None = None
    scenario_contract_hash: str | None = None
    risk_policy_version: str | None = None
    risk_policy_hash: str | None = None
    structure_count: int = 0
    contract_count: int = 0
    decision_authority: NewsAuthority = field(
        default=NewsAuthority.SUPPORTING_ONLY,
        init=False,
    )
    approval_eligible: bool = field(default=False, init=False)
    instruction_creation_allowed: bool = field(default=False, init=False)
    order_allowed: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "reason_codes", tuple(self.reason_codes))
        if self.status is ExternalTickStatus.READY and self.reason_codes:
            raise ValueError("READY external tick cannot carry blockers")
        if self.status is ExternalTickStatus.NO_TRADE and not self.reason_codes:
            raise ValueError("NO_TRADE external tick requires a blocker")

    def as_dict(self) -> dict[str, object]:
        return {
            "status": self.status.value,
            "slot": self.slot,
            "reason_codes": list(self.reason_codes),
            "scheduled_for": (
                None
                if self.scheduled_for is None
                else datetime_text(self.scheduled_for)
            ),
            "external_batch_id": self.external_batch_id,
            "external_batch_hash": self.external_batch_hash,
            "parent_structure_hash": self.parent_structure_hash,
            "scenario_contract_hash": self.scenario_contract_hash,
            "risk_policy_version": self.risk_policy_version,
            "risk_policy_hash": self.risk_policy_hash,
            "structure_count": self.structure_count,
            "contract_count": self.contract_count,
            "decision_authority": self.decision_authority.value,
            "approval_eligible": self.approval_eligible,
            "instruction_creation_allowed": self.instruction_creation_allowed,
            "order_allowed": self.order_allowed,
        }


class ExternalTop10TickRunner:
    """Validate one exact external tick and bind 09:35 to its 09:20 parent."""

    def __init__(
        self,
        *,
        session_gate: TradingSessionGate,
        feed_reader: ExternalReadonlyFeedReader,
        structure_source: ExternalTop10StructureSource,
    ) -> None:
        for name, value, method in (
            ("session_gate", session_gate, "is_trading_session"),
            ("feed_reader", feed_reader, "read"),
            ("structure_source", structure_source, "resolve_top10"),
        ):
            if not callable(getattr(value, method, None)):
                raise TypeError(f"{name} must expose {method}")
        self._session_gate = session_gate
        self._feed_reader = feed_reader
        self._structure_source = structure_source
        self._parent_binding: ParentStructureBinding | None = None

    @property
    def parent_binding(self) -> ParentStructureBinding | None:
        return self._parent_binding

    def tick(self, *, scheduled_for: datetime) -> ExternalTickResult:
        try:
            scheduled = utc_datetime(scheduled_for, field="scheduled_for")
            scheduled_et = scheduled.astimezone(NEW_YORK)
        except (TypeError, ValueError):
            return _blocked("SCHEDULED_SLOT_INVALID")
        slot_time = scheduled_et.timetz().replace(tzinfo=None)
        if (
            slot_time not in {PREMARKET_SLOT, OPEN_REPRICE_SLOT}
            or scheduled_et.second != 0
            or scheduled_et.microsecond != 0
        ):
            return _blocked("NON_EXACT_EXTERNAL_SLOT", scheduled_for=scheduled)
        slot = (
            "PREMARKET_0920"
            if slot_time == PREMARKET_SLOT
            else "OPEN_REPRICE_0935"
        )

        try:
            session = self._session_gate.is_trading_session(
                scheduled_for=scheduled_for
            )
        except Exception:
            session = None
        if session is not True:
            return _blocked(
                "SESSION_CLOSED" if session is False else "SESSION_GATE_UNKNOWN",
                slot=slot,
                scheduled_for=scheduled,
            )

        if slot_time == OPEN_REPRICE_SLOT and (
            self._parent_binding is None
            or self._parent_binding.trading_date != scheduled_et.date()
        ):
            return _blocked(
                "PARENT_0920_BINDING_MISSING",
                slot=slot,
                scheduled_for=scheduled,
            )

        try:
            batch = self._feed_reader.read()
        except Exception:
            return _blocked(
                "EXTERNAL_BATCH_UNAVAILABLE",
                slot=slot,
                scheduled_for=scheduled,
                parent=self._parent_binding,
            )
        batch_values, reason = validate_external_readonly_batch(
            batch,
            expected_purpose=(
                PREMARKET_ACCOUNT_PURPOSE
                if slot_time == PREMARKET_SLOT
                else OPEN_REPRICE_PURPOSE
            ),
            scheduled_for=scheduled,
        )
        if reason is not None:
            return _blocked(
                reason,
                slot=slot,
                scheduled_for=scheduled,
                parent=self._parent_binding,
            )
        assert batch_values is not None
        batch_id, batch_hash, secdef_rows, quote_rows = batch_values

        if slot_time == PREMARKET_SLOT:
            try:
                raw_structures = self._structure_source.resolve_top10(
                    scheduled_for=scheduled_for
                )
                structures = tuple(raw_structures)
                binding = _parent_binding(
                    structures,
                    trading_date=scheduled_et.date(),
                )
            except Exception:
                return _blocked(
                    "EXTERNAL_TOP10_INVALID",
                    slot=slot,
                    scheduled_for=scheduled,
                    external_batch_id=batch_id,
                    external_batch_hash=batch_hash,
                    parent=self._parent_binding,
                )
            if (
                self._parent_binding is not None
                and self._parent_binding.trading_date == binding.trading_date
                and self._parent_binding.structure_hash != binding.structure_hash
            ):
                return _blocked(
                    "PARENT_STRUCTURE_HASH_CONFLICT",
                    slot=slot,
                    scheduled_for=scheduled,
                    external_batch_id=batch_id,
                    external_batch_hash=batch_hash,
                    parent=self._parent_binding,
                )
            self._parent_binding = binding
            return _ready(
                slot=slot,
                scheduled_for=scheduled,
                external_batch_id=batch_id,
                external_batch_hash=batch_hash,
                parent=binding,
            )

        assert self._parent_binding is not None
        try:
            secdefs = external_contract_identities(secdef_rows)
            quotes = external_contract_identities(quote_rows)
        except Exception:
            return _blocked(
                "OPEN_PARENT_STRUCTURE_MISMATCH",
                slot=slot,
                scheduled_for=scheduled,
                external_batch_id=batch_id,
                external_batch_hash=batch_hash,
                parent=self._parent_binding,
            )
        if secdefs != self._parent_binding.contracts or quotes != secdefs:
            return _blocked(
                "OPEN_PARENT_STRUCTURE_MISMATCH",
                slot=slot,
                scheduled_for=scheduled,
                external_batch_id=batch_id,
                external_batch_hash=batch_hash,
                parent=self._parent_binding,
            )
        return _ready(
            slot=slot,
            scheduled_for=scheduled,
            external_batch_id=batch_id,
            external_batch_hash=batch_hash,
            parent=self._parent_binding,
        )


def validate_external_readonly_batch(
    batch: object,
    *,
    expected_purpose: str,
    scheduled_for: datetime,
) -> tuple[
    tuple[str, str, Sequence[object], Sequence[object]] | None,
    str | None,
]:
    required = (
        "purpose",
        "batch_id",
        "completed_at",
        "content_hash",
        "secdef_rows",
        "quote_rows",
    )
    if any(not hasattr(batch, name) for name in required) or not callable(
        getattr(batch, "verify_hash", None)
    ):
        return None, "EXTERNAL_BATCH_INVALID"
    batch_id = getattr(batch, "batch_id")
    content_hash = getattr(batch, "content_hash")
    secdefs = getattr(batch, "secdef_rows")
    quotes = getattr(batch, "quote_rows")
    if (
        not isinstance(batch_id, str)
        or not batch_id.strip()
        or _SHA256.fullmatch(str(content_hash or "")) is None
        or not isinstance(secdefs, Sequence)
        or isinstance(secdefs, (str, bytes, bytearray, memoryview))
        or not isinstance(quotes, Sequence)
        or isinstance(quotes, (str, bytes, bytearray, memoryview))
    ):
        return None, "EXTERNAL_BATCH_INVALID"
    try:
        if batch.verify_hash() is not True:
            return None, "EXTERNAL_BATCH_HASH_INVALID"
    except Exception:
        return None, "EXTERNAL_BATCH_HASH_INVALID"
    if getattr(batch, "purpose") != expected_purpose:
        return None, "EXTERNAL_BATCH_PURPOSE_MISMATCH"
    try:
        completed_at = utc_datetime(
            getattr(batch, "completed_at"),
            field="batch completed_at",
        )
    except (TypeError, ValueError):
        return None, "EXTERNAL_BATCH_INVALID"
    if not (
        scheduled_for <= completed_at < scheduled_for + _SLOT_WINDOW
    ):
        return None, "EXTERNAL_BATCH_SLOT_MISMATCH"
    if expected_purpose == PREMARKET_ACCOUNT_PURPOSE and (secdefs or quotes):
        return None, "EXTERNAL_BATCH_INVALID"
    if expected_purpose == OPEN_REPRICE_PURPOSE and (not secdefs or not quotes):
        return None, "EXTERNAL_BATCH_INVALID"
    return (batch_id, str(content_hash), secdefs, quotes), None


# Backward-compatible private alias for existing validation-only callers.  New
# production composition uses the public stateless validator so it never
# inherits this runner's in-memory parent-binding lifetime.
_validated_batch = validate_external_readonly_batch


def _parent_binding(
    structures: tuple[ResolvedStructure, ...],
    *,
    trading_date: date,
) -> ParentStructureBinding:
    if len(structures) != TOP10_COUNT or any(
        not isinstance(item, ExternalResolvedStructure) for item in structures
    ):
        raise ValueError(
            "external source must resolve exactly ten scenario-bound structures"
        )
    candidates = tuple(item.candidate for item in structures)
    preselection_ids = tuple(item.preselection_id for item in candidates)
    strategy_hashes = tuple(item.strategy_hash for item in candidates)
    if (
        len(set(preselection_ids)) != TOP10_COUNT
        or len(set(strategy_hashes)) != TOP10_COUNT
    ):
        raise ValueError("external Top-10 identities are duplicated")
    contracts: list[ParentContractIdentity] = []
    seen_contracts: set[int] = set()
    scenario_sets = tuple(item.scenario_set for item in structures)
    for candidate, scenario_set in zip(candidates, scenario_sets, strict=True):
        if (
            candidate.phase is not PreselectionPhase.PRE_MARKET
            or candidate.decision_authority is not NewsAuthority.SUPPORTING_ONLY
            or candidate.approval_eligible
            or candidate.instruction_creation_allowed
            or not candidate.legs
        ):
            raise ValueError("external structure authority is invalid")
        if (
            not scenario_set.verify_hash()
            or scenario_set.candidate_id != candidate.preselection_id
            or scenario_set.strategy_hash != candidate.strategy_hash
            or scenario_set.scenarios != candidate.terminal_scenarios
            or scenario_set.scenario_hash != candidate.scenario_hash
            or scenario_set.current_policy_version != INITIAL_POLICY_VERSION
            or scenario_set.current_policy_hash != INITIAL_POLICY_HASH
            or not scenario_set.scenarios
        ):
            raise ValueError("external scenario contract is invalid")
        scenario_asof = utc_datetime(
            scenario_set.scenario_asof,
            field="scenario_asof",
        )
        scheduled_at = datetime.combine(
            trading_date,
            PREMARKET_SLOT,
            tzinfo=NEW_YORK,
        ).astimezone(scenario_asof.tzinfo)
        scenario_age = Decimal(str((scheduled_at - scenario_asof).total_seconds()))
        if scenario_age < 0 or scenario_age > Decimal("300"):
            raise ValueError("external scenario time is invalid")
        probability = sum(
            (item.probability for item in scenario_set.scenarios),
            Decimal("0"),
        )
        prices = tuple(
            item.terminal_underlying_price for item in scenario_set.scenarios
        )
        if probability != Decimal("1") or len(prices) != len(set(prices)):
            raise ValueError("external scenario probabilities are invalid")
        for leg in candidate.legs:
            identity = leg.contract_ref
            if identity is None or identity.con_id in seen_contracts:
                raise ValueError("external structure identity is invalid")
            seen_contracts.add(identity.con_id)
            contracts.append(
                ParentContractIdentity(
                    con_id=identity.con_id,
                    local_symbol=identity.local_symbol,
                    trading_class=identity.trading_class,
                    multiplier=identity.multiplier,
                    exchange=identity.exchange.upper(),
                    expiry=identity.expiry,
                    strike=identity.strike,
                    right=identity.right.value,
                )
            )
    ordered_contracts = tuple(sorted(contracts))
    scenario_contract_hash = canonical_hash(
        {
            "schema": "options_copilot.external_top10_scenario_contract.v1",
            "trading_date": trading_date,
            "current_policy_version": INITIAL_POLICY_VERSION,
            "current_policy_hash": INITIAL_POLICY_HASH,
            "scenario_sets": tuple(item.as_dict() for item in scenario_sets),
        }
    )
    payload = {
        "schema": "options_copilot.external_top10_parent_binding.v1",
        "trading_date": trading_date,
        "structures": [candidate.as_dict() for candidate in candidates],
        "contracts": [item.as_dict() for item in ordered_contracts],
        "scenario_contract_hash": scenario_contract_hash,
        "scenario_sets": [item.as_dict() for item in scenario_sets],
        "current_policy_version": INITIAL_POLICY_VERSION,
        "current_policy_hash": INITIAL_POLICY_HASH,
    }
    return ParentStructureBinding(
        trading_date=trading_date,
        structure_hash=canonical_hash(payload),
        preselection_ids=preselection_ids,
        strategy_hashes=strategy_hashes,
        contracts=ordered_contracts,
        scenario_contract_hash=scenario_contract_hash,
        scenario_hashes=tuple(item.scenario_hash for item in scenario_sets),
        current_policy_version=INITIAL_POLICY_VERSION,
        current_policy_hash=INITIAL_POLICY_HASH,
    )


def external_contract_identities(
    rows: Sequence[object],
) -> tuple[ParentContractIdentity, ...]:
    values: list[ParentContractIdentity] = []
    seen: set[int] = set()
    for row in rows:
        if not isinstance(row, Mapping):
            raise ValueError("external contract row must be a mapping")
        identity = row.get("identity")
        if not isinstance(identity, Mapping) or set(identity) != _IDENTITY_FIELDS:
            raise ValueError("external identity fields are incomplete")
        normalized = _external_identity(identity)
        if normalized.con_id in seen:
            raise ValueError("duplicate external conId")
        seen.add(normalized.con_id)
        values.append(normalized)
    return tuple(sorted(values))


_external_contracts = external_contract_identities


def _external_identity(value: Mapping[object, object]) -> ParentContractIdentity:
    con_id = value["conId"]
    multiplier = value["multiplier"]
    if (
        isinstance(con_id, bool)
        or not isinstance(con_id, int)
        or con_id <= 0
        or isinstance(multiplier, bool)
        or not isinstance(multiplier, int)
        or multiplier != 100
    ):
        raise ValueError("external integer identity is invalid")
    local_symbol = _text(value["localSymbol"])
    trading_class = _text(value["tradingClass"])
    exchange = _text(value["exchange"]).upper()
    raw_expiry = value["expiry"]
    if isinstance(raw_expiry, datetime):
        raise ValueError("expiry cannot be a datetime")
    expiry = raw_expiry if isinstance(raw_expiry, date) else date.fromisoformat(_text(raw_expiry))
    try:
        strike = Decimal(str(value["strike"]))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError("strike is invalid") from exc
    if not strike.is_finite() or strike <= 0:
        raise ValueError("strike is invalid")
    raw_right = _text(value["right"]).upper()
    if raw_right in {"C", "CALL"}:
        right = OptionRight.CALL.value
    elif raw_right in {"P", "PUT"}:
        right = OptionRight.PUT.value
    else:
        raise ValueError("right is invalid")
    return ParentContractIdentity(
        con_id=con_id,
        local_symbol=local_symbol,
        trading_class=trading_class,
        multiplier=multiplier,
        exchange=exchange,
        expiry=expiry,
        strike=strike,
        right=right,
    )


def _text(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError("identity text cannot be blank")
    return text


def _ready(
    *,
    slot: str,
    scheduled_for: datetime,
    external_batch_id: str,
    external_batch_hash: str,
    parent: ParentStructureBinding,
) -> ExternalTickResult:
    return ExternalTickResult(
        status=ExternalTickStatus.READY,
        slot=slot,
        reason_codes=(),
        scheduled_for=scheduled_for,
        external_batch_id=external_batch_id,
        external_batch_hash=external_batch_hash,
        parent_structure_hash=parent.structure_hash,
        scenario_contract_hash=parent.scenario_contract_hash,
        risk_policy_version=parent.current_policy_version,
        risk_policy_hash=parent.current_policy_hash,
        structure_count=len(parent.preselection_ids),
        contract_count=len(parent.contracts),
    )


def _blocked(
    reason: str,
    *,
    slot: str | None = None,
    scheduled_for: datetime | None = None,
    external_batch_id: str | None = None,
    external_batch_hash: str | None = None,
    parent: ParentStructureBinding | None = None,
) -> ExternalTickResult:
    return ExternalTickResult(
        status=ExternalTickStatus.NO_TRADE,
        slot=slot,
        reason_codes=(reason,),
        scheduled_for=scheduled_for,
        external_batch_id=external_batch_id,
        external_batch_hash=external_batch_hash,
        parent_structure_hash=(None if parent is None else parent.structure_hash),
        scenario_contract_hash=(
            None if parent is None else parent.scenario_contract_hash
        ),
        risk_policy_version=(
            None if parent is None else parent.current_policy_version
        ),
        risk_policy_hash=(None if parent is None else parent.current_policy_hash),
        structure_count=(0 if parent is None else len(parent.preselection_ids)),
        contract_count=(0 if parent is None else len(parent.contracts)),
    )


__all__ = [
    "ExternalTickResult",
    "ExternalTickStatus",
    "ExternalTop10TickRunner",
    "ParentContractIdentity",
    "ParentStructureBinding",
    "external_contract_identities",
    "validate_external_readonly_batch",
]
