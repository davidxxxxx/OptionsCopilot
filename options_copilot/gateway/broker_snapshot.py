"""Atomic, immutable read-only broker snapshot construction.

Mutable account/order/secdef state is read before and after one coherent quote
batch.  State and contract identity must be equal; live quote prices are never
compared for equality and instead satisfy explicit batch, age, and skew gates.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, fields, is_dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from enum import Enum
from types import MappingProxyType
from typing import Callable, Protocol

from options_copilot.storage.canonical import (
    canonical_hash,
    canonical_json,
    freeze_json,
    utc_datetime,
)

from .ibkr_readonly import (
    BatchedOptionQuote,
    MarketDataPacingError,
    OptionContractRef,
    OptionQuoteBatch,
    OptionSecDefSnapshot,
    QuoteBatchStatus,
)


MAX_QUOTE_AGE_SECONDS = Decimal("5")
MAX_LEG_SKEW_SECONDS = Decimal("2")
_STATE_COMPONENTS = (
    "account",
    "positions",
    "working_orders",
    "unsubmitted_instructions",
)


class BrokerSnapshotStatus(str, Enum):
    COMPLETE = "COMPLETE"
    STATE_INCOMPLETE = "STATE_INCOMPLETE"
    MUTATED_DURING_BUILD = "MUTATED_DURING_BUILD"
    QUOTE_INCOHERENT = "QUOTE_INCOHERENT"


class BrokerSnapshotSource(Protocol):
    def account_snapshot(self) -> object:
        ...

    def positions(self) -> object:
        ...

    def working_orders(self) -> object:
        ...

    def unsubmitted_instructions(self) -> object:
        ...

    def option_contract_definitions(
        self, contracts: Sequence[OptionContractRef]
    ) -> Sequence[OptionSecDefSnapshot]:
        ...

    def option_quote_batch(
        self, contracts: Sequence[OptionContractRef]
    ) -> OptionQuoteBatch:
        ...


@dataclass(frozen=True, slots=True)
class StateComponentEvidence:
    name: str
    known: bool
    count: int | None
    pre_hash: str | None
    post_hash: str | None
    stable: bool
    state: object | None

    def as_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "known": self.known,
            "count": self.count,
            "pre_hash": self.pre_hash,
            "post_hash": self.post_hash,
            "stable": self.stable,
            "state": self.state,
        }


@dataclass(frozen=True, slots=True)
class SecDefEvidence:
    contract_id: int
    pre_identity: Mapping[str, object] | None
    post_identity: Mapping[str, object] | None
    pre_hash: str | None
    post_hash: str | None
    stable: bool
    standard_contract: bool
    adjusted: bool
    pre_source: str | None
    post_source: str | None

    def __post_init__(self) -> None:
        if self.pre_identity is not None:
            object.__setattr__(
                self,
                "pre_identity",
                MappingProxyType(dict(self.pre_identity)),
            )
        if self.post_identity is not None:
            object.__setattr__(
                self,
                "post_identity",
                MappingProxyType(dict(self.post_identity)),
            )

    def as_dict(self) -> dict[str, object]:
        return {
            "contract_id": self.contract_id,
            "pre_identity": self.pre_identity,
            "post_identity": self.post_identity,
            "pre_hash": self.pre_hash,
            "post_hash": self.post_hash,
            "stable": self.stable,
            "standard_contract": self.standard_contract,
            "adjusted": self.adjusted,
            "pre_source": self.pre_source,
            "post_source": self.post_source,
        }


@dataclass(frozen=True, slots=True)
class AtomicBrokerSnapshot:
    built_at: datetime
    status: BrokerSnapshotStatus
    reason_codes: tuple[str, ...]
    state_evidence: Mapping[str, StateComponentEvidence]
    secdef_evidence: tuple[SecDefEvidence, ...]
    quote_batch_id: str | None
    quote_batch_status: QuoteBatchStatus | None
    quote_batch_source: str | None
    quote_batch_requested_at: datetime | None
    quote_batch_completed_at: datetime | None
    quotes: tuple[BatchedOptionQuote, ...]
    oldest_quote_age_seconds: Decimal | None
    maximum_leg_skew_seconds: Decimal | None
    snapshot_hash: str
    quote_batch_observed_at: datetime | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "built_at",
            utc_datetime(self.built_at, field="built_at"),
        )
        object.__setattr__(self, "reason_codes", tuple(self.reason_codes))
        object.__setattr__(
            self,
            "state_evidence",
            MappingProxyType(dict(self.state_evidence)),
        )
        object.__setattr__(self, "secdef_evidence", tuple(self.secdef_evidence))
        object.__setattr__(self, "quotes", tuple(self.quotes))

    @property
    def complete(self) -> bool:
        return self.status is BrokerSnapshotStatus.COMPLETE

    def hash_payload(self) -> dict[str, object]:
        return {
            "built_at": self.built_at,
            "status": self.status.value,
            "reason_codes": self.reason_codes,
            "state_evidence": {
                name: evidence.as_dict()
                for name, evidence in sorted(self.state_evidence.items())
            },
            "secdef_evidence": [
                item.as_dict()
                for item in sorted(
                    self.secdef_evidence,
                    key=lambda value: value.contract_id,
                )
            ],
            "quote_batch_id": self.quote_batch_id,
            "quote_batch_status": (
                None
                if self.quote_batch_status is None
                else self.quote_batch_status.value
            ),
            "quote_batch_source": self.quote_batch_source,
            "quote_batch_requested_at": self.quote_batch_requested_at,
            "quote_batch_completed_at": self.quote_batch_completed_at,
            "quote_batch_observed_at": self.quote_batch_observed_at,
            "quotes": [
                _quote_document(item)
                for item in sorted(
                    self.quotes,
                    key=lambda value: (value.contract_id, value.request_id),
                )
            ],
            "oldest_quote_age_seconds": self.oldest_quote_age_seconds,
            "maximum_leg_skew_seconds": self.maximum_leg_skew_seconds,
        }

    def verify_hash(self) -> bool:
        return canonical_hash(self.hash_payload()) == self.snapshot_hash


class BrokerSnapshotBuilder:
    """Build one component-atomic broker snapshot from a read-only source."""

    def __init__(
        self,
        source: BrokerSnapshotSource,
        *,
        clock: Callable[[], datetime] | None = None,
        maximum_quote_age_seconds: Decimal = MAX_QUOTE_AGE_SECONDS,
        maximum_leg_skew_seconds: Decimal = MAX_LEG_SKEW_SECONDS,
    ) -> None:
        self.source = source
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self.maximum_quote_age_seconds = _nonnegative_decimal(
            maximum_quote_age_seconds,
            "maximum_quote_age_seconds",
        )
        self.maximum_leg_skew_seconds = _nonnegative_decimal(
            maximum_leg_skew_seconds,
            "maximum_leg_skew_seconds",
        )

    def build(
        self,
        contracts: Sequence[OptionContractRef],
    ) -> AtomicBrokerSnapshot:
        try:
            requested = tuple(contracts)
        except TypeError:
            return self._failure(
                BrokerSnapshotStatus.MUTATED_DURING_BUILD,
                ("INVALID_REQUESTED_CONTRACTS",),
            )
        if not requested or not all(
            isinstance(item, OptionContractRef) for item in requested
        ):
            return self._failure(
                BrokerSnapshotStatus.MUTATED_DURING_BUILD,
                ("INVALID_REQUESTED_CONTRACTS",),
            )
        requested_ids = tuple(item.contract_id for item in requested)
        reasons: set[str] = set()
        if len(set(requested_ids)) != len(requested_ids):
            reasons.add("DUPLICATE_REQUESTED_CONID")
        for item in requested:
            if (
                item.contract_id <= 0
                or item.multiplier != 100
                or item.currency.upper() != "USD"
                or item.right not in {"C", "P"}
            ):
                reasons.add("NONSTANDARD_REQUESTED_CONTRACT")
        if reasons:
            return self._failure(
                BrokerSnapshotStatus.MUTATED_DURING_BUILD,
                reasons,
            )

        pre_state, pre_errors, pre_upstream = self._read_state()
        pre_secdefs, pre_secdef_error = self._read_secdefs(requested)
        batch, quote_exception = self._read_quote_batch(requested)
        post_secdefs, post_secdef_error = self._read_secdefs(requested)
        post_state, post_errors, post_state_upstream = self._read_state()
        built_at = utc_datetime(self._clock(), field="clock result")

        state_evidence: dict[str, StateComponentEvidence] = {}
        state_incomplete = set(pre_errors).union(post_errors)
        state_mutated: set[str] = set()
        for name in _STATE_COMPONENTS:
            evidence, error = _state_evidence(
                name,
                pre_state.get(name),
                post_state.get(name),
            )
            state_evidence[name] = evidence
            if error == "INCOMPLETE":
                state_incomplete.add(f"{name.upper()}_UNKNOWN")
            elif error == "MUTATED":
                state_mutated.add(f"{name.upper()}_MUTATED")

        secdef_evidence, secdef_reasons = _secdef_evidence(
            requested,
            pre_secdefs,
            post_secdefs,
        )
        if pre_secdef_error is not None:
            secdef_reasons.add(pre_secdef_error)
        if post_secdef_error is not None:
            secdef_reasons.add(post_secdef_error)

        quote_reasons, quotes, oldest_age, maximum_skew = _quote_evidence(
            batch,
            requested_ids=requested_ids,
            built_at=built_at,
            maximum_age=self.maximum_quote_age_seconds,
            maximum_skew=self.maximum_leg_skew_seconds,
        )
        if quote_exception is not None:
            quote_reasons.add(quote_exception)

        post_upstream = self._upstream_identity()
        if pre_upstream is not None or post_upstream is not None:
            if (pre_upstream is None or post_upstream is None
                    or pre_upstream[0] is None or post_upstream[0] is None
                    or pre_upstream[0] != post_upstream[0]
                    or post_state_upstream != post_upstream):
                state_incomplete.add("BROKER_UPSTREAM_AUTHORITY_CHANGED")

        if state_incomplete:
            status = BrokerSnapshotStatus.STATE_INCOMPLETE
            reasons.update(state_incomplete)
        elif state_mutated or secdef_reasons:
            status = BrokerSnapshotStatus.MUTATED_DURING_BUILD
            reasons.update(state_mutated)
            reasons.update(secdef_reasons)
        elif quote_reasons:
            status = BrokerSnapshotStatus.QUOTE_INCOHERENT
            reasons.update(quote_reasons)
        else:
            status = BrokerSnapshotStatus.COMPLETE

        return _snapshot(
            built_at=built_at,
            status=status,
            reasons=reasons,
            state_evidence=state_evidence,
            secdef_evidence=secdef_evidence,
            batch=batch,
            quotes=quotes,
            oldest_age=oldest_age,
            maximum_skew=maximum_skew,
        )

    def _read_state(
        self,
    ) -> tuple[dict[str, object], set[str], tuple[int | None, str | None] | None]:
        values: dict[str, object] = {}
        errors: set[str] = set()
        readers = {
            "account": self.source.account_snapshot,
            "positions": self.source.positions,
            "working_orders": self.source.working_orders,
            "unsubmitted_instructions": self.source.unsubmitted_instructions,
        }
        batch_identity = None
        for name, reader in readers.items():
            try:
                values[name] = reader()
            except Exception:
                values[name] = None
                errors.add(f"{name.upper()}_READ_FAILED")
            upstream = self._upstream_identity()
            if name == "account":
                batch_identity = upstream
            if upstream is not None or batch_identity is not None:
                if upstream is None or upstream[0] is None or upstream != batch_identity:
                    errors.add("BROKER_CONTROL_BATCH_CHANGED_OR_UNVERIFIED")
                if name in {"account", "positions"} and upstream is not None:
                    try:
                        components = (values[name],) if name == "account" else values[name]
                        for component in components:
                            observed = (component.get("asof") if isinstance(component, Mapping)
                                        else getattr(component, "asof", None))
                            if isinstance(observed, str):
                                observed = datetime.fromisoformat(observed)
                            if utc_datetime(observed, field="control component time").isoformat() != upstream[1]:
                                raise ValueError("control component has a different batch")
                    except (TypeError, ValueError):
                        errors.add("BROKER_CONTROL_BATCH_TIME_MISMATCH")
        return values, errors, batch_identity

    def _upstream_identity(self) -> tuple[int | None, str | None] | None:
        """A cache-only fence for real gateway sources, not a connection probe."""

        try:
            reader = getattr(self.source, "upstream_health", None)
            if reader is None:
                return None
            raw = reader()
            if not isinstance(raw, Mapping) or raw.get("status") != "READY":
                return (None, None)
            generation, verified = raw.get("generation"), raw.get("verified_at")
            if type(generation) is not int or generation < 0 or not isinstance(verified, str):
                return (None, None)
            verified_at = utc_datetime(datetime.fromisoformat(verified), field="control batch time")
            age = (utc_datetime(self._clock(), field="clock result") - verified_at).total_seconds()
            if not 0 <= age <= 5:
                return (None, None)
            return generation, verified_at.isoformat()
        except Exception:
            return (None, None)

    def _read_secdefs(
        self,
        contracts: tuple[OptionContractRef, ...],
    ) -> tuple[tuple[OptionSecDefSnapshot, ...] | None, str | None]:
        try:
            value = self.source.option_contract_definitions(contracts)
            return tuple(value), None
        except MarketDataPacingError as exc:
            return None, f"SECDEF_{exc.reason_code}"
        except Exception:
            return None, "SECDEF_READ_FAILED"

    def _read_quote_batch(
        self,
        contracts: tuple[OptionContractRef, ...],
    ) -> tuple[OptionQuoteBatch | None, str | None]:
        try:
            return self.source.option_quote_batch(contracts), None
        except MarketDataPacingError as exc:
            return None, f"QUOTE_BATCH_{exc.reason_code}"
        except Exception:
            return None, "QUOTE_BATCH_READ_FAILED"

    def _failure(
        self,
        status: BrokerSnapshotStatus,
        reasons: object,
    ) -> AtomicBrokerSnapshot:
        return _snapshot(
            built_at=utc_datetime(self._clock(), field="clock result"),
            status=status,
            reasons=reasons,
            state_evidence={},
            secdef_evidence=(),
            batch=None,
            quotes=(),
            oldest_age=None,
            maximum_skew=None,
        )


def _state_evidence(
    name: str,
    pre_value: object,
    post_value: object,
) -> tuple[StateComponentEvidence, str | None]:
    try:
        pre = _state_document(name, pre_value)
        post = _state_document(name, post_value)
    except (TypeError, ValueError):
        return (
            StateComponentEvidence(
                name=name,
                known=False,
                count=None,
                pre_hash=None,
                post_hash=None,
                stable=False,
                state=None,
            ),
            "INCOMPLETE",
        )
    if pre is None or post is None:
        return (
            StateComponentEvidence(
                name=name,
                known=False,
                count=None,
                pre_hash=None if pre is None else canonical_hash(pre),
                post_hash=None if post is None else canonical_hash(post),
                stable=False,
                state=post,
            ),
            "INCOMPLETE",
        )
    if name == "account":
        try:
            pre_identity = _account_stability_document(pre)
            post_identity = _account_stability_document(post)
        except (TypeError, ValueError):
            return (
                StateComponentEvidence(
                    name=name,
                    known=False,
                    count=None,
                    pre_hash=None,
                    post_hash=None,
                    stable=False,
                    state=post,
                ),
                "INCOMPLETE",
            )
        stable = canonical_hash(pre_identity) == canonical_hash(post_identity)
        pre_hash = canonical_hash(pre)
        post_hash = canonical_hash(post)
        # NLV, margin, and buying power are mark-to-market observations.  Once
        # stable account identity/connection evidence is proven, bind the
        # complete post-read account sample as the atomic point-in-time state.
        # Keep both observation hashes truthful even when those volatile values
        # differ; ``stable`` records the narrower identity/connection verdict.
    elif name == "positions":
        try:
            pre_identity = _position_stability_document(pre)
            post_identity = _position_stability_document(post)
        except (TypeError, ValueError):
            return (
                StateComponentEvidence(
                    name=name,
                    known=False,
                    count=None,
                    pre_hash=None,
                    post_hash=None,
                    stable=False,
                    state=post,
                ),
                "INCOMPLETE",
            )
        stable = canonical_hash(pre_identity) == canonical_hash(post_identity)
        pre_hash = canonical_hash(pre)
        post_hash = canonical_hash(post)
        # IBKR updates marks and P/L while SECDEF and the coherent quote batch
        # are read.  Those observations must remain visible in the bound post
        # sample without masquerading as an account position change.  Quantity,
        # average cost, contract identity, and every other non-volatile field
        # remain part of the stability verdict and therefore fail closed.
    else:
        pre_hash = canonical_hash(pre)
        post_hash = canonical_hash(post)
        stable = pre_hash == post_hash
    if name == "account":
        count = 1
    else:
        assert isinstance(post, tuple)
        count = len(post)
    return (
        StateComponentEvidence(
            name=name,
            known=True,
            count=count,
            pre_hash=pre_hash,
            post_hash=post_hash,
            stable=stable,
            state=post,
        ),
        None if stable else "MUTATED",
    )


def _account_stability_document(value: object) -> object:
    if not isinstance(value, Mapping):
        raise TypeError("account state must be an object")
    identity: dict[str, object] = {}
    for canonical_name, aliases in (
        (
            "account_id",
            ("account_id", "accountId", "account", "account_number"),
        ),
        ("currency", ("currency",)),
        ("connected", ("connected",)),
    ):
        present = tuple(value[name] for name in aliases if name in value)
        if not present:
            continue
        if any(item != present[0] for item in present[1:]):
            raise ValueError(f"conflicting account {canonical_name}")
        identity[canonical_name] = present[0]
    if not identity:
        raise ValueError("stable account identity is unavailable")
    return freeze_json(identity)


def _position_stability_document(value: object) -> object:
    if isinstance(value, (str, bytes, bytearray, memoryview)) or not isinstance(
        value,
        Sequence,
    ):
        raise TypeError("positions state must be an array")
    volatile = {
        "asof",
        "marketprice",
        "marketvalue",
        "unrealizedpnl",
        "realizedpnl",
    }
    stable_rows: list[dict[str, object]] = []
    for item in value:
        if not isinstance(item, Mapping):
            raise TypeError("position state member must be an object")
        row = {
            str(key): field_value
            for key, field_value in item.items()
            if _normalized_state_field(key) not in volatile
        }
        if not row:
            raise ValueError("stable position identity is unavailable")
        stable_rows.append(row)
    stable_rows.sort(key=canonical_json)
    return freeze_json(stable_rows)


def _normalized_state_field(value: object) -> str:
    return "".join(character for character in str(value).lower() if character.isalnum())


def _state_document(name: str, value: object) -> object | None:
    if value is None:
        return None
    if name == "account":
        plain = _plain(value)
        if not isinstance(plain, Mapping):
            raise TypeError("account state must be an object")
        return freeze_json(plain)
    if isinstance(value, (str, bytes, bytearray, memoryview)) or not isinstance(
        value, Sequence
    ):
        raise TypeError(f"{name} state must be an array")
    rows = [_plain(item) for item in value]
    rows.sort(key=canonical_json)
    frozen = freeze_json(rows)
    assert isinstance(frozen, tuple)
    return frozen


def _plain(value: object) -> object:
    if is_dataclass(value) and not isinstance(value, type):
        return {
            field.name: _plain(getattr(value, field.name))
            for field in fields(value)
            if field.name != "asof"
        }
    if isinstance(value, Mapping):
        return {
            str(key): _plain(item) for key, item in value.items() if str(key) != "asof"
        }
    if isinstance(value, Sequence) and not isinstance(
        value, (str, bytes, bytearray, memoryview)
    ):
        return [_plain(item) for item in value]
    return value


def _secdef_evidence(
    requested: tuple[OptionContractRef, ...],
    pre_rows: tuple[OptionSecDefSnapshot, ...] | None,
    post_rows: tuple[OptionSecDefSnapshot, ...] | None,
) -> tuple[tuple[SecDefEvidence, ...], set[str]]:
    reasons: set[str] = set()
    requested_ids = {item.contract_id for item in requested}
    requested_identity = {
        item.contract_id: _requested_identity(item) for item in requested
    }
    pre, pre_duplicates = _secdef_map(pre_rows)
    post, post_duplicates = _secdef_map(post_rows)
    if pre_duplicates or post_duplicates:
        reasons.add("DUPLICATE_SECDEF_CONID")
    if set(pre) != requested_ids or set(post) != requested_ids:
        reasons.add("SECDEF_INCOMPLETE")
    if set(pre) != set(post):
        reasons.add("SECDEF_MUTATED")

    evidence: list[SecDefEvidence] = []
    for contract_id in sorted(requested_ids.union(pre).union(post)):
        before = pre.get(contract_id)
        after = post.get(contract_id)
        pre_identity = None if before is None else before.identity_dict()
        post_identity = None if after is None else after.identity_dict()
        pre_hash = None if pre_identity is None else canonical_hash(pre_identity)
        post_hash = None if post_identity is None else canonical_hash(post_identity)
        stable = pre_hash is not None and pre_hash == post_hash
        if not stable:
            reasons.add("SECDEF_MUTATED")
        expected_identity = requested_identity.get(contract_id)
        if (
            expected_identity is None
            or pre_identity != expected_identity
            or post_identity != expected_identity
        ):
            reasons.add("SECDEF_REQUEST_MISMATCH")
        records = tuple(item for item in (before, after) if item is not None)
        standard = bool(
            records
            and all(
                item.standard_contract
                and not item.adjusted
                and item.security_type.upper() == "OPT"
                and item.currency.upper() == "USD"
                and item.multiplier == 100
                for item in records
            )
        )
        adjusted = any(item.adjusted for item in records)
        if not standard or adjusted:
            reasons.add("NONSTANDARD_CONTRACT")
        evidence.append(
            SecDefEvidence(
                contract_id=contract_id,
                pre_identity=pre_identity,
                post_identity=post_identity,
                pre_hash=pre_hash,
                post_hash=post_hash,
                stable=stable,
                standard_contract=standard,
                adjusted=adjusted,
                pre_source=None if before is None else before.source,
                post_source=None if after is None else after.source,
            )
        )
    return tuple(evidence), reasons


def _secdef_map(
    rows: tuple[OptionSecDefSnapshot, ...] | None,
) -> tuple[dict[int, OptionSecDefSnapshot], bool]:
    if rows is None:
        return {}, False
    result: dict[int, OptionSecDefSnapshot] = {}
    duplicate = False
    for item in rows:
        if not isinstance(item, OptionSecDefSnapshot):
            continue
        if item.contract_id in result:
            duplicate = True
        else:
            result[item.contract_id] = item
    return result, duplicate


def _quote_evidence(
    batch: OptionQuoteBatch | None,
    *,
    requested_ids: tuple[int, ...],
    built_at: datetime,
    maximum_age: Decimal,
    maximum_skew: Decimal,
) -> tuple[set[str], tuple[BatchedOptionQuote, ...], Decimal | None, Decimal | None]:
    reasons: set[str] = set()
    if not isinstance(batch, OptionQuoteBatch):
        return {"MISSING_QUOTE_BATCH"}, (), None, None
    if batch.status is not QuoteBatchStatus.COMPLETE:
        reasons.add(f"QUOTE_BATCH_{batch.status.value}")
        reasons.update(str(item) for item in batch.blockers if str(item).strip())
    if not _nonblank(batch.batch_id):
        reasons.add("MISSING_QUOTE_BATCH_ID")
    if not _nonblank(batch.source):
        reasons.add("MISSING_QUOTE_SOURCE")
    try:
        requested_at = utc_datetime(batch.requested_at, field="batch.requested_at")
        completed_at = utc_datetime(batch.completed_at, field="batch.completed_at")
    except (TypeError, ValueError):
        reasons.add("INVALID_QUOTE_BATCH_TIME")
        requested_at = built_at
        completed_at = built_at
    if completed_at < requested_at:
        reasons.add("INVALID_QUOTE_BATCH_TIME")
    batch_observed_at: datetime | None = None
    if batch.observed_at is not None:
        try:
            batch_observed_at = utc_datetime(
                batch.observed_at,
                field="batch.observed_at",
            )
        except (TypeError, ValueError):
            reasons.add("INVALID_QUOTE_BATCH_TIME")
        else:
            if not (requested_at <= batch_observed_at <= completed_at):
                reasons.add("INVALID_QUOTE_BATCH_TIME")

    quotes = tuple(
        sorted(batch.quotes, key=lambda item: (item.contract_id, item.request_id))
    )
    ids = [item.contract_id for item in quotes]
    if len(ids) != len(set(ids)) or set(ids) != set(requested_ids):
        reasons.add("PARTIAL_OR_DUPLICATE_QUOTES")
    request_ids = [item.request_id for item in quotes]
    if any(not _nonblank(item) for item in request_ids) or len(request_ids) != len(
        set(request_ids)
    ):
        reasons.add("INVALID_QUOTE_REQUEST_ID")

    exchange_observations: list[datetime] = []
    ages: list[Decimal] = []
    for quote in quotes:
        try:
            quote_requested = utc_datetime(
                quote.requested_at,
                field="quote.requested_at",
            )
            observed = utc_datetime(quote.observed_at, field="quote.observed_at")
            quote_completed = utc_datetime(
                quote.completed_at,
                field="quote.completed_at",
            )
        except (TypeError, ValueError):
            reasons.add("INVALID_QUOTE_TIME")
            continue
        if (
            quote.batch_id != batch.batch_id
            or not _nonblank(quote.batch_id)
            or quote_requested != requested_at
            or quote_completed != completed_at
            or quote.source != batch.source
            or not (quote_requested <= observed <= quote_completed)
            or (batch_observed_at is not None and observed != batch_observed_at)
        ):
            reasons.add("QUOTE_BATCH_IDENTITY_MISMATCH")
        try:
            exchange_time = utc_datetime(
                quote.exchange_time,
                field="quote.exchange_time",
            )
        except (TypeError, ValueError):
            reasons.add("MISSING_OR_INVALID_QUOTE_EXCHANGE_TIME")
            continue
        if exchange_time > observed:
            reasons.add("QUOTE_EXCHANGE_TIME_AFTER_OBSERVATION")
        age = _seconds(built_at - exchange_time)
        if age < 0 or age > maximum_age:
            reasons.add("STALE_OR_FUTURE_QUOTE")
        exchange_observations.append(exchange_time)
        ages.append(age)
        if (
            quote.bid is None
            or quote.ask is None
            or not _valid_price(quote.bid)
            or not _valid_price(quote.ask)
            or quote.bid > quote.ask
        ):
            reasons.add("MISSING_OR_INVALID_EXECUTABLE_BID_ASK")

    oldest_age = max(ages) if ages else None
    leg_skew = (
        _seconds(max(exchange_observations) - min(exchange_observations))
        if exchange_observations
        else None
    )
    if leg_skew is not None and leg_skew > maximum_skew:
        reasons.add("QUOTE_LEG_SKEW_EXCEEDED")
    return reasons, quotes, oldest_age, leg_skew


def _quote_document(value: BatchedOptionQuote) -> dict[str, object]:
    document = asdict(value)
    return {str(key): item for key, item in document.items()}


def _requested_identity(value: OptionContractRef) -> dict[str, object]:
    return {
        "conId": value.contract_id,
        "localSymbol": value.local_symbol,
        "tradingClass": value.trading_class,
        "multiplier": value.multiplier,
        "exchange": value.exchange,
        "expiry": value.expiration,
        "strike": value.strike,
        "right": value.right,
    }


def _snapshot(
    *,
    built_at: datetime,
    status: BrokerSnapshotStatus,
    reasons: object,
    state_evidence: Mapping[str, StateComponentEvidence],
    secdef_evidence: tuple[SecDefEvidence, ...],
    batch: OptionQuoteBatch | None,
    quotes: tuple[BatchedOptionQuote, ...],
    oldest_age: Decimal | None,
    maximum_skew: Decimal | None,
) -> AtomicBrokerSnapshot:
    reason_tuple = tuple(sorted(str(item) for item in reasons))
    fields = dict(
        built_at=built_at,
        status=status,
        reason_codes=reason_tuple,
        state_evidence=state_evidence,
        secdef_evidence=secdef_evidence,
        quote_batch_id=None if batch is None else batch.batch_id,
        quote_batch_status=None if batch is None else batch.status,
        quote_batch_source=None if batch is None else batch.source,
        quote_batch_requested_at=None if batch is None else batch.requested_at,
        quote_batch_completed_at=None if batch is None else batch.completed_at,
        quote_batch_observed_at=None if batch is None else batch.observed_at,
        quotes=quotes,
        oldest_quote_age_seconds=oldest_age,
        maximum_leg_skew_seconds=maximum_skew,
    )
    provisional = AtomicBrokerSnapshot(**fields, snapshot_hash="0" * 64)
    return AtomicBrokerSnapshot(
        **fields,
        snapshot_hash=canonical_hash(provisional.hash_payload()),
    )


def atomic_account_nlv(snapshot: AtomicBrokerSnapshot) -> Decimal:
    """Return the unique positive NLV bound inside one stable atomic snapshot."""

    if not isinstance(snapshot, AtomicBrokerSnapshot):
        raise TypeError("snapshot must be an AtomicBrokerSnapshot")
    evidence = snapshot.state_evidence.get("account")
    if (
        not isinstance(evidence, StateComponentEvidence)
        or not evidence.known
        or not evidence.stable
        or not isinstance(evidence.state, Mapping)
    ):
        raise ValueError("ATOMIC_ACCOUNT_EVIDENCE_UNAVAILABLE")
    values = tuple(
        evidence.state[key]
        for key in (
            "net_liquidation_usd",
            "net_liquidation",
            "nlv_usd",
            "nlv",
        )
        if key in evidence.state
    )
    if not values:
        raise ValueError("ATOMIC_ACCOUNT_NLV_MISSING")
    parsed: list[Decimal] = []
    for value in values:
        if isinstance(value, bool) or not isinstance(value, (Decimal, int, str)):
            raise ValueError("ATOMIC_ACCOUNT_NLV_INVALID")
        try:
            amount = value if isinstance(value, Decimal) else Decimal(str(value).strip())
        except (InvalidOperation, ValueError) as exc:
            raise ValueError("ATOMIC_ACCOUNT_NLV_INVALID") from exc
        if not amount.is_finite() or amount <= 0:
            raise ValueError("ATOMIC_ACCOUNT_NLV_INVALID")
        parsed.append(amount)
    if any(value != parsed[0] for value in parsed[1:]):
        raise ValueError("ATOMIC_ACCOUNT_NLV_CONFLICT")
    return parsed[0]


def _nonnegative_decimal(value: object, field: str) -> Decimal:
    if not isinstance(value, Decimal):
        raise TypeError(f"{field} must be a Decimal")
    if not value.is_finite() or value < 0:
        raise ValueError(f"{field} must be finite and nonnegative")
    return value


def _valid_price(value: object) -> bool:
    return isinstance(value, Decimal) and value.is_finite() and value > 0


def _nonblank(value: object) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _seconds(value) -> Decimal:
    return Decimal(str(value.total_seconds()))


__all__ = [
    "AtomicBrokerSnapshot",
    "atomic_account_nlv",
    "BrokerSnapshotBuilder",
    "BrokerSnapshotSource",
    "BrokerSnapshotStatus",
    "MAX_LEG_SKEW_SECONDS",
    "MAX_QUOTE_AGE_SECONDS",
    "SecDefEvidence",
    "StateComponentEvidence",
]
