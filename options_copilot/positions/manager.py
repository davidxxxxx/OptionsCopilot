"""Authoritative proof engine for defined-risk close/reduce-only management."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import replace
from datetime import datetime
from decimal import Decimal
import re

from options_copilot.gateway.broker_snapshot import (
    AtomicBrokerSnapshot,
    BrokerSnapshotStatus,
    SecDefEvidence,
    StateComponentEvidence,
)
from options_copilot.gateway.ibkr_readonly import BatchedOptionQuote, QuoteBatchStatus
from options_copilot.governance.contracts import (
    ContractKind,
    ContractValidationError,
    SignedContract,
    verify_contract,
)
from options_copilot.storage.canonical import (
    canonical_hash,
    freeze_json,
    thaw_json,
    utc_datetime,
)

from .models import (
    AuthoritativePosition,
    CapitalUsageProof,
    PositionDelta,
    PositionManagementKind,
    PositionRiskMetrics,
    PositionTransitionProof,
    SecDefBinding,
)


ZERO = Decimal("0")
MAX_QUOTE_AGE_SECONDS = Decimal("5")
MAX_QUOTE_SKEW_SECONDS = Decimal("2")
_DIGEST_RE = re.compile(r"[0-9a-f]{64}")
_INTEGER_RE = re.compile(r"[+-]?(?:0|[1-9][0-9]*)")
_REQUIRED_STATE_COMPONENTS = (
    "account",
    "positions",
    "working_orders",
    "unsubmitted_instructions",
)


class TransitionRejected(ValueError):
    """Fail-closed management rejection with a stable machine-readable code."""

    def __init__(self, code: str, detail: str) -> None:
        self.code = code
        self.detail = detail
        super().__init__(f"{code}: {detail}")


def _reject(code: str, detail: str) -> None:
    raise TransitionRejected(code, detail)


def _is_digest(value: object) -> bool:
    return isinstance(value, str) and _DIGEST_RE.fullmatch(value) is not None


def _require_digest(value: object, *, code: str, field: str) -> str:
    if not _is_digest(value):
        _reject(code, f"{field} must be a lowercase 64-character SHA-256 hash")
    assert isinstance(value, str)
    return value


def _text(value: object, field: str, *, uppercase: bool = False) -> str:
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        _reject("POSITION_STATE_INVALID", f"{field} must be a nonblank trimmed string")
    return value.upper() if uppercase else value


def _signed_integer(value: object, field: str) -> int:
    if isinstance(value, bool) or isinstance(value, float):
        _reject("POSITION_QUANTITY_INVALID", f"{field} must be an exact integer")
    if isinstance(value, int):
        return value
    if isinstance(value, Decimal):
        if value.is_finite() and value == value.to_integral_value():
            return int(value)
        _reject("POSITION_QUANTITY_INVALID", f"{field} must be a finite integer")
    if isinstance(value, str) and _INTEGER_RE.fullmatch(value) is not None:
        return int(value)
    _reject(
        "POSITION_QUANTITY_INVALID",
        f"{field} must be an int, integer Decimal, or strict integer string",
    )


def _positive_contract_id(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        _reject("POSITION_CONID_INVALID", "contract_id must be a positive integer")
    return value


def _aware_utc(value: object, field: str, *, code: str) -> datetime:
    try:
        return utc_datetime(value, field=field)  # type: ignore[arg-type]
    except (TypeError, ValueError) as exc:
        _reject(code, str(exc))


def _nonnegative_decimal(value: object, field: str, *, code: str) -> Decimal:
    if not isinstance(value, Decimal) or not value.is_finite() or value < ZERO:
        _reject(code, f"{field} must be a finite nonnegative Decimal")
    return value


def _state_sequence(value: object, *, name: str) -> tuple[object, ...]:
    if isinstance(value, (str, bytes, bytearray, memoryview)) or not isinstance(
        value, Sequence
    ):
        _reject("STATE_EVIDENCE_INVALID", f"{name} state must be an immutable sequence")
    return tuple(value)


def _mapping_value(row: Mapping[str, object], *names: str) -> object:
    for name in names:
        if name in row:
            return row[name]
    _reject("POSITION_STATE_INVALID", f"position row is missing {names[0]}")


def _gross_contracts(positions: Sequence[AuthoritativePosition]) -> int:
    return sum(abs(item.signed_quantity) for item in positions)


def _net_short_contracts(positions: Sequence[AuthoritativePosition]) -> int:
    return max(0, -sum(item.signed_quantity for item in positions))


def _risk_metrics(value: object, field: str) -> PositionRiskMetrics:
    if isinstance(value, PositionRiskMetrics):
        return value
    if isinstance(value, Mapping):
        try:
            return PositionRiskMetrics(
                max_loss_usd=value["max_loss_usd"],  # type: ignore[arg-type]
                exposure_usd=value["exposure_usd"],  # type: ignore[arg-type]
                capital_usage_usd=value["capital_usage_usd"],  # type: ignore[arg-type]
            )
        except (KeyError, TypeError, ValueError) as exc:
            _reject("RISK_METRICS_INVALID", f"{field} is incomplete or invalid: {exc}")
    _reject("RISK_METRICS_INVALID", f"{field} must be PositionRiskMetrics")


def _normalize_deltas(value: object) -> tuple[PositionDelta, ...]:
    raw: list[object]
    if isinstance(value, Mapping):
        if "contract_id" in value:
            raw = [value]
        else:
            raw = [
                {"contract_id": contract_id, "signed_quantity_delta": quantity}
                for contract_id, quantity in value.items()
            ]
    elif isinstance(value, Sequence) and not isinstance(
        value, (str, bytes, bytearray, memoryview)
    ):
        raw = list(value)
    else:
        _reject("INVALID_DELTA", "deltas must be a mapping or sequence")
    if not raw:
        _reject("INVALID_DELTA", "at least one nonzero execution delta is required")

    normalized: list[PositionDelta] = []
    for item in raw:
        try:
            if isinstance(item, PositionDelta):
                delta = item
            elif isinstance(item, Mapping):
                quantity = item.get(
                    "signed_quantity_delta",
                    item.get("quantity_delta", item.get("delta")),
                )
                delta = PositionDelta(
                    contract_id=item.get(  # type: ignore[arg-type]
                        "contract_id", item.get("conId")
                    ),
                    signed_quantity_delta=quantity,  # type: ignore[arg-type]
                )
            elif isinstance(item, Sequence) and len(item) == 2:
                delta = PositionDelta(item[0], item[1])  # type: ignore[arg-type]
            else:
                _reject("INVALID_DELTA", "each delta must identify conId and signed quantity")
        except TransitionRejected:
            raise
        except (TypeError, ValueError) as exc:
            _reject("INVALID_DELTA", str(exc))
        normalized.append(delta)

    contract_ids = [item.contract_id for item in normalized]
    if len(contract_ids) != len(set(contract_ids)):
        _reject("DUPLICATE_DELTA_CONID", "each conId may appear in deltas only once")
    return tuple(sorted(normalized, key=lambda item: item.contract_id))


class PositionManager:
    """Build reproducible transition proofs from one immutable broker snapshot."""

    def __init__(self) -> None:
        # This projection is observation-only convenience for RuntimeServices.
        # It is never read by normalize_snapshot/prove_transition and therefore
        # cannot become broker or transition authority.
        self._latest_read_model = freeze_json(
            {
                "status": "NO_TRADE",
                "decision": "NO_TRADE",
                "available": True,
                "mode": "POSITION_MANAGEMENT",
                "reason": "NO_SNAPSHOT_DERIVED_MANAGEMENT_PREVIEW",
                "reason_codes": ("NO_SNAPSHOT_DERIVED_MANAGEMENT_PREVIEW",),
                "candidates": (),
                "approval_enabled": False,
                "review_only": True,
                "direct_order_submission": False,
            }
        )

    def publish_read_model(self, value: Mapping[str, object]) -> None:
        """Store a detached preview projection without granting authority."""

        if not isinstance(value, Mapping):
            raise TypeError("position management read model must be a mapping")
        payload = dict(value)
        payload["approval_enabled"] = False
        payload["review_only"] = True
        payload["direct_order_submission"] = False
        payload.setdefault("mode", "POSITION_MANAGEMENT")
        self._latest_read_model = freeze_json(payload)

    def read_model(self) -> Mapping[str, object]:
        detached = thaw_json(self._latest_read_model)
        if not isinstance(detached, Mapping):  # pragma: no cover - construction guard
            raise RuntimeError("position management read model is invalid")
        return detached

    latest = read_model
    management = read_model

    def normalize_snapshot(
        self,
        snapshot: AtomicBrokerSnapshot,
    ) -> tuple[AuthoritativePosition, ...]:
        positions_evidence = self._validate_snapshot_state(snapshot)
        positions = self._normalize_positions(snapshot, positions_evidence)
        self._validate_secdefs(snapshot, positions)
        self._validate_quotes(snapshot, positions)
        return positions

    def prove_transition(
        self,
        snapshot: AtomicBrokerSnapshot,
        *,
        kind: PositionManagementKind | str,
        deltas: object,
        before_risk: PositionRiskMetrics | Mapping[str, object],
        after_risk: PositionRiskMetrics | Mapping[str, object],
        exit_contract_hash: str,
        execution_cost_contract: SignedContract | Mapping[str, object],
    ) -> PositionTransitionProof:
        positions_evidence = self._validate_snapshot_state(snapshot)
        before = self._normalize_positions(snapshot, positions_evidence)
        secdef_bindings = self._validate_secdefs(snapshot, before)
        quote_batch_hash = self._validate_quotes(snapshot, before)
        management_kind = self._management_kind(kind)
        normalized_deltas = _normalize_deltas(deltas)
        before_metrics = _risk_metrics(before_risk, "before_risk")
        after_metrics = _risk_metrics(after_risk, "after_risk")
        exit_hash = _require_digest(
            exit_contract_hash,
            code="EXIT_CONTRACT_HASH_INVALID",
            field="exit_contract_hash",
        )
        cost_contract = self._verified_cost_contract(
            execution_cost_contract,
            as_of=snapshot.built_at,
        )

        after = self._derive_after(
            management_kind,
            before,
            normalized_deltas,
        )
        self._validate_risk_change(
            management_kind,
            before_metrics,
            after_metrics,
        )
        capital_usage = CapitalUsageProof(
            before_capital_usage_usd=before_metrics.capital_usage_usd,
            after_capital_usage_usd=after_metrics.capital_usage_usd,
            capital_change_usd=(
                after_metrics.capital_usage_usd - before_metrics.capital_usage_usd
            ),
            before_gross_contracts=_gross_contracts(before),
            after_gross_contracts=_gross_contracts(after),
            before_net_short_contracts=_net_short_contracts(before),
            after_net_short_contracts=_net_short_contracts(after),
        )
        if not capital_usage.non_increasing:
            _reject("CAPITAL_PROOF_INCREASE", "capital usage proof is not non-increasing")

        before_positions_hash = canonical_hash([item.as_dict() for item in before])
        after_positions_hash = canonical_hash([item.as_dict() for item in after])
        assert positions_evidence.post_hash is not None
        provisional = PositionTransitionProof(
            management_kind=management_kind,
            before_positions=before,
            deltas=normalized_deltas,
            after_positions=after,
            before_risk=before_metrics,
            after_risk=after_metrics,
            capital_usage=capital_usage,
            broker_snapshot_hash=snapshot.snapshot_hash,
            positions_state_hash=positions_evidence.post_hash,
            secdef_bindings=secdef_bindings,
            quote_batch_id=snapshot.quote_batch_id or "",
            quote_batch_hash=quote_batch_hash,
            exit_contract_hash=exit_hash,
            execution_cost_contract_version=cost_contract.version,
            execution_cost_contract_hash=cost_contract.contract_hash,
            verified_at=snapshot.built_at,
            before_positions_hash=before_positions_hash,
            after_positions_hash=after_positions_hash,
            proof_hash="0" * 64,
        )
        proof = replace(provisional, proof_hash=canonical_hash(provisional.hash_payload()))
        if not proof.verify_hash():  # pragma: no cover - defensive construction guard
            _reject("PROOF_HASH_MISMATCH", "constructed proof failed canonical verification")
        return proof

    @staticmethod
    def _management_kind(value: PositionManagementKind | str) -> PositionManagementKind:
        try:
            return PositionManagementKind.parse(value)
        except (TypeError, ValueError) as exc:
            _reject("MANAGEMENT_KIND_FORBIDDEN", str(exc))

    @staticmethod
    def _validate_snapshot_state(
        snapshot: AtomicBrokerSnapshot,
    ) -> StateComponentEvidence:
        if not isinstance(snapshot, AtomicBrokerSnapshot):
            _reject("SNAPSHOT_INVALID", "snapshot must be an AtomicBrokerSnapshot")
        if snapshot.status is not BrokerSnapshotStatus.COMPLETE or snapshot.reason_codes:
            _reject("SNAPSHOT_INCOMPLETE", "broker snapshot is not COMPLETE and reason-free")
        if not _is_digest(snapshot.snapshot_hash) or not snapshot.verify_hash():
            _reject("SNAPSHOT_HASH_MISMATCH", "broker snapshot hash is missing or invalid")

        evidence_by_name: dict[str, StateComponentEvidence] = {}
        for name in _REQUIRED_STATE_COMPONENTS:
            evidence = snapshot.state_evidence.get(name)
            if not isinstance(evidence, StateComponentEvidence) or evidence.name != name:
                _reject("STATE_EVIDENCE_INVALID", f"missing {name} state evidence")
            if (
                not evidence.known
                or not evidence.stable
                or evidence.state is None
                or not _is_digest(evidence.pre_hash)
                or not _is_digest(evidence.post_hash)
                or (
                    name not in {"account", "positions"}
                    and evidence.pre_hash != evidence.post_hash
                )
            ):
                _reject("STATE_EVIDENCE_INVALID", f"{name} state is unknown or unstable")
            try:
                state_hash = canonical_hash(evidence.state)
            except (TypeError, ValueError) as exc:
                _reject("STATE_EVIDENCE_INVALID", f"{name} state cannot be hashed: {exc}")
            if state_hash != evidence.post_hash:
                _reject("STATE_HASH_MISMATCH", f"{name} state does not match its hash")
            if name == "account":
                if not isinstance(evidence.state, Mapping) or evidence.count != 1:
                    _reject("STATE_EVIDENCE_INVALID", "account state/count is invalid")
            else:
                rows = _state_sequence(evidence.state, name=name)
                if evidence.count != len(rows):
                    _reject("STATE_EVIDENCE_INVALID", f"{name} count does not match state")
            evidence_by_name[name] = evidence

        for name, code in (
            ("working_orders", "WORKING_ORDERS_PRESENT"),
            ("unsubmitted_instructions", "UNSUBMITTED_INSTRUCTIONS_PRESENT"),
        ):
            evidence = evidence_by_name[name]
            if evidence.count != 0 or _state_sequence(evidence.state, name=name):
                _reject(code, f"{name} must be known empty before management")
        return evidence_by_name["positions"]

    @staticmethod
    def _normalize_positions(
        snapshot: AtomicBrokerSnapshot,
        evidence: StateComponentEvidence,
    ) -> tuple[AuthoritativePosition, ...]:
        rows = _state_sequence(evidence.state, name="positions")
        normalized: list[AuthoritativePosition] = []
        seen: set[int] = set()
        for raw in rows:
            if not isinstance(raw, Mapping):
                _reject("POSITION_STATE_INVALID", "each position row must be an object")
            contract_id = _positive_contract_id(
                _mapping_value(raw, "contract_id", "conId")
            )
            if contract_id in seen:
                _reject("DUPLICATE_POSITION_CONID", f"duplicate position conId {contract_id}")
            seen.add(contract_id)
            quantity = _signed_integer(
                _mapping_value(raw, "quantity", "position"),
                "quantity",
            )
            symbol = _text(_mapping_value(raw, "symbol"), "symbol", uppercase=True)
            local_symbol = _text(
                _mapping_value(raw, "local_symbol", "localSymbol"),
                "local_symbol",
            )
            security_type = _text(
                _mapping_value(raw, "security_type", "secType"),
                "security_type",
                uppercase=True,
            )
            currency = _text(
                _mapping_value(raw, "currency"),
                "currency",
                uppercase=True,
            )
            exchange = _text(
                _mapping_value(raw, "exchange"),
                "exchange",
                uppercase=True,
            )
            if security_type != "OPT" or currency != "USD":
                _reject(
                    "UNSUPPORTED_POSITION",
                    "position manager accepts only standard USD options",
                )
            if quantity == 0:
                continue
            normalized.append(
                AuthoritativePosition(
                    contract_id=contract_id,
                    symbol=symbol,
                    local_symbol=local_symbol,
                    security_type=security_type,
                    currency=currency,
                    exchange=exchange,
                    signed_quantity=quantity,
                    observed_at=snapshot.built_at,
                )
            )
        if not normalized:
            _reject("NO_OPEN_POSITION", "snapshot contains no nonzero option position")
        symbols = {item.symbol for item in normalized}
        if len(symbols) != 1:
            _reject(
                "MULTIPLE_OPEN_COMBINATIONS",
                "one atomic management proof may bind only one underlying",
            )
        return tuple(sorted(normalized, key=lambda item: item.contract_id))

    @staticmethod
    def _validate_secdefs(
        snapshot: AtomicBrokerSnapshot,
        positions: tuple[AuthoritativePosition, ...],
    ) -> tuple[SecDefBinding, ...]:
        expected = {item.contract_id: item for item in positions}
        values = snapshot.secdef_evidence
        if not isinstance(values, tuple):
            _reject("SECDEF_INCOMPLETE", "secdef evidence must be immutable")
        ids = [item.contract_id for item in values if isinstance(item, SecDefEvidence)]
        if len(ids) != len(values) or len(ids) != len(set(ids)) or set(ids) != set(expected):
            _reject("SECDEF_INCOMPLETE", "secdef conIds must exactly match open positions")

        bindings: list[SecDefBinding] = []
        for evidence in sorted(values, key=lambda item: item.contract_id):
            if (
                not evidence.stable
                or not evidence.standard_contract
                or evidence.adjusted
            ):
                _reject(
                    "NONSTANDARD_SECDEF",
                    f"conId {evidence.contract_id} is adjusted, unstable, or nonstandard",
                )
            if (
                not _is_digest(evidence.pre_hash)
                or not _is_digest(evidence.post_hash)
                or evidence.pre_hash != evidence.post_hash
                or not isinstance(evidence.pre_identity, Mapping)
                or not isinstance(evidence.post_identity, Mapping)
                or evidence.pre_identity != evidence.post_identity
            ):
                _reject("SECDEF_HASH_MISMATCH", "secdef identity/hash is missing or unstable")
            try:
                pre_hash = canonical_hash(evidence.pre_identity)
                post_hash = canonical_hash(evidence.post_identity)
            except (TypeError, ValueError) as exc:
                _reject("SECDEF_HASH_MISMATCH", f"secdef identity cannot be hashed: {exc}")
            if pre_hash != evidence.pre_hash or post_hash != evidence.post_hash:
                _reject("SECDEF_HASH_MISMATCH", "secdef identity does not match its hash")
            if not isinstance(evidence.pre_source, str) or not evidence.pre_source.strip():
                _reject("SECDEF_INCOMPLETE", "secdef pre source is missing")
            if not isinstance(evidence.post_source, str) or not evidence.post_source.strip():
                _reject("SECDEF_INCOMPLETE", "secdef post source is missing")

            identity = evidence.post_identity
            position = expected[evidence.contract_id]
            required = {
                "conId",
                "localSymbol",
                "tradingClass",
                "multiplier",
                "exchange",
                "expiry",
                "strike",
                "right",
            }
            if set(identity) != required:
                _reject("SECDEF_INCOMPLETE", "secdef identity fields are incomplete or unexpected")
            multiplier = identity["multiplier"]
            strike = identity["strike"]
            if (
                identity["conId"] != evidence.contract_id
                or identity["localSymbol"] != position.local_symbol
                or not str(identity["tradingClass"]).strip()
                or isinstance(multiplier, bool)
                or not isinstance(multiplier, int)
                or multiplier != 100
                or str(identity["exchange"]).upper() != position.exchange
                or not isinstance(strike, Decimal)
                or not strike.is_finite()
                or strike <= ZERO
                or identity["right"] not in ("C", "P")
            ):
                _reject("SECDEF_POSITION_MISMATCH", "secdef does not match normalized position")
            assert isinstance(evidence.pre_hash, str)
            assert isinstance(evidence.post_hash, str)
            bindings.append(
                SecDefBinding(
                    contract_id=evidence.contract_id,
                    pre_identity_hash=evidence.pre_hash,
                    post_identity_hash=evidence.post_hash,
                )
            )
        return tuple(bindings)

    @staticmethod
    def _validate_quotes(
        snapshot: AtomicBrokerSnapshot,
        positions: tuple[AuthoritativePosition, ...],
    ) -> str:
        if snapshot.quote_batch_status is not QuoteBatchStatus.COMPLETE:
            _reject("QUOTE_BATCH_INCOMPLETE", "quote batch must be COMPLETE")
        if not isinstance(snapshot.quote_batch_id, str) or not snapshot.quote_batch_id.strip():
            _reject("QUOTE_BATCH_INCOMPLETE", "quote batch id is missing")
        if not isinstance(
            snapshot.quote_batch_source, str
        ) or not snapshot.quote_batch_source.strip():
            _reject("QUOTE_BATCH_INCOMPLETE", "quote batch source is missing")
        requested_at = _aware_utc(
            snapshot.quote_batch_requested_at,
            "quote_batch_requested_at",
            code="QUOTE_TIME_INVALID",
        )
        completed_at = _aware_utc(
            snapshot.quote_batch_completed_at,
            "quote_batch_completed_at",
            code="QUOTE_TIME_INVALID",
        )
        if completed_at < requested_at:
            _reject("QUOTE_TIME_INVALID", "quote batch completed before it was requested")
        reported_age = _nonnegative_decimal(
            snapshot.oldest_quote_age_seconds,
            "oldest_quote_age_seconds",
            code="QUOTE_TIME_INVALID",
        )
        reported_skew = _nonnegative_decimal(
            snapshot.maximum_leg_skew_seconds,
            "maximum_leg_skew_seconds",
            code="QUOTE_TIME_INVALID",
        )
        if reported_age > MAX_QUOTE_AGE_SECONDS:
            _reject("QUOTE_STALE", "oldest quote exceeds the five-second hard limit")
        if reported_skew > MAX_QUOTE_SKEW_SECONDS:
            _reject("QUOTE_INCOHERENT", "quote leg skew exceeds the two-second hard limit")

        expected_ids = {item.contract_id for item in positions}
        quotes = snapshot.quotes
        if not isinstance(quotes, tuple) or not all(
            isinstance(item, BatchedOptionQuote) for item in quotes
        ):
            _reject("QUOTE_BATCH_INCOMPLETE", "quotes must be immutable BatchedOptionQuote values")
        quote_ids = [item.contract_id for item in quotes]
        if len(quote_ids) != len(set(quote_ids)) or set(quote_ids) != expected_ids:
            _reject("QUOTE_BATCH_INCOMPLETE", "quote conIds must exactly match open positions")
        request_ids: set[str] = set()
        observed_times: list[datetime] = []
        ages: list[Decimal] = []
        for quote in quotes:
            if (
                quote.batch_id != snapshot.quote_batch_id
                or quote.source != snapshot.quote_batch_source
                or quote.requested_at != snapshot.quote_batch_requested_at
                or quote.completed_at != snapshot.quote_batch_completed_at
            ):
                _reject("QUOTE_BATCH_MISMATCH", "quote is not bound to the parent batch")
            if (
                not isinstance(quote.request_id, str)
                or not quote.request_id.strip()
                or quote.request_id in request_ids
            ):
                _reject("QUOTE_BATCH_MISMATCH", "quote request ids must be unique and nonblank")
            request_ids.add(quote.request_id)
            observed_at = _aware_utc(
                quote.observed_at,
                "quote.observed_at",
                code="QUOTE_TIME_INVALID",
            )
            if not (requested_at <= observed_at <= completed_at):
                _reject("QUOTE_TIME_INVALID", "quote observation lies outside its batch")
            age = Decimal(str((snapshot.built_at - observed_at).total_seconds()))
            if age < ZERO or age > MAX_QUOTE_AGE_SECONDS:
                _reject("QUOTE_STALE", "quote is stale or future-dated")
            observed_times.append(observed_at)
            ages.append(age)
            if (
                not isinstance(quote.bid, Decimal)
                or not isinstance(quote.ask, Decimal)
                or not quote.bid.is_finite()
                or not quote.ask.is_finite()
                or quote.bid <= ZERO
                or quote.ask <= ZERO
            ):
                _reject("QUOTE_PRICE_INVALID", "each leg requires finite positive bid and ask")
            if quote.bid >= quote.ask:
                _reject("QUOTE_LOCKED_OR_CROSSED", "locked or crossed option quote is prohibited")

        computed_age = max(ages)
        computed_skew = Decimal(
            str((max(observed_times) - min(observed_times)).total_seconds())
        )
        if computed_age != reported_age or computed_skew != reported_skew:
            _reject("QUOTE_TIME_MISMATCH", "reported quote age/skew does not match observations")

        quote_payload = snapshot.hash_payload()["quotes"]
        return canonical_hash(
            {
                "quote_batch_id": snapshot.quote_batch_id,
                "quote_batch_status": snapshot.quote_batch_status.value,
                "quote_batch_source": snapshot.quote_batch_source,
                "quote_batch_requested_at": snapshot.quote_batch_requested_at,
                "quote_batch_completed_at": snapshot.quote_batch_completed_at,
                "quotes": quote_payload,
                "oldest_quote_age_seconds": reported_age,
                "maximum_leg_skew_seconds": reported_skew,
            }
        )

    @staticmethod
    def _verified_cost_contract(
        value: SignedContract | Mapping[str, object],
        *,
        as_of: datetime,
    ) -> SignedContract:
        try:
            contract = verify_contract(
                value,
                expected_kind=ContractKind.EXECUTION_COST,
                as_of=as_of,
            )
        except (ContractValidationError, TypeError, ValueError) as exc:
            _reject("EXECUTION_COST_CONTRACT_INVALID", str(exc))
        if not _is_digest(contract.contract_hash) or not contract.version:
            _reject(
                "EXECUTION_COST_CONTRACT_INVALID",
                "cost contract version/hash is missing",
            )
        return contract

    @staticmethod
    def _derive_after(
        kind: PositionManagementKind,
        before: tuple[AuthoritativePosition, ...],
        deltas: tuple[PositionDelta, ...],
    ) -> tuple[AuthoritativePosition, ...]:
        before_by_id = {item.contract_id: item for item in before}
        delta_by_id = {item.contract_id: item.signed_quantity_delta for item in deltas}
        unknown = sorted(set(delta_by_id).difference(before_by_id))
        if unknown:
            _reject("NEW_CONID", f"delta introduces unknown conId {unknown[0]}")

        if kind is PositionManagementKind.CLOSE_ALL:
            if set(delta_by_id) != set(before_by_id):
                _reject("CLOSE_ALL_INCOMPLETE", "CLOSE_ALL requires every existing conId")
            for contract_id, position in before_by_id.items():
                if delta_by_id[contract_id] != -position.signed_quantity:
                    _reject(
                        "CLOSE_ALL_NOT_EXACT",
                        "CLOSE_ALL requires the exact opposite quantity for every conId",
                    )
            return ()

        after: list[AuthoritativePosition] = []
        strict_reduction = False
        for contract_id, position in before_by_id.items():
            quantity = position.signed_quantity
            projected = quantity + delta_by_id.get(contract_id, 0)
            if projected != 0 and (projected > 0) != (quantity > 0):
                _reject("ZERO_CROSSING", f"conId {contract_id} crosses through zero")
            if abs(projected) > abs(quantity):
                _reject("QUANTITY_INCREASE", f"conId {contract_id} absolute quantity increases")
            if abs(projected) < abs(quantity):
                strict_reduction = True
            if projected != 0:
                after.append(replace(position, signed_quantity=projected))
        if not after:
            _reject("REDUCE_RISK_CANNOT_CLOSE_ALL", "use CLOSE_ALL for a zero after state")
        if not strict_reduction:
            _reject("NO_STRICT_REDUCTION", "REDUCE_RISK must strictly reduce at least one leg")
        if _gross_contracts(after) > _gross_contracts(before):
            _reject("GROSS_EXPOSURE_INCREASE", "gross contract exposure increases")
        if _net_short_contracts(after) > _net_short_contracts(before):
            _reject("NET_SHORT_INCREASE", "net short contracts increase")
        return tuple(sorted(after, key=lambda item: item.contract_id))

    @staticmethod
    def _validate_risk_change(
        kind: PositionManagementKind,
        before: PositionRiskMetrics,
        after: PositionRiskMetrics,
    ) -> None:
        if kind is PositionManagementKind.CLOSE_ALL:
            if (
                after.max_loss_usd != ZERO
                or after.exposure_usd != ZERO
                or after.capital_usage_usd != ZERO
            ):
                _reject("CLOSE_ALL_NONZERO_RISK", "CLOSE_ALL after risk must be exactly zero")
            return
        if after.max_loss_usd > before.max_loss_usd:
            _reject("MAX_LOSS_INCREASE", "maximum loss increases")
        if after.exposure_usd > before.exposure_usd:
            _reject("EXPOSURE_INCREASE", "monetary exposure increases")
        if after.capital_usage_usd > before.capital_usage_usd:
            _reject("CAPITAL_USAGE_INCREASE", "capital usage increases")


def prove_transition(
    snapshot: AtomicBrokerSnapshot,
    *,
    kind: PositionManagementKind | str,
    deltas: object,
    before_risk: PositionRiskMetrics | Mapping[str, object],
    after_risk: PositionRiskMetrics | Mapping[str, object],
    exit_contract_hash: str,
    execution_cost_contract: SignedContract | Mapping[str, object],
) -> PositionTransitionProof:
    """Convenience entry point using the stateless default manager."""

    return PositionManager().prove_transition(
        snapshot,
        kind=kind,
        deltas=deltas,
        before_risk=before_risk,
        after_risk=after_risk,
        exit_contract_hash=exit_contract_hash,
        execution_cost_contract=execution_cost_contract,
    )


__all__ = ["PositionManager", "TransitionRejected", "prove_transition"]
