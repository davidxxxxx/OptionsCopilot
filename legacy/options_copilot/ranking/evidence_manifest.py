"""Strict consumer for frozen candidate-evidence manifests.

The ranking producer owns manifest creation.  This module deliberately only
validates a manifest already embedded in an immutable ranking snapshot and
resolves the exact external evidence rows named by that manifest.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import re

from options_copilot.storage.canonical import canonical_hash


CANDIDATE_EVIDENCE_MANIFEST_SCHEMA = (
    "options_copilot.candidate_evidence_manifest.v1"
)

ALLOWED_PRIMARY_KINDS = frozenset(
    {
        "BROKER_SNAPSHOT",
        "CONTRACT_DEFINITION",
        "EXECUTABLE_QUOTE",
        "PAYOFF_MAX_LOSS",
        "LIQUIDITY",
        "EXECUTION_COST",
        "AFTER_COST_EV",
        "DTE_RISK",
    }
)
ALLOWED_PRIMARY_SOURCES = frozenset(
    {"IBKR_READ_ONLY", "LOCAL_DETERMINISTIC"}
)
_PRIMARY_SOURCE_BY_KIND = {
    "BROKER_SNAPSHOT": "IBKR_READ_ONLY",
    "CONTRACT_DEFINITION": "IBKR_READ_ONLY",
    "EXECUTABLE_QUOTE": "IBKR_READ_ONLY",
    "PAYOFF_MAX_LOSS": "LOCAL_DETERMINISTIC",
    "LIQUIDITY": "LOCAL_DETERMINISTIC",
    "EXECUTION_COST": "LOCAL_DETERMINISTIC",
    "AFTER_COST_EV": "LOCAL_DETERMINISTIC",
    "DTE_RISK": "LOCAL_DETERMINISTIC",
}

_MANIFEST_KEYS = frozenset(
    {
        "schema",
        "candidate_id",
        "symbol",
        "cutoff_at",
        "primary",
        "supporting",
        "contradicting",
        "manifest_hash",
    }
)
_PRIMARY_KEYS = frozenset({"kind", "source", "record", "record_hash"})
_EXTERNAL_REFERENCE_KEYS = frozenset(
    {"evidence_id", "content_hash", "row_hash"}
)
_HASH_RE = re.compile(r"[0-9a-f]{64}\Z")

_PRODUCER_PRIMARY_KINDS = (
    "BROKER_SNAPSHOT",
    "CONTRACT_DEFINITION",
    "EXECUTABLE_QUOTE",
    "PAYOFF_MAX_LOSS",
    "LIQUIDITY",
    "EXECUTION_COST",
    "AFTER_COST_EV",
    "DTE_RISK",
)
_MAX_QUOTE_AGE = timedelta(seconds=5)
_MAX_LEG_QUOTE_SKEW = timedelta(seconds=2)


class CandidateEvidenceManifestError(ValueError):
    """A fail-closed validation result with a stable public reason code."""

    def __init__(self, reason: str, detail: str = "") -> None:
        self.reason = reason
        self.detail = detail
        super().__init__(reason if not detail else f"{reason}: {detail}")


@dataclass(frozen=True, slots=True)
class CandidateEvidenceProjection:
    schema: str
    candidate_id: str
    symbol: str
    cutoff_at: str
    manifest_hash: str
    primary: tuple[dict[str, object], ...]
    supporting: tuple[dict[str, object], ...]
    contradicting: tuple[dict[str, object], ...]


@dataclass(frozen=True, slots=True)
class CandidateEvidenceManifestValidation:
    """Store-independent validation result for one immutable manifest."""

    schema: str
    candidate_id: str
    symbol: str
    cutoff_at: str
    manifest_hash: str
    primary: tuple[dict[str, object], ...]
    supporting_references: tuple[dict[str, str], ...]
    contradicting_references: tuple[dict[str, str], ...]


def build_candidate_evidence_manifest(
    candidate_body: Mapping[str, object],
    *,
    after_cost_expected_value: object,
    cutoff_at: datetime | str,
    ranking_valid_until: datetime | str,
    now: datetime | str,
    supporting: Sequence[Mapping[str, object]] = (),
    contradicting: Sequence[Mapping[str, object]] = (),
) -> dict[str, object]:
    """Build one deterministic, candidate-bound evidence manifest.

    Only fields already frozen into the canonical generated-candidate body,
    plus the deterministic after-cost EV produced in the same pipeline run,
    are allowed to become primary evidence.  External evidence is accepted
    solely as exact ``EvidenceStore`` references; this pure function never has
    a store, provider, query, or ``latest`` dependency.
    """

    if not isinstance(candidate_body, Mapping):
        _fail("CANDIDATE_EVIDENCE_PRIMARY_INVALID", "candidate_body")
    cutoff = _utc_timestamp(
        cutoff_at,
        reason="CANDIDATE_EVIDENCE_CUTOFF_INVALID",
    )
    current = _utc_timestamp(now, reason="CANDIDATE_EVIDENCE_CUTOFF_INVALID")
    valid_until = _utc_timestamp(
        ranking_valid_until,
        reason="CANDIDATE_EVIDENCE_CUTOFF_INVALID",
    )
    if cutoff > current or cutoff > valid_until or valid_until < current:
        _fail("CANDIDATE_EVIDENCE_CUTOFF_INVALID", "cutoff or validity")

    candidate_id = _producer_text(candidate_body, "candidate_id")
    symbol = _producer_text(candidate_body, "symbol")
    structure = _producer_text(candidate_body, "structure")
    broker_snapshot_hash = _producer_hash(candidate_body, "broker_snapshot_hash")
    quote_batch_id = _producer_text(candidate_body, "quote_batch_id")
    secdef_hash = _producer_hash(candidate_body, "secdef_hash")
    cost_version = _producer_text(
        candidate_body, "execution_cost_contract_version"
    )
    cost_hash = _producer_hash(candidate_body, "execution_cost_contract_hash")

    legs, latest_quote = _producer_legs(candidate_body.get("legs"), cutoff)
    con_ids = [int(item["con_id"]) for item in legs]
    debit = _producer_decimal(candidate_body, "debit_usd", minimum=Decimal("0"))
    credit = _producer_decimal(candidate_body, "credit_usd", minimum=Decimal("0"))
    all_in_cost = _producer_decimal(candidate_body, "all_in_cost_usd")
    execution_cost = all_in_cost - debit + credit
    if execution_cost < Decimal("0"):
        _fail("CANDIDATE_EVIDENCE_PRIMARY_INVALID", "execution cost")
    max_loss = _producer_decimal(
        candidate_body, "max_loss_usd", minimum=Decimal("0"), strict=True
    )
    max_profit_value = candidate_body.get("max_profit_usd")
    max_profit = (
        None
        if max_profit_value is None
        else _finite_decimal(max_profit_value, "max_profit_usd")
    )
    if max_profit is not None and max_profit < Decimal("0"):
        _fail("CANDIDATE_EVIDENCE_PRIMARY_INVALID", "max_profit_usd")
    breakevens = _producer_decimal_sequence(
        candidate_body.get("breakevens"), "breakevens"
    )
    liquidity = _producer_decimal(
        candidate_body, "liquidity_score", minimum=Decimal("0"), strict=True
    )
    after_cost_ev = _finite_decimal(
        after_cost_expected_value, "after_cost_expected_value"
    )
    if after_cost_ev <= Decimal("0"):
        _fail("CANDIDATE_EVIDENCE_PRIMARY_INVALID", "after-cost EV")
    dte = candidate_body.get("dte")
    if not isinstance(dte, int) or isinstance(dte, bool) or dte < 7:
        _fail("CANDIDATE_EVIDENCE_PRIMARY_INVALID", "dte")
    dte_exception_hash = candidate_body.get("dte_exception_hash")
    if dte_exception_hash is not None:
        dte_exception_hash = _digest(
            dte_exception_hash,
            reason="CANDIDATE_EVIDENCE_PRIMARY_INVALID",
        )

    primary_records = (
        (
            "BROKER_SNAPSHOT",
            "IBKR_READ_ONLY",
            {
                "symbol": symbol,
                "broker_snapshot_hash": broker_snapshot_hash,
                "quote_snapshot_id": quote_batch_id,
                "complete": True,
            },
        ),
        (
            "CONTRACT_DEFINITION",
            "IBKR_READ_ONLY",
            {
                "symbol": symbol,
                "snapshot_hash": secdef_hash,
                "legs": [{"con_id": con_id} for con_id in con_ids],
                "complete": True,
            },
        ),
        (
            "EXECUTABLE_QUOTE",
            "IBKR_READ_ONLY",
            {
                "symbol": symbol,
                "quote_snapshot_id": quote_batch_id,
                "quote_at": latest_quote.isoformat(timespec="microseconds"),
                "legs": legs,
                "executable": True,
            },
        ),
        (
            "PAYOFF_MAX_LOSS",
            "LOCAL_DETERMINISTIC",
            {
                "symbol": symbol,
                "status": "CALCULATED",
                "debit_usd": _decimal_text(debit),
                "credit_usd": _decimal_text(credit),
                "max_loss_usd": _decimal_text(max_loss),
                "max_profit_usd": (
                    None if max_profit is None else _decimal_text(max_profit)
                ),
                "breakevens": [_decimal_text(value) for value in breakevens],
            },
        ),
        (
            "LIQUIDITY",
            "LOCAL_DETERMINISTIC",
            {
                "symbol": symbol,
                "liquidity_score": _decimal_text(liquidity),
                "eligible": True,
            },
        ),
        (
            "EXECUTION_COST",
            "LOCAL_DETERMINISTIC",
            {
                "symbol": symbol,
                "debit_usd": _decimal_text(debit),
                "credit_usd": _decimal_text(credit),
                "execution_cost_usd": _decimal_text(execution_cost),
                "status": "BOUND",
                "contract_version": cost_version,
                "contract_hash": cost_hash,
            },
        ),
        (
            "AFTER_COST_EV",
            "LOCAL_DETERMINISTIC",
            {
                "symbol": symbol,
                "after_cost_ev_usd": _decimal_text(after_cost_ev),
                "eligible": True,
                "contract_version": cost_version,
                "contract_hash": cost_hash,
            },
        ),
        (
            "DTE_RISK",
            "LOCAL_DETERMINISTIC",
            {
                "symbol": symbol,
                "structure": structure,
                "dte": dte,
                "risk_status": "ALLOWED",
                "dte_exception_hash": dte_exception_hash,
            },
        ),
    )
    if tuple(item[0] for item in primary_records) != _PRODUCER_PRIMARY_KINDS:
        _fail("CANDIDATE_EVIDENCE_PRIMARY_INVALID", "primary kinds")
    primary = [
        {
            "kind": kind,
            "source": source,
            "record": record,
            "record_hash": canonical_hash(record),
        }
        for kind, source, record in primary_records
    ]

    supporting_refs = _producer_external_references(supporting, "supporting")
    contradicting_refs = _producer_external_references(
        contradicting, "contradicting"
    )
    evidence_ids = [
        str(item["evidence_id"])
        for item in (*supporting_refs, *contradicting_refs)
    ]
    if len(evidence_ids) != len(set(evidence_ids)):
        _fail("CANDIDATE_EVIDENCE_MANIFEST_INVALID", "duplicate evidence_id")

    payload: dict[str, object] = {
        "schema": CANDIDATE_EVIDENCE_MANIFEST_SCHEMA,
        "candidate_id": candidate_id,
        "symbol": symbol,
        "cutoff_at": cutoff.isoformat(timespec="microseconds"),
        "primary": primary,
        "supporting": supporting_refs,
        "contradicting": contradicting_refs,
    }
    payload["manifest_hash"] = canonical_hash(payload)
    return payload


def _producer_text(source: Mapping[str, object], field: str) -> str:
    value = source.get(field)
    if not isinstance(value, str) or not value or value != value.strip():
        _fail("CANDIDATE_EVIDENCE_PRIMARY_INVALID", field)
    return value


def _producer_hash(source: Mapping[str, object], field: str) -> str:
    return _digest(
        source.get(field), reason="CANDIDATE_EVIDENCE_PRIMARY_INVALID"
    )


def _finite_decimal(value: object, field: str) -> Decimal:
    if isinstance(value, bool):
        _fail("CANDIDATE_EVIDENCE_PRIMARY_INVALID", field)
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        _fail("CANDIDATE_EVIDENCE_PRIMARY_INVALID", field)
    if not result.is_finite():
        _fail("CANDIDATE_EVIDENCE_PRIMARY_INVALID", field)
    return result


def _decimal_text(value: Decimal) -> str:
    normalized = value.normalize()
    return "0" if not normalized else format(normalized, "f")


def _producer_decimal(
    source: Mapping[str, object],
    field: str,
    *,
    minimum: Decimal | None = None,
    strict: bool = False,
) -> Decimal:
    if field not in source:
        _fail("CANDIDATE_EVIDENCE_PRIMARY_INVALID", field)
    result = _finite_decimal(source[field], field)
    if minimum is not None and (
        result < minimum or (strict and result == minimum)
    ):
        _fail("CANDIDATE_EVIDENCE_PRIMARY_INVALID", field)
    return result


def _producer_decimal_sequence(value: object, field: str) -> tuple[Decimal, ...]:
    values = _sequence(value, reason="CANDIDATE_EVIDENCE_PRIMARY_INVALID")
    return tuple(_finite_decimal(item, field) for item in values)


def _producer_legs(
    value: object,
    cutoff: datetime,
) -> tuple[list[dict[str, object]], datetime]:
    values = _sequence(value, reason="CANDIDATE_EVIDENCE_PRIMARY_INVALID")
    if not values or len(values) > 8:
        _fail("CANDIDATE_EVIDENCE_PRIMARY_INVALID", "legs")
    legs: list[dict[str, object]] = []
    observed_values: list[datetime] = []
    con_ids: set[int] = set()
    for item in values:
        if not isinstance(item, Mapping):
            _fail("CANDIDATE_EVIDENCE_PRIMARY_INVALID", "leg")
        con_id = item.get("con_id")
        ratio = item.get("ratio")
        side = item.get("side")
        if (
            not isinstance(con_id, int)
            or isinstance(con_id, bool)
            or con_id <= 0
            or con_id in con_ids
        ):
            _fail("CANDIDATE_EVIDENCE_PRIMARY_INVALID", "leg con_id")
        if (
            not isinstance(ratio, int)
            or isinstance(ratio, bool)
            or ratio <= 0
        ):
            _fail("CANDIDATE_EVIDENCE_PRIMARY_INVALID", "leg ratio")
        if side not in {"LONG", "SHORT"}:
            _fail("CANDIDATE_EVIDENCE_PRIMARY_INVALID", "leg side")
        bid = _finite_decimal(item.get("bid"), "leg bid")
        ask = _finite_decimal(item.get("ask"), "leg ask")
        if bid <= Decimal("0") or ask <= bid:
            _fail("CANDIDATE_EVIDENCE_PRIMARY_INVALID", "leg bid/ask")
        observed = _utc_timestamp(
            item.get("observed_at"),
            reason="CANDIDATE_EVIDENCE_PRIMARY_INVALID",
        )
        age = cutoff - observed
        if age < timedelta(0):
            _fail("CANDIDATE_EVIDENCE_AFTER_CUTOFF", "leg observed_at")
        if age > _MAX_QUOTE_AGE:
            _fail("CANDIDATE_EVIDENCE_PRIMARY_INVALID", "stale leg quote")
        con_ids.add(con_id)
        observed_values.append(observed)
        legs.append(
            {
                "con_id": con_id,
                "side": side,
                "ratio": ratio,
                "bid": _decimal_text(bid),
                "ask": _decimal_text(ask),
                "observed_at": observed.isoformat(timespec="microseconds"),
            }
        )
    if max(observed_values) - min(observed_values) > _MAX_LEG_QUOTE_SKEW:
        _fail("CANDIDATE_EVIDENCE_PRIMARY_INVALID", "leg quote skew")
    return legs, max(observed_values)


def _producer_external_references(
    values: object,
    role: str,
) -> list[dict[str, str]]:
    references = _sequence(
        values, reason="CANDIDATE_EVIDENCE_MANIFEST_INVALID"
    )
    normalized: list[dict[str, str]] = []
    for value in references:
        raw = _exact_mapping(
            value,
            _EXTERNAL_REFERENCE_KEYS,
            reason="CANDIDATE_EVIDENCE_MANIFEST_INVALID",
        )
        normalized.append(
            {
                "evidence_id": _nonempty_text(
                    raw.get("evidence_id"),
                    reason="CANDIDATE_EVIDENCE_MANIFEST_INVALID",
                ),
                "content_hash": _digest(
                    raw.get("content_hash"),
                    reason="CANDIDATE_EVIDENCE_MANIFEST_INVALID",
                ),
                "row_hash": _digest(
                    raw.get("row_hash"),
                    reason="CANDIDATE_EVIDENCE_MANIFEST_INVALID",
                ),
            }
        )
    try:
        return sorted(
            normalized,
            key=lambda item: (
                item["evidence_id"],
                item["content_hash"],
                item["row_hash"],
            ),
        )
    except (KeyError, TypeError) as exc:
        raise CandidateEvidenceManifestError(
            "CANDIDATE_EVIDENCE_MANIFEST_INVALID", role
        ) from exc


def validate_candidate_evidence_manifest(
    manifest: object,
    *,
    candidate_id: str,
    candidate_symbol: str,
    candidate_body: Mapping[str, object],
    proposal_body: Mapping[str, object],
    ranked_after_cost_expected_value: object | None,
    ranking_broker_snapshot_hash: object,
    ranking_cost_version: object,
    ranking_cost_hash: object,
    ranking_valid_until: object,
    now: datetime | None = None,
) -> CandidateEvidenceManifestValidation:
    """Validate manifest identity and point-in-time semantics without I/O."""

    raw = _exact_mapping(
        manifest,
        _MANIFEST_KEYS,
        reason="CANDIDATE_EVIDENCE_MANIFEST_INVALID",
    )
    if raw.get("schema") != CANDIDATE_EVIDENCE_MANIFEST_SCHEMA:
        _fail("CANDIDATE_EVIDENCE_MANIFEST_INVALID", "schema")

    manifest_candidate_id = _nonempty_text(
        raw.get("candidate_id"),
        reason="CANDIDATE_EVIDENCE_MANIFEST_INVALID",
    )
    if manifest_candidate_id != candidate_id:
        _fail("CANDIDATE_EVIDENCE_CANDIDATE_MISMATCH")

    manifest_symbol = _nonempty_text(
        raw.get("symbol"),
        reason="CANDIDATE_EVIDENCE_MANIFEST_INVALID",
    )
    if manifest_symbol != candidate_symbol:
        _fail("CANDIDATE_EVIDENCE_SYMBOL_MISMATCH")

    supplied_hash = _digest(
        raw.get("manifest_hash"),
        reason="CANDIDATE_EVIDENCE_MANIFEST_INVALID",
    )
    hash_payload = dict(raw)
    hash_payload.pop("manifest_hash")
    try:
        expected_hash = canonical_hash(hash_payload)
    except (TypeError, ValueError) as exc:
        raise CandidateEvidenceManifestError(
            "CANDIDATE_EVIDENCE_MANIFEST_INVALID", "non-canonical payload"
        ) from exc
    if supplied_hash != expected_hash:
        _fail("CANDIDATE_EVIDENCE_MANIFEST_INVALID", "manifest_hash")

    cutoff = _utc_timestamp(
        raw.get("cutoff_at"),
        reason="CANDIDATE_EVIDENCE_CUTOFF_INVALID",
        string_only=True,
    )
    valid_until = _utc_timestamp(
        ranking_valid_until,
        reason="CANDIDATE_EVIDENCE_CUTOFF_INVALID",
        string_only=True,
    )
    current = _utc_timestamp(
        now or datetime.now(timezone.utc),
        reason="CANDIDATE_EVIDENCE_CUTOFF_INVALID",
    )
    if cutoff > current or cutoff > valid_until or current >= valid_until:
        _fail("CANDIDATE_EVIDENCE_CUTOFF_INVALID")

    primary_values = _sequence(
        raw.get("primary"), reason="CANDIDATE_EVIDENCE_MANIFEST_INVALID"
    )
    if not primary_values:
        _fail("CANDIDATE_EVIDENCE_PRIMARY_INVALID", "missing primary evidence")
    primary = tuple(
        _primary_reference(
            item,
            manifest_symbol,
            manifest_candidate_id,
            cutoff,
        )
        for item in primary_values
    )
    if (
        len(primary) != len(ALLOWED_PRIMARY_KINDS)
        or {item["kind"] for item in primary} != ALLOWED_PRIMARY_KINDS
    ):
        _fail("CANDIDATE_EVIDENCE_PRIMARY_INVALID", "incomplete primary kinds")
    _validate_primary_set_semantics(primary)
    _validate_primary_candidate_bindings(
        primary,
        candidate_id=manifest_candidate_id,
        candidate_symbol=manifest_symbol,
        candidate_body=candidate_body,
        proposal_body=proposal_body,
        ranked_after_cost_expected_value=ranked_after_cost_expected_value,
        ranking_broker_snapshot_hash=ranking_broker_snapshot_hash,
        ranking_cost_version=ranking_cost_version,
        ranking_cost_hash=ranking_cost_hash,
        cutoff=cutoff,
        ranking_valid_until=valid_until,
        now=current,
    )
    supporting_refs = tuple(
        _external_reference(item)
        for item in _sequence(
            raw.get("supporting"), reason="CANDIDATE_EVIDENCE_MANIFEST_INVALID"
        )
    )
    contradicting_refs = tuple(
        _external_reference(item)
        for item in _sequence(
            raw.get("contradicting"),
            reason="CANDIDATE_EVIDENCE_MANIFEST_INVALID",
        )
    )
    reference_ids = tuple(
        item["evidence_id"] for item in (*supporting_refs, *contradicting_refs)
    )
    if len(reference_ids) != len(set(reference_ids)):
        _fail("CANDIDATE_EVIDENCE_MANIFEST_INVALID", "duplicate evidence_id")

    return CandidateEvidenceManifestValidation(
        schema=CANDIDATE_EVIDENCE_MANIFEST_SCHEMA,
        candidate_id=manifest_candidate_id,
        symbol=manifest_symbol,
        cutoff_at=str(raw["cutoff_at"]),
        manifest_hash=supplied_hash,
        primary=primary,
        supporting_references=supporting_refs,
        contradicting_references=contradicting_refs,
    )


def resolve_candidate_evidence_manifest(
    manifest: object,
    *,
    candidate_id: str,
    candidate_symbol: str,
    candidate_body: Mapping[str, object],
    proposal_body: Mapping[str, object],
    ranked_after_cost_expected_value: object | None,
    ranking_broker_snapshot_hash: object,
    ranking_cost_version: object,
    ranking_cost_hash: object,
    ranking_valid_until: object,
    evidence_store: object,
    now: datetime | None = None,
) -> CandidateEvidenceProjection:
    """Validate and resolve one immutable candidate evidence manifest.

    External rows are addressed only by the three hashes frozen into the
    manifest.  The store's query/latest surfaces are intentionally never used.
    """

    validated = validate_candidate_evidence_manifest(
        manifest,
        candidate_id=candidate_id,
        candidate_symbol=candidate_symbol,
        candidate_body=candidate_body,
        proposal_body=proposal_body,
        ranked_after_cost_expected_value=ranked_after_cost_expected_value,
        ranking_broker_snapshot_hash=ranking_broker_snapshot_hash,
        ranking_cost_version=ranking_cost_version,
        ranking_cost_hash=ranking_cost_hash,
        ranking_valid_until=ranking_valid_until,
        now=now,
    )
    _verify_store(evidence_store)
    seen_ids: set[str] = set()
    supporting = tuple(
        _resolve_external_reference(
            reference,
            evidence_store=evidence_store,
            symbol=validated.symbol,
            cutoff=_utc_timestamp(
                validated.cutoff_at,
                reason="CANDIDATE_EVIDENCE_CUTOFF_INVALID",
                string_only=True,
            ),
            seen_ids=seen_ids,
        )
        for reference in validated.supporting_references
    )
    contradicting = tuple(
        _resolve_external_reference(
            reference,
            evidence_store=evidence_store,
            symbol=validated.symbol,
            cutoff=_utc_timestamp(
                validated.cutoff_at,
                reason="CANDIDATE_EVIDENCE_CUTOFF_INVALID",
                string_only=True,
            ),
            seen_ids=seen_ids,
        )
        for reference in validated.contradicting_references
    )

    return CandidateEvidenceProjection(
        schema=validated.schema,
        candidate_id=validated.candidate_id,
        symbol=validated.symbol,
        cutoff_at=validated.cutoff_at,
        manifest_hash=validated.manifest_hash,
        primary=validated.primary,
        supporting=supporting,
        contradicting=contradicting,
    )


def _primary_reference(
    value: object,
    symbol: str,
    candidate_id: str,
    cutoff: datetime,
) -> dict[str, object]:
    raw = _exact_mapping(
        value,
        _PRIMARY_KEYS,
        reason="CANDIDATE_EVIDENCE_PRIMARY_INVALID",
    )
    kind = raw.get("kind")
    source = raw.get("source")
    record = raw.get("record")
    if kind not in ALLOWED_PRIMARY_KINDS or source not in ALLOWED_PRIMARY_SOURCES:
        _fail("CANDIDATE_EVIDENCE_PRIMARY_INVALID", "kind or source")
    if _PRIMARY_SOURCE_BY_KIND.get(str(kind)) != source:
        _fail("CANDIDATE_EVIDENCE_PRIMARY_INVALID", "kind source")
    if not isinstance(record, Mapping):
        _fail("CANDIDATE_EVIDENCE_PRIMARY_INVALID", "record")
    record_hash = _digest(
        raw.get("record_hash"),
        reason="CANDIDATE_EVIDENCE_RECORD_HASH_MISMATCH",
    )
    try:
        actual_hash = canonical_hash(record)
    except (TypeError, ValueError) as exc:
        raise CandidateEvidenceManifestError(
            "CANDIDATE_EVIDENCE_PRIMARY_INVALID", "non-canonical record"
        ) from exc
    if record_hash != actual_hash:
        _fail("CANDIDATE_EVIDENCE_RECORD_HASH_MISMATCH")

    record_candidate_id = record.get("candidate_id")
    if record_candidate_id is not None and record_candidate_id != candidate_id:
        _fail("CANDIDATE_EVIDENCE_CANDIDATE_MISMATCH")
    for field in ("symbol", "underlying"):
        if field in record and record[field] != symbol:
            _fail("CANDIDATE_EVIDENCE_SYMBOL_MISMATCH")
    for field in (
        "observed_at",
        "asof",
        "captured_at",
        "quote_at",
        "computed_at",
    ):
        if field in record and _utc_timestamp(
            record[field],
            reason="CANDIDATE_EVIDENCE_PRIMARY_INVALID",
        ) > cutoff:
            _fail("CANDIDATE_EVIDENCE_AFTER_CUTOFF", field)
    _validate_primary_record_semantics(str(kind), record, cutoff)
    return {
        "kind": str(kind),
        "source": str(source),
        "record": dict(record),
        "record_hash": record_hash,
    }


def _validate_primary_record_semantics(
    kind: str,
    record: Mapping[str, object],
    cutoff: datetime,
) -> None:
    reason = "CANDIDATE_EVIDENCE_PRIMARY_INVALID"
    if kind == "BROKER_SNAPSHOT":
        raw = _exact_mapping(
            record,
            {"symbol", "broker_snapshot_hash", "quote_snapshot_id", "complete"},
            reason=reason,
        )
        _digest(raw.get("broker_snapshot_hash"), reason=reason)
        _nonempty_text(raw.get("quote_snapshot_id"), reason=reason)
        if raw.get("complete") is not True:
            _fail(reason, "broker snapshot incomplete")
        return
    if kind == "CONTRACT_DEFINITION":
        raw = _exact_mapping(
            record,
            {"symbol", "snapshot_hash", "legs", "complete"},
            reason=reason,
        )
        _digest(raw.get("snapshot_hash"), reason=reason)
        legs = _sequence(raw.get("legs"), reason=reason)
        con_ids: set[int] = set()
        if not legs or len(legs) > 8:
            _fail(reason, "contract legs")
        for value in legs:
            leg = _exact_mapping(value, {"con_id"}, reason=reason)
            con_id = leg.get("con_id")
            if (
                not isinstance(con_id, int)
                or isinstance(con_id, bool)
                or con_id <= 0
                or con_id in con_ids
            ):
                _fail(reason, "contract con_id")
            con_ids.add(con_id)
        if raw.get("complete") is not True:
            _fail(reason, "contract definition incomplete")
        return
    if kind == "EXECUTABLE_QUOTE":
        raw = _exact_mapping(
            record,
            {"symbol", "quote_snapshot_id", "quote_at", "legs", "executable"},
            reason=reason,
        )
        _nonempty_text(raw.get("quote_snapshot_id"), reason=reason)
        legs = _sequence(raw.get("legs"), reason=reason)
        for value in legs:
            _exact_mapping(
                value,
                {"con_id", "side", "ratio", "bid", "ask", "observed_at"},
                reason=reason,
            )
        _, latest_quote = _producer_legs(legs, cutoff)
        quote_at = _utc_timestamp(raw.get("quote_at"), reason=reason)
        if quote_at != latest_quote or raw.get("executable") is not True:
            _fail(reason, "executable quote")
        return
    if kind == "PAYOFF_MAX_LOSS":
        raw = _exact_mapping(
            record,
            {
                "symbol",
                "status",
                "debit_usd",
                "credit_usd",
                "max_loss_usd",
                "max_profit_usd",
                "breakevens",
            },
            reason=reason,
        )
        if raw.get("status") != "CALCULATED":
            _fail(reason, "payoff status")
        for field in ("debit_usd", "credit_usd"):
            if _finite_decimal(raw.get(field), field) < 0:
                _fail(reason, field)
        if _finite_decimal(raw.get("max_loss_usd"), "max_loss_usd") <= 0:
            _fail(reason, "max_loss_usd")
        maximum_profit = raw.get("max_profit_usd")
        if maximum_profit is not None and _finite_decimal(
            maximum_profit, "max_profit_usd"
        ) < 0:
            _fail(reason, "max_profit_usd")
        _producer_decimal_sequence(raw.get("breakevens"), "breakevens")
        return
    if kind == "LIQUIDITY":
        raw = _exact_mapping(
            record,
            {"symbol", "liquidity_score", "eligible"},
            reason=reason,
        )
        if (
            _finite_decimal(raw.get("liquidity_score"), "liquidity_score") <= 0
            or raw.get("eligible") is not True
        ):
            _fail(reason, "liquidity")
        return
    if kind == "EXECUTION_COST":
        raw = _exact_mapping(
            record,
            {
                "symbol",
                "debit_usd",
                "credit_usd",
                "execution_cost_usd",
                "status",
                "contract_version",
                "contract_hash",
            },
            reason=reason,
        )
        for field in ("debit_usd", "credit_usd", "execution_cost_usd"):
            if _finite_decimal(raw.get(field), field) < 0:
                _fail(reason, field)
        if raw.get("status") != "BOUND":
            _fail(reason, "execution cost status")
        _nonempty_text(raw.get("contract_version"), reason=reason)
        _digest(raw.get("contract_hash"), reason=reason)
        return
    if kind == "AFTER_COST_EV":
        raw = _exact_mapping(
            record,
            {
                "symbol",
                "after_cost_ev_usd",
                "eligible",
                "contract_version",
                "contract_hash",
            },
            reason=reason,
        )
        if (
            _finite_decimal(raw.get("after_cost_ev_usd"), "after_cost_ev_usd")
            <= 0
            or raw.get("eligible") is not True
        ):
            _fail(reason, "after cost EV")
        _nonempty_text(raw.get("contract_version"), reason=reason)
        _digest(raw.get("contract_hash"), reason=reason)
        return
    if kind == "DTE_RISK":
        raw = _exact_mapping(
            record,
            {"symbol", "structure", "dte", "risk_status", "dte_exception_hash"},
            reason=reason,
        )
        _nonempty_text(raw.get("structure"), reason=reason)
        dte = raw.get("dte")
        if (
            not isinstance(dte, int)
            or isinstance(dte, bool)
            or dte < 7
            or raw.get("risk_status") != "ALLOWED"
        ):
            _fail(reason, "dte risk")
        if raw.get("dte_exception_hash") is not None:
            _digest(raw.get("dte_exception_hash"), reason=reason)
        return
    _fail(reason, "unsupported primary kind")


def _validate_primary_set_semantics(
    primary: Sequence[Mapping[str, object]],
) -> None:
    by_kind = {str(item["kind"]): item["record"] for item in primary}
    broker = by_kind["BROKER_SNAPSHOT"]
    contract = by_kind["CONTRACT_DEFINITION"]
    quote = by_kind["EXECUTABLE_QUOTE"]
    payoff = by_kind["PAYOFF_MAX_LOSS"]
    execution = by_kind["EXECUTION_COST"]
    after_cost = by_kind["AFTER_COST_EV"]
    if not all(
        isinstance(value, Mapping)
        for value in (broker, contract, quote, payoff, execution, after_cost)
    ):
        _fail("CANDIDATE_EVIDENCE_PRIMARY_INVALID", "primary record mapping")
    if broker["quote_snapshot_id"] != quote["quote_snapshot_id"]:
        _fail("CANDIDATE_EVIDENCE_PRIMARY_INVALID", "quote snapshot binding")
    contract_con_ids = {item["con_id"] for item in contract["legs"]}
    quote_con_ids = {item["con_id"] for item in quote["legs"]}
    if contract_con_ids != quote_con_ids:
        _fail("CANDIDATE_EVIDENCE_PRIMARY_INVALID", "contract quote binding")
    for field in ("debit_usd", "credit_usd"):
        if payoff[field] != execution[field]:
            _fail("CANDIDATE_EVIDENCE_PRIMARY_INVALID", "payoff cost binding")
    for field in ("contract_version", "contract_hash"):
        if execution[field] != after_cost[field]:
            _fail("CANDIDATE_EVIDENCE_PRIMARY_INVALID", "cost EV binding")


def _validate_primary_candidate_bindings(
    primary: Sequence[Mapping[str, object]],
    *,
    candidate_id: str,
    candidate_symbol: str,
    candidate_body: Mapping[str, object],
    proposal_body: Mapping[str, object],
    ranked_after_cost_expected_value: object | None,
    ranking_broker_snapshot_hash: object,
    ranking_cost_version: object,
    ranking_cost_hash: object,
    cutoff: datetime,
    ranking_valid_until: datetime,
    now: datetime,
) -> None:
    """Bind all primary authority to the frozen candidate and proposal."""

    reason = "CANDIDATE_EVIDENCE_PRIMARY_BINDING_MISMATCH"

    def fail(detail: str) -> None:
        _fail(reason, detail)

    def decimal_value(value: object, field: str) -> Decimal:
        if isinstance(value, Mapping) and set(value) == {"$decimal"}:
            value = value.get("$decimal")
        if isinstance(value, bool):
            fail(field)
        try:
            parsed = Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError):
            fail(field)
        if not parsed.is_finite():
            fail(field)
        return parsed

    def decimal_sequence(value: object, field: str) -> tuple[Decimal, ...]:
        if not isinstance(value, Sequence) or isinstance(
            value, (str, bytes, bytearray, memoryview)
        ):
            fail(field)
        return tuple(decimal_value(item, field) for item in value)

    if not isinstance(candidate_body, Mapping) or not isinstance(
        proposal_body, Mapping
    ):
        fail("candidate or proposal body")
    if (
        candidate_body.get("candidate_id") != candidate_id
        or candidate_body.get("symbol") != candidate_symbol
        or proposal_body.get("candidate_id") != candidate_id
        or proposal_body.get("proposal_id") != candidate_id
        or proposal_body.get("symbol") != candidate_symbol
        or proposal_body.get("underlying") != candidate_symbol
    ):
        fail("candidate or proposal identity")

    candidate_broker_hash = candidate_body.get("broker_snapshot_hash")
    candidate_cost_version = candidate_body.get(
        "execution_cost_contract_version"
    )
    candidate_cost_hash = candidate_body.get("execution_cost_contract_hash")
    if (
        candidate_broker_hash != ranking_broker_snapshot_hash
        or candidate_cost_version != ranking_cost_version
        or candidate_cost_hash != ranking_cost_hash
        or proposal_body.get("broker_snapshot_hash") != candidate_broker_hash
        or proposal_body.get("secdef_hash") != candidate_body.get("secdef_hash")
        or proposal_body.get("quote_snapshot_id")
        != candidate_body.get("quote_batch_id")
        or proposal_body.get("structure") != candidate_body.get("structure")
        or proposal_body.get("dte") != candidate_body.get("dte")
    ):
        fail("ranking, candidate, or proposal authority")

    proposal_cost = proposal_body.get("execution_cost_contract")
    proposal_policy = proposal_body.get("policy")
    if (
        not isinstance(proposal_cost, Mapping)
        or proposal_cost.get("version") != candidate_cost_version
        or proposal_cost.get("hash") != candidate_cost_hash
        or not isinstance(proposal_policy, Mapping)
        or proposal_policy.get("dte_exception_hash")
        != candidate_body.get("dte_exception_hash")
    ):
        fail("cost or DTE authority")

    proposal_ev = decimal_value(
        proposal_body.get("expected_value_usd"),
        "proposal expected value",
    )
    ranked_ev = (
        proposal_ev
        if ranked_after_cost_expected_value is None
        else decimal_value(
            ranked_after_cost_expected_value,
            "ranked after-cost expected value",
        )
    )
    if proposal_ev != ranked_ev:
        fail("ranked proposal expected value")

    try:
        expected = build_candidate_evidence_manifest(
            candidate_body,
            after_cost_expected_value=ranked_ev,
            cutoff_at=cutoff,
            ranking_valid_until=ranking_valid_until,
            now=now,
        )
    except CandidateEvidenceManifestError:
        fail("candidate body")
    expected_primary = expected.get("primary")
    if not isinstance(expected_primary, Sequence) or tuple(primary) != tuple(
        expected_primary
    ):
        fail("primary records do not match candidate body")

    try:
        candidate_legs, _ = _producer_legs(candidate_body.get("legs"), cutoff)
    except CandidateEvidenceManifestError:
        fail("candidate legs")
    proposal_legs = proposal_body.get("legs")
    raw_candidate_legs = candidate_body.get("legs")
    if (
        not isinstance(proposal_legs, Sequence)
        or isinstance(proposal_legs, (str, bytes, bytearray, memoryview))
        or not isinstance(raw_candidate_legs, Sequence)
        or isinstance(raw_candidate_legs, (str, bytes, bytearray, memoryview))
        or len(proposal_legs) != len(candidate_legs)
        or len(raw_candidate_legs) != len(candidate_legs)
    ):
        fail("proposal leg count")
    identity_fields = (
        "contract_id_ex",
        "underlying",
        "security_type",
        "expiration",
        "right",
        "currency",
        "exchange",
    )
    for candidate_leg, raw_candidate_leg, proposal_leg in zip(
        candidate_legs,
        raw_candidate_legs,
        proposal_legs,
        strict=True,
    ):
        if not isinstance(raw_candidate_leg, Mapping) or not isinstance(
            proposal_leg, Mapping
        ):
            fail("proposal leg")
        expected_side = "BUY" if candidate_leg["side"] == "LONG" else "SELL"
        if (
            proposal_leg.get("con_id") != candidate_leg["con_id"]
            or proposal_leg.get("side") != expected_side
            or proposal_leg.get("ratio") != candidate_leg["ratio"]
            or proposal_leg.get("quote_snapshot_id")
            != candidate_body.get("quote_batch_id")
            or decimal_value(proposal_leg.get("bid"), "proposal leg bid")
            != decimal_value(candidate_leg["bid"], "candidate leg bid")
            or decimal_value(proposal_leg.get("ask"), "proposal leg ask")
            != decimal_value(candidate_leg["ask"], "candidate leg ask")
            or _utc_timestamp(
                proposal_leg.get("quote_time"),
                reason=reason,
            )
            != _utc_timestamp(
                candidate_leg["observed_at"],
                reason=reason,
            )
        ):
            fail("proposal executable leg")
        if any(
            proposal_leg.get(field) != raw_candidate_leg.get(field)
            for field in identity_fields
        ):
            fail("proposal contract identity")
        for field in ("strike", "multiplier"):
            if decimal_value(proposal_leg.get(field), field) != decimal_value(
                raw_candidate_leg.get(field), field
            ):
                fail("proposal contract numeric identity")

    debit = decimal_value(candidate_body.get("debit_usd"), "debit_usd")
    credit = decimal_value(candidate_body.get("credit_usd"), "credit_usd")
    all_in = decimal_value(
        candidate_body.get("all_in_cost_usd"), "all_in_cost_usd"
    )
    execution_cost = all_in - debit + credit
    pricing = proposal_body.get("pricing")
    risk = proposal_body.get("risk")
    if not isinstance(pricing, Mapping) or not isinstance(risk, Mapping):
        fail("proposal pricing or risk")
    if (
        decimal_value(pricing.get("reference_cost_usd"), "reference cost")
        != debit - credit
        or decimal_value(
            pricing.get("estimated_execution_costs_usd"),
            "execution cost",
        )
        != execution_cost
        or decimal_value(
            pricing.get("all_in_executable_cost_usd"),
            "all-in cost",
        )
        != all_in
        or decimal_value(risk.get("maximum_loss_usd"), "maximum loss")
        != decimal_value(candidate_body.get("max_loss_usd"), "maximum loss")
        or decimal_sequence(risk.get("breakevens"), "breakevens")
        != decimal_sequence(candidate_body.get("breakevens"), "breakevens")
    ):
        fail("proposal payoff or execution cost")
    candidate_profit = candidate_body.get("max_profit_usd")
    proposal_profit = risk.get("maximum_profit_usd")
    if (candidate_profit is None) != (proposal_profit is None) or (
        candidate_profit is not None
        and decimal_value(candidate_profit, "maximum profit")
        != decimal_value(proposal_profit, "maximum profit")
    ):
        fail("proposal maximum profit")


def _external_reference(value: object) -> dict[str, str]:
    raw = _exact_mapping(
        value,
        _EXTERNAL_REFERENCE_KEYS,
        reason="CANDIDATE_EVIDENCE_MANIFEST_INVALID",
    )
    return {
        "evidence_id": _nonempty_text(
            raw.get("evidence_id"),
            reason="CANDIDATE_EVIDENCE_MANIFEST_INVALID",
        ),
        "content_hash": _digest(
            raw.get("content_hash"),
            reason="CANDIDATE_EVIDENCE_MANIFEST_INVALID",
        ),
        "row_hash": _digest(
            raw.get("row_hash"),
            reason="CANDIDATE_EVIDENCE_MANIFEST_INVALID",
        ),
    }


def _verify_store(evidence_store: object) -> None:
    verifier = getattr(evidence_store, "verify_integrity", None)
    getter = getattr(evidence_store, "get", None)
    if not callable(verifier) or not callable(getter):
        _fail("CANDIDATE_EVIDENCE_STORE_UNAVAILABLE")
    try:
        verified = verifier()
    except Exception as exc:
        raise CandidateEvidenceManifestError(
            "CANDIDATE_EVIDENCE_STORE_UNAVAILABLE"
        ) from exc
    if verified is not True:
        _fail("CANDIDATE_EVIDENCE_STORE_UNAVAILABLE")


def _resolve_external_reference(
    reference: Mapping[str, str],
    *,
    evidence_store: object,
    symbol: str,
    cutoff: datetime,
    seen_ids: set[str],
) -> dict[str, object]:
    evidence_id = reference["evidence_id"]
    if evidence_id in seen_ids:
        _fail("CANDIDATE_EVIDENCE_MANIFEST_INVALID", "duplicate evidence_id")
    seen_ids.add(evidence_id)
    getter = getattr(evidence_store, "get")
    try:
        stored = getter(evidence_id)
    except Exception as exc:
        raise CandidateEvidenceManifestError(
            "CANDIDATE_EVIDENCE_STORE_UNAVAILABLE"
        ) from exc
    if stored is None:
        _fail("CANDIDATE_EVIDENCE_RECORD_MISSING")
    document = _stored_document(stored)

    if document.get("evidence_id") != evidence_id:
        _fail("CANDIDATE_EVIDENCE_RECORD_ID_MISMATCH")
    for field in ("content_hash", "row_hash"):
        actual = document.get(field)
        if not isinstance(actual, str) or actual != reference[field]:
            _fail("CANDIDATE_EVIDENCE_RECORD_HASH_MISMATCH", field)
    if document.get("symbol") != symbol:
        _fail("CANDIDATE_EVIDENCE_SYMBOL_MISMATCH")
    if document.get("decision_authority") != "SUPPORTING_ONLY":
        _fail("CANDIDATE_EVIDENCE_AUTHORITY_INVALID")
    first_seen = _utc_timestamp(
        document.get("first_seen_at"),
        reason="CANDIDATE_EVIDENCE_RECORD_INVALID",
    )
    if first_seen > cutoff:
        _fail("CANDIDATE_EVIDENCE_AFTER_CUTOFF")
    return document


def _stored_document(value: object) -> dict[str, object]:
    if isinstance(value, Mapping):
        return dict(value)
    converter = getattr(value, "as_dict", None)
    if callable(converter):
        try:
            converted = converter()
        except Exception as exc:
            raise CandidateEvidenceManifestError(
                "CANDIDATE_EVIDENCE_STORE_UNAVAILABLE"
            ) from exc
        if isinstance(converted, Mapping):
            return dict(converted)
    _fail("CANDIDATE_EVIDENCE_RECORD_INVALID")


def _exact_mapping(
    value: object,
    keys: frozenset[str],
    *,
    reason: str,
) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or set(value) != keys:
        _fail(reason, "keys")
    return value


def _sequence(value: object, *, reason: str) -> Sequence[object]:
    if not isinstance(value, Sequence) or isinstance(
        value, (str, bytes, bytearray, memoryview)
    ):
        _fail(reason, "collection")
    return value


def _nonempty_text(value: object, *, reason: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        _fail(reason, "text")
    return value


def _digest(value: object, *, reason: str) -> str:
    if not isinstance(value, str) or _HASH_RE.fullmatch(value) is None:
        _fail(reason, "hash")
    return value


def _utc_timestamp(
    value: object,
    *,
    reason: str,
    string_only: bool = False,
) -> datetime:
    if isinstance(value, datetime) and not string_only:
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(
                value[:-1] + "+00:00" if value.endswith("Z") else value
            )
        except ValueError as exc:
            raise CandidateEvidenceManifestError(reason, "timestamp") from exc
    else:
        _fail(reason, "timestamp")
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        _fail(reason, "timestamp must be aware UTC")
    return parsed.astimezone(timezone.utc)


def _fail(reason: str, detail: str = "") -> None:
    raise CandidateEvidenceManifestError(reason, detail)
