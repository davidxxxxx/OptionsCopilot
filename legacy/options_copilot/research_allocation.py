"""Replayable evidence for bounded supporting-only research allocation."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from decimal import Decimal, InvalidOperation


RESEARCH_ALLOCATION_SCHEMA = "options_copilot.research_allocation_evidence.v3"
RESEARCH_ALLOCATION_INPUT_INVALID = "RESEARCH_ALLOCATION_INPUT_INVALID"
DETERMINISTIC_PRIORITY_SOURCE = "NEWS_SUPPORTING_ONLY"
SHADOW_PRIORITY_SOURCE = "NEWS_SHADOW_PRIORITY_SUPPORTING_ONLY"
MAXIMUM_EVENT_ROWS = 50
MAXIMUM_PRIORITY_INPUTS = 30
MAXIMUM_RESEARCH_SYMBOLS = 30

ZERO = Decimal("0")
HUNDRED = Decimal("100")
_ALLOWED_SYMBOL_CHARACTERS = frozenset(
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-"
)


class ResearchAllocationInputError(ValueError):
    """One producer score was present but could not become truthful evidence."""

    reason_code = RESEARCH_ALLOCATION_INPUT_INVALID


def canonical_research_symbol(value: object) -> str | None:
    """Return one exact canonical US research symbol without type coercion."""

    if not isinstance(value, str):
        return None
    if value != value.strip() or value != value.upper():
        return None
    if (
        not value
        or len(value) > 16
        or value[0] not in "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        or any(character not in _ALLOWED_SYMBOL_CHARACTERS for character in value)
    ):
        return None
    return value


def canonical_research_score(value: object) -> Decimal | None:
    """Return one bounded exact score without manufacturing missing zeroes."""

    if isinstance(value, (bool, float)) or not isinstance(
        value, (str, int, Decimal)
    ):
        return None
    if isinstance(value, str) and (value != value.strip() or not value):
        return None
    try:
        score = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    if not score.is_finite() or not ZERO <= score <= HUNDRED:
        return None
    return ZERO if score == ZERO else score


def balanced_research_symbols(
    scanner_rows: Sequence[Mapping[str, object]],
    core_rows: Sequence[Mapping[str, object]],
    *,
    limit: int,
    preserve_scanner_order: bool = False,
) -> tuple[str, ...]:
    """Interleave ranked discovery and core fallbacks under one hard cap."""

    _validate_limit(limit)
    scanner = (
        _ordered_unique_symbols(scanner_rows)
        if preserve_scanner_order
        else _ranked_symbols(scanner_rows)
    )
    core = _ranked_symbols(core_rows)
    if not core:
        return scanner[:limit]
    if not scanner:
        return core[:limit]
    ordered: list[str] = []
    for index in range(max(len(core), len(scanner))):
        for group in (core, scanner):
            if index >= len(group):
                continue
            symbol = group[index]
            if symbol not in ordered:
                ordered.append(symbol)
            if len(ordered) >= limit:
                return tuple(ordered)
    return tuple(ordered)


def build_research_allocation_evidence(
    *,
    event_rows: Sequence[Mapping[str, object]],
    scanner_rows: Sequence[Mapping[str, object]],
    core_rows: Sequence[Mapping[str, object]],
    limit: int,
) -> dict[str, object]:
    """Build self-contained producer-equivalent allocation evidence."""

    _validate_limit(limit)
    if len(event_rows) > MAXIMUM_EVENT_ROWS:
        raise ValueError(
            "research allocation event rows exceed the bounded producer limit"
        )
    score_evidence = _aggregate_event_scores(event_rows)
    scanner_inputs = _aggregate_priority_inputs(scanner_rows)
    core_inputs = _aggregate_priority_inputs(core_rows)
    payload = _allocation_payload(
        score_evidence=score_evidence,
        scanner_inputs=scanner_inputs,
        core_inputs=core_inputs,
        limit=limit,
    )
    normalized = normalise_research_allocation_evidence(payload)
    if normalized is None:  # pragma: no cover - producer/consumer invariant
        raise RuntimeError("producer emitted invalid research allocation evidence")
    return normalized


def normalise_research_allocation_evidence(
    value: object,
) -> dict[str, object] | None:
    """Validate and replay one complete v3 scheduling evidence document."""

    if not isinstance(value, Mapping):
        return None
    expected_keys = frozenset(
        {
            "schema",
            "influence_scope",
            "decision_authority",
            "limit",
            "event_symbols",
            "total_event_symbol_count",
            "advisory_available_count",
            "advisory_selected_count",
            "advisory_coverage_count",
            "advisory_coverage_ratio",
            "advisory_order_changed_count",
            "advisory_selection_displacement_count",
            "advisory_promoted_symbols",
            "deterministic_baseline_symbols",
            "selected_symbols",
            "score_evidence",
            "scanner_score_inputs",
            "core_score_inputs",
            "eligibility_effect",
            "risk_effect",
            "approval_eligible",
            "instruction_creation_allowed",
            "order_allowed",
        }
    )
    if frozenset(value) != expected_keys:
        return None
    if (
        value.get("schema") != RESEARCH_ALLOCATION_SCHEMA
        or value.get("influence_scope") != "RESEARCH_SCHEDULING_HINT_ONLY"
        or value.get("decision_authority") != "SUPPORTING_ONLY"
        or value.get("eligibility_effect") != "NONE"
        or value.get("risk_effect") != "NONE"
        or value.get("approval_eligible") is not False
        or value.get("instruction_creation_allowed") is not False
        or value.get("order_allowed") is not False
    ):
        return None
    limit = value.get("limit")
    if (
        isinstance(limit, bool)
        or not isinstance(limit, int)
        or not 1 <= limit <= MAXIMUM_RESEARCH_SYMBOLS
    ):
        return None
    score_evidence = _normalise_event_scores(value.get("score_evidence"))
    scanner_inputs = _normalise_priority_inputs(
        value.get("scanner_score_inputs")
    )
    core_inputs = _normalise_priority_inputs(value.get("core_score_inputs"))
    if score_evidence is None or scanner_inputs is None or core_inputs is None:
        return None
    replayed = _allocation_payload(
        score_evidence=score_evidence,
        scanner_inputs=scanner_inputs,
        core_inputs=core_inputs,
        limit=limit,
    )
    return (
        replayed
        if _semantic_container(value) == _semantic_container(replayed)
        else None
    )


def normalise_research_allocation_read_model(
    value: Mapping[str, object],
) -> dict[str, object]:
    """Omit invalid allocation evidence from root and immutable read models."""

    result = dict(value)
    for container_name in (None, "immutable_inputs"):
        if container_name is None:
            container = result
        else:
            raw_container = result.get(container_name)
            if not isinstance(raw_container, Mapping):
                continue
            container = dict(raw_container)
        raw_trace = container.get("funnel_trace")
        if not isinstance(raw_trace, Mapping):
            continue
        trace = dict(raw_trace)
        allocation = normalise_research_allocation_evidence(
            trace.get("research_allocation")
        )
        if allocation is None:
            trace.pop("research_allocation", None)
        else:
            trace["research_allocation"] = allocation
        container["funnel_trace"] = trace
        if container_name is not None:
            result[container_name] = container
    return result


def _allocation_payload(
    *,
    score_evidence: tuple[dict[str, object], ...],
    scanner_inputs: tuple[dict[str, object], ...],
    core_inputs: tuple[dict[str, object], ...],
    limit: int,
) -> dict[str, object]:
    deterministic_event_rows = tuple(
        {
            "symbol": row["symbol"],
            "score": Decimal(str(row["deterministic_score"])),
        }
        for row in score_evidence
    )
    selected_event_rows = tuple(
        {
            "symbol": row["symbol"],
            "score": Decimal(str(row["selected_research_priority_score"])),
        }
        for row in score_evidence
    )
    scanner_rows = tuple(
        {"symbol": row["symbol"], "score": Decimal(str(row["score"]))}
        for row in scanner_inputs
    )
    core_rows = tuple(
        {"symbol": row["symbol"], "score": Decimal(str(row["score"]))}
        for row in core_inputs
    )
    deterministic_baseline = balanced_research_symbols(
        (*deterministic_event_rows, *scanner_rows),
        core_rows,
        limit=limit,
    )
    selected = balanced_research_symbols(
        (*selected_event_rows, *scanner_rows),
        core_rows,
        limit=limit,
    )
    event_symbols = tuple(row["symbol"] for row in score_evidence)
    advisory_symbols = {
        str(row["symbol"])
        for row in score_evidence
        if row["advisory_score"] is not None
    }
    selected_index = {symbol: index for index, symbol in enumerate(selected)}
    baseline_index = {
        symbol: index for index, symbol in enumerate(deterministic_baseline)
    }
    effective_shadow_symbols = {
        str(row["symbol"])
        for row in score_evidence
        if row["selected_research_priority_source"] == SHADOW_PRIORITY_SOURCE
    }
    promoted = tuple(
        symbol
        for symbol in selected
        if symbol in effective_shadow_symbols
        and (
            symbol not in baseline_index
            or selected_index[symbol] < baseline_index[symbol]
        )
    )
    order_changed = sum(
        left != right
        for left, right in zip(selected, deterministic_baseline, strict=False)
    ) + abs(len(selected) - len(deterministic_baseline))
    displaced = len(set(selected) - set(deterministic_baseline))
    coverage_count = len(advisory_symbols)
    coverage_ratio = (
        ZERO
        if not event_symbols
        else (Decimal(coverage_count) / Decimal(len(event_symbols))).quantize(
            Decimal("0.000001")
        )
    )
    return {
        "schema": RESEARCH_ALLOCATION_SCHEMA,
        "influence_scope": "RESEARCH_SCHEDULING_HINT_ONLY",
        "decision_authority": "SUPPORTING_ONLY",
        "limit": limit,
        "event_symbols": event_symbols,
        "total_event_symbol_count": len(event_symbols),
        "advisory_available_count": coverage_count,
        "advisory_selected_count": len(advisory_symbols & set(selected)),
        "advisory_coverage_count": coverage_count,
        "advisory_coverage_ratio": format(coverage_ratio, "f"),
        "advisory_order_changed_count": order_changed,
        "advisory_selection_displacement_count": displaced,
        "advisory_promoted_symbols": promoted,
        "deterministic_baseline_symbols": deterministic_baseline,
        "selected_symbols": selected,
        "score_evidence": score_evidence,
        "scanner_score_inputs": scanner_inputs,
        "core_score_inputs": core_inputs,
        "eligibility_effect": "NONE",
        "risk_effect": "NONE",
        "approval_eligible": False,
        "instruction_creation_allowed": False,
        "order_allowed": False,
    }


def _aggregate_event_scores(
    rows: Sequence[Mapping[str, object]],
) -> tuple[dict[str, object], ...]:
    aggregates: dict[str, dict[str, object]] = {}
    for row in rows:
        deterministic_score = _required_producer_score(
            row.get("deterministic_score"),
            field_name="event deterministic_score",
        )
        advisory_score = _optional_producer_score(
            row.get("advisory_score"),
            field_name="event advisory_score",
        )
        symbol = canonical_research_symbol(row.get("symbol"))
        if symbol is None:
            continue
        selected_score = deterministic_score
        selected_source = DETERMINISTIC_PRIORITY_SOURCE
        if advisory_score is not None and advisory_score > deterministic_score:
            selected_score = advisory_score
            selected_source = SHADOW_PRIORITY_SOURCE
        aggregate = aggregates.setdefault(
            symbol,
            {
                "symbol": symbol,
                "deterministic_score": deterministic_score,
                "advisory_score": advisory_score,
                "selected_research_priority_score": selected_score,
                "selected_research_priority_source": selected_source,
            },
        )
        aggregate["deterministic_score"] = max(
            Decimal(str(aggregate["deterministic_score"])), deterministic_score
        )
        current_advisory = aggregate["advisory_score"]
        if advisory_score is not None and (
            current_advisory is None
            or advisory_score > Decimal(str(current_advisory))
        ):
            aggregate["advisory_score"] = advisory_score
        current_selected = Decimal(
            str(aggregate["selected_research_priority_score"])
        )
        current_source = str(aggregate["selected_research_priority_source"])
        if selected_score > current_selected or (
            selected_score == current_selected
            and selected_source == DETERMINISTIC_PRIORITY_SOURCE
            and current_source == SHADOW_PRIORITY_SOURCE
        ):
            aggregate["selected_research_priority_score"] = selected_score
            aggregate["selected_research_priority_source"] = selected_source
    return tuple(
        {
            "symbol": symbol,
            "deterministic_score": format(
                Decimal(str(aggregates[symbol]["deterministic_score"])), "f"
            ),
            "advisory_score": (
                None
                if aggregates[symbol]["advisory_score"] is None
                else format(
                    Decimal(str(aggregates[symbol]["advisory_score"])), "f"
                )
            ),
            "selected_research_priority_score": format(
                Decimal(
                    str(aggregates[symbol]["selected_research_priority_score"])
                ),
                "f",
            ),
            "selected_research_priority_source": aggregates[symbol][
                "selected_research_priority_source"
            ],
        }
        for symbol in sorted(aggregates)
    )


def _aggregate_priority_inputs(
    rows: Sequence[Mapping[str, object]],
) -> tuple[dict[str, object], ...]:
    scores: dict[str, Decimal] = {}
    for row in rows:
        score = _required_producer_score(
            row.get("score"),
            field_name="priority score",
        )
        symbol = canonical_research_symbol(
            row.get("symbol", row.get("underlying"))
        )
        if symbol is None:
            continue
        scores[symbol] = max(scores.get(symbol, ZERO), score)
    return tuple(
        {"symbol": symbol, "score": format(score, "f")}
        for symbol, score in sorted(
            scores.items(), key=lambda item: (-item[1], item[0])
        )[:MAXIMUM_PRIORITY_INPUTS]
    )


def _normalise_event_scores(
    value: object,
) -> tuple[dict[str, object], ...] | None:
    if (
        isinstance(value, (str, bytes, bytearray, memoryview))
        or not isinstance(value, Sequence)
        or len(value) > MAXIMUM_EVENT_ROWS
    ):
        return None
    expected_keys = frozenset(
        {
            "symbol",
            "deterministic_score",
            "advisory_score",
            "selected_research_priority_score",
            "selected_research_priority_source",
        }
    )
    rows: list[dict[str, object]] = []
    symbols: list[str] = []
    for raw in value:
        if not isinstance(raw, Mapping) or frozenset(raw) != expected_keys:
            return None
        symbol = canonical_research_symbol(raw.get("symbol"))
        deterministic_score = canonical_research_score(
            raw.get("deterministic_score")
        )
        advisory_raw = raw.get("advisory_score")
        advisory_score = (
            None
            if advisory_raw is None
            else canonical_research_score(advisory_raw)
        )
        selected_score = canonical_research_score(
            raw.get("selected_research_priority_score")
        )
        source = raw.get("selected_research_priority_source")
        if (
            symbol is None
            or symbol in symbols
            or deterministic_score is None
            or (advisory_raw is not None and advisory_score is None)
            or selected_score is None
            or source
            not in {DETERMINISTIC_PRIORITY_SOURCE, SHADOW_PRIORITY_SOURCE}
        ):
            return None
        expected_score = deterministic_score
        expected_source = DETERMINISTIC_PRIORITY_SOURCE
        if advisory_score is not None and advisory_score > deterministic_score:
            expected_score = advisory_score
            expected_source = SHADOW_PRIORITY_SOURCE
        if selected_score != expected_score or source != expected_source:
            return None
        symbols.append(symbol)
        rows.append(
            {
                "symbol": symbol,
                "deterministic_score": format(deterministic_score, "f"),
                "advisory_score": (
                    None
                    if advisory_score is None
                    else format(advisory_score, "f")
                ),
                "selected_research_priority_score": format(selected_score, "f"),
                "selected_research_priority_source": source,
            }
        )
    if tuple(symbols) != tuple(sorted(symbols)):
        return None
    return tuple(rows)


def _normalise_priority_inputs(
    value: object,
) -> tuple[dict[str, object], ...] | None:
    if (
        isinstance(value, (str, bytes, bytearray, memoryview))
        or not isinstance(value, Sequence)
        or len(value) > MAXIMUM_PRIORITY_INPUTS
    ):
        return None
    rows: list[dict[str, object]] = []
    seen: set[str] = set()
    for raw in value:
        if not isinstance(raw, Mapping) or frozenset(raw) != {"symbol", "score"}:
            return None
        symbol = canonical_research_symbol(raw.get("symbol"))
        score = canonical_research_score(raw.get("score"))
        if symbol is None or symbol in seen or score is None:
            return None
        seen.add(symbol)
        rows.append({"symbol": symbol, "score": format(score, "f")})
    expected = sorted(
        rows,
        key=lambda row: (-Decimal(str(row["score"])), str(row["symbol"])),
    )
    return tuple(rows) if rows == expected else None


def _ranked_symbols(
    *groups: Sequence[Mapping[str, object]],
) -> tuple[str, ...]:
    scores: dict[str, Decimal] = {}
    for group in groups:
        for row in group:
            symbol = canonical_research_symbol(
                row.get("symbol", row.get("underlying"))
            )
            score = _required_producer_score(
                row.get("score"),
                field_name="ranked priority score",
            )
            if symbol is None:
                continue
            scores[symbol] = max(scores.get(symbol, ZERO), score)
    return tuple(
        symbol
        for symbol, _score in sorted(
            scores.items(), key=lambda item: (-item[1], item[0])
        )
    )


def _ordered_unique_symbols(
    rows: Sequence[Mapping[str, object]],
) -> tuple[str, ...]:
    symbols: list[str] = []
    for row in rows:
        symbol = canonical_research_symbol(
            row.get("symbol", row.get("underlying"))
        )
        _required_producer_score(
            row.get("score"),
            field_name="ordered priority score",
        )
        if symbol is not None and symbol not in symbols:
            symbols.append(symbol)
    return tuple(symbols)


def _required_producer_score(value: object, *, field_name: str) -> Decimal:
    score = canonical_research_score(value)
    if score is None:
        raise ResearchAllocationInputError(
            f"{RESEARCH_ALLOCATION_INPUT_INVALID}: invalid {field_name}"
        )
    return score


def _optional_producer_score(
    value: object,
    *,
    field_name: str,
) -> Decimal | None:
    if value is None:
        return None
    return _required_producer_score(value, field_name=field_name)


def _validate_limit(limit: int) -> None:
    if (
        isinstance(limit, bool)
        or not isinstance(limit, int)
        or not 1 <= limit <= MAXIMUM_RESEARCH_SYMBOLS
    ):
        raise ValueError("research allocation limit must be between 1 and 30")


def _semantic_container(value: object) -> object:
    """Treat JSON arrays and immutable tuples as the same evidence container."""

    if isinstance(value, Mapping):
        return {key: _semantic_container(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(
        value, (str, bytes, bytearray, memoryview)
    ):
        return [_semantic_container(item) for item in value]
    return value


__all__ = [
    "DETERMINISTIC_PRIORITY_SOURCE",
    "MAXIMUM_EVENT_ROWS",
    "MAXIMUM_PRIORITY_INPUTS",
    "MAXIMUM_RESEARCH_SYMBOLS",
    "RESEARCH_ALLOCATION_INPUT_INVALID",
    "RESEARCH_ALLOCATION_SCHEMA",
    "ResearchAllocationInputError",
    "SHADOW_PRIORITY_SOURCE",
    "balanced_research_symbols",
    "build_research_allocation_evidence",
    "canonical_research_score",
    "canonical_research_symbol",
    "normalise_research_allocation_evidence",
    "normalise_research_allocation_read_model",
]
