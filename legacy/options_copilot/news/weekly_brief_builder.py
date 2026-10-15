"""Build weekly-brief inputs from cached, sanitized news read models."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, time, timedelta
from typing import Any

from options_copilot.decision.gates import (
    GateId,
    GateStatus,
    ProvisionalWatchGatePreview,
    WatchLayerPreview,
)
from options_copilot.market.session_calendar import US_OPTIONS_TIMEZONE
from options_copilot.storage.canonical import canonical_hash, utc_datetime

from .research_session_calendar import ResearchSessionCalendarSnapshot
from .weekly_brief import (
    SourceHealthStatus,
    WeeklyBriefEvidenceItem,
    WeeklyBriefSourceHealth,
    weekly_brief_source_bundle_hash,
)


@dataclass(frozen=True, slots=True)
class WeeklyBriefInputs:
    evidence_items: tuple[WeeklyBriefEvidenceItem, ...]
    source_health: tuple[WeeklyBriefSourceHealth, ...]
    watch_items: tuple[ProvisionalWatchGatePreview, ...]


def build_weekly_brief_inputs(
    *,
    cutoff_at: datetime,
    calendar: ResearchSessionCalendarSnapshot,
    news_payload: Mapping[str, object],
    calendar_payload: Mapping[str, object],
) -> WeeklyBriefInputs:
    """Project cached evidence only; never invoke a provider or broker."""

    cutoff = utc_datetime(cutoff_at, field="cutoff_at").astimezone(
        US_OPTIONS_TIMEZONE
    )
    week_start = cutoff.date() - timedelta(days=cutoff.date().weekday())
    prior_start = datetime.combine(
        week_start - timedelta(days=7),
        time.min,
        tzinfo=US_OPTIONS_TIMEZONE,
    )
    current_week_start = datetime.combine(
        week_start,
        time.min,
        tzinfo=US_OPTIONS_TIMEZONE,
    )
    upcoming_start = datetime.combine(
        calendar.sessions[0],
        time.min,
        tzinfo=US_OPTIONS_TIMEZONE,
    )
    preview_end = upcoming_start + timedelta(days=15)
    evidence: dict[str, WeeklyBriefEvidenceItem] = {}
    accepted_news_item_ids: set[str] = set()
    news_rows = _mapping_rows(news_payload.get("news"))
    calendar_rows = _mapping_rows(calendar_payload.get("calendar"))
    for rows, allow_headline_summary, is_news in (
        (news_rows, False, True),
        (calendar_rows, True, False),
    ):
        for raw in rows:
            item = _evidence_item(
                raw,
                cutoff=cutoff,
                allow_headline_summary=allow_headline_summary,
            )
            if item is None:
                continue
            occurred = item.occurred_at.astimezone(US_OPTIONS_TIMEZONE)
            if not (
                prior_start <= occurred < current_week_start
                or upcoming_start <= occurred < preview_end
            ):
                continue
            evidence.setdefault(item.item_id, item)
            if is_news:
                accepted_news_item_ids.add(item.item_id)
    evidence_items = tuple(sorted(evidence.values(), key=lambda item: item.item_hash))

    health: list[WeeklyBriefSourceHealth] = [
        WeeklyBriefSourceHealth.build(
            source=calendar.source,
            status=(
                SourceHealthStatus.READY
                if calendar.ready
                else SourceHealthStatus.UNAVAILABLE
            ),
            mandatory=True,
            observed_at=calendar.effective_at,
            reason_codes=calendar.reason_codes,
            source_hash=calendar.calendar_hash if calendar.ready else None,
        )
    ]
    for raw in _mapping_rows(news_payload.get("source_health")):
        observed_at = _timestamp(raw.get("asof"))
        if observed_at is None or observed_at > cutoff:
            continue
        source = _text(raw.get("source"), maximum=80)
        if source is None or source.upper() == calendar.source.upper():
            continue
        raw_status = str(raw.get("status") or "").strip().upper()
        ready = raw_status in {"READY", "UP", "HEALTHY"}
        health.append(
            WeeklyBriefSourceHealth.build(
                source=source,
                status=(
                    SourceHealthStatus.READY
                    if ready
                    else SourceHealthStatus.DEGRADED
                ),
                mandatory=False,
                observed_at=observed_at,
                reason_codes=() if ready else (_reason(raw.get("reason")),),
                source_hash=canonical_hash(dict(raw)) if ready else None,
            )
        )
    health_by_source = {item.source: item for item in health}
    source_health = tuple(sorted(health_by_source.values(), key=lambda item: item.source))
    source_bundle_hash = weekly_brief_source_bundle_hash(
        calendar_hash=calendar.calendar_hash,
        evidence_items=evidence_items,
        source_health=source_health,
    )
    watches = _watch_items(
        cutoff=cutoff,
        source_bundle_hash=source_bundle_hash,
        news_rows=news_rows,
        evidence_items=evidence_items,
        accepted_news_item_ids=accepted_news_item_ids,
    )
    return WeeklyBriefInputs(
        evidence_items=evidence_items,
        source_health=source_health,
        watch_items=watches,
    )


def _evidence_item(
    raw: Mapping[str, object],
    *,
    cutoff: datetime,
    allow_headline_summary: bool,
) -> WeeklyBriefEvidenceItem | None:
    times = raw.get("times") if isinstance(raw.get("times"), Mapping) else {}
    occurred = _timestamp(
        times.get("event_at")
        or raw.get("scheduled_at")
        or times.get("published_at")
        or raw.get("published_at")
    )
    observed = _timestamp(
        times.get("observed_at")
        or raw.get("observed_at")
        or times.get("first_seen_at")
    )
    if occurred is None or observed is None or observed > cutoff:
        return None
    raw_id = _text(raw.get("id"), maximum=160)
    headline = _text(raw.get("title") or raw.get("headline"), maximum=400)
    summary = _text(
        raw.get("summary")
        or raw.get("description")
        or (headline if allow_headline_summary else None),
        maximum=1600,
    )
    source = _text(raw.get("source"), maximum=120)
    if raw_id is None or headline is None or summary is None or source is None:
        return None
    provenance = _mapping_rows(raw.get("provenance"))
    source_hash = next(
        (
            value
            for item in provenance
            if (value := _digest(item.get("content_hash"))) is not None
        ),
        None,
    ) or canonical_hash(
        {
            "id": raw_id,
            "occurred_at": occurred,
            "observed_at": observed,
            "source": source,
            "headline": headline,
            "summary": summary,
        }
    )
    symbols = _symbols(raw.get("symbols"))
    direction = (_text(raw.get("direction"), maximum=32) or "UNKNOWN").upper()
    advisory = raw.get("research_advisory")
    advisory = advisory if isinstance(advisory, Mapping) else {}
    advisory_classification = advisory.get("classification")
    advisory_classification = (
        advisory_classification
        if isinstance(advisory_classification, Mapping)
        else {}
    )
    counter_values = (
        *_sequence(raw.get("counter_evidence")),
        *_sequence(advisory_classification.get("counter_evidence")),
    )
    counter = tuple(
        canonical_hash({"counter_evidence": text})
        for value in counter_values[:16]
        if (text := _text(value, maximum=480)) is not None
    )
    classifier = (_text(raw.get("classifier"), maximum=80) or "").upper()
    advisory_classifier = (
        _text(advisory.get("classifier"), maximum=80) or ""
    ).upper()
    return WeeklyBriefEvidenceItem.build(
        item_id=raw_id,
        occurred_at=occurred,
        observed_at=observed,
        source=source,
        source_hash=source_hash,
        headline=headline,
        summary=summary,
        symbols=symbols,
        affected_assets=("US_EQUITIES", *symbols),
        direction=direction,
        supporting_evidence_ids=(source_hash,),
        contradicting_evidence_ids=counter,
        deepseek_summary=(
            _deepseek_summary(advisory_classification)
            if "DEEPSEEK" in advisory_classifier
            else summary
            if "DEEPSEEK" in classifier
            else None
        ),
    )


def _deepseek_summary(classification: Mapping[str, object]) -> str:
    category = (_text(classification.get("category"), maximum=48) or "UNCERTAIN").upper()
    direction = (_text(classification.get("direction"), maximum=32) or "UNCERTAIN").upper()
    horizon = (_text(classification.get("horizon"), maximum=32) or "UNCERTAIN").upper()
    raw_confidence = classification.get("confidence")
    confidence = (
        _text(str(raw_confidence), maximum=16)
        if raw_confidence is not None and not isinstance(raw_confidence, bool)
        else None
    ) or "UNAVAILABLE"
    return (
        "DeepSeek supporting view: "
        f"{category}; {direction}; {horizon}; confidence {confidence}."
    )


def _watch_items(
    *,
    cutoff: datetime,
    source_bundle_hash: str,
    news_rows: Sequence[Mapping[str, object]],
    evidence_items: Sequence[WeeklyBriefEvidenceItem],
    accepted_news_item_ids: set[str],
) -> tuple[ProvisionalWatchGatePreview, ...]:
    evidence_by_symbol: dict[str, list[str]] = {}
    for item in evidence_items:
        for symbol in item.symbols:
            evidence_by_symbol.setdefault(symbol, []).append(item.item_hash)
    ranked: list[tuple[int, str]] = []
    for raw in news_rows:
        raw_id = _text(raw.get("id"), maximum=160)
        if raw_id is None or raw_id not in accepted_news_item_ids:
            continue
        # New read models expose an independent bound-symbol watch rank. Keep
        # the legacy fallback only when the field is absent, never when an
        # explicit None quarantines a symbol-less observation.
        rank = raw.get("watch_rank", raw.get("research_rank"))
        if not isinstance(rank, int) or isinstance(rank, bool) or rank < 1:
            continue
        for symbol in _symbols(raw.get("symbols")):
            ranked.append((rank, symbol))
    symbols: list[str] = []
    for _, symbol in sorted(ranked):
        if symbol in evidence_by_symbol and symbol not in symbols:
            symbols.append(symbol)
        if len(symbols) == 10:
            break
    watches: list[ProvisionalWatchGatePreview] = []
    unavailable = {
        GateId.AUTHORITY_DATA,
        GateId.OPTION_EDGE_LIQUIDITY,
        GateId.STRUCTURE_ACCOUNT_RISK,
        GateId.RANKING_REVIEWABILITY,
    }
    for symbol in symbols:
        hashes = tuple(sorted(set(evidence_by_symbol[symbol])))
        layers = tuple(
            WatchLayerPreview.build(
                gate_id=gate_id,
                status=(
                    GateStatus.UNAVAILABLE if gate_id in unavailable else GateStatus.PASS
                ),
                observed_at=cutoff,
                reason_codes=(
                    ("PROVISIONAL_FIELD_UNAVAILABLE",)
                    if gate_id in unavailable
                    else ("PROVISIONAL_CONTEXT_ONLY",)
                ),
                source_hashes=hashes,
            )
            for gate_id in GateId
        )
        watches.append(
            ProvisionalWatchGatePreview.build(
                symbol=symbol,
                cutoff_at=cutoff,
                source_bundle_hash=source_bundle_hash,
                layers=layers,
            )
        )
    return tuple(watches)


def _mapping_rows(value: object) -> tuple[Mapping[str, object], ...]:
    return tuple(item for item in _sequence(value) if isinstance(item, Mapping))


def _sequence(value: object) -> tuple[object, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return ()
    return tuple(value)


def _timestamp(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(US_OPTIONS_TIMEZONE) if parsed.tzinfo is not None else None


def _symbols(value: object) -> tuple[str, ...]:
    result: list[str] = []
    for item in _sequence(value):
        symbol = _text(item, maximum=15)
        if symbol is not None:
            normalized = symbol.upper()
            if normalized not in result:
                result.append(normalized)
    return tuple(result[:16])


def _digest(value: object) -> str | None:
    if isinstance(value, str) and len(value) == 64:
        try:
            int(value, 16)
        except ValueError:
            return None
        return value.lower()
    return None


def _reason(value: object) -> str:
    return (_text(value, maximum=80) or "PROVIDER_DEGRADED").upper()


def _text(value: object, *, maximum: int) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = " ".join(value.split())
    return cleaned[:maximum] if cleaned else None


__all__ = ["WeeklyBriefInputs", "build_weekly_brief_inputs"]
