from __future__ import annotations

import json
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone

import pytest

from options_copilot.decision import (
    GateId,
    GateStatus,
    ProvisionalWatchGatePreview,
    WatchLayerPreview,
)
from options_copilot.market.session_calendar import US_OPTIONS_TIMEZONE
from options_copilot.news.weekly_brief import (
    SourceHealthStatus,
    WeeklyBrief,
    WeeklyBriefEvidenceItem,
    WeeklyBriefSlotStatus,
    WeeklyBriefSourceHealth,
    evaluate_weekly_brief_slot,
    weekly_brief_source_bundle_hash,
)
from options_copilot.storage.canonical import canonical_hash


SESSION_DATE = date(2026, 9, 8)
WEEK_START = date(2026, 9, 7)
CUTOFF = datetime(2026, 9, 8, 8, 30, tzinfo=US_OPTIONS_TIMEZONE)
CALENDAR_HASH = canonical_hash({"calendar": "labor-day-week"})


def _hash(label: str) -> str:
    return canonical_hash({"label": label})


def _slot(*, evaluated_at: datetime | None = None):
    return evaluate_weekly_brief_slot(
        scheduled_for=CUTOFF,
        evaluated_at=evaluated_at or (CUTOFF + timedelta(seconds=20)),
        official_session_dates=(SESSION_DATE, date(2026, 9, 9), date(2026, 9, 10)),
        calendar_hash=CALENDAR_HASH,
    )


def _health(
    source: str,
    *,
    status: SourceHealthStatus = SourceHealthStatus.READY,
    mandatory: bool = False,
) -> WeeklyBriefSourceHealth:
    return WeeklyBriefSourceHealth.build(
        source=source,
        status=status,
        mandatory=mandatory,
        observed_at=CUTOFF - timedelta(seconds=5),
        reason_codes=() if status is SourceHealthStatus.READY else ("SOURCE_DEGRADED",),
        source_hash=None if status is not SourceHealthStatus.READY else _hash(source),
    )


def _item(item_id: str, occurred_at: datetime) -> WeeklyBriefEvidenceItem:
    return WeeklyBriefEvidenceItem.build(
        item_id=item_id,
        occurred_at=occurred_at,
        observed_at=CUTOFF - timedelta(minutes=1),
        source="OFFICIAL_TEST_SOURCE",
        source_hash=_hash(f"source-{item_id}"),
        headline=f"Headline {item_id}",
        summary=f"Summary {item_id}",
        symbols=("SPY",),
        affected_assets=("US_EQUITIES", "USD"),
        direction="MIXED",
        supporting_evidence_ids=(f"support-{item_id}",),
        contradicting_evidence_ids=(f"counter-{item_id}",),
        deepseek_summary=f"Supporting-only analysis {item_id}",
    )


def _source_bundle(
    *,
    items: tuple[WeeklyBriefEvidenceItem, ...] = (),
    health: tuple[WeeklyBriefSourceHealth, ...] = (),
) -> str:
    return weekly_brief_source_bundle_hash(
        calendar_hash=CALENDAR_HASH,
        evidence_items=items,
        source_health=health,
    )


def _watch(symbol: str, source_bundle_hash: str) -> ProvisionalWatchGatePreview:
    unavailable = {
        GateId.AUTHORITY_DATA,
        GateId.OPTION_EDGE_LIQUIDITY,
        GateId.STRUCTURE_ACCOUNT_RISK,
        GateId.RANKING_REVIEWABILITY,
    }
    layers = tuple(
        WatchLayerPreview.build(
            gate_id=gate_id,
            status=GateStatus.UNAVAILABLE if gate_id in unavailable else GateStatus.PASS,
            observed_at=CUTOFF,
            reason_codes=(
                ("PROVISIONAL_FIELD_UNAVAILABLE",)
                if gate_id in unavailable
                else ("PROVISIONAL_CONTEXT_ONLY",)
            ),
            source_hashes=(_hash(f"{symbol}-{gate_id.value}"),),
        )
        for gate_id in GateId
    )
    return ProvisionalWatchGatePreview.build(
        symbol=symbol,
        cutoff_at=CUTOFF,
        source_bundle_hash=source_bundle_hash,
        layers=layers,
    )


def _valid_brief() -> WeeklyBrief:
    prior = _item(
        "prior-valid",
        datetime(2026, 9, 2, 12, 0, tzinfo=US_OPTIONS_TIMEZONE),
    )
    upcoming = _item(
        "upcoming-valid",
        datetime(2026, 9, 10, 8, 30, tzinfo=US_OPTIONS_TIMEZONE),
    )
    next_preview = _item(
        "next-valid",
        datetime(2026, 9, 18, 8, 30, tzinfo=US_OPTIONS_TIMEZONE),
    )
    items = (prior, upcoming, next_preview)
    health = (_health("OFFICIAL_EVENTS", mandatory=True),)
    source_bundle_hash = _source_bundle(items=items, health=health)
    return WeeklyBrief.build(
        slot=_slot(),
        evidence_items=items,
        source_health=health,
        watch_items=(_watch("SPY", source_bundle_hash),),
    )


def test_holiday_week_uses_tuesday_first_session_exact_0830_et() -> None:
    decision = _slot()

    assert decision.status is WeeklyBriefSlotStatus.DUE
    assert decision.produce_allowed is True
    assert decision.week_start == WEEK_START
    assert decision.first_session_date == SESSION_DATE
    assert decision.scheduled_for == CUTOFF
    assert decision.reason_codes == ()


def test_slot_missed_is_not_replayed_after_exact_minute() -> None:
    missed = _slot(evaluated_at=CUTOFF + timedelta(minutes=1))

    assert missed.status is WeeklyBriefSlotStatus.NOT_RUN
    assert missed.produce_allowed is False
    assert missed.reason_codes == ("WEEKLY_BRIEF_SLOT_MISSED",)
    with pytest.raises(ValueError, match="DUE slot"):
        WeeklyBrief.build(
            slot=missed,
            evidence_items=(),
            source_health=(),
            watch_items=(),
        )


def test_monday_holiday_callback_cannot_replace_tuesday_first_session() -> None:
    monday = datetime(2026, 9, 7, 8, 30, tzinfo=US_OPTIONS_TIMEZONE)
    decision = evaluate_weekly_brief_slot(
        scheduled_for=monday,
        evaluated_at=monday + timedelta(seconds=5),
        official_session_dates=(SESSION_DATE,),
        calendar_hash=CALENDAR_HASH,
    )

    assert decision.status is WeeklyBriefSlotStatus.NOT_RUN
    assert decision.reason_codes == ("WEEKLY_BRIEF_SLOT_MISSED",)


def test_weekly_windows_content_and_hash_are_stable_under_input_reordering() -> None:
    prior = _item(
        "prior",
        datetime(2026, 9, 2, 12, 0, tzinfo=US_OPTIONS_TIMEZONE),
    )
    upcoming = _item(
        "upcoming",
        datetime(2026, 9, 10, 8, 30, tzinfo=US_OPTIONS_TIMEZONE),
    )
    next_preview = _item(
        "next",
        datetime(2026, 9, 18, 8, 30, tzinfo=US_OPTIONS_TIMEZONE),
    )
    items = (prior, upcoming, next_preview)
    health = (_health("OFFICIAL_EVENTS", mandatory=True), _health("DEEPSEEK"))
    source_bundle_hash = _source_bundle(items=items, health=health)
    watches = (_watch("SPY", source_bundle_hash),)

    first = WeeklyBrief.build(
        slot=_slot(),
        evidence_items=items,
        source_health=health,
        watch_items=watches,
    )
    replay = WeeklyBrief.build(
        slot=_slot(evaluated_at=CUTOFF + timedelta(seconds=40)),
        evidence_items=tuple(reversed(items)),
        source_health=tuple(reversed(health)),
        watch_items=watches,
    )

    assert first.content_hash == replay.content_hash
    assert first.idempotency_key == replay.idempotency_key
    assert first.append_payload() == replay.append_payload()
    assert tuple(item.item_id for item in first.prior_week_items) == ("prior",)
    assert tuple(item.item_id for item in first.upcoming_items) == ("upcoming",)
    assert tuple(item.item_id for item in first.next_preview_items) == ("next",)
    assert first.prior_week_start.date() == date(2026, 8, 31)
    assert first.prior_week_end.date() == WEEK_START
    assert first.upcoming_start.date() == SESSION_DATE
    assert first.upcoming_end.date() == date(2026, 9, 16)
    assert first.next_preview_end.date() == date(2026, 9, 23)
    assert canonical_hash(first.hash_payload()) == first.content_hash
    assert first.calendar_hash == CALENDAR_HASH


@pytest.mark.parametrize("count", (0, 10))
def test_weekly_brief_accepts_zero_or_ten_provisional_watches(count: int) -> None:
    health = (_health("OFFICIAL_EVENTS", mandatory=True),)
    source_bundle_hash = _source_bundle(health=health)
    watches = tuple(_watch(f"W{index}", source_bundle_hash) for index in range(count))

    brief = WeeklyBrief.build(
        slot=_slot(),
        evidence_items=(),
        source_health=health,
        watch_items=watches,
    )

    assert len(brief.watch_items) == count
    assert brief.as_dict()["watch_count"] == count


def test_weekly_brief_rejects_more_than_ten_watches() -> None:
    health = (_health("OFFICIAL_EVENTS", mandatory=True),)
    source_bundle_hash = _source_bundle(health=health)
    watches = tuple(_watch(f"W{index}", source_bundle_hash) for index in range(11))

    with pytest.raises(ValueError, match="at most ten"):
        WeeklyBrief.build(
            slot=_slot(),
            evidence_items=(),
            source_health=health,
            watch_items=watches,
        )


def test_brief_is_provisional_only_and_missing_live_fields_are_explicit() -> None:
    optional_degraded = _health(
        "DEEPSEEK",
        status=SourceHealthStatus.DEGRADED,
        mandatory=False,
    )
    health = (_health("OFFICIAL_EVENTS", mandatory=True), optional_degraded)
    source_bundle_hash = _source_bundle(health=health)
    brief = WeeklyBrief.build(
        slot=_slot(),
        evidence_items=(),
        source_health=health,
        watch_items=(_watch("SPY", source_bundle_hash),),
    )
    document = brief.as_dict()
    rendered = json.dumps(document, sort_keys=True).upper()

    assert document["status"] == "PROVISIONAL"
    assert document["decision"] == "OBSERVATION_ONLY"
    assert document["decision_authority"] == "SUPPORTING_ONLY"
    assert document["execution_allowed"] is False
    assert document["review_allowed"] is False
    assert document["combination_generation_allowed"] is False
    assert "READY_TO_TRADE" not in rendered
    assert '"EXECUTABLE"' not in rendered
    assert "candidate_id" not in rendered.lower()
    assert set(document["unavailable_fields"]) == {
        "option_chain",
        "option_quotes",
        "strategy_nav",
        "positioning",
    }
    assert all(
        item["status"] == "UNAVAILABLE"
        for item in document["unavailable_fields"].values()
    )


def test_evidence_observed_after_cutoff_is_rejected() -> None:
    future_observed = WeeklyBriefEvidenceItem.build(
        item_id="late-evidence",
        occurred_at=CUTOFF + timedelta(days=1),
        observed_at=CUTOFF + timedelta(seconds=1),
        source="TEST",
        source_hash=_hash("late-source"),
        headline="Late evidence",
        summary="Arrived after the point-in-time cutoff",
    )

    with pytest.raises(ValueError, match="point-in-time"):
        WeeklyBrief.build(
            slot=_slot(),
            evidence_items=(future_observed,),
            source_health=(),
            watch_items=(),
        )


def test_mandatory_source_unavailable_prevents_brief() -> None:
    unavailable = _health(
        "OFFICIAL_EVENTS",
        status=SourceHealthStatus.UNAVAILABLE,
        mandatory=True,
    )

    with pytest.raises(ValueError, match="WEEKLY_BRIEF_MANDATORY_SOURCE_UNAVAILABLE"):
        WeeklyBrief.build(
            slot=_slot(),
            evidence_items=(),
            source_health=(unavailable,),
            watch_items=(),
        )


def test_direct_construction_rejects_evidence_in_the_wrong_window() -> None:
    brief = _valid_brief()
    misplaced = brief.prior_week_items[0]

    with pytest.raises(ValueError, match="upcoming_items contains evidence outside its window"):
        replace(
            brief,
            prior_week_items=(),
            upcoming_items=brief.upcoming_items + (misplaced,),
        )


def test_direct_construction_rejects_evidence_reused_across_windows() -> None:
    brief = _valid_brief()
    duplicated = brief.upcoming_items[0]

    with pytest.raises(ValueError, match="must not appear in multiple windows"):
        replace(
            brief,
            prior_week_items=(duplicated,),
            upcoming_items=(duplicated,),
        )


def test_direct_construction_recomputes_source_bundle_hash() -> None:
    brief = _valid_brief()

    with pytest.raises(ValueError, match="source_bundle_hash does not match"):
        replace(brief, source_bundle_hash=_hash("forged-source-bundle"))


def test_brief_requires_at_least_one_mandatory_ready_source() -> None:
    optional = _health("OPTIONAL_EVENTS", mandatory=False)

    with pytest.raises(ValueError, match="WEEKLY_BRIEF_MANDATORY_SOURCE_UNAVAILABLE"):
        WeeklyBrief.build(
            slot=_slot(),
            evidence_items=(),
            source_health=(optional,),
            watch_items=(),
        )


@pytest.mark.parametrize(
    ("changes", "message"),
    (
        ({"first_session_date": date(2026, 9, 14)}, "first_session_date must belong"),
        (
            {
                "cutoff_at": datetime(
                    2026,
                    9,
                    8,
                    8,
                    31,
                    tzinfo=US_OPTIONS_TIMEZONE,
                ),
            },
            "cutoff_at must be exactly 08:30 ET",
        ),
        (
            {
                "upcoming_start": datetime(
                    2026,
                    9,
                    9,
                    0,
                    0,
                    tzinfo=US_OPTIONS_TIMEZONE,
                ),
            },
            "weekly brief windows must match",
        ),
    ),
)
def test_direct_construction_rejects_inconsistent_calendar_identity(
    changes: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        replace(_valid_brief(), **changes)


def test_build_rejects_evidence_outside_all_declared_windows() -> None:
    outside = _item(
        "outside",
        datetime(2026, 8, 1, 12, 0, tzinfo=US_OPTIONS_TIMEZONE),
    )

    with pytest.raises(ValueError, match="outside declared weekly windows"):
        WeeklyBrief.build(
            slot=_slot(),
            evidence_items=(outside,),
            source_health=(_health("OFFICIAL_EVENTS", mandatory=True),),
            watch_items=(),
        )
