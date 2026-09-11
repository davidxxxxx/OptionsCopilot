from __future__ import annotations

from datetime import datetime, timedelta, timezone

from options_copilot.decision import GateId, GateStatus
from options_copilot.news.research_session_calendar import (
    BoundedResearchSessionCalendar,
)
from options_copilot.news.weekly_brief_builder import build_weekly_brief_inputs
from options_copilot.storage.canonical import canonical_hash


CUTOFF = datetime(2026, 8, 10, 12, 30, tzinfo=timezone.utc)


def _news_row(*, observed_at: datetime = CUTOFF - timedelta(minutes=1)) -> dict[str, object]:
    content_hash = canonical_hash({"news": "nvda"})
    return {
        "id": "news-nvda",
        "title": "Nvidia catalyst",
        "summary": "A bounded supporting-only summary.",
        "source": "SEC",
        "symbols": ["NVDA"],
        "direction": "BULLISH",
        "research_rank": 1,
        "classifier": "DETERMINISTIC_NEWS_V1",
        "research_advisory": {
            "classifier": "DEEPSEEK_STRUCTURED_V1",
            "classification": {
                "category": "EARNINGS",
                "direction": "BULLISH",
                "horizon": "ONE_WEEK",
                "confidence": "0.82",
                "counter_evidence": ["Valuation remains elevated"],
            },
            "decision_authority": "SUPPORTING_ONLY",
        },
        "times": {
            "published_at": "2026-08-07T12:00:00+00:00",
            "observed_at": observed_at.isoformat(),
        },
        "provenance": [{"content_hash": content_hash}],
        "counter_evidence": ["Valuation remains elevated"],
    }


def test_builder_uses_cached_point_in_time_rows_and_provisional_gate_preview() -> None:
    calendar = BoundedResearchSessionCalendar().snapshot(now=CUTOFF)
    result = build_weekly_brief_inputs(
        cutoff_at=CUTOFF,
        calendar=calendar,
        news_payload={
            "news": [_news_row()],
            "source_health": [
                {
                    "source": "SEC",
                    "status": "READY",
                    "asof": (CUTOFF - timedelta(minutes=1)).isoformat(),
                }
            ],
        },
        calendar_payload={"calendar": []},
    )

    assert len(result.evidence_items) == 1
    assert result.evidence_items[0].deepseek_summary == (
        "DeepSeek supporting view: EARNINGS; BULLISH; ONE_WEEK; confidence 0.82."
    )
    assert any(item.mandatory and item.status.value == "READY" for item in result.source_health)
    assert len(result.watch_items) == 1
    watch = result.watch_items[0]
    assert watch.symbol == "NVDA"
    layers = {item.gate_id: item for item in watch.layers}
    for gate_id in (
        GateId.AUTHORITY_DATA,
        GateId.OPTION_EDGE_LIQUIDITY,
        GateId.STRUCTURE_ACCOUNT_RISK,
        GateId.RANKING_REVIEWABILITY,
    ):
        assert layers[gate_id].status is GateStatus.UNAVAILABLE
    assert layers[GateId.MARKET_CREDIT_REGIME].status is GateStatus.PASS
    assert layers[GateId.UNDERLYING_EVENT].status is GateStatus.PASS


def test_builder_drops_post_cutoff_rows_and_does_not_fill_watchlist() -> None:
    calendar = BoundedResearchSessionCalendar().snapshot(now=CUTOFF)
    result = build_weekly_brief_inputs(
        cutoff_at=CUTOFF,
        calendar=calendar,
        news_payload={
            "news": [_news_row(observed_at=CUTOFF + timedelta(seconds=1))],
            "source_health": [],
        },
        calendar_payload={"calendar": []},
    )

    assert result.evidence_items == ()
    assert result.watch_items == ()


def test_bound_watch_rank_is_not_displaced_by_unbound_research_rank() -> None:
    calendar = BoundedResearchSessionCalendar().snapshot(now=CUTOFF)
    unbound = _news_row()
    unbound.update(
        {
            "id": "unbound-market-news",
            "symbols": [],
            "research_rank": 1,
            "watch_rank": None,
        }
    )
    bound = _news_row()
    bound.update(
        {
            "id": "bound-aapl-news",
            "symbols": ["AAPL"],
            "research_rank": None,
            "watch_rank": 1,
        }
    )

    result = build_weekly_brief_inputs(
        cutoff_at=CUTOFF,
        calendar=calendar,
        news_payload={"news": [unbound, bound], "source_health": []},
        calendar_payload={"calendar": []},
    )

    assert [item.symbol for item in result.watch_items] == ["AAPL"]


def test_builder_keeps_calendar_event_with_headline_only_summary() -> None:
    calendar = BoundedResearchSessionCalendar().snapshot(now=CUTOFF)
    event_hash = canonical_hash({"calendar": "mndy-earnings"})
    result = build_weekly_brief_inputs(
        cutoff_at=CUTOFF,
        calendar=calendar,
        news_payload={"news": [], "source_health": []},
        calendar_payload={
            "calendar": [
                {
                    "id": "earnings-mndy",
                    "title": "MNDY earnings",
                    "summary": None,
                    "source": "Finnhub",
                    "symbols": ["MNDY"],
                    "category": "EARNINGS",
                    "times": {
                        "event_at": "2026-08-10T20:00:00+08:00",
                        "observed_at": "2026-08-07T17:13:32+08:00",
                    },
                    "provenance": [{"content_hash": event_hash}],
                }
            ]
        },
    )

    assert len(result.evidence_items) == 1
    assert result.evidence_items[0].headline == "MNDY earnings"
    assert result.evidence_items[0].summary == "MNDY earnings"
    assert result.watch_items == ()


def test_builder_still_drops_headline_only_news_row() -> None:
    calendar = BoundedResearchSessionCalendar().snapshot(now=CUTOFF)
    news_row = _news_row()
    news_row["summary"] = None

    result = build_weekly_brief_inputs(
        cutoff_at=CUTOFF,
        calendar=calendar,
        news_payload={"news": [news_row], "source_health": []},
        calendar_payload={"calendar": []},
    )

    assert result.evidence_items == ()
    assert result.watch_items == ()


def test_calendar_evidence_cannot_qualify_rejected_ranked_news_watch() -> None:
    calendar = BoundedResearchSessionCalendar().snapshot(now=CUTOFF)
    news_row = _news_row()
    news_row.update(
        {
            "id": "news-mndy",
            "summary": None,
            "symbols": ["MNDY"],
        }
    )
    event_hash = canonical_hash({"calendar": "mndy-earnings"})

    result = build_weekly_brief_inputs(
        cutoff_at=CUTOFF,
        calendar=calendar,
        news_payload={"news": [news_row], "source_health": []},
        calendar_payload={
            "calendar": [
                {
                    "id": "earnings-mndy",
                    "title": "MNDY earnings",
                    "summary": None,
                    "source": "Finnhub",
                    "symbols": ["MNDY"],
                    "category": "EARNINGS",
                    "times": {
                        "event_at": "2026-08-10T20:00:00+08:00",
                        "observed_at": "2026-08-07T17:13:32+08:00",
                    },
                    "provenance": [{"content_hash": event_hash}],
                }
            ]
        },
    )

    assert tuple(item.item_id for item in result.evidence_items) == (
        "earnings-mndy",
    )
    assert result.watch_items == ()
