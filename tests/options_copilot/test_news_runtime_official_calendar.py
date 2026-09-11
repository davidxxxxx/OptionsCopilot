from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from options_copilot.news_runtime import NewsCoordinator
from options_copilot.providers import EarningsEvent, NewsEvent
from options_copilot.providers.official import (
    OfficialCalendarProvider,
    OfficialCalendarSource,
)


NOW = datetime(2026, 8, 5, 12, 0, tzinfo=timezone.utc)
FED_URL = "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"
BLS_URL = "https://www.bls.gov/schedule/news_release/bls.ics"
IR_URL = "https://investor.example.test/events"


def _parser(payload: object):
    assert isinstance(payload, dict)
    return payload["events"]


def _source(
    source: str,
    url: str,
    category: str,
    *,
    symbols: tuple[str, ...] = (),
) -> OfficialCalendarSource:
    return OfficialCalendarSource(
        source=source,
        source_url=url,
        category=category,
        parser=_parser,
        timezone_name="America/New_York",
        symbols=symbols,
    )


class _FinnhubCalendar:
    health = "READY"
    health_reason = None

    def earnings_calendar(self, start: date, end: date):
        assert start == NOW.date()
        assert end == NOW.date() + timedelta(days=14)
        return (
            EarningsEvent(
                event_id="finnhub-aapl",
                symbol="AAPL",
                report_date=date(2026, 8, 7),
                hour="bmo",
                eps_estimate=None,
                revenue_estimate=None,
                source="finnhub",
                first_seen_at=NOW,
                ingested_at=NOW,
                observed_at=NOW,
            ),
        )


class _NewsProvider:
    health = "READY"

    def news(self, symbols: tuple[str, ...], *, limit: int = 50):
        return (
            NewsEvent(
                event_id="macro-news",
                symbol="SPY",
                source="wire",
                headline="Federal Reserve decision is approaching",
                summary="FOMC statement due soon.",
                url="https://example.test/fomc",
                published_at=NOW - timedelta(minutes=2),
                first_seen_at=NOW - timedelta(minutes=1),
                ingested_at=NOW,
                observed_at=NOW,
                source_rank=4,
            ),
        )


def _ready_official_provider() -> OfficialCalendarProvider:
    payloads = {
        FED_URL: {
            "events": [
                {
                    "id": "fomc-2026-08-06",
                    "title": "Federal Open Market Committee decision",
                    "scheduled_at": "2026-08-06T14:00:00-04:00",
                    "published_at": "2026-01-01T12:00:00Z",
                }
            ]
        },
        BLS_URL: {
            "events": [
                {
                    "id": "cpi-2026-08",
                    "title": "Consumer Price Index",
                    "scheduled_at": "2026-08-12T08:30:00-04:00",
                    "published_at": "2026-07-15T13:00:00Z",
                }
            ]
        },
        IR_URL: {
            "events": [
                {
                    "id": "nvda-fy27-q2",
                    "title": "NVIDIA second-quarter results",
                    "event_date": "2026-08-18",
                    "symbols": ["NVDA"],
                    "url": "https://investor.example.test/events/nvda-fy27-q2",
                }
            ]
        },
    }
    return OfficialCalendarProvider(
        sources=(
            _source("Federal Reserve", FED_URL, "FOMC"),
            _source("Bureau of Labor Statistics", BLS_URL, "MACRO"),
            _source("NVIDIA Investor Relations", IR_URL, "EARNINGS", symbols=("NVDA",)),
        ),
        transport=lambda url, **_kwargs: payloads[url],
        now=lambda: NOW,
    )


class _CountingOfficialProvider:
    def __init__(self, delegate: OfficialCalendarProvider) -> None:
        self.delegate = delegate
        self.calls = 0

    @property
    def health(self) -> str:
        return self.delegate.health

    def future_two_weeks(self, *, now: datetime):
        self.calls += 1
        return self.delegate.future_two_weeks(now=now)


def test_ready_official_calendar_is_cached_for_fifteen_minutes_without_fake_freshness(
    tmp_path: Path,
) -> None:
    current = {"now": NOW}
    provider = _CountingOfficialProvider(_ready_official_provider())
    runtime = NewsCoordinator(
        tmp_path / "official-calendar-cache.sqlite3",
        official_calendar_provider=provider,
        clock=lambda: current["now"],
    )
    try:
        runtime.refresh_once()
        first = runtime.calendar_payload()
        assert provider.calls == 1
        assert first["provider"]["asof"] == NOW.isoformat()
        assert {item["observed_at"] for item in first["sources"]} == {
            NOW.isoformat()
        }
        assert runtime.news_payload()["source_health"] == [
            {
                "source": "OFFICIAL_CALENDAR",
                "source_kind": "OFFICIAL_CALENDAR",
                "status": "READY",
                "reason": None,
                "success_count": 3,
                "failure_date_count": 0,
                "asof": NOW.isoformat(),
                "decision_authority": "SUPPORTING_ONLY",
            }
        ]

        current["now"] = NOW + timedelta(minutes=14, seconds=59)
        runtime.refresh_once()
        cached = runtime.calendar_payload()
        assert provider.calls == 1
        assert cached["provider"] == {
            "name": "calendar-coordinator",
            "status": "READY",
            "latency_ms": None,
            "asof": NOW.isoformat(),
            "message": "official calendar cache remains current",
        }
        assert {item["observed_at"] for item in cached["sources"]} == {
            NOW.isoformat()
        }

        current["now"] = NOW + timedelta(minutes=15)
        runtime.refresh_once()
        refreshed = runtime.calendar_payload()
        assert provider.calls == 2
        assert refreshed["provider"]["asof"] == current["now"].isoformat()
        assert {item["observed_at"] for item in refreshed["sources"]} == {
            current["now"].isoformat()
        }
    finally:
        runtime.close()


def test_failed_official_calendar_waits_five_minutes_before_retry(
    tmp_path: Path,
) -> None:
    class FailingProvider:
        health = "DEGRADED"

        def __init__(self) -> None:
            self.calls = 0

        def future_two_weeks(self, *, now: datetime):
            self.calls += 1
            raise TimeoutError("redacted provider timeout")

    current = {"now": NOW}
    provider = FailingProvider()
    runtime = NewsCoordinator(
        tmp_path / "official-calendar-backoff.sqlite3",
        official_calendar_provider=provider,
        clock=lambda: current["now"],
    )
    try:
        runtime.refresh_once()
        first = runtime.calendar_payload()
        assert provider.calls == 1
        assert first["provider"]["asof"] == NOW.isoformat()
        assert first["reasons"] == ["OFFICIAL_CALENDAR_PROVIDER_UNAVAILABLE"]

        current["now"] = NOW + timedelta(minutes=4, seconds=59)
        runtime.refresh_once()
        cached = runtime.calendar_payload()
        assert provider.calls == 1
        assert cached["provider"] == {
            "name": "calendar-coordinator",
            "status": "DEGRADED",
            "latency_ms": None,
            "asof": NOW.isoformat(),
            "message": "one or more providers remain degraded; retry is deferred",
        }
        assert cached["calendar"] == []

        current["now"] = NOW + timedelta(minutes=5)
        runtime.refresh_once()
        assert provider.calls == 2
        assert runtime.calendar_payload()["provider"]["asof"] == current[
            "now"
        ].isoformat()
    finally:
        runtime.close()


def test_official_snapshot_merges_with_finnhub_and_preserves_auditable_windows(
    tmp_path: Path,
) -> None:
    runtime = NewsCoordinator(
        tmp_path / "official-calendar.sqlite3",
        calendar_providers=(_FinnhubCalendar(),),
        official_calendar_provider=_ready_official_provider(),
        clock=lambda: NOW,
    )
    try:
        result = runtime.refresh_once()
        payload = runtime.calendar_payload()

        assert result["status"] == "READY"
        assert payload["provider"]["status"] == "READY"
        assert payload["decision"] == "OBSERVATION_ONLY"
        assert payload["decision_authority"] == "SUPPORTING_ONLY"
        assert payload["approval_eligible"] is False
        assert payload["instruction_creation_allowed"] is False
        assert payload["window_start"] == NOW.isoformat()
        assert payload["window_end"] == (NOW + timedelta(days=14)).isoformat()
        assert payload["snapshot_hash"]
        assert payload["count"] == 4

        rows = {item.get("source_id", item["id"]): item for item in payload["calendar"]}
        fomc = rows["fomc-2026-08-06"]
        assert fomc["event_at"] == "2026-08-06T18:00:00+00:00"
        assert fomc["category"] == "FOMC"
        assert fomc["importance"] == "CRITICAL"
        assert fomc["windows"] == ["THIS_WEEK", "FUTURE_TWO_WEEKS"]
        assert fomc["source"] == "Federal Reserve"
        assert fomc["first_seen_at"] == NOW.isoformat()
        assert fomc["observed_at"] == NOW.isoformat()
        assert fomc["content_hash"]
        assert fomc["record_hash"]
        assert fomc["decision_authority"] == "SUPPORTING_ONLY"
        assert fomc["provenance"][0]["source"] == "Federal Reserve"

        cpi = rows["cpi-2026-08"]
        assert cpi["windows"] == ["NEXT_WEEK", "FUTURE_TWO_WEEKS"]
        assert cpi["importance"] == "HIGH"

        earnings = rows["nvda-fy27-q2"]
        assert earnings["event_at"] is None
        assert earnings["event_date"] == "2026-08-18"
        assert earnings["schedule_precision"] == "DATE_ONLY"
        assert earnings["windows"] == ["FUTURE_TWO_WEEKS"]
        assert earnings["symbols"] == ["NVDA"]

        legacy = rows["finnhub-aapl"]
        assert legacy["symbols"] == ["AAPL"]
        assert legacy["category"] == "EARNINGS"
        assert legacy["windows"] == ["THIS_WEEK", "FUTURE_TWO_WEEKS"]
        assert {item["source"] for item in payload["sources"]} == {
            "Federal Reserve",
            "Bureau of Labor Statistics",
            "NVIDIA Investor Relations",
        }
    finally:
        runtime.close()


def test_official_calendar_importance_does_not_promote_every_macro_release(
    tmp_path: Path,
) -> None:
    provider = OfficialCalendarProvider(
        sources=(_source("Bureau of Labor Statistics", BLS_URL, "MACRO"),),
        transport=lambda *_args, **_kwargs: {
            "events": [
                {
                    "id": "cpi-major",
                    "title": "Consumer Price Index",
                    "scheduled_at": "2026-08-12T08:30:00-04:00",
                },
                {
                    "id": "worker-displacement",
                    "title": "Worker Displacement for 2023-2025",
                    "scheduled_at": "2026-08-13T10:00:00-04:00",
                },
            ],
        },
        now=lambda: NOW,
    )
    runtime = NewsCoordinator(
        tmp_path / "official-calendar-importance.sqlite3",
        official_calendar_provider=provider,
        clock=lambda: NOW,
    )
    try:
        runtime.refresh_once()
        rows = {
            item["source_id"]: item
            for item in runtime.calendar_payload()["calendar"]
        }

        assert rows["cpi-major"]["importance"] == "HIGH"
        assert rows["worker-displacement"]["importance"] == "MEDIUM"
    finally:
        runtime.close()


def test_degraded_official_snapshot_keeps_only_valid_rows_and_forces_no_trade(
    tmp_path: Path,
) -> None:
    provider = OfficialCalendarProvider(
        sources=(_source("Bureau of Labor Statistics", BLS_URL, "MACRO"),),
        transport=lambda *_args, **_kwargs: {
            "events": [
                {
                    "id": "jobs",
                    "title": "Employment Situation",
                    "scheduled_at": "2026-08-07T08:30:00-04:00",
                },
                {"id": "missing-schedule", "title": "Unknown release date"},
            ]
        },
        now=lambda: NOW,
    )
    runtime = NewsCoordinator(
        tmp_path / "degraded-calendar.sqlite3",
        official_calendar_provider=provider,
        clock=lambda: NOW,
    )
    try:
        result = runtime.refresh_once()
        payload = runtime.calendar_payload()

        assert result["status"] == "DEGRADED"
        assert runtime.health()["status"] == "DEGRADED"
        assert payload["provider"]["status"] == "DEGRADED"
        assert payload["decision"] == "NO_TRADE"
        assert payload["decision_authority"] == "SUPPORTING_ONLY"
        assert payload["reasons"] == [
            "BUREAU_OF_LABOR_STATISTICS:INCOMPLETE_RECORDS"
        ]
        assert [item["source_id"] for item in payload["calendar"]] == ["jobs"]
        assert all(
            item.get("source_id") != "missing-schedule" for item in payload["calendar"]
        )
        assert all(
            item["decision_authority"] == "SUPPORTING_ONLY"
            for item in payload["calendar"]
        )
    finally:
        runtime.close()


def test_invalid_official_snapshot_fails_closed_without_inventing_events(tmp_path: Path) -> None:
    class InvalidProvider:
        health = "READY"

        def future_two_weeks(self, *, now: datetime):
            return {"status": "READY", "events": [{"title": "unsourced"}]}

    runtime = NewsCoordinator(
        tmp_path / "invalid-calendar.sqlite3",
        official_calendar_provider=InvalidProvider(),
        clock=lambda: NOW,
    )
    try:
        runtime.refresh_once()
        payload = runtime.calendar_payload()

        assert payload["calendar"] == []
        assert payload["provider"]["status"] == "DEGRADED"
        assert payload["decision"] == "NO_TRADE"
        assert payload["reasons"] == ["OFFICIAL_CALENDAR_SNAPSHOT_INVALID"]
    finally:
        runtime.close()


def test_news_read_model_exposes_classifier_and_model_unavailable_counter_evidence(
    tmp_path: Path,
) -> None:
    runtime = NewsCoordinator(
        tmp_path / "classifier.sqlite3",
        news_providers=(_NewsProvider(),),
        core_symbols=("SPY",),
        clock=lambda: NOW,
    )
    try:
        runtime.refresh_once()
        row = runtime.news_payload()["news"][0]

        assert row["classifier"] == "DETERMINISTIC_RULES"
        assert row["classification"]["classifier"] == "DETERMINISTIC_RULES"
        assert row["classification"]["counter_evidence"] == [
            "Deterministic fallback; model corroboration unavailable"
        ]
        assert row["counter_evidence"] == [
            "Deterministic fallback; model corroboration unavailable"
        ]
        assert row["decision_authority"] == "SUPPORTING_ONLY"
    finally:
        runtime.close()
