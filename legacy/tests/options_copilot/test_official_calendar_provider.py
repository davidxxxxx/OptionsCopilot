from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import re

from options_copilot.providers.official import (
    OfficialCalendarProvider,
    OfficialCalendarSource,
    OfficialCalendarTransportError,
)


NOW = datetime(2026, 8, 5, 12, 0, tzinfo=timezone.utc)
FED_URL = "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"
BLS_URL = "https://www.bls.gov/schedule/news_release/bls.ics"
IR_URL = "https://investor.example.com/events"


def _parser(payload: object):
    assert isinstance(payload, dict)
    return payload["events"]


def _source(
    *,
    source: str,
    url: str,
    category: str,
    timezone_name: str = "America/New_York",
    symbols: tuple[str, ...] = (),
) -> OfficialCalendarSource:
    return OfficialCalendarSource(
        source=source,
        source_url=url,
        category=category,
        parser=_parser,
        timezone_name=timezone_name,
        symbols=symbols,
    )


def test_future_two_week_snapshot_normalizes_official_events_without_guessing_time() -> None:
    payloads = {
        FED_URL: {
            "events": [
                {
                    "id": "fomc-2026-08-06",
                    "title": "Federal Open Market Committee decision",
                    "scheduled_at": "2026-08-06T14:00:00-04:00",
                    "published_at": "2026-01-01T12:00:00Z",
                },
                {
                    "id": "outside-window",
                    "title": "Later FOMC meeting",
                    "scheduled_at": "2026-09-16T14:00:00-04:00",
                    "published_at": "2026-01-01T12:00:00Z",
                },
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
                    "symbols": ["nvda"],
                    "url": "https://investor.example.com/events/nvda-fy27-q2",
                }
            ]
        },
    }

    def transport(url, *, headers, timeout_seconds):
        assert headers["Accept"]
        assert timeout_seconds == 8.0
        return payloads[url]

    provider = OfficialCalendarProvider(
        sources=(
            _source(source="Federal Reserve", url=FED_URL, category="FOMC"),
            _source(source="Bureau of Labor Statistics", url=BLS_URL, category="MACRO"),
            _source(
                source="NVIDIA Investor Relations",
                url=IR_URL,
                category="EARNINGS",
                symbols=("NVDA",),
            ),
        ),
        transport=transport,
        now=lambda: NOW,
    )

    snapshot = provider.future_two_weeks()

    assert snapshot.status == "READY"
    assert snapshot.decision == "OBSERVATION_ONLY"
    assert snapshot.window_start == NOW
    assert snapshot.window_end == NOW + timedelta(days=14)
    assert [item.source_id for item in snapshot.events] == [
        "fomc-2026-08-06",
        "cpi-2026-08",
        "nvda-fy27-q2",
    ]
    fomc, cpi, earnings = snapshot.events
    assert fomc.category == "FOMC"
    assert fomc.scheduled_at == datetime(2026, 8, 6, 18, tzinfo=timezone.utc)
    assert fomc.event_date == date(2026, 8, 6)
    assert fomc.timezone_name == "America/New_York"
    assert fomc.schedule_precision == "EXACT"
    assert cpi.scheduled_at == datetime(2026, 8, 12, 12, 30, tzinfo=timezone.utc)
    assert earnings.scheduled_at is None
    assert earnings.event_date == date(2026, 8, 18)
    assert earnings.schedule_precision == "DATE_ONLY"
    assert earnings.symbols == ("NVDA",)
    assert earnings.source_url == IR_URL
    assert earnings.first_seen_at == NOW
    assert earnings.observed_at == NOW
    assert earnings.decision_authority == "SUPPORTING_ONLY"
    assert len(earnings.provenance) == 1
    assert earnings.provenance[0].source_url == IR_URL
    assert re.fullmatch(r"[0-9a-f]{64}", earnings.content_hash)
    assert re.fullmatch(r"[0-9a-f]{64}", earnings.record_hash)
    assert re.fullmatch(r"[0-9a-f]{64}", snapshot.snapshot_hash)
    assert snapshot.approval_eligible is False
    assert snapshot.instruction_creation_allowed is False


def test_repeated_observation_preserves_content_first_seen_and_updates_observed_at() -> None:
    clock = [NOW]
    payload = {
        "events": [
            {
                "id": "fomc-1",
                "title": "FOMC decision",
                "scheduled_at": "2026-08-06T18:00:00Z",
                "published_at": "2026-01-01T00:00:00Z",
            }
        ]
    }
    provider = OfficialCalendarProvider(
        sources=(_source(source="Federal Reserve", url=FED_URL, category="FOMC"),),
        transport=lambda *_args, **_kwargs: payload,
        now=lambda: clock[0],
    )

    first = provider.future_two_weeks().events[0]
    clock[0] += timedelta(minutes=5)
    second = provider.future_two_weeks().events[0]

    assert first.first_seen_at == second.first_seen_at == NOW
    assert second.observed_at == NOW + timedelta(minutes=5)
    assert first.content_hash == second.content_hash
    assert first.record_hash != second.record_hash


def test_partial_parse_is_degraded_no_trade_and_never_invents_missing_date() -> None:
    payload = {
        "events": [
            {
                "id": "valid",
                "title": "Employment Situation",
                "scheduled_at": "2026-08-07T08:30:00-04:00",
            },
            {
                "id": "missing-schedule",
                "title": "Unknown release date",
            },
        ]
    }
    provider = OfficialCalendarProvider(
        sources=(_source(source="Bureau of Labor Statistics", url=BLS_URL, category="MACRO"),),
        transport=lambda *_args, **_kwargs: payload,
        now=lambda: NOW,
    )

    snapshot = provider.future_two_weeks()

    assert snapshot.status == "DEGRADED"
    assert snapshot.decision == "NO_TRADE"
    assert [event.source_id for event in snapshot.events] == ["valid"]
    assert "BUREAU_OF_LABOR_STATISTICS:INCOMPLETE_RECORDS" in snapshot.reasons
    assert snapshot.sources[0].status == "DEGRADED"
    assert snapshot.sources[0].reason == "INCOMPLETE_RECORDS"
    assert all(event.source_id != "missing-schedule" for event in snapshot.events)


def test_naive_time_requires_declared_timezone_and_ambiguous_local_time_is_rejected() -> None:
    no_timezone = OfficialCalendarSource(
        source="Official source",
        source_url="https://official.example.test/calendar",
        category="MACRO",
        parser=_parser,
        timezone_name=None,
    )
    provider = OfficialCalendarProvider(
        sources=(no_timezone,),
        transport=lambda *_args, **_kwargs: {
            "events": [
                {
                    "id": "naive",
                    "title": "Release",
                    "scheduled_at": "2026-08-07T08:30:00",
                }
            ]
        },
        now=lambda: NOW,
    )
    assert provider.future_two_weeks().decision == "NO_TRADE"

    ambiguous = OfficialCalendarProvider(
        sources=(_source(source="Official source", url="https://official.example.test/dst", category="MACRO"),),
        transport=lambda *_args, **_kwargs: {
            "events": [
                {
                    "id": "ambiguous",
                    "title": "Release",
                    "scheduled_at": "2026-11-01T01:30:00",
                }
            ]
        },
        now=lambda: datetime(2026, 10, 31, 12, tzinfo=timezone.utc),
    )
    snapshot = ambiguous.future_two_weeks()
    assert snapshot.status == "DEGRADED"
    assert snapshot.events == ()


def test_network_failure_keeps_healthy_rows_visible_but_degrades_entire_snapshot() -> None:
    failed_url = "https://www.bea.gov/news/schedule"
    healthy_url = "https://www.bls.gov/schedule"

    def transport(url, **_kwargs):
        if url == failed_url:
            raise TimeoutError
        return {
            "events": [
                {
                    "id": "jobs",
                    "title": "Employment Situation",
                    "scheduled_at": "2026-08-07T08:30:00-04:00",
                }
            ]
        }

    provider = OfficialCalendarProvider(
        sources=(
            _source(source="Bureau of Economic Analysis", url=failed_url, category="MACRO"),
            _source(source="Bureau of Labor Statistics", url=healthy_url, category="MACRO"),
        ),
        transport=transport,
        now=lambda: NOW,
    )

    snapshot = provider.future_two_weeks()

    assert snapshot.status == "DEGRADED"
    assert snapshot.decision == "NO_TRADE"
    assert [event.source_id for event in snapshot.events] == ["jobs"]
    assert [source.status for source in snapshot.sources] == ["DEGRADED", "READY"]
    assert snapshot.sources[0].reason == "REQUEST_TIMEOUT"


def test_sanitized_transport_reason_is_projected_without_raw_exception_text() -> None:
    def transport(_url, **_kwargs):
        raise OfficialCalendarTransportError("HTTP_403")

    provider = OfficialCalendarProvider(
        sources=(
            _source(
                source="Bureau of Labor Statistics",
                url=BLS_URL,
                category="MACRO",
            ),
        ),
        transport=transport,
        now=lambda: NOW,
    )

    snapshot = provider.future_two_weeks()

    assert snapshot.status == "DEGRADED"
    assert snapshot.decision == "NO_TRADE"
    assert snapshot.events == ()
    assert snapshot.sources[0].reason == "HTTP_403"
    assert snapshot.reasons == ("BUREAU_OF_LABOR_STATISTICS:HTTP_403",)


def test_unconfigured_official_calendar_fails_closed() -> None:
    provider = OfficialCalendarProvider(sources=(), transport=None, now=lambda: NOW)

    snapshot = provider.future_two_weeks()

    assert snapshot.status == "DEGRADED"
    assert snapshot.decision == "NO_TRADE"
    assert snapshot.events == ()
    assert snapshot.reasons == ("OFFICIAL_SOURCES_NOT_CONFIGURED",)
    payload = snapshot.as_dict()
    assert payload["decision_authority"] == "SUPPORTING_ONLY"
    assert payload["approval_eligible"] is False
    assert payload["instruction_creation_allowed"] is False


def test_non_https_source_is_rejected_at_configuration_boundary() -> None:
    try:
        OfficialCalendarSource(
            source="Federal Reserve",
            source_url="http://example.test/calendar",
            category="FOMC",
            parser=_parser,
            timezone_name="America/New_York",
        )
    except ValueError as exc:
        assert "HTTPS" in str(exc)
    else:  # pragma: no cover - assertion helper without pytest dependency
        raise AssertionError("non-HTTPS official source should be rejected")
