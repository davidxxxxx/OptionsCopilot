from __future__ import annotations

from datetime import datetime, timezone
from io import BytesIO
import ssl
from urllib.error import HTTPError, URLError

import pytest

from options_copilot.news.reaction_specs import reaction_descriptor_from_calendar
from options_copilot.providers.official import OfficialCalendarTransportError
from options_copilot.providers.official_sources import (
    BEA_RELEASE_DATES_URL,
    BEA_SCHEDULE_URL,
    BLS_CALENDAR_ICS_URL,
    BLS_CALENDAR_URL,
    FEDERAL_RESERVE_RELEASE_CALENDAR_URL,
    FEDERAL_RESERVE_FOMC_URL,
    MAXIMUM_OFFICIAL_RESPONSE_BYTES,
    OfficialHttpsTransport,
    _windows_system_proxy_opener,
    build_official_calendar_provider,
    build_official_calendar_sources,
    parse_bea_release_dates_json,
    parse_bea_schedule_html,
    parse_bls_calendar_ics,
    parse_bls_schedule_html,
    parse_federal_reserve_release_calendar_json,
    parse_fomc_2026_html,
)


NOW = datetime(2026, 9, 3, 12, 0, tzinfo=timezone.utc)

FOMC_HTML = """
<!doctype html>
<html><body>
  <div id="2025"><div class="row fomc-meeting">
    <div class="fomc-meeting__month">December</div>
    <div class="fomc-meeting__date">9-10</div>
  </div></div>
  <div id="2026" class="panel-collapse collapse in">
    <div class="row fomc-meeting">
      <div class="fomc-meeting__month"><strong>January</strong></div>
      <div class="fomc-meeting__date">27-28<sup>*</sup></div>
    </div>
    <div class="row fomc-meeting">
      <div class="fomc-meeting__month"><strong>September</strong></div>
      <div class="fomc-meeting__date">15-16</div>
    </div>
  </div>
</body></html>
"""

CURRENT_FOMC_HTML = """
<!doctype html>
<html><body>
  <div class="panel panel-default">
    <div class="panel-heading"><h4><a id="42828">2026 FOMC Meetings</a></h4></div>
    <div class="row fomc-meeting" "="">
      <div class="fomc-meeting__month col-xs-5"><strong>January</strong></div>
      <div class="fomc-meeting__date col-xs-4">27-28</div>
    </div>
    <div class="fomc-meeting--shaded row fomc-meeting" "="">
      <div class="fomc-meeting--shaded fomc-meeting__month"><strong>September</strong></div>
      <div class="fomc-meeting__date">15-16*</div>
    </div>
  </div>
</body></html>
"""

FEDERAL_RESERVE_RELEASE_CALENDAR_JSON = """
{
  "events": [
    {
      "description": "&lt;p&gt;Meeting of July 28-29&lt;/p&gt;",
      "title": "FOMC Minutes",
      "time": "2:00 p.m.",
      "month": "2026-08",
      "days": "19",
      "type": "FOMC"
    },
    {
      "title": "FOMC Meeting",
      "time": "2:00 p.m.",
      "month": "2026-09",
      "days": "16",
      "type": "FOMC"
    },
    {
      "title": "Senior Loan Officer Opinion Survey",
      "time": "2:00 p.m.",
      "month": "2026-08",
      "days": "3",
      "type": "statistical"
    },
    {}
  ],
  "announcement": [{}]
}
"""


def _federal_reserve_json_with_invalid_constant(
    constant: str,
    placement: str,
) -> str:
    if placement == "consumed":
        return FEDERAL_RESERVE_RELEASE_CALENDAR_JSON.replace(
            '"time": "2:00 p.m."',
            f'"time": {constant}',
            1,
        )
    if placement == "ignored_event":
        return FEDERAL_RESERVE_RELEASE_CALENDAR_JSON.replace(
            '"title": "Senior Loan Officer Opinion Survey",',
            f'"ignored": {constant},\n      "title": "Senior Loan Officer Opinion Survey",',
            1,
        )
    if placement == "announcement":
        return FEDERAL_RESERVE_RELEASE_CALENDAR_JSON.replace(
            '"announcement": [{}]',
            f'"announcement": [{{"ignored": {constant}}}]',
            1,
        )
    raise AssertionError(f"unknown placement: {placement}")

BEA_HTML = """
<!doctype html>
<html><body><section id="release-schedule">
  <article class="release-row" data-release-id="bea-gdp-2026-q2-third">
    <a href="/news/2026/gross-domestic-product-2nd-quarter-3rd-estimate">
      Gross Domestic Product, 2nd Quarter (Third Estimate)
    </a>
    <time datetime="2026-09-10T08:30:00-04:00">September 10 at 8:30 a.m.</time>
  </article>
  <article class="release-row" data-release-id="bea-personal-income-2026-09">
    <a href="https://www.bea.gov/news/schedule/personal-income">
      Personal Income and Outlays
    </a>
    <time datetime="2026-09-30">September 30</time>
  </article>
</section></body></html>
"""

CURRENT_BEA_HTML = """
<!doctype html>
<html><body>
  <table class="table table-hover table-striped" id="release-schedule-table">
    <thead><tr>
      <th id="view-field-scheduled-release-date-1-table-column">Year 2026</th>
      <th id="view-field-scheduled-release-subject-table-column">Release</th>
    </tr></thead>
    <tbody>
      <tr class="scheduled-releases-type-press">
        <td class="scheduled-date no-wrap">
          <div class="release-date">August 26</div><small class="text-muted">8:30 AM</small>
        </td>
        <td class="release-title views-field">GDP (Second Estimate), 2nd Quarter 2026</td>
      </tr>
      <tr class="scheduled-releases-type-data">
        <td class="scheduled-date no-wrap"><div class="release-date">October 6</div></td>
        <td class="release-title views-field">Services Supplied Through Affiliates, 2024</td>
      </tr>
      <tr class="scheduled-releases-type-press">
        <td class="scheduled-date no-wrap"><small class="text-muted">To Be Announced 2026</small></td>
        <td class="release-title views-field">Outdoor Recreation Economic Statistics</td>
      </tr>
    </tbody>
  </table>
</body></html>
"""

BEA_RELEASE_DATES_JSON = """
{
  "file_last_updated": "2026-08-01T12:00:00",
  "Gross Domestic Product": {
    "release_dates": [
      "2026-09-10T12:30:00+00:00",
      "2026-10-29T12:30:00+00:00"
    ]
  },
  "Services Supplied Through Affiliates": {
    "release_dates": ["2026-10-06T14:00:00Z"]
  }
}
"""

BLS_ICS = """BEGIN:VCALENDAR\r
VERSION:2.0\r
PRODID:-//U.S. Bureau of Labor Statistics//Release Calendar//EN\r
BEGIN:VEVENT\r
UID:bls-cpi-2026-09\r
DTSTART;TZID=America/New_York:20260910T083000\r
SUMMARY:Consumer Price Index\r
URL:https://www.bls.gov/news.release/cpi.nr0.htm\r
END:VEVENT\r
END:VCALENDAR\r
"""

BLS_HTML = """
<!doctype html>
<html><body>
  <table class="release-list"><thead><tr><th>Date</th><th>Time</th><th>Release</th></tr></thead>
    <tbody>
      <tr class="release-list-odd-row">
        <td class="date-cell"><p>Thursday, September 10, 2026</p></td>
        <td class="time-cell"><p>08:30 AM</p></td>
        <td class="desc-cell"><p><strong>Consumer Price Index</strong> for August 2026</p></td>
      </tr>
    </tbody>
  </table>
</body></html>
"""


def test_fomc_parser_selects_only_2026_and_uses_last_meeting_day_without_time() -> None:
    rows = tuple(parse_fomc_2026_html(FOMC_HTML))

    assert [item["id"] for item in rows] == [
        "fomc-2026-01-27-28",
        "fomc-2026-09-15-16",
    ]
    assert [item["event_date"] for item in rows] == ["2026-01-28", "2026-09-16"]
    assert all("scheduled_at" not in item for item in rows)
    assert all(item["timezone"] == "America/New_York" for item in rows)
    assert all(item["url"] == FEDERAL_RESERVE_FOMC_URL for item in rows)


def test_fomc_parser_accepts_current_panel_heading_structure() -> None:
    rows = tuple(parse_fomc_2026_html(CURRENT_FOMC_HTML))

    assert [item["id"] for item in rows] == [
        "fomc-2026-01-27-28",
        "fomc-2026-09-15-16",
    ]
    assert [item["event_date"] for item in rows] == ["2026-01-28", "2026-09-16"]
    assert all("scheduled_at" not in item for item in rows)


def test_fomc_parser_fails_closed_on_changed_or_partial_2026_markup() -> None:
    with pytest.raises(ValueError, match="2026 FOMC"):
        tuple(parse_fomc_2026_html("<html><body>No matching section</body></html>"))

    partial = FOMC_HTML.replace(
        '<div class="fomc-meeting__date">15-16</div>',
        '<div class="fomc-meeting__date"></div>',
    )
    with pytest.raises(ValueError, match="meeting row"):
        tuple(parse_fomc_2026_html(partial))


def test_federal_reserve_release_calendar_emits_exact_fomc_minutes_only() -> None:
    rows = tuple(
        parse_federal_reserve_release_calendar_json(
            FEDERAL_RESERVE_RELEASE_CALENDAR_JSON
        )
    )

    assert rows == (
        {
            "id": "fomc-minutes-2026-08-19",
            "title": "FOMC Minutes",
            "scheduled_at": "2026-08-19T14:00:00",
            "timezone": "America/New_York",
            "url": FEDERAL_RESERVE_RELEASE_CALENDAR_URL,
        },
    )


def test_federal_reserve_release_calendar_rejects_malformed_or_duplicate_minutes() -> None:
    malformed = FEDERAL_RESERVE_RELEASE_CALENDAR_JSON.replace(
        '"time": "2:00 p.m."',
        '"time": "soon"',
        1,
    )
    with pytest.raises(ValueError, match="FOMC calendar row"):
        tuple(parse_federal_reserve_release_calendar_json(malformed))

    duplicate = FEDERAL_RESERVE_RELEASE_CALENDAR_JSON.replace(
        '    {\n      "title": "FOMC Meeting",',
        '    {\n      "description": "&lt;p&gt;duplicate&lt;/p&gt;",\n'
        '      "title": "FOMC Minutes",',
        1,
    ).replace('"month": "2026-09"', '"month": "2026-08"', 1).replace(
        '"days": "16"',
        '"days": "19"',
        1,
    )
    with pytest.raises(ValueError, match="duplicate FOMC minutes"):
        tuple(parse_federal_reserve_release_calendar_json(duplicate))


@pytest.mark.parametrize("constant", ("NaN", "Infinity", "-Infinity"))
@pytest.mark.parametrize("placement", ("consumed", "ignored_event", "announcement"))
def test_federal_reserve_release_calendar_rejects_nonstandard_json_constants(
    constant: str,
    placement: str,
) -> None:
    payload = _federal_reserve_json_with_invalid_constant(constant, placement)

    with pytest.raises(ValueError, match="release calendar JSON is invalid"):
        tuple(parse_federal_reserve_release_calendar_json(payload))


def test_bea_parser_preserves_explicit_time_and_date_only_precision() -> None:
    rows = tuple(parse_bea_schedule_html(BEA_HTML))

    assert rows[0] == {
        "id": "bea-gdp-2026-q2-third",
        "title": "Gross Domestic Product, 2nd Quarter (Third Estimate)",
        "scheduled_at": "2026-09-10T08:30:00-04:00",
        "timezone": "America/New_York",
        "url": "https://www.bea.gov/news/2026/gross-domestic-product-2nd-quarter-3rd-estimate",
    }
    assert rows[1]["id"] == "bea-personal-income-2026-09"
    assert rows[1]["event_date"] == "2026-09-30"
    assert "scheduled_at" not in rows[1]


def test_bea_parser_accepts_current_table_and_skips_explicit_tba() -> None:
    rows = tuple(parse_bea_schedule_html(CURRENT_BEA_HTML))

    assert len(rows) == 2
    assert rows[0]["title"] == "GDP (Second Estimate), 2nd Quarter 2026"
    assert rows[0]["scheduled_at"] == "2026-08-26T08:30:00"
    assert rows[0]["timezone"] == "America/New_York"
    assert rows[0]["url"] == BEA_SCHEDULE_URL
    assert rows[1]["event_date"] == "2026-10-06"
    assert "scheduled_at" not in rows[1]
    assert all("Outdoor Recreation" not in str(row["title"]) for row in rows)


def test_bea_current_table_parser_fails_closed_on_malformed_visible_time() -> None:
    malformed = CURRENT_BEA_HTML.replace("8:30 AM", "8:30 ET")

    with pytest.raises(ValueError, match="visible time"):
        tuple(parse_bea_schedule_html(malformed))


def test_bea_parser_accepts_explicit_structured_content_in_official_table_row() -> None:
    fixture = """
    <table><tbody><tr id="bea-trade-2026-09">
      <td><a href="/news/schedule/international-trade">U.S. International Trade</a></td>
      <td><span property="dc:date" datatype="xsd:dateTime"
        content="2026-09-11T08:30:00-04:00">September 11, 2026 at 8:30 a.m.</span></td>
    </tr></tbody></table>
    """

    rows = tuple(parse_bea_schedule_html(fixture))

    assert rows[0]["id"] == "bea-trade-2026-09"
    assert rows[0]["scheduled_at"] == "2026-09-11T08:30:00-04:00"


def test_bea_parser_does_not_treat_page_metadata_as_a_release() -> None:
    fixture = """
    <html><head>
      <meta property="article:modified_time" content="2026-09-11T08:30:00-04:00">
    </head><body><a href="/about">About BEA</a></body></html>
    """

    with pytest.raises(ValueError, match="schedule rows"):
        tuple(parse_bea_schedule_html(fixture))


def test_bea_release_dates_json_parser_preserves_exact_utc_timestamps() -> None:
    rows = tuple(parse_bea_release_dates_json(BEA_RELEASE_DATES_JSON))

    assert [row["title"] for row in rows] == [
        "Gross Domestic Product",
        "Services Supplied Through Affiliates",
        "Gross Domestic Product",
    ]
    assert [row["scheduled_at"] for row in rows] == [
        "2026-09-10T12:30:00+00:00",
        "2026-10-06T14:00:00+00:00",
        "2026-10-29T12:30:00+00:00",
    ]
    assert all(row["timezone"] == "America/New_York" for row in rows)
    assert all(row["url"] == BEA_RELEASE_DATES_URL for row in rows)
    assert len({row["id"] for row in rows}) == 3


def test_bea_release_dates_json_parser_requires_audit_for_duplicates_and_rejects_floating_time() -> None:
    duplicate = BEA_RELEASE_DATES_JSON.replace(
        '"2026-10-29T12:30:00+00:00"',
        '"2026-09-10T12:30:00+00:00"',
    )
    with pytest.raises(ValueError, match="audited provider envelope"):
        tuple(parse_bea_release_dates_json(duplicate))

    floating = BEA_RELEASE_DATES_JSON.replace(
        '"2026-09-10T12:30:00+00:00"',
        '"2026-09-10T08:30:00"',
    )
    with pytest.raises(ValueError, match="explicit timezone"):
        tuple(parse_bea_release_dates_json(floating))


def test_bea_parser_fails_closed_instead_of_guessing_missing_schedule() -> None:
    malformed = BEA_HTML.replace(
        '<time datetime="2026-09-30">September 30</time>',
        "<span>Date to be announced</span>",
    )
    with pytest.raises(ValueError, match="release row"):
        tuple(parse_bea_schedule_html(malformed))


def test_bls_ics_parser_requires_explicit_dtstart_and_retains_timezone() -> None:
    rows = tuple(parse_bls_calendar_ics(BLS_ICS))

    assert rows == (
        {
            "id": "bls-cpi-2026-09",
            "title": "Consumer Price Index",
            "scheduled_at": "2026-09-10T08:30:00",
            "timezone": "America/New_York",
            "url": "https://www.bls.gov/news.release/cpi.nr0.htm",
        },
    )
    with pytest.raises(ValueError, match="DTSTART"):
        tuple(parse_bls_calendar_ics(BLS_ICS.replace("DTSTART", "X-DTSTART")))


def test_bls_parser_accepts_current_legacy_us_eastern_alias() -> None:
    rows = tuple(
        parse_bls_calendar_ics(
            BLS_ICS.replace("TZID=America/New_York", "TZID=US-Eastern")
        )
    )

    assert rows
    assert {row["timezone"] for row in rows} == {"America/New_York"}


def test_bls_ics_parser_requires_one_complete_calendar_envelope() -> None:
    bare_event = "\r\n".join(
        line
        for line in BLS_ICS.splitlines()
        if line not in {
            "BEGIN:VCALENDAR",
            "VERSION:2.0",
            "PRODID:-//U.S. Bureau of Labor Statistics//Release Calendar//EN",
            "END:VCALENDAR",
        }
    )
    with pytest.raises(ValueError, match="outside VCALENDAR"):
        tuple(parse_bls_calendar_ics(bare_event))

    with pytest.raises(ValueError, match="missing or unbalanced"):
        tuple(parse_bls_calendar_ics(BLS_ICS.replace("END:VCALENDAR\r\n", "")))

    duplicate_version = BLS_ICS.replace(
        "VERSION:2.0\r\n",
        "VERSION:2.0\r\nVERSION:2.0\r\n",
    )
    with pytest.raises(ValueError, match="duplicate or invalid VERSION"):
        tuple(parse_bls_calendar_ics(duplicate_version))

    outside_event = BLS_ICS + "\r\nBEGIN:VEVENT\r\nEND:VEVENT\r\n"
    with pytest.raises(ValueError, match="outside VCALENDAR"):
        tuple(parse_bls_calendar_ics(outside_event))


def test_bls_ics_parser_requires_version_and_prodid() -> None:
    with pytest.raises(ValueError, match="VERSION:2.0"):
        tuple(parse_bls_calendar_ics(BLS_ICS.replace("VERSION:2.0\r\n", "")))
    with pytest.raises(ValueError, match="PRODID"):
        tuple(
            parse_bls_calendar_ics(
                BLS_ICS.replace(
                    "PRODID:-//U.S. Bureau of Labor Statistics//Release Calendar//EN\r\n",
                    "",
                )
            )
        )


def test_bls_html_parser_requires_explicit_release_table_fields() -> None:
    rows = tuple(parse_bls_schedule_html(BLS_HTML))

    assert len(rows) == 1
    assert rows[0]["title"] == "Consumer Price Index for August 2026"
    assert rows[0]["scheduled_at"] == "2026-09-10T08:30:00"
    assert rows[0]["timezone"] == "America/New_York"
    assert rows[0]["url"] == BLS_CALENDAR_URL
    assert str(rows[0]["id"]).startswith("bls-release-")

    incomplete = BLS_HTML.replace('class="time-cell"', 'class="unknown-cell"')
    with pytest.raises(ValueError, match="row is incomplete"):
        tuple(parse_bls_schedule_html(incomplete))


def test_factory_builds_declared_sources_and_provider_normalizes_fixtures() -> None:
    sources = build_official_calendar_sources()
    assert [(item.source, item.source_url, item.category) for item in sources] == [
        ("Federal Reserve", FEDERAL_RESERVE_FOMC_URL, "FOMC"),
        (
            "Federal Reserve Release Calendar",
            FEDERAL_RESERVE_RELEASE_CALENDAR_URL,
            "FOMC",
        ),
        ("Bureau of Economic Analysis", BEA_RELEASE_DATES_URL, "MACRO"),
        ("Bureau of Labor Statistics", BLS_CALENDAR_URL, "MACRO"),
    ]
    payloads = {
        FEDERAL_RESERVE_FOMC_URL: FOMC_HTML,
        FEDERAL_RESERVE_RELEASE_CALENDAR_URL: FEDERAL_RESERVE_RELEASE_CALENDAR_JSON,
        BEA_RELEASE_DATES_URL: BEA_RELEASE_DATES_JSON,
        BLS_CALENDAR_URL: BLS_HTML,
    }
    provider = build_official_calendar_provider(
        transport=lambda url, **_kwargs: payloads[url],
        now=lambda: NOW,
    )

    snapshot = provider.future_two_weeks()

    assert snapshot.status == "READY"
    assert snapshot.decision == "OBSERVATION_ONLY"
    assert [(item.source, item.source_id) for item in snapshot.events] == [
        (
            "Bureau of Economic Analysis",
            tuple(parse_bea_release_dates_json(BEA_RELEASE_DATES_JSON))[0]["id"],
        ),
        (
            "Bureau of Labor Statistics",
            tuple(parse_bls_schedule_html(BLS_HTML))[0]["id"],
        ),
        ("Federal Reserve", "fomc-2026-09-15-16"),
    ]
    fomc = snapshot.events[-1]
    assert fomc.scheduled_at is None
    assert fomc.event_date.isoformat() == "2026-09-16"
    assert fomc.schedule_precision == "DATE_ONLY"


def test_reaction_schedule_retains_exact_pce_beyond_public_fourteen_day_window() -> None:
    bea = """
    {
      "file_last_updated": "2026-09-01T12:00:00",
      "Personal Income and Outlays": {
        "release_dates": ["2026-09-30T12:30:00+00:00"]
      }
    }
    """
    payloads = {
        FEDERAL_RESERVE_FOMC_URL: FOMC_HTML,
        FEDERAL_RESERVE_RELEASE_CALENDAR_URL: FEDERAL_RESERVE_RELEASE_CALENDAR_JSON,
        BEA_RELEASE_DATES_URL: bea,
        BLS_CALENDAR_URL: BLS_HTML,
    }
    provider = build_official_calendar_provider(
        transport=lambda url, **_kwargs: payloads[url],
        now=lambda: NOW,
    )

    public = provider.future_two_weeks()
    schedule = provider.reaction_schedule()
    pce = next(
        item
        for item in schedule.events
        if item.title == "Personal Income and Outlays"
    )

    assert all(item.event_id != pce.event_id for item in public.events)
    assert pce.scheduled_at == datetime(2026, 9, 30, 12, 30, tzinfo=timezone.utc)
    assert pce.schedule_precision == "EXACT"
    assert schedule.horizon_end > public.window_end
    assert schedule.schedule_hash != public.snapshot_hash


def test_reaction_schedule_adds_provenance_distinct_bea_descriptor_rows() -> None:
    observed = datetime(2026, 8, 20, 12, 0, tzinfo=timezone.utc)
    machine = """
    {
      "file_last_updated": "2026-08-20T12:00:00",
      "Gross Domestic Product": {
        "release_dates": ["2026-08-26T12:30:00+00:00"]
      }
    }
    """
    payloads = {
        FEDERAL_RESERVE_FOMC_URL: FOMC_HTML,
        FEDERAL_RESERVE_RELEASE_CALENDAR_URL: FEDERAL_RESERVE_RELEASE_CALENDAR_JSON,
        BEA_RELEASE_DATES_URL: machine,
        BEA_SCHEDULE_URL: CURRENT_BEA_HTML,
        BLS_CALENDAR_URL: BLS_HTML,
    }
    provider = build_official_calendar_provider(
        transport=lambda url, **_kwargs: payloads[url],
        now=lambda: observed,
    )

    schedule = provider.reaction_schedule()
    gdp = tuple(
        item
        for item in schedule.events
        if item.scheduled_at
        == datetime(2026, 8, 26, 12, 30, tzinfo=timezone.utc)
        and (
            "GDP" in item.title.upper()
            or "GROSS DOMESTIC PRODUCT" in item.title.upper()
        )
    )

    assert schedule.status == "READY"
    assert {item.title for item in gdp} == {
        "Gross Domestic Product",
        "GDP (Second Estimate), 2nd Quarter 2026",
    }
    assert {item.source_url for item in gdp} == {
        BEA_RELEASE_DATES_URL,
        BEA_SCHEDULE_URL,
    }
    assert all(item.decision_authority == "SUPPORTING_ONLY" for item in gdp)
    detail = next(item for item in gdp if item.source_url == BEA_SCHEDULE_URL)
    descriptor = reaction_descriptor_from_calendar(detail)
    assert descriptor.capture_eligible is True
    assert descriptor.reference_period == "2026-Q2"
    assert descriptor.estimate_label == "SECOND"


def test_provider_surfaces_current_fomc_minutes_with_exact_official_time() -> None:
    now = datetime(2026, 8, 19, 12, 0, tzinfo=timezone.utc)
    payloads = {
        FEDERAL_RESERVE_FOMC_URL: FOMC_HTML,
        FEDERAL_RESERVE_RELEASE_CALENDAR_URL: FEDERAL_RESERVE_RELEASE_CALENDAR_JSON,
        BEA_RELEASE_DATES_URL: BEA_RELEASE_DATES_JSON,
        BLS_CALENDAR_URL: BLS_HTML,
    }
    snapshot = build_official_calendar_provider(
        transport=lambda url, **_kwargs: payloads[url],
        now=lambda: now,
    ).future_two_weeks()

    minutes = next(item for item in snapshot.events if item.source_id.startswith("fomc-minutes-"))
    assert minutes.source == "Federal Reserve Release Calendar"
    assert minutes.source_url == FEDERAL_RESERVE_RELEASE_CALENDAR_URL
    assert minutes.scheduled_at == datetime(2026, 8, 19, 18, 0, tzinfo=timezone.utc)
    assert minutes.event_date.isoformat() == "2026-08-19"
    assert minutes.schedule_precision == "EXACT"
    assert minutes.decision_authority == "SUPPORTING_ONLY"


@pytest.mark.parametrize("constant", ("NaN", "Infinity", "-Infinity"))
@pytest.mark.parametrize("placement", ("consumed", "ignored_event", "announcement"))
def test_provider_degrades_on_nonstandard_federal_reserve_json_constants(
    constant: str,
    placement: str,
) -> None:
    payloads = {
        FEDERAL_RESERVE_FOMC_URL: FOMC_HTML,
        FEDERAL_RESERVE_RELEASE_CALENDAR_URL: (
            _federal_reserve_json_with_invalid_constant(constant, placement)
        ),
        BEA_RELEASE_DATES_URL: BEA_RELEASE_DATES_JSON,
        BLS_CALENDAR_URL: BLS_HTML,
    }
    snapshot = build_official_calendar_provider(
        transport=lambda url, **_kwargs: payloads[url],
        now=lambda: datetime(2026, 8, 19, 12, 0, tzinfo=timezone.utc),
    ).future_two_weeks()

    assert snapshot.status == "DEGRADED"
    assert snapshot.decision == "NO_TRADE"
    assert (
        "FEDERAL_RESERVE_RELEASE_CALENDAR:PARSER_FAILED"
        in snapshot.reasons
    )
    assert all(
        item.source != "Federal Reserve Release Calendar"
        for item in snapshot.events
    )


def test_machine_duplicate_fold_is_audited_and_does_not_activate_html_fallback() -> None:
    duplicate = BEA_RELEASE_DATES_JSON.replace(
        '"2026-10-29T12:30:00+00:00"',
        '"2026-09-10T12:30:00+00:00"',
    )
    payloads = {
        FEDERAL_RESERVE_FOMC_URL: FOMC_HTML,
        FEDERAL_RESERVE_RELEASE_CALENDAR_URL: FEDERAL_RESERVE_RELEASE_CALENDAR_JSON,
        BEA_RELEASE_DATES_URL: duplicate,
        BLS_CALENDAR_URL: BLS_HTML,
    }
    calls: list[str] = []

    def transport(url, **_kwargs):
        calls.append(url)
        return payloads[url]

    snapshot = build_official_calendar_provider(
        transport=transport,
        now=lambda: NOW,
    ).future_two_weeks()

    assert snapshot.status == "READY"
    assert BEA_SCHEDULE_URL not in calls
    machine = next(item for item in snapshot.as_dict()["sources"] if item["source_url"] == BEA_RELEASE_DATES_URL)
    assert machine["status"] == "READY"
    assert machine["duplicate_count"] == 1
    assert len(machine["duplicate_record_hashes"]) == 1
    assert machine["warnings"] == ["IDENTICAL_DUPLICATES_FOLDED"]
    assert len(str(machine["audit_hash"])) == 64


def test_machine_parser_failure_activates_provenance_distinct_html_fallback() -> None:
    conflicting_json = """
    {
      "file_last_updated": "2026-08-01T12:00:00",
      "Gross Domestic Product": {"release_dates": ["2026-09-10T12:30:00+00:00"]},
      "Gross Domestic Product": {"release_dates": ["2026-09-11T12:30:00+00:00"]}
    }
    """
    payloads = {
        FEDERAL_RESERVE_FOMC_URL: FOMC_HTML,
        FEDERAL_RESERVE_RELEASE_CALENDAR_URL: FEDERAL_RESERVE_RELEASE_CALENDAR_JSON,
        BEA_RELEASE_DATES_URL: conflicting_json,
        BEA_SCHEDULE_URL: BEA_HTML,
        BLS_CALENDAR_URL: BLS_HTML,
    }
    calls: list[str] = []

    def transport(url, **_kwargs):
        calls.append(url)
        return payloads[url]

    snapshot = build_official_calendar_provider(
        transport=transport,
        now=lambda: NOW,
    ).future_two_weeks()

    assert snapshot.status == "DEGRADED"
    assert snapshot.decision == "NO_TRADE"
    assert "BUREAU_OF_ECONOMIC_ANALYSIS:PARSER_FAILED" in snapshot.reasons
    assert "BEA_FALLBACK_ACTIVE" in snapshot.reasons
    assert calls.count(BEA_RELEASE_DATES_URL) == 1
    assert calls.count(BEA_SCHEDULE_URL) == 1
    machine_health = next(item for item in snapshot.sources if item.source_url == BEA_RELEASE_DATES_URL)
    fallback_health = next(item for item in snapshot.sources if item.source_url == BEA_SCHEDULE_URL)
    assert machine_health.status == "DEGRADED"
    assert machine_health.reason == "PARSER_FAILED"
    assert fallback_health.status == "READY"
    fallback_events = [
        item for item in snapshot.events if item.source_url == BEA_SCHEDULE_URL
    ]
    assert len(fallback_events) == 1
    assert fallback_events[0].source == "Bureau of Economic Analysis HTML Fallback"
    assert fallback_events[0].provenance[0].source_url == BEA_SCHEDULE_URL
    assert all(item.source_url != BEA_RELEASE_DATES_URL for item in snapshot.events)


def test_bls_html_403_uses_official_ics_with_exact_provenance() -> None:
    payloads = {
        FEDERAL_RESERVE_FOMC_URL: FOMC_HTML,
        FEDERAL_RESERVE_RELEASE_CALENDAR_URL: FEDERAL_RESERVE_RELEASE_CALENDAR_JSON,
        BEA_RELEASE_DATES_URL: BEA_RELEASE_DATES_JSON,
        BLS_CALENDAR_ICS_URL: BLS_ICS,
    }

    def transport(url, **_kwargs):
        if url == BLS_CALENDAR_URL:
            raise HTTPError(url, 403, "Forbidden", {}, None)
        return payloads[url]

    provider = build_official_calendar_provider(transport=transport, now=lambda: NOW)
    snapshot = provider.future_two_weeks()

    assert snapshot.status == "READY"
    assert snapshot.decision == "OBSERVATION_ONLY"
    assert snapshot.reasons == ()
    bls_health = next(
        item for item in snapshot.sources if item.source == "Bureau of Labor Statistics"
    )
    assert bls_health.status == "READY"
    assert bls_health.source_url == BLS_CALENDAR_ICS_URL
    assert bls_health.provenance == (
        f"FALLBACK_SELECTED:{BLS_CALENDAR_ICS_URL}",
        f"PRIMARY_UNAVAILABLE:{BLS_CALENDAR_URL}:REQUEST_FAILED",
    )
    bls_events = [
        item for item in snapshot.events if item.source == "Bureau of Labor Statistics"
    ]
    assert len(bls_events) == 1
    assert bls_events[0].source_url == BLS_CALENDAR_ICS_URL
    assert bls_events[0].provenance[0].source_url == BLS_CALENDAR_ICS_URL


def test_bls_primary_and_fallback_failure_remain_degraded_without_fake_rows() -> None:
    payloads = {
        FEDERAL_RESERVE_FOMC_URL: FOMC_HTML,
        FEDERAL_RESERVE_RELEASE_CALENDAR_URL: FEDERAL_RESERVE_RELEASE_CALENDAR_JSON,
        BEA_RELEASE_DATES_URL: BEA_RELEASE_DATES_JSON,
    }

    def transport(url, **_kwargs):
        if url in {BLS_CALENDAR_URL, BLS_CALENDAR_ICS_URL}:
            raise HTTPError(url, 403, "Forbidden", {}, None)
        return payloads[url]

    snapshot = build_official_calendar_provider(
        transport=transport,
        now=lambda: NOW,
    ).future_two_weeks()

    assert snapshot.status == "DEGRADED"
    assert snapshot.decision == "NO_TRADE"
    assert "BUREAU_OF_LABOR_STATISTICS:REQUEST_FAILED" in snapshot.reasons
    assert all(item.source != "Bureau of Labor Statistics" for item in snapshot.events)
    assert {item.source for item in snapshot.events} == {
        "Federal Reserve",
        "Bureau of Economic Analysis",
    }


class _FakeResponse:
    def __init__(self, url: str, body: bytes, content_type: str = "text/html; charset=utf-8"):
        self.status = 200
        self.headers = {
            "Content-Type": content_type,
            "Content-Length": str(len(body)),
            "Content-Encoding": "identity",
        }
        self._url = url
        self._body = BytesIO(body)

    def geturl(self) -> str:
        return self._url

    def read(self, size: int = -1) -> bytes:
        return self._body.read(size)

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None


class _FakeOpener:
    def __init__(self, body: bytes, content_type: str = "text/html; charset=utf-8"):
        self.body = body
        self.content_type = content_type
        self.requests = []

    def open(self, request, *, timeout):
        self.requests.append((request, timeout))
        return _FakeResponse(request.full_url, self.body, self.content_type)


class _SequenceOpener:
    def __init__(self, *outcomes: object) -> None:
        self.outcomes = list(outcomes)
        self.requests: list[tuple[object, float]] = []

    def open(self, request, *, timeout):
        self.requests.append((request, timeout))
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


class _ReadFailureResponse(_FakeResponse):
    def read(self, size: int = -1) -> bytes:
        raise TimeoutError("Cookie: sentinel-read-timeout")


def test_https_transport_is_get_only_allowlisted_bounded_and_credential_free() -> None:
    opener = _FakeOpener(FOMC_HTML.encode("utf-8"))
    transport = OfficialHttpsTransport(opener=opener)

    payload = transport(
        FEDERAL_RESERVE_FOMC_URL,
        headers={"Accept": "text/html"},
        timeout_seconds=3,
    )

    assert payload == FOMC_HTML
    request, timeout = opener.requests[0]
    assert request.get_method() == "GET"
    assert timeout == 3.0
    assert request.get_header("User-agent").startswith("OptionsCopilot/")
    assert request.get_header("Authorization") is None

    json_transport = OfficialHttpsTransport(
        opener=_FakeOpener(
            BEA_RELEASE_DATES_JSON.encode("utf-8"),
            "application/json; charset=utf-8",
        )
    )
    assert json_transport(
        BEA_RELEASE_DATES_URL,
        headers={"Accept": "application/json"},
        timeout_seconds=3,
    ) == BEA_RELEASE_DATES_JSON

    with pytest.raises(ValueError, match="allowlist"):
        transport(
            "https://attacker.example/calendar",
            headers={},
            timeout_seconds=3,
        )
    with pytest.raises(ValueError, match="HTTPS"):
        transport(
            FEDERAL_RESERVE_FOMC_URL.replace("https://", "http://"),
            headers={},
            timeout_seconds=3,
        )
    with pytest.raises(ValueError, match="credential"):
        transport(
            FEDERAL_RESERVE_FOMC_URL,
            headers={"Authorization": "Bearer forbidden"},
            timeout_seconds=3,
        )

    oversized = OfficialHttpsTransport(
        opener=_FakeOpener(b"x" * (MAXIMUM_OFFICIAL_RESPONSE_BYTES + 1))
    )
    with pytest.raises(ValueError, match="size limit"):
        oversized(FEDERAL_RESERVE_FOMC_URL, headers={}, timeout_seconds=3)


def test_https_transport_narrows_accept_to_the_declared_official_representation() -> None:
    cases = (
        (BLS_CALENDAR_URL, "text/html", "text/html; charset=utf-8"),
        (
            BLS_CALENDAR_ICS_URL,
            "text/calendar, text/plain;q=0.9",
            "text/calendar; charset=utf-8",
        ),
        (BEA_RELEASE_DATES_URL, "application/json", "application/json"),
        (
            FEDERAL_RESERVE_RELEASE_CALENDAR_URL,
            "application/json",
            "application/json; charset=utf-8",
        ),
    )
    for url, expected_accept, content_type in cases:
        opener = _FakeOpener(b"bounded official payload", content_type)
        transport = OfficialHttpsTransport(opener=opener)

        transport(
            url,
            headers={
                "Accept": "application/json, text/calendar, text/html;q=0.9"
            },
            timeout_seconds=3,
        )

        request, _ = opener.requests[0]
        assert request.get_header("Accept") == expected_accept

def test_https_transport_retries_one_idempotent_get_then_returns_success() -> None:
    secret = "Authorization: Bearer sentinel-official-secret"
    opener = _SequenceOpener(
        HTTPError(FEDERAL_RESERVE_FOMC_URL, 403, secret, {}, None),
        _FakeResponse(FEDERAL_RESERVE_FOMC_URL, FOMC_HTML.encode("utf-8")),
    )
    transport = OfficialHttpsTransport(opener=opener)

    assert transport(
        FEDERAL_RESERVE_FOMC_URL,
        headers={"Accept": "text/html"},
        timeout_seconds=3,
    ) == FOMC_HTML
    assert len(opener.requests) == 2
    assert all(timeout == 3.0 for _, timeout in opener.requests)


def test_https_transport_backs_off_bounded_bls_edge_rejections() -> None:
    opener = _SequenceOpener(
        HTTPError(BLS_CALENDAR_URL, 403, "Forbidden", {}, None),
        HTTPError(BLS_CALENDAR_URL, 429, "Rate limited", {}, None),
        _FakeResponse(BLS_CALENDAR_URL, BLS_HTML.encode("utf-8")),
    )
    delays: list[float] = []
    transport = OfficialHttpsTransport(opener=opener, sleeper=delays.append)

    assert transport(
        BLS_CALENDAR_URL,
        headers={"Accept": "text/html"},
        timeout_seconds=3,
    ) == BLS_HTML
    assert len(opener.requests) == 3
    assert delays == [1.0, 2.0]


def test_https_transport_rebuilds_default_bls_opener_after_edge_rejection() -> None:
    openers = [
        _SequenceOpener(
            HTTPError(BLS_CALENDAR_URL, 403, "Forbidden", {}, None),
        ),
        _FakeOpener(BLS_HTML.encode("utf-8")),
    ]
    factory_calls: list[int] = []

    def opener_factory() -> object:
        factory_calls.append(len(factory_calls) + 1)
        return openers.pop(0)

    delays: list[float] = []
    transport = OfficialHttpsTransport(
        opener_factory=opener_factory,
        sleeper=delays.append,
    )

    assert transport(
        BLS_CALENDAR_URL,
        headers={"Accept": "text/html"},
        timeout_seconds=3,
    ) == BLS_HTML
    assert factory_calls == [1, 2]
    assert delays == [1.0]


def test_https_transport_uses_bounded_system_proxy_fallback_for_bls_only() -> None:
    direct = _SequenceOpener(
        HTTPError(BLS_CALENDAR_URL, 403, "Forbidden", {}, None),
    )
    system_proxy = _FakeOpener(BLS_HTML.encode("utf-8"))
    proxy_factory_calls: list[int] = []

    def system_proxy_factory() -> object:
        proxy_factory_calls.append(1)
        return system_proxy

    delays: list[float] = []
    transport = OfficialHttpsTransport(
        opener_factory=lambda: direct,
        system_proxy_opener_factory=system_proxy_factory,
        sleeper=delays.append,
    )

    assert transport(
        BLS_CALENDAR_URL,
        headers={"Accept": "text/html"},
        timeout_seconds=3,
    ) == BLS_HTML
    assert proxy_factory_calls == [1]
    assert delays == [1.0]
    request, timeout = system_proxy.requests[0]
    assert request.full_url == BLS_CALENDAR_URL
    assert request.get_method() == "GET"
    assert request.get_header("Authorization") is None
    assert timeout == 3.0


def test_bls_proxy_fallback_is_request_local_before_fed_and_bea() -> None:
    direct = _SequenceOpener(
        HTTPError(BLS_CALENDAR_URL, 403, "Forbidden", {}, None),
        _FakeResponse(FEDERAL_RESERVE_FOMC_URL, FOMC_HTML.encode("utf-8")),
        _FakeResponse(
            BEA_RELEASE_DATES_URL,
            BEA_RELEASE_DATES_JSON.encode("utf-8"),
            "application/json",
        ),
    )
    system_proxy = _FakeOpener(BLS_HTML.encode("utf-8"))
    transport = OfficialHttpsTransport(
        opener_factory=lambda: direct,
        system_proxy_opener_factory=lambda: system_proxy,
        sleeper=lambda _seconds: None,
    )

    assert transport(
        BLS_CALENDAR_URL,
        headers={"Accept": "text/html"},
        timeout_seconds=3,
    ) == BLS_HTML
    assert transport(
        FEDERAL_RESERVE_FOMC_URL,
        headers={"Accept": "text/html"},
        timeout_seconds=3,
    ) == FOMC_HTML
    assert transport(
        BEA_RELEASE_DATES_URL,
        headers={"Accept": "application/json"},
        timeout_seconds=3,
    ) == BEA_RELEASE_DATES_JSON

    assert [request.full_url for request, _ in direct.requests] == [
        BLS_CALENDAR_URL,
        FEDERAL_RESERVE_FOMC_URL,
        BEA_RELEASE_DATES_URL,
    ]
    assert [request.full_url for request, _ in system_proxy.requests] == [
        BLS_CALENDAR_URL
    ]


@pytest.mark.parametrize(
    "registry_proxies",
    (
        {"https": "http://user:secret@proxy.example:8080"},
        {"http": "http://proxy.example:8080"},
        {"https": "socks5://proxy.example:1080"},
    ),
)
def test_windows_system_proxy_fallback_rejects_unsafe_registry_shapes(
    monkeypatch: pytest.MonkeyPatch,
    registry_proxies: dict[str, str],
) -> None:
    monkeypatch.setattr(
        "options_copilot.providers.official_sources.urllib_request.getproxies_registry",
        lambda: registry_proxies,
    )

    assert _windows_system_proxy_opener() is None


def test_https_transport_retries_one_bounded_response_read_failure() -> None:
    opener = _SequenceOpener(
        _ReadFailureResponse(
            FEDERAL_RESERVE_FOMC_URL,
            FOMC_HTML.encode("utf-8"),
        ),
        _FakeResponse(FEDERAL_RESERVE_FOMC_URL, FOMC_HTML.encode("utf-8")),
    )
    transport = OfficialHttpsTransport(opener=opener)

    assert transport(
        FEDERAL_RESERVE_FOMC_URL,
        headers={"Accept": "text/html"},
        timeout_seconds=3,
    ) == FOMC_HTML
    assert len(opener.requests) == 2


def test_https_transport_redacts_response_read_failure_after_only_one_retry() -> None:
    opener = _SequenceOpener(
        _ReadFailureResponse(
            FEDERAL_RESERVE_FOMC_URL,
            FOMC_HTML.encode("utf-8"),
        ),
        _ReadFailureResponse(
            FEDERAL_RESERVE_FOMC_URL,
            FOMC_HTML.encode("utf-8"),
        ),
    )
    transport = OfficialHttpsTransport(opener=opener)

    with pytest.raises(OfficialCalendarTransportError) as raised:
        transport(
            FEDERAL_RESERVE_FOMC_URL,
            headers={"Accept": "text/html"},
            timeout_seconds=3,
        )

    assert raised.value.reason == "REQUEST_TIMEOUT"
    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None
    assert len(opener.requests) == 2
    assert "sentinel" not in repr(raised.value)


@pytest.mark.parametrize(
    ("failure_factory", "expected_reason", "expected_calls"),
    (
        (
            lambda: HTTPError(
                FEDERAL_RESERVE_FOMC_URL,
                403,
                "Authorization: sentinel-403",
                {},
                None,
            ),
            "HTTP_403",
            2,
        ),
        (
            lambda: HTTPError(
                FEDERAL_RESERVE_FOMC_URL,
                429,
                "Cookie: sentinel-429",
                {},
                None,
            ),
            "HTTP_429",
            2,
        ),
        (
            lambda: HTTPError(
                FEDERAL_RESERVE_FOMC_URL,
                503,
                "Proxy-Authorization: sentinel-503",
                {},
                None,
            ),
            "HTTP_5XX",
            2,
        ),
        (
            lambda: HTTPError(
                FEDERAL_RESERVE_FOMC_URL,
                404,
                "Authorization: sentinel-404",
                {},
                None,
            ),
            "HTTP_ERROR",
            1,
        ),
        (lambda: TimeoutError("sentinel-timeout"), "REQUEST_TIMEOUT", 2),
        (lambda: ssl.SSLError("sentinel-tls"), "TLS_ERROR", 2),
        (lambda: URLError(OSError("sentinel-connect")), "CONNECT_ERROR", 2),
    ),
)
def test_https_transport_exposes_only_finite_redacted_failure_codes(
    failure_factory,
    expected_reason: str,
    expected_calls: int,
) -> None:
    opener = _SequenceOpener(*(failure_factory() for _ in range(expected_calls)))
    transport = OfficialHttpsTransport(opener=opener)

    with pytest.raises(OfficialCalendarTransportError) as raised:
        transport(
            FEDERAL_RESERVE_FOMC_URL,
            headers={"Accept": "text/html"},
            timeout_seconds=3,
        )

    assert raised.value.reason == expected_reason
    assert str(raised.value) == expected_reason
    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None
    assert len(opener.requests) == expected_calls
    rendered = repr(raised.value)
    assert "sentinel" not in rendered
    assert FEDERAL_RESERVE_FOMC_URL not in rendered
