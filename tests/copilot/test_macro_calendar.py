from __future__ import annotations

import sqlite3
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from copilot.analysis.brief import write_brief
from copilot.analysis.tagging import NewsTag
from copilot.news.collect import NewsSources, collect_news
from copilot.news.macro_calendar import (
    MacroEvent,
    category_for,
    fomc_events,
    group_major,
    parse_forexfactory,
    parse_tradingview,
)
from copilot.news.models import NewsItem
from copilot.store import Store

UTC = timezone.utc
NEW_YORK = ZoneInfo("America/New_York")


def event(
    source: str,
    source_id: str,
    moment: datetime,
    title: str,
    category: str,
    importance: int = 3,
) -> MacroEvent:
    return MacroEvent(source, source_id, moment, "US", title, category, importance)


def test_parse_tradingview_us_importance_raw_values_and_utc() -> None:
    parsed = parse_tradingview({"result": [
        {
            "id": "cpi", "title": "Inflation Rate YoY", "country": "US",
            "date": "2026-10-14T12:30:00.000Z", "importance": -1, "period": "Sep",
            "actual": 2.8, "actualRaw": "2.9%", "forecast": 2.7,
            "forecastRaw": "2.7%", "previous": 2.6, "previousRaw": "2.6%",
        },
        {
            "id": "ism", "title": "ISM Manufacturing PMI", "country": "United States",
            "date": "2026-10-15T14:00:00Z", "importance": 0,
        },
        {
            "id": "foreign", "title": "CPI", "country": "CA",
            "date": "2026-10-15T12:30:00Z", "importance": 1,
        },
    ]})
    assert [row.source_id for row in parsed] == ["cpi", "ism"]
    assert parsed[0].importance == 3
    assert parsed[0].actual == "2.9%" and parsed[0].forecast == "2.7%"
    assert parsed[0].previous == "2.6%" and parsed[0].period == "Sep"
    assert parsed[0].event_time == datetime(2026, 10, 14, 12, 30, tzinfo=UTC)
    assert parsed[1].importance == 2


def test_parse_forexfactory_usd_impact_and_offset_time() -> None:
    parsed = parse_forexfactory([
        {"title": "Existing Home Sales", "country": "USD", "date": "2026-10-14T08:30:00-04:00",
         "impact": "High", "forecast": "4.1M", "previous": "4.0M"},
        {"title": "ISM Services PMI", "country": "USD", "date": "2026-10-15T10:00:00-04:00",
         "impact": "Medium", "forecast": "52"},
        {"title": "CPI", "country": "CAD", "date": "2026-10-15T08:30:00-04:00", "impact": "Low"},
    ])
    assert len(parsed) == 2
    assert parsed[0].country == "US" and parsed[0].importance == 3
    assert parsed[0].event_time == datetime(2026, 10, 14, 12, 30, tzinfo=UTC)
    assert parsed[0].forecast == "4.1M" and parsed[0].previous == "4.0M"
    assert parsed[1].importance == 2


@pytest.mark.parametrize(("title", "category"), [
    ("Inflation Rate YoY", "cpi"),
    ("Core Inflation Rate MoM", "cpi"),
    ("PPI MoM", "ppi"),
    ("Retail Sales MoM", "retail_sales"),
    ("Fed Interest Rate Decision", "fomc_decision"),
    ("Core PCE Price Index MoM", "pce"),
    ("GDP Growth Rate QoQ Adv", "gdp"),
    ("Non Farm Payrolls", "nfp"),
    ("Non Farm Payrolls Annual Revision Prel", "nfp"),
    ("U-6 Unemployment Rate", "nfp"),
    ("Nonfarm Payrolls Private", "nfp"),
    ("CPI s.a", "cpi"),
    ("GDP Price Index QoQ Adv", "gdp"),
    ("Core PCE Prices QoQ Adv", "pce"),
    ("Unemployment Rate", "nfp"),
    ("Fed Chair Warsh Speech", "fed_chair"),
    ("Atlanta Fed GDPNow", "other"),
    ("ADP Non-Farm Employment Change", "other"),
    ("Existing Home Sales", "other"),
])
def test_category_rules_cover_real_titles(title: str, category: str) -> None:
    assert category_for(title) == category


def test_major_lookalikes_keep_source_importance() -> None:
    tradingview = parse_tradingview({"result": [{
        "id": "gdpnow", "title": "Atlanta Fed GDPNow", "country": "US",
        "date": "2026-10-14T14:00:00Z", "importance": 0,
    }]})
    forexfactory = parse_forexfactory([{
        "title": "ADP Non-Farm Employment Change", "country": "USD",
        "date": "2026-10-14T08:15:00-04:00", "impact": "Low",
    }])
    assert tradingview[0].category == "other" and tradingview[0].importance == 2
    assert forexfactory[0].category == "other" and forexfactory[0].importance == 1


def test_major_forexfactory_event_is_forced_to_importance_three() -> None:
    parsed = parse_forexfactory([
        {"title": "PPI MoM", "country": "USD", "date": "2026-10-14T08:30:00-04:00", "impact": "Low"},
    ])
    assert parsed[0].importance == 3


def test_fomc_events_use_1400_new_york() -> None:
    item = fomc_events([date(2026, 10, 28)])[0]
    assert item.event_time.astimezone(NEW_YORK).strftime("%Y-%m-%d %H:%M") == "2026-10-28 14:00"
    assert item.source_id == "fomc-2026-10-28"


def test_group_major_merges_cpi_rows_by_date_and_category() -> None:
    rows = [
        event("tradingview", "1", datetime(2026, 10, 14, 12, 30, tzinfo=UTC), "Inflation Rate YoY", "cpi"),
        event("tradingview", "2", datetime(2026, 10, 14, 12, 30, tzinfo=UTC), "Core Inflation Rate MoM", "cpi"),
        event("tradingview", "3", datetime(2026, 10, 14, 14, 0, tzinfo=UTC), "PPI MoM", "ppi"),
    ]
    grouped = group_major(rows, start=date(2026, 10, 14), end=date(2026, 10, 14))
    assert len(grouped) == 2
    assert grouped[0]["category"] == "cpi"
    assert grouped[0]["titles"] == ["Inflation Rate YoY", "Core Inflation Rate MoM"]


def test_replace_macro_events_removes_rescheduled_rows_only_in_source_window(store) -> None:
    start = datetime(2026, 10, 14, tzinfo=UTC)
    end = start + timedelta(days=1)
    store.replace_macro_events("tradingview", start, end, [
        event("tradingview", "move", start + timedelta(hours=12), "CPI", "cpi"),
    ])
    store.replace_macro_events("tradingview", end, end + timedelta(days=1), [
        event("tradingview", "outside", end + timedelta(hours=12), "PPI", "ppi"),
    ])
    store.replace_macro_events("fed", start, end, [
        event("fed", "fed", start + timedelta(hours=18), "FOMC Rate Decision", "fomc_decision"),
    ])
    store.replace_macro_events("tradingview", start, end, [
        event("tradingview", "replacement", start + timedelta(hours=13), "CPI revised", "cpi"),
    ])
    rows = store.macro_events_between(start, end + timedelta(days=1))
    assert {row.source_id for row in rows} == {"replacement", "outside", "fed"}


def test_schema_v3_to_v5_preserves_existing_rows(tmp_path: Path) -> None:
    path = tmp_path / "schema-v3.sqlite3"
    original = Store(path)
    original.add_news([NewsItem("jin10", "kept", datetime(2026, 10, 13, tzinfo=UTC), "保留")])
    original.close()
    with sqlite3.connect(path) as db:
        db.execute("PRAGMA user_version=3")
    upgraded = Store(path)
    try:
        assert upgraded.recent_news(datetime(2026, 1, 1, tzinfo=UTC))[0].title == "保留"
        with sqlite3.connect(path) as db:
            assert db.execute("PRAGMA user_version").fetchone()[0] == 5
            assert db.execute("SELECT name FROM sqlite_master WHERE name='macro_events'").fetchone()
    finally:
        upgraded.close()


class CalendarFake:
    def __init__(self, rows=None, failure: Exception | None = None) -> None:
        self.rows = rows or []
        self.failure = failure
        self.calls = 0

    def fetch(self, start: date, end: date):
        self.calls += 1
        if self.failure:
            raise self.failure
        return self.rows

    def fetch_this_week(self):
        self.calls += 1
        if self.failure:
            raise self.failure
        return self.rows


def test_calendar_refresh_interval_and_fomc_rows(settings, store) -> None:
    now = datetime(2026, 10, 13, 16, tzinfo=UTC)
    tv = CalendarFake([event("tradingview", "tv", now + timedelta(days=1), "CPI", "cpi")])
    sources = NewsSources(nasdaq_earnings=lambda _day: [], tradingview=tv)
    first = {row.source: row for row in collect_news(settings, store, sources, now=now)}
    second = {row.source: row for row in collect_news(settings, store, sources, now=now + timedelta(hours=1))}
    assert first["macro_calendar"].status == "ok" and tv.calls == 1
    assert second["macro_calendar"].detail == "未到刷新时间"
    rows = store.macro_events_between(now - timedelta(days=1), now + timedelta(days=90))
    assert {row.source for row in rows} >= {"tradingview", "fed"}


def test_calendar_tradingview_failure_uses_forexfactory(settings, store) -> None:
    now = datetime(2026, 10, 13, 16, tzinfo=UTC)
    ff = CalendarFake([event("forexfactory", "ff", now + timedelta(days=1), "PPI", "ppi")])
    sources = NewsSources(
        nasdaq_earnings=lambda _day: [],
        tradingview=CalendarFake(failure=RuntimeError("down")),
        forexfactory=ff,
    )
    result = {row.source: row for row in collect_news(settings, store, sources, now=now)}["macro_calendar"]
    assert result.status == "ok"
    assert result.detail == "TradingView 失败，已用 ForexFactory 本周数据"
    assert ff.calls == 1


def test_calendar_both_sources_fail_without_stopping_other_collection(settings, store) -> None:
    now = datetime(2026, 10, 13, 16, tzinfo=UTC)
    sources = NewsSources(
        nasdaq_earnings=lambda _day: [],
        tradingview=CalendarFake(failure=RuntimeError("tv down")),
        forexfactory=CalendarFake(failure=RuntimeError("ff down")),
    )
    results = {row.source: row for row in collect_news(settings, store, sources, now=now)}
    assert results["earnings"].status == "ok"
    assert results["macro_calendar"].status == "failed"
    rows = store.macro_events_between(now - timedelta(days=1), now + timedelta(days=90))
    assert rows and {row.source for row in rows} == {"fed"}


def test_calendar_fomc_store_failure_is_guarded(settings, store, monkeypatch) -> None:
    now = datetime(2026, 10, 13, 16, tzinfo=UTC)
    original = store.replace_macro_events

    def replace_macro_events(source, start, end, rows):
        if source == "fed":
            raise RuntimeError("FOMC write failed")
        return original(source, start, end, rows)

    monkeypatch.setattr(store, "replace_macro_events", replace_macro_events)
    sources = NewsSources(
        nasdaq_earnings=lambda _day: [],
        tradingview=CalendarFake([event(
            "tradingview", "tv", now + timedelta(days=1), "CPI", "cpi"
        )]),
    )
    results = {row.source: row for row in collect_news(settings, store, sources, now=now)}
    assert results["earnings"].status == "ok"
    assert results["macro_calendar"].status == "failed"
    assert results["macro_calendar"].detail == "FOMC write failed"


def test_write_brief_includes_upcoming_events_in_user_content() -> None:
    class RecordingModel:
        user = ""

        def complete_json(self, system: str, user: str, *, max_tokens: int):
            self.user = user
            return {"macro_summary": "摘要"}, {}

    model = RecordingModel()
    item = NewsItem("jin10", "n", datetime(2026, 10, 13, tzinfo=UTC), "新闻", id=1)
    tag = NewsTag(1, "macro", 3, "新闻摘要")
    write_brief(
        model, [(item, tag)], now=datetime(2026, 10, 13, tzinfo=UTC),
        upcoming=[{"date": "2026-10-14", "time_ny": "08:30", "name": "CPI"}],
    )
    assert "未来 7 天重大数据/会议" in model.user
    assert "2026-10-14 08:30（纽约）CPI" in model.user
