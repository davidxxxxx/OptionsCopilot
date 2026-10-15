"""US macro-event calendar models, parsers and public-data clients."""
from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from typing import Any
from zoneinfo import ZoneInfo

from copilot.http import request_json
from copilot.news.models import stable_id

NEW_YORK = ZoneInfo("America/New_York")
TRADINGVIEW_URL = "https://economic-calendar.tradingview.com/events"
FOREXFACTORY_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
TRADINGVIEW_HOSTS = ("economic-calendar.tradingview.com",)
FOREXFACTORY_HOSTS = ("nfs.faireconomy.media",)
_BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) OptionsCopilot/2.0",
    "Origin": "https://www.tradingview.com",
    "Referer": "https://www.tradingview.com/",
}

CATEGORY_RULES = (
    (re.compile(r"Fed Interest Rate Decision|Federal Funds Rate|FOMC Statement|FOMC Press Conference", re.I), "fomc_decision"),
    (re.compile(r"FOMC Minutes", re.I), "fomc_minutes"),
    (re.compile(r"Fed Chair .*?(?:Speech|Testimony)|Powell|Warsh", re.I), "fed_chair"),
    (re.compile(r"CPI|Inflation Rate", re.I), "cpi"),
    (re.compile(r"PPI", re.I), "ppi"),
    (re.compile(r"PCE", re.I), "pce"),
    (re.compile(r"Non[ -]?Farm Payrolls|Non-Farm Employment Change|Unemployment Rate", re.I), "nfp"),
    (re.compile(r"GDP", re.I), "gdp"),
    (re.compile(r"Retail Sales", re.I), "retail_sales"),
    (re.compile(r"ISM", re.I), "ism"),
    (re.compile(r"Jobless Claims|Unemployment Claims", re.I), "jobless_claims"),
)
CATEGORY_NAMES = {
    "fomc_decision": "美联储利率决议",
    "fomc_minutes": "FOMC 会议纪要",
    "fed_chair": "美联储主席讲话",
    "cpi": "CPI",
    "ppi": "PPI",
    "pce": "PCE 物价",
    "nfp": "非农/失业率",
    "gdp": "GDP",
    "retail_sales": "零售销售",
    "ism": "ISM",
    "jobless_claims": "初请失业金",
    "other": "其他",
}
MAJOR_CATEGORIES = frozenset({
    "fomc_decision", "cpi", "ppi", "nfp", "pce", "gdp", "retail_sales",
})


@dataclass(frozen=True)
class MacroEvent:
    source: str
    source_id: str
    event_time: datetime
    country: str
    title: str
    category: str
    importance: int
    period: str = ""
    actual: str | None = None
    forecast: str | None = None
    previous: str | None = None

    def __post_init__(self) -> None:
        if self.event_time.tzinfo is None:
            raise ValueError("event_time must be timezone-aware")
        object.__setattr__(self, "event_time", self.event_time.astimezone(timezone.utc))


def category_for(title: str) -> str:
    if re.search(r"GDPNow|ADP", title, re.I):
        return "other"
    for pattern, category in CATEGORY_RULES:
        if category == "cpi" and re.search(r"PCE|PPI", title, re.I):
            continue
        if pattern.search(title):
            return category
    return "other"


def parse_tradingview(payload: Any) -> list[MacroEvent]:
    rows = payload.get("result") if isinstance(payload, dict) else None
    events = []
    for row in rows or ():
        if not isinstance(row, dict) or str(row.get("country") or "").upper() not in {"US", "UNITED STATES"}:
            continue
        title = str(row.get("title") or row.get("indicator") or "").strip()
        moment = _parse_datetime(row.get("date"))
        if not title or moment is None:
            continue
        category = category_for(title)
        importance = {1: 3, 0: 2, -1: 1}.get(_integer(row.get("importance")), 0)
        if category in MAJOR_CATEGORIES:
            importance = 3
        events.append(MacroEvent(
            source="tradingview",
            source_id=str(row.get("id") or stable_id(title, moment.isoformat())),
            event_time=moment,
            country="US",
            title=title,
            category=category,
            importance=importance,
            period=str(row.get("period") or ""),
            actual=_source_value(row, "actual"),
            forecast=_source_value(row, "forecast"),
            previous=_source_value(row, "previous"),
        ))
    return events


def parse_forexfactory(payload: Any) -> list[MacroEvent]:
    events = []
    for row in payload if isinstance(payload, list) else ():
        if not isinstance(row, dict) or str(row.get("country") or "").upper() != "USD":
            continue
        title = str(row.get("title") or "").strip()
        moment = _parse_datetime(row.get("date"))
        if not title or moment is None:
            continue
        category = category_for(title)
        importance = {"high": 3, "medium": 2, "low": 1}.get(
            str(row.get("impact") or "").lower(), 0
        )
        if category in MAJOR_CATEGORIES:
            importance = 3
        events.append(MacroEvent(
            source="forexfactory",
            source_id=stable_id(title, "USD", moment.isoformat()),
            event_time=moment,
            country="US",
            title=title,
            category=category,
            importance=importance,
            forecast=_text(row.get("forecast")),
            previous=_text(row.get("previous")),
        ))
    return events


def fomc_events(dates: Sequence[date]) -> list[MacroEvent]:
    return [
        MacroEvent(
            source="fed",
            source_id=f"fomc-{day.isoformat()}",
            event_time=datetime.combine(day, time(14), NEW_YORK),
            country="US",
            title="FOMC Rate Decision",
            category="fomc_decision",
            importance=3,
        )
        for day in dates
    ]


def group_major(
    events: Sequence[MacroEvent], *, start: date, end: date
) -> list[dict[str, Any]]:
    groups: dict[tuple[date, str], dict[str, Any]] = {}
    for event in events:
        local = event.event_time.astimezone(NEW_YORK)
        if event.category not in MAJOR_CATEGORIES or not start <= local.date() <= end:
            continue
        key = (local.date(), event.category)
        group = groups.setdefault(key, {
            "date": local.date().isoformat(),
            "category": event.category,
            "name": CATEGORY_NAMES[event.category],
            "time_ny": local.strftime("%H:%M"),
            "titles": [],
        })
        group["time_ny"] = min(group["time_ny"], local.strftime("%H:%M"))
        if event.title not in group["titles"]:
            group["titles"].append(event.title)
    return sorted(groups.values(), key=lambda row: (row["date"], row["time_ny"], row["category"]))


class TradingViewCalendar:
    def __init__(self, timeout: float = 20.0) -> None:
        self.timeout = timeout

    def fetch(self, start: date, end: date) -> list[MacroEvent]:
        payload = request_json(
            "GET",
            TRADINGVIEW_URL,
            allowed_hosts=TRADINGVIEW_HOSTS,
            headers=_BROWSER_HEADERS,
            timeout=self.timeout,
            params={
                "from": f"{start.isoformat()}T00:00:00.000Z",
                "to": f"{end.isoformat()}T23:59:59.999Z",
                "countries": "US",
            },
        )
        return parse_tradingview(payload)


class ForexFactoryCalendar:
    def __init__(self, timeout: float = 20.0) -> None:
        self.timeout = timeout

    def fetch_this_week(self) -> list[MacroEvent]:
        return parse_forexfactory(request_json(
            "GET",
            FOREXFACTORY_URL,
            allowed_hosts=FOREXFACTORY_HOSTS,
            headers={"User-Agent": _BROWSER_HEADERS["User-Agent"]},
            timeout=self.timeout,
        ))


def _parse_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


def _source_value(row: dict[str, Any], name: str) -> str | None:
    raw = row.get(f"{name}Raw")
    return _text(raw if raw not in (None, "") else row.get(name))


def _text(value: Any) -> str | None:
    if value is None or value == "":
        return None
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


def _integer(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


__all__ = [
    "CATEGORY_NAMES",
    "CATEGORY_RULES",
    "ForexFactoryCalendar",
    "MAJOR_CATEGORIES",
    "MacroEvent",
    "TradingViewCalendar",
    "category_for",
    "fomc_events",
    "group_major",
    "parse_forexfactory",
    "parse_tradingview",
]
