from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

from copilot.analysis.deepseek import parse_json_object
from copilot.analysis.ledger import score_news, update_ledger
from copilot.analysis.tagging import Impact, NewsTag, NewsTagger, parse_tags
from copilot.config import ViewsSettings
from copilot.news.models import EarningsEvent, NewsItem

from .conftest import FakeLlm

NOW = datetime(2026, 10, 9, 14, 0, tzinfo=timezone.utc)
SETTINGS = ViewsSettings()
UNIVERSE = ["SPY", "QQQ", "NVDA", "JPM", "TLT"]


def news(news_id: int, title: str, *, hours_ago: float = 0.0) -> NewsItem:
    return NewsItem(source="jin10", source_id=str(news_id), title=title, id=news_id,
                    published_at=NOW - timedelta(hours=hours_ago))


def tag(news_id: int, *impacts: tuple, importance: int = 3) -> NewsTag:
    return NewsTag(news_id=news_id, category="company", importance=importance, summary=f"新闻{news_id}",
                   impacts=tuple(Impact(*impact) for impact in impacts))


def test_parse_tags_validates_and_clamps() -> None:
    payload = {"items": [
        {"id": 1, "category": "earnings", "importance": 7, "summary": "财报超预期",
         "impacts": [{"symbol": "nvda", "direction": 5, "magnitude": 9, "horizon": "months", "vol": "up"},
                     {"symbol": "ZZZZ", "direction": 1, "magnitude": 1},
                     {"symbol": "SPY", "direction": 0, "vol": "none"}]},
        {"id": 2, "category": "made_up", "importance": 0, "impacts": [{"symbol": "SPY", "direction": 1}]},
        {"id": 99, "category": "other", "importance": 1},
    ]}
    tags = {t.news_id: t for t in parse_tags(payload, {1: 101, 2: 102}, UNIVERSE)}
    assert set(tags) == {101, 102}
    first = tags[101]
    assert (first.category, first.importance) == ("earnings", 3)
    assert first.impacts == (Impact("NVDA", 1, 3, "days", "up"),)  # clamped; bad symbol/empty impact dropped
    assert tags[102].category == "other" and tags[102].impacts == ()  # importance 0 carries no impacts


def test_tagger_batches_and_reports_failures() -> None:
    llm = FakeLlm({"降息": {"category": "central_bank", "importance": 3, "summary": "降息",
                           "impacts": [{"symbol": "TLT", "direction": 1, "magnitude": 2, "horizon": "weeks"}]}})
    items = [news(i, "美联储暗示降息" if i == 1 else f"其他快讯{i}") for i in range(1, 6)]
    result = NewsTagger(llm, UNIVERSE, batch_size=2, workers=2).tag(items)
    assert result.calls == 3 and len(result.tags) == 5 and not result.failed_ids
    assert next(t for t in result.tags if t.news_id == 1).impacts[0].symbol == "TLT"
    failing = NewsTagger(FakeLlm(fail=RuntimeError("down")), UNIVERSE, batch_size=2).tag(items)
    assert sorted(failing.failed_ids) == [1, 2, 3, 4, 5] and failing.errors


def test_score_decays_with_age_and_importance() -> None:
    fresh = [(news(1, "a"), tag(1, ("NVDA", 1, 2, "days", "none")))]
    old = [(news(2, "b", hours_ago=72), tag(2, ("NVDA", 1, 2, "days", "none")))]
    minor = [(news(3, "c"), tag(3, ("NVDA", -1, 2, "days", "up"), importance=1))]
    assert score_news(fresh, now=NOW, settings=SETTINGS)["NVDA"].score == pytest.approx(4.0)  # 1 x 2 x 2.0
    assert score_news(old, now=NOW, settings=SETTINGS)["NVDA"].score == pytest.approx(2.0)  # one half-life
    evidence = score_news(minor, now=NOW, settings=SETTINGS)["NVDA"]
    assert evidence.score == pytest.approx(-1.0) and evidence.vol_score == pytest.approx(1.0)


def test_ledger_hysteresis_open_hold_close_and_reverse() -> None:
    def run(previous, score):
        evidence = score_news(
            [(news(1, "x"), tag(1, ("NVDA", 1 if score > 0 else -1, 1, "days", "none")))], now=NOW, settings=SETTINGS,
        ) if score else {}
        if evidence:
            evidence["NVDA"].score = score
        return update_ledger(previous, evidence, earnings=[], core=["SPY"], now=NOW, settings=SETTINGS)

    opened = run({}, 1.6)
    assert opened.book["NVDA"]["direction"] == "bullish" and opened.changes[0]["action"] == "open"
    weak = run(opened.book, 1.0)  # between close (0.75) and open (1.5): the view holds
    assert weak.book["NVDA"]["direction"] == "bullish"
    assert run({}, 1.0).book == {}  # the same score does not open a new view
    closed = run(opened.book, 0.5)
    assert "NVDA" not in closed.book and closed.changes[0]["action"] == "close"
    reversed_ = run(opened.book, -1.8)
    assert reversed_.book["NVDA"]["direction"] == "bearish" and reversed_.changes[0]["action"] == "reverse"
    assert reversed_.views["SPY"].direction == "neutral"  # core symbols always present


def test_ledger_entry_carries_score_sigma_catalyst_and_reasons() -> None:
    tagged = [(news(1, "x"), tag(1, ("JPM", -1, 3, "weeks", "up")))]
    result = update_ledger(
        {}, score_news(tagged, now=NOW, settings=SETTINGS),
        earnings=[EarningsEvent("JPM", date(2026, 10, 13), "bmo")], core=[], now=NOW, settings=SETTINGS,
    )
    entry = result.book["JPM"]
    assert entry["direction"] == "bearish" and entry["volatility"] == "expand"
    assert entry["score"] == pytest.approx(-6.0) and entry["conviction"] == 5
    assert entry["drift_sigma"] == pytest.approx(0.48)
    capped = update_ledger({}, score_news(tagged, now=NOW, settings=SETTINGS), earnings=[], core=[],
                           now=NOW, settings=SETTINGS, max_drift_sigma=0.4)
    assert capped.book["JPM"]["drift_sigma"] == pytest.approx(0.4)
    assert entry["catalyst_date"] == "2026-10-13" and "财报" in entry["catalyst"]
    assert entry["news_ids"] == [1] and "新闻1" in entry["rationale"]
    view = result.views["JPM"]
    assert view.drift_sigma == pytest.approx(0.48) and view.score == pytest.approx(-6.0)


def test_parse_json_object_handles_fences() -> None:
    assert parse_json_object('```json\n{"a": 1}\n```') == {"a": 1}
    assert parse_json_object('前言 {"b": 2} 结尾') == {"b": 2}


def test_deepseek_cost_uses_beijing_peak_hours() -> None:
    from copilot.analysis.deepseek import call_cost_cny, is_peak

    off_peak = datetime(2026, 10, 9, 13, 30, tzinfo=timezone.utc)  # 21:30 Beijing, Friday
    peak = datetime(2026, 10, 9, 2, 0, tzinfo=timezone.utc)  # 10:00 Beijing, Friday
    assert not is_peak(off_peak) and is_peak(peak)
    assert not is_peak(datetime(2026, 10, 10, 2, 0, tzinfo=timezone.utc))  # Saturday
    cost = call_cost_cny("deepseek-v4-flash", cache_hit=0, cache_miss=1_000_000, completion=1_000_000, moment=off_peak)
    assert cost == 5.0  # (2 + 8) / 2


def test_conviction_has_a_dead_band_around_level_boundaries() -> None:
    from copilot.analysis.ledger import _conviction

    assert _conviction(6.0, SETTINGS, None) == 5
    assert _conviction(5.99, SETTINGS, 5) == 5  # decayed by a hair: keep 5 stars
    assert _conviction(5.5, SETTINGS, 5) == 4  # clearly below the boundary
    assert _conviction(4.6, SETTINGS, 3) == 3  # just above the next boundary: keep
    assert _conviction(5.0, SETTINGS, 3) == 4  # clearly above


def test_repeated_reports_have_diminishing_weight() -> None:
    repeats = [(news(i, "金价"), tag(i, ("GLD", 1, 2, "days", "none"), importance=2)) for i in range(1, 11)]
    evidence = score_news(repeats, now=NOW, settings=SETTINGS)["GLD"]
    assert evidence.score == pytest.approx(2.0 * (2 - 0.5 ** 9))  # 2 + 1 + 0.5 + ... < 4
    mixed = repeats[:1] + [(news(20, "宏观"), NewsTag(20, "macro_data", 2, "宏观", (Impact("GLD", 1, 2, "days", "none"),)))]
    assert score_news(mixed, now=NOW, settings=SETTINGS)["GLD"].score == pytest.approx(4.0)  # independent kinds add
