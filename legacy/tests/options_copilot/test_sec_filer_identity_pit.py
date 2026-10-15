"""SEC filer entries through real refresh, legacy evidence and reopened read models."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
from xml.sax.saxutils import escape

import pytest

from options_copilot.news_runtime import NewsCoordinator
from options_copilot.providers.events import NewsEvent
from options_copilot.providers.sec_current import SecCurrent8KProvider
from options_copilot.storage.evidence import EvidenceStore


NOW = datetime(2026, 9, 9, 0, 0, tzinfo=timezone.utc)


@dataclass(frozen=True)
class _Filing:
    accession: str
    companies: tuple[str, str]
    ciks: tuple[str, str]
    published: datetime

    @property
    def source_id(self) -> str:
        return "urn:tag:sec.gov,2008:accession-number=" + self.accession

    def url(self, index: int) -> str:
        return (
            f"https://www.sec.gov/Archives/edgar/data/{int(self.ciks[index])}/"
            f"{self.accession.replace('-', '')}/{self.accession}-index.htm"
        )


# Public metadata captured in the bounded 2026-09-08T23:53:24Z SEC response.
# Injected ticker resolution below is intentionally synthetic, not production authority.
RENTALS = _Filing(
    "0001193125-26-385238",
    ("UNITED RENTALS, INC.", "UNITED RENTALS NORTH AMERICA INC"),
    ("0001067701", "0001047166"),
    datetime(2026, 9, 8, 20, 23, 50, tzinfo=timezone.utc),
)
DTE = _Filing(
    "0000936340-26-000156", ("DTE ENERGY CO", "DTE Electric Co"),
    ("0000936340", "0000028385"),
    datetime(2026, 9, 8, 20, 16, 54, tzinfo=timezone.utc),
)
SAME_NAME = _Filing(
    RENTALS.accession, ("Synthetic Same Name", "Synthetic Same Name"),
    RENTALS.ciks, RENTALS.published,
)


class _Clock:
    value = NOW

    def __call__(self) -> datetime:
        return self.value


def _feed(filing: _Filing, *, reverse: bool = False) -> str:
    entries = []
    for index in ((1, 0) if reverse else (0, 1)):
        entries.append(
            "<entry>"
            f"<title>{escape(f'8-K - {filing.companies[index]} ({filing.ciks[index]}) (Filer)')}</title>"
            f"<id>{escape(filing.source_id)}</id>"
            f"<updated>{filing.published.isoformat()}</updated>"
            '<category term="8-K"/>'
            f'<link rel="alternate" type="text/html" href="{filing.url(index)}"/>'
            "</entry>"
        )
    return '<feed xmlns="http://www.w3.org/2005/Atom">' + "".join(entries) + "</feed>"


def _legacy_event(filing: _Filing) -> NewsEvent:
    """Construct exactly the pre-filer provider identity, without new helpers."""
    observed = NOW - timedelta(seconds=30)
    return NewsEvent(
        event_id="sec-current:" + hashlib.sha256(filing.source_id.encode("utf-8")).hexdigest(),
        symbol="SPY", source="SEC", headline="8-K - " + filing.companies[0],
        summary=f"Official SEC current 8-K filing metadata for {filing.companies[0]}.",
        url=filing.url(0), published_at=filing.published,
        first_seen_at=observed, ingested_at=observed, observed_at=observed,
        source_rank=1, source_id=filing.source_id,
        provenance=("SEC", "SEC Current 8-K Atom"),
    )


def _provider(filing: _Filing, clock: _Clock, *, reverse: bool):
    calls = []

    def transport(_url: str, **_kwargs: object) -> str:
        calls.append(clock.value)
        clock.value += timedelta(seconds=2)
        return _feed(filing, reverse=reverse)

    provider = SecCurrent8KProvider(
        transport=transport, ticker_resolver=lambda _company, _cik: "SPY", now=clock,
    )
    return provider, calls


def _assert_separate_public_rows(payload: dict[str, object], filing: _Filing) -> None:
    assert payload["count"] == 2
    rows = payload["news"]
    assert {row["source_url"] for row in rows} == {filing.url(0), filing.url(1)}
    assert len({row["id"] for row in rows}) == 2
    assert len({row["story_identity"] for row in rows}) == 2
    assert all(row["status"] != "CONFLICTED" for row in rows)
    assert all(row["evidence_count"] == 1 for row in rows)
    assert all(row["merged_event_count"] == 1 for row in rows)
    assert all(row["provider_story_id"] is None for row in rows)
    assert all(row["classification"]["decision_authority"] == "SUPPORTING_ONLY" for row in rows)
    assert payload["approval_eligible"] is False


@pytest.mark.parametrize("filing", [RENTALS, DTE, SAME_NAME], ids=["rentals", "dte", "same-name"])
@pytest.mark.parametrize("reverse", [False, True], ids=["forward", "reverse"])
def test_distinct_filers_preserve_legacy_row_through_real_refresh_and_reopen(
    tmp_path: Path, filing: _Filing, reverse: bool,
) -> None:
    clock = _Clock()
    provider, calls = _provider(filing, clock, reverse=reverse)
    path = tmp_path / "filer-evidence.sqlite3"
    cadence_path = tmp_path / "cadence.json"
    runtime = NewsCoordinator(
        path, news_providers=(provider,), core_symbols=("SPY",),
        clock=clock, cadence_path=cadence_path,
    )
    try:
        legacy = _legacy_event(filing)
        runtime._append_news(legacy)
        original = runtime.evidence_store.query()[0].as_dict()
        runtime.refresh_once()
        assert len(calls) == 1
        assert provider.health == "READY"
        events = provider.news(("SPY",))
        assert len(events) == 2
        first_filer = next(event for event in events if event.url == filing.url(0))
        assert first_filer.event_id != legacy.event_id
        assert first_filer.identity_key == legacy.identity_key
        assert first_filer.source_id == legacy.source_id
        assert first_filer.content_hash == legacy.content_hash
        rows = runtime.evidence_store.query()
        assert len(rows) == 2
        assert rows[0].as_dict() == original
        assert rows[0].record.payload["event_id"] == legacy.event_id
        assert rows[1].record.first_seen_at == NOW + timedelta(seconds=2)
        assert all(row.record.status == "ACTIVE" for row in rows)
        assert [row.as_dict() for row in runtime.evidence_store.query(
            first_seen_at_or_before=NOW + timedelta(seconds=2) - timedelta(microseconds=1),
        )] == [original]
        _assert_separate_public_rows(runtime.news_payload(), filing)
        retained = [row.as_dict() for row in rows]
        runtime.evidence_store.verify_integrity()

        clock.value = NOW + timedelta(seconds=10)
        assert len(provider.news(("SPY",))) == 2
        assert len(calls) == 1
        clock.value = NOW + timedelta(seconds=90)
        runtime.refresh_once()
        assert len(calls) == 2
        assert [row.as_dict() for row in runtime.evidence_store.query()] == retained
        _assert_separate_public_rows(runtime.news_payload(), filing)
    finally:
        runtime.close()

    with EvidenceStore(path, clock=clock) as store:
        store.verify_integrity()
        assert [row.as_dict() for row in store.query()] == retained

    clock.value = NOW + timedelta(seconds=180)
    restarted_provider, restarted_calls = _provider(filing, clock, reverse=not reverse)
    restarted = NewsCoordinator(
        path, news_providers=(restarted_provider,), core_symbols=("SPY",),
        clock=clock, cadence_path=cadence_path,
    )
    try:
        restarted._finish_local_analysis_restore()
        _assert_separate_public_rows(restarted.news_payload(), filing)
        restarted.refresh_once()
        assert len(restarted_calls) == 1
        assert restarted_provider.health == "READY"
        assert [row.as_dict() for row in restarted.evidence_store.query()] == retained
        _assert_separate_public_rows(restarted.news_payload(), filing)
        restarted.evidence_store.verify_integrity()
    finally:
        restarted.close()


@pytest.mark.parametrize("changed_field", ["summary", "url"])
def test_new_filer_version_preserves_real_legacy_conflict(
    tmp_path: Path, changed_field: str,
) -> None:
    clock = _Clock()
    provider, _calls = _provider(SAME_NAME, clock, reverse=False)
    current = next(event for event in provider.news(("SPY",)) if event.url == SAME_NAME.url(0))
    change = (
        {"summary": "Changed source metadata"}
        if changed_field == "summary"
        else {"url": current.url.replace("-index.htm", "-index.html")}
    )
    current = replace(current, **change, content_hash=None)
    runtime = NewsCoordinator(tmp_path / "legacy-conflict.sqlite3", core_symbols=("SPY",), clock=clock)
    try:
        runtime._append_news(_legacy_event(SAME_NAME))
        original = runtime.evidence_store.query()[0].record
        runtime._append_news(current)
        rows = runtime.evidence_store.query()
        assert len(rows) == 2
        assert rows[0].record == original
        assert rows[0].identity == rows[1].identity
        assert all(row.status == "CONFLICTED" for row in rows)
        runtime._append_news(current)
        assert len(runtime.evidence_store.query()) == 2
        runtime._rebuild_read_model()
        assert runtime.news_payload()["news"]
        assert all(row["status"] == "CONFLICTED" for row in runtime.news_payload()["news"])
        runtime.evidence_store.verify_integrity()
    finally:
        runtime.close()


    restored = NewsCoordinator(
        tmp_path / "legacy-conflict.sqlite3", core_symbols=("SPY",), clock=clock,
    )
    try:
        restored._finish_local_analysis_restore()
        assert restored.news_payload()["news"]
        assert all(row["status"] == "CONFLICTED" for row in restored.news_payload()["news"])
        restored.evidence_store.verify_integrity()
    finally:
        restored.close()


def test_one_filer_with_legitimate_multi_ticker_preferences_keeps_entity_identity(tmp_path: Path) -> None:
    clock = _Clock()

    def make_provider():
        return SecCurrent8KProvider(
            transport=lambda *_args, **_kwargs: _feed(RENTALS),
            ticker_transport=lambda *_args, **_kwargs: json.dumps({
                "fields": ["cik", "name", "ticker", "exchange"],
                "data": [
                    [int(RENTALS.ciks[0]), RENTALS.companies[0], ticker, "NYSE"]
                    for ticker in ("AAA", "BBB")
                ],
            }),
            now=clock,
        )

    events = [
        next(event for event in make_provider().news((ticker,)) if event.symbol == ticker)
        for ticker in ("AAA", "BBB")
    ]
    assert events[0].event_id == events[1].event_id
    assert events[0].identity_key != events[1].identity_key
    runtime = NewsCoordinator(tmp_path / "multi-ticker.sqlite3", core_symbols=("AAA", "BBB"), clock=clock)
    try:
        for event in events:
            runtime._append_news(event)
        rows = runtime.evidence_store.query()
        assert len(rows) == 2
        assert len({row.identity for row in rows}) == 2
        assert all(row.status == "ACTIVE" for row in rows)
        runtime._rebuild_read_model()
        assert runtime.news_payload()["news"]
        assert all(row["status"] != "CONFLICTED" for row in runtime.news_payload()["news"])
        for event in reversed(events):
            runtime._append_news(event)
        assert len(runtime.evidence_store.query()) == 2
        runtime.evidence_store.verify_integrity()
    finally:
        runtime.close()


def test_later_mixed_marker_conflict_is_not_swallowed_by_semantic_dedup(tmp_path: Path) -> None:
    clock = _Clock()
    provider, _calls = _provider(SAME_NAME, clock, reverse=False)
    current = provider.news(("SPY",))[0]

    class _MutableProvider:
        source = "SEC"
        health = "READY"
        events = (current,)

        def news(self, _symbols, *, limit=50):
            return self.events

    mutable = _MutableProvider()
    runtime = NewsCoordinator(
        tmp_path / "mixed-conflict.sqlite3", news_providers=(mutable,),
        core_symbols=("SPY",), clock=clock, cadence_path=tmp_path / "cadence.json",
    )
    try:
        runtime.refresh_once()
        assert runtime.evidence_store.query()[0].status == "ACTIVE"
        before_conflict = runtime.evidence_store.query()[0].as_dict()
        mutable.events = (current, replace(
            current, event_id="tampered-event", lineage_id="tampered-lineage",
            url=current.url.replace("-index.htm", "-index.html"), content_hash=None,
        ))
        clock.value += timedelta(seconds=90)
        conflict_at = clock.value
        runtime.refresh_once()
        rows = runtime.evidence_store.query()
        assert len(rows) == 3
        assert all(row.status == "CONFLICTED" for row in rows)
        assert any(row.record.status == "CONFLICTED" for row in rows if row.identity == rows[0].identity)
        assert [row.as_dict() for row in runtime.evidence_store.query(
            first_seen_at_or_before=conflict_at - timedelta(microseconds=1),
        )] == [before_conflict]
        assert runtime.news_payload()["news"]
        assert all(row["status"] == "CONFLICTED" for row in runtime.news_payload()["news"])
        clock.value += timedelta(seconds=90)
        runtime.refresh_once()
        assert len(runtime.evidence_store.query()) == 3
        runtime.evidence_store.verify_integrity()
    finally:
        runtime.close()

    restored = NewsCoordinator(
        tmp_path / "mixed-conflict.sqlite3", core_symbols=("SPY",), clock=clock,
    )
    try:
        restored._finish_local_analysis_restore()
        assert restored.news_payload()["news"]
        assert all(row["status"] == "CONFLICTED" for row in restored.news_payload()["news"])
        assert [row.as_dict() for row in restored.evidence_store.query(
            first_seen_at_or_before=conflict_at - timedelta(microseconds=1),
        )] == [before_conflict]
        restored.evidence_store.verify_integrity()
    finally:
        restored.close()
