"""SEC timing through actual news append, immutable replay and PIT queries."""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from options_copilot.api import OptionsCopilotServices, create_app
from options_copilot.news_runtime import NewsCoordinator
from options_copilot.providers.sec_current import SecCurrent8KProvider
from options_copilot.storage.evidence import EvidenceStore
from tests.options_copilot.test_sec_current_provider import NOW, _entry, _feed


class _Clock:
    def __init__(self) -> None:
        self.value = NOW

    def __call__(self) -> datetime:
        return self.value


def _provider(clock: _Clock, *, include_future: bool = False):
    calls: list[datetime] = []

    def transport(_url: str, **_kwargs: object) -> str:
        calls.append(clock.value)
        clock.value += timedelta(seconds=2)
        entries = [_entry(updated=(NOW + timedelta(seconds=1)).isoformat())]
        if include_future:
            entries.append(_entry(
                identifier="urn:tag:sec.gov,2008:accession-number=0000320193-26-000002",
                updated=(NOW + timedelta(seconds=3)).isoformat(),
            ))
        return _feed(*entries)

    def resolver(_company: str, _cik: str) -> str:
        clock.value += timedelta(seconds=5)
        return "AAPL"

    return SecCurrent8KProvider(
        transport=transport,
        ticker_resolver=resolver,
        now=clock,
        cache_ttl_seconds=60,
    ), calls


def test_sec_materialization_is_the_first_visible_instant_in_real_news_ledger(
    tmp_path: Path,
) -> None:
    clock = _Clock()
    provider, _calls = _provider(clock)
    path = tmp_path / "sec-materialized.sqlite3"
    events = provider.news(("AAPL",))
    assert len(events) == 1
    assert provider.health == "READY"
    materialized_at = NOW + timedelta(seconds=7)
    assert clock.value == materialized_at
    runtime = NewsCoordinator(path, core_symbols=("AAPL",), clock=clock)
    try:
        runtime._append_news(events[0])
        for cutoff in (NOW, NOW + timedelta(seconds=2), materialized_at - timedelta(microseconds=1)):
            assert runtime.evidence_store.query(first_seen_at_or_before=cutoff) == ()
        rows = runtime.evidence_store.query(first_seen_at_or_before=materialized_at)
        assert len(rows) == 1
        record = rows[0].record
        assert record.published_at == NOW + timedelta(seconds=1)
        assert record.first_seen_at == record.ingested_at == record.observed_at == materialized_at
        assert record.symbol == "AAPL"
        assert record.decision_authority == "SUPPORTING_ONLY"
        assert record.payload["source_content_hash"] == events[0].content_hash
        retained = rows[0].as_dict()
        runtime.evidence_store.verify_integrity()
    finally:
        runtime.close()
    with EvidenceStore(path, clock=clock) as reopened:
        reopened.verify_integrity()
        assert reopened.query(first_seen_at_or_before=materialized_at - timedelta(microseconds=1)) == ()
        assert [row.as_dict() for row in reopened.query(first_seen_at_or_before=materialized_at)] == [retained]


def test_cache_hit_and_refetch_do_not_rewrite_existing_news_evidence(tmp_path: Path) -> None:
    clock = _Clock()
    provider, calls = _provider(clock)
    runtime = NewsCoordinator(tmp_path / "sec-retained.sqlite3", core_symbols=("AAPL",), clock=clock)
    try:
        first = provider.news(("AAPL",))
        assert len(first) == 1
        runtime._append_news(first[0])
        original = runtime.evidence_store.query()[0].as_dict()
        clock.value = NOW + timedelta(seconds=10)
        cached = provider.news(("AAPL",))
        assert cached == first
        assert provider.cache_state == "HIT"
        assert len(calls) == 1
        runtime._append_news(cached[0])
        assert [row.as_dict() for row in runtime.evidence_store.query()] == [original]

        clock.value = NOW + timedelta(seconds=80)
        refreshed = provider.news(("AAPL",))
        assert len(refreshed) == 1
        assert len(calls) == 2
        assert refreshed[0].first_seen_at == NOW + timedelta(seconds=87)
        assert refreshed[0].content_hash == first[0].content_hash
        runtime._append_news(refreshed[0])
        assert [row.as_dict() for row in runtime.evidence_store.query()] == [original]
        runtime.evidence_store.verify_integrity()
    finally:
        runtime.close()


def test_post_response_publication_never_enters_news_ledger_after_slow_mapping(
    tmp_path: Path,
) -> None:
    clock = _Clock()
    provider, _calls = _provider(clock, include_future=True)
    events = provider.news(("AAPL",))
    assert len(events) == 1
    assert events[0].published_at == NOW + timedelta(seconds=1)
    assert provider.health == "DEGRADED"
    assert provider.health_reason == "partial_parse"
    runtime = NewsCoordinator(tmp_path / "sec-future.sqlite3", core_symbols=("AAPL",), clock=clock)
    try:
        runtime._append_news(events[0])
        assert runtime.evidence_store.query(first_seen_at_or_before=NOW + timedelta(seconds=3)) == ()
        rows = runtime.evidence_store.query()
        assert len(rows) == 1
        assert rows[0].record.published_at == NOW + timedelta(seconds=1)
        assert rows[0].record.first_seen_at == NOW + timedelta(seconds=7)
        assert rows[0].record.payload["event_id"] == events[0].event_id
        runtime.evidence_store.verify_integrity()
    finally:
        runtime.close()


@pytest.mark.parametrize("reason", ["CLOCK_INVALID", "CLOCK_REGRESSED"])
@pytest.mark.parametrize("layer", ["runtime", "http"])
def test_sec_clock_failure_reason_survives_real_runtime_and_http_projection(
    tmp_path: Path, reason: str, layer: str,
) -> None:
    clock = _Clock()

    def transport(_url: str, **_kwargs: object) -> str:
        clock.value = NOW.replace(tzinfo=None) if reason == "CLOCK_INVALID" else NOW - timedelta(seconds=1)
        return _feed(_entry())

    provider = SecCurrent8KProvider(
        transport=transport, ticker_resolver=lambda _company, _cik: "AAPL", now=clock,
    )
    runtime = NewsCoordinator(
        tmp_path / "sec-clock-reason.sqlite3", news_providers=(provider,),
        core_symbols=("AAPL",), clock=lambda: NOW + timedelta(seconds=10),
        cadence_path=tmp_path / "cadence.json",
    )
    try:
        runtime.refresh_once()
        assert provider.health == "DEGRADED"
        assert runtime.evidence_store.query() == ()
        if layer == "http":
            app = create_app(OptionsCopilotServices(
                health_provider=lambda: {}, bootstrap_provider=lambda: {},
                candidates_provider=lambda: [], positions_provider=lambda: [],
                learning_provider=lambda: {}, news_provider=runtime.news_payload,
            ))
            with TestClient(app) as client:
                response = client.get("/api/news")
                assert response.status_code == 200
                payload = response.json()
        else:
            payload = runtime.news_payload()
        health = next(row for row in payload["source_health"] if row["source"] == "SEC")
        cadence = next(row for row in payload["source_runtime"] if row["source_id"] == "SEC")
        assert health["status"] == "DEGRADED"
        assert health["reason"] == reason
        assert cadence["failure_code"] == reason
        assert payload["approval_eligible"] is False
    finally:
        runtime.close()
