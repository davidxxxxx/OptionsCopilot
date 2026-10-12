from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from io import BytesIO
import json
import traceback
from urllib.request import Request

import pytest

from options_copilot.providers.sec_current import (
    MAXIMUM_SEC_ATOM_RESPONSE_BYTES,
    MAXIMUM_SEC_TICKER_RESPONSE_BYTES,
    SEC_COMPANY_TICKERS_EXCHANGE_URL,
    SEC_CURRENT_8K_ATOM_URL,
    SEC_USER_AGENT,
    SecAtomHttpsTransport,
    SecCikTickerResolver,
    SecCompanyTickersHttpsTransport,
    SecCurrent8KProvider,
    SecProviderRateLimited,
    SecProviderTimeout,
    SecProviderTransportError,
    SecTickerMappingError,
)
from options_copilot.providers.events import NewsAggregator


NOW = datetime(2026, 8, 3, 12, 0, tzinfo=timezone.utc)
BODY_SENTINEL = "full filing body must never be copied"


class _SequenceClock:
    def __init__(self, *values: datetime) -> None:
        self._values = iter(values)

    def __call__(self) -> datetime:
        return next(self._values)


def _ticker_payload(*rows: list[object]) -> str:
    return json.dumps(
        {
            "fields": ["cik", "name", "ticker", "exchange"],
            "data": list(rows),
        }
    )


def _feed(*entries: str) -> str:
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<feed xmlns="http://www.w3.org/2005/Atom">'
        '<title>Latest EDGAR Filings</title>'
        + "".join(entries)
        + "</feed>"
    )


def _entry(
    *,
    company: str = "Apple Inc.",
    cik: str = "0000320193",
    form: str = "8-K",
    updated: str = "2026-08-03T11:55:00Z",
    href: str | None = None,
    identifier: str = "urn:tag:sec.gov,2008:accession-number=0000320193-26-000001",
    summary: str = BODY_SENTINEL,
) -> str:
    selected_href = href or (
        f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/"
        "000032019326000001/aapl-20260803.htm"
    )
    return f"""
      <entry>
        <title>{form} - {company} ({cik}) (Filer)</title>
        <id>{identifier}</id>
        <updated>{updated}</updated>
        <category term="{form}" />
        <link rel="alternate" type="text/html" href="{selected_href}" />
        <summary type="html">{summary}</summary>
      </entry>
    """


def test_provider_fetches_exact_allowlisted_feed_and_builds_metadata_only_news() -> None:
    calls: list[tuple[str, dict[str, str], float]] = []

    def transport(url: str, *, headers, timeout_seconds: float):
        calls.append((url, dict(headers), timeout_seconds))
        return _feed(_entry())

    provider = SecCurrent8KProvider(
        transport=transport,
        now=lambda: NOW,
        ticker_resolver=lambda company, cik: (
            "AAPL" if company == "Apple Inc." and cik == "0000320193" else None
        ),
        timeout_seconds=7.0,
    )

    events = provider.news(("AAPL",), limit=10)

    assert len(events) == 1
    event = events[0]
    assert calls == [
        (
            SEC_CURRENT_8K_ATOM_URL,
            {
                "Accept": "application/atom+xml, application/xml;q=0.9",
                "User-Agent": SEC_USER_AGENT,
            },
            7.0,
        )
    ]
    assert event.symbol == "AAPL"
    assert event.source == "SEC"
    assert event.source_rank == 1
    assert event.published_at == datetime(2026, 8, 3, 11, 55, tzinfo=timezone.utc)
    assert event.first_seen_at == NOW
    assert event.observed_at == NOW
    assert event.url.startswith("https://www.sec.gov/Archives/")
    assert event.decision_authority == "SUPPORTING_ONLY"
    assert BODY_SENTINEL not in event.summary
    assert BODY_SENTINEL not in event.headline
    assert "Apple Inc." in event.headline
    assert provider.health == "READY"
    assert provider.health_reason is None


def test_publication_during_atom_request_uses_response_cutoff_and_materialized_time() -> None:
    request_started_at = NOW
    atom_received_at = NOW + timedelta(seconds=5)
    materialized_at = NOW + timedelta(seconds=8)
    provider = SecCurrent8KProvider(
        transport=lambda *_args, **_kwargs: _feed(
            _entry(updated="2026-08-03T12:00:03Z")
        ),
        now=_SequenceClock(request_started_at, atom_received_at, materialized_at),
        ticker_resolver=None,
    )

    event = provider.news(("AAPL",))[0]

    assert event.published_at == NOW + timedelta(seconds=3)
    assert event.first_seen_at == materialized_at
    assert event.ingested_at == materialized_at
    assert event.observed_at == materialized_at
    assert provider._cache is not None
    assert provider._cache.observed_at == atom_received_at
    assert provider._cache.expires_at == atom_received_at + timedelta(seconds=60)


def test_post_response_publication_stays_rejected_after_slow_ticker_resolution() -> None:
    class Clock:
        value = NOW

        def __call__(self) -> datetime:
            return self.value

    clock = Clock()
    resolver_calls = 0

    def transport(*_args, **_kwargs):
        clock.value = NOW + timedelta(seconds=5)
        return _feed(
            _entry(updated="2026-08-03T12:00:04Z"),
            _entry(
                company="Microsoft Corp",
                cik="0000789019",
                updated="2026-08-03T12:00:10Z",
                href=(
                    "https://www.sec.gov/Archives/edgar/data/789019/"
                    "000078901926000001/msft-20260803.htm"
                ),
                identifier=(
                    "urn:tag:sec.gov,2008:accession-number="
                    "0000789019-26-000001"
                ),
            ),
        )

    def resolver(_company: str, _cik: str) -> str:
        nonlocal resolver_calls
        resolver_calls += 1
        clock.value = NOW + timedelta(seconds=20)
        return "AAPL"

    provider = SecCurrent8KProvider(
        transport=transport,
        now=clock,
        ticker_resolver=resolver,
    )

    events = provider.news(("AAPL",))

    assert len(events) == 1
    assert events[0].published_at == NOW + timedelta(seconds=4)
    assert events[0].first_seen_at == NOW + timedelta(seconds=20)
    assert resolver_calls == 1
    assert provider.health == "DEGRADED"
    assert provider.health_reason == "partial_parse"


def test_delayed_ticker_mapping_availability_is_not_backdated() -> None:
    class Clock:
        value = NOW

        def __call__(self) -> datetime:
            return self.value

    clock = Clock()

    def transport(*_args, **_kwargs):
        clock.value = NOW + timedelta(seconds=2)
        return _feed(_entry(updated="2026-08-03T12:00:01Z"))

    def resolver(_company: str, _cik: str) -> str:
        clock.value = NOW + timedelta(seconds=17)
        return "AAPL"

    provider = SecCurrent8KProvider(
        transport=transport,
        now=clock,
        ticker_resolver=resolver,
    )

    event = provider.news(("AAPL",))[0]

    assert event.symbol == "AAPL"
    assert event.first_seen_at == NOW + timedelta(seconds=17)
    assert event.ingested_at == NOW + timedelta(seconds=17)
    assert event.observed_at == NOW + timedelta(seconds=17)


def test_provider_cache_hit_keeps_original_event_timestamps_and_hash() -> None:
    clock = _SequenceClock(
        NOW,
        NOW + timedelta(seconds=1),
        NOW + timedelta(seconds=3),
        NOW + timedelta(seconds=20),
        NOW + timedelta(seconds=20),
    )
    provider = SecCurrent8KProvider(
        transport=lambda *_args, **_kwargs: _feed(_entry()),
        now=clock,
        ticker_resolver=None,
    )

    first = provider.news(("AAPL",))[0]
    second = provider.news(("AAPL",))[0]

    assert second is first
    assert second.first_seen_at == NOW + timedelta(seconds=3)
    assert second.content_hash == first.content_hash
    assert provider.cache_state == "HIT"
    assert provider.health_snapshot()["asof"] == (
        NOW + timedelta(seconds=3)
    ).isoformat()


def test_cache_hit_finishing_at_atom_expiry_refetches_instead_of_returning_ready() -> None:
    class Clock:
        value = NOW

        def __call__(self) -> datetime:
            return self.value

    clock = Clock()
    calls = 0

    def transport(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        if calls > 1:
            raise SecProviderTimeout("must remain redacted")
        clock.value = NOW + timedelta(seconds=1)
        return _feed(_entry())

    def resolver(_company: str, _cik: str) -> str:
        clock.value = NOW + timedelta(seconds=3)
        return "AAPL"

    provider = SecCurrent8KProvider(
        transport=transport,
        now=clock,
        ticker_resolver=resolver,
        cache_ttl_seconds=60,
    )
    assert len(provider.news(("AAPL",))) == 1
    original_projection = provider._events_for_request

    def delayed_projection(*args, **kwargs):
        events = original_projection(*args, **kwargs)
        clock.value = NOW + timedelta(seconds=61)
        return events

    provider._events_for_request = delayed_projection  # type: ignore[method-assign]
    clock.value = NOW + timedelta(seconds=60)

    assert provider.news(("AAPL",)) == ()
    assert calls == 2
    assert provider.cache_state == "MISS"
    assert provider.health_reason == "request_timeout"


def test_cache_hit_finishing_at_ticker_expiry_refetches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Clock:
        value = NOW

        def __call__(self) -> datetime:
            return self.value

    clock = Clock()
    feed_calls = 0

    def feed_transport(*_args, **_kwargs):
        nonlocal feed_calls
        feed_calls += 1
        if feed_calls > 1:
            raise SecProviderTimeout("must remain redacted")
        clock.value = NOW + timedelta(seconds=1)
        return _feed(_entry())

    def ticker_transport(*_args, **_kwargs):
        clock.value = NOW + timedelta(seconds=3)
        return _ticker_payload([320193, "Apple Inc.", "AAPL", "Nasdaq"])

    provider = SecCurrent8KProvider(
        transport=feed_transport,
        ticker_transport=ticker_transport,
        now=clock,
        cache_ttl_seconds=60,
        ticker_cache_ttl_seconds=30,
    )
    assert len(provider.news(("AAPL",))) == 1
    original_snapshot = SecCikTickerResolver._preference_snapshot

    def delayed_snapshot(self, **kwargs):
        snapshot = original_snapshot(self, **kwargs)
        clock.value = NOW + timedelta(seconds=31)
        return snapshot

    monkeypatch.setattr(
        SecCikTickerResolver,
        "_preference_snapshot",
        delayed_snapshot,
    )
    clock.value = NOW + timedelta(seconds=30)

    assert provider.news(("AAPL",)) == ()
    assert feed_calls == 2
    assert provider.cache_state == "MISS"
    assert provider.health_reason == "request_timeout"


def test_atom_cache_expires_at_exact_response_anchored_boundary() -> None:
    calls = 0

    def transport(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            return _feed(_entry())
        raise SecProviderTimeout("must remain redacted")

    provider = SecCurrent8KProvider(
        transport=transport,
        now=_SequenceClock(
            NOW,
            NOW + timedelta(seconds=5),
            NOW + timedelta(seconds=6),
            NOW + timedelta(seconds=65),
        ),
        ticker_resolver=None,
        cache_ttl_seconds=60,
    )

    assert len(provider.news(("AAPL",))) == 1
    assert provider.news(("AAPL",)) == ()
    assert calls == 2
    assert provider.cache_state == "MISS"
    assert provider.health_reason == "request_timeout"


def test_processing_overrun_does_not_create_fresh_atom_cache() -> None:
    class Clock:
        value = NOW

        def __call__(self) -> datetime:
            return self.value

    clock = Clock()
    calls = 0

    def transport(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        if calls > 1:
            raise SecProviderTimeout("must remain redacted")
        clock.value = NOW + timedelta(seconds=1)
        return _feed(_entry())

    def resolver(_company: str, _cik: str) -> str:
        clock.value = NOW + timedelta(seconds=61)
        return "AAPL"

    provider = SecCurrent8KProvider(
        transport=transport,
        now=clock,
        ticker_resolver=resolver,
        cache_ttl_seconds=60,
    )

    assert len(provider.news(("AAPL",))) == 1
    assert provider._cache is None
    clock.value = NOW + timedelta(seconds=62)
    assert provider.news(("AAPL",)) == ()
    assert calls == 2


@pytest.mark.parametrize(
    "clock",
    [
        _SequenceClock(NOW, NOW - timedelta(seconds=1)),
        _SequenceClock(NOW, NOW + timedelta(seconds=2), NOW + timedelta(seconds=1)),
    ],
    ids=("response_before_request", "materialized_before_response"),
)
def test_clock_phase_regression_fails_closed_without_new_event_or_cache(
    clock: _SequenceClock,
) -> None:
    provider = SecCurrent8KProvider(
        transport=lambda *_args, **_kwargs: _feed(_entry()),
        now=clock,
        ticker_resolver=None,
    )

    assert provider.news(("AAPL",)) == ()
    assert provider._cache is None
    assert provider.health == "DEGRADED"
    assert provider.health_reason == "clock_regressed"
    assert provider.cache_state == "MISS"


@pytest.mark.parametrize("phase", ("request", "response", "materialized"))
def test_naive_provider_clock_fails_closed_at_every_phase(phase: str) -> None:
    naive = NOW.replace(tzinfo=None)
    values = {
        "request": (naive,),
        "response": (NOW, naive),
        "materialized": (NOW, NOW + timedelta(seconds=1), naive),
    }[phase]
    provider = SecCurrent8KProvider(
        transport=lambda *_args, **_kwargs: _feed(_entry()),
        now=_SequenceClock(*values),
        ticker_resolver=None,
    )

    assert provider.news(("AAPL",)) == ()
    assert provider._cache is None
    assert provider.health == "DEGRADED"
    assert provider.health_reason == "clock_invalid"


def test_provider_clock_exception_is_redacted_and_creates_no_cache() -> None:
    calls = 0

    def clock() -> datetime:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("clock credential must not escape")
        return NOW

    provider = SecCurrent8KProvider(
        transport=lambda *_args, **_kwargs: _feed(_entry()),
        now=clock,
        ticker_resolver=None,
    )

    assert provider.news(("AAPL",)) == ()
    assert provider._cache is None
    assert provider.health_reason == "clock_invalid"
    assert "credential" not in repr(provider.health_snapshot())


def test_backward_clock_after_cache_hit_cannot_revive_existing_cache() -> None:
    clock = _SequenceClock(
        NOW,
        NOW + timedelta(seconds=1),
        NOW + timedelta(seconds=2),
        NOW + timedelta(seconds=50),
        NOW + timedelta(seconds=50),
        NOW + timedelta(seconds=40),
    )
    calls = 0

    def transport(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        return _feed(_entry())

    provider = SecCurrent8KProvider(
        transport=transport,
        now=clock,
        ticker_resolver=None,
        cache_ttl_seconds=60,
    )

    first = provider.news(("AAPL",))
    assert provider.news(("AAPL",)) == first
    assert provider.news(("AAPL",)) == ()
    assert calls == 1
    assert provider.health == "DEGRADED"
    assert provider.health_reason == "clock_regressed"
    assert provider.cache_state == "MISS"


def test_response_clock_regression_cannot_reactivate_old_cache() -> None:
    clock = _SequenceClock(
        NOW,
        NOW + timedelta(seconds=1),
        NOW + timedelta(seconds=2),
        NOW + timedelta(seconds=61),
        NOW + timedelta(seconds=59),
        NOW + timedelta(seconds=59),
    )
    calls = 0

    def transport(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        return _feed(_entry())

    provider = SecCurrent8KProvider(
        transport=transport,
        now=clock,
        ticker_resolver=None,
        cache_ttl_seconds=60,
    )

    assert len(provider.news(("AAPL",))) == 1
    assert provider.news(("AAPL",)) == ()
    assert provider.news(("AAPL",)) == ()
    assert calls == 2
    assert provider.health == "DEGRADED"
    assert provider.health_reason == "clock_regressed"
    assert provider.cache_state == "MISS"


def test_success_is_cached_but_expired_cache_is_never_served_as_fresh() -> None:
    class Clock:
        value = NOW

        def __call__(self) -> datetime:
            return self.value

    clock = Clock()
    calls = 0

    def transport(_url: str, **_kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            return _feed(_entry())
        raise SecProviderTimeout("sentinel-token-must-not-escape")

    provider = SecCurrent8KProvider(
        transport=transport,
        now=clock,
        ticker_resolver=None,
        cache_ttl_seconds=60,
    )

    assert len(provider.news(("AAPL",))) == 1
    clock.value = NOW + timedelta(seconds=30)
    assert len(provider.news(("AAPL",))) == 1
    assert calls == 1
    assert provider.cache_state == "HIT"

    clock.value = NOW + timedelta(seconds=61)
    assert provider.news(("AAPL",)) == ()
    assert calls == 2
    assert provider.cache_state == "MISS"
    assert provider.health == "TIMEOUT"
    assert provider.health_reason == "request_timeout"


def test_partial_feed_degrades_but_keeps_valid_official_metadata() -> None:
    provider = SecCurrent8KProvider(
        transport=lambda *_args, **_kwargs: _feed(
            _entry(),
            _entry(identifier="missing-updated", updated="not-a-time"),
        ),
        now=lambda: NOW,
        ticker_resolver=None,
    )

    events = provider.news(("AAPL",))

    assert len(events) == 1
    assert provider.health == "DEGRADED"
    assert provider.health_reason == "partial_parse"


def test_identical_duplicate_source_id_is_folded_without_degrading() -> None:
    entry = _entry()
    provider = SecCurrent8KProvider(
        transport=lambda *_args, **_kwargs: _feed(entry, entry),
        now=lambda: NOW,
        ticker_resolver=None,
    )

    events = provider.news(("AAPL",))

    assert len(events) == 1
    assert provider.health == "READY"
    assert provider.health_reason is None


@pytest.mark.parametrize("reverse", (False, True))
def test_same_accession_distinct_filers_are_distinct_ready_events(
    reverse: bool,
) -> None:
    accession = "urn:tag:sec.gov,2008:accession-number=0001193125-26-385238"
    entries = (
        _entry(
            company="UNITED RENTALS, INC.",
            cik="0001067701",
            href=(
                "https://www.sec.gov/Archives/edgar/data/1067701/"
                "000119312526385238/0001193125-26-385238-index.htm"
            ),
            identifier=accession,
        ),
        _entry(
            company="UNITED RENTALS NORTH AMERICA INC",
            cik="0001047166",
            href=(
                "https://www.sec.gov/Archives/edgar/data/1047166/"
                "000119312526385238/0001193125-26-385238-index.htm"
            ),
            identifier=accession,
        ),
    )
    single_provider = SecCurrent8KProvider(
        transport=lambda *_args, **_kwargs: _feed(entries[0]),
        now=lambda: NOW,
        ticker_resolver=None,
    )
    original_content_hash = single_provider.news(("URI",))[0].content_hash
    ordered = tuple(reversed(entries)) if reverse else entries
    provider = SecCurrent8KProvider(
        transport=lambda *_args, **_kwargs: _feed(*ordered),
        now=lambda: NOW,
        ticker_resolver=None,
    )

    events = provider.news(("URI",))

    assert len(events) == 2
    assert provider.health == "READY"
    assert provider.health_reason is None
    assert {event.source_id for event in events} == {accession}
    assert len({event.event_id for event in events}) == 2
    assert len({event.lineage_id for event in events}) == 2
    assert len({event.evidence_ids for event in events}) == 2
    assert all(event.provider_story_id is None for event in events)
    parent = next(event for event in events if event.url.find("/1067701/") >= 0)
    assert parent.content_hash == original_content_hash


@pytest.mark.parametrize("reverse", (False, True))
def test_second_real_same_accession_filer_pair_is_order_independent(
    reverse: bool,
) -> None:
    accession = "urn:tag:sec.gov,2008:accession-number=0000936340-26-000156"
    entries = (
        _entry(
            company="DTE ENERGY CO",
            cik="0000936340",
            href=(
                "https://www.sec.gov/Archives/edgar/data/936340/"
                "000093634026000156/0000936340-26-000156-index.htm"
            ),
            identifier=accession,
        ),
        _entry(
            company="DTE Electric Co",
            cik="0000028385",
            href=(
                "https://www.sec.gov/Archives/edgar/data/28385/"
                "000093634026000156/0000936340-26-000156-index.htm"
            ),
            identifier=accession,
        ),
    )
    provider = SecCurrent8KProvider(
        transport=lambda *_args, **_kwargs: _feed(
            *(tuple(reversed(entries)) if reverse else entries)
        ),
        now=lambda: NOW,
        ticker_resolver=None,
    )

    events = provider.news(("DTE",))

    assert {event.headline for event in events} == {
        "8-K - DTE ENERGY CO",
        "8-K - DTE Electric Co",
    }
    assert len({event.event_id for event in events}) == 2
    assert provider.health == "READY"


def test_distinct_valid_sec_filers_do_not_conflict_on_same_news_identity() -> None:
    accession = "urn:tag:sec.gov,2008:accession-number=0001193125-26-385238"
    provider = SecCurrent8KProvider(
        transport=lambda *_args, **_kwargs: _feed(
            _entry(
                company="Shared Display Name",
                cik="0001067701",
                href=(
                    "https://www.sec.gov/Archives/edgar/data/1067701/"
                    "000119312526385238/filing-index.htm"
                ),
                identifier=accession,
            ),
            _entry(
                company="Shared Display Name",
                cik="0001047166",
                href=(
                    "https://www.sec.gov/Archives/edgar/data/1047166/"
                    "000119312526385238/filing-index.htm"
                ),
                identifier=accession,
            ),
        ),
        now=lambda: NOW,
        ticker_resolver=lambda _company, _cik: "SHARED",
    )

    events = provider.news(("SHARED",))
    merged = NewsAggregator.merge(events)

    assert len(events) == 2
    assert events[0].identity_key == events[1].identity_key
    assert len(merged) == 2
    assert {event.status for event in merged} == {"ACTIVE"}


@pytest.mark.parametrize(
    ("field", "value"),
    [
        (
            "source_id",
            "urn:tag:sec.gov,2008:accession-number=0000320193-26-000002",
        ),
        ("event_id", "sec-current:" + "0" * 64),
        ("lineage_id", "sec-current:" + "1" * 64),
        ("evidence_ids", ("sec-current:" + "2" * 64,)),
        (
            "url",
            "https://www.sec.gov/Archives/edgar/data/789019/"
            "000032019326000001/aapl-20260803.htm",
        ),
    ],
)
def test_single_inconsistent_sec_identity_marker_keeps_conflict_semantics(
    field: str,
    value: object,
) -> None:
    provider = SecCurrent8KProvider(
        transport=lambda *_args, **_kwargs: _feed(_entry()),
        now=lambda: NOW,
        ticker_resolver=lambda _company, _cik: "AAPL",
    )
    valid = provider.news(("AAPL",))[0]
    conflicting = replace(
        valid,
        **{
            field: value,
            "summary": "Conflicting metadata must remain visible.",
            "content_hash": None,
        },
    )

    merged = NewsAggregator.merge((valid, conflicting))

    assert len(merged) == 2
    assert {event.status for event in merged} == {"CONFLICTED"}


def test_new_sec_identity_mixed_with_legacy_non_sec_keeps_conflict_semantics() -> None:
    provider = SecCurrent8KProvider(
        transport=lambda *_args, **_kwargs: _feed(_entry()),
        now=lambda: NOW,
        ticker_resolver=lambda _company, _cik: "AAPL",
    )
    valid = provider.news(("AAPL",))[0]
    legacy = replace(
        valid,
        source="OTHER",
        event_id="legacy-event",
        lineage_id="legacy-event",
        evidence_ids=("legacy-event",),
        summary="Conflicting legacy metadata remains visible.",
        content_hash=None,
    )

    merged = NewsAggregator.merge((valid, legacy))

    assert len(merged) == 2
    assert {event.status for event in merged} == {"CONFLICTED"}


def test_same_accession_and_filer_with_changed_metadata_remains_partial_parse() -> None:
    provider = SecCurrent8KProvider(
        transport=lambda *_args, **_kwargs: _feed(
            _entry(),
            _entry(
                company="Conflicting Issuer Inc.",
                cik="0000320193",
                href=(
                    "https://www.sec.gov/Archives/edgar/data/320193/"
                    "000032019326000001/conflict-20260803.htm"
                ),
            ),
        ),
        now=lambda: NOW,
        ticker_resolver=None,
    )

    events = provider.news(("AAPL",))

    assert len(events) == 1
    assert events[0].headline == "8-K - Apple Inc."
    assert "Conflicting Issuer" not in repr(events)
    assert provider.health == "DEGRADED"
    assert provider.health_reason == "partial_parse"


def test_same_accession_and_filer_summary_difference_still_folds() -> None:
    provider = SecCurrent8KProvider(
        transport=lambda *_args, **_kwargs: _feed(
            _entry(summary="first ignored filing summary"),
            _entry(summary="second ignored filing summary"),
        ),
        now=lambda: NOW,
        ticker_resolver=None,
    )

    assert len(provider.news(("AAPL",))) == 1
    assert provider.health == "READY"


def test_same_accession_and_filer_url_change_remains_partial_parse() -> None:
    provider = SecCurrent8KProvider(
        transport=lambda *_args, **_kwargs: _feed(
            _entry(),
            _entry(
                href=(
                    "https://www.sec.gov/Archives/edgar/data/320193/"
                    "000032019326000001/changed-index.htm"
                )
            ),
        ),
        now=lambda: NOW,
        ticker_resolver=None,
    )

    assert len(provider.news(("AAPL",))) == 1
    assert provider.health == "DEGRADED"
    assert provider.health_reason == "partial_parse"


def test_same_accession_distinct_title_cik_with_mismatched_archive_cik_is_rejected() -> None:
    accession = "urn:tag:sec.gov,2008:accession-number=0000936340-26-000156"
    provider = SecCurrent8KProvider(
        transport=lambda *_args, **_kwargs: _feed(
            _entry(
                company="DTE ENERGY CO",
                cik="0000936340",
                href=(
                    "https://www.sec.gov/Archives/edgar/data/936340/"
                    "000093634026000156/0000936340-26-000156-index.htm"
                ),
                identifier=accession,
            ),
            _entry(
                company="DTE Electric Co",
                cik="0000028385",
                href=(
                    "https://www.sec.gov/Archives/edgar/data/936340/"
                    "000093634026000156/0000936340-26-000156-index.htm"
                ),
                identifier=(
                    "urn:tag:sec.gov,2008:accession-number="
                    "0000936340-26-000157"
                ),
            ),
        ),
        now=lambda: NOW,
        ticker_resolver=None,
    )

    events = provider.news(("DTE",))

    assert len(events) == 1
    assert events[0].headline == "8-K - DTE ENERGY CO"
    assert provider.health == "DEGRADED"
    assert provider.health_reason == "partial_parse"


@pytest.mark.parametrize(
    ("cik", "href"),
    [
        (
            "0000320193",
            "https://www.sec.gov/Archives/edgar/data/789019/"
            "000032019326000001/aapl.htm",
        ),
        (
            "0000320193",
            "https://www.sec.gov/Archives/edgar/data/320193/"
            "000032019326000002/aapl.htm",
        ),
        (
            "0000320193",
            "https://www.sec.gov/Archives/edgar/data/320193/"
            "000032019326000001/",
        ),
        (
            "0000320193",
            "https://www.sec.gov/Archives/edgar/data/320193/"
            "000032019326000001/..",
        ),
        (
            "0000320193",
            "https://www.sec.gov/Archives/edgar/data/320193/"
            "000032019326000001/%2e%2e",
        ),
        (
            "0000320193",
            "https://www.sec.gov/Archives/edgar/data/320193/"
            "000032019326000001/..%2fescape.htm",
        ),
        (
            "0000320193",
            "https://www.sec.gov/Archives/edgar/data/320193/"
            "000032019326000001/back\\slash.htm",
        ),
        (
            "0000000000",
            "https://www.sec.gov/Archives/edgar/data/0/"
            "000032019326000001/aapl.htm",
        ),
    ],
    ids=(
        "title_cik_mismatch",
        "accession_directory_mismatch",
        "empty_document_path",
        "parent_document_path",
        "encoded_parent_document_path",
        "encoded_separator_document_path",
        "backslash_document_path",
        "zero_cik",
    ),
)
def test_noncanonical_sec_archive_identity_is_rejected(
    cik: str,
    href: str,
) -> None:
    provider = SecCurrent8KProvider(
        transport=lambda *_args, **_kwargs: _feed(_entry(cik=cik, href=href)),
        now=lambda: NOW,
        ticker_resolver=None,
    )

    assert provider.news(("AAPL",)) == ()
    assert provider.health == "MISSING_FIELDS"
    assert provider.health_reason == "no_usable_records"


def test_ticker_resolution_failure_preserves_company_metadata_and_degrades() -> None:
    def broken_resolver(_company: str, _cik: str) -> str | None:
        raise RuntimeError("resolver secret must not escape")

    provider = SecCurrent8KProvider(
        transport=lambda *_args, **_kwargs: _feed(_entry()),
        now=lambda: NOW,
        ticker_resolver=broken_resolver,
    )

    events = provider.news(("AAPL",))

    assert len(events) == 1
    assert events[0].symbol is None
    assert "Apple Inc." in events[0].headline
    assert provider.health == "DEGRADED"
    assert provider.health_reason == "ticker_resolution_failed"


@pytest.mark.parametrize(
    ("failure", "health", "reason"),
    [
        (SecProviderTimeout("secret-bearing timeout"), "TIMEOUT", "request_timeout"),
        (
            SecProviderRateLimited("secret-bearing rate limit"),
            "RATE_LIMITED",
            "rate_limited",
        ),
        (
            RuntimeError("Authorization: Bearer sentinel-secret"),
            "DEGRADED",
            "request_failed",
        ),
    ],
)
def test_transport_failures_are_redacted_and_fail_closed(
    failure: Exception,
    health: str,
    reason: str,
) -> None:
    def transport(*_args, **_kwargs):
        raise failure

    provider = SecCurrent8KProvider(
        transport=transport,
        now=lambda: NOW,
        ticker_resolver=None,
    )

    assert provider.news(("AAPL",)) == ()
    assert provider.health == health
    assert provider.health_reason == reason
    rendered = repr(provider.health_snapshot())
    assert "sentinel-secret" not in rendered
    assert "Authorization" not in rendered
    assert "Bearer" not in rendered


@pytest.mark.parametrize(
    ("payload", "health", "reason"),
    [
        ("<feed>", "BAD_XML", "invalid_atom"),
        (_feed(_entry(href="https://evil.example/filing")), "MISSING_FIELDS", "no_usable_records"),
        (_feed(_entry(updated="2026-08-03T11:55:00")), "MISSING_FIELDS", "no_usable_records"),
        (
            '<!DOCTYPE feed [<!ENTITY x "boom">]>' + _feed(_entry()),
            "BAD_XML",
            "unsafe_xml",
        ),
    ],
)
def test_malformed_or_unsafe_atom_records_degrade_without_fabrication(
    payload: str,
    health: str,
    reason: str,
) -> None:
    provider = SecCurrent8KProvider(
        transport=lambda *_args, **_kwargs: payload,
        now=lambda: NOW,
        ticker_resolver=None,
    )

    assert provider.news(("AAPL",)) == ()
    assert provider.health == health
    assert provider.health_reason == reason


def test_non_8k_entries_are_ignored_without_becoming_news() -> None:
    provider = SecCurrent8KProvider(
        transport=lambda *_args, **_kwargs: _feed(_entry(form="10-K")),
        now=lambda: NOW,
        ticker_resolver=None,
    )

    assert provider.news(("AAPL",)) == ()
    assert provider.health == "READY"
    assert provider.health_reason is None


def test_default_provider_resolver_fetches_sec_mapping_and_assigns_ticker() -> None:
    ticker_calls: list[tuple[str, dict[str, str], float]] = []

    def ticker_transport(url: str, *, headers, timeout_seconds: float):
        ticker_calls.append((url, dict(headers), timeout_seconds))
        return _ticker_payload([320193, "Apple Inc.", "AAPL", "Nasdaq"])

    provider = SecCurrent8KProvider(
        transport=lambda *_args, **_kwargs: _feed(_entry()),
        ticker_transport=ticker_transport,
        now=lambda: NOW,
    )

    events = provider.news(("AAPL",))

    assert len(events) == 1
    assert events[0].symbol == "AAPL"
    assert ticker_calls == [
        (
            SEC_COMPANY_TICKERS_EXCHANGE_URL,
            {"Accept": "application/json", "User-Agent": SEC_USER_AGENT},
            8.0,
        )
    ]
    assert provider.health == "READY"


def test_sec_feed_prioritizes_requested_tickers_but_keeps_market_wide_discovery() -> None:
    provider = SecCurrent8KProvider(
        transport=lambda *_args, **_kwargs: _feed(
            _entry(
                company="Microsoft Corp",
                cik="0000789019",
                updated="2026-08-03T11:59:00Z",
                href=(
                    "https://www.sec.gov/Archives/edgar/data/789019/"
                    "000078901926000001/msft-20260803.htm"
                ),
                identifier="urn:tag:sec.gov,2008:accession-number=0000789019-26-000001",
            ),
            _entry(),
        ),
        now=lambda: NOW,
        ticker_resolver=lambda _company, cik: {
            "0000320193": "AAPL",
            "0000789019": "MSFT",
        }.get(cik),
    )

    events = provider.news(("AAPL",), limit=2)

    assert [event.symbol for event in events] == ["AAPL", "MSFT"]


def test_cik_ticker_resolver_normalizes_to_ten_digits_and_caches() -> None:
    calls = 0

    def transport(_url: str, **_kwargs):
        nonlocal calls
        calls += 1
        return _ticker_payload(
            [320193, "Apple Inc.", "aapl", "Nasdaq"],
            [789019, "Microsoft Corp", "MSFT", "Nasdaq"],
            [1067983, "Berkshire Hathaway Inc.", "BRK-B", "NYSE"],
        )

    resolver = SecCikTickerResolver(
        transport=transport,
        now=lambda: NOW,
        cache_ttl_seconds=3600,
    )

    assert resolver("Apple Inc.", "320193") == "AAPL"
    assert resolver("Microsoft Corp", "0000789019") == "MSFT"
    assert resolver("Berkshire Hathaway Inc.", "1067983") == "BRK.B"
    assert resolver("Unknown", "0000000001") is None
    assert calls == 1
    assert resolver.cache_state == "HIT"
    assert resolver.health == "READY"
    assert resolver.health_reason is None


def test_multi_ticker_issuer_uses_canonical_symbol_and_redacted_stats() -> None:
    company_sentinel = "issuer-name-must-not-be-retained"
    cik_sentinel = "0000320193"
    resolver = SecCikTickerResolver(
        transport=lambda *_args, **_kwargs: _ticker_payload(
            [320193, company_sentinel, "ZZZ-B", "NYSE"],
            [320193, company_sentinel, "ZZZ-A", "NYSE"],
            [789019, "Second Issuer", "BASEA", "Nasdaq"],
            [789019, "Second Issuer", "BASE", "Nasdaq"],
        ),
        now=lambda: NOW,
    )

    assert resolver(company_sentinel, cik_sentinel) == "ZZZ.A"
    assert resolver("Second Issuer", "0000789019") == "BASE"
    stats = resolver.resolution_stats()

    assert resolver.health == "READY"
    assert resolver.health_reason is None
    assert stats["attempt_count"] == 2
    assert stats["resolved_count"] == 2
    assert stats["missing_count"] == 0
    assert stats["failure_count"] == 0
    assert stats["cache_hit_count"] == 1
    assert stats["cache_miss_count"] == 1
    assert stats["refresh_count"] == 1
    assert stats["generation"] == 1
    assert stats["current_mapping_canonicalized_cik_count"] == 2
    assert stats["current_mapping_ambiguous_cik_count"] == 0
    assert stats["failure_counts"] == {}
    rendered = json.dumps(stats, sort_keys=True)
    assert company_sentinel not in rendered
    assert cik_sentinel not in rendered
    assert "ZZZ" not in rendered
    assert "BASE" not in rendered


def test_resolution_failure_stats_are_allowlisted_and_secret_free() -> None:
    def failed_transport(*_args, **_kwargs):
        raise RuntimeError("Authorization: Bearer ticker-resolver-secret")

    resolver = SecCikTickerResolver(
        transport=failed_transport,
        now=lambda: NOW,
    )

    with pytest.raises(SecTickerMappingError):
        resolver("Company metadata must not escape", "0000320193")

    stats = resolver.resolution_stats()
    assert stats["attempt_count"] == 1
    assert stats["resolved_count"] == 0
    assert stats["missing_count"] == 0
    assert stats["failure_count"] == 1
    assert stats["refresh_count"] == 1
    assert stats["generation"] == 0
    assert stats["current_mapping_canonicalized_cik_count"] == 0
    assert stats["current_mapping_ambiguous_cik_count"] == 0
    assert stats["failure_counts"] == {"request_failed": 1}
    rendered = json.dumps(stats, sort_keys=True)
    assert "Authorization" not in rendered
    assert "Bearer" not in rendered
    assert "ticker-resolver-secret" not in rendered
    assert "Company metadata" not in rendered
    assert "0000320193" not in rendered


def test_provider_exposes_queryable_ticker_resolution_stats() -> None:
    provider = SecCurrent8KProvider(
        transport=lambda *_args, **_kwargs: _feed(_entry()),
        ticker_transport=lambda *_args, **_kwargs: _ticker_payload(
            [320193, "Apple Inc.", "AAPL-B", "Nasdaq"],
            [320193, "Apple Inc.", "AAPL-A", "Nasdaq"],
        ),
        now=lambda: NOW,
    )

    events = provider.news(("AAPL.A",))
    stats = provider.ticker_resolution_stats()

    assert events[0].symbol == "AAPL.A"
    assert provider.health == "READY"
    assert provider.health_reason is None
    assert stats["attempt_count"] == 1
    assert stats["resolved_count"] == 1
    assert stats["refresh_count"] == 1
    assert stats["generation"] == 1
    assert stats["current_mapping_canonicalized_cik_count"] == 1
    assert stats["failure_counts"] == {}


@pytest.mark.parametrize(
    ("company", "cik", "tickers", "requested", "expected"),
    [
        (
            "Berkshire Hathaway Inc.",
            "0001067983",
            ("BRK-A", "BRK-B"),
            "BRK.B",
            "BRK.B",
        ),
        (
            "Apple Inc.",
            "0000320193",
            ("AAPL", "APPL"),
            "AAPL",
            "AAPL",
        ),
    ],
)
def test_default_provider_prefers_requested_multi_ticker_candidate(
    company: str,
    cik: str,
    tickers: tuple[str, str],
    requested: str,
    expected: str,
) -> None:
    provider = SecCurrent8KProvider(
        transport=lambda *_args, **_kwargs: _feed(
            _entry(company=company, cik=cik)
        ),
        ticker_transport=lambda *_args, **_kwargs: _ticker_payload(
            *(
                [int(cik), company, ticker, "NYSE"]
                for ticker in tickers
            ),
        ),
        now=lambda: NOW,
    )

    events = provider.news((requested,))

    assert events[0].symbol == expected
    assert provider.health == "READY"
    assert provider.health_reason is None


def test_cached_provider_reapplies_current_requested_multi_ticker_preference() -> None:
    feed_calls = 0

    def feed_transport(*_args, **_kwargs):
        nonlocal feed_calls
        feed_calls += 1
        return _feed(
            _entry(
                company="Berkshire Hathaway Inc.",
                cik="0001067983",
            )
        )

    provider = SecCurrent8KProvider(
        transport=feed_transport,
        ticker_transport=lambda *_args, **_kwargs: _ticker_payload(
            [1067983, "Berkshire Hathaway Inc.", "BRK-A", "NYSE"],
            [1067983, "Berkshire Hathaway Inc.", "BRK-B", "NYSE"],
        ),
        now=lambda: NOW,
    )

    first = provider.news(("BRK.A",))[0]
    cached = provider.news(("BRK.B",))[0]
    fresh = SecCurrent8KProvider(
        transport=lambda *_args, **_kwargs: _feed(
            _entry(
                company="Berkshire Hathaway Inc.",
                cik="0001067983",
            )
        ),
        ticker_transport=lambda *_args, **_kwargs: _ticker_payload(
            [1067983, "Berkshire Hathaway Inc.", "BRK-A", "NYSE"],
            [1067983, "Berkshire Hathaway Inc.", "BRK-B", "NYSE"],
        ),
        now=lambda: NOW,
    ).news(("BRK.B",))[0]

    assert first.symbol == "BRK.A"
    assert cached.symbol == "BRK.B"
    assert cached.entity_id == fresh.entity_id == "BRK.B"
    assert cached.identity_key == fresh.identity_key
    assert cached.content_hash == fresh.content_hash
    assert cached.event_id == first.event_id
    assert cached.lineage_id == first.lineage_id
    assert cached.evidence_ids == first.evidence_ids
    assert cached.first_seen_at == first.first_seen_at
    assert cached.ingested_at == first.ingested_at
    assert cached.observed_at == first.observed_at
    assert feed_calls == 1
    assert provider.cache_state == "HIT"
    assert provider.ticker_resolution_stats()["attempt_count"] == 1


def test_provider_cache_is_bound_to_resolver_generation_before_symbol_relabel() -> None:
    class Clock:
        value = NOW

        def __call__(self) -> datetime:
            return self.value

    clock = Clock()
    feed_calls = 0
    ticker_calls = 0

    def feed_transport(*_args, **_kwargs):
        nonlocal feed_calls
        feed_calls += 1
        company = "Old Issuer" if feed_calls == 1 else "New Issuer"
        return _feed(_entry(company=company, cik="0001067983"))

    def ticker_transport(*_args, **_kwargs):
        nonlocal ticker_calls
        ticker_calls += 1
        ticker = "OLD" if ticker_calls == 1 else "NEW"
        return _ticker_payload([1067983, f"{ticker} Issuer", ticker, "NYSE"])

    provider = SecCurrent8KProvider(
        transport=feed_transport,
        ticker_transport=ticker_transport,
        now=clock,
        cache_ttl_seconds=60,
        ticker_cache_ttl_seconds=30,
    )

    first = provider.news(("OLD",))[0]
    assert first.symbol == "OLD"
    assert "Old Issuer" in first.headline

    clock.value = NOW + timedelta(seconds=31)
    resolver = provider._ticker_resolver
    assert type(resolver) is SecCikTickerResolver
    assert resolver("New Issuer", "0001067983") == "NEW"

    second = provider.news(("NEW",))[0]

    assert second.symbol == "NEW"
    assert "New Issuer" in second.headline
    assert "Old Issuer" not in second.headline
    assert feed_calls == 2
    assert ticker_calls == 2
    assert provider.cache_state == "MISS"


def test_provider_cache_does_not_use_expired_default_resolver_mapping() -> None:
    class Clock:
        value = NOW

        def __call__(self) -> datetime:
            return self.value

    clock = Clock()
    feed_calls = 0
    ticker_calls = 0

    def feed_transport(*_args, **_kwargs):
        nonlocal feed_calls
        feed_calls += 1
        return _feed(_entry())

    def ticker_transport(*_args, **_kwargs):
        nonlocal ticker_calls
        ticker_calls += 1
        return _ticker_payload([320193, "Apple Inc.", "AAPL", "Nasdaq"])

    provider = SecCurrent8KProvider(
        transport=feed_transport,
        ticker_transport=ticker_transport,
        now=clock,
        cache_ttl_seconds=60,
        ticker_cache_ttl_seconds=30,
    )

    assert provider.news(("AAPL",))[0].symbol == "AAPL"
    clock.value = NOW + timedelta(seconds=31)
    assert provider.news(("AAPL",))[0].symbol == "AAPL"

    assert feed_calls == 2
    assert ticker_calls == 2
    assert provider.cache_state == "MISS"
    stats = provider.ticker_resolution_stats()
    assert stats["refresh_count"] == 2
    assert stats["generation"] == 2


def test_injected_resolver_subclass_uses_only_public_two_argument_contract() -> None:
    class SecretResolver(SecCikTickerResolver):
        def __init__(self) -> None:
            super().__init__(
                transport=lambda *_args, **_kwargs: _ticker_payload(
                    [1067983, "Berkshire Hathaway Inc.", "BRK-A", "NYSE"]
                ),
                now=lambda: NOW,
            )
            self.calls: list[tuple[str, str]] = []
            self.stats_calls = 0

        def __call__(self, company: str, cik: str) -> str:
            self.calls.append((company, cik))
            return "BRK.A"

        def resolve(self, *_args, **_kwargs):
            raise AssertionError("injected resolver resolve must not be called")

        def prefer_cached_symbol(self, *_args, **_kwargs):
            raise AssertionError("injected resolver preference must not be called")

        def resolution_stats(self) -> dict[str, object]:
            self.stats_calls += 1
            return {"secret": "resolver-secret-must-not-escape"}

    resolver = SecretResolver()
    provider = SecCurrent8KProvider(
        transport=lambda *_args, **_kwargs: _feed(
            _entry(company="Berkshire Hathaway Inc.", cik="0001067983")
        ),
        ticker_resolver=resolver,
        now=lambda: NOW,
    )

    assert provider.news(("BRK.B",))[0].symbol == "BRK.A"
    assert provider.news(("BRK.B",))[0].symbol == "BRK.A"
    health = provider.health_snapshot()

    assert resolver.calls == [("Berkshire Hathaway Inc.", "0001067983")]
    assert resolver.stats_calls == 0
    assert health["ticker_resolution_stats"] == {
        "schema": "options_copilot.sec_ticker_resolution_stats",
        "version": 1,
        "asof": None,
        "attempt_count": 0,
        "resolved_count": 0,
        "missing_count": 0,
        "failure_count": 0,
        "cache_hit_count": 0,
        "cache_miss_count": 0,
        "refresh_count": 0,
        "generation": 0,
        "current_mapping_canonicalized_cik_count": 0,
        "current_mapping_ambiguous_cik_count": 0,
        "failure_counts": {},
    }
    assert "resolver-secret-must-not-escape" not in json.dumps(health)


def test_requested_symbol_preference_does_not_change_custom_resolver_contract() -> None:
    calls: list[tuple[str, str]] = []

    def custom_resolver(company: str, cik: str) -> str:
        calls.append((company, cik))
        return "BRK.A"

    provider = SecCurrent8KProvider(
        transport=lambda *_args, **_kwargs: _feed(
            _entry(company="Berkshire Hathaway Inc.", cik="0001067983")
        ),
        ticker_resolver=custom_resolver,
        now=lambda: NOW,
    )

    events = provider.news(("BRK.B",))

    assert events[0].symbol == "BRK.A"
    assert calls == [("Berkshire Hathaway Inc.", "0001067983")]


def test_expired_ticker_cache_is_not_served_after_refresh_failure() -> None:
    class Clock:
        value = NOW

        def __call__(self) -> datetime:
            return self.value

    clock = Clock()
    calls = 0

    def transport(_url: str, **_kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            return _ticker_payload([320193, "Apple Inc.", "AAPL", "Nasdaq"])
        raise RuntimeError("Proxy-Authorization: sentinel-secret")

    resolver = SecCikTickerResolver(
        transport=transport,
        now=clock,
        cache_ttl_seconds=60,
    )
    assert resolver("Apple Inc.", "320193") == "AAPL"

    clock.value = NOW + timedelta(seconds=61)
    with pytest.raises(SecTickerMappingError) as captured:
        resolver("Apple Inc.", "320193")

    assert calls == 2
    assert resolver.health == "DEGRADED"
    assert resolver.health_reason == "request_failed"
    stats = resolver.resolution_stats()
    assert stats["refresh_count"] == 2
    assert stats["generation"] == 1
    assert stats["failure_counts"] == {"request_failed": 1}
    rendered = "".join(
        traceback.format_exception(captured.type, captured.value, captured.tb)
    )
    assert "sentinel-secret" not in rendered
    assert "Proxy-Authorization" not in rendered
    assert captured.value.__cause__ is None
    assert captured.value.__context__ is None


@pytest.mark.parametrize(
    ("payload", "status", "reason"),
    [
        (
            json.dumps({"fields": ["cik", "ticker"], "data": []}),
            "BAD_JSON",
            "invalid_mapping",
        ),
        (
            json.dumps(
                {
                    "fields": ["cik", "name", "ticker", "exchange"],
                    "data": "not-an-array",
                }
            ),
            "BAD_JSON",
            "invalid_mapping",
        ),
        (
            _ticker_payload(
                [320193, "Apple Inc.", "AAPL", "Nasdaq"],
                [320193, "Apple Inc.", "AAPL", "Nasdaq"],
            ),
            "CONFLICTED",
            "duplicate_or_conflicting_mapping",
        ),
        (
            _ticker_payload(
                [320193, "Apple Inc.", "AAPL", "Nasdaq"],
                [789019, "Another Corp", "AAPL", "NYSE"],
            ),
            "CONFLICTED",
            "duplicate_or_conflicting_mapping",
        ),
    ],
)
def test_ticker_mapping_schema_duplicates_and_cross_owner_conflicts_fail_closed(
    payload: str,
    status: str,
    reason: str,
) -> None:
    resolver = SecCikTickerResolver(
        transport=lambda *_args, **_kwargs: payload,
        now=lambda: NOW,
    )

    with pytest.raises(SecTickerMappingError):
        resolver("Apple Inc.", "320193")

    assert resolver.health == status
    assert resolver.health_reason == reason


@pytest.mark.parametrize("cik", [True, 0, -1, "", "ABC", "12345678901"])
def test_ticker_resolver_rejects_invalid_cik_without_transport(cik: object) -> None:
    calls = 0

    def transport(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        return _ticker_payload([320193, "Apple Inc.", "AAPL", "Nasdaq"])

    resolver = SecCikTickerResolver(transport=transport, now=lambda: NOW)

    with pytest.raises((TypeError, ValueError)):
        resolver("Apple Inc.", cik)  # type: ignore[arg-type]
    assert calls == 0


class _Response:
    def __init__(
        self,
        body: bytes,
        *,
        status: int = 200,
        url: str = SEC_CURRENT_8K_ATOM_URL,
        content_type: str = "application/atom+xml; charset=utf-8",
        content_length: int | None = None,
    ) -> None:
        self.status = status
        self._url = url
        self._body = BytesIO(body)
        self.headers = {
            "Content-Type": content_type,
            "Content-Encoding": "identity",
            "Content-Length": str(len(body) if content_length is None else content_length),
        }

    def geturl(self) -> str:
        return self._url

    def read(self, size: int = -1) -> bytes:
        return self._body.read(size)

    def __enter__(self):
        return self

    def __exit__(self, *_args) -> None:
        return None


class _Opener:
    def __init__(self, response: _Response | Exception) -> None:
        self.response = response
        self.calls: list[tuple[Request, float]] = []

    def open(self, request: Request, timeout: float):
        self.calls.append((request, timeout))
        if isinstance(self.response, Exception):
            raise self.response
        return self.response


def test_https_transport_sets_user_agent_and_enforces_exact_source() -> None:
    opener = _Opener(_Response(_feed(_entry()).encode()))
    transport = SecAtomHttpsTransport(opener=opener)

    payload = transport(
        SEC_CURRENT_8K_ATOM_URL,
        headers={
            "Accept": "application/atom+xml, application/xml;q=0.9",
            "User-Agent": SEC_USER_AGENT,
        },
        timeout_seconds=6.0,
    )

    assert "Latest EDGAR Filings" in payload
    request, timeout = opener.calls[0]
    assert request.full_url == SEC_CURRENT_8K_ATOM_URL
    assert request.get_method() == "GET"
    assert request.headers["User-agent"] == SEC_USER_AGENT
    assert timeout == 6.0

    with pytest.raises(SecProviderTransportError):
        transport(
            "https://www.sec.gov/cgi-bin/not-the-allowlisted-feed",
            headers={"Accept": "application/atom+xml", "User-Agent": SEC_USER_AGENT},
            timeout_seconds=6.0,
        )
    assert len(opener.calls) == 1


@pytest.mark.parametrize(
    "response",
    [
        _Response(b"ok", url="https://sec.gov/redirected"),
        _Response(b"ok", content_type="text/html"),
        _Response(b"ok", content_length=MAXIMUM_SEC_ATOM_RESPONSE_BYTES + 1),
        _Response(b"x" * (MAXIMUM_SEC_ATOM_RESPONSE_BYTES + 1)),
    ],
)
def test_https_transport_rejects_redirect_content_type_and_oversize(
    response: _Response,
) -> None:
    transport = SecAtomHttpsTransport(opener=_Opener(response))

    with pytest.raises(SecProviderTransportError) as captured:
        transport(
            SEC_CURRENT_8K_ATOM_URL,
            headers={
                "Accept": "application/atom+xml, application/xml;q=0.9",
                "User-Agent": SEC_USER_AGENT,
            },
            timeout_seconds=8.0,
        )

    assert captured.value.__cause__ is None
    assert captured.value.__context__ is None


def test_https_transport_rejects_credential_headers_and_invalid_timeout() -> None:
    opener = _Opener(_Response(b"ok"))
    transport = SecAtomHttpsTransport(opener=opener)

    with pytest.raises(SecProviderTransportError):
        transport(
            SEC_CURRENT_8K_ATOM_URL,
            headers={"Authorization": "Bearer sentinel-secret"},
            timeout_seconds=8.0,
        )
    with pytest.raises(ValueError):
        transport(
            SEC_CURRENT_8K_ATOM_URL,
            headers={
                "Accept": "application/atom+xml",
                "User-Agent": SEC_USER_AGENT,
            },
            timeout_seconds=0,
        )
    assert opener.calls == []


def test_ticker_https_transport_enforces_exact_json_source_and_user_agent() -> None:
    body = _ticker_payload([320193, "Apple Inc.", "AAPL", "Nasdaq"]).encode()
    opener = _Opener(
        _Response(
            body,
            url=SEC_COMPANY_TICKERS_EXCHANGE_URL,
            content_type="application/json; charset=utf-8",
        )
    )
    transport = SecCompanyTickersHttpsTransport(opener=opener)

    payload = transport(
        SEC_COMPANY_TICKERS_EXCHANGE_URL,
        headers={"Accept": "application/json", "User-Agent": SEC_USER_AGENT},
        timeout_seconds=5.0,
    )

    assert "Apple Inc." in payload
    request, timeout = opener.calls[0]
    assert request.full_url == SEC_COMPANY_TICKERS_EXCHANGE_URL
    assert request.get_method() == "GET"
    assert request.headers["User-agent"] == SEC_USER_AGENT
    assert timeout == 5.0

    with pytest.raises(SecProviderTransportError):
        transport(
            "https://www.sec.gov/files/company_tickers.json",
            headers={"Accept": "application/json", "User-Agent": SEC_USER_AGENT},
            timeout_seconds=5.0,
        )
    assert len(opener.calls) == 1


@pytest.mark.parametrize(
    "response",
    [
        _Response(
            b"{}",
            url="https://www.sec.gov/files/redirected.json",
            content_type="application/json",
        ),
        _Response(
            b"{}",
            url=SEC_COMPANY_TICKERS_EXCHANGE_URL,
            content_type="text/html",
        ),
        _Response(
            b"{}",
            url=SEC_COMPANY_TICKERS_EXCHANGE_URL,
            content_type="application/json",
            content_length=MAXIMUM_SEC_TICKER_RESPONSE_BYTES + 1,
        ),
        _Response(
            b"x" * (MAXIMUM_SEC_TICKER_RESPONSE_BYTES + 1),
            url=SEC_COMPANY_TICKERS_EXCHANGE_URL,
            content_type="application/json",
        ),
    ],
)
def test_ticker_https_transport_rejects_redirect_type_and_oversize(
    response: _Response,
) -> None:
    transport = SecCompanyTickersHttpsTransport(opener=_Opener(response))

    with pytest.raises(SecProviderTransportError) as captured:
        transport(
            SEC_COMPANY_TICKERS_EXCHANGE_URL,
            headers={"Accept": "application/json", "User-Agent": SEC_USER_AGENT},
            timeout_seconds=8.0,
        )

    assert captured.value.__cause__ is None
    assert captured.value.__context__ is None


def test_ticker_https_transport_redacts_transport_exception() -> None:
    opener = _Opener(RuntimeError("Authorization: Bearer ticker-map-secret"))
    transport = SecCompanyTickersHttpsTransport(opener=opener)

    with pytest.raises(SecProviderTransportError) as captured:
        transport(
            SEC_COMPANY_TICKERS_EXCHANGE_URL,
            headers={"Accept": "application/json", "User-Agent": SEC_USER_AGENT},
            timeout_seconds=8.0,
        )

    rendered = "".join(
        traceback.format_exception(captured.type, captured.value, captured.tb)
    )
    assert "ticker-map-secret" not in rendered
    assert "Authorization" not in rendered
    assert "Bearer" not in rendered
    assert captured.value.__cause__ is None
    assert captured.value.__context__ is None
