from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import asyncio
import hashlib
import sqlite3
import threading
from types import SimpleNamespace
from urllib.error import HTTPError

import pytest
import httpx
import options_copilot.news_runtime as news_runtime_module

from options_copilot.news.event_reaction_coverage import MarketSample, OptionRepriceBundle, ProspectiveMarketWindow
from options_copilot.news.reaction import ConsensusExpectation, EventReactionLedger, OfficialRelease, ScheduledEventIdentity
from options_copilot.news.reaction_runtime import MacroReactionError, ProductionMacroReactionProvider, ProductionReactionObserver, ReactionEvidenceStore
from options_copilot.news.reaction_specs import EventFamily, ParentEventIdentity, ScheduledReactionSpec, SupportState, assess_support, classify_event_family, reaction_descriptor_from_calendar
from options_copilot.providers.official_reaction_sources import BoundedOfficialDocumentClient, CapturedOfficialRelease, DiscoveryRecord, OfficialDocument, OfficialReactionSourceError, OfficialReleaseCaptureCoordinator, ParsedMeasure, ParsedOfficialRelease, parse_bea_gdp, parse_bea_pce, parse_bls_employment, parse_fomc_minutes, parse_fomc_statement
from options_copilot.news.reaction_specs import EventRole
from options_copilot.news_runtime import _deterministic_decision_calendar_row
from options_copilot.config import OptionsCopilotConfig
from options_copilot.api.app import OptionsCopilotServices, _normalise_reaction_provider
from options_copilot.news_runtime import NewsCoordinator
from options_copilot.runtime import OptionsCopilotRuntime, ReactionRuntimeOverrides
from options_copilot.api import create_app
from options_copilot.storage.canonical import canonical_hash
from options_copilot.providers.official import OfficialCalendarEvent, OfficialCalendarSnapshot, OfficialEventProvenance, OfficialSourceHealth


UTC = timezone.utc
RELEASE = datetime(2026, 8, 21, 12, 30, tzinfo=UTC)
EVENT_HASH = "a" * 64
RELEASE_HASH = "b" * 64
MACRO_SYMBOLS = ("SPY", "QQQ", "IWM", "TLT", "GLD", "UUP")


def _underlying_basket(observed_at: datetime):
    return tuple(
        SimpleNamespace(
            symbol=symbol,
            observed_at=observed_at,
            bid=Decimal("599"),
            ask=Decimal("601"),
        )
        for symbol in MACRO_SYMBOLS
    )


async def _http_get_json(app, path: str) -> dict[str, object]:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://options-copilot.test",
    ) as client:
        response = await client.get(path)
        response.raise_for_status()
        return response.json()


def _document(family: EventFamily, body: str) -> OfficialDocument:
    return OfficialDocument(family, EventRole.OFFICIAL_RELEASE_DOCUMENT, {
        EventFamily.EMPLOYMENT_SITUATION: "https://www.bls.gov/news.release/empsit.nr0.htm",
        EventFamily.PCE: "https://www.bea.gov/news/2026/personal-income-and-outlays-july-2026",
        EventFamily.GDP: "https://www.bea.gov/news/2026/gross-domestic-product-second-quarter-2026-second-estimate",
        EventFamily.FOMC_STATEMENT: "https://www.federalreserve.gov/newsevents/pressreleases/monetary20260821a.htm",
        EventFamily.FOMC_MINUTES: "https://www.federalreserve.gov/monetarypolicy/fomcminutes20260730.htm",
    }[family], RELEASE + timedelta(seconds=2), "text/html", body.encode())


def _scheduled_spec(parent: ParentEventIdentity) -> ScheduledReactionSpec:
    return ScheduledReactionSpec(
        parent,
        RELEASE,
        EVENT_HASH,
        RELEASE + timedelta(minutes=15),
    )


def _official_employment_event(
    event_id: str,
    *,
    scheduled_at: datetime,
    observed_at: datetime,
    source_id: str = "employment-situation-2026-07",
    title: str = "Employment Situation July 2026",
) -> OfficialCalendarEvent:
    source = "Bureau of Labor Statistics"
    source_url = "https://www.bls.gov/schedule/2026/home.htm"
    provenance = OfficialEventProvenance(
        source=source,
        source_url=source_url,
        source_id=source_id,
        source_payload_hash=canonical_hash(
            {"event_id": event_id, "scheduled_at": scheduled_at.isoformat()}
        ),
        first_seen_at=RELEASE - timedelta(days=20),
        ingested_at=observed_at,
        observed_at=observed_at,
    )
    return OfficialCalendarEvent(
        event_id=event_id,
        source=source,
        source_id=source_id,
        source_url=source_url,
        title=title,
        category="MACRO",
        scheduled_at=scheduled_at,
        published_at=RELEASE - timedelta(days=30),
        first_seen_at=RELEASE - timedelta(days=20),
        ingested_at=observed_at,
        observed_at=observed_at,
        schedule_precision="EXACT",
        symbols=("SPY",),
        provenance=(provenance,),
    )


def test_support_matrix_distinguishes_support_capture_surprise_and_progression() -> None:
    waiting = assess_support(EventFamily.EMPLOYMENT_SITUATION, scheduled_at=RELEASE, now=RELEASE - timedelta(minutes=1))
    assert waiting.supported is True
    assert waiting.capture_eligible is False
    assert waiting.surprise_eligible is False
    assert waiting.progressed is False
    assert waiting.reason == "WAITING_DECLARED_RELEASE_TIME"

    document_only = assess_support(EventFamily.FOMC_MINUTES, scheduled_at=RELEASE, now=RELEASE + timedelta(seconds=1), release_document_captured=True)
    assert document_only.supported is True
    assert document_only.capture_eligible is True
    assert document_only.surprise_eligible is False
    assert document_only.progressed is True
    assert document_only.reason == "NUMERIC_ACTUAL_AND_SURPRISE_UNSUPPORTED"

    retail = assess_support(EventFamily.RETAIL_SALES, scheduled_at=RELEASE, now=RELEASE)
    assert retail.support_state is SupportState.PREFLIGHT_ONLY
    assert retail.next_action == "PREFLIGHT_OFFICIAL_CONTRACT"
    assert retail.reason == "EXACT_INITIAL_RELEASE_VARIABLE_TUPLE_UNVERIFIED"

    for family in (EventFamily.JOBLESS_CLAIMS, EventFamily.ISM, EventFamily.FOMC_PRESS_CONFERENCE, EventFamily.EARNINGS_GUIDANCE):
        result = assess_support(family, scheduled_at=RELEASE, now=RELEASE)
        assert result.support_state is SupportState.UNSUPPORTED
        assert result.capture_eligible is False
        assert result.next_action == "NONE_UNSUPPORTED"


def test_parent_and_measure_identity_preserve_multi_measure_event() -> None:
    parent = ParentEventIdentity("BLS", EventFamily.EMPLOYMENT_SITUATION, "2026-07", date(2026, 8, 7))
    assert parent.stable_id == "BLS:EMPLOYMENT_SITUATION:2026-07:2026-08-07"
    assert classify_event_family("BLS", "Employment Situation") is EventFamily.EMPLOYMENT_SITUATION
    assert classify_event_family("BEA", "Personal Income and Outlays") is EventFamily.PCE
    assert classify_event_family("Federal Reserve", "FOMC minutes") is EventFamily.FOMC_MINUTES
    assert classify_event_family("Federal Reserve Release Calendar", "FOMC Minutes") is EventFamily.FOMC_MINUTES
    assert classify_event_family("Company IR", "SPCE Earnings") is EventFamily.EARNINGS_GUIDANCE
    assert classify_event_family("Company IR", "SPCE CPI GDP update") is EventFamily.UNKNOWN
    assert classify_event_family("BEA", "CPI update") is EventFamily.UNKNOWN
    assert classify_event_family("BLS", "Consumer Price Index", "EARNINGS") is EventFamily.EARNINGS_GUIDANCE


def test_runtime_coverage_keeps_document_event_supported_with_exact_wait(tmp_path) -> None:
    clock = {"now": RELEASE - timedelta(minutes=1)}
    store = ReactionEvidenceStore(tmp_path / "coverage.sqlite3")
    provider = ProductionMacroReactionProvider(store, jin10_client=None, jin10_secret_store=None, official_actual_provider=object(), clock=lambda: clock["now"])
    event = SimpleNamespace(event_id="fed-minutes", source="Federal Reserve", source_id="fed-minutes", title="FOMC minutes 2026-07-29/2026-07-30", category="FOMC", scheduled_at=RELEASE, published_at=None, first_seen_at=RELEASE - timedelta(days=20), observed_at=RELEASE - timedelta(days=1), symbols=())
    try:
        provider.restore_official_identities((event,))
        assert provider.projection()["supported_event_ids"] == ["fed-minutes"]
        assert provider.projection()["eligible_event_ids"] == []
        waiting = provider.coverage(("fed-minutes",))["fed-minutes"]
        assert waiting["supported"] is True
        assert waiting["capture_eligible"] is False
        assert waiting["next_action"] == "WAIT_FOR_DECLARED_RELEASE_TIME"
        identity = provider._identities["fed-minutes"]
        clock["now"] = RELEASE + timedelta(seconds=2)
        store.append_raw_document(event_id="fed-minutes", official_event_hash=identity.content_hash, source_role="OFFICIAL_RELEASE_DOCUMENT", official_url="https://www.federalreserve.gov/monetarypolicy/fomcminutes20260730.htm", received_at=clock["now"], media_type="text/html", raw_bytes=b"official minutes")
        progressed = provider.coverage(("fed-minutes",))["fed-minutes"]
        assert progressed["capture_eligible"] is True
        assert progressed["progressed"] is False
        assert progressed["capture_count"] == 0
        assert progressed["surprise_eligible"] is False
    finally:
        provider.close()


def test_production_runtime_composition_progresses_only_after_bound_parsed_employment_capture(tmp_path) -> None:
    runtime = OptionsCopilotRuntime(
        OptionsCopilotConfig(data_dir=tmp_path / "data", log_dir=tmp_path / "logs")
    )
    event = SimpleNamespace(
        event_id="bls-employment-2026-07",
        source="Bureau of Labor Statistics",
        source_id="bls-employment-2026-07",
        title="Employment Situation July 2026",
        category="MACRO",
        scheduled_at=RELEASE,
        published_at=RELEASE - timedelta(days=30),
        first_seen_at=RELEASE - timedelta(days=20),
        observed_at=RELEASE - timedelta(days=1),
        symbols=(),
    )

    class Capture:
        def capture(self, request, *, now):
            document = OfficialDocument(
                EventFamily.EMPLOYMENT_SITUATION,
                EventRole.OFFICIAL_RELEASE_DOCUMENT,
                "https://www.bls.gov/news.release/empsit.nr0.htm",
                now,
                "text/html",
                b"Employment Situation July 2026 official bytes",
                declared_release_at=request.scheduled_at,
                first_observed_release_at=now,
            )
            parsed = ParsedOfficialRelease(
                EventFamily.EMPLOYMENT_SITUATION,
                "2026-07",
                None,
                (
                    ParsedMeasure("total_nonfarm_payroll_change_thousands", Decimal("187"), "THOUSANDS", "SEASONALLY_ADJUSTED", "Total nonfarm payroll employment"),
                    ParsedMeasure("unemployment_rate_pct", Decimal("4.2"), "PERCENT", "SEASONALLY_ADJUSTED", "Unemployment rate"),
                ),
                (),
                document.raw_hash,
                declared_release_at=request.scheduled_at,
                first_observed_release_at=now,
            )
            return CapturedOfficialRelease(
                request,
                DiscoveryRecord(
                    EventFamily.EMPLOYMENT_SITUATION,
                    "https://www.bls.gov/schedule/news_release/empsit.htm",
                    now,
                    request.official_event_hash,
                    document.url,
                ),
                document,
                parsed,
            )

    try:
        assert isinstance(runtime.macro_reactions, ProductionMacroReactionProvider)
        runtime.macro_reactions._official_document_provider = Capture()
        runtime.macro_reactions.refresh(
            SimpleNamespace(events=(event,)),
            now=RELEASE + timedelta(seconds=2),
        )
        coverage = runtime.macro_reactions.coverage((event.event_id,))[event.event_id]
        assert coverage["progressed"] is True
        assert coverage["capture_count"] == 1
        assert [item["reaction_id"] for item in coverage["measure_reactions"]] == [
            "bls-employment-2026-07:total_nonfarm_payroll_change_thousands",
            "bls-employment-2026-07:unemployment_rate_pct",
        ]
        assert runtime.macro_reactions.projection()["progressed_event_count"] == 1
    finally:
        runtime.close()


def test_injected_runtime_e2e_projects_multimeasure_and_fomc_then_replays_restart(
    tmp_path,
) -> None:
    current = {"now": RELEASE - timedelta(minutes=1)}

    def official_event(
        event_id: str,
        source: str,
        source_id: str,
        source_url: str,
        title: str,
        category: str,
    ) -> OfficialCalendarEvent:
        provenance = OfficialEventProvenance(
            source=source,
            source_url=source_url,
            source_id=source_id,
            source_payload_hash=canonical_hash({"event_id": event_id}),
            first_seen_at=RELEASE - timedelta(days=20),
            ingested_at=RELEASE - timedelta(minutes=1),
            observed_at=RELEASE - timedelta(minutes=1),
        )
        return OfficialCalendarEvent(
            event_id=event_id,
            source=source,
            source_id=source_id,
            source_url=source_url,
            title=title,
            category=category,
            scheduled_at=RELEASE,
            published_at=RELEASE - timedelta(days=30),
            first_seen_at=RELEASE - timedelta(days=20),
            ingested_at=RELEASE - timedelta(minutes=1),
            observed_at=RELEASE - timedelta(minutes=1),
            schedule_precision="EXACT",
            symbols=("SPY",),
            provenance=(provenance,),
        )

    events = (
        official_event(
            "employment-runtime-e2e",
            "Bureau of Labor Statistics",
            "empsit-runtime-e2e",
            "https://www.bls.gov/schedule/2026/home.htm",
            "Employment Situation July 2026",
            "MACRO",
        ),
        official_event(
            "pce-runtime-e2e",
            "Bureau of Economic Analysis",
            "pce-runtime-e2e",
            "https://www.bea.gov/news/schedule",
            "Personal Income and Outlays July 2026",
            "MACRO",
        ),
        official_event(
            "fomc-minutes-runtime-e2e",
            "Federal Reserve",
            "fomc-minutes-runtime-e2e",
            "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm",
            "FOMC minutes 2026-07-29/2026-07-30",
            "FOMC",
        ),
    )
    sources = tuple(
        OfficialSourceHealth(
            source=event.source,
            source_url=event.source_url,
            status="READY",
            reason=None,
            observed_at=RELEASE - timedelta(minutes=1),
            event_count=1,
        )
        for event in events
    )
    class Calendar:
        health = "READY"
        health_reason = None

        def __init__(self) -> None:
            self.public_calls = 0
            self.schedule_calls = 0

        def future_two_weeks(self, *, now):
            self.public_calls += 1
            return OfficialCalendarSnapshot(
                status="READY",
                decision="OBSERVATION_ONLY",
                window_start=now,
                window_end=now + timedelta(days=14),
                observed_at=now,
                events=events,
                sources=sources,
                reasons=(),
            )

        def reaction_schedule(self, *, now):
            self.schedule_calls += 1
            return SimpleNamespace(
                status="READY",
                reasons=(),
                schedule_hash="d" * 64,
                events=events,
            )

    class Quotes:
        def cached_reaction_underlying_quotes(self, _symbols):
            return _underlying_basket(current["now"])

        def preselections(self):
            return ()

    class Jin10:
        calls = 0

        def fetch_calendar(self, _token):
            self.calls += 1
            return SimpleNamespace(
                payload={
                    "status": 200,
                    "data": [
                        {
                            "title": "\u7f8e\u56fd7\u6708\u975e\u519c\u5c31\u4e1a\u4eba\u53e3\u53d8\u52a8",
                            "pub_time": "2026-08-21 20:30",
                            "consensus": "180",
                        },
                        {
                            "title": "\u7f8e\u56fd7\u6708\u5931\u4e1a\u7387",
                            "pub_time": "2026-08-21 20:30",
                            "consensus": "4.2",
                        },
                        {
                            "title": "\u7f8e\u56fd7\u6708\u6838\u5fc3PCE\u7269\u4ef7\u6307\u6570\u5e74\u7387",
                            "pub_time": "2026-08-21 20:30",
                            "consensus": "2.8",
                        },
                    ],
                }
            )

    class Secrets:
        def get(self, _name):
            return "bounded-test-token"

    class Capture:
        calls: list[EventFamily] = []

        def capture(self, request, *, now):
            family = request.parent.family
            self.calls.append(family)
            url = {
                EventFamily.EMPLOYMENT_SITUATION: "https://www.bls.gov/news.release/empsit.nr0.htm",
                EventFamily.PCE: "https://www.bea.gov/news/2026/personal-income-and-outlays-july-2026",
                EventFamily.FOMC_MINUTES: "https://www.federalreserve.gov/monetarypolicy/fomcminutes20260730.htm",
            }[family]
            document = OfficialDocument(
                family,
                EventRole.OFFICIAL_RELEASE_DOCUMENT,
                url,
                now,
                "text/html",
                f"validated {family.value}".encode(),
                declared_release_at=request.scheduled_at,
                first_observed_release_at=now,
            )
            measures = {
                EventFamily.EMPLOYMENT_SITUATION: (
                    ParsedMeasure("total_nonfarm_payroll_change_thousands", Decimal("187"), "THOUSANDS", "SEASONALLY_ADJUSTED", "Payrolls"),
                    ParsedMeasure("unemployment_rate_pct", Decimal("4.2"), "PERCENT", "SEASONALLY_ADJUSTED", "Unemployment"),
                ),
                EventFamily.PCE: (
                    ParsedMeasure("core_pce_price_index_yoy_pct", Decimal("2.9"), "PERCENT", "NOT_SEASONALLY_ADJUSTED_YEAR_OVER_YEAR", "Core PCE"),
                ),
                EventFamily.FOMC_MINUTES: (),
            }[family]
            parsed = ParsedOfficialRelease(
                family,
                request.parent.reference_period,
                request.parent.estimate_label,
                measures,
                (),
                document.raw_hash,
                actual_parse_available=family is not EventFamily.FOMC_MINUTES,
                declared_release_at=request.scheduled_at,
                first_observed_release_at=now,
            )
            return CapturedOfficialRelease(
                request,
                DiscoveryRecord(family, "https://official.invalid/feed", now, None, url),
                document,
                parsed,
            )

    calendar = Calendar()
    jin10 = Jin10()
    capture = Capture()

    def overrides() -> ReactionRuntimeOverrides:
        def factory(store):
            return ProductionMacroReactionProvider(
                store,
                jin10_client=jin10,
                jin10_secret_store=Secrets(),
                official_actual_provider=object(),
                official_document_provider=capture,
                reaction_observer=ProductionReactionObserver(
                    Quotes(),
                    SimpleNamespace(ready=True),
                    store=store,
                ),
                clock=lambda: current["now"],
            )

        return ReactionRuntimeOverrides(
            provider_factory=factory,
            public_calendar_provider=calendar,
            schedule_calendar_provider=calendar,
            clock=lambda: current["now"],
        )

    config = OptionsCopilotConfig(
        data_dir=tmp_path / "runtime-data",
        log_dir=tmp_path / "runtime-logs",
    )
    runtime = OptionsCopilotRuntime(config, reaction_overrides=overrides())
    roots: set[str]
    try:
        runtime.news.refresh_once()
        initial_calendar = runtime.news.calendar_payload()
        assert initial_calendar["count"] == 3, (
            initial_calendar["provider"],
            initial_calendar.get("reasons"),
            calendar.public_calls,
        )
        runtime.news.refresh_reaction_schedule_once()
        roots = set(
            runtime.macro_reactions.reaction_roots(
                tuple(event.event_id for event in events)
            ).values()
        )
        assert len(roots) == 3
        current["now"] = RELEASE - timedelta(seconds=20)
        runtime.news.refresh_reaction_capture_once()
        assert jin10.calls == 1
        current["now"] = RELEASE - timedelta(seconds=5)
        runtime.news.refresh_reaction_once()
        current["now"] = RELEASE + timedelta(seconds=2)
        runtime.news.refresh_reaction_capture_once()
        assert set(capture.calls) == {
            EventFamily.EMPLOYMENT_SITUATION,
            EventFamily.PCE,
            EventFamily.FOMC_MINUTES,
        }
        child_counts = {
            event_id: len(children)
            for event_id, children in runtime.macro_reactions.child_reactions(
                tuple(event.event_id for event in events)
            ).items()
        }
        assert child_counts == {
            "employment-runtime-e2e": 2,
            "pce-runtime-e2e": 1,
        }
        current["now"] = RELEASE + timedelta(minutes=5)
        runtime.news.refresh_reaction_once()
        assert runtime.news.refresh_reaction_publication_once() is True
        raw_rows = {
            str(row.get("event_id")): row
            for row in runtime.news.calendar_payload()["calendar"]
        }
        assert runtime.news.calendar_payload()["reaction_provider"]["status"] == (
            "READY"
        ), runtime.news.calendar_payload()["reaction_provider"]
        assert len(raw_rows["employment-runtime-e2e"]["measure_reactions"]) == 2, (
            raw_rows["employment-runtime-e2e"],
            runtime.macro_reactions.reaction_roots(
                tuple(event.event_id for event in events)
            ),
        )

        app = create_app(runtime.services())
        payload = asyncio.run(_http_get_json(app, "/api/calendar"))
        rows = {
            str(row.get("event_id") or row.get("id")): row
            for row in payload["calendar"]
        }
        assert "employment-runtime-e2e" in rows, rows
        employment = rows["employment-runtime-e2e"]
        assert len(employment["measure_reactions"]) == 2
        assert all(
            item["analysis_available"] is True
            for item in employment["measure_reactions"]
        )
        assert len(rows["pce-runtime-e2e"]["measure_reactions"]) == 1
        fomc_market = rows["fomc-minutes-runtime-e2e"]["reaction"][
            "market_reaction"
        ]
        assert fomc_market is not None
        assert payload["reaction_provider"]["decision_authority"] == (
            "SUPPORTING_ONLY"
        )
        assert payload["approval_eligible"] is False
        assert "authorization" not in str(payload).lower()
        assert "order_submission" not in str(payload).lower()
    finally:
        runtime.close()

    public_calls_before_restart = calendar.public_calls
    restarted = OptionsCopilotRuntime(config, reaction_overrides=overrides())
    try:
        assert set(
            restarted.macro_reactions.reaction_roots(
                tuple(event.event_id for event in events)
            ).values()
        ) == roots
        assert calendar.public_calls == public_calls_before_restart
        assert restarted.macro_reactions.projection()["progressed_event_count"] == 3
        restarted.news.refresh_reaction_once()
        assert restarted.news.refresh_reaction_publication_once() is True
        app = create_app(restarted.services())
        payload = asyncio.run(_http_get_json(app, "/api/calendar"))
        assert payload["reaction_provider"]["decision_authority"] == (
            "SUPPORTING_ONLY"
        )
        assert payload["approval_eligible"] is False
    finally:
        restarted.close()


def test_v4_migration_preserves_v3_rows_and_head_byte_for_byte(tmp_path) -> None:
    path = tmp_path / "v3.sqlite3"
    connection = sqlite3.connect(path)
    document = '{"schema":"fixture.v1"}'
    content_hash = hashlib.sha256(document.encode()).hexdigest()
    from options_copilot.news.reaction_runtime import _reaction_row_hash
    row_hash = _reaction_row_hash(sequence=1, prior_hash="0" * 64, event_id="event", official_event_hash=EVENT_HASH, kind="JIN10_EXPECTATION", observed_at=RELEASE.isoformat(), content_hash=content_hash)
    try:
        connection.executescript("""
        CREATE TABLE macro_reaction_evidence(sequence INTEGER PRIMARY KEY,event_id TEXT NOT NULL,official_event_hash TEXT NOT NULL,kind TEXT NOT NULL,observed_at TEXT NOT NULL,document_json TEXT NOT NULL,content_hash TEXT NOT NULL,prior_hash TEXT NOT NULL,row_hash TEXT NOT NULL UNIQUE,UNIQUE(event_id,official_event_hash,kind,content_hash));
        PRAGMA user_version=3;
        """)
        connection.execute("INSERT INTO macro_reaction_evidence VALUES(?,?,?,?,?,?,?,?,?)", (1, "event", EVENT_HASH, "JIN10_EXPECTATION", RELEASE.isoformat(), document, content_hash, "0" * 64, row_hash))
        connection.commit()
    finally:
        connection.close()
    store = ReactionEvidenceStore(path)
    try:
        row = store._db.execute("SELECT document_json,row_hash FROM macro_reaction_evidence").fetchone()
        assert tuple(row) == (document, row_hash)
        assert store._db.execute("PRAGMA user_version").fetchone()[0] == 4
        assert store.append_raw_document(event_id="event", official_event_hash=EVENT_HASH, source_role="OFFICIAL_RELEASE_DOCUMENT", official_url="https://www.bls.gov/news.release/empsit.nr0.htm", received_at=RELEASE, media_type="text/html", raw_bytes=b"initial bytes") is True
        assert bytes(store.raw_documents("event", EVENT_HASH)[0]["raw_bytes"]) == b"initial bytes"
        store.assert_integrity()
    finally:
        store.close()


def test_raw_document_chain_detects_tampering(tmp_path) -> None:
    path = tmp_path / "raw.sqlite3"
    store = ReactionEvidenceStore(path)
    store.append_raw_document(event_id="event", official_event_hash=EVENT_HASH, source_role="OFFICIAL_RELEASE_DOCUMENT", official_url="https://www.bls.gov/news.release/empsit.nr0.htm", received_at=RELEASE, media_type="text/html", raw_bytes=b"initial bytes")
    store.close()
    connection = sqlite3.connect(path)
    try:
        connection.execute("DROP TRIGGER macro_reaction_raw_no_update")
        connection.execute("UPDATE macro_reaction_raw_documents SET raw_bytes=?", (b"tampered",))
        connection.commit()
    finally:
        connection.close()
    with pytest.raises(MacroReactionError, match="REACTION_RAW_DOCUMENT_INTEGRITY_FAILED"):
        ReactionEvidenceStore(path)


def test_observer_baseline_and_schedule_survive_fresh_store_instance(tmp_path) -> None:
    path = tmp_path / "restart-observer-state.sqlite3"
    identity = ScheduledEventIdentity(
        event_id="employment-2026-07",
        official_source="Bureau of Labor Statistics",
        official_source_id="employment-2026-07",
        title="Employment Situation July 2026",
        category="MACRO",
        scheduled_at=RELEASE,
        schedule_published_at=RELEASE - timedelta(days=30),
        schedule_first_seen_at=RELEASE - timedelta(days=20),
        schedule_observed_at=RELEASE - timedelta(minutes=10),
        symbols=("SPY",),
    )
    spec = ScheduledReactionSpec(
        ParentEventIdentity(
            "BLS",
            EventFamily.EMPLOYMENT_SITUATION,
            "2026-07",
            RELEASE.date(),
        ),
        RELEASE,
        identity.content_hash,
        RELEASE + timedelta(minutes=15),
    )
    baseline = MarketSample(
        RELEASE - timedelta(minutes=10),
        {"SPY_bid": Decimal("599"), "SPY_ask": Decimal("601")},
    )
    first = ReactionEvidenceStore(path)
    try:
        assert first.save_baseline(identity, baseline) is True
        assert first.save_schedule(
            identity.event_id,
            spec,
            observed_at=identity.schedule_observed_at,
        ) is True
    finally:
        first.close()

    restarted = ReactionEvidenceStore(path)
    try:
        restored = restarted.load_baseline(
            identity.content_hash,
            scheduled_at=identity.scheduled_at,
        )
        assert restored == baseline
        assert restarted.save_schedule(
            identity.event_id,
            spec,
            observed_at=identity.schedule_observed_at,
        ) is False
        restarted.assert_integrity()
    finally:
        restarted.close()


def test_official_release_fixture_parsers_preserve_measures_and_vintages() -> None:
    employment = parse_bls_employment(_document(EventFamily.EMPLOYMENT_SITUATION, "<p>Total nonfarm payroll employment increased by 187,000. The unemployment rate was 4.2 percent. June was revised down by 20,000.</p>"), reference_period="2026-07")
    assert [item.measure_id for item in employment.measures] == ["total_nonfarm_payroll_change_thousands", "unemployment_rate_pct"]
    assert [item.value for item in employment.measures] == [Decimal("187"), Decimal("4.2")]
    assert employment.revision_labels

    pce = parse_bea_pce(_document(EventFamily.PCE, "<p>From the preceding month, the PCE price index increased 0.2 percent. Excluding food and energy, the PCE price index increased 0.3 percent. From the same month one year ago, the PCE price index increased 2.6 percent. Excluding food and energy, the PCE price index increased 2.8 percent. Personal income increased.</p>"), reference_period="2026-07")
    assert len(pce.measures) == 4

    gdp = parse_bea_gdp(_document(EventFamily.GDP, "<h1>Gross Domestic Product, Second Estimate</h1><p>Real gross domestic product (GDP) increased at an annual rate of 3.1 percent.</p>"), reference_period="2026-Q2")
    assert gdp.estimate_label == "SECOND"
    assert gdp.measures[0].value == Decimal("3.1")


def test_bls_employment_parser_signs_declined_payrolls_in_thousands() -> None:
    parsed = parse_bls_employment(
        _document(
            EventFamily.EMPLOYMENT_SITUATION,
            "<p>Total nonfarm payroll employment declined by 13,000. "
            "The unemployment rate was 4.2 percent.</p>",
        ),
        reference_period="2026-07",
    )

    assert tuple(item.value for item in parsed.measures) == (
        Decimal("-13"),
        Decimal("4.2"),
    )
    assert parsed.measures[0].unit == "THOUSANDS"


def test_bls_employment_parser_accepts_official_comma_changed_little_rate() -> None:
    parsed = parse_bls_employment(
        _document(
            EventFamily.EMPLOYMENT_SITUATION,
            "<p>Total nonfarm payroll employment rose by 147,000. "
            "The unemployment rate, at 4.2 percent, changed little in July.</p>",
        ),
        reference_period="2026-07",
    )

    assert tuple(item.value for item in parsed.measures) == (
        Decimal("147"),
        Decimal("4.2"),
    )


def test_official_client_rejects_host_path_media_redirect_and_oversize() -> None:
    class Response:
        def __init__(self, *, url: str, media: str = "text/html", body: bytes = b"ok") -> None:
            self._url = url
            self.headers = {"Content-Type": media}
            self._body = body

        def geturl(self) -> str:
            return self._url

        def read(self, limit: int) -> bytes:
            return self._body[:limit]

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

    class Opener:
        def __init__(self, response: Response) -> None:
            self.response = response
            self.method = None

        def open(self, request, timeout: float):
            self.method = request.get_method()
            assert 0 < timeout <= 8
            return self.response

    url = "https://www.bls.gov/news.release/empsit.nr0.htm"
    opener = Opener(Response(url=url))
    document = BoundedOfficialDocumentClient(opener=opener).fetch(EventFamily.EMPLOYMENT_SITUATION, url, received_at=RELEASE, declared_release_at=RELEASE)
    assert opener.method == "GET"
    assert document.raw_bytes == b"ok"
    for bad_url in ("http://www.bls.gov/news.release/empsit.nr0.htm", "https://evil.example/news.release/empsit.nr0.htm", "https://www.bls.gov/undocumented/scrape"):
        with pytest.raises(OfficialReactionSourceError, match="OFFICIAL_URL_NOT_ALLOWLISTED"):
            BoundedOfficialDocumentClient(opener=opener).fetch(EventFamily.EMPLOYMENT_SITUATION, bad_url, received_at=RELEASE, declared_release_at=RELEASE)
    with pytest.raises(OfficialReactionSourceError, match="OFFICIAL_MEDIA_TYPE_INVALID"):
        BoundedOfficialDocumentClient(opener=Opener(Response(url=url, media="application/json"))).fetch(EventFamily.EMPLOYMENT_SITUATION, url, received_at=RELEASE, declared_release_at=RELEASE)
    with pytest.raises(OfficialReactionSourceError, match="OFFICIAL_REDIRECT_REJECTED"):
        BoundedOfficialDocumentClient(opener=Opener(Response(url="https://www.bls.gov/news.release/other.htm"))).fetch(EventFamily.EMPLOYMENT_SITUATION, url, received_at=RELEASE, declared_release_at=RELEASE)
    with pytest.raises(OfficialReactionSourceError, match="OFFICIAL_RESPONSE_TOO_LARGE"):
        BoundedOfficialDocumentClient(opener=Opener(Response(url=url, body=b"x" * (2 * 1024 * 1024 + 1)))).fetch(EventFamily.EMPLOYMENT_SITUATION, url, received_at=RELEASE, declared_release_at=RELEASE)
    with pytest.raises(OfficialReactionSourceError, match="OFFICIAL_URL_NOT_ALLOWLISTED"):
        BoundedOfficialDocumentClient(opener=opener).fetch(EventFamily.EMPLOYMENT_SITUATION, "https://www.bls.gov:444/news.release/empsit.nr0.htm", received_at=RELEASE, declared_release_at=RELEASE)
    with pytest.raises(OfficialReactionSourceError, match="OFFICIAL_CHARSET_INVALID"):
        BoundedOfficialDocumentClient(opener=Opener(Response(url=url, media="text/html; charset=windows-1252"))).fetch(EventFamily.EMPLOYMENT_SITUATION, url, received_at=RELEASE, declared_release_at=RELEASE)


def test_official_client_preserves_http_codes_and_bounds_retry() -> None:
    url = "https://www.bls.gov/news.release/empsit.nr0.htm"

    class Response:
        headers = {"Content-Type": "text/html; charset=utf-8"}

        def geturl(self):
            return url

        def read(self, _limit):
            return b"ok"

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

    class SequenceOpener:
        def __init__(self, values):
            self.values = list(values)
            self.calls = 0

        def open(self, request, timeout):
            self.calls += 1
            value = self.values.pop(0)
            if isinstance(value, BaseException):
                raise value
            return value

    forbidden = SequenceOpener([HTTPError(url, 403, "forbidden", {}, None)])
    with pytest.raises(OfficialReactionSourceError, match="HTTP_403"):
        BoundedOfficialDocumentClient(opener=forbidden).fetch(EventFamily.EMPLOYMENT_SITUATION, url, received_at=RELEASE, declared_release_at=RELEASE)
    assert forbidden.calls == 1

    sleeps: list[float] = []
    throttled = SequenceOpener([HTTPError(url, 429, "slow", {"Retry-After": "1"}, None), Response()])
    assert BoundedOfficialDocumentClient(opener=throttled, sleeper=sleeps.append).fetch(EventFamily.EMPLOYMENT_SITUATION, url, received_at=RELEASE, declared_release_at=RELEASE).raw_bytes == b"ok"
    assert throttled.calls == 2
    assert sleeps == [1]

    excessive = SequenceOpener([HTTPError(url, 429, "slow", {"Retry-After": "3"}, None)])
    with pytest.raises(OfficialReactionSourceError, match="HTTP_429_RETRY_AFTER_INVALID"):
        BoundedOfficialDocumentClient(opener=excessive, sleeper=sleeps.append).fetch(EventFamily.EMPLOYMENT_SITUATION, url, received_at=RELEASE, declared_release_at=RELEASE)
    assert excessive.calls == 1

    unavailable = SequenceOpener([HTTPError(url, 503, "down", {}, None), HTTPError(url, 503, "down", {}, None)])
    with pytest.raises(OfficialReactionSourceError, match="HTTP_503"):
        BoundedOfficialDocumentClient(opener=unavailable).fetch(EventFamily.EMPLOYMENT_SITUATION, url, received_at=RELEASE, declared_release_at=RELEASE)
    assert unavailable.calls == 2


def test_capture_rejects_late_and_reference_mismatched_documents() -> None:
    parent = ParentEventIdentity("BLS", EventFamily.EMPLOYMENT_SITUATION, "2026-07", RELEASE.date())
    request = _scheduled_spec(parent)
    with pytest.raises(OfficialReactionSourceError, match="WAIT_NEXT_ELIGIBLE_RELEASE:LATE_CAPTURE_DEADLINE"):
        OfficialReleaseCaptureCoordinator(opener=object()).capture(request, now=RELEASE + timedelta(minutes=16))

    class Response:
        headers = {"Content-Type": "text/html; charset=utf-8"}

        def geturl(self):
            return "https://www.bls.gov/news.release/empsit.nr0.htm"

        def read(self, _limit):
            return b"<h1>Employment Situation June 2026</h1><p>Total nonfarm payroll employment increased by 187,000. The unemployment rate was 4.2 percent.</p>"

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

    class Opener:
        def open(self, request, timeout):
            return Response()

    with pytest.raises(OfficialReactionSourceError, match="OFFICIAL_REFERENCE_PERIOD_MISMATCH"):
        OfficialReleaseCaptureCoordinator(opener=Opener()).capture(request, now=RELEASE + timedelta(seconds=1))


def test_fomc_capture_coordinator_uses_official_feed_then_allowlisted_document() -> None:
    feed_url = "https://www.federalreserve.gov/feeds/press_all.xml"
    document_url = "https://www.federalreserve.gov/newsevents/pressreleases/monetary20260821a.htm"

    class Response:
        def __init__(self, url: str, media: str, body: bytes) -> None:
            self.url, self.headers, self.body = url, {"Content-Type": media}, body

        def geturl(self):
            return self.url

        def read(self, limit: int):
            return self.body[:limit]

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

    class Opener:
        def __init__(self) -> None:
            self.urls: list[str] = []

        def open(self, request, timeout: float):
            assert request.get_method() == "GET"
            assert timeout <= 8
            self.urls.append(request.full_url)
            if request.full_url == feed_url:
                return Response(feed_url, "application/rss+xml", f"<rss><channel><item><title>Federal Reserve issues FOMC statement</title><link>{document_url}</link></item></channel></rss>".encode())
            return Response(document_url, "text/html", b"<p>The Committee decided to maintain the target range for the federal funds rate at 4.25 to 4.50 percent.</p>")

    opener = Opener()
    captured = OfficialReleaseCaptureCoordinator(opener=opener).capture(_scheduled_spec(ParentEventIdentity("FED", EventFamily.FOMC_STATEMENT, "2026-08-21", RELEASE.date())), now=RELEASE)
    assert opener.urls == [feed_url, document_url]
    assert captured.document.raw_bytes.startswith(b"<p>")
    assert captured.parsed.measures[0].label == "UNCHANGED:4.25-4.50"


def test_bea_gdp_capture_discovers_word_form_quarter_url() -> None:
    url = (
        "https://www.bea.gov/news/2026/"
        "gross-domestic-product-second-quarter-2026-second-estimate"
    )
    feed = (
        "<rss><channel><item>"
        "<title>Gross Domestic Product, Second Quarter 2026, Second Estimate</title>"
        f"<link>{url}</link>"
        "</item></channel></rss>"
    ).encode()

    class FeedResponse:
        headers = {"Content-Type": "application/rss+xml; charset=utf-8"}

        def geturl(self):
            return "https://apps.bea.gov/rss/rss.xml"

        def read(self, _limit):
            return feed

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

    class FeedOpener:
        def open(self, request, timeout):
            assert request.get_method() == "GET"
            assert timeout == 8.0
            return FeedResponse()

    class Documents:
        def fetch(self, family, discovered_url, **kwargs):
            assert family is EventFamily.GDP
            assert discovered_url == url
            return OfficialDocument(
                family,
                EventRole.OFFICIAL_RELEASE_DOCUMENT,
                discovered_url,
                kwargs["received_at"],
                "text/html",
                (
                    "<h1>Gross Domestic Product, Second Quarter 2026, "
                    "Second Estimate</h1><p>Real gross domestic product (GDP) "
                    "increased at an annual rate of 3.1 percent.</p>"
                ).encode(),
                declared_release_at=kwargs["declared_release_at"],
                first_observed_release_at=kwargs["received_at"],
            )

    request = ScheduledReactionSpec(
        ParentEventIdentity(
            "BEA",
            EventFamily.GDP,
            "2026-Q2",
            RELEASE.date(),
            "SECOND",
        ),
        RELEASE,
        EVENT_HASH,
        RELEASE + timedelta(minutes=15),
    )
    captured = OfficialReleaseCaptureCoordinator(
        document_client=Documents(),
        opener=FeedOpener(),
        sleeper=lambda _seconds: None,
    ).capture(request, now=RELEASE + timedelta(seconds=2))

    assert captured.discovery.discovered_url == url
    assert captured.parsed.reference_period == "2026-Q2"
    assert captured.parsed.estimate_label == "SECOND"


def test_fomc_document_lifecycle_never_invents_numeric_surprise() -> None:
    statement = parse_fomc_statement(_document(EventFamily.FOMC_STATEMENT, "<p>The Committee decided to maintain the target range for the federal funds rate at 4.25 to 4.50 percent.</p>"), meeting_end_date="2026-08-21")
    assert statement.measures[0].value is None
    assert statement.measures[0].label == "UNCHANGED:4.25-4.50"
    minutes = parse_fomc_minutes(_document(EventFamily.FOMC_MINUTES, "<p>Minutes of the Federal Open Market Committee, July 29-30, 2026.</p>"), meeting_range="2026-07-29/2026-07-30")
    assert minutes.actual_parse_available is False
    assert minutes.measures[0].value is None
    with pytest.raises(OfficialReactionSourceError, match="OFFICIAL_SCHEMA_DRIFT"):
        parse_fomc_minutes(_document(EventFamily.FOMC_MINUTES, "<p>hostile unrelated document</p>"), meeting_range="2026-07-29/2026-07-30")


def test_fomc_validated_document_progresses_to_cached_market_without_jin10_or_option(
    tmp_path,
) -> None:
    current = {"now": RELEASE - timedelta(seconds=5)}

    class Quotes:
        def cached_reaction_underlying_quotes(self, _symbols):
            return _underlying_basket(current["now"])

        def preselections(self):
            raise AssertionError("document-only FOMC path fabricated an option")

    class Jin10:
        calls = 0

        def fetch_calendar(self, _token):
            self.calls += 1
            raise AssertionError("document-only FOMC path called Jin10")

    class Secrets:
        calls = 0

        def get(self, _name):
            self.calls += 1
            raise AssertionError("document-only FOMC path read Jin10 credentials")

    class Capture:
        calls = 0

        def capture(self, request, *, now):
            self.calls += 1
            document = _document(
                EventFamily.FOMC_MINUTES,
                "<p>Minutes of the Federal Open Market Committee, July 29-30, 2026.</p>",
            )
            parsed = parse_fomc_minutes(
                document,
                meeting_range=request.parent.reference_period,
            )
            return CapturedOfficialRelease(
                request,
                DiscoveryRecord(
                    EventFamily.FOMC_MINUTES,
                    "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm",
                    now,
                    None,
                    document.url,
                ),
                document,
                parsed,
            )

    event = SimpleNamespace(
        event_id="fed-minutes-july",
        source="Federal Reserve",
        source_id="fed-minutes-july",
        title="FOMC minutes 2026-07-29/2026-07-30",
        category="FOMC",
        scheduled_at=RELEASE,
        published_at=RELEASE - timedelta(days=20),
        first_seen_at=RELEASE - timedelta(days=20),
        observed_at=RELEASE - timedelta(days=1),
        symbols=("SPY",),
    )
    store = ReactionEvidenceStore(tmp_path / "fomc-document-market.sqlite3")
    jin10 = Jin10()
    secrets = Secrets()
    capture = Capture()
    observer = ProductionReactionObserver(
        Quotes(),
        SimpleNamespace(ready=True),
        store=store,
    )
    provider = ProductionMacroReactionProvider(
        store,
        jin10_client=jin10,
        jin10_secret_store=secrets,
        official_actual_provider=object(),
        official_document_provider=capture,
        reaction_observer=observer,
        clock=lambda: current["now"],
    )
    try:
        provider.restore_official_identities((event,))
        provider.observe_local(now=current["now"])
        current["now"] = RELEASE + timedelta(seconds=2)
        provider.refresh_capture(now=current["now"])
        current["now"] = RELEASE + timedelta(minutes=5)
        provider.observe_local(now=current["now"])

        coverage = provider.coverage((event.event_id,))[event.event_id]
        market = coverage["document_market_reaction"]
        assert coverage["progressed"] is True
        assert market["metrics"]["document_only"] is True
        assert market["metrics"]["numeric_surprise"] == "UNAVAILABLE"
        identity = provider._state().identities[event.event_id]
        rows = store.records(((event.event_id, identity.event_hash),))
        assert [row["kind"] for row in rows].count("MARKET_REACTION") == 1
        assert all(row["kind"] != "OPTION_REEVALUATION" for row in rows)
        assert capture.calls == 1
        assert jin10.calls == 0
        assert secrets.calls == 0
    finally:
        provider.close()


def test_real_calendar_descriptor_preserves_feed_identity_and_waits_for_missing_precision() -> None:
    provenance = OfficialEventProvenance(
        source="Bureau of Labor Statistics",
        source_url="https://www.bls.gov/schedule/2026/home.htm",
        source_id="empsit-july",
        source_payload_hash="1" * 64,
        first_seen_at=RELEASE - timedelta(days=20),
        ingested_at=RELEASE - timedelta(days=1),
        observed_at=RELEASE - timedelta(days=1),
    )
    event = OfficialCalendarEvent(
        event_id="employment-july",
        source="Bureau of Labor Statistics",
        source_id="empsit-july",
        title="Employment Situation July 2026",
        scheduled_at=RELEASE,
        published_at=None,
        first_seen_at=RELEASE - timedelta(days=20),
        ingested_at=RELEASE - timedelta(days=1),
        observed_at=RELEASE - timedelta(days=1),
        source_url="https://www.bls.gov/schedule/2026/home.htm",
        category="MACRO",
        provenance=(provenance,),
    )
    descriptor = reaction_descriptor_from_calendar(event)
    assert descriptor.capture_eligible is True
    assert descriptor.reference_period == "2026-07"
    assert descriptor.calendar_feed_hash != descriptor.identity_hash
    assert descriptor.calendar_event_hash == event.content_hash

    fed_provenance = OfficialEventProvenance(
        source="Federal Reserve",
        source_url="https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm",
        source_id="meeting",
        source_payload_hash="2" * 64,
        first_seen_at=RELEASE - timedelta(days=20),
        ingested_at=RELEASE - timedelta(days=1),
        observed_at=RELEASE - timedelta(days=1),
    )
    date_only = OfficialCalendarEvent(
        event_id="fomc-date-only",
        source="Federal Reserve",
        source_id="meeting",
        title="FOMC Meeting (August 20-21, 2026)",
        scheduled_at=None,
        event_date=RELEASE.date(),
        timezone_name="America/New_York",
        schedule_precision="DATE_ONLY",
        published_at=None,
        first_seen_at=RELEASE - timedelta(days=20),
        ingested_at=RELEASE - timedelta(days=1),
        observed_at=RELEASE - timedelta(days=1),
        source_url="https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm",
        category="FOMC",
        provenance=(fed_provenance,),
    )
    waiting = reaction_descriptor_from_calendar(date_only)
    assert waiting.capture_eligible is False
    assert waiting.wait_reason == "WAIT_EXACT_RELEASE_TIME_UNAVAILABLE"


def test_v4_reaction_root_survives_new_calendar_observation_and_restart(tmp_path) -> None:
    def event_at(observed_at: datetime) -> OfficialCalendarEvent:
        provenance = OfficialEventProvenance(
            source="Bureau of Labor Statistics",
            source_url="https://www.bls.gov/schedule/2026/home.htm",
            source_id="empsit-july-root",
            source_payload_hash=canonical_hash(
                {"observed_at": observed_at.isoformat()}
            ),
            first_seen_at=RELEASE - timedelta(days=20),
            ingested_at=observed_at,
            observed_at=observed_at,
        )
        return OfficialCalendarEvent(
            event_id=f"employment-{observed_at:%H%M%S}",
            source=provenance.source,
            source_id=provenance.source_id,
            title="Employment Situation July 2026",
            scheduled_at=RELEASE,
            published_at=None,
            first_seen_at=RELEASE - timedelta(days=20),
            ingested_at=observed_at,
            observed_at=observed_at,
            source_url=provenance.source_url,
            category="MACRO",
            provenance=(provenance,),
        )

    path = tmp_path / "stable-reaction-root.sqlite3"
    first_event = event_at(RELEASE - timedelta(days=1))
    second_event = event_at(RELEASE - timedelta(hours=1))
    first = ProductionMacroReactionProvider(
        ReactionEvidenceStore(path),
        jin10_client=None,
        jin10_secret_store=None,
        official_actual_provider=object(),
    )
    try:
        first.restore_official_identities((first_event,))
        first_root = next(iter(first._state().identities.values())).event_hash
        first_identity_hash = next(
            iter(first._state().identities.values())
        ).content_hash
        first.restore_official_identities((second_event,))
        second_identity = first._state().identities[first_root]
        assert second_identity.event_hash == first_root
        assert second_identity.content_hash != first_identity_hash
        assert first_root == reaction_descriptor_from_calendar(second_event).stable_event_key
    finally:
        first.close()

    restarted = ProductionMacroReactionProvider(
        ReactionEvidenceStore(path),
        jin10_client=None,
        jin10_secret_store=None,
        official_actual_provider=object(),
    )
    try:
        restored_identity = restarted._state().identities[first_root]
        assert restored_identity.event_hash == first_root
        assert restarted.projection()["capture_spec_count"] == 1
    finally:
        restarted.close()


def test_schedule_correction_excludes_old_root_from_active_lanes_and_restart(
    tmp_path,
) -> None:
    def event_at(
        event_id: str,
        scheduled_at: datetime,
        observed_at: datetime,
    ) -> OfficialCalendarEvent:
        provenance = OfficialEventProvenance(
            source="Bureau of Labor Statistics",
            source_url="https://www.bls.gov/schedule/2026/home.htm",
            source_id="empsit-july-correction",
            source_payload_hash=canonical_hash(
                {
                    "event_id": event_id,
                    "scheduled_at": scheduled_at.isoformat(),
                    "observed_at": observed_at.isoformat(),
                }
            ),
            first_seen_at=RELEASE - timedelta(days=20),
            ingested_at=observed_at,
            observed_at=observed_at,
        )
        return OfficialCalendarEvent(
            event_id=event_id,
            source=provenance.source,
            source_id=provenance.source_id,
            title="Employment Situation July 2026",
            scheduled_at=scheduled_at,
            published_at=None,
            first_seen_at=RELEASE - timedelta(days=20),
            ingested_at=observed_at,
            observed_at=observed_at,
            source_url=provenance.source_url,
            category="MACRO",
            symbols=("SPY",),
            provenance=(provenance,),
        )

    class Observer:
        def __init__(self) -> None:
            self.ticked_roots: list[str] = []

        def tick(self, identity, *, now):
            self.ticked_roots.append(identity.event_hash)
            return False

        def observe(self, _ledger, *, now):
            raise AssertionError(f"inactive release replay called observer at {now}")

    path = tmp_path / "corrected-active-root.sqlite3"
    old_event = event_at(
        "employment-old-time",
        RELEASE,
        RELEASE - timedelta(days=2),
    )
    corrected_event = event_at(
        "employment-corrected-time",
        RELEASE + timedelta(minutes=1),
        RELEASE - timedelta(days=1),
    )
    observer = Observer()
    store = ReactionEvidenceStore(path)
    provider = ProductionMacroReactionProvider(
        store,
        jin10_client=None,
        jin10_secret_store=None,
        official_actual_provider=object(),
        reaction_observer=observer,
        clock=lambda: RELEASE + timedelta(minutes=5),
    )
    try:
        provider.restore_official_identities((old_event,))
        old_root = reaction_descriptor_from_calendar(old_event).stable_event_key
        store.append(
            event_id=old_root,
            official_event_hash=old_root,
            kind="JIN10_REPORTED_ACTUAL",
            observed_at=RELEASE + timedelta(seconds=1),
            document={"schema": "retained-old-evidence.v1"},
        )

        # Retained-history input deliberately includes both rows and puts the
        # correction first. observed_at, not tuple order, selects the active root.
        provider.restore_official_identities((corrected_event, old_event))
        corrected_root = reaction_descriptor_from_calendar(
            corrected_event
        ).stable_event_key
        state = provider._state()
        assert set(state.identities) == {corrected_root}
        assert set(state.specs) == {corrected_root}
        assert set(state.descriptors) == {corrected_root}
        assert old_root not in state.stable_to_public
        assert provider.projection()["lifecycle_supersessions"][
            corrected_root
        ] == old_root

        provider.observe_local(now=RELEASE + timedelta(minutes=5))
        assert observer.ticked_roots == [corrected_root]
        assert tuple(provider.reactions((old_event.event_id,))) == ()
        assert store.records(((old_root, old_root),))[0]["kind"] == (
            "JIN10_REPORTED_ACTUAL"
        )
    finally:
        provider.close()

    restarted = ProductionMacroReactionProvider(
        ReactionEvidenceStore(path),
        jin10_client=None,
        jin10_secret_store=None,
        official_actual_provider=object(),
    )
    try:
        state = restarted._state()
        assert set(state.identities) == {corrected_root}
        assert old_root not in state.specs
        assert restarted.projection()["lifecycle_supersessions"][
            corrected_root
        ] == old_root
        assert restarted._store.records(((old_root, old_root),))[0]["kind"] == (
            "JIN10_REPORTED_ACTUAL"
        )
    finally:
        restarted.close()


def test_provider_schedule_swaps_publish_one_immutable_consistent_snapshot(
    tmp_path,
) -> None:
    def event(event_id: str, title: str, scheduled_at: datetime):
        return SimpleNamespace(
            event_id=event_id,
            source="Bureau of Labor Statistics",
            source_id=event_id,
            title=title,
            category="MACRO",
            scheduled_at=scheduled_at,
            published_at=scheduled_at - timedelta(days=30),
            first_seen_at=scheduled_at - timedelta(days=20),
            observed_at=scheduled_at - timedelta(days=1),
            symbols=("SPY",),
        )

    first = event("cpi-july", "Consumer Price Index July 2026", RELEASE)
    second = event(
        "employment-july",
        "Employment Situation July 2026",
        RELEASE + timedelta(days=14),
    )
    provider = ProductionMacroReactionProvider(
        ReactionEvidenceStore(tmp_path / "atomic-provider-state.sqlite3"),
        jin10_client=None,
        jin10_secret_store=None,
        official_actual_provider=object(),
        clock=lambda: RELEASE - timedelta(minutes=1),
    )
    failures: list[BaseException] = []

    def writer() -> None:
        try:
            for index in range(100):
                provider.restore_official_identities(
                    (first,) if index % 2 == 0 else (first, second)
                )
        except BaseException as exc:  # pragma: no cover - asserted below
            failures.append(exc)

    def reader() -> None:
        try:
            for _ in range(200):
                state = provider._state()
                assert set(state.specs).issubset(state.identities)
                assert set(state.public_to_stable.values()).issubset(
                    state.identities
                )
                for stable_key, spec in state.specs.items():
                    assert spec.official_event_hash == state.identities[
                        stable_key
                    ].event_hash
                with pytest.raises(TypeError):
                    state.identities["partial"] = state.identities[stable_key]
                provider.coverage(tuple(state.public_to_stable))
                tuple(provider.reactions(tuple(state.public_to_stable)))
        except BaseException as exc:  # pragma: no cover - asserted below
            failures.append(exc)

    provider.restore_official_identities((first,))
    workers = [threading.Thread(target=writer), threading.Thread(target=reader)]
    try:
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join(timeout=10)
        assert all(not worker.is_alive() for worker in workers)
        assert failures == []
    finally:
        provider.close()


def test_schedule_correction_waits_for_inflight_observer_generation(tmp_path) -> None:
    entered = threading.Event()
    release = threading.Event()
    corrected = threading.Event()
    ticked: list[str] = []

    class Observer:
        def tick(self, identity, *, now):
            ticked.append(identity.event_hash)
            if len(ticked) == 1:
                entered.set()
                assert release.wait(timeout=5)
            return False

        def observe(self, _ledger, *, now):
            return None, None

    old_event = _official_employment_event(
        "employment-old",
        scheduled_at=RELEASE,
        observed_at=RELEASE - timedelta(days=2),
    )
    new_event = _official_employment_event(
        "employment-corrected",
        scheduled_at=RELEASE + timedelta(minutes=1),
        observed_at=RELEASE - timedelta(days=1),
    )
    provider = ProductionMacroReactionProvider(
        ReactionEvidenceStore(tmp_path / "observer-generation.sqlite3"),
        jin10_client=None,
        jin10_secret_store=None,
        official_actual_provider=object(),
        reaction_observer=Observer(),
    )
    provider.restore_official_identities((old_event,))
    old_root = reaction_descriptor_from_calendar(old_event).stable_event_key
    new_root = reaction_descriptor_from_calendar(new_event).stable_event_key
    observer_thread = threading.Thread(
        target=provider.observe_local,
        kwargs={"now": RELEASE},
    )

    def correct() -> None:
        provider.restore_official_identities((new_event, old_event))
        corrected.set()

    correction_thread = threading.Thread(target=correct)
    try:
        observer_thread.start()
        assert entered.wait(timeout=5)
        correction_thread.start()
        assert corrected.wait(timeout=0.1) is False
        release.set()
        observer_thread.join(timeout=5)
        correction_thread.join(timeout=5)
        assert corrected.is_set()
        assert ticked == [old_root]
        provider.observe_local(now=RELEASE + timedelta(minutes=1))
        assert ticked == [old_root, new_root]
    finally:
        release.set()
        observer_thread.join(timeout=5)
        correction_thread.join(timeout=5)
        provider.close()


def test_schedule_correction_waits_for_inflight_capture_generation(tmp_path) -> None:
    entered = threading.Event()
    release = threading.Event()
    corrected = threading.Event()
    captured_roots: list[str] = []

    class Capture:
        def capture(self, request, *, now):
            captured_roots.append(request.official_event_hash)
            if len(captured_roots) == 1:
                entered.set()
                assert release.wait(timeout=5)
            raise RuntimeError("OFFICIAL_RELEASE_DOCUMENT_UNAVAILABLE")

    old_event = _official_employment_event(
        "employment-old",
        scheduled_at=RELEASE,
        observed_at=RELEASE - timedelta(days=2),
    )
    new_event = _official_employment_event(
        "employment-corrected",
        scheduled_at=RELEASE + timedelta(minutes=1),
        observed_at=RELEASE - timedelta(days=1),
    )
    provider = ProductionMacroReactionProvider(
        ReactionEvidenceStore(tmp_path / "capture-generation.sqlite3"),
        jin10_client=None,
        jin10_secret_store=None,
        official_actual_provider=object(),
        official_document_provider=Capture(),
    )
    provider.restore_official_identities((old_event,))
    old_root = reaction_descriptor_from_calendar(old_event).stable_event_key
    new_root = reaction_descriptor_from_calendar(new_event).stable_event_key
    capture_thread = threading.Thread(
        target=provider.refresh_capture,
        kwargs={"now": RELEASE + timedelta(seconds=1)},
    )

    def correct() -> None:
        provider.restore_official_identities((new_event, old_event))
        corrected.set()

    correction_thread = threading.Thread(target=correct)
    try:
        capture_thread.start()
        assert entered.wait(timeout=5)
        correction_thread.start()
        assert corrected.wait(timeout=0.1) is False
        release.set()
        capture_thread.join(timeout=5)
        correction_thread.join(timeout=5)
        assert corrected.is_set()
        assert captured_roots == [old_root]
        provider.refresh_capture(now=RELEASE + timedelta(minutes=1, seconds=1))
        assert captured_roots == [old_root, new_root]
    finally:
        release.set()
        capture_thread.join(timeout=5)
        correction_thread.join(timeout=5)
        provider.close()


def test_t_plus_five_observer_uses_only_fresh_cache_while_broker_refresh_blocks(
    tmp_path,
) -> None:
    broker_entered = threading.Event()
    broker_release = threading.Event()
    cached_at = {"value": RELEASE + timedelta(minutes=4, seconds=50)}

    class Adapter:
        broker_calls = 0

        def bindings(self, _symbols):
            self.broker_calls += 1
            broker_entered.set()
            assert broker_release.wait(timeout=5)
            return ()

        def cached_reaction_underlying_quotes(self, _symbols):
            return _underlying_basket(cached_at["value"])

    identity = ScheduledEventIdentity(
        "cache-only-endpoint",
        "BLS",
        "cache-only-endpoint",
        "Employment Situation",
        "MACRO",
        RELEASE,
        RELEASE - timedelta(days=30),
        RELEASE - timedelta(days=20),
        RELEASE - timedelta(days=1),
        ("SPY",),
    )
    adapter = Adapter()
    observer = ProductionReactionObserver(
        adapter,
        SimpleNamespace(ready=True),
        store=ReactionEvidenceStore(tmp_path / "cache-only-observer.sqlite3"),
    )
    broker_thread = threading.Thread(target=adapter.bindings, args=(("SPY",),))
    broker_thread.start()
    try:
        assert broker_entered.wait(timeout=5)
        observed = threading.Event()

        def tick() -> None:
            assert observer.tick(
                identity,
                now=RELEASE + timedelta(minutes=5),
            ) is False
            observed.set()

        observer_thread = threading.Thread(target=tick)
        observer_thread.start()
        observer_thread.join(timeout=0.25)
        assert observed.is_set(), "observer blocked behind broker refresh"
        assert adapter.broker_calls == 1
        cached_at["value"] = RELEASE + timedelta(minutes=5, seconds=2)
        assert observer.tick(
            identity,
            now=RELEASE + timedelta(minutes=5, seconds=2),
        ) is True
        assert adapter.broker_calls == 1
    finally:
        broker_release.set()
        broker_thread.join(timeout=5)
        observer._store.close()


def test_gate3_positive_schema_excludes_all_reaction_lineage() -> None:
    base = {
        "id": "macro-1",
        "event_id": "macro-1",
        "title": "Employment Situation",
        "scheduled_at": RELEASE.isoformat(),
        "source": "BLS",
        "record_hash": "1" * 64,
        "evidence": [{"content_hash": "2" * 64}],
    }
    reaction_overlay = {
        "reaction": {"record_hash": "3" * 64},
        "measure_reactions": [{"reaction_hash": "4" * 64}],
        "revision_views": [{"revision_hash": "5" * 64}],
        "reaction_identity_hash": "6" * 64,
        "reaction_identity_provenance": {"calendar_record_hash": "7" * 64},
        "intelligence": {
            "reaction": {"reaction_hash": "8" * 64},
            "surprise": {"surprise_hash": "9" * 64},
            "intelligence_hash": "a" * 64,
        },
        "future_reaction_lineage": {"head_hash": "b" * 64},
    }
    assert _deterministic_decision_calendar_row(base) == (
        _deterministic_decision_calendar_row({**base, **reaction_overlay})
    )


def test_same_root_older_observation_cannot_replace_active_head_or_restart(
    tmp_path,
) -> None:
    newer = _official_employment_event(
        "employment-newer-observation",
        scheduled_at=RELEASE,
        observed_at=RELEASE - timedelta(hours=1),
    )
    older = _official_employment_event(
        "employment-older-observation",
        scheduled_at=RELEASE,
        observed_at=RELEASE - timedelta(hours=2),
    )
    path = tmp_path / "same-root-active-head.sqlite3"
    provider = ProductionMacroReactionProvider(
        ReactionEvidenceStore(path),
        jin10_client=None,
        jin10_secret_store=None,
        official_actual_provider=object(),
    )
    root = reaction_descriptor_from_calendar(newer).stable_event_key
    try:
        assert reaction_descriptor_from_calendar(older).stable_event_key == root
        provider.restore_official_identities((newer,))
        provider.restore_official_identities((older,))
        assert provider._state().identities[root].schedule_observed_at == (
            newer.observed_at
        )
    finally:
        provider.close()
    restarted = ProductionMacroReactionProvider(
        ReactionEvidenceStore(path),
        jin10_client=None,
        jin10_secret_store=None,
        official_actual_provider=object(),
    )
    try:
        assert restarted._state().identities[root].schedule_observed_at == (
            newer.observed_at
        )
    finally:
        restarted.close()


def test_same_root_stale_refresh_preserves_complete_projection_snapshot(
    tmp_path,
) -> None:
    newer = _official_employment_event(
        "employment-current",
        scheduled_at=RELEASE,
        observed_at=RELEASE - timedelta(minutes=1),
    )
    stale = _official_employment_event(
        "employment-stale",
        scheduled_at=RELEASE,
        observed_at=RELEASE - timedelta(minutes=2),
    )
    provider = ProductionMacroReactionProvider(
        ReactionEvidenceStore(tmp_path / "same-root-full-state.sqlite3"),
        jin10_client=None,
        jin10_secret_store=None,
        official_actual_provider=object(),
        clock=lambda: RELEASE - timedelta(seconds=30),
    )
    try:
        provider.refresh_schedule_with_health(
            SimpleNamespace(events=(newer,)),
            now=RELEASE - timedelta(seconds=30),
            status="READY",
            reason=None,
            schedule_hash="c" * 64,
        )
        provider.record_worker_failure(
            "OBSERVER",
            now=RELEASE - timedelta(seconds=20),
            reason="TEST_OBSERVER_DEGRADED",
        )
        before_state = provider._state()
        before_projection = provider.projection()
        provider.refresh_schedule_with_health(
            SimpleNamespace(events=(stale,)),
            now=RELEASE - timedelta(seconds=10),
            status="UNAVAILABLE",
            reason="STALE_MUST_NOT_REPLACE",
            schedule_hash="d" * 64,
        )
        assert provider._state() == before_state
        assert provider.projection() == before_projection
        assert "employment-stale" not in provider._state().public_to_stable
    finally:
        provider.close()


def test_schedule_storage_failure_rolls_back_and_retains_live_generation(
    tmp_path,
    monkeypatch,
) -> None:
    old_event = _official_employment_event(
        "employment-old",
        scheduled_at=RELEASE,
        observed_at=RELEASE - timedelta(days=2),
    )
    corrected = _official_employment_event(
        "employment-corrected",
        scheduled_at=RELEASE + timedelta(minutes=1),
        observed_at=RELEASE - timedelta(days=1),
    )
    store = ReactionEvidenceStore(tmp_path / "atomic-schedule-failure.sqlite3")
    provider = ProductionMacroReactionProvider(
        store,
        jin10_client=None,
        jin10_secret_store=None,
        official_actual_provider=object(),
    )
    try:
        provider.refresh_schedule_with_health(
            SimpleNamespace(events=(old_event,)),
            now=RELEASE - timedelta(minutes=5),
            status="READY",
            reason=None,
            schedule_hash="a" * 64,
        )
        prior_state = provider._state()
        prior_projection = provider.projection()
        schedule_count = store._db.execute(
            "SELECT COUNT(*) FROM macro_reaction_schedules"
        ).fetchone()[0]
        cache_count = store._db.execute(
            "SELECT COUNT(*) FROM macro_reaction_schedule_cache"
        ).fetchone()[0]

        def fail_cache(*_args, **_kwargs):
            raise OSError("simulated cache persistence failure")

        monkeypatch.setattr(store, "save_schedule_cache", fail_cache)
        with pytest.raises(OSError, match="simulated cache persistence failure"):
            provider.refresh_schedule_with_health(
                SimpleNamespace(events=(corrected, old_event)),
                now=RELEASE - timedelta(minutes=4),
                status="READY",
                reason=None,
                schedule_hash="b" * 64,
            )
        assert provider._state() == prior_state
        assert provider.projection() == prior_projection
        assert store._db.execute(
            "SELECT COUNT(*) FROM macro_reaction_schedules"
        ).fetchone()[0] == schedule_count
        assert store._db.execute(
            "SELECT COUNT(*) FROM macro_reaction_schedule_cache"
        ).fetchone()[0] == cache_count
        store.assert_integrity()
    finally:
        provider.close()


def test_schedule_storage_failure_drains_inflight_old_activity_before_raising(
    tmp_path,
    monkeypatch,
) -> None:
    entered = threading.Event()
    release = threading.Event()
    correction_done = threading.Event()
    failures: list[BaseException] = []

    class Observer:
        def tick(self, _identity, *, now):
            entered.set()
            assert release.wait(timeout=5)
            return False

        def observe(self, _ledger, *, now):
            return None, None

    old_event = _official_employment_event(
        "employment-old-storage-drain",
        scheduled_at=RELEASE,
        observed_at=RELEASE - timedelta(days=2),
    )
    corrected = _official_employment_event(
        "employment-new-storage-drain",
        scheduled_at=RELEASE + timedelta(minutes=1),
        observed_at=RELEASE - timedelta(days=1),
    )
    store = ReactionEvidenceStore(tmp_path / "storage-failure-drain.sqlite3")
    provider = ProductionMacroReactionProvider(
        store,
        jin10_client=None,
        jin10_secret_store=None,
        official_actual_provider=object(),
        reaction_observer=Observer(),
    )
    provider.restore_official_identities((old_event,))
    prior_state = provider._state()

    def fail_generation(*_args, **_kwargs):
        raise OSError("simulated atomic generation failure")

    monkeypatch.setattr(store, "save_schedule_generation", fail_generation)
    observer_thread = threading.Thread(
        target=provider.observe_local,
        kwargs={"now": RELEASE},
    )

    def correct() -> None:
        try:
            provider.restore_official_identities((corrected, old_event))
        except BaseException as exc:
            failures.append(exc)
        finally:
            correction_done.set()

    correction_thread = threading.Thread(target=correct)
    try:
        observer_thread.start()
        assert entered.wait(timeout=5)
        correction_thread.start()
        assert correction_done.wait(timeout=0.1) is False
        assert provider._state() == prior_state
        release.set()
        observer_thread.join(timeout=5)
        correction_thread.join(timeout=5)
        assert correction_done.is_set()
        assert len(failures) == 1
        assert isinstance(failures[0], OSError)
        assert str(failures[0]) == "simulated atomic generation failure"
        assert provider._state() == prior_state
    finally:
        release.set()
        observer_thread.join(timeout=5)
        correction_thread.join(timeout=5)
        provider.close()


def test_fresh_same_root_public_id_replaces_alias_live_restart_and_api(
    tmp_path,
) -> None:
    older = _official_employment_event(
        "employment-public-old",
        scheduled_at=RELEASE,
        observed_at=RELEASE - timedelta(hours=2),
    )
    newer = _official_employment_event(
        "employment-public-new",
        scheduled_at=RELEASE,
        observed_at=RELEASE - timedelta(hours=1),
    )
    path = tmp_path / "same-root-public-alias.sqlite3"
    provider = ProductionMacroReactionProvider(
        ReactionEvidenceStore(path),
        jin10_client=None,
        jin10_secret_store=None,
        official_actual_provider=object(),
    )
    try:
        provider.refresh_schedule_with_health(
            SimpleNamespace(events=(older,)),
            now=RELEASE - timedelta(hours=2),
            status="READY",
            reason=None,
            schedule_hash="a" * 64,
        )
        provider.refresh_schedule_with_health(
            SimpleNamespace(events=(newer,)),
            now=RELEASE - timedelta(hours=1),
            status="DEGRADED",
            reason="LATEST_GENERATION",
            schedule_hash="b" * 64,
        )
        live = provider._state()
        assert older.event_id not in live.public_to_stable
        assert tuple(live.public_to_stable) == (newer.event_id,)
        live_api = _normalise_reaction_provider(provider.projection())
    finally:
        provider.close()
    restarted = ProductionMacroReactionProvider(
        ReactionEvidenceStore(path),
        jin10_client=None,
        jin10_secret_store=None,
        official_actual_provider=object(),
    )
    try:
        restarted_state = restarted._state()
        assert older.event_id not in restarted_state.public_to_stable
        assert tuple(restarted_state.public_to_stable) == (newer.event_id,)
        assert _normalise_reaction_provider(restarted.projection()) == live_api
    finally:
        restarted.close()


def test_generation_authority_restores_latest_global_health_not_root_order(
    tmp_path,
) -> None:
    retained = _official_employment_event(
        "employment-retained",
        scheduled_at=RELEASE,
        observed_at=RELEASE - timedelta(hours=3),
    )
    fresh = _official_employment_event(
        "employment-fresh-root",
        scheduled_at=RELEASE + timedelta(minutes=1),
        observed_at=RELEASE - timedelta(hours=1),
        source_id="consumer-price-index-2026-07",
        title="Consumer Price Index July 2026",
    )
    stale_retained = _official_employment_event(
        "employment-retained-stale-alias",
        scheduled_at=RELEASE,
        observed_at=RELEASE - timedelta(hours=4),
    )
    unsupported = SimpleNamespace(
        event_id="unsupported-generation-row",
        source="Unknown Publisher",
        source_id="unsupported-generation-row",
        title="Unclassified event",
        category="OTHER",
        scheduled_at=RELEASE + timedelta(hours=1),
        published_at=None,
        first_seen_at=RELEASE - timedelta(days=1),
        observed_at=RELEASE - timedelta(hours=1),
        symbols=("SPY",),
    )
    path = tmp_path / "generation-authority-order.sqlite3"
    provider = ProductionMacroReactionProvider(
        ReactionEvidenceStore(path),
        jin10_client=None,
        jin10_secret_store=None,
        official_actual_provider=object(),
    )
    try:
        provider.refresh_schedule_with_health(
            SimpleNamespace(events=(retained,)),
            now=RELEASE - timedelta(hours=3),
            status="READY",
            reason="OLDER_GLOBAL_HEALTH",
            schedule_hash="a" * 64,
        )
        provider.refresh_schedule_with_health(
            SimpleNamespace(events=(stale_retained, fresh, unsupported)),
            now=RELEASE - timedelta(minutes=30),
            status="DEGRADED",
            reason="LATEST_GLOBAL_HEALTH",
            schedule_hash="f" * 64,
        )
        live_projection = provider.projection()
        assert live_projection["schedule_refresh_reason"] == "LATEST_GLOBAL_HEALTH"
        assert live_projection["schedule_hash"] == "f" * 64
        authority = provider._store.load_schedule_generation_authority()
        assert authority is not None
        assert authority["observed_at"] == (
            RELEASE - timedelta(minutes=30)
        ).isoformat()
        assert authority["root_set_hash"] == canonical_hash(
            tuple(authority["active_roots"])
        )
    finally:
        provider.close()
    restarted = ProductionMacroReactionProvider(
        ReactionEvidenceStore(path),
        jin10_client=None,
        jin10_secret_store=None,
        official_actual_provider=object(),
    )
    try:
        restarted_projection = restarted.projection()
        assert restarted_projection["schedule_refresh_status"] == "DEGRADED"
        assert restarted_projection["schedule_refresh_reason"] == (
            "LATEST_GLOBAL_HEALTH"
        )
        assert restarted_projection["schedule_hash"] == "f" * 64
        assert restarted_projection["unsupported_count"] == 1
        assert set(restarted_projection["supported_event_ids"]) == {
            retained.event_id,
            fresh.event_id,
        }
    finally:
        restarted.close()


def test_generation_authority_corruption_fails_closed_on_restart(tmp_path) -> None:
    event = _official_employment_event(
        "employment-authority-corrupt",
        scheduled_at=RELEASE,
        observed_at=RELEASE - timedelta(hours=1),
    )
    path = tmp_path / "generation-authority-corrupt.sqlite3"
    provider = ProductionMacroReactionProvider(
        ReactionEvidenceStore(path),
        jin10_client=None,
        jin10_secret_store=None,
        official_actual_provider=object(),
    )
    provider.restore_official_identities((event,))
    provider.close()
    db = sqlite3.connect(path)
    try:
        db.execute("DROP TRIGGER macro_reaction_schedule_generation_no_update")
        db.execute(
            "UPDATE macro_reaction_schedule_generations SET row_hash=? WHERE sequence=1",
            ("0" * 64,),
        )
        db.commit()
    finally:
        db.close()
    with pytest.raises(MacroReactionError, match="REACTION_CACHE_INTEGRITY_FAILED"):
        ReactionEvidenceStore(path)


def test_legacy_schedule_generation_restarts_with_live_parity_and_no_attempt(
    tmp_path,
) -> None:
    legacy = SimpleNamespace(
        event_id="legacy-employment",
        source="Bureau of Labor Statistics",
        source_id="legacy-employment-2026-07",
        source_url="https://www.bls.gov/schedule/2026/home.htm",
        title="Employment Situation July 2026",
        category="MACRO",
        scheduled_at=RELEASE,
        published_at=RELEASE - timedelta(days=30),
        first_seen_at=RELEASE - timedelta(days=20),
        ingested_at=RELEASE - timedelta(hours=1),
        observed_at=RELEASE - timedelta(hours=1),
        symbols=("SPY",),
    )
    path = tmp_path / "legacy-generation-restart.sqlite3"
    provider = ProductionMacroReactionProvider(
        ReactionEvidenceStore(path),
        jin10_client=None,
        jin10_secret_store=None,
        official_actual_provider=object(),
    )
    try:
        provider.restore_official_identities((legacy,))
        live = provider.projection()
        assert live["supported_event_ids"] == [legacy.event_id]
        assert live["eligible_event_ids"] == [legacy.event_id]
        assert live["last_attempt"] is None
    finally:
        provider.close()

    restarted = ProductionMacroReactionProvider(
        ReactionEvidenceStore(path),
        jin10_client=None,
        jin10_secret_store=None,
        official_actual_provider=object(),
    )
    try:
        restored = restarted.projection()
        assert restored["supported_event_ids"] == live["supported_event_ids"]
        assert restored["eligible_event_ids"] == live["eligible_event_ids"]
        assert restored["supported_count"] == live["supported_count"]
        assert restored["eligible_count"] == live["eligible_count"]
        assert restored["last_attempt"] is None
    finally:
        restarted.close()


def test_store_generation_health_has_one_authority_source_and_restarts(
    tmp_path,
) -> None:
    event = _official_employment_event(
        "employment-direct-generation-authority",
        scheduled_at=RELEASE,
        observed_at=RELEASE - timedelta(hours=1),
    )
    descriptor = reaction_descriptor_from_calendar(event)
    assert descriptor.parent is not None
    identity = ScheduledEventIdentity(
        event_id=descriptor.stable_event_key,
        official_source=event.source,
        official_source_id=event.source_id,
        title=event.title,
        category=event.category,
        scheduled_at=event.scheduled_at,
        schedule_published_at=event.published_at,
        schedule_first_seen_at=event.first_seen_at,
        schedule_observed_at=event.observed_at,
        symbols=event.symbols,
        reaction_root_hash=descriptor.stable_event_key,
    )
    spec = ScheduledReactionSpec(
        descriptor.parent,
        identity.scheduled_at,
        identity.event_hash,
        identity.scheduled_at + timedelta(minutes=15),
    )
    row = (
        descriptor.stable_event_key,
        descriptor,
        spec,
        event.event_id,
        identity,
        event.observed_at,
        None,
    )
    active_root = {
        "stable_event_key": descriptor.stable_event_key,
        "public_event_id": event.event_id,
        "official_event_hash": identity.event_hash,
        "cache_backed": True,
        "identity": identity.as_dict(),
        "spec": {
            "parent": {
                "publisher": spec.parent.publisher,
                "family": spec.parent.family.value,
                "reference_period": spec.parent.reference_period,
                "scheduled_date": spec.parent.scheduled_date.isoformat(),
                "estimate_label": spec.parent.estimate_label,
            },
            "scheduled_at": spec.scheduled_at.isoformat(),
            "official_event_hash": spec.official_event_hash,
            "capture_deadline": spec.capture_deadline.isoformat(),
            "next_eligible_release_at": None,
        },
    }
    path = tmp_path / "direct-generation-authority.sqlite3"
    store = ReactionEvidenceStore(path)
    with pytest.raises(ValueError):
        store.save_schedule_generation(
            (row + ("READY", "ROW_LEVEL_DRIFT", "a" * 64),),
            observed_at=RELEASE - timedelta(minutes=30),
            schedule_status="DEGRADED",
            schedule_reason="GENERATION_AUTHORITY",
            schedule_hash="b" * 64,
            unsupported_count=2,
            active_roots=(active_root,),
            attempted_at=RELEASE - timedelta(minutes=30),
        )
    store.save_schedule_generation(
        (row,),
        observed_at=RELEASE - timedelta(minutes=30),
        schedule_status="DEGRADED",
        schedule_reason="GENERATION_AUTHORITY",
        schedule_hash="b" * 64,
        unsupported_count=2,
        active_roots=(active_root,),
        attempted_at=RELEASE - timedelta(minutes=30),
    )
    cache_document = store.load_schedule_cache()[0]["document"]
    authority = store.load_schedule_generation_authority()
    assert authority is not None
    for document in (cache_document, authority):
        assert document["schedule_status"] == "DEGRADED"
        assert document["schedule_reason"] == "GENERATION_AUTHORITY"
        assert document["schedule_hash"] == "b" * 64
    store.close()
    restarted = ProductionMacroReactionProvider(
        ReactionEvidenceStore(path),
        jin10_client=None,
        jin10_secret_store=None,
        official_actual_provider=object(),
    )
    try:
        projection = restarted.projection()
        assert projection["schedule_refresh_status"] == "DEGRADED"
        assert projection["schedule_refresh_reason"] == "GENERATION_AUTHORITY"
        assert projection["schedule_hash"] == "b" * 64
        assert projection["unsupported_count"] == 2
    finally:
        restarted.close()


def test_pending_sampler_symbols_use_only_strict_missing_sample_windows(
    tmp_path,
) -> None:
    event = _official_employment_event(
        "employment-sampler",
        scheduled_at=RELEASE,
        observed_at=RELEASE - timedelta(days=1),
    )
    store = ReactionEvidenceStore(tmp_path / "pending-sampler.sqlite3")
    provider = ProductionMacroReactionProvider(
        store,
        jin10_client=None,
        jin10_secret_store=None,
        official_actual_provider=object(),
    )
    try:
        provider.restore_official_identities((event,))
        identity = next(iter(provider._state().identities.values()))
        assert provider.pending_reaction_sample_symbols(
            now=RELEASE - timedelta(seconds=6)
        ) == ()
        assert provider.pending_reaction_sample_symbols(
            now=RELEASE - timedelta(seconds=5)
        ) == MACRO_SYMBOLS
        store.save_baseline(
            identity,
            MarketSample(RELEASE - timedelta(seconds=1), {"SPY": Decimal("600")}),
        )
        assert provider.pending_reaction_sample_symbols(
            now=RELEASE - timedelta(seconds=1)
        ) == ()
        assert provider.pending_reaction_sample_symbols(
            now=RELEASE + timedelta(minutes=4, seconds=59)
        ) == ()
        assert provider.pending_reaction_sample_symbols(
            now=RELEASE + timedelta(minutes=5)
        ) == MACRO_SYMBOLS
        store.save_endpoint(
            identity,
            MarketSample(
                RELEASE + timedelta(minutes=5, seconds=1),
                {"SPY": Decimal("601")},
            ),
        )
        assert provider.pending_reaction_sample_symbols(
            now=RELEASE + timedelta(minutes=5, seconds=2)
        ) == ()
    finally:
        provider.close()


def test_sampler_loop_persists_one_fresh_quote_and_stops_requesting_root(
    tmp_path,
    monkeypatch,
) -> None:
    current = {"now": RELEASE - timedelta(seconds=4)}

    class Adapter:
        def __init__(self) -> None:
            self.calls = 0
            self.cached = ()

        def reaction_underlying_quotes(self, symbols):
            self.calls += 1
            observed_at = current["now"] + timedelta(milliseconds=100)
            current["now"] = observed_at + timedelta(milliseconds=100)
            self.cached = _underlying_basket(observed_at)
            return self.cached

        def cached_reaction_underlying_quotes(self, _symbols):
            return self.cached

    event = _official_employment_event(
        "employment-sampler-loop",
        scheduled_at=RELEASE,
        observed_at=RELEASE - timedelta(days=1),
    )
    store = ReactionEvidenceStore(tmp_path / "sampler-loop.sqlite3")
    adapter = Adapter()
    observer = ProductionReactionObserver(
        adapter,
        SimpleNamespace(ready=True),
        store=store,
    )
    provider = ProductionMacroReactionProvider(
        store,
        jin10_client=None,
        jin10_secret_store=None,
        official_actual_provider=object(),
        reaction_observer=observer,
        clock=lambda: current["now"],
    )
    provider.restore_official_identities((event,))
    coordinator = NewsCoordinator(
        tmp_path / "sampler-loop-news.sqlite3",
        reaction_provider=provider,
        ibkr_binding_provider=adapter,
        clock=lambda: current["now"],
    )
    original_pending = provider.pending_reaction_sample_symbols
    pending_calls = 0

    def pending(*, now):
        nonlocal pending_calls
        pending_calls += 1
        result = original_pending(now=now)
        if pending_calls >= 2:
            coordinator._stop.set()
        return result

    monkeypatch.setattr(provider, "pending_reaction_sample_symbols", pending)
    monkeypatch.setattr(
        news_runtime_module,
        "_REACTION_QUOTE_SAMPLE_POLL_SECONDS",
        0.01,
    )
    try:
        coordinator._stop.clear()
        coordinator._poll_reaction_quote_sampler()
        identity = next(iter(provider._state().identities.values()))
        assert adapter.calls == 1
        assert pending_calls == 2
        assert store.load_baseline(
            identity.event_hash,
            scheduled_at=identity.scheduled_at,
        ) is not None
        assert original_pending(now=current["now"]) == ()
    finally:
        coordinator.close()
        provider.close()


def test_sampler_future_quote_fails_closed_with_window_bounded_requests(
    tmp_path,
    monkeypatch,
) -> None:
    current = {"now": RELEASE - timedelta(seconds=5)}

    class Adapter:
        def __init__(self) -> None:
            self.calls = 0
            self.cached = ()

        def reaction_underlying_quotes(self, symbols):
            self.calls += 1
            observed_at = RELEASE + timedelta(seconds=30)
            self.cached = _underlying_basket(observed_at)
            current["now"] += timedelta(seconds=1)
            return self.cached

        def cached_reaction_underlying_quotes(self, _symbols):
            return self.cached

    event = _official_employment_event(
        "employment-future-sampler",
        scheduled_at=RELEASE,
        observed_at=RELEASE - timedelta(days=1),
    )
    store = ReactionEvidenceStore(tmp_path / "future-sampler.sqlite3")
    adapter = Adapter()
    observer = ProductionReactionObserver(
        adapter,
        SimpleNamespace(ready=True),
        store=store,
    )
    provider = ProductionMacroReactionProvider(
        store,
        jin10_client=None,
        jin10_secret_store=None,
        official_actual_provider=object(),
        reaction_observer=observer,
        clock=lambda: current["now"],
    )
    provider.restore_official_identities((event,))
    coordinator = NewsCoordinator(
        tmp_path / "future-sampler-news.sqlite3",
        reaction_provider=provider,
        ibkr_binding_provider=adapter,
        clock=lambda: current["now"],
    )
    original_pending = provider.pending_reaction_sample_symbols
    pending_calls = 0

    def pending(*, now):
        nonlocal pending_calls
        pending_calls += 1
        result = original_pending(now=now)
        if pending_calls >= 8:
            coordinator._stop.set()
        return result

    monkeypatch.setattr(provider, "pending_reaction_sample_symbols", pending)
    monkeypatch.setattr(
        news_runtime_module,
        "_REACTION_QUOTE_SAMPLE_POLL_SECONDS",
        0.01,
    )
    try:
        coordinator._stop.clear()
        coordinator._poll_reaction_quote_sampler()
        identity = next(iter(provider._state().identities.values()))
        assert adapter.calls == 1
        assert pending_calls == 8
        assert store.load_baseline(
            identity.event_hash,
            scheduled_at=identity.scheduled_at,
        ) is None
    finally:
        coordinator.close()
        provider.close()


def test_reaction_observer_rejects_option_mids_and_partial_underlying_basket(
    tmp_path,
) -> None:
    class Adapter:
        def cached_bindings(self, _symbols):
            return tuple(
                SimpleNamespace(
                    symbol=symbol,
                    confirmation=SimpleNamespace(observed_at=RELEASE),
                    tradability=SimpleNamespace(
                        bid=Decimal("99"),
                        ask=Decimal("101"),
                    ),
                )
                for symbol in MACRO_SYMBOLS
            )

        def cached_reaction_underlying_quotes(self, _symbols):
            return _underlying_basket(RELEASE)[:-1]

    identity = ScheduledEventIdentity(
        "underlying-basket-required",
        "BLS",
        "underlying-basket-required",
        "Employment Situation",
        "MACRO",
        RELEASE,
        RELEASE - timedelta(days=30),
        RELEASE - timedelta(days=20),
        RELEASE - timedelta(days=1),
        ("SPY",),
    )
    store = ReactionEvidenceStore(tmp_path / "underlying-basket-required.sqlite3")
    observer = ProductionReactionObserver(
        Adapter(),
        SimpleNamespace(ready=True),
        store=store,
    )
    try:
        assert observer.arm(identity, now=RELEASE) is False
        assert store.load_baseline(
            identity.event_hash,
            scheduled_at=identity.scheduled_at,
        ) is None
    finally:
        store.close()


def test_old_capture_generation_cannot_publish_display_state_after_swap(
    tmp_path,
) -> None:
    entered = threading.Event()
    release = threading.Event()

    class Capture:
        def capture(self, _request, *, now):
            entered.set()
            assert release.wait(timeout=5)
            raise RuntimeError("OLD_GENERATION_CAPTURE_FAILURE")

    old_event = _official_employment_event(
        "employment-old-display",
        scheduled_at=RELEASE,
        observed_at=RELEASE - timedelta(days=2),
    )
    new_event = _official_employment_event(
        "employment-new-display",
        scheduled_at=RELEASE + timedelta(minutes=1),
        observed_at=RELEASE - timedelta(days=1),
    )
    store = ReactionEvidenceStore(tmp_path / "capture-display-generation.sqlite3")
    provider = ProductionMacroReactionProvider(
        store,
        jin10_client=None,
        jin10_secret_store=None,
        official_actual_provider=object(),
        official_document_provider=Capture(),
        clock=lambda: RELEASE + timedelta(seconds=1),
    )
    provider.restore_official_identities((old_event,))
    old_generation = provider._state().generation
    capture_thread = threading.Thread(
        target=provider.refresh_capture,
        kwargs={"now": RELEASE + timedelta(seconds=1)},
    )
    correction_thread = threading.Thread(
        target=provider.restore_official_identities,
        args=((new_event, old_event),),
    )
    try:
        capture_thread.start()
        assert entered.wait(timeout=5)
        correction_thread.start()
        for _ in range(500):
            if provider._state().generation > old_generation:
                break
            threading.Event().wait(0.01)
        new_state = provider._state()
        assert new_state.generation > old_generation
        new_projection = provider.projection()
        new_coverage = provider.coverage((new_event.event_id,))
        release.set()
        capture_thread.join(timeout=5)
        correction_thread.join(timeout=5)
        assert not capture_thread.is_alive()
        assert not correction_thread.is_alive()
        assert provider._state() == new_state
        assert provider.projection() == new_projection
        assert provider.coverage((new_event.event_id,)) == new_coverage
        assert "OLD_GENERATION_CAPTURE_FAILURE" not in str(new_projection)
        assert store.worker_failures() == ()
    finally:
        release.set()
        capture_thread.join(timeout=5)
        correction_thread.join(timeout=5)
        provider.close()


def test_projection_schedule_generation_is_atomic_through_real_calendar_asgi(
    tmp_path,
    monkeypatch,
) -> None:
    now = RELEASE - timedelta(minutes=1)

    def event(event_id: str, scheduled_at: datetime, observed_at: datetime):
        source = "Federal Reserve"
        source_id = "fomc-minutes-2026-07"
        source_url = (
            "https://www.federalreserve.gov/monetarypolicy/"
            "fomccalendars.htm"
        )
        provenance = OfficialEventProvenance(
            source=source,
            source_url=source_url,
            source_id=source_id,
            source_payload_hash=canonical_hash(
                {"event_id": event_id, "scheduled_at": scheduled_at.isoformat()}
            ),
            first_seen_at=RELEASE - timedelta(days=20),
            ingested_at=observed_at,
            observed_at=observed_at,
        )
        return OfficialCalendarEvent(
            event_id=event_id,
            source=source,
            source_id=source_id,
            source_url=source_url,
            title="FOMC minutes 2026-07-29/2026-07-30",
            category="FOMC",
            scheduled_at=scheduled_at,
            published_at=RELEASE - timedelta(days=30),
            first_seen_at=RELEASE - timedelta(days=20),
            ingested_at=observed_at,
            observed_at=observed_at,
            schedule_precision="EXACT",
            symbols=("SPY",),
            provenance=(provenance,),
        )

    old_event = event("fomc-old", RELEASE, now)
    new_event = event(
        "fomc-corrected",
        RELEASE + timedelta(minutes=1),
        now + timedelta(seconds=1),
    )
    source_health = OfficialSourceHealth(
        source=old_event.source,
        source_url=old_event.source_url,
        status="READY",
        reason=None,
        observed_at=now,
        event_count=1,
    )
    snapshot = OfficialCalendarSnapshot(
        status="READY",
        decision="OBSERVATION_ONLY",
        window_start=now,
        window_end=now + timedelta(days=14),
        observed_at=now,
        events=(old_event,),
        sources=(source_health,),
        reasons=(),
    )
    class Calendar:
        health = "READY"
        health_reason = None

        def future_two_weeks(self, *, now):
            return snapshot

    provider = ProductionMacroReactionProvider(
        ReactionEvidenceStore(tmp_path / "projection-generation.sqlite3"),
        jin10_client=None,
        jin10_secret_store=None,
        official_actual_provider=object(),
        clock=lambda: now,
    )
    coordinator = NewsCoordinator(
        tmp_path / "projection-news.sqlite3",
        official_calendar_provider=Calendar(),
        reaction_provider=provider,
        clock=lambda: now,
    )
    entered = threading.Event()
    resume = threading.Event()
    try:
        provider.refresh_schedule_with_health(
            SimpleNamespace(events=(old_event,)),
            now=now,
            status="READY",
            reason=None,
            schedule_hash="c" * 64,
        )
        coordinator.refresh_once()
        assert coordinator._official_calendar_cache is not None
        assert coordinator._official_calendar_cache.failed is False, (
            coordinator._official_calendar_reasons
        )
        coordinator.refresh_reaction_once()
        old_state = provider._state()
        original_state = provider._state
        original_projection = provider.projection

        def interleaved_state():
            if threading.current_thread().name == "projection-generation-reader":
                entered.set()
                assert resume.wait(timeout=5)
                return old_state
            return original_state()

        monkeypatch.setattr(provider, "_state", interleaved_state)
        publication_failures: list[BaseException] = []
        projected: list[dict[str, object]] = []

        def publish() -> None:
            try:
                projected.append(original_projection())
            except BaseException as exc:
                publication_failures.append(exc)

        publication = threading.Thread(
            target=publish,
            name="projection-generation-reader",
        )
        publication.start()
        assert entered.wait(timeout=5), publication_failures
        provider.refresh_schedule_with_health(
            SimpleNamespace(events=(new_event, old_event)),
            now=now,
            status="READY",
            reason=None,
            schedule_hash="d" * 64,
        )
        resume.set()
        publication.join(timeout=5)
        assert not publication.is_alive()
        assert publication_failures == []
        assert len(projected) == 1
        monkeypatch.setattr(provider, "projection", lambda: projected[0])
        coordinator._rebuild_read_model(asof=now, analysis_budget=0)
        raw_first = coordinator.calendar_payload()["reaction_provider"]
        assert raw_first["supported_event_ids"] == [old_event.event_id], raw_first
        assert raw_first["supported_count"] == 1
        assert raw_first["event_count"] == 1
        assert raw_first["schedule_hash"] == "c" * 64

        services = OptionsCopilotServices(
            health_provider=lambda: {},
            bootstrap_provider=lambda: {},
            candidates_provider=lambda: (),
            positions_provider=lambda: (),
            learning_provider=lambda: {},
            calendar_provider=coordinator.calendar_payload,
        )
        app = create_app(services)
        first = asyncio.run(_http_get_json(app, "/api/calendar"))[
            "reaction_provider"
        ]
        assert first["supported_event_ids"] == [old_event.event_id]
        assert first["supported_count"] == 1

        monkeypatch.setattr(provider, "projection", original_projection)
        coordinator._rebuild_read_model(asof=now, analysis_budget=0)
        raw_second = coordinator.calendar_payload()["reaction_provider"]
        assert raw_second["supported_event_ids"] == [new_event.event_id]
        assert raw_second["supported_count"] == 1
        assert raw_second["event_count"] == 1
        assert raw_second["schedule_hash"] == "d" * 64
        second = asyncio.run(_http_get_json(app, "/api/calendar"))[
            "reaction_provider"
        ]
        assert second["supported_event_ids"] == [new_event.event_id]
        assert second["supported_count"] == 1
    finally:
        resume.set()
        coordinator.close()
        provider.close()


def test_runtime_close_timeout_returns_false_and_exposes_bounded_degraded_health(
    tmp_path,
    monkeypatch,
) -> None:
    runtime = OptionsCopilotRuntime(
        OptionsCopilotConfig(
            data_dir=tmp_path / "close-data",
            log_dir=tmp_path / "close-logs",
        )
    )
    original_close = runtime.news.close
    monkeypatch.setattr(runtime.news, "close", lambda: False)
    assert runtime.close() is False
    assert runtime.health()["dependencies"]["shutdown"] == {
        "status": "DEGRADED",
        "reason": "NEWS_WORKER_SHUTDOWN_TIMEOUT",
    }
    monkeypatch.setattr(runtime.news, "close", original_close)
    assert runtime.close() is True


def test_worker_failure_survives_durable_write_failure_through_api_projection(
    tmp_path,
    monkeypatch,
) -> None:
    store = ReactionEvidenceStore(tmp_path / "worker-health.sqlite3")
    provider = ProductionMacroReactionProvider(
        store,
        jin10_client=None,
        jin10_secret_store=None,
        official_actual_provider=object(),
        clock=lambda: RELEASE,
    )

    def fail_durable_write(*_args, **_kwargs):
        raise sqlite3.OperationalError("simulated durable health failure")

    monkeypatch.setattr(store, "record_worker_failure", fail_durable_write)
    try:
        provider.record_worker_failure(
            "CAPTURE",
            now=RELEASE,
            reason="REACTION_CAPTURE_WORKER_UNAVAILABLE",
        )
        scope = provider.projection()
        assert scope["worker_health"]["capture"] == {
            "status": "DEGRADED",
            "reason": "REACTION_CAPTURE_WORKER_UNAVAILABLE",
            "attempted_at": RELEASE.isoformat(),
            "durable": False,
        }
        api_projection = _normalise_reaction_provider(
            {
                **scope,
                "status": "UNAVAILABLE",
                "decision": "NO_TRADE",
                "ledger_count": 0,
                "matched_count": 0,
                "ignored_count": 0,
                "supported_count": 0,
                "eligible_count": 0,
                "unsupported_count": 0,
            }
        )
        assert api_projection["status"] == "UNAVAILABLE"
        assert api_projection["worker_health"]["capture"]["durable"] is False
        assert api_projection["worker_health"]["capture"]["status"] == "DEGRADED"
    finally:
        provider.close()


@pytest.mark.parametrize("lane", ("OBSERVER", "CAPTURE", "SCHEDULE"))
def test_successful_worker_cycle_clears_only_live_degraded_health(
    tmp_path,
    lane,
) -> None:
    store = ReactionEvidenceStore(tmp_path / f"recovered-{lane.lower()}.sqlite3")
    provider = ProductionMacroReactionProvider(
        store,
        jin10_client=None,
        jin10_secret_store=None,
        official_actual_provider=object(),
        clock=lambda: RELEASE,
    )
    try:
        provider.record_worker_failure(
            lane,
            now=RELEASE - timedelta(seconds=1),
            reason=f"REACTION_{lane}_WORKER_UNAVAILABLE",
        )
        assert lane.lower() in provider.projection()["worker_health"]

        if lane == "OBSERVER":
            provider.observe_local(now=RELEASE)
        elif lane == "CAPTURE":
            provider.refresh_capture(now=RELEASE)
        else:
            provider.refresh_schedule(SimpleNamespace(events=()), now=RELEASE)

        assert lane.lower() not in provider.projection()["worker_health"]
        assert store.worker_failures()[-1]["lane"] == lane
    finally:
        provider.close()


def test_fixed_five_minute_window_rejects_historical_fill_and_waits() -> None:
    window = ProspectiveMarketWindow(EVENT_HASH, RELEASE_HASH, RELEASE, RELEASE - timedelta(seconds=5))
    with pytest.raises(ValueError, match="historical"):
        window.append(MarketSample(RELEASE - timedelta(seconds=1), {"SPY": Decimal("600")}))
    window = window.with_baseline(MarketSample(RELEASE - timedelta(seconds=5), {"SPY": Decimal("599")})).append(MarketSample(RELEASE, {"SPY": Decimal("600")})).append(MarketSample(RELEASE + timedelta(minutes=5), {"SPY": Decimal("601")}))
    waiting = window.projection(now=RELEASE + timedelta(minutes=4, seconds=59))
    assert waiting["complete"] is False
    assert waiting["reason"] == "WAITING_PROSPECTIVE_FIVE_MINUTE_ENDPOINT"
    complete = window.projection(now=RELEASE + timedelta(minutes=5))
    assert complete["complete"] is True
    assert complete["decision_authority"] == "SUPPORTING_ONLY"


@pytest.mark.parametrize(
    ("case", "bid", "ask", "timestamp_mode"),
    (
        ("zero", Decimal("0"), Decimal("1"), "aware"),
        ("negative", Decimal("-1"), Decimal("1"), "aware"),
        ("nan", Decimal("NaN"), Decimal("1"), "aware"),
        ("infinity", Decimal("1"), Decimal("Infinity"), "aware"),
        ("float", 1.0, Decimal("2"), "aware"),
        ("string", Decimal("1"), "2", "aware"),
        ("naive", Decimal("1"), Decimal("2"), "naive"),
        ("mixed", Decimal("1"), Decimal("2"), "mixed"),
    ),
)
def test_reaction_underlying_sample_rejects_malformed_numeric_and_time_values(
    tmp_path,
    case,
    bid,
    ask,
    timestamp_mode,
) -> None:
    identity = ScheduledEventIdentity(
        event_id=f"malformed-{case}",
        official_source="Bureau of Labor Statistics",
        official_source_id=f"malformed-{case}",
        title="Consumer Price Index July 2026",
        category="MACRO",
        scheduled_at=RELEASE,
        schedule_published_at=RELEASE - timedelta(days=30),
        schedule_first_seen_at=RELEASE - timedelta(days=20),
        schedule_observed_at=RELEASE - timedelta(minutes=10),
        symbols=("SPY",),
    )
    observed_at = RELEASE - timedelta(minutes=10)
    rows = list(_underlying_basket(observed_at))
    timestamp = observed_at
    if timestamp_mode == "naive":
        timestamp = observed_at.replace(tzinfo=None)
    rows[0] = SimpleNamespace(
        symbol=rows[0].symbol,
        observed_at=timestamp,
        bid=bid,
        ask=ask,
    )
    if timestamp_mode == "mixed":
        rows[1] = SimpleNamespace(
            symbol=rows[1].symbol,
            observed_at=observed_at - timedelta(seconds=1),
            bid=rows[1].bid,
            ask=rows[1].ask,
        )

    adapter = SimpleNamespace(
        cached_reaction_underlying_quotes=lambda _symbols: tuple(rows)
    )
    store = ReactionEvidenceStore(tmp_path / f"malformed-{case}.sqlite3")
    try:
        observer = ProductionReactionObserver(
            adapter,
            SimpleNamespace(ready=True),
            store=store,
        )
        assert observer.arm(identity, now=observed_at) is False
        assert store.load_baseline(
            identity.event_hash,
            scheduled_at=identity.scheduled_at,
        ) is None
    finally:
        store.close()


def test_option_reprice_bundle_is_immutable_all_gates_bound_supporting_only() -> None:
    bundle = OptionRepriceBundle(EVENT_HASH, RELEASE_HASH, "c" * 64, "d" * 64, "e" * 64, {"GATE_1_AUTHORITY_DATA": True, "GATE_2_MARKET_CREDIT_REGIME": True, "GATE_3_UNDERLYING_EVENT": True, "GATE_4_OPTION_EDGE_LIQUIDITY": False, "GATE_5_STRUCTURE_ACCOUNT_RISK": True, "GATE_6_RANKING_REVIEWABILITY": True}, (EVENT_HASH, RELEASE_HASH), RELEASE + timedelta(minutes=5))
    waiting = bundle.as_dict()
    assert waiting["status"] == "WAIT"
    assert waiting["blockers"] == ("OPTION_GATE_4_OPTION_EDGE_LIQUIDITY_FAILED",)
    assert waiting["decision_authority"] == "SUPPORTING_ONLY"
    assert waiting["approval_eligible"] is False


def test_production_observer_requires_fixed_window_and_canonical_six_gate_bundle(tmp_path) -> None:
    identity = ScheduledEventIdentity(
        event_id="event",
        official_source="Bureau of Labor Statistics",
        official_source_id="event",
        title="Consumer Price Index July 2026",
        category="MACRO",
        scheduled_at=RELEASE,
        schedule_published_at=RELEASE - timedelta(days=30),
        schedule_first_seen_at=RELEASE - timedelta(days=20),
        schedule_observed_at=RELEASE - timedelta(minutes=10),
        symbols=("SPY",),
    )
    expectation = ConsensusExpectation(
        identity.content_hash,
        "headline_cpi_yoy_pct",
        Decimal("3.0"),
        "PERCENT",
        "Jin10 point-in-time calendar",
        "expectation",
        RELEASE - timedelta(minutes=5),
        RELEASE - timedelta(minutes=5),
        RELEASE - timedelta(minutes=5),
        "v1",
        period="2026-07",
        basis="NOT_SEASONALLY_ADJUSTED",
    )
    release = OfficialRelease(
        identity.content_hash,
        expectation.metric,
        Decimal("3.1"),
        expectation.unit,
        identity.official_source,
        "official-release",
        RELEASE,
        RELEASE + timedelta(seconds=1),
        RELEASE + timedelta(seconds=1),
        period=expectation.period,
        basis=expectation.basis,
    )
    ledger = EventReactionLedger.schedule(identity, expectation, recorded_at=expectation.observed_at)
    ledger = ledger.await_release(recorded_at=RELEASE)
    ledger = ledger.capture_release(release, recorded_at=release.captured_at)
    ledger = ledger.assess_surprise(recorded_at=release.captured_at)

    candidate_id = "candidate-1"
    ranking_body = {"candidate_id": candidate_id}
    ranking_candidate_hash = canonical_hash(ranking_body)
    candidate_row = {
        "candidate_id": candidate_id,
        "candidate_hash": ranking_candidate_hash,
        "layers": [
            {"gate_id": gate_id, "status": "PASS"}
            for gate_id in (
                "GATE_1_AUTHORITY_DATA",
                "GATE_2_MARKET_CREDIT_REGIME",
                "GATE_3_UNDERLYING_EVENT",
                "GATE_4_OPTION_EDGE_LIQUIDITY",
                "GATE_5_STRUCTURE_ACCOUNT_RISK",
                "GATE_6_RANKING_REVIEWABILITY",
            )
        ],
    }
    bundle_payload = {
        "schema": "options_copilot.gate_bundle.v1",
        "candidates": {"candidate-key": candidate_row},
    }
    gate_hash = canonical_hash(bundle_payload)
    gate_bundle = {**bundle_payload, "gate_bundle_hash": gate_hash}

    class RankingStore:
        def latest(self):
            return SimpleNamespace(ranking_snapshot_id="ranking-1")

        def read_snapshot(self, _snapshot_id):
            return {
                "ranking_snapshot_id": "ranking-1",
                "gate_bundle_hash": gate_hash,
                "decision_records": [{"record": {"gate_bundle": gate_bundle}}],
                "candidates": [
                    {
                        "candidate_id": candidate_id,
                        "candidate_hash": ranking_candidate_hash,
                        "candidate_body": ranking_body,
                    }
                ],
            }

    candidate_document = {
        "preselection_id": candidate_id,
        "maximum_loss_usd": "100",
        "estimated_cost_usd": "2",
        "cost_after_ev_usd": "10",
        "broker_snapshot_hash": "1" * 64,
        "account_snapshot_hash": "2" * 64,
        "risk_policy_hash": "3" * 64,
        "strategy_nav_post_hash": "4" * 64,
        "economics_calculation_hash": "5" * 64,
    }
    candidate = SimpleNamespace(
        preselection_id=candidate_id,
        underlying="SPY",
        evidence_hashes=("6" * 64,),
        economics_quote_asof=RELEASE + timedelta(minutes=5),
        ranking_snapshot_id="ranking-1",
        ranking_candidate_hash=ranking_candidate_hash,
        as_dict=lambda: dict(candidate_document),
    )

    class Adapter:
        ranking_store = RankingStore()

        def __init__(self):
            self.observed_at = RELEASE - timedelta(minutes=10)

        def cached_reaction_underlying_quotes(self, _symbols):
            return _underlying_basket(self.observed_at)

        def preselections(self):
            return (candidate,)

    adapter = Adapter()
    store = ReactionEvidenceStore(tmp_path / "observer.sqlite3")
    observer = ProductionReactionObserver(adapter, SimpleNamespace(ready=True), store=store)
    assert observer.arm(identity, now=RELEASE - timedelta(minutes=10)) is True
    observer = ProductionReactionObserver(adapter, SimpleNamespace(ready=True), store=store)
    assert observer.observe(ledger, now=RELEASE + timedelta(minutes=4, seconds=59)) == (None, None)
    assert observer.last_reason == "WAITING_PROSPECTIVE_FIVE_MINUTE_ENDPOINT"
    adapter.observed_at = RELEASE + timedelta(minutes=5, seconds=3)
    assert observer.tick(identity, now=adapter.observed_at) is True
    market, option = observer.observe(ledger, now=adapter.observed_at)
    assert market is not None and option is not None
    assert market.window_start == RELEASE
    assert market.window_end == RELEASE + timedelta(minutes=5)
    assert market.evidence_asof == adapter.observed_at
    assert option.result["reaction_gate_bundle"]["gate_bundle_hash"] == gate_hash
    assert option.result["reaction_gate_bundle"]["status"] == "AVAILABLE"
    assert option.release_hash == release.content_hash
    assert option.market_reaction_hash == market.content_hash
    candidate.economics_quote_asof = adapter.observed_at + timedelta(days=1)
    assert observer.reevaluate_option(
        ledger,
        market,
        now=adapter.observed_at,
    ) is None
    assert observer.last_reason == "OPTION_GATE_QUOTE_FRESHNESS_FAILED"
    store.close()


def test_gate_bundle_uses_exact_snapshot_and_candidate_hash_not_latest() -> None:
    candidate_id = "same-candidate-id"
    body_a = {"candidate_id": candidate_id, "version": "A"}
    body_b = {"candidate_id": candidate_id, "version": "B"}
    hash_a = canonical_hash(body_a)
    hash_b = canonical_hash(body_b)

    def bundle(candidate_hash, status):
        row = {
            "candidate_id": candidate_id,
            "candidate_hash": candidate_hash,
            "layers": [
                {"gate_id": gate_id, "status": status}
                for gate_id in (
                    "GATE_1_AUTHORITY_DATA",
                    "GATE_2_MARKET_CREDIT_REGIME",
                    "GATE_3_UNDERLYING_EVENT",
                    "GATE_4_OPTION_EDGE_LIQUIDITY",
                    "GATE_5_STRUCTURE_ACCOUNT_RISK",
                    "GATE_6_RANKING_REVIEWABILITY",
                )
            ],
        }
        payload = {
            "schema": "options_copilot.gate_bundle.v1",
            "candidates": {candidate_hash: row},
        }
        digest = canonical_hash(payload)
        return {**payload, "gate_bundle_hash": digest}, digest

    bundle_a, gate_hash_a = bundle(hash_a, "PASS")
    bundle_b, gate_hash_b = bundle(hash_b, "BLOCK")
    snapshots = {
        "ranking-a": {
            "ranking_snapshot_id": "ranking-a",
            "gate_bundle_hash": gate_hash_a,
            "decision_records": [{"record": {"gate_bundle": bundle_a}}],
            "candidates": [
                {
                    "candidate_id": candidate_id,
                    "candidate_hash": hash_a,
                    "candidate_body": body_a,
                }
            ],
        },
        "ranking-b": {
            "ranking_snapshot_id": "ranking-b",
            "gate_bundle_hash": gate_hash_b,
            "decision_records": [{"record": {"gate_bundle": bundle_b}}],
            "candidates": [
                {
                    "candidate_id": candidate_id,
                    "candidate_hash": hash_b,
                    "candidate_body": body_b,
                }
            ],
        },
    }

    class RankingStore:
        def latest(self):
            return SimpleNamespace(ranking_snapshot_id="ranking-b")

        def read_snapshot(self, snapshot_id):
            return snapshots[snapshot_id]

    observer = ProductionReactionObserver(
        SimpleNamespace(ranking_store=RankingStore()),
        SimpleNamespace(ready=True),
    )
    candidate = SimpleNamespace(
        preselection_id=candidate_id,
        ranking_snapshot_id="ranking-a",
        ranking_candidate_hash=hash_a,
    )

    result = observer._canonical_gate_bundle(candidate)

    assert result is not None
    gate_results, bound_hash = result
    assert bound_hash == gate_hash_a
    assert all(gate_results.values())
