from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import json
import sqlite3
from types import SimpleNamespace
from urllib.error import HTTPError

import pytest

from options_copilot.news.reaction import (
    ConsensusExpectation,
    MarketReactionEvidence,
    OfficialRelease,
    OptionReevaluationEvidence,
    ScheduledEventIdentity,
)
from options_copilot.news.reaction_runtime import (
    BlsPublicDataActualProvider,
    Jin10MacroObservation,
    MacroReactionError,
    ProductionMacroReactionProvider,
    ReactionEvidenceStore,
    _releases_from_vintages,
    normalize_jin10_calendar,
)
from options_copilot.news.reaction_specs import EventFamily, EventRole, ParentEventIdentity, ScheduledReactionSpec
from options_copilot.providers.official_reaction_sources import CapturedOfficialRelease, DiscoveryRecord, OfficialDocument, ParsedMeasure, ParsedOfficialRelease
from options_copilot.storage.canonical import canonical_hash, canonical_json


UTC = timezone.utc
RELEASE = datetime(2026, 8, 12, 12, 30, tzinfo=UTC)
EVENT_HASH = "a" * 64


def _official_event(identity: ScheduledEventIdentity, *, title: str | None = None) -> object:
    return SimpleNamespace(
        event_id=identity.event_id,
        source=identity.official_source,
        source_id=identity.official_source_id,
        title=title or identity.title,
        category=identity.category,
        scheduled_at=identity.scheduled_at,
        published_at=identity.schedule_published_at,
        first_seen_at=identity.schedule_first_seen_at,
        observed_at=identity.schedule_observed_at,
        symbols=identity.symbols,
    )


def test_reaction_provider_projects_zero_eligible_without_provider_calls(tmp_path) -> None:
    identity = replace(
        _identity(),
        event_id="fomc-2026-09",
        official_source="Federal Reserve",
        official_source_id="fomc-2026-09",
        title="FOMC rate decision",
        content_hash="",
    )
    attempted_at = RELEASE - timedelta(hours=1)
    provider = ProductionMacroReactionProvider(
        ReactionEvidenceStore(tmp_path / "zero-eligible.sqlite3"),
        jin10_client=None,
        jin10_secret_store=None,
        official_actual_provider=object(),
    )
    try:
        provider.refresh(
            SimpleNamespace(events=(_official_event(identity),)),
            now=attempted_at,
        )
        projection = provider.projection()
        assert projection["supported_event_ids"] == [identity.event_id]
        assert projection["eligible_event_ids"] == []
        assert projection["supported_count"] == 1
        assert projection["eligible_count"] == 0
        assert projection["unsupported_count"] == 0
        assert projection["last_attempt"] == attempted_at.isoformat()
        assert projection["reason"] == "NO_ARMED_REACTION_CAPTURE_WINDOW"
    finally:
        provider.close()


def test_reaction_provider_projects_one_supported_cpi_in_mixed_snapshot(tmp_path) -> None:
    cpi = _identity()
    fomc = replace(
        cpi,
        event_id="fomc-2026-09",
        official_source="Federal Reserve",
        official_source_id="fomc-2026-09",
        title="FOMC rate decision",
        content_hash="",
    )

    class Secrets:
        def get(self, _name):
            return "opaque-token"

    class Client:
        def fetch_calendar(self, _token):
            return SimpleNamespace(
                payload={
                    "status": 200,
                    "data": [
                        {
                            "id": "jin10-cpi-2026-07",
                            "title": "美国7月未季调CPI年率",
                            "pub_time": "2026-08-12 20:30",
                            "consensus": "3.4",
                        }
                    ],
                }
            )

    attempted_at = RELEASE - timedelta(minutes=30)
    provider = ProductionMacroReactionProvider(
        ReactionEvidenceStore(tmp_path / "mixed-scope.sqlite3"),
        jin10_client=Client(),
        jin10_secret_store=Secrets(),
        official_actual_provider=object(),
    )
    try:
        provider.refresh(
            SimpleNamespace(events=(_official_event(cpi), _official_event(fomc))),
            now=attempted_at,
        )
        projection = provider.projection()
        assert projection["supported_event_ids"] == [cpi.event_id, fomc.event_id]
        assert projection["eligible_event_ids"] == [cpi.event_id]
        assert projection["supported_count"] == 2
        assert projection["eligible_count"] == 1
        assert projection["unsupported_count"] == 0
        assert projection["last_attempt"] == attempted_at.isoformat()
        assert projection["reason"] == "NO_ARMED_REACTION_CAPTURE_WINDOW"
    finally:
        provider.close()


def test_jin10_calendar_preserves_pre_release_vintage_and_does_not_verify_actual() -> None:
    rows = normalize_jin10_calendar(
        {
            "status": 200,
            "data": [
                {
                    "title": "美国7月未季调CPI年率",
                    "pub_time": "2026-08-12 20:30",
                    "consensus": "3.4",
                    "actual": "3.4",
                    "previous": "3.50",
                }
            ],
        },
        observed_at=RELEASE - timedelta(minutes=20),
    )

    assert len(rows) == 1
    row = rows[0]
    assert row.metric == "headline_cpi_yoy_pct"
    assert row.period == "2026-07"
    assert row.consensus == Decimal("3.4")
    assert row.reported_actual == Decimal("3.4")
    assert row.as_dict()["official_actual_verified"] is False


def test_jin10_calendar_normalizes_all_admitted_numeric_families() -> None:
    rows = normalize_jin10_calendar(
        {
            "status": 200,
            "data": [
                {"title": "美国7月非农就业人口变动", "pub_time": "2026-08-12 20:30", "consensus": "180"},
                {"title": "美国7月失业率", "pub_time": "2026-08-12 20:30", "consensus": "4.2"},
                {"title": "美国7月核心PCE物价指数年率", "pub_time": "2026-08-12 20:30", "consensus": "2.8"},
                {"title": "美国第二季度实际GDP年化季率修正值", "pub_time": "2026-08-12 20:30", "consensus": "2.1"},
            ],
        },
        observed_at=RELEASE - timedelta(minutes=20),
    )
    assert [(row.metric, row.unit, row.period) for row in rows] == [
        ("total_nonfarm_payroll_change_thousands", "THOUSANDS", "2026-07"),
        ("unemployment_rate_pct", "PERCENT", "2026-07"),
        ("core_pce_price_index_yoy_pct", "PERCENT", "2026-07"),
        ("real_gdp_annual_rate_pct", "PERCENT", "2026-Q2"),
    ]


def test_release_replay_requires_explicit_revision_link_and_keeps_unlinked_vintage_independent() -> None:
    identity = _identity()
    expectation = _expectation(identity)

    def vintage(value: str, captured_at: datetime, revision_of=None):
        return {
            "document": {
                "reference_period": expectation.period,
                "official_url": "https://www.bls.gov/news.release/cpi.nr0.htm",
                "raw_hash": canonical_hash({"value": value, "at": captured_at}),
                "captured_at": captured_at.isoformat(),
                "declared_release_at": RELEASE.isoformat(),
                "first_observed_release_at": captured_at.isoformat(),
                "revision_of": revision_of,
                "measures": [
                    {
                        "measure_id": expectation.metric,
                        "value": value,
                        "unit": expectation.unit,
                        "basis": expectation.basis,
                    }
                ],
            }
        }

    initial = vintage("3.4", RELEASE + timedelta(seconds=1))
    first = _releases_from_vintages(identity, expectation, (initial,))
    explicit = _releases_from_vintages(
        identity,
        expectation,
        (
            initial,
            vintage(
                "3.5",
                RELEASE + timedelta(seconds=2),
                first[0].content_hash,
            ),
        ),
    )
    unlinked = _releases_from_vintages(
        identity,
        expectation,
        (
            initial,
            vintage("3.6", RELEASE + timedelta(seconds=3)),
        ),
    )

    assert [item.revision for item in explicit] == [0, 1]
    assert explicit[1].supersedes_hash == explicit[0].content_hash
    assert [item.actual_value for item in explicit] == [Decimal("3.4"), Decimal("3.5")]
    assert len(unlinked) == 1
    assert unlinked[0].actual_value == Decimal("3.4")
    assert unlinked[0].revision == 0
    assert unlinked[0].supersedes_hash is None


def test_provider_projects_initial_and_explicit_revision_as_separate_views(
    tmp_path,
) -> None:
    store = ReactionEvidenceStore(tmp_path / "revision-views.sqlite3")
    provider = ProductionMacroReactionProvider(
        store,
        jin10_client=None,
        jin10_secret_store=None,
        official_actual_provider=object(),
        clock=lambda: RELEASE + timedelta(minutes=1),
    )
    identity = _identity()

    def capture(value: str, captured_at: datetime, revision_of=None) -> None:
        spec = provider._state().specs[identity.event_id]
        document = OfficialDocument(
            EventFamily.CPI,
            EventRole.OFFICIAL_RELEASE_DOCUMENT,
            "https://www.bls.gov/news.release/cpi.nr0.htm",
            captured_at,
            "text/html",
            f"official CPI {value} {captured_at.isoformat()}".encode(),
            declared_release_at=RELEASE,
            first_observed_release_at=captured_at,
        )
        parsed = ParsedOfficialRelease(
            EventFamily.CPI,
            "2026-07",
            None,
            (
                ParsedMeasure(
                    "headline_cpi_yoy_pct",
                    Decimal(value),
                    "PERCENT",
                    "NOT_SEASONALLY_ADJUSTED",
                    "Headline CPI YoY",
                ),
            ),
            (),
            document.raw_hash,
            declared_release_at=RELEASE,
            first_observed_release_at=captured_at,
            revision_of=revision_of,
        )
        store.append_verified_capture(
            event_id=identity.event_id,
            captured=CapturedOfficialRelease(
                spec,
                DiscoveryRecord(
                    EventFamily.CPI,
                    "https://www.bls.gov/schedule/news_release/cpi.htm",
                    captured_at,
                    None,
                    document.url,
                ),
                document,
                parsed,
            ),
        )

    try:
        provider.restore_official_identities((_official_event(identity),))
        observation = normalize_jin10_calendar(
            {
                "status": 200,
                "data": [
                    {
                        "id": "jin10-cpi-revision",
                        "title": "美国7月未季调CPI年率",
                        "pub_time": "2026-08-12 20:30",
                        "consensus": "3.0",
                    }
                ],
            },
            observed_at=RELEASE - timedelta(minutes=20),
        )[0]
        provider_identity = provider._state().identities[identity.event_id]
        store.append(
            event_id=identity.event_id,
            official_event_hash=provider_identity.event_hash,
            kind="JIN10_EXPECTATION",
            observed_at=observation.observed_at,
            document=observation.as_dict(),
        )
        capture("3.1", RELEASE + timedelta(seconds=1))
        initial_ledger = provider.child_reactions((identity.event_id,))[
            identity.event_id
        ][0]
        initial_release = initial_ledger.release_chain[0]
        capture(
            "3.2",
            RELEASE + timedelta(seconds=2),
            revision_of=initial_release.content_hash,
        )
        capture("9.9", RELEASE + timedelta(seconds=3))

        replayed = provider.child_reactions((identity.event_id,))[
            identity.event_id
        ][0]
        view = provider.revision_views((identity.event_id,))[
            identity.event_id
        ][0]
        assert replayed.release_chain[0].actual_value == Decimal("3.1")
        assert replayed.surprise.delta == Decimal("0.1")
        assert view["initial_release"]["actual_value"] == Decimal("3.1")
        assert view["revised_release"]["actual_value"] == Decimal("3.2")
        assert [row["actual_value"] for row in view["revision_history"]] == [
            Decimal("3.2")
        ]
        assert view["initial_reaction_immutable"] is True
        assert "9.9" not in str(view)
    finally:
        provider.close()


@pytest.mark.parametrize(
    ("event_id", "source", "title", "jin10_title", "metric", "unit", "basis", "period", "value"),
    (
        (
            "employment-2026-07",
            "Bureau of Labor Statistics",
            "Employment Situation July 2026",
            "美国7月非农就业人口变动",
            "total_nonfarm_payroll_change_thousands",
            "THOUSANDS",
            "SEASONALLY_ADJUSTED",
            "2026-07",
            "187",
        ),
        (
            "pce-2026-07",
            "Bureau of Economic Analysis",
            "Personal Income and Outlays July 2026",
            "美国7月核心PCE物价指数年率",
            "core_pce_price_index_yoy_pct",
            "PERCENT",
            "NOT_SEASONALLY_ADJUSTED_YEAR_OVER_YEAR",
            "2026-07",
            "2.8",
        ),
        (
            "gdp-2026-q2-second",
            "Bureau of Economic Analysis",
            "Gross Domestic Product, Second Quarter 2026, Second Estimate",
            "美国第二季度实际GDP年化季率修正值",
            "real_gdp_annual_rate_pct",
            "PERCENT",
            "SEASONALLY_ADJUSTED_ANNUAL_RATE",
            "2026-Q2",
            "2.1",
        ),
    ),
)
def test_numeric_family_child_reactions_progress_expectation_to_captured_surprise(
    tmp_path,
    event_id,
    source,
    title,
    jin10_title,
    metric,
    unit,
    basis,
    period,
    value,
) -> None:
    store = ReactionEvidenceStore(tmp_path / f"{event_id}.sqlite3")
    provider = ProductionMacroReactionProvider(
        store,
        jin10_client=None,
        jin10_secret_store=None,
        official_actual_provider=object(),
        clock=lambda: RELEASE + timedelta(seconds=3),
    )
    event = SimpleNamespace(
        event_id=event_id,
        source=source,
        source_id=event_id,
        title=title,
        category="MACRO",
        scheduled_at=RELEASE,
        published_at=RELEASE - timedelta(days=30),
        first_seen_at=RELEASE - timedelta(days=20),
        observed_at=RELEASE - timedelta(days=1),
        symbols=(),
    )
    try:
        provider.restore_official_identities((event,))
        observation = normalize_jin10_calendar(
            {
                "status": 200,
                "data": [
                    {
                        "id": f"jin10-{event_id}",
                        "title": jin10_title,
                        "pub_time": "2026-08-12 20:30",
                        "consensus": value,
                    }
                ],
            },
            observed_at=RELEASE - timedelta(minutes=20),
        )[0]
        identity = provider._identities[event_id]
        assert observation.metric == metric
        store.append(
            event_id=event_id,
            official_event_hash=identity.content_hash,
            kind="JIN10_EXPECTATION",
            observed_at=observation.observed_at,
            document=observation.as_dict(),
        )
        document = OfficialDocument(
            provider._reaction_specs[event_id].parent.family,
            EventRole.OFFICIAL_RELEASE_DOCUMENT,
            (
                "https://www.bls.gov/news.release/empsit.nr0.htm"
                if "employment" in event_id
                else "https://www.bea.gov/news/2026/personal-income-and-outlays-july-2026"
                if "pce" in event_id
                else "https://www.bea.gov/news/2026/gross-domestic-product-second-quarter-2026-second-estimate"
            ),
            RELEASE + timedelta(seconds=1),
            "text/html",
            f"official {event_id}".encode(),
            declared_release_at=RELEASE,
            first_observed_release_at=RELEASE + timedelta(seconds=1),
        )
        parsed = ParsedOfficialRelease(
            provider._reaction_specs[event_id].parent.family,
            period,
            "SECOND" if "gdp" in event_id else None,
            (ParsedMeasure(metric, Decimal(value) + Decimal("0.1"), unit, basis, metric),),
            (),
            document.raw_hash,
            declared_release_at=RELEASE,
            first_observed_release_at=RELEASE + timedelta(seconds=1),
        )
        captured = CapturedOfficialRelease(
            provider._reaction_specs[event_id],
            DiscoveryRecord(
                provider._reaction_specs[event_id].parent.family,
                (
                    "https://www.bls.gov/schedule/news_release/empsit.htm"
                    if "employment" in event_id
                    else "https://apps.bea.gov/rss/rss.xml"
                ),
                RELEASE + timedelta(seconds=1),
                "7" * 64,
                document.url,
            ),
            document,
            parsed,
        )
        store.append_verified_capture(event_id=event_id, captured=captured)

        children = provider.child_reactions((event_id,))[event_id]

        assert len(children) == 1
        assert children[0].expectation.metric == metric
        assert children[0].release_chain[-1].actual_value == Decimal(value) + Decimal("0.1")
        assert children[0].surprise is not None
        assert children[0].surprise.delta == Decimal("0.1")
    finally:
        provider.close()


def test_reaction_store_is_append_only_hash_verified_and_replayable(tmp_path) -> None:
    store = ReactionEvidenceStore(tmp_path / "macro.sqlite3")
    try:
        document = {
            "schema": "fixture.v1",
            "consensus": "3.4",
            "decision_authority": "SUPPORTING_ONLY",
        }
        assert store.append(
            event_id="bls-cpi-2026-07",
            official_event_hash=EVENT_HASH,
            kind="JIN10_EXPECTATION",
            observed_at=RELEASE - timedelta(minutes=20),
            document=document,
        ) is True
        assert store.append(
            event_id="bls-cpi-2026-07",
            official_event_hash=EVENT_HASH,
            kind="JIN10_EXPECTATION",
            observed_at=RELEASE - timedelta(minutes=20),
            document=document,
        ) is False
        rows = store.records((("bls-cpi-2026-07", EVENT_HASH),))
        assert len(rows) == 1
        assert rows[0]["document"] == document
        store.assert_integrity()
    finally:
        store.close()


@pytest.mark.parametrize(
    ("field", "replacement"),
    (
        ("event_id", "bls-cpi-tampered"),
        ("official_event_hash", "b" * 64),
        ("kind", "JIN10_REPORTED_ACTUAL"),
        ("observed_at", RELEASE.isoformat()),
    ),
)
def test_reaction_store_row_hash_detects_identity_and_time_tampering(
    tmp_path,
    field: str,
    replacement: str,
) -> None:
    path = tmp_path / f"tampered-{field}.sqlite3"
    store = ReactionEvidenceStore(path)
    store.append(
        event_id="bls-cpi-2026-07",
        official_event_hash=EVENT_HASH,
        kind="JIN10_EXPECTATION",
        observed_at=RELEASE - timedelta(minutes=20),
        document={"schema": "fixture.v1", "consensus": "3.4"},
    )
    store.close()

    connection = sqlite3.connect(path)
    try:
        connection.execute("DROP TRIGGER macro_reaction_no_update")
        connection.execute(
            f"UPDATE macro_reaction_evidence SET {field}=? WHERE sequence=1",
            (replacement,),
        )
        connection.commit()
    finally:
        connection.close()

    with pytest.raises(
        MacroReactionError,
        match="REACTION_EVIDENCE_INTEGRITY_FAILED",
    ):
        ReactionEvidenceStore(path)


def test_reaction_store_atomically_migrates_verified_v1_chain(tmp_path) -> None:
    path = tmp_path / "legacy-v1.sqlite3"
    document = canonical_json({"schema": "fixture.v1", "consensus": "3.4"})
    content_hash = hashlib.sha256(document.encode("utf-8")).hexdigest()
    prior_hash = "0" * 64
    legacy_row_hash = hashlib.sha256(
        f"1:{prior_hash}:{content_hash}".encode("ascii")
    ).hexdigest()
    connection = sqlite3.connect(path)
    try:
        connection.executescript(
            """
            CREATE TABLE macro_reaction_evidence(
                sequence INTEGER PRIMARY KEY,
                event_id TEXT NOT NULL,
                kind TEXT NOT NULL,
                observed_at TEXT NOT NULL,
                document_json TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                prior_hash TEXT NOT NULL,
                row_hash TEXT NOT NULL UNIQUE,
                UNIQUE(event_id, kind, content_hash)
            );
            CREATE TRIGGER macro_reaction_no_update
            BEFORE UPDATE ON macro_reaction_evidence
            BEGIN SELECT RAISE(ABORT, 'immutable macro reaction evidence'); END;
            CREATE TRIGGER macro_reaction_no_delete
            BEFORE DELETE ON macro_reaction_evidence
            BEGIN SELECT RAISE(ABORT, 'immutable macro reaction evidence'); END;
            PRAGMA user_version=1;
            """
        )
        connection.execute(
            "INSERT INTO macro_reaction_evidence VALUES(?,?,?,?,?,?,?,?)",
            (
                1,
                "bls-cpi-2026-07",
                "JIN10_EXPECTATION",
                (RELEASE - timedelta(minutes=20)).isoformat(),
                document,
                content_hash,
                prior_hash,
                legacy_row_hash,
            ),
        )
        connection.commit()
    finally:
        connection.close()

    store = ReactionEvidenceStore(path)
    try:
        rows = store.records(
            (("bls-cpi-2026-07", "0" * 64),)
        )
        assert len(rows) == 1
        assert rows[0]["document"]["consensus"] == "3.4"
        assert rows[0]["row_hash"] != legacy_row_hash
        assert store._db.execute("PRAGMA user_version").fetchone()[0] == 4
        store.assert_integrity()
    finally:
        store.close()


def test_reaction_store_v2_migration_keeps_legacy_expectation_unbound(
    tmp_path,
) -> None:
    path = tmp_path / "legacy-v2.sqlite3"
    event_id = "bls-cpi-2026-07"
    observed_at = (RELEASE - timedelta(minutes=20)).isoformat()
    document = canonical_json({"schema": "fixture.v1", "consensus": "3.4"})
    content_hash = hashlib.sha256(document.encode("utf-8")).hexdigest()
    prior_hash = "0" * 64
    row_hash = canonical_hash(
        {
            "schema": "options_copilot.macro_reaction_row.v2",
            "sequence": 1,
            "prior_hash": prior_hash,
            "event_id": event_id,
            "kind": "JIN10_EXPECTATION",
            "observed_at": observed_at,
            "content_hash": content_hash,
        }
    )
    connection = sqlite3.connect(path)
    try:
        connection.executescript(
            """
            CREATE TABLE macro_reaction_evidence(
                sequence INTEGER PRIMARY KEY,
                event_id TEXT NOT NULL,
                kind TEXT NOT NULL,
                observed_at TEXT NOT NULL,
                document_json TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                prior_hash TEXT NOT NULL,
                row_hash TEXT NOT NULL UNIQUE,
                UNIQUE(event_id, kind, content_hash)
            );
            PRAGMA user_version=2;
            """
        )
        connection.execute(
            "INSERT INTO macro_reaction_evidence VALUES(?,?,?,?,?,?,?,?)",
            (
                1,
                event_id,
                "JIN10_EXPECTATION",
                observed_at,
                document,
                content_hash,
                prior_hash,
                row_hash,
            ),
        )
        connection.commit()
    finally:
        connection.close()

    store = ReactionEvidenceStore(path)
    try:
        assert len(store.records(((event_id, "0" * 64),))) == 1
        assert store.records(((event_id, EVENT_HASH),)) == ()
        assert store._db.execute("PRAGMA user_version").fetchone()[0] == 4
        store.assert_integrity()
    finally:
        store.close()


class _Response:
    status = 200
    headers = {"Content-Type": "application/json"}

    def __init__(self, payload: object) -> None:
        self._payload = json.dumps(payload).encode()

    def geturl(self) -> str:
        return "https://api.bls.gov/publicAPI/v2/timeseries/data/"

    def read(self, _limit: int) -> bytes:
        return self._payload

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None


class _Opener:
    def __init__(self, payload: object) -> None:
        self.payload = payload

    def open(self, request, timeout: float):
        assert request.full_url == "https://api.bls.gov/publicAPI/v2/timeseries/data/"
        assert 0 < timeout <= 8
        return _Response(self.payload)


def _identity() -> ScheduledEventIdentity:
    return ScheduledEventIdentity(
        event_id="bls-cpi-2026-07",
        official_source="Bureau of Labor Statistics",
        official_source_id="cpi-2026-07",
        title="Consumer Price Index - July 2026",
        category="MACRO",
        scheduled_at=RELEASE,
        schedule_published_at=RELEASE - timedelta(days=30),
        schedule_first_seen_at=RELEASE - timedelta(days=20),
        schedule_observed_at=RELEASE - timedelta(hours=1),
    )


def _expectation(identity: ScheduledEventIdentity) -> ConsensusExpectation:
    return ConsensusExpectation(
        event_hash=identity.content_hash,
        metric="headline_cpi_yoy_pct",
        expected_value=Decimal("3.4"),
        unit="PERCENT",
        period="2026-07",
        basis="NOT_SEASONALLY_ADJUSTED",
        provider="Jin10 point-in-time calendar",
        source_id="jin10-cpi-2026-07",
        published_at=RELEASE - timedelta(minutes=20),
        first_seen_at=RELEASE - timedelta(minutes=20),
        observed_at=RELEASE - timedelta(minutes=20),
        vintage="2026-08-12T12:10:00Z",
    )


def test_bls_public_data_is_latest_revised_only_and_recomputes_yoy() -> None:
    payload = {
        "status": "REQUEST_SUCCEEDED",
        "Results": {
            "series": [
                {
                    "seriesID": "CUUR0000SA0",
                    "data": [
                        {"year": "2026", "period": "M07", "value": "333.918"},
                        {"year": "2025", "period": "M07", "value": "323.048"},
                    ],
                }
            ]
        },
    }
    class GetResponse(_Response):
        def geturl(self) -> str:
            return "https://api.bls.gov/publicAPI/v2/timeseries/data/CUUR0000SA0?startyear=2025&endyear=2026"

    class GetOnly:
        def __init__(self) -> None:
            self.methods: list[str] = []

        def open(self, request, timeout: float):
            self.methods.append(request.get_method())
            return GetResponse(payload)

    opener = GetOnly()
    provider = BlsPublicDataActualProvider(opener=opener)
    assert provider.actual(
        _identity(),
        _expectation(_identity()),
        series_id="CUUR0000SA0",
        calculation="YOY",
        captured_at=RELEASE + timedelta(seconds=30),
    ) is None
    observation = provider.latest_revised(
        series_id="CUUR0000SA0",
        period="2026-07",
        calculation="YOY",
        observed_at=RELEASE + timedelta(seconds=30),
    )

    assert opener.methods == ["GET"]
    assert observation is not None
    assert observation.source_role == "LATEST_REVISED_SERIES"
    assert observation.value == Decimal("3.4")
    assert observation.raw_calculated_value == Decimal("3.3648")


def test_bls_public_data_uses_get_only_without_post_fallback() -> None:
    payload = {
        "status": "REQUEST_SUCCEEDED",
        "Results": {
            "series": [
                {
                    "seriesID": "CUUR0000SA0",
                    "data": [
                        {"year": "2026", "period": "M07", "value": "333.918"},
                        {"year": "2025", "period": "M07", "value": "323.048"},
                    ],
                }
            ]
        },
    }

    class GetResponse(_Response):
        def geturl(self) -> str:
            return (
                "https://api.bls.gov/publicAPI/v2/timeseries/data/"
                "CUUR0000SA0?startyear=2025&endyear=2026"
            )

    class GetReady:
        def __init__(self) -> None:
            self.methods: list[str] = []

        def open(self, request, timeout: float):
            self.methods.append(request.get_method())
            assert 0 < timeout <= 8
            assert request.full_url == (
                "https://api.bls.gov/publicAPI/v2/timeseries/data/"
                "CUUR0000SA0?startyear=2025&endyear=2026"
            )
            return GetResponse(payload)

    opener = GetReady()
    observation = BlsPublicDataActualProvider(opener=opener).latest_revised(
        series_id="CUUR0000SA0",
        period="2026-07",
        calculation="YOY",
        observed_at=RELEASE + timedelta(seconds=30),
    )

    assert opener.methods == ["GET"]
    assert observation is not None
    assert observation.value == Decimal("3.4")


def test_bls_public_data_rejects_unallowlisted_series_before_transport() -> None:
    class NeverCalled:
        def open(self, *_args, **_kwargs):
            raise AssertionError("unallowlisted series reached transport")

    assert BlsPublicDataActualProvider(opener=NeverCalled()).actual(
        _identity(),
        _expectation(_identity()),
        series_id="../../untrusted",
        calculation="YOY",
        captured_at=RELEASE + timedelta(seconds=30),
    ) is None


def test_macro_series_map_distinguishes_headline_and_core_cpi_ppi() -> None:
    rows = normalize_jin10_calendar(
        {
            "status": 200,
            "data": [
                {
                    "title": title,
                    "pub_time": "2026-08-12 20:30",
                    "consensus": "0.3",
                }
                for title in (
                    "美国7月未季调CPI年率",
                    "美国7月核心CPI月率",
                    "美国7月未季调PPI年率",
                    "美国7月核心PPI月率",
                )
            ],
        },
        observed_at=RELEASE - timedelta(minutes=20),
    )

    assert [row.series_id for row in rows] == [
        "CUUR0000SA0",
        "CUSR0000SA0L1E",
        "WPUFD4",
        "WPSFD49104",
    ]


def test_bls_transport_failure_does_not_promote_jin10_reported_actual() -> None:
    class Broken:
        def open(self, *_args, **_kwargs):
            raise OSError("offline")

    identity = _identity()
    assert BlsPublicDataActualProvider(opener=Broken()).actual(
        identity,
        _expectation(identity),
        series_id="CUUR0000SA0",
        calculation="YOY",
        captured_at=RELEASE + timedelta(seconds=30),
    ) is None


def test_complete_reaction_chain_survives_restart_without_market_refetch(
    tmp_path,
) -> None:
    path = tmp_path / "durable-reaction.sqlite3"
    identity = _identity()
    observation = Jin10MacroObservation(
        title="美国7月未季调CPI年率",
        scheduled_at=RELEASE,
        metric="headline_cpi_yoy_pct",
        unit="PERCENT",
        period="2026-07",
        basis="NOT_SEASONALLY_ADJUSTED",
        series_id="CUUR0000SA0",
        calculation="YOY",
        consensus=Decimal("3.4"),
        reported_actual=None,
        previous=Decimal("3.5"),
        source_id="jin10-cpi-2026-07",
        observed_at=RELEASE - timedelta(minutes=20),
    )
    parent = ParentEventIdentity("BLS", EventFamily.CPI, "2026-07", RELEASE.date())
    request = ScheduledReactionSpec(parent, RELEASE, identity.content_hash, RELEASE + timedelta(minutes=15))
    raw_bytes = b"official CPI release July 2026"
    document = OfficialDocument(
        EventFamily.CPI,
        EventRole.OFFICIAL_RELEASE_DOCUMENT,
        "https://www.bls.gov/news.release/cpi.nr0.htm",
        RELEASE + timedelta(seconds=1),
        "text/html",
        raw_bytes,
        declared_release_at=RELEASE,
        first_observed_release_at=RELEASE + timedelta(seconds=1),
    )
    parsed = ParsedOfficialRelease(
        EventFamily.CPI,
        "2026-07",
        None,
        (
            ParsedMeasure(
                observation.metric,
                Decimal("3.3"),
                observation.unit,
                observation.basis,
                "Headline CPI y/y",
            ),
        ),
        (),
        document.raw_hash,
        declared_release_at=RELEASE,
        first_observed_release_at=RELEASE + timedelta(seconds=1),
    )
    captured = CapturedOfficialRelease(
        request,
        DiscoveryRecord(
            EventFamily.CPI,
            "https://www.bls.gov/schedule/news_release/cpi.htm",
            RELEASE + timedelta(seconds=1),
            identity.content_hash,
            document.url,
        ),
        document,
        parsed,
    )
    release = OfficialRelease(
        event_hash=identity.content_hash,
        metric=observation.metric,
        actual_value=Decimal("3.3"),
        unit=observation.unit,
        period=observation.period,
        basis=observation.basis,
        official_source=identity.official_source,
        source_id=f"{document.url}#{document.raw_hash}",
        released_at=RELEASE,
        vintage_at=RELEASE + timedelta(seconds=1),
        captured_at=RELEASE + timedelta(seconds=1),
        published_precision="0.1",
    )
    market = MarketReactionEvidence(
        event_hash=identity.content_hash,
        release_hash=release.content_hash,
        source="IBKR_READ_ONLY",
        window_start=RELEASE,
        window_end=RELEASE + timedelta(seconds=2),
        evidence_asof=RELEASE + timedelta(seconds=2),
        observed_at=RELEASE + timedelta(seconds=3),
        metrics={"bindings": [], "decision_authority": "SUPPORTING_ONLY"},
    )
    option = OptionReevaluationEvidence(
        event_hash=identity.content_hash,
        release_hash=release.content_hash,
        market_reaction_hash=market.content_hash,
        option_id="SPY-defined-risk-vertical",
        candidate_hash="c" * 64,
        source="IBKR_READ_ONLY",
        evidence_asof=market.evidence_asof,
        observed_at=RELEASE + timedelta(seconds=3),
        input_evidence_hashes=(
            observation.content_hash,
            release.content_hash,
            market.content_hash,
        ),
        result={
            "maximum_loss_usd": "100.00",
            "estimated_cost_usd": "2.00",
            "cost_after_ev_usd": "12.00",
        },
    )

    class Actual:
        def actual(self, *_args, **_kwargs):
            return release

    class Observer:
        calls = 0

        def observe(self, *_args, **_kwargs):
            raise AssertionError("reaction replay must not acquire market evidence")

        def reevaluate_option(self, *_args, **_kwargs):
            raise AssertionError("observe already supplied option evidence")

    initial_store = ReactionEvidenceStore(path)
    initial_store.append(
        event_id=identity.event_id,
        official_event_hash=identity.content_hash,
        kind="JIN10_EXPECTATION",
        observed_at=observation.observed_at,
        document=observation.as_dict(),
    )
    initial_store.append_verified_capture(event_id=identity.event_id, captured=captured)
    initial_store.append(
        event_id=identity.event_id,
        official_event_hash=identity.content_hash,
        kind="MARKET_REACTION",
        observed_at=market.observed_at,
        document=market.as_dict(),
    )
    initial_store.append(
        event_id=identity.event_id,
        official_event_hash=identity.content_hash,
        kind="OPTION_REEVALUATION",
        observed_at=option.observed_at,
        document=option.as_dict(),
    )
    observer = Observer()
    initial = ProductionMacroReactionProvider(
        initial_store,
        jin10_client=None,
        jin10_secret_store=None,
        official_actual_provider=Actual(),
        reaction_observer=observer,
        clock=lambda: RELEASE + timedelta(seconds=3),
    )
    initial.restore_official_identities((_official_event(identity),))
    try:
        completed = tuple(initial.reactions((identity.event_id,)))[0]
        assert completed.current_stage.value == "OPTION_REEVALUATED"
        assert observer.calls == 0
        assert [
            row["kind"]
            for row in initial_store.records(
                ((identity.event_id, identity.content_hash),)
            )
        ] == [
            "JIN10_EXPECTATION",
            "MARKET_REACTION",
            "OPTION_REEVALUATION",
        ]
        first_projection = completed.as_dict()
    finally:
        initial.close()

    class NeverActual:
        def actual(self, *_args, **_kwargs):
            raise AssertionError("durable official actual must be replayed")

    class NeverObserver:
        def observe(self, *_args, **_kwargs):
            raise AssertionError("durable market evidence must be replayed")

        def reevaluate_option(self, *_args, **_kwargs):
            raise AssertionError("durable option evidence must be replayed")

    restarted = ProductionMacroReactionProvider(
        ReactionEvidenceStore(path),
        jin10_client=None,
        jin10_secret_store=None,
        official_actual_provider=NeverActual(),
        reaction_observer=NeverObserver(),
        clock=lambda: RELEASE + timedelta(hours=1),
    )
    restarted.restore_official_identities((_official_event(identity),))
    try:
        replayed = tuple(restarted.reactions((identity.event_id,)))[0]
        assert replayed.as_dict() == first_projection
    finally:
        restarted.close()


@pytest.mark.parametrize("mutation", ("reschedule", "title", "symbols"))
def test_same_event_id_never_rebinds_old_expectation_to_changed_official_event(
    tmp_path,
    mutation: str,
) -> None:
    path = tmp_path / f"identity-change-{mutation}.sqlite3"
    old_identity = _identity()
    changes: dict[str, object] = {"content_hash": ""}
    if mutation == "reschedule":
        changes["scheduled_at"] = old_identity.scheduled_at + timedelta(hours=1)
    elif mutation == "title":
        changes["title"] = "Consumer Price Index - July 2026 revised schedule"
    else:
        changes["symbols"] = ("QQQ",)
    current_identity = replace(old_identity, **changes)
    assert current_identity.event_id == old_identity.event_id
    assert current_identity.content_hash != old_identity.content_hash

    observation = Jin10MacroObservation(
        title="美国7月未季调CPI年率",
        scheduled_at=old_identity.scheduled_at,
        metric="headline_cpi_yoy_pct",
        unit="PERCENT",
        period="2026-07",
        basis="NOT_SEASONALLY_ADJUSTED",
        series_id="CUUR0000SA0",
        calculation="YOY",
        consensus=Decimal("3.4"),
        reported_actual=None,
        previous=Decimal("3.5"),
        source_id="jin10-cpi-2026-07",
        observed_at=old_identity.scheduled_at - timedelta(minutes=20),
    )
    store = ReactionEvidenceStore(path)
    try:
        store.append(
            event_id=old_identity.event_id,
            official_event_hash=old_identity.content_hash,
            kind="JIN10_EXPECTATION",
            observed_at=observation.observed_at,
            document=observation.as_dict(),
        )
    finally:
        store.close()

    class NeverActual:
        def actual(self, *_args, **_kwargs):
            raise AssertionError("old expectation reached current official event")

    restarted_store = ReactionEvidenceStore(path)
    provider = ProductionMacroReactionProvider(
        restarted_store,
        jin10_client=None,
        jin10_secret_store=None,
        official_actual_provider=NeverActual(),
        clock=lambda: current_identity.scheduled_at + timedelta(minutes=1),
    )
    provider._identities[current_identity.event_id] = current_identity
    try:
        assert tuple(provider.reactions((current_identity.event_id,))) == ()
        assert len(
            restarted_store.records(
                ((old_identity.event_id, old_identity.content_hash),)
            )
        ) == 1
        assert restarted_store.records(
            ((current_identity.event_id, current_identity.content_hash),)
        ) == ()
    finally:
        provider.close()
