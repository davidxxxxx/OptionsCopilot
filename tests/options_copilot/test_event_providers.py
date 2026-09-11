from __future__ import annotations

from collections.abc import Iterator
from datetime import date, datetime, timedelta, timezone
from dataclasses import replace
import json
from pathlib import Path
import tempfile
import traceback
from urllib.parse import parse_qs, urlsplit

import pytest

from options_copilot.providers.events import (
    AlphaVantageNewsProvider,
    FinnhubEventProvider,
    NewsAggregator,
    NewsEvent,
    ProviderUnavailable,
    SymbolBindingProof,
)
from options_copilot.providers.entity_linking import (
    ENTITY_LINK_CATALOG_HASH,
    ENTITY_LINK_CATALOG_VERSION,
)
from options_copilot.providers import cli as provider_cli
from options_copilot.providers.jin10 import Jin10EventProvider
from options_copilot.providers.jin10_mcp import Jin10McpNewsBatch
from options_copilot.providers.official import OfficialCalendarEvent, OfficialEventProvider
from options_copilot.security.jin10_credentials import (
    activate_jin10_credential,
    jin10_rotation_evidence_dir,
)
from options_copilot.security.local_api_keys import LocalJin10EnvelopeReader
from options_copilot.security.revocation import (
    JIN10_SECRET_NAME,
    RevocationAttestation,
    reserve_rotation,
    write_revocation_attestation,
)
from options_copilot.storage.canonical import canonical_hash, canonical_json


NOW = datetime(2026, 8, 3, 12, 0, tzinfo=timezone.utc)
ROOT = Path(__file__).resolve().parents[2]
CHECKPOINT_SOURCE_IDS = (
    "sec",
    "nasdaq",
    "company_ir",
    "finnhub",
    "alpha_vantage",
    "jin10",
)


def _checkpoint_contract_red(detail: str) -> None:
    assert False, f"PHASE2_EXPECTED_RED:CHECKPOINT_CONTRACT {detail}"


def _require_checkpoint_contract() -> None:
    if tuple(provider_cli.PROVIDER_NAMES) != CHECKPOINT_SOURCE_IDS:
        _checkpoint_contract_red("six-source provider probe contract is missing")


@pytest.fixture
def g_provider_probe_root() -> Iterator[Path]:
    parent = (
        ROOT
        / "data"
        / "options_copilot"
        / "evidence"
        / "checkpoints"
        / "P2"
        / "provider-probe"
        / "test-runs"
    )
    parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="checkpoint-contract-", dir=parent) as value:
        root = Path(value).resolve()
        assert root.drive.upper() == "G:"
        yield root


class FakeSecrets:
    def __init__(self, values):
        self.values = values

    def get(self, name):
        return self.values.get(name)


def test_finnhub_uses_header_token_and_builds_point_in_time_news() -> None:
    calls = []

    def transport(url, *, headers, timeout_seconds):
        calls.append((url, headers, timeout_seconds))
        return [
            {
                "id": 7312345,
                "datetime": 1785758400,
                "headline": "$AAPL announces a material update",
                "related": "AAPL",
                "source": "Reuters",
                "summary": "Summary only",
                "url": "https://example.test/story",
            }
        ]

    provider = FinnhubEventProvider(
        FakeSecrets({"FINNHUB_API_KEY": "not-logged-token"}),
        transport=transport,
        now=lambda: NOW,
    )
    events = provider.company_news("aapl", date(2026, 8, 1), date(2026, 8, 3))

    assert len(events) == 1 and events[0].symbol == "AAPL"
    assert events[0].provider_adapter == "FINNHUB"
    assert events[0].symbol_binding_status == "VERIFIED_PROVIDER_RELATED"
    assert events[0].symbol_binding_proof == SymbolBindingProof(
        schema_version=1,
        method="PROVIDER_RELATED_PLUS_ENTITY_LINK",
        provider_adapter="FINNHUB",
        requested_symbol="AAPL",
        provider_symbols=("AAPL",),
        corroborating_terms=(
            "AAPL",
            "METHOD=EXPLICIT_CASHTAG",
            f"CATALOG_VERSION={ENTITY_LINK_CATALOG_VERSION}",
            f"CATALOG_HASH={ENTITY_LINK_CATALOG_HASH.upper()}",
        ),
        verified=True,
    )
    assert events[0].first_seen_at == NOW and events[0].ingested_at == NOW
    assert events[0].provider_story_id == "FINNHUB:7312345"
    assert events[0].source_id == "FINNHUB:7312345"
    assert "not-logged-token" not in calls[0][0]
    assert calls[0][1]["X-Finnhub-Token"] == "not-logged-token"


def test_finnhub_story_id_is_stable_across_requested_symbols() -> None:
    def transport(_url, *, headers, timeout_seconds):
        del headers, timeout_seconds
        return [
            {
                "id": 998877,
                "datetime": 1785758400,
                "headline": "$AAPL and $MSFT announce a cloud partnership",
                "related": "AAPL,MSFT",
                "source": "Reuters",
                "summary": "Apple and Microsoft announced the same partnership.",
                "url": "https://example.test/shared-story",
            }
        ]

    provider = FinnhubEventProvider(
        FakeSecrets({"FINNHUB_API_KEY": "not-logged-token"}),
        transport=transport,
        now=lambda: NOW,
    )

    apple = provider.company_news("AAPL", NOW.date(), NOW.date())[0]
    microsoft = provider.company_news("MSFT", NOW.date(), NOW.date())[0]

    assert apple.symbol == "AAPL"
    assert microsoft.symbol == "MSFT"
    assert apple.event_id == microsoft.event_id
    assert apple.provider_story_id == microsoft.provider_story_id == "FINNHUB:998877"


def test_finnhub_related_without_entity_corroboration_is_quarantined() -> None:
    def transport(_url, *, headers, timeout_seconds):
        del headers, timeout_seconds
        return [
            {
                "datetime": 1785758400,
                "headline": "Embraer announces a material update",
                "related": "AMZN",
                "source": "Reuters",
                "summary": "The Brazilian aircraft maker updated guidance.",
                "url": "https://example.test/story",
            }
        ]

    provider = FinnhubEventProvider(
        FakeSecrets({"FINNHUB_API_KEY": "not-logged-token"}),
        transport=transport,
        now=lambda: NOW,
    )

    events = provider.company_news("AMZN", date(2026, 8, 1), date(2026, 8, 3))

    assert len(events) == 1
    assert events[0].symbol_binding_status == "PROVIDER_RELATED_UNVERIFIED"
    assert events[0].symbol_binding_proof is not None
    assert events[0].symbol_binding_proof.verified is False
    assert provider.health == "PARTIAL_PARSE"
    assert provider.health_reason == "SYMBOL_BINDING_UNVERIFIED"


def test_finnhub_exact_related_plus_controlled_company_alias_is_verified() -> None:
    def transport(_url, *, headers, timeout_seconds):
        del headers, timeout_seconds
        return [
            {
                "datetime": 1785758400,
                "headline": "Microsoft announces a material cloud update",
                "related": "MSFT",
                "source": "Reuters",
                "summary": "The company updated its Azure outlook.",
                "url": "https://example.test/story",
            }
        ]

    provider = FinnhubEventProvider(
        FakeSecrets({"FINNHUB_API_KEY": "not-logged-token"}),
        transport=transport,
        now=lambda: NOW,
    )

    events = provider.company_news("MSFT", date(2026, 8, 1), date(2026, 8, 3))

    assert len(events) == 1
    proof = events[0].symbol_binding_proof
    assert proof is not None
    assert events[0].symbol_binding_status == "VERIFIED_PROVIDER_RELATED"
    assert proof.method == "PROVIDER_RELATED_PLUS_ENTITY_LINK"
    assert proof.corroborating_terms == (
        "MSFT",
        "METHOD=CONTROLLED_ALIAS",
        f"CATALOG_VERSION={ENTITY_LINK_CATALOG_VERSION}",
        f"CATALOG_HASH={ENTITY_LINK_CATALOG_HASH.upper()}",
    )
    assert proof.verified is True
    assert provider.health == "READY"


@pytest.mark.parametrize(
    ("symbol", "headline"),
    [
        ("A", "A new factory opens in Europe"),
        ("ON", "Factory construction is on schedule"),
        ("IT", "IT spending rises across the sector"),
        ("AI", "AI adoption expands across the sector"),
        ("CAT", "Cat shelters expand nationally"),
        ("CAR", "Car prices fall again"),
        ("RUN", "Run clubs gain popularity"),
        ("LOVE", "Love stories return to cinemas"),
    ],
)
def test_finnhub_ambiguous_bare_ticker_tokens_are_quarantined(
    symbol: str,
    headline: str,
) -> None:
    def transport(_url, *, headers, timeout_seconds):
        del headers, timeout_seconds
        return [
            {
                "datetime": 1785758400,
                "headline": headline,
                "related": symbol,
                "source": "Reuters",
                "summary": "No authoritative issuer identity is supplied.",
                "url": "https://example.test/story",
            }
        ]

    provider = FinnhubEventProvider(
        FakeSecrets({"FINNHUB_API_KEY": "not-logged-token"}),
        transport=transport,
        now=lambda: NOW,
    )

    events = provider.company_news(symbol, date(2026, 8, 1), date(2026, 8, 3))

    assert len(events) == 1
    assert events[0].symbol_binding_status == "PROVIDER_RELATED_UNVERIFIED"
    assert events[0].symbol_binding_proof is not None
    assert events[0].symbol_binding_proof.corroborating_terms == ()
    assert events[0].symbol_binding_proof.verified is False
    assert provider.health == "PARTIAL_PARSE"
    assert provider.health_reason == "SYMBOL_BINDING_UNVERIFIED"


def test_finnhub_implements_coordinator_news_protocol_with_global_limit() -> None:
    calls: list[str] = []

    def transport(url, *, headers, timeout_seconds):
        del headers, timeout_seconds
        query = parse_qs(urlsplit(url).query)
        symbol = query["symbol"][0]
        calls.append(symbol)
        published = NOW - timedelta(minutes=2 if symbol == "AAPL" else 1)
        return [
            {
                "datetime": int(published.timestamp()),
                "headline": f"${symbol} material update",
                "related": symbol,
                "source": "Reuters",
                "summary": "Point-in-time company news.",
                "url": f"https://example.test/{symbol.lower()}",
            }
        ]

    provider = FinnhubEventProvider(
        FakeSecrets({"FINNHUB_API_KEY": "fixture-only-token"}),
        transport=transport,
        now=lambda: NOW,
    )

    events = provider.news(("AAPL", "MSFT"), limit=1)

    assert calls == ["AAPL", "MSFT"]
    assert [event.symbol for event in events] == ["MSFT"]
    assert provider.health == "READY"


def test_finnhub_production_rotation_bounds_each_news_cycle() -> None:
    calls: list[str] = []

    def transport(url, *, headers, timeout_seconds):
        del headers, timeout_seconds
        symbol = parse_qs(urlsplit(url).query)["symbol"][0]
        calls.append(symbol)
        return [
            {
                "datetime": int(NOW.timestamp()),
                "headline": f"${symbol} material update",
                "related": symbol,
                "source": "Reuters",
                "summary": "Point-in-time company news.",
                "url": f"https://example.test/{symbol.lower()}",
            }
        ]

    symbols = ("AAPL", "MSFT", "NVDA")
    provider = FinnhubEventProvider(
        FakeSecrets({"FINNHUB_API_KEY": "fixture-only-token"}),
        transport=transport,
        now=lambda: NOW,
        max_news_symbols_per_cycle=1,
    )
    first_index = int(NOW.timestamp() // 90) % len(symbols)

    first = provider.news(symbols)
    second = provider.news(symbols)

    assert calls == [symbols[first_index], symbols[(first_index + 1) % len(symbols)]]
    assert [item.symbol for item in first] == [symbols[first_index]]
    assert [item.symbol for item in second] == [symbols[(first_index + 1) % len(symbols)]]
    health = provider.health_snapshot()
    assert health["requested_symbol_count"] == 3
    assert health["queried_symbol_count"] == 1
    assert health["coverage_status"] == "BOUNDED"
    assert health["coverage_reason"] == "PROVIDER_SYMBOL_ROTATION"


@pytest.mark.parametrize(
    ("related", "expected_health"),
    [
        ("MSFT", "MISSING_FIELDS"),
        (None, "MISSING_FIELDS"),
        (["AAPL"], "MISSING_FIELDS"),
        ("AAPL/US", "MISSING_FIELDS"),
    ],
)
def test_finnhub_rejects_unverified_company_news_symbol_bindings(
    related: object,
    expected_health: str,
) -> None:
    def transport(_url, **_kwargs):
        return [
            {
                "datetime": int(NOW.timestamp()),
                "headline": "Unrelated company earnings update",
                "related": related,
                "source": "Yahoo",
                "summary": "Must not be attributed to the requested ticker.",
                "url": "https://example.test/unrelated",
            }
        ]

    provider = FinnhubEventProvider(
        FakeSecrets({"FINNHUB_API_KEY": "fixture-only-token"}),
        transport=transport,
        now=lambda: NOW,
    )

    assert provider.company_news("AAPL", NOW.date(), NOW.date()) == ()
    assert provider.health == expected_health
    assert provider.health_reason == "NO_VERIFIED_RELATED_RECORDS"


def test_finnhub_keeps_verified_rows_and_reports_partial_symbol_binding_rejection() -> None:
    def transport(_url, **_kwargs):
        return [
            {
                "datetime": int(NOW.timestamp()),
                "headline": "$AAPL verified update",
                "related": "MSFT, AAPL",
                "source": "Reuters",
                "summary": "Verified related field.",
                "url": "https://example.test/apple",
            },
            {
                "datetime": int((NOW - timedelta(minutes=1)).timestamp()),
                "headline": "Different company update",
                "related": "MSFT",
                "source": "Yahoo",
                "summary": "Must be dropped.",
                "url": "https://example.test/different-company",
            },
        ]

    provider = FinnhubEventProvider(
        FakeSecrets({"FINNHUB_API_KEY": "fixture-only-token"}),
        transport=transport,
        now=lambda: NOW,
    )

    events = provider.company_news("AAPL", NOW.date(), NOW.date())

    assert [event.headline for event in events] == ["$AAPL verified update"]
    assert provider.health == "PARTIAL_PARSE"
    assert provider.health_reason == "SYMBOL_BINDING_REJECTED"


def test_finnhub_distinguishes_verified_but_unusable_rows_from_binding_rejection() -> None:
    def transport(_url, **_kwargs):
        return [
            {
                "datetime": int(NOW.timestamp()),
                "headline": "Different company update",
                "related": "MSFT",
                "source": "Yahoo",
            },
            {
                "datetime": int(NOW.timestamp()),
                "headline": "",
                "related": "AAPL",
                "source": "Reuters",
            },
        ]

    provider = FinnhubEventProvider(
        FakeSecrets({"FINNHUB_API_KEY": "fixture-only-token"}),
        transport=transport,
        now=lambda: NOW,
    )

    assert provider.company_news("AAPL", NOW.date(), NOW.date()) == ()
    assert provider.health == "MISSING_FIELDS"
    assert provider.health_reason == "NO_USABLE_RECORDS"


def test_finnhub_earnings_calendar_is_structured() -> None:
    def transport(_url, **_kwargs):
        return {
            "earningsCalendar": [
                {
                    "symbol": "NVDA",
                    "date": "2026-08-19",
                    "hour": "amc",
                    "epsEstimate": "1.23",
                    "revenueEstimate": "52000000000",
                }
            ]
        }

    provider = FinnhubEventProvider(
        FakeSecrets({"FINNHUB_API_KEY": "token"}),
        transport=transport,
        now=lambda: NOW,
    )
    result = provider.earnings_calendar(date(2026, 8, 1), date(2026, 8, 31))
    assert result[0].symbol == "NVDA"
    assert str(result[0].eps_estimate) == "1.23"
    assert result[0].first_seen_at == NOW


def test_missing_provider_secret_fails_closed() -> None:
    provider = FinnhubEventProvider(FakeSecrets({}), transport=lambda *_a, **_k: [])
    with pytest.raises(ProviderUnavailable):
        provider.company_news("SPY", date(2026, 8, 1), date(2026, 8, 3))


def test_alpha_vantage_metadata_and_cross_provider_deduplication() -> None:
    def transport(_url, **_kwargs):
        return {
            "feed": [
                {
                    "title": "Earnings expectations rise",
                    "time_published": "20260803T120000",
                    "source": "Example",
                    "summary": "Short summary",
                    "url": "https://example.test/a",
                    "overall_sentiment_score": "0.2",
                    "ticker_sentiment": [
                        {"ticker": "AAPL", "ticker_sentiment_score": "0.4"}
                    ],
                }
            ]
        }

    provider = AlphaVantageNewsProvider(
        FakeSecrets({"ALPHA_VANTAGE_API_KEY": "token"}),
        transport=transport,
        now=lambda: NOW,
    )
    event = provider.news(["AAPL"])[0]
    merged = NewsAggregator.merge((event,), (event,))
    assert event.symbol == "AAPL"
    assert str(event.sentiment_score) == "0.4"
    assert len(merged) == 1


def test_alpha_vantage_bounds_daily_cross_check_to_ten_symbols() -> None:
    requested_urls: list[str] = []

    def transport(url: str, **_kwargs: object) -> object:
        requested_urls.append(url)
        return {"feed": []}

    provider = AlphaVantageNewsProvider(
        FakeSecrets({"ALPHA_VANTAGE_API_KEY": "token"}),
        transport=transport,
        now=lambda: NOW,
    )
    symbols = (
        "SPY",
        "QQQ",
        "IWM",
        "DIA",
        "AAPL",
        "MSFT",
        "NVDA",
        "AMZN",
        "META",
        "GOOGL",
        "TSLA",
        "AMD",
    )

    assert provider.news(symbols) == ()
    assert len(requested_urls) == 1
    query = parse_qs(urlsplit(requested_urls[0]).query)
    assert query["tickers"] == [",".join(symbols[:10])]
    assert provider.health_snapshot()["requested_symbol_count"] == 12
    assert provider.health_snapshot()["queried_symbol_count"] == 10
    assert provider.health_snapshot()["coverage_status"] == "BOUNDED"
    assert provider.health_snapshot()["coverage_reason"] == "PROVIDER_TICKER_LIMIT"


def test_alpha_vantage_transport_failure_does_not_chain_token_bearing_url() -> None:
    token = "sentinel-alpha-token-must-not-leak"

    def transport(url, **_kwargs):
        raise RuntimeError(f"GET {url} failed")

    provider = AlphaVantageNewsProvider(
        FakeSecrets({"ALPHA_VANTAGE_API_KEY": token}),
        transport=transport,
        now=lambda: NOW,
    )

    with pytest.raises(ProviderUnavailable) as captured:
        provider.news(["AAPL"])

    rendered = "".join(
        traceback.format_exception(
            captured.type,
            captured.value,
            captured.tb,
        )
    )
    assert token not in rendered
    assert "apikey=" not in rendered.lower()
    assert captured.value.__cause__ is None
    assert captured.value.__context__ is None
    assert provider.health == "DEGRADED"


@pytest.mark.parametrize("failure, health", [
    (TimeoutError(), "TIMEOUT"),
    (RuntimeError("429 too many requests"), "RATE_LIMITED"),
])
def test_jin10_fails_closed_with_fixed_degraded_health(failure, health) -> None:
    provider = Jin10EventProvider(
        FakeSecrets({"JIN10_MCP_TOKEN": "test-only"}),
        transport=lambda *_args, **_kwargs: (_ for _ in ()).throw(failure),
        now=lambda: NOW,
    )
    assert provider.news(["AAPL"]) == ()
    assert provider.health == health


def test_jin10_bad_json_and_missing_fields_are_degraded_without_secrets() -> None:
    provider = Jin10EventProvider(
        FakeSecrets({"JIN10_MCP_TOKEN": "test-only"}),
        transport=lambda *_args, **_kwargs: {"data": [{"title": ""}]},
        now=lambda: NOW,
    )
    assert provider.news(["AAPL"]) == ()
    assert provider.health == "MISSING_FIELDS"


def test_official_anchor_and_supplemental_conflict_remain_supporting_only() -> None:
    official = OfficialEventProvider(now=lambda: NOW)
    anchor = official.filing(
        symbol="AAPL", source_id="0001", title="Quarterly report", published_at=NOW - timezone.utc.utcoffset(NOW)
    )
    supplemental = replace(
        anchor, summary="conflicting summary", source="Jin10", source_rank=9,
        content_hash=None, provenance=("Jin10",),
    )
    merged = NewsAggregator.merge((anchor,), (supplemental,))
    assert len(merged) == 2
    assert {event.status for event in merged} == {"CONFLICTED"}
    assert all(event.decision_authority == "SUPPORTING_ONLY" for event in merged)


def test_official_calendar_is_injected_structured_and_supporting_only() -> None:
    calls = []

    def transport(url, *, headers, timeout_seconds):
        calls.append((url, headers, timeout_seconds))
        return {"events": [{"id": "fomc-2026-09-16", "title": "FOMC decision", "scheduled_at": "2026-09-16T18:00:00Z"}]}

    provider = OfficialEventProvider(transport=transport, now=lambda: NOW)
    events = provider.fetch_calendar(
        "https://calendar.example.test/fomc",
        parser=lambda payload: payload["events"],
        source="Federal Reserve",
    )
    assert isinstance(events[0], OfficialCalendarEvent)
    assert events[0].scheduled_at == datetime(2026, 9, 16, 18, tzinfo=timezone.utc)
    assert events[0].decision_authority == "SUPPORTING_ONLY"
    assert calls[0][2] == 8.0 and provider.health == "READY"


def test_provider_probe_cli_writes_exact_read_only_transport_methods(
    monkeypatch: pytest.MonkeyPatch,
    g_provider_probe_root: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _require_checkpoint_contract()
    data_dir = g_provider_probe_root / "data"
    evidence_dir = (
        data_dir / "evidence" / "checkpoints" / "P2" / "provider-probe"
    )
    monkeypatch.setenv("OPTIONS_COPILOT_DATA_DIR", str(data_dir))
    monkeypatch.setenv(
        "OPTIONS_COPILOT_LOG_DIR",
        str(g_provider_probe_root / "logs"),
    )
    calls: list[tuple[str, tuple[str, ...], int]] = []
    conflicted = NewsEvent(
        event_id="probe-conflict",
        symbol="SPY",
        source="Fixture",
        headline="Fixture conflict",
        summary="fixture-only",
        url="https://example.test/item?apikey=sentinel-provider-secret",
        published_at=NOW - timedelta(minutes=3),
        first_seen_at=NOW - timedelta(minutes=2),
        ingested_at=NOW - timedelta(minutes=1),
        observed_at=NOW,
        status="CONFLICTED",
    )

    class NasdaqProbe:
        health = "READY"
        health_reason = None

        def calendar_payload(self):
            calls.append(("nasdaq", (), 0))
            return {
                "status": "READY",
                "observed_at": NOW.isoformat(),
                "events": (),
                "sources": ({"source": "Fixture", "status": "READY"},),
            }

    class NewsProbe:
        health = "READY"
        health_reason = None

        def __init__(self, source_id: str) -> None:
            self.source_id = source_id

        def news(self, symbols, *, limit):
            calls.append((self.source_id, tuple(symbols), limit))
            return (conflicted,)

    class UnconfiguredCompanyIrProbe:
        health = "UNCONFIGURED"
        health_reason = "UNCONFIGURED"

        def news(self, _symbols, *, limit):
            del limit
            raise AssertionError("unconfigured company IR must not be called")

    class LeakyFailureProbe:
        health = "DEGRADED"
        health_reason = "Authorization: Bearer sentinel-provider-secret"

        @staticmethod
        def health_snapshot() -> dict[str, object]:
            return {
                "status": "DEGRADED",
                "reason": "Authorization: Bearer sentinel-provider-secret",
                "raw_body": "sentinel-provider-private-body",
                "raw_error": "sentinel-provider-private-error",
                "redirect_target": (
                    "https://provider.test/redirect?api_key=sentinel-provider-query-key"
                ),
                "account_id": "sentinel-provider-private-account",
                "broker_positions": ["sentinel-provider-private-position"],
                "creator_instruction": "sentinel-provider-private-instruction",
                "local_path": "C:\\Users\\xujie\\sentinel-provider-private-path.json",
            }

        def news(self, symbols, *, limit):
            calls.append(("alpha_vantage", tuple(symbols), limit))
            raise RuntimeError(
                "https://provider.test/query?apikey=sentinel-provider-secret"
            )

    class UnverifiedJin10Probe:
        health = "READY"
        health_reason = None

        def news(self, symbols, *, limit):
            raise AssertionError("unverified Jin10 transport must not be called")

    result = provider_cli.main(
        [
            "probe",
            "--providers",
            "sec,nasdaq,company_ir,finnhub,alpha_vantage,jin10",
            "--symbols",
            "SPY",
            "--limit",
            "1",
            "--json",
            "--evidence-dir",
            str(evidence_dir),
        ],
        provider_map={
            "sec": NewsProbe("sec"),
            "nasdaq": NasdaqProbe(),
            "company_ir": UnconfiguredCompanyIrProbe(),
            "finnhub": NewsProbe("finnhub"),
            "alpha_vantage": LeakyFailureProbe(),
            "jin10": UnverifiedJin10Probe(),
        },
        clock=lambda: NOW,
    )
    assert result == 0
    checkpoint_output = json.loads(capsys.readouterr().out)
    assert checkpoint_output["schema"] == (
        "options_copilot.provider_probe_checkpoint.v2"
    )
    assert checkpoint_output["read_only"] is True
    assert checkpoint_output["decision_authority"] == "SUPPORTING_ONLY"
    assert checkpoint_output["approval_eligible"] is False
    assert checkpoint_output["instruction_creation_allowed"] is False
    assert checkpoint_output["order_submission_allowed"] is False
    assert calls == [
        ("sec", ("SPY",), 1),
        ("nasdaq", (), 0),
        ("finnhub", ("SPY",), 1),
        ("alpha_vantage", ("SPY",), 1),
    ]

    checkpoint_paths = sorted(evidence_dir.glob("*.json"))
    assert len(checkpoint_paths) == 1
    checkpoint_path = checkpoint_paths[0]
    checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    assert checkpoint == checkpoint_output
    digest = canonical_hash(checkpoint)
    assert checkpoint_path.name == f"provider_probe_checkpoint.{digest}.json"
    assert checkpoint_path.read_text(encoding="utf-8") == canonical_json(checkpoint) + "\n"
    assert checkpoint["contract"] == {
        "provider_names": list(CHECKPOINT_SOURCE_IDS),
        "requested_symbols": ["SPY"],
        "limit": 1,
        "read_only": True,
    }
    rows = checkpoint["sources"]
    assert [row["source_id"] for row in rows] == list(CHECKPOINT_SOURCE_IDS)
    assert len(rows) == 6
    for row in rows:
        assert {
            "configured",
            "readiness",
            "status",
            "reason",
            "observed_at",
            "as_of",
            "last_success_at",
            "freshness_age_seconds",
            "provenance",
            "pacing",
            "request_methods",
            "transport_verified",
            "read_only",
            "decision_authority",
        }.issubset(row)
        assert row["read_only"] is True
        assert row["decision_authority"] == "SUPPORTING_ONLY"
    assert checkpoint["redaction"] == {"status": "PASS", "finding_count": 0}
    assert isinstance(checkpoint["conflicts"], list)
    rendered = json.dumps(checkpoint, sort_keys=True).lower()
    assert "sentinel-provider-secret" not in rendered
    assert "authorization:" not in rendered
    assert "bearer " not in rendered
    assert "apikey=" not in rendered
    assert "api_key=" not in rendered
    assert "raw_body" not in rendered
    assert "raw_error" not in rendered
    assert "redirect_target" not in rendered
    assert "account_id" not in rendered
    assert "broker_positions" not in rendered
    assert "creator_instruction" not in rendered
    assert "local_path" not in rendered
    assert "sentinel-provider-private" not in rendered

    original_bytes = checkpoint_path.read_bytes()
    duplicate_result = provider_cli.main(
        [
            "probe",
            "--providers",
            "sec,nasdaq,company_ir,finnhub,alpha_vantage,jin10",
            "--symbols",
            "SPY",
            "--limit",
            "1",
            "--json",
            "--evidence-dir",
            str(evidence_dir),
        ],
        provider_map={
            "sec": NewsProbe("sec"),
            "nasdaq": NasdaqProbe(),
            "company_ir": UnconfiguredCompanyIrProbe(),
            "finnhub": NewsProbe("finnhub"),
            "alpha_vantage": LeakyFailureProbe(),
            "jin10": UnverifiedJin10Probe(),
        },
        clock=lambda: NOW,
    )
    duplicate_output = json.loads(capsys.readouterr().out)
    assert duplicate_result == 2
    assert duplicate_output == {
        "schema": "options_copilot.provider_probe_error.v1",
        "status": "DEGRADED",
        "decision": "NO_TRADE",
        "reason_code": "PROVIDER_PROBE_FAILED",
        "read_only": True,
    }
    assert sorted(evidence_dir.glob("*.json")) == [checkpoint_path]
    assert checkpoint_path.read_bytes() == original_bytes


def test_provider_probe_rejects_escape_and_c_drive_targets_without_artifacts(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    g_provider_probe_root: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _require_checkpoint_contract()
    data_dir = g_provider_probe_root / "data"
    monkeypatch.setenv("OPTIONS_COPILOT_DATA_DIR", str(data_dir))
    monkeypatch.setenv(
        "OPTIONS_COPILOT_LOG_DIR",
        str(g_provider_probe_root / "logs"),
    )
    targets = (
        g_provider_probe_root / "outside-configured-evidence-root",
        Path("C:/") / f"options-copilot-forbidden-{g_provider_probe_root.name}",
    )
    assert targets[0].drive.upper() == "G:"
    assert targets[1].drive.upper() != "G:"

    for target in targets:
        result = provider_cli.main(
            [
                "probe",
                "--providers",
                "sec,nasdaq,company_ir,finnhub,alpha_vantage,jin10",
                "--symbols",
                "SPY",
                "--limit",
                "1",
                "--json",
                "--evidence-dir",
                str(target),
            ],
            provider_map={name: object() for name in CHECKPOINT_SOURCE_IDS},
            clock=lambda: NOW,
        )
        output = json.loads(capsys.readouterr().out)
        assert result == 2
        assert output["reason_code"] == "PROVIDER_PROBE_FAILED"
        assert not target.exists()


def test_provider_probe_redaction_failure_leaves_no_partial_checkpoint(
    monkeypatch: pytest.MonkeyPatch,
    g_provider_probe_root: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _require_checkpoint_contract()
    data_dir = g_provider_probe_root / "data"
    evidence_dir = (
        data_dir / "evidence" / "checkpoints" / "P2" / "provider-probe"
    )
    monkeypatch.setenv("OPTIONS_COPILOT_DATA_DIR", str(data_dir))
    monkeypatch.setenv(
        "OPTIONS_COPILOT_LOG_DIR",
        str(g_provider_probe_root / "logs"),
    )

    def reject_redaction(_value: object, *, path: str = "evidence") -> None:
        del path
        raise provider_cli.ProviderProbeError("provider evidence redaction failed")

    monkeypatch.setattr(provider_cli, "_assert_redacted", reject_redaction)
    result = provider_cli.main(
        [
            "probe",
            "--providers",
            "sec,nasdaq,company_ir,finnhub,alpha_vantage,jin10",
            "--symbols",
            "SPY",
            "--limit",
            "1",
            "--json",
            "--evidence-dir",
            str(evidence_dir),
        ],
        provider_map={name: object() for name in CHECKPOINT_SOURCE_IDS},
        clock=lambda: NOW,
    )
    output = json.loads(capsys.readouterr().out)
    assert result == 2
    assert output == {
        "schema": "options_copilot.provider_probe_error.v1",
        "status": "DEGRADED",
        "decision": "NO_TRADE",
        "reason_code": "PROVIDER_PROBE_FAILED",
        "read_only": True,
    }
    assert not evidence_dir.exists()


def test_provider_probe_calls_verified_jin10_only_with_activated_generation(
    monkeypatch: pytest.MonkeyPatch,
    g_provider_probe_root: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _require_checkpoint_contract()
    secret_value = "sentinel-jin10-value-never-render"
    data_dir = g_provider_probe_root / "data"
    evidence_dir = jin10_rotation_evidence_dir(data_dir)
    attestation = RevocationAttestation.create(
        name=JIN10_SECRET_NAME,
        actor="human:test",
        signed_at=NOW - timedelta(minutes=2),
    )
    write_revocation_attestation(attestation, evidence_dir)
    class Store:
        @staticmethod
        def names() -> tuple[str, ...]:
            return (JIN10_SECRET_NAME,)

        @staticmethod
        def contains(name: str) -> bool:
            return name == JIN10_SECRET_NAME

        @staticmethod
        def get(name: str) -> str | None:
            return secret_value if name == JIN10_SECRET_NAME else None

    class BindingStore:
        value: str | None = None

        def get(self, _name: str) -> str | None:
            return self.value

        def set(self, _name: str, value: str) -> None:
            self.value = value

    binding_store = BindingStore()
    reader = LocalJin10EnvelopeReader(Store(), binding_store)
    generation = reader.create_opaque_binding()
    assert generation is not None

    calls: list[tuple[str, int]] = []

    class VerifiedMcp:
        transport_verified = True

        def fetch_news(self, token: str, *, limit: int) -> Jin10McpNewsBatch:
            calls.append((token, limit))
            return Jin10McpNewsBatch(
                payloads={
                    "list_news": {
                        "status": 200,
                        "data": {
                            "items": [
                                {
                                    "id": "fixture-news-1",
                                    "time": NOW.isoformat(),
                                    "title": "Fixture market update",
                                    "introduction": "Fixture only",
                                    "url": "https://www.jin10.com/article/fixture",
                                }
                            ]
                        },
                    }
                }
            )

    monkeypatch.setenv("OPTIONS_COPILOT_DATA_DIR", str(data_dir))
    monkeypatch.setenv(
        "OPTIONS_COPILOT_LOG_DIR",
        str(g_provider_probe_root / "logs"),
    )
    monkeypatch.setattr(provider_cli, "LocalApiKeyStore", lambda _path: Store())
    monkeypatch.setattr(
        provider_cli,
        "DPAPISecretStore",
        lambda _path: binding_store,
    )
    monkeypatch.setattr(provider_cli, "Jin10McpHttpClient", VerifiedMcp)

    assert (
        provider_cli.main(
            [
                "probe",
                "--providers",
                "jin10",
                "--symbols",
                "SPY",
                "--json",
                "--evidence-dir",
                str(
                    data_dir
                    / "evidence"
                    / "checkpoints"
                    / "P2"
                    / "provider-probe"
                    / "inactive"
                ),
            ],
            clock=lambda: NOW,
        )
        == 0
    )
    inactive_output = capsys.readouterr().out
    assert calls == []
    assert secret_value not in inactive_output

    with reserve_rotation(attestation, evidence_dir) as reservation:
        activate_jin10_credential(
            evidence_dir,
            attestation_hash=attestation.canonical_hash,
            credential_generation=generation,
            activated_at=NOW - timedelta(minutes=1),
        )
        reservation.commit(rotated_at=NOW)

    assert (
        provider_cli.main(
            [
                "probe",
                "--providers",
                "jin10",
                "--symbols",
                "SPY",
                "--json",
                "--evidence-dir",
                str(
                    data_dir
                    / "evidence"
                    / "checkpoints"
                    / "P2"
                    / "provider-probe"
                    / "active"
                ),
            ],
            clock=lambda: NOW,
        )
        == 0
    )

    output = capsys.readouterr().out
    assert calls == [(secret_value, 50)]
    assert secret_value not in output
    rendered_artifacts = "\n".join(
        path.read_text(encoding="utf-8")
        for path in (
            data_dir
            / "evidence"
            / "checkpoints"
            / "P2"
            / "provider-probe"
            / "active"
        ).glob("*.json")
    )
    assert secret_value not in rendered_artifacts
    assert "authorization" not in rendered_artifacts.lower()
