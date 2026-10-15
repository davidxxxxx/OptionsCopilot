from __future__ import annotations

from datetime import date, datetime, timezone
import json
from pathlib import Path

from options_copilot.config import OptionsCopilotConfig
from options_copilot.news_runtime import NewsCoordinator
from options_copilot.providers import (
    AlphaVantageNewsProvider,
    FinnhubEventProvider,
    Jin10EventProvider,
    NasdaqEarningsEvent,
    NasdaqEarningsProvider,
    SecCurrent8KProvider,
)
from options_copilot.security.jin10_credentials import (
    activate_jin10_credential,
)
from options_copilot.security.local_api_keys import (
    LocalJin10EnvelopeReader,
    local_api_key_path,
)
from options_copilot.security.revocation import (
    JIN10_SECRET_NAME,
    RevocationAttestation,
    reserve_rotation,
    write_revocation_attestation,
)
import options_copilot.runtime as runtime_module
from options_copilot.runtime import _configured_event_providers


def _source_api_contract_red(detail: str) -> None:
    assert False, f"PHASE2_EXPECTED_RED:SOURCE_API_CONTRACT {detail}"


def test_public_sec_and_nasdaq_providers_are_default_without_api_keys(
    tmp_path: Path,
) -> None:
    config = OptionsCopilotConfig(
        data_dir=tmp_path / "data",
        log_dir=tmp_path / "logs",
    )

    news, calendars = _configured_event_providers(config)

    assert [type(item) for item in news if isinstance(item, SecCurrent8KProvider)] == [
        SecCurrent8KProvider
    ]
    assert [type(item) for item in news if isinstance(item, Jin10EventProvider)] == [
        Jin10EventProvider
    ]
    assert [
        type(item) for item in calendars if isinstance(item, NasdaqEarningsProvider)
    ] == [NasdaqEarningsProvider]
    jin10 = next(item for item in news if isinstance(item, Jin10EventProvider))
    assert jin10.news(["SPY"]) == ()
    assert jin10.health == "DOWN"
    assert jin10.health_reason == "credential_not_activated"
    assert all(
        getattr(item, "decision_authority", "SUPPORTING_ONLY")
        == "SUPPORTING_ONLY"
        for item in (*news, *calendars)
    )


def test_default_composition_includes_explicit_unconfigured_company_ir_row(
    tmp_path: Path,
) -> None:
    config = OptionsCopilotConfig(
        data_dir=tmp_path / "data",
        log_dir=tmp_path / "logs",
    )

    news, calendars = _configured_event_providers(config)
    matching: list[tuple[object, dict[str, object]]] = []
    for provider in (*news, *calendars):
        health_snapshot = getattr(provider, "health_snapshot", None)
        if not callable(health_snapshot):
            continue
        snapshot = health_snapshot()
        if isinstance(snapshot, dict) and snapshot.get("source_id") == "company_ir":
            matching.append((provider, snapshot))
    if not matching:
        _source_api_contract_red("explicit company_ir composition is missing")

    assert len(matching) == 1
    provider, snapshot = matching[0]
    assert snapshot == {
        "source_id": "company_ir",
        "configured": False,
        "readiness": "UNCONFIGURED",
        "status": "UNCONFIGURED",
        "observed_at": None,
        "as_of": None,
        "last_success_at": None,
        "freshness_age_seconds": None,
        "provenance": (),
        "pacing": "PACING_UNVERIFIED",
        "reason": "UNCONFIGURED",
        "decision_authority": "SUPPORTING_ONLY",
    }
    assert getattr(provider, "decision_authority", "SUPPORTING_ONLY") == (
        "SUPPORTING_ONLY"
    )
    assert not callable(getattr(provider, "discover_url", None))


def test_optional_news_providers_are_composed_from_local_json(tmp_path: Path) -> None:
    config = OptionsCopilotConfig(
        data_dir=tmp_path / "data",
        log_dir=tmp_path / "logs",
    )
    config.ensure_runtime_directories()
    local_api_key_path(config.data_dir).write_text(
        json.dumps(
            {
                "jin10_mcp_token": "",
                "finnhub_api_key": "finnhub-fixture",
                "alpha_vantage_api_key": "alpha-fixture",
                "deepseek_api_key": "",
            }
        ),
        encoding="utf-8",
    )

    news, calendars = _configured_event_providers(config)

    news_finnhub = next(item for item in news if isinstance(item, FinnhubEventProvider))
    calendar_finnhub = next(
        item for item in calendars if isinstance(item, FinnhubEventProvider)
    )
    assert news_finnhub is not calendar_finnhub
    assert any(isinstance(item, AlphaVantageNewsProvider) for item in news)


def test_corrupt_optional_secret_store_does_not_disable_public_sources(
    tmp_path: Path,
) -> None:
    config = OptionsCopilotConfig(
        data_dir=tmp_path / "data",
        log_dir=tmp_path / "logs",
    )
    config.ensure_runtime_directories()
    local_api_key_path(config.data_dir).write_text("not-json", encoding="utf-8")

    news, calendars = _configured_event_providers(config)

    assert any(isinstance(item, SecCurrent8KProvider) for item in news)
    assert any(isinstance(item, NasdaqEarningsProvider) for item in calendars)


def test_jin10_is_visible_but_transport_disabled_for_unactivated_credential(
    monkeypatch,
    tmp_path: Path,
) -> None:
    config = OptionsCopilotConfig(
        data_dir=tmp_path / "data",
        log_dir=tmp_path / "logs",
    )

    class Store:
        value: str | None = "legacy-unactivated-value"

        def names(self) -> tuple[str, ...]:
            return (JIN10_SECRET_NAME,) if self.value is not None else ()

        def get(self, name: str) -> str | None:
            return self.value if name == JIN10_SECRET_NAME else None

    store = Store()
    monkeypatch.setattr(runtime_module, "LocalApiKeyStore", lambda _path: store)

    news, calendars = _configured_event_providers(config)
    jin10 = [item for item in news if isinstance(item, Jin10EventProvider)]
    assert len(jin10) == 1
    assert jin10[0].news(["SPY"]) == ()
    assert jin10[0].health == "DOWN"
    assert jin10[0].health_reason == "credential_not_activated"
    assert any(isinstance(item, SecCurrent8KProvider) for item in news)
    assert any(isinstance(item, NasdaqEarningsProvider) for item in calendars)

    store.value = "fixture-only-value"
    news, _calendars = _configured_event_providers(config)
    jin10 = [item for item in news if isinstance(item, Jin10EventProvider)]
    assert len(jin10) == 1
    assert jin10[0].news(["SPY"]) == ()
    assert jin10[0].health == "DOWN"
    assert jin10[0].health_reason == "credential_not_activated"


def test_jin10_is_composed_only_for_latest_activated_generation(
    monkeypatch,
    tmp_path: Path,
) -> None:
    config = OptionsCopilotConfig(
        data_dir=tmp_path / "data",
        log_dir=tmp_path / "logs",
    )
    evidence_dir = (
        config.data_dir / "evidence" / "checkpoints" / "P3" / "jin10-rotation"
    )
    attestation = RevocationAttestation.create(
        name=JIN10_SECRET_NAME,
        actor="human:test",
        signed_at=datetime(2026, 8, 6, 1, tzinfo=timezone.utc),
    )
    write_revocation_attestation(attestation, evidence_dir)
    class Store:
        value = "fixture-only-value"

        @classmethod
        def names(cls) -> tuple[str, ...]:
            return (JIN10_SECRET_NAME,)

        @classmethod
        def get(cls, name: str) -> str | None:
            return cls.value if name == JIN10_SECRET_NAME else None

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

    calls: list[str] = []

    class Mcp:
        transport_verified = True

        def fetch_news(self, token: str, *, limit: int):
            calls.append(token)
            raise AssertionError("credential drift must stop before transport")

    with reserve_rotation(attestation, evidence_dir) as reservation:
        activate_jin10_credential(
            evidence_dir,
            attestation_hash=attestation.canonical_hash,
            credential_generation=generation,
            activated_at=datetime(2026, 8, 6, 1, 1, tzinfo=timezone.utc),
        )
        reservation.commit(
            rotated_at=datetime(2026, 8, 6, 1, 2, tzinfo=timezone.utc)
        )
    monkeypatch.setattr(runtime_module, "LocalApiKeyStore", lambda _path: Store())
    monkeypatch.setattr(runtime_module, "Jin10McpHttpClient", Mcp)

    news, calendars = _configured_event_providers(
        config,
        jin10_binding_store=binding_store,
    )

    jin10 = [item for item in news if isinstance(item, Jin10EventProvider)]
    assert len(jin10) == 1
    assert jin10[0].transport_verified is True
    assert any(isinstance(item, SecCurrent8KProvider) for item in news)
    assert any(isinstance(item, NasdaqEarningsProvider) for item in calendars)

    Store.value = "unactivated-replacement"
    assert jin10[0].news(["SPY"]) == ()
    assert jin10[0].health == "DOWN"
    assert jin10[0].health_reason == "credential_not_activated"
    assert calls == []


def test_nasdaq_estimate_metadata_survives_calendar_projection(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 8, 6, 12, tzinfo=timezone.utc)
    event = NasdaqEarningsEvent(
        event_id="nasdaq-earnings-2026-08-06-nvda",
        symbol="NVDA",
        report_date=date(2026, 8, 6),
        hour="amc",
        eps_estimate=None,
        revenue_estimate=None,
        source="Nasdaq Earnings Calendar",
        first_seen_at=now,
        ingested_at=now,
        observed_at=now,
        published_at=now,
        source_url=(
            "https://api.nasdaq.com/api/calendar/earnings?date=2026-08-06"
        ),
        report_session="AFTER_MARKET",
    )

    class CalendarProvider:
        health = "READY"

        def earnings_calendar(self, _start: date, _end: date):
            return (event,)

    coordinator = NewsCoordinator(
        tmp_path / "news.sqlite3",
        calendar_providers=(CalendarProvider(),),
        core_symbols=("NVDA",),
        clock=lambda: now,
    )
    try:
        coordinator.refresh_once()
        row = coordinator.calendar_payload()["calendar"][0]
        assert row["report_session"] == "AFTER_MARKET"
        assert row["is_estimated"] is True
        assert row["schedule_precision"] == "ESTIMATED"
        assert row["source_url"].startswith("https://api.nasdaq.com/")
        assert row["decision_authority"] == "SUPPORTING_ONLY"
    finally:
        coordinator.close()
