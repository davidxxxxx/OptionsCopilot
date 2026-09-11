"""Hostile provider runtime truth tests for optional news sources."""
from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from options_copilot.news_runtime import NewsCoordinator
from options_copilot.providers.events import (
    AlphaVantageNewsProvider,
    FinnhubEventProvider,
)
from options_copilot.providers.official import CompanyIrEventProvider


NOW = datetime(2026, 8, 22, 12, tzinfo=timezone.utc)


class _Secrets:
    def __init__(self, values: dict[str, str]) -> None:
        self._values = dict(values)

    def get(self, name: str) -> str | None:
        return self._values.get(name)


def test_finnhub_related_metadata_without_entity_proof_stays_quarantined() -> None:
    def transport(_url: str, **_kwargs: object) -> object:
        return [
            {
                "datetime": int(NOW.timestamp()),
                "headline": "Cloud demand remains resilient",
                "related": "MSFT",
                "source": "Example Wire",
                "summary": "The provider payload supplies no issuer identity proof.",
                "url": "https://example.test/story",
            }
        ]

    provider = FinnhubEventProvider(
        _Secrets({"FINNHUB_API_KEY": "secret-must-not-render"}),
        transport=transport,
        now=lambda: NOW,
    )

    events = provider.company_news("MSFT", date(2026, 8, 20), NOW.date())

    assert len(events) == 1
    assert events[0].symbol_binding_status == "PROVIDER_RELATED_UNVERIFIED"
    assert events[0].symbol_binding_proof is not None
    assert events[0].symbol_binding_proof.verified is False
    assert provider.health == "PARTIAL_PARSE"
    assert provider.health_reason == "SYMBOL_BINDING_UNVERIFIED"
    snapshot = provider.health_snapshot()
    assert snapshot["reason"] == "PROVIDER_RELATED_ENTITY_PROOF_MISSING"
    assert "secret-must-not-render" not in repr(snapshot)


def test_finnhub_precise_binding_reason_survives_runtime_projection(
    tmp_path: Path,
) -> None:
    provider = FinnhubEventProvider(
        _Secrets({"FINNHUB_API_KEY": "fixture-only"}),
        transport=lambda _url, **_kwargs: [
            {
                "datetime": int(NOW.timestamp()),
                "headline": "Cloud demand remains resilient",
                "related": "MSFT",
                "source": "Example Wire",
                "summary": "No deterministic issuer identity is supplied.",
                "url": "https://example.test/story",
            }
        ],
        now=lambda: NOW,
    )
    coordinator = NewsCoordinator(
        tmp_path / "news.sqlite3",
        news_providers=(provider,),
        core_symbols=("MSFT",),
        clock=lambda: NOW,
        cadence_path=tmp_path / "cadence.json",
    )
    try:
        coordinator.refresh_once()
        payload = coordinator.news_payload()
        source_health = next(
            row for row in payload["source_health"] if row["source"] == "FINNHUB"
        )
        source_runtime = next(
            row
            for row in payload["source_runtime"]
            if row["source_id"] == "FINNHUB" and row["source_kind"] == "NEWS"
        )
        assert source_health["reason"] == (
            "PROVIDER_RELATED_ENTITY_PROOF_MISSING"
        )
        assert source_runtime["failure_code"] == (
            "PROVIDER_RELATED_ENTITY_PROOF_MISSING"
        )
    finally:
        coordinator.close()


@pytest.mark.parametrize(
    ("payload", "status", "reason"),
    [
        (
            {"Note": "Standard API rate limit is 25 requests per day."},
            "RATE_LIMITED",
            "RATE_LIMITED",
        ),
        (
            {"Information": "API request limit has been reached."},
            "RATE_LIMITED",
            "RATE_LIMITED",
        ),
        (
            {"Note": "This endpoint is temporarily in maintenance."},
            "DEGRADED",
            "PROVIDER_NOTE_RESPONSE",
        ),
        (
            {"Error Message": "Invalid API call."},
            "DEGRADED",
            "PROVIDER_ERROR_RESPONSE",
        ),
    ],
)
def test_alpha_vantage_control_envelopes_are_distinguished_without_text_leak(
    payload: object,
    status: str,
    reason: str,
) -> None:
    provider = AlphaVantageNewsProvider(
        _Secrets({"ALPHA_VANTAGE_API_KEY": "alpha-secret-must-not-render"}),
        transport=lambda _url, **_kwargs: payload,
        now=lambda: NOW,
    )

    assert provider.news(("SPY",)) == ()
    assert provider.health == status
    assert provider.health_reason == reason
    snapshot = provider.health_snapshot()
    assert snapshot["status"] == status
    assert snapshot["reason"] == reason
    assert snapshot["last_success_at"] is None
    assert "alpha-secret-must-not-render" not in repr(snapshot)
    assert "Invalid API call" not in repr(snapshot)


def test_alpha_vantage_empty_feed_is_a_successful_empty_observation() -> None:
    provider = AlphaVantageNewsProvider(
        _Secrets({"ALPHA_VANTAGE_API_KEY": "fixture-only"}),
        transport=lambda _url, **_kwargs: {"feed": []},
        now=lambda: NOW,
    )

    assert provider.news(("SPY",)) == ()
    assert provider.health == "READY"
    assert provider.health_reason is None
    assert provider.last_observed_at == NOW
    assert provider.last_success_at == NOW


def test_alpha_vantage_daily_cadence_does_not_retry_rate_limit_response(
    tmp_path: Path,
) -> None:
    calls = 0

    def transport(_url: str, **_kwargs: object) -> object:
        nonlocal calls
        calls += 1
        return {"Note": "Standard API rate limit is 25 requests per day."}

    provider = AlphaVantageNewsProvider(
        _Secrets({"ALPHA_VANTAGE_API_KEY": "fixture-only"}),
        transport=transport,
        now=lambda: NOW,
    )
    coordinator = NewsCoordinator(
        tmp_path / "news.sqlite3",
        news_providers=(provider,),
        core_symbols=("SPY",),
        clock=lambda: NOW,
        cadence_path=tmp_path / "cadence.json",
    )
    try:
        coordinator.refresh_once()
        coordinator.refresh_once()
        assert calls == 1
        runtime = {
            (row["source_id"], row["source_kind"]): row
            for row in coordinator.news_payload()["source_runtime"]
        }
        alpha = runtime[("ALPHA_VANTAGE", "NEWS")]
        assert alpha["failure_code"] == "RATE_LIMITED"
        assert alpha["attempt_count"] == 1
        assert alpha["next_due"] == "2026-08-23T12:00:00+00:00"
    finally:
        coordinator.close()


def test_empty_company_ir_registry_is_suppressed_not_retried_or_configured(
    tmp_path: Path,
) -> None:
    provider = CompanyIrEventProvider(now=lambda: NOW)
    coordinator = NewsCoordinator(
        tmp_path / "news.sqlite3",
        news_providers=(provider,),
        core_symbols=("SPY",),
        clock=lambda: NOW,
        cadence_path=tmp_path / "cadence.json",
    )
    try:
        coordinator.refresh_once()
        coordinator.refresh_once()
        runtime = {
            (row["source_id"], row["source_kind"]): row
            for row in coordinator.news_payload()["source_runtime"]
        }
        company_ir = runtime[("COMPANY_IR", "NEWS")]
        assert company_ir["configured"] is False
        assert company_ir["cadence_status"] == "SUPPRESSED"
        assert company_ir["attempt_count"] == 0
        assert company_ir["last_attempt"] is None
        assert provider.health_snapshot()["configured"] is False
        assert provider.last_observed_at is None
    finally:
        coordinator.close()
