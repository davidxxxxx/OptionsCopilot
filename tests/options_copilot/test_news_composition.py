from __future__ import annotations

import importlib
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Mapping

import pytest

from options_copilot.news.composition import (
    build_optional_news_classifier,
    build_optional_shadow_news_classifier,
)
from options_copilot.news.models import NewsAuthority, NewsInput


NOW = datetime(2026, 8, 4, 12, 0, tzinfo=timezone.utc)


def _expected_red(detail: str) -> None:
    assert False, f"PHASE2_EXPECTED_RED:DEEPSEEK_CONTRACT {detail}"


class _Secrets:
    def __init__(self, value: str | None) -> None:
        self.value = value
        self.reads = 0

    def get(self, name: str) -> str | None:
        assert name == "DEEPSEEK_API_KEY"
        self.reads += 1
        return self.value


@dataclass
class _Result:
    model_json: Mapping[str, object] | None


class _Client:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def complete(self, **_: object) -> _Result:
        self.calls.append(dict(_))
        return _Result(
            {
                "category": "GUIDANCE",
                "symbols": ["AAPL"],
                "direction": "BULLISH",
                "horizon": "DAYS_1_3",
                "confidence": "0.81",
                "counter_evidence": ["Demand could normalize"],
                "evidence_ids": ["evidence-1"],
            }
        )


def _news() -> NewsInput:
    return NewsInput(
        event_id="news-composition-1",
        headline="Company raises guidance",
        summary="Demand exceeded the prior range.",
        source="Company IR",
        source_url="https://example.test/news",
        published_at=NOW,
        first_seen_at=NOW + timedelta(seconds=2),
        evidence_ids=("evidence-1",),
        symbols=("AAPL",),
        authority=NewsAuthority.ANCHORED,
    )


def test_disabled_model_does_not_read_the_secret(tmp_path: Path) -> None:
    secrets = _Secrets("must-not-be-read")
    factory_calls: list[dict[str, object]] = []

    def factory(**kwargs: object) -> _Client:
        factory_calls.append(dict(kwargs))
        return _Client()

    classifier = build_optional_news_classifier(
        enabled=False,
        secrets=secrets,
        cost_state_path=tmp_path / "cost.json",
        client_factory=factory,
    )

    assert classifier is None
    assert secrets.reads == 0
    assert factory_calls == []


def test_missing_model_secret_preserves_deterministic_default(tmp_path: Path) -> None:
    secrets = _Secrets(None)

    classifier = build_optional_news_classifier(
        enabled=True,
        secrets=secrets,
        cost_state_path=tmp_path / "cost.json",
    )

    assert classifier is None
    assert secrets.reads == 1


def test_enabled_model_is_schema_validated_and_cost_bounded(tmp_path: Path) -> None:
    captured: dict[str, object] = {}
    client = _Client()

    def factory(**kwargs: object) -> _Client:
        captured.update(kwargs)
        return client

    classifier = build_optional_news_classifier(
        enabled=True,
        secrets=_Secrets("fixture-only-key"),
        cost_state_path=tmp_path / "cost.json",
        client_factory=factory,
    )

    assert classifier is not None
    result = classifier.classify(_news())
    assert result.classifier == "STRUCTURED_LLM"
    meter = captured["cost_meter"]
    assert client.calls[0]["model"] == "deepseek-v4-flash"
    assert client.calls[0]["stream"] is False
    assert client.calls[0]["thinking"] is False
    assert meter.policy.daily_spend_cap_usd == 0.25  # type: ignore[attr-defined]
    assert meter.policy.flash_call_cap == 100  # type: ignore[attr-defined]
    assert meter.policy.pro_call_cap == 1  # type: ignore[attr-defined]
    assert captured["timeout_seconds"] == 6.5
    assert captured["max_completion_tokens"] == 600
    if captured.get("max_request_bytes") != 65_536:
        _expected_red(
            f"request byte cap differs: {captured.get('max_request_bytes')!r}"
        )


def test_shadow_composition_does_not_hide_model_failure_behind_fallback(
    tmp_path: Path,
) -> None:
    class FailingClient:
        def complete(self, **_: object) -> _Result:
            raise RuntimeError("provider detail must not escape")

    classifier = build_optional_shadow_news_classifier(
        enabled=True,
        secrets=_Secrets("fixture-only-key"),
        cost_state_path=tmp_path / "cost.json",
        client_factory=lambda **_: FailingClient(),
    )

    assert classifier is not None
    with pytest.raises(RuntimeError, match="provider detail must not escape"):
        classifier.classify(_news())


def test_missing_blank_or_unreadable_secret_never_constructs_client(
    tmp_path: Path,
) -> None:
    class RaisingSecrets:
        def get(self, name: str) -> str | None:
            assert name == "DEEPSEEK_API_KEY"
            raise OSError("synthetic unreadable key store")

    for secrets in (_Secrets(None), _Secrets(" \t"), RaisingSecrets()):
        factory_calls: list[dict[str, object]] = []

        def factory(**kwargs: object) -> _Client:
            factory_calls.append(dict(kwargs))
            return _Client()

        classifier = build_optional_news_classifier(
            enabled=True,
            secrets=secrets,
            cost_state_path=tmp_path / "cost.json",
            client_factory=factory,
        )

        assert classifier is None
        assert factory_calls == []


def test_invalid_secret_shape_never_constructs_client(tmp_path: Path) -> None:
    factory_calls: list[dict[str, object]] = []

    def factory(**kwargs: object) -> _Client:
        factory_calls.append(dict(kwargs))
        return _Client()

    classifier = build_optional_news_classifier(
        enabled=True,
        secrets=_Secrets("fixture\ninjected-header"),
        cost_state_path=tmp_path / "cost.json",
        client_factory=factory,
    )

    if classifier is not None or factory_calls:
        _expected_red("invalid secret shape reached client construction")


def _phase2_builder() -> object:
    module = importlib.import_module("options_copilot.news.composition")
    builder = getattr(module, "build_optional_phase2_advisory", None)
    if not callable(builder):
        _expected_red("build_optional_phase2_advisory is unavailable")
    return builder


def test_phase2_disabled_mode_performs_zero_secret_or_client_work(
    tmp_path: Path,
) -> None:
    builder = _phase2_builder()
    secrets = _Secrets("must-not-be-read")
    factory_calls: list[dict[str, object]] = []

    def factory(**kwargs: object) -> _Client:
        factory_calls.append(dict(kwargs))
        return _Client()

    result = builder(  # type: ignore[operator]
        enabled=False,
        secrets=secrets,
        cost_state_path=tmp_path / "phase2-cost.json",
        readiness_evidence=None,
        client_factory=factory,
    )

    if secrets.reads != 0 or factory_calls:
        _expected_red("disabled advisory touched secret or client factory")
    reason = str(getattr(result, "fallback_reason", ""))
    if not reason.endswith("MODEL_DISABLED"):
        _expected_red(f"disabled reason differs: {reason!r}")


def test_phase2_evaluation_pending_stops_before_secret_or_client_work(
    tmp_path: Path,
) -> None:
    builder = _phase2_builder()
    secrets = _Secrets("must-not-be-read")
    factory_calls: list[dict[str, object]] = []

    def factory(**kwargs: object) -> _Client:
        factory_calls.append(dict(kwargs))
        return _Client()

    result = builder(  # type: ignore[operator]
        enabled=True,
        secrets=secrets,
        cost_state_path=tmp_path / "phase2-cost.json",
        readiness_evidence=None,
        client_factory=factory,
    )

    if secrets.reads != 0 or factory_calls:
        _expected_red("evaluation-pending advisory touched secret or client factory")
    reason = str(getattr(result, "fallback_reason", ""))
    if not reason.endswith("MODEL_EVALUATION_PENDING"):
        _expected_red(f"evaluation-pending reason differs: {reason!r}")


def test_phase2_alias_or_readiness_mismatch_cannot_substitute_model(
    tmp_path: Path,
) -> None:
    builder = _phase2_builder()
    secrets = _Secrets("must-not-be-read")
    factory_calls: list[dict[str, object]] = []

    def factory(**kwargs: object) -> _Client:
        factory_calls.append(dict(kwargs))
        return _Client()

    forged_readiness = {
        "review_status": "SELF_HASHED",
        "formal_gold": True,
        "critical_second_human": "actor-label-only",
        "listed_options_cases_reviewed": ["P2-18"],
        "alias_compatibility_model": "deepseek-v4-pro",
        "pricing_verified_at": NOW.isoformat(),
    }
    result = builder(  # type: ignore[operator]
        enabled=True,
        secrets=secrets,
        cost_state_path=tmp_path / "phase2-cost.json",
        readiness_evidence=forged_readiness,
        client_factory=factory,
    )

    if secrets.reads != 0 or factory_calls:
        _expected_red("forged readiness or alias mismatch reached secret/client work")
    reason = str(getattr(result, "fallback_reason", ""))
    if not (
        reason.endswith("MODEL_EVALUATION_PENDING")
        or reason.endswith("MODEL_ALIAS_MISMATCH")
    ):
        _expected_red(f"readiness mismatch reason differs: {reason!r}")
