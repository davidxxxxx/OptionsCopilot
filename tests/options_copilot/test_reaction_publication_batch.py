"""The real calendar merge keeps verified reads bounded across event counts."""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from options_copilot.news.reaction_runtime import (
    Jin10MacroObservation,
    ProductionMacroReactionProvider,
    ReactionEvidenceStore,
)
from options_copilot.news_runtime import NewsCoordinator
from options_copilot.providers.official import OfficialCalendarEvent, OfficialEventProvenance


NOW = datetime(2026, 9, 9, 12, 0, tzinfo=timezone.utc)
READERS = ("projection", "coverage", "reactions", "child_reactions", "revision_views")


def _event(index: int, *, future: bool) -> OfficialCalendarEvent:
    source = "Bureau of Labor Statistics"
    source_id = f"synthetic-cpi-{index}"
    source_url = "https://www.bls.gov/news.release/cpi.nr0.htm"
    published = NOW - timedelta(days=30)
    seen = NOW - timedelta(days=20)
    observed = NOW - timedelta(days=1)
    return OfficialCalendarEvent(
        event_id=f"projection-cpi-{index}",
        source=source, source_id=source_id, source_url=source_url,
        title=f"Consumer Price Index August 2026 {index}",
        category="MACRO",
        scheduled_at=(
            NOW + timedelta(minutes=index + 1)
            if future else NOW - timedelta(minutes=index + 10)
        ),
        published_at=published, first_seen_at=seen, observed_at=observed,
        ingested_at=observed, timezone_name="UTC",
        schedule_precision="EXACT",
        symbols=(),
        provenance=(OfficialEventProvenance(
            source=source, source_id=source_id, source_url=source_url,
            source_payload_hash=f"{index + 1:064x}", published_at=published,
            first_seen_at=seen, observed_at=observed, ingested_at=observed,
        ),),
    )


def _seed_synthetic_release_chain(
    store: ReactionEvidenceStore,
    provider: ProductionMacroReactionProvider,
    event: OfficialCalendarEvent,
) -> None:
    state = provider._state()
    root = state.public_to_stable[event.event_id]
    identity = state.identities[root]
    period = state.specs[root].parent.reference_period
    observed = event.scheduled_at - timedelta(seconds=30)
    expectation = Jin10MacroObservation(
        title=event.title, scheduled_at=event.scheduled_at,
        metric="core_cpi_mom_pct", unit="PERCENT", period=period,
        basis="SEASONALLY_ADJUSTED", series_id="synthetic-cpi",
        calculation="LEVEL", consensus=Decimal("0.3"),
        reported_actual=None, previous=None, source_id=f"expectation-{root}",
        observed_at=observed,
    )
    store.append(
        event_id=root, official_event_hash=identity.event_hash,
        kind="JIN10_EXPECTATION", observed_at=observed,
        document=expectation.as_dict(),
    )
    initial_hash = None
    for revision in range(2):
        captured = event.scheduled_at + timedelta(seconds=revision + 1)
        raw_hash = str(revision + 3) * 64
        document = {
            "reference_period": period,
            "measures": [{
                "measure_id": expectation.metric, "unit": expectation.unit,
                "basis": expectation.basis, "value": str(Decimal("0.4") + Decimal(revision) / 10),
            }],
            "declared_release_at": event.scheduled_at.isoformat(),
            "first_observed_release_at": captured.isoformat(),
            "captured_at": captured.isoformat(),
            "official_url": event.source_url, "raw_hash": raw_hash,
            "revision_of": initial_hash, "decision_authority": "SUPPORTING_ONLY",
        }
        # Synthetic hash-valid rows exercise projection, not source authentication.
        with store._lock:
            store._append_extension_row(
                "macro_reaction_release_vintages", event_id=root,
                official_event_hash=identity.event_hash, parent_hash=identity.event_hash,
                raw_hash=raw_hash, observed_at=captured, document=document,
            )
        if revision == 0:
            child = provider.child_reactions((event.event_id,))[event.event_id][0]
            assert child.release_chain
            initial_hash = child.release_chain[0].content_hash


@pytest.mark.parametrize("event_count", [1, 7])
@pytest.mark.parametrize("mode", ["future_empty", "due_empty", "released_with_revision"])
def test_actual_news_merge_has_constant_verification_count(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, event_count: int, mode: str,
) -> None:
    store = ReactionEvidenceStore(tmp_path / "reactions.sqlite3")
    provider = ProductionMacroReactionProvider(
        store, jin10_client=None, jin10_secret_store=None,
        official_actual_provider=object(), clock=lambda: NOW,
    )
    try:
        events = tuple(_event(index, future=mode == "future_empty") for index in range(event_count))
        provider.restore_official_identities(events)
        if mode == "released_with_revision":
            for event in events:
                _seed_synthetic_release_chain(store, provider, event)
        calls: Counter[str] = Counter()

        def counted(name: str, operation: Callable[..., object]) -> Callable[..., object]:
            def invoke(*args: object, **kwargs: object) -> object:
                calls[name] += 1
                return operation(*args, **kwargs)
            return invoke

        monkeypatch.setattr(store, "assert_integrity", counted("integrity", store.assert_integrity))
        for name in READERS:
            monkeypatch.setattr(provider, name, counted(name, getattr(provider, name)))

        def forbidden(*args: object, **kwargs: object) -> None:
            pytest.fail("Projection must not acquire provider or broker data")

        monkeypatch.setattr(provider, "refresh", forbidden)
        monkeypatch.setattr(provider, "refresh_schedule", forbidden)
        monkeypatch.setattr(provider, "refresh_capture", forbidden)
        coordinator = NewsCoordinator.__new__(NewsCoordinator)
        coordinator._reaction_provider = provider
        rows = [{
            **{key: getattr(event, key) for key in (
                "event_id", "source", "source_id", "source_url", "title",
                "category", "schedule_precision", "symbols",
            )},
            **{key: getattr(event, key).isoformat() for key in (
                "scheduled_at", "published_at", "first_seen_at", "observed_at",
            )},
            "calendar_origin": "OFFICIAL", "content_hash": "a" * 64,
            "record_hash": "b" * 64,
        } for event in events]
        projected, status = coordinator._merge_reaction_read_models(
            rows, now=NOW, window_start=NOW - timedelta(days=1),
            window_end=NOW + timedelta(days=14),
            current_official_event_versions={event.event_id: ("a" * 64, "b" * 64) for event in events},
        )

        assert len(projected) == event_count
        if mode == "future_empty":
            assert calls == {"projection": 1, "coverage": 1, "integrity": 2}
            assert all(row["reaction"]["reasons"] == ["WAIT_FOR_DECLARED_RELEASE_TIME"] for row in projected)
        else:
            assert {name: calls[name] for name in READERS} == dict.fromkeys(READERS, 1), status
            assert calls["integrity"] == 5
        if mode == "released_with_revision":
            assert status["status"] == "READY", status
            for row in projected:
                assert len(row["measure_reactions"]) == 1
                assert len(row["revision_views"]) == 1
                assert len(row["revision_views"][0]["revision_history"]) == 1
        elif mode == "due_empty":
            assert status["status"] == "UNAVAILABLE"
        assert status["approval_eligible"] is False
        assert status["order_creation_allowed"] is False
    finally:
        provider.close()
