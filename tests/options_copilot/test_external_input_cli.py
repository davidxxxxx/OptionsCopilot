from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from options_copilot.gateway.external_readonly_feed import (
    OPEN_REPRICE_PURPOSE,
    PREMARKET_ACCOUNT_PURPOSE,
    ExternalReadonlyFeedReader,
)
from options_copilot.market.external_session_calendar import (
    ExternalSessionCalendarProvider,
)
from options_copilot.news.external_top10_source import (
    ExternalTop10StructureSource,
)
from tests.options_copilot.test_external_readonly_feed import (
    COMPLETED_AT,
    REQUESTED_AT,
    _payload as readonly_payload,
)
from tests.options_copilot.test_external_session_calendar import (
    _payload as calendar_payload,
)
from tests.options_copilot.test_external_top10_source import (
    SCHEDULED_FOR,
    _payload as top10_payload,
)


PUBLISHED_AT = COMPLETED_AT + timedelta(seconds=1)
OPEN_REQUESTED_AT = REQUESTED_AT + timedelta(minutes=15)
OPEN_COMPLETED_AT = COMPLETED_AT + timedelta(minutes=15)
OPEN_PUBLISHED_AT = PUBLISHED_AT + timedelta(minutes=15)
OPEN_SCHEDULED_FOR = SCHEDULED_FOR.replace(hour=9, minute=35)


def _premarket_readonly_payload() -> dict[str, object]:
    payload = readonly_payload()
    payload["purpose"] = PREMARKET_ACCOUNT_PURPOSE
    payload["secdefs"] = []
    payload["quotes"] = []
    return payload


def _open_readonly_payload() -> dict[str, object]:
    payload = readonly_payload()
    batch_id = "external-batch-20260806-133500"
    payload["purpose"] = OPEN_REPRICE_PURPOSE
    payload["batch_id"] = batch_id
    payload["requested_at"] = OPEN_REQUESTED_AT
    payload["completed_at"] = OPEN_COMPLETED_AT
    payload["account"]["asof"] = OPEN_COMPLETED_AT
    payload["nav"]["asof"] = OPEN_COMPLETED_AT
    for row in payload["positions"]:
        row["asof"] = OPEN_COMPLETED_AT
    for row in payload["secdefs"]:
        row["batch_id"] = batch_id
        row["requested_at"] = OPEN_REQUESTED_AT
        row["observed_at"] = OPEN_COMPLETED_AT
        row["completed_at"] = OPEN_COMPLETED_AT
    for row in payload["quotes"]:
        row["batch_id"] = batch_id
        row["requested_at"] = OPEN_REQUESTED_AT
        row["observed_at"] = OPEN_COMPLETED_AT
        row["completed_at"] = OPEN_COMPLETED_AT
    return payload


def _bundle() -> dict[str, object]:
    readonly = _premarket_readonly_payload()
    top10 = top10_payload()
    top10["batch_id"] = readonly["batch_id"]
    return {
        "schema": "options_copilot.external_input_bundle",
        "version": 1,
        "scheduled_for": SCHEDULED_FOR,
        "readonly_feed": readonly,
        "top10": top10,
        "session_calendar": calendar_payload(observed_at=COMPLETED_AT),
    }


def _open_bundle() -> dict[str, object]:
    return {
        "schema": "options_copilot.external_input_bundle",
        "version": 1,
        "scheduled_for": OPEN_SCHEDULED_FOR,
        "readonly_feed": _open_readonly_payload(),
        "top10": None,
        "session_calendar": calendar_payload(observed_at=OPEN_COMPLETED_AT),
    }


def _destinations(tmp_path: Path) -> dict[str, Path]:
    return {
        "readonly_feed_path": tmp_path / "external-readonly.json",
        "top10_path": tmp_path / "external-top10.json",
        "session_calendar_path": tmp_path / "external-calendar.json",
    }


def test_complete_bundle_is_validated_then_atomically_published(
    tmp_path: Path,
) -> None:
    from options_copilot.external_input_cli import publish_external_input_bundle

    destinations = _destinations(tmp_path)
    result = publish_external_input_bundle(
        _bundle(),
        **destinations,
        clock=lambda: PUBLISHED_AT,
    )

    assert result["schema"] == "options_copilot.external_input_publish_result.v1"
    assert result["status"] == "PUBLISHED"
    assert result["purpose"] == PREMARKET_ACCOUNT_PURPOSE
    assert result["scheduled_for"] == SCHEDULED_FOR.astimezone(
        COMPLETED_AT.tzinfo
    ).isoformat()
    assert result["decision"] == "OBSERVATION_ONLY"
    assert result["read_only"] is True
    assert result["decision_authority"] == "SUPPORTING_ONLY"
    assert result["instruction_creation_allowed"] is False
    assert result["order_submission_allowed"] is False
    assert len(result["generation_id"]) == 32
    assert len(result["content_hash"]) == 64
    assert {
        role: value["status"] for role, value in result["outputs"].items()
    } == {
        "readonly_feed": "PUBLISHED",
        "top10": "PUBLISHED",
        "session_calendar": "PUBLISHED",
    }
    assert all(
        set(value) == {"role", "status", "sha256", "bytes"}
        and value["role"] == role
        and len(value["sha256"]) == 64
        for role, value in result["outputs"].items()
    )
    assert str(tmp_path) not in json.dumps(result, sort_keys=True)
    assert ExternalReadonlyFeedReader(
        destinations["readonly_feed_path"],
        clock=lambda: PUBLISHED_AT,
    ).read().batch_id == "external-batch-20260806-132000"
    assert len(
        ExternalTop10StructureSource(
            destinations["top10_path"],
            clock=lambda: PUBLISHED_AT,
        ).resolve_top10(scheduled_for=SCHEDULED_FOR)
    ) == 10
    assert ExternalSessionCalendarProvider(
        destinations["session_calendar_path"],
        clock=lambda: PUBLISHED_AT,
    ).snapshot(now=PUBLISHED_AT).verify_hash()
    assert not tuple(tmp_path.glob(".*.prepared"))
    assert not tuple(tmp_path.glob(".*.tmp"))


def test_open_reprice_publishes_fresh_market_inputs_without_touching_top10(
    tmp_path: Path,
) -> None:
    from options_copilot.external_input_cli import publish_external_input_bundle

    destinations = _destinations(tmp_path)
    publish_external_input_bundle(
        _bundle(),
        **destinations,
        clock=lambda: PUBLISHED_AT,
    )
    top10_before = destinations["top10_path"].read_bytes()

    result = publish_external_input_bundle(
        _open_bundle(),
        **destinations,
        clock=lambda: OPEN_PUBLISHED_AT,
    )

    assert result["outputs"]["readonly_feed"]["status"] == "PUBLISHED"
    assert result["outputs"]["session_calendar"]["status"] == "PUBLISHED"
    assert result["outputs"]["top10"] == {
        "role": "top10",
        "status": "UNCHANGED",
        "sha256": result["outputs"]["top10"]["sha256"],
        "bytes": len(top10_before),
    }
    assert destinations["top10_path"].read_bytes() == top10_before
    open_batch = ExternalReadonlyFeedReader(
        destinations["readonly_feed_path"],
        clock=lambda: OPEN_PUBLISHED_AT,
    ).read()
    assert open_batch.purpose == OPEN_REPRICE_PURPOSE
    assert open_batch.batch_id == "external-batch-20260806-133500"
    assert ExternalSessionCalendarProvider(
        destinations["session_calendar_path"],
        clock=lambda: OPEN_PUBLISHED_AT,
    ).snapshot(now=OPEN_PUBLISHED_AT).verify_hash()
    assert not tuple(tmp_path.glob(".*.prepared"))
    assert not tuple(tmp_path.glob(".*.tmp"))


def test_nested_missing_field_fails_closed_without_replacing_last_good_files(
    tmp_path: Path,
) -> None:
    from options_copilot.external_input_cli import publish_external_input_bundle

    destinations = _destinations(tmp_path)
    publish_external_input_bundle(
        _bundle(),
        **destinations,
        clock=lambda: PUBLISHED_AT,
    )
    before = {name: path.read_bytes() for name, path in destinations.items()}
    invalid = _bundle()
    invalid["top10"]["structures"][0].pop("scenario_hash")

    with pytest.raises(ValueError, match="fields are incomplete"):
        publish_external_input_bundle(
            invalid,
            **destinations,
            clock=lambda: PUBLISHED_AT,
        )

    assert {name: path.read_bytes() for name, path in destinations.items()} == before
    assert not tuple(tmp_path.glob(".*.prepared"))
    assert not tuple(tmp_path.glob(".*.tmp"))


def test_premarket_rejects_mismatched_source_batch_ids_without_publishing(
    tmp_path: Path,
) -> None:
    from options_copilot.external_input_cli import (
        ExternalInputBundleError,
        publish_external_input_bundle,
    )

    invalid = _bundle()
    invalid["top10"]["batch_id"] = "different-valid-source-batch"
    destinations = _destinations(tmp_path)

    with pytest.raises(ExternalInputBundleError, match="do not match"):
        publish_external_input_bundle(
            invalid,
            **destinations,
            clock=lambda: PUBLISHED_AT,
        )

    assert all(not path.exists() for path in destinations.values())
    assert not tuple(tmp_path.glob(".*.prepared"))
    assert not tuple(tmp_path.glob(".*.tmp"))


@pytest.mark.parametrize("component", ("readonly_feed", "top10"))
def test_premarket_rejects_missing_shared_source_batch_id(
    tmp_path: Path,
    component: str,
) -> None:
    from options_copilot.external_input_cli import (
        ExternalInputBundleError,
        publish_external_input_bundle,
    )

    invalid = _bundle()
    invalid[component].pop("batch_id")
    destinations = _destinations(tmp_path)

    with pytest.raises(ExternalInputBundleError, match="batch_id is required"):
        publish_external_input_bundle(
            invalid,
            **destinations,
            clock=lambda: PUBLISHED_AT,
        )

    assert all(not path.exists() for path in destinations.values())


def test_matching_source_batch_is_idempotently_republished_with_same_components(
    tmp_path: Path,
) -> None:
    from options_copilot.external_input_cli import publish_external_input_bundle

    destinations = _destinations(tmp_path)
    first = publish_external_input_bundle(
        _bundle(),
        **destinations,
        clock=lambda: PUBLISHED_AT,
    )
    first_bytes = {name: path.read_bytes() for name, path in destinations.items()}

    second = publish_external_input_bundle(
        _bundle(),
        **destinations,
        clock=lambda: PUBLISHED_AT,
    )

    assert {name: path.read_bytes() for name, path in destinations.items()} == first_bytes
    assert {
        role: (row["sha256"], row["bytes"])
        for role, row in second["outputs"].items()
    } == {
        role: (row["sha256"], row["bytes"])
        for role, row in first["outputs"].items()
    }
    assert ExternalReadonlyFeedReader(
        destinations["readonly_feed_path"],
        clock=lambda: PUBLISHED_AT,
    ).read().batch_id == _bundle()["readonly_feed"]["batch_id"]
    assert len(
        ExternalTop10StructureSource(
            destinations["top10_path"],
            clock=lambda: PUBLISHED_AT,
        ).resolve_top10(scheduled_for=SCHEDULED_FOR)
    ) == 10
    assert not tuple(tmp_path.glob(".*.prepared"))
    assert not tuple(tmp_path.glob(".*.tmp"))


@pytest.mark.parametrize(
    "scheduled_for",
    (
        SCHEDULED_FOR.replace(second=1),
        SCHEDULED_FOR.replace(microsecond=1),
        SCHEDULED_FOR.replace(hour=9, minute=19),
        SCHEDULED_FOR + timedelta(days=1),
        OPEN_SCHEDULED_FOR,
    ),
    ids=("second", "microsecond", "early", "wrong-day", "purpose-mismatch"),
)
def test_bundle_scheduled_for_must_match_exact_purpose_slot_and_feed_day(
    tmp_path: Path,
    scheduled_for: datetime,
) -> None:
    from options_copilot.external_input_cli import publish_external_input_bundle

    invalid = _bundle()
    invalid["scheduled_for"] = scheduled_for
    destinations = _destinations(tmp_path)

    with pytest.raises(ValueError):
        publish_external_input_bundle(
            invalid,
            **destinations,
            clock=lambda: PUBLISHED_AT,
        )

    assert all(not path.exists() for path in destinations.values())


def test_successful_cli_stdout_contains_no_absolute_paths(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from options_copilot.external_input_cli import main

    bundle_path = tmp_path / "bundle.json"
    bundle_path.write_text(
        json.dumps(_bundle(), default=str),
        encoding="utf-8",
    )
    destinations = _destinations(tmp_path)

    result = main(
        [
            "--bundle",
            str(bundle_path),
            "--readonly-feed-out",
            str(destinations["readonly_feed_path"]),
            "--top10-out",
            str(destinations["top10_path"]),
            "--session-calendar-out",
            str(destinations["session_calendar_path"]),
        ],
        clock=lambda: PUBLISHED_AT,
    )

    captured = capsys.readouterr()
    assert result == 0
    assert captured.err == ""
    assert str(tmp_path) not in captured.out
    payload = json.loads(captured.out)
    assert payload["status"] == "PUBLISHED"
    assert set(payload["outputs"]) == {
        "readonly_feed",
        "top10",
        "session_calendar",
    }


def test_cli_rejects_incomplete_bundle_without_creating_outputs(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from options_copilot.external_input_cli import main

    bundle = _bundle()
    bundle.pop("session_calendar")
    bundle_path = tmp_path / "bundle.json"
    bundle_path.write_text(json.dumps(bundle, default=str), encoding="utf-8")
    destinations = _destinations(tmp_path)

    result = main(
        [
            "--bundle",
            str(bundle_path),
            "--readonly-feed-out",
            str(destinations["readonly_feed_path"]),
            "--top10-out",
            str(destinations["top10_path"]),
            "--session-calendar-out",
            str(destinations["session_calendar_path"]),
        ]
    )

    assert result == 2
    assert capsys.readouterr().err == (
        "NO_TRADE: EXTERNAL_INPUT_BUNDLE_INVALID\n"
    )
    assert all(not path.exists() for path in destinations.values())
