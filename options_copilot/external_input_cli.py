"""Fail-closed publisher for connector-collected external input bundles.

The command consumes an already collected JSON bundle and delegates semantic
validation and canonical document creation to the existing read-only feed,
Top-10, and broker-session-calendar publishers.  It has no broker client,
credential reader, instruction creator, approval path, or order API.

The purpose-selected documents are first published to same-directory prepared
paths.  A pre-market batch selects all three publishers; an open-reprice batch
selects only the read-only feed and session calendar and leaves the frozen
Top-10 document untouched.  Visible files are replaced only after every
selected publisher accepts its payload.
"""

from __future__ import annotations

import argparse
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timezone
from decimal import Decimal
import json
import os
from pathlib import Path
import sys

from options_copilot.external_bundle_commit import (
    ExternalBundleCommitWriter,
    ExternalBundlePaths,
    exact_external_slot,
)

from options_copilot.gateway.external_readonly_feed import (
    OPEN_REPRICE_PURPOSE,
    PREMARKET_ACCOUNT_PURPOSE,
    ExternalReadonlyFeedPublisher,
)
from options_copilot.market.external_session_calendar import (
    ExternalSessionCalendarPublisher,
)
from options_copilot.news.external_top10_source import ExternalTop10Publisher


EXTERNAL_INPUT_BUNDLE_SCHEMA = "options_copilot.external_input_bundle"
EXTERNAL_INPUT_BUNDLE_VERSION = 1
EXTERNAL_INPUT_PUBLISH_RESULT_SCHEMA = (
    "options_copilot.external_input_publish_result.v1"
)
MAXIMUM_BUNDLE_BYTES = 32 * 1024 * 1024

_BUNDLE_FIELDS = {
    "schema",
    "version",
    "scheduled_for",
    "readonly_feed",
    "top10",
    "session_calendar",
}


class ExternalInputBundleError(ValueError):
    """The connector bundle is missing, ambiguous, or structurally unsafe."""


def load_external_input_bundle(path: str | Path) -> Mapping[str, object]:
    """Load one bounded UTF-8 JSON object while rejecting duplicate keys."""

    source = Path(path)
    try:
        size = source.stat().st_size
        if size <= 0 or size > MAXIMUM_BUNDLE_BYTES:
            raise ExternalInputBundleError("external input bundle size is invalid")
        raw = source.read_bytes()
    except ExternalInputBundleError:
        raise
    except OSError as exc:
        raise ExternalInputBundleError("external input bundle is unavailable") from exc
    try:
        value = json.loads(
            raw.decode("utf-8"),
            parse_float=Decimal,
            object_pairs_hook=_unique_object,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ExternalInputBundleError("external input bundle JSON is invalid") from exc
    return _validated_bundle(value)


def publish_external_input_bundle(
    bundle: Mapping[str, object],
    *,
    readonly_feed_path: str | Path,
    top10_path: str | Path,
    session_calendar_path: str | Path,
    clock: Callable[[], datetime] | None = None,
) -> dict[str, object]:
    """Validate the purpose-selected payloads before replacing visible files."""

    checked = _validated_bundle(bundle)
    scheduled_for, expected_purpose = exact_external_slot(
        _timestamp(checked.get("scheduled_for"), "scheduled_for")
    )
    readonly_payload = _required_mapping(checked, "readonly_feed")
    purpose = readonly_payload.get("purpose")
    if purpose != expected_purpose:
        raise ExternalInputBundleError(
            "external readonly feed purpose does not match scheduled_for"
        )
    if purpose == PREMARKET_ACCOUNT_PURPOSE:
        top10_payload = _required_mapping(checked, "top10")
        _require_shared_source_batch(readonly_payload, top10_payload)
        top10_scheduled = _timestamp(
            top10_payload.get("scheduled_for"),
            "top10.scheduled_for",
        ).astimezone(timezone.utc)
        if top10_scheduled != scheduled_for:
            raise ExternalInputBundleError(
                "Top-10 scheduled_for does not match bundle"
            )
    elif purpose == OPEN_REPRICE_PURPOSE:
        if checked.get("top10") is not None:
            raise ExternalInputBundleError(
                "open reprice bundle top10 must be null"
            )
        top10_payload = None
    else:
        raise ExternalInputBundleError(
            "external readonly feed purpose is unsupported"
        )
    calendar_payload = _required_mapping(checked, "session_calendar")
    feed_completed_at = _timestamp(
        readonly_payload.get("completed_at"),
        "readonly_feed.completed_at",
    )
    paths = ExternalBundlePaths(
        readonly_feed=readonly_feed_path,
        top10=top10_path,
        session_calendar=session_calendar_path,
    )

    def prepare(prepared: Mapping[str, Path], published_at: datetime) -> None:
        fixed_clock = lambda: published_at
        ExternalReadonlyFeedPublisher(
            prepared["readonly_feed"],
            clock=fixed_clock,
        ).publish(readonly_payload)
        if top10_payload is not None:
            ExternalTop10Publisher(
                prepared["top10"],
                clock=fixed_clock,
            ).publish(top10_payload)
        ExternalSessionCalendarPublisher(
            prepared["session_calendar"],
            clock=fixed_clock,
        ).publish(calendar_payload)

    manifest = ExternalBundleCommitWriter(
        paths,
        clock=clock,
    ).commit(
        purpose=str(purpose),
        scheduled_for=scheduled_for,
        feed_completed_at=feed_completed_at,
        prepare=prepare,
    )
    return manifest.as_result()


def _validated_bundle(value: object) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or set(value) != _BUNDLE_FIELDS:
        raise ExternalInputBundleError("external input bundle fields are incomplete")
    if value["schema"] != EXTERNAL_INPUT_BUNDLE_SCHEMA:
        raise ExternalInputBundleError("external input bundle schema is unsupported")
    version = value["version"]
    if isinstance(version, bool) or version != EXTERNAL_INPUT_BUNDLE_VERSION:
        raise ExternalInputBundleError("external input bundle version is unsupported")
    for name in ("readonly_feed", "session_calendar"):
        _required_mapping(value, name)
    top10 = value.get("top10")
    if top10 is not None:
        _required_mapping(value, "top10")
    return value


def _required_mapping(
    value: Mapping[str, object],
    field: str,
) -> Mapping[str, object]:
    item = value.get(field)
    if not isinstance(item, Mapping) or not all(
        isinstance(key, str) for key in item
    ):
        raise ExternalInputBundleError(f"{field} must be a known object")
    return item


def _require_shared_source_batch(
    readonly_payload: Mapping[str, object],
    top10_payload: Mapping[str, object],
) -> str:
    """Bind the 09:20 account feed and Top-10 to one connector batch.

    Both existing publisher schemas already require ``batch_id``.  Requiring
    equality here prevents two individually valid payloads from different
    connector collections from being committed under one atomic manifest.
    The component publishers and manifest writer remain responsible for their
    existing format, content-hash, and byte-count checks.
    """

    readonly_batch_id = readonly_payload.get("batch_id")
    top10_batch_id = top10_payload.get("batch_id")
    if not isinstance(readonly_batch_id, str) or not readonly_batch_id.strip():
        raise ExternalInputBundleError(
            "readonly_feed batch_id is required for the shared source batch"
        )
    if not isinstance(top10_batch_id, str) or not top10_batch_id.strip():
        raise ExternalInputBundleError(
            "top10 batch_id is required for the shared source batch"
        )
    if readonly_batch_id != top10_batch_id:
        raise ExternalInputBundleError(
            "09:20 readonly_feed and Top-10 source batch_id values do not match"
        )
    return readonly_batch_id


def _aware_utc(value: datetime, field: str) -> datetime:
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise ExternalInputBundleError(f"{field} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _timestamp(value: object, field: str) -> datetime:
    if isinstance(value, datetime):
        return _aware_utc(value, field)
    if not isinstance(value, str):
        raise ExternalInputBundleError(f"{field} must be a timestamp")
    try:
        return _aware_utc(
            datetime.fromisoformat(value.replace("Z", "+00:00")),
            field,
        )
    except ValueError as exc:
        raise ExternalInputBundleError(f"{field} must be a timestamp") from exc


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ExternalInputBundleError(
                f"external input bundle repeats key {key!r}"
            )
        result[key] = value
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Publish one complete connector-collected read-only external input bundle"
        ),
    )
    parser.add_argument("--bundle", required=True, type=Path)
    parser.add_argument("--readonly-feed-out", required=True, type=Path)
    parser.add_argument("--top10-out", required=True, type=Path)
    parser.add_argument("--session-calendar-out", required=True, type=Path)
    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    clock: Callable[[], datetime] | None = None,
) -> int:
    args = _parser().parse_args(list(argv) if argv is not None else None)
    try:
        bundle_path = Path(args.bundle)
        outputs = (
            Path(args.readonly_feed_out),
            Path(args.top10_out),
            Path(args.session_calendar_out),
        )
        source = os.path.normcase(os.path.abspath(os.fspath(bundle_path)))
        if source in {
            os.path.normcase(os.path.abspath(os.fspath(path))) for path in outputs
        }:
            raise ExternalInputBundleError(
                "external input bundle cannot also be an output"
            )
        result = publish_external_input_bundle(
            load_external_input_bundle(bundle_path),
            readonly_feed_path=outputs[0],
            top10_path=outputs[1],
            session_calendar_path=outputs[2],
            clock=clock,
        )
    except Exception:
        # Connector and transport errors may contain account or credential
        # material.  The CLI intentionally emits only a stable fail-closed code.
        sys.stderr.write("NO_TRADE: EXTERNAL_INPUT_BUNDLE_INVALID\n")
        return 2
    sys.stdout.write(json.dumps(result, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":  # pragma: no cover - exercised through python -m
    raise SystemExit(main())


__all__ = [
    "EXTERNAL_INPUT_BUNDLE_SCHEMA",
    "EXTERNAL_INPUT_BUNDLE_VERSION",
    "EXTERNAL_INPUT_PUBLISH_RESULT_SCHEMA",
    "ExternalInputBundleError",
    "load_external_input_bundle",
    "main",
    "publish_external_input_bundle",
]
