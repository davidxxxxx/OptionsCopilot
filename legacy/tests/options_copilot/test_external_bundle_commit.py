from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest

import options_copilot.external_bundle_commit as commit_module
from options_copilot.external_bundle_commit import (
    BundleFileLock,
    BundleLockUnavailable,
    ExternalBundleCommitWriter,
    ExternalBundleCommitGuard,
    ExternalBundleNotReady,
    ExternalBundlePaths,
)
from options_copilot.gateway.external_readonly_feed import (
    OPEN_REPRICE_PURPOSE,
    ExternalReadonlyFeedReader,
)
from options_copilot.external_input_cli import publish_external_input_bundle
from options_copilot.market.external_session_calendar import (
    ExternalSessionCalendarProvider,
)
from options_copilot.news.external_top10_source import (
    ExternalTop10StructureSource,
)
from tests.options_copilot.test_external_input_cli import (
    OPEN_COMPLETED_AT,
    OPEN_PUBLISHED_AT,
    OPEN_SCHEDULED_FOR,
    PUBLISHED_AT,
    SCHEDULED_FOR,
    _bundle,
    _destinations,
    _open_bundle,
)


def _paths(destinations: dict[str, Path]) -> ExternalBundlePaths:
    return ExternalBundlePaths(
        readonly_feed=destinations["readonly_feed_path"],
        top10=destinations["top10_path"],
        session_calendar=destinations["session_calendar_path"],
    )


def _publish(destinations: dict[str, Path], bundle: dict[str, object]) -> None:
    publish_external_input_bundle(
        bundle,
        **destinations,
        clock=lambda: PUBLISHED_AT,
    )


def test_second_replace_failure_leaves_old_manifest_and_guard_rejects_mixed_files(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    destinations = _destinations(tmp_path)
    original = _bundle()
    _publish(destinations, original)
    paths = _paths(destinations)
    guard = ExternalBundleCommitGuard(paths)
    old_manifest = paths.manifest_path.read_bytes()
    guard.verify(scheduled_for=SCHEDULED_FOR, now=PUBLISHED_AT)

    changed = _bundle()
    changed["readonly_feed"]["batch_id"] = "replacement-generation"
    changed["top10"]["batch_id"] = "replacement-generation"
    real_replace = commit_module._replace_committed_file
    calls = 0

    def fail_second(source: Path, destination: Path) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("simulated second replace failure")
        real_replace(source, destination)

    monkeypatch.setattr(commit_module, "_replace_committed_file", fail_second)
    with pytest.raises(OSError, match="second replace"):
        _publish(destinations, changed)

    assert paths.manifest_path.read_bytes() == old_manifest
    with pytest.raises(ExternalBundleNotReady, match="hash mismatch"):
        guard.verify(scheduled_for=SCHEDULED_FOR, now=PUBLISHED_AT)


def test_success_after_partial_replace_recovers_one_committed_generation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    destinations = _destinations(tmp_path)
    _publish(destinations, _bundle())
    changed = _bundle()
    changed["readonly_feed"]["batch_id"] = "recovered-generation"
    changed["top10"]["batch_id"] = "recovered-generation"
    real_replace = commit_module._replace_committed_file
    calls = 0

    def fail_second_once(source: Path, destination: Path) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("simulated second replace failure")
        real_replace(source, destination)

    with monkeypatch.context() as failure_patch:
        failure_patch.setattr(
            commit_module,
            "_replace_committed_file",
            fail_second_once,
        )
        with pytest.raises(OSError):
            _publish(destinations, changed)

    result = publish_external_input_bundle(
        changed,
        **destinations,
        clock=lambda: PUBLISHED_AT,
    )
    manifest = ExternalBundleCommitGuard(_paths(destinations)).verify(
        scheduled_for=SCHEDULED_FOR,
        now=PUBLISHED_AT,
    )

    assert manifest.generation_id == result["generation_id"]
    assert manifest.content_hash == result["content_hash"]
    assert manifest.file_hashes["readonly_feed"] == (
        result["outputs"]["readonly_feed"]["sha256"]
    )


def test_open_reprice_rejects_uncommitted_top10_and_preserves_prior_manifest(
    tmp_path: Path,
) -> None:
    destinations = _destinations(tmp_path)
    _publish(destinations, _bundle())
    paths = _paths(destinations)
    prior_manifest = paths.manifest_path.read_bytes()
    prior_readonly = Path(paths.readonly_feed).read_bytes()
    prior_calendar = Path(paths.session_calendar).read_bytes()

    Path(paths.top10).write_bytes(Path(paths.top10).read_bytes() + b"\n")

    with pytest.raises(ValueError, match="does not match the prior commit"):
        publish_external_input_bundle(
            _open_bundle(),
            **destinations,
            clock=lambda: OPEN_PUBLISHED_AT,
        )

    assert paths.manifest_path.read_bytes() == prior_manifest
    assert Path(paths.readonly_feed).read_bytes() == prior_readonly
    assert Path(paths.session_calendar).read_bytes() == prior_calendar
    with pytest.raises(ExternalBundleNotReady, match="top10 hash mismatch"):
        ExternalBundleCommitGuard(paths).verify(
            scheduled_for=SCHEDULED_FOR,
            now=PUBLISHED_AT,
        )
    with pytest.raises(ExternalBundleNotReady, match="scheduled_for"):
        ExternalBundleCommitGuard(paths).verify(
            scheduled_for=OPEN_SCHEDULED_FOR,
            now=OPEN_PUBLISHED_AT,
        )
    assert not tuple(tmp_path.glob(".*.prepared"))


def test_open_reprice_requires_top10_commit_from_same_trading_day(
    tmp_path: Path,
) -> None:
    destinations = _destinations(tmp_path)
    _publish(destinations, _bundle())
    paths = _paths(destinations)
    prior_manifest = paths.manifest_path.read_bytes()
    next_day = timedelta(days=1)

    def prepare_must_not_run(*_args: object) -> None:
        raise AssertionError("prepare must not run without same-day Top-10")

    with pytest.raises(ValueError, match="another trading day"):
        ExternalBundleCommitWriter(
            paths,
            clock=lambda: OPEN_PUBLISHED_AT + next_day,
        ).commit(
            purpose=OPEN_REPRICE_PURPOSE,
            scheduled_for=OPEN_SCHEDULED_FOR + next_day,
            feed_completed_at=OPEN_COMPLETED_AT + next_day,
            prepare=prepare_must_not_run,
        )

    assert paths.manifest_path.read_bytes() == prior_manifest
    assert not tuple(tmp_path.glob(".*.prepared"))


def test_open_reprice_rejects_top10_change_during_prepare(
    tmp_path: Path,
) -> None:
    destinations = _destinations(tmp_path)
    _publish(destinations, _bundle())
    paths = _paths(destinations)
    prior_manifest = paths.manifest_path.read_bytes()
    prior_readonly = Path(paths.readonly_feed).read_bytes()
    prior_calendar = Path(paths.session_calendar).read_bytes()

    def tampering_prepare(prepared: dict[str, Path], _now: object) -> None:
        prepared["readonly_feed"].write_bytes(b"prepared readonly feed\n")
        prepared["session_calendar"].write_bytes(b"prepared calendar\n")
        Path(paths.top10).write_bytes(Path(paths.top10).read_bytes() + b"\n")

    with pytest.raises(ValueError, match="changed during preparation"):
        ExternalBundleCommitWriter(
            paths,
            clock=lambda: OPEN_PUBLISHED_AT,
        ).commit(
            purpose=OPEN_REPRICE_PURPOSE,
            scheduled_for=OPEN_SCHEDULED_FOR,
            feed_completed_at=OPEN_COMPLETED_AT,
            prepare=tampering_prepare,
        )

    assert paths.manifest_path.read_bytes() == prior_manifest
    assert Path(paths.readonly_feed).read_bytes() == prior_readonly
    assert Path(paths.session_calendar).read_bytes() == prior_calendar
    with pytest.raises(ExternalBundleNotReady, match="top10 hash mismatch"):
        ExternalBundleCommitGuard(paths).verify(
            scheduled_for=SCHEDULED_FOR,
            now=PUBLISHED_AT,
        )
    assert not tuple(tmp_path.glob(".*.prepared"))


def test_nonblocking_guard_cannot_cross_concurrent_writer_lock(
    tmp_path: Path,
) -> None:
    destinations = _destinations(tmp_path)
    _publish(destinations, _bundle())
    paths = _paths(destinations)
    guard = ExternalBundleCommitGuard(paths)

    with BundleFileLock(paths.lock_path, timeout_seconds=1):
        with pytest.raises(BundleLockUnavailable):
            guard.verify(
                scheduled_for=SCHEDULED_FOR,
                now=PUBLISHED_AT,
                lock_timeout_seconds=0,
            )

    assert guard.verify(
        scheduled_for=SCHEDULED_FOR,
        now=PUBLISHED_AT,
    ).purpose == "PREMARKET_ACCOUNT"

    with pytest.raises(ValueError, match="must be nonblocking"):
        guard.verify(
            scheduled_for=SCHEDULED_FOR,
            now=PUBLISHED_AT,
            lock_timeout_seconds=0.01,
        )


def test_guard_consumes_frozen_bytes_when_original_paths_change(
    tmp_path: Path,
) -> None:
    destinations = _destinations(tmp_path)
    _publish(destinations, _bundle())
    paths = _paths(destinations)
    guard = ExternalBundleCommitGuard(paths)
    frozen_root: Path | None = None

    with guard.consume(
        scheduled_for=SCHEDULED_FOR,
        now=PUBLISHED_AT,
    ) as snapshot:
        frozen_root = snapshot.paths.directory
        assert frozen_root != paths.directory
        for path in paths.as_mapping().values():
            path.write_bytes(b"bypass-lock mutation\n")

        assert ExternalReadonlyFeedReader(
            snapshot.paths.readonly_feed,
            clock=lambda: PUBLISHED_AT,
        ).read().purpose == "PREMARKET_ACCOUNT"
        assert len(
            ExternalTop10StructureSource(
                snapshot.paths.top10,
                clock=lambda: PUBLISHED_AT,
            ).resolve_top10(scheduled_for=SCHEDULED_FOR)
        ) == 10
        assert ExternalSessionCalendarProvider(
            snapshot.paths.session_calendar,
            clock=lambda: PUBLISHED_AT,
        ).snapshot(now=PUBLISHED_AT).verify_hash()

    assert frozen_root is not None and not frozen_root.exists()
    with pytest.raises(ExternalBundleNotReady, match="hash mismatch"):
        guard.verify(scheduled_for=SCHEDULED_FOR, now=PUBLISHED_AT)


def test_paths_must_be_three_distinct_files_in_one_directory(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="same directory"):
        ExternalBundlePaths(
            readonly_feed=tmp_path / "a" / "feed.json",
            top10=tmp_path / "b" / "top10.json",
            session_calendar=tmp_path / "a" / "calendar.json",
        )
    with pytest.raises(ValueError, match="distinct"):
        ExternalBundlePaths(
            readonly_feed=tmp_path / "feed.json",
            top10=tmp_path / "feed.json",
            session_calendar=tmp_path / "calendar.json",
        )
    with pytest.raises(ValueError, match="distinct"):
        ExternalBundlePaths(
            readonly_feed=tmp_path / "nested" / ".." / "feed.json",
            top10=tmp_path / "feed.json",
            session_calendar=tmp_path / "calendar.json",
        )


def test_paths_reject_file_symlink_even_when_link_is_in_bundle_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    link = tmp_path / "feed-link.json"
    real_is_symlink = Path.is_symlink
    monkeypatch.setattr(
        Path,
        "is_symlink",
        lambda path: path == link or real_is_symlink(path),
    )

    with pytest.raises(ValueError, match="symbolic links"):
        ExternalBundlePaths(
            readonly_feed=link,
            top10=tmp_path / "top10.json",
            session_calendar=tmp_path / "calendar.json",
        )
