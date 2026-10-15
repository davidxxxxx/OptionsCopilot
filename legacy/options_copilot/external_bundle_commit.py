"""Cross-process commit point for external Options Copilot input bundles.

The three configured data files are not a committed generation by themselves.
Only a hash-bound ``current-manifest.json`` written after every selected file
replacement makes a generation consumable.  Readers hold the same OS file lock
while validating the manifest and using the files, so a writer cannot replace
one component between verification and consumption.

This module is pure stdlib apart from canonical JSON helpers.  It owns no
broker, approval, instruction, creator, or order capability.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
import re
import tempfile
import time
from typing import BinaryIO
from uuid import uuid4
from zoneinfo import ZoneInfo

from options_copilot.gateway.external_readonly_feed import (
    OPEN_REPRICE_PURPOSE,
    PREMARKET_ACCOUNT_PURPOSE,
)
from options_copilot.storage.canonical import canonical_hash, canonical_json


EXTERNAL_BUNDLE_MANIFEST_SCHEMA = "options_copilot.external_bundle_manifest"
EXTERNAL_BUNDLE_MANIFEST_VERSION = 1
CURRENT_MANIFEST_FILENAME = "current-manifest.json"
BUNDLE_LOCK_FILENAME = ".external-input-bundle.lock"
FEED_FRESHNESS_SECONDS = 5
MAXIMUM_MANIFEST_BYTES = 2 * 1024 * 1024
MAXIMUM_COMPONENT_BYTES = 16 * 1024 * 1024

_NEW_YORK = ZoneInfo("America/New_York")
_ROLES = ("readonly_feed", "top10", "session_calendar")
_GENERATION_ID = re.compile(r"^[0-9a-f]{32}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_MANIFEST_FIELDS = {
    "schema",
    "version",
    "generation_id",
    "purpose",
    "scheduled_for",
    "committed_at",
    "feed_completed_at",
    "feed_expires_at",
    "files",
    "content_hash",
}
_FILE_FIELDS = {"sha256", "bytes", "status"}


class ExternalBundleError(ValueError):
    """The external bundle protocol contract is invalid."""


class ExternalBundleNotReady(ExternalBundleError):
    """No one complete, fresh, hash-matching generation is consumable."""


class BundleLockUnavailable(ExternalBundleNotReady):
    """Another process currently owns the external bundle commit boundary."""


@dataclass(frozen=True, slots=True)
class ExternalBundlePaths:
    readonly_feed: Path | str
    top10: Path | str
    session_calendar: Path | str

    def __post_init__(self) -> None:
        for name in _ROLES:
            candidate = Path(getattr(self, name)).absolute()
            if candidate.is_symlink():
                raise ExternalBundleError(
                    "external bundle data paths cannot be symbolic links"
                )
            object.__setattr__(self, name, candidate.resolve(strict=False))
        values = tuple(getattr(self, name) for name in _ROLES)
        parents = {_normalized_path(path.parent) for path in values}
        if len(parents) != 1:
            raise ExternalBundleError(
                "external bundle paths must share the same directory"
            )
        normalized = {_normalized_path(path) for path in values}
        if len(normalized) != len(values):
            raise ExternalBundleError("external bundle paths must be distinct")
        reserved = {
            _normalized_path(self.manifest_path),
            _normalized_path(self.lock_path),
        }
        if normalized.intersection(reserved):
            raise ExternalBundleError(
                "external bundle data paths cannot use reserved protocol names"
            )

    @property
    def directory(self) -> Path:
        return Path(self.readonly_feed).parent

    @property
    def manifest_path(self) -> Path:
        return self.directory / CURRENT_MANIFEST_FILENAME

    @property
    def lock_path(self) -> Path:
        return self.directory / BUNDLE_LOCK_FILENAME

    def as_mapping(self) -> dict[str, Path]:
        return {name: Path(getattr(self, name)) for name in _ROLES}


@dataclass(frozen=True, slots=True)
class ExternalBundleManifest:
    generation_id: str
    purpose: str
    scheduled_for: datetime
    committed_at: datetime
    feed_completed_at: datetime
    feed_expires_at: datetime
    files: Mapping[str, Mapping[str, object]]
    content_hash: str

    @property
    def file_hashes(self) -> dict[str, str]:
        return {name: str(self.files[name]["sha256"]) for name in _ROLES}

    def as_result(self) -> dict[str, object]:
        return {
            "schema": "options_copilot.external_input_publish_result.v1",
            "status": "PUBLISHED",
            "purpose": self.purpose,
            "scheduled_for": self.scheduled_for.isoformat(),
            "generation_id": self.generation_id,
            "content_hash": self.content_hash,
            "decision": "OBSERVATION_ONLY",
            "read_only": True,
            "decision_authority": "SUPPORTING_ONLY",
            "instruction_creation_allowed": False,
            "order_submission_allowed": False,
            "outputs": {
                name: {
                    "role": name,
                    "status": str(self.files[name]["status"]),
                    "sha256": str(self.files[name]["sha256"]),
                    "bytes": int(self.files[name]["bytes"]),
                }
                for name in _ROLES
            },
        }


@dataclass(frozen=True, slots=True)
class ExternalBundleSnapshot:
    """One hash-verified immutable-on-disk view used for this consumption."""

    manifest: ExternalBundleManifest
    paths: ExternalBundlePaths


class BundleFileLock:
    """Exclusive one-byte OS lock shared by writer and runtime consumer."""

    def __init__(
        self,
        path: Path | str,
        *,
        timeout_seconds: float = 5,
        poll_seconds: float = 0.01,
    ) -> None:
        if isinstance(timeout_seconds, bool) or not 0 <= timeout_seconds <= 30:
            raise ValueError("bundle lock timeout must be between zero and 30 seconds")
        if isinstance(poll_seconds, bool) or not 0 < poll_seconds <= 0.1:
            raise ValueError("bundle lock poll interval is invalid")
        self.path = Path(path)
        self.timeout_seconds = float(timeout_seconds)
        self.poll_seconds = float(poll_seconds)
        self._handle: BinaryIO | None = None

    def __enter__(self) -> "BundleFileLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.path.open("a+b")
        try:
            handle.seek(0, os.SEEK_END)
            if handle.tell() == 0:
                handle.write(b"\0")
                handle.flush()
                os.fsync(handle.fileno())
            deadline = time.monotonic() + self.timeout_seconds
            while True:
                try:
                    _try_lock(handle)
                    break
                except (OSError, BlockingIOError):
                    if time.monotonic() >= deadline:
                        raise BundleLockUnavailable(
                            "external bundle lock is currently unavailable"
                        )
                    time.sleep(
                        min(self.poll_seconds, max(0, deadline - time.monotonic()))
                    )
        except BaseException:
            handle.close()
            raise
        self._handle = handle
        return self

    def __exit__(self, *_exc: object) -> None:
        handle = self._handle
        self._handle = None
        if handle is None:
            return
        try:
            _unlock(handle)
        finally:
            handle.close()


class ExternalBundleCommitWriter:
    """Prepare selected files under lock and commit one manifest last."""

    def __init__(
        self,
        paths: ExternalBundlePaths,
        *,
        clock: Callable[[], datetime] | None = None,
        lock_timeout_seconds: float = 5,
    ) -> None:
        if not isinstance(paths, ExternalBundlePaths):
            raise TypeError("paths must be ExternalBundlePaths")
        self.paths = paths
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self.lock_timeout_seconds = lock_timeout_seconds

    def commit(
        self,
        *,
        purpose: str,
        scheduled_for: datetime,
        feed_completed_at: datetime,
        prepare: Callable[[Mapping[str, Path], datetime], None],
    ) -> ExternalBundleManifest:
        scheduled, expected_purpose = exact_external_slot(scheduled_for)
        if purpose != expected_purpose:
            raise ExternalBundleError(
                "external bundle purpose does not match scheduled_for"
            )
        completed = _aware_utc(feed_completed_at, "feed_completed_at")
        if not scheduled <= completed < scheduled + timedelta(minutes=1):
            raise ExternalBundleError(
                "external feed completion does not match scheduled_for"
            )
        if not callable(prepare):
            raise TypeError("prepare must be callable")
        publish_roles = (
            _ROLES
            if purpose == PREMARKET_ACCOUNT_PURPOSE
            else ("readonly_feed", "session_calendar")
        )
        generation_id = uuid4().hex
        prepared = {
            role: self.paths.directory
            / f".{Path(getattr(self.paths, role)).name}.{generation_id}.prepared"
            for role in publish_roles
        }
        self.paths.directory.mkdir(parents=True, exist_ok=True)

        try:
            with BundleFileLock(
                self.paths.lock_path,
                timeout_seconds=self.lock_timeout_seconds,
            ):
                prior_top10: Mapping[str, object] | None = None
                if purpose == OPEN_REPRICE_PURPOSE:
                    previous = _read_manifest(self.paths.manifest_path)
                    if (
                        previous.scheduled_for.astimezone(_NEW_YORK).date()
                        != scheduled.astimezone(_NEW_YORK).date()
                    ):
                        raise ExternalBundleError(
                            "open reprice Top-10 generation is from another trading day"
                        )
                    prior_top10 = previous.files["top10"]
                    current_top10 = _component_digest(Path(self.paths.top10))
                    if (
                        current_top10["sha256"] != prior_top10["sha256"]
                        or current_top10["bytes"] != prior_top10["bytes"]
                    ):
                        raise ExternalBundleError(
                            "open reprice Top-10 does not match the prior commit"
                        )
                prepare_at = _aware_utc(self._clock(), "publisher clock")
                prepare(prepared, prepare_at)
                for role, path in prepared.items():
                    _require_component(path, role)
                if purpose == OPEN_REPRICE_PURPOSE:
                    if prior_top10 is None:  # pragma: no cover - defensive invariant
                        raise ExternalBundleError(
                            "open reprice prior Top-10 commitment is unavailable"
                        )
                    current_top10 = _component_digest(Path(self.paths.top10))
                    if (
                        current_top10["sha256"] != prior_top10["sha256"]
                        or current_top10["bytes"] != prior_top10["bytes"]
                    ):
                        raise ExternalBundleError(
                            "open reprice Top-10 changed during preparation"
                        )

                committed_at = _aware_utc(self._clock(), "commit clock")
                expires_at = completed + timedelta(seconds=FEED_FRESHNESS_SECONDS)
                if not completed <= committed_at <= expires_at:
                    raise ExternalBundleError(
                        "external feed is not fresh at bundle commit"
                    )
                sources = {
                    role: (
                        prepared[role]
                        if role in prepared
                        else Path(getattr(self.paths, role))
                    )
                    for role in _ROLES
                }
                files: dict[str, dict[str, object]] = {}
                for role in _ROLES:
                    if role == "top10" and purpose == OPEN_REPRICE_PURPOSE:
                        if prior_top10 is None:  # pragma: no cover - invariant
                            raise ExternalBundleError(
                                "open reprice prior Top-10 commitment is unavailable"
                            )
                        digest = {
                            "sha256": prior_top10["sha256"],
                            "bytes": prior_top10["bytes"],
                        }
                        status = "UNCHANGED"
                    else:
                        digest = _component_digest(sources[role])
                        status = "PUBLISHED"
                    files[role] = {**digest, "status": status}
                document = _manifest_document(
                    generation_id=generation_id,
                    purpose=purpose,
                    scheduled_for=scheduled,
                    committed_at=committed_at,
                    feed_completed_at=completed,
                    feed_expires_at=expires_at,
                    files=files,
                )
                manifest = _parse_manifest(document)
                destinations = self.paths.as_mapping()
                for role in publish_roles:
                    _replace_committed_file(prepared[role], destinations[role])
                _atomic_write_manifest(self.paths.manifest_path, document)
                return manifest
        finally:
            for path in prepared.values():
                try:
                    path.unlink()
                except FileNotFoundError:
                    pass


class ExternalBundleCommitGuard:
    """Hold the commit lock while validating and consuming one generation."""

    def __init__(self, paths: ExternalBundlePaths) -> None:
        if not isinstance(paths, ExternalBundlePaths):
            raise TypeError("paths must be ExternalBundlePaths")
        self.paths = paths

    @contextmanager
    def consume(
        self,
        *,
        scheduled_for: datetime,
        now: datetime,
        lock_timeout_seconds: float = 0,
    ) -> Iterator[ExternalBundleSnapshot]:
        if isinstance(lock_timeout_seconds, bool) or lock_timeout_seconds != 0:
            raise ValueError("bundle guard consumption must be nonblocking")
        scheduled, expected_purpose = exact_external_slot(scheduled_for)
        checked_now = _aware_utc(now, "bundle guard clock")
        with BundleFileLock(
            self.paths.lock_path,
            timeout_seconds=lock_timeout_seconds,
        ):
            manifest = _read_manifest(self.paths.manifest_path)
            if manifest.scheduled_for != scheduled:
                raise ExternalBundleNotReady(
                    "external bundle scheduled_for does not match current slot"
                )
            if manifest.purpose != expected_purpose:
                raise ExternalBundleNotReady(
                    "external bundle purpose does not match current slot"
                )
            if not (
                manifest.feed_completed_at
                <= manifest.committed_at
                <= checked_now
                <= manifest.feed_expires_at
            ):
                raise ExternalBundleNotReady(
                    "external bundle manifest is not fresh"
                )
            if manifest.feed_expires_at != manifest.feed_completed_at + timedelta(
                seconds=FEED_FRESHNESS_SECONDS
            ):
                raise ExternalBundleNotReady(
                    "external bundle feed expiry is invalid"
                )
            if not (
                scheduled
                <= manifest.feed_completed_at
                < scheduled + timedelta(minutes=1)
            ):
                raise ExternalBundleNotReady(
                    "external bundle feed completion is outside the slot"
                )
            components: dict[str, bytes] = {}
            for role, path in self.paths.as_mapping().items():
                expected = manifest.files[role]
                try:
                    raw = _component_bytes(path)
                except ExternalBundleError as exc:
                    raise ExternalBundleNotReady(
                        f"external bundle {role} is unavailable"
                    ) from exc
                actual = _bytes_digest(raw)
                if (
                    actual["sha256"] != expected["sha256"]
                    or actual["bytes"] != expected["bytes"]
                ):
                    raise ExternalBundleNotReady(
                        f"external bundle {role} hash mismatch"
                    )
                components[role] = raw
            with tempfile.TemporaryDirectory(
                dir=self.paths.directory,
                prefix=f".external-bundle-snapshot.{manifest.generation_id}.",
            ) as snapshot_directory:
                root = Path(snapshot_directory)
                snapshot_paths = ExternalBundlePaths(
                    readonly_feed=root / "readonly-feed.json",
                    top10=root / "top10.json",
                    session_calendar=root / "session-calendar.json",
                )
                for role, path in snapshot_paths.as_mapping().items():
                    _write_snapshot_component(path, components[role])
                yield ExternalBundleSnapshot(manifest, snapshot_paths)

    def verify(
        self,
        *,
        scheduled_for: datetime,
        now: datetime,
        lock_timeout_seconds: float = 0,
    ) -> ExternalBundleManifest:
        with self.consume(
            scheduled_for=scheduled_for,
            now=now,
            lock_timeout_seconds=lock_timeout_seconds,
        ) as snapshot:
            return snapshot.manifest


def exact_external_slot(value: datetime) -> tuple[datetime, str]:
    checked = _aware_utc(value, "scheduled_for")
    local = checked.astimezone(_NEW_YORK)
    if local.second != 0 or local.microsecond != 0:
        raise ExternalBundleError(
            "scheduled_for must have zero seconds and microseconds"
        )
    key = (local.hour, local.minute)
    purpose = {
        (9, 20): PREMARKET_ACCOUNT_PURPOSE,
        (9, 35): OPEN_REPRICE_PURPOSE,
    }.get(key)
    if purpose is None:
        raise ExternalBundleError(
            "scheduled_for must be exactly 09:20 or 09:35 America/New_York"
        )
    return checked, purpose


def external_slot_for_instant(value: datetime) -> datetime | None:
    checked = _aware_utc(value, "scheduler clock")
    local = checked.astimezone(_NEW_YORK)
    if (local.hour, local.minute) not in {(9, 20), (9, 35)}:
        return None
    return local.replace(second=0, microsecond=0).astimezone(timezone.utc)


def _manifest_document(
    *,
    generation_id: str,
    purpose: str,
    scheduled_for: datetime,
    committed_at: datetime,
    feed_completed_at: datetime,
    feed_expires_at: datetime,
    files: Mapping[str, Mapping[str, object]],
) -> dict[str, object]:
    unsigned: dict[str, object] = {
        "schema": EXTERNAL_BUNDLE_MANIFEST_SCHEMA,
        "version": EXTERNAL_BUNDLE_MANIFEST_VERSION,
        "generation_id": generation_id,
        "purpose": purpose,
        "scheduled_for": scheduled_for.isoformat(timespec="microseconds"),
        "committed_at": committed_at.isoformat(timespec="microseconds"),
        "feed_completed_at": feed_completed_at.isoformat(timespec="microseconds"),
        "feed_expires_at": feed_expires_at.isoformat(timespec="microseconds"),
        "files": {name: dict(files[name]) for name in _ROLES},
    }
    return {**unsigned, "content_hash": canonical_hash(unsigned)}


def _parse_manifest(value: object) -> ExternalBundleManifest:
    if not isinstance(value, Mapping) or set(value) != _MANIFEST_FIELDS:
        raise ExternalBundleNotReady("external bundle manifest is incomplete")
    if (
        value["schema"] != EXTERNAL_BUNDLE_MANIFEST_SCHEMA
        or value["version"] != EXTERNAL_BUNDLE_MANIFEST_VERSION
    ):
        raise ExternalBundleNotReady("external bundle manifest schema is unsupported")
    generation_id = value["generation_id"]
    if (
        not isinstance(generation_id, str)
        or _GENERATION_ID.fullmatch(generation_id) is None
    ):
        raise ExternalBundleNotReady("external bundle generation id is invalid")
    unsigned = dict(value)
    content_hash = unsigned.pop("content_hash")
    if (
        not isinstance(content_hash, str)
        or _SHA256.fullmatch(content_hash) is None
        or canonical_hash(unsigned) != content_hash
    ):
        raise ExternalBundleNotReady("external bundle manifest hash is invalid")
    scheduled, expected_purpose = exact_external_slot(
        _timestamp(value["scheduled_for"], "scheduled_for")
    )
    purpose = value["purpose"]
    if purpose != expected_purpose:
        raise ExternalBundleNotReady("external bundle manifest purpose is invalid")
    files_raw = value["files"]
    if not isinstance(files_raw, Mapping) or set(files_raw) != set(_ROLES):
        raise ExternalBundleNotReady("external bundle manifest files are incomplete")
    files: dict[str, dict[str, object]] = {}
    for role in _ROLES:
        row = files_raw[role]
        if not isinstance(row, Mapping) or set(row) != _FILE_FIELDS:
            raise ExternalBundleNotReady(
                f"external bundle manifest {role} fields are incomplete"
            )
        digest = row["sha256"]
        size = row["bytes"]
        status = row["status"]
        expected_status = (
            "UNCHANGED"
            if role == "top10" and purpose == OPEN_REPRICE_PURPOSE
            else "PUBLISHED"
        )
        if not isinstance(digest, str) or _SHA256.fullmatch(digest) is None:
            raise ExternalBundleNotReady(
                f"external bundle manifest {role} hash is invalid"
            )
        if isinstance(size, bool) or not isinstance(size, int) or not (
            0 < size <= MAXIMUM_COMPONENT_BYTES
        ):
            raise ExternalBundleNotReady(
                f"external bundle manifest {role} size is invalid"
            )
        if status != expected_status:
            raise ExternalBundleNotReady(
                f"external bundle manifest {role} status is invalid"
            )
        files[role] = {"sha256": digest, "bytes": size, "status": status}
    committed = _timestamp(value["committed_at"], "committed_at")
    completed = _timestamp(value["feed_completed_at"], "feed_completed_at")
    expires = _timestamp(value["feed_expires_at"], "feed_expires_at")
    if not completed <= committed <= expires:
        raise ExternalBundleNotReady("external bundle manifest timestamps are invalid")
    return ExternalBundleManifest(
        generation_id=generation_id,
        purpose=str(purpose),
        scheduled_for=scheduled,
        committed_at=committed,
        feed_completed_at=completed,
        feed_expires_at=expires,
        files=files,
        content_hash=content_hash,
    )


def _read_manifest(path: Path) -> ExternalBundleManifest:
    try:
        size = path.stat().st_size
        if not 0 < size <= MAXIMUM_MANIFEST_BYTES:
            raise ExternalBundleNotReady("external bundle manifest size is invalid")
        raw = path.read_bytes()
    except ExternalBundleNotReady:
        raise
    except OSError as exc:
        raise ExternalBundleNotReady("external bundle manifest is unavailable") from exc
    try:
        document = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_unique_object,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ExternalBundleError) as exc:
        raise ExternalBundleNotReady("external bundle manifest JSON is invalid") from exc
    return _parse_manifest(document)


def _atomic_write_manifest(path: Path, document: Mapping[str, object]) -> None:
    rendered = (canonical_json(document) + "\n").encode("utf-8")
    if len(rendered) > MAXIMUM_MANIFEST_BYTES:
        raise ExternalBundleError("external bundle manifest is too large")
    descriptor = -1
    temporary: Path | None = None
    try:
        descriptor, raw_path = tempfile.mkstemp(
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
        )
        temporary = Path(raw_path)
        with os.fdopen(descriptor, "wb") as handle:
            descriptor = -1
            handle.write(rendered)
            handle.flush()
            os.fsync(handle.fileno())
        _replace_manifest_file(temporary, path)
        temporary = None
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if temporary is not None:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass


def _require_component(path: Path, role: str) -> None:
    try:
        size = path.stat().st_size
    except OSError as exc:
        raise ExternalBundleError(f"external bundle {role} is unavailable") from exc
    if not path.is_file() or not 0 < size <= MAXIMUM_COMPONENT_BYTES:
        raise ExternalBundleError(f"external bundle {role} size is invalid")


def _component_digest(path: Path) -> dict[str, object]:
    return _bytes_digest(_component_bytes(path))


def _component_bytes(path: Path) -> bytes:
    try:
        with path.open("rb") as handle:
            raw = handle.read(MAXIMUM_COMPONENT_BYTES + 1)
    except OSError as exc:
        raise ExternalBundleError("external bundle component is unreadable") from exc
    if not raw or len(raw) > MAXIMUM_COMPONENT_BYTES:
        raise ExternalBundleError("external bundle component size is invalid")
    return raw


def _bytes_digest(raw: bytes) -> dict[str, object]:
    return {"sha256": sha256(raw).hexdigest(), "bytes": len(raw)}


def _write_snapshot_component(path: Path, raw: bytes) -> None:
    try:
        with path.open("xb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
    except OSError as exc:
        raise ExternalBundleNotReady(
            "external bundle frozen snapshot is unavailable"
        ) from exc


def _replace_committed_file(source: Path, destination: Path) -> None:
    os.replace(source, destination)


def _replace_manifest_file(source: Path, destination: Path) -> None:
    os.replace(source, destination)


def _timestamp(value: object, field: str) -> datetime:
    if isinstance(value, datetime):
        return _aware_utc(value, field)
    if not isinstance(value, str):
        raise ExternalBundleNotReady(f"{field} must be a timestamp")
    try:
        return _aware_utc(datetime.fromisoformat(value.replace("Z", "+00:00")), field)
    except ValueError as exc:
        raise ExternalBundleNotReady(f"{field} must be a timestamp") from exc


def _aware_utc(value: datetime, field: str) -> datetime:
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise ExternalBundleError(f"{field} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _normalized_path(path: Path) -> str:
    return os.path.normcase(os.fspath(path.resolve(strict=False)))


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ExternalBundleError(f"external bundle repeats key {key!r}")
        result[key] = value
    return result


if os.name == "nt":
    import msvcrt

    def _try_lock(handle: BinaryIO) -> None:
        handle.seek(0)
        msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)

    def _unlock(handle: BinaryIO) -> None:
        handle.seek(0)
        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)

else:  # pragma: no cover - Windows is the production target
    import fcntl

    def _try_lock(handle: BinaryIO) -> None:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

    def _unlock(handle: BinaryIO) -> None:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


__all__ = [
    "BUNDLE_LOCK_FILENAME",
    "CURRENT_MANIFEST_FILENAME",
    "EXTERNAL_BUNDLE_MANIFEST_SCHEMA",
    "EXTERNAL_BUNDLE_MANIFEST_VERSION",
    "BundleFileLock",
    "BundleLockUnavailable",
    "ExternalBundleCommitGuard",
    "ExternalBundleCommitWriter",
    "ExternalBundleError",
    "ExternalBundleManifest",
    "ExternalBundleNotReady",
    "ExternalBundlePaths",
    "ExternalBundleSnapshot",
    "exact_external_slot",
    "external_slot_for_instant",
]
