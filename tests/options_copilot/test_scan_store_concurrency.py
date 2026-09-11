"""Deterministic shared-connection locking tests for the scan run store."""
from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sqlite3
import threading

import pytest

from options_copilot.market.session_calendar import US_OPTIONS_TIMEZONE
from options_copilot.scanner.scheduler import ScanRunStore, ScanSlot


NOW = datetime(2026, 9, 9, 14, 0, tzinfo=timezone.utc)


class _ObservedRLock:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.attempted = threading.Event()

    def acquire(self) -> bool:
        self.attempted.set()
        return self._lock.acquire()

    def release(self) -> None:
        self._lock.release()

    def __enter__(self) -> "_ObservedRLock":
        self.acquire()
        return self

    def __exit__(self, *_args: object) -> None:
        self.release()


def _store(tmp_path: Path) -> tuple[ScanRunStore, ScanSlot, str]:
    store = ScanRunStore(tmp_path / "scan-runs.sqlite3")
    slot_at = NOW.astimezone(US_OPTIONS_TIMEZONE).replace(second=0, microsecond=0)
    slot = ScanSlot(slot_at.date(), slot_at)
    acquired = store.acquire(
        slot,
        pipeline_version="concurrency-v1",
        owner="test-owner",
        now=NOW,
    )
    return store, slot, acquired.run.scan_run_id


def _read_call(
    store: ScanRunStore,
    slot: ScanSlot,
    scan_run_id: str,
    name: str,
) -> Callable[[], object]:
    calls: dict[str, Callable[[], object]] = {
        "get": lambda: store.get(scan_run_id),
        "operational_timing": lambda: store.operational_timing(scan_run_id),
        "daily_result": lambda: store.daily_result(scan_run_id),
        "latest_daily_result": lambda: store.latest_daily_result("RESEARCH_REFRESH"),
        "producer_result": lambda: store.producer_result(scan_run_id),
        "latest_producer_result": lambda: store.latest_producer_result(
            trading_date=slot.trading_date,
            pipeline_version="concurrency-v1",
        ),
        "runs_for_slot": lambda: store.runs_for_slot(
            slot,
            pipeline_version="concurrency-v1",
        ),
        "latest_daily_manifest": lambda: store.latest_daily_manifest(
            trading_date=slot.trading_date,
        ),
        "latest_completed_daily_manifest": store.latest_completed_daily_manifest,
        "assert_daily_integrity": lambda: store.assert_daily_integrity(
            trading_date=slot.trading_date,
        ),
    }
    return calls[name]


@pytest.mark.parametrize(
    "read_name",
    (
        "get",
        "operational_timing",
        "daily_result",
        "latest_daily_result",
        "producer_result",
        "latest_producer_result",
        "runs_for_slot",
        "latest_daily_manifest",
        "latest_completed_daily_manifest",
        "assert_daily_integrity",
    ),
)
def test_public_shared_connection_reads_wait_for_active_transaction(
    tmp_path: Path,
    read_name: str,
) -> None:
    store, slot, scan_run_id = _store(tmp_path)
    observed_lock = _ObservedRLock()
    store._lock = observed_lock  # type: ignore[assignment]
    transaction_entered = threading.Event()
    release_transaction = threading.Event()
    read_finished = threading.Event()
    failures: list[BaseException] = []

    def hold_transaction() -> None:
        try:
            with store._transaction():
                transaction_entered.set()
                if not release_transaction.wait(5):
                    raise AssertionError("transaction release was not signaled")
        except BaseException as exc:
            failures.append(exc)

    def read() -> None:
        try:
            _read_call(store, slot, scan_run_id, read_name)()
        except BaseException as exc:
            failures.append(exc)
        finally:
            read_finished.set()

    holder = threading.Thread(target=hold_transaction)
    reader = threading.Thread(target=read)
    holder.start()
    assert transaction_entered.wait(5)
    observed_lock.attempted.clear()
    reader.start()
    try:
        assert observed_lock.attempted.wait(1), read_name
        assert not read_finished.is_set(), read_name
    finally:
        release_transaction.set()
        holder.join(5)
        reader.join(5)
        store.close()
    assert not holder.is_alive()
    assert not reader.is_alive()
    assert failures == []


def test_failed_nested_transaction_entry_releases_its_lock_level(
    tmp_path: Path,
) -> None:
    store, _slot, _scan_run_id = _store(tmp_path)
    acquired_after_failure = threading.Event()

    with store._transaction():
        with pytest.raises(sqlite3.OperationalError):
            with store._transaction():
                raise AssertionError("nested BEGIN unexpectedly succeeded")

    def acquire_after_failure() -> None:
        if store._lock.acquire(timeout=1):
            acquired_after_failure.set()
            store._lock.release()

    worker = threading.Thread(target=acquire_after_failure)
    worker.start()
    worker.join(2)
    try:
        assert not worker.is_alive()
        assert acquired_after_failure.is_set()
    finally:
        store.close()


def test_failed_transaction_exit_releases_lock(tmp_path: Path) -> None:
    store, _slot, _scan_run_id = _store(tmp_path)
    acquired_after_failure = threading.Event()

    with pytest.raises(sqlite3.ProgrammingError):
        with store._transaction():
            store._connection.close()

    def acquire_after_failure() -> None:
        if store._lock.acquire(timeout=1):
            acquired_after_failure.set()
            store._lock.release()

    worker = threading.Thread(target=acquire_after_failure)
    worker.start()
    worker.join(2)
    assert not worker.is_alive()
    assert acquired_after_failure.is_set()


@pytest.mark.parametrize("repetition", range(16))
def test_eight_readers_materialize_sixteen_valid_runs_without_partial_rows(
    tmp_path: Path,
    repetition: int,
) -> None:
    store = ScanRunStore(tmp_path / "shared-read-runs.sqlite3")
    pipeline_version = f"shared-read-v1-{repetition}"
    slots: list[ScanSlot] = []
    run_ids: list[str] = []
    for offset in range(16):
        slot_at = (
            NOW.astimezone(US_OPTIONS_TIMEZONE) + timedelta(minutes=offset)
        ).replace(second=0, microsecond=0)
        slot = ScanSlot(slot_at.date(), slot_at)
        acquired = store.acquire(
            slot,
            pipeline_version=pipeline_version,
            owner=f"owner-{offset}",
            now=NOW,
        )
        slots.append(slot)
        run_ids.append(acquired.run.scan_run_id)

    start = threading.Barrier(9)
    failures: list[BaseException] = []

    def read_rows(reader_index: int) -> None:
        try:
            start.wait()
            for iteration in range(200):
                index = (reader_index + iteration) % len(run_ids)
                run = store.get(run_ids[index])
                if (
                    run.scan_run_id != run_ids[index]
                    or run.slot_at != slots[index].slot_at
                    or run.pipeline_version != pipeline_version
                ):
                    raise AssertionError("shared read returned a partial row")
        except BaseException as exc:
            failures.append(exc)

    workers = [
        threading.Thread(target=read_rows, args=(index,))
        for index in range(8)
    ]
    for worker in workers:
        worker.start()
    start.wait()
    for worker in workers:
        worker.join(10)
    try:
        assert all(not worker.is_alive() for worker in workers)
        assert failures == []
    finally:
        store.close()
