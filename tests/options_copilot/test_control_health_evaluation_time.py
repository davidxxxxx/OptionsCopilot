"""Control freshness is evaluated after diagnostic work and lock acquisition."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import timedelta
from types import SimpleNamespace

import pytest

from test_upstream_control_lifecycle import Gateway, NOW, lifecycle


def _runtime():
    gateway = Gateway()
    clock = [NOW]
    runtime = lifecycle(gateway)
    runtime.clock = lambda: clock[0]
    runtime._refresh_control_snapshot()
    return runtime, gateway, clock


def _scanner_projection(runtime, callback):
    runtime.scanner_loop = SimpleNamespace(health=callback, summary=callback)


@pytest.mark.parametrize("method", ["health", "summary"])
def test_control_expires_while_scanner_projection_is_built(method):
    runtime, gateway, clock = _runtime()

    def delayed_scanner():
        clock[0] = NOW + timedelta(seconds=16)
        return {"status": "READY"}

    _scanner_projection(runtime, delayed_scanner)
    reads = gateway.reads
    result = getattr(runtime, method)()

    assert result["control_snapshot"]["status"] == "STALE"
    assert result["control_snapshot"]["observed_at"] == NOW.isoformat()
    assert result["control_snapshot"]["age_ms"] == 16000
    assert result["broker_upstream"]["status"] == "DEGRADED"
    assert gateway.reads == reads


@pytest.mark.parametrize("method", ["health", "summary"])
def test_new_control_batch_during_scanner_projection_is_not_future_dated(method):
    runtime, gateway, clock = _runtime()
    published_at = NOW + timedelta(seconds=4)

    def publish_during_scanner():
        clock[0] = published_at
        gateway.upstream["verified_at"] = published_at.isoformat()
        gateway.account_snapshot = lambda: SimpleNamespace(
            asof=published_at, net_liquidation="2000",
        )
        gateway.positions = lambda: (
            SimpleNamespace(asof=published_at, contract_id=1, quantity="1"),
        )
        runtime._refresh_control_snapshot()
        clock[0] = published_at + timedelta(seconds=2)
        return {"status": "READY"}

    _scanner_projection(runtime, publish_during_scanner)
    result = getattr(runtime, method)()

    assert result["control_snapshot"]["status"] == "CURRENT"
    assert result["control_snapshot"]["observed_at"] == published_at.isoformat()
    assert result["control_snapshot"]["age_ms"] == 2000
    assert result["broker_upstream"]["verified_at"] == published_at.isoformat()
    assert result["broker_upstream"]["status"] == "UP"


@pytest.mark.parametrize("method", ["health", "summary"])
@pytest.mark.parametrize("invalid", [False, True])
def test_clock_failure_during_scanner_work_still_fails_closed(method, invalid):
    runtime, gateway, clock = _runtime()

    def delayed_scanner():
        clock[0] = None if invalid else NOW - timedelta(seconds=1)
        return {"status": "READY"}

    _scanner_projection(runtime, delayed_scanner)
    reads = gateway.reads
    result = getattr(runtime, method)()

    assert result["control_snapshot"]["status"] == (
        "UNAVAILABLE" if invalid else "STALE"
    )
    assert result["broker_upstream"]["status"] == "DEGRADED"
    assert result["review_only"] is True
    assert result["direct_order_submission"] is False
    assert gateway.reads == reads


def test_control_snapshot_ages_after_lifecycle_lock_acquisition():
    runtime, gateway, clock = _runtime()
    original_lock = runtime._lock

    @contextmanager
    def delayed_lock():
        clock[0] = NOW + timedelta(seconds=16)
        with original_lock:
            yield

    runtime._lock = delayed_lock()
    reads = gateway.reads
    result = runtime.control_snapshot()

    assert result["status"] == "STALE"
    assert result["observed_at"] == NOW.isoformat()
    assert result["age_ms"] == 16000
    assert result["decision_authority"] == "LAST_KNOWN_ONLY"
    assert gateway.reads == reads


@pytest.mark.parametrize("method", ["health", "summary", "control_snapshot"])
def test_new_upstream_batch_without_matching_control_remains_stale(method):
    runtime, gateway, clock = _runtime()
    gateway.upstream["verified_at"] = (NOW + timedelta(seconds=1)).isoformat()
    clock[0] = NOW + timedelta(seconds=2)
    runtime.scanner_loop.summary = runtime.scanner_loop.health
    reads = gateway.reads
    result = getattr(runtime, method)()
    control = result if method == "control_snapshot" else result["control_snapshot"]

    assert control["status"] == "STALE"
    assert control["observed_at"] == NOW.isoformat()
    assert control["age_ms"] == 2000
    assert gateway.reads == reads


@pytest.mark.parametrize("method", ["health", "summary", "control_snapshot"])
def test_evaluation_clock_follows_the_single_upstream_observation(method):
    runtime, gateway, clock = _runtime()
    runtime.scanner_loop.summary = runtime.scanner_loop.health
    calls = []

    def delayed_upstream():
        calls.append("upstream")
        clock[0] = NOW + timedelta(seconds=16)
        return dict(gateway.upstream)

    def evaluation_clock():
        calls.append("clock")
        return clock[0]

    gateway.upstream_health = delayed_upstream
    runtime.clock = evaluation_clock
    reads = gateway.reads
    result = getattr(runtime, method)()
    control = result if method == "control_snapshot" else result["control_snapshot"]

    assert control["status"] == "STALE"
    assert control["observed_at"] == NOW.isoformat()
    assert control["age_ms"] == 16000
    assert calls == ["upstream", "clock"]
    assert gateway.reads == reads
