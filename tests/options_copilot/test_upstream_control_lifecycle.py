"""Control freshness follows broker response batches, not cache read time."""

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from options_copilot.production_runtime import ProductionLifecycle


NOW = datetime(2026, 9, 8, 21, 5, tzinfo=timezone.utc)


class Gateway:
    connected = True

    def __init__(self):
        self.upstream = {
            "status": "READY", "generation": 1,
            "verified_at": NOW.isoformat(), "reason_codes": [],
        }
        self.after_orders = None
        self.reads = 0

    def upstream_health(self):
        return dict(self.upstream)

    def account_snapshot(self):
        self.reads += 1
        return SimpleNamespace(asof=NOW, net_liquidation="2000")

    def positions(self):
        return (SimpleNamespace(asof=NOW, contract_id=1, quantity="1"),)

    def working_orders(self):
        if self.after_orders:
            self.after_orders()
        return ()

    def unsubmitted_instructions(self):
        return ()


def lifecycle(gateway, now=NOW):
    result = ProductionLifecycle(
        gateway=gateway,
        scanner_loop=SimpleNamespace(health=lambda: {"status": "READY"}),
        stores=(),
        pacing_guard=SimpleNamespace(ready=True, reasons=lambda: ()),
        clock=lambda: now,
    )
    result._api_connected = True
    result._started = True
    result._supervisor_started = True
    return result


def test_repeated_cache_reads_do_not_advance_control_time():
    gateway = Gateway()
    runtime = lifecycle(gateway, NOW + timedelta(seconds=4))
    runtime._refresh_control_snapshot()
    assert runtime.control_snapshot()["observed_at"] == NOW.isoformat()
    assert runtime.control_snapshot()["age_ms"] == 4000


@pytest.mark.parametrize("state", ["LOST", "RECOVERY_PENDING", "RECONNECT_REQUIRED"])
def test_upstream_loss_immediately_invalidates_health_without_control_poll(state):
    gateway = Gateway()
    runtime = lifecycle(gateway)
    runtime._refresh_control_snapshot()
    assert runtime.control_snapshot()["status"] == "CURRENT"
    gateway.upstream.update(status=state, generation=2, reason_codes=["IBKR_UPSTREAM_UNVERIFIED"])
    before = gateway.reads
    snapshot = runtime.control_snapshot()
    assert snapshot["status"] == "STALE"
    assert snapshot["observed_at"] == NOW.isoformat()
    assert len(snapshot["positions"]) == 1
    assert snapshot["decision_authority"] == "LAST_KNOWN_ONLY"
    assert runtime.health()["broker_upstream"]["status"] != "UP"
    assert runtime.health()["broker_upstream"]["authority_state"] == state
    assert gateway.reads == before


def test_recovered_gateway_does_not_revalidate_previous_control_batch():
    gateway = Gateway()
    runtime = lifecycle(gateway)
    runtime._refresh_control_snapshot()
    gateway.upstream.update(generation=2)
    assert runtime.control_snapshot()["status"] == "STALE"


def test_epoch_changed_during_control_read_cannot_publish():
    gateway = Gateway()
    runtime = lifecycle(gateway)
    gateway.after_orders = lambda: gateway.upstream.update(status="LOST", generation=2)
    runtime._refresh_control_snapshot()
    snapshot = runtime.control_snapshot()
    assert snapshot["observed_at"] is None
    assert snapshot["status"] == "UNAVAILABLE"
    assert runtime._last_control_success_at is None


def test_different_response_batch_during_component_reads_cannot_publish():
    gateway = Gateway()
    runtime = lifecycle(gateway)
    gateway.after_orders = lambda: gateway.upstream.update(verified_at=(NOW + timedelta(seconds=1)).isoformat())
    runtime._refresh_control_snapshot()
    assert runtime.control_snapshot()["observed_at"] is None


def test_untrusted_or_missing_upstream_response_time_is_not_current():
    gateway = Gateway()
    gateway.upstream["verified_at"] = None
    runtime = lifecycle(gateway)
    runtime._refresh_control_snapshot()
    assert runtime.control_snapshot()["status"] == "UNAVAILABLE"


def test_unavailable_upstream_health_provider_fails_closed():
    gateway = Gateway()
    runtime = lifecycle(gateway)
    runtime._refresh_control_snapshot()

    def unavailable():
        raise ValueError("private information must not be exposed")

    gateway.upstream_health = unavailable
    assert runtime.control_snapshot()["status"] == "STALE"
    assert "private information" not in str(runtime.health())


def test_api_exposes_only_sanitized_upstream_evidence():
    from options_copilot.api.app import _normalise_health

    result = _normalise_health({"dependencies": {"production_scanner": {
        "status": "DEGRADED", "connected": True,
        "broker_upstream": {
            "status": "DEGRADED", "authority_state": "LOST", "generation": 2,
            "verified_at": NOW.isoformat(), "account": "private",
            "reason_codes": ["IBKR_UPSTREAM_LOST", "private error text"],
        },
    }}})
    state = result["dependencies"]["production_scanner"]["broker_upstream"]
    assert state["authority_state"] == "LOST"
    assert state["reason_codes"] == ["IBKR_UPSTREAM_LOST"]
    assert "private" not in str(result)


def test_health_uses_one_consistent_upstream_observation():
    gateway = Gateway()
    runtime = lifecycle(gateway)
    runtime._refresh_control_snapshot()
    calls = []

    def changing():
        calls.append(None)
        if len(calls) > 1:
            gateway.upstream.update(status="LOST", generation=2)
        return dict(gateway.upstream)

    gateway.upstream_health = changing
    health = runtime.health()
    assert len(calls) == 1
    assert (health["control_snapshot"]["status"] == "CURRENT") == (
        health["broker_upstream"]["authority_state"] == "READY"
    )
