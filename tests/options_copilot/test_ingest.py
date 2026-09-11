from __future__ import annotations

from pathlib import Path

import pytest

from options_copilot.ingest import ingest_payload
from options_copilot.state import ManagedSnapshotStore


def test_ingest_locks_ten_k_baseline_and_limits_candidates(tmp_path: Path) -> None:
    store = ManagedSnapshotStore(tmp_path / "runtime.json")
    snapshot = ingest_payload(
        {
            "observed_at": "2026-08-03T12:00:00+00:00",
            "source": "managed_ibkr_connector",
            "account": {"net_liquidation": 2012.44},
            "positions": [],
            "candidates": [],
            "warnings": ["market closed"],
            "broker_snapshot_complete": True,
            "working_order_count": 0,
            "unsubmitted_instruction_count": 0,
        },
        store=store,
    )
    assert snapshot.campaign["start_nlv_usd"] == 2012.44
    assert snapshot.campaign["target_nlv_usd"] == 10000.0
    assert snapshot.campaign["external_cash_flows_excluded"] is True
    restored = store.read()
    assert restored is not None
    assert restored.broker_snapshot_complete is True
    assert restored.working_order_count == 0
    assert restored.unsubmitted_instruction_count == 0


def test_ingest_rejects_secrets_and_more_than_three_candidates(tmp_path: Path) -> None:
    store = ManagedSnapshotStore(tmp_path / "runtime.json")
    base = {
        "observed_at": "2026-08-03T12:00:00+00:00",
        "account": {"net_liquidation": 2012.44},
        "positions": [],
        "warnings": [],
    }
    with pytest.raises(ValueError, match="secret-like"):
        ingest_payload({**base, "api_token": "forbidden", "candidates": []}, store=store)
    with pytest.raises(ValueError, match="at most three"):
        ingest_payload({**base, "candidates": [{"id": i} for i in range(4)]}, store=store)


def test_ingest_missing_or_malformed_broker_gates_fail_closed(tmp_path: Path) -> None:
    store = ManagedSnapshotStore(tmp_path / "runtime.json")
    base = {
        "observed_at": "2026-08-03T12:00:00+00:00",
        "account": {"net_liquidation": 2012.44},
        "positions": [],
        "candidates": [],
        "warnings": [],
    }
    snapshot = ingest_payload(base, store=store)
    assert snapshot.broker_snapshot_complete is False
    assert snapshot.working_order_count is None
    assert snapshot.unsubmitted_instruction_count is None

    with pytest.raises(ValueError, match="working_order_count"):
        ingest_payload(
            {
                **base,
                "broker_snapshot_complete": True,
                "working_order_count": True,
                "unsubmitted_instruction_count": 0,
            },
            store=store,
        )
