"""Focused append-only and replay tests for the G035 equity pool ledger."""

from __future__ import annotations

import sqlite3
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from options_copilot.equity_pool import (
    CanonicalClassification,
    ClassificationSource,
    EquityCategory,
    EquityPoolAllocator,
    EquityPoolInput,
    EquityPoolStore,
    EquityPoolStoreConflict,
    EquityPoolStoreCorruption,
    FactorEvidence,
    FactorKind,
    FactorStatus,
    LiquidityEvidence,
    PositionMode,
)
from options_copilot.storage.canonical import canonical_hash


NOW = datetime(2026, 8, 21, 13, 30, tzinfo=timezone.utc)


def _input(
    symbol: str,
    *,
    rank: int,
    signal: Decimal = Decimal("1"),
    overlay: dict[str, object] | None = None,
) -> EquityPoolInput:
    factors = tuple(
        FactorEvidence(
            factor=kind,
            status=FactorStatus.AVAILABLE,
            signed_signal=signal,
            confidence=Decimal("0.8"),
            horizon="5D",
            observed_at=NOW,
            effective_at=NOW - timedelta(minutes=1),
            valid_until=NOW + timedelta(days=1),
            source_hashes=(canonical_hash({"source": symbol, "factor": kind.value}),),
            reasons=(),
            payload_hash=canonical_hash({"payload": symbol, "factor": kind.value}),
        )
        for kind in FactorKind
    )
    return EquityPoolInput(
        classification=CanonicalClassification(
            symbol=symbol,
            category=EquityCategory.ENERGY,
            source=ClassificationSource.LOCAL_EXACT,
        ),
        factors=factors,
        liquidity=LiquidityEvidence(
            status=FactorStatus.AVAILABLE,
            score=Decimal("80"),
            observed_at=NOW,
            source_hashes=(canonical_hash({"liquidity": symbol}),),
            reasons=(),
            payload_hash=canonical_hash({"liquidity-payload": symbol}),
        ),
        discovery_rank=rank,
        discovery_source="CAPTURED_IBKR_REPLAY",
        captured_at=NOW,
        deepseek_overlay=overlay,
    )


def test_store_uses_wal_full_fk_and_round_trips_offline_replay(tmp_path) -> None:
    path = tmp_path / "equity-pool.sqlite3"
    allocator = EquityPoolAllocator()
    inputs = (_input("XOM", rank=1), _input("CVX", rank=2))
    with EquityPoolStore(path) as store:
        assert store.journal_mode == "wal"
        assert store.synchronous == "full"
        assert store.schema_version == 2
        assert store._connection.execute("PRAGMA foreign_keys").fetchone()[0] == 1

        appended = store.append(
            slot=NOW,
            inputs=inputs,
            allocator=allocator,
            position_mode=PositionMode.BLOCKED_OPEN_POSITION,
            recorded_at=NOW + timedelta(minutes=1),
        )
        replayed = store.replay(appended.snapshot.pool_id, allocator=allocator)
        latest = store.latest()
        assert latest is not None
        assert latest.snapshot.pool_id == appended.snapshot.pool_id
        assert replayed.snapshot_hash == appended.snapshot.snapshot_hash
        assert replayed.position_mode is PositionMode.BLOCKED_OPEN_POSITION
        assert tuple(item.symbol for item in appended.normalized_inputs) == ("XOM", "CVX")
        store.assert_integrity()


def test_equity_store_commit_guard_rolls_back_without_late_record(tmp_path) -> None:
    with EquityPoolStore(tmp_path / "equity-pool.sqlite3") as store:
        with pytest.raises(TimeoutError, match="commit cancelled"):
            store.append(
                slot=NOW,
                inputs=(_input("XOM", rank=1),),
                allocator=EquityPoolAllocator(),
                recorded_at=NOW,
                commit_guard=lambda: False,
            )
        assert store.latest() is None


def test_missing_discovery_provenance_is_explicitly_excluded() -> None:
    candidate = replace(
        _input("XOM", rank=1),
        discovery_source="MISSING_PROVENANCE",
    )
    snapshot = EquityPoolAllocator().allocate((candidate,), slot=NOW)
    assert snapshot.selected == ()
    assert snapshot.excluded[0].reasons == (
        "DISCOVERY_PROVENANCE_UNAVAILABLE",
    )


def test_same_slot_and_same_canonical_inputs_is_idempotent_and_shadow_is_ignored(tmp_path) -> None:
    path = tmp_path / "equity-pool.sqlite3"
    allocator = EquityPoolAllocator()
    first_input = _input("XOM", rank=1, overlay={"view": "BULLISH"})
    second_input = _input("XOM", rank=1, overlay={"view": "BEARISH"})
    with EquityPoolStore(path) as store:
        first = store.append(
            slot=NOW,
            inputs=(first_input,),
            allocator=allocator,
            recorded_at=NOW + timedelta(minutes=1),
        )
        second = store.append(
            slot=NOW,
            inputs=(second_input,),
            allocator=allocator,
            recorded_at=NOW + timedelta(hours=1),
        )
        assert second.sequence == first.sequence
        assert second.snapshot_hash == first.snapshot_hash
        assert second.recorded_at == first.recorded_at
        count = store._connection.execute(
            "SELECT COUNT(*) FROM equity_pool_snapshots"
        ).fetchone()[0]
        assert count == 1


def test_same_slot_with_different_inputs_is_a_conflict(tmp_path) -> None:
    path = tmp_path / "equity-pool.sqlite3"
    allocator = EquityPoolAllocator()
    with EquityPoolStore(path) as store:
        store.append(slot=NOW, inputs=(_input("XOM", rank=1),), allocator=allocator)
        with pytest.raises(EquityPoolStoreConflict, match="different snapshot"):
            store.append(
                slot=NOW,
                inputs=(_input("XOM", rank=1, signal=Decimal("-1")),),
                allocator=allocator,
            )


def test_chain_covers_multiple_daily_slots_and_latest(tmp_path) -> None:
    path = tmp_path / "equity-pool.sqlite3"
    allocator = EquityPoolAllocator()
    with EquityPoolStore(path) as store:
        first = store.append(
            slot=NOW,
            inputs=(_input("XOM", rank=1),),
            allocator=allocator,
        )
        second = store.append(
            slot=NOW + timedelta(days=1),
            inputs=(_input("CVX", rank=1),),
            allocator=allocator,
        )
        assert second.previous_chain_hash == first.chain_hash
        assert store.latest().snapshot.pool_id == second.snapshot.pool_id  # type: ignore[union-attr]
        store.assert_integrity()


@pytest.mark.parametrize(
    ("statement", "expected"),
    (
        (
            "UPDATE equity_pool_snapshots SET normalized_inputs_hash = ? WHERE sequence = 1",
            "normalized equity inputs were tampered",
        ),
        (
            "UPDATE equity_pool_snapshots SET body_json = ? WHERE sequence = 1",
            "snapshot cannot be decoded",
        ),
        (
            "UPDATE equity_pool_rows SET row_hash = ? WHERE snapshot_sequence = 1 AND ordinal = 1",
            "row hash mismatch",
        ),
    ),
)
def test_tamper_fails_closed(tmp_path, statement: str, expected: str) -> None:
    path = tmp_path / "equity-pool.sqlite3"
    allocator = EquityPoolAllocator()
    with EquityPoolStore(path) as store:
        store.append(slot=NOW, inputs=(_input("XOM", rank=1),), allocator=allocator)
        connection = sqlite3.connect(path)
        try:
            trigger = (
                "equity_pool_rows_no_update"
                if "equity_pool_rows" in statement
                else "equity_pool_snapshots_no_update"
            )
            connection.execute(f"DROP TRIGGER {trigger}")
            connection.execute(statement, ("tampered",))
            connection.commit()
        finally:
            connection.close()
        store._create_required_triggers()
        with pytest.raises(EquityPoolStoreCorruption, match=expected):
            store.assert_integrity()


def test_replay_detects_policy_drift_without_network_or_filler(tmp_path) -> None:
    path = tmp_path / "equity-pool.sqlite3"
    allocator = EquityPoolAllocator()
    with EquityPoolStore(path) as store:
        stored = store.append(
            slot=NOW,
            inputs=(_input("XOM", rank=1),),
            allocator=allocator,
        )
        drifted = EquityPoolAllocator(replace(allocator.policy, unclassified_cap=1))
        with pytest.raises(EquityPoolStoreCorruption, match="does not replay"):
            store.replay(stored.snapshot.pool_id, allocator=drifted)


def test_replay_detects_current_scoring_policy_drift(tmp_path, monkeypatch) -> None:
    import options_copilot.equity_pool.allocator as allocator_module

    path = tmp_path / "equity-pool.sqlite3"
    allocator = EquityPoolAllocator()
    with EquityPoolStore(path) as store:
        stored = store.append(
            slot=NOW,
            inputs=(_input("XOM", rank=1),),
            allocator=allocator,
        )
        monkeypatch.setattr(allocator_module, "SCORING_HASH", "f" * 64)
        with pytest.raises(EquityPoolStoreCorruption, match="does not replay"):
            store.replay(stored.snapshot.pool_id, allocator=allocator)


def test_latest_and_read_verify_predecessors_and_row_ledger(tmp_path) -> None:
    path = tmp_path / "equity-pool.sqlite3"
    allocator = EquityPoolAllocator()
    with EquityPoolStore(path) as store:
        first = store.append(slot=NOW, inputs=(_input("XOM", rank=1),), allocator=allocator)
        store.append(
            slot=NOW + timedelta(days=1),
            inputs=(_input("CVX", rank=1),),
            allocator=allocator,
        )
        connection = sqlite3.connect(path)
        try:
            connection.execute("DROP TRIGGER equity_pool_rows_no_update")
            connection.execute(
                "UPDATE equity_pool_rows SET row_hash = ? WHERE snapshot_sequence = 1 AND ordinal = 1",
                ("f" * 64,),
            )
            connection.commit()
        finally:
            connection.close()
        store._create_required_triggers()
        with pytest.raises(EquityPoolStoreCorruption, match="row hash mismatch"):
            store.latest()
        with pytest.raises(EquityPoolStoreCorruption, match="row hash mismatch"):
            store.read(first.snapshot.pool_id)


def test_strict_integer_decode_rejects_boolean_snapshot_counts(tmp_path) -> None:
    path = tmp_path / "equity-pool.sqlite3"
    allocator = EquityPoolAllocator()
    with EquityPoolStore(path) as store:
        stored = store.append(slot=NOW, inputs=(_input("XOM", rank=1),), allocator=allocator)
        row = store._connection.execute(
            "SELECT body_json FROM equity_pool_snapshots WHERE sequence = 1"
        ).fetchone()
        assert row is not None
        import json

        body = json.loads(row[0])
        body["discovery_count"] = True
        store._connection.execute("DROP TRIGGER equity_pool_snapshots_no_update")
        store._connection.execute(
            "UPDATE equity_pool_snapshots SET body_json = ? WHERE sequence = 1",
            (json.dumps(body),),
        )
        store._create_required_triggers()
        with pytest.raises(EquityPoolStoreCorruption, match="cannot be decoded"):
            store.read(stored.snapshot.pool_id)


def test_append_verifies_existing_ledger_and_rolls_back_without_mutation(tmp_path) -> None:
    path = tmp_path / "equity-pool.sqlite3"
    allocator = EquityPoolAllocator()
    with EquityPoolStore(path) as store:
        store.append(slot=NOW, inputs=(_input("XOM", rank=1),), allocator=allocator)
        store._connection.execute(
            "DROP TRIGGER equity_pool_rows_no_update"
        )
        store._connection.execute(
            "UPDATE equity_pool_rows SET row_hash = ? WHERE snapshot_sequence = 1 AND ordinal = 1",
            ("f" * 64,),
        )
        store._create_required_triggers()
        before = store._connection.execute(
            "SELECT COUNT(*) FROM equity_pool_snapshots"
        ).fetchone()[0]
        with pytest.raises(EquityPoolStoreCorruption, match="row hash mismatch"):
            store.append(
                slot=NOW + timedelta(days=1),
                inputs=(_input("CVX", rank=1),),
                allocator=allocator,
            )
        after = store._connection.execute(
            "SELECT COUNT(*) FROM equity_pool_snapshots"
        ).fetchone()[0]
        assert before == after == 1


@pytest.mark.parametrize(
    "statement",
    (
        "UPDATE equity_pool_snapshots SET recorded_at = recorded_at WHERE sequence = 1",
        "DELETE FROM equity_pool_snapshots WHERE sequence = 1",
        "UPDATE equity_pool_rows SET symbol = symbol WHERE snapshot_sequence = 1",
        "DELETE FROM equity_pool_rows WHERE snapshot_sequence = 1",
    ),
)
def test_immutable_triggers_block_tail_update_and_delete(tmp_path, statement: str) -> None:
    with EquityPoolStore(tmp_path / "immutable.sqlite3") as store:
        store.append(slot=NOW, inputs=(_input("XOM", rank=1),), allocator=EquityPoolAllocator())
        names = {
            row[0] for row in store._connection.execute(
                "SELECT name FROM sqlite_master WHERE type='trigger'"
            ).fetchall()
        }
        from options_copilot.equity_pool.store import REQUIRED_TRIGGERS

        assert set(REQUIRED_TRIGGERS) <= names
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            store._connection.execute(statement)


def test_missing_immutable_trigger_is_detected_fail_closed(tmp_path) -> None:
    with EquityPoolStore(tmp_path / "missing-trigger.sqlite3") as store:
        store._connection.execute("DROP TRIGGER equity_pool_rows_no_delete")
        with pytest.raises(EquityPoolStoreCorruption, match="triggers missing"):
            store.latest()


def test_same_name_noop_trigger_replacement_is_detected(tmp_path) -> None:
    with EquityPoolStore(tmp_path / "noop-trigger.sqlite3") as store:
        store._connection.execute("DROP TRIGGER equity_pool_rows_no_delete")
        store._connection.execute(
            "CREATE TRIGGER equity_pool_rows_no_delete BEFORE DELETE ON equity_pool_rows BEGIN SELECT 1; END"
        )
        with pytest.raises(EquityPoolStoreCorruption, match="definition invalid"):
            store.latest()


def test_v1_ledger_migrates_triggers_without_changing_contents(tmp_path) -> None:
    path = tmp_path / "v1.sqlite3"
    with EquityPoolStore(path) as store:
        stored = store.append(
            slot=NOW,
            inputs=(_input("XOM", rank=1),),
            allocator=EquityPoolAllocator(),
        )
        before = store._connection.execute(
            "SELECT pool_id, body_json, normalized_inputs_json, chain_hash FROM equity_pool_snapshots"
        ).fetchall()
        for name in tuple(
            row[0] for row in store._connection.execute(
                "SELECT name FROM sqlite_master WHERE type='trigger'"
            ).fetchall()
        ):
            store._connection.execute(f"DROP TRIGGER {name}")
        store._connection.execute("PRAGMA user_version=1")
    with EquityPoolStore(path) as migrated:
        after = migrated._connection.execute(
            "SELECT pool_id, body_json, normalized_inputs_json, chain_hash FROM equity_pool_snapshots"
        ).fetchall()
        assert migrated.schema_version == 2
        assert tuple(map(tuple, after)) == tuple(map(tuple, before))
        assert migrated.read(stored.snapshot.pool_id).snapshot_hash == stored.snapshot_hash
