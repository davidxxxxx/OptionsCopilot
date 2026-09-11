"""Durable native-history claims remain bounded, immutable and observation-only."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
import sqlite3
import threading

import pytest

from options_copilot.history_source_contracts import (
    build_native_history_request,
    make_native_history_result,
    native_history_wire_parameters,
)
from options_copilot.market.session_calendar import UsOptionsSessionCalendar
from options_copilot.storage.canonical import canonical_hash, datetime_text
from options_copilot.storage import history_sources
from options_copilot.storage.history_sources import HistorySourceStore, HistorySourceStoreError


NOW = datetime(2026, 9, 9, 20, 40, tzinfo=timezone.utc)
INGESTED = NOW + timedelta(seconds=20)


def _prepared(*, symbol="SPY", con_id=756733, kind="PRICE_HISTORY", incremental=False):
    calendar = UsOptionsSessionCalendar().normalize(
        liquid_hours="20260909:0930-1600", trading_hours="20260909:0930-1600",
        timezone_id="America/New_York", observed_at=NOW,
        source="IBKR_REQ_CONTRACT_DETAILS_READONLY", now=NOW,
    )
    return build_native_history_request(
        contract={"con_id": con_id, "symbol": symbol, "sec_type": "STK", "currency": "USD",
                  "exchange": "SMART", "primary_exchange": "ARCA"},
        kind=kind, cutoff={"calendar": calendar.as_dict(), "scheduled_for": NOW.isoformat()},
        incremental=incremental, prepared_at=NOW,
    )


def _parent(request, *, run="scheduled.1"):
    return {
        "parent_run_id": run, "owner": "daily-operation.next_session_preparation",
        "operation": "NEXT_SESSION_PREPARATION", "session_date": "2026-09-09",
        "scheduled_for": NOW.isoformat(), "deadline_at": (NOW + timedelta(minutes=5)).isoformat(),
        "calendar_hash": request["cutoff"]["calendar_hash"],
    }


def _freeze(store, request, *, run="scheduled.1"):
    return store.freeze_manifest(_parent(request, run=run), [request], guard=lambda: True)


def _claim(store, manifest, request, *, guard=lambda: True):
    return store.claim_for_send(manifest["manifest_id"], request["request_hash"], request["basis_hash"], guard=guard)


def _fragment(request, permit, *, state="DELIVERED", close=None):
    response = {
        "broker_request_id": 12, "send_state": "DISPATCHED", "response_end_received": True,
        "response_end": {"start": "20260908", "end": "20260909"},
        "wire_parameters": native_history_wire_parameters(request), "start_generation": 2,
        "end_generation": 2, "broker_error_codes": [], "bars": [{
            "raw_date": "20260909", "session_date": "2026-09-09", "open": "1", "high": "1",
            "low": "1", "close": close or ("0.2" if request["kind"] == "IV_HISTORY" else "100"),
            "volume": "10", "valid_close": True, "date_eligible": True,
        }], "received_bar_count": 1, "timed_out": False, "epoch_valid": True,
    }
    reasons = []
    if state == "PARTIAL":
        response.update(response_end_received=False, response_end=None, timed_out=True)
        reasons = ["NATIVE_HISTORY_TIMEOUT", "NATIVE_HISTORY_END_UNVERIFIED"]
    elif state == "UNAVAILABLE":
        response.update(bars=[], received_bar_count=0)
        reasons = ["NATIVE_HISTORY_EMPTY"]
    return make_native_history_result(
        request, claim_id=permit.claim_id, requested_at=NOW + timedelta(seconds=1),
        available_at=NOW + timedelta(seconds=3), response=response, reason_codes=reasons,
    )


def _publish(store, request=None, *, run="scheduled.1", state="DELIVERED", close=None):
    request = request or _prepared()
    manifest = _freeze(store, request, run=run)
    permit = _claim(store, manifest, request).permit
    fragment = _fragment(request, permit, state=state, close=close)
    result = store.complete(permit, fragment, guard=lambda: True)
    return request, manifest, permit, fragment, result


def _read(store, request=None, *, cutoff=INGESTED):
    request = request or _prepared()
    return store.read_fragments(con_id=request["con_id"], request_hashes=[request["request_hash"]],
                                basis_hash=request["basis_hash"], cutoff=cutoff)


class _FailOneCommit:
    def __init__(self, connection):
        self.connection = connection
        self.fail = True

    def __getattr__(self, name):
        return getattr(self.connection, name)

    def execute(self, statement, *args):
        if statement == "COMMIT" and self.fail:
            self.fail = False
            raise sqlite3.OperationalError("fixture commit failure")
        return self.connection.execute(statement, *args)


def _tamper_first(path, *, reseal=False):
    with sqlite3.connect(path) as connection:
        connection.row_factory = sqlite3.Row
        trigger = connection.execute("SELECT sql FROM sqlite_master WHERE name='history_sources_no_update'").fetchone()[0]
        connection.execute("DROP TRIGGER history_sources_no_update")
        if not reseal:
            connection.execute("UPDATE history_source_events SET first_seen_at=? WHERE sequence=1", (datetime_text(NOW),))
        else:
            # Change a semantically valid retained manifest, then reseal every
            # chain link. The live trusted head must still detect replacement.
            previous = "0" * 64
            for raw in connection.execute("SELECT * FROM history_source_events ORDER BY sequence").fetchall():
                row = dict(raw)
                if row["sequence"] == 1:
                    row["first_seen_at"] = datetime_text(NOW)
                row["previous_hash"] = previous
                row["row_hash"] = canonical_hash({key: value for key, value in row.items() if key != "row_hash"})
                connection.execute("UPDATE history_source_events SET first_seen_at=?,previous_hash=?,row_hash=? WHERE sequence=?",
                                   (row["first_seen_at"], row["previous_hash"], row["row_hash"], row["sequence"]))
                previous = row["row_hash"]
        connection.execute(trigger)


def test_frozen_manifest_is_exact_detached_sorted_and_idempotent(tmp_path):
    price, iv = _prepared(), _prepared(kind="IV_HISTORY")
    parent = _parent(price)
    with HistorySourceStore(tmp_path / "history.sqlite3", clock=lambda: INGESTED) as store:
        first = store.freeze_manifest(parent, [price, iv], guard=lambda: True)
        second = store.freeze_manifest(parent, [iv, price], guard=lambda: True)
        assert first["inserted"] is True and second["inserted"] is False
        assert first["manifest_id"] == second["manifest_id"]
        raw = store._connection.execute("SELECT payload_json FROM history_source_events").fetchone()[0]
        document = json.loads(raw)
        assert document["parent_document"] == parent
        assert [row["kind"] for row in document["requests"]] == ["IV_HISTORY", "PRICE_HISTORY"]
        parent["parent_run_id"] = "changed"
        price["symbol"] = "CHANGED"
        store.verify_integrity()
        assert store.status()["manifest_count"] == 1


@pytest.mark.parametrize("case", ["empty", "duplicate", "seven", "four_symbols", "symbol_identity", "con_id_identity"])
def test_manifest_rejects_unbounded_or_ambiguous_requests(tmp_path, case):
    request = _prepared()
    requests = {
        "empty": [], "duplicate": [request, request], "seven": [request] * 7,
        "four_symbols": [_prepared(symbol=symbol, con_id=index + 1) for index, symbol in enumerate(("SPY", "QQQ", "IWM", "DIA"))],
        "symbol_identity": [request, _prepared(kind="IV_HISTORY", con_id=1)],
        "con_id_identity": [request, _prepared(symbol="QQQ", kind="IV_HISTORY")],
    }[case]
    with HistorySourceStore(tmp_path / "history.sqlite3", clock=lambda: INGESTED) as store:
        with pytest.raises(HistorySourceStoreError):
            store.freeze_manifest(_parent(request), requests, guard=lambda: True)
        assert store.status()["manifest_count"] == 0


@pytest.mark.parametrize("field,value", [
    ("owner", "manual"), ("operation", "INTRADAY"), ("session_date", "2026-09-08"),
    ("scheduled_for", "2026-09-09T20:41:00+00:00"), ("calendar_hash", "f" * 64),
    ("deadline_at", NOW.isoformat()), ("extra", True),
])
def test_parent_document_must_match_frozen_requests(tmp_path, field, value):
    request = _prepared()
    parent = {**_parent(request), field: value}
    with HistorySourceStore(tmp_path / "history.sqlite3", clock=lambda: INGESTED) as store:
        with pytest.raises(HistorySourceStoreError):
            store.freeze_manifest(parent, [request], guard=lambda: True)
        assert store.status()["manifest_count"] == 0


def test_same_parent_cannot_replace_frozen_request_manifest(tmp_path):
    with HistorySourceStore(tmp_path / "history.sqlite3", clock=lambda: INGESTED) as store:
        _freeze(store, _prepared())
        with pytest.raises(HistorySourceStoreError, match="MANIFEST_CONFLICT"):
            _freeze(store, _prepared(incremental=True))
        assert store.status()["manifest_count"] == 1


def test_exact_six_request_three_symbol_boundary_is_supported(tmp_path):
    requests = [_prepared(symbol=symbol, con_id=index + 1, kind=kind)
                for index, symbol in enumerate(("SPY", "QQQ", "IWM"))
                for kind in ("PRICE_HISTORY", "IV_HISTORY")]
    with HistorySourceStore(tmp_path / "history.sqlite3", clock=lambda: INGESTED) as store:
        manifest = store.freeze_manifest(_parent(requests[0]), requests, guard=lambda: True)
        assert manifest["request_count"] == 6
        assert all(_claim(store, manifest, request).status == "CLAIMED" for request in requests)
        assert store.status()["intent_count"] == 6


def test_send_permit_is_at_most_once_and_reopen_never_reissues_it(tmp_path):
    path, request = tmp_path / "history.sqlite3", _prepared()
    with HistorySourceStore(path, clock=lambda: INGESTED) as store:
        manifest = _freeze(store, request)
        first = _claim(store, manifest, request)
        again = _claim(store, manifest, request)
        assert first.status == "CLAIMED" and first.permit is not None
        assert again.status == "ALREADY_CLAIMED" and again.permit is None
        assert first.reference == again.reference
    with HistorySourceStore(path, clock=lambda: INGESTED + timedelta(days=365)) as store:
        result = _claim(store, manifest, request)
        assert result.status == "UNCERTAIN" and result.permit is None
        assert store.status()["intent_count"] == store.status()["uncertain_count"] == 1


def test_two_instances_cannot_both_claim_same_frozen_request(tmp_path):
    path, request = tmp_path / "history.sqlite3", _prepared()
    with HistorySourceStore(path, clock=lambda: INGESTED) as first, HistorySourceStore(path, clock=lambda: INGESTED) as second:
        manifest = _freeze(first, request)
        barrier = threading.Barrier(2)

        def claim(store):
            barrier.wait(timeout=3)
            return _claim(store, manifest, request)

        with ThreadPoolExecutor(max_workers=2) as pool:
            jobs = [pool.submit(claim, store) for store in (first, second)]
            results = [job.result(timeout=5) for job in jobs]
        assert sorted(row.status for row in results) == ["CLAIMED", "UNCERTAIN"]
        assert sum(row.permit is not None for row in results) == 1
        assert first.status()["intent_count"] == 1
        second.verify_integrity()


def test_permits_cannot_be_copied_forged_or_used_in_other_store(tmp_path):
    path, request = tmp_path / "history.sqlite3", _prepared()
    with HistorySourceStore(path, clock=lambda: INGESTED) as first, HistorySourceStore(path, clock=lambda: INGESTED) as second:
        manifest = _freeze(first, request)
        permit = _claim(first, manifest, request).permit
        fragment = _fragment(request, permit)
        for store, candidate in ((first, replace(permit)), (first, replace(permit, _instance=object())), (second, permit)):
            with pytest.raises(HistorySourceStoreError, match="SEND_PERMIT_INVALID"):
                store.complete(candidate, fragment, guard=lambda: True)
        assert first.status()["completed_count"] == 0


@pytest.mark.parametrize("state", ["DELIVERED", "PARTIAL", "UNAVAILABLE"])
def test_terminal_fragment_and_response_state_are_one_atomic_immutable_event(tmp_path, state):
    path = tmp_path / "history.sqlite3"
    with HistorySourceStore(path, clock=lambda: INGESTED) as store:
        request, manifest, permit, fragment, result = _publish(store, state=state)
        assert result["reference"]["sequence"] == 3
        stored = _read(store)[0]
        assert stored["fragment"] == fragment
        assert stored["prepared_request"] == request
        assert stored["parent_document"] == _parent(request)
        assert stored["reference"] == result["reference"]
        assert fragment["status"] == state
        assert fragment["model_input_complete"] is False
        assert fragment["production_eligible"] is False
        assert _claim(store, manifest, request).status == "COMPLETED"
        assert _claim(store, manifest, request).permit is None
        status = store.status()
        assert status["completed_count"] == 1 and status["uncertain_count"] == 0
        assert status["decision_authority"] == "OBSERVATION_ONLY"
        fragment["response"]["bars"].clear()
        assert _read(store)[0] == stored
    with HistorySourceStore(path, clock=lambda: INGESTED) as store:
        assert _read(store)[0] == stored


def test_identical_completion_is_idempotent_but_revision_or_failure_cannot_replace_it(tmp_path):
    with HistorySourceStore(tmp_path / "history.sqlite3", clock=lambda: INGESTED) as store:
        request, _, permit, fragment, first = _publish(store)
        second = store.complete(permit, fragment, guard=lambda: True)
        assert second["inserted"] is False and first["reference"] == second["reference"]
        with pytest.raises(HistorySourceStoreError, match="TERMINAL_CONFLICT"):
            store.complete(permit, _fragment(request, permit, close="101"), guard=lambda: True)
        with pytest.raises(HistorySourceStoreError, match="TERMINAL_CONFLICT"):
            store.record_failure(permit, "KNOWN_FAILURE", guard=lambda: True)
        assert store.status()["completed_count"] == 1


def test_known_failure_is_terminal_without_publishing_a_fragment(tmp_path):
    request = _prepared()
    with HistorySourceStore(tmp_path / "history.sqlite3", clock=lambda: INGESTED) as store:
        manifest = _freeze(store, request)
        permit = _claim(store, manifest, request).permit
        result = store.record_failure(permit, "KNOWN_FAILURE", guard=lambda: True)
        assert result["reason_code"] == "KNOWN_FAILURE"
        assert store.record_failure(permit, "KNOWN_FAILURE", guard=lambda: True)["inserted"] is False
        assert _claim(store, manifest, request).status == "FAILED"
        assert _read(store) == ()
        assert store.status()["failed_count"] == 1
        with pytest.raises(HistorySourceStoreError, match="TERMINAL_CONFLICT"):
            store.complete(permit, _fragment(request, permit), guard=lambda: True)


@pytest.mark.parametrize("operation", ["manifest", "claim", "completion"])
@pytest.mark.parametrize("reject_at", [1, 2])
def test_parent_guard_checked_before_transaction_and_before_commit(tmp_path, operation, reject_at):
    request = _prepared()
    with HistorySourceStore(tmp_path / "history.sqlite3", clock=lambda: INGESTED) as store:
        manifest = _freeze(store, request) if operation != "manifest" else None
        permit = _claim(store, manifest, request).permit if operation == "completion" else None
        before = store.status()
        checkpoint = store._checkpoint
        calls = 0

        def guard():
            nonlocal calls
            calls += 1
            return calls != reject_at

        with pytest.raises(HistorySourceStoreError, match="PARENT_GUARD_REJECTED"):
            if operation == "manifest":
                store.freeze_manifest(_parent(request), [request], guard=guard)
            elif operation == "claim":
                _claim(store, manifest, request, guard=guard)
            else:
                store.complete(permit, _fragment(request, permit), guard=guard)
        assert store._checkpoint == checkpoint
        assert store.status() == before
        assert len(store._permits) == (1 if operation == "completion" else 0)


@pytest.mark.parametrize("operation", ["manifest", "claim", "completion"])
def test_commit_failure_cannot_publish_checkpoint_permit_or_partial_terminal(tmp_path, operation):
    request = _prepared()
    with HistorySourceStore(tmp_path / "history.sqlite3", clock=lambda: INGESTED) as store:
        manifest = _freeze(store, request) if operation != "manifest" else None
        permit = _claim(store, manifest, request).permit if operation == "completion" else None
        before, checkpoint = store.status(), store._checkpoint
        store._connection = _FailOneCommit(store._connection)
        with pytest.raises(HistorySourceStoreError):
            if operation == "manifest":
                _freeze(store, request)
            elif operation == "claim":
                _claim(store, manifest, request)
            else:
                store.complete(permit, _fragment(request, permit), guard=lambda: True)
        assert store._checkpoint == checkpoint
        assert store.status() == before
        assert len(store._permits) == (1 if operation == "completion" else 0)
        if operation == "claim":
            assert _claim(store, manifest, request).status == "CLAIMED"
        elif operation == "completion":
            assert store.complete(permit, _fragment(request, permit), guard=lambda: True)["inserted"] is True


@pytest.mark.parametrize("operation", ["claim", "completion"])
def test_error_after_successful_commit_never_reissues_send_or_replaces_terminal(tmp_path, operation):
    request = _prepared()
    with HistorySourceStore(tmp_path / "history.sqlite3", clock=lambda: INGESTED) as store:
        manifest = _freeze(store, request)
        permit = _claim(store, manifest, request).permit if operation == "completion" else None
        checkpoint = store._checkpoint
        connection = store._connection

        class ErrorAfterCommit:
            fail = True

            def __getattr__(self, name):
                return getattr(connection, name)

            def execute(self, statement, *args):
                result = connection.execute(statement, *args)
                if statement == "COMMIT" and self.fail:
                    self.fail = False
                    raise sqlite3.OperationalError("fixture response lost after commit")
                return result

        store._connection = ErrorAfterCommit()
        with pytest.raises(HistorySourceStoreError):
            if operation == "claim":
                _claim(store, manifest, request)
            else:
                store.complete(permit, _fragment(request, permit), guard=lambda: True)
        assert store._checkpoint == checkpoint
        retry = _claim(store, manifest, request)
        assert retry.permit is None
        assert retry.status == ("UNCERTAIN" if operation == "claim" else "COMPLETED")
        assert store.status()["intent_count"] == 1
        assert len(_read(store)) == (1 if operation == "completion" else 0)


def test_real_local_first_seen_blocks_backfill_into_earlier_point_in_time(tmp_path):
    with HistorySourceStore(tmp_path / "history.sqlite3", clock=lambda: INGESTED) as store:
        request, _, _, fragment, result = _publish(store)
        assert result["reference"]["first_seen_at"] == datetime_text(INGESTED)
        assert result["reference"]["first_seen_at"] != fragment["available_at"]
        assert _read(store, cutoff=INGESTED - timedelta(microseconds=1)) == ()
        assert len(_read(store, cutoff=INGESTED)) == 1
        assert store.find_fragments(symbol=request["symbol"], con_id=request["con_id"], cutoff=NOW) == ()


def test_windows_revisions_and_sources_remain_separate_and_queries_are_exact(tmp_path):
    with HistorySourceStore(tmp_path / "history.sqlite3", clock=lambda: INGESTED) as store:
        request, *_ = _publish(store)
        _publish(store, run="scheduled.2", close="101")
        incremental = _prepared(incremental=True)
        _publish(store, incremental, run="scheduled.3")
        _publish(store, _prepared(kind="IV_HISTORY"), run="scheduled.4")
        assert len(_read(store)) == 2
        assert len(store.read_fragments(con_id=request["con_id"], request_hashes=[request["request_hash"], incremental["request_hash"]],
                                        basis_hash=request["basis_hash"], cutoff=INGESTED)) == 3
        assert len(store.find_fragments(symbol="SPY", con_id=request["con_id"], cutoff=INGESTED)) == 4
        assert store.find_fragments(symbol="QQQ", con_id=request["con_id"], cutoff=INGESTED) == ()
        assert store.find_fragments(symbol="SPY", con_id=1, cutoff=INGESTED) == ()
        assert store.read_fragments(con_id=request["con_id"], request_hashes=["f" * 64], basis_hash=request["basis_hash"], cutoff=INGESTED) == ()
        assert store.read_fragments(con_id=request["con_id"], request_hashes=[request["request_hash"]], basis_hash="f" * 64, cutoff=INGESTED) == ()


def test_paginated_retrieval_retains_over_128_fragments_without_loss_or_silent_truncation(tmp_path):
    request = _prepared()
    path = tmp_path / "history.sqlite3"
    clock = [INGESTED]
    with HistorySourceStore(path, clock=lambda: clock[0]) as store:
        for index in range(130):
            _publish(store, request, run=f"scheduled.{index}")
        with pytest.raises(HistorySourceStoreError, match="QUERY_LIMIT"):
            store.find_fragments(symbol="SPY", con_id=request["con_id"], cutoff=INGESTED)
        first = store.find_fragments_page(symbol="SPY", con_id=request["con_id"], cutoff=INGESTED)
        assert set(first) == {"fragments", "has_more", "next_before_sequence"}
        assert len(first["fragments"]) == 128 and first["has_more"] is True
        sequences = [row["reference"]["sequence"] for row in first["fragments"]]
        assert sequences == sorted(sequences, reverse=True)
        assert first["next_before_sequence"] == sequences[-1]
        # Newer commits cannot shift this immutable exclusive sequence cursor.
        _publish(store, request, run="scheduled.newer_same_cutoff")
        clock[0] += timedelta(seconds=1)
        _publish(store, request, run="scheduled.future")
        second = store.find_fragments_page(symbol="SPY", con_id=request["con_id"], cutoff=INGESTED,
                                           before_sequence=first["next_before_sequence"])
        assert len(second["fragments"]) == 2
        assert second["has_more"] is False and second["next_before_sequence"] is None
        pages = (*first["fragments"], *second["fragments"])
        assert len({row["claim_id"] for row in pages}) == 130
        assert {row["parent_document"]["parent_run_id"] for row in pages} == {f"scheduled.{index}" for index in range(130)}
        latest = store.find_fragments_page(symbol="SPY", con_id=request["con_id"], cutoff=INGESTED, limit=1)
        assert latest["fragments"][0]["parent_document"]["parent_run_id"] == "scheduled.newer_same_cutoff"
        assert latest["has_more"] is True
        assert all(row["reference"]["first_seen_at"] <= datetime_text(INGESTED) for row in pages)
        assert store.status()["completed_count"] == 132
        store.verify_integrity()


@pytest.mark.parametrize("field,value", [
    ("before_sequence", True), ("before_sequence", 0), ("before_sequence", -1),
    ("before_sequence", "3"), ("before_sequence", 3.0), ("before_sequence", 2**63),
    ("limit", True), ("limit", 0), ("limit", -1), ("limit", 129), ("limit", "1"),
    ("symbol", "spy"), ("con_id", True),
])
def test_pagination_rejects_invalid_or_unbounded_cursors_and_queries(tmp_path, field, value):
    with HistorySourceStore(tmp_path / "history.sqlite3", clock=lambda: INGESTED) as store:
        query = {"symbol": "SPY", "con_id": 756733, "cutoff": INGESTED, field: value}
        with pytest.raises(HistorySourceStoreError, match="QUERY_INVALID"):
            store.find_fragments_page(**query)


def test_paginated_result_is_detached_and_excludes_wrong_identity_and_future_ingestion(tmp_path):
    request = _prepared()
    with HistorySourceStore(tmp_path / "history.sqlite3", clock=lambda: INGESTED) as store:
        _publish(store)
        for query in ({"symbol": "QQQ", "con_id": request["con_id"], "cutoff": INGESTED},
                      {"symbol": "SPY", "con_id": 1, "cutoff": INGESTED},
                      {"symbol": "SPY", "con_id": request["con_id"], "cutoff": NOW},
                      {"symbol": "SPY", "con_id": request["con_id"], "cutoff": INGESTED, "before_sequence": 1}):
            assert store.find_fragments_page(**query) == {"fragments": (), "has_more": False, "next_before_sequence": None}
        page = store.find_fragments_page(symbol="SPY", con_id=request["con_id"], cutoff=INGESTED, limit=1)
        assert page["has_more"] is False and page["next_before_sequence"] is None
        page["fragments"][0]["fragment"]["response"]["bars"].clear()
        assert len(_read(store)[0]["fragment"]["response"]["bars"]) == 1


@pytest.mark.parametrize("reseal", [False, True])
def test_external_old_row_tamper_invalidates_trusted_checkpoint(tmp_path, reseal):
    path = tmp_path / "history.sqlite3"
    with HistorySourceStore(path, clock=lambda: INGESTED) as store:
        _publish(store)
        _tamper_first(path, reseal=reseal)
        with pytest.raises(HistorySourceStoreError, match="STORE_INVALID"):
            _read(store)
        with pytest.raises(HistorySourceStoreError, match="STORE_INVALID"):
            _freeze(store, _prepared(), run="scheduled.2")


def test_truncated_previously_verified_tail_is_rejected(tmp_path):
    path = tmp_path / "history.sqlite3"
    with HistorySourceStore(path, clock=lambda: INGESTED) as store:
        _publish(store)
        with sqlite3.connect(path) as connection:
            trigger = connection.execute("SELECT sql FROM sqlite_master WHERE name='history_sources_no_delete'").fetchone()[0]
            connection.execute("DROP TRIGGER history_sources_no_delete")
            connection.execute("DELETE FROM history_source_events WHERE sequence=3")
            connection.execute(trigger)
        with pytest.raises(HistorySourceStoreError, match="STORE_INVALID"):
            store.status()


@pytest.mark.parametrize("change", ["drop_index", "rogue_trigger", "temp_trigger", "temp_shadow"])
def test_schema_and_main_or_temp_trigger_injection_fail_closed(tmp_path, change):
    with HistorySourceStore(tmp_path / "history.sqlite3", clock=lambda: INGESTED) as store:
        _publish(store)
        commands = {
            "drop_index": "DROP INDEX history_sources_lookup",
            "rogue_trigger": "CREATE TRIGGER rogue AFTER INSERT ON history_source_events BEGIN SELECT 1; END",
            "temp_trigger": "CREATE TEMP TRIGGER rogue AFTER INSERT ON main.history_source_events BEGIN SELECT 1; END",
            "temp_shadow": "CREATE TEMP TABLE history_source_events(sequence)",
        }
        store._connection.execute(commands[change])
        with pytest.raises(HistorySourceStoreError, match="STORE_INVALID"):
            _read(store)


def test_immutable_database_triggers_reject_direct_edits(tmp_path):
    path = tmp_path / "history.sqlite3"
    with HistorySourceStore(path, clock=lambda: INGESTED) as store:
        _publish(store)
        with sqlite3.connect(path) as connection:
            for sql in ("DELETE FROM history_source_events", "UPDATE history_source_events SET con_id=1"):
                with pytest.raises(sqlite3.IntegrityError, match="immutable"):
                    connection.execute(sql)
        store.verify_integrity()


def test_external_commit_during_read_never_certifies_unseen_database_version(tmp_path, monkeypatch):
    path = tmp_path / "history.sqlite3"
    with HistorySourceStore(path, clock=lambda: INGESTED) as store:
        _publish(store)
        original = history_sources._fragment
        changed = False

        def validate_and_tamper(*args, **kwargs):
            nonlocal changed
            if not changed:
                changed = True
                _tamper_first(path)
            return original(*args, **kwargs)

        monkeypatch.setattr(history_sources, "_fragment", validate_and_tamper)
        assert len(_read(store)) == 1
        assert store._checkpoint.state is None
        with pytest.raises(HistorySourceStoreError, match="STORE_INVALID"):
            _read(store)


def test_owned_operations_reuse_checkpoint_but_external_append_forces_full_audit(tmp_path, monkeypatch):
    path = tmp_path / "history.sqlite3"
    with HistorySourceStore(path, clock=lambda: INGESTED) as first, HistorySourceStore(path, clock=lambda: INGESTED) as second:
        _publish(first)
        audits = []
        verify = first._verify

        def count_audit(*args, **kwargs):
            audits.append(True)
            return verify(*args, **kwargs)

        monkeypatch.setattr(first, "_verify", count_audit)
        _publish(first, run="scheduled.2")
        assert len(_read(first)) == 2
        first.status()
        assert audits == []
        _publish(second, run="scheduled.3")
        assert len(_read(first)) == 3
        first.status()
        assert audits == [True]
        first.verify_integrity()
        assert audits == [True, True]


def test_same_connection_tampering_cannot_hide_behind_restored_schema_version(tmp_path):
    with HistorySourceStore(tmp_path / "history.sqlite3", clock=lambda: INGESTED) as store:
        _publish(store)
        connection = store._connection
        version = connection.execute("PRAGMA schema_version").fetchone()[0]
        trigger = connection.execute("SELECT sql FROM sqlite_master WHERE name='history_sources_no_update'").fetchone()[0]
        connection.execute("DROP TRIGGER history_sources_no_update")
        connection.execute("UPDATE history_source_events SET con_id=1 WHERE sequence=3")
        connection.execute(trigger)
        connection.execute(f"PRAGMA schema_version={version}")
        with pytest.raises(HistorySourceStoreError, match="STORE_INVALID"):
            _read(store)


def test_verification_budget_returns_no_partial_result_and_does_not_poison_checkpoint(tmp_path, monkeypatch):
    with HistorySourceStore(tmp_path / "history.sqlite3", clock=lambda: INGESTED) as store:
        _publish(store)
        checkpoint = store._checkpoint
        ticks = iter((0.0, 3.0))
        with monkeypatch.context() as patch:
            patch.setattr(history_sources.time, "monotonic", lambda: next(ticks))
            with pytest.raises(HistorySourceStoreError, match="VERIFICATION_BUDGET_EXCEEDED"):
                _read(store)
        assert store._checkpoint == checkpoint
        assert len(_read(store)) == 1


def test_unknown_schema_is_not_downgraded_and_closed_store_refuses_operations(tmp_path):
    path = tmp_path / "unknown.sqlite3"
    with sqlite3.connect(path) as connection:
        connection.execute("PRAGMA user_version=99")
    with pytest.raises(HistorySourceStoreError, match="SCHEMA_UNSUPPORTED"):
        HistorySourceStore(path)
    with sqlite3.connect(path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 99
    store = HistorySourceStore(tmp_path / "history.sqlite3")
    store.close()
    store.close()
    with pytest.raises(HistorySourceStoreError, match="STORE_CLOSED"):
        store.status()
