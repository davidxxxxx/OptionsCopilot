"""Durable normalized source observations remain PIT-bound and non-authoritative."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import json
import sqlite3
import threading
from zoneinfo import ZoneInfo

import pytest

from options_copilot.feature_source_diagnostic import SOURCE_KINDS
from options_copilot.storage.canonical import canonical_hash
from options_copilot.storage.feature_sources import FeatureSourceObservationStore, FeatureSourceStoreError


NOW = datetime(2026, 9, 8, 14, 0, tzinfo=timezone.utc)


def _seal(raw):
    raw = {key: value for key, value in raw.items() if key != "content_hash"}
    return {**raw, "content_hash": canonical_hash(raw)}


def _source(kind="PRICE_HISTORY", *, cutoff=NOW, unavailable=False):
    available = (cutoff + timedelta(seconds=1)).isoformat()
    raw = {
        "schema": "options_copilot.feature_source_diagnostic.v1", "kind": kind,
        "status": "UNAVAILABLE" if unavailable else "DELIVERED", "symbol": "SPY", "source": "IBKR",
        "contract": {"con_id": 101, "symbol": "SPY", "sec_type": "STK", "currency": "USD",
                     "exchange": "SMART", "primary_exchange": "ARCA"},
        "requested_at": cutoff.isoformat(), "cutoff_at": cutoff.isoformat(), "available_at": available,
        "request_sent": True, "broker_request_id": 12, "broker_error_codes": [],
        "basis_status": "PROVIDER_NATIVE_UNRESOLVED", "decision_authority": "OBSERVATION_ONLY",
        "model_input_complete": False, "production_eligible": False, "point_in_time_verified": False,
        "reason_codes": ["FEATURE_SOURCE_UNAVAILABLE"] if unavailable else [],
    }
    if kind == "CURRENT_IV":
        raw.update({
            "request_parameters": {"genericTickList": "106", "snapshot": False, "regulatorySnapshot": False},
            "value": None if unavailable else "0.21", "received_at": None if unavailable else available,
            "tick_type": 24, "generic_tick": 106, "market_data_type": 1, "source_event_timestamp": None,
        })
    else:
        raw.update({
            "request_parameters": {
                "endDateTime": "" if kind == "PRICE_HISTORY" else cutoff.astimezone(ZoneInfo("America/New_York")).replace(hour=0, minute=0, second=0, microsecond=0).isoformat(),
                "durationStr": "1 Y" if kind == "PRICE_HISTORY" else "2 Y", "barSizeSetting": "1 day",
                "whatToShow": "ADJUSTED_LAST" if kind == "PRICE_HISTORY" else "OPTION_IMPLIED_VOLATILITY",
                "useRTH": True, "formatDate": 1, "keepUpToDate": False,
            },
            "bars": [] if unavailable else [{
                "raw_date": "20260904", "trading_date": "2026-09-04", "open": "0.2", "high": "0.3",
                "low": "0.1", "close": "0.21", "volume": "-1", "prior_date_row": True, "valid_close": True,
            }],
            "received_bar_count": 0 if unavailable else 1, "prior_completed_bar_count": 0 if unavailable else 1,
            "invalid_bar_count": 0, "duplicate_prior_date_count": 0, "excluded_current_or_future_bar_count": 0,
            "required_prior_bar_count": 60 if kind == "PRICE_HISTORY" else 252,
            "enough_prior_bars": False, "calendar_coverage_verified": False,
        })
    return _seal(raw)


def _read(store, *, cutoff=NOW + timedelta(seconds=2), symbol="SPY", con_id=101):
    return store.read(symbol=symbol, con_id=con_id, cutoff=cutoff)


def test_all_kinds_preserve_exact_projection_and_explicit_unresolved_basis(tmp_path):
    with FeatureSourceObservationStore(tmp_path / "sources.sqlite3", clock=lambda: NOW + timedelta(seconds=2)) as store:
        originals = [_source(kind) for kind in SOURCE_KINDS]
        refs = [store.append(raw, operation_id="probe.1", cutoff=NOW) for raw in originals]
        rows = _read(store)
        assert tuple(row["source"] for row in rows) == tuple(originals)
        for row, ref, original in zip(rows, refs, originals):
            assert ref["inserted"] is True
            assert set(ref) == {"observation_id", "sequence", "row_hash", "source_hash", "request_hash", "basis_hash", "first_seen_at", "inserted"}
            assert row["reference"] == {key: value for key, value in ref.items() if key != "inserted"}
            assert row["operation_id"] == "probe.1"
            assert row["request_document"]["request_parameters"] == original["request_parameters"]
            assert canonical_hash(row["request_document"]) == ref["request_hash"]
            assert canonical_hash(row["basis_document"]) == ref["basis_hash"]
            assert row["basis_document"]["basis_status"] == "PROVIDER_NATIVE_UNRESOLVED"
            assert row["basis_document"]["production_eligible"] is False
            assert row["basis_document"]["point_in_time_verified"] is False
        status = store.status()
        assert status["observation_count"] == 3
        assert status["counts_by_kind"] == dict.fromkeys(SOURCE_KINDS, 1)
        assert _read(store, con_id=102) == ()
        assert _read(store, symbol="AAPL") == ()
        store.verify_integrity()


def test_idempotency_reopen_and_local_first_seen_do_not_backdate_source(tmp_path):
    path = tmp_path / "sources.sqlite3"
    acquired = NOW + timedelta(minutes=2)
    original = _source()
    with FeatureSourceObservationStore(path, clock=lambda: acquired) as store:
        first = store.append(original, operation_id="probe.1", cutoff=NOW)
        assert _read(store) == ()
        assert first["first_seen_at"] == "2026-09-08T14:02:00.000000+00:00"
    with FeatureSourceObservationStore(path, clock=lambda: acquired + timedelta(hours=1)) as store:
        duplicate = store.append(original, operation_id="probe.1", cutoff=NOW)
        assert duplicate == {**first, "inserted": False}
        assert _read(store, cutoff=acquired)[0]["source"] == original
        assert store.status()["observation_count"] == 1


@pytest.mark.parametrize("kind", SOURCE_KINDS)
def test_latest_unavailable_revision_never_falls_back_to_old_success(tmp_path, kind):
    current = [NOW + timedelta(seconds=2)]
    later = NOW + timedelta(minutes=5)
    with FeatureSourceObservationStore(tmp_path / "sources.sqlite3", clock=lambda: current[0]) as store:
        first = store.append(_source(kind), operation_id="probe.1", cutoff=NOW)
        current[0] = later + timedelta(seconds=2)
        raw = _source(kind, cutoff=later, unavailable=True)
        latest = store.append(raw, operation_id="probe.1", cutoff=later)
        assert latest["observation_id"] != first["observation_id"]
        assert latest["sequence"] == 2
        assert _read(store)[0]["source"]["status"] == "DELIVERED"
        rows = _read(store, cutoff=current[0])
        assert len(rows) == 1
        assert rows[0]["source"] == raw


@pytest.mark.parametrize("operation_id", ["", "../escape", "a b", "x" * 129, "READ_FEATURE_SOURCE_DIAGNOSTIC"])
def test_operation_id_is_bounded_and_never_a_confirmation_token(tmp_path, operation_id):
    with FeatureSourceObservationStore(tmp_path / "sources.sqlite3", clock=lambda: NOW + timedelta(seconds=2)) as store:
        with pytest.raises(FeatureSourceStoreError, match="FEATURE_SOURCE_OPERATION_ID_INVALID"):
            store.append(_source(), operation_id=operation_id, cutoff=NOW)
        assert store.status()["observation_count"] == 0


@pytest.mark.parametrize("change", ["null_contract", "missing_requested_at", "extra_authority"])
def test_store_refuses_identity_free_method_failures_and_extra_fields(tmp_path, change):
    raw = _source(unavailable=True)
    if change == "null_contract":
        raw["contract"] = None
    elif change == "missing_requested_at":
        del raw["requested_at"]
    else:
        raw["confirmation_token"] = "READ_FEATURE_SOURCE_DIAGNOSTIC"
    with FeatureSourceObservationStore(tmp_path / "sources.sqlite3", clock=lambda: NOW + timedelta(seconds=2)) as store:
        with pytest.raises(FeatureSourceStoreError):
            store.append(_seal(raw), operation_id="probe.1", cutoff=NOW)
        assert store.status()["observation_count"] == 0


def test_future_source_and_mismatched_cutoff_cannot_be_written(tmp_path):
    with FeatureSourceObservationStore(tmp_path / "sources.sqlite3", clock=lambda: NOW) as store:
        with pytest.raises(FeatureSourceStoreError, match="FEATURE_SOURCE_FROM_FUTURE"):
            store.append(_source(), operation_id="probe.1", cutoff=NOW)
        with pytest.raises(FeatureSourceStoreError):
            store.append(_source(), operation_id="probe.1", cutoff=NOW - timedelta(seconds=1))
        assert store.status()["observation_count"] == 0


@pytest.mark.parametrize("field,value", [("symbol", "AAPL"), ("con_id", 102), ("kind", "IV_HISTORY"),
    ("source_hash", "f" * 64), ("request_hash", "f" * 64), ("basis_hash", "f" * 64),
    ("request_json", "{}"), ("basis_json", "{}"), ("available_at", "2026-09-08T14:00:00.000000+00:00"),
    ("observation_json", "not-json"), ("first_seen_at", "2026-09-08T13:59:00.000000+00:00")])
def test_self_resealed_sql_projection_corruption_fails_closed(tmp_path, field, value):
    path = tmp_path / "sources.sqlite3"
    with FeatureSourceObservationStore(path, clock=lambda: NOW + timedelta(seconds=2)) as store:
        store.append(_source(), operation_id="probe.1", cutoff=NOW)
        with sqlite3.connect(path) as connection:
            connection.row_factory = sqlite3.Row
            trigger = connection.execute("SELECT sql FROM sqlite_master WHERE name='feature_sources_no_update'").fetchone()[0]
            connection.execute("DROP TRIGGER feature_sources_no_update")
            row = dict(connection.execute("SELECT * FROM feature_source_observations").fetchone())
            row[field] = value
            row["row_hash"] = canonical_hash({key: item for key, item in row.items() if key != "row_hash"})
            connection.execute(f"UPDATE feature_source_observations SET {field}=?,row_hash=?", (value, row["row_hash"]))
            connection.execute(trigger)
        with pytest.raises(FeatureSourceStoreError, match="FEATURE_SOURCE_STORE_INVALID"):
            _read(store)


def test_commit_failure_rolls_back_complete_observation(tmp_path):
    with FeatureSourceObservationStore(tmp_path / "sources.sqlite3", clock=lambda: NOW + timedelta(seconds=2)) as store:
        connection = store._connection

        class FailOneCommit:
            fail = True

            def __getattr__(self, name):
                return getattr(connection, name)

            def execute(self, statement, *args):
                if statement == "COMMIT" and self.fail:
                    self.fail = False
                    raise sqlite3.OperationalError("fixture commit failure")
                return connection.execute(statement, *args)

        store._connection = FailOneCommit()
        with pytest.raises(FeatureSourceStoreError):
            store.append(_source(), operation_id="probe.1", cutoff=NOW)
        assert store.status()["observation_count"] == 0
        assert store.append(_source(), operation_id="probe.1", cutoff=NOW)["sequence"] == 1


def test_multiple_instances_serialize_idempotent_append(tmp_path):
    path = tmp_path / "sources.sqlite3"
    barrier = threading.Barrier(2)
    with FeatureSourceObservationStore(path, clock=lambda: NOW + timedelta(seconds=2)) as first, FeatureSourceObservationStore(path, clock=lambda: NOW + timedelta(seconds=2)) as second:
        def append(store):
            barrier.wait(timeout=3)
            return store.append(_source(), operation_id="probe.1", cutoff=NOW)
        with ThreadPoolExecutor(max_workers=2) as pool:
            jobs = [pool.submit(append, store) for store in (first, second)]
            assert sorted(job.result(timeout=5)["inserted"] for job in jobs) == [False, True]
        assert first.status()["observation_count"] == 1
        second.verify_integrity()


def test_immutable_triggers_and_schema_are_checked(tmp_path):
    path = tmp_path / "sources.sqlite3"
    with FeatureSourceObservationStore(path, clock=lambda: NOW + timedelta(seconds=2)) as store:
        store.append(_source(), operation_id="probe.1", cutoff=NOW)
        with sqlite3.connect(path) as connection:
            for sql in ("DELETE FROM feature_source_observations", "UPDATE feature_source_observations SET symbol='AAPL'"):
                with pytest.raises(sqlite3.IntegrityError):
                    connection.execute(sql)
            connection.execute("DROP INDEX feature_sources_lookup")
        with pytest.raises(FeatureSourceStoreError, match="FEATURE_SOURCE_STORE_INVALID"):
            store.verify_integrity()


def test_partial_wire_response_is_retained_without_promoting_its_basis(tmp_path):
    raw = _source()
    raw.update(status="PARTIAL", reason_codes=["FEATURE_HISTORY_INSUFFICIENT"])
    raw = _seal(raw)
    with FeatureSourceObservationStore(tmp_path / "sources.sqlite3", clock=lambda: NOW + timedelta(seconds=2)) as store:
        store.append(raw, operation_id="probe.partial", cutoff=NOW)
        assert _read(store)[0]["source"] == raw
        assert _read(store)[0]["basis_document"]["comparability_authority"] == "UNRESOLVED"


def test_verification_budget_never_returns_partial_history(tmp_path, monkeypatch):
    from options_copilot.storage import feature_sources

    with FeatureSourceObservationStore(tmp_path / "sources.sqlite3", clock=lambda: NOW + timedelta(seconds=2)) as store:
        store.append(_source(), operation_id="probe.1", cutoff=NOW)
        ticks = iter((0.0, 3.0))
        with monkeypatch.context() as patch:
            patch.setattr(feature_sources.time, "monotonic", lambda: next(ticks))
            with pytest.raises(FeatureSourceStoreError, match="FEATURE_SOURCE_STORE_VERIFICATION_BUDGET_EXCEEDED"):
                _read(store)
        assert len(_read(store)) == 1


def test_unknown_schema_is_never_downgraded_and_closed_store_refuses_reads(tmp_path):
    path = tmp_path / "sources.sqlite3"
    with sqlite3.connect(path) as connection:
        connection.execute("PRAGMA user_version=99")
    with pytest.raises(FeatureSourceStoreError, match="FEATURE_SOURCE_STORE_SCHEMA_UNSUPPORTED"):
        FeatureSourceObservationStore(path)
    with sqlite3.connect(path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 99
    store = FeatureSourceObservationStore(tmp_path / "closed.sqlite3")
    store.close()
    store.close()
    with pytest.raises(FeatureSourceStoreError, match="FEATURE_SOURCE_STORE_CLOSED"):
        store.status()


def test_insert_trigger_cannot_replace_observation_with_unverified_content(tmp_path):
    path = tmp_path / "sources.sqlite3"
    with FeatureSourceObservationStore(path, clock=lambda: NOW + timedelta(seconds=2)) as store:
        with sqlite3.connect(path) as connection:
            connection.execute("""CREATE TRIGGER test_only_substitute BEFORE INSERT ON feature_source_observations
                BEGIN
                    INSERT INTO feature_source_observations VALUES (
                        NEW.sequence, NEW.observation_id, NEW.operation_id, NEW.symbol, NEW.con_id, NEW.kind,
                        NEW.source_hash, NEW.request_hash, NEW.basis_hash, 'not-json', NEW.request_json, NEW.basis_json,
                        NEW.available_at, NEW.first_seen_at, NEW.previous_hash, NEW.row_hash
                    );
                    SELECT RAISE(IGNORE);
                END""")
        with pytest.raises(FeatureSourceStoreError, match="FEATURE_SOURCE_STORE_INVALID"):
            store.append(_source(), operation_id="probe.1", cutoff=NOW)
        assert store._connection.execute("SELECT COUNT(*) FROM feature_source_observations").fetchone()[0] == 0


def test_unexpected_insert_trigger_cannot_replace_an_old_row_before_append(tmp_path):
    path = tmp_path / "sources.sqlite3"
    with FeatureSourceObservationStore(path, clock=lambda: NOW + timedelta(seconds=2)) as store:
        store.append(_source(), operation_id="probe.1", cutoff=NOW)
        original = dict(store._connection.execute("SELECT * FROM feature_source_observations").fetchone())
        with sqlite3.connect(path) as connection:
            connection.execute("""CREATE TRIGGER test_only_replace_old BEFORE INSERT ON feature_source_observations
                BEGIN
                    INSERT OR REPLACE INTO feature_source_observations
                    SELECT sequence, observation_id, operation_id, symbol, con_id, kind,
                           source_hash, request_hash, basis_hash, 'not-json', request_json, basis_json,
                           available_at, first_seen_at, previous_hash, row_hash
                    FROM feature_source_observations WHERE sequence=1;
                END""")
        changes_before = store._connection.total_changes
        with pytest.raises(FeatureSourceStoreError, match="FEATURE_SOURCE_STORE_INVALID"):
            store.append(_source("IV_HISTORY"), operation_id="probe.2", cutoff=NOW)
        assert store._connection.total_changes == changes_before
        rows = store._connection.execute("SELECT * FROM feature_source_observations").fetchall()
        assert [dict(row) for row in rows] == [original]
