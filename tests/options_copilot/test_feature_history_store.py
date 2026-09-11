"""Append-only history cache regressions with offline, non-authoritative data."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import timedelta
from decimal import Decimal
import json
import sqlite3
import threading

import pytest

from test_feature_contracts import NOW, batch, basis, point, request
from options_copilot.analytics.feature_contracts import FeatureContractError, FeatureHistoryBatch
from options_copilot.storage.canonical import canonical_hash, canonical_json
from options_copilot.storage.feature_inputs import FeatureInputStore


def test_append_idempotency_reopen_and_first_seen_time(tmp_path):
    path = tmp_path / "features.sqlite3"
    original = batch()
    with FeatureInputStore(path, clock=lambda: NOW) as store:
        first = store.append(original)
        assert first["inserted"] is True
        assert store.append(original)["inserted"] is False
        assert store.count() == 1
        assert store.verify_integrity() is None
    with FeatureInputStore(path, clock=lambda: NOW + timedelta(hours=1)) as store:
        assert store.append(original)["inserted"] is False
        loaded = store.read(con_id=101, request_hashes=(original.request.contract_hash,),
                            basis_hash=original.basis.contract_hash, cutoff=NOW)
        assert loaded[0].batch_hash == original.batch_hash


def test_old_market_dates_imported_now_are_not_available_to_old_decisions(tmp_path):
    acquired = NOW + timedelta(hours=1)
    original = batch()
    with FeatureInputStore(tmp_path / "features.sqlite3", clock=lambda: acquired) as store:
        store.append(original)
        args = dict(con_id=101, request_hashes=(original.request.contract_hash,), basis_hash=original.basis.contract_hash)
        assert store.read(**args, cutoff=NOW) == ()
        assert store.read(**args, cutoff=acquired) == (original,)


def test_revision_is_append_only_and_identity_requires_both_hashes(tmp_path):
    original = batch()
    updated = batch(points=(point(close=Decimal(107)),), source_revision_hash="d" * 64)
    other_basis = batch(basis=basis(methodology_version="2"))
    other_request = batch(request=request(use_rth=False))
    with FeatureInputStore(tmp_path / "features.sqlite3", clock=lambda: NOW) as store:
        for row in (original, updated, other_basis, other_request):
            store.append(row)
        found = store.read(con_id=101, request_hashes=(original.request.contract_hash,),
                           basis_hash=original.basis.contract_hash, cutoff=NOW)
        assert tuple(row.batch_hash for row in found) == (original.batch_hash, updated.batch_hash)
        assert store.read(con_id=102, request_hashes=(original.request.contract_hash,),
                          basis_hash=original.basis.contract_hash, cutoff=NOW) == ()
        assert store.count() == 4


def test_same_request_and_source_revision_cannot_change_payload(tmp_path):
    with FeatureInputStore(tmp_path / "features.sqlite3", clock=lambda: NOW) as store:
        store.append(batch())
        with pytest.raises(FeatureContractError, match="FEATURE_HISTORY_REVISION_CONFLICT"):
            store.append(batch(points=(point(close=Decimal(999)),)))
        assert store.count() == 1


def test_future_batch_is_rejected_without_writing(tmp_path):
    with FeatureInputStore(tmp_path / "features.sqlite3", clock=lambda: NOW) as store:
        with pytest.raises(FeatureContractError, match="FEATURE_BATCH_FROM_FUTURE"):
            store.append(batch(available_at=NOW + timedelta(seconds=1)))
        assert store.count() == 0


def test_sql_triggers_prevent_rewrite_and_delete(tmp_path):
    path = tmp_path / "features.sqlite3"
    with FeatureInputStore(path, clock=lambda: NOW) as store:
        store.append(batch())
        with sqlite3.connect(path) as connection:
            for sql in ("DELETE FROM feature_history", "UPDATE feature_history SET batch_hash='x'"):
                with pytest.raises(sqlite3.IntegrityError):
                    connection.execute(sql)
        store.verify_integrity()


def test_tamper_fails_closed_on_read(tmp_path):
    path = tmp_path / "features.sqlite3"
    original = batch()
    with FeatureInputStore(path, clock=lambda: NOW) as store:
        store.append(original)
        with sqlite3.connect(path) as connection:
            connection.execute("DROP TRIGGER feature_history_no_update")
            connection.execute("UPDATE feature_history SET batch_hash=?", ("f" * 64,))
        with pytest.raises(FeatureContractError, match="FEATURE_HISTORY_STORE_INVALID"):
            store.read(con_id=101, request_hashes=(original.request.contract_hash,),
                       basis_hash=original.basis.contract_hash, cutoff=NOW)


def test_unknown_schema_refused_without_downgrade(tmp_path):
    path = tmp_path / "newer.sqlite3"
    with sqlite3.connect(path) as connection:
        connection.execute("PRAGMA user_version=99")
    with pytest.raises(FeatureContractError, match="FEATURE_HISTORY_SCHEMA_UNSUPPORTED"):
        FeatureInputStore(path)
    with sqlite3.connect(path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 99


@pytest.mark.parametrize("changes", ({"con_id": True}, {"request_hashes": ()},
    {"request_hashes": ("invalid",)}, {"basis_hash": "x"}, {"cutoff": NOW.replace(tzinfo=None)}))
def test_queries_are_explicit_and_bounded(tmp_path, changes):
    row = batch()
    args = dict(con_id=101, request_hashes=(row.request.contract_hash,), basis_hash=row.basis.contract_hash, cutoff=NOW)
    args.update(changes)
    with FeatureInputStore(tmp_path / "features.sqlite3") as store:
        with pytest.raises((FeatureContractError, ValueError)):
            store.read(**args)


def test_two_store_writers_serialize_into_one_valid_hash_chain(tmp_path):
    path = tmp_path / "features.sqlite3"
    barrier = threading.Barrier(2)
    with FeatureInputStore(path, clock=lambda: NOW) as first, FeatureInputStore(path, clock=lambda: NOW) as second:
        def append(store, row):
            barrier.wait(timeout=3)
            return store.append(row)
        with ThreadPoolExecutor(max_workers=2) as pool:
            jobs = (pool.submit(append, first, batch()),
                    pool.submit(append, second, batch(source_revision_hash="d" * 64)))
            assert {job.result(timeout=5)["sequence"] for job in jobs} == {1, 2}
        assert first.count() == 2
        first.verify_integrity()
        second.verify_integrity()


def test_interrupted_insert_rolls_back_without_partial_batch(tmp_path):
    path = tmp_path / "features.sqlite3"
    with FeatureInputStore(path, clock=lambda: NOW) as store:
        with sqlite3.connect(path) as connection:
            connection.execute("CREATE TRIGGER test_only_abort BEFORE INSERT ON feature_history BEGIN SELECT RAISE(ABORT, 'fixture interruption'); END")
        with pytest.raises(sqlite3.IntegrityError, match="fixture interruption"):
            store.append(batch())
        assert store.count() == 0
        with sqlite3.connect(path) as connection:
            connection.execute("DROP TRIGGER test_only_abort")
        assert store.append(batch())["sequence"] == 1
        store.verify_integrity()


def test_existing_unversioned_legacy_content_is_not_relabelled(tmp_path):
    path = tmp_path / "legacy.sqlite3"
    with sqlite3.connect(path) as connection:
        connection.execute("CREATE TABLE legacy (value TEXT)")
        connection.execute("INSERT INTO legacy VALUES ('retained')")
    with pytest.raises(FeatureContractError, match="LEGACY_BASIS_UNRESOLVED"):
        FeatureInputStore(path)
    with sqlite3.connect(path) as connection:
        assert connection.execute("SELECT value FROM legacy").fetchall() == [("retained",)]
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 0


def test_simultaneous_initial_open_does_not_misclassify_new_store_as_legacy(tmp_path):
    path = tmp_path / "features.sqlite3"
    barrier = threading.Barrier(8)
    def open_store():
        barrier.wait(timeout=3)
        with FeatureInputStore(path) as store:
            return store.count()
    with ThreadPoolExecutor(max_workers=8) as pool:
        jobs = [pool.submit(open_store) for _ in range(8)]
        assert [job.result(timeout=10) for job in jobs] == [0] * 8


def test_verification_budget_fails_closed_without_partial_reads(tmp_path, monkeypatch):
    from options_copilot.storage import feature_inputs
    row = batch()
    with FeatureInputStore(tmp_path / "features.sqlite3", clock=lambda: NOW) as store:
        store.append(row)
        ticks = iter((0.0, 3.0))
        monkeypatch.setattr(feature_inputs.time, "monotonic", lambda: next(ticks))
        with pytest.raises(FeatureContractError, match="^FEATURE_HISTORY_VERIFICATION_BUDGET_EXCEEDED$"):
            store.read(con_id=101, request_hashes=(row.request.contract_hash,), basis_hash=row.basis.contract_hash, cutoff=NOW)


def test_same_named_noop_trigger_cannot_fake_immutability(tmp_path):
    path = tmp_path / "features.sqlite3"
    with FeatureInputStore(path, clock=lambda: NOW) as store:
        with sqlite3.connect(path) as connection:
            connection.execute("DROP TRIGGER feature_history_no_update")
            connection.execute("CREATE TRIGGER feature_history_no_update BEFORE UPDATE ON feature_history BEGIN SELECT 1; END")
        with pytest.raises(FeatureContractError, match="FEATURE_HISTORY_STORE_INVALID"):
            store.append(batch())


def test_request_fingerprint_is_part_of_idempotency(tmp_path):
    with FeatureInputStore(tmp_path / "features.sqlite3", clock=lambda: NOW) as store:
        store.append(batch())
        assert store.append(batch(request_fingerprint="f" * 64))["inserted"] is True
        assert store.count() == 2


def test_missing_lookup_index_is_not_accepted_as_integrity_valid(tmp_path):
    path = tmp_path / "features.sqlite3"
    with FeatureInputStore(path, clock=lambda: NOW) as store:
        store.append(batch())
        with sqlite3.connect(path) as connection:
            connection.execute("DROP INDEX feature_history_lookup")
        with pytest.raises(FeatureContractError, match="FEATURE_HISTORY_STORE_INVALID"):
            store.verify_integrity()


def _query(row):
    return dict(con_id=row.request.con_id, request_hashes=(row.request.contract_hash,),
                basis_hash=row.basis.contract_hash, cutoff=NOW)


def _tamper_old_row(path, *, reseal=False):
    with sqlite3.connect(path) as connection:
        connection.row_factory = sqlite3.Row
        trigger = connection.execute(
            "SELECT sql FROM sqlite_master WHERE name='feature_history_no_update'"
        ).fetchone()[0]
        connection.execute("DROP TRIGGER feature_history_no_update")
        connection.execute("UPDATE feature_history SET batch_hash=? WHERE sequence=1", ("f" * 64,))
        if reseal:
            previous = "0" * 64
            for stored in connection.execute("SELECT * FROM feature_history ORDER BY sequence").fetchall():
                row = dict(stored)
                if row["sequence"] == 1:
                    changed = batch(points=(point(close=Decimal(999)),))
                    row["batch_json"] = json.dumps(changed.as_dict())
                    row["batch_hash"] = changed.batch_hash
                row["previous_hash"] = previous
                row["row_hash"] = canonical_hash({key: value for key, value in row.items() if key != "row_hash"})
                connection.execute(
                    "UPDATE feature_history SET batch_json=?,batch_hash=?,previous_hash=?,row_hash=? WHERE sequence=?",
                    (row["batch_json"], row["batch_hash"], previous, row["row_hash"], row["sequence"]),
                )
                previous = row["row_hash"]
        connection.execute(trigger)


def test_unchanged_store_work_decodes_only_new_or_returned_batches(tmp_path, monkeypatch):
    with FeatureInputStore(tmp_path / "features.sqlite3", clock=lambda: NOW) as store:
        for index in range(30):
            store.append(batch(source_revision_hash=f"{index:064x}"))
        decoded = []
        from_document = FeatureHistoryBatch.from_document

        def count_decode(cls, document):
            decoded.append(document["batch_hash"])
            return from_document(document)

        monkeypatch.setattr(FeatureHistoryBatch, "from_document", classmethod(count_decode))
        added = batch(request=request(con_id=102), source_revision_hash="f" * 64)
        assert store.append(added)["inserted"] is True
        assert store.read(**_query(added)) == (added,)
        assert store.append(added)["inserted"] is False
        assert store.count() == 31
        # One append-boundary decode, one returned-row decode, and both checks
        # for an idempotent append. No unrelated historical JSON is decoded.
        assert decoded == [added.batch_hash] * 4
        decoded.clear()
        store.verify_integrity()
        assert len(decoded) == 31


@pytest.mark.parametrize("reseal", [False, True])
def test_external_old_row_tamper_rechecks_prefix_even_with_restored_triggers(tmp_path, reseal):
    path = tmp_path / "features.sqlite3"
    last = batch(request=request(con_id=102), source_revision_hash="d" * 64)
    with FeatureInputStore(path, clock=lambda: NOW) as store:
        store.append(batch())
        store.append(last)
        _tamper_old_row(path, reseal=reseal)
        with pytest.raises(FeatureContractError, match="FEATURE_HISTORY_STORE_INVALID"):
            store.read(**_query(last))
        with pytest.raises(FeatureContractError, match="FEATURE_HISTORY_STORE_INVALID"):
            store.append(batch(source_revision_hash="e" * 64))


def test_checkpoint_is_not_advanced_when_commit_rolls_back(tmp_path):
    with FeatureInputStore(tmp_path / "features.sqlite3", clock=lambda: NOW) as store:
        store.append(batch())
        verified = store._verified_checkpoint
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
        with pytest.raises(sqlite3.OperationalError, match="fixture commit failure"):
            store.append(batch(source_revision_hash="d" * 64))
        assert store._verified_checkpoint == verified
        assert store.count() == 1
        assert store.append(batch(source_revision_hash="e" * 64))["sequence"] == 2
        store.verify_integrity()


def test_external_commit_during_read_cannot_certify_an_unseen_database_version(tmp_path, monkeypatch):
    path = tmp_path / "features.sqlite3"
    last = batch(request=request(con_id=102), source_revision_hash="d" * 64)
    with FeatureInputStore(path, clock=lambda: NOW) as store:
        store.append(batch())
        store.append(last)
        from_document = FeatureHistoryBatch.from_document
        changed = False

        def tamper_while_reading(cls, document):
            nonlocal changed
            if not changed and document["batch_hash"] == last.batch_hash:
                changed = True
                _tamper_old_row(path)
            return from_document(document)

        monkeypatch.setattr(FeatureHistoryBatch, "from_document", classmethod(tamper_while_reading))
        # This transaction may return its already verified old SQLite snapshot.
        assert store.read(**_query(last)) == (last,)
        assert changed is True
        assert store._verified_checkpoint.state is None
        # Its checkpoint must not adopt the writer's newer data_version: that
        # version contains a tampered non-returned row outside the old snapshot.
        with pytest.raises(FeatureContractError, match="FEATURE_HISTORY_STORE_INVALID"):
            store.read(**_query(last))


def test_external_append_requires_full_reaudit_before_reusing_checkpoint(tmp_path, monkeypatch):
    path = tmp_path / "features.sqlite3"
    original = batch()
    with FeatureInputStore(path, clock=lambda: NOW) as first, FeatureInputStore(path, clock=lambda: NOW) as second:
        first.append(original)
        second.append(batch(source_revision_hash="d" * 64))
        audited = []
        verify = first._verify

        def count_audit(*args, **kwargs):
            audited.append(True)
            return verify(*args, **kwargs)

        monkeypatch.setattr(first, "_verify", count_audit)
        assert len(first.read(**_query(original))) == 2
        assert len(first.read(**_query(original))) == 2
        assert first.count() == 2
        assert audited == [True]
        first.verify_integrity()
        assert audited == [True, True]


def test_same_connection_changes_cannot_hide_behind_unchanged_data_and_schema_versions(tmp_path):
    last = batch(request=request(con_id=102), source_revision_hash="d" * 64)
    with FeatureInputStore(tmp_path / "features.sqlite3", clock=lambda: NOW) as store:
        store.append(batch())
        store.append(last)
        connection = store._connection
        data_version = connection.execute("PRAGMA data_version").fetchone()[0]
        schema_version = connection.execute("PRAGMA schema_version").fetchone()[0]
        trigger = connection.execute(
            "SELECT sql FROM sqlite_master WHERE name='feature_history_no_update'"
        ).fetchone()[0]
        connection.execute("DROP TRIGGER feature_history_no_update")
        connection.execute("UPDATE feature_history SET batch_hash=? WHERE sequence=1", ("f" * 64,))
        connection.execute(trigger)
        connection.execute(f"PRAGMA schema_version={schema_version}")
        assert connection.execute("PRAGMA data_version").fetchone()[0] == data_version
        assert connection.execute("PRAGMA schema_version").fetchone()[0] == schema_version
        with pytest.raises(FeatureContractError, match="FEATURE_HISTORY_STORE_INVALID"):
            store.read(**_query(last))


def test_truncating_a_previously_verified_tail_is_rejected(tmp_path):
    path = tmp_path / "features.sqlite3"
    with FeatureInputStore(path, clock=lambda: NOW) as store:
        store.append(batch())
        store.append(batch(source_revision_hash="d" * 64))
        verified = store._verified_checkpoint
        with sqlite3.connect(path) as connection:
            trigger = connection.execute(
                "SELECT sql FROM sqlite_master WHERE name='feature_history_no_delete'"
            ).fetchone()[0]
            connection.execute("DROP TRIGGER feature_history_no_delete")
            connection.execute("DELETE FROM feature_history WHERE sequence=2")
            connection.execute(trigger)
        with pytest.raises(FeatureContractError, match="FEATURE_HISTORY_STORE_INVALID"):
            store.count()
        assert store._verified_checkpoint == verified


def test_reopen_always_reconstructs_checkpoint_from_complete_audit(tmp_path, monkeypatch):
    path = tmp_path / "features.sqlite3"
    with FeatureInputStore(path, clock=lambda: NOW) as store:
        for index in range(4):
            store.append(batch(source_revision_hash=f"{index:064x}"))
    decoded = []
    from_document = FeatureHistoryBatch.from_document

    def count_decode(cls, document):
        decoded.append(document["batch_hash"])
        return from_document(document)

    monkeypatch.setattr(FeatureHistoryBatch, "from_document", classmethod(count_decode))
    with FeatureInputStore(path, clock=lambda: NOW) as store:
        assert len(decoded) == 4
        assert store.count() == 4
        assert len(decoded) == 4
        store.verify_integrity()
        assert len(decoded) == 8


@pytest.mark.parametrize("field,value", [
    ("identity", "f" * 64), ("con_id", 102), ("request_hash", "f" * 64),
    ("basis_hash", "f" * 64), ("batch_hash", "f" * 64),
    ("first_seen_at", (NOW - timedelta(seconds=1)).isoformat()),
    ("previous_hash", "f" * 64), ("batch_json", "not-json"), ("row_hash", "f" * 64),
])
def test_returned_row_validation_is_independent_of_a_self_resealed_row_hash(tmp_path, field, value):
    with FeatureInputStore(tmp_path / "features.sqlite3", clock=lambda: NOW) as store:
        store.append(batch())
        row = dict(store._connection.execute("SELECT * FROM feature_history").fetchone())
        row[field] = value
        if field != "row_hash":
            row["row_hash"] = canonical_hash({key: item for key, item in row.items() if key != "row_hash"})
        with pytest.raises(FeatureContractError, match="FEATURE_HISTORY_STORE_INVALID"):
            store._decode_row(row)


def test_insert_trigger_cannot_substitute_unverified_content_behind_expected_head_hash(tmp_path):
    path = tmp_path / "features.sqlite3"
    with FeatureInputStore(path, clock=lambda: NOW) as store:
        with sqlite3.connect(path) as connection:
            connection.execute("""CREATE TRIGGER test_only_substitute BEFORE INSERT ON feature_history
                BEGIN
                    INSERT INTO feature_history VALUES (
                        NEW.sequence, NEW.identity, NEW.con_id, NEW.request_hash, NEW.basis_hash,
                        NEW.batch_hash, 'not-json', NEW.first_seen_at, NEW.previous_hash, NEW.row_hash
                    );
                    SELECT RAISE(IGNORE);
                END""")
        with pytest.raises(FeatureContractError, match="FEATURE_HISTORY_STORE_INVALID"):
            store.append(batch())
        assert store._connection.execute("SELECT COUNT(*) FROM feature_history").fetchone()[0] == 0
        assert store._verified_checkpoint.sequence == 0


def test_replacement_table_without_unique_identity_cannot_pass_verified_anchor(tmp_path):
    path = tmp_path / "features.sqlite3"
    original = batch()
    changed = batch(points=(point(close=Decimal(999)),))
    with FeatureInputStore(path, clock=lambda: NOW) as store:
        store.append(original)
        checkpoint = store._verified_checkpoint
        with closing(sqlite3.connect(path)) as connection, connection:
            connection.row_factory = sqlite3.Row
            saved = dict(connection.execute("SELECT * FROM feature_history").fetchone())
            table_sql = connection.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='feature_history'"
            ).fetchone()[0]
            index_sql = connection.execute(
                "SELECT sql FROM sqlite_master WHERE name='feature_history_lookup'"
            ).fetchone()[0]
            triggers = tuple(connection.execute(
                "SELECT name,sql FROM sqlite_master WHERE type='trigger'"
            ))
            for name, _sql in triggers:
                connection.execute(f"DROP TRIGGER {name}")
            connection.execute("DROP INDEX feature_history_lookup")
            connection.execute("ALTER TABLE feature_history RENAME TO old_history")
            replacement_sql = table_sql.replace(
                "identity TEXT NOT NULL UNIQUE", "identity TEXT NOT NULL"
            )
            assert replacement_sql != table_sql
            connection.execute(replacement_sql)
            connection.execute("INSERT INTO feature_history SELECT * FROM old_history")
            connection.execute("DROP TABLE old_history")
            connection.execute(index_sql)
            for _name, sql in triggers:
                connection.execute(sql)
            # Retain the entire trusted prefix, then append a conflicting source
            # revision that only the removed UNIQUE constraint would prevent.
            saved.update(sequence=2, batch_json=canonical_json(changed.as_dict()),
                         batch_hash=changed.batch_hash, previous_hash=checkpoint.row_hash)
            saved["row_hash"] = canonical_hash({
                key: value for key, value in saved.items() if key != "row_hash"
            })
            connection.execute(
                "INSERT INTO feature_history VALUES(?,?,?,?,?,?,?,?,?,?)",
                tuple(saved.values()),
            )
            assert connection.execute(
                "SELECT row_hash FROM feature_history WHERE sequence=1"
            ).fetchone()[0] == checkpoint.row_hash
        for operation in (
            lambda: store.read(**_query(original)),
            lambda: store.append(changed),
            store.count,
            store.verify_integrity,
        ):
            with pytest.raises(FeatureContractError, match="^FEATURE_HISTORY_STORE_INVALID$"):
                operation()
            assert store._verified_checkpoint == checkpoint
    with pytest.raises(FeatureContractError, match="^FEATURE_HISTORY_STORE_INVALID$"):
        FeatureInputStore(path, clock=lambda: NOW)


def test_valid_v1_schema_and_history_remain_unchanged_on_reopen(tmp_path, monkeypatch):
    from options_copilot.storage import feature_inputs

    path = tmp_path / "features.sqlite3"
    legacy_table_sql = """CREATE TABLE feature_history (
                        sequence INTEGER PRIMARY KEY, identity TEXT NOT NULL UNIQUE,
                        con_id INTEGER NOT NULL, request_hash TEXT NOT NULL, basis_hash TEXT NOT NULL,
                        batch_hash TEXT NOT NULL, batch_json TEXT NOT NULL, first_seen_at TEXT NOT NULL,
                        previous_hash TEXT NOT NULL, row_hash TEXT NOT NULL UNIQUE
                    )"""
    with monkeypatch.context() as legacy:
        legacy.setattr(feature_inputs, "_TABLE_SQL", legacy_table_sql)
        with FeatureInputStore(path, clock=lambda: NOW) as store:
            original = batch()
            store.append(original)
            before_schema = tuple(tuple(row) for row in store._connection.execute(
                "SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name"
            ))
            before_rows = tuple(tuple(row) for row in store._connection.execute(
                "SELECT * FROM feature_history ORDER BY sequence"
            ))
    with FeatureInputStore(path, clock=lambda: NOW + timedelta(hours=1)) as store:
        assert store._connection.execute("PRAGMA user_version").fetchone()[0] == 1
        assert store.read(**_query(original)) == (original,)
        assert tuple(tuple(row) for row in store._connection.execute(
            "SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name"
        )) == before_schema
        assert tuple(tuple(row) for row in store._connection.execute(
            "SELECT * FROM feature_history ORDER BY sequence"
        )) == before_rows
