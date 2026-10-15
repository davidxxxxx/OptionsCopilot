"""Ordinary acquisition fairness must survive restart without changing eligibility."""
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
import pytest

from options_copilot.production_runtime import ProductionPipelineInputs, _CoarseCandidateRead
from options_copilot.scanner.coverage import OrdinaryScanCoverage
from options_copilot.storage.evidence import EvidenceStore


NOW = datetime(2026, 9, 8, 14, tzinfo=timezone.utc)


def test_three_slots_rotate_deferred_symbols_across_restart(tmp_path):
    path = tmp_path / "coverage.sqlite3"
    attempts = []
    for index in range(4):
        # Reconstruct both service and store, as after a real process restart.
        with EvidenceStore(path) as store:
            cursor = OrdinaryScanCoverage(store)
            inputs = ProductionPipelineInputs(
                object(), object(), object(), core_symbols=(), ordinary_coverage=cursor,
                clock=lambda: NOW + timedelta(minutes=30 * index),
            )

            def acquire(**kwargs):
                symbols = kwargs["symbols"]
                attempts.append(symbols[0])
                return _CoarseCandidateRead(
                    (), attempted_symbols=(symbols[0],), deferred_symbols=symbols[1:]
                )

            inputs._acquire_coarse_candidate_outcome = acquire
            outcome = inputs._coarse_candidate_outcome(
                scan_run_id=f"scan.{index}", slot_at=NOW,
                symbols=("AMD", "GLD", "XOM"),
            )
            assert outcome.reason_codes == ()
            assert len(outcome.attempted_symbols) == 1
            assert outcome.coverage_evidence["affects_pacing_limits"] is False
            assert store.verify_integrity()
    assert attempts == ["AMD", "GLD", "XOM", "AMD"]


def test_cursor_never_adds_removed_names_or_changes_manual_order(tmp_path):
    with EvidenceStore(tmp_path / "coverage.sqlite3") as store:
        cursor = OrdinaryScanCoverage(store)
        cursor.record(scan_run_id="scan.1", ordered_symbols=("AMD", "GLD", "XOM"),
                      visited_symbols=("AMD",), observed_at=NOW)
        assert cursor.arrange(("AMD", "SPY")) == ("AMD", "SPY")
        inputs = ProductionPipelineInputs(
            object(), object(), object(), core_symbols=("AMD", "GLD"),
            ordinary_coverage=cursor, clock=lambda: NOW,
        )
        seen = []
        inputs._acquire_coarse_candidate_outcome = lambda **kw: (
            seen.append(kw["symbols"]) or _CoarseCandidateRead(())
        )
        with inputs.manual_core_only():
            inputs._coarse_candidate_outcome(scan_run_id="manual.1", slot_at=NOW,
                                             symbols=("AMD", "GLD"))
        assert seen == [("AMD", "GLD")]
        assert len(store.query()) == 1


def test_invalid_coverage_fails_before_any_broker_read(tmp_path):
    with EvidenceStore(tmp_path / "coverage.sqlite3") as store:
        cursor = OrdinaryScanCoverage(store)
        cursor.record(scan_run_id="scan.1", ordered_symbols=("AMD", "GLD"),
                      visited_symbols=("AMD",), observed_at=NOW)
        def corrupt():
            raise ValueError("integrity mismatch")
        store.assert_integrity = corrupt
        inputs = ProductionPipelineInputs(
            object(), object(), object(), core_symbols=(), ordinary_coverage=cursor,
        )
        inputs._acquire_coarse_candidate_outcome = lambda **kw: pytest.fail("broker read")
        outcome = inputs._coarse_candidate_outcome(scan_run_id="scan.2", slot_at=NOW,
                                                  symbols=("AMD", "GLD"))
        assert outcome.reason_codes == ("ORDINARY_COVERAGE_EVIDENCE_INVALID",)


def test_append_failure_drops_candidates(tmp_path):
    with EvidenceStore(tmp_path / "coverage.sqlite3") as store:
        cursor = OrdinaryScanCoverage(store)
        inputs = ProductionPipelineInputs(
            object(), object(), object(), core_symbols=(), ordinary_coverage=cursor,
        )
        inputs._acquire_coarse_candidate_outcome = lambda **kw: _CoarseCandidateRead(
            ({"symbol": "AMD"},), attempted_symbols=("AMD",)
        )
        def fail(**kw):
            raise OSError("disk full")
        cursor.record = fail
        outcome = inputs._coarse_candidate_outcome(scan_run_id="scan.2", slot_at=NOW,
                                                  symbols=("AMD", "GLD"))
        assert outcome.candidates == ()
        assert outcome.reason_codes == ("ORDINARY_COVERAGE_APPEND_FAILED",)


def test_real_preflight_denial_preserves_unvisited_tail_across_restart(tmp_path):
    class DeniedPacing:
        ready = True
        def decision(self, request_class):
            assert request_class == "secdef"
            return SimpleNamespace(allowed=False, reason="REQUEST_WINDOW_EXHAUSTED")

    class Gateway:
        def option_expirations(self, *args, **kwargs):
            pytest.fail("denied lease must not contact broker")

    attempts = []
    for index in range(3):
        with EvidenceStore(tmp_path / "denied.sqlite3") as store:
            inputs = ProductionPipelineInputs(
                Gateway(), DeniedPacing(), object(), core_symbols=(),
                ordinary_coverage=OrdinaryScanCoverage(store), clock=lambda: NOW,
            )
            outcome = inputs._coarse_candidate_outcome(
                scan_run_id=f"denied.{index}", slot_at=NOW,
                symbols=("AMD", "GLD", "XOM"),
                equity_theses={"rows": tuple({"symbol": symbol, "direction_label": "BULLISH",
                                             "uncertainty": "0.2"}
                                            for symbol in ("AMD", "GLD", "XOM"))},
            )
            assert "OPTIONABILITY_PACING_DENIED" in outcome.reason_codes
            assert len(outcome.attempted_symbols) == 1
            assert len(outcome.deferred_symbols) == 2
            assert set(outcome.attempted_symbols).isdisjoint(outcome.deferred_symbols)
            attempts.extend(outcome.attempted_symbols)
    assert attempts == ["AMD", "GLD", "XOM"]
