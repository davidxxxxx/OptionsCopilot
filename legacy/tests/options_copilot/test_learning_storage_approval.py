from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest
from unittest.mock import patch

from options_copilot.approval import (
    ActiveApprovalExists,
    APPROVAL_TTL_SECONDS,
    NonceReplayError,
    ProposalApprovalStore,
    hash_proposal,
)
from options_copilot.learning import (
    LearningGovernance,
    LearningStage,
    PRODUCTION_APPROVAL_MARKER,
    PromotionBlocked,
)
from options_copilot.learning.outcomes import (
    IndependenceSpecValidationError,
    VerifiedIndependenceSpec,
    verify_independence_spec,
)
from options_copilot.storage import (
    DecisionHashCollision,
    DecisionIdentityConflict,
    DecisionKind,
    DecisionLedger,
    DecisionRecord,
    PointInTime,
    canonical_hash,
)


BASE = datetime(2026, 8, 3, 1, 2, 3, tzinfo=timezone.utc)
INITIAL_POLICY_HASH = (
    "b5d969d13fc624cdb2c49b3a78ce0fa34119bedd95f51db7b25ba9c977f55a3c"
)
EXECUTION_COST_HASH = (
    "d5922fa9808c4f1e7150362026037d17fb550d6f64daf8f86985042053e5428b"
)


class MutableClock:
    def __init__(self, current: datetime) -> None:
        self.current = current

    def __call__(self) -> datetime:
        return self.current

    def set(self, current: datetime) -> None:
        self.current = current


def timing(offset_seconds: int = 0) -> PointInTime:
    asof = BASE + timedelta(seconds=offset_seconds)
    return PointInTime(
        published=asof - timedelta(seconds=3),
        first_seen=asof - timedelta(seconds=2),
        ingested=asof - timedelta(seconds=1),
        asof=asof,
    )


def decision(
    decision_id: str,
    kind: DecisionKind,
    *,
    offset_seconds: int = 0,
    payload: dict[str, object] | None = None,
    related: str | None = None,
) -> DecisionRecord:
    return DecisionRecord(
        decision_id=decision_id,
        kind=kind,
        scenario_id=f"scenario-{offset_seconds}",
        source="unit-test",
        model_version="baseline-v1",
        timing=timing(offset_seconds),
        payload=payload or {"rank": offset_seconds},
        related_decision_id=related,
    )


def proposal() -> dict[str, object]:
    return {
        "underlying": "SPY",
        "strategy": "DEBIT_CALL_SPREAD",
        "legs": [
            {
                "contract": "SPY-20260918-C-500",
                "side": "BUY",
                "quantity": 1,
                "strike": 500,
                "ask": 5.10,
            },
            {
                "contract": "SPY-20260918-C-510",
                "side": "SELL",
                "quantity": 1,
                "strike": 510,
                "bid": 4.10,
            },
        ],
        "risk": {
            "max_loss_usd": 100.0,
            "max_profit_usd": 900.0,
            "defined_risk": True,
        },
        "pricing": {"net_debit_usd": 100.0, "quoted_at": BASE.isoformat()},
    }


class DecisionLedgerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_wal_full_required_record_types_query_and_export(self) -> None:
        path = self.root / "ledger.db"
        kinds = (
            DecisionKind.CANDIDATE,
            DecisionKind.REJECTED,
            DecisionKind.RANDOM_CONTROL,
            DecisionKind.ACTUAL_TRADE,
            DecisionKind.OUTCOME_LABEL,
        )
        with DecisionLedger(path, clock=lambda: BASE) as ledger:
            self.assertEqual("wal", ledger.journal_mode)
            self.assertEqual("full", ledger.synchronous)
            for index, kind in enumerate(kinds):
                ledger.append(
                    decision(
                        f"decision-{index}",
                        kind,
                        offset_seconds=index,
                        related="decision-0" if index else None,
                    )
                )
            self.assertEqual(5, ledger.count())
            self.assertEqual(
                [DecisionKind.REJECTED],
                [row.record.kind for row in ledger.query(kinds=["rejected_candidate"])],
            )
            output = self.root / "exports" / "ledger.jsonl"
            self.assertEqual(5, ledger.export_jsonl(output))
            self.assertTrue(ledger.verify_integrity())

        rows = [json.loads(line) for line in output.read_text(encoding="utf-8").splitlines()]
        self.assertEqual([1, 2, 3, 4, 5], [row["sequence"] for row in rows])
        self.assertEqual(
            {"first_seen", "published", "ingested", "asof"},
            {name for name in rows[0] if name in {"first_seen", "published", "ingested", "asof"}},
        )
        connection = sqlite3.connect(path)
        try:
            columns = {
                str(row[1]) for row in connection.execute("PRAGMA table_info(decision_records)")
            }
        finally:
            connection.close()
        self.assertTrue({"first_seen", "published", "ingested", "asof"} <= columns)

    def test_integrity_verification_is_reused_until_ledger_changes(self) -> None:
        with DecisionLedger(self.root / "cached-integrity.db", clock=lambda: BASE) as ledger:
            ledger.append(decision("one", DecisionKind.CANDIDATE))
            uncached = ledger._assert_integrity_uncached
            calls = 0

            def counted_uncached() -> None:
                nonlocal calls
                calls += 1
                uncached()

            ledger._assert_integrity_uncached = counted_uncached  # type: ignore[method-assign]
            ledger._verified_integrity_token = None

            self.assertTrue(ledger.verify_integrity())
            self.assertTrue(ledger.verify_integrity())
            self.assertEqual(1, calls)

            ledger.append(
                decision("two", DecisionKind.REJECTED, offset_seconds=1)
            )
            self.assertTrue(ledger.verify_integrity())
            self.assertEqual(2, calls)

    def test_point_in_time_is_timezone_aware_and_causally_ordered(self) -> None:
        naive = datetime(2026, 1, 1)
        with self.assertRaisesRegex(ValueError, "timezone-aware"):
            PointInTime(naive, naive, naive, naive)
        with self.assertRaisesRegex(ValueError, "published cannot be after first_seen"):
            PointInTime(
                published=BASE,
                first_seen=BASE - timedelta(seconds=1),
                ingested=BASE,
                asof=BASE,
            )
        with self.assertRaisesRegex(ValueError, "ingested cannot be after asof"):
            PointInTime(
                published=BASE,
                first_seen=BASE,
                ingested=BASE + timedelta(seconds=1),
                asof=BASE,
            )

    def test_exact_retry_is_idempotent_and_changed_id_content_is_rejected(self) -> None:
        with DecisionLedger(self.root / "ledger.db", clock=lambda: BASE) as ledger:
            original = decision("same-id", DecisionKind.CANDIDATE)
            first = ledger.append(original)
            retry = ledger.append(original)
            self.assertTrue(first.inserted)
            self.assertFalse(retry.inserted)
            self.assertEqual(first.decision.content_hash, retry.decision.content_hash)
            self.assertEqual(1, ledger.count())
            with self.assertRaises(DecisionIdentityConflict):
                ledger.append(
                    decision("same-id", DecisionKind.CANDIDATE, payload={"changed": True})
                )

    def test_digest_collision_is_detected_before_unique_constraint(self) -> None:
        constant_digest = lambda _: "a" * 64
        with DecisionLedger(
            self.root / "collision.db",
            clock=lambda: BASE,
            content_hasher=constant_digest,
        ) as ledger:
            ledger.append(decision("first", DecisionKind.CANDIDATE))
            with self.assertRaises(DecisionHashCollision):
                ledger.append(
                    decision(
                        "second",
                        DecisionKind.REJECTED,
                        offset_seconds=1,
                        payload={"different": True},
                    )
                )

    def test_sql_update_and_delete_are_forbidden_and_chain_survives_reopen(self) -> None:
        path = self.root / "immutable.db"
        with DecisionLedger(path, clock=lambda: BASE) as ledger:
            ledger.append(decision("one", DecisionKind.CANDIDATE))
            ledger.append(decision("two", DecisionKind.REJECTED, offset_seconds=1))
        connection = sqlite3.connect(path)
        try:
            with self.assertRaisesRegex(sqlite3.IntegrityError, "update forbidden"):
                connection.execute(
                    "UPDATE decision_records SET source='tampered' WHERE decision_id='one'"
                )
            with self.assertRaisesRegex(sqlite3.IntegrityError, "delete forbidden"):
                connection.execute("DELETE FROM decision_records WHERE decision_id='one'")
        finally:
            connection.close()
        with DecisionLedger(path) as reopened:
            rows = reopened.query()
            self.assertEqual(rows[0].chain_hash, rows[1].previous_hash)
            self.assertTrue(reopened.verify_integrity())


class LearningGovernanceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / "learning.db"
        self.governance = LearningGovernance(self.path)
        self.governance.register_champion("champion-v1", "a" * 64, registered_at=BASE)
        self.governance.register_challenger(
            "challenger-v2",
            "b" * 64,
            parent_version="champion-v1",
            registered_at=BASE + timedelta(seconds=1),
        )

    def tearDown(self) -> None:
        self.governance.close()
        self.temp.cleanup()

    def _add_scenarios(self, count: int) -> None:
        for index in range(count):
            self.governance.record_shadow_result(
                "challenger-v2",
                f"event-family-{index}",
                outcome="counterfactual_win" if index % 2 else "actual_win",
                metrics={"net_ev_usd": float(index)},
                observed_at=BASE + timedelta(minutes=1, seconds=index),
            )

    def test_thirty_independent_scenarios_are_discovery_not_a_or_auto_promotion(self) -> None:
        self._add_scenarios(29)
        collecting = self.governance.assess("challenger-v2")
        self.assertEqual(LearningStage.COLLECTING, collecting.stage)
        with self.assertRaises(PromotionBlocked):
            self.governance.create_promotion_report(
                "too-early",
                "challenger-v2",
                report={"finding": "premature"},
                generated_at=BASE + timedelta(minutes=3),
            )

        self._add_scenarios(1)  # retry of scenario 0 is idempotent, not independent
        self.assertEqual(29, self.governance.assess("challenger-v2").independent_scenarios)
        self.governance.record_shadow_result(
            "challenger-v2",
            "event-family-29",
            outcome="actual_loss",
            metrics={"net_ev_usd": -1.0},
            observed_at=BASE + timedelta(minutes=2),
        )
        discovery = self.governance.assess("challenger-v2")
        self.assertEqual(30, discovery.independent_scenarios)
        self.assertEqual(LearningStage.DISCOVERY, discovery.stage)
        self.assertEqual("DISCOVERY", discovery.grade)
        self.assertIsNone(discovery.automatic_grade)
        self.assertNotEqual("A", discovery.grade)
        self.assertFalse(discovery.can_auto_promote)
        self.assertEqual("champion-v1", self.governance.current_champion())

    def test_production_needs_report_and_explicit_human_approval(self) -> None:
        self._add_scenarios(30)
        report = self.governance.create_promotion_report(
            "report-v2",
            "challenger-v2",
            report={"sample_outside": True, "cost_adjusted_ev": 12.5},
            generated_at=BASE + timedelta(minutes=5),
        )
        with self.assertRaises(PromotionBlocked):
            self.governance.promote(
                "challenger-v2",
                report_id=report.report_id,
                approval_id="missing",
                promoted_at=BASE + timedelta(minutes=6),
                reason="must fail closed",
            )
        with self.assertRaises(PromotionBlocked):
            self.governance.approve_promotion(
                "approval-v2",
                report.report_id,
                approved_by="operator",
                approved_at=BASE + timedelta(minutes=6),
                explicit_approval=False,
                approval_marker=PRODUCTION_APPROVAL_MARKER,
            )

        approval = self.governance.approve_promotion(
            "approval-v2",
            report.report_id,
            approved_by="operator",
            approved_at=BASE + timedelta(minutes=6),
            explicit_approval=True,
            approval_marker=PRODUCTION_APPROVAL_MARKER,
        )
        transition = self.governance.promote(
            "challenger-v2",
            report_id=report.report_id,
            approval_id=approval.approval_id,
            promoted_at=BASE + timedelta(minutes=7),
            reason="operator reviewed report",
        )
        self.assertEqual(("champion-v1", "challenger-v2"), self.governance.champion_history())
        self.assertEqual("challenger-v2", transition.to_version)
        self.assertEqual("challenger-v2", self.governance.current_champion())

    def test_promotion_state_rebuilds_after_restart_and_can_roll_back(self) -> None:
        self._add_scenarios(30)
        report = self.governance.create_promotion_report(
            "report-v2",
            "challenger-v2",
            report={"signed_findings": ["forward", "costs", "drawdown"]},
            generated_at=BASE + timedelta(minutes=5),
        )
        approval = self.governance.approve_promotion(
            "approval-v2",
            report.report_id,
            approved_by="operator",
            approved_at=BASE + timedelta(minutes=6),
            explicit_approval=True,
            approval_marker=PRODUCTION_APPROVAL_MARKER,
        )
        self.governance.promote(
            "challenger-v2",
            report_id=report.report_id,
            approval_id=approval.approval_id,
            promoted_at=BASE + timedelta(minutes=7),
            reason="review complete",
        )
        self.governance.close()
        self.governance = LearningGovernance(self.path)
        self.assertEqual("challenger-v2", self.governance.current_champion())
        rollback = self.governance.rollback(
            "rollback-v2",
            "champion-v1",
            requested_by="operator",
            reason="live calibration drift",
            rolled_back_at=BASE + timedelta(minutes=8),
        )
        self.assertTrue(rollback.rollback)
        self.assertEqual("champion-v1", self.governance.current_champion())
        with self.assertRaisesRegex(PromotionBlocked, "single-use"):
            self.governance.promote(
                "challenger-v2",
                report_id=report.report_id,
                approval_id=approval.approval_id,
                promoted_at=BASE + timedelta(minutes=9),
                reason="stale approval must not be replayed",
            )
        output = self.path.parent / "learning-audit.jsonl"
        self.assertGreaterEqual(self.governance.export_audit(output), 35)


class IndependenceSpecFixtureTests(unittest.TestCase):
    def test_test_fixture_is_explicit_and_cannot_spoof_a_production_human_signature(
        self,
    ) -> None:
        fixture = VerifiedIndependenceSpec.for_test(
            version="v1",
            effective_at=BASE,
            initial_policy_version="v1",
            initial_policy_hash=INITIAL_POLICY_HASH,
            execution_cost_version="v1",
            execution_cost_hash=EXECUTION_COST_HASH,
        )

        self.assertTrue(fixture.test_only)
        self.assertEqual("test:fixture", fixture.actor)
        self.assertEqual(
            ("ticker", "issuer_id", "provider"),
            fixture.rules["event_identity_namespace_fields"],
        )
        with self.assertRaisesRegex(
            IndependenceSpecValidationError, "allow_test_fixture"
        ):
            verify_independence_spec(fixture)
        self.assertEqual(
            fixture,
            verify_independence_spec(fixture, allow_test_fixture=True),
        )

        forged = fixture.as_dict()
        forged["actor"] = "human:forged-test-name"
        forged.pop("spec_hash")
        forged["spec_hash"] = canonical_hash(
            {key: value for key, value in forged.items() if key != "spec_hash"}
        )
        with self.assertRaisesRegex(
            IndependenceSpecValidationError, "test fixture actor"
        ):
            verify_independence_spec(forged, allow_test_fixture=True)

        canonical_only = fixture.as_dict()
        canonical_only["test_only"] = False
        canonical_only["actor"] = "human:canonical-hash-is-not-a-signature"
        canonical_only.pop("spec_hash")
        canonical_only["spec_hash"] = canonical_hash(canonical_only)
        with self.assertRaisesRegex(
            IndependenceSpecValidationError, "production.*unavailable"
        ):
            verify_independence_spec(canonical_only)


class ProposalApprovalTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.path = self.root / "approvals.db"
        self.clock = MutableClock(BASE)
        self.store = ProposalApprovalStore(self.path, clock=self.clock)

    def tearDown(self) -> None:
        self.store.close()
        self.temp.cleanup()

    def _approve(self, *, nonce: str = "nonce-for-one-proposal"):
        return self.store.approve(
            "proposal-1",
            proposal(),
            nonce=nonce,
            approved_by="gui-user",
            approval_id="approval-1",
        )

    def test_approval_is_wal_full_hash_bound_five_minutes_and_hides_raw_nonce(self) -> None:
        approved = self._approve()
        self.assertEqual("wal", self.store.journal_mode)
        self.assertEqual("full", self.store.synchronous)
        self.assertEqual(APPROVAL_TTL_SECONDS, 300)
        self.assertEqual(timedelta(minutes=5), approved.expires_at - approved.approved_at)
        self.assertEqual(64, len(approved.proposal_hash))
        self.assertEqual(Decimal("5.00"), approved.adverse_tolerance_usd)
        self.assertNotEqual("nonce-for-one-proposal", approved.nonce_hash)
        connection = sqlite3.connect(self.path)
        try:
            stored = connection.execute(
                "SELECT nonce_hash, adverse_tolerance_usd FROM proposal_approvals"
            ).fetchone()
        finally:
            connection.close()
        self.assertEqual(64, len(stored[0]))
        self.assertEqual("5.00", stored[1])

    def test_generated_approval_id_remains_valid_for_symbol_prefixed_token(self) -> None:
        with patch(
            "options_copilot.approval.store.secrets.token_urlsafe",
            return_value="_symbol-prefixed-token",
        ):
            approved = self.store.approve(
                "proposal-generated-id",
                proposal(),
                nonce="nonce-for-generated-approval-id",
                approved_by="gui-user",
            )

        self.assertEqual("approval-_symbol-prefixed-token", approved.approval_id)

    def test_nonce_replay_is_rejected_durably_across_restart(self) -> None:
        self._approve()
        self.store.close()
        self.store = ProposalApprovalStore(self.path, clock=self.clock)
        with self.assertRaises(NonceReplayError):
            self.store.approve(
                "proposal-2",
                proposal(),
                nonce="nonce-for-one-proposal",
                approved_by="gui-user",
                approval_id="approval-2",
            )

    def test_quote_reprice_within_five_dollars_is_valid_but_more_is_not(self) -> None:
        approved = self._approve()
        repriced = proposal()
        repriced["legs"][0]["ask"] = 5.14
        repriced["pricing"]["net_debit_usd"] = 104.0
        self.clock.set(BASE + timedelta(minutes=1))
        within = self.store.validate(
            approved.approval_id,
            repriced,
        )
        self.assertTrue(within.valid, within.reasons)
        self.assertEqual(Decimal("4.0"), within.adverse_change_usd)

        repriced["legs"][0]["ask"] = 5.1501
        repriced["pricing"]["net_debit_usd"] = 105.01
        beyond = self.store.validate(
            approved.approval_id,
            repriced,
        )
        self.assertFalse(beyond.valid)
        self.assertIn("adverse_tolerance_exceeded", beyond.reasons)
        understated = self.store.validate(
            approved.approval_id,
            repriced,
            adverse_change_usd=0,
            current_cost_usd=100,
        )
        self.assertFalse(understated.valid)
        self.assertEqual(Decimal("5.0100"), understated.adverse_change_usd)

    def test_declared_reference_cannot_replace_a_missing_executable_quote(self) -> None:
        missing = proposal()
        missing["legs"][0].pop("ask")
        missing["reference_cost_usd"] = "100.00"

        with self.assertRaisesRegex(ValueError, "executable ask"):
            self.store.approve(
                "proposal-no-ask",
                missing,
                nonce="nonce-without-executable-ask",
                approved_by="gui-user",
            )

    def test_decimal_proposals_hash_canonically_and_reprice_exactly(self) -> None:
        original = proposal()
        original["legs"][0]["strike"] = Decimal("500.00")
        original["pricing"]["net_debit_usd"] = Decimal("100.00")
        equivalent = deepcopy(original)
        equivalent["legs"][0]["strike"] = Decimal("500.0")
        equivalent["pricing"]["net_debit_usd"] = Decimal("100.0")
        self.assertEqual(hash_proposal(original), hash_proposal(equivalent))

        approved = self.store.approve(
            "decimal-proposal",
            original,
            nonce="nonce-for-decimal-proposal",
            approved_by="gui-user",
            approval_id="decimal-approval",
        )
        repriced = deepcopy(equivalent)
        repriced["legs"][0]["ask"] = Decimal("5.1499")
        repriced["pricing"]["net_debit_usd"] = Decimal("104.99")
        self.clock.set(BASE + timedelta(minutes=1))
        result = self.store.validate(
            approved.approval_id,
            repriced,
        )
        self.assertTrue(result.valid, result.reasons)
        self.assertEqual(Decimal("4.99"), result.adverse_change_usd)

    def test_any_leg_or_risk_material_change_invalidates_approval(self) -> None:
        approved = self._approve()
        self.clock.set(BASE + timedelta(seconds=1))
        changed_leg = proposal()
        changed_leg["legs"][0]["strike"] = 501
        leg_result = self.store.validate(
            approved.approval_id,
            changed_leg,
            adverse_change_usd=0,
        )
        self.assertFalse(leg_result.valid)
        self.assertIn("proposal_legs_changed", leg_result.reasons)

        changed_risk = proposal()
        changed_risk["risk"]["defined_risk"] = False
        risk_result = self.store.validate(
            approved.approval_id,
            changed_risk,
            adverse_change_usd=0,
        )
        self.assertFalse(risk_result.valid)
        self.assertIn("proposal_risk_changed", risk_result.reasons)

        repriced_risk = proposal()
        repriced_risk["legs"][0]["ask"] = 5.14
        repriced_risk["risk"]["max_loss_usd"] = 104.0
        repriced_risk["pricing"]["net_debit_usd"] = 104.0
        repriced_risk["quote_snapshot_id"] = "quotes-new"
        repriced_risk["legs"][0]["quote_snapshot_id"] = "quotes-new"
        repriced_risk["legs"][0]["quote_time"] = (
            BASE + timedelta(seconds=1)
        ).isoformat()
        reprice_result = self.store.validate(
            approved.approval_id,
            repriced_risk,
        )
        self.assertTrue(reprice_result.valid, reprice_result.reasons)

    def test_ttl_boundary_expires_and_valid_approval_is_single_use(self) -> None:
        approved = self._approve()
        self.clock.set(BASE + timedelta(seconds=299, milliseconds=999))
        just_before = self.store.validate(
            approved.approval_id,
            proposal(),
        )
        self.assertTrue(just_before.valid)
        self.clock.set(BASE + timedelta(seconds=300))
        expired = self.store.validate(
            approved.approval_id,
            proposal(),
        )
        self.assertFalse(expired.valid)
        self.assertIn("approval_expired", expired.reasons)

        self.clock.set(BASE + timedelta(minutes=1))
        first, consumption = self.store.consume(
            approved.approval_id,
            proposal(),
            execution={"route": "IBKR_REVIEW", "order_ref": "review-1"},
        )
        self.assertTrue(first.valid)
        self.assertIsNotNone(consumption)
        self.clock.set(BASE + timedelta(minutes=2))
        replay, duplicate = self.store.consume(
            approved.approval_id,
            proposal(),
            execution={"route": "IBKR_REVIEW", "order_ref": "review-1"},
        )
        self.assertFalse(replay.valid)
        self.assertIn("approval_already_consumed", replay.reasons)
        self.assertIsNone(duplicate)

    def test_caller_cannot_backdate_the_store_clock(self) -> None:
        approved = self._approve()
        self.clock.set(BASE + timedelta(minutes=10))
        with self.assertRaisesRegex(ValueError, "controlled by the approval store clock"):
            self.store.validate(
                approved.approval_id,
                proposal(),
                checked_at=BASE + timedelta(minutes=1),
            )

    def test_tolerance_is_capped_approvals_are_immutable_and_exportable(self) -> None:
        with self.assertRaisesRegex(ValueError, "at most 5.00"):
            self.store.approve(
                "proposal-over-tolerance",
                proposal(),
                nonce="unique-nonce-over-tolerance",
                approved_by="gui-user",
                approval_id="over-tolerance",
                adverse_tolerance_usd="5.01",
            )
        self._approve()
        connection = sqlite3.connect(self.path)
        try:
            with self.assertRaisesRegex(sqlite3.IntegrityError, "update forbidden"):
                connection.execute(
                    "UPDATE proposal_approvals SET approved_by='attacker' "
                    "WHERE approval_id='approval-1'"
                )
            with self.assertRaisesRegex(sqlite3.IntegrityError, "delete forbidden"):
                connection.execute(
                    "DELETE FROM proposal_approvals WHERE approval_id='approval-1'"
                )
        finally:
            connection.close()
        output = self.root / "approval-audit.jsonl"
        self.assertEqual(1, self.store.export_jsonl(output))
        row = json.loads(output.read_text(encoding="utf-8"))
        self.assertEqual("approval-1", row["approval_id"])
        self.assertEqual("5.00", row["adverse_tolerance_usd"])

    def test_concurrent_duplicate_nonce_has_exactly_one_winner(self) -> None:
        barrier = threading.Barrier(2)
        outcomes: list[str] = []
        outcome_lock = threading.Lock()

        def approve(index: int) -> None:
            barrier.wait()
            try:
                self.store.approve(
                    f"proposal-{index}",
                    proposal(),
                    nonce="same-concurrent-nonce",
                    approved_by="gui-user",
                    approval_id=f"concurrent-{index}",
                )
            except NonceReplayError:
                result = "replay"
            else:
                result = "approved"
            with outcome_lock:
                outcomes.append(result)

        threads = [threading.Thread(target=approve, args=(index,)) for index in (1, 2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=5)
        self.assertEqual(["approved", "replay"], sorted(outcomes))
        self.assertEqual(1, len(self.store.query()))

    def test_active_query_cannot_lose_current_approval_after_500_history_rows(self) -> None:
        for index in range(501):
            self.store.approve(
                f"historical-proposal-{index}",
                proposal(),
                nonce=f"historical-nonce-{index:04d}",
                approved_by="gui-user",
                approval_id=f"historical-approval-{index}",
            )
        self.clock.set(BASE + timedelta(seconds=301))
        current = self.store.approve(
            "current-proposal",
            proposal(),
            nonce="current-active-nonce-after-history",
            approved_by="gui-user",
            approval_id="current-active-approval",
        )

        active = self.store.list_active()

        self.assertEqual((current.approval_id,), tuple(row.approval_id for row in active))

    def test_exclusive_active_approval_is_atomic_across_store_processes(self) -> None:
        second_store = ProposalApprovalStore(self.path, clock=self.clock)
        barrier = threading.Barrier(2)
        outcomes: list[str] = []
        outcome_lock = threading.Lock()

        def attempt(store: ProposalApprovalStore, index: int) -> None:
            barrier.wait()
            try:
                store.approve(
                    f"exclusive-proposal-{index}",
                    proposal(),
                    nonce=f"exclusive-active-nonce-{index}",
                    approved_by="gui-user",
                    approval_id=f"exclusive-active-approval-{index}",
                    exclusive_active=True,
                )
            except ActiveApprovalExists:
                outcome = "blocked"
            else:
                outcome = "approved"
            with outcome_lock:
                outcomes.append(outcome)

        try:
            threads = [
                threading.Thread(target=attempt, args=(store, index))
                for index, store in enumerate((self.store, second_store), start=1)
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=5)
            self.assertEqual(["approved", "blocked"], sorted(outcomes))
            self.assertEqual(1, len(self.store.list_active()))
        finally:
            second_store.close()


if __name__ == "__main__":
    unittest.main()
