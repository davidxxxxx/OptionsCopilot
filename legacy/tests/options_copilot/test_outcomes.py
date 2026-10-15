from __future__ import annotations

from copy import deepcopy
from contextlib import contextmanager
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest
from unittest.mock import patch

from options_copilot.execution_cost import (
    CandidateCostResolution,
    EXECUTION_COST_HASH,
    EXECUTION_COST_VERSION,
    ExecutionCostResolution,
    SignedExecutionCostResolver,
)
from options_copilot.learning.outcomes import (
    BROKER_OUTCOME_EVIDENCE_REF_SCHEMA,
    BrokerOutcomeEvidenceStore,
    IndependenceSpecUnavailable,
    IndependenceSpecValidationError,
    MixedIndependenceSpecError,
    OutcomeIdentityConflict,
    OutcomeRecorder,
    OutcomeValidationError,
    VerifiedIndependenceSpec,
)
from options_copilot.storage.canonical import canonical_hash


BASE = datetime(2026, 8, 3, 14, 0, tzinfo=timezone.utc)
INITIAL_POLICY_HASH = "b5d969d13fc624cdb2c49b3a78ce0fa34119bedd95f51db7b25ba9c977f55a3c"


def authority_locked(method):
    def locked(self, *args, **kwargs):
        with self._lock:
            return method(self, *args, **kwargs)

    return locked


class FixtureBrokerOutcomeEvidenceStore:
    """Explicit TEST_ONLY append-only authority used by outcome tests."""

    test_only = True
    authority_hash = "b" * 64

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._entries: dict[str, dict[str, object]] = {}
        self._heads: dict[str, str] = {}
        self._sequences: dict[str, int] = {}
        self.head_change_after_reads: int | None = None
        self._head_reads = 0

    @contextmanager
    def acquire_read_lease(self):
        with self._lock:
            yield self

    @authority_locked
    def append(
        self,
        *,
        suffix: str,
        broker_snapshot_hash: str,
        valuation: dict[str, object],
        execution: dict[str, object],
        broker_cost: dict[str, object],
        costs: dict[str, object],
    ) -> dict[str, object]:
        stream_id = f"broker-stream-{suffix}"
        sequence = self._sequences.get(stream_id, 0) + 1
        previous_head_hash = self._heads.get(stream_id, "0" * 64)
        entry_id = f"{stream_id}:v{sequence}"
        body = {
            "schema": "options_copilot.broker_outcome_evidence.v1",
            "stream_id": stream_id,
            "entry_id": entry_id,
            "sequence": sequence,
            "previous_head_hash": previous_head_hash,
            "broker_snapshot_hash": broker_snapshot_hash,
            "authority_hash": self.authority_hash,
            "valuation_evidence": deepcopy(valuation),
            "execution_evidence": deepcopy(execution),
            "broker_cost_evidence": deepcopy(broker_cost),
            "costs": deepcopy(costs),
        }
        evidence_hash = canonical_hash(body)
        ledger_head_hash = canonical_hash(
            {
                "schema": "options_copilot.broker_outcome_evidence_head.v1",
                "stream_id": stream_id,
                "sequence": sequence,
                "previous_head_hash": previous_head_hash,
                "evidence_hash": evidence_hash,
            }
        )
        entry = {
            **body,
            "evidence_hash": evidence_hash,
            "ledger_head_hash": ledger_head_hash,
        }
        self._entries[entry_id] = entry
        self._heads[stream_id] = ledger_head_hash
        self._sequences[stream_id] = sequence
        ref_body = {
            "schema": BROKER_OUTCOME_EVIDENCE_REF_SCHEMA,
            "stream_id": stream_id,
            "entry_id": entry_id,
            "evidence_hash": evidence_hash,
            "broker_snapshot_hash": broker_snapshot_hash,
            "ledger_head_hash": ledger_head_hash,
            "authority_hash": self.authority_hash,
        }
        return {**ref_body, "reference_hash": canonical_hash(ref_body)}

    @authority_locked
    def supersede(
        self,
        document: dict[str, object],
        mutate: object,
    ) -> None:
        reference = document["broker_outcome_evidence_ref"]
        prior = deepcopy(self._entries[reference["entry_id"]])
        mutate(prior)
        valuation = prior["valuation_evidence"]
        execution = prior["execution_evidence"]
        broker_cost = prior["broker_cost_evidence"]
        costs = prior["costs"]
        document["broker_outcome_evidence_ref"] = self.append(
            suffix=str(reference["stream_id"]).removeprefix("broker-stream-"),
            broker_snapshot_hash=str(prior["broker_snapshot_hash"]),
            valuation=valuation,
            execution=execution,
            broker_cost=broker_cost,
            costs=costs,
        )

    @authority_locked
    def resolve(self, entry_id: str) -> dict[str, object]:
        if entry_id not in self._entries:
            raise KeyError(entry_id)
        return deepcopy(self._entries[entry_id])

    @authority_locked
    def read_head(self, stream_id: str) -> str:
        self._head_reads += 1
        if (
            self.head_change_after_reads is not None
            and self._head_reads >= self.head_change_after_reads
        ):
            self._heads[stream_id] = "f" * 64
        return self._heads[stream_id]


class PausingOutcomeRecorder(OutcomeRecorder):
    def __init__(
        self,
        *args,
        insert_window: threading.Event,
        writer_started: threading.Event,
        writer_finished: threading.Event,
        **kwargs,
    ):
        self.insert_window = insert_window
        self.writer_started = writer_started
        self.writer_finished = writer_finished
        super().__init__(*args, **kwargs)

    def _record_prepared(self, prepared, *, recorded_at, broker_outcome_evidence_store):
        self.insert_window.set()
        if not self.writer_started.wait(timeout=5):
            raise AssertionError("authority writer did not reach the insert window")
        if self.writer_finished.is_set():
            raise AssertionError("authority writer crossed an active read lease")
        return super()._record_prepared(
            prepared,
            recorded_at=recorded_at,
            broker_outcome_evidence_store=broker_outcome_evidence_store,
        )


_ACTIVE_EVIDENCE_STORE: FixtureBrokerOutcomeEvidenceStore | None = None


class FixtureCostAuthority:
    def __init__(self) -> None:
        self.resolve_calls: list[dict[str, object]] = []

    def resolve(self, **request: object) -> ExecutionCostResolution:
        self.resolve_calls.append(dict(request))
        candidates = request["candidates"]
        candidate = candidates[0]
        suffix = str(candidate["candidate_id"]).removeprefix("candidate-")
        return cost_resolution(suffix, resolved_at=request["now"])

    def assert_current(
        self, resolution: ExecutionCostResolution
    ) -> ExecutionCostResolution:
        return resolution


def cost_resolution(
    suffix: str,
    *,
    resolved_at: datetime = BASE,
) -> ExecutionCostResolution:
    candidate = CandidateCostResolution(
        candidate_id=f"candidate-{suffix}",
        cost_version=EXECUTION_COST_VERSION,
        cost_hash=EXECUTION_COST_HASH,
        quote_batch_id=f"entry-batch-{suffix}",
        commission_usd=Decimal("5.00"),
        slippage_usd=Decimal("15.00"),
        execution_cost_usd=Decimal("20.00"),
        expected_value_before_costs_usd=Decimal("390.00"),
        after_cost_expected_value=Decimal("370.00"),
        stress_execution_cost_usd=Decimal("25.00"),
        stress_after_cost_expected_value=Decimal("365.00"),
        scenario_count=5,
        calculation_hash=canonical_hash(
            {"fixture": "candidate-cost", "candidate_id": f"candidate-{suffix}"}
        ),
    )
    candidate_document = {
        "candidate_id": candidate.candidate_id,
        "cost_version": candidate.cost_version,
        "cost_hash": candidate.cost_hash,
        "quote_batch_id": candidate.quote_batch_id,
        "commission_usd": candidate.commission_usd,
        "slippage_usd": candidate.slippage_usd,
        "execution_cost_usd": candidate.execution_cost_usd,
        "expected_value_before_costs_usd": candidate.expected_value_before_costs_usd,
        "after_cost_expected_value": candidate.after_cost_expected_value,
        "stress_execution_cost_usd": candidate.stress_execution_cost_usd,
        "stress_after_cost_expected_value": candidate.stress_after_cost_expected_value,
        "scenario_count": candidate.scenario_count,
        "calculation_hash": candidate.calculation_hash,
    }
    body = {
        "schema": "options_copilot.execution_cost_resolution.v1",
        "cost_version": EXECUTION_COST_VERSION,
        "cost_hash": EXECUTION_COST_HASH,
        "contract_effective_at": BASE - timedelta(days=2),
        "contract_signed_at": BASE - timedelta(days=2),
        "contract_marker_hash": "a" * 64,
        "resolved_at": resolved_at,
        "scan_run_id": f"scan-{suffix}",
        "candidates": [candidate_document],
    }
    return ExecutionCostResolution(
        cost_version=EXECUTION_COST_VERSION,
        cost_hash=EXECUTION_COST_HASH,
        contract_effective_at=BASE - timedelta(days=2),
        contract_signed_at=BASE - timedelta(days=2),
        contract_marker_hash="a" * 64,
        resolved_at=resolved_at,
        scan_run_id=f"scan-{suffix}",
        candidates=(candidate,),
        resolution_hash=canonical_hash(body),
    )


def _contract(
    con_id: int,
    strike: str,
    side: str,
) -> dict[str, object]:
    identity = {
        "con_id": con_id,
        "underlying": "SPY",
        "security_type": "OPT",
        "expiration": "2026-08-21",
        "strike": strike,
        "right": "C",
        "multiplier": "100",
        "currency": "USD",
        "exchange": "SMART",
    }
    return {
        **identity,
        "side": side,
        "quantity": 1,
        "contract_hash": canonical_hash(identity),
    }


def _quote(
    con_id: int,
    *,
    bid: str,
    ask: str,
    observed_at: datetime,
    batch_id: str,
) -> dict[str, object]:
    body = {
        "con_id": con_id,
        "bid": bid,
        "ask": ask,
        "observed_at": observed_at,
        "batch_id": batch_id,
    }
    return {**body, "quote_hash": canonical_hash(body)}


def cost_recompute_request(
    suffix: str,
    *,
    decision_at: datetime,
) -> dict[str, object]:
    batch_id = f"entry-batch-{suffix}"
    entry_at = decision_at - timedelta(seconds=1)
    long_contract = _contract(10001, "100", "LONG")
    short_contract = _contract(10002, "110", "SHORT")
    candidate = {
        "candidate_id": f"candidate-{suffix}",
        "symbol": "SPY",
        "structure": "DEBIT_VERTICAL",
        "legs": (
            {
                **long_contract,
                "ratio": 1,
                "bid": "2.00",
                "ask": "2.10",
                "observed_at": entry_at,
                "quote_snapshot_id": batch_id,
            },
            {
                **short_contract,
                "ratio": 1,
                "bid": "1.00",
                "ask": "1.10",
                "observed_at": entry_at,
                "quote_snapshot_id": batch_id,
            },
        ),
        "estimated_commissions_usd": "5.00",
        "estimated_slippage_usd": "15.00",
        "debit_usd": "210.00",
        "credit_usd": "100.00",
        "all_in_cost_usd": "130.00",
        "max_loss_usd": "130.00",
        "quote_batch_id": batch_id,
        "execution_cost_contract_version": EXECUTION_COST_VERSION,
        "execution_cost_contract_hash": EXECUTION_COST_HASH,
    }
    scenario = {
        "candidate_id": f"candidate-{suffix}",
        "action": "TRADE",
        "cost_version": EXECUTION_COST_VERSION,
        "cost_hash": EXECUTION_COST_HASH,
        "scenarios": (
            {"terminal_price": "100", "probability": "0.50"},
            {"terminal_price": "120", "probability": "0.50"},
        ),
    }
    return {
        "scan_run_id": f"scan-{suffix}",
        "candidate": candidate,
        "scenario": scenario,
    }


def valuation_evidence(
    suffix: str,
    *,
    decision_at: datetime,
    economic_observed_at: datetime,
) -> dict[str, object]:
    entry_batch = f"entry-batch-{suffix}"
    exit_batch = f"exit-batch-{suffix}"
    entry_at = decision_at - timedelta(seconds=1)
    exit_at = economic_observed_at - timedelta(seconds=1)
    legs = [
        {
            **_contract(10001, "100", "LONG"),
            "entry_quote": _quote(
                10001,
                bid="2.00",
                ask="2.10",
                observed_at=entry_at,
                batch_id=entry_batch,
            ),
            "exit_quote": _quote(
                10001,
                bid="2.58",
                ask="2.68",
                observed_at=exit_at,
                batch_id=exit_batch,
            ),
        },
        {
            **_contract(10002, "110", "SHORT"),
            "entry_quote": _quote(
                10002,
                bid="1.00",
                ask="1.10",
                observed_at=entry_at,
                batch_id=entry_batch,
            ),
            "exit_quote": _quote(
                10002,
                bid="1.30",
                ask="1.40",
                observed_at=exit_at,
                batch_id=exit_batch,
            ),
        },
    ]
    mark_snapshots = [
        {
            "observed_at": decision_at,
            "batch_id": f"mark-open-{suffix}",
            "quotes": [
                _quote(
                    10001,
                    bid="2.00",
                    ask="2.10",
                    observed_at=decision_at,
                    batch_id=f"mark-open-{suffix}",
                ),
                _quote(
                    10002,
                    bid="1.00",
                    ask="1.10",
                    observed_at=decision_at,
                    batch_id=f"mark-open-{suffix}",
                ),
            ],
        },
        {
            "observed_at": decision_at + timedelta(hours=12),
            "batch_id": f"mark-high-{suffix}",
            "quotes": [
                _quote(
                    10001,
                    bid="2.65",
                    ask="2.75",
                    observed_at=decision_at + timedelta(hours=12),
                    batch_id=f"mark-high-{suffix}",
                ),
                _quote(
                    10002,
                    bid="1.35",
                    ask="1.45",
                    observed_at=decision_at + timedelta(hours=12),
                    batch_id=f"mark-high-{suffix}",
                ),
            ],
        },
    ]
    body = {
        "schema": "options_copilot.valuation_evidence.v1",
        "broker_snapshot_hash": "7" * 64,
        "legs": legs,
        "mark_snapshots": mark_snapshots,
    }
    return {**body, "valuation_hash": canonical_hash(body)}


def execution_evidence(
    suffix: str,
    valuation: dict[str, object],
) -> dict[str, object]:
    contracts = [
        {
            "con_id": leg["con_id"],
            "quantity": leg["quantity"],
            "multiplier": leg["multiplier"],
            "side": leg["side"],
            "contract_hash": leg["contract_hash"],
            "entry_quote_hash": leg["entry_quote"]["quote_hash"],
            "exit_quote_hash": leg["exit_quote"]["quote_hash"],
        }
        for leg in valuation["legs"]
    ]
    body = {
        "broker_snapshot_hash": "7" * 64,
        "entry_quote_batch_id": f"entry-batch-{suffix}",
        "exit_quote_batch_id": f"exit-batch-{suffix}",
        "contracts": contracts,
        "contracts_hash": canonical_hash(contracts),
        "total_contract_sides": 2,
    }
    return {**body, "broker_evidence_hash": canonical_hash(body)}


def broker_cost_evidence(suffix: str) -> dict[str, object]:
    components: dict[str, object] = {}
    for name, amount, source in (
        ("fees_usd", "0.50", "IBKR_FILL"),
        ("assignment_usd", "0.00", "IBKR_STATEMENT"),
        ("exercise_usd", "0.00", "IBKR_STATEMENT"),
        ("dividend_usd", "1.00", "IBKR_STATEMENT"),
    ):
        body = {
            "component": name,
            "amount_usd": amount,
            "source": source,
            "reference_id": f"{source.lower()}-{suffix}-{name}",
            "broker_snapshot_hash": "7" * 64,
        }
        components[name] = {**body, "evidence_hash": canonical_hash(body)}
    body = {
        "schema": "options_copilot.broker_cost_evidence.v1",
        "components": components,
    }
    return {**body, "evidence_hash": canonical_hash(body)}


def rehash_market_evidence(evidence: dict[str, object]) -> None:
    valuation = evidence["valuation_evidence"]
    for leg in valuation["legs"]:
        for name in ("entry_quote", "exit_quote"):
            quote = leg[name]
            quote["quote_hash"] = canonical_hash(
                {key: value for key, value in quote.items() if key != "quote_hash"}
            )
    for snapshot in valuation["mark_snapshots"]:
        for quote in snapshot["quotes"]:
            quote["quote_hash"] = canonical_hash(
                {key: value for key, value in quote.items() if key != "quote_hash"}
            )
    valuation["valuation_hash"] = canonical_hash(
        {key: value for key, value in valuation.items() if key != "valuation_hash"}
    )
    execution = evidence["execution_evidence"]
    by_con_id = {leg["con_id"]: leg for leg in valuation["legs"]}
    for contract in execution["contracts"]:
        leg = by_con_id[contract["con_id"]]
        contract["entry_quote_hash"] = leg["entry_quote"]["quote_hash"]
        contract["exit_quote_hash"] = leg["exit_quote"]["quote_hash"]
    execution["contracts_hash"] = canonical_hash(execution["contracts"])
    execution["broker_evidence_hash"] = canonical_hash(
        {
            key: value
            for key, value in execution.items()
            if key != "broker_evidence_hash"
        }
    )


def rehash_reference(document: dict[str, object]) -> None:
    reference = document["broker_outcome_evidence_ref"]
    reference["reference_hash"] = canonical_hash(
        {key: value for key, value in reference.items() if key != "reference_hash"}
    )


def independence_fixture(version: str = "v1") -> VerifiedIndependenceSpec:
    return VerifiedIndependenceSpec.for_test(
        version=version,
        effective_at=BASE - timedelta(days=1),
        initial_policy_version="v1",
        initial_policy_hash=INITIAL_POLICY_HASH,
        execution_cost_version=EXECUTION_COST_VERSION,
        execution_cost_hash=EXECUTION_COST_HASH,
    )


def outcome(
    suffix: str,
    *,
    economic_observed_at: datetime | None = None,
    revision_received_at: datetime | None = None,
    decision_at: datetime = BASE,
    cluster_evidence: dict[str, object] | None = None,
    executable_exit_pnl_before_costs_usd: object = "8.00",
    cost_version: str | None = EXECUTION_COST_VERSION,
    cost_hash: str | None = EXECUTION_COST_HASH,
    quote_quality: dict[str, object] | None = None,
    candidate_hash: str | None = None,
    ranking_snapshot_id: str | None = None,
    ranking_snapshot_hash: str | None = None,
    exit_policy_hash: str | None = None,
    current_policy_hash: str = INITIAL_POLICY_HASH,
) -> dict[str, object]:
    if _ACTIVE_EVIDENCE_STORE is None:
        raise AssertionError("TEST_ONLY broker outcome evidence store is not active")
    horizon_at = decision_at + timedelta(days=1)
    actual_observed_at = economic_observed_at or horizon_at + timedelta(seconds=1)
    valuation = valuation_evidence(
        suffix,
        decision_at=decision_at,
        economic_observed_at=actual_observed_at,
    )
    cost_request = cost_recompute_request(
        suffix,
        decision_at=decision_at,
    )
    normalized_cluster = dict(
        cluster_evidence
        or {
            "status": "KNOWN",
            "ticker": "SPY",
            "event_id": f"event-{suffix}",
            "slot_at": decision_at,
        }
    )
    if normalized_cluster.get("status") == "KNOWN":
        ticker = normalized_cluster.get("ticker")
        if ticker is not None:
            normalized_cluster.setdefault("issuer_id", f"issuer-{ticker}")
        normalized_cluster.setdefault("provider", "TEST_PROVIDER")
    costs = {
        "commission_usd": "5.00",
        "fees_usd": "0.50",
        "spread_usd": "5.00",
        "slippage_usd": "10.00",
        "assignment_usd": "0.00",
        "exercise_usd": "0.00",
        "dividend_usd": "1.00",
    }
    reference = _ACTIVE_EVIDENCE_STORE.append(
        suffix=suffix,
        broker_snapshot_hash="7" * 64,
        valuation=valuation,
        execution=execution_evidence(suffix, valuation),
        broker_cost=broker_cost_evidence(suffix),
        costs=costs,
    )
    return {
        "decision_id": f"decision-{suffix}",
        "decision_hash": "1" * 64,
        "candidate_id": f"candidate-{suffix}",
        "candidate_hash": candidate_hash
        or canonical_hash(cost_request["candidate"]),
        "ranking_snapshot_id": ranking_snapshot_id or f"ranking-{suffix}",
        "ranking_snapshot_hash": ranking_snapshot_hash or "3" * 64,
        "ranking_basis_hash": "4" * 64,
        "horizon": "1d",
        "horizon_at": horizon_at,
        "decision_at": decision_at,
        "economic_observed_at": actual_observed_at,
        "revision_received_at": revision_received_at
        or actual_observed_at + timedelta(minutes=1),
        "input_hash": "5" * 64,
        "evidence_hash": "6" * 64,
        "broker_snapshot_hash": "7" * 64,
        "current_policy_version": "v1",
        "current_policy_hash": current_policy_hash,
        "policy_authority_marker_hash": "8" * 64,
        "cost_version": cost_version,
        "cost_hash": cost_hash,
        "cost_recompute_request": cost_request,
        "broker_outcome_evidence_ref": reference,
        "exit_policy_hash": exit_policy_hash or "9" * 64,
        "mfe_usd": "20.00",
        "mae_usd": "-10.00",
        "mark_pnl_before_costs_usd": "18.00",
        "executable_exit_pnl_before_costs_usd": (executable_exit_pnl_before_costs_usd),
        "exit_rule_hits": ["PROFIT_TAKE"],
        "quote_quality": quote_quality
        if quote_quality is not None
        else {
            "status": "EXECUTABLE",
            "bid_ask_complete": True,
            "source": "IBKR_READ_ONLY",
            "age_seconds": "1.0",
        },
        "cluster_evidence": normalized_cluster,
    }


class OutcomeRecorderTests(unittest.TestCase):
    def setUp(self) -> None:
        global _ACTIVE_EVIDENCE_STORE
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / "outcomes.db"
        self.evidence_store = FixtureBrokerOutcomeEvidenceStore()
        _ACTIVE_EVIDENCE_STORE = self.evidence_store

    def tearDown(self) -> None:
        global _ACTIVE_EVIDENCE_STORE
        _ACTIVE_EVIDENCE_STORE = None
        self.temp.cleanup()

    def recorder(
        self,
        spec: VerifiedIndependenceSpec | None = None,
    ) -> OutcomeRecorder:
        return OutcomeRecorder(
            self.path,
            independence_spec=spec,
            allow_test_fixture=True,
            execution_cost_resolver=FixtureCostAuthority(),
            broker_outcome_evidence_store=self.evidence_store,
        )

    def test_verified_empty_ledger_has_zero_count_and_exact_retry_is_idempotent(
        self,
    ) -> None:
        with self.recorder(independence_fixture()) as recorder:
            empty = recorder.aggregate()
            self.assertEqual(0, empty.total_outcomes)
            self.assertEqual(0, empty.independent_count)
            self.assertEqual(0, empty.diagnostic_cluster_count)
            self.assertTrue(empty.test_only)

            retry_document = outcome("retry")
            first = recorder.record(retry_document)
            retry = recorder.record(retry_document)
            self.assertEqual(first.outcome_id, retry.outcome_id)
            self.assertEqual(1, recorder.count())

    def test_test_fixture_requires_explicit_constructor_opt_in(self) -> None:
        with self.assertRaisesRegex(
            IndependenceSpecValidationError, "allow_test_fixture"
        ):
            OutcomeRecorder(
                self.path,
                independence_spec=independence_fixture(),
                execution_cost_resolver=FixtureCostAuthority(),
            )

    def test_non_signed_cost_resolver_requires_test_fixture_opt_in(self) -> None:
        with self.assertRaisesRegex(
            OutcomeValidationError,
            "SignedExecutionCostResolver",
        ):
            OutcomeRecorder(
                self.path,
                execution_cost_resolver=FixtureCostAuthority(),
            )

        with self.assertRaisesRegex(
            OutcomeValidationError,
            "trusted production broker outcome evidence adapter is unavailable",
        ):
            OutcomeRecorder(
                self.path,
                broker_outcome_evidence_store=self.evidence_store,
            )

    def test_public_callback_or_subclass_cannot_manufacture_production_authority(self) -> None:
        with self.assertRaisesRegex(
            OutcomeValidationError,
            "trusted production broker outcome evidence adapter is unavailable",
        ):
            BrokerOutcomeEvidenceStore(
                authority_hash="a" * 64,
                resolve_entry=lambda _: {},
                read_stream_head=lambda _: "b" * 64,
            )

        class ForgedProductionStore(BrokerOutcomeEvidenceStore):
            test_only = False

            def __init__(self) -> None:
                self.authority_hash = "a" * 64

            @contextmanager
            def acquire_read_lease(self):
                yield self

        with self.assertRaisesRegex(
            OutcomeValidationError,
            "trusted production broker outcome evidence adapter is unavailable",
        ):
            OutcomeRecorder(
                self.path,
                broker_outcome_evidence_store=ForgedProductionStore(),
            )

    def test_caller_raw_broker_evidence_is_ignored_and_never_persisted(self) -> None:
        with self.recorder(independence_fixture()) as recorder:
            document = outcome("caller-raw")
            forged_valuation = valuation_evidence(
                "caller-forged",
                decision_at=BASE,
                economic_observed_at=BASE + timedelta(days=1, seconds=1),
            )
            for leg in forged_valuation["legs"]:
                leg["exit_quote"]["bid"] = "99.00"
                leg["exit_quote"]["ask"] = "100.00"
            for snapshot in forged_valuation["mark_snapshots"]:
                for quote in snapshot["quotes"]:
                    quote["bid"] = "99.00"
                    quote["ask"] = "100.00"
            forged_execution = execution_evidence("caller-forged", forged_valuation)
            forged_bundle = {
                "valuation_evidence": forged_valuation,
                "execution_evidence": forged_execution,
            }
            rehash_market_evidence(forged_bundle)
            forged_broker_cost = broker_cost_evidence("caller-forged")
            for component in forged_broker_cost["components"].values():
                component["reference_id"] = "caller-controlled-reference"
                component["evidence_hash"] = canonical_hash(
                    {
                        key: value
                        for key, value in component.items()
                        if key != "evidence_hash"
                    }
                )
            forged_broker_cost["evidence_hash"] = canonical_hash(
                {
                    key: value
                    for key, value in forged_broker_cost.items()
                    if key != "evidence_hash"
                }
            )
            document["valuation_evidence"] = forged_valuation
            document["execution_evidence"] = forged_execution
            document["broker_cost_evidence"] = forged_broker_cost
            document["costs"] = {name: "0" for name in (
                "commission_usd", "fees_usd", "spread_usd", "slippage_usd",
                "assignment_usd", "exercise_usd", "dividend_usd",
            )}

            stored = recorder.record(document)

            self.assertFalse(stored.diagnostic_eligible)
            self.assertIn(
                "CALLER_BROKER_OUTCOME_EVIDENCE_FORBIDDEN",
                stored.exclusion_reasons,
            )
            for field in (
                "valuation_evidence",
                "execution_evidence",
                "broker_cost_evidence",
            ):
                self.assertNotIn(field, stored.body)
            self.assertEqual(Decimal("8.00"), stored.executable_exit_pnl_before_costs_usd)
            self.assertEqual(Decimal("21.50"), stored.total_cost_usd)

    def test_missing_evidence_store_is_observation_only_and_never_counts(self) -> None:
        with OutcomeRecorder(
            self.path,
            independence_spec=independence_fixture(),
            allow_test_fixture=True,
            execution_cost_resolver=FixtureCostAuthority(),
        ) as recorder:
            stored = recorder.record(outcome("no-store"))
            aggregate = recorder.aggregate()

        self.assertFalse(stored.evaluation_eligible)
        self.assertFalse(stored.diagnostic_eligible)
        self.assertIn(
            "BROKER_OUTCOME_EVIDENCE_STORE_UNAVAILABLE",
            stored.exclusion_reasons,
        )
        self.assertEqual(0, stored.count_weight)
        self.assertEqual(0, aggregate.independent_count)
        self.assertEqual(0, aggregate.eligible_outcomes)

    def test_reference_hash_snapshot_and_head_are_fail_closed(self) -> None:
        with self.recorder(independence_fixture()) as recorder:
            missing = outcome("missing-reference")
            missing["broker_outcome_evidence_ref"]["entry_id"] = "missing-entry"
            rehash_reference(missing)
            stored_missing = recorder.record(missing)
            self.assertIn(
                "BROKER_OUTCOME_EVIDENCE_REFERENCE_NOT_FOUND",
                stored_missing.exclusion_reasons,
            )

            wrong_hash = outcome("wrong-reference-hash")
            wrong_hash["broker_outcome_evidence_ref"]["evidence_hash"] = "d" * 64
            rehash_reference(wrong_hash)
            stored_hash = recorder.record(wrong_hash)
            self.assertIn(
                "BROKER_OUTCOME_EVIDENCE_REFERENCE_MISMATCH",
                stored_hash.exclusion_reasons,
            )

            wrong_snapshot = outcome("wrong-reference-snapshot")
            wrong_snapshot["broker_outcome_evidence_ref"][
                "broker_snapshot_hash"
            ] = "e" * 64
            rehash_reference(wrong_snapshot)
            stored_snapshot = recorder.record(wrong_snapshot)
            self.assertIn(
                "BROKER_OUTCOME_EVIDENCE_SNAPSHOT_MISMATCH",
                stored_snapshot.exclusion_reasons,
            )

            concurrent = outcome("head-changed-during-read")
            self.evidence_store._head_reads = 0
            self.evidence_store.head_change_after_reads = 4
            count_before = recorder.count()
            with self.assertRaisesRegex(
                OutcomeValidationError,
                "BROKER_OUTCOME_EVIDENCE_NOT_CURRENT",
            ):
                recorder.record(concurrent)
            self.evidence_store.head_change_after_reads = None
            self.assertEqual(count_before, recorder.count())

    def test_authority_read_lease_spans_outcome_insert_and_commit(self) -> None:
        document = outcome("lease-window")
        writer_document = deepcopy(document)
        insert_window = threading.Event()
        writer_started = threading.Event()
        writer_finished = threading.Event()

        with PausingOutcomeRecorder(
            self.path,
            independence_spec=independence_fixture(),
            allow_test_fixture=True,
            execution_cost_resolver=FixtureCostAuthority(),
            broker_outcome_evidence_store=self.evidence_store,
            insert_window=insert_window,
            writer_started=writer_started,
            writer_finished=writer_finished,
        ) as recorder:
            def advance_authority() -> None:
                self.assertTrue(insert_window.wait(timeout=5))
                writer_started.set()
                self.evidence_store.supersede(writer_document, lambda _: None)
                writer_finished.set()

            writer = threading.Thread(target=advance_authority)
            writer.start()
            stored = recorder.record(document)
            self.assertTrue(stored.diagnostic_eligible)

        writer.join(timeout=5)
        self.assertFalse(writer.is_alive())
        self.assertTrue(writer_finished.is_set())

    def test_test_only_evidence_store_cannot_become_production_evaluation(self) -> None:
        with self.recorder(independence_fixture()) as recorder:
            stored = recorder.record(outcome("test-only-broker-store"))

        self.assertTrue(stored.diagnostic_eligible)
        self.assertFalse(stored.evaluation_eligible)
        self.assertTrue(stored.body["broker_outcome_evidence_test_only"])
        self.assertIn(
            "BROKER_OUTCOME_EVIDENCE_TEST_ONLY",
            stored.exclusion_reasons,
        )

    def test_diagnostic_evidence_never_receives_production_count_weight(self) -> None:
        production_spec = replace(
            independence_fixture(),
            actor="human:test-only-unit-boundary",
            test_only=False,
            spec_hash="a" * 64,
        )
        with patch(
            "options_copilot.learning.outcomes.verify_independence_spec",
            return_value=production_spec,
        ):
            with OutcomeRecorder(
                self.path,
                independence_spec=production_spec,
                allow_test_fixture=True,
                execution_cost_resolver=FixtureCostAuthority(),
                broker_outcome_evidence_store=self.evidence_store,
            ) as recorder:
                stored = recorder.record(outcome("diagnostic-only-production-spec"))
                aggregate = recorder.aggregate()

        self.assertTrue(stored.diagnostic_eligible)
        self.assertFalse(stored.evaluation_eligible)
        self.assertEqual(0, stored.count_weight)
        self.assertEqual(1, stored.diagnostic_count_weight)
        self.assertEqual(0, aggregate.independent_count)
        self.assertEqual(0, aggregate.eligible_outcomes)
        self.assertEqual(0, aggregate.clusters[0]["count_weight"])

    def test_horizon_and_quote_freshness_are_recomputed_from_timestamps(self) -> None:
        with self.recorder(independence_fixture()) as recorder:
            before_horizon = outcome(
                "before-horizon",
                economic_observed_at=BASE + timedelta(hours=23),
            )
            with self.assertRaisesRegex(
                OutcomeValidationError,
                "economic_observed_at cannot predate horizon_at",
            ):
                recorder.record(before_horizon)

            outside_window = outcome(
                "outside-window",
                economic_observed_at=BASE + timedelta(days=1, seconds=6),
            )
            with self.assertRaisesRegex(
                OutcomeValidationError,
                "economic observation window",
            ):
                recorder.record(outside_window)

            stale = outcome("stale-quote")
            def make_stale(evidence: dict[str, object]) -> None:
                for leg in evidence["valuation_evidence"]["legs"]:
                    leg["exit_quote"]["observed_at"] = (
                        stale["economic_observed_at"] - timedelta(seconds=6)
                    )
                rehash_market_evidence(evidence)

            self.evidence_store.supersede(stale, make_stale)
            stale["quote_quality"]["age_seconds"] = "0"
            stale_result = recorder.record(stale)
            self.assertFalse(stale_result.diagnostic_eligible)
            self.assertIn("EXECUTABLE_EXIT_QUOTE_STALE", stale_result.exclusion_reasons)
            self.assertEqual(
                Decimal("6"), stale_result.quote_age_at_observation_seconds
            )

            future = outcome("future-quote")
            def make_future(evidence: dict[str, object]) -> None:
                for leg in evidence["valuation_evidence"]["legs"]:
                    leg["exit_quote"]["observed_at"] = (
                        future["economic_observed_at"] + timedelta(seconds=1)
                    )
                rehash_market_evidence(evidence)

            self.evidence_store.supersede(future, make_future)
            future_result = recorder.record(future)
            self.assertFalse(future_result.diagnostic_eligible)
            self.assertIn(
                "EXECUTABLE_EXIT_QUOTE_FUTURE", future_result.exclusion_reasons
            )

    def test_cost_resolution_and_contract_quantity_broker_binding_are_required(
        self,
    ) -> None:
        with self.recorder(independence_fixture()) as recorder:
            missing_resolution = outcome("missing-resolution")
            missing_resolution.pop("cost_recompute_request")
            missing = recorder.record(missing_resolution)
            self.assertFalse(missing.diagnostic_eligible)
            self.assertIn(
                "COST_RECOMPUTE_REQUEST_MISSING",
                missing.exclusion_reasons,
            )

            mapping_resolution = outcome("mapping-resolution")
            mapping_resolution["execution_cost_resolution"] = {
                "cost_version": EXECUTION_COST_VERSION,
                "cost_hash": EXECUTION_COST_HASH,
            }
            mapping = recorder.record(mapping_resolution)
            self.assertFalse(mapping.diagnostic_eligible)
            self.assertIn(
                "CALLER_COST_RESOLUTION_FORBIDDEN",
                mapping.exclusion_reasons,
            )

            forged_resolution = outcome("forged-resolution")
            forged_resolution["execution_cost_resolution"] = cost_resolution(
                "forged-resolution"
            )
            forged = recorder.record(forged_resolution)
            self.assertFalse(forged.diagnostic_eligible)
            self.assertIn(
                "CALLER_COST_RESOLUTION_FORBIDDEN",
                forged.exclusion_reasons,
            )

            missing_contracts = outcome("missing-contracts")
            def remove_contracts(evidence: dict[str, object]) -> None:
                execution = evidence["execution_evidence"]
                execution["contracts"] = []
                execution["contracts_hash"] = canonical_hash([])
                execution["broker_evidence_hash"] = canonical_hash(
                    {
                        key: value
                        for key, value in execution.items()
                        if key != "broker_evidence_hash"
                    }
                )

            self.evidence_store.supersede(missing_contracts, remove_contracts)
            missing_contract = recorder.record(missing_contracts)
            self.assertFalse(missing_contract.diagnostic_eligible)
            self.assertIn(
                "EXECUTION_CONTRACT_EVIDENCE_INVALID",
                missing_contract.exclusion_reasons,
            )

            for suffix, mutate in (
                (
                    "broker-binding",
                    lambda evidence: evidence.__setitem__(
                        "broker_snapshot_hash", "e" * 64
                    ),
                ),
                (
                    "quote-batch-binding",
                    lambda evidence: evidence.__setitem__(
                        "entry_quote_batch_id", "entry-batch-other"
                    ),
                ),
                (
                    "quantity-binding",
                    lambda evidence: evidence["contracts"][0].__setitem__(
                        "quantity", 0
                    ),
                ),
                (
                    "total-sides-binding",
                    lambda evidence: evidence.__setitem__(
                        "total_contract_sides", 3
                    ),
                ),
            ):
                with self.subTest(suffix=suffix):
                    document = outcome(suffix)
                    def mutate_execution(bundle: dict[str, object]) -> None:
                        evidence = bundle["execution_evidence"]
                        mutate(evidence)
                        evidence["contracts_hash"] = canonical_hash(
                            evidence["contracts"]
                        )
                        evidence["broker_evidence_hash"] = canonical_hash(
                            {
                                key: value
                                for key, value in evidence.items()
                                if key != "broker_evidence_hash"
                            }
                        )

                    self.evidence_store.supersede(document, mutate_execution)
                    stored = recorder.record(document)
                    self.assertFalse(stored.diagnostic_eligible)
                    self.assertIn(
                        "EXECUTION_CONTRACT_EVIDENCE_INVALID",
                        stored.exclusion_reasons,
                    )

            zero_cost = outcome("zero-cost")
            self.evidence_store.supersede(
                zero_cost,
                lambda evidence: evidence.__setitem__(
                    "costs", {name: "0.00" for name in (
                        "commission_usd", "fees_usd", "spread_usd",
                        "slippage_usd", "assignment_usd", "exercise_usd",
                        "dividend_usd",
                    )}
                ),
            )
            zero_result = recorder.record(zero_cost)
            self.assertFalse(zero_result.diagnostic_eligible)
            self.assertIn(
                "COST_RESOLUTION_COMPONENT_MISMATCH", zero_result.exclusion_reasons
            )

    def test_resolver_and_leg_quotes_are_the_only_cost_and_pnl_authorities(
        self,
    ) -> None:
        authority = FixtureCostAuthority()
        with OutcomeRecorder(
            self.path,
            independence_spec=independence_fixture(),
            allow_test_fixture=True,
            execution_cost_resolver=authority,
            broker_outcome_evidence_store=self.evidence_store,
        ) as recorder:
            forged = outcome("forged-pnl")
            forged["mfe_usd"] = "999999.00"
            forged["mae_usd"] = "-999999.00"
            forged["mark_pnl_before_costs_usd"] = "999999.00"
            forged["executable_exit_pnl_before_costs_usd"] = "999999.00"
            stored = recorder.record(forged)

            self.assertEqual(1, len(authority.resolve_calls))
            request = authority.resolve_calls[0]
            self.assertEqual(
                forged["cost_recompute_request"]["candidate"],
                request["candidates"][0],
            )
            self.assertFalse(stored.diagnostic_eligible)
            self.assertIn("PNL_RECOMPUTATION_MISMATCH", stored.exclusion_reasons)
            self.assertEqual(Decimal("20.00"), stored.mfe_usd)
            self.assertEqual(Decimal("-10.00"), stored.mae_usd)
            self.assertEqual(Decimal("18.00"), stored.mark_pnl_before_costs_usd)
            self.assertEqual(
                Decimal("8.00"),
                stored.executable_exit_pnl_before_costs_usd,
            )

    def test_real_signed_cost_resolver_recomputes_frozen_candidate(self) -> None:
        decision_at = datetime(2026, 8, 5, 14, 30, tzinfo=timezone.utc)
        resolver = SignedExecutionCostResolver(
            clock=lambda: decision_at + timedelta(days=2)
        )
        with OutcomeRecorder(
            self.path,
            independence_spec=independence_fixture(),
            allow_test_fixture=True,
            execution_cost_resolver=resolver,
            broker_outcome_evidence_store=self.evidence_store,
            clock=lambda: decision_at + timedelta(days=2),
        ) as recorder:
            stored = recorder.record(
                outcome("real-resolver", decision_at=decision_at)
            )

            self.assertTrue(stored.diagnostic_eligible)
            self.assertIsNotNone(stored.candidate_cost_calculation_hash)
            self.assertEqual(Decimal("21.50"), stored.total_cost_usd)

    def test_broker_statement_components_are_required_and_hash_bound(self) -> None:
        with self.recorder(independence_fixture()) as recorder:
            missing = outcome("missing-broker-cost")
            self.evidence_store.supersede(
                missing,
                lambda evidence: evidence.__setitem__("broker_cost_evidence", None),
            )
            stored_missing = recorder.record(missing)
            self.assertFalse(stored_missing.diagnostic_eligible)
            self.assertIn(
                "BROKER_COST_EVIDENCE_MISSING",
                stored_missing.exclusion_reasons,
            )

            forged = outcome("forged-broker-cost")
            def forge_broker_cost(evidence: dict[str, object]) -> None:
                broker_cost = evidence["broker_cost_evidence"]
                component = broker_cost["components"]["fees_usd"]
                component["amount_usd"] = "0.00"
                component["evidence_hash"] = canonical_hash(
                    {
                        key: value
                        for key, value in component.items()
                        if key != "evidence_hash"
                    }
                )
                broker_cost["evidence_hash"] = canonical_hash(
                    {
                        key: value
                        for key, value in broker_cost.items()
                        if key != "evidence_hash"
                    }
                )

            self.evidence_store.supersede(forged, forge_broker_cost)
            stored_forged = recorder.record(forged)
            self.assertFalse(stored_forged.diagnostic_eligible)
            self.assertIn(
                "BROKER_COST_EVIDENCE_INVALID",
                stored_forged.exclusion_reasons,
            )

    def test_out_of_order_insert_and_late_data_use_one_cluster_projection(self) -> None:
        shared = {
            "status": "KNOWN",
            "ticker": "AAPL",
            "event_id": "aapl-earnings",
        }
        with self.recorder(independence_fixture()) as recorder:
            later = recorder.record(
                outcome(
                    "later",
                    decision_at=BASE + timedelta(hours=2),
                    cluster_evidence={
                        **shared,
                        "slot_at": BASE + timedelta(hours=2),
                    },
                )
            )
            earlier = recorder.record(
                outcome(
                    "earlier",
                    decision_at=BASE,
                    cluster_evidence={**shared, "slot_at": BASE},
                )
            )

            heads = {
                item.decision_id: item for item in recorder.query(latest_only=True)
            }
            self.assertEqual(
                heads["decision-earlier"].cluster_id,
                heads["decision-later"].cluster_id,
            )
            self.assertEqual(
                "decision-earlier",
                heads["decision-later"].representative_decision_id,
            )
            self.assertEqual(1, heads["decision-earlier"].diagnostic_count_weight)
            self.assertEqual(0, heads["decision-later"].diagnostic_count_weight)

            correction = outcome(
                "earlier",
                revision_received_at=BASE + timedelta(days=1, hours=3),
                cluster_evidence={**shared, "slot_at": BASE},
            )
            correction["exit_rule_hits"] = ["MANUAL_REVIEW"]
            recorder.record(correction)
            evaluation = {
                item.decision_id: item for item in recorder.evaluation_inputs()
            }
            stored = {
                item.decision_id: item for item in recorder.query(latest_only=True)
            }
            for decision_id in evaluation:
                self.assertEqual(
                    stored[decision_id].cluster_id,
                    evaluation[decision_id].cluster_id,
                )
                self.assertEqual(
                    stored[decision_id].diagnostic_count_weight,
                    evaluation[decision_id].diagnostic_count_weight,
                )
            self.assertNotEqual(later.cluster_id, stored["decision-later"].cluster_id)
            self.assertEqual(1, earlier.diagnostic_count_weight)

    def test_known_requires_ticker_and_event_identity_is_namespaced(self) -> None:
        with self.recorder(independence_fixture()) as recorder:
            with self.assertRaisesRegex(OutcomeValidationError, "KNOWN.*ticker"):
                recorder.record(
                    outcome(
                        "ticker-missing",
                        cluster_evidence={
                            "status": "KNOWN",
                            "event_id": "same-event",
                        },
                    )
                )

            first = recorder.record(
                outcome(
                    "event-a",
                    cluster_evidence={
                        "status": "KNOWN",
                        "ticker": "SPY",
                        "event_id": "global-event-id",
                    },
                )
            )
            second = recorder.record(
                outcome(
                    "event-b",
                    cluster_evidence={
                        "status": "KNOWN",
                        "ticker": "QQQ",
                        "event_id": "global-event-id",
                    },
                )
            )
            self.assertNotEqual(first.cluster_id, second.cluster_id)
            self.assertEqual(1, second.diagnostic_count_weight)

            same_namespace = recorder.record(
                outcome(
                    "event-c",
                    cluster_evidence={
                        "status": "KNOWN",
                        "ticker": "SPY",
                        "issuer_id": "issuer-SPY",
                        "provider": "TEST_PROVIDER",
                        "event_id": "global-event-id",
                    },
                )
            )
            self.assertEqual(first.cluster_id, same_namespace.cluster_id)
            self.assertEqual(0, same_namespace.diagnostic_count_weight)

            another_provider = recorder.record(
                outcome(
                    "event-d",
                    decision_at=BASE + timedelta(hours=5),
                    cluster_evidence={
                        "status": "KNOWN",
                        "ticker": "SPY",
                        "issuer_id": "issuer-SPY",
                        "provider": "ANOTHER_PROVIDER",
                        "event_id": "global-event-id",
                        "slot_at": BASE + timedelta(hours=5),
                    },
                )
            )
            self.assertNotEqual(first.cluster_id, another_provider.cluster_id)

    def test_recorded_at_is_content_hashed_and_tampering_is_detected(self) -> None:
        first_path = self.path
        second_path = Path(self.temp.name) / "other.db"
        first_clock = lambda: BASE + timedelta(days=2)
        second_clock = lambda: BASE + timedelta(days=2, seconds=1)
        with OutcomeRecorder(
            first_path,
            independence_spec=independence_fixture(),
            allow_test_fixture=True,
            execution_cost_resolver=FixtureCostAuthority(),
            broker_outcome_evidence_store=self.evidence_store,
            clock=first_clock,
        ) as recorder:
            first = recorder.record(outcome("recorded-at"))
        with OutcomeRecorder(
            second_path,
            independence_spec=independence_fixture(),
            allow_test_fixture=True,
            execution_cost_resolver=FixtureCostAuthority(),
            broker_outcome_evidence_store=self.evidence_store,
            clock=second_clock,
        ) as recorder:
            second = recorder.record(outcome("recorded-at"))
        self.assertNotEqual(first.content_hash, second.content_hash)

        connection = sqlite3.connect(first_path)
        try:
            connection.execute("DROP TRIGGER outcome_records_no_update")
            connection.execute(
                "UPDATE outcome_records SET recorded_at=? WHERE sequence=1",
                ((BASE + timedelta(days=3)).isoformat(),),
            )
            connection.commit()
        finally:
            connection.close()
        with self.assertRaisesRegex(Exception, "content hash"):
            with self.recorder(independence_fixture()) as recorder:
                recorder.verify_integrity()

    def test_records_separate_pnl_cost_and_quote_evidence_append_only(self) -> None:
        with self.recorder(independence_fixture()) as recorder:
            stored = recorder.record(outcome("cost-flip"))

            self.assertEqual(Decimal("20.00"), stored.mfe_usd)
            self.assertEqual(Decimal("-10.00"), stored.mae_usd)
            self.assertEqual(Decimal("18.00"), stored.mark_pnl_before_costs_usd)
            self.assertEqual(
                Decimal("8.00"), stored.executable_exit_pnl_before_costs_usd
            )
            self.assertEqual(Decimal("21.50"), stored.total_cost_usd)
            self.assertEqual(
                Decimal("-13.50"), stored.executable_exit_pnl_after_costs_usd
            )
            self.assertEqual("COST_FLIPPED_NEGATIVE", stored.pnl_classification)
            self.assertEqual(("PROFIT_TAKE",), stored.exit_rule_hits)
            self.assertEqual("EXECUTABLE", stored.quote_quality_status)
            self.assertFalse(stored.evaluation_eligible)
            self.assertTrue(stored.diagnostic_eligible)
            self.assertTrue(stored.independence_test_only)
            self.assertEqual(0, stored.count_weight)
            self.assertEqual(1, stored.diagnostic_count_weight)
            self.assertEqual(
                cost_resolution("cost-flip").candidates[0].calculation_hash,
                stored.candidate_cost_calculation_hash,
            )
            self.assertTrue(recorder.verify_integrity())

        connection = sqlite3.connect(self.path)
        try:
            with self.assertRaisesRegex(sqlite3.IntegrityError, "immutable"):
                connection.execute(
                    "UPDATE outcome_records SET decision_id='tampered' WHERE sequence=1"
                )
            with self.assertRaisesRegex(sqlite3.IntegrityError, "immutable"):
                connection.execute("DELETE FROM outcome_records WHERE sequence=1")
        finally:
            connection.close()

    def test_late_data_appends_superseding_version_and_preserves_original(self) -> None:
        with self.recorder(independence_fixture()) as recorder:
            first = recorder.record(outcome("late"))
            corrected = outcome(
                "late",
                revision_received_at=BASE + timedelta(days=1, hours=2),
            )
            corrected["exit_rule_hits"] = ["MANUAL_REVIEW"]
            def improve_exit(evidence: dict[str, object]) -> None:
                long_exit = evidence["valuation_evidence"]["legs"][0]["exit_quote"]
                long_exit["bid"] = "2.68"
                long_exit["ask"] = "2.78"
                rehash_market_evidence(evidence)

            self.evidence_store.supersede(corrected, improve_exit)
            corrected["mfe_usd"] = "28.00"
            corrected["mark_pnl_before_costs_usd"] = "28.00"
            corrected["executable_exit_pnl_before_costs_usd"] = "18.00"
            second = recorder.record(corrected)

            self.assertEqual(1, first.version)
            self.assertEqual(2, second.version)
            self.assertEqual(first.outcome_id, second.supersedes_outcome_id)
            self.assertEqual(first.content_hash, second.supersedes_hash)
            self.assertEqual(
                Decimal("18.00"),
                second.executable_exit_pnl_before_costs_usd,
            )
            self.assertEqual(
                (first.outcome_id, second.outcome_id),
                tuple(
                    item.outcome_id
                    for item in recorder.history(first.base_identity_hash)
                ),
            )
            self.assertEqual(2, recorder.count())

            changed_economic_time = outcome(
                "late",
                economic_observed_at=BASE + timedelta(days=1, seconds=2),
                revision_received_at=BASE + timedelta(days=1, hours=3),
            )
            changed_economic_time["exit_rule_hits"] = ["LATE_CORRECTION"]
            with self.assertRaisesRegex(
                OutcomeIdentityConflict,
                "economic_observed_at",
            ):
                recorder.record(changed_economic_time)

        with self.recorder(independence_fixture()) as reopened:
            self.assertEqual(2, reopened.latest(first.base_identity_hash).version)
            self.assertTrue(reopened.verify_integrity())

    def test_late_representative_keeps_the_cluster_weight(self) -> None:
        shared = {
            "status": "KNOWN",
            "ticker": "SPY",
            "event_id": "cpi-release",
            "slot_at": BASE,
        }
        with self.recorder(independence_fixture()) as recorder:
            representative = recorder.record(
                outcome("representative", cluster_evidence=shared)
            )
            duplicate = recorder.record(
                outcome(
                    "duplicate",
                    decision_at=BASE + timedelta(minutes=30),
                    cluster_evidence={
                        **shared,
                        "slot_at": BASE + timedelta(minutes=30),
                    },
                )
            )
            late = recorder.record(
                outcome(
                    "representative",
                    revision_received_at=BASE + timedelta(days=1, hours=3),
                    cluster_evidence=shared,
                )
            )

            self.assertEqual(0, representative.count_weight)
            self.assertEqual(0, duplicate.count_weight)
            self.assertEqual(0, late.count_weight)
            self.assertEqual(1, representative.diagnostic_count_weight)
            self.assertEqual(0, duplicate.diagnostic_count_weight)
            self.assertEqual(1, late.diagnostic_count_weight)
            aggregate = recorder.aggregate()
            self.assertEqual(0, aggregate.independent_count)
            self.assertEqual(1, aggregate.diagnostic_cluster_count)

    def test_bound_identity_drift_is_rejected_while_new_candidate_is_distinct(
        self,
    ) -> None:
        with self.recorder(independence_fixture()) as recorder:
            original = recorder.record(outcome("identity"))
            for field, changed in (
                ("current_policy_hash", "a" * 64),
                ("cost_hash", "b" * 64),
                ("exit_policy_hash", "c" * 64),
                ("ranking_snapshot_hash", "d" * 64),
            ):
                document = outcome(
                    "identity",
                    revision_received_at=BASE + timedelta(days=1, hours=2),
                )
                document[field] = changed
                with self.assertRaises(OutcomeIdentityConflict, msg=field):
                    recorder.record(document)

            distinct = recorder.record(
                outcome(
                    "identity",
                    candidate_hash="e" * 64,
                    ranking_snapshot_id="ranking-identity-2",
                    ranking_snapshot_hash="f" * 64,
                )
            )
            self.assertNotEqual(
                original.base_identity_hash, distinct.base_identity_hash
            )

    def test_missing_or_mismatched_cost_and_quote_data_cannot_enter_evaluation(
        self,
    ) -> None:
        with self.recorder(independence_fixture()) as recorder:
            missing_cost = outcome("missing-cost", cost_version=None, cost_hash=None)
            self.evidence_store.supersede(
                missing_cost,
                lambda evidence: evidence.__setitem__("costs", {}),
            )
            stored_missing = recorder.record(missing_cost)
            self.assertFalse(stored_missing.evaluation_eligible)
            self.assertIn("COST_CONTRACT_MISSING", stored_missing.exclusion_reasons)
            self.assertIn("COST_COMPONENTS_MISSING", stored_missing.exclusion_reasons)

            mismatch = recorder.record(outcome("mismatch-cost", cost_hash="a" * 64))
            self.assertFalse(mismatch.evaluation_eligible)
            self.assertIn("COST_CONTRACT_MISMATCH", mismatch.exclusion_reasons)

            no_quote = outcome("no-quote", quote_quality={})
            no_quote["executable_exit_pnl_before_costs_usd"] = None
            no_quote.pop("broker_outcome_evidence_ref")
            stored_quote = recorder.record(no_quote)
            self.assertEqual("MISSING", stored_quote.quote_quality_status)
            self.assertIsNone(stored_quote.executable_exit_pnl_after_costs_usd)
            self.assertIn(
                "VALUATION_EVIDENCE_MISSING",
                stored_quote.exclusion_reasons,
            )

            aggregate = recorder.aggregate()
            self.assertEqual(0, aggregate.eligible_outcomes)
            self.assertEqual(0, aggregate.independent_count)

    def test_signed_rules_cluster_duplicates_and_exclude_unknown(self) -> None:
        documents = [
            outcome(
                "same-event-a",
                cluster_evidence={
                    "status": "KNOWN",
                    "ticker": "AAPL",
                    "event_id": "earnings-aapl-q3",
                    "slot_at": BASE,
                },
            ),
            outcome(
                "same-event-b",
                decision_at=BASE + timedelta(minutes=30),
                cluster_evidence={
                    "status": "KNOWN",
                    "ticker": "AAPL",
                    "event_id": "earnings-aapl-q3",
                    "slot_at": BASE + timedelta(minutes=30),
                },
            ),
            outcome(
                "adjacent-a",
                cluster_evidence={
                    "status": "KNOWN",
                    "ticker": "MSFT",
                    "event_id": "scan-open",
                    "slot_at": BASE,
                },
            ),
            outcome(
                "adjacent-b",
                decision_at=BASE + timedelta(hours=2),
                cluster_evidence={
                    "status": "KNOWN",
                    "ticker": "MSFT",
                    "event_id": "scan-midday",
                    "slot_at": BASE + timedelta(hours=2),
                },
            ),
            outcome(
                "overlap-a",
                cluster_evidence={
                    "status": "KNOWN",
                    "ticker": "NVDA",
                    "event_id": "window-a",
                    "window_start": BASE,
                    "window_end": BASE + timedelta(hours=3),
                    "slot_at": BASE,
                },
            ),
            outcome(
                "overlap-b",
                decision_at=BASE + timedelta(hours=3),
                cluster_evidence={
                    "status": "KNOWN",
                    "ticker": "NVDA",
                    "event_id": "window-b",
                    "window_start": BASE + timedelta(hours=1),
                    "window_end": BASE + timedelta(hours=4),
                    "slot_at": BASE + timedelta(hours=3),
                },
            ),
            outcome(
                "macro-a",
                cluster_evidence={
                    "status": "KNOWN",
                    "ticker": "JPM",
                    "macro_event_id": "fomc-2026-08",
                    "correlation_group": "US-BANKS",
                    "slot_at": BASE,
                },
            ),
            outcome(
                "macro-b",
                cluster_evidence={
                    "status": "KNOWN",
                    "ticker": "BAC",
                    "macro_event_id": "fomc-2026-08",
                    "correlation_group": "US-BANKS",
                    "slot_at": BASE,
                },
            ),
            outcome(
                "provider-a",
                cluster_evidence={
                    "status": "KNOWN",
                    "ticker": "TSLA",
                    "provider_content_hash": "a" * 64,
                    "slot_at": BASE,
                },
            ),
            outcome(
                "provider-b",
                cluster_evidence={
                    "status": "KNOWN",
                    "ticker": "TSLA",
                    "provider_content_hash": "a" * 64,
                    "slot_at": BASE + timedelta(hours=3),
                },
            ),
            outcome(
                "independent-a",
                cluster_evidence={
                    "status": "KNOWN",
                    "ticker": "GLD",
                    "event_id": "gold-a",
                    "slot_at": BASE,
                },
            ),
            outcome(
                "independent-b",
                cluster_evidence={
                    "status": "KNOWN",
                    "ticker": "IWM",
                    "event_id": "small-cap-b",
                    "slot_at": BASE + timedelta(hours=5),
                },
            ),
            outcome(
                "unknown",
                cluster_evidence={"status": "UNKNOWN", "ticker": "META"},
            ),
        ]

        with self.recorder(independence_fixture()) as recorder:
            stored = tuple(recorder.record(item) for item in documents)
            aggregate = recorder.aggregate()

            self.assertEqual(0, aggregate.independent_count)
            self.assertEqual(7, aggregate.diagnostic_cluster_count)
            self.assertEqual(0, aggregate.eligible_outcomes)
            self.assertEqual(12, aggregate.diagnostic_eligible_outcomes)
            self.assertEqual(1, aggregate.unknown_outcomes)
            self.assertEqual("v1", aggregate.independence_version)
            self.assertEqual(
                independence_fixture().spec_hash, aggregate.independence_hash
            )
            self.assertEqual(0, stored[-1].count_weight)
            self.assertEqual(
                "INDEPENDENCE_UNKNOWN", stored[-1].cluster_exclusion_reason
            )
            evaluations = recorder.evaluation_inputs()
            self.assertEqual(0, sum(item.count_weight for item in evaluations))
            self.assertEqual(
                7, sum(item.diagnostic_count_weight for item in evaluations)
            )
            self.assertTrue(all(item.test_only for item in evaluations))
            self.assertTrue(
                all(
                    item.independence_hash == independence_fixture().spec_hash
                    for item in evaluations
                )
            )

    def test_no_spec_fails_closed_and_v1_v2_never_mix(self) -> None:
        with self.recorder() as recorder:
            unbound = recorder.record(outcome("unbound"))
            self.assertFalse(unbound.evaluation_eligible)
            self.assertEqual(0, unbound.count_weight)
            with self.assertRaises(IndependenceSpecUnavailable):
                recorder.aggregate()

        with self.recorder(independence_fixture("v1")) as recorder:
            recorder.record(outcome("v1"))
        with self.recorder(independence_fixture("v2")) as recorder:
            recorder.record(outcome("v2"))
            with self.assertRaises(MixedIndependenceSpecError):
                recorder.aggregate()
            v2 = recorder.aggregate(
                independence_version="v2",
                independence_hash=independence_fixture("v2").spec_hash,
            )
            self.assertEqual(0, v2.independent_count)
            self.assertEqual(1, v2.diagnostic_cluster_count)


if __name__ == "__main__":
    unittest.main()
