from __future__ import annotations

import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

import options_copilot.learning.outcome_processor as outcome_processor_module

from options_copilot.analytics.scenarios import ResolvedPolicy
from options_copilot.ranking.basis import build_ranking_basis
from options_copilot.ranking.store import (
    RankingStore,
    RankingStoreConflict,
    RankingStoreCorruption,
)
from options_copilot.risk.authorization import RiskAuthorityTier, RiskTierAuthority
from options_copilot.storage.canonical import canonical_hash, canonical_json, freeze_json


NOW = datetime(2026, 8, 4, tzinfo=timezone.utc)
INPUT_HASH = "a" * 64
EVIDENCE_HASH = "b" * 64
BROKER_HASH = "c" * 64
POLICY_HASH = "d" * 64
POLICY_MARKER_HASH = "1" * 64
COST_HASH = "e" * 64
RISK_CONTRACT_HASH = "2" * 64


class _Resolver:
    def __init__(self, value: object) -> None:
        self.value = value
        self.resolve_calls = 0
        self.resolve_kwargs: list[dict[str, object]] = []
        self.current = True

    def resolve(self, **kwargs: object) -> object:
        self.resolve_calls += 1
        self.resolve_kwargs.append(dict(kwargs))
        return self.value

    def is_current(self, value: object) -> bool:
        return self.current and value == self.value

    def guard_current(self, value: object, *, callback):
        if not self.is_current(value):
            return None
        return callback()


class _ResolveOnly:
    def __init__(self, value: object) -> None:
        self.value = value

    def resolve(self, **_: object) -> object:
        return self.value


def _policy() -> ResolvedPolicy:
    return ResolvedPolicy(
        "v1",
        POLICY_HASH,
        POLICY_MARKER_HASH,
        NOW,
        freeze_json({"policy": "initial"}),
        freeze_json({"source": "locked"}),
    )


def _authority() -> RiskTierAuthority:
    return RiskTierAuthority.normal(RISK_CONTRACT_HASH)


def _a_grade_authority(candidate: dict[str, object]) -> RiskTierAuthority:
    return RiskTierAuthority(
        version="v2",
        tier=RiskAuthorityTier.A_GRADE,
        risk_contract_hash=RISK_CONTRACT_HASH,
        risk_authority_marker_hash="7" * 64,
        current_policy_version="v1",
        current_policy_hash=POLICY_HASH,
        policy_authority_marker_hash=POLICY_MARKER_HASH,
        proposal_hash=str(candidate["proposal_hash"]),
        candidate_hash=str(candidate["candidate_hash"]),
        execution_cost_version="v1",
        execution_cost_hash=COST_HASH,
        ranking_basis_hash=str(candidate["ranking_basis_hash"]),
        a_grade_approved=True,
        actor="human:owner",
        signed_at=NOW,
        expires_at=NOW + timedelta(hours=1),
    )


def _evidence_inputs(**changes: object) -> dict[str, object]:
    values: dict[str, object] = {
        "input_hash": INPUT_HASH,
        "evidence_hash": EVIDENCE_HASH,
        "broker_snapshot_hash": BROKER_HASH,
        "volatility_evidence_hash": "9" * 64,
    }
    values.update(changes)
    return values


def _candidate_body(candidate_id: str) -> dict[str, object]:
    return {
        "candidate_id": candidate_id,
        "underlying": "SPY",
        "structure": "DEBIT_VERTICAL",
        "thesis": "UP",
        "max_loss": Decimal("100"),
        "legs": [
            {
                "con_id": 101,
                "action": "BUY",
                "ratio": 1,
                "right": "C",
                "strike": Decimal("500"),
            },
            {
                "con_id": 102,
                "action": "SELL",
                "ratio": 1,
                "right": "C",
                "strike": Decimal("505"),
            },
        ],
        "quote_snapshot": {
            "observed_at": NOW,
            "bid": Decimal("1.20"),
            "ask": Decimal("1.25"),
        },
    }


def _candidate(
    candidate_id: str,
    *,
    authorizable: bool = True,
    evidence_inputs: dict[str, object] | None = None,
    candidate_body: dict[str, object] | None = None,
    proposal_body: dict[str, object] | None = None,
) -> dict[str, object]:
    body = candidate_body or _candidate_body(candidate_id)
    evidence = evidence_inputs or _evidence_inputs()
    binding = build_ranking_basis(
        candidate_body=body,
        proposal_body=proposal_body,
        current_policy_version="v1",
        current_policy_hash=POLICY_HASH,
        policy_authority_marker_hash=POLICY_MARKER_HASH,
        cost_version="v1",
        cost_hash=COST_HASH,
        risk_contract_hash=RISK_CONTRACT_HASH,
        evidence_inputs=evidence,
    )
    return {
        "candidate_id": candidate_id,
        "candidate_body": body,
        "proposal_body": proposal_body,
        "proposal_hash": binding.proposal_hash,
        "candidate_hash": binding.candidate_hash,
        "ranking_basis_hash": binding.ranking_basis_hash,
        "evidence_inputs": evidence,
        "authority_status": "NORMAL" if authorizable else "A_GRADE_PENDING",
        "authorizable": authorizable,
        "score_components": {"net_ev": Decimal("10")},
    }


def _append(
    store: RankingStore,
    scan_run_id: str,
    input_hash: str = INPUT_HASH,
    *,
    candidates: tuple[dict[str, object], ...] | None = None,
    governance_evidence: tuple[dict[str, object], ...] = (),
    policy_resolver: object | None = None,
    risk_resolver: object | None = None,
    resolved_policy: object | None = None,
    risk_authority: object | None = None,
    decision_records: tuple[dict[str, object], ...] = (),
):
    policy_resolver = policy_resolver or _Resolver(_policy())
    resolved_risk_authority = risk_authority or _authority()
    risk_resolver = risk_resolver or _Resolver(resolved_risk_authority)
    return store.append_snapshot(
        scan_run_id=scan_run_id,
        input_hash=input_hash,
        evidence_hash=EVIDENCE_HASH,
        broker_snapshot_hash=BROKER_HASH,
        policy_version="v1",
        policy_hash=POLICY_HASH,
        policy_authority_marker_hash=POLICY_MARKER_HASH,
        cost_version="v1",
        cost_hash=COST_HASH,
        risk_contract_hash=RISK_CONTRACT_HASH,
        risk_authority_version=resolved_risk_authority.version,
        risk_authority_marker_hash=resolved_risk_authority.marker_hash,
        valid_until=NOW + timedelta(minutes=1),
        candidates=candidates if candidates is not None else (_candidate("one"),),
        governance_evidence=governance_evidence,
        policy_resolver=policy_resolver,
        risk_authority_resolver=risk_resolver,
        resolved_policy=resolved_policy or _policy(),
        risk_authority=resolved_risk_authority,
        decision_records=decision_records,
        now=NOW,
    )


def test_store_hash_chain_requires_full_current_authority_and_rank_one(tmp_path) -> None:
    with RankingStore(tmp_path / "rankings.sqlite") as store:
        snapshot = _append(
            store,
            "scan-1",
            candidates=(_candidate("one"), _candidate("two")),
        )
        assert store.verify_integrity()
        assert store.authorize_frozen_rank_one(
            snapshot.ranking_snapshot_id,
            "one",
            snapshot.expected_hashes,
            now=NOW,
        ) is None
        assert store.authorize_frozen_rank_one(
            snapshot.ranking_snapshot_id,
            "one",
            snapshot.expected_hashes,
            policy_resolver=_ResolveOnly(_policy()),
            risk_authority_resolver=_Resolver(_authority()),
            now=NOW,
        ) is None
        subset = {"candidate_hash": snapshot.expected_hashes["candidate_hash"]}
        assert store.authorize_frozen_rank_one(
            snapshot.ranking_snapshot_id,
            "one",
            subset,
            policy_resolver=_Resolver(_policy()),
            risk_authority_resolver=_Resolver(_authority()),
            now=NOW,
        ) is None

        policy_resolver = _Resolver(_policy())
        risk_resolver = _Resolver(_authority())
        authorized = store.authorize_frozen_rank_one(
            snapshot.ranking_snapshot_id,
            "one",
            snapshot.expected_hashes,
            policy_resolver=policy_resolver,
            risk_authority_resolver=risk_resolver,
            now=NOW,
        )
        assert authorized is not None
        assert authorized.candidate_id == "one"
        assert authorized.current_policy_hash == POLICY_HASH
        assert authorized.risk_authority_marker_hash == _authority().marker_hash
        assert policy_resolver.resolve_calls == risk_resolver.resolve_calls == 1
        assert risk_resolver.resolve_kwargs == [
            {
                "now": NOW,
                "current_policy": _policy(),
                "resolved_policy": _policy(),
                "proposal_hash": authorized.proposal_hash,
                "candidate_hash": authorized.candidate_hash,
                "execution_cost_version": authorized.cost_version,
                "execution_cost_hash": authorized.cost_hash,
                "ranking_basis_hash": authorized.ranking_basis_hash,
            }
        ]
        assert store.authorize_frozen_rank_one(
            snapshot.ranking_snapshot_id,
            "two",
            snapshot.expected_hashes,
            policy_resolver=_Resolver(_policy()),
            risk_authority_resolver=_Resolver(_authority()),
            now=NOW,
        ) is None


def test_store_persists_ten_ranked_candidates_but_only_rank_one_authorizes(
    tmp_path,
) -> None:
    candidates = tuple(_candidate(f"candidate-{index}") for index in range(1, 11))
    with RankingStore(tmp_path / "top10.sqlite") as store:
        snapshot = _append(store, "scan-top10", candidates=candidates)

        ranks = tuple(
            row[0]
            for row in store._connection.execute(
                "SELECT rank FROM ranking_rows "
                "WHERE ranking_snapshot_id=? ORDER BY rank",
                (snapshot.ranking_snapshot_id,),
            ).fetchall()
        )
        assert ranks == tuple(range(1, 11))
        assert store.verify_integrity()
        assert store.authorize_frozen_rank_one(
            snapshot.ranking_snapshot_id,
            "candidate-10",
            snapshot.expected_hashes,
            policy_resolver=_Resolver(_policy()),
            risk_authority_resolver=_Resolver(_authority()),
            now=NOW,
        ) is None
        assert store.authorize_frozen_rank_one(
            snapshot.ranking_snapshot_id,
            "candidate-1",
            snapshot.expected_hashes,
            policy_resolver=_Resolver(_policy()),
            risk_authority_resolver=_Resolver(_authority()),
            now=NOW,
        ) is not None


def test_outcome_targets_cover_historical_snapshots_and_all_ten_ranks(
    tmp_path,
) -> None:
    first_candidates = tuple(
        _candidate(f"candidate-{index}") for index in range(1, 11)
    )
    second_candidates = tuple(
        _candidate(f"candidate-{index}") for index in range(1, 4)
    )
    with RankingStore(tmp_path / "outcome-targets.sqlite") as store:
        first = _append(store, "scan-outcome-first", candidates=first_candidates)
        second = _append(store, "scan-outcome-second", candidates=second_candidates)
        integrity_calls = 0
        verify = store.assert_integrity

        def counted_integrity() -> None:
            nonlocal integrity_calls
            integrity_calls += 1
            verify()

        store.assert_integrity = counted_integrity  # type: ignore[method-assign]
        targets = store.outcome_targets()

        assert integrity_calls == 1
        assert len(targets) == 13
        assert [
            target["outcome_template"]["ranking_snapshot_id"]  # type: ignore[index]
            for target in targets
        ] == [first.ranking_snapshot_id] * 10 + [second.ranking_snapshot_id] * 3
        assert [
            target["outcome_template"]["candidate_id"]  # type: ignore[index]
            for target in targets[:10]
        ] == [f"candidate-{index}" for index in range(1, 11)]
        assert len(
            {
                (target["subject_id"], target["subject_hash"])
                for target in targets
            }
        ) == 13
        assert all(
            target["outcome_template"]["outcome_subject_id"]  # type: ignore[index]
            == target["subject_id"]
            and target["outcome_template"]["outcome_subject_hash"]  # type: ignore[index]
            == target["subject_hash"]
            for target in targets
        )
        assert all(
            target["capture_plan"]["status"] == "BLOCKED"  # type: ignore[index]
            and "OUTCOME_CAPTURE_BASELINE_UNAVAILABLE"
            in target["capture_plan"]["reason_codes"]  # type: ignore[index]
            for target in targets
        )


def test_outcome_target_thaws_decimal_candidate_into_result_authority(
    tmp_path,
) -> None:
    body = _candidate_body("candidate-authority")
    body.update(
        {
            "debit_usd": Decimal("210.00"),
            "credit_usd": Decimal("100.00"),
            "estimated_commissions_usd": Decimal("5.00"),
            "estimated_slippage_usd": Decimal("15.00"),
            "max_loss_usd": Decimal("130.00"),
        }
    )
    body["legs"] = [
        {**body["legs"][0], "side": "LONG", "quantity": 1},
        {**body["legs"][1], "side": "SHORT", "quantity": 1},
    ]
    candidate = _candidate("candidate-authority", candidate_body=body)
    with RankingStore(tmp_path / "outcome-authority.sqlite") as store:
        _append(store, "scan-outcome-authority", candidates=(candidate,))
        target = store.outcome_targets()[0]

    authority = target["result_authority"]
    assert authority["candidate_hash"] == candidate["candidate_hash"]
    assert authority["cost_contract_hash"] == COST_HASH
    assert authority["entry_value_usd"] == Decimal("110.00")
    assert authority["costs_usd"] == Decimal("20.00")
    assert authority["max_loss_usd"] == Decimal("130.00")
    assert authority["legs"] == (
        {"contract_id": "101", "side": "BUY", "quantity": 1},
        {"contract_id": "102", "side": "SELL", "quantity": 1},
    )
    assert authority["authority_hash"] == canonical_hash(
        {key: value for key, value in authority.items() if key != "authority_hash"}
    )
def test_real_ranking_cursor_verifies_once_then_pages_new_snapshots_and_guards_triggers(
    tmp_path,
) -> None:
    with RankingStore(tmp_path / "ranking-cursor.sqlite") as store:
        first = _append(store, "scan-cursor-first", candidates=(_candidate("first"),))
        integrity_calls = 0
        full_target_calls = 0
        verify = store.assert_integrity
        full_targets = store.outcome_targets

        def counted_integrity() -> None:
            nonlocal integrity_calls
            integrity_calls += 1
            verify()

        def counted_full_targets(**kwargs):
            nonlocal full_target_calls
            full_target_calls += 1
            return full_targets(**kwargs)

        store.assert_integrity = counted_integrity  # type: ignore[method-assign]
        store.outcome_targets = counted_full_targets  # type: ignore[method-assign]
        provider_type = getattr(
            outcome_processor_module,
            "RankingOutcomeTargetCursor",
            None,
        )
        assert provider_type is not None
        provider = provider_type(store)

        startup = provider(after_sequence=0)
        for _ in range(5):
            assert provider(after_sequence=0) == ()

        assert len(startup) == 1
        assert startup[0]["binding_context"]["ranking_snapshot_id"] == first.ranking_snapshot_id
        assert integrity_calls == 1
        assert full_target_calls == 0

        _append(store, "scan-cursor-second", candidates=(_candidate("second"),))
        calls_after_append = integrity_calls
        appended = provider(after_sequence=0)
        assert [item["binding_context"]["candidate_id"] for item in appended] == ["second"]
        assert provider(after_sequence=0) == ()
        assert integrity_calls == calls_after_append

        store._connection.execute("DROP TRIGGER ranking_rows_no_update")
        with pytest.raises(RankingStoreCorruption, match="trigger"):
            provider(after_sequence=0)


def test_guard_revalidates_exact_a_grade_frozen_row_bindings(tmp_path) -> None:
    candidate = _candidate("a-grade")
    candidate["authority_status"] = "A_GRADE"
    candidate["authorizable"] = True
    authority = _a_grade_authority(candidate)
    with RankingStore(tmp_path / "a-grade-bindings.sqlite") as store:
        snapshot = _append(
            store,
            "a-grade-bindings",
            candidates=(candidate,),
            risk_authority=authority,
        )
        callbacks: list[str] = []
        result = store.guard_frozen_rank_one(
            snapshot.ranking_snapshot_id,
            "a-grade",
            policy_resolver=_Resolver(_policy()),
            risk_authority_resolver=_Resolver(authority),
            execution_cost_contract=_Resolver(
                {"cost_version": "v1", "cost_hash": COST_HASH}
            ),
            callback=lambda _authorization: callbacks.append("called") or "authorized",
            now=NOW,
        )

        assert result == "authorized"
        assert callbacks == ["called"]

        wrong = replace(
            authority,
            proposal_hash="0" * 64,
            candidate_hash="8" * 64,
            execution_cost_hash="9" * 64,
            ranking_basis_hash="a" * 64,
        )
        callbacks.clear()
        assert store.guard_frozen_rank_one(
            snapshot.ranking_snapshot_id,
            "a-grade",
            policy_resolver=_Resolver(_policy()),
            risk_authority_resolver=_Resolver(wrong),
            execution_cost_contract=_Resolver(
                {"cost_version": "v1", "cost_hash": COST_HASH}
            ),
            callback=lambda _authorization: callbacks.append("called") or "bad",
            now=NOW,
        ) is None
        assert callbacks == []


def test_guard_rejects_is_current_only_resolvers_without_invoking_callback(
    tmp_path,
) -> None:
    class IsCurrentOnly:
        def __init__(self, value):
            self.value = value

        def resolve(self, **_):
            return self.value

        def is_current(self, value):
            return value == self.value

    candidate = _candidate("unguarded")
    with RankingStore(tmp_path / "unguarded-resolver.sqlite") as store:
        snapshot = _append(store, "unguarded-resolver", candidates=(candidate,))
        callbacks = []
        result = store.guard_frozen_rank_one(
            snapshot.ranking_snapshot_id,
            "unguarded",
            policy_resolver=IsCurrentOnly(_policy()),
            risk_authority_resolver=IsCurrentOnly(_authority()),
            execution_cost_contract=IsCurrentOnly(
                {"cost_version": "v1", "cost_hash": COST_HASH}
            ),
            callback=lambda _: callbacks.append("called"),
            now=NOW,
        )

    assert result is None
    assert callbacks == []


def test_append_rejects_missing_resolve_only_mismatched_or_stale_authority(tmp_path) -> None:
    cases = (
        {},
        {
            "policy_resolver": _ResolveOnly(_policy()),
            "risk_authority_resolver": _Resolver(_authority()),
            "resolved_policy": _policy(),
            "risk_authority": _authority(),
        },
        {
            "policy_resolver": _Resolver(_policy()),
            "risk_authority_resolver": _Resolver(_authority()),
            "resolved_policy": ResolvedPolicy(
                "v2",
                "f" * 64,
                POLICY_MARKER_HASH,
                NOW,
                freeze_json({"policy": "other"}),
                freeze_json({}),
            ),
            "risk_authority": _authority(),
        },
    )
    for index, authority_kwargs in enumerate(cases):
        with RankingStore(tmp_path / f"reject-{index}.sqlite") as store:
            with pytest.raises(RankingStoreConflict):
                store.append_snapshot(
                    scan_run_id=f"reject-{index}",
                    input_hash=INPUT_HASH,
                    evidence_hash=EVIDENCE_HASH,
                    broker_snapshot_hash=BROKER_HASH,
                    policy_version="v1",
                    policy_hash=POLICY_HASH,
                    policy_authority_marker_hash=POLICY_MARKER_HASH,
                    cost_version="v1",
                    cost_hash=COST_HASH,
                    risk_contract_hash=RISK_CONTRACT_HASH,
                    risk_authority_version="v1",
                    risk_authority_marker_hash=_authority().marker_hash,
                    valid_until=NOW + timedelta(minutes=1),
                    candidates=(_candidate("one"),),
                    now=NOW,
                    **authority_kwargs,
                )
            assert store.record_counts()["ranking_rows"] == 0

    stale = _Resolver(_policy())
    stale.current = False
    with RankingStore(tmp_path / "stale.sqlite") as store:
        with pytest.raises(RankingStoreConflict):
            _append(store, "stale", policy_resolver=stale)
        assert store.record_counts()["ranking_rows"] == 0


def test_snapshot_atomically_binds_scenarios_and_store_generated_trade_terminal(tmp_path) -> None:
    scenario = {
        "record_type": "SCENARIO",
        "record": {
            "scan_run_id": "scenario-scan",
            "candidate_id": "one",
            "candidate_hash": _candidate("one")["candidate_hash"],
            "scenario": {"action": "TRADE", "probability": Decimal("0.6")},
        },
    }
    with RankingStore(tmp_path / "scenario.sqlite") as store:
        snapshot = _append(
            store,
            "scenario-scan",
            decision_records=(scenario,),
        )
        decisions = store.read_decisions("scenario-scan")
        assert [item.record_type for item in decisions] == ["SCENARIO", "TRADE"]
        assert decisions[-1].record == {
            "schema": "options_copilot.ranking_finalized.v1",
            "status": "TRADE",
            "event": "RANKING_FINALIZED",
            "scan_run_id": "scenario-scan",
            "ranking_snapshot_id": snapshot.ranking_snapshot_id,
            "ranking_snapshot_hash": snapshot.snapshot_hash,
        }
        read_model = store.read_snapshot(snapshot.ranking_snapshot_id)
        assert read_model["decision_records"][0]["record_type"] == "SCENARIO"
        assert read_model["decision_hash"] == decisions[-1].decision_hash
        assert read_model["record_hash"] == decisions[-1].record_hash
        assert store.verify_integrity()


def test_same_scan_idempotent_restart_and_different_binding_conflicts(tmp_path) -> None:
    path = tmp_path / "conflict.sqlite"
    with RankingStore(path) as store:
        first = _append(store, "same")
        assert _append(store, "same").ranking_snapshot_id == first.ranking_snapshot_id
        different_body = _candidate_body("one")
        different_body["legs"][0]["strike"] = Decimal("501")
        with pytest.raises(RankingStoreConflict):
            _append(
                store,
                "same",
                candidates=(_candidate("one", candidate_body=different_body),),
            )
    with RankingStore(path) as restarted:
        assert restarted.verify_integrity()
        assert _append(restarted, "same").ranking_snapshot_id == first.ranking_snapshot_id


def test_candidate_leg_quote_proposal_and_evidence_tamper_reject(tmp_path) -> None:
    valid = _candidate("one")

    leg_tamper = deepcopy(valid)
    leg_tamper["candidate_body"]["legs"][0]["ratio"] = 2
    with RankingStore(tmp_path / "leg.sqlite") as store:
        with pytest.raises(ValueError, match="candidate_hash"):
            _append(store, "leg", candidates=(leg_tamper,))

    quote_tamper = deepcopy(valid)
    quote_tamper["candidate_body"]["quote_snapshot"]["ask"] = Decimal("9.99")
    with RankingStore(tmp_path / "quote.sqlite") as store:
        with pytest.raises(ValueError, match="candidate_hash"):
            _append(store, "quote", candidates=(quote_tamper,))

    proposal_body = {
        "schema": "proposal.v1",
        "candidate_id": "one",
        "limit": Decimal("1.25"),
    }
    proposal_tamper = _candidate("one", proposal_body=proposal_body)
    proposal_tamper["proposal_body"]["limit"] = Decimal("1.30")
    with RankingStore(tmp_path / "proposal.sqlite") as store:
        with pytest.raises(ValueError, match="proposal_hash"):
            _append(store, "proposal", candidates=(proposal_tamper,))

    evidence_tamper = _candidate(
        "one",
        evidence_inputs=_evidence_inputs(broker_snapshot_hash="f" * 64),
    )
    with RankingStore(tmp_path / "evidence.sqlite") as store:
        with pytest.raises(ValueError, match="evidence_inputs"):
            _append(store, "evidence", candidates=(evidence_tamper,))


def test_two_connections_preserve_one_head_and_detect_persisted_body_tamper(tmp_path) -> None:
    path = tmp_path / "two.sqlite"
    left, right = RankingStore(path), RankingStore(path)
    try:
        with ThreadPoolExecutor(max_workers=2) as workers:
            results = list(
                workers.map(
                    lambda store: _append(store, "parallel"),
                    (left, right),
                )
            )
        assert {item.ranking_snapshot_id for item in results} == {
            results[0].ranking_snapshot_id
        }
        assert left.verify_integrity()
        left._connection.execute("DROP TRIGGER ranking_bases_no_update")
        body = json.loads(
            left._connection.execute(
                "SELECT candidate_body_json FROM ranking_bases"
            ).fetchone()[0]
        )
        body["legs"][1]["strike"] = {"$decimal": "510"}
        left._connection.execute(
            "UPDATE ranking_bases SET candidate_body_json=?",
            (canonical_json(body),),
        )
        with pytest.raises(RankingStoreCorruption):
            left.read_snapshot(results[0].ranking_snapshot_id)
        with pytest.raises(RankingStoreCorruption):
            left.assert_integrity()
    finally:
        left.close()
        right.close()


def test_governance_only_legacy_append_never_creates_ranked_row(tmp_path) -> None:
    with RankingStore(tmp_path / "pending.sqlite") as store:
        snapshot = store.append_snapshot(
            scan_run_id="pending-scan",
            input_hash=INPUT_HASH,
            evidence_hash=EVIDENCE_HASH,
            broker_snapshot_hash=BROKER_HASH,
            policy_version="v1",
            policy_hash=POLICY_HASH,
            policy_authority_marker_hash=POLICY_MARKER_HASH,
            cost_version="v1",
            cost_hash=COST_HASH,
            risk_contract_hash=RISK_CONTRACT_HASH,
            risk_authority_version="v1",
            risk_authority_marker_hash=_authority().marker_hash,
            candidates=(),
            governance_evidence=(_candidate("pending", authorizable=False),),
            valid_until=NOW + timedelta(minutes=1),
            now=NOW,
        )
        read_model = store.read_snapshot(snapshot.ranking_snapshot_id)
        assert read_model["candidates"] == []
        assert read_model["governance_evidence"][0]["authority_status"] == "A_GRADE_PENDING"
        assert store.record_counts()["ranking_rows"] == 0
        assert store.authorize_frozen_rank_one(
            snapshot.ranking_snapshot_id,
            "pending",
            snapshot.expected_hashes,
            policy_resolver=_Resolver(_policy()),
            risk_authority_resolver=_Resolver(_authority()),
            now=NOW,
        ) is None
        assert store.verify_integrity()


def test_head_or_resolver_change_rejects_without_mutating_store(tmp_path) -> None:
    with RankingStore(tmp_path / "heads.sqlite") as store:
        first = _append(store, "first")
        second = _append(store, "second")
        assert store.authorize_frozen_rank_one(
            first.ranking_snapshot_id,
            "one",
            first.expected_hashes,
            policy_resolver=_Resolver(_policy()),
            risk_authority_resolver=_Resolver(_authority()),
            now=NOW,
        ) is None

        policy_resolver = _Resolver(_policy())
        policy_resolver.current = False
        before = store.record_counts()
        assert store.authorize_frozen_rank_one(
            second.ranking_snapshot_id,
            "one",
            second.expected_hashes,
            policy_resolver=policy_resolver,
            risk_authority_resolver=_Resolver(_authority()),
            now=NOW,
        ) is None
        expected = dict(second.expected_hashes)
        expected["risk_authority_marker_hash"] = "0" * 64
        assert store.authorize_frozen_rank_one(
            second.ranking_snapshot_id,
            "one",
            expected,
            policy_resolver=_Resolver(_policy()),
            risk_authority_resolver=_Resolver(_authority()),
            now=NOW,
        ) is None
        assert store.record_counts() == before


def test_decision_chain_is_append_only_idempotent_restart_safe_and_tamper_evident(tmp_path) -> None:
    path = tmp_path / "decisions.sqlite"
    with RankingStore(path) as store:
        scenario = store.append_decision(
            "scan-1",
            "SCENARIO",
            {"action": "TRADE", "probability": Decimal("0.6")},
            now=NOW,
        )
        assert store.append_decision(
            "scan-1",
            "SCENARIO",
            {"action": "TRADE", "probability": Decimal("0.6")},
            now=NOW + timedelta(seconds=1),
        ).decision_hash == scenario.decision_hash
        no_trade = store.append_decision(
            "scan-2",
            "NO_TRADE",
            {"reasons": ["STALE_INPUT"]},
            now=NOW,
        )
        result = store.append_decision(
            "scan-1",
            "RESULT",
            {"status": "TRADE", "ranking_snapshot_id": "rank-1"},
            now=NOW,
        )
        assert no_trade.previous_decision_hash == scenario.decision_hash
        assert result.previous_decision_hash == no_trade.decision_hash
        assert store.record_counts()["ranking_decisions"] == 3
        assert store.verify_integrity()

    with RankingStore(path) as restarted:
        assert [row.record_type for row in restarted.read_decisions()] == [
            "SCENARIO",
            "NO_TRADE",
            "RESULT",
        ]
        assert restarted.verify_integrity()
        restarted._connection.execute("DROP TRIGGER ranking_decisions_no_update")
        restarted._connection.execute(
            "UPDATE ranking_decisions SET record_json='{}' WHERE sequence=2"
        )
        with pytest.raises(RankingStoreCorruption):
            restarted.assert_integrity()


def test_latest_decision_reuses_verified_chain_until_sqlite_changes(
    tmp_path,
    monkeypatch,
) -> None:
    with RankingStore(tmp_path / "cached-read.sqlite") as store:
        store.append_decisions(
            scan_run_id="scan-cached-read",
            records=(
                {
                    "record_type": "SCENARIO",
                    "record": {"scenario_id": "scenario-cached-read"},
                },
                {
                    "record_type": "NO_TRADE",
                    "record": {
                        "status": "NO_TRADE",
                        "scan_run_id": "scan-cached-read",
                    },
                },
            ),
            now=NOW,
        )
        uncached = store._assert_integrity_uncached
        calls = 0

        def counted_uncached() -> None:
            nonlocal calls
            calls += 1
            uncached()

        monkeypatch.setattr(store, "_assert_integrity_uncached", counted_uncached)
        store._verified_integrity_token = None

        assert store.latest_decision() is not None
        assert store.latest_decision() is not None
        assert calls == 1

        store._connection.execute("DROP TRIGGER ranking_decisions_no_update")
        store._connection.execute(
            "UPDATE ranking_decisions SET record_json='{}' WHERE sequence=1"
        )
        with pytest.raises(RankingStoreCorruption):
            store.latest_decision()
        assert calls == 2


def test_no_trade_batch_is_atomic_idempotent_and_supersedes_trade_authorization(tmp_path) -> None:
    with RankingStore(tmp_path / "batch.sqlite") as store:
        snapshot = _append(store, "trade-scan")
        assert store.latest_decision().record_type == "TRADE"
        assert store.current_terminal_for_snapshot(snapshot.ranking_snapshot_id)
        assert store.authorize_frozen_rank_one(
            snapshot.ranking_snapshot_id,
            "one",
            snapshot.expected_hashes,
            policy_resolver=_Resolver(_policy()),
            risk_authority_resolver=_Resolver(_authority()),
            now=NOW,
        ) is not None
        records = (
            {
                "record_type": "SCENARIO",
                "record": {
                    "scan_run_id": "no-trade-scan",
                    "candidate_id": "rejected",
                    "scenario": {"action": "NO_TRADE"},
                },
            },
            {
                "record_type": "NO_TRADE",
                "record": {
                    "scan_run_id": "no-trade-scan",
                    "status": "NO_TRADE",
                    "reasons": ["STALE_INPUT"],
                },
            },
        )
        batch = store.append_decisions(
            scan_run_id="no-trade-scan", records=records, now=NOW
        )
        replay = store.append_decisions(
            scan_run_id="no-trade-scan",
            records=records,
            now=NOW + timedelta(seconds=1),
        )
        assert [item.record_type for item in batch] == ["SCENARIO", "NO_TRADE"]
        assert [item.decision_hash for item in replay] == [
            item.decision_hash for item in batch
        ]
        assert store.latest_decision().record_type == "NO_TRADE"
        assert not store.current_terminal_for_snapshot(
            snapshot.ranking_snapshot_id
        )
        assert store.authorize_frozen_rank_one(
            snapshot.ranking_snapshot_id,
            "one",
            snapshot.expected_hashes,
            policy_resolver=_Resolver(_policy()),
            risk_authority_resolver=_Resolver(_authority()),
            now=NOW,
        ) is None
        assert store.verify_integrity()


def test_decision_and_snapshot_terminal_batches_roll_back_as_one_transaction(tmp_path) -> None:
    with RankingStore(tmp_path / "batch-rollback.sqlite") as store:
        store._connection.execute(
            """
            CREATE TRIGGER test_reject_no_trade
            BEFORE INSERT ON ranking_decisions
            WHEN NEW.record_type='NO_TRADE'
            BEGIN SELECT RAISE(ABORT,'simulated terminal failure'); END
            """
        )
        with pytest.raises(sqlite3.IntegrityError, match="simulated terminal"):
            store.append_decisions(
                scan_run_id="failed-no-trade",
                records=(
                    {
                        "record_type": "SCENARIO",
                        "record": {
                            "scan_run_id": "failed-no-trade",
                            "scenario": {"action": "NO_TRADE"},
                        },
                    },
                    {
                        "record_type": "NO_TRADE",
                        "record": {
                            "scan_run_id": "failed-no-trade",
                            "status": "NO_TRADE",
                        },
                    },
                ),
                now=NOW,
            )
        assert store.record_counts()["ranking_decisions"] == 0
        store._connection.execute("DROP TRIGGER test_reject_no_trade")

        store._connection.execute(
            """
            CREATE TRIGGER test_reject_trade
            BEFORE INSERT ON ranking_decisions
            WHEN NEW.record_type='TRADE'
            BEGIN SELECT RAISE(ABORT,'simulated ranking terminal failure'); END
            """
        )
        with pytest.raises(sqlite3.IntegrityError, match="ranking terminal"):
            _append(store, "failed-trade")
        counts = store.record_counts()
        assert counts["ranking_snapshots"] == 0
        assert counts["ranking_bases"] == 0
        assert counts["ranking_rows"] == 0
        assert counts["ranking_decisions"] == 0


def _create_v1_database(path) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.executescript(
            """
            CREATE TABLE ranking_snapshots(
                sequence INTEGER PRIMARY KEY,
                ranking_snapshot_id TEXT NOT NULL,
                scan_run_id TEXT NOT NULL,
                snapshot_hash TEXT NOT NULL
            );
            CREATE TABLE ranking_rows(
                ranking_snapshot_id TEXT NOT NULL,
                rank INTEGER NOT NULL,
                candidate_id TEXT NOT NULL,
                candidate_hash TEXT NOT NULL
            );
            INSERT INTO ranking_snapshots VALUES(
                1,'legacy-snapshot','legacy-scan',
                'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
            );
            INSERT INTO ranking_rows VALUES(
                'legacy-snapshot',1,'legacy-candidate',
                'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'
            );
            PRAGMA user_version=1;
            """
        )
    finally:
        connection.close()


def test_v1_migration_preserves_history_anchor_and_new_chains_across_restart(tmp_path) -> None:
    path = tmp_path / "legacy.sqlite"
    _create_v1_database(path)
    with RankingStore(path) as store:
        assert store.schema_version == 3
        legacy_tables = {
            row[0]
            for row in store._connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name LIKE 'ranking_%_legacy_v1%'"
            )
        }
        assert legacy_tables == {
            "ranking_snapshots_legacy_v1",
            "ranking_rows_legacy_v1",
        }
        assert store._connection.execute(
            "SELECT candidate_id FROM ranking_rows_legacy_v1"
        ).fetchone()[0] == "legacy-candidate"
        anchor = store._connection.execute(
            "SELECT anchor_hash FROM ranking_migration_anchors"
        ).fetchone()[0]
        snapshot = _append(store, "after-migration")
        terminal = store.read_decisions()[0]
        decision = store.append_decision(
            "after-migration", "RESULT", {"status": "TRADE"}, now=NOW
        )
        assert snapshot.previous_snapshot_hash == anchor
        assert terminal.record_type == "TRADE"
        assert terminal.previous_decision_hash == anchor
        assert terminal.record["ranking_snapshot_hash"] == snapshot.snapshot_hash
        assert decision.previous_decision_hash == terminal.decision_hash
        assert store.verify_integrity()

    with RankingStore(path) as restarted:
        assert restarted.verify_integrity()
        assert restarted._connection.execute(
            "SELECT COUNT(*) FROM ranking_migration_anchors"
        ).fetchone()[0] == 1
        assert restarted.latest().ranking_snapshot_id == snapshot.ranking_snapshot_id


def test_v2_top3_schema_migrates_in_place_without_rehashing_rows(tmp_path) -> None:
    path = tmp_path / "v2-top3.sqlite"
    with RankingStore(path) as store:
        snapshot = _append(
            store,
            "before-v2-migration",
            candidates=(_candidate("one"), _candidate("two")),
        )
        before = store.read_snapshot(snapshot.ranking_snapshot_id)

    connection = sqlite3.connect(path)
    try:
        triggers = connection.execute(
            "SELECT name FROM sqlite_master WHERE type='trigger' "
            "AND name LIKE 'ranking_%'"
        ).fetchall()
        for (name,) in triggers:
            connection.execute(f'DROP TRIGGER "{name}"')
        connection.execute("ALTER TABLE ranking_rows RENAME TO ranking_rows_v3_top10")
        connection.execute(
            """
            CREATE TABLE ranking_rows(
                ranking_snapshot_id TEXT NOT NULL,
                rank INTEGER NOT NULL CHECK(rank BETWEEN 1 AND 3),
                candidate_id TEXT NOT NULL,
                proposal_hash TEXT NOT NULL,
                candidate_hash TEXT NOT NULL,
                candidate_body_json TEXT NOT NULL,
                candidate_body_hash TEXT NOT NULL,
                proposal_body_json TEXT NOT NULL,
                proposal_body_hash TEXT NOT NULL,
                ranking_basis_hash TEXT NOT NULL,
                authority_status TEXT NOT NULL,
                authorizable INTEGER NOT NULL CHECK(authorizable=1),
                risk_authority_marker_hash TEXT NOT NULL,
                score_json TEXT NOT NULL,
                score_hash TEXT NOT NULL,
                row_hash TEXT NOT NULL,
                PRIMARY KEY(ranking_snapshot_id,rank),
                UNIQUE(ranking_snapshot_id,candidate_id),
                FOREIGN KEY(ranking_snapshot_id,candidate_id)
                    REFERENCES ranking_bases(ranking_snapshot_id,candidate_id)
            )
            """
        )
        columns = (
            "ranking_snapshot_id,rank,candidate_id,proposal_hash,candidate_hash,"
            "candidate_body_json,candidate_body_hash,proposal_body_json,"
            "proposal_body_hash,ranking_basis_hash,authority_status,authorizable,"
            "risk_authority_marker_hash,score_json,score_hash,row_hash"
        )
        connection.execute(
            f"INSERT INTO ranking_rows({columns}) "
            f"SELECT {columns} FROM ranking_rows_v3_top10 ORDER BY rank"
        )
        connection.execute("DROP TABLE ranking_rows_v3_top10")
        connection.execute("PRAGMA user_version=2")
        connection.commit()
    finally:
        connection.close()

    with RankingStore(path) as migrated:
        assert migrated.schema_version == 3
        assert migrated.read_snapshot(snapshot.ranking_snapshot_id) == before
        assert migrated.verify_integrity()
        schema = migrated._connection.execute(
            "SELECT sql FROM sqlite_master WHERE name='ranking_rows'"
        ).fetchone()[0]
        assert "BETWEEN 1 AND 10" in schema


def test_v1_migration_failure_rolls_back_rename_then_restart_recovers(
    tmp_path, monkeypatch
) -> None:
    path = tmp_path / "migration-crash.sqlite"
    _create_v1_database(path)
    original = RankingStore._create_schema_in_transaction

    def _fail_mid_migration(self: RankingStore) -> None:
        self._connection.execute("CREATE TABLE migration_should_rollback(value TEXT)")
        raise RuntimeError("simulated migration interruption")

    monkeypatch.setattr(
        RankingStore, "_create_schema_in_transaction", _fail_mid_migration
    )
    with pytest.raises(RuntimeError, match="simulated migration"):
        RankingStore(path)

    probe = sqlite3.connect(path)
    try:
        assert probe.execute("PRAGMA user_version").fetchone()[0] == 1
        names = {
            row[0]
            for row in probe.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        assert "ranking_snapshots" in names
        assert "ranking_rows" in names
        assert "migration_should_rollback" not in names
        assert not any("legacy_v1" in name for name in names)
    finally:
        probe.close()

    monkeypatch.setattr(RankingStore, "_create_schema_in_transaction", original)
    with RankingStore(path) as restarted:
        assert restarted.schema_version == 3
        assert restarted.verify_integrity()
        assert restarted._connection.execute(
            "SELECT COUNT(*) FROM ranking_snapshots_legacy_v1"
        ).fetchone()[0] == 1


def test_legacy_history_tamper_is_detected_by_migration_anchor(tmp_path) -> None:
    path = tmp_path / "legacy-tamper.sqlite"
    _create_v1_database(path)
    with RankingStore(path) as store:
        trigger = store._connection.execute(
            "SELECT name FROM sqlite_master WHERE type='trigger' "
            "AND tbl_name='ranking_rows_legacy_v1' AND name LIKE '%no_update'"
        ).fetchone()[0]
        store._connection.execute(f'DROP TRIGGER "{trigger}"')
        store._connection.execute(
            "UPDATE ranking_rows_legacy_v1 SET candidate_id='tampered'"
        )
        with pytest.raises(RankingStoreCorruption, match="legacy ranking history"):
            store.assert_integrity()
