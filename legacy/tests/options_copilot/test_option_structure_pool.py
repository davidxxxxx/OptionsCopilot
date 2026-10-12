"""G036 durable multi-strategy option structure pool tests."""

from __future__ import annotations

from datetime import timedelta
from decimal import Decimal
from pathlib import Path
import runpy

import pytest
from fastapi.testclient import TestClient

from options_copilot.api import OptionsCopilotServices, create_app
from options_copilot.option_pool import (
    V1_SCHEMA,
    OptionStructureDecision,
    OptionStructurePoolService,
    OptionStructurePoolSnapshot,
    OptionStructurePoolStore,
    OptionStructurePoolStoreCorruption,
    StructureDisposition,
    ThesisClass,
)
from options_copilot.option_pool.models import normalize_equity_thesis_row
from options_copilot.option_pool.service import FinalizedOptionPoolCandidate
from options_copilot.storage.canonical import canonical_hash, freeze_json, thaw_json
from options_copilot.strategies import StrategyKind


_HELPERS = runpy.run_path(
    str(Path(__file__).with_name("test_strategy_generator.py"))
)
NOW = _HELPERS["NOW"]


def _equity_reference(*symbols: str) -> dict[str, object]:
    return {
        "schema": "options_copilot.equity_pool_reference.v1",
        "snapshot_id": "1" * 64,
        "snapshot_hash": "2" * 64,
        "input_manifest_hash": "3" * 64,
        "rows_hash": "4" * 64,
        "policy_hash": "5" * 64,
        "taxonomy_hash": "6" * 64,
        "scoring_hash": "7" * 64,
        "selected_symbols": tuple(symbols),
        "discovery_count": len(symbols),
        "selected_count": len(symbols),
        "excluded_count": 0,
        "exclusion_stats": {},
    }


def _equity_theses(reference: dict[str, object], *symbols: str) -> dict[str, object]:
    raw_rows = tuple(
        {
            "schema": "options_copilot.equity_thesis_evidence.v1",
            "symbol": symbol,
            "direction_label": "BULLISH",
            "direction_score": "40",
            "uncertainty": "0.20",
            "observed_at": NOW,
            "source_hashes": ("8" * 64,),
            "canonical_input_hash": "9" * 64,
            "selected_rank": index,
        }
        for index, symbol in enumerate(symbols, 1)
    )
    rows = tuple(
        normalize_equity_thesis_row(row, expected_symbol=str(row["symbol"]))
        for row in raw_rows
    )
    return {
        "schema": "options_copilot.equity_theses.v1",
        "equity_pool_reference_hash": canonical_hash(reference),
        "rows": rows,
        "rows_hash": canonical_hash(rows),
    }


def _complete_candidate(*, candidate_id: str = "vertical-1", shift: int = 0) -> dict[str, object]:
    source = _HELPERS["_generate"](with_iv=True).candidates[0].hash_payload()
    payload = thaw_json(freeze_json(source))
    assert isinstance(payload, dict)
    payload["candidate_id"] = candidate_id
    for index, leg in enumerate(payload["legs"]):
        leg["con_id"] += shift
        leg["contract_id_ex"] = f"{leg['con_id']}@SMART"
        leg["strike"] = str(100 + shift + (index * 5))
        leg["exchange_time"] = (NOW - timedelta(seconds=1)).isoformat()
        leg["market_data_type"] = 1
        leg["quote_age_seconds"] = "1"
        leg["delta"] = "0.55" if index == 0 else "0.35"
        leg["gamma"] = "0.03"
        leg["theta"] = "-0.04"
        leg["vega"] = "0.11"
        leg["side"] = "BUY" if index == 0 else "SELL"
        leg["liquidity"] = {
            "status": "MEASURED",
            "bid_ask_spread": str(
                Decimal(str(leg["ask"])) - Decimal(str(leg["bid"]))
            ),
            "volume": leg["volume"],
            "open_interest": leg["open_interest"],
        }
    payload["scenario_pnl"] = (
        {"underlying_price": "95", "probability": "0.5", "pnl_usd": "-130"},
        {"underlying_price": "110", "probability": "0.5", "pnl_usd": "370"},
    )
    payload["after_cost_ev_usd"] = "120"
    payload["final_costs"] = {
        "cost_version": "v1",
        "cost_hash": "a" * 64,
        "commission_usd": "5",
        "slippage_usd": "15",
        "execution_cost_usd": "20",
    }
    payload["invalidation_evidence"] = {"rule": "trend reverses", "status": "BOUND"}
    payload["assignment_evidence"] = {"status": "SUPPORTED", "short_legs": 1}
    payload["ex_dividend_evidence"] = {"status": "SUPPORTED"}
    return {"payload": payload, "candidate_hash": canonical_hash(payload)}


def _trusted_candidate(
    candidate: dict[str, object],
) -> FinalizedOptionPoolCandidate:
    payload = candidate["payload"]
    assert isinstance(payload, dict)
    source_candidate_hash = canonical_hash(payload)
    payload["source_candidate_hash"] = source_candidate_hash
    candidate_hash = canonical_hash(payload)
    final_costs = payload["final_costs"]
    assert isinstance(final_costs, dict)
    return FinalizedOptionPoolCandidate(
        payload=payload,
        candidate_hash=candidate_hash,
        source_candidate_hash=source_candidate_hash,
        broker_snapshot_hash=str(payload["broker_snapshot_hash"]),
        quote_batch_id=str(payload["quote_batch_id"]),
        strategy_nav_hash=str(payload["strategy_nav_hash"]),
        secdef_hash=str(payload["secdef_hash"]),
        signed_cost_hash=str(final_costs["cost_hash"]),
        scenario_hash=canonical_hash(payload["scenario_pnl"]),
    )


def test_zero_candidates_records_every_template_for_every_equity(tmp_path) -> None:
    with OptionStructurePoolStore(tmp_path / "option-pool.sqlite3") as store:
        snapshot = OptionStructurePoolService(store).capture_generation(
            scan_run_id="zero",
            candidates=(),
            observed_at=NOW,
            equity_pool_reference=_equity_reference("SPY", "XOM"),
        )
    assert len(snapshot.decisions) == 2 * len(tuple(StrategyKind))
    assert all(item.disposition is not StructureDisposition.EXACT_EVIDENCE_CAPTURED for item in snapshot.decisions)
    assert {item.underlying for item in snapshot.decisions} == {"SPY", "XOM"}


def test_research_capture_retains_only_real_unselected_candidate_and_guard_rolls_back(
    tmp_path,
) -> None:
    reference = _equity_reference()
    reference.update(
        {
            "discovered_symbols": ("SPY",),
            "discovery_count": 1,
            "excluded_count": 1,
        }
    )
    candidate = _complete_candidate()
    with OptionStructurePoolStore(tmp_path / "option-pool.sqlite3") as store:
        service = OptionStructurePoolService(store, clock=lambda: NOW)
        snapshot = service.capture_research_candidates(
            scan_run_id="after-hours-real-only",
            candidates=(candidate,),
            observed_at=NOW,
            equity_pool_reference=reference,
        )
        assert len(snapshot.decisions) == 1
        assert snapshot.decisions[0].candidate_identity is not None
        assert snapshot.decisions[0].disposition is StructureDisposition.RESEARCH_ONLY
        with pytest.raises(TimeoutError, match="commit cancelled"):
            service.capture_research_candidates(
                scan_run_id="after-hours-cancelled",
                candidates=(candidate,),
                observed_at=NOW + timedelta(seconds=1),
                equity_pool_reference=reference,
                commit_guard=lambda: False,
            )
        with pytest.raises(KeyError):
            store.replay("after-hours-cancelled")


def test_research_capture_preserves_hash_bound_equity_thesis(tmp_path) -> None:
    reference = _equity_reference("SPY")
    theses = _equity_theses(reference, "SPY")
    candidate = _complete_candidate()
    with OptionStructurePoolStore(tmp_path / "option-pool.sqlite3") as store:
        snapshot = OptionStructurePoolService(store).capture_research_candidates(
            scan_run_id="after-hours-with-thesis",
            candidates=(candidate,),
            observed_at=NOW,
            equity_pool_reference=reference,
            equity_theses=theses,
        )

    decision = snapshot.decisions[0]
    assert decision.thesis_class is ThesisClass.DIRECTIONAL_BULLISH
    assert decision.equity_thesis_evidence is not None
    assert "EQUITY_THESIS_EVIDENCE_UNAVAILABLE" not in decision.reason_codes
    assert decision.disposition is StructureDisposition.RESEARCH_ONLY


def test_pool_retains_same_template_across_multiple_exact_contract_sets(tmp_path) -> None:
    first = _complete_candidate(candidate_id="vertical-1", shift=0)
    second = _complete_candidate(candidate_id="vertical-2", shift=10)
    reference = _equity_reference("SPY")
    with OptionStructurePoolStore(tmp_path / "option-pool.sqlite3") as store:
        service = OptionStructurePoolService(store, clock=lambda: NOW)
        snapshot = service.capture_generation(
            scan_run_id="scan-1",
            candidates=(_trusted_candidate(first), _trusted_candidate(second)),
            observed_at=NOW,
            equity_pool_reference=reference,
            equity_theses=_equity_theses(reference, "SPY"),
        )
        exact = tuple(item for item in snapshot.decisions if item.disposition is StructureDisposition.EXACT_EVIDENCE_CAPTURED)
        assert len(exact) == 2
        assert len({item.candidate_identity for item in exact}) == 2
        assert all(item.structure is StrategyKind.DEBIT_VERTICAL for item in exact)
        assert all(item.equity_pool_reference["snapshot_hash"] == "2" * 64 for item in exact)
        assert all(item.thesis_observed_at == NOW for item in exact)
        assert store.replay("scan-1").snapshot_hash == snapshot.snapshot_hash
        assert service.latest_payload()["exact_count"] == 2


def test_complete_self_hashed_mapping_is_never_exact_authority(tmp_path) -> None:
    reference = _equity_reference("SPY")
    with OptionStructurePoolStore(tmp_path / "mapping-research-only.sqlite3") as store:
        snapshot = OptionStructurePoolService(store).capture_generation(
            scan_run_id="mapping-research-only",
            candidates=(_complete_candidate(),),
            observed_at=NOW,
            equity_pool_reference=reference,
            equity_theses=_equity_theses(reference, "SPY"),
        )
    row = next(item for item in snapshot.decisions if item.candidate_id)
    assert row.disposition is StructureDisposition.RESEARCH_ONLY
    assert "UNTRUSTED_OPTION_POOL_CANDIDATE_SOURCE" in row.reason_codes


@pytest.mark.parametrize(
    "mutation",
    ("one_leg_vertical", "expired_dte_mismatch", "negative_max_profit"),
)
def test_typed_candidate_rejects_invalid_template_dte_and_payoff(
    tmp_path,
    mutation: str,
) -> None:
    candidate = _complete_candidate()
    payload = candidate["payload"]
    assert isinstance(payload, dict)
    if mutation == "one_leg_vertical":
        payload["legs"] = payload["legs"][:1]
    elif mutation == "expired_dte_mismatch":
        for leg in payload["legs"]:
            leg["expiration"] = "2020-08-21"
        payload["dte"] = 28
    else:
        payload["max_profit_usd"] = "-1"
    reference = _equity_reference("SPY")
    with OptionStructurePoolStore(tmp_path / f"semantic-{mutation}.sqlite3") as store:
        snapshot = OptionStructurePoolService(store).capture_generation(
            scan_run_id=f"semantic-{mutation}",
            candidates=(_trusted_candidate(candidate),),
            observed_at=NOW,
            equity_pool_reference=reference,
            equity_theses=_equity_theses(reference, "SPY"),
        )
    row = next(item for item in snapshot.decisions if item.candidate_id)
    assert row.disposition is StructureDisposition.RESEARCH_ONLY
    if mutation == "one_leg_vertical":
        assert "STRUCTURE_TEMPLATE_SEMANTICS_INVALID" in row.reason_codes
    elif mutation == "expired_dte_mismatch":
        assert "DTE_EVIDENCE_INCOMPLETE" in row.reason_codes
    else:
        assert "STRUCTURE_PAYOFF_EVIDENCE_INCOMPLETE" in row.reason_codes


def test_generated_candidate_with_missing_greeks_is_research_only(tmp_path) -> None:
    candidate = _complete_candidate()
    reference = _equity_reference("SPY")
    candidate["payload"]["legs"][0].pop("gamma")
    candidate["candidate_hash"] = canonical_hash(candidate["payload"])
    with OptionStructurePoolStore(tmp_path / "option-pool.sqlite3") as store:
        snapshot = OptionStructurePoolService(store).capture_generation(
            scan_run_id="missing-greeks",
            candidates=(_trusted_candidate(candidate),),
            observed_at=NOW,
            equity_pool_reference=reference,
            equity_theses=_equity_theses(reference, "SPY"),
        )
    candidate = next(item for item in snapshot.decisions if item.candidate_id)
    assert candidate.disposition is StructureDisposition.RESEARCH_ONLY
    assert "OPTION_GREEKS_INCOMPLETE" in candidate.reason_codes
    assert candidate.exact_economics["legs"][0]["bid"] == "2.00"


def test_nonpositive_after_cost_ev_is_retained_as_rejected_research(
    tmp_path: Path,
) -> None:
    candidate = _complete_candidate(candidate_id="negative-ev-research")
    payload = candidate["payload"]
    assert isinstance(payload, dict)
    payload["scenario_pnl"] = (
        {"underlying_price": "95", "probability": "0.5", "pnl_usd": "-20"},
        {"underlying_price": "110", "probability": "0.5", "pnl_usd": "0"},
    )
    payload["after_cost_ev_usd"] = "-10"
    candidate["candidate_hash"] = canonical_hash(payload)
    reference = _equity_reference("SPY")
    with OptionStructurePoolStore(tmp_path / "negative-ev-option-pool.sqlite3") as store:
        service = OptionStructurePoolService(store, clock=lambda: NOW)
        snapshot = service.capture_generation(
            scan_run_id="negative-ev-research",
            candidates=(_trusted_candidate(candidate),),
            observed_at=NOW,
            equity_pool_reference=reference,
            equity_theses=_equity_theses(reference, "SPY"),
        )
        base = lambda: {}
        app = create_app(OptionsCopilotServices(
            health_provider=base,
            bootstrap_provider=base,
            candidates_provider=lambda: (),
            positions_provider=lambda: (),
            learning_provider=base,
            option_pool_provider=service.latest_payload,
        ))
        api_payload = TestClient(app).get("/api/option-pool/latest").json()

    decision = next(item for item in snapshot.decisions if item.candidate_id)
    assert decision.disposition is StructureDisposition.RESEARCH_ONLY
    assert "CANDIDATE_AFTER_COST_EV_NONPOSITIVE" in decision.reason_codes
    assert "AFTER_COST_ECONOMICS_INCOMPLETE" not in decision.reason_codes
    assert decision.exact_economics["after_cost_ev_usd"] == "-10"
    assert decision.exact_economics["max_loss_usd"] is not None
    assert decision.exact_economics["legs"]
    api_decision = next(
        item for item in api_payload["decisions"] if item["candidate_id"]
    )
    assert api_payload["exact_count"] == 0
    assert api_payload["research_only_count"] >= 1
    assert api_decision["economics"]["after_cost_ev_usd"] == "-10"
    assert api_decision["entry_eligible"] is False
    assert api_decision["approval_eligible"] is False
    assert api_decision["order_allowed"] is False


@pytest.mark.parametrize("positive_ev", (True, False))
def test_credit_cashflow_is_complete_but_nonpositive_ev_stays_rejected(
    tmp_path: Path, positive_ev: bool,
) -> None:
    candidate = _complete_candidate(candidate_id="credit-research")
    payload = candidate["payload"]
    payload.update({
        "structure": "CREDIT_VERTICAL",
        "debit_usd": "110", "credit_usd": "200", "all_in_cost_usd": "-70",
        "max_loss_usd": "430", "max_profit_usd": "70",
        "breakevens": ("100.7",),
        "scenario_pnl": (
            {"terminal_underlying_price": "95", "probability": "0.9" if positive_ev else "0.5", "pnl_usd": "70"},
            {"terminal_underlying_price": "110", "probability": "0.1" if positive_ev else "0.5", "pnl_usd": "-430"},
        ),
        "after_cost_ev_usd": "20" if positive_ev else "-180",
    })
    for leg in payload["legs"]:
        leg["side"] = "SELL" if leg["side"] == "BUY" else "BUY"
    reference = _equity_reference("SPY")
    with OptionStructurePoolStore(tmp_path / "credit.sqlite3") as store:
        snapshot = OptionStructurePoolService(store).capture_generation(
            scan_run_id="credit", candidates=(_trusted_candidate(candidate),),
            observed_at=NOW, equity_pool_reference=reference,
            equity_theses=_equity_theses(reference, "SPY"),
        )
    decision = next(item for item in snapshot.decisions if item.candidate_id)
    assert "AFTER_COST_ECONOMICS_INCOMPLETE" not in decision.reason_codes
    assert ("CANDIDATE_AFTER_COST_EV_NONPOSITIVE" in decision.reason_codes) is not positive_ev
    assert decision.exact_economics["all_in_cost_usd"] == "-70"


@pytest.mark.parametrize("all_in", ("-999", "999", "NaN", True))
def test_cost_cashflow_mismatch_cannot_pass_completeness(all_in: object) -> None:
    from options_copilot.option_pool.service import _valid_after_cost_economics

    payload = _complete_candidate()["payload"]
    payload["all_in_cost_usd"] = all_in
    assert not _valid_after_cost_economics(freeze_json(payload))


def test_option_pool_api_preserves_production_quote_and_scenario_fields(tmp_path) -> None:
    candidate = _complete_candidate()
    payload = candidate["payload"]
    for row in payload["scenario_pnl"]:
        row["terminal_underlying_price"] = row.pop("underlying_price")
        row["name"] = "TEST_SCENARIO"
    reference = _equity_reference("SPY")
    with OptionStructurePoolStore(tmp_path / "projection.sqlite3") as store:
        service = OptionStructurePoolService(store, clock=lambda: NOW)
        service.capture_generation(
            scan_run_id="projection", candidates=(_trusted_candidate(candidate),),
            observed_at=NOW, equity_pool_reference=reference,
            equity_theses=_equity_theses(reference, "SPY"),
        )
        base = lambda: {}
        app = create_app(OptionsCopilotServices(
            health_provider=base, bootstrap_provider=base,
            candidates_provider=lambda: (), positions_provider=lambda: (),
            learning_provider=base, option_pool_provider=service.latest_payload,
        ))
        result = TestClient(app).get("/api/option-pool/latest").json()
    row = next(item for item in result["decisions"] if item["candidate_id"])
    economics = row["economics"]
    leg = economics["legs"][0]
    assert str(leg["multiplier"]) == "100"
    for field in ("implied_volatility", "exchange_time", "market_data_type", "observed_at", "completed_at"):
        assert leg[field] == payload["legs"][0][field]
    assert leg["bid_ask_spread"] == payload["legs"][0]["liquidity"]["bid_ask_spread"]
    scenario = economics["scenario_pnl"][0]
    assert scenario["terminal_underlying_price"] == "95"
    assert scenario["underlying_price"] == "95"
    assert scenario["probability"] == "0.5"
    assert scenario["name"] == "TEST_SCENARIO"
    assert row["entry_eligible"] is False
    assert row["order_allowed"] is False


def test_missing_assignment_evidence_and_term_structures_fail_closed(tmp_path) -> None:
    candidate = _complete_candidate()
    reference = _equity_reference("SPY")
    candidate["payload"].pop("assignment_evidence")
    candidate["payload"].pop("ex_dividend_evidence")
    for leg in candidate["payload"]["legs"]:
        leg.pop("short_leg_risk_evidence", None)
    candidate["candidate_hash"] = canonical_hash(candidate["payload"])
    with OptionStructurePoolStore(tmp_path / "option-pool.sqlite3") as store:
        snapshot = OptionStructurePoolService(store).capture_generation(
            scan_run_id="assignment",
            candidates=(_trusted_candidate(candidate),),
            observed_at=NOW,
            equity_pool_reference=reference,
            equity_theses=_equity_theses(reference, "SPY"),
        )
    decision = next(item for item in snapshot.decisions if item.candidate_id)
    assert decision.disposition is StructureDisposition.RESEARCH_ONLY
    assert "ASSIGNMENT_AND_EX_DIVIDEND_EVIDENCE_INCOMPLETE" in decision.reason_codes
    term_rows = tuple(item for item in snapshot.decisions if item.structure in {StrategyKind.CALENDAR, StrategyKind.DIAGONAL})
    assert all("CROSS_EXPIRY_ECONOMICS_UNSUPPORTED" in item.reason_codes for item in term_rows)


def test_latest_read_degrades_exact_candidate_when_quotes_turn_stale(tmp_path) -> None:
    reference = _equity_reference("SPY")
    with OptionStructurePoolStore(tmp_path / "option-pool.sqlite3") as store:
        service = OptionStructurePoolService(
            store,
            clock=lambda: NOW + timedelta(seconds=8),
        )
        service.capture_generation(
            scan_run_id="stale",
            candidates=(_trusted_candidate(_complete_candidate()),),
            observed_at=NOW,
            equity_pool_reference=reference,
            equity_theses=_equity_theses(reference, "SPY"),
        )
        payload = service.latest_payload()
    row = next(item for item in payload["decisions"] if item["candidate_id"])
    assert payload["exact_count"] == 0
    assert row["disposition"] == "RESEARCH_ONLY"
    assert row["freshness_degraded"] is True
    assert "EXECUTABLE_QUOTES_STALE" in row["reason_codes"]


def test_equity_pool_reference_tampering_is_rejected(tmp_path) -> None:
    reference = _equity_reference("SPY")
    reference["snapshot_hash"] = "tampered"
    with OptionStructurePoolStore(tmp_path / "option-pool.sqlite3") as store:
        with pytest.raises(ValueError, match="snapshot_hash"):
            OptionStructurePoolService(store).capture_generation(
                scan_run_id="tampered-reference",
                candidates=(),
                observed_at=NOW,
                equity_pool_reference=reference,
            )


def test_v1_rows_replay_after_v2_store_migration(tmp_path) -> None:
    path = tmp_path / "option-pool.sqlite3"
    legacy_payload = _HELPERS["_generate"](with_iv=True).candidates[0].hash_payload()
    legacy = OptionStructureDecision(
        underlying="SPY",
        thesis_class=ThesisClass.UNCERTAIN,
        structure=StrategyKind.DEBIT_VERTICAL,
        disposition=StructureDisposition.EXACT_EVIDENCE_CAPTURED,
        reason_codes=("LEGACY",),
        candidate_id=str(legacy_payload["candidate_id"]),
        candidate_hash=canonical_hash(legacy_payload),
        exact_economics=legacy_payload,
        schema=V1_SCHEMA,
    )
    snapshot = OptionStructurePoolSnapshot(
        scan_run_id="legacy",
        observed_at=NOW,
        decisions=(legacy,),
        schema=V1_SCHEMA,
    )
    with OptionStructurePoolStore(path) as store:
        store.append(snapshot)
    with OptionStructurePoolStore(path) as reopened:
        replayed = reopened.replay("legacy")
        assert replayed.schema == V1_SCHEMA
        assert replayed.snapshot_hash == snapshot.snapshot_hash
        assert reopened._connection.execute("PRAGMA user_version").fetchone()[0] == 2


def test_pool_rejects_tampered_candidate_and_noop_trigger(tmp_path) -> None:
    candidate = _complete_candidate()
    candidate["candidate_hash"] = "f" * 64
    store = OptionStructurePoolStore(tmp_path / "option-pool.sqlite3")
    try:
        service = OptionStructurePoolService(store)
        with pytest.raises(ValueError, match="candidate hash"):
            service.capture_generation(
                scan_run_id="tampered",
                candidates=(candidate,),
                observed_at=NOW,
                equity_pool_reference=_equity_reference("SPY"),
            )
        store._connection.execute("DROP TRIGGER option_structure_pools_no_update")
        store._connection.execute(
            "CREATE TRIGGER option_structure_pools_no_update BEFORE UPDATE ON "
            "option_structure_pools BEGIN SELECT 1; END"
        )
        with pytest.raises(OptionStructurePoolStoreCorruption, match="immutable trigger invalid"):
            store.assert_integrity()
    finally:
        store.close()


def test_store_recent_is_bounded_newest_first_and_integrity_checked(tmp_path) -> None:
    path = tmp_path / "recent.sqlite3"
    snapshots = tuple(
        OptionStructurePoolSnapshot(
            scan_run_id=f"scan.{index}",
            observed_at=NOW + timedelta(minutes=index),
            decisions=(),
        )
        for index in range(3)
    )
    with OptionStructurePoolStore(path) as store:
        for snapshot in snapshots:
            store.append(snapshot)

        assert store.recent(limit=2) == (snapshots[2], snapshots[1])
        for invalid_limit in (True, 0, 101):
            with pytest.raises(ValueError, match="limit"):
                store.recent(limit=invalid_limit)

        store._connection.execute("DROP TRIGGER option_structure_pools_no_update")
        store._connection.execute(
            "UPDATE option_structure_pools SET snapshot_hash=? WHERE sequence=3",
            ("f" * 64,),
        )
        store._connection.execute(
            "CREATE TRIGGER option_structure_pools_no_update BEFORE UPDATE ON "
            "option_structure_pools BEGIN SELECT RAISE(ABORT, "
            "'option_structure_pools immutable'); END"
        )
        with pytest.raises(
            OptionStructurePoolStoreCorruption,
            match="snapshot hash mismatch",
        ):
            store.recent(limit=2)


def test_option_pool_api_preserves_supporting_only_authority(tmp_path) -> None:
    reference = _equity_reference("SPY")
    with OptionStructurePoolStore(tmp_path / "option-pool.sqlite3") as store:
        service = OptionStructurePoolService(store, clock=lambda: NOW)
        service.capture_generation(
            scan_run_id="api-scan",
            candidates=(_trusted_candidate(_complete_candidate()),),
            observed_at=NOW,
            equity_pool_reference=reference,
            equity_theses=_equity_theses(reference, "SPY"),
        )
        base = lambda: {}
        app = create_app(OptionsCopilotServices(
            health_provider=base,
            bootstrap_provider=base,
            candidates_provider=lambda: (),
            positions_provider=lambda: (),
            learning_provider=base,
            option_pool_provider=service.latest_payload,
        ))
        payload = TestClient(app).get("/api/option-pool/latest").json()
    assert payload["exact_count"] == 1
    assert payload["entry_authority"] is False
    assert payload["approval_eligible"] is False
    assert payload["instruction_creation_allowed"] is False
    assert payload["order_allowed"] is False


def test_option_pool_api_preserves_unknown_volume_and_open_interest(tmp_path) -> None:
    candidate = _complete_candidate()
    for leg in candidate["payload"]["legs"]:
        leg.pop("volume")
        leg.pop("open_interest")
    candidate["candidate_hash"] = canonical_hash(candidate["payload"])
    with OptionStructurePoolStore(tmp_path / "option-pool.sqlite3") as store:
        service = OptionStructurePoolService(store, clock=lambda: NOW)
        service.capture_research_candidates(
            scan_run_id="api-unknown-liquidity",
            candidates=(candidate,),
            observed_at=NOW,
            equity_pool_reference=_equity_reference("SPY"),
        )
        base = lambda: {}
        app = create_app(OptionsCopilotServices(
            health_provider=base,
            bootstrap_provider=base,
            candidates_provider=lambda: (),
            positions_provider=lambda: (),
            learning_provider=base,
            option_pool_provider=service.latest_payload,
        ))
        payload = TestClient(app).get("/api/option-pool/latest").json()

    leg = payload["decisions"][0]["economics"]["legs"][0]
    assert leg["volume"] is None
    assert leg["open_interest"] is None


@pytest.mark.parametrize(
    ("column", "tampered_value", "message"),
    (
        ("scan_run_id", "column-tampered", "scan_run_id column mismatch"),
        ("observed_at", (NOW + timedelta(seconds=1)).isoformat(), "observed_at column mismatch"),
    ),
)
def test_store_integrity_binds_authority_columns_to_snapshot_body(
    tmp_path,
    column: str,
    tampered_value: str,
    message: str,
) -> None:
    path = tmp_path / f"tampered-{column}.sqlite3"
    snapshot = OptionStructurePoolSnapshot(
        scan_run_id="original-scan",
        observed_at=NOW,
        decisions=(),
    )
    with OptionStructurePoolStore(path) as store:
        store.append(snapshot)
        store._connection.execute("DROP TRIGGER option_structure_pools_no_update")
        store._connection.execute(
            f"UPDATE option_structure_pools SET {column}=? WHERE sequence=1",
            (tampered_value,),
        )
        store._connection.execute(
            "CREATE TRIGGER option_structure_pools_no_update BEFORE UPDATE ON "
            "option_structure_pools BEGIN SELECT RAISE(ABORT, "
            "'option_structure_pools immutable'); END"
        )
        with pytest.raises(OptionStructurePoolStoreCorruption, match=message):
            store.assert_integrity()


def test_replay_key_must_match_hash_bound_snapshot_body(tmp_path) -> None:
    with OptionStructurePoolStore(tmp_path / "replay-key.sqlite3") as store:
        store.append(OptionStructurePoolSnapshot(
            scan_run_id="body-key",
            observed_at=NOW,
            decisions=(),
        ))
        store._connection.execute("DROP TRIGGER option_structure_pools_no_update")
        store._connection.execute(
            "UPDATE option_structure_pools SET scan_run_id='query-key' WHERE sequence=1"
        )
        store._connection.execute(
            "CREATE TRIGGER option_structure_pools_no_update BEFORE UPDATE ON "
            "option_structure_pools BEGIN SELECT RAISE(ABORT, "
            "'option_structure_pools immutable'); END"
        )
        with pytest.raises(OptionStructurePoolStoreCorruption, match="scan_run_id column mismatch"):
            store.replay("query-key")


def test_materially_future_exchange_timestamp_is_never_exact(tmp_path) -> None:
    candidate = _complete_candidate()
    for leg in candidate["payload"]["legs"]:
        leg["exchange_time"] = (NOW + timedelta(seconds=2)).isoformat()
    candidate["candidate_hash"] = canonical_hash(candidate["payload"])
    reference = _equity_reference("SPY")
    with OptionStructurePoolStore(tmp_path / "future.sqlite3") as store:
        snapshot = OptionStructurePoolService(store).capture_generation(
            scan_run_id="future",
            candidates=(_trusted_candidate(candidate),),
            observed_at=NOW,
            equity_pool_reference=reference,
            equity_theses=_equity_theses(reference, "SPY"),
        )
    row = next(item for item in snapshot.decisions if item.candidate_id)
    assert row.disposition is StructureDisposition.RESEARCH_ONLY
    assert "EXECUTABLE_LEG_QUOTE_INCOMPLETE" in row.reason_codes


def test_read_projection_does_not_clamp_future_timestamp_to_fresh(tmp_path) -> None:
    reference = _equity_reference("SPY")
    with OptionStructurePoolStore(tmp_path / "future-projection.sqlite3") as store:
        service = OptionStructurePoolService(
            store,
            clock=lambda: NOW - timedelta(seconds=2),
        )
        service.capture_generation(
            scan_run_id="future-projection",
            candidates=(_trusted_candidate(_complete_candidate()),),
            observed_at=NOW,
            equity_pool_reference=reference,
            equity_theses=_equity_theses(reference, "SPY"),
        )
        payload = service.latest_payload()
    row = next(item for item in payload["decisions"] if item["candidate_id"])
    assert row["disposition"] == "RESEARCH_ONLY"
    assert row["freshness_degraded"] is True
    assert row["quote_age_seconds"] is None


@pytest.mark.parametrize(
    "mutation",
    (
        "expiration",
        "strike",
        "right",
        "side",
        "ratio",
        "multiplier",
        "contract_id_ex",
        "liquidity_status",
        "liquidity_spread",
        "liquidity_volume",
    ),
)
def test_malformed_exact_leg_or_liquidity_is_research_only(
    tmp_path,
    mutation: str,
) -> None:
    candidate = _complete_candidate()
    leg = candidate["payload"]["legs"][0]
    if mutation == "expiration":
        leg["expiration"] = "20260821"
    elif mutation == "strike":
        leg["strike"] = "NaN"
    elif mutation == "right":
        leg["right"] = "UNKNOWN"
    elif mutation == "side":
        leg["side"] = "UNKNOWN"
    elif mutation == "ratio":
        leg["ratio"] = 0
    elif mutation == "multiplier":
        leg["multiplier"] = "0"
    elif mutation == "contract_id_ex":
        leg["contract_id_ex"] = "999@SMART"
    elif mutation == "liquidity_status":
        leg["liquidity"]["status"] = "LIQUID"
    elif mutation == "liquidity_spread":
        leg["liquidity"]["bid_ask_spread"] = "999"
    else:
        leg["liquidity"]["volume"] = int(leg["volume"]) + 1
    candidate["candidate_hash"] = canonical_hash(candidate["payload"])
    reference = _equity_reference("SPY")
    with OptionStructurePoolStore(tmp_path / f"malformed-{mutation}.sqlite3") as store:
        snapshot = OptionStructurePoolService(store).capture_generation(
            scan_run_id=f"malformed-{mutation}",
            candidates=(_trusted_candidate(candidate),),
            observed_at=NOW,
            equity_pool_reference=reference,
            equity_theses=_equity_theses(reference, "SPY"),
        )
    row = next(item for item in snapshot.decisions if item.candidate_id)
    assert row.disposition is StructureDisposition.RESEARCH_ONLY


@pytest.mark.parametrize(
    "mutation",
    (
        "commission_nan",
        "execution_cost_mismatch",
        "scenario_probability",
        "scenario_pnl_nan",
        "after_cost_ev_mismatch",
    ),
)
def test_malformed_cost_scenario_or_ev_is_research_only(
    tmp_path,
    mutation: str,
) -> None:
    candidate = _complete_candidate()
    payload = candidate["payload"]
    if mutation == "commission_nan":
        payload["estimated_commissions_usd"] = "NaN"
    elif mutation == "execution_cost_mismatch":
        payload["final_costs"]["execution_cost_usd"] = "999"
    elif mutation == "scenario_probability":
        payload["scenario_pnl"][0]["probability"] = "0.9"
    elif mutation == "scenario_pnl_nan":
        payload["scenario_pnl"][0]["pnl_usd"] = "NaN"
    else:
        payload["after_cost_ev_usd"] = "121"
    candidate["candidate_hash"] = canonical_hash(payload)
    reference = _equity_reference("SPY")
    with OptionStructurePoolStore(tmp_path / f"economics-{mutation}.sqlite3") as store:
        snapshot = OptionStructurePoolService(store).capture_generation(
            scan_run_id=f"economics-{mutation}",
            candidates=(_trusted_candidate(candidate),),
            observed_at=NOW,
            equity_pool_reference=reference,
            equity_theses=_equity_theses(reference, "SPY"),
        )
    row = next(item for item in snapshot.decisions if item.candidate_id)
    assert row.disposition is StructureDisposition.RESEARCH_ONLY
    assert any(
        reason in row.reason_codes
        for reason in (
            "STRUCTURE_PAYOFF_EVIDENCE_INCOMPLETE",
            "AFTER_COST_ECONOMICS_INCOMPLETE",
        )
    )
