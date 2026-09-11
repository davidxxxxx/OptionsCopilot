"""Adversarial tests for immutable G037 joint ranking."""

from __future__ import annotations

import copy
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient

from options_copilot.api import OptionsCopilotServices, create_app
from options_copilot.option_pool.service import FinalizedOptionPoolCandidate
from options_copilot.ranking import JointDisposition, JointRankingEngine, PortfolioRanker
from options_copilot.storage.canonical import canonical_hash


NOW = datetime(2026, 8, 21, 14, 0, tzinfo=timezone.utc)
BROKER_HASH = "b" * 64
NAV_HASH = "c" * 64
COST_HASH = "d" * 64


def _theses(*symbols: str) -> dict[str, object]:
    rows = tuple(
        {
            "symbol": symbol,
            "direction_label": "BULLISH",
            "direction_score": "80",
            "uncertainty": "0.20",
        }
        for symbol in sorted(set(symbols))
    )
    return {
        "rows": rows
    }


def _candidate(
    candidate_id: str,
    symbol: str,
    *,
    quote_age: str = "1",
    exchange_time: datetime | None = None,
    ev: str = "20",
) -> FinalizedOptionPoolCandidate:
    source_hash = canonical_hash({"source": candidate_id})
    scenario_pnl = (
        {"underlying_price": "95", "probability": "0.40", "pnl_usd": "-100"},
        {"underlying_price": "110", "probability": "0.60", "pnl_usd": "100"},
    )
    legs = tuple(
        {
            "con_id": index + 1,
            "contract_id_ex": f"{index + 1}@SMART",
            "expiration": "2026-09-18",
            "strike": str(100 + index * 5),
            "right": "CALL",
            "side": "BUY" if index == 0 else "SELL",
            "ratio": 1,
            "multiplier": 100,
            "exchange": "SMART",
            "bid": "1.00",
            "ask": "1.10",
            "exchange_time": (exchange_time or NOW - timedelta(seconds=1)).isoformat(),
            "quote_age_seconds": quote_age,
            "market_data_type": 1,
            "delta": "0.55" if index == 0 else "0.35",
            "gamma": "0.03",
            "theta": "-0.04",
            "vega": "0.11",
            "volume": 100,
            "open_interest": 500,
        }
        for index in range(2)
    )
    payload: dict[str, object] = {
        "candidate_id": candidate_id,
        "symbol": symbol,
        "structure": "DEBIT_VERTICAL",
        "source_candidate_hash": source_hash,
        "broker_snapshot_hash": BROKER_HASH,
        "quote_batch_id": "batch-1",
        "strategy_nav_hash": NAV_HASH,
        "strategy_nav_usd": "2000",
        "secdef_hash": "e" * 64,
        "legs": legs,
        "liquidity_score": "0.8",
        "max_loss_usd": "100",
        "after_cost_ev_usd": ev,
        "event_evidence_status": "AVAILABLE",
        "earnings_overlap": False,
        "event_defined": False,
        "scenario_pnl": scenario_pnl,
        "final_costs": {"cost_hash": COST_HASH},
    }
    thesis = _theses(symbol)["rows"][0]
    payload["equity_thesis_hash"] = canonical_hash(thesis)
    return FinalizedOptionPoolCandidate(
        payload=payload,
        candidate_hash=canonical_hash(payload),
        source_candidate_hash=source_hash,
        broker_snapshot_hash=BROKER_HASH,
        quote_batch_id="batch-1",
        strategy_nav_hash=NAV_HASH,
        secdef_hash="e" * 64,
        signed_cost_hash=COST_HASH,
        scenario_hash=canonical_hash(scenario_pnl),
    )


def _rank(*candidates: FinalizedOptionPoolCandidate, **kwargs: object):
    symbols = tuple(str(item.payload["symbol"]) for item in candidates)
    kwargs.setdefault(
        "gate_reasons_by_candidate",
        {str(item.payload["candidate_id"]): () for item in candidates},
    )
    kwargs.setdefault(
        "concentration_by_underlying",
        {symbol: Decimal("0") for symbol in symbols},
    )
    return JointRankingEngine().rank(
        candidates,
        scan_run_id="scan-1",
        now=NOW,
        equity_theses=_theses(*symbols),
        broker_snapshot_hash=BROKER_HASH,
        strategy_nav_hash=NAV_HASH,
        **kwargs,
    )


def _replace_payload(
    candidate: FinalizedOptionPoolCandidate,
    **updates: object,
) -> FinalizedOptionPoolCandidate:
    payload = dict(candidate.payload)
    payload.update(updates)
    return replace(candidate, payload=payload, candidate_hash=canonical_hash(payload))


def test_rejects_untyped_hostile_candidate() -> None:
    with pytest.raises(TypeError, match="FinalizedOptionPoolCandidate"):
        JointRankingEngine().rank(
            ({"candidate_id": "forged"},),  # type: ignore[arg-type]
            scan_run_id="scan-1",
            now=NOW,
            equity_theses={"rows": ()},
            broker_snapshot_hash=BROKER_HASH,
            strategy_nav_hash=NAV_HASH,
            gate_reasons_by_candidate={"forged": ()},
            concentration_by_underlying={},
        )


def test_requires_complete_gate_and_concentration_evidence() -> None:
    candidate = _candidate("spy", "SPY")
    with pytest.raises(ValueError, match="gate evidence"):
        JointRankingEngine().rank(
            (candidate,),
            scan_run_id="scan-1",
            now=NOW,
            equity_theses=_theses("SPY"),
            broker_snapshot_hash=BROKER_HASH,
            strategy_nav_hash=NAV_HASH,
            gate_reasons_by_candidate={},
            concentration_by_underlying={"SPY": Decimal("0")},
        )
    with pytest.raises(ValueError, match="concentration evidence"):
        JointRankingEngine().rank(
            (candidate,),
            scan_run_id="scan-1",
            now=NOW,
            equity_theses=_theses("SPY"),
            broker_snapshot_hash=BROKER_HASH,
            strategy_nav_hash=NAV_HASH,
            gate_reasons_by_candidate={"spy": ()},
            concentration_by_underlying={},
        )


def test_concentration_keys_are_normalized_without_defaulting_to_zero() -> None:
    candidate = _candidate("spy", "SPY")
    snapshot = JointRankingEngine().rank(
        (candidate,),
        scan_run_id="scan-1",
        now=NOW,
        equity_theses=_theses("SPY"),
        broker_snapshot_hash=BROKER_HASH,
        strategy_nav_hash=NAV_HASH,
        gate_reasons_by_candidate={"spy": ()},
        concentration_by_underlying={"spy": Decimal("0.25")},
    )
    assert snapshot.executable[0].score_components["concentration_quality"] == Decimal("0.75")
    with pytest.raises(ValueError, match="normalized duplicates"):
        JointRankingEngine().rank(
            (candidate,),
            scan_run_id="scan-1",
            now=NOW,
            equity_theses=_theses("SPY"),
            broker_snapshot_hash=BROKER_HASH,
            strategy_nav_hash=NAV_HASH,
            gate_reasons_by_candidate={"spy": ()},
            concentration_by_underlying={"spy": Decimal("0"), "SPY": Decimal("0")},
        )
    with pytest.raises(ValueError, match="value is invalid"):
        JointRankingEngine().rank(
            (candidate,),
            scan_run_id="scan-1",
            now=NOW,
            equity_theses=_theses("SPY"),
            broker_snapshot_hash=BROKER_HASH,
            strategy_nav_hash=NAV_HASH,
            gate_reasons_by_candidate={"spy": ()},
            concentration_by_underlying={"spy": Decimal("2")},
        )


def test_future_and_stale_quotes_fail_to_research_watchlist() -> None:
    stale = _candidate("stale", "SPY", quote_age="5.01")
    future = _candidate("future", "QQQ", exchange_time=NOW + timedelta(milliseconds=1))
    snapshot = _rank(stale, future)
    assert snapshot.executable == ()
    reasons = {row.candidate_id: row.reason_codes for row in snapshot.research_watchlist}
    assert "EXECUTABLE_QUOTE_STALE" in reasons["stale"]
    assert "EXECUTABLE_QUOTE_FUTURE_OR_MISSING" in reasons["future"]


def test_supplied_quote_age_cannot_hide_stale_exchange_time() -> None:
    stale = _candidate(
        "stale-binding",
        "QQQ",
        exchange_time=NOW - timedelta(minutes=30),
        quote_age="1",
    )
    snapshot = _rank(stale)

    assert snapshot.executable == ()
    assert snapshot.research_watchlist[0].candidate_id == "stale-binding"
    assert "EXECUTABLE_QUOTE_STALE" in snapshot.research_watchlist[0].reason_codes
    assert (
        "EXECUTABLE_QUOTE_AGE_BINDING_MISMATCH"
        in snapshot.research_watchlist[0].reason_codes
    )


def test_thesis_hash_direction_and_duplicates_are_fail_closed() -> None:
    base = _candidate("spy", "SPY")
    missing_hash = _replace_payload(base, equity_thesis_hash=None)
    missing = _rank(missing_hash)
    assert "EQUITY_THESIS_HASH_MISMATCH" in missing.research_watchlist[0].reason_codes

    bearish = {
        "symbol": "SPY",
        "direction_label": "BEARISH",
        "direction_score": "-80",
        "uncertainty": "0.20",
    }
    opposite = _replace_payload(base, equity_thesis_hash=canonical_hash(bearish))
    opposite_snapshot = JointRankingEngine().rank(
        (opposite,),
        scan_run_id="scan-1",
        now=NOW,
        equity_theses={"rows": (bearish,)},
        broker_snapshot_hash=BROKER_HASH,
        strategy_nav_hash=NAV_HASH,
        gate_reasons_by_candidate={"spy": ()},
        concentration_by_underlying={"SPY": Decimal("0")},
    )
    assert (
        "THESIS_PAYOFF_DIRECTION_MISMATCH"
        in opposite_snapshot.research_watchlist[0].reason_codes
    )

    bullish = _theses("SPY")["rows"][0]
    duplicate_snapshot = JointRankingEngine().rank(
        (base,),
        scan_run_id="scan-1",
        now=NOW,
        equity_theses={"rows": (bullish, dict(bullish))},
        broker_snapshot_hash=BROKER_HASH,
        strategy_nav_hash=NAV_HASH,
        gate_reasons_by_candidate={"spy": ()},
        concentration_by_underlying={"SPY": Decimal("0")},
    )
    assert "EQUITY_THESIS_DUPLICATE" in duplicate_snapshot.research_watchlist[0].reason_codes

def test_non_live_option_quotes_are_research_only() -> None:
    candidate = _candidate("delayed", "SPY")
    payload = dict(candidate.payload)
    legs = [dict(item) for item in payload["legs"]]
    legs[0]["market_data_type"] = 4
    payload["legs"] = legs
    payload["source_candidate_hash"] = canonical_hash({"delayed": True})
    delayed = FinalizedOptionPoolCandidate(
        payload=payload,
        candidate_hash=canonical_hash(payload),
        source_candidate_hash=str(payload["source_candidate_hash"]),
        broker_snapshot_hash=BROKER_HASH,
        quote_batch_id="batch-1",
        strategy_nav_hash=NAV_HASH,
        secdef_hash="e" * 64,
        signed_cost_hash=COST_HASH,
        scenario_hash=canonical_hash(payload["scenario_pnl"]),
    )
    snapshot = _rank(delayed)
    assert snapshot.executable == ()
    assert "EXECUTABLE_MARKET_DATA_NOT_LIVE" in snapshot.research_watchlist[0].reason_codes


def test_duplicate_underlying_keeps_one_executable_and_one_research_alternative() -> None:
    first = _candidate("spy-a", "SPY", ev="25")
    second = _candidate("spy-b", "SPY", ev="10")
    snapshot = _rank(first, second)
    assert tuple(row.candidate_id for row in snapshot.executable) == ("spy-a",)
    assert snapshot.research_watchlist[0].candidate_id == "spy-b"
    assert snapshot.research_watchlist[0].reason_codes == ("ALTERNATIVE_SAME_UNDERLYING",)


def test_top10_is_hard_limit_without_filler() -> None:
    candidates = tuple(_candidate(f"candidate-{index}", f"S{index}") for index in range(12))
    snapshot = _rank(*candidates)
    assert len(snapshot.executable) == 10
    assert len(snapshot.research_watchlist) == 2
    assert all(row.reason_codes == ("TOP10_LIMIT",) for row in snapshot.research_watchlist)
    two = _rank(*candidates[:2])
    assert len(two.executable) == 2
    assert two.research_watchlist == ()


def test_gate_open_position_and_account_capacity_are_research_only() -> None:
    spy = _candidate("spy", "SPY")
    qqq = _candidate("qqq", "QQQ")
    snapshot = _rank(
        spy,
        qqq,
        gate_reasons_by_candidate={"spy": ("GATE_4_BLOCK",), "qqq": ()},
        open_position_underlyings=("QQQ",),
    )
    assert snapshot.executable == ()
    reasons = {row.candidate_id: row.reason_codes for row in snapshot.research_watchlist}
    assert reasons["spy"] == ("GATE_4_BLOCK",)
    assert reasons["qqq"] == ("OPEN_POSITION_UNDERLYING_BLOCKED",)


def test_existing_aggregate_exposure_above_twenty_percent_is_research_only() -> None:
    snapshot = _rank(
        _candidate("spy", "SPY"),
        aggregate_open_risk_usd=Decimal("350"),
    )
    assert snapshot.executable == ()
    assert (
        "AGGREGATE_RISK_CAPACITY_EXCEEDED"
        in snapshot.research_watchlist[0].reason_codes
    )


def test_snapshot_and_row_hash_tampering_fail_replay_validation() -> None:
    snapshot = _rank(_candidate("spy", "SPY"))
    with pytest.raises(ValueError, match="snapshot hash mismatch"):
        replace(snapshot, snapshot_hash="0" * 64)
    with pytest.raises(ValueError, match="row hash mismatch"):
        replace(snapshot.executable[0], score=Decimal("99"))


def test_hash_and_order_are_replay_deterministic() -> None:
    spy = _candidate("spy", "SPY", ev="20")
    qqq = _candidate("qqq", "QQQ", ev="30")
    first = _rank(spy, qqq)
    second = _rank(qqq, spy)
    assert first.snapshot_hash == second.snapshot_hash
    assert tuple(row.candidate_id for row in first.executable) == ("qqq", "spy")
    assert all(row.disposition is JointDisposition.EXECUTABLE_REVIEW for row in first.executable)


def test_portfolio_ranker_uses_only_hash_bound_joint_score() -> None:
    snapshot = _rank(
        _candidate("spy", "SPY", ev="20"),
        _candidate("qqq", "QQQ", ev="30"),
    )
    evidence = {"joint_ranking": snapshot.as_dict()}
    rows = [
        {
            "candidate_id": row.candidate_id,
            "eligible": True,
            "after_cost_expected_value": Decimal("100") if row.candidate_id == "spy" else Decimal("1"),
            "liquidity_score": Decimal("50"),
            "max_loss": Decimal("100"),
            "structure": "DEBIT_VERTICAL",
            "underlying": row.underlying,
            "open_combinations": 0,
            "joint_score": row.score,
            "joint_score_components": row.score_components,
            "joint_row_hash": row.row_hash,
            "joint_snapshot_hash": snapshot.snapshot_hash,
            "joint_candidate_hash": row.candidate_hash,
        }
        for row in snapshot.executable
    ]
    ranked = PortfolioRanker().rank(rows, evidence_inputs=evidence)
    assert tuple(row.candidate_id for row in ranked.candidates) == ("qqq", "spy")

    rows[0]["joint_score"] = Decimal("99")
    tampered = PortfolioRanker().rank(rows, evidence_inputs=evidence)
    assert "JOINT_RANKING_BINDING_MISMATCH" in tampered.rejections


def test_api_keeps_research_watchlist_separate_from_executable_candidates() -> None:
    snapshot = _rank(_candidate("stale", "SPY", quote_age="6"))
    payload = {
        "decision": "NO_TRADE",
        "approval_enabled": False,
        "candidates": (),
        "immutable_inputs": {
            "funnel_trace": {"joint_ranking": snapshot.as_dict()}
        },
    }
    client = TestClient(
        create_app(
            OptionsCopilotServices(
                health_provider=lambda: {},
                bootstrap_provider=lambda: {},
                candidates_provider=lambda: (),
                positions_provider=lambda: (),
                learning_provider=lambda: {},
                latest_ranking_provider=lambda: payload,
            )
        )
    )
    response = client.get("/api/rankings/latest")
    assert response.status_code == 200
    body = response.json()
    assert body["candidates"] == []
    assert body["research_watchlist"][0]["candidate_id"] == "stale"
    assert body["research_watchlist"][0]["disposition"] == "RESEARCH_ONLY"
    assert body["research_watchlist_integrity_reason"] is None
    assert body["approval_enabled"] is False

    terminal_payload = {
        "decision": "NO_TRADE",
        "approval_enabled": False,
        "candidates": (),
        "funnel_trace": {"joint_ranking": snapshot.as_dict()},
    }
    terminal_client = TestClient(
        create_app(
            OptionsCopilotServices(
                health_provider=lambda: {},
                bootstrap_provider=lambda: {},
                candidates_provider=lambda: (),
                positions_provider=lambda: (),
                learning_provider=lambda: {},
                latest_ranking_provider=lambda: terminal_payload,
            )
        )
    )
    terminal = terminal_client.get("/api/rankings/latest").json()
    assert terminal["decision"] == "NO_TRADE"
    assert terminal["approval_enabled"] is False
    assert terminal["candidates"] == []
    assert terminal["research_watchlist"][0]["candidate_id"] == "stale"
    assert terminal["research_watchlist_integrity_reason"] is None

    tampered_payload = copy.deepcopy(payload)
    tampered_payload["immutable_inputs"]["funnel_trace"]["joint_ranking"][
        "research_watchlist"
    ][0]["score"] = "99"
    tampered_client = TestClient(
        create_app(
            OptionsCopilotServices(
                health_provider=lambda: {},
                bootstrap_provider=lambda: {},
                candidates_provider=lambda: (),
                positions_provider=lambda: (),
                learning_provider=lambda: {},
                latest_ranking_provider=lambda: tampered_payload,
            )
        )
    )
    tampered = tampered_client.get("/api/rankings/latest").json()
    assert tampered["research_watchlist"] == []
    assert tampered["research_watchlist_integrity_reason"] == (
        "JOINT_RANKING_INTEGRITY_INVALID"
    )
