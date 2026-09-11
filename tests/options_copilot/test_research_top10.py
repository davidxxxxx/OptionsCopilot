from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from hashlib import sha256
import json
from pathlib import Path
import sqlite3
from zoneinfo import ZoneInfo

import pytest

import options_copilot.news.research_top10 as research_top10_module
from options_copilot.api.app import OptionsCopilotServices, create_app
from options_copilot.news.research_top10 import (
    INTRADAY_RECOVERY,
    RESEARCH_TOP10_DIRECT_SOURCE,
    ResearchTop10Conflict,
    ResearchTop10Store,
    ResearchTop10ValidationError,
    import_research_top10,
    main,
    read_research_top10,
    safe_research_top10_read_model,
)


NEW_YORK = ZoneInfo("America/New_York")
TRADE_DATE = "2026-08-06"
PREMARKET_AT = datetime(2026, 8, 6, 9, 18, tzinfo=NEW_YORK)
REPRICE_AT = datetime(2026, 8, 6, 9, 35, 2, tzinfo=NEW_YORK)
EXPIRATION = "2026-08-21"
MISSING_GREEK_BLOCKERS = [
    "DELTA_UNAVAILABLE",
    "GAMMA_UNAVAILABLE",
    "THETA_UNAVAILABLE",
    "VEGA_UNAVAILABLE",
    "MARKET_DATA_TYPE_UNVERIFIED",
    "LOCAL_SYMBOL_UNAVAILABLE",
    "MULTIPLIER_UNAVAILABLE",
    "STANDARD_ADJUSTED_UNVERIFIED",
    "QUOTE_EXCHANGE_TIMESTAMP_UNAVAILABLE",
]


@pytest.fixture(autouse=True)
def _fixed_import_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    now = REPRICE_AT.astimezone(ZoneInfo("UTC")) + timedelta(minutes=1)
    monkeypatch.setattr(research_top10_module, "_utc_now", lambda: now, raising=False)


def _digest(value: str) -> str:
    return sha256(value.encode("utf-8")).hexdigest()


def _leg(index: int, *, side: str, strike: int) -> dict[str, object]:
    is_buy = side == "BUY"
    contract_id = 800_000_000 + (index * 10) + (1 if is_buy else 2)
    return {
        "contract_id": contract_id,
        "contract_id_ex": f"{contract_id}@SMART",
        "side": side,
        "right": "C",
        "strike": str(strike),
        "expiration": EXPIRATION,
        "exchange": "SMART",
        "trading_class": f"T{index:02d}",
        "local_symbol": None,
        "multiplier": None,
        "standard_or_adjusted": None,
        "bid": "1.10" if is_buy else "0.40",
        "ask": "1.20" if is_buy else "0.50",
        "collected_at": PREMARKET_AT.isoformat(),
        "quote_asof": None,
        "implied_volatility": "0.25",
        "delta": None,
        "gamma": None,
        "theta": None,
        "vega": None,
        "volume": 100,
        "open_interest": 1000,
        "market_data_type": None,
        "identity_evidence_hash": _digest(f"identity-{contract_id}"),
        "quote_evidence_hash": _digest(f"quote-{contract_id}-premarket"),
        "blockers": list(MISSING_GREEK_BLOCKERS),
    }


def _candidate(index: int) -> dict[str, object]:
    symbol = f"T{index:02d}"
    lower = 100 + (index * 2)
    return {
        "research_id": f"research-{TRADE_DATE}-{index:02d}",
        "rank": index,
        "underlying": symbol,
        "strategy_type": "BULL_CALL_VERTICAL",
        "expiration": EXPIRATION,
        "dte": 15,
        "quantity": 1,
        "entry_debit_usd": None,
        "indicative_entry_debit_usd": "80.00",
        "execution_cost_cap_usd": "10.00",
        "maximum_loss_usd": None,
        "indicative_maximum_loss_usd": "90.00",
        "cost_after_ev_usd": None,
        "indicative_cost_after_ev_usd": "10.00",
        "assumed_multiplier": 100,
        "risk_cap_verified": False,
        "trade_status": "NO_TRADE",
        "entry_condition": "Only consider after the opening quote remains within the debit cap.",
        "invalidation_condition": "The catalyst or directional thesis is invalidated.",
        "profit_target_condition": "Close at the declared research target.",
        "stop_loss_condition": "Close before the declared defined-risk threshold.",
        "research_summary": f"Supporting-only debit vertical research for {symbol}.",
        "evidence_ids": [f"news-{index:02d}", f"ibkr-chain-{index:02d}"],
        "evidence_hashes": [
            _digest(f"news-{index:02d}"),
            _digest(f"ibkr-chain-{index:02d}"),
        ],
        "blockers": [
            "ASSUMED_MULTIPLIER_USED",
            "ENTRY_DEBIT_UNVERIFIED",
            "MAXIMUM_LOSS_UNVERIFIED",
            "AFTER_COST_EV_UNVERIFIED",
            "RISK_CAP_UNVERIFIED",
        ],
        "legs": [
            _leg(index, side="BUY", strike=lower),
            _leg(index, side="SELL", strike=lower + 1),
        ],
        "decision_authority": "SUPPORTING_ONLY",
        "approval_eligible": False,
        "instruction_creation_allowed": False,
        "order_allowed": False,
        "action_pool_eligible": False,
    }


def _payload(*, count: int = 10) -> dict[str, object]:
    return {
        "schema": "options_copilot.managed_plugin_research_top10.v2",
        "version": 2,
        "batch_id": f"research-{TRADE_DATE}-premarket",
        "phase": "PREMARKET_RESEARCH",
        "trading_date": TRADE_DATE,
        "observed_at": PREMARKET_AT.isoformat(),
        "source": "IBKR_MANAGED_PLUGIN",
        "strategy_nav_usd": "2207.51",
        "normal_risk_fraction": "0.10",
        "target_count": 10,
        "parent_content_hash": None,
        "session": {
            "liquid_hours": None,
            "trading_hours": None,
            "timezone_id": None,
            "observed_at": None,
            "source": None,
            "blockers": ["BROKER_SESSION_HOURS_UNAVAILABLE"],
        },
        "blockers": [],
        "candidates": [_candidate(index) for index in range(1, count + 1)],
        "decision_authority": "SUPPORTING_ONLY",
        "approval_eligible": False,
        "instruction_creation_allowed": False,
        "order_allowed": False,
        "action_pool_eligible": False,
    }


def _unquoted_payload(*, count: int = 10) -> dict[str, object]:
    payload = _payload(count=count)
    for candidate in payload["candidates"]:
        candidate["indicative_entry_debit_usd"] = None
        candidate["indicative_maximum_loss_usd"] = None
        candidate["indicative_cost_after_ev_usd"] = None
        candidate["blockers"].append("QUOTE_UNAVAILABLE")
        candidate["blockers"].append("EXPECTED_PAYOFF_UNAVAILABLE")
        for leg in candidate["legs"]:
            leg["bid"] = None
            leg["ask"] = None
            leg["blockers"].append("QUOTE_UNAVAILABLE")
    return payload


def _intraday_recovery_payload(*, count: int = 10) -> dict[str, object]:
    payload = _unquoted_payload(count=count)
    payload["batch_id"] = f"research-{TRADE_DATE}-intraday-recovery"
    payload["phase"] = INTRADAY_RECOVERY
    payload["observed_at"] = REPRICE_AT.isoformat()
    payload["source"] = RESEARCH_TOP10_DIRECT_SOURCE
    payload["blockers"] = ["INTRADAY_RECOVERY_AFTER_MISSED_SLOTS"]
    for candidate in payload["candidates"]:
        candidate["blockers"].append("INTRADAY_RECOVERY_AFTER_MISSED_SLOTS")
        for leg in candidate["legs"]:
            leg["local_symbol"] = f"{candidate['underlying']}  260821C00100000"
            leg["multiplier"] = 100
            leg["standard_or_adjusted"] = "STANDARD"
            for blocker in (
                "LOCAL_SYMBOL_UNAVAILABLE",
                "MULTIPLIER_UNAVAILABLE",
                "STANDARD_ADJUSTED_UNVERIFIED",
            ):
                leg["blockers"].remove(blocker)
    return payload


def _repriced(parent_hash: str) -> dict[str, object]:
    payload = _payload()
    payload["batch_id"] = f"research-{TRADE_DATE}-reprice"
    payload["phase"] = "INDICATIVE_REPRICE"
    payload["observed_at"] = REPRICE_AT.isoformat()
    payload["parent_content_hash"] = parent_hash
    for candidate in payload["candidates"]:
        for leg in candidate["legs"]:
            leg["bid"] = "1.15" if leg["side"] == "BUY" else "0.45"
            leg["ask"] = "1.25" if leg["side"] == "BUY" else "0.55"
            leg["collected_at"] = REPRICE_AT.isoformat()
            leg["quote_asof"] = None
            leg["quote_evidence_hash"] = _digest(
                f"quote-{leg['contract_id']}-reprice"
            )
        # The executable-side quote remains a $0.80 debit after repricing.
        candidate["indicative_entry_debit_usd"] = "80.00"
        candidate["indicative_maximum_loss_usd"] = "90.00"
    return payload


def _verified_payload(*, count: int = 10) -> dict[str, object]:
    payload = _payload(count=count)
    for candidate in payload["candidates"]:
        candidate["entry_debit_usd"] = "80.00"
        candidate["indicative_entry_debit_usd"] = None
        candidate["maximum_loss_usd"] = "90.00"
        candidate["indicative_maximum_loss_usd"] = None
        candidate["cost_after_ev_usd"] = "10.00"
        candidate["indicative_cost_after_ev_usd"] = None
        candidate["assumed_multiplier"] = None
        candidate["risk_cap_verified"] = True
        candidate["blockers"] = []
        for leg in candidate["legs"]:
            leg["local_symbol"] = f"{candidate['underlying']}  260821C00100000"
            leg["multiplier"] = 100
            leg["standard_or_adjusted"] = "STANDARD"
            leg["market_data_type"] = 1
            leg["quote_asof"] = PREMARKET_AT.isoformat()
            for blocker in (
                "LOCAL_SYMBOL_UNAVAILABLE",
                "MULTIPLIER_UNAVAILABLE",
                "STANDARD_ADJUSTED_UNVERIFIED",
                "MARKET_DATA_TYPE_UNVERIFIED",
                "QUOTE_EXCHANGE_TIMESTAMP_UNAVAILABLE",
            ):
                leg["blockers"].remove(blocker)
    return payload


def _services(**overrides: object) -> OptionsCopilotServices:
    values: dict[str, object] = {
        "health_provider": lambda: {},
        "bootstrap_provider": lambda: {},
        "candidates_provider": lambda: [],
        "positions_provider": lambda: [],
        "learning_provider": lambda: {},
    }
    values.update(overrides)
    return OptionsCopilotServices(**values)  # type: ignore[arg-type]


def test_import_is_durable_idempotent_and_default_api_reads_exact_ten(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store_path = tmp_path / "research-top10.sqlite3"
    monkeypatch.setenv("OPTIONS_COPILOT_RESEARCH_TOP10_PATH", str(store_path))

    first = import_research_top10(_payload(), store_path=store_path)
    duplicate = import_research_top10(_payload(), store_path=store_path)

    assert first["status"] == "IMPORTED"
    assert duplicate["status"] == "IDEMPOTENT"
    assert duplicate["content_hash"] == first["content_hash"]
    with ResearchTop10Store(store_path) as store:
        assert store.batch_count() == 1

    body = asyncio.run(_route(create_app(_services()), "/api/research-top10")())
    assert body["status"] == "AVAILABLE"
    assert body["phase"] == "PREMARKET_RESEARCH"
    assert body["source"] == "IBKR_MANAGED_PLUGIN"
    assert body["strategy_nav_usd"] == "2207.51"
    assert body["normal_risk_fraction"] == "0.10"
    assert body["available_count"] == 10
    assert body["target_count"] == 10
    assert body["target_met"] is True
    assert body["action_pool_count"] == 0
    assert len(body["candidates"]) == 10
    assert body["candidates"] == body["premarket"]
    assert body["open_repriced"] == []
    assert "SOURCE_TRUST_NOT_ESTABLISHED" in body["reason_codes"]
    first_candidate = body["candidates"][0]
    first_leg = first_candidate["legs"][0]
    assert first_candidate["trade_status"] == "NO_TRADE"
    assert first_candidate["entry_debit_usd"] is None
    assert first_candidate["maximum_loss_usd"] is None
    assert first_candidate["indicative_maximum_loss_usd"] == "90.00"
    assert first_candidate["assumed_multiplier"] == 100
    assert first_candidate["risk_cap_verified"] is False
    assert "MAXIMUM_LOSS_UNVERIFIED" in first_candidate["blockers"]
    assert first_leg["local_symbol"] is None
    assert first_leg["multiplier"] is None
    assert first_leg["standard_or_adjusted"] is None
    assert first_leg["quote_asof"] is None
    assert first_leg["collected_at"] == PREMARKET_AT.astimezone(
        ZoneInfo("UTC")
    ).isoformat(timespec="microseconds")
    assert "QUOTE_EXCHANGE_TIMESTAMP_UNAVAILABLE" in first_leg["blockers"]
    assert all(
        item["decision_authority"] == "SUPPORTING_ONLY"
        and item["approval_eligible"] is False
        and item["instruction_creation_allowed"] is False
        and item["order_allowed"] is False
        and item["action_pool_eligible"] is False
        for item in body["candidates"]
    )


def test_premarket_keeps_unquoted_contract_research_visible_without_inventing_prices(
    tmp_path: Path,
) -> None:
    store_path = tmp_path / "research.sqlite3"
    imported = import_research_top10(_unquoted_payload(), store_path=store_path)

    assert imported["status"] == "IMPORTED"
    model = read_research_top10(store_path=store_path)
    assert model["status"] == "AVAILABLE"
    assert model["available_count"] == 10
    assert "QUOTE_UNAVAILABLE" in model["reason_codes"]
    first = model["candidates"][0]
    assert first["indicative_entry_debit_usd"] is None
    assert first["indicative_maximum_loss_usd"] is None
    assert first["indicative_cost_after_ev_usd"] is None
    assert first["assumed_multiplier"] == 100
    assert first["risk_cap_verified"] is False
    assert first["legs"][0]["bid"] is None
    assert first["legs"][0]["ask"] is None


def test_intraday_recovery_imports_parentless_direct_structures_without_authority(
    tmp_path: Path,
) -> None:
    store_path = tmp_path / "research.sqlite3"
    imported = import_research_top10(
        _intraday_recovery_payload(),
        store_path=store_path,
    )

    assert imported["status"] == "IMPORTED"
    model = read_research_top10(store_path=store_path)
    assert model["status"] == "AVAILABLE"
    assert model["phase"] == INTRADAY_RECOVERY
    assert model["source"] == RESEARCH_TOP10_DIRECT_SOURCE
    assert model["parent_content_hash"] is None
    assert model["premarket"] == []
    assert model["open_repriced"] == model["candidates"]
    assert len(model["candidates"]) == 10
    assert all(
        item["trade_status"] == "NO_TRADE"
        and item["decision_authority"] == "SUPPORTING_ONLY"
        and item["approval_eligible"] is False
        and item["instruction_creation_allowed"] is False
        and item["order_allowed"] is False
        and item["risk_cap_verified"] is False
        and "QUOTE_UNAVAILABLE" in item["blockers"]
        for item in model["candidates"]
    )


@pytest.mark.parametrize(
    ("mutation", "match"),
    [
        (
            lambda value: value.__setitem__("observed_at", PREMARKET_AT.isoformat()),
            "cannot precede 09:30",
        ),
        (
            lambda value: value.__setitem__("source", "IBKR_MANAGED_PLUGIN"),
            "direct read-only source",
        ),
        (
            lambda value: value.__setitem__("parent_content_hash", "a" * 64),
            "parentless research",
        ),
    ],
)
def test_intraday_recovery_rejects_invalid_time_source_or_parent(
    tmp_path: Path,
    mutation,
    match: str,
) -> None:
    payload = _intraday_recovery_payload(count=1)
    mutation(payload)

    with pytest.raises(ResearchTop10ValidationError, match=match):
        import_research_top10(payload, store_path=tmp_path / "research.sqlite3")


@pytest.mark.parametrize(
    ("mutation", "match"),
    [
        (
            lambda value: value["candidates"][0]["legs"][0].__setitem__(
                "ask", "1.20"
            ),
            "bid and ask",
        ),
        (
            lambda value: value["candidates"][0]["legs"][0]["blockers"].remove(
                "QUOTE_UNAVAILABLE"
            ),
            "QUOTE_UNAVAILABLE",
        ),
        (
            lambda value: value["candidates"][0]["legs"][0].update(
                {"bid": "1.10", "ask": "1.20"}
            ),
            "QUOTE_UNAVAILABLE",
        ),
        (
            lambda value: (
                value["candidates"][0]["legs"][0].__setitem__(
                    "quote_asof", PREMARKET_AT.isoformat()
                ),
                value["candidates"][0]["legs"][0]["blockers"].remove(
                    "QUOTE_EXCHANGE_TIMESTAMP_UNAVAILABLE"
                ),
            ),
            "quote_asof",
        ),
        (
            lambda value: value["candidates"][0].__setitem__(
                "indicative_entry_debit_usd", "80.00"
            ),
            "unquoted",
        ),
        (
            lambda value: value["candidates"][0].__setitem__(
                "risk_cap_verified", True
            ),
            "risk_cap_verified",
        ),
    ],
)
def test_premarket_unquoted_contracts_fail_closed_on_mixed_or_fabricated_economics(
    tmp_path: Path,
    mutation,
    match: str,
) -> None:
    payload = _unquoted_payload(count=1)
    mutation(payload)

    with pytest.raises(ResearchTop10ValidationError, match=match):
        import_research_top10(payload, store_path=tmp_path / "research.sqlite3")


def test_numeric_quotes_conflict_with_candidate_quote_unavailable_blocker(
    tmp_path: Path,
) -> None:
    payload = _payload(count=1)
    payload["candidates"][0]["blockers"].append("QUOTE_UNAVAILABLE")

    with pytest.raises(ResearchTop10ValidationError, match="QUOTE_UNAVAILABLE"):
        import_research_top10(payload, store_path=tmp_path / "research.sqlite3")


def test_assumed_multiplier_cannot_understate_known_contract_risk(
    tmp_path: Path,
) -> None:
    payload = _payload(count=1)
    payload["strategy_nav_usd"] = "500.00"
    candidate = payload["candidates"][0]
    candidate["assumed_multiplier"] = 1
    candidate["indicative_entry_debit_usd"] = "0.80"
    candidate["indicative_maximum_loss_usd"] = "10.80"
    for leg in candidate["legs"]:
        leg["multiplier"] = 100
        leg["blockers"].remove("MULTIPLIER_UNAVAILABLE")

    with pytest.raises(ResearchTop10ValidationError, match="assumed_multiplier"):
        import_research_top10(payload, store_path=tmp_path / "research.sqlite3")


@pytest.mark.parametrize("cost_cap", ["10.00", "20.00"])
def test_unquoted_candidate_rejects_known_cost_floor_at_ten_percent_nav(
    tmp_path: Path,
    cost_cap: str,
) -> None:
    payload = _unquoted_payload(count=1)
    payload["strategy_nav_usd"] = "100.00"
    payload["candidates"][0]["execution_cost_cap_usd"] = cost_cap

    with pytest.raises(ResearchTop10ValidationError, match="10%"):
        import_research_top10(payload, store_path=tmp_path / "research.sqlite3")


def test_available_ev_conflicts_with_expected_payoff_unavailable_blocker(
    tmp_path: Path,
) -> None:
    payload = _payload(count=1)
    payload["candidates"][0]["blockers"].append(
        "EXPECTED_PAYOFF_UNAVAILABLE"
    )

    with pytest.raises(
        ResearchTop10ValidationError,
        match="EXPECTED_PAYOFF_UNAVAILABLE",
    ):
        import_research_top10(payload, store_path=tmp_path / "research.sqlite3")


def test_open_reprice_requires_quotes_but_allows_bound_debit_and_loss_without_ev(
    tmp_path: Path,
) -> None:
    store_path = tmp_path / "research.sqlite3"
    parent = import_research_top10(_unquoted_payload(), store_path=store_path)
    child = _repriced(str(parent["content_hash"]))
    for candidate in child["candidates"]:
        candidate["indicative_cost_after_ev_usd"] = None
        candidate["blockers"].append("EXPECTED_PAYOFF_UNAVAILABLE")

    assert import_research_top10(child, store_path=store_path)["status"] == "IMPORTED"
    model = read_research_top10(store_path=store_path)
    first = model["open_repriced"][0]
    assert first["indicative_entry_debit_usd"] == "80.00"
    assert first["indicative_maximum_loss_usd"] == "90.00"
    assert first["indicative_cost_after_ev_usd"] is None
    assert "EXPECTED_PAYOFF_UNAVAILABLE" in first["blockers"]

    model["open_repriced"][0]["legs"][0]["bid"] = None
    assert safe_research_top10_read_model(model)["status"] == "UNAVAILABLE"


def test_unquoted_parent_rejects_forged_open_expected_value(tmp_path: Path) -> None:
    store_path = tmp_path / "research.sqlite3"
    parent = import_research_top10(_unquoted_payload(), store_path=store_path)
    forged = _repriced(str(parent["content_hash"]))

    with pytest.raises(ResearchTop10Conflict, match="expected payoff"):
        import_research_top10(forged, store_path=store_path)


def test_quoted_parent_rejects_repriced_ev_that_breaks_bound_payoff(
    tmp_path: Path,
) -> None:
    store_path = tmp_path / "research.sqlite3"
    parent = import_research_top10(_payload(count=1), store_path=store_path)
    forged = _repriced(str(parent["content_hash"]))
    forged["candidates"] = forged["candidates"][:1]
    forged["candidates"][0]["indicative_cost_after_ev_usd"] = "9.00"

    with pytest.raises(ResearchTop10Conflict, match="expected payoff"):
        import_research_top10(forged, store_path=store_path)


@pytest.mark.parametrize(
    ("mutation", "match"),
    [
        (
            lambda value: value["candidates"][0]["legs"][0].update(
                {"bid": None, "ask": None}
            ),
            "INDICATIVE_REPRICE",
        ),
        (
            lambda value: value["candidates"][0]["blockers"].remove(
                "EXPECTED_PAYOFF_UNAVAILABLE"
            ),
            "EXPECTED_PAYOFF_UNAVAILABLE",
        ),
        (
            lambda value: value["candidates"][0].__setitem__(
                "indicative_maximum_loss_usd", None
            ),
            "partial",
        ),
    ],
)
def test_open_reprice_rejects_missing_quotes_or_unbound_partial_economics(
    tmp_path: Path,
    mutation,
    match: str,
) -> None:
    store_path = tmp_path / "research.sqlite3"
    parent = import_research_top10(_unquoted_payload(), store_path=store_path)
    child = _repriced(str(parent["content_hash"]))
    for candidate in child["candidates"]:
        candidate["indicative_cost_after_ev_usd"] = None
        candidate["blockers"].append("EXPECTED_PAYOFF_UNAVAILABLE")
    mutation(child)

    with pytest.raises(ResearchTop10ValidationError, match=match):
        import_research_top10(child, store_path=store_path)


@pytest.mark.parametrize(
    ("mutation", "match"),
    [
        (
            lambda value: value["candidates"][0]["legs"][0]["blockers"].remove(
                "DELTA_UNAVAILABLE"
            ),
            "DELTA_UNAVAILABLE",
        ),
        (
            lambda value: value["session"]["blockers"].clear(),
            "BROKER_SESSION_HOURS_UNAVAILABLE",
        ),
    ],
)
def test_null_greeks_and_session_fields_require_explicit_blockers(
    tmp_path: Path,
    mutation,
    match: str,
) -> None:
    payload = _payload()
    mutation(payload)

    with pytest.raises(ResearchTop10ValidationError, match=match):
        import_research_top10(payload, store_path=tmp_path / "research.sqlite3")


@pytest.mark.parametrize(
    ("field", "blocker"),
    [
        ("local_symbol", "LOCAL_SYMBOL_UNAVAILABLE"),
        ("multiplier", "MULTIPLIER_UNAVAILABLE"),
        ("standard_or_adjusted", "STANDARD_ADJUSTED_UNVERIFIED"),
        ("quote_asof", "QUOTE_EXCHANGE_TIMESTAMP_UNAVAILABLE"),
    ],
)
def test_missing_contract_identity_and_quote_timestamp_are_null_with_blockers(
    tmp_path: Path,
    field: str,
    blocker: str,
) -> None:
    payload = _payload(count=1)
    leg = payload["candidates"][0]["legs"][0]
    assert leg[field] is None
    assert blocker in leg["blockers"]
    leg["blockers"].remove(blocker)

    with pytest.raises(ResearchTop10ValidationError, match=blocker):
        import_research_top10(payload, store_path=tmp_path / "research.sqlite3")


def test_unverified_multiplier_never_masquerades_as_exact_risk(tmp_path: Path) -> None:
    payload = _payload(count=1)
    candidate = payload["candidates"][0]

    assert candidate["entry_debit_usd"] is None
    assert candidate["maximum_loss_usd"] is None
    assert candidate["risk_cap_verified"] is False
    assert candidate["trade_status"] == "NO_TRADE"
    candidate["maximum_loss_usd"] = candidate["indicative_maximum_loss_usd"]

    with pytest.raises(ResearchTop10ValidationError, match="maximum_loss_usd"):
        import_research_top10(payload, store_path=tmp_path / "research.sqlite3")


@pytest.mark.parametrize(
    "mutation",
    [
        lambda value: [
            leg.__setitem__("multiplier", 50)
            for leg in value["candidates"][0]["legs"]
        ],
        lambda value: (
            value["candidates"][0]["legs"][0].__setitem__(
                "market_data_type", None
            ),
            value["candidates"][0]["legs"][0]["blockers"].append(
                "MARKET_DATA_TYPE_UNVERIFIED"
            ),
        ),
        lambda value: (
            value["candidates"][0]["legs"][0].__setitem__("quote_asof", None),
            value["candidates"][0]["legs"][0]["blockers"].append(
                "QUOTE_EXCHANGE_TIMESTAMP_UNAVAILABLE"
            ),
        ),
        lambda value: [
            leg.__setitem__(
                "quote_asof", (PREMARKET_AT - timedelta(seconds=6)).isoformat()
            )
            for leg in value["candidates"][0]["legs"]
        ],
        lambda value: value["candidates"][0]["legs"][1].__setitem__(
            "quote_asof", (PREMARKET_AT - timedelta(seconds=3)).isoformat()
        ),
    ],
)
def test_exact_risk_requires_standard_live_synchronised_quote_inputs(
    tmp_path: Path,
    mutation,
) -> None:
    payload = _verified_payload(count=1)
    mutation(payload)

    with pytest.raises(ResearchTop10ValidationError, match="risk_cap_verified"):
        import_research_top10(payload, store_path=tmp_path / "research.sqlite3")


@pytest.mark.parametrize(
    ("mutation", "match"),
    [
        (
            lambda value: value["candidates"][0]["legs"][1].__setitem__(
                "strike", "102.50"
            ),
            "width",
        ),
        (lambda value: value.__setitem__("strategy_nav_usd", "899.99"), "10%"),
    ],
)
def test_indicative_risk_still_respects_width_and_normal_nav_cap(
    tmp_path: Path,
    mutation,
    match: str,
) -> None:
    payload = _payload(count=1)
    mutation(payload)

    with pytest.raises(ResearchTop10ValidationError, match=match):
        import_research_top10(payload, store_path=tmp_path / "research.sqlite3")


@pytest.mark.parametrize(
    ("mutation", "match"),
    [
        (
            lambda value: (
                value["candidates"][0]["legs"][0].__setitem__(
                    "quote_asof", (PREMARKET_AT + timedelta(seconds=1)).isoformat()
                ),
                value["candidates"][0]["legs"][0]["blockers"].remove(
                    "QUOTE_EXCHANGE_TIMESTAMP_UNAVAILABLE"
                ),
            ),
            "quote_asof.*collected_at",
        ),
        (
            lambda value: value["candidates"][0]["legs"][0].__setitem__(
                "collected_at", (PREMARKET_AT + timedelta(seconds=1)).isoformat()
            ),
            "collected_at.*observed_at",
        ),
    ],
)
def test_quote_collection_time_cannot_move_after_its_point_in_time_parent(
    tmp_path: Path,
    mutation,
    match: str,
) -> None:
    payload = _payload(count=1)
    mutation(payload)

    with pytest.raises(ResearchTop10ValidationError, match=match):
        import_research_top10(payload, store_path=tmp_path / "research.sqlite3")


@pytest.mark.parametrize(
    ("mutation", "match"),
    [
        (
            lambda value: value["candidates"][0].__setitem__(
                "maximum_loss_usd", "90.01"
            ),
            "maximum loss",
        ),
        (
            lambda value: value["candidates"][0].__setitem__("dte", 13),
            "14-35 DTE",
        ),
        (lambda value: value.__setitem__("strategy_nav_usd", "899.99"), "10%"),
        (
            lambda value: value["candidates"][0].__setitem__(
                "strategy_type", "BULL_PUT_VERTICAL"
            ),
            "debit vertical",
        ),
    ],
)
def test_only_exact_loss_capped_14_to_35_dte_debit_verticals_are_accepted(
    tmp_path: Path,
    mutation,
    match: str,
) -> None:
    payload = _verified_payload()
    mutation(payload)

    with pytest.raises(ResearchTop10ValidationError, match=match):
        import_research_top10(payload, store_path=tmp_path / "research.sqlite3")


@pytest.mark.parametrize("verified", [False, True])
def test_direct_import_rejects_impossible_vertical_expected_payoff(
    tmp_path: Path,
    verified: bool,
) -> None:
    payload = _verified_payload() if verified else _payload()
    field = (
        "cost_after_ev_usd"
        if verified
        else "indicative_cost_after_ev_usd"
    )
    # $80 debit + $10 execution cost + $10.01 EV implies a $100.01
    # gross payoff, above the $100 cap of this one-point vertical.
    payload["candidates"][0][field] = "10.01"

    with pytest.raises(
        ResearchTop10ValidationError,
        match="expected payoff exceeds vertical maximum payout",
    ):
        import_research_top10(payload, store_path=tmp_path / "research.sqlite3")


def test_direct_unquoted_import_requires_expected_payoff_blocker(
    tmp_path: Path,
) -> None:
    payload = _unquoted_payload(count=1)
    payload["candidates"][0]["blockers"].remove(
        "EXPECTED_PAYOFF_UNAVAILABLE"
    )

    with pytest.raises(
        ResearchTop10ValidationError,
        match="EXPECTED_PAYOFF_UNAVAILABLE",
    ):
        import_research_top10(payload, store_path=tmp_path / "research.sqlite3")


def test_exact_risk_rejects_zero_execution_cost_assumption(tmp_path: Path) -> None:
    payload = _verified_payload(count=1)
    payload["candidates"][0]["execution_cost_cap_usd"] = "0.00"
    payload["candidates"][0]["maximum_loss_usd"] = "80.00"

    with pytest.raises(ResearchTop10ValidationError, match="positive"):
        import_research_top10(payload, store_path=tmp_path / "research.sqlite3")


def test_shortfall_is_visible_but_never_padded_or_actionable(tmp_path: Path) -> None:
    result = import_research_top10(
        _payload(count=7),
        store_path=tmp_path / "research.sqlite3",
    )

    assert result["status"] == "IMPORTED"
    with ResearchTop10Store(tmp_path / "research.sqlite3") as store:
        model = store.read_model()
    assert model["available_count"] == 7
    assert model["target_met"] is False
    assert "TOP10_RESEARCH_SHORTFALL" in model["reason_codes"]
    assert model["action_pool_count"] == 0


def test_reprice_binds_exact_parent_and_conflicts_roll_back_atomically(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store_path = tmp_path / "research.sqlite3"
    monkeypatch.setenv("OPTIONS_COPILOT_RESEARCH_TOP10_PATH", str(store_path))
    parent = import_research_top10(_payload(), store_path=store_path)
    repriced_payload = _repriced(str(parent["content_hash"]))
    repriced = import_research_top10(repriced_payload, store_path=store_path)

    assert repriced["status"] == "IMPORTED"
    with ResearchTop10Store(store_path) as store:
        before = store.read_model()
        assert store.batch_count() == 2
    assert before["phase"] == "INDICATIVE_REPRICE"
    assert before["parent_content_hash"] == parent["content_hash"]
    assert before["action_pool_count"] == 0
    assert len(before["premarket"]) == 10
    assert len(before["open_repriced"]) == 10
    assert before["candidates"] == before["open_repriced"]
    assert before["premarket"] != before["open_repriced"]
    api_model = asyncio.run(_route(create_app(_services()), "/api/research-top10")())
    assert len(api_model["premarket"]) == 10
    assert len(api_model["open_repriced"]) == 10
    assert api_model["candidates"] == api_model["open_repriced"]

    conflict = _repriced(str(parent["content_hash"]))
    conflict["batch_id"] = "different-reprice-batch"
    conflict["candidates"][0]["research_summary"] = "Conflicting rewrite."
    with pytest.raises(ResearchTop10Conflict):
        import_research_top10(conflict, store_path=store_path)

    isolated_path = tmp_path / "identity-mismatch.sqlite3"
    isolated_parent = import_research_top10(_payload(), store_path=isolated_path)
    identity_mismatch = _repriced(str(isolated_parent["content_hash"]))
    identity_mismatch["candidates"][0]["legs"][0]["exchange"] = "CBOE"
    with pytest.raises(ResearchTop10Conflict, match="parent"):
        import_research_top10(identity_mismatch, store_path=isolated_path)

    with ResearchTop10Store(store_path) as store:
        assert store.batch_count() == 2
        assert store.read_model() == before


@pytest.mark.parametrize(
    "field",
    ["strategy_nav_usd", "execution_cost_cap_usd", "assumed_multiplier"],
)
def test_reprice_cannot_change_parent_budget_or_cost_assumptions(
    tmp_path: Path,
    field: str,
) -> None:
    store_path = tmp_path / "research.sqlite3"
    parent = import_research_top10(_payload(count=1), store_path=store_path)
    child = _repriced(str(parent["content_hash"]))
    child["candidates"] = child["candidates"][:1]
    if field == "strategy_nav_usd":
        child[field] = "2300.00"
    elif field == "execution_cost_cap_usd":
        child["candidates"][0][field] = "11.00"
        child["candidates"][0]["indicative_maximum_loss_usd"] = "91.00"
        child["candidates"][0]["indicative_cost_after_ev_usd"] = "9.00"
    else:
        child["candidates"][0][field] = 50
        child["candidates"][0]["indicative_entry_debit_usd"] = "40.00"
        child["candidates"][0]["indicative_maximum_loss_usd"] = "50.00"

    expected_error = (
        ResearchTop10ValidationError
        if field == "assumed_multiplier"
        else ResearchTop10Conflict
    )
    expected_match = "assumed_multiplier" if field == "assumed_multiplier" else "parent"
    with pytest.raises(expected_error, match=expected_match):
        import_research_top10(child, store_path=store_path)


def test_import_rejects_observation_more_than_five_seconds_in_future(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        research_top10_module,
        "_utc_now",
        lambda: PREMARKET_AT.astimezone(ZoneInfo("UTC")) - timedelta(seconds=6),
    )

    with pytest.raises(ResearchTop10ValidationError, match="future"):
        import_research_top10(_payload(count=1), store_path=tmp_path / "research.sqlite3")


def test_stored_payload_hash_is_reverified_on_every_read(tmp_path: Path) -> None:
    store_path = tmp_path / "research.sqlite3"
    import_research_top10(_payload(count=1), store_path=store_path)
    with sqlite3.connect(store_path) as connection:
        connection.execute("DROP TRIGGER research_top10_no_update")
        connection.execute(
            "UPDATE research_top10_batches SET content_hash=?",
            ("a" * 64,),
        )

    model = read_research_top10(store_path=store_path)
    assert model["status"] == "UNAVAILABLE"
    assert model["reason_codes"] == ["RESEARCH_TOP10_STORE_INVALID"]


def test_api_custom_provider_cannot_escalate_research_authority() -> None:
    unsafe = {
        "schema": "options_copilot.research_top10_read_model.v2",
        "status": "AVAILABLE",
        "phase": "PREMARKET_RESEARCH",
        "trading_date": TRADE_DATE,
        "observed_at": PREMARKET_AT.isoformat(),
        "batch_id": "unsafe",
        "content_hash": "a" * 64,
        "parent_content_hash": None,
        "source": "IBKR_MANAGED_PLUGIN",
        "strategy_nav_usd": "2207.51",
        "normal_risk_fraction": "0.10",
        "target_count": 10,
        "available_count": 1,
        "target_met": False,
        "reason_codes": [],
        "blockers": [],
        "session": {},
        "candidates": [],
        "premarket": [],
        "open_repriced": [],
        "decision_authority": "SUPPORTING_ONLY",
        "approval_eligible": False,
        "instruction_creation_allowed": False,
        "order_allowed": True,
        "action_pool_eligible": False,
        "action_pool_count": 0,
    }
    body = asyncio.run(
        _route(
            create_app(_services(research_top10_provider=lambda: unsafe)),
            "/api/research-top10",
        )()
    )
    assert body["status"] == "UNAVAILABLE"
    assert body["reason_codes"] == ["RESEARCH_TOP10_INVALID"]
    assert body["order_allowed"] is False
    assert body["action_pool_count"] == 0


@pytest.mark.parametrize("nested", ["candidate", "leg"])
def test_safe_read_model_rejects_nested_extra_fields(
    tmp_path: Path,
    nested: str,
) -> None:
    store_path = tmp_path / "research.sqlite3"
    import_research_top10(_payload(count=1), store_path=store_path)
    with ResearchTop10Store(store_path) as store:
        model = store.read_model()
    target = (
        model["candidates"][0]
        if nested == "candidate"
        else model["candidates"][0]["legs"][0]
    )
    target["unexpected"] = "must not pass provider seam"

    safe = safe_research_top10_read_model(model)
    assert safe["status"] == "UNAVAILABLE"
    assert safe["reason_codes"] == ["RESEARCH_TOP10_INVALID"]


@pytest.mark.parametrize(
    "mutation",
    [
        lambda row: row.__setitem__("strategy_type", "NAKED_CALL"),
        lambda row: row.__setitem__("dte", 0),
        lambda row: row.__setitem__("quantity", -1),
        lambda row: row.__setitem__("maximum_loss_usd", "999.00"),
        lambda row: row["legs"][1].__setitem__("side", "BUY"),
    ],
)
def test_safe_read_model_reuses_full_envelope_validation(
    tmp_path: Path,
    mutation,
) -> None:
    store_path = tmp_path / "research.sqlite3"
    import_research_top10(_verified_payload(count=1), store_path=store_path)
    with ResearchTop10Store(store_path) as store:
        model = store.read_model()
    mutation(model["candidates"][0])
    model["premarket"] = model["candidates"]

    safe = safe_research_top10_read_model(model)
    assert safe["status"] == "UNAVAILABLE"
    assert safe["reason_codes"] == ["RESEARCH_TOP10_INVALID"]


def test_safe_read_model_rejects_tampered_historical_stage(tmp_path: Path) -> None:
    store_path = tmp_path / "research.sqlite3"
    parent = import_research_top10(_payload(), store_path=store_path)
    import_research_top10(_repriced(str(parent["content_hash"])), store_path=store_path)
    with ResearchTop10Store(store_path) as store:
        model = store.read_model()
    model["premarket"][0]["research_summary"] = "tampered historical stage"

    safe = safe_research_top10_read_model(model)
    assert safe["status"] == "UNAVAILABLE"
    assert safe["reason_codes"] == ["RESEARCH_TOP10_INVALID"]


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("available_count", 2),
        ("target_count", 9),
        ("target_met", True),
        ("status", "AVAILABLE"),
    ],
)
def test_safe_read_model_rejects_forged_counts_and_status(
    tmp_path: Path,
    field: str,
    value: object,
) -> None:
    store_path = tmp_path / "research.sqlite3"
    import_research_top10(_payload(count=1), store_path=store_path)
    with ResearchTop10Store(store_path) as store:
        model = store.read_model()
    model[field] = value

    safe = safe_research_top10_read_model(model)
    assert safe["status"] == "UNAVAILABLE"
    assert safe["reason_codes"] == ["RESEARCH_TOP10_INVALID"]


def test_cli_imports_from_file_without_exposing_store_path(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    input_path = tmp_path / "input.json"
    input_path.write_text(json.dumps(_payload()), encoding="utf-8")
    store_path = tmp_path / "research.sqlite3"

    assert main(["--input", str(input_path), "--store", str(store_path)]) == 0
    output = capsys.readouterr()
    parsed = json.loads(output.out)
    assert parsed["status"] == "IMPORTED"
    assert str(store_path) not in output.out
    assert output.err == ""


@pytest.mark.parametrize(
    ("value", "match"),
    [
        ("sk-testCredentialValue123", "secret-like"),
        (r"C:\Users\example\private\research.json", "local path"),
        ("x" * 4097, "4096"),
    ],
)
def test_import_rejects_secrets_local_paths_and_oversized_free_text(
    tmp_path: Path,
    value: str,
    match: str,
) -> None:
    payload = _payload(count=1)
    payload["candidates"][0]["research_summary"] = value

    with pytest.raises(ResearchTop10ValidationError, match=match):
        import_research_top10(payload, store_path=tmp_path / "research.sqlite3")


@pytest.mark.parametrize(
    "value",
    [
        "Bearer dangerous-provider-credential",
        "x" * ((1024 * 1024) + 1),
    ],
    ids=["secret", "oversized"],
)
def test_safe_provider_projection_rejects_secret_or_one_mib_amplification(
    tmp_path: Path,
    value: str,
) -> None:
    store_path = tmp_path / "research.sqlite3"
    import_research_top10(_payload(count=1), store_path=store_path)
    with ResearchTop10Store(store_path) as store:
        model = store.read_model()
    model["candidates"][0]["research_summary"] = value

    safe = safe_research_top10_read_model(model)
    assert safe["status"] == "UNAVAILABLE"
    assert safe["reason_codes"] == ["RESEARCH_TOP10_INVALID"]


def test_cli_rejects_import_larger_than_one_mib(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    input_path = tmp_path / "oversized.json"
    input_path.write_bytes(b" " * ((1024 * 1024) + 1))

    assert main(["--input", str(input_path), "--store", str(tmp_path / "db.sqlite3")]) == 2
    output = capsys.readouterr()
    assert output.out == ""
    assert output.err == "NO_TRADE: RESEARCH_TOP10_IMPORT_INVALID\n"


def test_inprocess_import_rejects_normalized_envelope_larger_than_one_mib(
    tmp_path: Path,
) -> None:
    payload = _payload(count=1)
    evidence_ids = [f"{index:04d}-" + ("x" * 4090) for index in range(260)]
    payload["candidates"][0]["evidence_ids"] = evidence_ids
    payload["candidates"][0]["evidence_hashes"] = [
        _digest(value) for value in evidence_ids
    ]

    with pytest.raises(ResearchTop10ValidationError, match="one MiB"):
        import_research_top10(payload, store_path=tmp_path / "research.sqlite3")


def _route(app, path: str):
    return next(route.endpoint for route in app.routes if route.path == path)
