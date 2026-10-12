from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta
import json
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

import options_copilot.news.research_top10 as research_top10_module
import options_copilot.research_top10_producer_cli as producer_cli_module
from options_copilot.news.managed_research_producer import (
    ManagedResearchInputError,
    assemble_managed_research,
    atomic_write_research_envelope,
)
from options_copilot.news.research_top10 import (
    import_research_top10,
    read_research_top10,
    validate_research_top10_envelope,
)
from options_copilot.research_top10_producer_cli import main
from options_copilot.storage.canonical import canonical_hash


NEW_YORK = ZoneInfo("America/New_York")
TRADE_DATE = "2026-08-06"
EXPIRATION = "2026-08-21"
PREMARKET_AT = datetime(2026, 8, 6, 9, 20, 12, tzinfo=NEW_YORK)
REPRICE_AT = datetime(2026, 8, 6, 9, 35, 18, tzinfo=NEW_YORK)


@pytest.fixture(autouse=True)
def _fixed_import_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    now = REPRICE_AT.astimezone(ZoneInfo("UTC")) + timedelta(minutes=1)
    monkeypatch.setattr(research_top10_module, "_utc_now", lambda: now)


def _run_document(
    *,
    phase: str = "PREMARKET_RESEARCH",
    reprice: bool = False,
) -> dict[str, object]:
    observed_at = REPRICE_AT if reprice else PREMARKET_AT
    run_id = "managed-run-open" if reprice else "managed-run-premarket"
    batch_id = "managed-batch-open" if reprice else "managed-batch-premarket"
    account = {
        "run_id": run_id,
        "batch_id": batch_id,
        "observed_at": observed_at.isoformat(),
        "currency": "USD",
        "strategy_nav_usd": "2207.51",
        "blockers": [],
    }
    session = {
        "run_id": run_id,
        "batch_id": batch_id,
        "liquid_hours": "20260806:0930-1600",
        "trading_hours": "20260806:0930-1600",
        "timezone_id": "America/New_York",
        "observed_at": observed_at.isoformat(),
        "source": "IBKR_MANAGED_PLUGIN",
        "blockers": [],
    }
    secdefs: list[dict[str, object]] = []
    quotes: list[dict[str, object]] = []
    evidence: list[dict[str, object]] = []
    for index in range(1, 11):
        symbol = f"T{index:02d}"
        evidence.append(
            {
                "run_id": run_id,
                "batch_id": batch_id,
                "evidence_id": f"news-{index:02d}",
                "evidence_hash": canonical_hash({"news": index}),
            }
        )
        for leg_index, strike in enumerate((100 + index * 2, 101 + index * 2)):
            contract_id = 810_000_000 + index * 10 + leg_index
            secdefs.append(
                {
                    "run_id": run_id,
                    "batch_id": batch_id,
                    "contract_id": contract_id,
                    "contract_id_ex": f"{contract_id}@SMART",
                    "underlying": symbol,
                    "right": "C",
                    "strike": str(strike),
                    "expiration": EXPIRATION,
                    "exchange": "SMART",
                    "trading_class": symbol,
                    "local_symbol": f"{symbol}  260821C00{strike}000",
                    "multiplier": 100,
                    "standard_or_adjusted": "STANDARD",
                    "evidence_hash": canonical_hash(
                        {"contract_id": contract_id, "identity": "SMART"}
                    ),
                    "blockers": [],
                }
            )
            is_buy = leg_index == 0
            if is_buy:
                bid, ask = (("1.20", "1.35") if reprice else ("1.10", "1.20"))
            else:
                bid, ask = (("0.46", "0.55") if reprice else ("0.40", "0.50"))
            quotes.append(
                {
                    "run_id": run_id,
                    "batch_id": batch_id,
                    "contract_id": contract_id,
                    "bid": bid,
                    "ask": ask,
                    "collected_at": observed_at.isoformat(),
                    "quote_asof": observed_at.isoformat(),
                    "implied_volatility": "0.25",
                    "delta": "0.50" if is_buy else "0.45",
                    "gamma": "0.02",
                    "theta": "-0.03",
                    "vega": "0.04",
                    "volume": 100 + index,
                    "open_interest": 1000 + index,
                    "market_data_type": 1,
                    "evidence_hash": canonical_hash(
                        {"contract_id": contract_id, "reprice": reprice}
                    ),
                    "blockers": [],
                }
            )

    component_hashes = {
        "account": canonical_hash(account),
        "session": canonical_hash(session),
        "secdefs": canonical_hash(secdefs),
        "quotes": canonical_hash(quotes),
        "evidence": canonical_hash(evidence),
    }
    if reprice:
        candidates: list[dict[str, object]] = [
            {
                "research_id": f"research-{TRADE_DATE}-{index:02d}",
                "rank": index,
                "bindings": dict(component_hashes),
            }
            for index in range(1, 11)
        ]
    else:
        candidates = []
        for index in range(1, 11):
            lower_id = 810_000_000 + index * 10
            candidates.append(
                {
                    "research_id": f"research-{TRADE_DATE}-{index:02d}",
                    "rank": index,
                    "underlying": f"T{index:02d}",
                    "strategy_type": "BULL_CALL_VERTICAL",
                    "expiration": EXPIRATION,
                    "quantity": 1,
                    "assumed_multiplier": 100,
                    "execution_cost_cap_usd": "10.00",
                    "expected_payoff_usd": "100.00",
                    "entry_condition": "Enter only when the declared debit cap holds.",
                    "invalidation_condition": "The directional thesis is invalidated.",
                    "profit_target_condition": "Close at the declared research target.",
                    "stop_loss_condition": "Close before the defined-risk threshold.",
                    "research_summary": f"Supporting-only research for T{index:02d}.",
                    "evidence_ids": [f"news-{index:02d}"],
                    "legs": [
                        {"contract_id": lower_id, "side": "BUY"},
                        {"contract_id": lower_id + 1, "side": "SELL"},
                    ],
                    "bindings": dict(component_hashes),
                }
            )
    component_hashes["candidates"] = canonical_hash(candidates)
    return {
        "schema": "options_copilot.managed_research_run.v1",
        "version": 1,
        "run_id": run_id,
        "batch_id": batch_id,
        "phase": phase,
        "trading_date": TRADE_DATE,
        "observed_at": observed_at.isoformat(),
        "source": "IBKR_MANAGED_PLUGIN",
        "component_hashes": component_hashes,
        "account": account,
        "session": session,
        "secdefs": secdefs,
        "quotes": quotes,
        "evidence": evidence,
        "candidates": candidates,
    }


def _rehash_component(document: dict[str, object], name: str) -> None:
    component_hashes = document["component_hashes"]
    assert isinstance(component_hashes, dict)
    component_hashes[name] = canonical_hash(document[name])
    if name == "candidates":
        return
    candidates = document["candidates"]
    assert isinstance(candidates, list)
    for candidate in candidates:
        assert isinstance(candidate, dict)
        bindings = candidate["bindings"]
        assert isinstance(bindings, dict)
        bindings[name] = component_hashes[name]
    component_hashes["candidates"] = canonical_hash(candidates)


def _make_quotes_unavailable(document: dict[str, object]) -> None:
    for quote in document["quotes"]:
        quote["bid"] = None
        quote["ask"] = None
        quote["blockers"].append("QUOTE_UNAVAILABLE")
        quote["quote_asof"] = None
        quote["blockers"].append("QUOTE_EXCHANGE_TIMESTAMP_UNAVAILABLE")
    _rehash_component(document, "quotes")


def _set_expected_payoff(
    document: dict[str, object], value: str | None
) -> None:
    for candidate in document["candidates"]:
        candidate["expected_payoff_usd"] = value
    _rehash_component(document, "candidates")


def test_premarket_assembler_builds_valid_bound_v2_envelope() -> None:
    document = _run_document()

    envelope = assemble_managed_research(document)
    validated = validate_research_top10_envelope(envelope)

    assert validated["phase"] == "PREMARKET_RESEARCH"
    assert validated["batch_id"] == "managed-batch-premarket"
    assert len(validated["candidates"]) == 10
    first = validated["candidates"][0]
    assert first["indicative_entry_debit_usd"] == "80.00"
    assert first["indicative_maximum_loss_usd"] == "90.00"
    assert first["indicative_cost_after_ev_usd"] == "10.00"
    assert first["entry_debit_usd"] is None
    assert first["risk_cap_verified"] is False
    assert first["legs"][0]["identity_evidence_hash"] == document["secdefs"][0]["evidence_hash"]
    assert first["legs"][0]["quote_evidence_hash"] == document["quotes"][0]["evidence_hash"]


def test_premarket_assembler_keeps_unquoted_contracts_and_null_economics() -> None:
    document = _run_document()
    _make_quotes_unavailable(document)
    _set_expected_payoff(document, None)

    envelope = assemble_managed_research(document)
    first = validate_research_top10_envelope(envelope)["candidates"][0]

    assert first["indicative_entry_debit_usd"] is None
    assert first["indicative_maximum_loss_usd"] is None
    assert first["indicative_cost_after_ev_usd"] is None
    assert first["assumed_multiplier"] == 100
    assert first["risk_cap_verified"] is False
    assert "QUOTE_UNAVAILABLE" in first["blockers"]
    assert first["legs"][0]["bid"] is None
    assert first["legs"][0]["ask"] is None


@pytest.mark.parametrize(
    ("mutation", "match"),
    [
        (lambda quote: quote.__setitem__("ask", "1.20"), "bid and ask"),
        (lambda quote: quote["blockers"].remove("QUOTE_UNAVAILABLE"), "QUOTE_UNAVAILABLE"),
        (
            lambda quote: quote.update({"bid": "1.10", "ask": "1.20"}),
            "QUOTE_UNAVAILABLE",
        ),
        (
            lambda quote: (
                quote.__setitem__("quote_asof", PREMARKET_AT.isoformat()),
                quote["blockers"].remove("QUOTE_EXCHANGE_TIMESTAMP_UNAVAILABLE"),
            ),
            "quote_asof",
        ),
    ],
)
def test_premarket_assembler_rejects_mixed_or_conflicting_unavailable_quotes(
    mutation,
    match: str,
) -> None:
    document = _run_document()
    _make_quotes_unavailable(document)
    mutation(document["quotes"][0])
    _rehash_component(document, "quotes")

    with pytest.raises(ManagedResearchInputError, match=match):
        assemble_managed_research(document)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda document: document["account"].__setitem__("strategy_nav_usd", "9999"),
        lambda document: document["quotes"][0].__setitem__("run_id", "different-run"),
        lambda document: document["candidates"][0]["bindings"].__setitem__(
            "session", "0" * 64
        ),
    ],
)
def test_assembler_rejects_component_hash_run_and_candidate_binding_mismatch(
    mutate,
) -> None:
    document = _run_document()
    mutate(document)

    with pytest.raises(ManagedResearchInputError):
        assemble_managed_research(document)


def test_missing_trade_grade_value_requires_its_explicit_blocker() -> None:
    document = _run_document()
    quote = document["quotes"][0]
    quote["implied_volatility"] = None
    document["component_hashes"]["quotes"] = canonical_hash(document["quotes"])
    for candidate in document["candidates"]:
        candidate["bindings"]["quotes"] = document["component_hashes"]["quotes"]
    document["component_hashes"]["candidates"] = canonical_hash(
        document["candidates"]
    )

    with pytest.raises(ManagedResearchInputError):
        assemble_managed_research(document)

    quote["blockers"] = ["IMPLIED_VOLATILITY_UNAVAILABLE"]
    document["component_hashes"]["quotes"] = canonical_hash(document["quotes"])
    for candidate in document["candidates"]:
        candidate["bindings"]["quotes"] = document["component_hashes"]["quotes"]
    document["component_hashes"]["candidates"] = canonical_hash(
        document["candidates"]
    )
    envelope = assemble_managed_research(document)
    assert "IMPLIED_VOLATILITY_UNAVAILABLE" in envelope["candidates"][0]["legs"][0]["blockers"]


def test_assembler_rejects_capture_before_premarket_window() -> None:
    document = _run_document()
    too_early = PREMARKET_AT.replace(hour=9, minute=15, second=0)
    document["observed_at"] = too_early.isoformat()

    with pytest.raises(ManagedResearchInputError, match="slot window"):
        assemble_managed_research(document)


def test_quote_freshness_accepts_five_seconds_and_rejects_after_boundary() -> None:
    boundary = _run_document()
    for quote in boundary["quotes"]:
        quote["collected_at"] = (PREMARKET_AT - timedelta(seconds=5)).isoformat()
        quote["quote_asof"] = (PREMARKET_AT - timedelta(seconds=5)).isoformat()
    _rehash_component(boundary, "quotes")
    assert len(assemble_managed_research(boundary)["candidates"]) == 10

    stale = _run_document()
    for quote in stale["quotes"]:
        quote["collected_at"] = (
            PREMARKET_AT - timedelta(seconds=5, microseconds=1)
        ).isoformat()
        quote["quote_asof"] = (
            PREMARKET_AT - timedelta(seconds=5, microseconds=1)
        ).isoformat()
    _rehash_component(stale, "quotes")
    with pytest.raises(ManagedResearchInputError, match="stale"):
        assemble_managed_research(stale)


def test_account_freshness_accepts_five_seconds_and_rejects_after_boundary() -> None:
    boundary = _run_document()
    boundary["account"]["observed_at"] = (
        PREMARKET_AT - timedelta(seconds=5)
    ).isoformat()
    _rehash_component(boundary, "account")
    assert len(assemble_managed_research(boundary)["candidates"]) == 10

    stale = _run_document()
    stale["account"]["observed_at"] = (
        PREMARKET_AT - timedelta(seconds=5, microseconds=1)
    ).isoformat()
    _rehash_component(stale, "account")
    with pytest.raises(ManagedResearchInputError, match="account.observed_at is stale"):
        assemble_managed_research(stale)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    (
        ("observed_at", (PREMARKET_AT - timedelta(seconds=6)).isoformat(), "stale"),
        ("timezone_id", "UTC", "timezone"),
        ("source", "UNTRUSTED", "source"),
        ("liquid_hours", "20260805:0930-1600", "trading date"),
    ),
)
def test_session_rejects_stale_or_semantically_unbound_values(
    field: str,
    value: str,
    message: str,
) -> None:
    document = _run_document()
    document["session"][field] = value
    _rehash_component(document, "session")

    with pytest.raises(ManagedResearchInputError, match=message):
        assemble_managed_research(document)


def test_vertical_quote_skew_accepts_two_seconds_and_rejects_after_boundary() -> None:
    boundary = _run_document()
    boundary["quotes"][1]["quote_asof"] = (
        PREMARKET_AT - timedelta(seconds=2)
    ).isoformat()
    _rehash_component(boundary, "quotes")
    assert len(assemble_managed_research(boundary)["candidates"]) == 10

    skewed = _run_document()
    skewed["quotes"][1]["quote_asof"] = (
        PREMARKET_AT - timedelta(seconds=2, microseconds=1)
    ).isoformat()
    _rehash_component(skewed, "quotes")
    with pytest.raises(ManagedResearchInputError, match="skew"):
        assemble_managed_research(skewed)


@pytest.mark.parametrize("declared", [1, 50])
def test_declared_multiplier_must_match_known_contract_multiplier(declared: int) -> None:
    document = _run_document()
    document["candidates"][0]["assumed_multiplier"] = declared
    _rehash_component(document, "candidates")

    with pytest.raises(ManagedResearchInputError, match="multiplier"):
        assemble_managed_research(document)


def test_candidate_rejects_inconsistent_leg_multipliers() -> None:
    document = _run_document()
    document["secdefs"][0]["multiplier"] = 50
    _rehash_component(document, "secdefs")

    with pytest.raises(ManagedResearchInputError, match="multiplier"):
        assemble_managed_research(document)


def test_unknown_contract_multiplier_uses_blocked_policy_assumption() -> None:
    document = _run_document()
    for secdef in document["secdefs"][:2]:
        secdef["multiplier"] = None
        secdef["blockers"] = ["MULTIPLIER_UNAVAILABLE"]
    _rehash_component(document, "secdefs")

    envelope = assemble_managed_research(document)
    first = envelope["candidates"][0]
    assert first["assumed_multiplier"] == 100
    assert "MULTIPLIER_UNAVAILABLE" in first["blockers"]


def test_reprice_recovers_parent_thesis_identity_and_evidence_from_store(
    tmp_path: Path,
) -> None:
    store = tmp_path / "research.sqlite3"
    parent = assemble_managed_research(_run_document())
    imported = import_research_top10(parent, store_path=store)

    child = assemble_managed_research(
        _run_document(phase="INDICATIVE_REPRICE", reprice=True),
        store_path=store,
    )

    assert child["parent_content_hash"] == imported["content_hash"]
    assert len(child["candidates"]) == 10
    for parent_candidate, child_candidate in zip(
        parent["candidates"], child["candidates"], strict=True
    ):
        for field in (
            "research_id",
            "rank",
            "underlying",
            "strategy_type",
            "expiration",
            "quantity",
            "entry_condition",
            "invalidation_condition",
            "profit_target_condition",
            "stop_loss_condition",
            "research_summary",
            "evidence_ids",
            "evidence_hashes",
        ):
            assert child_candidate[field] == parent_candidate[field]
        assert child_candidate["indicative_entry_debit_usd"] == "89.00"
        assert child_candidate["indicative_maximum_loss_usd"] == "99.00"
        assert child_candidate["indicative_cost_after_ev_usd"] == "1.00"
        assert (
            child_candidate["legs"][0]["quote_evidence_hash"]
            != parent_candidate["legs"][0]["quote_evidence_hash"]
        )

    imported_child = import_research_top10(child, store_path=store)
    assert imported_child["status"] == "IMPORTED"
    assert read_research_top10(store_path=store)["phase"] == "INDICATIVE_REPRICE"


def test_reprice_of_unquoted_parent_uses_open_quotes_without_inventing_ev(
    tmp_path: Path,
) -> None:
    store = tmp_path / "research.sqlite3"
    premarket_run = _run_document()
    _make_quotes_unavailable(premarket_run)
    _set_expected_payoff(premarket_run, None)
    parent = assemble_managed_research(premarket_run)
    imported = import_research_top10(parent, store_path=store)

    child = assemble_managed_research(
        _run_document(phase="INDICATIVE_REPRICE", reprice=True),
        store_path=store,
    )

    assert child["parent_content_hash"] == imported["content_hash"]
    first = child["candidates"][0]
    assert first["indicative_entry_debit_usd"] == "89.00"
    assert first["indicative_maximum_loss_usd"] == "99.00"
    assert first["indicative_cost_after_ev_usd"] is None
    assert first["assumed_multiplier"] == 100
    assert "QUOTE_UNAVAILABLE" not in first["blockers"]
    assert "EXPECTED_PAYOFF_UNAVAILABLE" in first["blockers"]
    assert import_research_top10(child, store_path=store)["status"] == "IMPORTED"


def test_reprice_never_accepts_unavailable_quotes(tmp_path: Path) -> None:
    store = tmp_path / "research.sqlite3"
    parent = assemble_managed_research(_run_document())
    import_research_top10(parent, store_path=store)
    reprice = _run_document(phase="INDICATIVE_REPRICE", reprice=True)
    _make_quotes_unavailable(reprice)

    with pytest.raises(ManagedResearchInputError, match="INDICATIVE_REPRICE"):
        assemble_managed_research(reprice, store_path=store)


def test_reprice_rebuilds_candidate_blockers_from_current_legs(tmp_path: Path) -> None:
    store = tmp_path / "research.sqlite3"
    parent = assemble_managed_research(_run_document())
    parent["candidates"][0]["blockers"].extend(["QUOTE_STALE", "NEWS_CONFLICT"])
    import_research_top10(parent, store_path=store)

    child = assemble_managed_research(
        _run_document(phase="INDICATIVE_REPRICE", reprice=True),
        store_path=store,
    )

    assert "QUOTE_STALE" not in child["candidates"][0]["blockers"]
    assert "NEWS_CONFLICT" in child["candidates"][0]["blockers"]


def test_reprice_supports_verified_parent_without_changing_parent_identity(
    tmp_path: Path,
) -> None:
    store = tmp_path / "research.sqlite3"
    parent = assemble_managed_research(_run_document())
    for candidate in parent["candidates"]:
        candidate["entry_debit_usd"] = candidate["indicative_entry_debit_usd"]
        candidate["maximum_loss_usd"] = candidate["indicative_maximum_loss_usd"]
        candidate["cost_after_ev_usd"] = candidate["indicative_cost_after_ev_usd"]
        candidate["indicative_entry_debit_usd"] = None
        candidate["indicative_maximum_loss_usd"] = None
        candidate["indicative_cost_after_ev_usd"] = None
        candidate["assumed_multiplier"] = None
        candidate["risk_cap_verified"] = True
        candidate["blockers"] = []
    imported = import_research_top10(parent, store_path=store)

    child = assemble_managed_research(
        _run_document(phase="INDICATIVE_REPRICE", reprice=True),
        store_path=store,
    )

    assert child["parent_content_hash"] == imported["content_hash"]
    assert child["candidates"][0]["risk_cap_verified"] is True
    assert child["candidates"][0]["assumed_multiplier"] is None
    assert child["candidates"][0]["entry_debit_usd"] == "89.00"
    assert import_research_top10(child, store_path=store)["status"] == "IMPORTED"


def test_cli_atomically_writes_envelope_and_strictly_imports_clean_store(
    tmp_path: Path,
) -> None:
    source = tmp_path / "managed-run.json"
    output = tmp_path / "research-envelope.json"
    store = tmp_path / "research.sqlite3"
    source.write_text(json.dumps(_run_document()), encoding="utf-8")

    assert main(
        [
            "--input",
            str(source),
            "--store",
            str(store),
            "--envelope-out",
            str(output),
        ]
    ) == 0

    assert output.is_file()
    assert json.loads(output.read_text(encoding="utf-8"))["phase"] == "PREMARKET_RESEARCH"
    assert read_research_top10(store_path=store)["available_count"] == 10
    assert not list(tmp_path.glob(f".{output.name}.*.tmp"))


def test_cli_imports_unquoted_premarket_research_without_price_substitution(
    tmp_path: Path,
) -> None:
    source = tmp_path / "managed-unquoted-run.json"
    output = tmp_path / "research-unquoted-envelope.json"
    store = tmp_path / "research-unquoted.sqlite3"
    document = _run_document()
    _make_quotes_unavailable(document)
    _set_expected_payoff(document, None)
    source.write_text(json.dumps(document), encoding="utf-8")

    assert main(
        [
            "--input",
            str(source),
            "--store",
            str(store),
            "--envelope-out",
            str(output),
        ]
    ) == 0

    model = read_research_top10(store_path=store)
    assert model["status"] == "AVAILABLE"
    assert model["available_count"] == 10
    assert model["candidates"][0]["indicative_entry_debit_usd"] is None
    assert model["candidates"][0]["indicative_cost_after_ev_usd"] is None
    assert model["candidates"][0]["legs"][0]["bid"] is None
    assert "QUOTE_UNAVAILABLE" in model["reason_codes"]
    assert "EXPECTED_PAYOFF_UNAVAILABLE" in model["reason_codes"]


def test_unquoted_premarket_research_rejects_fabricated_expected_payoff() -> None:
    document = _run_document()
    _make_quotes_unavailable(document)

    with pytest.raises(
        ManagedResearchInputError,
        match="unquoted legs require null expected_payoff_usd",
    ):
        assemble_managed_research(document)


def test_quoted_premarket_research_requires_expected_payoff() -> None:
    document = _run_document()
    _set_expected_payoff(document, None)

    with pytest.raises(
        ManagedResearchInputError,
        match="quoted legs require numeric expected_payoff_usd",
    ):
        assemble_managed_research(document)


def test_quoted_candidate_rejects_zero_after_cost_ev() -> None:
    document = _run_document()
    _set_expected_payoff(document, "90.00")

    with pytest.raises(
        ManagedResearchInputError,
        match="indicative after-cost EV must be positive",
    ):
        assemble_managed_research(document)


def test_quoted_candidate_rejects_payoff_above_vertical_cap() -> None:
    document = _run_document()
    _set_expected_payoff(document, "100.01")

    with pytest.raises(
        ManagedResearchInputError,
        match="expected payoff exceeds vertical maximum payout",
    ):
        assemble_managed_research(document)


def test_quoted_candidate_accepts_one_cent_after_cost_ev() -> None:
    document = _run_document()
    _set_expected_payoff(document, "90.01")

    envelope = assemble_managed_research(document)

    assert envelope["candidates"][0]["indicative_cost_after_ev_usd"] == "0.01"


def test_expected_payoff_must_use_exact_cents() -> None:
    document = _run_document()
    _set_expected_payoff(document, "90.001")

    with pytest.raises(
        ManagedResearchInputError,
        match="expected_payoff_usd must use exact cents",
    ):
        assemble_managed_research(document)


def test_only_unquoted_candidate_requires_null_expected_payoff() -> None:
    document = _run_document()
    for quote in document["quotes"][:2]:
        quote["bid"] = None
        quote["ask"] = None
        quote["quote_asof"] = None
        quote["blockers"].extend(
            ["QUOTE_UNAVAILABLE", "QUOTE_EXCHANGE_TIMESTAMP_UNAVAILABLE"]
        )
    document["candidates"][0]["expected_payoff_usd"] = None
    _rehash_component(document, "quotes")
    _rehash_component(document, "candidates")

    envelope = assemble_managed_research(document)

    assert envelope["candidates"][0]["indicative_entry_debit_usd"] is None
    assert "EXPECTED_PAYOFF_UNAVAILABLE" in envelope["candidates"][0]["blockers"]
    assert envelope["candidates"][1]["indicative_cost_after_ev_usd"] == "10.00"


def test_subcent_leg_prices_bind_all_economics_to_one_rounded_debit() -> None:
    document = _run_document()
    document["quotes"][0]["ask"] = "1.20005"
    _rehash_component(document, "quotes")

    envelope = assemble_managed_research(document)
    first = envelope["candidates"][0]

    assert first["indicative_entry_debit_usd"] == "80.01"
    assert first["indicative_maximum_loss_usd"] == "90.01"
    assert first["indicative_cost_after_ev_usd"] == "9.99"


def test_cli_rejects_store_output_path_collision_before_import(tmp_path: Path) -> None:
    source = tmp_path / "managed-run.json"
    store = tmp_path / "research.sqlite3"
    source.write_text(json.dumps(_run_document()), encoding="utf-8")

    assert main(
        [
            "--input",
            str(source),
            "--store",
            str(store),
            "--envelope-out",
            str(tmp_path / "." / store.name),
        ]
    ) == 2
    assert not store.exists()


def test_atomic_output_rejects_existing_symlink(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    alias = tmp_path / "alias.json"
    original_is_symlink = Path.is_symlink
    monkeypatch.setattr(
        Path,
        "is_symlink",
        lambda path: path == alias or original_is_symlink(path),
    )

    with pytest.raises(ManagedResearchInputError, match="symlink"):
        atomic_write_research_envelope(alias, assemble_managed_research(_run_document()))
    assert not alias.exists()


def test_cli_reports_export_failure_after_successful_import(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    source = tmp_path / "managed-run.json"
    output = tmp_path / "research-envelope.json"
    store = tmp_path / "research.sqlite3"
    source.write_text(json.dumps(_run_document()), encoding="utf-8")

    def fail_export(*_args: object, **_kwargs: object) -> None:
        raise OSError("simulated export failure")

    monkeypatch.setattr(
        producer_cli_module,
        "atomic_write_research_envelope",
        fail_export,
    )
    assert main(
        [
            "--input",
            str(source),
            "--store",
            str(store),
            "--envelope-out",
            str(output),
        ]
    ) == 3
    assert "MANAGED_RESEARCH_EXPORT_FAILED_AFTER_IMPORT" in capsys.readouterr().err
    assert read_research_top10(store_path=store)["available_count"] == 10


def test_reprice_requires_clean_store_parent() -> None:
    with pytest.raises(ManagedResearchInputError, match="parent"):
        assemble_managed_research(
            _run_document(phase="INDICATIVE_REPRICE", reprice=True),
            store_path=Path("missing-research-store.sqlite3"),
        )
