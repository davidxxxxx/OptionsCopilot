import json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from options_copilot.analytics.scenarios import InitialPolicyResolver, ScenarioAction, ScenarioEngine
from options_copilot.analytics.volatility import EvidenceRole, VolatilityEngine
from options_copilot.governance.contracts import ContractValidationError


NOW = datetime(2026, 8, 4, tzinfo=timezone.utc)


def test_volatility_requires_fresh_executable_ibkr_surface_and_keeps_supporting_role() -> None:
    evidence = VolatilityEngine().evaluate(
        {
            "source": "IBKR",
            "observed_at": NOW,
            "secdef_hash": "a" * 64,
            "quote_hash": "b" * 64,
            "quotes": (
                {"bid": Decimal("1"), "ask": Decimal("1.1"), "iv": Decimal("0.2"), "volume": 20, "open_interest": 200},
            ),
            "iv_history": (Decimal("0.15"), Decimal("0.2"), Decimal("0.25")),
        },
        now=NOW,
    )
    assert evidence.eligible
    assert evidence.role is EvidenceRole.HARD
    assert VolatilityEngine.role_for("NEWS") is EvidenceRole.SUPPORTING_ONLY
    assert not VolatilityEngine().evaluate({"source": "NEWS"}, now=NOW).eligible


def test_volatility_uses_bound_evidence_hash_before_internal_domain_objects() -> None:
    evidence_hash = "c" * 64
    evidence = VolatilityEngine().evaluate(
        {
            "source": "IBKR",
            "observed_at": NOW,
            "secdef_hash": "a" * 64,
            "quote_hash": "b" * 64,
            "evidence_hash": evidence_hash,
            "contracts": (object(),),
            "quotes": (
                {
                    "bid": Decimal("1"),
                    "ask": Decimal("1.1"),
                    "iv": Decimal("0.2"),
                    "volume": 20,
                    "open_interest": 200,
                },
            ),
            "iv_history": (Decimal("0.15"), Decimal("0.2"), Decimal("0.25")),
        },
        now=NOW,
    )

    assert evidence.eligible
    assert evidence.input_hash == evidence_hash


def test_scenarios_fail_closed_without_all_hard_evidence_or_positive_after_cost_ev() -> None:
    engine = ScenarioEngine()
    baseline = {
        "spot": Decimal("100"), "atm_iv": Decimal("0.2"), "dte": 20,
        "market_score": Decimal("0"), "volatility_score": Decimal("0"),
        "max_loss": Decimal("100"), "after_cost_expected_value": Decimal("10"),
        "max_profit": Decimal("200"), "execution_cost_usd": Decimal("20"),
        "stress_after_cost_expected_value": Decimal("5"),
        "cost_hash": "d5922fa9808c4f1e7150362026037d17fb550d6f64daf8f86985042053e5428b", "cost_version": "v1",
        "hard_evidence": {"MARKET": {"eligible": True, "hash": "d" * 64}, "VOLATILITY": {"eligible": True, "hash": "e" * 64}, "LIQUIDITY": {"eligible": True, "hash": "f" * 64}},
    }
    decision = engine.evaluate(baseline, now=NOW)
    assert decision.action is ScenarioAction.TRADE
    assert sum(item.probability for item in decision.scenarios) == Decimal("1.000000")
    assert engine.evaluate({**baseline, "after_cost_expected_value": Decimal("0")}, now=NOW).action is ScenarioAction.NO_TRADE
    assert engine.evaluate({**baseline, "hard_evidence": {"NEWS": {"eligible": True}}}, now=NOW).action is ScenarioAction.NO_TRADE


def test_initial_policy_resolver_recomputes_contract_hash_and_rejects_tamper(tmp_path) -> None:
    resolver = InitialPolicyResolver()
    resolved = resolver.resolve(now=NOW)
    assert resolved.current_policy_hash == "b5d969d13fc624cdb2c49b3a78ce0fa34119bedd95f51db7b25ba9c977f55a3c"
    assert resolver.is_current(resolved)

    source = Path(__file__).resolve().parents[2] / "options_copilot" / "governance" / "initial_champion_scenario_policy.v1.json"
    document = json.loads(source.read_text(encoding="utf-8"))
    document["contract_hash"] = "0" * 64
    tampered = tmp_path / "tampered-policy.json"
    tampered.write_text(json.dumps(document), encoding="utf-8")

    tampered_resolver = InitialPolicyResolver(tampered)
    with pytest.raises(ContractValidationError, match="contract hash mismatch"):
        tampered_resolver.resolve(now=NOW)
    assert not tampered_resolver.is_current(resolved)
