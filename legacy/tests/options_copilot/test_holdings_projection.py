"""Full-inventory management projection and browser expiry without live trading."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta
from decimal import Decimal
import json
from pathlib import Path
import subprocess

import pytest
from fastapi.testclient import TestClient

from options_copilot.api import create_app
from options_copilot.api import holdings_projection
from options_copilot.api.app import _normalise_management
from tests.options_copilot.test_api_frontend import _services
from tests.options_copilot.test_position_management_runtime_adapter import (
    NOW, _Gateway, _coordinator, _position, _quote_batch,
)


ROOT = Path(__file__).resolve().parents[2]


class _HoldingsGateway(_Gateway):
    """Three-leg synthetic BBOs; the legacy QQQ fixture has only two prices."""

    def option_quote_batch(self, contracts):
        self.quote_requests.append(tuple(contracts))
        template = _quote_batch(tuple(contracts[:1]), observed_at=self.quote_observed_at)
        prices = {650: ("1.00", "1.20"), 680: ("4.00", "4.30"), 760: ("2.00", "2.10")}
        quotes = tuple(
            replace(
                template.quotes[0], contract_id=contract.contract_id,
                request_id=f"request-{contract.contract_id}",
                bid=Decimal(prices[int(contract.strike)][0]),
                ask=Decimal(prices[int(contract.strike)][1]),
            ) for contract in contracts
        )
        return replace(template, quotes=quotes)


def _result():
    positions = (
        _position(867925870, 650, -1, symbol="QQQ", right="P"),
        _position(867925978, 680, 1, symbol="QQQ", right="P"),
        _position(867924832, 760, 1, symbol="QQQ", right="C"),
    )
    coordinator, _, _ = _coordinator(_HoldingsGateway(positions=positions))
    result = coordinator.refresh()
    assert len(result.candidates) == 1, result.reason_codes
    assert result.status == "NO_TRADE"
    return result


def _project(*, now=NOW):
    return _normalise_management(_result().as_dict(), now=now)


def test_actual_coordinator_to_projection_has_explicit_non_strategy_scope():
    payload = _project()
    candidate = payload["candidates"][0]
    assert payload["decision"] == payload["status"] == "NO_TRADE"
    assert candidate["source_payload_valid"] is True
    assert candidate["quote_reference_status"] == "CURRENT_COMPONENT_REFERENCE"
    assert candidate["scope"] == "ALL_OBSERVED_OPTION_HOLDINGS"
    assert candidate["grouping_status"] == "NOT_INFERRED"
    assert candidate["review_state"] == "PREVIEW_UNVERIFIED"
    assert candidate["gross_component_liquidation_cashflow_usd"] is not None
    assert candidate["before_payoff"]["valuation_basis"] == "GROSS_COMPONENT_NATURAL_LIQUIDATION"
    assert candidate["before_payoff"]["future_exit_cost_included_in_max_loss"] is True
    assert candidate["before_payoff"]["historical_entry_cost_included"] is False
    assert candidate["before_payoff"]["reopening_premium_included"] is False
    assert candidate["all_in_close_cashflow_usd"] is not None
    assert candidate["after_risk_condition"] == "ALL_LEGS_FILLED_AND_RECONCILED"
    assert candidate["projection_is_source_payload"] is False
    assert {leg["contract_id"] for leg in candidate["execution_legs"]} == {867925870, 867925978, 867924832}
    for field in ("combo_quantity_before", "unit_ratio", "exit_plan", "structure", "entry_net_cost_usd"):
        assert field not in candidate
    for field in ("approval_enabled", "instruction_enabled", "order_allowed", "instruction_creation_allowed", "affects_eligibility", "direct_order_submission"):
        assert candidate[field] is False
    assert payload["action_enabled"] is False


@pytest.mark.parametrize("age", [timedelta(seconds=5, microseconds=1), timedelta(days=1)])
def test_old_source_projection_cannot_restamp_current_financial_values(age):
    raw = _result().as_dict()
    payload = _normalise_management(raw, now=NOW + age)
    candidate = payload["candidates"][0]
    assert candidate["source_payload_valid"] is True
    assert candidate["quote_reference_status"] == "STALE_OR_UNAVAILABLE"
    assert candidate["generated_at"] == raw["candidates"][0]["generated_at"]
    for field in ("gross_component_liquidation_cashflow_usd", "all_in_close_cashflow_usd", "estimated_execution_cost_usd"):
        assert candidate[field] is None
    assert candidate["before_payoff"] == candidate["before_risk"] == candidate["after_risk"] == {}
    for leg in candidate["execution_legs"]:
        assert leg["bid"] is leg["ask"] is leg["executable_price"] is None
    assert "transition_proof" not in candidate


def test_source_reference_is_current_at_exact_five_seconds_only():
    raw = _result().as_dict()
    oldest = datetime.fromisoformat(raw["candidates"][0]["oldest_quote_at"])
    for age, expected in (
        (timedelta(seconds=5), "CURRENT_COMPONENT_REFERENCE"),
        (timedelta(seconds=5, microseconds=1), "STALE_OR_UNAVAILABLE"),
        (timedelta(microseconds=-1), "STALE_OR_UNAVAILABLE"),
    ):
        candidate = _normalise_management(raw, now=oldest + age)["candidates"][0]
        assert candidate["quote_reference_status"] == expected
        assert (candidate["all_in_close_cashflow_usd"] is not None) == (
            expected == "CURRENT_COMPONENT_REFERENCE"
        )


def test_real_http_management_route_expires_cached_source_without_acquisition(monkeypatch):
    raw = _result().as_dict()
    source_reads = []
    oldest = datetime.fromisoformat(raw["candidates"][0]["oldest_quote_at"])
    clock = [oldest + timedelta(seconds=5)]

    class _Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return clock[0].astimezone(tz) if tz is not None else clock[0].replace(tzinfo=None)

    def cached_source():
        source_reads.append(1)
        return raw

    monkeypatch.setattr(holdings_projection, "datetime", _Clock)
    app = create_app(_services(management_provider=cached_source))
    with TestClient(app) as client:
        current_response = client.get("/api/management/current")
        clock[0] += timedelta(microseconds=1)
        stale_response = client.get("/api/management/current")
    assert current_response.status_code == stale_response.status_code == 200
    current, stale = current_response.json(), stale_response.json()
    assert len(source_reads) == 2
    assert current["status"] == stale["status"] == "NO_TRADE"
    assert current["action_enabled"] is stale["action_enabled"] is False
    before, after = current["candidates"][0], stale["candidates"][0]
    assert before["quote_reference_status"] == "CURRENT_COMPONENT_REFERENCE"
    assert before["all_in_close_cashflow_usd"] is not None
    assert after["quote_reference_status"] == "STALE_OR_UNAVAILABLE"
    assert after["all_in_close_cashflow_usd"] is None
    assert after["before_payoff"] == after["before_risk"] == after["after_risk"] == {}
    assert before["candidate_hash"] == after["candidate_hash"]
    assert before["generated_at"] == after["generated_at"]
    assert before["oldest_quote_at"] == after["oldest_quote_at"]
    assert "transition_proof" not in after
    assert all(leg["bid"] is leg["ask"] is leg["executable_price"] is None for leg in after["execution_legs"])


@pytest.mark.parametrize("field,value", [
    ("gross_component_liquidation_cashflow_usd", "999999"),
    ("approval_enabled", True),
    ("grouping_status", "HUMAN_APPROVED"),
    ("oldest_quote_at", "2026-08-04T17:00:01Z"),
])
def test_tampered_source_is_nonfinancial_and_never_authority(field, value):
    raw = deepcopy(_result().as_dict())
    raw["candidates"][0][field] = value
    candidate = _normalise_management(raw, now=NOW)["candidates"][0]
    assert candidate["source_payload_valid"] is False
    assert candidate["all_in_close_cashflow_usd"] is None
    assert candidate["review_state"] == "PREVIEW_UNVERIFIED"
    assert candidate["approval_enabled"] is False
    assert candidate["grouping_status"] == "NOT_INFERRED"


def _node(source):
    script_uri = (ROOT / "options_copilot/frontend/app.js").as_uri()
    completed = subprocess.run(
        ["node", "--input-type=module", "--eval", source.replace("SCRIPT_URI", script_uri)],
        cwd=ROOT, check=True, capture_output=True, text=True, encoding="utf-8",
    )
    return json.loads(completed.stdout)


def test_browser_source_clock_gate_and_real_dom_card_expire_without_a_fetch():
    candidate = _project()["candidates"][0]
    source = r'''
class Element {
  constructor(tag) { this.tagName=tag; this.children=[]; this.dataset={}; this.textContent=""; this.className=""; }
  append(...nodes) { this.children.push(...nodes); }
  querySelectorAll(selector) {
    const key = {"[data-holdings-current-value]":"holdingsCurrentValue", "[data-holdings-freshness]":"holdingsFreshness"}[selector];
    return this.children.flatMap(child => typeof child === "object" ? [...(key in child.dataset ? [child] : []), ...child.querySelectorAll(selector)] : []);
  }
}
let card;
globalThis.document={addEventListener(){}, createElement:tag=>new Element(tag), querySelectorAll:()=>card ? [card] : []};
const {holdingsCloseView,buildHoldingsClosePreview,expireHoldingsClosePreviews}=await import("SCRIPT_URI");
const candidate=CANDIDATE;
const now=Date.parse(candidate.generated_at);
const before=holdingsCloseView(candidate,now);
card=buildHoldingsClosePreview(candidate,now);
function texts(node){return [node.textContent,...node.children.filter(child=>typeof child==="object").flatMap(texts)].join(" ");}
const rendered=texts(card);
function actionCount(node){return Number(["button","a","input"].includes(node.tagName))+node.children.filter(child=>typeof child==="object").reduce((sum,child)=>sum+actionCount(child),0);}
const controls=actionCount(card);
const exactBoundary=holdingsCloseView(candidate,Date.parse(candidate.oldest_quote_at)+5000);
const pastBoundary=holdingsCloseView(candidate,Date.parse(candidate.oldest_quote_at)+5001);
expireHoldingsClosePreviews(Date.parse(candidate.oldest_quote_at)+5001);
const values=card.querySelectorAll("[data-holdings-current-value]").map(node=>node.textContent);
const freshness=card.querySelectorAll("[data-holdings-freshness]").map(node=>node.textContent);
card=buildHoldingsClosePreview(candidate,now);
expireHoldingsClosePreviews(now-1);
const reversedClockValues=card.querySelectorAll("[data-holdings-current-value]").map(node=>node.textContent);
console.log(JSON.stringify({before,rendered,controls,exactBoundary,pastBoundary,values,freshness,reversedClockValues,expired:holdingsCloseView(candidate,now+6000),future:holdingsCloseView(candidate,now-1000)}));
'''.replace("CANDIDATE", json.dumps(candidate))
    result = _node(source)
    assert result["before"]["current"] is True
    assert result["exactBoundary"]["current"] is True
    assert result["pastBoundary"]["current"] is False
    assert result["expired"]["current"] is False
    assert result["expired"]["gross"] is None
    assert result["future"]["current"] is False
    assert result["controls"] == 0
    assert all(value == "--" for value in result["values"])
    assert all(value == "--" for value in result["reversedClockValues"])
    assert "数值已停用" in result["freshness"][0]
    for text in ("全部已观察持仓", "策略归属未推断", "NO_TRADE", "不保证能够原子成交", "所有腿均成交", "不是券商保证金", "不是退出建议"):
        assert text in result["rendered"]
    for absent in ("HOLD_MONITOR", "止盈复核线", "60%", "第二个组合"):
        assert absent not in result["rendered"]
