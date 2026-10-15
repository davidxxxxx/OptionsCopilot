from __future__ import annotations

import json
from pathlib import Path
import subprocess


ROOT = Path(__file__).resolve().parents[2]
FRONTEND = ROOT / "options_copilot" / "frontend"


def _run_module(source: str) -> object:
    result = subprocess.run(
        ["node", "--input-type=module", "--eval", source],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    return json.loads(result.stdout)


def test_research_v2_has_a_separate_supporting_only_top10_surface() -> None:
    html = (FRONTEND / "index.html").read_text(encoding="utf-8")
    script = (FRONTEND / "app.js").read_text(encoding="utf-8")
    styles = (FRONTEND / "styles.css").read_text(encoding="utf-8")
    news_page = html.split('<main id="news-page"', 1)[1].split("</main>", 1)[0]
    panel = news_page.split('<section id="research-top10-panel"', 1)[1].split(
        "</section>", 1
    )[0]

    for identifier in (
        "research-top10-panel",
        "research-top10-status",
        "research-top10-count",
        "research-top10-asof",
        "research-top10-stage-summary",
        "research-top10-list",
    ):
        assert f'id="{identifier}"' in html
        assert identifier in script

    for text in (
        "条件式期权研究 Top-10",
        "SUPPORTING_ONLY",
        "不可审批",
        "不可创建指令",
        "NO_TRADE",
        "09:35 指示性重报价",
    ):
        assert text in panel

    assert 'data-research-top10-stage="pre-market"' in panel
    assert 'data-research-top10-stage="open-repriced"' in panel
    assert 'researchTop10: "/api/research-top10"' in script
    assert "researchTop10Fallback" not in script
    assert ".research-top10-panel" in styles
    assert ".research-top10-card" in styles
    assert ".research-top10-availability" in styles
    assert 'data-action="approve"' not in panel
    assert "ibkr-review-link" not in panel
    assert "/api/approval" not in panel
    assert "/api/orders" not in panel


def test_overview_exposes_research_rows_only_when_actionable_pool_is_empty() -> None:
    html = (FRONTEND / "index.html").read_text(encoding="utf-8")
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    overview = html.split('<main id="overview-page"', 1)[1].split("</main>", 1)[0]
    source = f'''globalThis.document = {{ addEventListener() {{}} }};
const {{ overviewResearchRows }} = await import("{script_uri}");
const research = {{ premarket: Array.from({{ length: 9 }}, (_, index) => ({{ rank: index + 1 }})), open_repriced: [] }};
console.log(JSON.stringify({{
  emptyActionPool: overviewResearchRows({{ candidates: [] }}, research).length,
  viewOnlyNoTrade: overviewResearchRows({{
    decision: "NO_TRADE", recommendations_available: false, approval_enabled: false,
    candidates: [{{ rank: 1, interaction: "VIEW_ONLY", recommendation_ready: false }}],
  }}, research).length,
  activeActionPool: overviewResearchRows({{
    decision: "CANDIDATES_AVAILABLE", recommendations_available: true, approval_enabled: true,
    candidates: [{{ rank: 1, interaction: "CHALLENGE_ALLOWED", recommendation_ready: true }}],
  }}, research).length,
  creatorBlockedReviewPool: overviewResearchRows({{
    decision: "CANDIDATES_AVAILABLE", recommendations_available: true, approval_enabled: false,
    approval_blockers: ["CREATOR_TRANSPORT_UNAVAILABLE"],
    candidates: [{{ rank: 1, interaction: "VIEW_ONLY", recommendation_ready: true }}],
  }}, research).length,
}}));'''

    payload = _run_module(source)

    assert payload == {
        "emptyActionPool": 9,
        "viewOnlyNoTrade": 9,
        "activeActionPool": 0,
        "creatorBlockedReviewPool": 0,
    }
    for identifier in (
        "overview-research-fallback",
        "overview-research-count",
        "overview-research-list",
    ):
        assert f'id="{identifier}"' in overview
    assert "研究候选 · 当前不可下单" in overview
    assert "SUPPORTING_ONLY" in overview
    assert "QUOTE REQUIRED" in overview
    assert 'data-action="approve"' not in overview.split(
        '<section id="overview-research-fallback"', 1
    )[1].split("</section>", 1)[0]


def test_overview_research_availability_keeps_regular_and_after_hours_counts_separate() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''globalThis.document = {{ addEventListener() {{}} }};
const {{ overviewResearchAvailability }} = await import("{script_uri}");
console.log(JSON.stringify(overviewResearchAvailability(
  {{
    asof: "2026-08-24T13:20:00Z",
    premarket: [{{ rank: 1 }}, {{ rank: 2 }}, {{ rank: 3 }}],
    open_repriced: [],
  }},
  {{
    observed_at: "2026-08-24T20:40:00Z",
    candidates: Array.from({{ length: 10 }}, (_, index) => ({{ rank: index + 1 }})),
  }},
)));'''

    payload = _run_module(source)

    assert payload == {
        "regularCount": 3,
        "afterHoursCount": 10,
        "regularAsOf": "2026-08-24T13:20:00Z",
        "afterHoursAsOf": "2026-08-24T20:40:00Z",
    }


def test_daily_funnel_distinguishes_completion_from_failed_or_missed() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''globalThis.document = {{ addEventListener() {{}} }};
const {{ dailyFunnelTruth, dailyRunPresentation }} = await import("{script_uri}");
console.log(JSON.stringify({{
  allComplete: dailyFunnelTruth([{{ status: "COMPLETED" }}], "a".repeat(64)),
  idleDurable: dailyFunnelTruth([], "c".repeat(64)),
  emptyUnavailable: dailyFunnelTruth([], ""),
  mixed: dailyFunnelTruth([
    {{ status: "COMPLETED" }},
    {{ status: "FAILED", reason_codes: ["LEASE_EXPIRED"] }},
    {{ status: "MISSED_NOT_REPLAYED", recovery_policy: "EXACT_ONLY_NO_REPLAY" }},
  ], "b".repeat(64)),
  failedRow: dailyRunPresentation({{ status: "FAILED", reason_codes: ["LEASE_EXPIRED"] }}),
}}));'''

    payload = _run_module(source)

    assert payload["allComplete"]["stateClass"] == "status-up"
    assert payload["idleDurable"] == {
        "terminalCount": 0,
        "failureCount": 0,
        "stateClass": "status-stale",
        "summary": "0 OPERATIONS DUE · DURABLE",
        "emptyMessage": "本日 durable manifest 已记录；当前没有到期的 daily operation。",
    }
    assert payload["emptyUnavailable"] == {
        "terminalCount": 0,
        "failureCount": 0,
        "stateClass": "status-unknown",
        "summary": "UNAVAILABLE",
        "emptyMessage": "当前 runtime 未提供 durable daily operation manifest。",
    }
    assert payload["mixed"]["stateClass"] == "status-down"
    assert payload["mixed"]["failureCount"] == 2
    assert payload["failedRow"] == {
        "status": "FAILED",
        "stateClass": "status-down",
        "label": "FAILED · LEASE_EXPIRED",
    }


def test_research_top10_normalization_keeps_stages_separate_and_fails_closed() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''globalThis.document = {{ addEventListener() {{}} }};
const {{ normalizeResearchTop10 }} = await import("{script_uri}");
const authority = {{
  decision_authority: "SUPPORTING_ONLY",
  approval_eligible: false,
  instruction_creation_allowed: false,
  order_creation_allowed: false,
}};
const leg = {{ side: "BUY", ratio: 1, expiry: "2026-08-21", strike: "225", right: "CALL" }};
const leg2 = {{ ...leg, side: "SELL", strike: "230" }};
const pre = {{
  ...authority, preselection_id: "pre-1", phase: "PRE_MARKET", research_rank: 1,
  underlying: "AAPL", direction: "BULLISH", strategy_type: "CALL_DEBIT_SPREAD",
  expiry: "2026-08-21", legs: [leg, leg2], indicative_debit_usd: "185",
  maximum_loss_usd: "185", news_catalyst: "Earnings after close",
  oldest_quote_asof: "2026-08-06T13:20:01Z", blockers: ["PREMARKET_QUOTE_INDICATIVE_ONLY"],
}};
const open = {{
  ...authority, preselection_id: "pre-1", phase: "OPEN_REPRICED", repriced_rank: 2,
  underlying: "AAPL", direction: "BULLISH", strategy_type: "CALL_DEBIT_SPREAD",
  expiry: "2026-08-21", legs: [leg, leg2], debit_usd: "210", maximum_loss_usd: "210",
  catalyst: "Earnings after close", economics_quote_asof: "2026-08-06T13:35:02Z", blockers: [],
}};
const normalized = normalizeResearchTop10({{
  ...authority, decision: "NO_TRADE", asof: "2026-08-06T13:35:03Z",
  premarket: [pre], open_repriced: [open],
}});
const unsafe = normalizeResearchTop10({{
  ...authority, premarket: [{{ ...pre, approval_eligible: true }}], open_repriced: [open],
}});
const primary = normalizeResearchTop10({{
  phase: "INDICATIVE_REPRICE", observed_at: "2026-08-06T13:35:04Z",
  available_count: 1, target_count: 10,
  decision_authority: "SUPPORTING_ONLY", approval_eligible: false,
  instruction_creation_allowed: false, order_allowed: false, action_pool_eligible: false,
  stages: [
    {{ phase: "PREMARKET_RESEARCH", observed_at: "2026-08-06T13:20:04Z" }},
    {{ phase: "INDICATIVE_REPRICE", observed_at: "2026-08-06T13:35:04Z" }},
  ],
  premarket: [{{
    research_id: "research-1", rank: 1, underlying: "MSFT", strategy_type: "CALL_DEBIT_SPREAD",
    expiration: "2026-08-21", dte: 15, entry_debit_usd: "195", maximum_loss_usd: null,
    research_summary: "Cloud catalyst", evidence_ids: ["news-1"],
    decision_authority: "SUPPORTING_ONLY", approval_eligible: false,
    instruction_creation_allowed: false, order_allowed: false, action_pool_eligible: false,
    blockers: ["MAXIMUM_LOSS_UNAVAILABLE", "QUOTE_EXCHANGE_TIMESTAMP_UNAVAILABLE"],
    legs: [{{
      side: "BUY", right: "CALL", strike: "420", expiration: "2026-08-21",
      bid: "2.10", ask: "2.20", quote_asof: null, collected_at: "2026-08-06T13:20:03Z",
      implied_volatility: "0.31", volume: 120, open_interest: 900, multiplier: null,
      blockers: ["QUOTE_EXCHANGE_TIMESTAMP_UNAVAILABLE"],
    }}, {{
      side: "SELL", right: "CALL", strike: "425", expiration: "2026-08-21",
      bid: "1.10", ask: "1.20", quote_asof: null, collected_at: "2026-08-06T13:20:03Z",
      implied_volatility: "0.29", volume: 110, open_interest: 850, multiplier: null,
      blockers: ["QUOTE_EXCHANGE_TIMESTAMP_UNAVAILABLE"],
    }}],
  }}],
  open_repriced: [{{
    research_id: "research-1", rank: 1, underlying: "MSFT", strategy_type: "CALL_DEBIT_SPREAD",
    expiration: "2026-08-21", dte: 15, entry_debit_usd: "205", maximum_loss_usd: null,
    research_summary: "Cloud catalyst", evidence_ids: ["news-1"],
    decision_authority: "SUPPORTING_ONLY", approval_eligible: false,
    instruction_creation_allowed: false, order_allowed: false, action_pool_eligible: false,
    blockers: ["MAXIMUM_LOSS_UNAVAILABLE", "QUOTE_EXCHANGE_TIMESTAMP_UNAVAILABLE"],
    legs: [{{
      side: "BUY", right: "CALL", strike: "420", expiration: "2026-08-21",
      bid: "2.20", ask: "2.30", quote_asof: null, collected_at: "2026-08-06T13:35:03Z",
      implied_volatility: "0.32", volume: 140, open_interest: 920, multiplier: null,
      blockers: ["QUOTE_EXCHANGE_TIMESTAMP_UNAVAILABLE"],
    }}, {{
      side: "SELL", right: "CALL", strike: "425", expiration: "2026-08-21",
      bid: "1.20", ask: "1.30", quote_asof: null, collected_at: "2026-08-06T13:35:03Z",
      implied_volatility: "0.30", volume: 130, open_interest: 880, multiplier: null,
      blockers: ["QUOTE_EXCHANGE_TIMESTAMP_UNAVAILABLE"],
    }}],
  }}],
}});
console.log(JSON.stringify({{ normalized, unsafe, primary }}));'''

    payload = _run_module(source)
    normalized = payload["normalized"]
    assert normalized["decision"] == "NO_TRADE"
    assert normalized["authority"] == "SUPPORTING_ONLY"
    premarket = normalized["premarket"][0]
    assert premarket["id"] == "pre-1"
    assert premarket["rank"] == 1
    assert premarket["symbol"] == "AAPL"
    assert premarket["direction"] == "BULLISH"
    assert premarket["strategy"] == "CALL_DEBIT_SPREAD"
    assert premarket["expiry"] == "2026-08-21"
    assert premarket["legs"][0]["side"] == "BUY"
    assert premarket["legs"][0]["ratio"] == 1
    assert premarket["indicative_debit_usd"] == 185
    assert premarket["maximum_loss_usd"] == 185
    assert premarket["news_catalyst"] == "Earnings after close"
    assert premarket["quote_asof"] == "2026-08-06T13:20:01Z"
    assert premarket["blockers"] == ["PREMARKET_QUOTE_INDICATIVE_ONLY"]
    assert premarket["phase"] == "PRE_MARKET"
    assert normalized["open_repriced"][0]["rank"] == 2
    assert normalized["open_repriced"][0]["indicative_debit_usd"] == 210
    assert normalized["open_repriced"][0]["quote_asof"] == "2026-08-06T13:35:02Z"
    assert payload["unsafe"]["status"] == "UNAVAILABLE"
    assert payload["unsafe"]["premarket"] == []
    assert payload["unsafe"]["open_repriced"] == []
    primary = payload["primary"]["premarket"][0]
    assert primary["id"] == "research-1"
    assert primary["symbol"] == "MSFT"
    assert primary["indicative_debit_usd"] == 195
    assert primary["maximum_loss_usd"] is None
    assert primary["quote_asof"] is None
    assert primary["collected_at"] == "2026-08-06T13:20:03Z"
    assert primary["batch_observed_at"] == "2026-08-06T13:20:04Z"
    assert primary["time_label"] == "逐腿采集完成时间"
    assert "QUOTE_EXCHANGE_TIMESTAMP_UNAVAILABLE" in primary["blockers"]
    assert primary["legs"][0]["multiplier"] is None
    assert primary["legs"][0]["quote_asof"] is None
    open_primary = payload["primary"]["open_repriced"][0]
    assert open_primary["collected_at"] == "2026-08-06T13:35:03Z"
    assert open_primary["batch_observed_at"] == "2026-08-06T13:35:04Z"


def test_primary_research_contract_uses_indicative_debit_without_inventing_ratio() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''globalThis.document = {{ addEventListener() {{}} }};
const {{ normalizeResearchTop10 }} = await import("{script_uri}");
const normalized = normalizeResearchTop10({{
  phase: "PREMARKET_RESEARCH", observed_at: "2026-08-06T13:18:00Z",
  decision_authority: "SUPPORTING_ONLY", approval_eligible: false,
  instruction_creation_allowed: false, order_allowed: false,
  action_pool_eligible: false, candidates: [{{
    research_id: "primary-1", rank: 1, underlying: "SPY",
    strategy_type: "BEAR_PUT_VERTICAL", expiration: "2026-08-21",
    dte: 15, indicative_entry_debit_usd: "50.00", maximum_loss_usd: null,
    indicative_maximum_loss_usd: "60.00", indicative_cost_after_ev_usd: "12.00",
    assumed_multiplier: 100, entry_condition: "Debit <= $50",
    invalidation_condition: "Macro catalyst fades", profit_target_condition: "Take at $80",
    stop_loss_condition: "Exit at $30",
    research_summary: "Macro hedge research", blockers: ["MAXIMUM_LOSS_UNVERIFIED"],
    decision_authority: "SUPPORTING_ONLY", approval_eligible: false,
    instruction_creation_allowed: false, order_allowed: false,
    action_pool_eligible: false, legs: [{{ side: "BUY", expiration: "2026-08-21",
      strike: "770", right: "P", multiplier: null, bid: "4.00", ask: "4.50",
      collected_at: "2026-08-06T13:18:00Z",
      blockers: ["MULTIPLIER_UNAVAILABLE", "QUOTE_EXCHANGE_TIMESTAMP_UNAVAILABLE"] }},
      {{ side: "SELL", expiration: "2026-08-21", strike: "765", right: "P",
      multiplier: null, bid: "3.20", ask: "3.60", collected_at: "2026-08-06T13:18:00Z",
      blockers: ["MULTIPLIER_UNAVAILABLE", "QUOTE_EXCHANGE_TIMESTAMP_UNAVAILABLE"] }}]
  }}]
}});
console.log(JSON.stringify(normalized));'''

    normalized = _run_module(source)
    row = normalized["premarket"][0]
    assert row["indicative_debit_usd"] == 50
    assert row["maximum_loss_usd"] is None
    assert row["indicative_maximum_loss_usd"] == 60
    assert row["indicative_after_cost_ev_usd"] == 12
    assert row["assumed_multiplier"] == 100
    assert row["dte"] == 15
    assert row["direction"] == "BEARISH"
    assert row["stop_loss_condition"] == "Exit at $30"
    assert row["legs"][0]["ratio"] is None
    assert "MAXIMUM_LOSS_UNAVAILABLE" in row["blockers"]


def test_primary_premarket_shows_unquoted_state_and_open_stage_fails_closed() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''globalThis.document = {{ addEventListener() {{}} }};
const {{ normalizeResearchTop10 }} = await import("{script_uri}");
const authority = {{
  decision_authority: "SUPPORTING_ONLY", approval_eligible: false,
  instruction_creation_allowed: false, order_allowed: false, action_pool_eligible: false,
}};
const candidate = {{
  ...authority, research_id: "unquoted-1", rank: 1, underlying: "SPY",
  strategy_type: "BEAR_PUT_VERTICAL", expiration: "2026-08-21", dte: 15,
  entry_debit_usd: null, indicative_entry_debit_usd: null,
  maximum_loss_usd: null, indicative_maximum_loss_usd: null,
  indicative_cost_after_ev_usd: null, assumed_multiplier: 100,
  research_summary: "Macro research", blockers: ["QUOTE_UNAVAILABLE", "EXPECTED_PAYOFF_UNAVAILABLE"],
  legs: [{{ side: "BUY", expiration: "2026-08-21", strike: "770", right: "P",
    bid: null, ask: null, collected_at: "2026-08-06T13:20:00Z",
    blockers: ["QUOTE_UNAVAILABLE", "QUOTE_EXCHANGE_TIMESTAMP_UNAVAILABLE"] }},
    {{ side: "SELL", expiration: "2026-08-21", strike: "765", right: "P",
    bid: null, ask: null, collected_at: "2026-08-06T13:20:00Z",
    blockers: ["QUOTE_UNAVAILABLE", "QUOTE_EXCHANGE_TIMESTAMP_UNAVAILABLE"] }}],
}};
const premarket = normalizeResearchTop10({{
  ...authority, phase: "PREMARKET_RESEARCH", observed_at: "2026-08-06T13:20:01Z",
  candidates: [candidate],
}});
const open = normalizeResearchTop10({{
  ...authority, phase: "INDICATIVE_REPRICE", observed_at: "2026-08-06T13:35:01Z",
  candidates: [candidate],
}});
console.log(JSON.stringify({{ premarket, open }}));'''

    payload = _run_module(source)
    row = payload["premarket"]["premarket"][0]
    assert row["indicative_debit_usd"] is None
    assert row["legs"][0]["bid"] is None
    assert row["legs"][0]["ask"] is None
    assert row["quote_status"] == "UNAVAILABLE"
    assert row["ev_status"] == "UNAVAILABLE"
    assert "QUOTE_UNAVAILABLE" in row["blockers"]
    assert payload["open"]["status"] == "UNAVAILABLE"
    assert payload["open"]["premarket"] == []
    assert payload["open"]["open_repriced"] == []

    script = (FRONTEND / "app.js").read_text(encoding="utf-8")
    assert "盘前期权报价不可用，等待 09:35" in script
    assert "EXPECTED_PAYOFF_UNAVAILABLE" in script


def test_intraday_recovery_keeps_exact_contract_structures_visible_but_unquoted() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''globalThis.document = {{ addEventListener() {{}} }};
const {{ normalizeResearchTop10 }} = await import("{script_uri}");
const authority = {{
  decision_authority: "SUPPORTING_ONLY", approval_eligible: false,
  instruction_creation_allowed: false, order_allowed: false, action_pool_eligible: false,
}};
const candidate = {{
  ...authority, research_id: "recovery-1", rank: 1, underlying: "SPY",
  strategy_type: "BULL_CALL_VERTICAL", expiration: "2026-08-28", dte: 18,
  entry_debit_usd: null, indicative_entry_debit_usd: null,
  maximum_loss_usd: null, indicative_maximum_loss_usd: null,
  indicative_cost_after_ev_usd: null, assumed_multiplier: 100,
  research_summary: "Intraday exact-identity recovery", blockers: [
    "QUOTE_UNAVAILABLE", "EXPECTED_PAYOFF_UNAVAILABLE", "RISK_CAP_UNVERIFIED",
  ],
  legs: [{{ side: "BUY", expiration: "2026-08-28", strike: "640", right: "C",
    con_id: 101, local_symbol: "SPY   260828C00640000", trading_class: "SPY",
    exchange: "SMART", multiplier: 100, bid: null, ask: null,
    collected_at: "2026-08-10T14:40:00Z",
    blockers: ["QUOTE_UNAVAILABLE", "QUOTE_EXCHANGE_TIMESTAMP_UNAVAILABLE"] }},
    {{ side: "SELL", expiration: "2026-08-28", strike: "645", right: "C",
    con_id: 102, local_symbol: "SPY   260828C00645000", trading_class: "SPY",
    exchange: "SMART", multiplier: 100, bid: null, ask: null,
    collected_at: "2026-08-10T14:40:00Z",
    blockers: ["QUOTE_UNAVAILABLE", "QUOTE_EXCHANGE_TIMESTAMP_UNAVAILABLE"] }}],
}};
const normalized = normalizeResearchTop10({{
  ...authority, phase: "INTRADAY_RECOVERY", observed_at: "2026-08-10T14:40:01Z",
  trading_date: "2026-08-10",
  stages: [{{ phase: "INTRADAY_RECOVERY", trading_date: "2026-08-10", observed_at: "2026-08-10T14:40:00Z" }}],
  available_count: 1, target_count: 10, candidates: [candidate], open_repriced: [candidate],
}});
console.log(JSON.stringify(normalized));'''

    normalized = _run_module(source)
    assert normalized["phase"] == "INTRADAY_RECOVERY"
    assert normalized["trading_date"] == "2026-08-10"
    assert normalized["premarket"] == []
    assert len(normalized["open_repriced"]) == 1
    row = normalized["open_repriced"][0]
    assert row["symbol"] == "SPY"
    assert row["trading_date"] == "2026-08-10"
    assert row["batch_observed_at"] == "2026-08-10T14:40:00Z"
    assert row["intraday_recovery"] is True
    assert row["quote_status"] == "UNAVAILABLE"
    assert row["legs"][0]["con_id"] == 101
    assert row["legs"][0]["local_symbol"] == "SPY   260828C00640000"
    assert "QUOTE_UNAVAILABLE" in row["blockers"]

    script = (FRONTEND / "app.js").read_text(encoding="utf-8")
    assert "盘中恢复结构" in script
    assert "盘中恢复仅确认真实合约身份" in script


def test_research_freshness_uses_new_york_calendar_days_and_preserves_batch_dte() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''globalThis.document = {{ addEventListener() {{}} }};
const {{ researchFreshness }} = await import("{script_uri}");
const historical = {{ trading_date: "2026-08-19", expiry: "2026-09-04", dte: 16 }};
const original = JSON.stringify(historical);
const expiryDay = {{ trading_date: "2026-09-08", expiry: "2026-09-08", dte: 0 }};
console.log(JSON.stringify({{
  historical: researchFreshness(historical, new Date("2026-09-09T00:30:00+08:00")),
  beforeNewYorkMidnight: researchFreshness(expiryDay, new Date("2026-09-09T03:59:59Z")),
  afterNewYorkMidnight: researchFreshness(expiryDay, new Date("2026-09-09T04:00:00Z")),
  fallDst: researchFreshness({{ expiry: "2026-11-15" }}, new Date("2026-11-01T13:00:00Z")),
  missing: researchFreshness({{}}, new Date("2026-09-08T14:00:00Z")),
  invalidExpiry: researchFreshness({{ expiry: "2026-02-30" }}, new Date("2026-09-08T14:00:00Z")),
  observedFallback: researchFreshness({{ observed_at: "2026-09-08T01:00:00Z" }}, new Date("2026-09-08T14:00:00Z")),
  unchanged: original === JSON.stringify(historical),
}}));'''

    result = _run_module(source)
    assert result["historical"] == {
        "today": "2026-09-08",
        "batch_date": "2026-08-19",
        "historical": True,
        "future_dated": False,
        "expired": True,
        "current_dte": -4,
    }
    assert result["beforeNewYorkMidnight"]["current_dte"] == 0
    assert result["beforeNewYorkMidnight"]["expired"] is False
    assert result["afterNewYorkMidnight"]["current_dte"] == -1
    assert result["afterNewYorkMidnight"]["expired"] is True
    assert result["fallDst"]["current_dte"] == 14
    assert result["missing"]["batch_date"] is None
    assert result["missing"]["current_dte"] is None
    assert result["invalidExpiry"]["current_dte"] is None
    assert result["observedFallback"]["batch_date"] == "2026-09-07"
    assert result["observedFallback"]["historical"] is True
    assert result["unchanged"] is True


def test_expired_research_cards_render_history_and_remove_reprice_entry_prompt() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''class Node {{
  constructor() {{ this.children = []; this.textContent = ""; }}
  append(...children) {{ this.children.push(...children); }}
}}
globalThis.document = {{ addEventListener() {{}}, createElement() {{ return new Node(); }} }};
const {{ buildOverviewResearchCard, buildResearchTop10Card, researchTop10StageSummary }} = await import("{script_uri}");
const item = {{
  id: "old-xlp", rank: 1, symbol: "XLP", direction: "BULLISH",
  strategy: "BULL_CALL_VERTICAL", expiry: "2026-09-04", dte: 16,
  trading_date: "2026-08-19", batch_observed_at: "2026-08-19T13:35:06Z",
  legs: [{{ side: "BUY", ratio: 1, expiry: "2026-09-04", strike: "87", right: "C" }}],
  quote_status: "UNAVAILABLE", ev_status: "UNAVAILABLE", intraday_recovery: true,
  entry_condition: "Wait for a fresh atomic IBKR reprice before review.",
  blockers: ["QUOTE_UNAVAILABLE"],
}};
const now = new Date("2026-09-08T14:00:00Z");
function text(node) {{ return [node.textContent, ...node.children.map(text)].join(" "); }}
const current = {{ ...item, expiry: "2026-09-25", trading_date: "2026-09-08", dte: 17 }};
console.log(JSON.stringify({{
  overview: text(buildOverviewResearchCard(item, now)),
  detail: text(buildResearchTop10Card(item, now)),
  current: text(buildResearchTop10Card(current, now)),
  summary: researchTop10StageSummary({{
    phase: "INTRADAY_RECOVERY", trading_date: "2026-08-19",
    target_count: 10, premarket: [], open_repriced: [item],
  }}, "open-repriced", now),
}}));'''

    result = _run_module(source)
    for name in ("overview", "detail"):
        assert "历史批次 2026-08-19" in result[name]
        assert "当前 DTE -4 · 批次时 DTE 16" in result[name]
        assert "合约已到期" in result[name]
        assert "重新发现有效到期日" in result[name]
        assert "Wait for a fresh atomic IBKR reprice" not in result[name]
        assert "等待 09:35" not in result[name]
        assert "NO_TRADE" in result[name]
    assert "EXPIRED" in result["overview"]
    assert "QUOTE REQUIRED" not in result["overview"]
    assert "不可重报价或入场" in result["detail"]
    assert "当前 DTE 17 · 批次时 DTE 17" in result["current"]
    assert "Wait for a fresh atomic IBKR reprice" in result["current"]
    assert "历史批次" not in result["current"]
    assert "历史批次 2026-08-19" in result["summary"]
    assert "1 条合约已到期" in result["summary"]
    assert "当前无本阶段可用交易建议" in result["summary"]


def test_primary_exact_ev_is_available_and_crossed_open_quote_fails_closed() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''globalThis.document = {{ addEventListener() {{}} }};
const {{ normalizeResearchTop10 }} = await import("{script_uri}");
const authority = {{
  decision_authority: "SUPPORTING_ONLY", approval_eligible: false,
  instruction_creation_allowed: false, order_allowed: false, action_pool_eligible: false,
}};
const candidate = {{
  ...authority, research_id: "verified-1", rank: 1, underlying: "SPY",
  strategy_type: "BULL_CALL_VERTICAL", expiration: "2026-08-21", dte: 15,
  entry_debit_usd: "80.00", maximum_loss_usd: "90.00", cost_after_ev_usd: "15.00",
  indicative_entry_debit_usd: null, indicative_maximum_loss_usd: null,
  indicative_cost_after_ev_usd: null, assumed_multiplier: null,
  research_summary: "Verified research", blockers: [],
  legs: [{{ side: "BUY", expiration: "2026-08-21", strike: "770", right: "C",
    bid: "1.10", ask: "1.20", collected_at: "2026-08-06T13:35:00Z", blockers: [] }},
    {{ side: "SELL", expiration: "2026-08-21", strike: "775", right: "C",
    bid: "0.40", ask: "0.50", collected_at: "2026-08-06T13:35:00Z", blockers: [] }}],
}};
const valid = normalizeResearchTop10({{
  ...authority, phase: "INDICATIVE_REPRICE", observed_at: "2026-08-06T13:35:01Z",
  candidates: [candidate],
}});
const crossed = JSON.parse(JSON.stringify(candidate));
crossed.legs[0].bid = "1.30";
const invalid = normalizeResearchTop10({{
  ...authority, phase: "INDICATIVE_REPRICE", observed_at: "2026-08-06T13:35:01Z",
  candidates: [crossed],
}});
console.log(JSON.stringify({{ valid, invalid }}));'''

    payload = _run_module(source)
    row = payload["valid"]["open_repriced"][0]
    assert row["indicative_after_cost_ev_usd"] == 15
    assert row["ev_status"] == "AVAILABLE"
    assert "EXPECTED_PAYOFF_UNAVAILABLE" not in row["blockers"]
    assert payload["invalid"]["status"] == "UNAVAILABLE"
    assert payload["invalid"]["open_repriced"] == []


def test_primary_research_contract_requires_explicit_authority_at_both_levels() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''globalThis.document = {{ addEventListener() {{}} }};
const {{ normalizeResearchTop10 }} = await import("{script_uri}");
const valid = {{
  phase: "PREMARKET_RESEARCH", observed_at: "2026-08-06T13:18:00Z",
  decision_authority: "SUPPORTING_ONLY", approval_eligible: false,
  instruction_creation_allowed: false, order_allowed: false,
  action_pool_eligible: false, candidates: [{{
    research_id: "primary-1", rank: 1, underlying: "SPY",
    strategy_type: "CALL_DEBIT_SPREAD", expiration: "2026-08-21",
    entry_debit_usd: "50", maximum_loss_usd: "50",
    decision_authority: "SUPPORTING_ONLY", approval_eligible: false,
    instruction_creation_allowed: false, order_allowed: false,
    action_pool_eligible: false, legs: [{{
      side: "BUY", expiration: "2026-08-21", strike: "770", right: "C",
      collected_at: "2026-08-06T13:17:59Z",
    }}],
  }}],
}};
const missingTop = JSON.parse(JSON.stringify(valid));
delete missingTop.approval_eligible;
const missingCandidate = JSON.parse(JSON.stringify(valid));
delete missingCandidate.candidates[0].order_allowed;
console.log(JSON.stringify({{
  missingTop: normalizeResearchTop10(missingTop),
  missingCandidate: normalizeResearchTop10(missingCandidate),
}}));'''

    payload = _run_module(source)
    for normalized in payload.values():
        assert normalized["status"] == "UNAVAILABLE"
        assert normalized["decision"] == "NO_TRADE"
        assert normalized["premarket"] == []
        assert normalized["open_repriced"] == []


def test_research_top10_fetch_never_falls_back_to_news() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''globalThis.document = {{ addEventListener() {{}} }};
const {{ fetchResearchTop10 }} = await import("{script_uri}");
const seen = [];
globalThis.fetch = async (path) => {{
  seen.push(path);
  return {{ ok: false, status: 404, async json() {{ return {{ detail: "missing" }}; }} }};
}};
let missing = null;
try {{ await fetchResearchTop10(); }} catch (error) {{ missing = {{ status: error.status, message: error.message }}; }}
const failedSeen = [];
globalThis.fetch = async (path) => {{
  failedSeen.push(path);
  return {{ ok: false, status: 503, async json() {{ return {{ detail: "down" }}; }} }};
}};
let failure = null;
try {{ await fetchResearchTop10(); }} catch (error) {{ failure = {{ status: error.status, message: error.message }}; }}
console.log(JSON.stringify({{ seen, missing, failedSeen, failure }}));'''

    payload = _run_module(source)
    assert payload["seen"] == ["/api/research-top10"]
    assert payload["missing"] == {"status": 404, "message": "missing"}
    assert payload["failedSeen"] == ["/api/research-top10"]
    assert payload["failure"] == {"status": 503, "message": "down"}
