from __future__ import annotations

import json
from pathlib import Path
import subprocess


ROOT = Path(__file__).resolve().parents[2]
FRONTEND = ROOT / "options_copilot" / "frontend"


def test_news_gui_has_independent_top10_and_open_observation_surfaces() -> None:
    html = (FRONTEND / "index.html").read_text(encoding="utf-8")
    script = (FRONTEND / "app.js").read_text(encoding="utf-8")
    news_page = html.split('<main id="news-page"', 1)[1].split("</main>", 1)[0]

    for identifier in (
        "sec-source-health",
        "jin10-source-health",
        "nasdaq-source-health",
        "official-calendar-source-health",
        "option-preselection-list",
        "option-preselection-detail",
        "option-pre-market-count",
        "option-open-repriced-count",
        "option-action-observation-count",
        "option-preselection-ledger-source",
        "option-preselection-ledger-status",
        "option-open-observation-status",
        "option-preselection-ledger-reason",
        "option-preselection-ledger-lineage",
    ):
        assert f'id="{identifier}"' in html
        assert identifier in script
    for value in (
        'data-option-pool="pre-market"',
        'data-option-pool="open-repriced"',
        "pre_market_preselections",
        "open_market_repriced",
        "option_action_pool_count",
        "maximum_loss_usd",
        "cost_after_ev_usd",
        "entry_condition",
        "stop_loss_condition",
        "ledger_lineage",
        "open_interest",
    ):
        assert value in html or value in script
    assert "合约级条件式期权预选" in html
    assert "逐腿只读报价证据" in html
    assert "开盘 Observation（只读）" in html
    assert "未从 Ranking Top 10 回填" in news_page
    assert "OPEN_REPRICE 尚未发生" in script
    assert "ACTION OBS" not in script
    assert "quote_batch_id" not in news_page
    assert "strategy_hash" not in news_page
    assert "SUPPORTING_ONLY" in news_page
    assert 'data-action="approve"' not in news_page
    assert "ibkr-review-link" not in news_page


def test_news_source_health_client_accepts_only_fixed_display_fields() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''globalThis.document = {{ addEventListener() {{}} }};
const {{ normalizeNewsSourceHealth }} = await import("{script_uri}");
console.log(JSON.stringify(normalizeNewsSourceHealth([
  {{ source: "SEC", source_kind: "NEWS", status: "DEGRADED", reason: "TICKER_RESOLUTION_FAILED", success_count: 50, failure_date_count: 0 }},
  {{ source: "Jin10", source_kind: "NEWS", status: "DOWN", reason: "AUTHENTICATION_FAILED", success_count: 0, failure_date_count: 0 }},
  {{ source: "ALPHA_VANTAGE", source_kind: "NEWS", status: "READY", reason: null, success_count: 0, failure_date_count: 0, coverage_status: "BOUNDED", coverage_reason: "PROVIDER_TICKER_LIMIT", requested_symbol_count: 21, queried_symbol_count: 10 }},
  {{ source: "NASDAQ", source_kind: "CALENDAR", status: "READY", reason: "NASDAQ_EARNINGS_PARTIAL_WINDOW", success_count: 1906, failure_date_count: 4 }},
  {{ source: "OFFICIAL_CALENDAR", source_kind: "OFFICIAL_CALENDAR", status: "ERROR", reason: "Authorization: Bearer secret", success_count: -1, failure_date_count: -1 }},
])))'''
    result = subprocess.run(
        ["node", "--input-type=module", "--eval", source],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )

    assert json.loads(result.stdout) == [
        {
            "source": "SEC",
            "source_kind": "NEWS",
            "status": "DEGRADED",
            "reason": "TICKER_RESOLUTION_FAILED",
            "success_count": 50,
            "failure_date_count": 0,
        },
        {
            "source": "JIN10",
            "source_kind": "NEWS",
            "status": "DOWN",
            "reason": "AUTHENTICATION_FAILED",
            "success_count": 0,
            "failure_date_count": 0,
        },
        {
            "source": "ALPHA_VANTAGE",
            "source_kind": "NEWS",
            "status": "READY",
            "reason": None,
            "success_count": 0,
            "failure_date_count": 0,
            "coverage_status": "BOUNDED",
            "coverage_reason": "PROVIDER_TICKER_LIMIT",
            "requested_symbol_count": 21,
            "queried_symbol_count": 10,
        },
        {
            "source": "NASDAQ",
            "source_kind": "CALENDAR",
            "status": "DEGRADED",
            "reason": "NASDAQ_EARNINGS_PARTIAL_WINDOW",
            "success_count": 1906,
            "failure_date_count": 4,
        },
        {
            "source": "OFFICIAL_CALENDAR",
            "source_kind": "OFFICIAL_CALENDAR",
            "status": "DEGRADED",
            "reason": "PROVIDER_DEGRADED",
            "success_count": 0,
            "failure_date_count": 0,
        },
    ]


def test_news_source_health_renders_jin10_down_badge() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''const nodes = new Map();
globalThis.document = {{
  addEventListener() {{}},
  getElementById(id) {{
    if (!nodes.has(id)) nodes.set(id, {{ textContent: "", classes: new Set(), classList: {{
      add(...values) {{ values.forEach((value) => nodes.get(id).classes.add(value)); }},
      remove(...values) {{ values.forEach((value) => nodes.get(id).classes.delete(value)); }},
    }} }});
    return nodes.get(id);
  }},
}};
const {{ renderNewsSourceHealth }} = await import("{script_uri}");
renderNewsSourceHealth([
  {{ source: "JIN10", source_kind: "NEWS", status: "DOWN", reason: "COOLDOWN_ACTIVE", success_count: 0, failure_date_count: 0 }},
]);
const node = nodes.get("jin10-source-health");
console.log(JSON.stringify({{ textContent: node.textContent, classes: [...node.classes].sort() }}));'''
    result = subprocess.run(
        ["node", "--input-type=module", "--eval", source],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )

    assert json.loads(result.stdout) == {
        "textContent": "金十 · DOWN · COOLDOWN_ACTIVE · cadence UNAVAILABLE · 成功 0 · 失败日期 0",
        "classes": ["status-down"],
    }


def test_option_structure_card_claims_thesis_binding_only_with_valid_evidence() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''globalThis.document = {{ addEventListener() {{}} }};
const {{ normalizeOptionStructurePool, optionStructureThesisBoundary }} = await import("{script_uri}");
const digest = "A".repeat(64);
const normalize = (equity_thesis_hash, reason_codes = []) => normalizeOptionStructurePool({{
  decisions: [{{
    underlying: "QQQ", structure: "BULL_CALL_VERTICAL", candidate_id: "candidate-1",
    equity_thesis_hash, reason_codes,
  }}],
}}).decisions[0];
const bound = normalize(digest);
const missing = normalize(digest, ["EQUITY_THESIS_EVIDENCE_UNAVAILABLE"]);
const malformed = normalize(`${{digest}}0`);
console.log(JSON.stringify({{
  boundHash: bound.equityThesisHash,
  boundMessage: optionStructureThesisBoundary(bound),
  missingMessage: optionStructureThesisBoundary(missing),
  malformedHash: malformed.equityThesisHash,
  malformedMessage: optionStructureThesisBoundary(malformed),
}}));'''
    result = subprocess.run(
        ["node", "--input-type=module", "--eval", source],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )

    payload = json.loads(result.stdout)
    assert payload["boundHash"] == "a" * 64
    assert payload["boundMessage"].startswith("股票 thesis 已绑定")
    assert payload["missingMessage"].startswith("股票 thesis 未绑定 · 仅保留结构研究")
    assert payload["malformedHash"] == ""
    assert payload["malformedMessage"].startswith("股票 thesis 未绑定 · 仅保留结构研究")


def test_option_pool_projection_accepts_ten_and_rejects_overflow() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''globalThis.document = {{ addEventListener() {{}} }};
const {{ optionPoolRows }} = await import("{script_uri}");
const authority = {{
  decision_authority: "SUPPORTING_ONLY",
  approval_eligible: false,
  instruction_creation_allowed: false,
  order_creation_allowed: false,
}};
const leg = {{ underlying: "AAPL", con_id: 101, expiry: "2026-08-21", strike: "225", right: "CALL", side: "BUY", ratio: 1, quantity: 1 }};
const digest = (index) => (index + 1).toString(16).padStart(64, "0");
const lineage = (id, phase, rank, index) => ({{
  source: "INDEPENDENT_TOP10_LEDGER", run_id: "run-1", run_created_at: "2026-08-05T13:00:00+00:00",
  head_hash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", row_id: `row-${{index}}`, row_hash: digest(index), premarket_rank: rank,
  ...(phase === "OPEN_REPRICED" ? {{ observation_id: `obs-${{id}}`, observed_at: "2026-08-05T13:01:00+00:00", observation_hash: digest(index + 20) }} : {{}}),
}});
const pre = Array.from({{ length: 10 }}, (_, index) => ({{
  ...authority, preselection_id: `p-${{index}}`, underlying: "AAPL", phase: "PRE_MARKET", research_rank: index + 1,
  legs: [leg], action_pool_eligible: false, ledger_lineage: lineage(`p-${{index}}`, "PRE_MARKET", index + 1, index),
}}));
const open = Array.from({{ length: 10 }}, (_, index) => ({{
  ...authority, preselection_id: `o-${{index}}`, underlying: "AAPL", phase: "OPEN_REPRICED", repriced_rank: index + 1,
  legs: [leg], action_pool_eligible: false, ledger_lineage: lineage(`o-${{index}}`, "OPEN_REPRICED", index + 1, index),
}}));
const payload = {{ pre_market_preselections: pre, open_market_repriced: open }};
console.log(JSON.stringify({{
  pre: optionPoolRows(payload, "pre-market").map((item) => item.preselection_id),
  open: optionPoolRows(payload, "open-repriced").map((item) => item.preselection_id),
  overflowPre: optionPoolRows({{ pre_market_preselections: [...pre, pre[0]] }}, "pre-market"),
  overflowOpen: optionPoolRows({{ open_market_repriced: [...open, open[0]] }}, "open-repriced"),
  invalid: optionPoolRows(payload, "invalid"),
}}));'''
    result = subprocess.run(
        ["node", "--input-type=module", "--eval", source],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )

    payload = json.loads(result.stdout)
    assert payload["pre"] == [f"p-{index}" for index in range(10)]
    assert payload["open"] == [f"o-{index}" for index in range(10)]
    assert payload["overflowPre"] == []
    assert payload["overflowOpen"] == []
    assert payload["invalid"] == []


def test_client_atomic_projection_requires_batch_slots_and_leg_quote_binding() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''globalThis.document = {{ addEventListener() {{}} }};
const {{ normalizePreselectionCoverage, optionPoolRows, preselectionClientProjectionValid }} = await import("{script_uri}");
const authority = {{ decision_authority: "SUPPORTING_ONLY", approval_eligible: false, instruction_creation_allowed: false, order_creation_allowed: false }};
const head = "a".repeat(64);
const rowHash = "b".repeat(64);
const batchHead = "c".repeat(64);
const baseLeg = {{
  underlying: "AAPL", con_id: 101, local_symbol: "AAPL  260821C00225000", trading_class: "AAPL", multiplier: 100, exchange: "SMART",
  expiry: "2026-08-21", strike: "225", right: "CALL", side: "BUY", ratio: 1, quantity: 1,
  bid: "2.10", ask: "2.16", quote_asof: "2026-08-05T13:30:00+00:00", quote_batch_id: "quote-open-1",
  implied_volatility: "0.31", delta: "0.42", gamma: "0.021", theta: "-0.08", vega: "0.11", volume: 240, open_interest: 1800, dte: 16,
}};
const pre = {{
  ...authority, preselection_id: "p-1", underlying: "AAPL", phase: "PRE_MARKET", research_rank: 1, action_rank: null,
  legs: [{{ ...baseLeg, quote_batch_id: "quote-pre-1", quote_asof: "2026-08-05T13:00:00+00:00" }}], action_pool_eligible: false,
  ledger_lineage: {{ source: "INDEPENDENT_TOP10_LEDGER", run_id: "run-1", run_created_at: "2026-08-05T13:00:00+00:00", head_hash: head, row_id: "row-1", row_hash: rowHash, premarket_rank: 1, production_parent_eligible: true, production_parent_blocker: null }},
}};
const open = {{
  ...authority, preselection_id: "p-1", underlying: "AAPL", phase: "OPEN_REPRICED", repriced_rank: 1, action_rank: 1,
  legs: [baseLeg], action_pool_eligible: true, risk_defined: true, maximum_loss_usd: "216", estimated_cost_usd: "216", cost_after_ev_usd: "48", maximum_quote_age_seconds: 1,
  terminal_scenarios: [{{ terminal_underlying_price: "220", probability: "0.5" }}, {{ terminal_underlying_price: "230", probability: "0.5" }}],
  scenario_asof: "2026-08-05T13:29:00+00:00", scenario_hash: "1".repeat(64),
  execution_cost_contract_version: "v1", execution_cost_contract_hash: "2".repeat(64),
  risk_policy_version: "v1", risk_policy_hash: "3".repeat(64), broker_snapshot_hash: "4".repeat(64),
  strategy_nav_usd: "10000", strategy_nav_post_hash: "5".repeat(64), economics_quote_batch_id: "quote-open-1",
  economics_quote_asof: "2026-08-05T13:30:00+00:00", payoff_hash: "6".repeat(64), economics_calculation_hash: "7".repeat(64),
  debit_usd: "206", credit_usd: "0", net_entry_cost_usd: "216", estimated_commission_usd: "2.5",
  estimated_entry_slippage_usd: "2.5", estimated_exit_slippage_usd: "5", estimated_slippage_usd: "7.5",
  expected_value_before_costs_usd: "58", risk_fraction: "0.0216",
  oldest_quote_asof: "2026-08-05T13:30:00+00:00", blockers: [], quote_batch_id: "quote-open-1",
  ledger_lineage: {{ source: "INDEPENDENT_TOP10_LEDGER", run_id: "run-1", run_created_at: "2026-08-05T13:00:00+00:00", head_hash: head, row_id: "row-1", row_hash: rowHash, premarket_rank: 1, observation_id: "obs-1", observed_at: "2026-08-05T13:30:00+00:00", observation_hash: "d".repeat(64), batch_id: "batch-1", batch_head_hash: batchHead, scheduled_for: "2026-08-05T13:30:00+00:00", quote_batch_id: "quote-open-1" }},
}};
const rawCoverage = {{
  requested_count: 10, available_count: 1, open_count: 1, source: "INDEPENDENT_TOP10_LEDGER", status: "PARTIAL",
  reason: "TOP10_PREMARKET_COVERAGE_INCOMPLETE", ledger_reason: "TOP10_PREMARKET_COVERAGE_INCOMPLETE",
  latest_run_id: "run-1", latest_head_hash: head, freeze_slot: "2026-08-05T13:00:00+00:00",
  latest_open_batch_id: "batch-1", latest_open_batch_head_hash: batchHead, reprice_slot: "2026-08-05T13:30:00+00:00",
  open_reprice_producer_status: "AVAILABLE", open_observation_status: "AVAILABLE", atomic_batch_available: true, atomic_batch_blocker: null,
  ...authority,
}};
const coverage = normalizePreselectionCoverage(rawCoverage, 1, 1);
const projectedPre = optionPoolRows({{ pre_market_preselections: [pre] }}, "pre-market");
const projectedOpen = optionPoolRows({{ open_market_repriced: [open] }}, "open-repriced");
const mixed = structuredClone(open);
mixed.legs[0].quote_batch_id = "mixed-quote";
const wrongBatchCoverage = {{ ...rawCoverage, latest_open_batch_head_hash: "e".repeat(64) }};
console.log(JSON.stringify({{
  coverage,
  valid: preselectionClientProjectionValid(projectedPre, projectedOpen, coverage, rawCoverage),
  mixed: preselectionClientProjectionValid(projectedPre, [mixed], coverage, rawCoverage),
  wrongBatch: preselectionClientProjectionValid(projectedPre, projectedOpen, normalizePreselectionCoverage(wrongBatchCoverage, 1, 1), wrongBatchCoverage),
}}));'''
    result = subprocess.run(
        ["node", "--input-type=module", "--eval", source],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )

    payload = json.loads(result.stdout)
    assert payload["valid"] is True
    assert payload["mixed"] is False
    assert payload["wrongBatch"] is False
    assert payload["coverage"]["atomic_batch_available"] is True
    assert payload["coverage"]["latest_open_batch_id"] == "batch-1"
    assert payload["coverage"]["open_reprice_producer_status"] == "AVAILABLE"


def test_top10_gui_distinguishes_unavailable_partial_and_open_not_started() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''globalThis.document = {{ addEventListener() {{}} }};
const {{ normalizePreselectionCoverage, preselectionCoverageMessage }} = await import("{script_uri}");
const unavailable = normalizePreselectionCoverage(null, 0, 0);
const partial = normalizePreselectionCoverage({{
  source: "INDEPENDENT_TOP10_LEDGER",
  status: "PARTIAL",
  reason: "OPEN_REPRICE_PRODUCER_UNAVAILABLE",
  open_observation_status: "NOT_STARTED",
  latest_run_id: "run-1",
  latest_head_hash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
}}, 4, 0);
const observed = normalizePreselectionCoverage({{
  source: "INDEPENDENT_TOP10_LEDGER",
  status: "PARTIAL",
  reason: "OPEN_REPRICE_PRODUCER_UNAVAILABLE",
  open_observation_status: "PARTIAL",
}}, 4, 2);
console.log(JSON.stringify({{
  unavailable,
  unavailableMessage: preselectionCoverageMessage(unavailable, "pre-market"),
  partialMessage: preselectionCoverageMessage(partial, "pre-market"),
  openMessage: preselectionCoverageMessage(partial, "open-repriced"),
  observedMessage: preselectionCoverageMessage(observed, "open-repriced"),
}}));'''
    result = subprocess.run(
        ["node", "--input-type=module", "--eval", source],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )

    payload = json.loads(result.stdout)
    assert payload["unavailable"]["status"] == "UNAVAILABLE"
    assert payload["unavailable"]["open_reprice_producer_status"] == "UNAVAILABLE"
    assert payload["unavailableMessage"] == (
        "独立盘前 Top‑10 ledger 未就绪；未从 Ranking Top 10 回填。"
    )
    assert payload["partialMessage"] == "独立盘前 ledger 已冻结 4/10；其余结构不推断。"
    assert payload["openMessage"] == (
        "盘前结构已冻结；OPEN_REPRICE 尚未发生，保持 NO_TRADE。"
    )
    assert payload["observedMessage"] == (
        "已记录 2/4 条开盘 observation；未观察结构保持 NO_TRADE。"
    )


def test_top10_gui_rejects_unrelated_or_misbound_open_observation() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''globalThis.document = {{ addEventListener() {{}} }};
const {{ normalizePreselectionCoverage, preselectionClientProjectionValid }} = await import("{script_uri}");
const head = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const row = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const rawCoverage = {{
  source: "INDEPENDENT_TOP10_LEDGER", requested_count: 10, available_count: 1, open_count: 1,
  status: "PARTIAL", reason: "OPEN_REPRICE_PRODUCER_UNAVAILABLE", latest_run_id: "run-1", latest_head_hash: head,
  open_reprice_producer_status: "UNAVAILABLE", decision_authority: "SUPPORTING_ONLY",
  approval_eligible: false, instruction_creation_allowed: false, order_creation_allowed: false,
}};
const coverage = normalizePreselectionCoverage(rawCoverage, 1, 1);
const pre = {{
  preselection_id: "pre", research_rank: 1,
  ledger_lineage: {{ source: "INDEPENDENT_TOP10_LEDGER", run_id: "run-1", run_created_at: "2026-08-05T13:00:00+00:00", head_hash: head, row_hash: row, premarket_rank: 1 }},
}};
const unrelated = {{
  preselection_id: "unrelated",
  ledger_lineage: {{ source: "INDEPENDENT_TOP10_LEDGER", run_id: "run-1", run_created_at: "2026-08-05T13:00:00+00:00", head_hash: head, row_hash: row, premarket_rank: 1, observation_id: "obs-1", observed_at: "2026-08-05T13:01:00+00:00", observation_hash: "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc" }},
}};
const misbound = {{ ...unrelated, preselection_id: "pre", ledger_lineage: {{ ...unrelated.ledger_lineage, row_hash: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd" }} }};
console.log(JSON.stringify({{
  unrelated: preselectionClientProjectionValid([pre], [unrelated], coverage, rawCoverage),
  misbound: preselectionClientProjectionValid([pre], [misbound], coverage, rawCoverage),
}}));'''
    result = subprocess.run(
        ["node", "--input-type=module", "--eval", source],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )

    assert json.loads(result.stdout) == {"unrelated": False, "misbound": False}


def test_calendar_uses_server_window_membership_for_date_only_official_events() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''globalThis.document = {{ addEventListener() {{}} }};
const {{ calendarEventInWindow }} = await import("{script_uri}");
const item = {{
  event_date: "2026-08-18",
  times: {{ event_at: null }},
  windows: ["FUTURE_TWO_WEEKS"],
}};
console.log(JSON.stringify({{
  thisWeek: calendarEventInWindow(item, "this-week"),
  nextWeek: calendarEventInWindow(item, "next-week"),
  twoWeeks: calendarEventInWindow(item, "two-weeks"),
}}));'''
    result = subprocess.run(
        ["node", "--input-type=module", "--eval", source],
        check=True,
        capture_output=True,
        text=True,
    )

    assert json.loads(result.stdout) == {
        "thisWeek": False,
        "nextWeek": False,
        "twoWeeks": True,
    }
