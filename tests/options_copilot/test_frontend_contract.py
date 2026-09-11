from __future__ import annotations

import json
from pathlib import Path
import subprocess


ROOT = Path(__file__).resolve().parents[2]
FRONTEND = ROOT / "options_copilot" / "frontend"


def test_hidden_page_panels_override_layout_display_rules() -> None:
    styles = (FRONTEND / "styles.css").read_text(encoding="utf-8")

    assert "[data-page-panel][hidden]" in styles
    hidden_rule = styles.split("[data-page-panel][hidden]", 1)[1].split("}", 1)[0]
    assert "display: none" in hidden_rule


def test_news_workbench_has_read_only_navigation_and_required_research_surfaces() -> None:
    html = (FRONTEND / "index.html").read_text(encoding="utf-8")
    script = (FRONTEND / "app.js").read_text(encoding="utf-8")
    news_page = html.split('<main id="news-page"', 1)[1].split("</main>", 1)[0]

    for text in (
        'data-page="overview"',
        'data-page="news"',
        "实时新闻",
        "按综合机会分排序",
        "详情与关联期权研究",
        "独立盘前 Top 10 ledger",
        "开盘 Observation（只读）",
        "本周",
        "下周",
        "两周",
        "读模型发布 / 每 60 秒轮询",
        "本周事件、上周回顾与 provisional watchlist",
        "08:30 ET 精确生成",
        "Gate 1/4/5/6 必须 UNAVAILABLE",
    ):
        assert text in html
    assert "data-action=\"approve\"" not in news_page
    assert "ibkr-review-link" not in news_page
    assert "未从 Ranking Top 10 回填" in news_page
    assert "SUPPORTING_ONLY" in news_page
    assert "OPEN_REPRICE 尚未发生" in script
    assert "ACTION OBS" not in script
    assert "宏观研究代理" in script
    assert "仅进入确定性股票研究因子" in script
    assert 'news: "/api/news"' in script
    assert 'calendar: "/api/calendar"' in script
    assert 'weeklyBrief: "/api/weekly-brief"' in script
    assert "function renderWeeklyBrief(payload)" in script
    assert "weekly-brief-status" in html
    assert "weekly-watch-list" in html
    assert "READ_ONLY_REFRESH_INTERVAL_MS = 60_000" in script
    assert "window.setInterval(refreshNewsData, READ_ONLY_REFRESH_INTERVAL_MS)" in script
    assert "innerHTML" not in script


def test_reaction_summary_distinguishes_healthy_idle_and_unsupported_scope() -> None:
    script = (FRONTEND / "app.js").read_text(encoding="utf-8")

    assert "NO_ELIGIBLE_REACTION_EVENTS" in script
    assert "eligible ${reactionCountLabel(summary.eligibleCount)}" in script
    assert "unsupported ${reactionCountLabel(summary.unsupportedCount)}" in script
    assert "健康空闲，未伪造反应进度" in script


def test_after_hours_ui_separates_marks_from_supported_economics_and_ratios() -> None:
    script = (FRONTEND / "app.js").read_text(encoding="utf-8")

    assert "逐腿 marks ${markEvidenceCount}" in script
    assert "完成成本核算 ${pricedCount}" in script
    assert '["逐腿价格证据", item.mark_evidence_status' in script
    assert '["成本核算状态", item.pricing_status' in script
    assert '["可执行报价", item.quote_status' in script
    assert '["Greeks", item.greeks_status' in script
    assert '["流动性", item.liquidity_status' in script
    assert '["DTE", formatInteger(item.dte)]' in script
    assert (
        '["指示性最大盈利", formatMoney(item.indicative_maximum_profit_usd)]'
        in script
    )
    assert '["指示性盈亏平衡", formatQuote(item.breakeven_price)]' in script
    assert (
        '["指示性成本后 EV", formatMoney(item.indicative_cost_after_ev_usd)]'
        in script
    )
    assert "组合比 ${ratio}" in script
    assert '["逐腿完整证据", afterHoursLegEvidence(item)]' in script
    assert "function afterHoursLegEvidence(item)" in script
    assert "function afterHoursMarketDataTypeLabel(value)" in script
    for evidence_fragment in (
        "contract ${contractIdentity}",
        "local ${localSymbol}",
        "class ${tradingClass}",
        "exchange ${exchange}",
        "multiplier ${formatInteger(leg.multiplier)}",
        "bid ${formatQuote(leg.bid)} / ask ${formatQuote(leg.ask)}",
        "last ${formatQuote(leg.last)} / close ${formatQuote(leg.close)}",
        "mark ${formatQuote(leg.indicative_mark)}",
        "价格基础 ${leg.price_basis || \"--\"}",
        "行情类型 ${afterHoursMarketDataTypeLabel(leg.market_data_type)}",
        "行情时间 ${formatTime(leg.quote_asof)}",
    ):
        assert evidence_fragment in script
    assert "INDICATIVE_VERTICAL_RATIO_UNSUPPORTED" in script
    for code, label in {
        "AFTER_HOURS_RESEARCH_ONLY": "收盘后结果仅供研究",
        "CANDIDATE_AFTER_COST_EV_NONPOSITIVE": "成本后 EV 不为正",
        "FRESH_EXECUTABLE_OPTION_EVIDENCE_REQUIRED": "等待正常交易时段五秒内逐腿可执行行情",
        "EXECUTABLE_LEG_QUOTE_INCOMPLETE": "逐腿可执行 bid/ask 不完整",
        "OPTION_GREEKS_INCOMPLETE": "逐腿 IV/Delta/Gamma/Theta/Vega 不完整",
        "OPTION_LIQUIDITY_EVIDENCE_INCOMPLETE": "逐腿成交量、OI 或价差流动性证据不完整",
        "STRUCTURE_PAYOFF_EVIDENCE_INCOMPLETE": "组合最大盈亏、盈亏平衡点或情景 payoff 不完整",
        "AFTER_COST_ECONOMICS_INCOMPLETE": "佣金、滑点与成本后 EV 尚未完整验证",
        "OPTION_CONTRACT_EXPIRED": "期权合约已过期",
        "OPTION_EXPIRATION_MISMATCH": "组合各腿到期日不一致",
        "INDICATIVE_VERTICAL_GEOMETRY_INVALID": "垂直价差腿结构无法严格验证",
        "INDICATIVE_AFTER_COST_UPSIDE_NONPOSITIVE": "计入成本后的最大盈利不为正",
    }.items():
        assert f'{code}: "{label}' in script


def test_after_hours_leg_evidence_renders_exact_identity_and_quote_provenance() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''globalThis.document = {{ addEventListener() {{}} }};
const {{ afterHoursLegEvidence, afterHoursMarketDataTypeLabel }} = await import("{script_uri}");
console.log(JSON.stringify({{
  evidence: afterHoursLegEvidence({{
    underlying: "QQQ",
    expiration: "2026-09-11",
    quantity: 2,
    legs: [{{
      side: "BUY",
      ratio: 1,
      underlying: "QQQ",
      contract_id_ex: "912345678@SMART",
      local_symbol: "QQQ   260911C00600000",
      trading_class: "QQQ",
      exchange: "SMART",
      multiplier: 100,
      expiration: "2026-09-11",
      dte: 15,
      right: "C",
      strike: "600",
      bid: "2.10",
      ask: "2.14",
      last: "2.12",
      close: "2.05",
      indicative_mark: "2.14",
      price_basis: "FROZEN_BBO",
      market_data_type: 2,
      quote_asof: "2026-08-27T20:00:00+08:00",
    }}],
  }}),
  types: [1, 2, 3, 4, 9, null].map(afterHoursMarketDataTypeLabel),
}}));'''
    result = subprocess.run(
        ["node", "--input-type=module", "--eval", source],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )

    payload = json.loads(result.stdout)
    for token in (
        "买入 2 张（组合比 1）",
        "QQQ 2026-09-11 600.00 Call",
        "contract 912345678@SMART",
        "local QQQ   260911C00600000",
        "class QQQ",
        "exchange SMART",
        "multiplier 100",
        "DTE 15",
        "bid 2.10 / ask 2.14",
        "last 2.12 / close 2.05",
        "mark 2.14",
        "价格基础 FROZEN_BBO",
        "行情类型 FROZEN (2)",
        "行情时间",
    ):
        assert token in payload["evidence"]
    assert payload["types"] == [
        "REALTIME (1)",
        "FROZEN (2)",
        "DELAYED (3)",
        "DELAYED_FROZEN (4)",
        "UNKNOWN (9)",
        "--",
    ]


def test_scan_failure_reasons_are_operator_readable() -> None:
    script = (FRONTEND / "app.js").read_text(encoding="utf-8")

    for reason_code in (
        "IBKR_SCANNER_MOST_ACTIVE_UNAVAILABLE",
        "IBKR_SCANNER_TOP_PERC_GAIN_UNAVAILABLE",
        "IBKR_SCANNER_TOP_PERC_LOSE_UNAVAILABLE",
        "IBKR_SCANNER_PACING_DENIED",
        "RESEARCH_ALLOCATION_INPUT_INVALID",
        "UNIVERSE_EMPTY",
    ):
        assert f"{reason_code}:" in script


def test_learning_stage_renders_comparison_before_discovery() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''globalThis.document = {{ addEventListener() {{}} }};
const {{ learningEvaluationStage }} = await import("{script_uri}");
console.log(JSON.stringify([
  learningEvaluationStage(1),
  learningEvaluationStage(29),
  learningEvaluationStage(30),
]));'''
    result = subprocess.run(
        ["node", "--input-type=module", "--eval", source],
        check=True,
        capture_output=True,
        encoding="utf-8",
        text=True,
    )

    assert json.loads(result.stdout) == [
        "COMPARISON_AVAILABLE",
        "COMPARISON_AVAILABLE",
        "DISCOVERY",
    ]


def test_overview_separates_current_readiness_from_historical_scan_evidence() -> None:
    html = (FRONTEND / "index.html").read_text(encoding="utf-8")
    script = (FRONTEND / "app.js").read_text(encoding="utf-8")

    for identifier in (
        "readiness-reasons",
        "historical-ranking-state",
        "upcoming-exact-slots",
    ):
        assert identifier in html
        assert identifier in script
    assert "不代表当前 readiness" in script
    assert "RESEARCH READY · APPROVAL BLOCKED" in script
    assert "下一精确时槽" in script
    assert 'id="scan-duration"' in html
    assert 'setText("scan-duration", scanOperationalTimingSummary(scan))' in script
    readiness_renderer = script.split("function renderReadiness", 1)[1].split(
        "function renderManagement", 1
    )[0]
    assert "scan.reasons" not in readiness_renderer
    assert "ranking.reasons" not in readiness_renderer


def test_scan_operational_timing_summary_is_bounded_and_fail_closed() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''globalThis.document = {{ addEventListener() {{}} }};
const {{ scanOperationalTimingSummary }} = await import("{script_uri}");
const base = {{
  operational_timing: {{
    schema: "options_copilot.scan_operational_timing.v1",
    scan_run_id: "scan-timed",
    total_duration_ms: 12345,
    stages: [
      {{ stage: "INPUT_ACQUISITION", duration_ms: 2345 }},
      {{ stage: "BROKER_EVIDENCE", duration_ms: 10000 }},
    ],
    decision_authority: "OBSERVATION_ONLY",
    affects_decision: false,
  }},
}};
console.log(JSON.stringify([
  scanOperationalTimingSummary(base),
  scanOperationalTimingSummary({{
    operational_timing: {{ ...base.operational_timing, affects_decision: true }},
  }}),
  scanOperationalTimingSummary({{
    operational_timing: {{ ...base.operational_timing, total_duration_ms: 1 }},
  }}),
]));'''
    result = subprocess.run(
        ["node", "--input-type=module", "--eval", source],
        check=True,
        capture_output=True,
        encoding="utf-8",
        text=True,
    )

    assert json.loads(result.stdout) == [
        "12.35s · 最慢 BROKER_EVIDENCE 10.00s",
        "--",
        "--",
    ]


def test_news_source_health_preserves_explicit_unconfigured_state() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''globalThis.document = {{ addEventListener() {{}} }};
const {{ normalizeNewsSourceHealth }} = await import("{script_uri}");
console.log(JSON.stringify(normalizeNewsSourceHealth([{{
  source: "CompanyIrEventProvider",
  source_kind: "NEWS",
  status: "UNCONFIGURED",
  reason: "UNCONFIGURED",
  success_count: 0,
  failure_date_count: 0,
}}])));'''
    result = subprocess.run(
        ["node", "--input-type=module", "--eval", source],
        check=True,
        capture_output=True,
        encoding="utf-8",
        text=True,
    )

    assert json.loads(result.stdout) == [
        {
            "source": "COMPANYIREVENTPROVIDER",
            "source_kind": "NEWS",
            "status": "UNCONFIGURED",
            "reason": "UNCONFIGURED",
            "success_count": 0,
            "failure_date_count": 0,
        }
    ]


def test_overview_renders_current_process_immediate_scan_campaign_evidence() -> None:
    html = (FRONTEND / "index.html").read_text(encoding="utf-8")
    script = (FRONTEND / "app.js").read_text(encoding="utf-8")

    assert 'scanCampaign: "/api/scans/campaign"' in script
    assert '"scanCampaign"' in script
    assert "renderImmediateScanCampaign(snapshots.scanCampaign || {})" in script
    for identifier in (
        "scan-campaign-state",
        "scan-campaign-summary",
        "scan-campaign-id",
        "scan-campaign-attempts",
    ):
        assert identifier in html
    for identifier in (
        "equity-pool-list",
        "daily-funnel-runs",
        "next-session-preparation",
        "joint-research-list",
    ):
        assert identifier in script
    assert "stopped_at_gate" in script
    assert "这里不会用旧 Top-10 填充" in html
    assert "IBKR_OPTION_EXECUTABLE_TICKS_UNAVAILABLE" in script
    assert "Gate 4 已拒绝，本批没有可下单组合" in script
    assert "operatorReasonList(attempt.reason_codes)" in script


def test_overview_consumes_equity_pool_daily_manifest_and_joint_research_rows() -> None:
    html = (FRONTEND / "index.html").read_text(encoding="utf-8")
    script = (FRONTEND / "app.js").read_text(encoding="utf-8")

    assert 'equityPool: "/api/equity-pool/latest"' in script
    assert "renderEquityPool(snapshots.equityPool || {})" in script
    assert "function renderDailyFunnel(health = {})" in script
    assert "function renderJointResearchWatchlist(payload = {})" in script
    for identifier in (
        "equity-pool-region",
        "equity-pool-list",
        "daily-funnel-region",
        "daily-funnel-runs",
        "next-session-preparation",
        "joint-research-region",
        "joint-research-list",
    ):
        assert identifier in html
    for identifier in (
        "equity-pool-list",
        "daily-funnel-runs",
        "next-session-preparation",
        "joint-research-list",
    ):
        assert identifier in script
    assert 'ranking.approval_enabled === true' in script
    assert "function recommendationGateOpen(" in script
    assert "candidate.recommendation_ready === true" in script
    assert "期权池中没有同 candidate_id/hash" in script
    assert "不可 challenge" in script
    assert 'preparationStatus === "READY"' in script
    assert '"历史准备结果 · 未完成"' in script
    assert "验证于" in script
    for label in (
        "股票研究发现",
        "严格入选",
        "期权研究结构",
        "可执行建议",
    ):
        assert label in script
    for field in (
        "equity_research_count",
        "equity_selected_count",
        "option_research_structure_count",
        "executable_count",
    ):
        assert f"preparation.{field}" in script


def test_readiness_truth_model_keeps_historical_pacing_out_of_current_reasons() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''globalThis.document = {{ addEventListener() {{}} }};
const {{ readinessTruthModel }} = await import("{script_uri}");
const result = readinessTruthModel(
  {{ decision: "READY", research_enabled: true, approval_enabled: false, approval_blockers: ["CREATOR_TRANSPORT_UNAVAILABLE"] }},
  {{ decision: "NO_TRADE", recorded_at: "2026-08-11T03:35:45+08:00", reasons: ["PACING_BUDGET_EXHAUSTED:SECDEF"] }},
  {{ approval_enabled: false, reasons: ["PACING_BUDGET_EXHAUSTED:SECDEF"] }},
  {{ dependencies: {{ production_scanner: {{ daily_operations: {{ runs: [
    {{ operation: "RESEARCH_REFRESH", scheduled_at: "2026-08-11T20:30:00+08:00", status: "PENDING" }},
    {{ operation: "TOP10_FREEZE", scheduled_at: "2026-08-11T21:20:00+08:00", status: "PENDING" }},
    {{ operation: "TOP10_REPRICE", scheduled_at: "2026-08-11T21:35:00+08:00", status: "PENDING" }},
    {{ operation: "ORDINARY_SCAN", scheduled_at: "2026-08-11T22:00:00+08:00", status: "PENDING" }}
  ]}} }} }} }},
  new Date("2026-08-11T17:00:00+08:00"),
);
console.log(JSON.stringify({{
  researchReady: result.researchReady,
  currentReasons: result.currentReasons,
  historicalReasons: result.historicalReasons,
  upcoming: result.upcomingSlots.map((item) => item.operation),
}}));'''
    result = subprocess.run(
        ["node", "--input-type=module", "--eval", source],
        check=True,
        capture_output=True,
        text=True,
    )

    assert json.loads(result.stdout) == {
        "researchReady": True,
        "currentReasons": ["CREATOR_TRANSPORT_UNAVAILABLE"],
        "historicalReasons": ["PACING_BUDGET_EXHAUSTED:SECDEF"],
        "upcoming": ["RESEARCH_REFRESH", "TOP10_FREEZE", "TOP10_REPRICE", "ORDINARY_SCAN"],
    }


def test_feature_data_chain_metadata_has_priority_in_readiness_display_only() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''globalThis.document = {{ addEventListener() {{}} }};
const {{ readinessTruthModel, readinessBadgePresentation, overviewBlockedActionSummary, actionControlGate }} = await import("{script_uri}");
const readiness = {{
  status: "DEGRADED", readiness_scope: "DEPENDENCY_WIRING_ONLY",
  decision: "READY", research_enabled: true, approval_enabled: false,
  approval_blockers: ["CREATOR_TRANSPORT_UNAVAILABLE"],
  feature_data_chain: {{
    status: "INCOMPLETE", model_input_complete: false,
    reason_codes: ["FEATURE_HISTORY_PRODUCER_UNWIRED", "FEATURE_PRODUCTION_AUTHORITY_UNRESOLVED", "CANDIDATE_FEATURE_BINDING_UNWIRED"],
    decision_authority: "OBSERVATION_ONLY", affects_decision: false,
  }},
}};
const original = JSON.stringify(readiness);
const truth = readinessTruthModel(readiness, {{ decision: "NO_TRADE", reasons: ["OLD_PACING_FAILURE"] }}, {{ approval_enabled: false }});
const missing = readinessTruthModel({{ status: "DEGRADED", decision: "NO_TRADE", missing_dependencies: ["IBKR_SNAPSHOT"] }});
const complete = readinessTruthModel({{
  ...readiness, feature_data_chain: {{ status: "COMPLETE", model_input_complete: true, reason_codes: [] }},
}});
const now = Date.parse("2026-09-08T15:00:00Z");
const context = {{
  readinessStatus: "READY", scanDecision: "CANDIDATES_AVAILABLE", approvalEnabled: true,
  strategyNavReady: true, brokerState: "FRESH", lastControlPollSucceeded: true,
  lastControlPollAtMs: now, lastSuccessfulControlAtMs: now,
}};
console.log(JSON.stringify({{
  truth, badge: readinessBadgePresentation(truth, "DEGRADED", false),
  inconsistentBadge: readinessBadgePresentation(truth, "READY", true),
  summary: overviewBlockedActionSummary({{ no_trade_reason: "CREATOR_TRANSPORT_UNAVAILABLE" }}, {{}}, {{}}, readiness),
  missingBadge: readinessBadgePresentation(missing, "DEGRADED", false),
  missingFeature: missing.featureChain,
  completeBadge: readinessBadgePresentation(complete, "DEGRADED", false),
  controlsUnchanged: actionControlGate(context, now),
  unchanged: original === JSON.stringify(readiness),
}}));'''
    result = subprocess.run(
        ["node", "--input-type=module", "--eval", source],
        check=True,
        capture_output=True,
        encoding="utf-8",
        text=True,
    )
    payload = json.loads(result.stdout)

    assert payload["badge"] == {
        "label": "数据链路不完整 · NO_TRADE",
        "className": "status-stale",
    }
    assert payload["inconsistentBadge"] == payload["badge"]
    assert payload["truth"]["readinessDecision"] == "READY"
    assert payload["truth"]["researchReady"] is True
    assert payload["truth"]["approvalEnabled"] is False
    assert payload["truth"]["featureChain"]["wiringOnly"] is True
    assert "OLD_PACING_FAILURE" not in payload["truth"]["currentReasons"]
    for text in ("历史特征生产者未接入", "特征口径未获验证", "分标的到期日绑定未接入"):
        assert text in payload["summary"]
    assert "CREATOR_TRANSPORT_UNAVAILABLE" not in payload["summary"]
    assert "模型错误" not in payload["summary"]
    assert payload["missingBadge"]["label"] == "DEGRADED · NO_TRADE"
    assert payload["missingFeature"]["incomplete"] is False
    assert payload["completeBadge"]["label"] == "RESEARCH READY · APPROVAL BLOCKED"
    assert payload["controlsUnchanged"] is True
    assert payload["unchanged"] is True

    script = (FRONTEND / "app.js").read_text(encoding="utf-8")
    renderer = script.split("function renderReadiness", 1)[1].split("function renderDailyFunnel", 1)[0]
    assert "readinessBadgePresentation(truth, readinessStatus, ready)" in renderer
    assert "state.textContent = badge.label" in renderer
    assert "依赖接线检查" in renderer
    assert "operatorReasonList([...new Set(currentReasons)])" in renderer
    html = (FRONTEND / "index.html").read_text(encoding="utf-8")
    assert 'src="/assets/app.js?v=20260909.6"' in html


def test_broker_review_mode_never_claims_instruction_creation_without_creator() -> None:
    html = (FRONTEND / "index.html").read_text(encoding="utf-8")
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''globalThis.document = {{ addEventListener() {{}} }};
const {{ brokerReviewModeText }} = await import("{script_uri}");
console.log(JSON.stringify([
  brokerReviewModeText(
    {{ approval_enabled: false, approval_blockers: ["CREATOR_TRANSPORT_UNAVAILABLE"] }},
    {{ approval_enabled: false }},
  ),
  brokerReviewModeText(
    {{ approval_enabled: true, approval_blockers: ["CREATOR_TRANSPORT_UNAVAILABLE"] }},
    {{ approval_enabled: true }},
  ),
  brokerReviewModeText(
    {{ approval_enabled: true, approval_blockers: [] }},
    {{ approval_enabled: true }},
  ),
]));'''
    result = subprocess.run(
        ["node", "--input-type=module", "--eval", source],
        check=True,
        capture_output=True,
        encoding="utf-8",
        text=True,
    )

    assert 'id="ibkr-review-mode"' in html
    assert "仅创建指令" not in html
    assert json.loads(result.stdout) == [
        "仅供审核 · 不可创建指令",
        "仅供审核 · 不可创建指令",
        "可发起审核 · 仍需人工确认",
    ]


def test_daily_funnel_truth_distinguishes_market_closed_from_empty_manifest() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''globalThis.document = {{ addEventListener() {{}} }};
const {{ dailyFunnelTruth }} = await import("{script_uri}");
console.log(JSON.stringify(dailyFunnelTruth([], "", "CLOSED", "2026-08-24")));'''
    result = subprocess.run(
        ["node", "--input-type=module", "--eval", source],
        check=True,
        capture_output=True,
        encoding="utf-8",
        text=True,
    )

    assert json.loads(result.stdout) == {
        "terminalCount": 0,
        "failureCount": 0,
        "stateClass": "status-stale",
        "summary": "MARKET CLOSED · NEXT 2026-08-24",
        "emptyMessage": (
            "当前是非交易日；下一交易日 2026-08-24，"
            "不会补跑或伪造今日时槽。"
        ),
    }


def test_daily_funnel_truth_exposes_inner_top10_no_trade() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''globalThis.document = {{ addEventListener() {{}} }};
const {{ dailyFunnelTruth, dailyRunPresentation }} = await import("{script_uri}");
const runs = [
  {{
    operation: "TOP10_FREEZE",
    status: "COMPLETED",
    producer_status: "NO_TRADE",
    producer_written_count: 0,
    reason_codes: ["UNDERLYING_QUOTE_EMPTY"],
  }},
];
console.log(JSON.stringify({{
  presentation: dailyRunPresentation(runs[0]),
  truth: dailyFunnelTruth(runs, "a".repeat(64), "TRADING_SESSION", null),
}}));'''
    result = subprocess.run(
        ["node", "--input-type=module", "--eval", source],
        check=True,
        capture_output=True,
        encoding="utf-8",
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["presentation"] == {
        "status": "COMPLETED",
        "stateClass": "status-down",
        "label": "COMPLETED · NO_TRADE · 写入 0/10 · UNDERLYING_QUOTE_EMPTY",
    }
    assert payload["truth"]["terminalCount"] == 1
    assert payload["truth"]["failureCount"] == 0
    assert payload["truth"]["stateClass"] == "status-stale"
    assert payload["truth"]["summary"] == (
        "1/1 TERMINAL · 0 FAILED/MISSED · 1 NO_TRADE · DURABLE"
    )


def test_overview_action_summary_prefers_current_top10_producer_truth() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''globalThis.document = {{ addEventListener() {{}} }};
const {{ overviewBlockedActionSummary, top10ProducerTruth }} = await import("{script_uri}");
const health = {{ dependencies: {{ production_scanner: {{ top10_producer: {{
  last_producer_status: "NO_TRADE",
  last_written_count: 0,
  last_reason_codes: ["UNDERLYING_QUOTE_EMPTY"],
  last_producer_run_id: "run.current",
}} }} }} }};
console.log(JSON.stringify({{
  truth: top10ProducerTruth(health),
  current: overviewBlockedActionSummary(
    {{ reasons: ["RESEARCH_ALLOCATION_INPUT_INVALID"] }},
    health,
    {{}},
  ),
  historicalOnly: overviewBlockedActionSummary(
    {{ reasons: ["RESEARCH_ALLOCATION_INPUT_INVALID"] }},
    {{}},
    {{}},
  ),
}}));'''
    result = subprocess.run(
        ["node", "--input-type=module", "--eval", source],
        check=True,
        capture_output=True,
        encoding="utf-8",
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["truth"] == {
        "available": True,
        "producerStatus": "NO_TRADE",
        "writtenCount": 0,
        "reasonCodes": ["UNDERLYING_QUOTE_EMPTY"],
        "runId": "run.current",
    }
    assert payload["current"] == (
        "当前 Top-10 producer NO_TRADE · 写入 0/10 · UNDERLYING_QUOTE_EMPTY"
    )
    assert payload["historicalOnly"] == (
        "当前 producer 尚无新终态；历史 ranking 原因："
        "新闻到股票池的研究分配输入无效；本轮在股票池生成前停止"
    )


def test_overview_action_summary_exposes_latest_completed_scan_funnel() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''globalThis.document = {{ addEventListener() {{}} }};
const {{ overviewBlockedActionSummary }} = await import("{script_uri}");
const ranking = {{
  decision: "NO_TRADE",
  recommendations_available: false,
  candidates: [],
  funnel_trace: {{
    discovered_underlyings: 138,
    deep_scan_requested: 3,
    deep_scan_attempted: 1,
    deep_scan_completed: 1,
    deep_scan_deferred: 2,
    deep_scan_deferred_symbols: ["AMZN", "GLD"],
    ranked_count: 0,
    optionability_exclusion_reasons: [
      {{ symbol: "LGCL", reason_code: "OPTIONABILITY_NO_ELIGIBLE_EXPIRATION" }},
    ],
    underlying_quote_exclusion_reasons: [
      {{ symbol: "NVDA", reason_code: "EQUITY_THESIS_UNCERTAINTY_ABOVE_STRUCTURE_LIMIT" }},
      {{ symbol: "SLV", reason_code: "EQUITY_THESIS_UNCERTAINTY_ABOVE_STRUCTURE_LIMIT" }},
    ],
  }},
}};
console.log(overviewBlockedActionSummary(ranking, {{}}, {{}}));'''
    result = subprocess.run(
        ["node", "--input-type=module", "--eval", source],
        check=True,
        capture_output=True,
        encoding="utf-8",
        text=True,
    )

    assert result.stdout.strip() == (
        "最近一次只读扫描已完成：发现 138，入选深扫 3，实际尝试 1，"
        "完成 1，预算延期 2，进入排名 0；AMZN、GLD 因本轮 SECDEF pacing "
        "预算延期，未请求行情；"
        "LGCL 无 14–35 DTE 合格期权到期日；NVDA、SLV 股票 thesis "
        "不确定度超过 0.55 结构生成上限。"
    )


def test_overview_action_summary_rejects_malformed_scan_funnel() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''globalThis.document = {{ addEventListener() {{}} }};
const {{ overviewBlockedActionSummary }} = await import("{script_uri}");
const ranking = {{
  decision: "NO_TRADE",
  recommendations_available: false,
  candidates: [],
  reasons: ["UNIVERSE_EMPTY"],
  funnel_trace: {{
    discovered_underlyings: "138",
    deep_scan_requested: 3,
    deep_scan_completed: 3,
    ranked_count: 0,
    underlying_quote_exclusion_reasons: [
      {{ symbol: "<script>", reason_code: "EQUITY_THESIS_DIRECTION_UNSUPPORTED" }},
    ],
  }},
}};
console.log(overviewBlockedActionSummary(ranking, {{}}, {{}}));'''
    result = subprocess.run(
        ["node", "--input-type=module", "--eval", source],
        check=True,
        capture_output=True,
        encoding="utf-8",
        text=True,
    )

    assert result.stdout.strip() == (
        "当前 producer 尚无新终态；历史 ranking 原因："
        "本轮没有形成可进入期权深扫的股票池"
    )


def test_top10_producer_truth_recovers_zero_count_from_matching_daily_result() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''globalThis.document = {{ addEventListener() {{}} }};
const {{ top10ProducerTruth }} = await import("{script_uri}");
const evidenceHash = "e".repeat(64);
const health = {{ dependencies: {{ production_scanner: {{
  top10_producer: {{
    last_producer_status: "NO_TRADE",
    last_written_count: null,
    last_reason_codes: ["EQUITY_THESIS_EVIDENCE_UNAVAILABLE"],
    last_producer_run_id: "top10-premarket-2026-08-28",
    last_producer_evidence_hash: evidenceHash,
  }},
  daily_operations: {{ today: {{ runs: [{{
    operation: "TOP10_FREEZE",
    status: "COMPLETED",
    producer_status: "NO_TRADE",
    producer_written_count: 0,
    producer_evidence_hash: evidenceHash,
  }}] }} }},
}} }} }};
console.log(JSON.stringify(top10ProducerTruth(health)));'''
    result = subprocess.run(
        ["node", "--input-type=module", "--eval", source],
        check=True,
        capture_output=True,
        encoding="utf-8",
        text=True,
    )

    assert json.loads(result.stdout) == {
        "available": True,
        "producerStatus": "NO_TRADE",
        "writtenCount": 0,
        "reasonCodes": ["EQUITY_THESIS_EVIDENCE_UNAVAILABLE"],
        "runId": "top10-premarket-2026-08-28",
    }


def test_overview_action_summary_explains_repaired_next_session_chain() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''globalThis.document = {{ addEventListener() {{}} }};
const {{ overviewBlockedActionSummary }} = await import("{script_uri}");
const health = {{ dependencies: {{ production_scanner: {{
  top10_producer: {{
    last_producer_status: "NO_TRADE",
    last_written_count: null,
    last_reason_codes: ["TODAY_0920_PARENT_MISSING"],
  }},
  daily_operations: {{ next_session_preparation: {{
    status: "DEGRADED",
    next_trading_date: "2026-08-28",
    reconciled_from_verified_after_hours: true,
    option_research_structure_count: 8,
    premarket_parent_eligible_structure_count: 0,
    executable_count: 0,
  }} }},
}} }} }};
console.log(overviewBlockedActionSummary({{}}, health, {{}}));'''
    result = subprocess.run(
        ["node", "--input-type=module", "--eval", source],
        check=True,
        capture_output=True,
        encoding="utf-8",
        text=True,
    )

    assert result.stdout.strip() == (
        "当前 Top-10 producer NO_TRADE · 写入数未验证 · "
        "TODAY_0920_PARENT_MISSING；收盘研究池保留 8 个结构，但独立股票 "
        "thesis 与静态风险证据合格的 09:20 父结构为 0，不能作为下一交易日可执行建议。"
    )


def test_broker_data_chain_truth_distinguishes_connection_and_snapshot_state() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''globalThis.document = {{ addEventListener() {{}} }};
const {{ brokerDataChainTruth }} = await import("{script_uri}");
const nowMs = Date.parse("2026-08-28T18:43:10Z");
console.log(JSON.stringify({{
  disconnected: brokerDataChainTruth({{dependencies: {{
    production_scanner: {{connected: false}},
    ibkr_snapshot: {{status: "STALE", stale: true}},
  }}}}),
  stale: brokerDataChainTruth({{dependencies: {{
    production_scanner: {{connected: true}},
    ibkr_snapshot: {{status: "STALE", stale: true}},
  }}}}),
  current: brokerDataChainTruth({{dependencies: {{
    production_scanner: {{connected: true}},
    ibkr_snapshot: {{status: "UP", stale: false}},
  }}}}),
  bootstrapCurrent: brokerDataChainTruth({{}}, {{account: {{
    status: "CURRENT",
    connected: true,
    reconciled: true,
    observed_at: "2026-08-28T18:43:08Z",
  }}}}, nowMs),
  bootstrapStale: brokerDataChainTruth({{}}, {{account: {{
    status: "CURRENT",
    connected: true,
    reconciled: true,
    observed_at: "2026-08-28T18:42:50Z",
  }}}}, nowMs),
  bootstrapUnreconciled: brokerDataChainTruth({{}}, {{account: {{
    status: "CURRENT",
    connected: true,
    reconciled: false,
    observed_at: "2026-08-28T18:43:08Z",
  }}}}, nowMs),
  disconnectedWins: brokerDataChainTruth({{dependencies: {{
    production_scanner: {{connected: false}},
  }}}}, {{account: {{
    status: "CURRENT",
    connected: true,
    reconciled: true,
    observed_at: "2026-08-28T18:43:08Z",
  }}}}, nowMs),
}}));'''
    result = subprocess.run(
        ["node", "--input-type=module", "--eval", source],
        check=True,
        capture_output=True,
        encoding="utf-8",
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["disconnected"]["status"] == "DISCONNECTED"
    assert payload["disconnected"]["label"] == "IBKR 未连接"
    assert "自动重连" in payload["disconnected"]["summary"]
    assert payload["stale"]["status"] == "STALE"
    assert payload["stale"]["label"] == "控制快照陈旧"
    assert "15 秒" in payload["stale"]["summary"]
    assert payload["current"]["status"] == "CURRENT"
    assert payload["current"]["label"] == "IBKR 只读链已连接"
    assert "六道 Gate" in payload["current"]["summary"]
    assert payload["bootstrapCurrent"]["status"] == "CURRENT"
    assert "账户对账" in payload["bootstrapCurrent"]["summary"]
    assert payload["bootstrapStale"]["status"] == "STALE"
    assert "15 秒" in payload["bootstrapStale"]["summary"]
    assert payload["bootstrapUnreconciled"]["status"] == "UNRECONCILED"
    assert "Strategy NAV" in payload["bootstrapUnreconciled"]["summary"]
    assert payload["disconnectedWins"]["status"] == "DISCONNECTED"


def test_candidate_banner_uses_current_producer_and_preparation_truth() -> None:
    script = (FRONTEND / "app.js").read_text(encoding="utf-8")
    renderer = script.split("function renderCandidates(payload)", 1)[1].split(
        "function renderJointResearchWatchlist",
        1,
    )[0]

    assert "overviewBlockedActionSummary(" in renderer
    assert "appState.healthSnapshot || {}" in renderer
    assert ": noTradeReason(payload)" not in renderer


def test_review_only_recommendations_do_not_require_creator_transport() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''globalThis.document = {{ addEventListener() {{}} }};
const {{ recommendationGateOpen }} = await import("{script_uri}");
console.log(recommendationGateOpen({{
  decision: "CANDIDATES_AVAILABLE",
  recommendations_available: true,
  approval_enabled: false,
  approval_blockers: ["CREATOR_TRANSPORT_UNAVAILABLE"],
}}, [{{ rank: 1, interaction: "VIEW_ONLY", recommendation_ready: true }}]));'''
    result = subprocess.run(
        ["node", "--input-type=module", "--eval", source],
        check=True,
        capture_output=True,
        encoding="utf-8",
        text=True,
    )

    assert result.stdout.strip() == "true"


def test_classifier_coverage_reports_shadow_rate_predictions_and_drift() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''globalThis.document = {{ addEventListener() {{}} }};
const {{ classifierCoverage }} = await import("{script_uri}");
console.log(JSON.stringify(classifierCoverage([
  {{ classifier: "DETERMINISTIC_RULES", category: "EARNINGS", direction: "NEUTRAL", horizon: "DAYS_1_3", research_advisory: {{ classifier: "STRUCTURED_LLM", shadow_prediction_count: 5, classification: {{ category: "EARNINGS", direction: "BULLISH", horizon: "DAYS_1_3" }} }} }},
  {{ classifier: "DETERMINISTIC_RULES", category: "OTHER", direction: "NEUTRAL", horizon: "DAYS_1_3" }},
  {{ classifier: "UNDECLARED" }}
])));'''
    result = subprocess.run(
        ["node", "--input-type=module", "--eval", source],
        check=True,
        capture_output=True,
        text=True,
    )

    assert json.loads(result.stdout) == {
        "total": 3,
        "deepseekShadow": 1,
        "structuredPrimary": 0,
        "deterministic": 2,
        "comparableShadow": 1,
        "shadowPredictions": 5,
        "undeclared": 1,
    }


def test_learning_refresh_cannot_hold_the_gui_for_the_research_timeout() -> None:
    script = (FRONTEND / "app.js").read_text(encoding="utf-8")
    refresh = script.split("async function refreshAll", 1)[1].split(
        "async function refreshControlSnapshot",
        1,
    )[0]

    for name in (
        '"news"',
        '"calendar"',
        '"weeklyBrief"',
        '"fundamentals"',
        '"advisory"',
        '"positioning"',
        '"providerConfiguration"',
    ):
        assert name in refresh
    assert (
        'if (name === "learning") {\n'
        "      return fetchJson(ENDPOINTS[name], { timeoutMs: FETCH_TIMEOUT_MS });"
    ) in refresh
    assert "return fetchResearchJson(ENDPOINTS[name]);" in refresh
    assert "void refreshLearningSnapshot(snapshots.news || appState.newsSnapshot);" in refresh


def test_full_refresh_resolves_control_authority_before_research_reads() -> None:
    script = (FRONTEND / "app.js").read_text(encoding="utf-8")
    refresh = script.split("async function refreshAll", 1)[1].split(
        "async function refreshControlSnapshot",
        1,
    )[0]

    control = refresh.index(
        "const controlNames = names.filter(\n"
        "    (name) => CONTROL_ENDPOINT_NAMES.includes(name)\n"
        "      && !DIAGNOSTIC_CONTROL_ENDPOINT_NAMES.includes(name),\n"
        "  );"
    )
    diagnostic = refresh.index(
        "const diagnosticNames = names.filter(\n"
        "    (name) => DIAGNOSTIC_CONTROL_ENDPOINT_NAMES.includes(name),\n"
        "  );"
    )
    research = refresh.index(
        "const researchNames = names.filter((name) => "
        "!CONTROL_ENDPOINT_NAMES.includes(name));"
    )
    batches = refresh.index(
        "for (const batchNames of [controlNames, diagnosticNames, researchNames])"
    )
    assert control < diagnostic < research < batches
    assert "if (DIAGNOSTIC_CONTROL_ENDPOINT_NAMES.includes(name))" in refresh
    assert "return fetchResearchJson(ENDPOINTS[name]);" in refresh
    assert (
        "const controlFailures = failures.filter(\n"
        "    (name) => POLL_CONTROL_ENDPOINT_NAMES.includes(name),\n"
        "  );"
    ) in refresh


def test_news_refresh_retries_learning_and_learning_redraws_news_counts() -> None:
    script = (FRONTEND / "app.js").read_text(encoding="utf-8")
    news_refresh = script.split("async function refreshNewsData", 1)[1].split(
        "function renderFundamentals",
        1,
    )[0]
    learning_renderer = script.split("function renderLearning", 1)[1].split(
        "function renderModel",
        1,
    )[0]

    assert "await refreshLearningSnapshot(" in news_refresh
    assert "fetchJson(ENDPOINTS.learning, { timeoutMs: FETCH_TIMEOUT_MS })" in news_refresh
    assert "renderNewsCapabilitySummary();" in learning_renderer
    assert "renderOverviewPriority();" in learning_renderer


def test_research_allocation_summary_discloses_shadow_scope_and_no_gate_effect() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''globalThis.document = {{ addEventListener() {{}} }};
const {{ researchAllocationSummary }} = await import("{script_uri}");
console.log(JSON.stringify(researchAllocationSummary({{
  decision: "NO_TRADE",
  funnel_trace: {{ research_allocation: {{
    schema: "options_copilot.research_allocation_evidence.v3",
    influence_scope: "RESEARCH_SCHEDULING_HINT_ONLY",
    decision_authority: "SUPPORTING_ONLY",
    limit: 3,
    event_symbols: ["AAPL", "MSFT"],
    total_event_symbol_count: 2,
    advisory_available_count: 1,
    advisory_selected_count: 1,
    advisory_coverage_count: 1,
    advisory_coverage_ratio: "0.500000",
    advisory_order_changed_count: 2,
    advisory_selection_displacement_count: 1,
    advisory_promoted_symbols: ["AAPL"],
    deterministic_baseline_symbols: ["SPY", "MSFT", "NVDA"],
    selected_symbols: ["SPY", "AAPL", "MSFT"],
    scanner_score_inputs: [{{ symbol: "NVDA", score: "70" }}],
    core_score_inputs: [{{ symbol: "SPY", score: "100" }}],
    eligibility_effect: "NONE",
    risk_effect: "NONE",
    approval_eligible: false,
    instruction_creation_allowed: false,
    order_allowed: false,
    score_evidence: [{{
      symbol: "AAPL",
      deterministic_score: "40",
      advisory_score: "91",
      selected_research_priority_score: "91",
      selected_research_priority_source: "NEWS_SHADOW_PRIORITY_SUPPORTING_ONLY"
    }}, {{
      symbol: "MSFT",
      deterministic_score: "80",
      advisory_score: null,
      selected_research_priority_score: "80",
      selected_research_priority_source: "NEWS_SUPPORTING_ONLY"
    }}]
  }} }}
}})));'''
    result = subprocess.run(
        ["node", "--input-type=module", "--eval", source],
        check=True,
        capture_output=True,
        text=True,
    )

    assert json.loads(result.stdout) == {
        "available": True,
        "scope": "RESEARCH_SCHEDULING_HINT_ONLY",
        "authority": "SUPPORTING_ONLY",
        "advisoryAvailable": 1,
        "advisorySelected": 1,
        "coverageCount": 1,
        "totalEventSymbols": 2,
        "coverageRatio": 0.5,
        "orderChanged": 2,
        "selectionDisplacement": 1,
        "eligibilityEffect": "NONE",
        "riskEffect": "NONE",
        "approvalEligible": False,
        "instructionCreationAllowed": False,
        "orderAllowed": False,
        "scoreEvidence": [
            {
                "symbol": "AAPL",
                "deterministicScore": 40,
                "advisoryScore": 91,
                "selectedScore": 91,
                "selectedSource": "NEWS_SHADOW_PRIORITY_SUPPORTING_ONLY",
            },
            {
                "symbol": "MSFT",
                "deterministicScore": 80,
                "advisoryScore": None,
                "selectedScore": 80,
                "selectedSource": "NEWS_SUPPORTING_ONLY",
            },
        ],
    }


def test_research_allocation_summary_rejects_legacy_and_incomplete_v3() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''globalThis.document = {{ addEventListener() {{}} }};
const {{ researchAllocationSummary }} = await import("{script_uri}");
const authorityFlags = {{
  influence_scope: "RESEARCH_SCHEDULING_HINT_ONLY",
  decision_authority: "SUPPORTING_ONLY",
  eligibility_effect: "NONE",
  risk_effect: "NONE",
  approval_eligible: false,
  instruction_creation_allowed: false,
  order_allowed: false,
}};
const summarize = (research_allocation) => researchAllocationSummary({{
  funnel_trace: {{ research_allocation }},
}});
console.log(JSON.stringify({{
  legacy: summarize({{ schema: "options_copilot.research_allocation_evidence.v2", ...authorityFlags }}),
  incompleteV3: summarize({{ schema: "options_copilot.research_allocation_evidence.v3", ...authorityFlags }}),
}}));'''
    result = subprocess.run(
        ["node", "--input-type=module", "--eval", source],
        check=True,
        capture_output=True,
    )

    payload = json.loads(result.stdout.decode("utf-8"))
    for summary in payload.values():
        assert summary["available"] is False
        assert summary["scope"] == "UNAVAILABLE"
        assert summary["authority"] == "UNAVAILABLE"
        assert summary["scoreEvidence"] == []
        assert summary["coverageRatio"] is None


def test_scanner_source_evidence_summary_distinguishes_empty_from_unrun() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''globalThis.document = {{ addEventListener() {{}} }};
const {{ scannerSourceEvidenceSummary }} = await import("{script_uri}");
const funnel_trace = {{
  scanner_completed_scan_codes: ["MOST_ACTIVE", "TOP_PERC_GAIN", "TOP_PERC_LOSE"],
  scanner_failed_scan_codes: [],
  scanner_source_row_counts: [
    {{ scan_code: "MOST_ACTIVE", row_count: 0 }},
    {{ scan_code: "TOP_PERC_GAIN", row_count: 0 }},
    {{ scan_code: "TOP_PERC_LOSE", row_count: 0 }},
  ],
}};
console.log(JSON.stringify({{
  completedEmpty: scannerSourceEvidenceSummary({{ funnel_trace }}),
  unrun: scannerSourceEvidenceSummary({{}}),
  missingSource: scannerSourceEvidenceSummary({{
    funnel_trace: {{
      scanner_completed_scan_codes: ["MOST_ACTIVE"],
      scanner_failed_scan_codes: [],
      scanner_source_row_counts: [
        {{ scan_code: "MOST_ACTIVE", row_count: 1 }},
      ],
    }},
  }}),
}}));'''
    result = subprocess.run(
        ["node", "--input-type=module", "--eval", source],
        check=True,
        capture_output=True,
    )

    payload = json.loads(result.stdout.decode("utf-8"))
    assert "MOST_ACTIVE 完成 0 行" in payload["completedEmpty"]
    assert "TOP_PERC_LOSE 完成 0 行" in payload["completedEmpty"]
    assert "不能区分空结果与未运行" in payload["unrun"]
    assert "INVALID" in payload["missingSource"]


def test_calendar_copy_does_not_imply_post_release_analysis_exists() -> None:
    script = (FRONTEND / "app.js").read_text(encoding="utf-8")

    assert "当前只证明事件预告；尚未证明 actual → surprise → 市场反应 → 期权重评闭环" in script
    assert "formatDualMarketTime(eventAt)" in script
    assert "reaction.analysisAvailable === true" in script


def test_positioning_surface_discloses_partial_chain_and_has_no_action_authority() -> None:
    html = (FRONTEND / "index.html").read_text(encoding="utf-8")
    script = (FRONTEND / "app.js").read_text(encoding="utf-8")

    for value in (
        "Max Pain / 期权墙 / PCR / GEX",
        "SUPPORTING_ONLY",
        "positioning-status",
        "positioning-list",
    ):
        assert value in html
    assert 'positioning: "/api/positioning"' in script
    assert "FROZEN_FINALIST_LEGS_ONLY" in script
    assert "不影响 eligibility / approval / instruction" in script
    assert "function renderPositioning(payload)" in script
    positioning_renderer = script.split("function renderPositioning", 1)[1].split(
        "function buildManagementPreview", 1
    )[0]
    assert "fetch(" not in positioning_renderer
    assert "challenge" not in positioning_renderer.lower()
    assert "order" not in positioning_renderer.lower()


def test_observed_vertical_plan_uses_real_position_costs_without_order_authority() -> None:
    html = (FRONTEND / "index.html").read_text(encoding="utf-8")
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''globalThis.document = {{ addEventListener() {{}} }};
const {{ deriveDefinedRiskVerticalPlan }} = await import("{script_uri}");
console.log(JSON.stringify(deriveDefinedRiskVerticalPlan([
  {{ symbol: "QQQ", security_type: "OPT", quantity: "1", average_cost: "875.04028", market_value: "857.88", unrealized_pnl: "-17.16", expiration: "2026-08-21", strike: "724", right: "C", multiplier: 100 }},
  {{ symbol: "QQQ", security_type: "OPT", quantity: "-1", average_cost: "708.941804", market_value: "-696.69", unrealized_pnl: "12.25", expiration: "2026-08-21", strike: "727", right: "C", multiplier: 100 }}
], 2121.24)));'''
    result = subprocess.run(
        ["node", "--input-type=module", "--eval", source],
        check=True,
        capture_output=True,
        text=True,
    )

    plan = json.loads(result.stdout)
    assert plan["action"] == "HOLD_MONITOR"
    assert plan["authority"] == "OBSERVATION_ONLY"
    assert plan["entryDebitUsd"] == 166.10
    assert plan["maxProfitUsd"] == 133.90
    assert plan["breakeven"] == 725.66
    assert plan["currentPnlUsd"] == -4.91
    assert plan["stopCredit"] == 1.00
    assert plan["profitTargetCredit"] == 2.46
    assert plan["timeExitDate"] == "2026-08-19"
    assert "position-plan-list" in html
    assert "position-leg-economics" in (FRONTEND / "app.js").read_text(encoding="utf-8")
    assert "no-trade-title" in html


def test_observed_credit_vertical_plan_uses_buy_to_close_without_order_authority() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''globalThis.document = {{ addEventListener() {{}} }};
const {{ deriveDefinedRiskVerticalPlan }} = await import("{script_uri}");
console.log(JSON.stringify(deriveDefinedRiskVerticalPlan([
  {{ symbol: "QQQ", security_type: "OPT", quantity: "-1", average_cost: "800", market_value: "-650", expiration: "2026-08-21", strike: "724", right: "C", multiplier: 100 }},
  {{ symbol: "QQQ", security_type: "OPT", quantity: "1", average_cost: "600", market_value: "500", expiration: "2026-08-21", strike: "727", right: "C", multiplier: 100 }}
], 2121.24)));'''
    result = subprocess.run(
        ["node", "--input-type=module", "--eval", source],
        check=True,
        capture_output=True,
        text=True,
    )

    plan = json.loads(result.stdout)
    assert plan["action"] == "HOLD_MONITOR"
    assert plan["authority"] == "OBSERVATION_ONLY"
    assert plan["strategy"] == "BEAR_CALL_CREDIT_VERTICAL"
    assert plan["closeAction"] == "BUY"
    assert plan["entryCreditUsd"] == 200
    assert plan["maxLossUsd"] == 100
    assert plan["currentClosePerShare"] == 1.5


def test_formal_management_preview_renders_generic_vertical_review_states() -> None:
    script = (FRONTEND / "app.js").read_text(encoding="utf-8")
    renderer = script.split("function buildManagementPreview", 1)[1].split(
        "function appendManagementMetric", 1
    )[0]

    assert "candidate.structure" in renderer
    assert "candidate.review_state" in renderer
    assert "candidate.entry_net_cost_usd" in renderer
    assert "candidate.entry_net_credit_usd" in renderer
    assert "candidate.entry_max_loss_usd" in renderer
    assert "candidate.stop_review_cashflow_usd" in renderer
    assert "candidate.thesis_invalidation_state" in renderer
    assert "candidate.risk_stop_state" in renderer
    assert "candidate.profit_take_state" in renderer
    assert "candidate.time_stop_state" in renderer
    assert '|| "GLD"' not in renderer
    assert "fetch(" not in renderer
    assert "submit" not in renderer.lower()

    management_renderer = script.split("function renderManagement", 1)[1].split(
        "function renderPositioning", 1
    )[0]
    assert "NO_OPEN_GLD_POSITION" not in management_renderer
    assert "正式 transition proof 仍只支持旧 GLD" not in management_renderer


def test_management_empty_state_is_suppressed_when_position_plan_is_derived() -> None:
    script = (FRONTEND / "app.js").read_text(encoding="utf-8")
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''globalThis.document = {{ addEventListener() {{}} }};
const {{ managementEmptyStateMessage }} = await import("{script_uri}");
console.log(JSON.stringify({{
  derived: managementEmptyStateMessage(0, true),
  genuinelyEmpty: managementEmptyStateMessage(0, false),
  formalCandidate: managementEmptyStateMessage(1, false),
}}));'''
    result = subprocess.run(
        ["node", "--input-type=module", "--eval", source],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )

    states = json.loads(result.stdout)
    assert states == {
        "derived": None,
        "genuinelyEmpty": "当前没有快照派生、成本与退出规则均完整的管理预览。",
        "formalCandidate": None,
    }
    assert "appState.hasDerivedManagementPreview = true" in script
    assert "appState.hasDerivedManagementPreview = false" in script
    assert "NO_TRADE" in script
    assert "不是可执行组合报价" in script


def test_position_management_only_is_not_described_as_market_ev_rejection() -> None:
    script = (FRONTEND / "app.js").read_text(encoding="utf-8")

    assert "POSITION_MANAGEMENT_ONLY" in script
    assert "当前持仓已核验，可用于持仓管理" in script
    assert "逐腿可执行退出报价不可用" in script
    assert "这不是全市场扫描后判定没有正期望机会" in script
    assert "当前仓位未验证 · 旧扫描不能确认开放组合" in script
    assert "POSITION_MANAGEMENT_ONLY 来自历史扫描" in script


def test_position_management_truth_keeps_unknown_or_stale_state_visible() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''globalThis.document = {{ addEventListener() {{}} }};
const {{ positionManagementTruth }} = await import("{script_uri}");
console.log(JSON.stringify({{
  unknown: positionManagementTruth(null, {{ reasons: [] }}),
  stale: positionManagementTruth(
    {{ positions: [], status: "STALE", position_state_known: false }},
    {{ reasons: ["POSITION_MANAGEMENT_ONLY"] }},
  ),
  verified: positionManagementTruth(
    {{ positions: [{{ symbol: "QQQ" }}], status: "CURRENT", position_state_known: true }},
    {{ reasons: ["POSITION_MANAGEMENT_ONLY"] }},
  ),
  flat: positionManagementTruth(
    {{ positions: [], status: "CURRENT", position_state_known: true }},
    {{ reasons: ["POSITION_MANAGEMENT_ONLY"] }},
  ),
}}));'''
    result = subprocess.run(
        ["node", "--input-type=module", "--eval", source],
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["unknown"]["forceVisible"] is True
    assert payload["unknown"]["positionStateKnown"] is False
    assert payload["stale"]["forceVisible"] is True
    assert payload["stale"]["verifiedOpenPosition"] is False
    assert payload["stale"]["positionManagementOnly"] is True
    assert payload["verified"]["forceVisible"] is True
    assert payload["verified"]["verifiedOpenPosition"] is True
    assert payload["flat"]["forceVisible"] is True
    assert payload["flat"]["positionStateKnown"] is True
    assert payload["flat"]["verifiedOpenPosition"] is False
    assert payload["flat"]["verifiedFlat"] is True


def test_positions_are_refreshed_with_the_five_second_control_snapshot() -> None:
    script = (FRONTEND / "app.js").read_text(encoding="utf-8")
    control_names = script.split("const CONTROL_ENDPOINT_NAMES", 1)[1].split(");", 1)[0]
    poll_names = script.split("const POLL_CONTROL_ENDPOINT_NAMES", 1)[1].split(");", 1)[0]
    diagnostic_names = script.split(
        "const DIAGNOSTIC_CONTROL_ENDPOINT_NAMES",
        1,
    )[1].split(");", 1)[0]
    control_refresh = script.split("async function refreshControlSnapshot", 1)[1].split(
        "function markControlSnapshotSucceeded",
        1,
    )[0]

    for name in (
        '"bootstrap"',
        '"health"',
        '"readiness"',
        '"scans"',
        '"scanCampaign"',
        '"rankings"',
        '"management"',
        '"positions"',
    ):
        assert name in control_names
    for name in ('"bootstrap"', '"rankings"', '"management"', '"positions"'):
        assert name in poll_names
    assert '"scans"' not in poll_names
    for name in ('"health"', '"readiness"', '"scanCampaign"'):
        assert name not in poll_names
        assert name in diagnostic_names
    assert "requireNew\n      ? CONTROL_ENDPOINT_NAMES" in control_refresh
    assert ": POLL_CONTROL_ENDPOINT_NAMES" in control_refresh
    assert "renderPositions(snapshots.positions" in control_refresh
    assert "if (snapshots.readiness && snapshots.health)" in control_refresh
    assert "if (snapshots.scanCampaign)" in control_refresh
    assert "const SCAN_REFRESH_INTERVAL_MS = 30_000;" in script
    assert "window.setInterval(refreshScanSnapshot, SCAN_REFRESH_INTERVAL_MS);" in script


def test_background_tabs_and_full_refresh_do_not_multiply_read_only_polls() -> None:
    script = (FRONTEND / "app.js").read_text(encoding="utf-8")
    refresh_all = script.split("async function refreshAll", 1)[1].split(
        "async function refreshControlSnapshot",
        1,
    )[0]
    control_refresh = script.split("async function refreshControlSnapshot", 1)[1].split(
        "function markControlSnapshotSucceeded",
        1,
    )[0]
    news_refresh = script.split("async function refreshNewsData", 1)[1].split(
        "async function refreshLearningSnapshot",
        1,
    )[0]

    assert "if (document.hidden || appState.refreshing) return;" in refresh_all
    assert "if (document.hidden || appState.refreshing) return false;" in control_refresh
    assert (
        "if (document.hidden || appState.refreshing || appState.newsRefreshing) return;"
        in news_refresh
    )
    assert 'document.addEventListener("visibilitychange", () => {' in script
    assert "if (!document.hidden) void refreshAll();" in script


def test_local_provider_configuration_surface_never_renders_credential_values() -> None:
    html = (FRONTEND / "index.html").read_text(encoding="utf-8")
    script = (FRONTEND / "app.js").read_text(encoding="utf-8")

    assert 'providerConfiguration: "/api/configuration/providers"' in script
    assert "function renderProviderConfiguration(payload)" in script
    for identifier in (
        "provider-config-jin10",
        "provider-config-finnhub",
        "provider-config-alpha-vantage",
        "provider-config-deepseek",
        "provider-config-note",
    ):
        assert identifier in html
        assert identifier in script
    assert "密钥值永不通过 API 或 GUI 返回" in html
    renderer = script.split("function renderProviderConfiguration", 1)[1].split(
        "function renderBroker", 1
    )[0]
    assert "item.value" not in renderer
    assert "item.credential" not in renderer
    assert "item.runtime_loaded" in renderer
    assert "item.restart_required" in renderer
    assert "需要重启加载" in renderer
    assert "已加载" in renderer


def test_news_page_renders_four_times_named_scores_filters_and_fail_closed_statuses() -> None:
    html = (FRONTEND / "index.html").read_text(encoding="utf-8")
    script = (FRONTEND / "app.js").read_text(encoding="utf-8")

    for value in (
        '"event_at"',
        '"published_at"',
        '"received_at"',
        '"observed_at"',
        '"event_impact_score"',
        '"option_tradability_score"',
        '"combined_opportunity_score"',
        '"PROVISIONAL"',
        '"MARKET_CONFIRMED"',
        '"CONFLICTED"',
        '"NO_TRADE"',
    ):
        assert value in script
    for identifier in ("news-category-filter", "news-source-filter", "news-symbol-filter"):
        assert identifier in html
        assert identifier in script
    assert "combined_opportunity_score" in script
    assert ".slice(0, 20)" not in script
    for identifier in ("news-research-pool-count", "news-action-pool-count"):
        assert identifier in html
        assert identifier in script
    assert "research_pool_count" in script
    assert "action_pool_count" in script
    assert "VERIFIED_PROVIDER_RELATED" in script
    assert "不进入标的 watch" in script
    assert "盘前研究池" in html
    assert "新闻开盘观察" in html
    assert "function calendarWindowBounds(windowName, now = new Date())" in script
    assert "if (windowName === \"two-weeks\")" in script
    assert "until.setDate(today.getDate() + 14)" in script


def test_news_restart_integrity_restore_is_distinct_from_zero_source_results() -> None:
    script = (FRONTEND / "app.js").read_text(encoding="utf-8")
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''globalThis.document = {{ addEventListener() {{}} }};
const {{ analysisBackfillState }} = await import("{script_uri}");
console.log(JSON.stringify({{
  restoring: analysisBackfillState({{
    analysis_backfill: {{
      status: "PENDING",
      reason: "ANALYSIS_LEDGER_INTEGRITY_PENDING",
      integrity: {{ verified_rows: 10000, remaining_rows: 2795, complete: false }},
    }},
  }}),
  ready: analysisBackfillState({{
    analysis_backfill: {{
      status: "READY",
      reason: null,
      integrity: {{ verified_rows: 12795, remaining_rows: 0, complete: true }},
    }},
  }}),
}}));'''
    result = subprocess.run(
        ["node", "--input-type=module", "--eval", source],
        check=True,
        capture_output=True,
        text=True,
    )

    states = json.loads(result.stdout)
    assert states["restoring"] == {
        "status": "PENDING",
        "reason": "ANALYSIS_LEDGER_INTEGRITY_PENDING",
        "verifiedRows": 10000,
        "remainingRows": 2795,
        "totalRows": 12795,
        "restoring": True,
    }
    assert states["ready"]["restoring"] is False
    assert "这不代表来源返回 0 条" in script
    assert "已采集，等待历史分析账本校验后显示" in script


def test_frontend_labels_provider_related_unverified_as_unverified() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''globalThis.document = {{ addEventListener() {{}} }};
const {{
  isUnverifiedNewsSymbolBinding,
  isUnverifiedSymbolBindingStatus,
}} = await import("{script_uri}");
console.log(JSON.stringify({{
  providerRelated: isUnverifiedSymbolBindingStatus("PROVIDER_RELATED_UNVERIFIED"),
  legacy: isUnverifiedSymbolBindingStatus("UNVERIFIED_LEGACY_PROVIDER"),
  verified: isUnverifiedSymbolBindingStatus("VERIFIED_PROVIDER_RELATED"),
  declared: isUnverifiedSymbolBindingStatus("SOURCE_DECLARED"),
  declaredUnverifiedReason: isUnverifiedNewsSymbolBinding({{
    symbol_binding: {{ status: "SOURCE_DECLARED" }},
    intelligence: {{ affected_assets: {{ reason: "SYMBOL_BINDING_UNVERIFIED" }} }},
  }}),
  declaredWithoutReason: isUnverifiedNewsSymbolBinding({{
    symbol_binding: {{ status: "SOURCE_DECLARED" }},
  }}),
}}));'''
    result = subprocess.run(
        ["node", "--input-type=module", "--eval", source],
        check=True,
        capture_output=True,
        text=True,
    )

    assert json.loads(result.stdout) == {
        "providerRelated": True,
        "legacy": True,
        "verified": False,
        "declared": False,
        "declaredUnverifiedReason": True,
        "declaredWithoutReason": False,
    }
    assert "isUnverifiedNewsSymbolBinding(item)" in (
        FRONTEND / "app.js"
    ).read_text(encoding="utf-8")


def test_calendar_window_bounds_keeps_two_weeks_relative_to_today() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''globalThis.document = {{ addEventListener() {{}} }};
const {{ calendarWindowBounds }} = await import("{script_uri}");
const now = new Date(2026, 7, 5, 12, 0, 0);
const show = (name) => {{ const value = calendarWindowBounds(name, now); return [value.from.getDate(), value.until.getDate()]; }};
console.log(JSON.stringify({{ thisWeek: show("this-week"), nextWeek: show("next-week"), twoWeeks: show("two-weeks") }}));'''
    result = subprocess.run(
        ["node", "--input-type=module", "--eval", source],
        check=True,
        capture_output=True,
        text=True,
    )

    assert json.loads(result.stdout) == {
        "thisWeek": [3, 10],
        "nextWeek": [10, 17],
        "twoWeeks": [5, 19],
    }


def test_outcome_processing_summary_uses_normalized_api_contract_fields() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''globalThis.document = {{ addEventListener() {{}} }};
const {{ outcomeProcessingSummary }} = await import("{script_uri}");
console.log(outcomeProcessingSummary({{
  status: "WAITING_FOR_OBSERVATIONS",
  recorded_count: 5,
  skipped_count: 4,
  blocked_count: 5,
  error_count: 7,
  remaining_count: 6,
  records_appended: 99,
  records_blocked: 98,
}}));'''
    result = subprocess.run(
        ["node", "--input-type=module", "--eval", source],
        check=True,
        capture_output=True,
        text=True,
    )
    summary = result.stdout.strip()
    assert summary.startswith("WAITING_FOR_OBSERVATIONS")
    for token in (
        "recorded 5",
        "skipped 4",
        "blocked 5",
        "errors 7",
        "remaining 6",
        "SUPPORTING_ONLY",
    ):
        assert token in summary
    assert "99" not in summary
    assert "98" not in summary
