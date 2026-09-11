const ENDPOINTS = Object.freeze({
  health: "/api/health/summary",
  bootstrap: "/api/bootstrap",
  readiness: "/api/readiness",
  scans: "/api/scans/latest",
  scanCampaign: "/api/scans/campaign",
  rankings: "/api/rankings/latest",
  management: "/api/management/current",
  positions: "/api/positions",
  learning: "/api/learning",
  advisory: "/api/advisory",
  fundamentals: "/api/fundamentals",
  news: "/api/news",
  calendar: "/api/calendar",
  weeklyBrief: "/api/weekly-brief",
  positioning: "/api/positioning",
  providerConfiguration: "/api/configuration/providers",
  equityPool: "/api/equity-pool/latest",
  optionStructurePool: "/api/option-pool/latest",
});

const RESEARCH_TOP10_ROUTES = Object.freeze({
  researchTop10: "/api/research-top10",
  afterHoursIndicative: "/api/research-top10/indicative",
});

const APPROVAL_CONFIRMATION_TOKEN = "CREATE_IBKR_REVIEW_ONLY";
const MAX_CANDIDATES = 10;
const APPROVAL_POLL_INTERVAL_MS = 2000;
const READ_ONLY_REFRESH_INTERVAL_MS = 60_000;
const CONTROL_REFRESH_INTERVAL_MS = 5_000;
const SCAN_REFRESH_INTERVAL_MS = 30_000;
const CONTROL_SNAPSHOT_STALE_AFTER_MS = 15_000;
const SCAN_SNAPSHOT_STALE_AFTER_MS = 35_000;
const FETCH_TIMEOUT_MS = 4_000;
const RESEARCH_FETCH_TIMEOUT_MS = 60_000;
const POST_OUTCOME_UNKNOWN_AFTER_MS = 15_000;
const CONTROL_ENDPOINT_NAMES = Object.freeze([
  "bootstrap",
  "health",
  "readiness",
  "scans",
  "scanCampaign",
  "rankings",
  "management",
  "positions",
]);
const POLL_CONTROL_ENDPOINT_NAMES = Object.freeze([
  "bootstrap",
  "rankings",
  "management",
  "positions",
]);
const DIAGNOSTIC_CONTROL_ENDPOINT_NAMES = Object.freeze([
  "health",
  "readiness",
  "scanCampaign",
]);
const LEARNING_DISCOVERY_SAMPLE_TARGET = 30;
const OUTCOME_HORIZONS = Object.freeze(["30M", "SESSION_CLOSE", "1D", "3D", "5D"]);
const LEARNING_GOVERNANCE_SCHEMA = "options_copilot.learning.governance.v1";
const LEARNING_GOVERNANCE_MESSAGE = "影子学习只到 Discovery；生产治理保持只读锁定。NORMAL 10%，15% A-grade 未解锁，20% 绝对拒绝。";
const LEARNING_POLICY_AVAILABLE_STATUSES = new Set(["ACTIVE", "AVAILABLE", "CURRENT", "VERIFIED"]);
const LEARNING_EVALUATION_AVAILABLE_STATUSES = new Set(["AVAILABLE", "COLLECTING", "DISCOVERY", "READY", "VERIFIED"]);
const LEARNING_AUTHORITY_ACTIVE_STATUSES = new Set(["ACTIVE", "APPLIED", "APPROVED"]);
const REACTION_UNAVAILABLE = "UNAVAILABLE";
const REACTION_STAGES = new Set([
  "SCHEDULED",
  "AWAITING_RELEASE",
  "RELEASE_CAPTURED",
  "SURPRISE_ASSESSED",
  "MARKET_REACTION_OBSERVED",
  "OPTION_REEVALUATED",
  "DEGRADED",
  "CONFLICTED",
  "NO_TRADE",
]);
const REACTION_STATUSES = new Set(["READY", "DEGRADED", "CONFLICTED", "NO_TRADE", "UNAVAILABLE"]);
const REACTION_DECISIONS = new Set(["OBSERVATION_ONLY", "NO_TRADE"]);
const OPERATOR_REASON_LABELS = Object.freeze({
  IBKR_OPTION_EXECUTABLE_TICKS_UNAVAILABLE: "IBKR 已连接，但期权腿没有返回实时 bid/ask、Greeks、成交量与 OI；Gate 4 已拒绝，本批没有可下单组合",
  IBKR_SCANNER_MOST_ACTIVE_UNAVAILABLE: "IBKR 最活跃股票扫描不可用；本轮市场覆盖不完整",
  IBKR_SCANNER_TOP_PERC_GAIN_UNAVAILABLE: "IBKR 涨幅榜扫描不可用；本轮市场覆盖不完整",
  IBKR_SCANNER_TOP_PERC_LOSE_UNAVAILABLE: "IBKR 跌幅榜扫描不可用；本轮市场覆盖不完整",
  IBKR_SCANNER_PACING_DENIED: "IBKR 股票扫描触及已批准的 pacing 限额；本轮保持 NO_TRADE",
  IBKR_SCANNER_PACING_REQUEST_WINDOW_EXHAUSTED: "IBKR 股票扫描请求窗口已用尽；等待自然恢复后再扫描",
  RESEARCH_ALLOCATION_INPUT_INVALID: "新闻到股票池的研究分配输入无效；本轮在股票池生成前停止",
  UNIVERSE_EMPTY: "本轮没有形成可进入期权深扫的股票池",
  OPTIONABILITY_NO_ELIGIBLE_EXPIRATION: "无 14–35 DTE 合格期权到期日",
  EQUITY_THESIS_UNCERTAINTY_ABOVE_STRUCTURE_LIMIT: "股票 thesis 不确定度超过 0.55 结构生成上限",
  EQUITY_THESIS_DIRECTION_UNSUPPORTED: "股票 thesis 方向不受当前结构模板支持",
  EQUITY_THESIS_HAS_NO_SUPPORTED_TEMPLATE: "股票 thesis 未形成受支持结构模板（旧记录未细分不确定度、方向或 strike 网格）",
  POSITION_MANAGEMENT_ONLY: "历史扫描记录为持仓管理模式；必须用当前仓位快照复核",
  RESEARCH_LEG_RATIO_INVALID: "期权腿比例不是严格正整数，已在读取行情前拒绝",
  INDICATIVE_VERTICAL_RATIO_UNSUPPORTED: "逐腿 marks 已取得，但当前成本模型不支持该非 1:1 结构",
  AFTER_HOURS_RESEARCH_ONLY: "收盘后结果仅供研究，不能作为当前开仓建议",
  FRESH_EXECUTABLE_OPTION_EVIDENCE_REQUIRED: "等待正常交易时段五秒内逐腿可执行行情与同快照证据",
  EXECUTABLE_LEG_QUOTE_INCOMPLETE: "逐腿可执行 bid/ask 不完整",
  OPTION_GREEKS_INCOMPLETE: "逐腿 IV/Delta/Gamma/Theta/Vega 不完整",
  OPTION_LIQUIDITY_EVIDENCE_INCOMPLETE: "逐腿成交量、OI 或价差流动性证据不完整",
  STRUCTURE_PAYOFF_EVIDENCE_INCOMPLETE: "组合最大盈亏、盈亏平衡点或情景 payoff 不完整",
  AFTER_COST_ECONOMICS_INCOMPLETE: "佣金、滑点与成本后 EV 尚未完整验证",
  CANDIDATE_AFTER_COST_EV_NONPOSITIVE: "成本后 EV 不为正，仅保留为研究证据",
  OPTION_CONTRACT_EXPIRED: "期权合约已过期；不能继续核算为当前候选",
  FEATURE_HISTORY_PRODUCER_UNWIRED: "历史特征生产者未接入",
  FEATURE_HISTORY_SCHEDULED_OBSERVATIONS_NOT_MODEL_AUTHORITY: "历史定时采集已接线；实采与模型口径尚待验证",
  FEATURE_PRODUCTION_AUTHORITY_UNRESOLVED: "特征口径未获验证",
  CANDIDATE_FEATURE_BINDING_UNWIRED: "分标的到期日绑定未接入",
  OPTION_EXPIRATION_MISMATCH: "组合各腿到期日不一致；不能作为标准垂直价差",
  INDICATIVE_VERTICAL_GEOMETRY_INVALID: "垂直价差腿结构无法严格验证；不计算指示性 payoff",
  INDICATIVE_AFTER_COST_UPSIDE_NONPOSITIVE: "计入成本后的最大盈利不为正；该结构不能进入候选",
});
const AFTER_HOURS_REASON_PREFIXES = Object.freeze({
  HISTORICAL_OPTION_TRADE_TIMEOUT: "IBKR 期权历史成交查询超时",
  HISTORICAL_OPTION_TRADE_DEADLINE_EXCEEDED: "只读查询达到 8 秒安全时限，剩余期权腿未继续请求",
  HISTORICAL_OPTION_CLOSE_DEADLINE_EXCEEDED: "历史收盘价回退达到安全时限",
  HISTORICAL_OPTION_CLOSE_UNAVAILABLE: "IBKR 未提供该期权的历史收盘价",
  IBKR_HISTORICAL_OPTION_DATA_ERROR: "IBKR 不提供该期权的日线 EOD 数据",
  IBKR_HISTORICAL_OPTION_DATA_NOT_SUBSCRIBED: "当前 IBKR 行情权限不包含该期权历史数据",
  INDICATIVE_OPTION_PRICE_UNAVAILABLE: "期权腿没有可用 mark",
  RESEARCH_LEG_RATIO_INVALID: "期权腿比例不是严格正整数，已在读取行情前拒绝",
  INDICATIVE_VERTICAL_RATIO_UNSUPPORTED: "逐腿 marks 已取得，但当前成本模型不支持该非 1:1 结构",
  AFTER_HOURS_INDICATIVE_PARTIAL: "部分或全部组合无法核算",
  MORE_COMPLETE_RUNTIME_BATCH_RETAINED: "IBKR 后续返回不完整，继续显示本次运行中覆盖更完整的同合约批次",
});
const SOURCE_HEALTH_REASON_CODES = new Set([
  "INVALID_ATOM",
  "INVALID_RECORDS",
  "NASDAQ_EARNINGS_PARTIAL_WINDOW",
  "NASDAQ_EARNINGS_UNAVAILABLE",
  "NOT_FETCHED",
  "NOT_OBSERVED",
  "NO_USABLE_RECORDS",
  "OFFICIAL_CALENDAR_EVENT_INCOMPLETE",
  "OFFICIAL_CALENDAR_EVENT_REJECTED",
  "OFFICIAL_CALENDAR_PROVIDER_DEGRADED",
  "OFFICIAL_CALENDAR_PROVIDER_UNAVAILABLE",
  "OFFICIAL_CALENDAR_SNAPSHOT_INVALID",
  "OFFICIAL_CALENDAR_SNAPSHOT_STALE_OR_MISALIGNED",
  "OFFICIAL_CALENDAR_SOURCE_HEALTH_MISSING",
  "OFFICIAL_SOURCES_NOT_CONFIGURED",
  "OFFICIAL_SOURCE_DEGRADED",
  "PARTIAL_PARSE",
  "PARTIAL_TOOL_FAILURE",
  "PROVIDER_DEGRADED",
  "SOURCE_STATUS_STALE",
  "SOURCE_STATUS_CLOCK_REGRESSED",
  "RATE_LIMITED",
  "COOLDOWN_ACTIVE",
  "AUTHENTICATION_FAILED",
  "BAD_JSON",
  "CREDENTIAL_NOT_ACTIVATED",
  "REQUEST_FAILED",
  "REQUEST_TIMEOUT",
  "TICKER_RESOLUTION_FAILED",
  "TRANSPORT_UNVERIFIED",
  "UNCONFIGURED",
  "NOT_CONFIGURED",
  "UNSAFE_XML",
]);
const BEIJING_TIME_ZONE = "Asia/Shanghai";
const NEW_YORK_TIME_ZONE = "America/New_York";
const NEWS_DIGEST_LIMIT = 6;
const NEWS_DIGEST_CLUSTER_LIMIT = 2;
const CALENDAR_DIGEST_LIMIT = 8;
const CALENDAR_DETAIL_LIMIT = 50;
const CALENDAR_IMPORTANCE_WEIGHTS = Object.freeze({
  CRITICAL: 4,
  HIGH: 3,
  MEDIUM: 2,
  LOW: 1,
});
const PRESELECTION_LEDGER_SOURCE = "INDEPENDENT_TOP10_LEDGER";
const PRESELECTION_COVERAGE_STATUSES = new Set(["UNAVAILABLE", "PARTIAL", "AVAILABLE"]);
const PRESELECTION_OPEN_STATUSES = new Set(["UNAVAILABLE", "NOT_STARTED", "PARTIAL", "AVAILABLE"]);
const PENDING_APPROVAL_STATES = new Set([
  "PENDING_CODEX_BRIDGE",
  "CLAIMED",
  "AUTHORIZED",
  "UNKNOWN_OUTCOME",
]);
const moneyFormatter = new Intl.NumberFormat("en-US", {
  style: "currency",
  currency: "USD",
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
});
const integerFormatter = new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 });
const appState = {
  countdowns: [],
  refreshing: false,
  scanRefreshing: false,
  controlRefreshing: false,
  controlRefreshPromise: null,
  newsRefreshing: false,
  approvalPolls: new Map(),
  news: [],
  calendar: [],
  preMarketOptions: [],
  openRepricedOptions: [],
  preselectionCoverage: null,
  researchTop10: null,
  afterHoursIndicative: null,
  optionStructurePool: null,
  equityPool: null,
  weeklyBrief: null,
  newsSnapshot: null,
  calendarSnapshot: null,
  advisorySnapshot: null,
  fundamentalsSnapshot: null,
  learningSnapshot: null,
  researchTop10Stage: "pre-market",
  selectedNewsId: null,
  selectedOptionId: null,
  optionPoolStage: "pre-market",
  newsFilter: "ALL",
  newsCategoryFilter: "ALL",
  newsSourceFilter: "ALL",
  newsSymbolFilter: "ALL",
  calendarWindow: "this-week",
  ranking: null,
  readinessSnapshot: null,
  healthSnapshot: null,
  bootstrapSnapshot: null,
  scanSnapshot: null,
  lastSuccessfulScanAtMs: null,
  lastScanPollSucceeded: false,
  selectedCandidateId: null,
  strategyNavUsd: null,
  positionsSnapshot: null,
  hasDerivedManagementPreview: false,
  reviewLinkHref: null,
  approvalWorkflowLocks: new Set(),
  postInFlight: false,
  postOutcomeUnknownReason: null,
  overviewDetailsExpanded: false,
  lastReadOnlyRefreshAtMs: null,
  controlContext: {
    readinessStatus: "DEGRADED",
    scanDecision: "NO_TRADE",
    approvalEnabled: false,
    strategyNavReady: false,
    brokerState: "PARTIAL",
    lastControlPollAtMs: null,
    lastSuccessfulControlAtMs: null,
    lastControlPollSucceeded: false,
    controlFailureReason: "CONTROL_SNAPSHOT_NOT_LOADED",
  },
};

function byId(id) {
  return document.getElementById(id);
}

function setText(target, value) {
  const node = typeof target === "string" ? byId(target) : target;
  if (node) node.textContent = value ?? "--";
}

function numberOrNull(value) {
  if (value === null || value === undefined || value === "") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function firstValue(source, keys, fallback = null) {
  if (!source || typeof source !== "object") return fallback;
  for (const key of keys) {
    if (source[key] !== undefined && source[key] !== null) return source[key];
  }
  return fallback;
}

function formatMoney(value, { signed = false } = {}) {
  const number = numberOrNull(value);
  if (number === null) return "--";
  const formatted = moneyFormatter.format(Math.abs(number));
  if (!signed || number === 0) return number < 0 ? `-${formatted}` : formatted;
  return number > 0 ? `+${formatted}` : `-${formatted}`;
}

function formatPercent(value) {
  const number = numberOrNull(value);
  if (number === null) return "--";
  const percent = Math.abs(number) <= 1 ? number * 100 : number;
  return `${percent.toFixed(1)}%`;
}

function formatQuote(value) {
  const number = numberOrNull(value);
  return number === null ? "--" : number.toFixed(2);
}

function formatInteger(value) {
  const number = numberOrNull(value);
  return number === null ? "--" : integerFormatter.format(number);
}

function formatTime(value) {
  if (!value) return "--";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value);
  return new Intl.DateTimeFormat("zh-CN", {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  }).format(date);
}

function formatTimeInZone(value, timeZone) {
  if (!value) return "--";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "--";
  return new Intl.DateTimeFormat("zh-CN", {
    timeZone,
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).format(date);
}

function formatDualMarketTime(value) {
  if (!value || Number.isNaN(new Date(value).getTime())) return "--";
  return `${formatTimeInZone(value, BEIJING_TIME_ZONE)} 北京 / ${formatTimeInZone(value, NEW_YORK_TIME_ZONE)} ET`;
}

function dateKeyInZone(value, timeZone = BEIJING_TIME_ZONE) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return null;
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(date);
  const values = Object.fromEntries(parts.map((item) => [item.type, item.value]));
  return `${values.year}-${values.month}-${values.day}`;
}

function maskAccount(value) {
  const account = String(value || "").trim();
  if (!account) return "••••";
  if (account.length <= 4) return "••••";
  return `${account.slice(0, 2)}••••${account.slice(-2)}`;
}

async function fetchJson(path, options = {}) {
  const {
    signal: externalSignal,
    timeoutMs: requestedTimeoutMs,
    headers,
    ...requestOptions
  } = options;
  const method = String(requestOptions.method || "GET").toUpperCase();
  const timeoutEnabled = method === "GET";
  const parsedTimeoutMs = Number(requestedTimeoutMs);
  const timeoutMs = Number.isFinite(parsedTimeoutMs) && parsedTimeoutMs > 0
    ? Math.min(parsedTimeoutMs, RESEARCH_FETCH_TIMEOUT_MS)
    : FETCH_TIMEOUT_MS;
  const controller = new AbortController();
  let timedOut = false;
  const forwardAbort = () => controller.abort(externalSignal?.reason);
  if (externalSignal?.aborted) forwardAbort();
  else externalSignal?.addEventListener?.("abort", forwardAbort, { once: true });
  const timeout = timeoutEnabled
    ? globalThis.setTimeout(() => {
      timedOut = true;
      controller.abort();
    }, timeoutMs)
    : null;

  try {
    const response = await fetch(path, {
      cache: "no-store",
      ...requestOptions,
      headers: { "Content-Type": "application/json", ...(headers || {}) },
      signal: controller.signal,
    });
    let payload = {};
    try {
      payload = await response.json();
    } catch (_error) {
      payload = {};
    }
    if (!response.ok) {
      const error = new Error(payload.detail || `请求失败 (${response.status})`);
      error.status = response.status;
      throw error;
    }
    return payload;
  } catch (error) {
    if (timedOut) {
      const timeoutError = new Error(`请求超时 (${timeoutMs}ms)`);
      timeoutError.code = "FETCH_TIMEOUT";
      throw timeoutError;
    }
    throw error;
  } finally {
    if (timeout !== null) globalThis.clearTimeout(timeout);
    externalSignal?.removeEventListener?.("abort", forwardAbort);
  }
}

async function fetchResearchJson(path, options = {}) {
  return fetchJson(path, { ...options, timeoutMs: RESEARCH_FETCH_TIMEOUT_MS });
}

async function fetchHealthSummary() {
  try {
    return await fetchJson(ENDPOINTS.health);
  } catch (error) {
    if (error?.status !== 404) throw error;
    return fetchResearchJson("/health");
  }
}

async function fetchResearchTop10() {
  return fetchResearchJson(RESEARCH_TOP10_ROUTES.researchTop10);
}

async function fetchAfterHoursIndicative() {
  return fetchResearchJson(RESEARCH_TOP10_ROUTES.afterHoursIndicative);
}

async function readAfterHoursIndicative() {
  const button = byId("read-after-hours-marks");
  if (!button || button.disabled) return;
  button.disabled = true;
  setText("after-hours-indicative-summary", "正在读取 IBKR 最后可得期权 marks；仅用于指示性研究。 ");
  try {
    const payload = await fetchJson(RESEARCH_TOP10_ROUTES.afterHoursIndicative, {
      method: "POST",
      body: JSON.stringify({ confirmation_token: "READ_AFTER_HOURS_OPTION_MARKS" }),
    });
    renderAfterHoursIndicative(payload);
  } catch (error) {
    setText("after-hours-indicative-summary", `${error.message || "收盘后指示性读取失败"} · NO_TRADE`);
    byId("after-hours-indicative-list")?.replaceChildren();
  } finally {
    button.disabled = false;
  }
}

function renderAfterHoursIndicative(payload = {}) {
  const rows = Array.isArray(payload.candidates) ? payload.candidates.slice(0, MAX_CANDIDATES) : [];
  appState.afterHoursIndicative = {
    observed_at: payload.observed_at || null,
    candidates: rows,
  };
  renderOverviewPriority();
  const pricedCount = numberOrNull(payload.priced_count) ?? 0;
  const markEvidenceCount = numberOrNull(payload.mark_evidence_count) ?? pricedCount;
  const reasonCodes = afterHoursReasonSummary(payload.reason_codes);
  const sectors = numberOrNull(payload.sector_coverage?.distinct_count) ?? 0;
  const structures = numberOrNull(payload.strategy_coverage?.distinct_count) ?? 0;
  setText(
    "after-hours-indicative-summary",
    `${payload.status || "UNAVAILABLE"} · 逐腿 marks ${markEvidenceCount}/${rows.length || payload.requested_count || 0} · 完成成本核算 ${pricedCount}/${rows.length || payload.requested_count || 0} · ${sectors} 个行业组 · ${structures} 种已验证结构 · ${payload.discovery_mode || "UNKNOWN_DISCOVERY"} · ${formatTime(payload.observed_at)} · ${payload.quote_source || "数据源不可用"}${reasonCodes.length ? ` · ${reasonCodes.join(" · ")}` : ""} · AFTER_HOURS_INDICATIVE · NO_TRADE`,
  );
  const container = byId("after-hours-indicative-list");
  if (!container) return;
  container.replaceChildren();
  if (!rows.length) {
    container.append(createElement("p", "empty-state", "没有可核算的收盘后期权结构；不会用估算值补齐。"));
    return;
  }
  rows.forEach((item, index) => container.append(buildAfterHoursMarkCard(item, index + 1)));
}

function afterHoursReasonSummary(values) {
  const labels = [];
  const seen = new Set();
  (Array.isArray(values) ? values : []).forEach((value) => {
    const code = String(value || "").trim();
    if (!code) return;
    const prefix = code.split(":", 1)[0];
    const label = AFTER_HOURS_REASON_PREFIXES[prefix] || OPERATOR_REASON_LABELS[code] || code;
    if (!seen.has(label)) {
      seen.add(label);
      labels.push(label);
    }
  });
  return labels.slice(0, 4);
}

function buildAfterHoursMarkCard(item, displayRank = null) {
  const card = createElement("article", "after-hours-mark-card");
  const heading = createElement("div", "research-top10-card-heading");
  const identity = createElement("div", "research-top10-identity");
  identity.append(
    createElement(
      "span",
      "research-top10-rank",
      `#${formatInteger(displayRank ?? item.rank)}`,
    ),
    createElement("strong", "", researchText(item.underlying, "--", 24)),
    createElement(
      "span",
      `research-direction ${researchDirectionClass(item.direction)}`,
      afterHoursDirectionLabel(item.direction),
    ),
  );
  heading.append(identity, createElement("span", "status-chip status-no-trade", "NO_TRADE"));
  const details = createElement("dl", "");
  [
    ["行业 / 来源", `${researchText(item.sector, "UNCLASSIFIED", 80)} · ${researchText(item.source_scan, "UNKNOWN", 40)}`],
    ["开仓方向", `${afterHoursDirectionLabel(item.direction)} · 当前不允许开仓`],
    ["组合选择", `${afterHoursStrategyLabel(item.strategy_type)} · 1 组 · 到期 ${item.expiration || "--"}`],
    ["DTE", formatInteger(item.dte)],
    ["期权腿", afterHoursLegSummary(item)],
    ["为何进入研究池", afterHoursResearchReason(item)],
    ["何时才考虑开仓", afterHoursEntryCondition(item)],
    ["放弃条件", afterHoursInvalidationCondition(item)],
    ["逐腿价格证据", item.mark_evidence_status || "UNAVAILABLE"],
    ["成本核算状态", item.pricing_status || "UNAVAILABLE"],
    ["可执行报价", item.quote_status || "UNAVAILABLE"],
    ["Greeks", item.greeks_status || "UNAVAILABLE"],
    ["流动性", item.liquidity_status || "UNAVAILABLE"],
    ["指示性 Debit", formatMoney(item.indicative_entry_debit_usd)],
    ["含成本最大亏损", formatMoney(item.indicative_maximum_loss_usd)],
    ["指示性最大盈利", formatMoney(item.indicative_maximum_profit_usd)],
    ["指示性盈亏平衡", formatQuote(item.breakeven_price)],
    ["指示性成本后 EV", formatMoney(item.indicative_cost_after_ev_usd)],
    ["Strategy NAV 占比", formatPercent(item.strategy_nav_fraction)],
    ["价格基础", item.indicative_price_basis || "UNAVAILABLE"],
    ["逐腿完整证据", afterHoursLegEvidence(item)],
    ["阻塞项", operatorReasonList(item.blockers).join(" · ") || "仅因收盘后数据固定 NO_TRADE"],
  ].forEach(([label, value]) => {
    details.append(createElement("dt", "", label), createElement("dd", "", value));
  });
  card.append(heading, details, createElement("p", "research-top10-boundary", "AFTER_HOURS_INDICATIVE · SUPPORTING_ONLY · NO_TRADE"));
  return card;
}

function afterHoursDirectionLabel(direction) {
  const value = String(direction || "").toUpperCase();
  if (value === "BULLISH") return "看涨";
  if (value === "BEARISH") return "看跌";
  return "方向未验证";
}

function afterHoursStrategyLabel(strategyType) {
  const value = String(strategyType || "").toUpperCase();
  const labels = {
    BULL_CALL_VERTICAL: "牛市看涨价差（Bull Call Vertical）",
    BULL_PUT_VERTICAL: "牛市看跌价差（Bull Put Vertical）",
    BEAR_PUT_VERTICAL: "熊市看跌价差（Bear Put Vertical）",
    BEAR_CALL_VERTICAL: "熊市看涨价差（Bear Call Vertical）",
    DEBIT_VERTICAL: "借记价差（Debit Vertical）",
    CREDIT_VERTICAL: "信用价差（Credit Vertical）",
    CALENDAR: "日历价差（Calendar）",
    DIAGONAL: "对角价差（Diagonal）",
    BUTTERFLY: "蝶式（Butterfly）",
    IRON_CONDOR: "铁鹰式（Iron Condor）",
    LONG_OPTION: "单腿长权（Long Option）",
  };
  return labels[value] || researchText(strategyType, "结构未定义", 80);
}

function afterHoursLegSummary(item) {
  const quantity = Math.max(1, Math.trunc(numberOrNull(item.quantity) || 1));
  const rows = Array.isArray(item.legs) ? item.legs : [];
  return rows.map((leg) => {
    const side = String(leg.side || "").toUpperCase() === "SELL" ? "卖出" : "买入";
    const right = String(leg.right || "").toUpperCase() === "P" ? "Put" : "Call";
    const ratio = Math.max(1, Math.trunc(numberOrNull(leg.ratio) || 1));
    return `${side} ${quantity * ratio} 张（组合比 ${ratio}）${item.underlying || "--"} ${leg.expiration || item.expiration || "--"} ${formatQuote(leg.strike)} ${right}（conId ${leg.contract_id || "--"}）`;
  }).join("；") || "逐腿定义不可用";
}

function afterHoursMarketDataTypeLabel(value) {
  const numericValue = numberOrNull(value);
  if (numericValue === null) return "--";
  const code = Math.trunc(numericValue);
  const labels = {
    1: "REALTIME",
    2: "FROZEN",
    3: "DELAYED",
    4: "DELAYED_FROZEN",
  };
  return `${labels[code] || "UNKNOWN"} (${code})`;
}

function afterHoursLegEvidence(item) {
  const quantity = Math.max(1, Math.trunc(numberOrNull(item.quantity) || 1));
  const rows = Array.isArray(item.legs) ? item.legs : [];
  return rows.map((leg, index) => {
    const sideValue = String(leg.side || "").toUpperCase();
    const side = sideValue === "SELL" ? "卖出" : sideValue === "BUY" ? "买入" : "方向未验证";
    const rightValue = String(leg.right || "").toUpperCase();
    const right = rightValue === "P" ? "Put" : rightValue === "C" ? "Call" : "--";
    const ratio = Math.max(1, Math.trunc(numberOrNull(leg.ratio) || 1));
    const contractIdentity = String(leg.contract_id_ex || leg.contract_id || leg.con_id || "--");
    const localSymbol = researchText(leg.local_symbol, "--", 80);
    const tradingClass = researchText(leg.trading_class, "--", 40);
    const exchange = researchText(leg.exchange, "--", 24);
    return [
      `腿 ${index + 1}`,
      `${side} ${quantity * ratio} 张（组合比 ${ratio}）`,
      `${leg.underlying || item.underlying || "--"} ${leg.expiration || item.expiration || "--"} ${formatQuote(leg.strike)} ${right}`,
      `contract ${contractIdentity}`,
      `local ${localSymbol}`,
      `class ${tradingClass}`,
      `exchange ${exchange}`,
      `multiplier ${formatInteger(leg.multiplier)}`,
      `DTE ${formatInteger(leg.dte)}`,
      `bid ${formatQuote(leg.bid)} / ask ${formatQuote(leg.ask)}`,
      `last ${formatQuote(leg.last)} / close ${formatQuote(leg.close)}`,
      `mark ${formatQuote(leg.indicative_mark)}`,
      `价格基础 ${leg.price_basis || "--"}`,
      `行情类型 ${afterHoursMarketDataTypeLabel(leg.market_data_type)}`,
      `行情时间 ${formatTime(leg.quote_asof)}`,
    ].join(" · ");
  }).join(" ｜ ") || "逐腿证据不可用";
}

function afterHoursResearchReason(item) {
  const summary = String(item.research_summary || "").trim();
  if (summary.includes("exact-identity recovery")) {
    return "IBKR 已确认两条标准期权的真实合约身份；当前只是补回错过时槽后的结构研究，并没有形成基本面、新闻或成本后 EV 支持的正式开仓理由。";
  }
  return summary || "结构进入研究池，但尚未提供可审计的正式开仓论点。";
}

function afterHoursEntryCondition(item) {
  const condition = String(item.entry_condition || "").trim();
  if (condition.includes("fresh atomic IBKR reprice")) {
    return "下一正常交易时段重新取得同一 AtomicBrokerSnapshot、5 秒内逐腿可执行 bid/ask、完整 Greeks/流动性，并通过成本后 EV、NAV 风险和全部 Gate 后，才进入人工选择。";
  }
  return condition || "尚未定义；当前固定 NO_TRADE。";
}

function afterHoursInvalidationCondition(item) {
  const condition = String(item.invalidation_condition || "").trim();
  if (condition.includes("direction, event evidence")) {
    return "方向、事件证据、合约身份或流动性任一变化即放弃；报价陈旧或任一腿缺失也不考虑开仓。";
  }
  return condition || "尚未定义；任何关键证据缺失都保持 NO_TRADE。";
}

function setPageStatus(mode, message) {
  const status = byId("page-status");
  status.classList.remove("status-ready", "status-error", "status-pending");
  status.classList.add(`status-${mode}`);
  status.lastChild.textContent = ` ${message}`;
}

function markReadOnlyRefresh(value = new Date().toISOString()) {
  const parsed = Date.parse(value);
  const observedAtMs = Number.isFinite(parsed) ? parsed : Date.now();
  if (
    appState.lastReadOnlyRefreshAtMs !== null
    && observedAtMs < appState.lastReadOnlyRefreshAtMs
  ) return false;
  appState.lastReadOnlyRefreshAtMs = observedAtMs;
  setText("last-refresh", formatTime(new Date(observedAtMs).toISOString()));
  return true;
}

async function refreshAll() {
  if (document.hidden || appState.refreshing) return;
  appState.refreshing = true;
  const refreshButton = byId("refresh-data");
  refreshButton.disabled = true;
  setPageStatus("pending", "正在同步");

  const names = [
    ...Object.keys(ENDPOINTS),
    "researchTop10",
    "afterHoursIndicative",
  ];
  const fetchSnapshot = (name) => {
    if (name === "researchTop10") return fetchResearchTop10();
    if (name === "afterHoursIndicative") return fetchAfterHoursIndicative();
    if (name === "health") return fetchHealthSummary();
    if (name === "learning") {
      return fetchJson(ENDPOINTS[name], { timeoutMs: FETCH_TIMEOUT_MS });
    }
    if (DIAGNOSTIC_CONTROL_ENDPOINT_NAMES.includes(name)) {
      return fetchResearchJson(ENDPOINTS[name]);
    }
    if ([
      "news",
      "calendar",
      "weeklyBrief",
      "fundamentals",
      "advisory",
      "positioning",
      "providerConfiguration",
      "equityPool",
      "optionStructurePool",
    ].includes(name)) {
      return fetchResearchJson(ENDPOINTS[name]);
    }
    return fetchJson(ENDPOINTS[name]);
  };
  // Control authority must not compete with long research reads for the
  // four-second fail-closed timeout.  Resolve the current IBKR/account/ranking
  // batch first, then load supporting research without weakening either gate.
  const controlNames = names.filter(
    (name) => CONTROL_ENDPOINT_NAMES.includes(name)
      && !DIAGNOSTIC_CONTROL_ENDPOINT_NAMES.includes(name),
  );
  const diagnosticNames = names.filter(
    (name) => DIAGNOSTIC_CONTROL_ENDPOINT_NAMES.includes(name),
  );
  const researchNames = names.filter((name) => !CONTROL_ENDPOINT_NAMES.includes(name));
  const outcomesByName = new Map();
  for (const batchNames of [controlNames, diagnosticNames, researchNames]) {
    const batchOutcomes = await Promise.allSettled(batchNames.map(fetchSnapshot));
    batchOutcomes.forEach((outcome, index) => {
      outcomesByName.set(batchNames[index], outcome);
    });
  }
  const snapshots = {};
  const failures = [];
  names.forEach((name) => {
    const outcome = outcomesByName.get(name);
    if (outcome?.status === "fulfilled") snapshots[name] = outcome.value;
    else failures.push(name);
  });

  const controlPollAtMs = Date.now();
  const controlFailures = failures.filter(
    (name) => POLL_CONTROL_ENDPOINT_NAMES.includes(name),
  );
  if (controlFailures.length === 0) markControlSnapshotSucceeded(controlPollAtMs);

  if (snapshots.bootstrap) renderCampaign(snapshots.bootstrap);
  else appState.strategyNavUsd = null;
  if (snapshots.bootstrap || snapshots.health) {
    renderBroker(snapshots.bootstrap || {}, snapshots.health || {});
  } else {
    appState.controlContext.brokerState = "PARTIAL";
  }
  renderReadiness(
    snapshots.readiness || {},
    snapshots.scans || {},
    snapshots.rankings || {},
    snapshots.health || {},
  );
  appState.scanSnapshot = snapshots.scans || {};
  appState.lastSuccessfulScanAtMs = snapshots.scans ? controlPollAtMs : null;
  appState.lastScanPollSucceeded = Boolean(snapshots.scans);
  renderImmediateScanCampaign(snapshots.scanCampaign || {});
  if (snapshots.positions) renderPositions(snapshots.positions);
  else appState.hasDerivedManagementPreview = false;
  renderProviderConfiguration(snapshots.providerConfiguration || null);
  renderManagement(snapshots.management || null);
  renderPositioning(snapshots.positioning || null);
  if (snapshots.rankings) renderCandidates(snapshots.rankings);
  else renderCandidates({
    decision: "NO_TRADE",
    no_trade_reason: "候选扫描不可用；系统已按失败关闭原则禁止创建审核指令。",
    candidates: [],
  });
  if (controlFailures.length > 0) {
    failClosedControlSnapshot(
      controlPollAtMs,
      `CONTROL_POLL_FAILED_${controlFailures.join("_").toUpperCase()}`,
    );
  } else if (appState.postOutcomeUnknownReason) {
    failClosedControlSnapshot(controlPollAtMs, appState.postOutcomeUnknownReason);
  }
  appState.advisorySnapshot = snapshots.advisory || {};
  appState.fundamentalsSnapshot = snapshots.fundamentals || {};
  if (snapshots.learning) {
    renderLearning(
      snapshots.learning,
      appState.advisorySnapshot,
      snapshots.news || {},
    );
  } else {
    void refreshLearningSnapshot(snapshots.news || appState.newsSnapshot);
  }
  if (snapshots.news) renderNews(snapshots.news);
  if (snapshots.calendar) renderCalendar(snapshots.calendar);
  renderFundamentals(appState.fundamentalsSnapshot);
  renderWeeklyBrief(snapshots.weeklyBrief || {
    status: "NOT_RUN",
    reason_codes: ["WEEKLY_BRIEF_UNAVAILABLE"],
  });
  if (snapshots.researchTop10) renderResearchTop10(snapshots.researchTop10);
  else renderResearchTop10Unavailable();
  if (snapshots.afterHoursIndicative) renderAfterHoursIndicative(snapshots.afterHoursIndicative);
  renderOptionStructurePool(snapshots.optionStructurePool || {});
  renderEquityPool(snapshots.equityPool || {});

  markReadOnlyRefresh();
  if (failures.length === 0) setPageStatus("ready", "数据已同步");
  else if (failures.length < names.length) setPageStatus("error", `部分数据不可用 · ${failures.length}`);
  else setPageStatus("error", "服务不可用");

  refreshButton.disabled = false;
  appState.refreshing = false;
}

async function refreshControlSnapshot({ requireNew = false } = {}) {
  if (document.hidden || appState.refreshing) return false;
  while (appState.controlRefreshPromise) {
    const activeRefresh = appState.controlRefreshPromise;
    if (!requireNew) return activeRefresh;
    await activeRefresh;
  }

  appState.controlRefreshing = true;
  const pollAtMs = Date.now();
  const refreshPromise = (async () => {
    const endpointNames = requireNew
      ? CONTROL_ENDPOINT_NAMES
      : POLL_CONTROL_ENDPOINT_NAMES;
    const outcomes = await Promise.allSettled(
      endpointNames.map((name) => (
        name === "health" ? fetchHealthSummary() : fetchJson(ENDPOINTS[name])
      )),
    );
    const snapshots = {};
    const failures = [];
    outcomes.forEach((outcome, index) => {
      const name = endpointNames[index];
      if (outcome.status === "fulfilled") snapshots[name] = outcome.value;
      else failures.push(name);
    });
    if (snapshots.scans) {
      appState.scanSnapshot = snapshots.scans;
      appState.lastSuccessfulScanAtMs = pollAtMs;
      appState.lastScanPollSucceeded = true;
    }
    const scanAgeMs = appState.lastSuccessfulScanAtMs === null
      ? Number.POSITIVE_INFINITY
      : pollAtMs - appState.lastSuccessfulScanAtMs;
    if (
      !appState.lastScanPollSucceeded
      || scanAgeMs < 0
      || scanAgeMs > SCAN_SNAPSHOT_STALE_AFTER_MS
    ) {
      if (!failures.includes("scans")) failures.push("scans");
    }
    if (failures.length > 0) {
      failClosedControlSnapshot(
        pollAtMs,
        `CONTROL_POLL_FAILED_${failures.join("_").toUpperCase()}`,
      );
      return false;
    }

    renderCampaign(snapshots.bootstrap || {});
    renderBroker(snapshots.bootstrap || {}, snapshots.health || {});
    markControlSnapshotSucceeded(pollAtMs);
    if (snapshots.readiness && snapshots.health) {
      renderReadiness(
        snapshots.readiness,
        snapshots.scans || {},
        snapshots.rankings || {},
        snapshots.health,
      );
    }
    if (snapshots.scans) appState.scanSnapshot = snapshots.scans;
    if (appState.newsSnapshot) {
      renderDeepSeekAdvisory(appState.advisorySnapshot || {}, appState.newsSnapshot);
    }
    if (snapshots.scanCampaign) {
      renderImmediateScanCampaign(snapshots.scanCampaign);
    }
    renderPositions(snapshots.positions || {
      positions: [],
      status: "UNAVAILABLE",
      position_state_known: false,
      reason: "POSITION_SNAPSHOT_UNAVAILABLE",
    });
    renderManagement(snapshots.management || null);
    renderCandidates(snapshots.rankings || {
      decision: "NO_TRADE",
      no_trade_reason: "控制快照没有提供当前排名；系统保持 VIEW_ONLY。",
      candidates: [],
    });
    if (appState.postOutcomeUnknownReason) {
      failClosedControlSnapshot(pollAtMs, appState.postOutcomeUnknownReason);
      return false;
    }
    synchronizeActionControls(pollAtMs);
    return true;
  })().catch(() => {
    failClosedControlSnapshot(pollAtMs, "CONTROL_POLL_RENDER_FAILED");
    return false;
  });
  appState.controlRefreshPromise = refreshPromise;
  try {
    return await refreshPromise;
  } finally {
    if (appState.controlRefreshPromise === refreshPromise) {
      appState.controlRefreshPromise = null;
      appState.controlRefreshing = false;
    }
  }
}

async function refreshScanSnapshot() {
  if (document.hidden || appState.refreshing || appState.scanRefreshing) return false;
  appState.scanRefreshing = true;
  const pollAtMs = Date.now();
  try {
    const scan = await fetchJson(ENDPOINTS.scans);
    appState.scanSnapshot = scan;
    appState.lastSuccessfulScanAtMs = pollAtMs;
    appState.lastScanPollSucceeded = true;
    renderReadiness(
      appState.readinessSnapshot || {},
      scan,
      appState.ranking || {},
      appState.healthSnapshot || {},
    );
    return true;
  } catch (_error) {
    appState.lastScanPollSucceeded = false;
    failClosedControlSnapshot(pollAtMs, "SCAN_POLL_FAILED");
    return false;
  } finally {
    appState.scanRefreshing = false;
  }
}

function markControlSnapshotSucceeded(pollAtMs = Date.now()) {
  if (appState.postOutcomeUnknownReason) {
    appState.controlContext = {
      ...appState.controlContext,
      brokerState: "SAVED_INSTRUCTION_UNKNOWN",
      lastControlPollAtMs: pollAtMs,
      lastControlPollSucceeded: false,
      controlFailureReason: appState.postOutcomeUnknownReason,
    };
    return false;
  }
  appState.controlContext = {
    ...appState.controlContext,
    lastControlPollAtMs: pollAtMs,
    lastSuccessfulControlAtMs: pollAtMs,
    lastControlPollSucceeded: true,
    controlFailureReason: null,
  };
  return true;
}

function failClosedControlSnapshot(
  pollAtMs = Date.now(),
  reason = "CONTROL_POLL_FAILED",
  { synchronize = true } = {},
) {
  const currentBrokerState = String(appState.controlContext.brokerState || "PARTIAL").toUpperCase();
  appState.controlContext = {
    ...appState.controlContext,
    readinessStatus: "DEGRADED",
    approvalEnabled: false,
    strategyNavReady: false,
    brokerState: ["DISCONNECTED", "SAVED_INSTRUCTION_UNKNOWN"].includes(currentBrokerState)
      ? currentBrokerState
      : "STALE",
    lastControlPollAtMs: pollAtMs,
    lastControlPollSucceeded: false,
    controlFailureReason: reason,
  };
  const readiness = byId("readiness-state");
  if (readiness) {
    readiness.classList.remove("status-up", "status-down", "status-stale", "status-unknown");
    readiness.classList.add("status-stale");
    readiness.textContent = "STALE · NO_TRADE";
  }
  setText("readiness-reasons", `${reason} · 控制快照失败关闭；所有可创建动作已禁用。`);
  const brokerStatus = byId("ibkr-status");
  if (brokerStatus) {
    brokerStatus.classList.remove("status-up", "status-down", "status-stale", "status-unknown");
    brokerStatus.classList.add(appState.controlContext.brokerState === "DISCONNECTED" ? "status-down" : "status-stale");
    brokerStatus.textContent = `${appState.controlContext.brokerState} · LAST_KNOWN_ONLY`;
  }
  setText("ibkr-reconciled", "LAST_KNOWN_ONLY");
  setText("ibkr-market-data", "LAST_KNOWN_ONLY");
  setText("ibkr-review-mode", "仅供审核 · 不可创建指令");
  if (appState.positionsSnapshot) {
    renderPositions(appState.positionsSnapshot);
  } else {
    setText("position-count", "LAST_KNOWN_ONLY");
  }
  if (synchronize) synchronizeActionControls(pollAtMs, { enforceFreshness: false });
}

function enforceControlSnapshotFreshness(now = Date.now()) {
  if (appState.postOutcomeUnknownReason) {
    if (
      appState.controlContext.brokerState !== "SAVED_INSTRUCTION_UNKNOWN"
      || appState.controlContext.controlFailureReason !== appState.postOutcomeUnknownReason
    ) {
      appState.controlContext = {
        ...appState.controlContext,
        brokerState: "SAVED_INSTRUCTION_UNKNOWN",
      };
      failClosedControlSnapshot(
        now,
        appState.postOutcomeUnknownReason,
        { synchronize: false },
      );
    }
    return true;
  }
  const lastSuccessAtMs = numberOrNull(appState.controlContext.lastSuccessfulControlAtMs);
  if (
    lastSuccessAtMs === null
    || now - lastSuccessAtMs <= CONTROL_SNAPSHOT_STALE_AFTER_MS
  ) return false;
  if (
    appState.controlContext.lastControlPollSucceeded === false
    && appState.controlContext.controlFailureReason === "CONTROL_SNAPSHOT_ELAPSED_STALE"
  ) return true;
  failClosedControlSnapshot(
    now,
    "CONTROL_SNAPSHOT_ELAPSED_STALE",
    { synchronize: false },
  );
  return true;
}

async function refreshNewsData() {
  if (document.hidden || appState.refreshing || appState.newsRefreshing) return;
  appState.newsRefreshing = true;
  const healthSnapshotAtStart = appState.healthSnapshot;
  let refreshed = false;
  try {
    const [news, calendar, health, researchTop10, weeklyBrief, afterHoursIndicative, fundamentals] = await Promise.allSettled([
      fetchResearchJson(ENDPOINTS.news),
      fetchResearchJson(ENDPOINTS.calendar),
      fetchJson(ENDPOINTS.health),
      fetchResearchTop10(),
      fetchResearchJson(ENDPOINTS.weeklyBrief),
      fetchAfterHoursIndicative(),
      fetchResearchJson(ENDPOINTS.fundamentals),
    ]);
    refreshed = [news, calendar, health, researchTop10, weeklyBrief, afterHoursIndicative, fundamentals]
      .some((outcome) => outcome.status === "fulfilled");
    if (news.status === "fulfilled") renderNews(news.value);
    if (calendar.status === "fulfilled") renderCalendar(calendar.value);
    // A newer full refresh owns its diagnostic snapshot, even if this read fails.
    if (appState.healthSnapshot === healthSnapshotAtStart) {
      publishDiagnosticHealth(
        health.status === "fulfilled" ? health.value : {},
      );
    }
    if (researchTop10.status === "fulfilled") renderResearchTop10(researchTop10.value);
    else renderResearchTop10Unavailable();
    if (weeklyBrief.status === "fulfilled") renderWeeklyBrief(weeklyBrief.value);
    else renderWeeklyBrief({ status: "NOT_RUN", reason_codes: ["WEEKLY_BRIEF_UNAVAILABLE"] });
    if (afterHoursIndicative.status === "fulfilled") renderAfterHoursIndicative(afterHoursIndicative.value);
    if (fundamentals.status === "fulfilled") renderFundamentals(fundamentals.value);
    await refreshLearningSnapshot(
      news.status === "fulfilled" ? news.value : appState.newsSnapshot,
    );
  } finally {
    if (refreshed) markReadOnlyRefresh();
    appState.newsRefreshing = false;
  }
}

function publishDiagnosticHealth(health = {}) {
  const snapshot = health && typeof health === "object" && !Array.isArray(health)
    ? health
    : {};
  appState.healthSnapshot = snapshot;
  renderNewsBrokerHealth(snapshot);
  const truth = readinessTruthModel(
    appState.readinessSnapshot || {},
    {},
    appState.ranking || {},
    snapshot,
  );
  renderUpcomingExactSlots(truth.upcomingSlots);
  renderOverviewPriority();
  renderDailyFunnel(snapshot);
}

async function refreshLearningSnapshot(newsPayload = appState.newsSnapshot) {
  try {
    const learning = await fetchJson(ENDPOINTS.learning, { timeoutMs: FETCH_TIMEOUT_MS });
    renderLearning(learning, appState.advisorySnapshot, newsPayload || {});
    return true;
  } catch {
    return false;
  }
}

function renderFundamentals(payload) {
  const source = payload && typeof payload === "object" && !Array.isArray(payload)
    ? payload
    : {};
  appState.fundamentalsSnapshot = source;
  const rows = Array.isArray(source.rows) ? source.rows : [];
  const categories = source.categories && typeof source.categories === "object"
    ? source.categories
    : {};
  const available = Object.values(categories).filter((item) => item?.status === "AVAILABLE").length;
  const revisions = Array.isArray(source.revisions) ? source.revisions : [];
  const status = String(source.status || "UNAVAILABLE").toUpperCase();
  setText("fundamentals-status", `${status} · ${rows.length} 条 · ${available}/6 类`);
  setText(
    "fundamentals-summary",
    rows.length
      ? `截至 ${formatTime(source.as_of)} · 修订 ${numberOrNull(source.revision_count) ?? 0} · 首次观察时点生效`
      : "尚无可靠 point-in-time 基本面；缺失项保持 UNAVAILABLE。",
  );
  setSummaryCardState("fundamentals-status", status === "READY" ? "ready" : "blocked");
  const container = byId("fundamentals-list");
  if (!container) return;
  container.replaceChildren();
  if (!rows.length) {
    container.append(createElement("p", "empty-state", "暂无可验证的结构化基本面记录；不会从新闻文本猜测 EPS、指引或估值。"));
    return;
  }
  const categoryLine = createElement("p", "news-meta", Object.entries(categories)
    .map(([name, item]) => `${name} ${item?.status === "AVAILABLE" ? "✓" : "—"}`)
    .join(" · "));
  container.append(categoryLine);
  revisions.slice(0, 6).forEach((item) => {
    container.append(createElement(
      "p",
      "fundamental-revision",
      `修订 · ${item.symbol || "--"} ${item.metric || "--"} ${item.period_end || "--"} · ${item.previous_value ?? "--"} → ${item.current_value ?? "--"} (Δ ${item.delta ?? "--"}) · ${formatTime(item.observed_at)}`,
    ));
  });
  rows.slice(0, 24).forEach((item) => {
    const card = createElement("article", "fundamental-row");
    const title = createElement("div", "fundamental-row-head");
    title.append(createElement("strong", "", `${item.symbol || "--"} · ${item.metric || "--"}`));
    title.append(createElement("span", "authority-chip", "SUPPORTING_ONLY"));
    card.append(title);
    card.append(createElement("p", "fundamental-value", `${item.value ?? "--"} ${item.unit || ""}`.trim()));
    card.append(createElement("p", "news-meta", `${item.period_end || "--"} · ${item.fiscal_period || "--"} · ${item.source || "--"}/${item.form || "--"} · rev ${item.revision_number || 1}`));
    card.append(createElement("p", "news-meta", `${item.taxonomy || "--"}:${item.tag || "--"} · source ${item.source_id || "--"}`));
    card.append(createElement("p", "news-meta", `observed ${formatTime(item.observed_at)} · filed ${item.source_filed_date || "--"}`));
    container.append(card);
  });
}

function renderNews(payload) {
  appState.newsSnapshot = payload && typeof payload === "object" && !Array.isArray(payload)
    ? payload
    : {};
  appState.news = Array.isArray(payload.news) ? payload.news : [];
  renderNewsCapabilitySummary();
  renderOverviewPriority();
  const preMarketOptions = optionPoolRows(payload, "pre-market");
  const openRepricedOptions = optionPoolRows(payload, "open-repriced");
  const coverage = normalizePreselectionCoverage(
    payload.preselection_coverage,
    preMarketOptions.length,
    openRepricedOptions.length,
  );
  if (preselectionClientProjectionValid(
    preMarketOptions,
    openRepricedOptions,
    coverage,
    payload.preselection_coverage,
  )) {
    appState.preMarketOptions = preMarketOptions;
    appState.openRepricedOptions = openRepricedOptions;
    appState.preselectionCoverage = coverage;
  } else {
    appState.preMarketOptions = [];
    appState.openRepricedOptions = [];
    appState.preselectionCoverage = normalizePreselectionCoverage({
      source: PRESELECTION_LEDGER_SOURCE,
      status: "UNAVAILABLE",
      reason: "PRESELECTION_CLIENT_VALIDATION_FAILED",
      ledger_reason: "CLIENT_FAIL_CLOSED",
      open_observation_status: "UNAVAILABLE",
    }, 0, 0);
  }
  if (!appState.selectedNewsId || !appState.news.some((item) => item.id === appState.selectedNewsId)) {
    appState.selectedNewsId = appState.news[0]?.id ?? null;
  }
  const liveSourceDiagnostic = payload.source_status_scope === "LIVE_ACQUISITION_DIAGNOSTIC";
  renderProviderHealth("news-provider-health", payload.provider, liveSourceDiagnostic ? "新闻读模型" : "新闻");
  renderNewsSourceHealth(payload.source_health, payload.source_runtime);
  renderNewsPublication(payload);
  setText("news-research-pool-count", formatPoolCount(payload.research_pool_count, 10));
  setText("news-action-pool-count", formatPoolCount(payload.action_pool_count, 3));
  setText("option-pre-market-count", formatPoolCount(appState.preMarketOptions.length, 10));
  setText("option-open-repriced-count", formatPoolCount(appState.openRepricedOptions.length, 10));
  setText("option-action-observation-count", formatPoolCount(payload.option_action_pool_count, 3));
  renderNewsFilterOptions();
  renderNewsLists();
  renderOptionPreselections();
  renderNewsChronology();
  renderDeepSeekAdvisory(appState.advisorySnapshot || {}, appState.newsSnapshot);
  setText("last-news-refresh", formatTime(liveSourceDiagnostic ? payload.read_model_published_at : payload.provider?.asof || payload.asof));
}

function normalizeResearchTop10(payload) {
  const source = payload && typeof payload === "object" && !Array.isArray(payload)
    ? payload
    : {};
  const primaryStage = researchPrimaryStage(source);
  const authorityInvalid = (
    (source.decision_authority !== undefined && source.decision_authority !== "SUPPORTING_ONLY")
    || source.approval_eligible === true
    || source.instruction_creation_allowed === true
    || source.order_creation_allowed === true
    || source.order_submission_allowed === true
    || source.order_allowed === true
    || source.action_pool_eligible === true
    || (
      primaryStage !== null
      && (
        source.decision_authority !== "SUPPORTING_ONLY"
        || source.approval_eligible !== false
        || source.instruction_creation_allowed !== false
        || source.order_allowed !== false
        || source.action_pool_eligible !== false
      )
    )
  );
  const normalizedPremarket = authorityInvalid
    ? null
    : normalizeResearchTop10Stage(source, "pre-market");
  const normalizedOpenRepriced = authorityInvalid
    ? null
    : normalizeResearchTop10Stage(source, "open-repriced");
  const stageInvalid = normalizedPremarket === null || normalizedOpenRepriced === null;
  const premarket = stageInvalid ? [] : normalizedPremarket;
  const openRepriced = stageInvalid ? [] : normalizedOpenRepriced;
  const asof = firstValue(source, ["asof", "generated_at", "observed_at"]);
  const upstreamStatus = researchText(
    firstValue(source, ["status", "research_status", "producer_status"]),
    premarket.length || openRepriced.length ? "AVAILABLE" : "UNAVAILABLE",
    40,
  ).toUpperCase();
  return {
    decision: "NO_TRADE",
    authority: "SUPPORTING_ONLY",
    status: authorityInvalid || stageInvalid ? "UNAVAILABLE" : upstreamStatus,
    phase: researchText(source.phase, "", 40).toUpperCase(),
    asof,
    trading_date: researchDateKey(source.trading_date),
    available_count: Number.isInteger(source.available_count) ? source.available_count : premarket.length + openRepriced.length,
    target_count: Number.isInteger(source.target_count) && source.target_count > 0 ? source.target_count : 10,
    premarket,
    open_repriced: openRepriced,
  };
}

function researchPrimaryStage(source) {
  const phase = researchText(source?.phase, "", 40).toUpperCase();
  if (phase === "PREMARKET_RESEARCH") return "pre-market";
  if (["INDICATIVE_REPRICE", "INTRADAY_RECOVERY"].includes(phase)) return "open-repriced";
  return null;
}

function normalizeResearchTop10Stage(source, stage) {
  const expectedPhase = stage === "pre-market" ? "PRE_MARKET" : "OPEN_REPRICED";
  const envelopePhase = stage === "pre-market"
    ? "PREMARKET_RESEARCH"
    : source.phase === "INTRADAY_RECOVERY" ? "INTRADAY_RECOVERY" : "INDICATIVE_REPRICE";
  const stageEnvelope = Array.isArray(source.stages)
    ? source.stages.find((item) => item?.phase === envelopePhase)
    : null;
  const stageObservedAt = firstValue(
    stageEnvelope,
    ["observed_at"],
    firstValue(source, ["observed_at"]),
  );
  const stageTradingDate = researchDateKey(firstValue(stageEnvelope, ["trading_date"], source.trading_date));
  const keys = stage === "pre-market"
    ? ["premarket", "pre_market", "pre_market_preselections", "research_top10"]
    : ["open_repriced", "open_market_repriced", "open_reprice", "repriced_top10"];
  let rawRows = null;
  for (const key of keys) {
    if (Array.isArray(source[key])) {
      rawRows = source[key];
      break;
    }
  }
  if (
    rawRows === null
    && researchPrimaryStage(source) === stage
    && Array.isArray(source.candidates)
  ) rawRows = source.candidates;
  if (rawRows === null && Array.isArray(source.items)) {
    rawRows = source.items.filter((item) => item?.phase === expectedPhase);
  }
  if (rawRows === null) return [];
  if (!Array.isArray(rawRows) || rawRows.length > 10) return null;

  const rows = [];
  const identifiers = new Set();
  const ranks = new Set();
  for (const raw of rawRows) {
    const row = normalizeResearchTop10Row(raw, source, expectedPhase, stageObservedAt, stageTradingDate);
    if (!row || identifiers.has(row.id) || ranks.has(row.rank)) return null;
    identifiers.add(row.id);
    ranks.add(row.rank);
    rows.push(row);
  }
  return rows.sort((left, right) => left.rank - right.rank);
}

function normalizeResearchTop10Row(raw, source, expectedPhase, stageObservedAt, stageTradingDate) {
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) return null;
  const primary = researchPrimaryStage(source) !== null;
  const authority = primary
    ? raw.decision_authority
    : firstValue(raw, ["decision_authority"], source.decision_authority);
  const approvalEligible = primary
    ? raw.approval_eligible
    : firstValue(raw, ["approval_eligible"], source.approval_eligible);
  const instructionAllowed = primary
    ? raw.instruction_creation_allowed
    : firstValue(raw, ["instruction_creation_allowed"], source.instruction_creation_allowed);
  const orderAllowed = primary
    ? raw.order_allowed
    : firstValue(
      raw,
      ["order_allowed", "order_creation_allowed", "order_submission_allowed"],
      firstValue(source, ["order_allowed", "order_creation_allowed", "order_submission_allowed"]),
    );
  const actionPoolEligible = primary
    ? raw.action_pool_eligible
    : firstValue(raw, ["action_pool_eligible"], source.action_pool_eligible ?? false);
  if (
    authority !== "SUPPORTING_ONLY"
    || approvalEligible !== false
    || instructionAllowed !== false
    || orderAllowed !== false
    || (primary && actionPoolEligible !== false)
  ) return null;

  const phase = researchText(raw.phase, expectedPhase, 32).toUpperCase();
  if (phase !== expectedPhase) return null;
  const rankKeys = expectedPhase === "PRE_MARKET"
    ? ["research_rank", "premarket_rank", "rank"]
    : ["repriced_rank", "open_rank", "rank"];
  const rank = numberOrNull(firstValue(raw, rankKeys));
  if (!Number.isInteger(rank) || rank < 1 || rank > 10) return null;
  const id = researchText(
    firstValue(raw, ["research_id", "preselection_id", "candidate_id", "id"]),
    "",
    160,
  );
  const symbol = researchText(firstValue(raw, ["symbol", "underlying"]), "--", 24).toUpperCase();
  const strategy = researchText(firstValue(raw, ["strategy", "strategy_type", "structure"]), "UNSPECIFIED", 80);
  const declaredDirection = researchText(firstValue(raw, ["direction", "bias", "outlook"]), "UNSPECIFIED", 32).toUpperCase();
  const strategyDirection = strategy.toUpperCase().includes("BULL")
    ? "BULLISH"
    : strategy.toUpperCase().includes("BEAR")
      ? "BEARISH"
      : "UNSPECIFIED";
  const direction = declaredDirection === "UNSPECIFIED" ? strategyDirection : declaredDirection;
  const rawLegs = Array.isArray(raw.legs) ? raw.legs : [];
  const legs = normalizeResearchLegs(rawLegs);
  const blockers = normalizeResearchBlockers(firstValue(raw, ["blockers", "reason_codes", "reasons"], []));
  const quoteUnavailable = legs.length === 0 || legs.some(
    (leg) => leg.bid === null || leg.ask === null,
  );
  const quoteContractInvalid = (
    legs.length !== rawLegs.length
    || legs.length !== 2
    || legs.some((leg) => (
      (leg.bid === null) !== (leg.ask === null)
      || (
        leg.bid !== null
        && leg.ask !== null
        && (
          leg.bid < 0
          || leg.ask <= 0
          || leg.bid > leg.ask
          || leg.blockers.includes("QUOTE_UNAVAILABLE")
        )
      )
    ))
    || (!quoteUnavailable && blockers.includes("QUOTE_UNAVAILABLE"))
  );
  if (primary && quoteContractInvalid) return null;
  const intradayRecovery = researchText(source?.phase, "", 40).toUpperCase() === "INTRADAY_RECOVERY";
  if (primary && expectedPhase === "OPEN_REPRICED" && quoteUnavailable && !intradayRecovery) return null;
  const expiry = researchText(
    firstValue(raw, ["expiry", "expiration", "expiration_date"], legs[0]?.expiry),
    "--",
    32,
  );
  const indicativeDebit = numberOrNull(firstValue(raw, [
    "indicative_debit_usd",
    "indicative_entry_debit_usd",
    "entry_debit_usd",
    "debit_usd",
    "estimated_debit_usd",
    "net_debit_usd",
    "estimated_cost_usd",
  ]));
  const maximumLoss = numberOrNull(firstValue(raw, ["maximum_loss_usd", "max_loss_usd"]));
  const indicativeMaximumLoss = numberOrNull(raw.indicative_maximum_loss_usd);
  const indicativeAfterCostEv = numberOrNull(firstValue(raw, [
    "indicative_cost_after_ev_usd",
    "cost_after_ev_usd",
  ]));
  const assumedMultiplier = numberOrNull(raw.assumed_multiplier);
  const dte = numberOrNull(raw.dte);
  const catalyst = researchText(
    firstValue(raw, ["news_catalyst", "catalyst", "catalyst_summary", "research_summary"]),
    "未提供新闻催化",
    360,
  );
  const legQuoteAsof = firstResearchLegValue(rawLegs, ["quote_asof"]);
  const quoteAsof = firstValue(raw, [
    "quote_asof",
    "oldest_quote_asof",
    "economics_quote_asof",
  ], legQuoteAsof);
  const legCollectedAt = firstResearchLegValue(rawLegs, ["collected_at"]);
  const collectedAt = firstValue(raw, ["collected_at"], legCollectedAt);
  const batchObservedAt = stageObservedAt;
  legs.flatMap((leg) => leg.blockers).forEach((reason) => blockers.push(reason));
  if (!id || symbol === "--" || strategy === "UNSPECIFIED" || expiry === "--" || legs.length === 0) {
    blockers.push("RESEARCH_FIELDS_INCOMPLETE");
  }
  if (indicativeDebit === null) blockers.push("INDICATIVE_DEBIT_UNAVAILABLE");
  if (maximumLoss === null) blockers.push("MAXIMUM_LOSS_UNAVAILABLE");
  if (!quoteAsof) blockers.push("QUOTE_EXCHANGE_TIMESTAMP_UNAVAILABLE");

  return {
    id,
    rank,
    symbol,
    direction,
    strategy,
    expiry,
    legs,
    indicative_debit_usd: indicativeDebit,
    maximum_loss_usd: maximumLoss,
    indicative_maximum_loss_usd: indicativeMaximumLoss,
    indicative_after_cost_ev_usd: indicativeAfterCostEv,
    quote_status: quoteUnavailable ? "UNAVAILABLE" : "AVAILABLE",
    ev_status: indicativeAfterCostEv === null ? "UNAVAILABLE" : "AVAILABLE",
    assumed_multiplier: assumedMultiplier,
    dte,
    news_catalyst: catalyst,
    entry_condition: researchText(raw.entry_condition, "未提供入场条件", 360),
    invalidation_condition: researchText(raw.invalidation_condition, "未提供反证条件", 360),
    profit_target_condition: researchText(raw.profit_target_condition, "未提供止盈条件", 360),
    stop_loss_condition: researchText(raw.stop_loss_condition, "未提供止损条件", 360),
    quote_asof: quoteAsof,
    collected_at: collectedAt,
    batch_observed_at: batchObservedAt,
    trading_date: stageTradingDate,
    time_label: quoteAsof
      ? "行情时间"
      : collectedAt
        ? "逐腿采集完成时间"
        : "时间不可用",
    blockers: [...new Set(blockers)],
    phase,
    intraday_recovery: intradayRecovery,
  };
}

function researchDateKey(value) {
  if (typeof value !== "string" || !/^\d{4}-\d{2}-\d{2}$/.test(value)) return null;
  const timestamp = Date.parse(`${value}T00:00:00Z`);
  return Number.isFinite(timestamp) && new Date(timestamp).toISOString().slice(0, 10) === value
    ? value
    : null;
}

function researchFreshness(item, now = new Date()) {
  const today = dateKeyInZone(now, NEW_YORK_TIME_ZONE);
  const observedAt = item?.batch_observed_at || item?.asof || item?.observed_at;
  const batchDate = researchDateKey(item?.trading_date)
    || (observedAt ? dateKeyInZone(observedAt, NEW_YORK_TIME_ZONE) : null);
  const expiry = researchDateKey(item?.expiry || item?.expiration);
  const currentDte = today && expiry
    ? Math.round((Date.parse(`${expiry}T00:00:00Z`) - Date.parse(`${today}T00:00:00Z`)) / 86_400_000)
    : null;
  return {
    today,
    batch_date: batchDate,
    historical: Boolean(today && batchDate && batchDate < today),
    future_dated: Boolean(today && batchDate && batchDate > today),
    expired: currentDte !== null && currentDte < 0,
    current_dte: currentDte,
  };
}

function researchFreshnessText(freshness) {
  const batch = freshness.historical
    ? `历史批次 ${freshness.batch_date}`
    : freshness.future_dated
      ? `未来日期批次 ${freshness.batch_date} · 日期异常`
      : freshness.batch_date ? `研究交易日 ${freshness.batch_date}` : "研究交易日未验证";
  return freshness.expired
    ? `${batch} · 合约已到期，仅供历史查看；需重新发现有效到期日的结构。`
    : freshness.historical
      ? `${batch} · 仅供历史查看，须重新完成当日研究与行情、风险验证。`
      : `${batch} · 当前纽约日期 ${freshness.today || "未验证"}`;
}

function researchDteText(item, freshness) {
  return `当前 DTE ${formatInteger(freshness.current_dte)} · 批次时 DTE ${formatInteger(item.dte)}`;
}

function normalizeResearchLegs(value) {
  if (!Array.isArray(value) || value.length > 8) return [];
  return value.map((raw) => {
    if (!raw || typeof raw !== "object" || Array.isArray(raw)) return null;
    const ratio = numberOrNull(firstValue(raw, ["ratio", "quantity"]));
    if (ratio !== null && ratio <= 0) return null;
    return {
      side: researchText(raw.side, "--", 12).toUpperCase(),
      ratio,
      expiry: researchText(firstValue(raw, ["expiry", "expiration"]), "--", 32),
      strike: researchText(raw.strike, "--", 32),
      right: researchText(firstValue(raw, ["right", "option_type"]), "--", 16).toUpperCase(),
      con_id: firstValue(raw, ["con_id", "conId"]),
      local_symbol: researchText(raw.local_symbol, "", 80),
      trading_class: researchText(raw.trading_class, "", 40),
      exchange: researchText(raw.exchange, "", 40),
      multiplier: numberOrNull(raw.multiplier),
      bid: numberOrNull(raw.bid),
      ask: numberOrNull(raw.ask),
      quote_asof: firstValue(raw, ["quote_asof"]),
      collected_at: firstValue(raw, ["collected_at"]),
      implied_volatility: numberOrNull(firstValue(raw, ["implied_volatility", "iv"])),
      volume: numberOrNull(raw.volume),
      open_interest: numberOrNull(firstValue(raw, ["open_interest", "oi"])),
      blockers: normalizeResearchBlockers(raw.blockers),
    };
  }).filter(Boolean);
}

function firstResearchLegValue(legs, keys) {
  for (const leg of legs) {
    const value = firstValue(leg, keys);
    if (value !== null && value !== undefined && value !== "") return value;
  }
  return null;
}

function normalizeResearchBlockers(value) {
  const rows = Array.isArray(value) ? value : value ? [value] : [];
  return rows.map((item) => researchText(item, "UNKNOWN_BLOCKER", 160));
}

function researchText(value, fallback = "--", maximumLength = 200) {
  if (value === null || value === undefined) return fallback;
  const text = String(value).trim();
  if (!text) return fallback;
  return text.slice(0, maximumLength);
}

function renderWeeklyBrief(payload) {
  const source = payload && typeof payload === "object" && !Array.isArray(payload)
    ? payload
    : {};
  const provisional = source.status === "PROVISIONAL"
    && source.decision === "OBSERVATION_ONLY"
    && source.decision_authority === "SUPPORTING_ONLY"
    && source.execution_allowed === false
    && source.review_allowed === false
    && source.combination_generation_allowed === false;
  appState.weeklyBrief = provisional ? source : null;
  const statusNode = byId("weekly-brief-status");
  if (statusNode) {
    statusNode.className = `status-chip ${provisional ? "status-stale" : "status-no-trade"}`;
    statusNode.textContent = provisional ? "PROVISIONAL" : "NOT_RUN";
  }
  const watches = provisional && Array.isArray(source.watch_items)
    ? source.watch_items.slice(0, 10)
    : [];
  setText("weekly-brief-count", `${watches.length}/10`);
  setText("weekly-brief-cutoff", provisional ? formatTime(source.cutoff_at) : "--");
  setText("weekly-brief-hash", `hash ${provisional ? shortHash(source.content_hash) : "--"}`);
  const reasons = provisional
    ? weeklySourceSummary(source.source_health)
    : (Array.isArray(source.reason_codes) ? source.reason_codes : ["WEEKLY_BRIEF_NOT_AVAILABLE"]);
  setText(
    "weekly-brief-reasons",
    reasons.join(" · ") || "周报已生成；全部内容仍为 SUPPORTING_ONLY。",
  );
  renderWeeklyBriefItems(
    "weekly-prior-items",
    provisional ? source.prior_week_items : [],
    "暂无可验证的上周 point-in-time 证据。",
  );
  renderWeeklyBriefItems(
    "weekly-upcoming-items",
    provisional ? source.upcoming_items : [],
    "未来 1–7 天暂无可验证事件。",
  );
  renderWeeklyBriefItems(
    "weekly-next-items",
    provisional ? source.next_preview_items : [],
    "未来 8–14 天暂无可验证事件。",
  );
  renderWeeklyWatches(watches);
}

function weeklySourceSummary(value) {
  if (!Array.isArray(value)) return ["WEEKLY_BRIEF_SOURCE_HEALTH_UNAVAILABLE"];
  return value.map((item) => {
    const source = researchText(item?.source, "UNKNOWN_SOURCE", 80);
    const status = researchText(item?.status, "UNAVAILABLE", 24);
    const mandatory = item?.mandatory === true ? "MANDATORY" : "OPTIONAL";
    const reasons = Array.isArray(item?.reason_codes) ? item.reason_codes.join(",") : "";
    return `${source} ${status} ${mandatory}${reasons ? ` ${reasons}` : ""}`;
  });
}

function renderWeeklyBriefItems(targetId, value, emptyMessage) {
  const container = byId(targetId);
  if (!container) return;
  container.replaceChildren();
  const rows = Array.isArray(value)
    ? [...value].sort((left, right) => (
      (numberOrNull(right?.event_impact_score) ?? calendarImpactValue(right))
      - (numberOrNull(left?.event_impact_score) ?? calendarImpactValue(left))
      || String(left?.occurred_at || "").localeCompare(String(right?.occurred_at || ""))
    )).slice(0, 200)
    : [];
  if (rows.length === 0) {
    container.append(createElement("p", "empty-state", emptyMessage));
    return;
  }
  rows.forEach((item) => {
    const card = createElement("article", "weekly-evidence-card");
    card.append(createElement("strong", "", researchText(item?.headline, "未命名证据", 400)));
    const symbols = Array.isArray(item?.symbols) ? item.symbols.join(" / ") : "MARKET";
    card.append(createElement(
      "p",
      "news-meta",
      `${researchText(item?.source, "UNKNOWN", 120)} · ${symbols || "MARKET"} · ${researchText(item?.direction, "UNKNOWN", 32)} · ${formatTime(item?.occurred_at)}`,
    ));
    card.append(createElement("p", "", researchText(item?.summary, "摘要不可用", 1600)));
    if (item?.deepseek_summary) {
      card.append(createElement(
        "p",
        "weekly-deepseek-note",
        `DeepSeek SUPPORTING_ONLY · ${researchText(item.deepseek_summary, "", 1600)}`,
      ));
    }
    container.append(card);
  });
}

function renderWeeklyWatches(value) {
  const container = byId("weekly-watch-list");
  if (!container) return;
  container.replaceChildren();
  if (!Array.isArray(value) || value.length === 0) {
    container.append(createElement("p", "empty-state", "没有为凑数而生成观察项；保持 0/10。"));
    return;
  }
  value.forEach((watch, index) => {
    const card = createElement("article", "weekly-watch-card");
    card.append(createElement("strong", "", `#${index + 1} · ${researchText(watch?.symbol, "UNKNOWN", 16)}`));
    card.append(createElement("p", "news-meta", "PROVISIONAL · OBSERVATION_ONLY · SUPPORTING_ONLY"));
    const gates = createElement("div", "weekly-gate-grid");
    const layers = Array.isArray(watch?.layers) ? watch.layers : [];
    layers.forEach((layer) => {
      const gate = createElement("span", `weekly-gate weekly-gate-${String(layer?.status || "UNAVAILABLE").toLowerCase()}`);
      gate.textContent = `${researchText(layer?.gate_id, "GATE", 80)} · ${researchText(layer?.status, "UNAVAILABLE", 24)}`;
      gates.append(gate);
    });
    card.append(gates);
    card.append(createElement("p", "weekly-watch-boundary", "option chain / quotes / NAV / positioning 未绑定；不可审批、不可创建指令。"));
    container.append(card);
  });
}

function renderResearchTop10(payload) {
  appState.researchTop10 = normalizeResearchTop10(payload);
  const primaryStage = researchPrimaryStage(payload);
  if (primaryStage !== null) appState.researchTop10Stage = primaryStage;
  renderResearchTop10Stage();
  renderOverviewResearchFallback();
  renderOverviewPriority();
}

function normalizeOptionStructurePool(payload) {
  const decisions = Array.isArray(payload?.decisions)
    ? payload.decisions.map((item) => {
      const economics = item?.economics && typeof item.economics === "object"
        ? item.economics
        : null;
      return {
        underlying: researchText(item?.underlying, "--", 16),
        thesisClass: researchText(item?.thesis_class, "UNCERTAIN", 48),
        direction: researchText(item?.direction_label, "UNCERTAIN", 32),
        directionScore: numberOrNull(item?.direction_score),
        uncertainty: numberOrNull(item?.uncertainty),
        structure: researchText(item?.structure, "UNKNOWN", 48),
        disposition: researchText(item?.disposition, "RESEARCH_ONLY", 48),
        reasonCodes: Array.isArray(item?.reason_codes)
          ? item.reason_codes.map((reason) => researchText(reason, "UNKNOWN", 120))
          : [],
        candidateId: researchText(item?.candidate_id, "", 160),
        candidateHash: researchText(item?.candidate_hash, "", 64),
        equityThesisHash: normalizeSha256Hash(item?.equity_thesis_hash),
        quoteAgeSeconds: numberOrNull(item?.quote_age_seconds),
        freshnessDegraded: item?.freshness_degraded === true,
        economics,
      };
    })
    : [];
  decisions.sort((left, right) => (
    Number(Boolean(right.candidateId)) - Number(Boolean(left.candidateId))
    || Number(right.disposition === "EXACT_EVIDENCE_CAPTURED")
      - Number(left.disposition === "EXACT_EVIDENCE_CAPTURED")
    || left.underlying.localeCompare(right.underlying)
    || left.structure.localeCompare(right.structure)
  ));
  return {
    status: researchText(payload?.status, "UNAVAILABLE", 40),
    decision: researchText(payload?.decision, "NO_TRADE", 40),
    observedAt: payload?.observed_at || null,
    exactCount: Number.isInteger(payload?.exact_count) ? payload.exact_count : 0,
    researchOnlyCount: Number.isInteger(payload?.research_only_count)
      ? payload.research_only_count
      : 0,
    excludedCount: Number.isInteger(payload?.excluded_count) ? payload.excluded_count : 0,
    generationReasons: Array.isArray(payload?.generation_reason_codes)
      ? payload.generation_reason_codes.map((reason) => researchText(reason, "UNKNOWN", 120))
      : [],
    decisions,
  };
}

function renderEquityPool(payload) {
  const selected = Array.isArray(payload?.selected) ? payload.selected.slice(0, 30) : [];
  const excluded = Array.isArray(payload?.excluded) ? payload.excluded.slice(0, 150) : [];
  const status = researchText(payload?.status, "UNAVAILABLE", 40).toUpperCase();
  const decision = researchText(payload?.decision, "RESEARCH_ONLY", 40).toUpperCase();
  appState.equityPool = payload;
  setText("equity-pool-status", `${decision} · ${status}`);
  setText(
    "equity-pool-counts",
    `发现 ${formatInteger(payload?.discovery_count)} · 深评 ${formatInteger(payload?.considered_count)} · 入池 ${selected.length} · 排除 ${excluded.length}`,
  );
  setText("equity-pool-asof", formatTime(payload?.slot));
  const concentration = payload?.concentration_counts && typeof payload.concentration_counts === "object"
    ? Object.entries(payload.concentration_counts)
      .sort((left, right) => Number(right[1]) - Number(left[1]))
      .slice(0, 8)
      .map(([name, count]) => `${name} ${count}`)
      .join(" · ")
    : "";
  setText(
    "equity-pool-summary",
    selected.length > 0
      ? `行业/集中度：${concentration || "已执行行业平衡"}。方向仅为股票 thesis，不能直接当作期权开仓信号。`
      : operatorReasonList(payload?.reason_codes).join(" · ") || "当前没有合格股票研究标的；不会用大型科技股或虚构标的补足。",
  );
  const container = byId("equity-pool-list");
  if (!container) return;
  container.replaceChildren();
  if (selected.length === 0) {
    container.append(createElement("p", "empty-state", "股票池尚无可复现 selected 记录；保持 RESEARCH_ONLY。"));
  }
  selected.forEach((item) => {
    const score = item?.score && typeof item.score === "object" ? item.score : {};
    const classification = item?.classification && typeof item.classification === "object"
      ? item.classification
      : {};
    const reasons = operatorReasonList(item?.reasons);
    const card = createElement("article", "equity-pool-card");
    const heading = createElement("div", "research-top10-card-heading");
    heading.append(
      createElement("strong", "", `#${formatInteger(item?.selected_rank)} · ${researchText(item?.symbol, "--", 16)}`),
      createElement("span", "authority-chip", "RESEARCH_ONLY"),
    );
    card.append(
      heading,
      createElement(
        "p",
        "research-top10-metrics",
        `${researchText(classification.category, "UNCLASSIFIED", 80)} · ${researchText(score.direction_label, "UNCERTAIN", 40)} · 机会 ${formatQuote(score.opportunity_score)} · 不确定度 ${formatQuote(score.uncertainty)} · 流动性 ${formatQuote(score.liquidity_score)}`,
      ),
      createElement("p", reasons.length ? "option-structure-reasons has-blocker" : "option-structure-reasons", reasons.join(" · ") || "股票研究证据完整；等待期权结构与全部 Gate。"),
    );
    container.append(card);
  });
  const exclusion = byId("equity-pool-exclusions");
  if (exclusion) {
    const counts = new Map();
    excluded.forEach((item) => {
      const reason = operatorReasonList(item?.reasons)[0] || researchText(item?.disposition, "EXCLUDED", 48);
      counts.set(reason, (counts.get(reason) || 0) + 1);
    });
    exclusion.textContent = counts.size > 0
      ? `主要排除：${[...counts.entries()].sort((a, b) => b[1] - a[1]).slice(0, 6).map(([reason, count]) => `${reason} ×${count}`).join(" · ")}`
      : "当前没有排除记录。";
  }
}

function renderOptionStructurePool(payload) {
  const snapshot = normalizeOptionStructurePool(payload);
  appState.optionStructurePool = snapshot;
  setText("option-structure-pool-status", `${snapshot.decision} · ${snapshot.status}`);
  setText(
    "option-structure-pool-counts",
    `精确证据 ${snapshot.exactCount} · 研究 ${snapshot.researchOnlyCount} · 排除 ${snapshot.excludedCount}`,
  );
  setText("option-structure-pool-asof", formatTime(snapshot.observedAt));
  const candidateRows = snapshot.decisions.filter((item) => item.candidateId).slice(0, 30);
  const templateRows = snapshot.decisions.filter((item) => !item.candidateId);
  setText(
    "option-structure-pool-summary",
    candidateRows.length
      ? `显示 ${candidateRows.length} 个真实合约身份；另有 ${templateRows.length} 个模板 disposition。池本身只有研究权限，最终建议仍须通过全部 Gate。`
      : snapshot.generationReasons.length
        ? `本轮没有真实合约候选：${snapshot.generationReasons.slice(0, 4).join(" · ")}`
        : "当前没有真实合约候选；模板处置仍保留，系统不会用虚构价格补足。",
  );
  const container = byId("option-structure-pool-list");
  if (!container) return;
  container.replaceChildren();
  if (candidateRows.length === 0) {
    container.append(createElement("p", "empty-state", "等待股票研究池方向与逐腿期权证据进入结构池；当前保持 NO_TRADE。"));
    return;
  }
  candidateRows.forEach((item) => container.append(buildOptionStructurePoolCard(item)));
  if (appState.ranking) renderJointResearchWatchlist(appState.ranking);
}

function buildOptionStructurePoolCard(item) {
  const card = createElement("article", "option-structure-pool-card");
  const heading = createElement("div", "research-top10-card-heading");
  const identity = createElement("div", "research-top10-identity");
  identity.append(
    createElement("strong", "", item.underlying),
    createElement("span", `research-direction ${researchDirectionClass(item.direction)}`, item.direction),
    createElement("span", "authority-chip", item.disposition),
  );
  heading.append(identity, createElement("span", "status-chip status-no-trade", "RESEARCH_ONLY"));
  const economics = item.economics || {};
  const metrics = createElement("div", "research-top10-metrics");
  [
    ["结构", item.structure],
    ["DTE", formatInteger(economics.dte)],
    ["最大亏损", formatMoney(economics.max_loss_usd)],
    ["成本后 EV", formatMoney(economics.after_cost_ev_usd)],
    ["总成本", formatMoney(economics.all_in_cost_usd)],
    ["报价年龄", item.quoteAgeSeconds === null ? "--" : `${formatQuote(item.quoteAgeSeconds)}s`],
  ].forEach(([label, value]) => {
    const metric = createElement("div", "", label);
    metric.append(createElement("strong", "", value));
    metrics.append(metric);
  });
  const legs = Array.isArray(economics.legs) ? economics.legs : [];
  const legList = createElement("div", "option-structure-leg-list");
  legs.forEach((leg) => {
    legList.append(createElement(
      "p",
      "research-top10-legs",
      `${leg.side || "--"} ${formatInteger(leg.ratio)}x ${leg.expiration || "--"} ${formatQuote(leg.strike)} ${leg.right || "--"} · bid ${formatQuote(leg.bid)} / ask ${formatQuote(leg.ask)} · Δ ${formatQuote(leg.delta)} · Vol ${formatInteger(leg.volume)} · OI ${formatInteger(leg.open_interest)} · ${leg.liquidity_status || "LIQUIDITY --"}`,
    ));
  });
  const reasons = createElement(
    "p",
    item.reasonCodes.length ? "option-structure-reasons has-blocker" : "option-structure-reasons",
    item.reasonCodes.join(" · ") || "FRESH_EXACT_OPTION_EVIDENCE_CAPTURED",
  );
  const thesisBoundary = optionStructureThesisBoundary(item);
  card.append(
    heading,
    metrics,
    legList,
    reasons,
    createElement("p", "research-top10-boundary", thesisBoundary),
  );
  return card;
}

function normalizeSha256Hash(value) {
  const normalized = typeof value === "string" ? value.trim().toLowerCase() : "";
  return /^[0-9a-f]{64}$/.test(normalized) ? normalized : "";
}

function optionStructureThesisBoundary(item) {
  const reasons = Array.isArray(item?.reasonCodes) ? item.reasonCodes : [];
  const thesisBound = Boolean(item?.equityThesisHash)
    && !reasons.includes("EQUITY_THESIS_EVIDENCE_UNAVAILABLE");
  return thesisBound
    ? "股票 thesis 已绑定 · SUPPORTING_ONLY · 不可审批 · 不可创建指令 · NO_TRADE"
    : "股票 thesis 未绑定 · 仅保留结构研究 · SUPPORTING_ONLY · 不可审批 · 不可创建指令 · NO_TRADE";
}

function recommendationGateOpen(ranking, candidates = groupRankedCandidates(ranking || {})) {
  return String(ranking?.decision || "").toUpperCase() === "CANDIDATES_AVAILABLE"
    && ranking?.recommendations_available === true
    && candidates.length > 0
    && candidates.every((candidate) => candidate.recommendation_ready === true);
}

function renderResearchTop10Unavailable() {
  appState.researchTop10 = normalizeResearchTop10({
    status: "UNAVAILABLE",
    decision_authority: "SUPPORTING_ONLY",
    approval_eligible: false,
    instruction_creation_allowed: false,
    order_creation_allowed: false,
    premarket: [],
    open_repriced: [],
  });
  renderResearchTop10Stage();
  renderOverviewResearchFallback();
  renderOverviewPriority();
}

function overviewResearchRows(ranking, researchTop10) {
  const actionable = groupRankedCandidates(ranking || {});
  const actionGateOpen = recommendationGateOpen(ranking, actionable);
  if (actionGateOpen || !researchTop10) return [];
  const stage = researchTop10.open_repriced?.length ? "open_repriced" : "premarket";
  const rows = Array.isArray(researchTop10[stage]) ? researchTop10[stage] : [];
  return rows.slice(0, MAX_CANDIDATES);
}

function overviewResearchAvailability(researchTop10, afterHoursIndicative) {
  const regularRows = researchTop10
    ? (researchTop10.open_repriced?.length
      ? researchTop10.open_repriced
      : researchTop10.premarket)
    : [];
  const afterHoursRows = Array.isArray(afterHoursIndicative?.candidates)
    ? afterHoursIndicative.candidates.slice(0, MAX_CANDIDATES)
    : [];
  return {
    regularCount: Math.min(regularRows.length, MAX_CANDIDATES),
    afterHoursCount: afterHoursRows.length,
    regularAsOf: regularRows[0]?.batch_observed_at || researchTop10?.asof || null,
    afterHoursAsOf: afterHoursIndicative?.observed_at || null,
  };
}

function renderOverviewResearchFallback() {
  const panel = byId("overview-research-fallback");
  const container = byId("overview-research-list");
  if (!panel || !container) return;
  const rows = overviewResearchRows(appState.ranking, appState.researchTop10);
  panel.hidden = rows.length === 0;
  setText("overview-research-count", `${rows.length} / ${MAX_CANDIDATES}`);
  container.replaceChildren();
  rows.forEach((item) => container.append(buildOverviewResearchCard(item)));
}

function setSummaryCardState(targetId, state) {
  const node = byId(targetId);
  const card = node?.closest?.("article");
  if (card) card.dataset.state = state;
}

function renderOverviewPriority() {
  const ranking = appState.ranking || {};
  const positionTruth = positionManagementTruth(appState.positionsSnapshot, ranking);
  const grouped = groupRankedCandidates(ranking);
  const actionGateOpen = recommendationGateOpen(ranking, grouped);
  const actionCount = actionGateOpen
    ? grouped.reduce((count, candidate) => count + 1 + candidate.alternatives.length, 0)
    : 0;
  const featureChain = featureDataChainTruth(appState.readinessSnapshot || {});
  setText("overview-action-state", `${actionCount} / ${MAX_CANDIDATES}`);
  setText(
    "overview-action-summary",
    featureChain.incomplete
      ? featureChain.summary
      : actionCount > 0
      ? ranking.approval_enabled === true
        ? "六道 Gate 与联合 ranking 已通过；仍只允许人工审核和双确认。"
        : "六道 Gate 与联合 ranking 已通过；当前显示 review-only 建议，Creator/挑战不可用且不会创建指令。"
      : overviewBlockedActionSummary(
        ranking,
        appState.healthSnapshot || {},
        positionTruth,
        appState.readinessSnapshot || {},
      ),
  );
  setSummaryCardState("overview-action-state", !featureChain.incomplete && actionCount > 0 ? "ready" : "blocked");

  const research = overviewResearchAvailability(
    appState.researchTop10,
    appState.afterHoursIndicative,
  );
  const hasRegularResearch = research.regularCount > 0;
  const hasAfterHoursResearch = research.afterHoursCount > 0;
  const researchDates = researchFreshness(appState.researchTop10);
  const historicalResearch = hasRegularResearch && researchDates.historical;
  setText(
    "overview-research-state",
    historicalResearch
      ? `历史研究 ${research.regularCount} 条${hasAfterHoursResearch ? ` · ${research.afterHoursCount} 收盘` : ""}`
      : hasAfterHoursResearch
      ? `${research.regularCount} 正常时段 · ${research.afterHoursCount} 收盘`
      : `${research.regularCount} / ${MAX_CANDIDATES}`,
  );
  setText(
    "overview-research-summary",
    historicalResearch
      ? `${researchFreshnessText(researchDates)}${hasAfterHoursResearch ? ` 另有收盘只读记录 ${research.afterHoursCount} 条（${formatTime(research.afterHoursAsOf)}）。` : ""}`
      : hasRegularResearch && hasAfterHoursResearch
      ? `正常时段研究 ${research.regularCount} 条（${formatTime(research.regularAsOf)}）；另有收盘只读结构 ${research.afterHoursCount} 条（${formatTime(research.afterHoursAsOf)}）。两者都仍需下一正常时段的新鲜逐腿行情。`
      : hasRegularResearch
        ? `正常时段真实结构 ${research.regularCount} 条；${formatTime(research.regularAsOf)}，仍需新鲜逐腿行情。`
        : hasAfterHoursResearch
          ? `收盘只读结构 ${research.afterHoursCount} 条；${formatTime(research.afterHoursAsOf)}，固定 NO_TRADE，等待下一正常时段重报价。`
          : "尚无可验证研究结构；不会用虚构候选补足。",
  );
  setSummaryCardState(
    "overview-research-state",
    hasRegularResearch || hasAfterHoursResearch ? "supporting" : "degraded",
  );

  const newsRows = Array.isArray(appState.newsSnapshot?.news)
    ? appState.newsSnapshot.news
    : [];
  const newsRestore = analysisBackfillState(appState.newsSnapshot);
  const coverage = classifierCoverage(newsRows);
  const durableShadow = durableShadowCounts(appState.learningSnapshot);
  const durablePredictions = durableShadowCountText(durableShadow, "predictions");
  const durableOutcomes = durableShadowCountText(durableShadow, "outcomes");
  setText(
    "overview-news-state",
    newsRestore.restoring
      ? `历史校验 ${formatInteger(newsRestore.verifiedRows)}/${formatInteger(newsRestore.totalRows)}`
      : newsRows.length
      ? `${newsRows.length} 条 · 当前 DS ${coverage.deepseekShadow} · 累计预测 ${durablePredictions}`
      : "等待数据",
  );
  setText(
    "overview-news-summary",
    newsRestore.restoring
      ? "采集仍在运行；追加式分析账本校验完成前暂不投影历史新闻或分类结果。"
      : newsRows.length
      ? `生产规则 ${coverage.deterministic}/${coverage.total}；当前 DeepSeek shadow ${coverage.deepseekShadow}/${coverage.total}；durable ledger 已结算 outcome ${durableOutcomes}。`
      : "尚未取得新闻与分类覆盖。",
  );
  setSummaryCardState(
    "overview-news-state",
    newsRestore.restoring
      ? "supporting"
      : appState.newsSnapshot?.provider?.status === "READY" ? "ready" : "degraded",
  );

  const truth = readinessTruthModel(
    appState.readinessSnapshot || {},
    {},
    ranking,
    appState.healthSnapshot || {},
  );
  const next = truth.upcomingSlots[0];
  const scheduleAvailable = Array.isArray(
    appState.healthSnapshot?.dependencies?.production_scanner?.daily_operations?.runs,
  );
  const brokerTruth = brokerDataChainTruth(
    appState.healthSnapshot || {},
    appState.bootstrapSnapshot || {},
  );
  const brokerBlocked = brokerTruth.status !== "CURRENT";
  setText(
    "overview-next-state",
    brokerBlocked
      ? brokerTruth.label
      : !scheduleAvailable
      ? "日程诊断不可用"
      : next ? formatTime(next.scheduled_at) : "下个美股交易日",
  );
  setText(
    "overview-next-summary",
    brokerBlocked
      ? brokerTruth.summary
      : !scheduleAvailable
      ? "未取得当前 health 的日程状态；等待正常刷新，不推断下一交易日或时槽。"
      : next
      ? `${researchText(next.operation, "精确时槽", 48)} · 以 scheduler 实际状态为准。`
      : "08:30 ET 研究刷新；09:35 ET 开盘重报价，只有全部 Gate 通过才成为建议。",
  );
  setSummaryCardState(
    "overview-next-state",
    brokerBlocked || !scheduleAvailable ? "degraded" : next ? "pending" : "supporting",
  );
}

function renderNewsCapabilitySummary() {
  const snapshot = appState.newsSnapshot || {};
  const rows = Array.isArray(snapshot.news) ? snapshot.news : [];
  const restore = analysisBackfillState(snapshot);
  const coverage = classifierCoverage(rows);
  const durableShadow = durableShadowCounts(appState.learningSnapshot);
  const durablePredictions = durableShadowCountText(durableShadow, "predictions");
  const durableOutcomes = durableShadowCountText(durableShadow, "outcomes");
  const providerStatus = researchText(snapshot.provider?.status, "UNAVAILABLE", 32).toUpperCase();
  setText(
    "news-live-status",
    restore.restoring
      ? `历史分析恢复中 · ${formatInteger(restore.verifiedRows)}/${formatInteger(restore.totalRows)}`
      : `${rows.length} 条 · ${providerStatus}`,
  );
  setText(
    "news-live-summary",
    restore.restoring
      ? "外部采集继续运行；历史记录仅在追加式分析账本完整校验后显示，这不代表来源返回 0 条。"
      : rows.length
      ? `最近刷新 ${formatTime(snapshot.asof)}；持续轮询，但来源降级会保留标记。`
      : "尚未取得可显示新闻。",
  );
  setSummaryCardState(
    "news-live-status",
    restore.restoring
      ? "supporting"
      : providerStatus === "READY" ? "ready" : rows.length ? "degraded" : "blocked",
  );

  const jin10Health = (Array.isArray(snapshot.source_health) ? snapshot.source_health : []).find((item) => (
    String(item?.source || "").toUpperCase() === "JIN10"
    && String(item?.source_kind || "").toUpperCase() === "NEWS"
  ));
  const jin10Rows = rows.filter((item) => String(item?.source || "").toUpperCase() === "JIN10").length;
  const jin10Status = researchText(jin10Health?.status, "UNAVAILABLE", 32).toUpperCase();
  setText("jin10-live-status", `${jin10Rows} 条 · ${jin10Status}`);
  setText(
    "jin10-live-summary",
    jin10Health
      ? restore.restoring
        ? `本轮成功 ${formatInteger(jin10Health.success_count)}；已采集，等待历史分析账本校验后显示。`
        : `${researchText(jin10Health.reason, "OK", 64)} · 本轮成功 ${formatInteger(jin10Health.success_count)}`
      : "金十运行状态尚未进入新闻快照。",
  );
  setSummaryCardState("jin10-live-status", jin10Status === "READY" ? "ready" : jin10Rows ? "degraded" : "blocked");

  setText(
    "news-model-status",
    restore.restoring
      ? `分类等待账本校验 · 累计预测 ${durablePredictions}`
      : `规则 ${coverage.deterministic}/${coverage.total} · 当前 DS ${coverage.deepseekShadow}/${coverage.total} · 累计预测 ${durablePredictions}`,
  );
  setText(
    "news-model-summary",
    restore.restoring
      ? "校验完成前不把未验证历史分类投影为当前覆盖；DeepSeek 仍仅为 shadow。"
      : coverage.total
      ? `生产分类仍为确定性规则；DeepSeek 当前覆盖与 durable 累计分开统计，已结算 outcome ${durableOutcomes}，不影响交易资格。`
      : "分类覆盖尚不可用。",
  );
  setSummaryCardState(
    "news-model-status",
    restore.restoring
      ? "supporting"
      : coverage.deepseekShadow === coverage.total && coverage.total > 0 ? "ready" : "supporting",
  );

  const reaction = appState.calendarSnapshot?.reaction_provider || {};
  const reactionStatus = researchText(reaction.status, "UNAVAILABLE", 32).toUpperCase();
  setText("news-reaction-status", reactionStatus);
  setText(
    "news-reaction-summary",
    reactionStatus === "READY"
      ? reaction.reason === "NO_ELIGIBLE_REACTION_EVENTS"
        ? `当前 eligible 0，unsupported ${formatInteger(reaction.unsupported_count)}；健康空闲，未伪造反应进度；仍为 SUPPORTING_ONLY。`
        : `eligible ${formatInteger(reaction.eligible_count)}，覆盖 ${formatInteger(reaction.matched_count)}/${formatInteger(reaction.ledger_count)}，unsupported ${formatInteger(reaction.unsupported_count)}；仍为 SUPPORTING_ONLY。`
      : `${researchText(reaction.reason, "actual → surprise → 市场反应 → 期权重评尚未闭环", 120)}。`,
  );
  setSummaryCardState("news-reaction-status", reactionStatus === "READY" ? "ready" : "blocked");
}

function syncOverviewDetailVisibility() {
  const positionTruth = positionManagementTruth(
    appState.positionsSnapshot,
    appState.ranking,
  );
  document.querySelectorAll(".overview-secondary-card").forEach((panel) => {
    const critical = panel.id === "position-management-region" && positionTruth.forceVisible;
    panel.hidden = !appState.overviewDetailsExpanded && !critical;
  });
  const button = byId("toggle-overview-details");
  if (button) {
    button.setAttribute("aria-expanded", String(appState.overviewDetailsExpanded));
    button.textContent = appState.overviewDetailsExpanded
      ? "收起系统、持仓与模型细节"
      : "查看系统、持仓与模型细节";
  }
}

function positionManagementTruth(positionPayload = null, rankingPayload = null) {
  const payload = positionPayload && typeof positionPayload === "object"
    && !Array.isArray(positionPayload)
    ? positionPayload
    : null;
  const ranking = rankingPayload && typeof rankingPayload === "object"
    && !Array.isArray(rankingPayload)
    ? rankingPayload
    : {};
  const positions = Array.isArray(payload?.positions) ? payload.positions : [];
  const status = String(payload?.status || "UNKNOWN").trim().toUpperCase();
  const unverifiedStatuses = new Set([
    "UNKNOWN",
    "STALE",
    "PARTIAL",
    "UNAVAILABLE",
    "DISCONNECTED",
  ]);
  const positionStateKnown = payload !== null
    && payload.position_state_known !== false
    && !unverifiedStatuses.has(status);
  const observedOpenPosition = positions.length > 0;
  const reasons = Array.isArray(ranking.reasons)
    ? ranking.reasons.map((value) => String(value || "").trim().toUpperCase())
    : [];
  const positionManagementOnly = reasons.includes("POSITION_MANAGEMENT_ONLY");
  const verifiedOpenPosition = positionStateKnown && observedOpenPosition;
  const verifiedFlat = positionStateKnown && !observedOpenPosition;
  return {
    status,
    positionStateKnown,
    observedOpenPosition,
    verifiedOpenPosition,
    verifiedFlat,
    positionManagementOnly,
    forceVisible: !positionStateKnown || observedOpenPosition || positionManagementOnly,
  };
}

function buildOverviewResearchCard(item, now = new Date()) {
  const freshness = researchFreshness(item, now);
  const card = createElement("article", "overview-research-card");
  const heading = createElement("div", "research-top10-card-heading");
  const identity = createElement("div", "research-top10-identity");
  identity.append(
    createElement("span", "research-top10-rank", `#${item.rank}`),
    createElement("strong", "", item.symbol),
    createElement("span", `research-direction ${researchDirectionClass(item.direction)}`, item.direction),
  );
  heading.append(identity, createElement("span", "status-chip status-no-trade", "NO_TRADE"));
  const dteText = researchDteText(item, freshness);
  const blockers = item.blockers.slice(0, 3).join(" · ") || "主决策流水线尚未验证";
  card.append(
    heading,
    createElement("p", "research-top10-structure", `${item.strategy} · 到期 ${item.expiry} · ${dteText}`),
    createElement("p", "research-top10-availability", researchFreshnessText(freshness)),
    createElement("p", "research-top10-legs", item.legs.map(formatResearchLeg).join(" ｜ ") || "腿定义缺失"),
    createElement("p", "overview-research-blockers", `${freshness.expired ? "EXPIRED" : "QUOTE REQUIRED"} · ${blockers}`),
    createElement("p", "research-top10-boundary", `SUPPORTING_ONLY · ${item.blockers.length} 个阻塞项 · NO_TRADE`),
  );
  return card;
}

function renderResearchTop10Stage() {
  const snapshot = appState.researchTop10 || normalizeResearchTop10({});
  const stage = appState.researchTop10Stage;
  const rows = stage === "open-repriced" ? snapshot.open_repriced : snapshot.premarket;
  const freshness = researchFreshness(snapshot);
  const container = byId("research-top10-list");
  if (!container) return;
  const panel = byId("research-top10-panel");
  if (panel) {
    panel.dataset.status = snapshot.status;
    panel.dataset.stage = stage;
  }
  setText("research-top10-status", `NO_TRADE · ${freshness.historical ? "HISTORICAL · " : ""}${snapshot.status}`);
  const statusNode = byId("research-top10-status");
  if (statusNode) statusNode.className = "status-chip status-no-trade";
  setText("research-top10-count", `${rows.length}/${snapshot.target_count}`);
  setText("research-top10-asof", formatTime(rows[0]?.batch_observed_at || snapshot.asof));
  document.querySelectorAll("[data-research-top10-stage]").forEach((tab) => {
    tab.classList.toggle("is-active", tab.dataset.researchTop10Stage === stage);
    if (tab.dataset.researchTop10Stage === "open-repriced") {
      const recovery = snapshot.phase === "INTRADAY_RECOVERY";
      tab.textContent = recovery ? "盘中恢复结构" : "09:35 指示性重报价";
      tab.title = recovery
        ? "查看错过计划时段后生成的只读盘中恢复结构"
        : "查看 09:35 独立指示性重报价";
    }
  });
  setText("research-top10-stage-summary", researchTop10StageSummary(snapshot, stage));
  container.replaceChildren();
  if (rows.length === 0) {
    container.append(createElement(
      "p",
      "empty-state",
      stage === "open-repriced"
        ? "09:35 指示性重报价尚不可用；不使用盘前价格替代，保持 NO_TRADE。"
        : "盘前 Top-10 尚不可用；不从可审批候选回填，保持 NO_TRADE。",
    ));
    return;
  }
  rows.forEach((item) => container.append(buildResearchTop10Card(item)));
}

function researchTop10StageSummary(snapshot, stage, now = new Date()) {
  const rows = stage === "open-repriced" ? snapshot.open_repriced : snapshot.premarket;
  const freshness = researchFreshness(snapshot, now);
  const expiredCount = rows.filter((item) => researchFreshness(item, now).expired).length;
  if (rows.length && (freshness.historical || freshness.future_dated || expiredCount)) {
    return `${researchFreshnessText(freshness)} 本阶段保留 ${rows.length}/${snapshot.target_count} 条原始研究，${expiredCount} 条合约已到期；当前无本阶段可用交易建议，保持 NO_TRADE。`;
  }
  if (stage === "open-repriced") {
    const count = snapshot.open_repriced.length;
    if (snapshot.phase === "INTRADAY_RECOVERY") {
      return count
        ? `已记录 ${count}/${snapshot.target_count} 条真实 IBKR 合约身份的盘中恢复结构；报价、流动性、EV 与风险 Gate 尚未验证，保持 SUPPORTING_ONLY、NO_TRADE。`
        : "盘中恢复结构尚不可用；不伪造报价或风险结论，保持 NO_TRADE。";
    }
    return count
      ? `已记录 ${count}/${snapshot.target_count} 条 09:35 指示性重报价；仍为 SUPPORTING_ONLY、不可审批、不可创建指令、NO_TRADE。`
      : "09:35 指示性重报价尚未发布；盘前结构不替代开盘价格，保持 NO_TRADE。";
  }
  const count = snapshot.premarket.length;
  return count
    ? `已冻结 ${count}/${snapshot.target_count} 条盘前条件式研究；等待 09:35 独立重报价，保持 NO_TRADE。`
    : "盘前 Top-10 尚未发布；不推断缺失结构，保持 NO_TRADE。";
}

function buildResearchTop10Card(item, now = new Date()) {
  const freshness = researchFreshness(item, now);
  const card = createElement("article", "research-top10-card");
  const heading = createElement("div", "research-top10-card-heading");
  const identity = createElement("div", "research-top10-identity");
  identity.append(
    createElement("span", "research-top10-rank", `#${item.rank}`),
    createElement("strong", "", item.symbol),
    createElement("span", `research-direction ${researchDirectionClass(item.direction)}`, item.direction),
  );
  heading.append(
    identity,
    createElement("span", "status-chip status-no-trade", "NO_TRADE"),
  );
  const dteText = researchDteText(item, freshness);
  const structure = createElement("p", "research-top10-structure", `${item.strategy} · 到期 ${item.expiry} · ${dteText}`);
  const legs = createElement("p", "research-top10-legs", item.legs.map(formatResearchLeg).join(" ｜ ") || "腿定义缺失");
  const availability = createElement("p", "research-top10-availability");
  const availabilityMessages = [researchFreshnessText(freshness)];
  if (item.quote_status === "UNAVAILABLE" && !freshness.expired && !freshness.historical && !freshness.future_dated) {
    availabilityMessages.push(item.intraday_recovery
      ? "盘中恢复仅确认真实合约身份；当前缺少可审计完整报价，不以 0 或本地接收时间替代交易所行情。"
      : "盘前期权报价不可用，等待 09:35；不以 0 或盘前替代价计算风险。");
  }
  if (item.ev_status === "UNAVAILABLE") {
    availabilityMessages.push("EXPECTED_PAYOFF_UNAVAILABLE · 成本后 EV 暂不可计算。");
  }
  availability.textContent = availabilityMessages.join(" ");
  const metrics = createElement("div", "research-top10-metrics");
  [
    ["指示性 Debit", formatMoney(item.indicative_debit_usd)],
    ["精确最大亏损", formatMoney(item.maximum_loss_usd)],
    ["假设最大亏损", formatMoney(item.indicative_maximum_loss_usd)],
    ["指示性成本后 EV", formatMoney(item.indicative_after_cost_ev_usd)],
    ["假设乘数", formatInteger(item.assumed_multiplier)],
    [item.time_label, formatTime(item.quote_asof || item.collected_at)],
    ["批次观察时间", formatTime(item.batch_observed_at)],
  ].forEach(([label, value]) => {
    const metric = createElement("div", "", label);
    metric.append(createElement("strong", "", value));
    metrics.append(metric);
  });
  const catalyst = createElement("p", "research-top10-catalyst", "新闻催化");
  catalyst.append(createElement("strong", "", item.news_catalyst));
  const plan = createElement("dl", "research-top10-plan");
  const planRows = freshness.expired || freshness.future_dated ? [
    ["当前处理", freshness.expired
      ? "合约已到期，不可重报价或入场；保留历史证据，需重新发现有效到期日的结构。"
      : "批次日期晚于当前纽约日期；日期验证前仅供查看。"],
  ] : [
    [freshness.historical ? "原批次入场条件（历史记录）" : "入场", item.entry_condition],
    ["反证", item.invalidation_condition],
    ["止盈", item.profit_target_condition],
    ["止损", item.stop_loss_condition],
  ];
  planRows.forEach(([label, value]) => {
    plan.append(createElement("dt", "", label), createElement("dd", "", value));
  });
  const blockerBox = createElement(
    "div",
    item.blockers.length ? "research-top10-blockers has-blocker" : "research-top10-blockers",
  );
  blockerBox.append(createElement("strong", "", item.blockers.length ? `Blockers · ${item.blockers.length}` : "Blockers · 0"));
  const blockerList = createElement("ul", "");
  (item.blockers.length ? item.blockers : ["NONE_REPORTED · 仍需主决策流水线独立验证"]).forEach((reason) => {
    blockerList.append(createElement("li", "", reason));
  });
  blockerBox.append(blockerList);
  card.append(
    heading,
    structure,
    legs,
    ...(availabilityMessages.length ? [availability] : []),
    metrics,
    catalyst,
    plan,
    blockerBox,
    createElement("p", "research-top10-boundary", "SUPPORTING_ONLY · 不可审批 · 不可创建指令 · NO_TRADE"),
  );
  return card;
}

function researchDirectionClass(direction) {
  const value = String(direction || "").toUpperCase();
  if (value.includes("BULL") || value.includes("LONG") || value.includes("看多")) return "is-bullish";
  if (value.includes("BEAR") || value.includes("SHORT") || value.includes("看空")) return "is-bearish";
  return "is-neutral";
}

function formatResearchLeg(leg) {
  const right = leg.right === "CALL" ? "C" : leg.right === "PUT" ? "P" : leg.right;
  const identity = [
    leg.side,
    leg.ratio === null ? null : `${formatQuote(leg.ratio)}x`,
    leg.expiry,
    leg.strike,
    right,
    `mult ${formatInteger(leg.multiplier)}`,
  ]
    .filter((value) => value && value !== "--")
    .join(" ");
  const market = `bid ${formatQuote(leg.bid)} / ask ${formatQuote(leg.ask)} · IV ${formatPercent(leg.implied_volatility)} · Vol ${formatInteger(leg.volume)} · OI ${formatInteger(leg.open_interest)}`;
  const timestamp = leg.quote_asof
    ? `行情时间 ${formatTime(leg.quote_asof)}`
    : `逐腿采集完成时间 ${formatTime(leg.collected_at)}`;
  return `${identity} · ${market} · ${timestamp}`;
}

function optionPoolRows(payload, stage) {
  const key = stage === "pre-market"
    ? "pre_market_preselections"
    : stage === "open-repriced"
      ? "open_market_repriced"
      : null;
  if (!key || !Array.isArray(payload?.[key])) return [];
  const expectedPhase = stage === "pre-market" ? "PRE_MARKET" : "OPEN_REPRICED";
  const expectedRank = stage === "pre-market" ? "research_rank" : "repriced_rank";
  if (payload[key].length > 10) return [];
  const rows = payload[key];
  const identifiers = new Set();
  const ranks = new Set();
  const actionRanks = new Set();
  for (const item of rows) {
    if (!item || typeof item !== "object" || Array.isArray(item)) return [];
    const identifier = typeof item.preselection_id === "string" ? item.preselection_id.trim() : "";
    const rank = Number.isInteger(item[expectedRank]) ? item[expectedRank] : null;
    const actionRank = Number.isInteger(item.action_rank) ? item.action_rank : null;
    if (
      !identifier
      || item.phase !== expectedPhase
      || item.decision_authority !== "SUPPORTING_ONLY"
      || item.approval_eligible !== false
      || item.instruction_creation_allowed !== false
      || item.order_creation_allowed !== false
      || rank === null
      || rank < 1
      || rank > 10
      || identifiers.has(identifier)
      || ranks.has(rank)
      || !clientOptionRowShapeValid(item)
      || !clientLedgerLineageValid(item, expectedPhase)
      || (expectedPhase === "PRE_MARKET" && actionRank !== null)
      || (actionRank !== null && (actionRank < 1 || actionRank > 3 || actionRanks.has(actionRank)))
      || (actionRank !== null && item.action_pool_eligible !== true)
      || (item.action_pool_eligible === true && !clientActionObservationComplete(item))
    ) return [];
    identifiers.add(identifier);
    ranks.add(rank);
    if (actionRank !== null) actionRanks.add(actionRank);
  }
  const continuousRanks = new Set(Array.from({ length: rows.length }, (_, index) => index + 1));
  if ([...ranks].some((rank) => !continuousRanks.has(rank))) return [];
  const continuousActionRanks = new Set(Array.from({ length: actionRanks.size }, (_, index) => index + 1));
  if ([...actionRanks].some((rank) => !continuousActionRanks.has(rank))) return [];
  return rows;
}

function normalizePreselectionCoverage(value, preMarketCount, openCount) {
  const raw = value && typeof value === "object" && !Array.isArray(value) ? value : {};
  const available = Number.isInteger(preMarketCount) && preMarketCount >= 0
    ? Math.min(preMarketCount, 10)
    : 0;
  const observed = Number.isInteger(openCount) && openCount >= 0
    ? Math.min(openCount, available)
    : 0;
  const rawStatus = String(raw.status || "UNAVAILABLE").toUpperCase();
  const rawOpenStatus = String(raw.open_observation_status || "UNAVAILABLE").toUpperCase();
  const rawProducerStatus = String(raw.open_reprice_producer_status || "UNAVAILABLE").toUpperCase();
  const status = available === 0
    ? "UNAVAILABLE"
    : PRESELECTION_COVERAGE_STATUSES.has(rawStatus)
      ? rawStatus
      : "PARTIAL";
  const derivedOpenStatus = available === 0
    ? "UNAVAILABLE"
    : observed === 0
      ? "NOT_STARTED"
      : observed === available
        ? "AVAILABLE"
        : "PARTIAL";
  const openObservationStatus = PRESELECTION_OPEN_STATUSES.has(rawOpenStatus)
    && rawOpenStatus === derivedOpenStatus
    ? rawOpenStatus
    : derivedOpenStatus;
  return {
    source: PRESELECTION_LEDGER_SOURCE,
    status,
    reason: preselectionReason(raw.reason, "OPEN_REPRICE_PRODUCER_UNAVAILABLE"),
    ledger_reason: preselectionReason(raw.ledger_reason, null),
    requested_count: 10,
    available_count: available,
    open_count: observed,
    open_reprice_producer_status: ["AVAILABLE", "NOT_STARTED", "UNAVAILABLE"].includes(rawProducerStatus)
      ? rawProducerStatus
      : "UNAVAILABLE",
    open_observation_status: openObservationStatus,
    latest_run_id: ledgerIdentifier(raw.latest_run_id),
    latest_head_hash: ledgerDigest(raw.latest_head_hash),
    freeze_slot: clientTimestampText(raw.freeze_slot),
    latest_open_batch_id: ledgerIdentifier(raw.latest_open_batch_id),
    latest_open_batch_head_hash: ledgerDigest(raw.latest_open_batch_head_hash),
    reprice_slot: clientTimestampText(raw.reprice_slot),
    atomic_batch_available: raw.atomic_batch_available === true,
    atomic_batch_blocker: preselectionReason(raw.atomic_batch_blocker, null),
    decision_authority: "SUPPORTING_ONLY",
    approval_eligible: false,
    instruction_creation_allowed: false,
    order_creation_allowed: false,
  };
}

function preselectionReason(value, fallback) {
  if (typeof value !== "string") return fallback;
  const normalized = value.trim().toUpperCase();
  return /^[A-Z0-9_]{1,96}$/.test(normalized) ? normalized : fallback;
}

function ledgerIdentifier(value) {
  if (typeof value !== "string") return null;
  const normalized = value.trim();
  return /^[A-Za-z0-9][A-Za-z0-9._:/-]{0,255}$/.test(normalized) ? normalized : null;
}

function ledgerDigest(value) {
  if (typeof value !== "string") return null;
  const normalized = value.trim().toLowerCase();
  return /^[0-9a-f]{64}$/.test(normalized) ? normalized : null;
}

function clientTimestamp(value) {
  if (typeof value !== "string" || !value.trim()) return null;
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function clientTimestampText(value) {
  return clientTimestamp(value) === null ? null : value.trim();
}

function clientLedgerLineageValid(item, phase) {
  const lineage = item?.ledger_lineage;
  if (!lineage || typeof lineage !== "object" || Array.isArray(lineage)) return false;
  if (
    lineage.source !== PRESELECTION_LEDGER_SOURCE
    || !ledgerIdentifier(lineage.run_id)
    || clientTimestamp(lineage.run_created_at) === null
    || !ledgerDigest(lineage.head_hash)
    || !ledgerIdentifier(lineage.row_id)
    || !ledgerDigest(lineage.row_hash)
    || !Number.isInteger(lineage.premarket_rank)
    || lineage.premarket_rank < 1
    || lineage.premarket_rank > 10
  ) return false;
  if (phase === "OPEN_REPRICED") {
    const batchFields = [
      ledgerIdentifier(lineage.batch_id),
      ledgerDigest(lineage.batch_head_hash),
      clientTimestampText(lineage.scheduled_for),
      ledgerIdentifier(lineage.quote_batch_id),
    ];
    const presentBatchFields = batchFields.filter((value) => value !== null).length;
    return Boolean(
      ledgerIdentifier(lineage.observation_id)
      && clientTimestamp(lineage.observed_at) !== null
      && ledgerDigest(lineage.observation_hash)
      && (presentBatchFields === 0 || presentBatchFields === batchFields.length),
    );
  }
  const hasEligibility = Object.hasOwn(lineage, "production_parent_eligible")
    || Object.hasOwn(lineage, "production_parent_blocker");
  const eligibilityValid = !hasEligibility || (
    typeof lineage.production_parent_eligible === "boolean"
    && (
      (lineage.production_parent_eligible === true && lineage.production_parent_blocker === null)
      || (
        lineage.production_parent_eligible === false
        && lineage.production_parent_blocker === "LEGACY_V1_IBKR_IDENTITY_INCOMPLETE"
      )
    )
  );
  return phase === "PRE_MARKET"
    && lineage.observation_id === undefined
    && lineage.observation_hash === undefined
    && eligibilityValid;
}

function clientOptionRowShapeValid(item) {
  if (!Array.isArray(item.legs) || item.legs.length < 1 || item.legs.length > 8) return false;
  return item.legs.every((leg) => {
    const strike = numberOrNull(leg?.strike);
    const additionalIdentity = [
      typeof leg?.local_symbol === "string" && leg.local_symbol.trim() ? leg.local_symbol : null,
      typeof leg?.trading_class === "string" && leg.trading_class.trim() ? leg.trading_class : null,
      Number.isInteger(leg?.multiplier) && leg.multiplier > 0 ? leg.multiplier : null,
      typeof leg?.exchange === "string" && leg.exchange.trim() ? leg.exchange : null,
    ];
    const identityCount = additionalIdentity.filter((value) => value !== null).length;
    return leg
      && typeof leg === "object"
      && !Array.isArray(leg)
      && leg.underlying === item.underlying
      && Number.isInteger(leg.con_id)
      && leg.con_id > 0
      && typeof leg.expiry === "string"
      && /^\d{4}-\d{2}-\d{2}$/.test(leg.expiry)
      && strike !== null
      && strike > 0
      && ["CALL", "PUT"].includes(leg.right)
      && ["BUY", "SELL"].includes(leg.side)
      && Number.isInteger(leg.ratio)
      && leg.ratio > 0
      && Number.isInteger(leg.quantity)
      && leg.quantity > 0
      && (identityCount === 0 || identityCount === additionalIdentity.length);
  });
}

function clientOptionLegIdentityComplete(leg) {
  return Boolean(
    leg
    && Number.isInteger(leg.con_id)
    && leg.con_id > 0
    && typeof leg.local_symbol === "string"
    && leg.local_symbol.trim()
    && typeof leg.trading_class === "string"
    && leg.trading_class.trim()
    && Number.isInteger(leg.multiplier)
    && leg.multiplier > 0
    && typeof leg.exchange === "string"
    && leg.exchange.trim()
    && /^\d{4}-\d{2}-\d{2}$/.test(leg.expiry)
    && numberOrNull(leg.strike) > 0
    && ["CALL", "PUT"].includes(leg.right)
  );
}

function clientActionObservationComplete(item) {
  if (
    item.phase !== "OPEN_REPRICED"
    || item.risk_defined !== true
    || numberOrNull(item.maximum_loss_usd) === null
    || numberOrNull(item.maximum_loss_usd) <= 0
    || numberOrNull(item.cost_after_ev_usd) === null
    || numberOrNull(item.cost_after_ev_usd) <= 0
    || numberOrNull(item.maximum_quote_age_seconds) === null
    || numberOrNull(item.maximum_quote_age_seconds) < 0
    || numberOrNull(item.maximum_quote_age_seconds) > 5
    || clientTimestamp(item.oldest_quote_asof) === null
    || !Array.isArray(item.blockers)
    || item.blockers.length !== 0
    || !ledgerIdentifier(item.quote_batch_id)
    || !clientLedgerLineageValid(item, "OPEN_REPRICED")
    || item.ledger_lineage.quote_batch_id !== item.quote_batch_id
    || !clientOpenEconomicsLineageComplete(item)
  ) return false;
  const observedAt = clientTimestamp(item.ledger_lineage?.observed_at);
  if (observedAt === null) return false;
  const quoteTimes = [];
  for (const leg of item.legs) {
    const bid = numberOrNull(leg.bid);
    const ask = numberOrNull(leg.ask);
    const iv = numberOrNull(leg.implied_volatility);
    const quoteTime = clientTimestamp(leg.quote_asof);
    if (
      bid === null
      || bid < 0
      || ask === null
      || ask < 0
      || bid > ask
      || iv === null
      || iv < 0
      || quoteTime === null
      || quoteTime > observedAt
      || !clientOptionLegIdentityComplete(leg)
      || ledgerIdentifier(leg.quote_batch_id) !== item.quote_batch_id
      || !Number.isInteger(leg.dte)
      || leg.dte < 7
      || ["delta", "gamma", "theta", "vega"].some((name) => numberOrNull(leg[name]) === null)
      || numberOrNull(leg.delta) < -1
      || numberOrNull(leg.delta) > 1
      || !Number.isInteger(leg.volume)
      || leg.volume < 0
      || !Number.isInteger(leg.open_interest)
      || leg.open_interest < 0
    ) return false;
    quoteTimes.push(quoteTime);
  }
  if (Math.min(...quoteTimes) !== clientTimestamp(item.oldest_quote_asof)) return false;
  const exposure = new Map();
  for (const leg of item.legs) {
    const key = `${leg.expiry}:${leg.right}`;
    const bucket = exposure.get(key) || { BUY: 0, SELL: 0 };
    bucket[leg.side] += leg.ratio * leg.quantity;
    exposure.set(key, bucket);
  }
  return [...exposure.values()].every((bucket) => bucket.SELL <= bucket.BUY);
}

function clientOpenEconomicsLineageComplete(item) {
  const scenarioAsOf = clientTimestamp(item.scenario_asof);
  const economicsAsOf = clientTimestamp(item.economics_quote_asof);
  const digests = [
    item.scenario_hash,
    item.execution_cost_contract_hash,
    item.risk_policy_hash,
    item.broker_snapshot_hash,
    item.strategy_nav_post_hash,
    item.payoff_hash,
    item.economics_calculation_hash,
  ];
  if (
    scenarioAsOf === null
    || economicsAsOf === null
    || scenarioAsOf > economicsAsOf
    || digests.some((value) => ledgerDigest(value) === null)
    || typeof item.execution_cost_contract_version !== "string"
    || !item.execution_cost_contract_version.trim()
    || typeof item.risk_policy_version !== "string"
    || !item.risk_policy_version.trim()
    || ledgerIdentifier(item.economics_quote_batch_id) !== item.quote_batch_id
    || !Array.isArray(item.terminal_scenarios)
    || item.terminal_scenarios.length === 0
  ) return false;
  const probabilities = [];
  const prices = new Set();
  for (const scenario of item.terminal_scenarios) {
    const price = numberOrNull(scenario?.terminal_underlying_price);
    const probability = numberOrNull(scenario?.probability);
    if (
      price === null
      || price < 0
      || probability === null
      || probability <= 0
      || probability > 1
      || prices.has(String(price))
    ) return false;
    prices.add(String(price));
    probabilities.push(probability);
  }
  if (!approximatelyEqual(probabilities.reduce((total, value) => total + value, 0), 1)) {
    return false;
  }
  const values = Object.fromEntries([
    "maximum_loss_usd",
    "estimated_cost_usd",
    "cost_after_ev_usd",
    "strategy_nav_usd",
    "debit_usd",
    "credit_usd",
    "net_entry_cost_usd",
    "estimated_commission_usd",
    "estimated_entry_slippage_usd",
    "estimated_exit_slippage_usd",
    "estimated_slippage_usd",
    "expected_value_before_costs_usd",
    "risk_fraction",
  ].map((name) => [name, numberOrNull(item[name])]));
  if (Object.values(values).some((value) => value === null)) return false;
  if (
    values.maximum_loss_usd <= 0
    || values.strategy_nav_usd <= 0
    || values.cost_after_ev_usd <= 0
    || [
      values.debit_usd,
      values.credit_usd,
      values.net_entry_cost_usd,
      values.estimated_commission_usd,
      values.estimated_entry_slippage_usd,
      values.estimated_exit_slippage_usd,
      values.estimated_slippage_usd,
      values.risk_fraction,
    ].some((value) => value < 0)
    || !approximatelyEqual(
      values.estimated_entry_slippage_usd + values.estimated_exit_slippage_usd,
      values.estimated_slippage_usd,
    )
    || !approximatelyEqual(
      values.debit_usd - values.credit_usd + values.estimated_commission_usd + values.estimated_slippage_usd,
      values.net_entry_cost_usd,
    )
    || !approximatelyEqual(values.estimated_cost_usd, values.net_entry_cost_usd)
    || !approximatelyEqual(
      values.cost_after_ev_usd + values.estimated_commission_usd + values.estimated_slippage_usd,
      values.expected_value_before_costs_usd,
    )
    || !approximatelyEqual(
      values.maximum_loss_usd / values.strategy_nav_usd,
      values.risk_fraction,
    )
    || values.risk_fraction > 0.10
  ) return false;
  return true;
}

function approximatelyEqual(left, right) {
  const scale = Math.max(1, Math.abs(left), Math.abs(right));
  return Math.abs(left - right) <= scale * 1e-9;
}

function preselectionClientProjectionValid(preMarket, opened, coverage, rawCoverage) {
  if (!Array.isArray(preMarket) || !Array.isArray(opened)) return false;
  if (preMarket.length === 0) return opened.length === 0;
  const raw = rawCoverage && typeof rawCoverage === "object" && !Array.isArray(rawCoverage)
    ? rawCoverage
    : null;
  if (
    !raw
    || raw.source !== PRESELECTION_LEDGER_SOURCE
    || raw.requested_count !== 10
    || raw.available_count !== preMarket.length
    || raw.open_count !== opened.length
    || raw.decision_authority !== "SUPPORTING_ONLY"
    || raw.approval_eligible !== false
    || raw.instruction_creation_allowed !== false
    || raw.order_creation_allowed !== false
    || coverage.latest_run_id === null
    || coverage.latest_head_hash === null
  ) return false;
  const parents = new Map();
  const rowIds = new Set();
  const rowHashes = new Set();
  let runCreatedAt = null;
  for (const item of preMarket) {
    const lineage = item.ledger_lineage;
    const createdAt = clientTimestamp(lineage.run_created_at);
    if (
      lineage.run_id !== coverage.latest_run_id
      || lineage.head_hash !== coverage.latest_head_hash
      || lineage.premarket_rank !== item.research_rank
      || rowIds.has(lineage.row_id)
      || rowHashes.has(lineage.row_hash)
      || (runCreatedAt !== null && createdAt !== runCreatedAt)
    ) return false;
    runCreatedAt = createdAt;
    parents.set(item.preselection_id, lineage);
    rowIds.add(lineage.row_id);
    rowHashes.add(lineage.row_hash);
  }
  const openedIds = new Set();
  const observationIds = new Set();
  const observations = new Set();
  for (const item of opened) {
    const parent = parents.get(item.preselection_id);
    const lineage = item.ledger_lineage;
    const observedAt = clientTimestamp(lineage.observed_at);
    if (
      !parent
      || lineage.run_id !== coverage.latest_run_id
      || lineage.head_hash !== coverage.latest_head_hash
      || lineage.row_id !== parent.row_id
      || lineage.row_hash !== parent.row_hash
      || lineage.premarket_rank !== parent.premarket_rank
      || observedAt === null
      || runCreatedAt === null
      || observedAt < runCreatedAt
      || openedIds.has(item.preselection_id)
      || observationIds.has(lineage.observation_id)
      || observations.has(lineage.observation_hash)
    ) return false;
    openedIds.add(item.preselection_id);
    observationIds.add(lineage.observation_id);
    observations.add(lineage.observation_hash);
  }
  if (coverage.atomic_batch_available !== true) {
    return opened.every(
      (item) => item.action_pool_eligible !== true && !Number.isInteger(item.action_rank),
    );
  }
  if (
    raw.atomic_batch_available !== true
    || raw.atomic_batch_blocker !== null
    || coverage.atomic_batch_blocker !== null
    || coverage.open_reprice_producer_status !== "AVAILABLE"
    || coverage.open_observation_status !== "AVAILABLE"
    || coverage.freeze_slot === null
    || coverage.latest_open_batch_id === null
    || coverage.latest_open_batch_head_hash === null
    || coverage.reprice_slot === null
    || opened.length !== preMarket.length
    || openedIds.size !== parents.size
    || [...parents.keys()].some((identifier) => !openedIds.has(identifier))
  ) return false;
  const freezeSlot = clientTimestamp(coverage.freeze_slot);
  const repriceSlot = clientTimestamp(coverage.reprice_slot);
  if (freezeSlot === null || repriceSlot === null || freezeSlot !== runCreatedAt) return false;
  for (const item of preMarket) {
    const lineage = item.ledger_lineage;
    if (
      lineage.production_parent_eligible !== true
      || lineage.production_parent_blocker !== null
    ) return false;
  }
  let quoteBatchId = null;
  for (const item of opened) {
    const lineage = item.ledger_lineage;
    const observedAt = clientTimestamp(lineage.observed_at);
    const scheduledFor = clientTimestamp(lineage.scheduled_for);
    if (
      lineage.batch_id !== coverage.latest_open_batch_id
      || lineage.batch_head_hash !== coverage.latest_open_batch_head_hash
      || scheduledFor === null
      || scheduledFor !== repriceSlot
      || observedAt < scheduledFor
      || !ledgerIdentifier(lineage.quote_batch_id)
      || lineage.quote_batch_id !== item.quote_batch_id
      || (quoteBatchId !== null && quoteBatchId !== lineage.quote_batch_id)
      || !Array.isArray(item.legs)
      || item.legs.some((leg) => leg.quote_batch_id !== lineage.quote_batch_id)
    ) return false;
    quoteBatchId = lineage.quote_batch_id;
  }
  return true;
}

function reactionRecord(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function reactionText(value) {
  if (typeof value === "string") {
    const normalized = value.trim();
    return normalized || REACTION_UNAVAILABLE;
  }
  if (typeof value === "number" && Number.isFinite(value)) return String(value);
  return REACTION_UNAVAILABLE;
}

function reactionTimestamp(value) {
  const normalized = reactionText(value);
  if (normalized === REACTION_UNAVAILABLE) return REACTION_UNAVAILABLE;
  return Number.isFinite(Date.parse(normalized)) ? normalized : REACTION_UNAVAILABLE;
}

function reactionDigest(value) {
  const normalized = reactionText(value).toLowerCase();
  return /^[0-9a-f]{64}$/.test(normalized) ? normalized : REACTION_UNAVAILABLE;
}

function reactionEnum(value, allowed, fallback) {
  if (typeof value !== "string") return fallback;
  const normalized = value.trim().toUpperCase();
  return allowed.has(normalized) ? normalized : fallback;
}

function reactionCount(value) {
  if (typeof value !== "number" && typeof value !== "string") return null;
  if (typeof value === "string" && !value.trim()) return null;
  const count = numberOrNull(value);
  return count !== null && Number.isInteger(count) && count >= 0 ? count : null;
}

function normalizeCalendarReaction(event) {
  const source = reactionRecord(event);
  const reaction = reactionRecord(source.reaction);
  const expectation = reactionRecord(reaction.expectation);
  const release = reactionRecord(reaction.release);
  const surprise = reactionRecord(reaction.surprise);
  const marketReaction = reactionRecord(reaction.market_reaction);
  const optionReevaluation = reactionRecord(reaction.option_reevaluation);
  const decisionIsReported = typeof reaction.decision === "string"
    && REACTION_DECISIONS.has(reaction.decision.trim().toUpperCase());
  const decision = reactionEnum(reaction.decision, REACTION_DECISIONS, "NO_TRADE");
  const suppliedReasons = Array.isArray(reaction.reasons) ? reaction.reasons : null;
  const reportedReasons = suppliedReasons === null
    ? []
    : suppliedReasons
      .filter((reason) => typeof reason === "string" && reason.trim())
      .map((reason) => reason.trim());
  const noTradeReasons = [...new Set(reportedReasons)];
  if (!decisionIsReported) noTradeReasons.unshift("REACTION_DECISION_UNAVAILABLE");
  const reasonsState = !decisionIsReported || suppliedReasons === null
    ? "UNAVAILABLE"
    : suppliedReasons.length === 0
      ? "NONE_REPORTED"
      : noTradeReasons.length > 0
        ? "REPORTED"
        : "UNAVAILABLE";

  return {
    status: reactionEnum(reaction.status, REACTION_STATUSES, REACTION_UNAVAILABLE),
    currentStage: reactionEnum(reaction.current_stage, REACTION_STAGES, REACTION_UNAVAILABLE),
    decision,
    decisionAuthority: "SUPPORTING_ONLY",
    analysisAvailable: typeof reaction.analysis_available === "boolean"
      ? reaction.analysis_available
      : REACTION_UNAVAILABLE,
    expectation: {
      metric: reactionText(expectation.metric),
      value: reactionText(expectation.expected_value),
      unit: reactionText(expectation.unit),
      observedAt: reactionTimestamp(expectation.observed_at),
    },
    officialActual: {
      value: reactionText(release.actual_value),
      unit: reactionText(release.unit),
      releasedAt: reactionTimestamp(release.released_at),
      revision: reactionText(release.revision),
    },
    surprise: {
      delta: reactionText(surprise.delta),
      relativeDelta: reactionText(surprise.relative_delta),
      assessedAt: reactionTimestamp(surprise.assessed_at),
    },
    marketReactionWindow: {
      start: reactionTimestamp(marketReaction.window_start),
      end: reactionTimestamp(marketReaction.window_end),
      evidenceAsof: reactionTimestamp(marketReaction.evidence_asof),
    },
    optionReevaluation: {
      optionId: reactionText(optionReevaluation.option_id),
      candidateHash: reactionDigest(optionReevaluation.candidate_hash),
      evidenceAsof: reactionTimestamp(optionReevaluation.evidence_asof),
      observedAt: reactionTimestamp(optionReevaluation.observed_at),
    },
    noTradeReasons,
    reasonsState,
  };
}

function normalizeReactionProviderSummary(payload) {
  const source = reactionRecord(payload);
  const provider = reactionRecord(source.reaction_provider);
  const eventCount = Array.isArray(source.calendar)
    ? source.calendar.length
    : reactionCount(source.count);
  return {
    status: reactionEnum(provider.status, REACTION_STATUSES, REACTION_UNAVAILABLE),
    decision: reactionEnum(source.reaction_decision, REACTION_DECISIONS, "NO_TRADE"),
    reason: reactionText(provider.reason),
    eventCount,
    ledgerCount: reactionCount(provider.ledger_count),
    matchedCount: reactionCount(provider.matched_count),
    ignoredCount: reactionCount(provider.ignored_count),
    supportedCount: reactionCount(provider.supported_count),
    eligibleCount: reactionCount(provider.eligible_count),
    unsupportedCount: reactionCount(provider.unsupported_count),
    measureCount: reactionCount(provider.measure_count),
    captureSpecCount: reactionCount(provider.capture_spec_count),
    capturedVintageCount: reactionCount(provider.captured_release_vintage_count),
    capturedMeasureCount: reactionCount(provider.captured_measure_count),
    captureEligibleCount: reactionCount(provider.capture_eligible_count),
    surpriseReadyCount: reactionCount(provider.surprise_ready_count),
    progressedEventCount: reactionCount(provider.progressed_event_count),
    nextEligibleReleaseAt: reactionText(provider.next_eligible_release_at),
    scheduleRefreshStatus: reactionText(provider.schedule_refresh_status),
    scheduleRefreshReason: reactionText(provider.schedule_refresh_reason),
    descriptorWaitCount: reactionCount(provider.descriptor_wait_count),
    nextAction: reactionText(provider.next_action),
    supportMatrix: Array.isArray(provider.support_matrix)
      ? provider.support_matrix.slice(0, 32).map((item) => reactionRecord(item))
      : [],
    lastAttempt: reactionTimestamp(provider.last_attempt),
  };
}

function renderCalendar(payload) {
  const source = reactionRecord(payload);
  appState.calendarSnapshot = source;
  appState.calendar = Array.isArray(source.calendar)
    ? source.calendar.filter((item) => Object.keys(reactionRecord(item)).length > 0)
    : [];
  renderNewsCapabilitySummary();
  renderProviderHealth("calendar-provider-health", source.provider, "日历");
  renderReactionProviderSummary(source);
  const decision = reactionEnum(source.decision, REACTION_DECISIONS, "NO_TRADE");
  const decisionNode = byId("calendar-decision-status");
  if (decisionNode) {
    decisionNode.textContent = decision;
    decisionNode.classList.remove("status-up", "status-down", "status-stale", "status-unknown");
    decisionNode.classList.add(decision === "OBSERVATION_ONLY" ? "status-up" : "status-stale");
  }
  const reasons = Array.isArray(source.reasons) ? source.reasons.filter((item) => typeof item === "string") : [];
  const sourceSummary = (Array.isArray(source.sources) ? source.sources : [])
    .filter((item) => Object.keys(reactionRecord(item)).length > 0)
    .map((source) => {
      const duplicateNote = Number(source.duplicate_count) > 0
        ? ` · 完全重复折叠 ${source.duplicate_count} 条 · audit ${shortHash(source.audit_hash)}`
        : "";
      const reasonNote = source.reason ? ` · ${source.reason}` : "";
      return `${source.source || "官方来源"} ${source.status || "DEGRADED"}${reasonNote}${duplicateNote} · observed ${formatTime(source.observed_at)}`;
    });
  setText(
    "calendar-status-reasons",
    [...reasons, ...sourceSummary].join(" · ")
      || (decision === "OBSERVATION_ONLY" ? "官方日历快照完整；仍仅作为辅助证据。" : "等待完整官方日历快照。"),
  );
  renderCalendarList();
  renderNewsChronology();
}

function renderReactionProviderSummary(payload) {
  const summary = normalizeReactionProviderSummary(payload);
  const statusNode = byId("calendar-reaction-provider-status");
  if (statusNode) {
    statusNode.classList.remove("status-up", "status-down", "status-stale", "status-unknown");
    const healthy = summary.status === "READY" && summary.decision === "OBSERVATION_ONLY";
    const blocked = summary.status === "CONFLICTED" || summary.status === "NO_TRADE";
    statusNode.classList.add(healthy ? "status-up" : blocked ? "status-down" : "status-stale");
    statusNode.textContent = `事件反应 provider · ${summary.status} · ${summary.decision}`;
  }
  const coverage = [
    `eligible ${reactionCountLabel(summary.eligibleCount)}`,
    `覆盖 ${reactionCountLabel(summary.matchedCount)}/${reactionCountLabel(summary.eligibleCount)}`,
    `unsupported ${reactionCountLabel(summary.unsupportedCount)}`,
    `measures ${reactionCountLabel(summary.measureCount)}`,
    `capture specs ${reactionCountLabel(summary.captureSpecCount)}`,
    `captured vintages ${reactionCountLabel(summary.capturedVintageCount)}`,
    `captured measures ${reactionCountLabel(summary.capturedMeasureCount)}`,
    `capture eligible ${reactionCountLabel(summary.captureEligibleCount)}`,
    `surprise ready ${reactionCountLabel(summary.surpriseReadyCount)}`,
    `progressed ${reactionCountLabel(summary.progressedEventCount)}`,
    `schedule ${summary.scheduleRefreshStatus}`,
    `descriptor waits ${reactionCountLabel(summary.descriptorWaitCount)}`,
    `ledger ${reactionCountLabel(summary.ledgerCount)}`,
    `ignored ${reactionCountLabel(summary.ignoredCount)}`,
    `last attempt ${summary.lastAttempt === REACTION_UNAVAILABLE ? "--" : formatTime(summary.lastAttempt)}`,
  ];
  if (summary.nextAction !== REACTION_UNAVAILABLE) coverage.push(`next ${summary.nextAction}`);
  if (summary.scheduleRefreshReason !== REACTION_UNAVAILABLE) {
    coverage.push(`schedule reason ${summary.scheduleRefreshReason}`);
  }
  if (summary.nextEligibleReleaseAt !== REACTION_UNAVAILABLE) {
    coverage.push(
      summary.nextEligibleReleaseAt === "NEXT_ELIGIBLE_RELEASE_UNKNOWN"
        ? summary.nextEligibleReleaseAt
        : `next release ${formatTime(summary.nextEligibleReleaseAt)}`,
    );
  }
  const familyCoverage = summary.supportMatrix
    .map((item) => `${reactionText(item.family)}:${reactionText(item.support_state)}`)
    .filter((item) => !item.includes(REACTION_UNAVAILABLE));
  if (familyCoverage.length > 0) coverage.push(familyCoverage.join(", "));
  if (summary.reason !== REACTION_UNAVAILABLE) coverage.push(summary.reason);
  setText("calendar-reaction-provider-coverage", coverage.join(" · "));
}

function reactionCountLabel(value) {
  return value === null ? "--" : String(value);
}

function renderNewsBrokerHealth(health) {
  const dependency = health.dependencies?.news || health.dependencies?.calendar;
  if (!dependency) return;
  const existing = byId("news-provider-health");
  if (existing?.textContent?.includes("--")) renderProviderHealth("news-provider-health", dependency, "新闻");
}

function renderProviderHealth(targetId, provider, label) {
  const node = byId(targetId);
  if (!node) return;
  const status = String(provider?.status || "UNKNOWN").toUpperCase();
  node.classList.remove("status-up", "status-down", "status-stale", "status-unknown");
  const up = ["UP", "READY", "HEALTHY"].includes(status);
  const down = ["DOWN", "ERROR", "UNCONFIGURED"].includes(status);
  node.classList.add(up ? "status-up" : down ? "status-down" : "status-stale");
  const latency = numberOrNull(provider?.latency_ms);
  node.textContent = `${label} · ${status}${latency === null ? "" : ` · ${latency.toFixed(0)}ms`}`;
}

function normalizeNewsSourceHealth(value) {
  if (!Array.isArray(value)) return [];
  return value.slice(0, 16).flatMap((item) => {
    if (!item || typeof item !== "object" || Array.isArray(item)) return [];
    const rawSource = typeof item.source === "string" ? item.source.trim().toUpperCase() : "UNKNOWN";
    const source = rawSource.replace(/[^A-Z0-9_]+/g, "_").replace(/^_+|_+$/g, "") || "UNKNOWN";
    const rawKind = typeof item.source_kind === "string" ? item.source_kind.trim().toUpperCase() : "UNKNOWN";
    const sourceKind = ["NEWS", "CALENDAR", "OFFICIAL_CALENDAR"].includes(rawKind) ? rawKind : "UNKNOWN";
    const successCount = Number.isInteger(item.success_count) && item.success_count >= 0 ? item.success_count : 0;
    const failureDateCount = Number.isInteger(item.failure_date_count) && item.failure_date_count >= 0
      ? item.failure_date_count
      : 0;
    const rawStatus = typeof item.status === "string" ? item.status.trim().toUpperCase() : "DEGRADED";
    const status = ["UP", "READY", "HEALTHY"].includes(rawStatus) && failureDateCount === 0
      ? "READY"
      : rawStatus === "DOWN"
        ? "DOWN"
        : ["UNCONFIGURED", "NOT_CONFIGURED"].includes(rawStatus) ? rawStatus : "DEGRADED";
    const rawReason = typeof item.reason === "string" ? item.reason.trim().toUpperCase() : "";
    const reason = ["UNCONFIGURED", "NOT_CONFIGURED"].includes(status)
      ? status
      : status === "READY"
        ? null
        : SOURCE_HEALTH_REASON_CODES.has(rawReason) ? rawReason : "PROVIDER_DEGRADED";
    const normalized = {
      source,
      source_kind: sourceKind,
      status,
      reason,
      success_count: successCount,
      failure_date_count: failureDateCount,
    };
    const coverageStatus = researchText(item.coverage_status, "", 24).toUpperCase();
    const requestedSymbolCount = Number.isInteger(item.requested_symbol_count)
      ? Math.max(0, item.requested_symbol_count)
      : null;
    const queriedSymbolCount = Number.isInteger(item.queried_symbol_count)
      ? Math.max(0, item.queried_symbol_count)
      : null;
    if (
      ["FULL", "BOUNDED"].includes(coverageStatus)
      && requestedSymbolCount !== null
      && queriedSymbolCount !== null
      && queriedSymbolCount <= requestedSymbolCount
    ) {
      normalized.coverage_status = coverageStatus;
      const coverageReason = researchText(item.coverage_reason, "", 64).toUpperCase();
      normalized.coverage_reason = coverageStatus === "BOUNDED"
        && ["PROVIDER_TICKER_LIMIT", "PROVIDER_SYMBOL_ROTATION"].includes(coverageReason)
        ? coverageReason
        : null;
      normalized.requested_symbol_count = requestedSymbolCount;
      normalized.queried_symbol_count = queriedSymbolCount;
    }
    return [normalized];
  });
}

function normalizeSourceRuntime(value, now = Date.now()) {
  if (!Array.isArray(value)) return [];
  return value.slice(0, 16).flatMap((item) => {
    if (!item || typeof item !== "object" || Array.isArray(item)) return [];
    const row = {
      source: researchText(item.source_id, "UNKNOWN", 64).toUpperCase(),
      source_kind: researchText(item.source_kind, "UNKNOWN", 32).toUpperCase(),
      freshness: researchText(item.freshness, "UNAVAILABLE", 24).toUpperCase(),
      cadence_status: researchText(item.cadence_status, "SUPPRESSED", 24).toUpperCase(),
      failure_code: item.failure_code ? researchText(item.failure_code, "PROVIDER_DEGRADED", 96).toUpperCase() : null,
      last_success: item.last_success,
      next_due: item.next_due,
      interval_seconds: Number.isInteger(item.interval_seconds) ? item.interval_seconds : null,
    };
    const successAt = Date.parse(row.last_success);
    const attemptAt = Date.parse(item.last_attempt);
    const nextDue = Date.parse(row.next_due);
    // Client time can expire a displayed observation, never renew it.
    if (!Number.isFinite(now) || successAt > now || attemptAt > now) {
      row.freshness = "UNAVAILABLE";
      row.cadence_status = "SUPPRESSED";
      row.failure_code = "SOURCE_STATUS_CLOCK_REGRESSED";
    } else if (row.freshness === "CURRENT") {
      if (!Number.isFinite(successAt) || !(row.interval_seconds > 0)) {
        row.freshness = "UNAVAILABLE";
      } else if (now > successAt + row.interval_seconds * 1000) {
        row.freshness = "STALE";
        row.failure_code ||= "SOURCE_STATUS_STALE";
      }
    }
    if (row.cadence_status === "WAITING" && Number.isFinite(nextDue) && now >= nextDue) {
      row.cadence_status = "DUE";
    }
    return [row];
  });
}

function renderNewsPublication(payload) {
  if (payload?.source_status_scope !== "LIVE_ACQUISITION_DIAGNOSTIC") {
    setText("news-publication-status", "采集与发布进度不可用 · 不代表交易就绪");
    return;
  }
  const progress = payload.refresh_progress || {};
  const stages = Object.freeze({
    IDLE: "空闲",
    LOCAL_ANALYSIS_RESTORE: "恢复本地分析",
    NEWS_PROVIDERS: "采集新闻",
    NEWS_APPEND: "写入新闻证据",
    IBKR_BINDINGS: "核验标的绑定",
    PRESELECTIONS: "读取合约预选",
    CALENDAR_PROVIDERS: "采集日历",
    READ_MODEL_REBUILD: "重建读模型",
  });
  const statuses = Object.freeze({IDLE: "空闲", RUNNING: "进行中", COMPLETED: "已完成", FAILED: "失败"});
  const elapsed = typeof progress.stage_elapsed_ms === "number"
    && Number.isFinite(progress.stage_elapsed_ms) && progress.stage_elapsed_ms >= 0
    ? `${(progress.stage_elapsed_ms / 1000).toFixed(1)}s`
    : "--";
  setText("news-publication-status", `最新采集状态 ${formatTime(payload.source_status_observed_at)} · 状态核验 ${formatTime(payload.source_status_evaluated_at)} · 决策数据截止 ${formatTime(payload.asof)} · 发布完成 ${formatTime(payload.read_model_published_at)} · ${stages[progress.stage] || "未知阶段"} ${statuses[progress.status] || "不可用"} ${elapsed} · 不代表交易就绪`);
}

function renderNewsSourceHealth(value, sourceRuntime = [], now = Date.now()) {
  const rows = new Map(normalizeNewsSourceHealth(value).map((item) => [`${item.source}:${item.source_kind}`, item]));
  const runtime = new Map(normalizeSourceRuntime(sourceRuntime, now).map((item) => [`${item.source}:${item.source_kind}`, item]));
  [
    ["SEC", "NEWS", "sec-source-health", "SEC"],
    ["COMPANY_IR", "NEWS", "company-ir-source-health", "Company IR"],
    ["FINNHUB", "NEWS", "finnhub-news-source-health", "Finnhub 新闻"],
    ["ALPHA_VANTAGE", "NEWS", "alpha-vantage-source-health", "Alpha Vantage"],
    ["JIN10", "NEWS", "jin10-source-health", "金十"],
    ["NASDAQ", "CALENDAR", "nasdaq-source-health", "Nasdaq 日历"],
    ["FINNHUB", "CALENDAR", "finnhub-calendar-source-health", "Finnhub 日历"],
    ["OFFICIAL_CALENDAR", "OFFICIAL_CALENDAR", "official-calendar-source-health", "官方日历"],
  ].forEach(([source, sourceKind, targetId, label]) => {
    const item = rows.get(`${source}:${sourceKind}`)
      || (source === "COMPANY_IR" ? rows.get(`COMPANYIREVENTPROVIDER:${sourceKind}`) : null)
      || {
      status: "DEGRADED",
      reason: "NOT_OBSERVED",
      success_count: 0,
      failure_date_count: 0,
    };
    const node = byId(targetId);
    if (!node) return;
    const lane = runtime.get(`${source}:${sourceKind}`);
    const status = item.status === "READY" && lane
      && (lane.failure_code || lane.freshness !== "CURRENT")
      ? "DEGRADED" : item.status;
    node.classList.remove("status-up", "status-down", "status-stale", "status-unknown");
    node.classList.add(
      status === "READY"
        ? "status-up"
        : status === "DOWN" ? "status-down" : "status-stale",
    );
    const failure = lane?.failure_code || item.reason || "OK";
    const cadence = lane
      ? `${lane.freshness}/${lane.cadence_status} · ${lane.interval_seconds ?? "--"}s · 下次 ${formatTime(lane.next_due)} · 最近成功 ${formatTime(lane.last_success)}`
      : "cadence UNAVAILABLE";
    const coverage = item.coverage_status !== "BOUNDED"
      ? ""
      : item.coverage_reason === "PROVIDER_SYMBOL_ROTATION"
        ? ` · 每轮轮换抓取 ${item.queried_symbol_count}/${item.requested_symbol_count} 标的`
        : ` · 每日交叉核验 ${item.queried_symbol_count}/${item.requested_symbol_count} 标的`;
    node.textContent = `${label} · ${status} · ${failure} · ${cadence} · 成功 ${item.success_count} · 失败日期 ${item.failure_date_count}${coverage}`;
  });
}

function intelligenceSummary(item) {
  const intelligence = item?.intelligence && typeof item.intelligence === "object"
    ? item.intelligence
    : {};
  const direction = intelligence.direction?.value || intelligence.direction?.reason || "DIRECTION_UNAVAILABLE";
  const horizon = intelligence.horizon?.value || intelligence.horizon?.reason || "HORIZON_UNAVAILABLE";
  const confidence = intelligence.confidence?.value ?? intelligence.confidence?.reason ?? "CONFIDENCE_UNAVAILABLE";
  const impact = intelligence.impact?.value ?? intelligence.impact?.reason ?? "IMPACT_UNAVAILABLE";
  const assets = Array.isArray(intelligence.affected_assets?.values) && intelligence.affected_assets.values.length > 0
    ? intelligence.affected_assets.values.join(" / ")
    : intelligence.affected_assets?.reason || "AFFECTED_ASSETS_UNAVAILABLE";
  return `${intelligence.primary_category || "UNKNOWN"} · facets ${(intelligence.facets || []).join("/") || "UNKNOWN"} · ${direction} · ${horizon} · confidence ${confidence} · impact ${impact} · assets ${assets}`;
}

function newsImpactValue(item) {
  return numberOrNull(item?.scores?.event_impact_score) ?? 0;
}

function newsImpactTier(item) {
  const score = newsImpactValue(item);
  if (score >= 85) return { label: "重大", className: "impact-critical" };
  if (score >= 70) return { label: "高", className: "impact-high" };
  if (score >= 50) return { label: "中", className: "impact-medium" };
  return { label: "一般", className: "impact-low" };
}

function calendarImpactValue(item) {
  return CALENDAR_IMPORTANCE_WEIGHTS[String(item?.importance || "").toUpperCase()] ?? 0;
}

function calendarMarketScopeValue(item) {
  return ["FOMC", "MACRO"].includes(String(item?.category || "").toUpperCase()) ? 1 : 0;
}

function compareCalendarRows(left, right) {
  return (
    calendarImpactValue(right) - calendarImpactValue(left)
    || calendarMarketScopeValue(right) - calendarMarketScopeValue(left)
    || Number(String(right?.schedule_precision || "").toUpperCase() === "EXACT")
      - Number(String(left?.schedule_precision || "").toUpperCase() === "EXACT")
    || String(calendarEventTime(left) || left?.event_date || "").localeCompare(String(calendarEventTime(right) || right?.event_date || ""))
  );
}

function calendarImpactTier(item) {
  const importance = String(item?.importance || "UNKNOWN").toUpperCase();
  if (importance === "CRITICAL") return { label: "重大", className: "impact-critical" };
  if (importance === "HIGH") return { label: "高", className: "impact-high" };
  if (importance === "MEDIUM") return { label: "中", className: "impact-medium" };
  return { label: importance === "LOW" ? "一般" : "未评级", className: "impact-low" };
}

function newsChronologyTimestamp(item) {
  return firstValue(item?.times, ["published_at", "event_at", "first_seen_at", "observed_at"]);
}

function canonicalNewsStoryUrl(item) {
  const evidence = Array.isArray(item?.evidence) ? item.evidence : [];
  const raw = firstValue(
    item,
    ["source_url", "url"],
    firstValue(evidence[0], ["url"], ""),
  );
  const text = String(raw || "").trim();
  if (!text) return "";
  try {
    const parsed = new URL(text);
    if (!["http:", "https:"].includes(parsed.protocol)) return text;
    parsed.hash = "";
    const query = [...parsed.searchParams.entries()]
      .sort(([leftKey, leftValue], [rightKey, rightValue]) => (
        leftKey.localeCompare(rightKey) || leftValue.localeCompare(rightValue)
      ));
    parsed.search = "";
    query.forEach(([key, value]) => parsed.searchParams.append(key, value));
    return parsed.toString();
  } catch (_error) {
    return text;
  }
}

function exactNewsStoryKey(item) {
  const projected = String(item?.story_identity || "").trim();
  if (projected) return `PROJECTED\u001f${projected}`;
  const providerStoryId = String(item?.provider_story_id || "").trim();
  const providerAdapter = String(
    item?.symbol_binding?.provider_adapter || item?.provider_adapter || "",
  ).trim().toUpperCase();
  if (providerStoryId && providerAdapter) {
    return `PROVIDER\u001f${providerAdapter}\u001f${providerStoryId}`;
  }
  return [
    "EXACT",
    String(item?.source || "").trim().toLowerCase().replace(/\s+/g, " "),
    String(item?.title || item?.headline || "").trim().toLowerCase().replace(/\s+/g, " "),
    String(newsChronologyTimestamp(item) || ""),
    canonicalNewsStoryUrl(item),
  ].join("\u001f");
}

function dailyNewsDisplayClusterKey(item) {
  const source = String(item?.source || "UNKNOWN").trim().toUpperCase();
  const category = String(item?.category || "OTHER").trim().toUpperCase();
  if (source === "UNKNOWN" || category === "OTHER") {
    return `EXACT\u001f${exactNewsStoryKey(item)}`;
  }
  const bindingStatus = String(item?.symbol_binding?.status || "UNBOUND").trim().toUpperCase();
  if (isUnverifiedNewsSymbolBinding(item)) {
    return [source, "UNVERIFIED_SYMBOL_BINDING"].join("\u001f");
  }
  const verifiedSymbols = bindingStatus === "VERIFIED_PROVIDER_RELATED"
    ? [...new Set(
      (Array.isArray(item?.symbols) ? item.symbols : [])
        .map((symbol) => String(symbol || "").trim().toUpperCase())
        .filter((symbol) => /^[A-Z][A-Z0-9.-]{0,15}$/.test(symbol)),
    )].sort()
    : [];
  return [
    source,
    category,
    verifiedSymbols.length > 0 ? verifiedSymbols.join("/") : "UNVERIFIED_OR_MARKET",
  ].join("\u001f");
}

function dailyMajorNews(value, now = new Date(), limit = NEWS_DIGEST_LIMIT) {
  const today = dateKeyInZone(now, BEIJING_TIME_ZONE);
  if (today === null || !Array.isArray(value)) return [];
  const seen = new Set();
  const exactRows = value
    .filter((item) => dateKeyInZone(newsChronologyTimestamp(item), BEIJING_TIME_ZONE) === today)
    .sort((left, right) => (
      newsImpactValue(right) - newsImpactValue(left)
      || (numberOrNull(right?.confidence) ?? 0) - (numberOrNull(left?.confidence) ?? 0)
      || String(newsChronologyTimestamp(right) || "").localeCompare(String(newsChronologyTimestamp(left) || ""))
    ))
    .filter((item) => {
      const key = exactNewsStoryKey(item);
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
  const clusterCounts = new Map();
  const selected = [];
  for (const item of exactRows) {
    const cluster = dailyNewsDisplayClusterKey(item);
    const count = clusterCounts.get(cluster) || 0;
    if (count >= NEWS_DIGEST_CLUSTER_LIMIT) continue;
    clusterCounts.set(cluster, count + 1);
    selected.push(item);
    if (selected.length >= limit) break;
  }
  return selected;
}

function calendarEventTime(item) {
  return firstValue(item?.times, ["event_at"], firstValue(item, ["event_at", "scheduled_at"]));
}

function calendarDigestRows(value, windowName, limit = CALENDAR_DIGEST_LIMIT) {
  if (!Array.isArray(value)) return [];
  return value
    .filter((item) => calendarEventInWindow(item, windowName))
    .sort(compareCalendarRows)
    .slice(0, limit);
}

function eventTimeProjection(item) {
  const reaction = item?.reaction && typeof item.reaction === "object" ? item.reaction : {};
  const release = reaction.release && typeof reaction.release === "object" ? reaction.release : {};
  return {
    expected: calendarEventTime(item),
    actual: firstValue(release, ["released_at", "observed_at"]),
    published: firstValue(item?.times, ["published_at", "first_seen_at"]),
    precision: researchText(item?.schedule_precision, "UNAVAILABLE", 32).toUpperCase(),
  };
}

function renderNewsChronology() {
  const daily = dailyMajorNews(appState.news);
  const thisWeek = calendarDigestRows(appState.calendar, "this-week");
  const nextWeek = calendarDigestRows(appState.calendar, "next-week");
  renderChronologyRows("daily-major-news-list", daily, "news", "今日暂无有时间证明的新闻。", buildDailyNewsChronologyCard);
  renderChronologyRows("this-week-major-events-list", thisWeek, "this-week", "本周暂无可验证重大事件。", buildCalendarChronologyCard);
  renderChronologyRows("next-week-outlook-list", nextWeek, "next-week", "下周暂无可验证事件。", buildCalendarChronologyCard);
  setText("daily-major-news-count", String(daily.length));
  setText("this-week-major-events-count", String(thisWeek.length));
  setText("next-week-outlook-count", String(nextWeek.length));
  renderClassifierCoverage(appState.news);
}

function renderChronologyRows(targetId, rows, _kind, emptyMessage, builder) {
  const container = byId(targetId);
  if (!container) return;
  container.replaceChildren();
  if (rows.length === 0) {
    container.append(createElement("p", "empty-state", emptyMessage));
    return;
  }
  rows.forEach((item, index) => container.append(builder(item, index)));
}

function renderClassifierCoverage(value) {
  const coverage = classifierCoverage(value);
  const node = byId("news-classifier-summary");
  if (!node) return;
  node.classList.remove("status-up", "status-down", "status-stale", "status-unknown");
  node.classList.add(
    coverage.total > 0 && coverage.deterministic + coverage.structuredPrimary === coverage.total
      ? "status-up"
      : "status-stale",
  );
  const primaryModel = coverage.structuredPrimary > 0
    ? ` · 模型主分类 ${coverage.structuredPrimary}`
    : "";
  const shadowRate = coverage.total > 0
    ? `${((coverage.deepseekShadow / coverage.total) * 100).toFixed(1)}%`
    : "0.0%";
  const comparison = coverage.comparableShadow > 0
    ? ` · 当前可比覆盖 N=${coverage.comparableShadow}（非固定评估队列，不作为模型质量结论）`
    : " · 当前可比覆盖 尚未形成样本";
  node.textContent = `分类器 · 生产确定性 ${coverage.deterministic}/${coverage.total} · DeepSeek shadow ${coverage.deepseekShadow}/${coverage.total} (${shadowRate}) · 五时域预测 ${coverage.shadowPredictions}${comparison}${primaryModel}${coverage.undeclared ? ` · 未声明 ${coverage.undeclared}` : ""} · SUPPORTING_ONLY`;
}

function classifierCoverage(value) {
  const rows = Array.isArray(value) ? value : [];
  const structuredPrimary = rows.filter((item) => item?.classifier === "STRUCTURED_LLM").length;
  const deterministic = rows.filter((item) => item?.classifier === "DETERMINISTIC_RULES").length;
  const deepseekShadow = rows.filter((item) => (
    item?.research_advisory?.classifier === "STRUCTURED_LLM"
  )).length;
  const comparable = rows.filter((item) => (
    item?.classifier === "DETERMINISTIC_RULES"
    && item?.research_advisory?.classifier === "STRUCTURED_LLM"
    && item.research_advisory.classification
    && typeof item.research_advisory.classification === "object"
  ));
  const shadowPredictions = rows.reduce((total, item) => {
    const count = numberOrNull(item?.research_advisory?.shadow_prediction_count);
    return total + (Number.isInteger(count) && count >= 0 ? count : 0);
  }, 0);
  return {
    total: rows.length,
    deepseekShadow,
    structuredPrimary,
    deterministic,
    comparableShadow: comparable.length,
    shadowPredictions,
    undeclared: Math.max(0, rows.length - structuredPrimary - deterministic),
  };
}

function durableShadowCounts(value) {
  const learning = value && typeof value === "object" && !Array.isArray(value)
    ? value
    : {};
  const shadow = learning.shadow_learning && typeof learning.shadow_learning === "object" && !Array.isArray(learning.shadow_learning)
    ? learning.shadow_learning
    : {};
  const counts = shadow.record_counts && typeof shadow.record_counts === "object" && !Array.isArray(shadow.record_counts)
    ? shadow.record_counts
    : {};
  const contract = shadow.prediction_contract && typeof shadow.prediction_contract === "object" && !Array.isArray(shadow.prediction_contract)
    ? shadow.prediction_contract
    : {};
  const count = (...keys) => {
    for (const key of keys) {
      const candidate = numberOrNull(counts[key]);
      if (Number.isInteger(candidate) && candidate >= 0) return candidate;
    }
    return null;
  };
  const theses = count("THESIS", "theses");
  const evidence = count("EVIDENCE", "evidence");
  const predictions = count("PREDICTION", "predictions");
  const outcomes = count("OUTCOME", "outcomes");
  const integrity = shadow.ledger && typeof shadow.ledger === "object" && !Array.isArray(shadow.ledger)
    ? shadow.ledger.integrity_verified
    : null;
  return {
    available: integrity === true && [theses, evidence, predictions, outcomes].every((item) => item !== null),
    integrityVerified: integrity === true,
    theses: theses ?? 0,
    evidence: evidence ?? 0,
    predictions: predictions ?? 0,
    outcomes: outcomes ?? 0,
    challenger: researchText(contract.challenger, "UNAVAILABLE", 160),
    legacyExcluded: validLearningCount(contract.legacy_excluded_count),
    exclusionReasons: contract.exclusion_reasons && typeof contract.exclusion_reasons === "object" && !Array.isArray(contract.exclusion_reasons)
      ? contract.exclusion_reasons
      : {},
  };
}

function durableShadowCountText(counts, key) {
  if (!counts || counts.available !== true || !["predictions", "outcomes"].includes(key)) return "--";
  return formatInteger(counts[key]);
}

function deepseekAdvisorySummary(item) {
  const advisory = item?.research_advisory;
  if (advisory?.classifier !== "STRUCTURED_LLM") return "未覆盖";
  const classification = advisory.classification && typeof advisory.classification === "object"
    ? advisory.classification
    : {};
  const confidence = numberOrNull(classification.confidence);
  const priority = numberOrNull(advisory.research_priority_score);
  return [
    researchText(classification.direction, "UNKNOWN", 24).toUpperCase(),
    researchText(classification.horizon, "UNKNOWN", 32).toUpperCase(),
    `置信度 ${confidence === null ? "--" : `${(confidence <= 1 ? confidence * 100 : confidence).toFixed(1)} / 100`}`,
    `研究优先 ${priority === null ? "--" : priority.toFixed(1)}`,
    "SUPPORTING_ONLY",
  ].join(" · ");
}

function buildDailyNewsChronologyCard(item, index) {
  const card = createElement("article", "chronology-card");
  const tier = newsImpactTier(item);
  const heading = createElement("div", "chronology-card-heading");
  heading.append(
    createElement("span", `impact-chip ${tier.className}`, `#${index + 1} · ${tier.label} ${newsImpactValue(item).toFixed(1)}`),
    createElement("span", "chronology-category", researchText(item?.category, "OTHER", 32)),
  );
  card.append(
    heading,
    createElement("strong", "chronology-title", researchText(item?.title, "未命名新闻", 400)),
    createElement("p", "chronology-summary", researchText(item?.summary, "摘要不可用", 500)),
    createElement("p", "chronology-meta", intelligenceSummary(item)),
    createElement("p", "chronology-meta", `${researchText(item?.source, "UNKNOWN", 80)} · ${(Array.isArray(item?.symbols) ? item.symbols : []).join(" / ") || "MARKET"} · 生产 ${researchText(item?.classifier, "UNDECLARED", 40)} · DeepSeek shadow ${deepseekAdvisorySummary(item)}`),
    buildChronologyTimeGrid({
      expected: item?.times?.event_at,
      actual: item?.times?.published_at,
      published: item?.times?.published_at,
      precision: item?.times?.event_at ? "EVENT_TIME" : "NOT_PROVIDED",
    }),
  );
  return card;
}

function buildCalendarChronologyCard(item, index) {
  const card = createElement("article", "chronology-card");
  const tier = calendarImpactTier(item);
  const heading = createElement("div", "chronology-card-heading");
  heading.append(
    createElement("span", `impact-chip ${tier.className}`, `#${index + 1} · ${tier.label}`),
    createElement("span", "chronology-category", researchText(item?.category, "EVENT", 32)),
  );
  card.append(
    heading,
    createElement("strong", "chronology-title", researchText(item?.title, "未命名事件", 400)),
    createElement("p", "chronology-meta", `${researchText(item?.source, "UNKNOWN", 100)} · ${(Array.isArray(item?.symbols) ? item.symbols : []).join(" / ") || item?.country || "MARKET"} · SUPPORTING_ONLY`),
    createElement("p", "chronology-meta", intelligenceSummary(item)),
    buildChronologyTimeGrid(eventTimeProjection(item)),
  );
  return card;
}

function buildChronologyTimeGrid(projection) {
  const grid = createElement("dl", "chronology-time-grid");
  [
    ["预计发生", projection.expected ? `${formatDualMarketTime(projection.expected)} · ${projection.precision}` : "未提供 / 不推断"],
    ["实际发布/释放", projection.actual ? formatDualMarketTime(projection.actual) : "待验证"],
    ["来源发布时间", projection.published ? formatDualMarketTime(projection.published) : "未提供"],
  ].forEach(([label, value]) => {
    grid.append(createElement("dt", "", label), createElement("dd", "", value));
  });
  return grid;
}

function renderNewsLists() {
  const filtered = sortedNews().filter((item) => (
    (appState.newsFilter === "ALL" || item.status === appState.newsFilter)
    && (appState.newsCategoryFilter === "ALL" || item.category === appState.newsCategoryFilter)
    && (appState.newsSourceFilter === "ALL" || item.source === appState.newsSourceFilter)
    && (appState.newsSymbolFilter === "ALL" || (item.symbols || []).includes(appState.newsSymbolFilter))
  ));
  const research = filtered
    .filter((item) => item.research_pool === true && Number.isInteger(item.research_rank))
    .sort((left, right) => left.research_rank - right.research_rank)
    .slice(0, 10);
  const actionCount = research.filter((item) => item.action_pool === true).length;
  setText("news-count", `${research.length} research · ${actionCount} action`);
  const realtime = byId("realtime-news-list");
  const opportunities = byId("news-opportunity-list");
  realtime.replaceChildren();
  opportunities.replaceChildren();
  if (filtered.length === 0) {
    const empty = createElement("p", "empty-state", "当前过滤条件下没有可显示的只读研究。 ");
    realtime.append(empty);
    opportunities.append(createElement("p", "empty-state", "等待 provider 提供经规范化的新闻。"));
  }
  filtered.forEach((item) => {
    const selected = item.id === appState.selectedNewsId;
    const mini = document.createElement("button");
    mini.type = "button";
    mini.className = `news-mini${selected ? " is-selected" : ""}`;
    mini.title = "查看新闻详情";
    mini.append(createElement("strong", "", item.title || "Untitled observation"));
    mini.append(createElement("span", "", `${statusLabel(item.status)} · ${formatTime(item.times?.published_at || item.times?.event_at)}`));
    mini.addEventListener("click", () => selectNews(item.id));
    realtime.append(mini);
  });
  if (research.length === 0 && filtered.length > 0) {
    opportunities.append(createElement("p", "empty-state", "当前只有发现层新闻；尚无满足 Top 10 研究池约束的条目。"));
  }
  research.forEach((item) => {
    opportunities.append(buildNewsCard(item, item.research_rank - 1, item.id === appState.selectedNewsId));
  });
  renderNewsDetail(appState.news.find((item) => item.id === appState.selectedNewsId));
}

function sortedNews() {
  return [...appState.news].sort((left, right) => scoreValue(right, "combined_opportunity_score") - scoreValue(left, "combined_opportunity_score"));
}

function renderNewsFilterOptions() {
  setNewsFilterOptions("news-category-filter", "全部分类", appState.news.map((item) => item.category), "newsCategoryFilter");
  setNewsFilterOptions("news-source-filter", "全部文章来源", appState.news.map((item) => item.source), "newsSourceFilter");
  setNewsFilterOptions("news-symbol-filter", "全部标的", appState.news.flatMap((item) => Array.isArray(item.symbols) ? item.symbols : []), "newsSymbolFilter");
}

function setNewsFilterOptions(id, allLabel, values, stateKey) {
  const select = byId(id);
  if (!select) return;
  const selected = appState[stateKey];
  const options = [...new Set(values.filter((value) => typeof value === "string" && value))].sort();
  select.replaceChildren(createElement("option", "", allLabel));
  select.firstChild.value = "ALL";
  options.forEach((value) => {
    const option = createElement("option", "", value);
    option.value = value;
    select.append(option);
  });
  if (options.includes(selected)) select.value = selected;
  else appState[stateKey] = "ALL";
}

function scoreValue(item, key) {
  return numberOrNull(item?.scores?.[key]) ?? -1;
}

function statusLabel(value) {
  const status = String(value || "PROVISIONAL").toUpperCase();
  return ["PROVISIONAL", "CONFIRMED", "MARKET_CONFIRMED", "CONFLICTED", "NO_TRADE"].includes(status) ? status : "PROVISIONAL";
}

function statusClass(value) {
  return `status-${statusLabel(value).toLowerCase().replace("_", "-")}`;
}

function buildNewsCard(item, index, selected) {
  const card = createElement("article", `news-card${selected ? " is-selected" : ""}`);
  const head = createElement("div", "news-card-head");
  const pool = item.action_pool === true && Number.isInteger(item.action_rank)
    ? `OPEN OBS #${item.action_rank}`
    : "RESEARCH";
  head.append(createElement("span", "news-meta", `#${index + 1} · ${pool} · ${(item.symbols || []).join(" / ") || "MARKET"}`));
  head.append(createElement("span", `status-chip ${statusClass(item.status)}`, statusLabel(item.status)));
  card.append(head, createElement("h3", "", item.title || "Untitled observation"));
  card.append(createElement("p", "", item.summary || "暂无摘要；请检查证据详情。"));
  const scores = createElement("div", "score-row");
  [["事件影响", "event_impact_score"], ["期权可交易", "option_tradability_score"], ["综合机会", "combined_opportunity_score"]].forEach(([label, key]) => {
    const score = createElement("span", "", label);
    score.append(createElement("strong", "", formatScore(item.scores?.[key])));
    scores.append(score);
  });
  card.append(scores);
  card.addEventListener("click", () => selectNews(item.id));
  return card;
}

function formatScore(value) {
  const score = numberOrNull(value);
  return score === null ? "--" : `${score.toFixed(1)} / 100`;
}

function formatPoolCount(value, maximum) {
  const count = numberOrNull(value);
  return `${count !== null && Number.isInteger(count) && count >= 0 ? count : 0}/${maximum}`;
}

function selectNews(identifier) {
  appState.selectedNewsId = identifier;
  renderNewsLists();
}

function renderNewsDetail(item) {
  const container = byId("news-detail-content");
  container.replaceChildren();
  if (!item) {
    container.append(createElement("p", "empty-state", "选择一条新闻以查看证据与关联研究。"));
    return;
  }
  container.append(createElement("h3", "", item.title || "Untitled observation"));
  container.append(createElement("p", "news-meta", [item.category, item.source, (item.symbols || []).join(" / ")].filter(Boolean).join(" · ")));
  container.append(createElement("p", "detail-summary", item.summary || "暂无摘要。"));
  container.append(createElement("p", "news-meta", `事件智能 · ${intelligenceSummary(item)}`));
  const times = createElement("div", "time-grid");
  [["事件", "event_at"], ["发布", "published_at"], ["首次发现", "first_seen_at"], ["分析完成", "analysis_completed_at"], ["接收", "received_at"], ["观察", "observed_at"]].forEach(([label, key]) => {
    const cell = createElement("div", "", label);
    const time = document.createElement("time");
    time.textContent = formatTime(item.times?.[key]);
    cell.append(time);
    times.append(cell);
  });
  container.append(times);
  const discoveryLatency = numberOrNull(item.latency?.published_to_first_seen_ms);
  const analysisLatency = numberOrNull(item.latency?.first_seen_to_analysis_ms);
  container.append(createElement(
    "p",
    "news-meta",
    `发现延迟 ${discoveryLatency === null ? "--" : `${discoveryLatency.toFixed(0)}ms`} · 分析延迟 ${analysisLatency === null ? "--" : `${analysisLatency.toFixed(0)}ms`}`,
  ));
  container.append(createElement("p", "detail-subhead", `决策权限 · ${item.decision_authority || "SUPPORTING_ONLY"}`));
  container.append(createElement("p", "news-meta", `分类器 · ${item.classifier || "未声明 / 按模型不可用处理"}`));
  container.append(createElement("p", "news-meta", `DeepSeek shadow · ${deepseekAdvisorySummary(item)}`));
  const binding = item.symbol_binding && typeof item.symbol_binding === "object" ? item.symbol_binding : {};
  const bindingStatus = String(binding.status || "SOURCE_DECLARED").toUpperCase();
  const bindingAdapter = String(binding.provider_adapter || "来源").toUpperCase();
  const bindingMessage = bindingStatus === "VERIFIED_PROVIDER_RELATED"
    ? `标的绑定 · ${bindingAdapter} 已验证上游关联字段（SUPPORTING_ONLY）`
    : bindingStatus === "UNBOUND"
      ? "标的绑定 · 无可验证公司标的；按 MARKET 普通新闻展示"
      : isUnverifiedNewsSymbolBinding(item)
        ? `标的绑定 · 未验证（${binding.provider_adapter ? bindingAdapter : "旧来源未证明"}）；按 MARKET 普通新闻展示，不进入标的 watch`
        : "标的绑定 · 来源声明（SUPPORTING_ONLY）";
  container.append(createElement("p", "news-meta", bindingMessage));
  const researchProxy = item.research_proxy_binding && typeof item.research_proxy_binding === "object"
    ? item.research_proxy_binding
    : {};
  if (String(researchProxy.proxy_symbol || "").trim()) {
    container.append(createElement(
      "p",
      "news-meta",
      `宏观研究代理 · ${researchText(researchProxy.proxy_symbol, "--", 15)} · 仅进入确定性股票研究因子，不改变 eligibility、risk、approval 或下单权限`,
    ));
  }
  container.append(createElement("p", "detail-subhead", "证据"));
  const evidence = createElement("ul", "detail-list");
  const evidenceItems = Array.isArray(item.evidence) ? item.evidence : [];
  (evidenceItems.length ? evidenceItems : [{ source: "暂无已规范化证据", title: "" }]).forEach((entry) => {
    evidence.append(createElement("li", "", [entry.source, entry.title, entry.observed_at ? formatTime(entry.observed_at) : null].filter(Boolean).join(" · ")));
  });
  container.append(evidence, createElement("p", "detail-subhead", "关联期权研究（只读）"));
  const related = createElement("ul", "detail-list");
  const research = Array.isArray(item.related_options) ? item.related_options : [];
  (research.length ? research : [{ summary: "暂无关联期权研究。" }]).forEach((entry) => {
    related.append(createElement("li", "", [entry.symbol, entry.summary, entry.asof ? formatTime(entry.asof) : null].filter(Boolean).join(" · ")));
  });
  container.append(related);
}

function activeOptionPool() {
  return appState.optionPoolStage === "open-repriced"
    ? appState.openRepricedOptions
    : appState.preMarketOptions;
}

function renderPreselectionCoverage() {
  const coverage = appState.preselectionCoverage || normalizePreselectionCoverage(
    null,
    appState.preMarketOptions.length,
    appState.openRepricedOptions.length,
  );
  setText("option-preselection-ledger-source", coverage.source);
  setText("option-preselection-ledger-status", coverage.status);
  setText("option-open-observation-status", `OPEN · ${coverage.open_observation_status}`);
  setText(
    "option-preselection-ledger-reason",
    preselectionCoverageMessage(coverage, appState.optionPoolStage),
  );
  setText(
    "option-preselection-ledger-lineage",
    `run ${coverage.latest_run_id || "--"} · head ${formatDigestPreview(coverage.latest_head_hash)} · freeze ${formatTime(coverage.freeze_slot)} · open batch ${coverage.latest_open_batch_id || "--"} · batch head ${formatDigestPreview(coverage.latest_open_batch_head_hash)} · reprice ${formatTime(coverage.reprice_slot)}`,
  );
  setPreselectionStatusClass("option-preselection-ledger-status", coverage.status);
  setPreselectionStatusClass("option-open-observation-status", coverage.open_observation_status);
}

function preselectionCoverageMessage(coverage, stage) {
  const value = coverage && typeof coverage === "object" ? coverage : {};
  const available = Number.isInteger(value.available_count) ? value.available_count : 0;
  const observed = Number.isInteger(value.open_count) ? value.open_count : 0;
  if (stage === "open-repriced") {
    if (value.atomic_batch_available === true) {
      return `已验证 ${observed}/${available} 条同一原子开盘批次；producer AVAILABLE，仍为 SUPPORTING_ONLY。`;
    }
    if (value.open_observation_status === "NOT_STARTED") {
      return "盘前结构已冻结；OPEN_REPRICE 尚未发生，保持 NO_TRADE。";
    }
    if (value.open_observation_status === "PARTIAL") {
      return `已记录 ${observed}/${available} 条开盘 observation；未观察结构保持 NO_TRADE。`;
    }
    if (value.open_observation_status === "AVAILABLE") {
      return `已有 ${observed} 条开盘只读 observation，但原子绑定未通过：${value.atomic_batch_blocker || "UNKNOWN"}。`;
    }
    return "开盘 observation 不可用；不会用盘前报价或 Ranking Top 10 代替。";
  }
  if (value.status === "UNAVAILABLE" || available === 0) {
    return "独立盘前 Top‑10 ledger 未就绪；未从 Ranking Top 10 回填。";
  }
  if (available < 10) {
    return `独立盘前 ledger 已冻结 ${available}/10；其余结构不推断。`;
  }
  return "独立盘前 ledger 已冻结 10/10；仅供研究，仍为 SUPPORTING_ONLY。";
}

function setPreselectionStatusClass(identifier, status) {
  const node = byId(identifier);
  if (!node) return;
  const normalized = String(status || "UNAVAILABLE").toUpperCase();
  node.className = `status-chip ${normalized === "AVAILABLE" ? "status-up" : normalized === "PARTIAL" || normalized === "NOT_STARTED" ? "status-stale" : "status-no-trade"}`;
}

function formatDigestPreview(value) {
  const digest = ledgerDigest(value);
  return digest ? `${digest.slice(0, 10)}…${digest.slice(-6)}` : "--";
}

function renderOptionPreselections() {
  const container = byId("option-preselection-list");
  const rows = activeOptionPool();
  if (!container) return;
  renderPreselectionCoverage();
  document.querySelectorAll("[data-option-pool]").forEach((tab) => {
    tab.classList.toggle("is-active", tab.dataset.optionPool === appState.optionPoolStage);
  });
  if (!rows.some((item) => item.preselection_id === appState.selectedOptionId)) {
    appState.selectedOptionId = rows[0]?.preselection_id ?? null;
  }
  container.replaceChildren();
  if (rows.length === 0) {
    container.append(createElement(
      "p",
      "empty-state",
      preselectionCoverageMessage(appState.preselectionCoverage, appState.optionPoolStage),
    ));
    renderOptionPreselectionDetail(null);
    return;
  }
  rows.forEach((item, index) => {
    container.append(buildOptionPreselectionCard(item, index));
  });
  renderOptionPreselectionDetail(
    rows.find((item) => item.preselection_id === appState.selectedOptionId),
  );
}

function buildOptionPreselectionCard(item, index) {
  const selected = item.preselection_id === appState.selectedOptionId;
  const card = document.createElement("button");
  card.type = "button";
  card.className = `option-preselection-card${selected ? " is-selected" : ""}`;
  card.title = "查看逐腿只读报价证据、风险与条件；不会创建审批或指令";
  const head = createElement("div", "option-preselection-head");
  const rank = numberOrNull(item.repriced_rank ?? item.research_rank) ?? index + 1;
  const pool = item.action_pool_eligible === true && Number.isInteger(item.action_rank)
    ? `OPEN OBS #${item.action_rank}`
    : "RESEARCH ONLY";
  head.append(
    createElement("strong", "", `#${rank} · ${item.underlying || "--"} · ${item.strategy_type || "未定义结构"}`),
    createElement("span", item.action_pool_eligible === true ? "status-chip status-up" : "status-chip status-stale", pool),
  );
  const metrics = createElement("div", "option-preselection-metrics");
  [
    ["最大亏损", formatMoney(item.maximum_loss_usd)],
    ["成本后 EV", formatMoney(item.cost_after_ev_usd, { signed: true })],
    ["最旧报价", formatTime(item.oldest_quote_asof)],
  ].forEach(([label, value]) => {
    const metric = createElement("span", "", label);
    metric.append(createElement("strong", "", value));
    metrics.append(metric);
  });
  const blockerCount = Array.isArray(item.blockers) ? item.blockers.length : 0;
  card.append(
    head,
    createElement("p", "", item.research_summary || "暂无研究摘要；保持只读。"),
    metrics,
    createElement(
      "span",
      blockerCount ? "option-blocker-summary has-blocker" : "option-blocker-summary",
      blockerCount
        ? `${blockerCount} 个硬阻断 · NO_TRADE`
        : item.phase === "OPEN_REPRICED"
          ? "观察字段完整 · SUPPORTING_ONLY"
          : "盘前冻结结构 · RESEARCH_ONLY · SUPPORTING_ONLY",
    ),
  );
  card.addEventListener("click", () => {
    appState.selectedOptionId = item.preselection_id;
    renderOptionPreselections();
  });
  return card;
}

function renderOptionPreselectionDetail(item) {
  const container = byId("option-preselection-detail");
  if (!container) return;
  container.replaceChildren();
  if (!item) {
    container.append(createElement("p", "empty-state", "选择一个合约级条件式预选以查看逐腿只读报价证据、Greeks、流动性与退出条件。"));
    return;
  }
  container.append(
    createElement("h3", "", `${item.underlying || "--"} · ${item.strategy_type || "未定义结构"}`),
    createElement("p", "news-meta", `${item.phase || "UNKNOWN"} · ${item.decision_authority || "SUPPORTING_ONLY"} · 不可审批 · 不可创建指令`),
  );

  const summary = createElement("div", "option-risk-grid");
  [
    ["最大亏损", formatMoney(item.maximum_loss_usd)],
    ["估算成本", formatMoney(item.estimated_cost_usd)],
    ["成本后 EV", formatMoney(item.cost_after_ev_usd, { signed: true })],
    ["风险调整 EV", formatQuote(item.risk_adjusted_ev)],
    ["Ledger run", item.ledger_lineage?.run_id || "--"],
    ["Open batch", item.ledger_lineage?.batch_id || "--"],
    ["Quote batch", item.quote_batch_id || "--"],
    ["最旧报价", formatTime(item.oldest_quote_asof)],
  ].forEach(([label, value]) => {
    const cell = createElement("div", "", label);
    cell.append(createElement("strong", "", value));
    summary.append(cell);
  });
  container.append(summary, createElement("p", "detail-subhead", "交易条件（研究约束）"));
  const conditions = createElement("dl", "option-condition-list");
  [
    ["入场", item.entry_condition],
    ["论点失效", item.invalidation_condition],
    ["获利", item.profit_target_condition],
    ["止损", item.stop_loss_condition],
  ].forEach(([label, value]) => {
    conditions.append(createElement("dt", "", label), createElement("dd", "", value || "缺失 · NO_TRADE"));
  });
  const lineage = item.ledger_lineage && typeof item.ledger_lineage === "object"
    ? item.ledger_lineage
    : {};
  container.append(
    conditions,
    createElement("p", "detail-subhead", "最小账本 lineage（只读）"),
    createElement(
      "p",
      "news-meta",
      `run ${lineage.run_id || "--"} · row id ${lineage.row_id || "--"} · head ${formatDigestPreview(lineage.head_hash)} · row ${formatDigestPreview(lineage.row_hash)} · observation ${formatDigestPreview(lineage.observation_hash)} · batch ${lineage.batch_id || "--"} · batch head ${formatDigestPreview(lineage.batch_head_hash)} · scheduled ${formatTime(lineage.scheduled_for)} · quote batch ${lineage.quote_batch_id || item.quote_batch_id || "--"}`,
    ),
    createElement("p", "detail-subhead", "逐腿只读报价证据 / Greeks / 流动性"),
  );

  const legs = createElement("div", "option-leg-list");
  const rows = Array.isArray(item.legs) ? item.legs : [];
  if (rows.length === 0) {
    legs.append(createElement("p", "empty-state", "逐腿合约或报价缺失 · NO_TRADE"));
  }
  rows.forEach((leg, index) => legs.append(buildOptionLegRow(leg, index)));
  container.append(legs);

  const blockers = Array.isArray(item.blockers) ? item.blockers : [];
  container.append(createElement("p", "detail-subhead", "硬阻断"));
  const blockerList = createElement("ul", "detail-list");
  (blockers.length ? blockers : ["NONE_REPORTED · 仍需主决策流水线独立复核"]).forEach((reason) => {
    blockerList.append(createElement("li", "", String(reason)));
  });
  container.append(blockerList);
}

function buildOptionLegRow(leg, index) {
  const row = createElement("article", "option-leg-row");
  const identity = [
    `腿 ${index + 1}`,
    leg.side,
    `${formatInteger(leg.ratio)}x`,
    leg.expiry,
    leg.strike,
    leg.right,
  ].filter((value) => value !== null && value !== undefined && value !== "").join(" · ");
  row.append(createElement("strong", "", identity || `腿 ${index + 1} · 合约定义缺失`));
  row.append(createElement(
    "span",
    "news-meta",
    `conId ${leg.con_id ?? "--"} · local ${leg.local_symbol || "--"} · class ${leg.trading_class || "--"} · multiplier ${leg.multiplier ?? "--"} · exchange ${leg.exchange || "--"} · bid ${formatQuote(leg.bid)} / ask ${formatQuote(leg.ask)} · DTE ${formatInteger(leg.dte)} · quote batch ${leg.quote_batch_id || "--"} · ${formatTime(leg.quote_asof)}`,
  ));
  const greeks = createElement("div", "option-leg-metrics");
  [
    ["IV", formatPercent(leg.implied_volatility)],
    ["Δ", formatQuote(leg.delta)],
    ["Γ", formatQuote(leg.gamma)],
    ["Θ", formatQuote(leg.theta)],
    ["Vega", formatQuote(leg.vega)],
    ["Vol", formatInteger(leg.volume)],
    ["OI", formatInteger(leg.open_interest)],
  ].forEach(([label, value]) => greeks.append(createElement("span", "", `${label} ${value}`)));
  row.append(greeks);
  return row;
}

function renderCalendarList() {
  const container = byId("calendar-list");
  if (!container) return;
  container.replaceChildren();
  const events = appState.calendar.filter((item) => calendarEventInWindow(item, appState.calendarWindow));
  const visibleEvents = events.sort(compareCalendarRows).slice(0, CALENDAR_DETAIL_LIMIT);
  setText(
    "calendar-list-summary",
    `按重要度、市场级事件、时间精度排序 · 展示 ${visibleEvents.length} / ${events.length}`,
  );
  if (events.length === 0) {
    container.append(createElement("p", "empty-state", "此范围没有已规范化事件。"));
    return;
  }
  visibleEvents.forEach((item) => {
    const reaction = normalizeCalendarReaction(item);
    const row = createElement("article", "calendar-row");
    const header = createElement("div", "calendar-row-header");
    const text = createElement("div", "calendar-event-copy");
    text.append(createElement("strong", "", item.title || "Untitled event"));
    const eventAt = item.times?.event_at || item.event_at || item.scheduled_at;
    const precision = item.schedule_precision || (item.is_estimated ? "ESTIMATED" : "EXACT");
    const schedule = eventAt
      ? `${formatDualMarketTime(eventAt)} · ${precision}`
      : [item.event_date, item.event_timezone, item.schedule_precision].filter(Boolean).join(" · ") || "时间未确认";
    const session = item.report_session ? ` · ${item.report_session}` : "";
    text.append(createElement("span", "", `${(item.symbols || []).join(" / ") || item.country || "MARKET"} · ${schedule}`));
    text.append(createElement(
      "span",
      "news-meta",
      `${item.source || "来源未声明"} · ${item.category || "UNCATEGORIZED"}${session} · SUPPORTING_ONLY · observed ${formatTime(item.times?.observed_at || item.observed_at)}`,
    ));
    text.append(createElement("span", "news-meta", `事件智能 · ${intelligenceSummary(item)}`));
    const statuses = createElement("div", "calendar-statuses");
    statuses.append(
      createElement("span", `status-chip ${statusClass(item.status)}`, statusLabel(item.status)),
      createElement(
        "span",
        `status-chip ${reactionStatusClass(reaction.status)}`,
        `反应 ${reaction.status}`,
      ),
    );
    header.append(text, statuses);
    row.append(header, buildCalendarReactionDetails(reaction));
    container.append(row);
  });
}

function buildCalendarReactionDetails(reaction) {
  const details = createElement("div", "calendar-reaction-details");
  const stage = createElement("div", "calendar-stage-line");
  stage.append(
    createElement("span", "calendar-stage-chip", `当前阶段 · ${reaction.currentStage}`),
    createElement(
      "span",
      `status-chip ${reaction.decision === "OBSERVATION_ONLY" ? "status-up" : "status-no-trade"}`,
      reaction.decision,
    ),
    createElement("span", "calendar-authority-label", reaction.decisionAuthority),
  );
  const analysisState = reaction.analysisAvailable === true && reaction.status === "READY"
    ? "公布后分析证据已提供 · 仍为 SUPPORTING_ONLY"
    : `当前只证明事件预告；尚未证明 actual → surprise → 市场反应 → 期权重评闭环${reaction.noTradeReasons.length > 0 ? ` · ${reaction.noTradeReasons.join(" · ")}` : " · REACTION_ANALYSIS_UNAVAILABLE"}`;
  stage.append(createElement("span", "calendar-authority-label", analysisState));

  const grid = createElement("div", "calendar-reaction-grid");
  appendCalendarReactionField(
    grid,
    "期望 vs 官方实际",
    `期望 · ${reaction.expectation.metric} · ${reactionMeasurement(reaction.expectation.value, reaction.expectation.unit)}`,
    `官方 · ${reactionMeasurement(reaction.officialActual.value, reaction.officialActual.unit)} · revision ${reaction.officialActual.revision}`,
    `期望 observed ${reactionTimeLabel(reaction.expectation.observedAt)} · released ${reactionTimeLabel(reaction.officialActual.releasedAt)}`,
  );
  appendCalendarReactionField(
    grid,
    "Surprise",
    `Δ ${reaction.surprise.delta} · relative ${reaction.surprise.relativeDelta}`,
    `assessed ${reactionTimeLabel(reaction.surprise.assessedAt)}`,
  );
  appendCalendarReactionField(
    grid,
    "市场反应窗口",
    `${reactionTimeLabel(reaction.marketReactionWindow.start)} → ${reactionTimeLabel(reaction.marketReactionWindow.end)}`,
    `evidence asof ${reactionTimeLabel(reaction.marketReactionWindow.evidenceAsof)}`,
  );
  const candidate = reaction.optionReevaluation.candidateHash === REACTION_UNAVAILABLE
    ? REACTION_UNAVAILABLE
    : shortHash(reaction.optionReevaluation.candidateHash);
  appendCalendarReactionField(
    grid,
    "期权重评",
    reaction.optionReevaluation.optionId,
    `candidate ${candidate} · evidence asof ${reactionTimeLabel(reaction.optionReevaluation.evidenceAsof)}`,
    `observed ${reactionTimeLabel(reaction.optionReevaluation.observedAt)}`,
  );

  const noTrade = createElement(
    "div",
    `calendar-no-trade${reaction.decision === "NO_TRADE" ? " is-blocked" : ""}`,
  );
  const reasonHeading = reaction.decision === "OBSERVATION_ONLY"
    ? "NO_TRADE 原因 · 当前 OBSERVATION_ONLY"
    : "NO_TRADE 原因";
  noTrade.append(createElement("strong", "", reasonHeading));
  const reasonList = createElement("ul", "calendar-reason-list");
  const reasons = reaction.noTradeReasons.length > 0
    ? reaction.noTradeReasons
    : [reaction.reasonsState];
  reasons.forEach((reason) => reasonList.append(createElement("li", "", reason)));
  noTrade.append(reasonList);
  details.append(stage, grid, noTrade);
  return details;
}

function appendCalendarReactionField(container, label, ...lines) {
  const field = createElement("div", "calendar-reaction-field");
  field.append(createElement("span", "calendar-reaction-label", label));
  lines.forEach((line, index) => {
    field.append(createElement(index === 0 ? "strong" : "small", "", line));
  });
  container.append(field);
}

function reactionMeasurement(value, unit) {
  if (value === REACTION_UNAVAILABLE && unit === REACTION_UNAVAILABLE) {
    return REACTION_UNAVAILABLE;
  }
  return `${value} ${unit}`;
}

function reactionTimeLabel(value) {
  return value === REACTION_UNAVAILABLE ? REACTION_UNAVAILABLE : formatTime(value);
}

function reactionStatusClass(status) {
  if (status === "READY") return "status-up";
  if (status === "CONFLICTED" || status === "NO_TRADE") return "status-down";
  return "status-stale";
}

function calendarEventInWindow(item, windowName, now = new Date()) {
  const membership = {
    "this-week": "THIS_WEEK",
    "next-week": "NEXT_WEEK",
    "two-weeks": "FUTURE_TWO_WEEKS",
  }[windowName];
  if (membership && Array.isArray(item?.windows) && item.windows.length > 0) {
    return item.windows.includes(membership);
  }
  return isInCalendarWindow(item?.times?.event_at, windowName, now);
}

function isInCalendarWindow(value, windowName = appState.calendarWindow, now = new Date()) {
  const eventTime = new Date(value).getTime();
  if (!Number.isFinite(eventTime)) return windowName === "two-weeks";
  const { from, until } = calendarWindowBounds(windowName, now);
  return eventTime >= from.getTime() && eventTime < until.getTime();
}

function calendarWindowBounds(windowName, now = new Date()) {
  const today = new Date(now);
  today.setHours(0, 0, 0, 0);
  if (windowName === "two-weeks") {
    const until = new Date(today);
    until.setDate(today.getDate() + 14);
    return { from: today, until };
  }
  const weekStart = new Date(today);
  weekStart.setDate(today.getDate() - ((today.getDay() + 6) % 7));
  const from = new Date(weekStart);
  if (windowName === "next-week") from.setDate(from.getDate() + 7);
  const until = new Date(from);
  until.setDate(from.getDate() + 7);
  return { from, until };
}

function isUnverifiedSymbolBindingStatus(value) {
  const status = String(value || "").trim().toUpperCase();
  return status === "PROVIDER_RELATED_UNVERIFIED" || status.startsWith("UNVERIFIED");
}

function isUnverifiedNewsSymbolBinding(item) {
  const binding = item?.symbol_binding && typeof item.symbol_binding === "object"
    ? item.symbol_binding
    : {};
  const status = String(binding.status || "").trim().toUpperCase();
  if (isUnverifiedSymbolBindingStatus(status)) return true;
  const bindingReason = String(binding.reason || "").trim().toUpperCase();
  const affectedAssetsReason = String(
    item?.intelligence?.affected_assets?.reason || "",
  ).trim().toUpperCase();
  return status === "SOURCE_DECLARED"
    && [bindingReason, affectedAssetsReason].includes("SYMBOL_BINDING_UNVERIFIED");
}

function analysisBackfillState(snapshot) {
  const source = snapshot && typeof snapshot === "object" && !Array.isArray(snapshot)
    ? snapshot
    : {};
  const backfill = source.analysis_backfill && typeof source.analysis_backfill === "object"
    && !Array.isArray(source.analysis_backfill)
    ? source.analysis_backfill
    : {};
  const integrity = backfill.integrity && typeof backfill.integrity === "object"
    && !Array.isArray(backfill.integrity)
    ? backfill.integrity
    : {};
  const verifiedRows = Math.max(0, Math.trunc(numberOrNull(integrity.verified_rows) || 0));
  const remainingRows = Math.max(0, Math.trunc(numberOrNull(integrity.remaining_rows) || 0));
  const status = String(backfill.status || "UNAVAILABLE").trim().toUpperCase();
  const reason = String(backfill.reason || "").trim().toUpperCase();
  return {
    status,
    reason,
    verifiedRows,
    remainingRows,
    totalRows: verifiedRows + remainingRows,
    restoring: (
      status === "PENDING"
      && reason === "ANALYSIS_LEDGER_INTEGRITY_PENDING"
      && integrity.complete !== true
    ),
  };
}

export {
  analysisBackfillState,
  afterHoursLegEvidence,
  afterHoursMarketDataTypeLabel,
  brokerReviewModeText,
  candidateChallengeGate,
  candidateUnderlying,
  candidateView,
  calendarDigestRows,
  calendarEventInWindow,
  calendarImpactValue,
  calendarMarketScopeValue,
  calendarWindowBounds,
  classifierCoverage,
  dailyFunnelTruth,
  dailyMajorNews,
  dailyRunPresentation,
  deepseekAdvisorySummary,
  deepseekRuntimeSummary,
  researchAllocationSummary,
  scanOperationalTimingSummary,
  scannerSourceEvidenceSummary,
  durableShadowCounts,
  durableShadowCountText,
  deriveDefinedRiskVerticalPlan,
  enforceControlSnapshotFreshness,
  eventTimeProjection,
  fetchJson,
  fetchResearchJson,
  fetchAfterHoursIndicative,
  fetchResearchTop10,
  formatDualMarketTime,
  isUnverifiedNewsSymbolBinding,
  isUnverifiedSymbolBindingStatus,
  managementEmptyStateMessage,
  holdingsCloseView,
  buildHoldingsClosePreview,
  expireHoldingsClosePreviews,
  newsImpactValue,
  normalizeCalendarReaction,
  normalizeBrokerState,
  normalizeCandidateReasonBuckets,
  normalizeOutcomeHorizons,
  outcomeProcessingSummary,
  normalizeNewsSourceHealth,
  normalizeOptionStructurePool,
  normalizeSourceRuntime,
  intelligenceSummary,
  learningEvaluationStage,
  normalizePreselectionCoverage,
  normalizePositionDisplayState,
  positionManagementTruth,
  normalizeResearchTop10,
  researchFreshness,
  researchTop10StageSummary,
  buildOverviewResearchCard,
  buildResearchTop10Card,
  normalizeReactionProviderSummary,
  overviewBlockedActionSummary,
  brokerDataChainTruth,
  overviewResearchAvailability,
  overviewResearchRows,
  recommendationGateOpen,
  optionPoolRows,
  optionStructureThesisBoundary,
  preselectionClientProjectionValid,
  preselectionCoverageMessage,
  readinessTruthModel,
  featureDataChainTruth,
  readinessBadgePresentation,
  top10ProducerTruth,
  actionControlGate,
  approvalWorkflowLocked,
  applyApprovalWorkflowLock,
  groupRankedCandidates,
  lockApprovalWorkflow,
  markControlSnapshotSucceeded,
  renderLearning,
  renderNewsSourceHealth,
  renderNewsPublication,
  refreshControlSnapshot,
  requestRankOneChallenge,
  synchronizeActionControls,
  updateApprovalState,
};

function actionControlGate(context = {}, now = Date.now()) {
  const lastPollAtMs = numberOrNull(context.lastControlPollAtMs);
  const lastSuccessAtMs = numberOrNull(context.lastSuccessfulControlAtMs);
  const snapshotAgeMs = lastSuccessAtMs === null ? null : now - lastSuccessAtMs;
  return String(context.readinessStatus || "DEGRADED").toUpperCase() === "READY"
    && String(context.scanDecision || "NO_TRADE").toUpperCase() !== "NO_TRADE"
    && context.approvalEnabled === true
    && context.strategyNavReady === true
    && String(context.brokerState || "PARTIAL").toUpperCase() === "FRESH"
    && context.lastControlPollSucceeded === true
    && lastPollAtMs !== null
    && lastSuccessAtMs !== null
    && snapshotAgeMs >= 0
    && snapshotAgeMs <= CONTROL_SNAPSHOT_STALE_AFTER_MS;
}

function candidateChallengeGate(candidate = {}, context = {}, now = Date.now()) {
  const sourceHealth = candidate.sourceHealth || candidate.source_health || {};
  const accountCapacity = candidate.accountCapacity || candidate.account_capacity || {};
  return actionControlGate(context, now)
    && String(sourceHealth.status || "UNAVAILABLE").toUpperCase() === "READY"
    && String(accountCapacity.status || "UNAVAILABLE").toUpperCase() === "READY";
}

function approvalWorkflowKey(approval = {}) {
  const rankingSnapshotId = String(approval.rankingSnapshotId || "").trim();
  const candidateId = String(approval.candidateId || "").trim();
  return rankingSnapshotId && candidateId
    ? `${rankingSnapshotId}\u0000${candidateId}`
    : null;
}

function approvalWorkflowLocked(approval = {}) {
  const key = approvalWorkflowKey(approval);
  return approval.workflowLocked === true
    || (key !== null && appState.approvalWorkflowLocks.has(key));
}

function lockApprovalWorkflow(approval = {}) {
  const key = approvalWorkflowKey(approval);
  approval.workflowLocked = true;
  if (key !== null) appState.approvalWorkflowLocks.add(key);
}

function unlockApprovalWorkflow(approval = {}) {
  const key = approvalWorkflowKey(approval);
  approval.workflowLocked = false;
  if (key !== null) appState.approvalWorkflowLocks.delete(key);
}

function applyApprovalWorkflowLock(approval = {}) {
  if (!approvalWorkflowLocked(approval)) return false;
  lockApprovalWorkflow(approval);
  if (approval.checkbox) {
    approval.checkbox.checked = false;
    approval.checkbox.disabled = true;
  }
  if (approval.button) {
    approval.button.disabled = true;
    approval.button.textContent = "审批流程已冻结 · 禁止重建";
  }
  return true;
}

function synchronizeActionControls(now = Date.now(), { enforceFreshness = true } = {}) {
  if (appState.newsSnapshot) {
    renderNewsSourceHealth(appState.newsSnapshot.source_health, appState.newsSnapshot.source_runtime, now);
  }
  expireHoldingsClosePreviews(now);
  if (enforceFreshness) enforceControlSnapshotFreshness(now);
  appState.countdowns.forEach((approval) => updateApprovalState(approval, now));
  const managementAction = byId("management-review-action");
  if (managementAction) {
    managementAction.disabled = managementAction.dataset.authorityEnabled !== "true"
      || !actionControlGate(appState.controlContext, now);
  }
  configureReviewLink(undefined, now);
}

function featureDataChainTruth(readiness = {}) {
  const raw = readiness.feature_data_chain;
  const chain = raw && typeof raw === "object" && !Array.isArray(raw) ? raw : {};
  const status = researchText(chain.status, "UNAVAILABLE", 40).toUpperCase();
  const incomplete = status === "INCOMPLETE" || chain.model_input_complete === false;
  const reasonCodes = Array.isArray(chain.reason_codes)
    ? [...new Set(chain.reason_codes.map((value) => researchText(value, "", 96)).filter(Boolean))]
    : [];
  const reasons = operatorReasonList(reasonCodes);
  return {
    status,
    incomplete,
    wiringOnly: readiness.readiness_scope === "DEPENDENCY_WIRING_ONLY",
    reasonCodes,
    summary: incomplete
      ? `数据链路不完整 · NO_TRADE${reasons.length ? ` · ${reasons.join("；")}` : " · 生产特征输入尚未完整验证"}。当前无法形成完整数据支持的交易建议。`
      : null,
  };
}

function readinessBadgePresentation(truth, readinessStatus, ready) {
  if (truth.featureChain.incomplete) {
    return { label: "数据链路不完整 · NO_TRADE", className: "status-stale" };
  }
  return {
    label: ready
      ? "READY · REVIEW_ONLY"
      : truth.researchReady
        ? "RESEARCH READY · APPROVAL BLOCKED"
        : `${readinessStatus} · NO_TRADE`,
    className: ready ? "status-up" : truth.researchReady ? "status-stale" : "status-down",
  };
}

function readinessTruthModel(readiness = {}, scan = {}, ranking = {}, health = {}, now = new Date()) {
  const readinessDecision = String(readiness.decision || "NO_TRADE").toUpperCase();
  const researchReady = readinessDecision === "READY" && readiness.research_enabled === true;
  const approvalEnabled = readiness.approval_enabled === true && ranking.approval_enabled === true;
  const featureChain = featureDataChainTruth(readiness);
  const currentReasons = [...new Set([
    ...featureChain.reasonCodes,
    ...(Array.isArray(readiness.readiness_guard_reasons) ? readiness.readiness_guard_reasons : []),
    ...(Array.isArray(readiness.missing_dependencies) ? readiness.missing_dependencies : []),
    ...(Array.isArray(readiness.invalid_dependencies) ? readiness.invalid_dependencies : []),
    ...(Array.isArray(readiness.wiring_disagreements) ? readiness.wiring_disagreements : []),
    ...(Array.isArray(readiness.approval_blockers) ? readiness.approval_blockers : []),
  ].map((value) => String(value || "").trim()).filter(Boolean))];
  const historicalReasons = [...new Set([
    ...(Array.isArray(scan.reasons) ? scan.reasons : []),
    ...(Array.isArray(ranking.reasons) ? ranking.reasons : []),
  ].map((value) => String(value || "").trim()).filter(Boolean))];
  const recordedAt = firstValue(scan, ["recorded_at", "slot_at"], firstValue(ranking, ["recorded_at"]));
  const historicalDecision = String(scan.decision || ranking.decision || "UNAVAILABLE").toUpperCase();
  const daily = health?.dependencies?.production_scanner?.daily_operations;
  const runs = Array.isArray(daily?.runs) ? daily.runs : [];
  const nowMs = now instanceof Date ? now.getTime() : new Date(now).getTime();
  const exactOperations = new Set([
    "ORDINARY_SCAN",
    "RESEARCH_REFRESH",
    "TOP10_FREEZE",
    "TOP10_REPRICE",
    "AFTER_HOURS_DISCOVERY",
    "AFTER_HOURS_REPRICE",
    "NEXT_SESSION_PREPARATION",
  ]);
  const upcomingSlots = runs
    .filter((item) => (
      exactOperations.has(String(item?.operation || "").toUpperCase())
      && String(item?.status || "").toUpperCase() === "PENDING"
      && Number.isFinite(Date.parse(item?.scheduled_at))
      && Date.parse(item.scheduled_at) >= nowMs
    ))
    .sort((left, right) => Date.parse(left.scheduled_at) - Date.parse(right.scheduled_at));
  return {
    readinessDecision,
    researchReady,
    approvalEnabled,
    featureChain,
    currentReasons,
    historicalReasons,
    historicalDecision,
    recordedAt,
    upcomingSlots,
  };
}

function brokerDataChainTruth(health = {}, bootstrap = {}, nowMs = Date.now()) {
  const scanner = health?.dependencies?.production_scanner;
  const snapshot = health?.dependencies?.ibkr_snapshot
    || health?.dependencies?.ibkr
    || health?.dependencies?.IBKR
    || {};
  const connected = scanner?.connected;
  const snapshotStatus = researchText(
    firstValue(snapshot, ["status", "state"], "UNKNOWN"),
    "UNKNOWN",
    32,
  ).toUpperCase();
  const bootstrapBroker = bootstrap?.ibkr || bootstrap?.broker || bootstrap?.account || {};
  const bootstrapStatus = researchText(
    firstValue(bootstrapBroker, ["status", "state"], "UNKNOWN"),
    "UNKNOWN",
    32,
  ).toUpperCase();
  const bootstrapConnected = firstValue(bootstrapBroker, ["connected"], null);
  const bootstrapReconciled = firstValue(
    bootstrapBroker,
    ["reconciled", "account_reconciled"],
    null,
  );
  const bootstrapObservedAt = firstValue(
    bootstrapBroker,
    ["observed_at", "asof", "snapshot_at"],
    null,
  );
  const bootstrapObservedAtMs = Date.parse(bootstrapObservedAt || "");
  const bootstrapAgeMs = Number.isFinite(bootstrapObservedAtMs)
    ? nowMs - bootstrapObservedAtMs
    : Number.POSITIVE_INFINITY;
  const bootstrapFresh = (
    bootstrapAgeMs >= 0
    && bootstrapAgeMs <= CONTROL_SNAPSHOT_STALE_AFTER_MS
  );
  const bootstrapCurrent = (
    ["CURRENT", "FRESH"].includes(bootstrapStatus)
    && bootstrapConnected === true
    && bootstrapReconciled === true
    && bootstrapFresh
  );
  if (connected === false) {
    return {
      status: "DISCONNECTED",
      label: "IBKR 未连接",
      summary: "只读会话未连接；runtime supervisor 正在有界自动重连。请确认 Gateway/TWS 已启动、登录且 API 端口正确。",
    };
  }
  if (bootstrapCurrent) {
    return {
      status: "CURRENT",
      label: "IBKR 只读链已连接",
      summary: "轻量控制快照已验证连接、账户对账且不超过 15 秒；逐腿五秒行情、Greeks、流动性、成本后 EV 与六道 Gate 仍须独立通过。",
    };
  }
  if (bootstrapConnected === false) {
    return {
      status: "DISCONNECTED",
      label: "IBKR 未连接",
      summary: "轻量控制快照确认只读会话未连接；请确认 Gateway/TWS 已启动、登录且 API 端口正确。",
    };
  }
  if (
    ["CURRENT", "FRESH"].includes(bootstrapStatus)
    && bootstrapReconciled === false
  ) {
    return {
      status: "UNRECONCILED",
      label: "账户尚未对账",
      summary: "IBKR 只读会话已连接，但账户 NLV 与 Strategy NAV 尚未完成同一快照对账；当前候选不可授权。",
    };
  }
  if (
    ["CURRENT", "FRESH", "STALE"].includes(bootstrapStatus)
    && !bootstrapFresh
    && Number.isFinite(bootstrapObservedAtMs)
  ) {
    return {
      status: "STALE",
      label: "控制快照陈旧",
      summary: "IBKR 轻量控制快照已超过 15 秒；当前账户、持仓、NAV 对账与候选均不可授权。",
    };
  }
  if (
    snapshot.stale === true
    || snapshotStatus === "STALE"
  ) {
    return {
      status: "STALE",
      label: "控制快照陈旧",
      summary: "IBKR 控制快照已超过 15 秒；当前账户、持仓、NAV 对账与候选均不可授权。",
    };
  }
  if (
    connected === true
    && ["UP", "READY", "CURRENT", "FRESH"].includes(snapshotStatus)
    && snapshot.stale !== true
  ) {
    return {
      status: "CURRENT",
      label: "IBKR 只读链已连接",
      summary: "控制快照当前；逐腿五秒行情、Greeks、流动性、成本后 EV 与六道 Gate 仍需在自然 producer 时槽独立通过。",
    };
  }
  return {
    status: "UNKNOWN",
    label: "IBKR 状态待验证",
    summary: "尚无足够的只读连接与控制快照证据；不会把端口或历史快照当作实时链路。",
  };
}

function brokerReviewModeText(readiness = {}, ranking = {}) {
  const blockers = Array.isArray(readiness.approval_blockers)
    ? readiness.approval_blockers.map((value) => String(value || "").trim().toUpperCase())
    : [];
  if (
    readiness.approval_enabled === true
    && ranking.approval_enabled === true
    && !blockers.includes("CREATOR_TRANSPORT_UNAVAILABLE")
  ) {
    return "可发起审核 · 仍需人工确认";
  }
  return "仅供审核 · 不可创建指令";
}

function top10ProducerTruth(health = {}) {
  const raw = health?.dependencies?.production_scanner?.top10_producer;
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) {
    return {
      available: false,
      producerStatus: "NOT_RUN",
      writtenCount: null,
      reasonCodes: [],
      runId: "--",
    };
  }
  let writtenCount = Number.isInteger(raw.last_written_count)
    && raw.last_written_count >= 0
    && raw.last_written_count <= MAX_CANDIDATES
    ? raw.last_written_count
    : null;
  if (writtenCount === null) {
    const evidenceHash = ledgerDigest(raw.last_producer_evidence_hash);
    const daily = health?.dependencies?.production_scanner?.daily_operations;
    const todayRuns = Array.isArray(daily?.today?.runs)
      ? daily.today.runs
      : Array.isArray(daily?.runs)
        ? daily.runs
        : [];
    const matchingRun = todayRuns.find((item) => (
      ["TOP10_FREEZE", "TOP10_REPRICE"].includes(String(item?.operation || "").toUpperCase())
      && String(item?.status || "").toUpperCase() === "COMPLETED"
      && String(item?.producer_status || "").toUpperCase()
        === researchText(raw.last_producer_status, "NOT_RUN", 32).toUpperCase()
      && evidenceHash !== null
      && ledgerDigest(item?.producer_evidence_hash) === evidenceHash
    ));
    const durableWrittenCount = numberOrNull(matchingRun?.producer_written_count);
    if (
      Number.isInteger(durableWrittenCount)
      && durableWrittenCount >= 0
      && durableWrittenCount <= MAX_CANDIDATES
    ) {
      writtenCount = durableWrittenCount;
    }
  }
  return {
    available: true,
    producerStatus: researchText(raw.last_producer_status, "NOT_RUN", 32).toUpperCase(),
    writtenCount,
    reasonCodes: Array.isArray(raw.last_reason_codes)
      ? raw.last_reason_codes.map((item) => researchText(item, "UNKNOWN", 96))
      : [],
    runId: researchText(raw.last_producer_run_id, "--", 160),
  };
}

function overviewBlockedActionSummary(ranking = {}, health = {}, positionTruth = {}, readiness = {}) {
  const featureChain = featureDataChainTruth(readiness);
  if (featureChain.incomplete) return featureChain.summary;
  if (positionTruth.positionManagementOnly && !positionTruth.positionStateKnown) {
    return "当前仓位未验证；旧扫描的 POSITION_MANAGEMENT_ONLY 只作历史证据。";
  }
  if (positionTruth.positionManagementOnly && positionTruth.verifiedFlat) {
    return "当前仓位已核验为空；旧扫描的 POSITION_MANAGEMENT_ONLY 已失效，不代表仍有开放组合。";
  }
  const latestScanFunnel = blockedScanFunnelSummary(ranking);
  if (latestScanFunnel) return latestScanFunnel;
  const producer = top10ProducerTruth(health);
  if (
    producer.available
    && ["NO_TRADE", "POSITION_MANAGEMENT_ONLY"].includes(producer.producerStatus)
  ) {
    const written = producer.writtenCount === null
      ? "写入数未验证"
      : `写入 ${producer.writtenCount}/${MAX_CANDIDATES}`;
    const reason = operatorReasonList(producer.reasonCodes)[0]
      || "NO_ELIGIBLE_EXECUTABLE_CANDIDATES";
    const preparation = health?.dependencies?.production_scanner
      ?.daily_operations?.next_session_preparation;
    const carriedResearchCount = numberOrNull(
      preparation?.option_research_structure_count,
    );
    const carriedResearchVerified = (
      preparation?.reconciled_from_verified_after_hours === true
      || preparation?.reconciled_from_durable_after_hours === true
    );
    const parentEligibleCount = numberOrNull(
      preparation?.premarket_parent_eligible_structure_count,
    );
    const nextTradingDate = researchText(
      preparation?.next_trading_date,
      "下一交易日",
      32,
    );
    if (
      carriedResearchVerified
      && Number.isInteger(carriedResearchCount)
      && carriedResearchCount > 0
      && carriedResearchCount <= MAX_CANDIDATES
      && Number.isInteger(parentEligibleCount)
      && parentEligibleCount > 0
      && numberOrNull(preparation?.executable_count) === 0
    ) {
      return `最近正常时段 Top-10 producer ${producer.producerStatus} · ${written} · ${reason}；当前收盘研究链已恢复并携带 ${carriedResearchCount} 个结构到 ${nextTradingDate}，等待下一自然时槽补齐可执行证据。`;
    }
    if (
      carriedResearchVerified
      && Number.isInteger(carriedResearchCount)
      && carriedResearchCount > 0
      && parentEligibleCount === 0
    ) {
      return `当前 Top-10 producer ${producer.producerStatus} · ${written} · ${reason}；收盘研究池保留 ${carriedResearchCount} 个结构，但独立股票 thesis 与静态风险证据合格的 09:20 父结构为 0，不能作为下一交易日可执行建议。`;
    }
    return `当前 Top-10 producer ${producer.producerStatus} · ${written} · ${reason}`;
  }
  const explicitReason = firstValue(
    ranking,
    ["no_trade_reason", "decision_reason", "reason"],
  );
  const missingSymbols = Array.isArray(ranking.missing_symbols)
    ? [...new Set(ranking.missing_symbols
      .map((value) => researchText(value, "", 16).toUpperCase())
      .filter(Boolean))]
    : [];
  const historicalReason = explicitReason
    || operatorReasonList(ranking.reasons)[0]
    || (missingSymbols.length > 0
      ? `缺失底层报价：${missingSymbols.join(" / ")}`
      : null);
  return historicalReason
    ? `当前 producer 尚无新终态；历史 ranking 原因：${historicalReason}`
    : "当前没有通过报价、流动性、成本后 EV 与风险 Gate 的组合。";
}

function blockedScanFunnelSummary(ranking = {}) {
  if (
    !ranking
    || typeof ranking !== "object"
    || Array.isArray(ranking)
    || ranking.decision !== "NO_TRADE"
    || ranking.recommendations_available !== false
    || !Array.isArray(ranking.candidates)
    || ranking.candidates.length !== 0
  ) return null;
  const trace = ranking.funnel_trace;
  if (!trace || typeof trace !== "object" || Array.isArray(trace)) return null;
  const discovered = trace.discovered_underlyings;
  const requested = trace.deep_scan_requested;
  const attemptedRaw = trace.deep_scan_attempted;
  const completed = trace.deep_scan_completed;
  const deferredRaw = trace.deep_scan_deferred;
  const deferredSymbolsRaw = trace.deep_scan_deferred_symbols;
  const ranked = trace.ranked_count;
  const hasBudgetObservability = attemptedRaw !== undefined
    || deferredRaw !== undefined
    || deferredSymbolsRaw !== undefined;
  const attempted = hasBudgetObservability ? attemptedRaw : requested;
  const deferred = hasBudgetObservability ? deferredRaw : 0;
  const deferredSymbols = deferredSymbolsRaw === undefined
    ? []
    : deferredSymbolsRaw;
  if (
    !Number.isInteger(discovered)
    || discovered < 0
    || !Number.isInteger(requested)
    || requested < 0
    || requested > discovered
    || !Number.isInteger(attempted)
    || attempted < 0
    || attempted > requested
    || !Number.isInteger(completed)
    || completed < 0
    || completed > attempted
    || !Number.isInteger(deferred)
    || deferred < 0
    || attempted + deferred > requested
    || !Array.isArray(deferredSymbols)
    || deferredSymbols.length !== deferred
    || !Number.isInteger(ranked)
    || ranked !== 0
  ) return null;
  const normalizedDeferredSymbols = deferredSymbols.map((value) => (
    typeof value === "string" ? value.trim().toUpperCase() : ""
  ));
  if (
    normalizedDeferredSymbols.some((symbol) => !/^[A-Z0-9.-]{1,24}$/.test(symbol))
    || new Set(normalizedDeferredSymbols).size !== normalizedDeferredSymbols.length
  ) return null;

  const rejectionGroups = new Map();
  for (const field of [
    "optionability_exclusion_reasons",
    "underlying_quote_exclusion_reasons",
  ]) {
    const rows = trace[field];
    if (rows === undefined || rows === null) continue;
    if (!Array.isArray(rows) || rows.length > 30) return null;
    for (const row of rows) {
      if (!row || typeof row !== "object" || Array.isArray(row)) return null;
      const symbol = typeof row.symbol === "string" ? row.symbol.trim().toUpperCase() : "";
      const reason = typeof row.reason_code === "string"
        ? row.reason_code.trim().toUpperCase()
        : "";
      if (
        !/^[A-Z0-9.-]{1,24}$/.test(symbol)
        || !/^[A-Z0-9_]{1,96}$/.test(reason)
      ) return null;
      if (!rejectionGroups.has(reason)) rejectionGroups.set(reason, []);
      const symbols = rejectionGroups.get(reason);
      if (!symbols.includes(symbol)) symbols.push(symbol);
    }
  }

  const detail = [...rejectionGroups.entries()].map(([reason, symbols]) => (
    `${symbols.join("、")} ${OPERATOR_REASON_LABELS[reason] || reason}`
  ));
  if (normalizedDeferredSymbols.length > 0) {
    detail.unshift(
      `${normalizedDeferredSymbols.join("、")} 因本轮 SECDEF pacing 预算延期，未请求行情`,
    );
  }
  const base = hasBudgetObservability
    ? `最近一次只读扫描已完成：发现 ${discovered}，入选深扫 ${requested}，实际尝试 ${attempted}，完成 ${completed}，预算延期 ${deferred}，进入排名 0`
    : `最近一次只读扫描已完成：发现 ${discovered}，深扫 ${completed}/${requested}，进入排名 0`;
  return detail.length > 0 ? `${base}；${detail.join("；")}。` : `${base}。`;
}

function scanOperationalTimingSummary(scan = {}) {
  const timing = scan && typeof scan === "object" && !Array.isArray(scan)
    ? scan.operational_timing
    : null;
  if (
    !timing
    || typeof timing !== "object"
    || Array.isArray(timing)
    || timing.schema !== "options_copilot.scan_operational_timing.v1"
    || timing.decision_authority !== "OBSERVATION_ONLY"
    || timing.affects_decision !== false
    || !Number.isInteger(timing.total_duration_ms)
    || timing.total_duration_ms < 0
    || !Array.isArray(timing.stages)
    || timing.stages.length > 32
  ) return "--";
  const stages = [];
  for (const row of timing.stages) {
    const stage = row && typeof row === "object" && !Array.isArray(row)
      ? String(row.stage || "").trim().toUpperCase()
      : "";
    const duration = row && typeof row === "object" && !Array.isArray(row)
      ? row.duration_ms
      : null;
    if (!/^[A-Z0-9_]{1,64}$/.test(stage) || !Number.isInteger(duration) || duration < 0) {
      return "--";
    }
    stages.push({ stage, duration });
  }
  if (stages.reduce((total, row) => total + row.duration, 0) > timing.total_duration_ms) {
    return "--";
  }
  const seconds = (timing.total_duration_ms / 1000).toFixed(2);
  const slowest = stages.reduce(
    (current, row) => (!current || row.duration > current.duration ? row : current),
    null,
  );
  return slowest
    ? `${seconds}s · 最慢 ${slowest.stage} ${(slowest.duration / 1000).toFixed(2)}s`
    : `${seconds}s`;
}

function renderImmediateScanCampaign(payload = {}) {
  const status = researchText(payload.status, "NOT_RUN", 48).toUpperCase();
  const attempts = Array.isArray(payload.attempts) ? payload.attempts.slice(-42) : [];
  const completed = numberOrNull(payload.completed_symbol_count) ?? 0;
  const target = numberOrNull(payload.target_symbol_count) ?? 0;
  const nextSymbol = researchText(payload.next_symbol, "--", 16);
  const state = byId("scan-campaign-state");
  state.classList.remove("status-up", "status-down", "status-stale", "status-unknown");
  state.classList.add(
    status === "CANDIDATES_OBSERVED_REQUIRES_CURRENT_RANKING"
      ? "status-stale"
      : status === "PAUSED_PACING"
        ? "status-stale"
        : status === "NOT_RUN"
          ? "status-unknown"
          : "status-stale",
  );
  state.textContent = status;
  setText(
    "scan-campaign-summary",
    status === "NOT_RUN"
      ? "尚未运行本进程的即时只读扫描；这里不会用旧 Top-10 填充。"
      : `累计 ${completed}/${target || "--"} 个标的复评 · ${attempts.length} 次原子尝试 · 下一标的 ${nextSymbol} · observation-only`,
  );
  setText(
    "scan-campaign-id",
    researchText(payload.campaign_id, "--", 160),
  );
  const container = byId("scan-campaign-attempts");
  container.replaceChildren();
  if (attempts.length === 0) {
    container.append(createElement("p", "empty-state", "没有本次即时扫描证据。"));
    return;
  }
  for (const attempt of attempts.slice().reverse()) {
    const symbol = researchText(attempt.target_symbol, "UNKNOWN", 16);
    const kind = researchText(attempt.attempt_kind, "UNKNOWN", 24).toUpperCase();
    const gate = researchText(attempt.stopped_at_gate, "GATE_UNAVAILABLE", 64);
    const decision = researchText(attempt.decision, "NO_TRADE", 32).toUpperCase();
    const reasonCodes = operatorReasonList(attempt.reason_codes);
    const row = createElement("article", "scan-campaign-attempt");
    const heading = createElement("div", "scan-campaign-attempt-heading");
    heading.append(
      createElement("strong", "", `${symbol} · ${kind === "WARMUP" ? "预热" : kind === "REEVALUATION" ? "复评" : kind}`),
      createElement("span", "status-chip status-stale", decision),
    );
    row.append(
      heading,
      createElement("p", "scan-campaign-gate", `${gate} · ${formatTime(attempt.recorded_at || attempt.finished_at)}`),
      createElement(
        "p",
        "scan-campaign-reasons",
        reasonCodes.length > 0 ? reasonCodes.join(" · ") : "全部硬 Gate 通过；请只查看同一最新 ranking 的候选。",
      ),
      createElement(
        "p",
        "scan-campaign-proof",
        `scan ${researchText(attempt.scan_run_id, "--", 160)} · decision ${researchText(attempt.decision_hash, "--", 64)} · gate ${researchText(attempt.gate_bundle_hash, "--", 64)}`,
      ),
    );
    container.append(row);
  }
}

function scannerSourceEvidenceSummary(scan = {}) {
  const funnel = scan?.funnel_trace && typeof scan.funnel_trace === "object"
    && !Array.isArray(scan.funnel_trace)
    ? scan.funnel_trace
    : {};
  const completed = Array.isArray(funnel.scanner_completed_scan_codes)
    ? funnel.scanner_completed_scan_codes
    : null;
  const failed = Array.isArray(funnel.scanner_failed_scan_codes)
    ? funnel.scanner_failed_scan_codes
    : null;
  const rows = Array.isArray(funnel.scanner_source_row_counts)
    ? funnel.scanner_source_row_counts
    : null;
  if (completed === null && failed === null && rows === null) {
    return "Scanner 来源 · 未记录；不能区分空结果与未运行。";
  }
  const allowed = ["MOST_ACTIVE", "TOP_PERC_GAIN", "TOP_PERC_LOSE"];
  const completedSet = new Set(completed || []);
  const failedSet = new Set(failed || []);
  const rowCounts = new Map();
  const valid = completed !== null
    && failed !== null
    && rows !== null
    && completed.every((code) => allowed.includes(code))
    && failed.every((code) => allowed.includes(code))
    && completed.length === completedSet.size
    && failed.length === failedSet.size
    && [...completedSet].every((code) => !failedSet.has(code))
    && rows.every((row) => {
      const code = row && typeof row === "object" ? row.scan_code : null;
      const count = row && typeof row === "object" ? numberOrNull(row.row_count) : null;
      if (
        !allowed.includes(code)
        || rowCounts.has(code)
        || !Number.isInteger(count)
        || count < 0
        || count > 50
        || (failedSet.has(code) && count !== 0)
      ) return false;
      rowCounts.set(code, count);
      return true;
    })
    && rowCounts.size === completedSet.size + failedSet.size
    && rowCounts.size === allowed.length
    && allowed.every((code) => rowCounts.has(code))
    && [...rowCounts.keys()].every((code) => completedSet.has(code) || failedSet.has(code));
  if (!valid) return "Scanner 来源 · INVALID · 当前逐来源证据不可用。";
  return `Scanner 来源 · ${allowed
    .filter((code) => rowCounts.has(code))
    .map((code) => `${code} ${completedSet.has(code) ? "完成" : "失败"} ${rowCounts.get(code)} 行`)
    .join(" · ")}`;
}

function renderUpcomingExactSlots(upcomingSlots = []) {
  const operationLabels = {
    RESEARCH_REFRESH: "研究刷新",
    TOP10_FREEZE: "Top10 冻结",
    TOP10_REPRICE: "开盘重估",
    AFTER_HOURS_DISCOVERY: "收盘宽市场筛选",
    AFTER_HOURS_REPRICE: "收盘期权补价",
    NEXT_SESSION_PREPARATION: "下一交易日准备",
  };
  setText(
    "upcoming-exact-slots",
    upcomingSlots.length > 0
      ? `下一精确时槽 · ${upcomingSlots.map((item) => `${operationLabels[item.operation] || item.operation} ${formatDualMarketTime(item.scheduled_at)}`).join(" · ")}`
      : "下一精确时槽 · 当前 health 未提供未来 PENDING 精确时槽；不推断、不补跑。",
  );
}

function renderReadiness(readiness, scan, ranking, health = {}) {
  appState.readinessSnapshot = readiness;
  appState.healthSnapshot = health;
  const readinessStatus = String(readiness.status || "DEGRADED").toUpperCase();
  const scanDecision = String(scan.decision || ranking.decision || "NO_TRADE").toUpperCase();
  const approvalEnabled = ranking.approval_enabled === true;
  const strategyNavReady = appState.strategyNavUsd !== null && appState.strategyNavUsd > 0;
  const truth = readinessTruthModel(readiness, scan, ranking, health);
  setText("ibkr-review-mode", brokerReviewModeText(readiness, ranking));
  const top10Runtime = top10ProducerTruth(health);
  if (top10Runtime.available) {
    const written = top10Runtime.writtenCount === null
      ? "--"
      : top10Runtime.writtenCount;
    setText(
      "research-top10-runtime-truth",
      `精确时槽真相 · ${top10Runtime.producerStatus} · 写入 ${written}/10 · run ${top10Runtime.runId}${top10Runtime.reasonCodes.length > 0 ? ` · ${top10Runtime.reasonCodes.join(" · ")}` : ""}`,
    );
  } else {
    setText("research-top10-runtime-truth", "精确时槽结果尚未记录；不沿用旧 ranking 原因。");
  }
  appState.controlContext = {
    ...appState.controlContext,
    readinessStatus,
    scanDecision,
    approvalEnabled,
    strategyNavReady,
  };
  const ready = actionControlGate(appState.controlContext);
  const state = byId("readiness-state");
  const badge = readinessBadgePresentation(truth, readinessStatus, ready);
  state.classList.remove("status-up", "status-down", "status-stale", "status-unknown");
  state.classList.add(badge.className);
  state.textContent = badge.label;

  const currentReasons = [...truth.currentReasons];
  if (!strategyNavReady) currentReasons.push("STRATEGY_NAV_UNAVAILABLE");
  const brokerTruth = brokerDataChainTruth(
    health,
    appState.bootstrapSnapshot || {},
  );
  if (brokerTruth.status === "DISCONNECTED") {
    currentReasons.push("IBKR_RUNTIME_DISCONNECTED");
  } else if (brokerTruth.status === "STALE") {
    currentReasons.push("IBKR_CONTROL_SNAPSHOT_STALE");
  } else if (brokerTruth.status === "UNKNOWN") {
    currentReasons.push("IBKR_RUNTIME_STATE_UNVERIFIED");
  }
  if (appState.controlContext.brokerState !== "FRESH") {
    currentReasons.push(`BROKER_${appState.controlContext.brokerState}`);
  }
  setText(
    "readiness-reasons",
    `当前运行 · ${truth.featureChain.wiringOnly ? "依赖接线检查" : "decision"} ${truth.readinessDecision}${truth.featureChain.wiringOnly ? "（仅表示依赖接线状态）" : ""} · research ${truth.researchReady ? "READY" : "BLOCKED"} · approval ${truth.approvalEnabled ? "ENABLED" : "DISABLED"}${currentReasons.length > 0 ? ` · ${operatorReasonList([...new Set(currentReasons)]).join(" · ")}` : ""}`,
  );
  setText(
    "historical-ranking-state",
    `历史不可变结果 · ${truth.recordedAt ? formatTime(truth.recordedAt) : "时间不可用"} · ${truth.historicalDecision}${truth.historicalReasons.length > 0 ? ` · ${operatorReasonList(truth.historicalReasons).join(" · ")}` : " · 无原因码"} · 不代表当前 readiness`,
  );
  setText("scanner-source-evidence", scannerSourceEvidenceSummary(scan));
  renderUpcomingExactSlots(truth.upcomingSlots);
  setText("scan-run-id", firstValue(scan, ["scan_run_id"], firstValue(ranking, ["scan_run_id"], "--")));
  setText("scan-duration", scanOperationalTimingSummary(scan));
  setText("ranking-snapshot-id", firstValue(ranking, ["ranking_snapshot_id"], "--"));
  setText(
    "policy-identity",
    [ranking.current_policy_version, shortHash(ranking.current_policy_hash)].filter(Boolean).join(" · ") || "--",
  );
  setText(
    "cost-identity",
    [ranking.cost_version, shortHash(ranking.cost_hash)].filter(Boolean).join(" · ") || "--",
  );
  renderOverviewPriority();
  renderDailyFunnel(health);
  synchronizeActionControls();
}

function renderDailyFunnel(health = {}) {
  const daily = health?.dependencies?.production_scanner?.daily_operations;
  const runs = Array.isArray(daily?.runs) ? daily.runs : [];
  const manifestHash = researchText(daily?.day_manifest?.manifest_hash, "", 64);
  const truth = dailyFunnelTruth(
    runs,
    manifestHash,
    daily?.today?.market_status,
    daily?.today?.next_trading_date,
  );
  const state = byId("daily-funnel-status");
  if (state) {
    state.classList.remove("status-up", "status-down", "status-stale", "status-unknown");
    state.classList.add(truth.stateClass);
    state.textContent = truth.summary;
  }
  const preparation = daily?.next_session_preparation && typeof daily.next_session_preparation === "object"
    ? daily.next_session_preparation
    : {};
  const preparationStatus = String(preparation.status || "UNAVAILABLE").toUpperCase();
  const preparationCheckedAt = researchText(preparation.checked_at, "", 64);
  const preparationResearchCarried = (
    preparation.reconciled_from_verified_after_hours === true
    || preparation.reconciled_from_durable_after_hours === true
  ) && (numberOrNull(preparation.option_research_structure_count) ?? 0) > 0;
  const preparationParentEligibleCount = (
    numberOrNull(preparation.premarket_parent_eligible_structure_count) ?? 0
  );
  const preparationLabel = preparationStatus === "READY"
    ? "准备完成"
    : preparationResearchCarried
      ? preparationParentEligibleCount > 0
        ? "研究链已接续 · 可执行证据待市场"
        : "研究池已保留 · 09:20 父结构不可用"
      : preparationCheckedAt
        ? "历史准备结果 · 未完成"
        : "准备未完成";
  const preparationAsOf = preparationCheckedAt
    ? ` · 验证于 ${formatDualMarketTime(preparationCheckedAt)}`
    : "";
  setText(
    "next-session-preparation",
    preparationStatus === "UNAVAILABLE"
      ? "下一交易日准备没有持久化结果；系统不会把固定日历文字冒充准备完成。"
      : `${preparationLabel}${preparationAsOf} · 下一交易日 ${preparation.next_trading_date || "待官方日历确认"} · 股票研究发现 ${formatInteger(preparation.equity_research_count)} · 严格入选 ${formatInteger(preparation.equity_selected_count)} · 期权研究结构 ${formatInteger(preparation.option_research_structure_count)} · 09:20 父结构 ${formatInteger(preparation.premarket_parent_eligible_structure_count)} · 可执行建议 ${formatInteger(preparation.executable_count)} · 联合研究 ${formatInteger(preparation.research_watchlist_count)} · ${operatorReasonList(preparation.reason_codes).join(" · ") || "SUPPORTING_ONLY · NO_TRADE"}`,
  );
  const container = byId("daily-funnel-runs");
  if (!container) return;
  container.replaceChildren();
  if (runs.length === 0) {
    container.append(createElement("p", "empty-state", truth.emptyMessage));
    return;
  }
  runs.forEach((item) => {
    const presentation = dailyRunPresentation(item);
    const row = createElement("article", "daily-funnel-run");
    row.append(
      createElement("strong", "", researchText(item.operation, "UNKNOWN", 48)),
      createElement("span", `status-chip ${presentation.stateClass}`, presentation.label),
      createElement("time", "", formatDualMarketTime(item.scheduled_at)),
    );
    container.append(row);
  });
}

function dailyRunPresentation(item = {}) {
  const status = String(item?.status || "UNKNOWN").toUpperCase();
  const producerStatus = String(item?.producer_status || "").toUpperCase();
  const producerFailure = new Set(["NO_TRADE", "POSITION_MANAGEMENT_ONLY"]);
  const producerTerminal = new Set([
    "PREMARKET_FROZEN",
    "OPEN_REPRICED",
    ...producerFailure,
  ]);
  const failure = new Set(["FAILED", "MISSED_NOT_REPLAYED", "NO_TRADE"]);
  const pending = new Set(["PENDING", "DUE", "RECOVERABLE", "LEASED"]);
  const reason = operatorReasonList(item?.reason_codes)[0]
    || researchText(item?.terminalization_error, "", 64)
    || (status === "MISSED_NOT_REPLAYED" ? researchText(item?.recovery_policy, "", 64) : "");
  const written = Number.isInteger(item?.producer_written_count)
    ? ` · 写入 ${item.producer_written_count}/${MAX_CANDIDATES}`
    : "";
  const producerLabel = producerTerminal.has(producerStatus)
    ? ` · ${producerStatus}${written}`
    : "";
  const stateClass = producerFailure.has(producerStatus)
    ? "status-down"
    : status === "COMPLETED"
      ? "status-up"
      : failure.has(status)
        ? "status-down"
        : pending.has(status)
          ? "status-stale"
          : "status-unknown";
  return {
    status,
    stateClass,
    label: `${status}${producerLabel}${reason ? ` · ${reason}` : ""}`,
  };
}

function dailyFunnelTruth(
  runs = [],
  manifestHash = "",
  marketStatus = "UNVERIFIED",
  nextTradingDate = null,
) {
  const rows = Array.isArray(runs) ? runs : [];
  const manifestAvailable = ledgerDigest(manifestHash) !== null;
  const presentations = rows.map((item) => dailyRunPresentation(item));
  const terminal = new Set(["COMPLETED", "FAILED", "MISSED_NOT_REPLAYED", "NO_TRADE"]);
  const failed = new Set(["FAILED", "MISSED_NOT_REPLAYED"]);
  const terminalCount = presentations.filter((item) => terminal.has(item.status)).length;
  const failureCount = presentations.filter((item) => failed.has(item.status)).length;
  const noTradeCount = rows.filter((item) => (
    ["NO_TRADE", "POSITION_MANAGEMENT_ONLY"].includes(
      String(item?.producer_status || "").toUpperCase(),
    )
  )).length;
  const normalizedMarketStatus = ["TRADING_SESSION", "CLOSED", "UNVERIFIED"].includes(String(marketStatus).toUpperCase())
    ? String(marketStatus).toUpperCase()
    : "UNVERIFIED";
  const normalizedNextDate = /^\d{4}-\d{2}-\d{2}$/.test(String(nextTradingDate || ""))
    ? String(nextTradingDate)
    : null;
  if (normalizedMarketStatus === "CLOSED") {
    return {
      terminalCount: 0,
      failureCount: 0,
      stateClass: "status-stale",
      summary: `MARKET CLOSED${normalizedNextDate ? ` · NEXT ${normalizedNextDate}` : ""}`,
      emptyMessage: normalizedNextDate
        ? `当前是非交易日；下一交易日 ${normalizedNextDate}，不会补跑或伪造今日时槽。`
        : "当前是非交易日；等待日历确认下一交易日，不会补跑或伪造今日时槽。",
    };
  }
  return {
    terminalCount,
    failureCount,
    stateClass: rows.length === 0
      ? manifestAvailable ? "status-stale" : "status-unknown"
      : failureCount > 0
        ? "status-down"
        : noTradeCount > 0
          ? "status-stale"
        : terminalCount === rows.length
          ? "status-up"
          : "status-stale",
    summary: rows.length === 0
      ? manifestAvailable ? "0 OPERATIONS DUE · DURABLE" : "UNAVAILABLE"
      : `${terminalCount}/${rows.length} TERMINAL · ${failureCount} FAILED/MISSED${noTradeCount ? ` · ${noTradeCount} NO_TRADE` : ""}${manifestHash ? " · DURABLE" : " · MANIFEST WAIT"}`,
    emptyMessage: manifestAvailable
      ? "本日 durable manifest 已记录；当前没有到期的 daily operation。"
      : "当前 runtime 未提供 durable daily operation manifest。",
  };
}

function renderManagement(payload) {
  const container = byId("management-preview-list");
  const action = byId("management-review-action");
  const actionReason = byId("management-action-reason");
  if (!payload) {
    setText("management-status", "NO_TRADE · PositionManager 不可用；entry 路径不能替代持仓管理。 ");
    if (container) {
      container.replaceChildren(createElement("p", "empty-state", "持仓管理只读投影不可用。"));
    }
    if (action) {
      action.dataset.authorityEnabled = "false";
      action.disabled = true;
    }
    setText(actionReason, "POSITION_MANAGER_UNAVAILABLE · 禁止创建审核指令。");
    return;
  }
  const status = String(payload.status || payload.decision || "NO_TRADE").toUpperCase();
  const reason = firstValue(payload, ["reason", "message", "no_trade_reason"], "当前没有经过独立 transition proof 的管理动作。");
  setText(
    "management-status",
    `${status} · ${reason}`,
  );
  const candidates = Array.isArray(payload.candidates) ? payload.candidates.slice(0, 10) : [];
  if (container) {
    container.replaceChildren();
    const emptyState = managementEmptyStateMessage(
      candidates.length,
      appState.hasDerivedManagementPreview,
    );
    if (emptyState) {
      container.append(createElement("p", "empty-state", emptyState));
    } else {
      candidates.forEach((candidate) => container.append(buildManagementPreview(candidate)));
    }
  }
  if (action) {
    action.textContent = String(payload.action_text || "Create IBKR review instruction");
    action.dataset.authorityEnabled = payload.action_enabled === true ? "true" : "false";
    action.disabled = payload.action_enabled !== true
      || !actionControlGate(appState.controlContext);
  }
  setText(
    actionReason,
    `${payload.creator_transport_status || "CREATOR_TRANSPORT_UNAVAILABLE"} · `
      + `external attempts ${formatInteger(payload.external_attempt_count || 0)} · `
      + `instructions ${formatInteger(payload.instruction_count || 0)}`,
  );
}

function managementEmptyStateMessage(candidateCount, hasDerivedPreview) {
  if (candidateCount > 0 || hasDerivedPreview === true) return null;
  return "当前没有快照派生、成本与退出规则均完整的管理预览。";
}

function renderPositioning(payload) {
  const container = byId("positioning-list");
  const statusNode = byId("positioning-status");
  if (!container || !statusNode) return;
  container.replaceChildren();
  statusNode.classList.remove("status-up", "status-down", "status-stale", "status-unknown");
  if (!payload) {
    statusNode.classList.add("status-stale");
    statusNode.textContent = "UNAVAILABLE · SUPPORTING_ONLY";
    setText("positioning-reasons", "POSITIONING_SOURCE_UNAVAILABLE · 不得用于交易资格判断。");
    container.append(createElement("p", "empty-state", "定位指标只读投影不可用。"));
    return;
  }

  const status = String(payload.status || "UNAVAILABLE").toUpperCase();
  statusNode.classList.add(status === "READY" ? "status-up" : "status-stale");
  statusNode.textContent = `${status} · SUPPORTING_ONLY`;
  const reasons = Array.isArray(payload.reasons) ? payload.reasons : [];
  setText(
    "positioning-reasons",
    reasons.length > 0
      ? `${reasons.join(" · ")} · 不影响 eligibility / approval / instruction`
      : "仅作辅助证据；OI 可能延迟，GEX 不代表真实 dealer 方向。",
  );
  const rows = Array.isArray(payload.positioning) ? payload.positioning : [];
  if (rows.length === 0) {
    container.append(createElement("p", "empty-state", "没有新鲜、覆盖范围透明的定位指标。"));
    return;
  }
  rows.forEach((row) => container.append(buildPositioningRow(row)));
}

function buildPositioningRow(row) {
  const card = createElement("article", "positioning-row");
  const head = createElement("div", "positioning-row-head");
  head.append(
    createElement(
      "strong",
      "",
      `${row.underlying || "--"} · ${row.expiration || "--"}`,
    ),
    createElement(
      "span",
      "",
      `${row.chain_scope || "FROZEN_FINALIST_LEGS_ONLY 未就绪"} · asof ${formatTime(row.data_asof)}`,
    ),
  );
  const metrics = createElement("div", "positioning-metrics");
  for (const [label, value] of [
    ["Max Pain", formatQuote(row.max_pain)],
    ["Call Wall", formatQuote(row.call_wall)],
    ["Put Wall", formatQuote(row.put_wall)],
    ["OI PCR", formatQuote(row.put_call_open_interest_ratio)],
    ["估算 GEX / 1%", formatMoney(row.estimated_net_gex_usd_per_one_percent, { signed: true })],
  ]) {
    const cell = createElement("div");
    cell.append(createElement("span", "", label), createElement("strong", "", value));
    metrics.append(cell);
  }
  const coverage = row.option_chain_coverage_rate === null || row.option_chain_coverage_rate === undefined
    ? "未知"
    : formatPercent(row.option_chain_coverage_rate);
  const quality = createElement(
    "p",
    "positioning-quality",
    `覆盖 ${coverage} · 样本 ${formatInteger(row.unique_contract_count)}`
      + ` · 缺 OI ${formatPercent(row.missing_open_interest_rate)}`
      + ` · 缺 Greeks ${formatPercent(row.missing_greeks_rate)}`
      + ` · ${row.coverage_limitation || "完整链覆盖未声明"}`,
  );
  card.append(head, metrics, quality);
  return card;
}

function buildManagementPreview(candidate) {
  if (candidate.schema === "options_copilot.holdings_close_preview.v1") {
    return buildHoldingsClosePreview(candidate);
  }
  const card = createElement("article", "management-preview");
  const header = createElement("div", "management-preview-header");
  const titleBlock = createElement("div");
  titleBlock.append(
    createElement(
      "p",
      "management-preview-title",
      `${String(candidate.underlying || candidate.symbol || "--").toUpperCase()} · ${String(candidate.structure || candidate.management_kind || "MANAGEMENT")}`,
    ),
    createElement(
      "p",
      "management-preview-meta",
      `candidate ${shortHash(candidate.candidate_hash)} · proof ${shortHash(candidate.transition_proof_hash)}`,
    ),
  );
  header.append(
    titleBlock,
    createElement(
      "span",
      "status-chip status-unknown",
      `${String(candidate.review_state || "OBSERVATION_ONLY")} · PREVIEW_ONLY`,
    ),
  );

  const proof = candidate.transition_proof && typeof candidate.transition_proof === "object"
    ? candidate.transition_proof
    : {};
  const beforeRisk = proof.before_risk || candidate.before_risk || {};
  const afterRisk = proof.after_risk || candidate.after_risk || {};
  const capital = proof.capital_usage || {};
  const grid = createElement("div", "management-proof-grid");
  appendManagementMetric(
    grid,
    "可执行退出现金流",
    formatMoney(candidate.all_in_close_cashflow_usd, { signed: true }),
  );
  appendManagementMetric(
    grid,
    "入场净成本(信用) / 最大亏损 / 最大盈利",
    `${formatMoney(candidate.entry_net_cost_usd ?? candidate.entry_net_credit_usd)} / ${formatMoney(candidate.entry_max_loss_usd)} / ${formatMoney(candidate.entry_max_profit_usd)}`,
  );
  appendManagementMetric(
    grid,
    "止损 / 止盈复核线",
    `${formatMoney(candidate.stop_review_cashflow_usd)} / ${formatMoney(candidate.profit_review_cashflow_usd)}`,
  );
  appendManagementMetric(
    grid,
    "签署成本 / 压力滑点",
    `${formatMoney(candidate.estimated_execution_cost_usd)} / ${formatMoney(candidate.stress_slippage_usd)}`,
  );
  appendManagementMetric(
    grid,
    "报价年龄 / 腿间偏差",
    `${firstValue(candidate, ["oldest_quote_age_seconds"], "--")}s / ${firstValue(candidate, ["maximum_leg_skew_seconds"], "--")}s`,
  );
  appendManagementMetric(
    grid,
    "最大亏损变化",
    `${formatMoney(beforeRisk.max_loss_usd)} → ${formatMoney(afterRisk.max_loss_usd)}`,
  );
  appendManagementMetric(
    grid,
    "资本占用变化",
    `${formatMoney(capital.before_capital_usage_usd)} → ${formatMoney(capital.after_capital_usage_usd)}`,
  );
  appendManagementMetric(
    grid,
    "原子快照",
    `${shortHash(candidate.broker_snapshot_hash)} · ${shortHash(candidate.quote_batch_hash)}`,
  );

  const legs = createElement("div", "management-leg-list");
  const before = Array.isArray(proof.before_positions) ? proof.before_positions : [];
  const after = Array.isArray(proof.after_positions) ? proof.after_positions : [];
  legs.append(
    createElement("strong", "management-preview-title", "Before → After"),
    createElement(
      "div",
      "management-leg-row",
      `${managementPositionSummary(before)} → ${managementPositionSummary(after)}`,
    ),
  );
  const executionLegs = Array.isArray(candidate.execution_legs) ? candidate.execution_legs : [];
  for (const leg of executionLegs) {
    const row = createElement("div", "management-leg-row");
    const contractTerms = [leg.right, leg.strike, leg.expiration]
      .filter((value) => value !== null && value !== undefined && String(value).trim() !== "")
      .join(" ");
    row.append(
      createElement(
        "span",
        "",
        `${leg.action || "--"} ${leg.action_quantity ?? "--"}× ${leg.local_symbol || `conId ${leg.contract_id || "--"}`}`
          + `${contractTerms ? ` · ${contractTerms}` : ""}`,
      ),
      createElement(
        "span",
        "",
        `position ${formatInteger(leg.current_signed_quantity)} · delta ${formatInteger(leg.signed_quantity_delta)}`
          + ` · bid/ask ${formatQuote(leg.bid)}/${formatQuote(leg.ask)}`
          + ` · executable ${formatQuote(leg.executable_price)}`,
      ),
    );
    legs.append(row);
  }

  const exitList = createElement("ul", "management-exit-list");
  const exitPlan = candidate.exit_plan && typeof candidate.exit_plan === "object"
    ? candidate.exit_plan
    : {};
  for (const [label, value] of [
    ["失效", `${candidate.thesis_invalidation_state || "NOT_EVALUATED"} · ${exitPlan.thesis_invalidation || "--"}`],
    ["风险止损", `${candidate.risk_stop_state || "NOT_EVALUATED"} · ${exitPlan.risk_stop || "--"}`],
    ["获利", `${candidate.profit_take_state || "NOT_EVALUATED"} · ${exitPlan.profit_take || "--"}`],
    ["时间退出", `${candidate.time_stop_state || "NOT_EVALUATED"} · ${exitPlan.time_stop || "--"}`],
    ["最晚持有", exitPlan.maximum_holding_date],
    ["坏报价", exitPlan.bad_quote_action],
  ]) {
    exitList.append(createElement("li", "", `${label}: ${value || "--"}`));
  }
  card.append(header, grid, legs, exitList);
  return card;
}

function appendManagementMetric(container, label, value) {
  const cell = createElement("div");
  cell.append(createElement("span", "", label), createElement("strong", "", value));
  container.append(cell);
}

function managementPositionSummary(positions) {
  if (!Array.isArray(positions) || positions.length === 0) return "FLAT";
  return positions.map((position) => {
    const symbol = position.local_symbol || `conId ${position.contract_id || "--"}`;
    return `${position.signed_quantity ?? "--"}×${symbol}`;
  }).join(" / ");
}

function holdingsCloseView(candidate, now = Date.now()) {
  const oldest = Date.parse(candidate.oldest_quote_at);
  const generated = Date.parse(candidate.generated_at);
  const deadline = oldest + 5000;
  const legs = Array.isArray(candidate.execution_legs) ? candidate.execution_legs : [];
  const current = candidate.quote_reference_status === "CURRENT_COMPONENT_REFERENCE"
    && candidate.source_payload_valid === true
    && candidate.scope === "ALL_OBSERVED_OPTION_HOLDINGS"
    && candidate.grouping_status === "NOT_INFERRED"
    && candidate.review_state === "PREVIEW_UNVERIFIED"
    && Number.isFinite(now) && Number.isFinite(oldest) && Number.isFinite(generated)
    && oldest <= generated && generated <= now && now <= deadline
    && legs.length > 0 && legs.length <= 8
    && legs.every((leg) => {
      const at = Date.parse(leg.quote_observed_at);
      return Number.isFinite(at) && oldest <= at && at <= generated && now - at <= 5000;
    });
  return {
    current,
    deadline,
    status: current ? "逐腿参考新鲜 · 仍不可交易" : "逐腿参考过期或不可用 · 数值已停用",
    gross: current ? candidate.gross_component_liquidation_cashflow_usd : null,
    net: current ? candidate.all_in_close_cashflow_usd : null,
    costs: current ? candidate.estimated_execution_cost_usd : null,
    stress: current ? candidate.stress_slippage_usd : null,
    holdingLoss: current ? candidate.before_payoff?.max_loss_usd : null,
    legs,
  };
}

function buildHoldingsClosePreview(candidate, now = Date.now()) {
  const view = holdingsCloseView(candidate, now);
  const card = createElement("article", "management-preview");
  card.dataset.holdingsQuoteDeadline = String(view.deadline);
  card.dataset.holdingsGeneratedAt = String(Date.parse(candidate.generated_at));
  card.dataset.holdingsQuoteExpired = view.current ? "false" : "true";
  card.append(
    createElement("p", "management-preview-title", `${candidate.symbol || "--"} · 全部已观察持仓 · 策略归属未推断`),
    createElement("p", "position-plan-boundary", "PREVIEW_UNVERIFIED / NO_TRADE · 仅为全部持仓平仓的条件核算，不是退出建议或已获批准的管理方案。"),
    createElement("p", "management-preview-meta", `源报价 ${formatTime(candidate.oldest_quote_at)} · 核算 ${formatTime(candidate.generated_at)} · 来源 ${shortHash(candidate.candidate_hash)}`),
  );
  const freshness = createElement("p", "position-plan-boundary", view.status);
  freshness.dataset.holdingsFreshness = "true";
  card.append(freshness);
  const grid = createElement("div", "management-proof-grid");
  for (const [label, value] of [
    ["逐腿自然买卖价合计", view.gross],
    ["扣除一次退出成本的条件估值", view.net],
    ["一次退出成本（佣金＋压力预留）", view.costs],
    ["其中压力滑点（不再次相加）", view.stress],
    ["相对逐腿估值＋退出预留的到期亏损上界", view.holdingLoss],
  ]) {
    const cell = createElement("div");
    const metric = createElement("strong", "", formatMoney(value, { signed: true }));
    metric.dataset.holdingsCurrentValue = "true";
    cell.append(createElement("span", "", label), metric);
    grid.append(cell);
  }
  card.append(grid, createElement("p", "position-plan-boundary", "以上不是历史入场成本损益，也不是券商保证金或释放资本。逐腿报价之和不保证能够原子成交。"));
  const legs = createElement("div", "management-leg-list");
  legs.append(createElement("strong", "", "精确持仓反向核对（不是分腿执行顺序）"));
  for (const leg of view.legs) {
    const row = createElement("div", "management-leg-row");
    row.append(createElement("span", "", `${leg.local_symbol || `conId ${leg.contract_id || "--"}`} · 持仓 ${leg.current_signed_quantity ?? "--"} · 全量反向 ${leg.signed_quantity_delta ?? "--"}`));
    const quote = createElement("span", "", view.current ? `bid/ask ${formatQuote(leg.bid)}/${formatQuote(leg.ask)}` : "--");
    quote.dataset.holdingsCurrentValue = "true";
    row.append(quote);
    legs.append(row);
  }
  card.append(
    legs,
    createElement("p", "position-plan-boundary", "仅在所有腿均成交并完成券商对账后，持仓终态才为零；这不证明成交过程、指派或资金风险为零。"),
    createElement("p", "position-plan-boundary", "交易日历、除息/提前行权、指派、篮子成交、分腿过程及保证金均未完成验证。不得拆腿执行或据此创建指令。"),
    createElement("p", "management-preview-meta", `原子快照 ${shortHash(candidate.broker_snapshot_hash)} · 报价批次 ${shortHash(candidate.quote_batch_hash)} · 条件终态证明 ${shortHash(candidate.transition_proof_hash)}`),
  );
  return card;
}

function expireHoldingsClosePreviews(now = Date.now()) {
  if (typeof document.querySelectorAll !== "function") return;
  document.querySelectorAll("[data-holdings-quote-deadline]").forEach((card) => {
    if (card.dataset.holdingsQuoteExpired === "true") return;
    const deadline = Number(card.dataset.holdingsQuoteDeadline);
    const generated = Number(card.dataset.holdingsGeneratedAt);
    if (Number.isFinite(now) && Number.isFinite(deadline) && Number.isFinite(generated)
        && generated <= now && now <= deadline) return;
    card.dataset.holdingsQuoteExpired = "true";
    card.querySelectorAll("[data-holdings-current-value]").forEach((node) => { node.textContent = "--"; });
    card.querySelectorAll("[data-holdings-freshness]").forEach((node) => {
      node.textContent = "逐腿参考过期或不可用 · 数值已停用";
    });
  });
}

function renderCampaign(bootstrap) {
  const campaign = bootstrap.campaign || {};
  const account = bootstrap.account || bootstrap.ibkr || bootstrap.broker || {};
  const start = numberOrNull(firstValue(campaign, ["starting_nlv_usd", "start_nlv_usd", "start_nlv"])) ?? 2012.44;
  const current = numberOrNull(firstValue(campaign, ["strategy_nav_usd", "strategy_nav"]));
  appState.strategyNavUsd = current !== null && current > 0 ? current : null;
  const target = numberOrNull(firstValue(campaign, ["target_nlv_usd", "target_usd", "target"])) ?? 10000;

  setText("nlv-current", formatMoney(current));
  setText(
    "account-nlv-observed",
    formatMoney(firstValue(account, ["net_liquidation_usd", "net_liquidation", "nlv_usd", "nlv"])),
  );
  setText("nlv-target", formatMoney(target));
  setText("campaign-remaining", current === null ? "距目标 --" : `距目标 ${formatMoney(Math.max(target - current, 0))}`);

  const suppliedProgress = numberOrNull(firstValue(campaign, ["progress_fraction", "progress"]));
  const progress = suppliedProgress !== null
    ? Math.max(0, Math.min(100, Math.abs(suppliedProgress) <= 1 ? suppliedProgress * 100 : suppliedProgress))
    : current === null || target <= 0
      ? 0
      : Math.max(0, Math.min(100, (current / target) * 100));
  const track = byId("campaign-progress");
  track.setAttribute("aria-valuenow", progress.toFixed(1));
  byId("campaign-progress-bar").style.width = `${progress}%`;
  setText("campaign-percent", `${progress.toFixed(1)}% 已完成`);

  const milestones = [2500, 3500, 5000, 7500, 10000];
  const suppliedNext = numberOrNull(firstValue(campaign, ["next_milestone", "next_milestone_usd"]));
  const next = suppliedNext ?? milestones.find((value) => current === null || value > current) ?? target;
  setText("campaign-stage", `${compactMoney(start)} → ${compactMoney(next)}`);

  const reconciled = firstValue(account, ["reconciled", "account_reconciled"], null);
  const reconciliationStatus = String(account.reconciliation_status || "UNAVAILABLE").toUpperCase();
  const strategyNavAsOf = firstValue(account, ["strategy_nav_asof"], campaign.strategy_nav_asof);
  const accountObservedAt = firstValue(account, ["observed_at"], campaign.account_observed_at);
  const difference = numberOrNull(firstValue(
    account,
    ["reconciliation_difference_usd"],
    campaign.reconciliation_difference_usd,
  ));
  const authorityHash = firstValue(
    account,
    ["strategy_nav_authority_hash"],
    campaign.strategy_nav_authority_hash,
  );
  const reconciliationLabel = reconciled === true && reconciliationStatus === "VERIFIED"
    ? "VERIFIED"
    : reconciliationStatus;
  setText(
    "nav-reconciliation-evidence",
    `Strategy NAV 对账 · ${reconciliationLabel} · NAV ${formatTime(strategyNavAsOf)} · `
      + `账户 ${formatTime(accountObservedAt)} · 差额 ${formatMoney(difference)} · `
      + `authority ${shortHash(authorityHash) || "--"}`,
  );
}

function shortHash(value) {
  const text = typeof value === "string" ? value.trim() : "";
  return text.length > 12 ? `${text.slice(0, 8)}…${text.slice(-4)}` : text;
}

function compactMoney(value) {
  const number = numberOrNull(value) ?? 0;
  return number >= 1000 ? `$${(number / 1000).toFixed(number % 1000 === 0 ? 0 : 1)}K` : `$${number.toFixed(0)}`;
}

function renderProviderConfiguration(payload) {
  const specs = [
    ["JIN10", "provider-config-jin10", "金十"],
    ["FINNHUB", "provider-config-finnhub", "Finnhub"],
    ["ALPHA_VANTAGE", "provider-config-alpha-vantage", "Alpha Vantage"],
    ["DEEPSEEK", "provider-config-deepseek", "DeepSeek"],
  ];
  const rows = Array.isArray(payload?.providers) ? payload.providers : [];
  const byProvider = new Map(rows.map((item) => [String(item?.provider || "").toUpperCase(), item]));
  let errorCount = 0;
  let restartCount = 0;
  for (const [provider, identifier, label] of specs) {
    const item = byProvider.get(provider) || {};
    const status = String(item.status || "UNKNOWN").toUpperCase();
    const activated = item.activated === true;
    const composed = item.composed === true;
    const runtimeLoaded = item.runtime_loaded === true;
    const restartRequired = item.restart_required === true;
    const node = byId(identifier);
    if (!node) continue;
    node.classList.remove("status-up", "status-down", "status-stale", "status-unknown");
    if (status === "CONFIGURED" && runtimeLoaded) node.classList.add("status-up");
    else if (status === "ERROR") {
      node.classList.add("status-down");
      errorCount += 1;
    } else if (status === "DISABLED" || status === "CONFIGURED") node.classList.add("status-stale");
    else node.classList.add("status-unknown");
    if (restartRequired) restartCount += 1;
    let runtimeState = "";
    if (status === "CONFIGURED") {
      if (restartRequired) runtimeState = " · 需要重启加载";
      else if (provider === "JIN10" && !activated) runtimeState = " · 仍需 activation";
      else if (runtimeLoaded) runtimeState = " · 已加载";
      else if (!composed) runtimeState = " · 运行时未启用";
      else runtimeState = " · 未就绪";
    }
    node.textContent = `${label} · ${status}${runtimeState}`;
  }
  setText(
    "provider-config-note",
    errorCount > 0
      ? `api_keys.local.json 读取失败 · ${errorCount} 个 provider 已安全禁用`
      : restartCount > 0
        ? `检测到 ${restartCount} 项配置或 activation 变化；重启 Options Copilot 后才会加载。`
      : "读取 api_keys.local.json；密钥值永不通过 API 或 GUI 返回。",
  );
}

function normalizeBrokerState(broker = {}, envelope = {}) {
  const status = String(firstValue(broker, ["freshness_status", "snapshot_status", "status", "state"], "UNKNOWN")).toUpperCase();
  const connected = firstValue(broker, ["connected"], null);
  const warnings = [
    ...(Array.isArray(envelope.warnings) ? envelope.warnings : []),
    ...(Array.isArray(broker.reasons) ? broker.reasons : []),
    firstValue(broker, ["reason"], null),
  ].filter(Boolean).map((value) => String(value).toUpperCase());
  if (connected === false || ["DISCONNECTED", "DOWN", "ERROR"].includes(status)) {
    return "DISCONNECTED";
  }
  if (
    status === "SAVED_INSTRUCTION_UNKNOWN"
    || warnings.some((reason) => (
      reason.includes("SAVED_INSTRUCTION_STATE_UNKNOWN")
      || reason.includes("UNSUBMITTED_INSTRUCTIONS_UNKNOWN")
      || reason.includes("CONTROL_INSTRUCTIONS_STATE_UNKNOWN")
    ))
  ) {
    return "SAVED_INSTRUCTION_UNKNOWN";
  }
  if (status === "STALE" || broker.stale === true) return "STALE";
  if (["PARTIAL", "UNAVAILABLE", "UNKNOWN", "DEGRADED"].includes(status)) {
    return "PARTIAL";
  }
  if (["FRESH", "CURRENT"].includes(status)) return "FRESH";
  const reconciled = firstValue(broker, ["reconciled", "account_reconciled"], null);
  const marketData = firstValue(broker, ["market_data_status", "quote_status", "market_data"], null);
  const marketDataReady = marketData === true
    || ["READY", "AVAILABLE", "LIVE", "CURRENT"].includes(String(marketData || "").toUpperCase());
  if (
    connected === true
    && reconciled === true
    && marketDataReady
    && ["UP", "READY", "CONNECTED", "HEALTHY"].includes(status)
  ) {
    return "FRESH";
  }
  return "PARTIAL";
}

function renderBroker(bootstrap, health) {
  if (bootstrap?.account || bootstrap?.ibkr || bootstrap?.broker) {
    appState.bootstrapSnapshot = bootstrap;
  }
  const healthIbkr = health.dependencies?.ibkr_snapshot
    || health.dependencies?.ibkr
    || health.dependencies?.IBKR
    || {};
  const broker = bootstrap.ibkr || bootstrap.broker || bootstrap.account || {};
  const brokerState = appState.postOutcomeUnknownReason
    ? "SAVED_INSTRUCTION_UNKNOWN"
    : normalizeBrokerState(
      { ...healthIbkr, ...broker },
      { warnings: bootstrap.warnings },
    );
  appState.controlContext.brokerState = brokerState;
  const status = byId("ibkr-status");
  status.classList.remove("status-up", "status-down", "status-stale", "status-unknown");
  status.classList.add(
    brokerState === "FRESH"
      ? "status-up"
      : brokerState === "DISCONNECTED"
        ? "status-down"
        : "status-stale",
  );
  status.textContent = brokerState;

  setText("ibkr-account", maskAccount(firstValue(broker, ["account_masked", "account_id", "account"])));
  const reconciled = firstValue(broker, ["reconciled", "account_reconciled"]);
  setText("ibkr-reconciled", reconciled === true ? "已对账" : reconciled === false ? "未对账" : "--");
  const marketData = firstValue(broker, ["market_data_status", "quote_status", "market_data"]);
  setText("ibkr-market-data", marketData === true ? "可用" : marketData === false ? "不可用" : marketData || "--");
  synchronizeActionControls();
}

function renderPositions(payload) {
  appState.positionsSnapshot = payload;
  syncOverviewDetailVisibility();
  const positions = Array.isArray(payload.positions) ? payload.positions : [];
  const positionStatus = String(payload.status || "UNKNOWN").toUpperCase();
  const display = normalizePositionDisplayState(
    positionStatus,
    appState.controlContext.brokerState,
  );
  const explicitlyStale = positionStatus === "STALE";
  const lastKnownOnly = display.last_known_only || explicitlyStale;
  const lastKnownDescription = display.status === "DISCONNECTED"
    ? "断线前最后已知仓位"
    : "最后已知仓位";
  const positionStateKnown = payload.position_state_known !== false
    && display.displayable;
  const positionStateDisplayable = positionStateKnown || lastKnownOnly;
  const container = byId("legacy-position-list");
  container.replaceChildren();
  renderObservedPositionPlans(
    positionStateDisplayable ? positions : [],
    appState.strategyNavUsd,
    { lastKnownOnly },
  );
  setText(
    "position-count",
    positionStateDisplayable
      ? `${positions.length}${lastKnownOnly ? `（${display.status} / LAST_KNOWN_ONLY）` : ""}`
      : "--",
  );
  if (!positionStateDisplayable) {
    const reason = String(payload.reason || "SNAPSHOT_UNAVAILABLE");
    container.append(createElement("p", "empty-state", `账户仓位状态不可用 · ${reason}`));
    return;
  }
  if (lastKnownOnly) {
    container.append(createElement(
      "p",
      "empty-state",
      `${display.status} · 以下仅为${lastKnownDescription}，不可用于风险、排名、审批、指令或下单`,
    ));
  }
  if (positions.length === 0) {
    container.append(createElement("p", "empty-state", lastKnownOnly ? "最后已知快照中没有开放仓位" : "当前没有开放仓位"));
    return;
  }

  for (const position of positions) {
    const symbol = String(firstValue(position, ["underlying", "symbol"], "--")).toUpperCase();
    const legacy = position.legacy === true || position.is_legacy === true || symbol === "GLD";
    const row = createElement("article", `position-row${legacy ? " legacy" : ""}`);
    if (lastKnownOnly) {
      row.classList.add("position-last-known");
      row.dataset.authority = "LAST_KNOWN_ONLY";
    }
    const main = createElement("div", "position-main");
    const symbolNode = createElement("div", "position-symbol", symbol);
    if (legacy) symbolNode.append(createElement("span", "legacy-label", "遗留仓位"));
    const pnl = numberOrNull(firstValue(position, ["unrealized_pnl_usd", "pnl_usd", "unrealized_pnl"]));
    const pnlNode = createElement("div", `position-pnl ${pnl !== null && pnl >= 0 ? "positive" : "negative"}`, formatMoney(pnl, { signed: true }));
    main.append(symbolNode, pnlNode);

    const description = firstValue(position, ["description", "contract", "strategy", "local_symbol"], "期权组合");
    const quantity = firstValue(position, ["quantity", "qty", "contracts"]);
    const contract = createElement("p", "position-contract", `${description} · 数量 ${quantity ?? "--"}`);
    const numericQuantity = numberOrNull(quantity);
    const securityType = String(firstValue(position, ["security_type", "sec_type"], "")).toUpperCase();
    const multiplier = numberOrNull(position.multiplier) ?? 100;
    const averageCostUsd = numberOrNull(firstValue(position, ["average_cost", "average_cost_usd"]));
    const marketPrice = numberOrNull(firstValue(position, ["market_price", "mark", "last_price"]));
    const marketValueUsd = numberOrNull(firstValue(position, ["market_value", "market_value_usd"]));
    const realizedPnlUsd = numberOrNull(firstValue(position, ["realized_pnl", "realized_pnl_usd"]));
    const isOption = securityType === "OPT" && multiplier > 0;
    const entryBasisUsd = averageCostUsd !== null && numericQuantity !== null
      ? averageCostUsd * Math.abs(numericQuantity)
      : null;
    const entryPerShare = isOption && averageCostUsd !== null
      ? averageCostUsd / multiplier
      : null;
    const side = numericQuantity !== null && numericQuantity < 0 ? "SELL/空头" : "BUY/多头";
    const entryLabel = numericQuantity !== null && numericQuantity < 0 ? "入场信用" : "入场借记";
    const legEconomics = createElement(
      "p",
      "position-leg-economics",
      `${side} · ${entryLabel} ${formatMoney(entryBasisUsd)}`
        + `${entryPerShare === null ? "" : ` (${formatQuote(entryPerShare)}/股)`}`
        + ` · 当前 mark ${formatQuote(marketPrice)}/股`
        + ` · 当前市值 ${formatMoney(marketValueUsd, { signed: true })}`
        + ` · 未实现 ${formatMoney(pnl, { signed: true })}`
        + ` · 已实现 ${formatMoney(realizedPnlUsd, { signed: true })}`,
    );
    row.append(main, contract, legEconomics);
    container.append(row);
  }
}

function deriveDefinedRiskVerticalPlan(positions, strategyNavUsd = null) {
  if (!Array.isArray(positions) || positions.length !== 2) return null;
  const optionLegs = positions.map((position) => ({
    source: position,
    symbol: String(firstValue(position, ["underlying", "symbol"], "")).trim().toUpperCase(),
    securityType: String(firstValue(position, ["security_type", "sec_type"], "")).trim().toUpperCase(),
    expiration: String(firstValue(position, ["expiration", "expiry"], "")).trim(),
    right: String(firstValue(position, ["right", "option_right"], "")).trim().toUpperCase(),
    quantity: numberOrNull(firstValue(position, ["quantity", "qty", "contracts"])),
    strike: numberOrNull(position.strike),
    averageCostUsd: numberOrNull(firstValue(position, ["average_cost", "average_cost_usd"])),
    marketValueUsd: numberOrNull(firstValue(position, ["market_value", "market_value_usd"])),
    unrealizedPnlUsd: numberOrNull(firstValue(position, ["unrealized_pnl", "unrealized_pnl_usd", "pnl_usd"])),
    multiplier: numberOrNull(position.multiplier) ?? 100,
  }));
  if (optionLegs.some((leg) => (
    leg.securityType !== "OPT"
    || !leg.symbol
    || !leg.expiration
    || !["C", "CALL", "P", "PUT"].includes(leg.right)
    || leg.quantity === null
    || leg.quantity === 0
    || leg.strike === null
    || leg.averageCostUsd === null
    || leg.multiplier <= 0
  ))) return null;
  const [first, second] = optionLegs;
  if (
    first.symbol !== second.symbol
    || first.expiration !== second.expiration
    || first.right[0] !== second.right[0]
    || first.multiplier !== second.multiplier
    || Math.abs(first.quantity) !== Math.abs(second.quantity)
    || Math.sign(first.quantity) === Math.sign(second.quantity)
  ) return null;
  const longLeg = optionLegs.find((leg) => leg.quantity > 0);
  const shortLeg = optionLegs.find((leg) => leg.quantity < 0);
  if (!longLeg || !shortLeg) return null;
  const contractCount = Math.abs(longLeg.quantity);
  const multiplier = longLeg.multiplier;
  const widthUsd = Math.abs(shortLeg.strike - longLeg.strike) * multiplier * contractCount;
  const rawEntryUsd = optionLegs.reduce(
    (total, leg) => total + (leg.averageCostUsd * leg.quantity),
    0,
  );
  const isDebit = rawEntryUsd > 0;
  const entryDebitUsd = isDebit ? rawEntryUsd : null;
  const entryCreditUsd = !isDebit && rawEntryUsd < 0 ? -rawEntryUsd : null;
  const maxLossUsd = isDebit ? entryDebitUsd : widthUsd - entryCreditUsd;
  const maxProfitUsd = isDebit ? widthUsd - entryDebitUsd : entryCreditUsd;
  if (maxLossUsd <= 0 || maxProfitUsd <= 0) return null;
  const right = longLeg.right[0];
  let strategy = null;
  if (right === "C" && longLeg.strike < shortLeg.strike) strategy = "BULL_CALL_DEBIT_VERTICAL";
  if (right === "C" && longLeg.strike > shortLeg.strike) strategy = "BEAR_CALL_CREDIT_VERTICAL";
  if (right === "P" && longLeg.strike > shortLeg.strike) strategy = "BEAR_PUT_DEBIT_VERTICAL";
  if (right === "P" && longLeg.strike < shortLeg.strike) strategy = "BULL_PUT_CREDIT_VERTICAL";
  if (!strategy || strategy.includes("DEBIT") !== isDebit) return null;
  const currentValueAvailable = optionLegs.every((leg) => leg.marketValueUsd !== null);
  const currentValueUsd = currentValueAvailable
    ? optionLegs.reduce((total, leg) => total + leg.marketValueUsd, 0)
    : null;
  const reportedPnlAvailable = optionLegs.every((leg) => leg.unrealizedPnlUsd !== null);
  const currentPnlUsd = reportedPnlAvailable
    ? optionLegs.reduce((total, leg) => total + leg.unrealizedPnlUsd, 0)
    : currentValueUsd === null
      ? null
      : currentValueUsd - rawEntryUsd;
  const entryDebitPerShare = entryDebitUsd === null ? null : entryDebitUsd / multiplier / contractCount;
  const entryCreditPerShare = entryCreditUsd === null ? null : entryCreditUsd / multiplier / contractCount;
  const maxProfitPerShare = maxProfitUsd / multiplier / contractCount;
  const currentClosePerShare = currentValueUsd === null
    ? null
    : (isDebit ? currentValueUsd : -currentValueUsd) / multiplier / contractCount;
  const stopClosePerShare = isDebit
    ? entryDebitPerShare * 0.60
    : entryCreditPerShare + ((maxLossUsd / multiplier / contractCount) * 0.60);
  const profitClosePerShare = isDebit
    ? entryDebitPerShare + (maxProfitPerShare * 0.60)
    : entryCreditPerShare * 0.40;
  const action = currentClosePerShare === null
    ? "HOLD_MONITOR"
    : isDebit
      ? currentClosePerShare <= stopClosePerShare
        ? "EXIT_REVIEW"
        : currentClosePerShare >= profitClosePerShare
          ? "TAKE_PROFIT_REVIEW"
          : "HOLD_MONITOR"
      : currentClosePerShare >= stopClosePerShare
        ? "EXIT_REVIEW"
        : currentClosePerShare <= profitClosePerShare
          ? "TAKE_PROFIT_REVIEW"
          : "HOLD_MONITOR";
  const expiryParts = longLeg.expiration.split("-").map((value) => Number(value));
  let timeExitDate = null;
  if (expiryParts.length === 3 && expiryParts.every(Number.isInteger)) {
    const cursor = new Date(Date.UTC(expiryParts[0], expiryParts[1] - 1, expiryParts[2]));
    let remaining = 2;
    while (remaining > 0) {
      cursor.setUTCDate(cursor.getUTCDate() - 1);
      if (![0, 6].includes(cursor.getUTCDay())) remaining -= 1;
    }
    timeExitDate = cursor.toISOString().slice(0, 10);
  }
  const rounded = (value) => value === null ? null : Math.round((value + Number.EPSILON) * 100) / 100;
  return {
    action,
    authority: "OBSERVATION_ONLY",
    symbol: longLeg.symbol,
    strategy,
    closeAction: isDebit ? "SELL" : "BUY",
    expiration: longLeg.expiration,
    timeExitDate,
    longStrike: longLeg.strike,
    shortStrike: shortLeg.strike,
    contractCount,
    entryDebitUsd: rounded(entryDebitUsd),
    entryCreditUsd: rounded(entryCreditUsd),
    entryDebitPerShare: rounded(entryDebitPerShare),
    entryCreditPerShare: rounded(entryCreditPerShare),
    maxLossUsd: rounded(maxLossUsd),
    maxProfitUsd: rounded(maxProfitUsd),
    breakeven: rounded(
      strategy === "BULL_CALL_DEBIT_VERTICAL"
        ? longLeg.strike + entryDebitPerShare
        : strategy === "BEAR_CALL_CREDIT_VERTICAL"
          ? shortLeg.strike + entryCreditPerShare
          : strategy === "BEAR_PUT_DEBIT_VERTICAL"
            ? longLeg.strike - entryDebitPerShare
            : shortLeg.strike - entryCreditPerShare
    ),
    currentValueUsd: rounded(currentValueUsd),
    currentPnlUsd: rounded(currentPnlUsd),
    currentClosePerShare: rounded(currentClosePerShare),
    currentCredit: rounded(isDebit ? currentClosePerShare : null),
    stopClosePerShare: rounded(stopClosePerShare),
    profitClosePerShare: rounded(profitClosePerShare),
    stopCredit: rounded(isDebit ? stopClosePerShare : null),
    profitTargetCredit: rounded(isDebit ? profitClosePerShare : null),
    riskFraction: rounded(numberOrNull(strategyNavUsd) > 0 ? maxLossUsd / Number(strategyNavUsd) : null),
  };
}

function renderObservedPositionPlans(positions, strategyNavUsd, { lastKnownOnly = false } = {}) {
  const container = byId("position-plan-list");
  appState.hasDerivedManagementPreview = false;
  if (!container) return;
  container.replaceChildren();
  if (lastKnownOnly) {
    container.append(createElement("p", "empty-state", "LAST_KNOWN_ONLY · 陈旧仓位不能生成当前退出判断。"));
    return;
  }
  if (!Array.isArray(positions) || positions.length === 0) {
    container.append(createElement("p", "empty-state", "当前没有需要管理的开放组合。"));
    return;
  }
  const plan = deriveDefinedRiskVerticalPlan(positions, strategyNavUsd);
  if (!plan) {
    container.append(createElement("p", "empty-state", "当前持仓不能套用标准两腿策略模板；策略归属未推断。全部持仓平仓核算另需逐腿实时报价、成本与风险验证。"));
    return;
  }
  appState.hasDerivedManagementPreview = true;
  const card = createElement("article", "position-plan-card");
  const header = createElement("div", "position-plan-header");
  header.append(
    createElement("strong", "", `${plan.symbol} · ${plan.longStrike}/${plan.shortStrike} ${plan.strategy}`),
    createElement("span", "status-chip status-stale", plan.action),
  );
  const boundary = createElement(
    "p",
    "position-plan-boundary",
    "OBSERVATION_ONLY · 当前价值来自 IBKR position mark，不是可执行组合报价；真正退出前必须刷新两腿 bid/ask，并以整组限价单复核。",
  );
  const metrics = createElement("div", "position-plan-metrics");
  for (const [label, value] of [
    [plan.entryDebitUsd !== null ? "净成本 / 最大亏损" : "净信用 / 最大亏损", `${formatMoney(plan.entryDebitUsd ?? plan.entryCreditUsd)} · ${formatMoney(plan.maxLossUsd)}`],
    ["最大盈利 / 到期盈亏平衡", `${formatMoney(plan.maxProfitUsd)} · ${plan.breakeven}`],
    ["当前 IBKR mark / 未实现盈亏", `${formatMoney(plan.currentValueUsd)} · ${formatMoney(plan.currentPnlUsd, { signed: true })} · 平仓${plan.closeAction === "SELL" ? "信用" : "借记"} ${formatQuote(plan.currentClosePerShare)}/股`],
    ["止损复核", `整组平仓${plan.closeAction === "SELL" ? "信用 ≤" : "借记 ≥"} ${formatQuote(plan.stopClosePerShare)}`],
    ["止盈复核", `整组平仓${plan.closeAction === "SELL" ? "信用 ≥" : "借记 ≤"} ${formatQuote(plan.profitClosePerShare)}`],
    ["时间退出", `${plan.timeExitDate || "到期前两个工作日"} · 仅按周一至周五，未校验交易所休市 · 不进入到期/指派窗口`],
  ]) {
    const cell = createElement("div");
    cell.append(createElement("span", "", label), createElement("strong", "", value));
    metrics.append(cell);
  }
  const instruction = createElement(
    "p",
    "position-plan-instruction",
    `当前计划：${plan.action}。退出只使用 ${plan.closeAction} ${plan.contractCount} ${plan.symbol} ${plan.longStrike}/${plan.shortStrike} vertical 组合限价；不拆腿、不用市价单。`,
  );
  card.append(header, boundary, metrics, instruction);
  container.append(card);
}

function normalizePositionDisplayState(positionStatus, brokerState) {
  const status = String(positionStatus || "UNKNOWN").toUpperCase();
  const broker = String(brokerState || "PARTIAL").toUpperCase();
  if (status === "UNAVAILABLE") {
    return {
      status: "UNAVAILABLE",
      displayable: false,
      last_known_only: false,
      approval_eligible: false,
    };
  }
  const blockedBrokerStates = new Set([
    "PARTIAL",
    "STALE",
    "DISCONNECTED",
    "SAVED_INSTRUCTION_UNKNOWN",
  ]);
  const currentStates = new Set(["CURRENT", "FRESH"]);
  let effectiveStatus = "UNAVAILABLE";
  if (blockedBrokerStates.has(broker)) {
    effectiveStatus = broker;
  } else if (!currentStates.has(broker)) {
    effectiveStatus = "UNAVAILABLE";
  } else if (["PARTIAL", "STALE", "DISCONNECTED", "SAVED_INSTRUCTION_UNKNOWN"].includes(status)) {
    effectiveStatus = status;
  } else if (currentStates.has(status)) {
    effectiveStatus = "CURRENT";
  }
  return {
    status: effectiveStatus,
    displayable: true,
    last_known_only: effectiveStatus !== "CURRENT",
    approval_eligible: false,
  };
}

function candidateUnderlying(candidate) {
  const view = candidateView(candidate);
  const value = String(firstValue(view, ["underlying", "symbol", "ticker"], "")).trim().toUpperCase();
  return /^[A-Z][A-Z0-9.\-]{0,15}$/.test(value) ? value : null;
}

function candidateIdentity(candidate) {
  const view = candidateView(candidate);
  const value = String(firstValue(view, ["candidate_id"], candidate?.candidate_id || "")).trim();
  return /^[A-Za-z0-9][A-Za-z0-9._:\-]{0,159}$/.test(value) ? value : null;
}

function groupRankedCandidates(payload = {}) {
  const source = (Array.isArray(payload.candidates) ? payload.candidates : []).slice(0, MAX_CANDIDATES);
  const flattened = [];
  source.forEach((candidate) => {
    if (!candidate || typeof candidate !== "object" || Array.isArray(candidate)) return;
    const alternatives = Array.isArray(candidate.alternatives) ? candidate.alternatives : [];
    flattened.push({ ...candidate, alternatives: [] });
    alternatives.forEach((alternative) => {
      if (alternative && typeof alternative === "object" && !Array.isArray(alternative)) {
        flattened.push({ ...alternative, alternatives: [] });
      }
    });
  });
  if (flattened.length > MAX_CANDIDATES) return [];
  const seenRanks = new Set();
  for (const candidate of flattened) {
    if (
      !Number.isInteger(candidate.rank)
      || candidate.rank < 1
      || candidate.rank > MAX_CANDIDATES
      || seenRanks.has(candidate.rank)
    ) {
      return [];
    }
    seenRanks.add(candidate.rank);
  }
  flattened.sort((left, right) => left.rank - right.rank);
  const groups = [];
  const preferredByUnderlying = new Map();
  flattened.forEach((candidate) => {
    const underlying = candidateUnderlying(candidate);
    const candidateId = candidateIdentity(candidate) || `rank-${candidate.rank}`;
    const key = underlying || `candidate:${candidateId}`;
    const interaction = candidate.rank === 1
      && String(candidate.interaction || "VIEW_ONLY").toUpperCase() === "CHALLENGE_ALLOWED"
      ? "CHALLENGE_ALLOWED"
      : "VIEW_ONLY";
    const row = {
      ...candidate,
      candidate_id: candidateId,
      interaction,
      preferred_for_underlying: !preferredByUnderlying.has(key),
      alternatives: [],
    };
    const preferred = preferredByUnderlying.get(key);
    if (preferred) {
      row.interaction = "VIEW_ONLY";
      row.preferred_for_underlying = false;
      preferred.alternatives.push(row);
    } else {
      preferredByUnderlying.set(key, row);
      groups.push(row);
    }
  });
  return groups;
}

function renderCandidates(payload) {
  const rankedCandidates = groupRankedCandidates(payload);
  const decision = String(payload.decision || "").toUpperCase();
  const actionGateOpen = recommendationGateOpen(payload, rankedCandidates);
  const candidates = actionGateOpen ? rankedCandidates : [];
  const totalRankedCount = candidates.reduce(
    (count, candidate) => count + 1 + candidate.alternatives.length,
    0,
  );
  appState.ranking = payload;
  syncOverviewDetailVisibility();
  setText("candidate-count", `${candidates.length} 主 · ${totalRankedCount} 组合 / ${MAX_CANDIDATES}`);
  const noTrade = byId("no-trade-banner");
  const shouldShowNoTrade = decision === "NO_TRADE" || candidates.length === 0;
  noTrade.hidden = !shouldShowNoTrade;
  const reasons = Array.isArray(payload.reasons)
    ? payload.reasons.map((value) => String(value || "").trim().toUpperCase())
    : [];
  const positionTruth = positionManagementTruth(appState.positionsSnapshot, payload);
  const positionManagementOnly = positionTruth.positionManagementOnly;
  const verifiedPositionManagement = positionManagementOnly
    && positionTruth.verifiedOpenPosition;
  const verifiedFlatConflict = positionManagementOnly
    && positionTruth.verifiedFlat;
  const unverifiedPositionManagement = positionManagementOnly
    && !positionTruth.positionStateKnown;
  setText(
    "no-trade-title",
    verifiedPositionManagement
      ? "当前持仓已核验，可用于持仓管理；第二组新开仓仍暂停。逐腿可执行退出报价不可用，退出成本与建议暂不可执行。"
      : verifiedFlatConflict
        ? "当前持仓已核验为空 · 旧扫描不再代表开放组合"
      : unverifiedPositionManagement
        ? "当前仓位未验证 · 旧扫描不能确认开放组合"
        : featureDataChainTruth(appState.readinessSnapshot || {}).incomplete
          ? "数据链路不完整 · 当前暂无可验证交易建议"
          : "当前没有满足成本后正期望与风控门槛的机会",
  );
  setText(
    "no-trade-reason",
    verifiedPositionManagement
      ? "这不是全市场扫描后判定没有正期望机会；单组合硬上限已触发，当前流程只管理已有组合。"
      : verifiedFlatConflict
        ? "当前只读仓位 authority 为 CURRENT 且数量为 0；历史 POSITION_MANAGEMENT_ONLY 已被当前快照否定，系统不会声称仍有开放组合。"
      : unverifiedPositionManagement
        ? "POSITION_MANAGEMENT_ONLY 来自历史扫描；当前 IBKR 仓位快照未知、陈旧或与其不一致，必须先恢复只读快照，不能据此声称仍有开放组合。"
        : overviewBlockedActionSummary(
          payload,
          appState.healthSnapshot || {},
          positionTruth,
          appState.readinessSnapshot || {},
        ),
  );

  const container = byId("candidate-list");
  container.replaceChildren();
  appState.countdowns = [];
  candidates.forEach((candidate, index) => container.append(buildCandidateGroup(candidate, index)));
  const selected = candidates.find((candidate) => candidate.candidate_id === appState.selectedCandidateId)
    || candidates[0]
    || null;
  appState.selectedCandidateId = selected?.candidate_id ?? null;
  renderSelectedCandidate(selected);
  renderJointResearchWatchlist(payload);
  updateCountdowns();
  renderOverviewResearchFallback();
  renderOverviewPriority();
}

function renderJointResearchWatchlist(payload = {}) {
  const rows = Array.isArray(payload.research_watchlist)
    ? payload.research_watchlist.slice(0, MAX_CANDIDATES)
    : [];
  const panel = byId("joint-research-region");
  const container = byId("joint-research-list");
  if (!panel || !container) return;
  panel.hidden = rows.length === 0;
  setText("joint-research-count", `${rows.length} / ${MAX_CANDIDATES}`);
  setText(
    "joint-research-summary",
    rows.length > 0
      ? "这些行来自 hash-verified joint ranking，但报价、流动性、风险或成本后 EV 至少一项未过；只显示研究身份，绝不进入审批。"
      : `联合研究池不可用 · ${researchText(payload.research_watchlist_integrity_reason, "NO_VERIFIED_RESEARCH_ROWS", 120)}`,
  );
  container.replaceChildren();
  rows.forEach((item) => {
    const reasons = operatorReasonList(item.reason_codes);
    const optionRows = Array.isArray(appState.optionStructurePool?.decisions)
      ? appState.optionStructurePool.decisions
      : [];
    const bound = optionRows.find((option) => (
      option.candidateId === item.candidate_id
      && (!item.candidate_hash || option.candidateHash === item.candidate_hash)
    ));
    const card = createElement("article", "joint-research-card");
    const heading = createElement("div", "research-top10-card-heading");
    heading.append(
      createElement("strong", "", researchText(item.underlying, "--", 16)),
      createElement("span", "status-chip status-no-trade", "RESEARCH_ONLY"),
    );
    card.append(
      heading,
      createElement("p", "research-top10-stage-summary", `candidate ${shortHash(item.candidate_id)} · score ${formatQuote(item.score)} · row ${shortHash(item.row_hash)}`),
      createElement(
        "p",
        "research-top10-stage-summary",
        bound
          ? `${bound.structure} · 最大亏损 ${formatMoney(bound.economics?.max_loss_usd)} · 成本后 EV ${formatMoney(bound.economics?.after_cost_ev_usd, { signed: true })} · ${Array.isArray(bound.economics?.legs) ? bound.economics.legs.length : 0} 腿已按 candidate hash 关联`
          : "期权池中没有同 candidate_id/hash 的逐腿记录；结构、入场和退出信息一律标记不可用。",
      ),
      createElement("p", "option-structure-reasons has-blocker", reasons.join(" · ") || "INCOMPLETE_JOINT_GATE_EVIDENCE"),
      createElement("p", "research-top10-boundary", bound
        ? "入场条件：全部 Gate 重新通过；退出计划：当前研究行未携带完整 exit contract，保持不可用 · VIEW_ONLY"
        : "无 hash-bound 逐腿展示即不声称结构完整 · VIEW_ONLY · 不可 challenge · 不可创建指令"),
    );
    container.append(card);
  });
}

function operatorReasonList(values) {
  const codes = [...new Set(
    (Array.isArray(values) ? values : [])
      .map((value) => String(value || "").trim())
      .filter(Boolean),
  )];
  const noTicks = "IBKR_OPTION_EXECUTABLE_TICKS_UNAVAILABLE";
  if (codes.includes(noTicks)) {
    return [`${OPERATOR_REASON_LABELS[noTicks]}（${noTicks}）`];
  }
  return codes.map((code) => OPERATOR_REASON_LABELS[code] || code);
}

function buildCandidateGroup(candidate, index) {
  const group = createElement("section", "candidate-group");
  group.dataset.underlying = candidateUnderlying(candidate) || "UNKNOWN";
  group.append(buildCandidateCard(candidate, index));
  const alternatives = Array.isArray(candidate.alternatives) ? candidate.alternatives : [];
  if (alternatives.length > 0) {
    const groupDisclosure = document.createElement("details");
    groupDisclosure.className = "candidate-alternatives";
    const summary = document.createElement("summary");
    summary.className = "alternative-heading";
    summary.textContent = `同标的替代组合 ${alternatives.length} · 全部 VIEW_ONLY`;
    summary.setAttribute("aria-expanded", "false");
    groupDisclosure.addEventListener("toggle", () => {
      summary.setAttribute("aria-expanded", groupDisclosure.open ? "true" : "false");
    });
    groupDisclosure.append(summary);
    const nested = createElement("div", "alternative-list");
    alternatives.forEach((alternative) => nested.append(buildAlternativeCandidate(alternative)));
    groupDisclosure.append(nested);
    group.append(groupDisclosure);
  }
  return group;
}

function buildAlternativeCandidate(candidate) {
  const view = candidateView(candidate);
  const row = createElement("article", "candidate-alternative");
  row.dataset.candidateId = candidateIdentity(candidate) || "";
  const identity = createElement("div", "alternative-identity");
  identity.append(
    createElement("strong", "", `#${candidate.rank} · ${candidateUnderlying(candidate) || "--"}`),
    createElement("span", "", String(firstValue(view, ["strategy", "strategy_name", "structure"], "期权组合"))),
  );
  const metrics = createElement(
    "span",
    "alternative-metrics",
    `最大亏损 ${formatMoney(firstValue(view, ["max_loss_usd", "max_loss"]))} · `
      + `成本后 EV ${formatMoney(firstValue(view, ["expected_value_usd", "ev_after_cost_usd", "ev_usd", "expected_value"]), { signed: true })}`,
  );
  row.append(
    identity,
    metrics,
    createElement("span", "view-only-label", "VIEW_ONLY"),
  );
  const reasons = createElement("div", "alternative-reasons");
  renderReasonBuckets(reasons, view);
  const evidence = createElement("section", "alternative-evidence");
  evidence.append(createElement("h4", "alternative-subheading", "只读证据"));
  const evidenceList = createElement("ul", "reason-list");
  renderReasons(evidenceList, evidenceLines(view));
  evidence.append(evidenceList);
  const exitPlan = createElement("section", "alternative-exit-plan");
  exitPlan.append(createElement("h4", "alternative-subheading", "退出计划"));
  const exitPlanList = createElement("ul", "reason-list");
  renderReasons(exitPlanList, exitPlanLines(view));
  exitPlan.append(exitPlanList);
  row.append(reasons, evidence, exitPlan);
  return row;
}

function buildCandidateCard(candidate, index) {
  const fragment = byId("candidate-template").content.cloneNode(true);
  const card = fragment.querySelector(".candidate-card");
  const view = candidateView(candidate);
  const rank = Number.isInteger(candidate.rank) ? candidate.rank : index + 1;
  const candidateId = candidateIdentity(candidate) || `candidate-${rank}`;
  const symbol = String(firstValue(view, ["underlying", "symbol", "ticker"], "--")).toUpperCase();
  const strategy = firstValue(view, ["strategy", "strategy_name", "structure"], "期权组合");
  const expiry = firstValue(view, ["expiration", "expiry"]);
  const dte = firstValue(view, ["dte", "days_to_expiry"]);
  const grade = firstValue(view, ["grade", "quality_grade", "rating"], candidate.authority_status || "候选");

  card.dataset.candidateId = candidateId;
  const selectButton = card.querySelector('[data-field="select-candidate"]');
  if (rank === 1) {
    selectButton.addEventListener("click", () => {
      appState.selectedCandidateId = candidateId;
      renderSelectedCandidate(candidate);
    });
  } else {
    selectButton.remove();
  }
  setField(card, "rank", `#${rank}`);
  setField(card, "candidate-title", `${symbol} · ${strategy}`);
  setField(card, "candidate-subtitle", [expiry ? `到期 ${expiry}` : null, dte !== null && dte !== undefined ? `${dte} DTE` : null].filter(Boolean).join(" · ") || "等待合约摘要");
  setField(card, "grade", String(grade));
  setField(card, "max-loss", formatMoney(firstValue(view, ["max_loss_usd", "max_loss"])), "negative");
  setField(card, "max-profit", formatMoney(firstValue(view, ["max_profit_usd", "max_profit"])), "positive");
  const ev = numberOrNull(firstValue(view, ["expected_value_usd", "ev_after_cost_usd", "ev_usd", "expected_value"]));
  const evNode = setField(card, "expected-value", formatMoney(ev, { signed: true }));
  evNode.classList.add(ev !== null && ev >= 0 ? "positive" : "negative");
  setField(card, "probability", formatPercent(firstValue(view, ["probability_of_profit", "pop", "win_probability"])));

  renderFreshness(card, view);
  const quotesComplete = renderLegs(
    card.querySelector('[data-field="legs"]'),
    Array.isArray(view.legs) ? view.legs : [],
  );
  if (!quotesComplete || numberOrNull(firstValue(view, ["max_loss_usd", "max_loss"])) === null) {
    card.dataset.stale = "true";
    const statusNode = card.querySelector('[data-field="freshness-status"]');
    statusNode.textContent = "INCOMPLETE · 禁止批准";
    statusNode.classList.add("stale");
  }
  renderReasonBuckets(card.querySelector('[data-field="reasons"]'), view);
  setField(card, "risk-note", firstValue(view, ["risk_note", "invalidation", "risk_summary"], "报价陈旧、任一腿缺失或最大亏损变化时，challenge 自动失效。"));
  renderReasons(card.querySelector('[data-field="evidence-summary"]'), evidenceLines(view));
  renderReasons(card.querySelector('[data-field="exit-plan"]'), exitPlanLines(view));
  setField(card, "candidate-identities", `${candidateId} / ${shortHash(candidate.proposal_hash) || "--"}`);
  setField(card, "secdef-status", firstValue(view, ["secdef_status", "contract_definition_status"], "--"));
  const quoteAge = firstValue(view, ["quote_age_ms", "quote_age_seconds"]);
  const quoteSkew = firstValue(view, ["quote_skew_ms", "leg_quote_skew_ms"]);
  setField(card, "quote-health", `${quoteAge ?? "--"} / ${quoteSkew ?? "--"}`);
  const strategyNav = firstValue(view, ["strategy_nav_usd", "strategy_nav"], appState.strategyNavUsd);
  const riskFraction = firstValue(view, ["risk_fraction", "max_loss_fraction"]);
  setField(card, "risk-fraction", `${formatPercent(riskFraction)} / ${formatMoney(strategyNav)}`);
  setField(
    card,
    "authority-identity",
    `${appState.ranking?.current_policy_version || "--"} / ${appState.ranking?.cost_version || "--"}`,
  );

  const viewOnly = card.querySelector('[data-field="view-only"]');
  const interaction = rank === 1
    ? String(candidate.interaction || "VIEW_ONLY").toUpperCase()
    : "VIEW_ONLY";
  viewOnly.textContent = interaction;
  if (
    rank === 1
    && interaction === "CHALLENGE_ALLOWED"
    && appState.strategyNavUsd !== null
    && candidateChallengeGate(candidate, appState.controlContext)
  ) {
    viewOnly.remove();
    appendRankOneChallengeAction(card, {
      rankingSnapshotId: String(appState.ranking?.ranking_snapshot_id || ""),
      candidateId,
      expiresAt: Date.parse(String(appState.ranking?.valid_until || "")),
      sourceHealth: candidate.source_health,
      accountCapacity: candidate.account_capacity,
    });
  }
  return fragment;
}

function candidateView(candidate) {
  const source = candidate && typeof candidate === "object" && !Array.isArray(candidate) ? candidate : {};
  const proposal = source.proposal_body && typeof source.proposal_body === "object" && !Array.isArray(source.proposal_body)
    ? source.proposal_body
    : {};
  const body = source.candidate_body && typeof source.candidate_body === "object" && !Array.isArray(source.candidate_body)
    ? source.candidate_body
    : {};
  return { ...source, ...proposal, ...body };
}

function appendRankOneChallengeAction(card, authority) {
  if (!authority.rankingSnapshotId || !authority.candidateId) {
    card.dataset.stale = "true";
  }
  const slot = card.querySelector('[data-field="candidate-authority-slot"]');
  const label = document.createElement("label");
  label.className = "risk-check";
  const checkbox = document.createElement("input");
  checkbox.type = "checkbox";
  checkbox.setAttribute("aria-label", "确认已核对最大亏损、报价与组合腿");
  label.append(checkbox, createElement("span", "", "我已核对最大亏损、Strategy NAV、逐腿报价与退出计划"));
  const actions = createElement("div", "approval-actions");
  const countdown = createElement("span", "countdown", "有效期 --:--");
  const button = createElement("button", "button button-primary", "发起双确认 challenge");
  button.type = "button";
  button.disabled = true;
  button.title = "仅为当前不可变 Rank 1 发起双确认";
  actions.append(countdown, button);
  slot.append(label, actions);
  const approval = { ...authority, checkbox, button, countdown };
  applyApprovalWorkflowLock(approval);
  checkbox.addEventListener("change", () => updateApprovalState(approval));
  button.addEventListener("click", (event) => {
    event.stopPropagation();
    requestRankOneChallenge(approval);
  });
  appState.countdowns.push(approval);
}

function evidenceLines(candidate) {
  const lines = [];
  const append = (label, values) => {
    if (!Array.isArray(values)) return;
    values.forEach((item) => {
      if (typeof item === "string") lines.push(`${label}: ${item}`);
      else if (item && typeof item === "object") {
        const source = firstValue(item, ["source", "provider", "type"], label);
        const summary = firstValue(item, ["summary", "title", "headline", "record_hash"], "已绑定证据");
        lines.push(`${label} · ${source}: ${summary}`);
      }
    });
  };
  append("PRIMARY", candidate.primary_evidence);
  append("SUPPORTING_ONLY", candidate.supporting_evidence);
  append("EVIDENCE", candidate.evidence);
  return lines.length > 0 ? lines : ["证据明细未出现在当前只读投影；禁止仅凭新闻发起交易。"];
}

function exitPlanLines(candidate) {
  const plan = firstValue(candidate, ["exit_plan", "management_plan", "exit_rules"], null);
  if (Array.isArray(plan)) return plan.map((item) => typeof item === "string" ? item : JSON.stringify(item));
  if (plan && typeof plan === "object") {
    return Object.entries(plan).map(([key, value]) => `${key}: ${typeof value === "string" ? value : JSON.stringify(value)}`);
  }
  if (typeof plan === "string" && plan.trim()) return [plan];
  return ["缺少完整止盈、止损、时间退出和事件失效规则；该候选必须保持 VIEW_ONLY。"];
}

function renderSelectedCandidate(candidate) {
  const evidenceContainer = byId("selected-evidence");
  const exitContainer = byId("selected-exit-plan");
  evidenceContainer.replaceChildren();
  exitContainer.replaceChildren();
  if (!candidate) {
    evidenceContainer.append(createElement("p", "empty-state", "当前没有可展示的 Top 10 证据。"));
    exitContainer.append(createElement("p", "empty-state", "当前没有可展示的退出计划。"));
    return;
  }
  const view = candidateView(candidate);
  const title = `${candidate.candidate_id || "--"} · ${firstValue(view, ["symbol", "underlying"], "--")}`;
  evidenceContainer.append(createElement("h3", "", title));
  const evidenceList = createElement("ul", "reason-list");
  renderReasons(evidenceList, evidenceLines(view));
  evidenceContainer.append(evidenceList);
  exitContainer.append(createElement("h3", "", "完整退出计划"));
  const exitList = createElement("ul", "reason-list");
  renderReasons(exitList, exitPlanLines(view));
  exitContainer.append(exitList);
  refreshCandidateEvidence(candidate).catch(() => {
    if (appState.selectedCandidateId !== candidate.candidate_id) return;
    evidenceContainer.replaceChildren(
      createElement("p", "empty-state", "UNCERTAIN · CANDIDATE_EVIDENCE_REFRESH_UNAVAILABLE"),
    );
  });
}

async function refreshCandidateEvidence(candidate) {
  const scanRunId = String(appState.ranking?.scan_run_id || "");
  const candidateId = String(candidate?.candidate_id || "");
  if (!scanRunId || !candidateId) return;
  const evidence = await fetchJson(
    `/api/scans/${encodeURIComponent(scanRunId)}/candidates/${encodeURIComponent(candidateId)}/evidence`,
  );
  if (appState.selectedCandidateId !== candidateId) return;
  const container = byId("selected-evidence");
  const lines = [];
  for (const [label, values] of [
    ["PRIMARY", evidence.primary],
    ["SUPPORTING_ONLY", evidence.supporting],
    ["CONTRADICTING", evidence.contradicting],
  ]) {
    if (!Array.isArray(values)) continue;
    values.forEach((item) => {
      const source = typeof item === "string" ? item : firstValue(item, ["source", "provider", "title", "record_hash"], "bound evidence");
      lines.push(`${label} · ${source}`);
    });
  }
  if (lines.length === 0) return;
  container.replaceChildren(createElement("h3", "", `${candidateId} · immutable evidence`));
  const list = createElement("ul", "reason-list");
  renderReasons(list, lines);
  container.append(list);
}

function renderFreshness(card, candidate) {
  const freshness = candidate.data_freshness || candidate.freshness || {};
  const stale = freshness.stale === true || candidate.stale === true || String(freshness.status || "").toUpperCase() === "STALE";
  const statusNode = card.querySelector('[data-field="freshness-status"]');
  statusNode.textContent = stale ? "STALE · 禁止批准" : "LIVE · 报价有效";
  statusNode.classList.toggle("stale", stale);
  const asof = firstValue(freshness, ["asof", "quote_time", "timestamp"], firstValue(candidate, ["quote_time", "asof"]));
  setField(card, "freshness-time", formatTime(asof));
  if (stale) card.dataset.stale = "true";
}

function renderLegs(tbody, legs) {
  tbody.replaceChildren();
  if (legs.length === 0) {
    const row = document.createElement("tr");
    const cell = document.createElement("td");
    cell.colSpan = 9;
    cell.className = "missing-quote";
    cell.textContent = "缺少逐腿报价，禁止批准";
    row.append(cell);
    tbody.append(row);
    return false;
  }
  let complete = true;
  legs.forEach((leg, index) => {
    const row = document.createElement("tr");
    const side = String(firstValue(leg, ["action", "side"], "--")).toUpperCase();
    const quantity = firstValue(leg, ["quantity", "qty", "ratio"], 1);
    const contract = firstValue(leg, ["contract", "description", "local_symbol"], buildContractLabel(leg));
    const requiredQuoteFields = [
      leg.bid,
      leg.ask,
      leg.last,
      firstValue(leg, ["iv", "implied_volatility"]),
      firstValue(leg, ["oi", "open_interest"]),
      firstValue(leg, ["volume", "daily_volume"]),
      firstValue(leg, ["quote_time", "asof", "timestamp"]),
    ];
    if (requiredQuoteFields.some((value) => value === null || value === undefined || value === "")) {
      complete = false;
    }
    const values = [
      { text: `${side} ${quantity}`, className: side.includes("BUY") || side.includes("买") ? "leg-action-buy" : "leg-action-sell" },
      { text: contract },
      { text: formatQuote(leg.bid), quote: true },
      { text: formatQuote(leg.ask), quote: true },
      { text: formatQuote(leg.last), quote: true },
      { text: formatPercent(firstValue(leg, ["iv", "implied_volatility"])) },
      { text: formatInteger(firstValue(leg, ["oi", "open_interest"])) },
      { text: formatInteger(firstValue(leg, ["volume", "daily_volume"])) },
      { text: formatTime(firstValue(leg, ["quote_time", "asof", "timestamp"])) },
    ];
    values.forEach((value) => {
      const cell = document.createElement("td");
      cell.textContent = String(value.text ?? "--");
      if (value.className) cell.classList.add(value.className);
      if (value.quote && value.text === "--") cell.classList.add("missing-quote");
      row.append(cell);
    });
    row.dataset.leg = String(index + 1);
    tbody.append(row);
  });
  return complete;
}

function buildContractLabel(leg) {
  const strike = firstValue(leg, ["strike"]);
  const right = String(firstValue(leg, ["right", "option_type"], "")).toUpperCase();
  const expiry = firstValue(leg, ["expiration", "expiry"]);
  return [expiry, strike, right].filter((value) => value !== null && value !== undefined && value !== "").join(" ") || "--";
}

function sanitizeReasonText(value, maximum = 160) {
  if (typeof value !== "string") return null;
  const text = value.trim().replace(/\s+/g, " ");
  if (
    !text
    || text.length > maximum
    || /[<>\u0000-\u001f\u007f]/.test(text)
    || /(authorization|bearer|api[_-]?key|password|credential|secret|access[_-]?token)/i.test(text)
  ) {
    return null;
  }
  return text;
}

function normalizeCandidateReasonBuckets(candidate = {}) {
  const buckets = {
    SUPPORTED: [],
    INVALIDATED: [],
    STALE: [],
    UNCERTAIN: [],
    BLOCKED: [],
  };
  const append = (category, values) => {
    const rows = Array.isArray(values) ? values : values === null || values === undefined ? [] : [values];
    rows.forEach((value) => {
      const text = sanitizeReasonText(value);
      if (text && !buckets[category].includes(text) && buckets[category].length < 5) {
        buckets[category].push(text);
      }
    });
  };
  append("SUPPORTED", candidate.supported_reasons);
  append("SUPPORTED", firstValue(candidate, ["reasons", "rationale", "thesis"], []));
  append("INVALIDATED", firstValue(candidate, ["invalidation_reasons", "invalidated_reasons", "invalidation"], []));
  append("STALE", candidate.stale_reasons);
  append("UNCERTAIN", firstValue(candidate, ["uncertainty_reasons", "uncertain_reasons"], []));
  append("BLOCKED", firstValue(candidate, ["blocked_reasons", "approval_blocked_reason"], []));

  const freshness = candidate.data_freshness || candidate.freshness || {};
  if (
    candidate.stale === true
    || freshness.stale === true
    || String(freshness.status || "").toUpperCase() === "STALE"
  ) {
    append("STALE", "QUOTE_OR_SNAPSHOT_STALE");
  }
  const sourceHealth = candidate.source_health;
  if (sourceHealth && typeof sourceHealth === "object" && !Array.isArray(sourceHealth)) {
    const status = sanitizeReasonText(String(sourceHealth.status || "UNKNOWN"), 32) || "UNKNOWN";
    const reason = sanitizeReasonText(String(sourceHealth.reason || "SOURCE_HEALTH_UNAVAILABLE"), 96)
      || "SOURCE_HEALTH_UNAVAILABLE";
    if (["READY", "HEALTHY", "AVAILABLE"].includes(status.toUpperCase())) {
      append("SUPPORTED", `SOURCE_HEALTH · ${status.toUpperCase()} · ${reason}`);
    } else {
      append("UNCERTAIN", `SOURCE_HEALTH · ${status.toUpperCase()} · ${reason}`);
    }
  }
  const accountCapacity = candidate.account_capacity;
  if (accountCapacity && typeof accountCapacity === "object" && !Array.isArray(accountCapacity)) {
    const status = sanitizeReasonText(String(accountCapacity.status || "BLOCKED"), 32) || "BLOCKED";
    const reason = sanitizeReasonText(String(accountCapacity.reason || "ACCOUNT_CAPACITY_UNAVAILABLE"), 96)
      || "ACCOUNT_CAPACITY_UNAVAILABLE";
    if (["READY", "AVAILABLE"].includes(status.toUpperCase())) {
      append("SUPPORTED", `ACCOUNT_CAPACITY · ${status.toUpperCase()} · ${reason}`);
    } else {
      append("BLOCKED", `ACCOUNT_CAPACITY · ${status.toUpperCase()} · ${reason}`);
    }
  }
  return buckets;
}

function renderReasonBuckets(container, candidate) {
  container.replaceChildren();
  const buckets = normalizeCandidateReasonBuckets(candidate);
  if (Object.values(buckets).every((values) => values.length === 0)) {
    buckets.UNCERTAIN.push("CANDIDATE_REASON_EVIDENCE_UNAVAILABLE");
  }
  Object.entries(buckets).forEach(([category, reasons]) => {
    const section = createElement("li", "reason-category");
    section.append(createElement("strong", "reason-category-label", category));
    const values = createElement("ul", "reason-category-values");
    if (reasons.length === 0) {
      values.append(createElement("li", "reason-empty", "--"));
    } else {
      reasons.forEach((reason) => values.append(createElement("li", "", reason)));
    }
    section.append(values);
    container.append(section);
  });
}

function renderReasons(list, value) {
  list.replaceChildren();
  const reasons = Array.isArray(value) ? value : value ? [value] : [];
  (reasons.length ? reasons : ["等待模型提供可审计理由。"])
    .slice(0, 5)
    .forEach((reason) => list.append(createElement("li", "", String(reason))));
}

function updateCountdowns() {
  synchronizeActionControls();
}

function updateApprovalState(approval, now = Date.now()) {
  const remaining = approval.expiresAt - now;
  const card = approval.button.closest(".candidate-card");
  const stale = card?.dataset.stale === "true";
  const valid = Number.isFinite(remaining)
    && remaining > 0
    && !stale
    && !appState.postInFlight
    && !appState.postOutcomeUnknownReason
    && !approvalWorkflowLocked(approval)
    && !approval.checkbox.disabled
    && candidateChallengeGate(
      approval,
      approval.controlContext || appState.controlContext,
      now,
    );
  if (!Number.isFinite(approval.expiresAt)) {
    approval.countdown.textContent = "等待有效期";
    approval.countdown.classList.add("expired");
  } else if (remaining <= 0) {
    approval.countdown.textContent = "报价已过期";
    approval.countdown.classList.add("expired");
  } else {
    const seconds = Math.ceil(remaining / 1000);
    const minutes = Math.floor(seconds / 60);
    approval.countdown.textContent = `有效期 ${String(minutes).padStart(2, "0")}:${String(seconds % 60).padStart(2, "0")}`;
    approval.countdown.classList.remove("expired");
  }
  approval.button.disabled = !valid || !approval.checkbox.checked;
}

async function requestRankOneChallenge(approval) {
  if (
    !approval.checkbox.checked
    || approval.button.disabled
    || appState.postInFlight
    || appState.postOutcomeUnknownReason
    || approvalWorkflowLocked(approval)
    || !candidateChallengeGate(
      approval,
      approval.controlContext || appState.controlContext,
    )
  ) return;
  lockApprovalWorkflow(approval);
  approval.button.disabled = true;
  approval.button.textContent = "正在冻结 Rank 1…";
  setText("approval-status", "正在重新验证 ranking、broker、policy、cost 与 risk authority；尚未创建审批。 ");
  configureReviewLink(null);
  try {
    const challengeDispatch = await dispatchReviewOnlyPost(
      `/api/rankings/${encodeURIComponent(approval.rankingSnapshotId)}/candidates/${encodeURIComponent(approval.candidateId)}/challenge`,
      {
        method: "POST",
        body: JSON.stringify({}),
      },
      approval,
      "CHALLENGE",
      (payload) => validateChallenge(payload, approval),
    );
    if (challengeDispatch.outcomeUnknown) return;
    const challenge = challengeDispatch.payload;
    const secondConfirmation = window.confirm(
      "二次确认：仅创建供 IBKR 审核的 instruction，不会直接下单。你仍需在 IBKR 中核对并最终提交。",
    );
    if (!secondConfirmation) {
      approval.button.textContent = "challenge 已创建，未二次确认";
      setText("approval-status", "你取消了二次确认；持久化 challenge 将自动过期，不会创建审批或 IBKR 指令。 ");
      return;
    }
    enforceControlSnapshotFreshness(Date.now());
    approval.button.textContent = "正在刷新控制快照…";
    setText("approval-status", "二次确认已收到；正在重新获取完整控制快照，尚未创建审批或 IBKR 指令。 ");
    const refreshed = await refreshControlSnapshot({ requireNew: true });
    const refreshedRanking = appState.ranking || {};
    const freshCandidate = groupRankedCandidates(refreshedRanking)
      .flatMap((candidate) => [candidate, ...(candidate.alternatives || [])])
      .find((candidate) => candidateIdentity(candidate) === approval.candidateId);
    const refreshedAuthorityValid = refreshed === true
      && String(refreshedRanking.ranking_snapshot_id || "") === approval.rankingSnapshotId
      && freshCandidate?.rank === 1
      && String(freshCandidate?.interaction || "VIEW_ONLY").toUpperCase() === "CHALLENGE_ALLOWED"
      && candidateChallengeGate(freshCandidate, appState.controlContext, Date.now());
    if (!refreshedAuthorityValid) {
      approval.button.disabled = true;
      approval.button.textContent = "控制快照已变化 · 禁止确认";
      setText(
        "approval-status",
        `二次确认已停止：${appState.controlContext.controlFailureReason || "RANK_ONE_AUTHORITY_CHANGED"}；不会创建审批或 IBKR 指令。`,
      );
      return;
    }
    const confirmationDispatch = await dispatchReviewOnlyPost(
      `/api/approval-challenges/${encodeURIComponent(challenge.challenge_id)}/confirm`,
      {
        method: "POST",
        body: JSON.stringify({
          challenge_response: challenge.challenge_response,
          risk_acknowledged: true,
          second_confirmation: true,
          confirmation_token: APPROVAL_CONFIRMATION_TOKEN,
        }),
      },
      approval,
      "CONFIRM",
      validatePendingHandoff,
    );
    if (confirmationDispatch.outcomeUnknown) return;
    const result = confirmationDispatch.payload;
    setText("approval-status", "双确认已持久化；等待安全 bridge 重新验证，尚未创建 IBKR 指令。 ");
    approval.button.textContent = "等待 Codex 重新报价";
    approval.checkbox.disabled = true;
    startApprovalPolling(result, approval);
  } catch (error) {
    if (appState.postOutcomeUnknownReason) {
      disableUnknownOutcomeRetry(approval);
      setText(
        "approval-status",
        `${appState.postOutcomeUnknownReason} · 结果仍为 SAVED_INSTRUCTION_UNKNOWN；禁止重试，需人工核对持久化状态。`,
      );
      return;
    }
    const retryableInitialRejection = error?.reviewOnlyStage === "CHALLENGE"
      && knownPreMutationPostRejection(error);
    if (retryableInitialRejection) {
      unlockApprovalWorkflow(approval);
      setText("approval-status", `challenge 被明确拒绝且未持久化：${error.message}`);
      approval.button.textContent = "重新尝试";
      updateApprovalState(approval);
      return;
    }
    lockApprovalWorkflow(approval);
    approval.checkbox.checked = false;
    approval.checkbox.disabled = true;
    approval.button.disabled = true;
    approval.button.textContent = "审批流程已冻结 · 禁止重建";
    setText(
      "approval-status",
      `审批失败：${error.message}；已有 challenge 或结果不适合重建，保持失败关闭。`,
    );
  }
}

function knownPreMutationPostRejection(error) {
  const status = numberOrNull(error?.status);
  return Number.isInteger(status) && status >= 400 && status < 500;
}

async function dispatchReviewOnlyPost(path, options, approval, stage, validate) {
  if (appState.postInFlight || appState.postOutcomeUnknownReason) {
    disableUnknownOutcomeRetry(approval);
    return { outcomeUnknown: true, payload: null };
  }
  appState.postInFlight = true;
  approval.button.disabled = true;

  let watchdogId = null;
  const dispatched = fetchJson(path, options).then(
    (payload) => ({ kind: "RESPONSE", payload }),
    (error) => ({ kind: "ERROR", error }),
  );
  const watchdog = new Promise((resolve) => {
    watchdogId = globalThis.setTimeout(
      () => resolve({ kind: "WATCHDOG" }),
      POST_OUTCOME_UNKNOWN_AFTER_MS,
    );
  });
  const outcome = await Promise.race([dispatched, watchdog]);
  if (watchdogId !== null) globalThis.clearTimeout(watchdogId);

  if (outcome.kind === "WATCHDOG") {
    appState.postInFlight = false;
    await enterPostOutcomeUnknown(approval, stage);
    return { outcomeUnknown: true, payload: null };
  }
  if (outcome.kind === "ERROR") {
    appState.postInFlight = false;
    const error = outcome.error;
    if (knownPreMutationPostRejection(error)) {
      error.reviewOnlyStage = stage;
      throw error;
    }
    await enterPostOutcomeUnknown(approval, stage);
    return { outcomeUnknown: true, payload: null };
  }
  const payload = outcome.payload;
  try {
    validate(payload);
  } catch (_error) {
    appState.postInFlight = false;
    await enterPostOutcomeUnknown(approval, stage);
    return { outcomeUnknown: true, payload: null };
  }
  appState.postInFlight = false;
  return { outcomeUnknown: false, payload };
}

function disableUnknownOutcomeRetry(approval) {
  const approvals = [approval, ...appState.countdowns].filter(Boolean);
  approvals.forEach((item) => {
    lockApprovalWorkflow(item);
    if (item.checkbox) {
      item.checkbox.checked = false;
      item.checkbox.disabled = true;
    }
    if (item.button) {
      item.button.disabled = true;
      item.button.textContent = "结果未知 · 禁止重试";
    }
  });
}

async function enterPostOutcomeUnknown(approval, stage) {
  const reason = `${stage}_POST_OUTCOME_UNKNOWN`;
  appState.postInFlight = false;
  appState.postOutcomeUnknownReason = reason;
  appState.controlContext = {
    ...appState.controlContext,
    brokerState: "SAVED_INSTRUCTION_UNKNOWN",
  };
  disableUnknownOutcomeRetry(approval);
  configureReviewLink(null);
  failClosedControlSnapshot(Date.now(), reason);
  setText(
    "approval-status",
    `${reason} · POST 已发出但结果无法确认；禁止重试，正在执行只读控制快照对账。`,
  );
  try {
    await refreshControlSnapshot({ requireNew: true });
  } finally {
    appState.controlContext = {
      ...appState.controlContext,
      brokerState: "SAVED_INSTRUCTION_UNKNOWN",
    };
    disableUnknownOutcomeRetry(approval);
    failClosedControlSnapshot(Date.now(), reason);
    setText(
      "approval-status",
      `${reason} · 结果仍为 SAVED_INSTRUCTION_UNKNOWN；禁止重试，需人工核对持久化状态。`,
    );
  }
}

function validateChallenge(result, approval) {
  validateReviewOnly(result);
  if (
    result.status !== "PENDING_SECOND_CONFIRMATION"
    || result.ranking_snapshot_id !== approval.rankingSnapshotId
    || result.candidate_id !== approval.candidateId
    || typeof result.challenge_id !== "string"
    || !result.challenge_id
    || typeof result.challenge_response !== "string"
    || result.challenge_response.length < 24
    || result.approval_id !== null
    || result.instruction_id !== null
    || result.ibkr_deep_link !== null
  ) {
    throw new Error("服务返回了无效的持久化双确认 challenge");
  }
}

function validateReviewOnly(result) {
  if (
    !result
    || result.review_only !== true
    || result.order_submitted !== false
    || result.transmitted_to_broker !== false
  ) {
    throw new Error("服务未能证明审批链路是 review-only 且未提交订单");
  }
}

function validatePendingHandoff(result) {
  validateReviewOnly(result);
  if (
    result.status !== "PENDING_CODEX_BRIDGE"
    || result.instruction_id !== null
    || result.ibkr_deep_link !== null
    || typeof result.approval_id !== "string"
    || !result.approval_id
    || typeof result.status_url !== "string"
    || result.status_url !== `/api/approvals/${encodeURIComponent(result.approval_id)}`
    || !Number.isFinite(Date.parse(result.expires_at))
  ) {
    throw new Error("服务返回了无效的异步审批凭据");
  }
}

function startApprovalPolling(handoff, approval) {
  if (appState.approvalPolls.has(handoff.approval_id)) return;
  const poll = pollApprovalStatus(handoff, approval)
    .catch((error) => {
      setText("approval-status", `审批状态验证失败：${error.message}`);
      approval.button.textContent = "审批状态异常";
      configureReviewLink(null);
    })
    .finally(() => appState.approvalPolls.delete(handoff.approval_id));
  appState.approvalPolls.set(handoff.approval_id, poll);
}

async function pollApprovalStatus(handoff, approval) {
  const expiresAt = Date.parse(handoff.expires_at);
  let unknownOutcomeSeen = false;
  while (Date.now() < expiresAt) {
    await wait(APPROVAL_POLL_INTERVAL_MS);
    let result;
    try {
      result = await fetchJson(handoff.status_url);
    } catch (error) {
      setText("approval-status", `状态查询暂时失败，将继续轮询：${error.message}`);
      continue;
    }
    validateApprovalStatus(handoff.approval_id, result);

    if (result.status === "READY_FOR_IBKR_REVIEW") {
      setText("approval-status", `审核指令已创建 · ${result.instruction_id} · 尚未下单`);
      configureReviewLink(result.ibkr_deep_link);
      approval.button.textContent = "已创建审核指令";
      return;
    }
    if (result.status === "FAILED" || result.status === "EXPIRED") {
      renderApprovalFailure(result.status, result.failure_reason, approval);
      return;
    }
    if (result.status === "CLAIMED") {
      setText("approval-status", "Codex 已接管审批，正在重新报价…");
    } else if (result.status === "AUTHORIZED") {
      setText("approval-status", "风险复核通过，正在创建 IBKR 审核指令…");
    } else if (result.status === "UNKNOWN_OUTCOME") {
      unknownOutcomeSeen = true;
      setText(
        "approval-status",
        "外部结果尚未确认，系统已禁止自动重试；继续等待当前调用的最终状态…",
      );
    } else {
      setText("approval-status", "已批准，等待Codex重新报价");
    }
  }
  if (unknownOutcomeSeen) {
    setText(
      "approval-status",
      "外部结果仍未确认，禁止自动重试；请先在 IBKR 指令列表人工核对。",
    );
    approval.button.textContent = "结果待人工核对";
    configureReviewLink(null);
    return;
  }
  renderApprovalFailure("EXPIRED", "本地五分钟审批窗口已结束", approval);
}

function validateApprovalStatus(approvalId, result) {
  validateReviewOnly(result);
  if (result.approval_id !== approvalId) {
    throw new Error("审批状态标识不匹配");
  }
  const status = String(result.status || "").toUpperCase();
  if (PENDING_APPROVAL_STATES.has(status)) {
    if (result.instruction_id !== null || result.ibkr_deep_link !== null) {
      throw new Error("未就绪状态暴露了 IBKR 指令");
    }
    return;
  }
  if (status === "READY_FOR_IBKR_REVIEW") {
    throw new Error("Creator 审核目标合同不可用；拒绝 READY 状态和任何推测链接");
  }
  if (status === "FAILED" || status === "EXPIRED") {
    if (result.instruction_id !== null || result.ibkr_deep_link !== null) {
      throw new Error("失败状态不得暴露 IBKR 指令");
    }
    return;
  }
  throw new Error(`未知审批状态：${status || "EMPTY"}`);
}

function renderApprovalFailure(status, reason, approval) {
  const label = status === "EXPIRED" ? "审批已过期" : "审批失败";
  setText("approval-status", `${label}：${reason || "未创建 IBKR 审核指令"}`);
  approval.button.textContent = label;
  configureReviewLink(null);
}

function wait(milliseconds) {
  return new Promise((resolve) => window.setTimeout(resolve, milliseconds));
}

function isAbsoluteHttpsUrl(_value) {
  // No authoritative review destination contract exists in this release.
  return false;
}

function configureReviewLink(value, now = Date.now()) {
  const link = byId("ibkr-review-link");
  if (!link) return;
  if (value !== undefined) {
    appState.reviewLinkHref = null;
  }
  link.href = "#";
  link.classList.add("button-disabled");
  link.classList.remove("button-secondary");
  link.setAttribute("aria-disabled", "true");
  link.setAttribute("tabindex", "-1");
  if (!appState.reviewLinkHref || !actionControlGate(appState.controlContext, now)) {
    link.textContent = "IBKR 审核入口（审核目标合同不可用）";
    return;
  }
  link.href = appState.reviewLinkHref;
  link.classList.remove("button-disabled");
  link.classList.add("button-secondary");
  link.removeAttribute("aria-disabled");
  link.removeAttribute("tabindex");
  link.textContent = "打开 IBKR 审核页面";
}

function normalizeLearningModel(value) {
  if (typeof value === "string") {
    const name = value.trim();
    return name ? { name } : {};
  }
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function validLearningCount(value) {
  const count = numberOrNull(value);
  return count !== null && Number.isInteger(count) && count >= 0 ? count : null;
}

function learningObject(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function learningText(value, maximum = 160) {
  if (typeof value !== "string") return null;
  const normalized = value.trim();
  return normalized && normalized.length <= maximum ? normalized : null;
}

function learningStatus(value) {
  const normalized = learningText(value, 64);
  if (!normalized) return null;
  const upper = normalized.toUpperCase();
  return /^[A-Z][A-Z0-9_-]{0,63}$/.test(upper) ? upper : null;
}

function learningDigest(value) {
  const normalized = learningText(value, 64);
  return normalized && /^[0-9a-f]{64}$/.test(normalized) ? normalized : null;
}

function shortLearningDigest(value) {
  const digest = learningDigest(value);
  return digest ? `${digest.slice(0, 8)}…${digest.slice(-6)}` : "UNAVAILABLE";
}

function learningFraction(value, expected) {
  const fraction = numberOrNull(value);
  return fraction !== null && Math.abs(fraction - expected) < 1e-9 ? fraction : null;
}

function learningEvaluationStage(independentSamples) {
  const count = validLearningCount(independentSamples);
  return count === null || count === 0
    ? "COLLECTING"
    : count < LEARNING_DISCOVERY_SAMPLE_TARGET
      ? "COMPARISON_AVAILABLE"
      : "DISCOVERY";
}

function renderP9Governance(source) {
  const governance = learningObject(source.governance);
  const schemaValid = governance.schema === LEARNING_GOVERNANCE_SCHEMA;
  const governanceStatus = schemaValid ? learningStatus(governance.status) : null;
  const governanceReason = schemaValid ? learningStatus(governance.reason) : null;
  const authority = schemaValid ? learningObject(governance.authority) : {};
  const humanSignerStatus = learningStatus(authority.human_signer_status);
  const authorityIsReadOnly = authority.read_only === true
    && authority.can_sign === false
    && authority.can_auto_promote === false
    && authority.approval_authority === false
    && authority.bridge_authority === false
    && authority.order_authority === false;
  const productionViewReady = Boolean(
    schemaValid
    && governanceStatus === "READY"
    && humanSignerStatus === "TRUSTED"
    && authorityIsReadOnly,
  );
  const resolverViewReady = Boolean(
    schemaValid
    && authorityIsReadOnly
    && (
      (
        governanceStatus === "READY"
        && ["TRUSTED", "NO_TRUSTED_HUMAN_SIGNER"].includes(humanSignerStatus)
      )
      || (
        governanceStatus === "BLOCKED"
        && governanceReason === "NO_TRUSTED_HUMAN_SIGNER"
        && humanSignerStatus === "NO_TRUSTED_HUMAN_SIGNER"
      )
    ),
  );
  const currentPolicy = schemaValid ? learningObject(governance.current_policy) : {};
  const policyStatus = learningStatus(currentPolicy.status);
  const policyVersion = learningText(currentPolicy.version, 160);
  const policyHash = learningDigest(currentPolicy.hash);
  const policyMarkerHash = learningDigest(currentPolicy.authority_marker_hash);
  const authorityHeadHash = learningDigest(currentPolicy.authority_head_hash);
  const initialPolicyHash = learningDigest(currentPolicy.immutable_initial_policy_hash);
  const policyComplete = Boolean(
    resolverViewReady
    && policyVersion
    && policyHash
    && policyMarkerHash
    && authorityHeadHash
    && initialPolicyHash
    && LEARNING_POLICY_AVAILABLE_STATUSES.has(policyStatus),
  );
  setText("learning-policy-status", policyComplete ? policyStatus : "UNAVAILABLE · LOCKED");
  setText("learning-policy-version", policyComplete ? policyVersion : "UNAVAILABLE");
  setText("learning-policy-hash", policyComplete ? shortLearningDigest(policyHash) : "UNAVAILABLE");
  setText("learning-policy-marker", policyComplete ? shortLearningDigest(policyMarkerHash) : "UNAVAILABLE");
  setText("learning-authority-head", policyComplete ? shortLearningDigest(authorityHeadHash) : "UNAVAILABLE");
  setText("learning-initial-policy-hash", policyComplete ? shortLearningDigest(initialPolicyHash) : "UNAVAILABLE");

  const evaluation = schemaValid ? learningObject(governance.evaluation) : {};
  const evaluationStatus = learningStatus(evaluation.status);
  const evaluationCount = validLearningCount(evaluation.independent_count);
  const evaluationStage = learningEvaluationStage(evaluationCount);
  const reportHash = learningDigest(evaluation.report_hash);
  const datasetHash = learningDigest(evaluation.dataset_hash);
  const independenceHash = learningDigest(evaluation.independence_spec_hash);
  const evaluationComplete = Boolean(
    productionViewReady
    && ["COLLECTING", "COMPARISON_AVAILABLE", "DISCOVERY"].includes(evaluationStage)
    && evaluationCount !== null
    && reportHash
    && datasetHash
    && independenceHash
    && LEARNING_EVALUATION_AVAILABLE_STATUSES.has(evaluationStatus),
  );
  setText("learning-evaluation-status", evaluationComplete ? evaluationStatus : "UNAVAILABLE · LOCKED");
  setText(
    "learning-evaluation-stage",
    evaluationComplete ? `${evaluationStage} · ${formatInteger(evaluationCount)} independent` : "UNAVAILABLE",
  );
  setText("learning-evaluation-report", evaluationComplete ? shortLearningDigest(reportHash) : "UNAVAILABLE");
  setText("learning-evaluation-dataset", evaluationComplete ? shortLearningDigest(datasetHash) : "UNAVAILABLE");
  setText("learning-evaluation-independence", evaluationComplete ? shortLearningDigest(independenceHash) : "UNAVAILABLE");

  const promotion = schemaValid ? learningObject(governance.promotion) : {};
  const promotionStatus = learningStatus(promotion.status);
  const promotionHash = learningDigest(promotion.authority_hash);
  const promotionActive = Boolean(
    productionViewReady
    && promotionHash
    && LEARNING_AUTHORITY_ACTIVE_STATUSES.has(promotionStatus),
  );
  setText("learning-promotion-status", promotionActive ? promotionStatus : "UNAVAILABLE · LOCKED");
  setText("learning-promotion-hash", promotionActive ? shortLearningDigest(promotionHash) : "UNAVAILABLE");

  const rollback = schemaValid ? learningObject(governance.rollback) : {};
  const rollbackStatus = learningStatus(rollback.status);
  const rollbackHash = learningDigest(rollback.authority_hash);
  const rollbackTarget = learningDigest(rollback.target_policy_hash);
  const rollbackActive = Boolean(
    productionViewReady
    && rollbackHash
    && rollbackTarget
    && rollbackTarget === policyHash
    && LEARNING_AUTHORITY_ACTIVE_STATUSES.has(rollbackStatus),
  );
  setText("learning-rollback-status", rollbackActive ? rollbackStatus : "UNAVAILABLE · LOCKED");
  setText("learning-rollback-hash", rollbackActive ? shortLearningDigest(rollbackHash) : "UNAVAILABLE");
  setText("learning-rollback-target", rollbackActive ? shortLearningDigest(rollbackTarget) : "UNAVAILABLE");
  setText(
    "learning-transition-status",
    promotionActive || rollbackActive ? "AUTHORITY-RECORDED · READ_ONLY" : "LOCKED · READ_ONLY",
  );

  const aGrade = schemaValid ? learningObject(governance.a_grade) : {};
  const aGradeStatus = learningStatus(aGrade.status);
  const aGradeMarker = learningDigest(aGrade.marker_hash);
  const proposalId = learningText(aGrade.proposal_id, 160);
  const proposalHash = learningDigest(aGrade.proposal_hash);
  const candidateHash = learningDigest(aGrade.candidate_hash);
  const rankingBasisHash = learningDigest(aGrade.ranking_basis_hash);
  const aGradePolicyVersion = learningText(aGrade.current_policy_version, 160);
  const aGradePolicyHash = learningDigest(aGrade.current_policy_hash);
  const aGradePolicyMarkerHash = learningDigest(aGrade.policy_authority_marker_hash);
  const executionCostVersion = learningText(aGrade.execution_cost_version, 160);
  const executionCostHash = learningDigest(aGrade.execution_cost_hash);
  const aGradeEvaluationHash = learningDigest(aGrade.evaluation_report_hash);
  const aGradeDatasetHash = learningDigest(aGrade.dataset_hash);
  const aGradeIndependenceHash = learningDigest(aGrade.independence_spec_hash);
  const riskContractHash = learningDigest(aGrade.risk_contract_hash);
  const aGradeFraction = learningFraction(aGrade.max_risk_fraction, 0.15);
  const aGradeActive = Boolean(
    productionViewReady
    && policyComplete
    && evaluationComplete
    && aGradeMarker
    && proposalId
    && proposalHash
    && candidateHash
    && rankingBasisHash
    && aGradePolicyVersion === policyVersion
    && aGradePolicyHash === policyHash
    && aGradePolicyMarkerHash === policyMarkerHash
    && executionCostVersion
    && executionCostHash
    && aGradeEvaluationHash === reportHash
    && aGradeDatasetHash === datasetHash
    && aGradeIndependenceHash === independenceHash
    && riskContractHash
    && aGradeFraction !== null
    && LEARNING_AUTHORITY_ACTIVE_STATUSES.has(aGradeStatus),
  );
  setText("learning-a-grade-status", aGradeActive ? `${aGradeStatus} · PROPOSAL-BOUND` : "UNAVAILABLE · LOCKED");
  setText(
    "learning-a-grade-proposal",
    aGradeActive ? `${proposalId} · ${shortLearningDigest(proposalHash)}` : "UNAVAILABLE",
  );
  setText("learning-a-grade-marker", aGradeActive ? shortLearningDigest(aGradeMarker) : "UNAVAILABLE");
  setText("learning-a-grade-risk", aGradeActive ? `${formatPercent(aGradeFraction)} PROPOSAL-BOUND` : "15% LOCKED");

  setText("learning-production-governance", "LOCKED · READ_ONLY");
  setText(
    "learning-human-signer",
    humanSignerStatus === "TRUSTED" || humanSignerStatus === "NO_TRUSTED_HUMAN_SIGNER"
      ? humanSignerStatus
      : "UNAVAILABLE",
  );

  const risk = schemaValid ? learningObject(governance.risk) : {};
  const normalFraction = learningFraction(risk.normal_max_fraction, 0.10);
  const maximumAGradeFraction = learningFraction(risk.a_grade_max_fraction, 0.15);
  const absoluteRejectFraction = learningFraction(risk.absolute_reject_fraction, 0.20);
  const riskVersion = learningText(risk.authority_version, 160);
  const riskMarkerHash = learningDigest(risk.authority_marker_hash);
  const riskComplete = Boolean(
    resolverViewReady
    && normalFraction !== null
    && maximumAGradeFraction !== null
    && absoluteRejectFraction !== null
    && riskVersion
    && riskMarkerHash,
  );
  setText(
    "learning-risk-contract",
    riskComplete
      ? `NORMAL ${formatPercent(normalFraction)} · A-GRADE ${formatPercent(maximumAGradeFraction)} LOCKED · ${formatPercent(absoluteRejectFraction)} REJECT`
      : "UNAVAILABLE · LOCKED",
  );
  setText(
    "learning-risk-authority",
    riskComplete ? `${riskVersion} · ${shortLearningDigest(riskMarkerHash)}` : "UNAVAILABLE",
  );

  const creatorStatus = source.creator_transport_status === "CREATOR_TRANSPORT_UNAVAILABLE"
    ? "CREATOR_TRANSPORT_UNAVAILABLE"
    : "UNAVAILABLE";
  setText("learning-creator-transport", creatorStatus);
  setText(
    "learning-governance-reason",
    governanceReason || (productionViewReady ? "READY · NO BLOCKER" : "UNAVAILABLE · LOCKED"),
  );
}

function normalizeOutcomeHorizons(payload = {}) {
  const unavailable = (reason) => Object.fromEntries(
    OUTCOME_HORIZONS.map((horizon) => [horizon, {
      status: "UNCERTAIN",
      count: 0,
      observed_count: 0,
      blocked_count: 0,
      uncertain_count: 0,
      reason,
      observed_at: null,
      decision_authority: "SUPPORTING_ONLY",
    }]),
  );
  const source = payload && typeof payload === "object" && !Array.isArray(payload)
    ? payload
    : {};
  if (String(source.status || "UNAVAILABLE").toUpperCase() !== "READY") {
    return unavailable("OUTCOME_HORIZON_SUMMARY_UNAVAILABLE");
  }
  if (
    source.decision_authority !== "SUPPORTING_ONLY"
    || !sanitizeReasonText(String(source.selected_challenger || source.challenger_version || ""), 160)
  ) {
    return unavailable("OUTCOME_HORIZON_SUMMARY_INVALID");
  }

  const completeness = source.completeness && typeof source.completeness === "object"
    && !Array.isArray(source.completeness)
    ? source.completeness
    : {};
  const verifiedHead = source.verified_head && typeof source.verified_head === "object"
    && !Array.isArray(source.verified_head)
    ? source.verified_head
    : {};
  const verifiedHeadSequence = numberOrNull(firstValue(
    source,
    ["verified_head_sequence", "head_sequence", "ledger_head_sequence"],
    firstValue(verifiedHead, ["sequence"], firstValue(completeness, ["verified_head_sequence", "head_sequence"])),
  ));
  const completeThroughSequence = numberOrNull(firstValue(
    source,
    ["complete_through_sequence", "scanned_through_sequence"],
    firstValue(completeness, ["complete_through_sequence", "scanned_through_sequence"]),
  ));
  const verifiedHeadHash = String(firstValue(
    source,
    ["verified_head_hash", "head_hash", "ledger_head_hash"],
    firstValue(verifiedHead, ["hash", "head_hash"], firstValue(completeness, ["verified_head_hash", "head_hash"], "")),
  )).toLowerCase();
  if (
    !Number.isInteger(verifiedHeadSequence)
    || verifiedHeadSequence < 0
    || !Number.isInteger(completeThroughSequence)
    || completeThroughSequence !== verifiedHeadSequence
    || !/^[0-9a-f]{64}$/.test(verifiedHeadHash)
  ) {
    return unavailable("OUTCOME_HORIZON_SUMMARY_INCOMPLETE");
  }

  const rawHorizons = source.horizons && typeof source.horizons === "object"
    && !Array.isArray(source.horizons)
    ? source.horizons
    : null;
  if (!rawHorizons) return unavailable("OUTCOME_HORIZON_SUMMARY_INVALID");
  const result = {};
  for (const horizon of OUTCOME_HORIZONS) {
    const raw = rawHorizons[horizon];
    if (!raw || typeof raw !== "object" || Array.isArray(raw)) {
      return unavailable("OUTCOME_HORIZON_SUMMARY_INVALID");
    }
    const count = numberOrNull(raw.count);
    const observedCount = numberOrNull(raw.observed_count);
    const blockedCount = numberOrNull(raw.blocked_count);
    const uncertainCount = numberOrNull(raw.uncertain_count);
    if (
      ![count, observedCount, blockedCount, uncertainCount].every(
        (value) => Number.isInteger(value) && value >= 0,
      )
      || observedCount + blockedCount + uncertainCount !== count
    ) {
      return unavailable("OUTCOME_HORIZON_SUMMARY_INVALID");
    }
    const rawStatus = String(raw.status || "").toUpperCase();
    const status = ["OBSERVED", "BLOCKED", "UNCERTAIN", "NOT_OBSERVED"].includes(rawStatus)
      ? rawStatus
      : uncertainCount > 0
        ? "UNCERTAIN"
        : blockedCount > 0
          ? "BLOCKED"
          : observedCount > 0
            ? "OBSERVED"
            : "NOT_OBSERVED";
    const defaultReason = status === "NOT_OBSERVED"
      ? "OUTCOME_NOT_YET_OBSERVABLE"
      : `OUTCOME_HORIZON_${status}`;
    result[horizon] = {
      status,
      count,
      observed_count: observedCount,
      blocked_count: blockedCount,
      uncertain_count: uncertainCount,
      reason: sanitizeReasonText(String(raw.reason || raw.reason_code || defaultReason), 96)
        || "OUTCOME_REASON_REDACTED",
      observed_at: firstValue(raw, ["observed_at", "latest_observed_at"], null),
      decision_authority: "SUPPORTING_ONLY",
    };
  }
  return result;
}

function outcomeProcessingSummary(processing = {}) {
  const source = processing && typeof processing === "object" && !Array.isArray(processing)
    ? processing
    : {};
  const processingStatus = String(source.status || "UNAVAILABLE").toUpperCase();
  const recorded = numberOrNull(source.recorded_count);
  const skipped = numberOrNull(source.skipped_count);
  const blocked = numberOrNull(source.blocked_count);
  const errors = numberOrNull(source.error_count);
  const remaining = numberOrNull(source.remaining_count);
  return `${processingStatus} · recorded ${formatInteger(recorded ?? 0)} · `
    + `skipped ${formatInteger(skipped ?? 0)} · `
    + `blocked ${formatInteger(blocked ?? 0)} · `
    + `errors ${formatInteger(errors ?? 0)} · `
    + `remaining ${formatInteger(remaining ?? 0)} · SUPPORTING_ONLY`;
}

function renderOutcomeHorizons(payload = {}, processing = {}, capture = {}) {
  const horizons = normalizeOutcomeHorizons(payload);
  const ids = {
    "30M": "outcome-horizon-30m",
    SESSION_CLOSE: "outcome-horizon-session-close",
    "1D": "outcome-horizon-1d",
    "3D": "outcome-horizon-3d",
    "5D": "outcome-horizon-5d",
  };
  Object.entries(horizons).forEach(([horizon, value]) => {
    setText(
      ids[horizon],
      `${value.status} · OBSERVED ${formatInteger(value.observed_count)} · `
        + `BLOCKED ${formatInteger(value.blocked_count)} · `
        + `UNCERTAIN ${formatInteger(value.uncertain_count)} · SUPPORTING_ONLY`,
    );
  });
  setText(
    "outcome-processing-status",
    outcomeProcessingSummary(processing),
  );
  const blockers = capture && typeof capture.durable_blocker_counts === "object"
    && !Array.isArray(capture.durable_blocker_counts)
    ? capture.durable_blocker_counts
    : {};
  const blockerSummary = Object.entries(blockers)
    .filter(([reason, count]) => /^[A-Z][A-Z0-9_]{0,95}$/.test(reason) && Number.isInteger(count) && count > 0)
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([reason, count]) => `${reason}=${count}`)
    .join(" · ");
  const directionState = capture.direction_outcomes_enabled === true
    ? "方向 outcome 已启用"
    : "方向 outcome 未启用";
  const optionState = capture.option_economics_requires_bound_candidate === true
    ? "期权经济 outcome 仅在真实 rank-1 组合绑定后计算"
    : "期权经济 outcome 状态未知";
  setText(
    "outcome-capture-summary",
    `${directionState}；${optionState}${blockerSummary ? `；durable blockers ${blockerSummary}` : ""}；SHADOW_ONLY`,
  );
}

function deepseekRuntimeSummary(advisoryPayload = {}, newsPayload = {}) {
  const advisory = advisoryPayload && typeof advisoryPayload === "object" && !Array.isArray(advisoryPayload)
    ? advisoryPayload
    : {};
  const news = newsPayload && typeof newsPayload === "object" && !Array.isArray(newsPayload)
    ? newsPayload
    : {};
  const rows = Array.isArray(news.news) ? news.news : [];
  const coverage = classifierCoverage(rows);
  const shadow = news.shadow_advisory && typeof news.shadow_advisory === "object" && !Array.isArray(news.shadow_advisory)
    ? news.shadow_advisory
    : {};
  const declaredCount = numberOrNull(shadow.advisory_count);
  const advisoryCount = Number.isInteger(declaredCount) && declaredCount >= 0
    ? declaredCount
    : coverage.deepseekShadow;
  const declaredStatus = String(shadow.status || "UNAVAILABLE").trim().toUpperCase();
  const shadowStatus = ["READY", "DEGRADED", "PARTIAL", "UNAVAILABLE", "DISABLED"].includes(declaredStatus)
    ? declaredStatus
    : "UNAVAILABLE";
  const legacyModelState = ["MODEL", "FALLBACK"].includes(String(advisory.model_state || "").toUpperCase())
    ? String(advisory.model_state).toUpperCase()
    : "FALLBACK";
  return {
    shadowStatus,
    advisoryCount,
    rowAdvisoryCount: coverage.deepseekShadow,
    total: coverage.total,
    deterministic: coverage.deterministic,
    shadowPredictions: coverage.shadowPredictions,
    modelOutputAvailable: shadowStatus === "READY" && advisoryCount > 0,
    countMismatch: advisoryCount !== coverage.deepseekShadow,
    legacyModelState,
    legacyFallbackReason: String(advisory.fallback_reason || "MODEL_ADVISORY_UNAVAILABLE").trim().toUpperCase(),
    failureReasons: shadow.failure_reasons && typeof shadow.failure_reasons === "object" && !Array.isArray(shadow.failure_reasons)
      ? shadow.failure_reasons
      : {},
  };
}

const RESEARCH_ALLOCATION_SCHEMA = "options_copilot.research_allocation_evidence.v3";
const RESEARCH_ALLOCATION_KEYS = Object.freeze([
  "advisory_available_count",
  "advisory_coverage_count",
  "advisory_coverage_ratio",
  "advisory_order_changed_count",
  "advisory_promoted_symbols",
  "advisory_selected_count",
  "advisory_selection_displacement_count",
  "approval_eligible",
  "core_score_inputs",
  "decision_authority",
  "deterministic_baseline_symbols",
  "eligibility_effect",
  "event_symbols",
  "influence_scope",
  "instruction_creation_allowed",
  "limit",
  "order_allowed",
  "risk_effect",
  "scanner_score_inputs",
  "schema",
  "score_evidence",
  "selected_symbols",
  "total_event_symbol_count",
].sort());
const RESEARCH_ALLOCATION_SCORE_KEYS = Object.freeze([
  "advisory_score",
  "deterministic_score",
  "selected_research_priority_score",
  "selected_research_priority_source",
  "symbol",
].sort());
const RESEARCH_PRIORITY_INPUT_KEYS = Object.freeze(["score", "symbol"]);

function sameExactKeys(value, expected) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const keys = Object.keys(value).sort();
  return keys.length === expected.length && keys.every((key, index) => key === expected[index]);
}

function canonicalResearchSymbol(value) {
  return typeof value === "string"
    && /^[A-Z][A-Z0-9.-]{0,15}$/.test(value)
    ? value
    : null;
}

function strictResearchScore(value) {
  if (typeof value !== "string" || !/^(?:0|[1-9]\d*)(?:\.\d+)?$/.test(value)) return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed >= 0 && parsed <= 100 ? value : null;
}

function compareResearchScores(left, right) {
  const [leftInteger, leftFraction = ""] = left.split(".");
  const [rightInteger, rightFraction = ""] = right.split(".");
  if (leftInteger.length !== rightInteger.length) return leftInteger.length - rightInteger.length;
  if (leftInteger !== rightInteger) return leftInteger < rightInteger ? -1 : 1;
  const width = Math.max(leftFraction.length, rightFraction.length);
  const leftPadded = leftFraction.padEnd(width, "0");
  const rightPadded = rightFraction.padEnd(width, "0");
  if (leftPadded === rightPadded) return 0;
  return leftPadded < rightPadded ? -1 : 1;
}

function compareResearchSymbols(left, right) {
  if (left === right) return 0;
  return left < right ? -1 : 1;
}

function sameSymbols(left, right) {
  return left.length === right.length && left.every((symbol, index) => symbol === right[index]);
}

function normalizedSymbolArray(value, maximum) {
  if (!Array.isArray(value) || value.length > maximum) return null;
  const symbols = value.map(canonicalResearchSymbol);
  return symbols.every(Boolean) && new Set(symbols).size === symbols.length ? symbols : null;
}

function normalizedPriorityInputs(value) {
  if (!Array.isArray(value) || value.length > 30) return null;
  const rows = [];
  const seen = new Set();
  for (const item of value) {
    if (!sameExactKeys(item, RESEARCH_PRIORITY_INPUT_KEYS)) return null;
    const symbol = canonicalResearchSymbol(item.symbol);
    const score = strictResearchScore(item.score);
    if (!symbol || score === null || seen.has(symbol)) return null;
    seen.add(symbol);
    rows.push({ symbol, score });
  }
  const ranked = [...rows].sort((left, right) => (
    compareResearchScores(right.score, left.score) || compareResearchSymbols(left.symbol, right.symbol)
  ));
  return rows.every((row, index) => row.symbol === ranked[index].symbol && row.score === ranked[index].score)
    ? rows
    : null;
}

function rankedResearchSymbols(...groups) {
  const scores = new Map();
  groups.flat().forEach(({ symbol, score }) => {
    const current = scores.get(symbol);
    if (current === undefined || compareResearchScores(score, current) > 0) scores.set(symbol, score);
  });
  return [...scores.entries()]
    .sort(([leftSymbol, leftScore], [rightSymbol, rightScore]) => (
      compareResearchScores(rightScore, leftScore) || compareResearchSymbols(leftSymbol, rightSymbol)
    ))
    .map(([symbol]) => symbol);
}

function balancedResearchSymbols(scannerRows, coreRows, limit) {
  const scanner = rankedResearchSymbols(scannerRows);
  const core = rankedResearchSymbols(coreRows);
  if (!core.length) return scanner.slice(0, limit);
  if (!scanner.length) return core.slice(0, limit);
  const selected = [];
  for (let index = 0; index < Math.max(core.length, scanner.length); index += 1) {
    for (const group of [core, scanner]) {
      const symbol = group[index];
      if (symbol && !selected.includes(symbol)) selected.push(symbol);
      if (selected.length >= limit) return selected;
    }
  }
  return selected;
}

function validatedResearchAllocation(raw) {
  if (
    !sameExactKeys(raw, RESEARCH_ALLOCATION_KEYS)
    || raw.schema !== RESEARCH_ALLOCATION_SCHEMA
    || raw.influence_scope !== "RESEARCH_SCHEDULING_HINT_ONLY"
    || raw.decision_authority !== "SUPPORTING_ONLY"
    || raw.eligibility_effect !== "NONE"
    || raw.risk_effect !== "NONE"
    || raw.approval_eligible !== false
    || raw.instruction_creation_allowed !== false
    || raw.order_allowed !== false
    || !Number.isInteger(raw.limit)
    || raw.limit < 1
    || raw.limit > 30
  ) return null;
  const eventSymbols = normalizedSymbolArray(raw.event_symbols, 50);
  const baseline = normalizedSymbolArray(raw.deterministic_baseline_symbols, 30);
  const selected = normalizedSymbolArray(raw.selected_symbols, 30);
  const promoted = normalizedSymbolArray(raw.advisory_promoted_symbols, 30);
  const scannerInputs = normalizedPriorityInputs(raw.scanner_score_inputs);
  const coreInputs = normalizedPriorityInputs(raw.core_score_inputs);
  if (!eventSymbols || !baseline || !selected || !promoted || !scannerInputs || !coreInputs) return null;
  if (baseline.length > raw.limit || selected.length > raw.limit) return null;
  if (!Array.isArray(raw.score_evidence) || raw.score_evidence.length > 50) return null;
  const scoreEvidence = [];
  for (const item of raw.score_evidence) {
    if (!sameExactKeys(item, RESEARCH_ALLOCATION_SCORE_KEYS)) return null;
    const symbol = canonicalResearchSymbol(item.symbol);
    const deterministicScore = strictResearchScore(item.deterministic_score);
    const advisoryScore = item.advisory_score === null ? null : strictResearchScore(item.advisory_score);
    const selectedScore = strictResearchScore(item.selected_research_priority_score);
    const selectedSource = item.selected_research_priority_source;
    if (
      !symbol
      || deterministicScore === null
      || (item.advisory_score !== null && advisoryScore === null)
      || selectedScore === null
    ) return null;
    const winningSource = advisoryScore !== null && compareResearchScores(advisoryScore, deterministicScore) > 0
      ? "NEWS_SHADOW_PRIORITY_SUPPORTING_ONLY"
      : "NEWS_SUPPORTING_ONLY";
    const deterministicSelected = selectedSource === "NEWS_SUPPORTING_ONLY"
      && compareResearchScores(selectedScore, deterministicScore) === 0;
    const shadowSelected = selectedSource === "NEWS_SHADOW_PRIORITY_SUPPORTING_ONLY"
      && advisoryScore !== null
      && compareResearchScores(selectedScore, advisoryScore) === 0;
    if (
      selectedSource !== winningSource
      || (!deterministicSelected && !shadowSelected)
    ) return null;
    scoreEvidence.push({ symbol, deterministicScore, advisoryScore, selectedScore, selectedSource });
  }
  const scoreSymbols = scoreEvidence.map((item) => item.symbol);
  if (!sameSymbols(scoreSymbols, [...scoreSymbols].sort()) || !sameSymbols(eventSymbols, scoreSymbols)) return null;

  const deterministicRows = scoreEvidence.map((item) => ({ symbol: item.symbol, score: item.deterministicScore }));
  const selectedRows = scoreEvidence.map((item) => ({ symbol: item.symbol, score: item.selectedScore }));
  const replayedBaseline = balancedResearchSymbols([...deterministicRows, ...scannerInputs], coreInputs, raw.limit);
  const replayedSelected = balancedResearchSymbols([...selectedRows, ...scannerInputs], coreInputs, raw.limit);
  if (!sameSymbols(baseline, replayedBaseline) || !sameSymbols(selected, replayedSelected)) return null;

  const advisorySymbols = new Set(scoreEvidence.filter((item) => item.advisoryScore !== null).map((item) => item.symbol));
  const selectedSet = new Set(selected);
  const baselineIndex = new Map(baseline.map((symbol, index) => [symbol, index]));
  const selectedIndex = new Map(selected.map((symbol, index) => [symbol, index]));
  const replayedPromoted = selected.filter((symbol) => {
    const row = scoreEvidence.find((item) => item.symbol === symbol);
    return row?.selectedSource === "NEWS_SHADOW_PRIORITY_SUPPORTING_ONLY"
      && (!baselineIndex.has(symbol) || selectedIndex.get(symbol) < baselineIndex.get(symbol));
  });
  const orderChanged = selected.reduce(
    (count, symbol, index) => count + (symbol !== baseline[index] ? 1 : 0),
    0,
  ) + Math.max(0, baseline.length - selected.length);
  const displacement = selected.filter((symbol) => !baselineIndex.has(symbol)).length;
  const coverageRatio = eventSymbols.length ? (advisorySymbols.size / eventSymbols.length).toFixed(6) : "0";
  if (
    raw.total_event_symbol_count !== eventSymbols.length
    || raw.advisory_available_count !== advisorySymbols.size
    || raw.advisory_coverage_count !== advisorySymbols.size
    || raw.advisory_selected_count !== [...advisorySymbols].filter((symbol) => selectedSet.has(symbol)).length
    || raw.advisory_coverage_ratio !== coverageRatio
    || raw.advisory_order_changed_count !== orderChanged
    || raw.advisory_selection_displacement_count !== displacement
    || !sameSymbols(promoted, replayedPromoted)
  ) return null;
  return { eventSymbols, baseline, selected, promoted, scoreEvidence };
}

function researchAllocationSummary(scanPayload = {}) {
  const scan = scanPayload && typeof scanPayload === "object" && !Array.isArray(scanPayload)
    ? scanPayload
    : {};
  const funnel = scan.funnel_trace && typeof scan.funnel_trace === "object" && !Array.isArray(scan.funnel_trace)
    ? scan.funnel_trace
    : {};
  const raw = funnel.research_allocation && typeof funnel.research_allocation === "object" && !Array.isArray(funnel.research_allocation)
    ? funnel.research_allocation
    : {};
  const validated = validatedResearchAllocation(raw);
  const scope = validated ? raw.influence_scope : "UNAVAILABLE";
  const authority = validated ? raw.decision_authority : "UNAVAILABLE";
  const eligibilityEffect = validated ? raw.eligibility_effect : "UNAVAILABLE";
  const riskEffect = validated ? raw.risk_effect : "UNAVAILABLE";
  const available = validated !== null;
  const count = (value) => {
    const parsed = numberOrNull(value);
    return Number.isInteger(parsed) && parsed >= 0 ? parsed : 0;
  };
  const scoreEvidence = validated
    ? validated.scoreEvidence.slice(0, 10).map((item) => ({
      symbol: item.symbol,
      deterministicScore: numberOrNull(item.deterministicScore),
      advisoryScore: numberOrNull(item.advisoryScore),
      selectedScore: numberOrNull(item.selectedScore),
      selectedSource: item.selectedSource,
    }))
    : [];
  return {
    available,
    scope,
    authority,
    advisoryAvailable: available ? count(raw.advisory_available_count) : 0,
    advisorySelected: available ? count(raw.advisory_selected_count) : 0,
    coverageCount: available ? count(raw.advisory_coverage_count) : 0,
    totalEventSymbols: available ? count(raw.total_event_symbol_count) : 0,
    coverageRatio: available ? numberOrNull(raw.advisory_coverage_ratio) : null,
    orderChanged: available ? count(raw.advisory_order_changed_count) : 0,
    selectionDisplacement: available ? count(raw.advisory_selection_displacement_count) : 0,
    eligibilityEffect,
    riskEffect,
    approvalEligible: available && raw.approval_eligible === true,
    instructionCreationAllowed: available && raw.instruction_creation_allowed === true,
    orderAllowed: available && raw.order_allowed === true,
    scoreEvidence,
  };
}

function renderDeepSeekAdvisory(payload = {}, newsPayload = {}) {
  const source = payload && typeof payload === "object" && !Array.isArray(payload) ? payload : {};
  const runtime = deepseekRuntimeSummary(source, newsPayload);
  const durableShadow = durableShadowCounts(appState.learningSnapshot);
  const durablePredictions = durableShadowCountText(durableShadow, "predictions");
  const durableOutcomes = durableShadowCountText(durableShadow, "outcomes");
  const symbol = sanitizeReasonText(String(source.symbol || "NO_SYMBOL"), 24) || "NO_SYMBOL";
  const consensus = sanitizeReasonText(String(source.consensus_state || "UNCERTAIN"), 32) || "UNCERTAIN";
  setText(
    "deepseek-advisory-status",
    `${runtime.modelOutputAvailable ? "SHADOW READY" : runtime.shadowStatus} · ${runtime.advisoryCount}/${runtime.total} · SUPPORTING_ONLY`,
  );
  setText(
    "deepseek-advisory-summary",
    runtime.modelOutputAvailable
      ? `生产分类 ${runtime.deterministic}/${runtime.total} DETERMINISTIC_RULES；当前批次 ${runtime.advisoryCount} 条 shadow advisory / ${runtime.shadowPredictions} 条五时域预测；shadow 建议仅单独展示，绝不改变 deterministic research/watch 排序、deep-scan 选择或 decision-event 输入；durable 累计预测 ${durablePredictions}、已结算 outcome ${durableOutcomes}；不改变 Gate、eligibility、risk、approval、instruction 或 order`
      : `${symbol} · ${consensus} · 当前新闻批次尚无 DeepSeek 输出；durable 累计预测 ${durablePredictions}、已结算 outcome ${durableOutcomes}；不改变 Gate、eligibility、risk、approval、instruction 或 order`,
  );
  const details = byId("deepseek-advisory-details");
  if (!details) return;
  details.replaceChildren();
  const append = (label, value) => {
    const text = sanitizeReasonText(value, 240);
    if (text) details.append(createElement("li", "", `${label} · ${text}`));
  };
  append(
    "NEWS SHADOW",
    `${runtime.shadowStatus} · API ${runtime.advisoryCount} · ROWS ${runtime.rowAdvisoryCount}${runtime.countMismatch ? " · COVERAGE_MISMATCH" : ""}`,
  );
  const failureSummary = Object.entries(runtime.failureReasons)
    .filter(([reason, count]) => /^[A-Z][A-Z0-9_]{0,95}$/.test(reason) && Number.isInteger(count) && count > 0)
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([reason, count]) => `${reason}=${count}`)
    .join(" · ");
  if (failureSummary) append("SHADOW BLOCKERS", failureSummary);
  const exclusionSummary = Object.entries(durableShadow.exclusionReasons)
    .filter(([reason, count]) => /^[A-Z][A-Z0-9_]{0,119}$/.test(reason) && Number.isInteger(count) && count > 0)
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([reason, count]) => `${reason}=${count}`)
    .join(" · ");
  append(
    "DURABLE CONTRACT",
    `${durableShadow.challenger} · LEGACY EXCLUDED ${durableShadow.legacyExcluded}${exclusionSummary ? ` · ${exclusionSummary}` : ""}`,
  );
  append(
    "LEGACY PHASE2 ADAPTER",
    `${runtime.legacyModelState} · ${runtime.legacyFallbackReason}`,
  );
  const facts = Array.isArray(source.event_news_facts) ? source.event_news_facts : [];
  facts.slice(0, 3).forEach((fact) => append("EVENT/NEWS", String(fact?.statement || "")));
  for (const [label, key] of [
    ["FUNDAMENTALS", "fundamental_support"],
    ["PRICE", "expected_price_impact"],
    ["VOLATILITY", "options_volatility_impact"],
  ]) {
    const slice = source[key];
    if (slice && typeof slice === "object" && !Array.isArray(slice)) {
      append(label, `${slice.status || "UNCERTAIN"} · ${slice.direction || "UNCERTAIN"} · ${slice.summary || ""}`);
    }
  }
  const counter = Array.isArray(source.counter_evidence) ? source.counter_evidence : [];
  counter.slice(0, 3).forEach((value) => append("COUNTER", String(value)));
  if (details.childNodes.length === 0) {
    details.append(createElement("li", "", "UNCERTAIN · MODEL_ADVISORY_UNAVAILABLE"));
  }
}

function renderLearning(payload = {}, advisoryPayload = {}, newsPayload = {}) {
  const source = payload && typeof payload === "object" && !Array.isArray(payload) ? payload : {};
  appState.learningSnapshot = source;
  const models = normalizeLearningModel(source.models);
  const shadow = normalizeLearningModel(source.shadow_learning);
  const shadowChallenger = Array.isArray(shadow.challengers) ? shadow.challengers[0] : null;
  const champion = {
    ...normalizeLearningModel(models.champion),
    ...normalizeLearningModel(source.champion),
  };
  const challenger = {
    ...normalizeLearningModel(models.challenger),
    ...normalizeLearningModel(shadowChallenger),
    ...normalizeLearningModel(source.challenger),
  };
  const independentSamples = validLearningCount(shadow.independent_samples);
  const recordCount = validLearningCount(shadow.record_count);
  const stage = learningEvaluationStage(independentSamples);

  renderModel(byId("learning-champion"), champion, "暂无生产基准");
  renderModel(
    byId("learning-challenger"),
    challenger,
    "暂无影子模型",
    independentSamples === null ? undefined : independentSamples,
  );

  const gate = byId("learning-gate");
  if (gate) {
    gate.classList.remove("status-up", "status-down", "status-stale", "status-unknown");
    gate.classList.add("status-stale");
    gate.textContent = `SHADOW_ONLY · ${stage}`;
  }
  setText("learning-stage", stage);
  setText("learning-samples", `${independentSamples === null ? "--" : formatInteger(independentSamples)} / ${LEARNING_DISCOVERY_SAMPLE_TARGET}`);
  setText("learning-record-count", recordCount === null ? "--" : formatInteger(recordCount));

  const integrity = normalizeLearningModel(shadow.ledger).integrity_verified;
  const integrityNode = byId("learning-integrity");
  if (integrityNode) {
    integrityNode.dataset.state = integrity === true ? "verified" : integrity === false ? "failed" : "unknown";
    integrityNode.textContent = integrity === true ? "已验证" : integrity === false ? "验证失败" : "未提供";
  }
  renderP9Governance(source);
  appState.advisorySnapshot = advisoryPayload;
  renderDeepSeekAdvisory(advisoryPayload, newsPayload);
  renderOutcomeHorizons(
    source.outcome_horizons || {},
    source.outcome_processing || {},
    source.outcome_capture || {},
  );
  setText("learning-summary", LEARNING_GOVERNANCE_MESSAGE);
  renderNewsCapabilitySummary();
  renderOverviewPriority();
}

function renderModel(card, model, emptyName, independentSamples = undefined) {
  const normalized = normalizeLearningModel(model);
  const metrics = normalized.metadata && typeof normalized.metadata === "object" && !Array.isArray(normalized.metadata)
    ? normalized.metadata
    : normalized;
  const name = firstValue(normalized, ["name", "model_name", "version", "version_id"], emptyName);
  setModelField(card, "model-name", typeof name === "string" ? name.trim() || emptyName : name);
  setModelField(card, "model-ev", formatMoney(firstValue(metrics, ["ev_after_cost_usd", "expected_value_usd", "ev_usd"]), { signed: true }));
  const calibration = firstValue(metrics, ["calibration_error", "brier_score", "forward_status", "status"]);
  setModelField(card, "model-calibration", typeof calibration === "number" ? calibration.toFixed(3) : calibration ?? "--");
  const sampleValue = independentSamples === undefined
    ? firstValue(metrics, ["independent_samples", "sample_count", "samples"])
    : independentSamples;
  setModelField(card, "model-samples", formatInteger(sampleValue));
}

function setModelField(card, field, value) {
  const node = card.querySelector(`[data-field="${field}"]`);
  if (node) node.textContent = value ?? "--";
}

function setField(card, field, value, className = "") {
  const node = card.querySelector(`[data-field="${field}"]`);
  if (node) {
    node.textContent = value ?? "--";
    if (className) node.classList.add(className);
  }
  return node;
}

function createElement(tag, className = "", text = "") {
  const node = document.createElement(tag);
  if (className) node.className = className;
  node.textContent = text;
  return node;
}

document.addEventListener("DOMContentLoaded", () => {
  byId("refresh-data").addEventListener("click", refreshAll);
  byId("toggle-overview-details").addEventListener("click", () => {
    appState.overviewDetailsExpanded = !appState.overviewDetailsExpanded;
    syncOverviewDetailVisibility();
  });
  byId("read-after-hours-marks").addEventListener("click", readAfterHoursIndicative);
  document.querySelectorAll("[data-page]").forEach((button) => {
    button.addEventListener("click", () => showPage(button.dataset.page));
  });
  byId("news-filter").addEventListener("change", (event) => {
    appState.newsFilter = event.target.value;
    renderNewsLists();
  });
  [["news-category-filter", "newsCategoryFilter"], ["news-source-filter", "newsSourceFilter"], ["news-symbol-filter", "newsSymbolFilter"]].forEach(([id, stateKey]) => {
    byId(id).addEventListener("change", (event) => {
      appState[stateKey] = event.target.value;
      renderNewsLists();
    });
  });
  document.querySelectorAll("[data-calendar-window]").forEach((button) => {
    button.addEventListener("click", () => {
      appState.calendarWindow = button.dataset.calendarWindow;
      document.querySelectorAll("[data-calendar-window]").forEach((tab) => tab.classList.toggle("is-active", tab === button));
      renderCalendarList();
    });
  });
  document.querySelectorAll("[data-option-pool]").forEach((button) => {
    button.addEventListener("click", () => {
      appState.optionPoolStage = button.dataset.optionPool === "open-repriced"
        ? "open-repriced"
        : "pre-market";
      appState.selectedOptionId = null;
      renderOptionPreselections();
    });
  });
  document.querySelectorAll("[data-research-top10-stage]").forEach((button) => {
    button.addEventListener("click", () => {
      appState.researchTop10Stage = button.dataset.researchTop10Stage === "open-repriced"
        ? "open-repriced"
        : "pre-market";
      renderResearchTop10Stage();
    });
  });
  window.setInterval(synchronizeActionControls, 1000);
  window.setInterval(refreshControlSnapshot, CONTROL_REFRESH_INTERVAL_MS);
  window.setInterval(refreshScanSnapshot, SCAN_REFRESH_INTERVAL_MS);
  window.setInterval(refreshNewsData, READ_ONLY_REFRESH_INTERVAL_MS);
  document.addEventListener("visibilitychange", () => {
    expireHoldingsClosePreviews();
    if (!document.hidden) void refreshAll();
  });
  syncOverviewDetailVisibility();
  void refreshAll();
});

function showPage(page) {
  const selected = page === "news" ? "news" : "overview";
  document.querySelectorAll("[data-page-panel]").forEach((panel) => {
    panel.hidden = panel.dataset.pagePanel !== selected;
  });
  document.querySelectorAll("[data-page]").forEach((tab) => {
    const active = tab.dataset.page === selected;
    tab.classList.toggle("is-active", active);
    if (active) tab.setAttribute("aria-current", "page");
    else tab.removeAttribute("aria-current");
  });
  if (selected === "news") refreshNewsData();
}
