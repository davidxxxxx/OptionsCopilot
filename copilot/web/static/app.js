"use strict";

const ENDPOINTS = Object.freeze({
  status: "/api/status",
  latest: "/api/runs/latest",
  runs: "/api/runs",
  news: "/api/news",
  earnings: "/api/earnings",
  macroEvents: "/api/macro-events",
  majorMacroEvents: "/api/macro-events/major",
  eventStudy: "/api/event-study",
  eventStudyLatest: "/api/event-study/latest",
  eventReactions: "/api/event-reactions",
  newsCalibration: "/api/news-calibration",
  flowStats: "/api/flow-stats",
  views: "/api/views",
  tracking: "/api/tracking",
  marketState: "/api/market-state",
  intradayStatus: "/api/intraday/status",
  meta: "/api/meta",
});
const ACTION_NAMES = Object.freeze({
  open: "建立", reverse: "反转", close_direction: "转为中性", close: "结束",
  strengthen: "加强", weaken: "减弱", volatility: "波动判断变化",
});
const STATUS_REFRESH_MS = 15000;
const DIRECTION_NAMES = Object.freeze({ bullish: "看涨", bearish: "看跌", neutral: "中性" });
const VOL_NAMES = Object.freeze({ expand: "波动放大", contract: "波动收敛", neutral: "波动不变" });
const RUN_STATUS = Object.freeze({ ok: "完成", partial: "部分完成", failed: "失败", running: "运行中" });
const SOURCE_NAMES = Object.freeze({
  jin10: "金十", finnhub_market: "Finnhub 市场", finnhub_company: "Finnhub 公司", finnhub: "Finnhub", earnings: "财报日历",
  tradingview: "TradingView", forexfactory: "ForexFactory", fed: "美联储", nasdaq: "Nasdaq", ibkr: "IBKR",
});
const HOUR_NAMES = Object.freeze({ bmo: "盘前", amc: "盘后", dmh: "盘中" });

const appState = { latestRunId: null, wasRunning: false, run: null, loaded: false, meta: null, tracking: null, status: null, calibration: null, flowStats: null };

function el(tag, options = {}, children = []) {
  const node = document.createElement(tag);
  if (options.text !== undefined && options.text !== null) node.textContent = String(options.text);
  if (options.className) node.className = options.className;
  if (options.title) node.title = options.title;
  for (const child of children) if (child) node.appendChild(child);
  return node;
}

function setText(id, text) {
  const node = document.getElementById(id);
  if (node) node.textContent = text;
}

function clear(node) {
  while (node.firstChild) node.removeChild(node.firstChild);
}

function numberOrNull(value) {
  const number = Number(value);
  return value === null || value === undefined || !Number.isFinite(number) ? null : number;
}

function money(value, digits = 0) {
  const number = numberOrNull(value);
  if (number === null) return "—";
  return `${number < 0 ? "-" : ""}$${Math.abs(number).toLocaleString("en-US", {
    minimumFractionDigits: digits, maximumFractionDigits: digits,
  })}`;
}

function percent(value, digits = 0) {
  const number = numberOrNull(value);
  return number === null ? "—" : `${(number * 100).toFixed(digits)}%`;
}

function localTime(iso) {
  if (!iso) return "—";
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) return "—";
  return date.toLocaleString("zh-CN", { hour12: false, month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit" });
}

async function getJson(url, init) {
  const response = await fetch(url, init);
  if (!response.ok) {
    const error = new Error(`HTTP ${response.status}`);
    error.status = response.status;
    throw error;
  }
  return response.json();
}

function chip(text, tone) {
  return el("span", { text, className: `chip ${tone || ""}` });
}

async function refreshStatus() {
  let status;
  try {
    status = await getJson(ENDPOINTS.status);
  } catch (error) {
    const chips = document.getElementById("chips");
    clear(chips);
    chips.appendChild(chip("服务不可达", "bad"));
    return;
  }
  setText("clock", `纽约 ${status.new_york} ｜ 北京 ${status.beijing}`);
  appState.status = status;
  const chips = document.getElementById("chips");
  clear(chips);
  const latest = status.latest_run;
  const ibkr = latest ? latest.ibkr : null;
  chips.appendChild(chip(
    `IBKR ${ibkr === "ok" ? "已连接" : ibkr === "unavailable" ? "未连接" : ibkr || "—"} (${status.ibkr.host}:${status.ibkr.port})`,
    ibkr === "ok" ? "ok" : "bad",
  ));
  const llmOk = status.secrets.deepseek_api_key && status.llm.enabled;
  chips.appendChild(chip(`LLM ${llmOk ? status.llm.model : "未启用"}`, llmOk ? "ok" : "warn"));
  const counts = status.news_24h || {};
  chips.appendChild(chip(`新闻24h 金十${counts.jin10 || 0} / Finnhub${counts.finnhub || 0}`, (counts.jin10 || 0) > 0 ? "ok" : "warn"));
  const schedule = status.schedule || {};
  if (schedule.enabled) {
    const next = (schedule.next_runs || [])[0];
    chips.appendChild(chip(`下次自动运行 ${next ? localTime(next) : "—"}`, "ok"));
  } else {
    chips.appendChild(chip("自动调度关闭", "warn"));
  }
  const running = Boolean(status.running);
  const button = document.getElementById("run-button");
  button.disabled = running;
  button.textContent = running ? "运行中…" : "立即运行";
  if (running) chips.appendChild(chip(`运行中（${status.running.trigger === "manual" ? "手动" : status.running.slot}）`, "warn"));
  renderEventRunBanner(status.latest_event_run);
  notifyEventRun(status.latest_event_run);

  const latestId = latest && latest.status !== "running" ? latest.id : null;
  if (!appState.loaded || latestId !== appState.latestRunId || (appState.wasRunning && !running)) {
    appState.loaded = true;
    appState.latestRunId = latestId;
    await refreshRun();
    await refreshRuns();
  }
  appState.wasRunning = running;
}

async function refreshRun() {
  try {
    appState.run = await getJson(ENDPOINTS.latest);
  } catch (error) {
    appState.run = null;
  }
  try {
    appState.calibration = await getJson(ENDPOINTS.newsCalibration);
  } catch (error) {
    appState.calibration = null;
  }
  try {
    appState.flowStats = await getJson(ENDPOINTS.flowStats);
  } catch (error) {
    appState.flowStats = null;
  }
  renderRun();
  renderIdeas();
  await refreshEventStudy();
  renderViews();
  renderPositions();
  await refreshTracking();
  renderNewsCalibration();
  renderFlowStats();
  await refreshMajorEvents();
}

function calibrationText(calibration) {
  if (!calibration) return "新闻观点校准：—";
  return `新闻观点校准：漂移 k=${Number(calibration.k).toFixed(2)}，扩波 k=${Number(calibration.k_vol_expand).toFixed(2)}，缩波 k=${Number(calibration.k_vol_contract).toFixed(2)}，${calibration.days || 0} 天`;
}

function renderNewsCalibration() {
  const calibration = appState.calibration;
  setText("news-calibration-line", calibrationText(calibration));
  setText("news-calibration-status", calibration
    ? (calibration.verified ? "已达到验证门槛" : `未验证：需至少 ${calibration.min_days_verified} 天且 sd ≤ ${Number(calibration.max_sd_verified).toFixed(2)}`)
    : "暂无数据");
  const summary = document.getElementById("news-calibration-summary");
  const body = document.querySelector("#news-calibration-table tbody");
  if (!summary || !body) return;
  clear(summary);
  clear(body);
  if (!calibration) return;
  summary.appendChild(stageBox("校准系数", Number(calibration.k).toFixed(2), `原始 ${Number(calibration.k_raw).toFixed(2)}`, calibration.verified ? "pos" : "warn"));
  summary.appendChild(stageBox("后验不确定性", Number(calibration.sd).toFixed(2), `样本 ${calibration.samples || 0}；交易日 ${calibration.days || 0}`, ""));
  summary.appendChild(stageBox("方向命中率", percent(calibration.hit_rate, 1), `平均 x ${calibration.mean_x === null ? "—" : Number(calibration.mean_x).toFixed(2)}；平均 y ${calibration.mean_y === null ? "—" : Number(calibration.mean_y).toFixed(2)}`, ""));
  summary.appendChild(stageBox("扩波校准", Number(calibration.k_vol_expand).toFixed(2), `样本 ${calibration.vol_expand.samples || 0}；中性 ${calibration.vol_expand.neutral_samples || 0}；比率 ${calibration.vol_expand.ratio === null ? "—" : Number(calibration.vol_expand.ratio).toFixed(2)}`, ""));
  summary.appendChild(stageBox("缩波校准", Number(calibration.k_vol_contract).toFixed(2), `样本 ${calibration.vol_contract.samples || 0}；中性 ${calibration.vol_contract.neutral_samples || 0}；比率 ${calibration.vol_contract.ratio === null ? "—" : Number(calibration.vol_contract.ratio).toFixed(2)}`, ""));
  summary.appendChild(stageBox("波动校准样本", calibration.vol_samples || 0, `交易日 ${calibration.vol_days || 0}；按交易日固定种子 bootstrap`, ""));
  const sources = calibration.volatility_sources || {};
  summary.appendChild(stageBox("波动率来源", `${sources.iv30 || 0} / ${sources.market_state || 0} / ${sources.hv20 || 0}`, "IV30 / 市场状态 / HV20", ""));
  for (let conviction = 2; conviction <= 5; conviction += 1) {
    const row = (calibration.by_conviction || {})[String(conviction)] || {};
    body.appendChild(el("tr", {}, [
      el("td", { text: `${conviction}★` }),
      el("td", { className: "num", text: row.samples || 0 }),
      el("td", { className: "num", text: row.days || 0 }),
      el("td", { className: "num", text: row.k === undefined ? "—" : Number(row.k).toFixed(2) }),
      el("td", { className: "num", text: row.sd === undefined ? "—" : Number(row.sd).toFixed(2) }),
      el("td", { className: "num", text: percent(row.hit_rate, 1) }),
    ]));
  }
}

function stageBox(label, value, note, tone) {
  return el("div", { className: "stage" }, [
    el("div", { className: "label", text: label }),
    el("div", { className: `value ${tone || ""}`, text: value }),
    note ? el("div", { className: "note", text: note }) : null,
  ]);
}

function renderRun() {
  const run = appState.run;
  const stages = document.getElementById("stage-list");
  clear(stages);
  const errorNode = document.getElementById("run-error");
  errorNode.classList.add("hidden");
  if (!run) {
    setText("run-meta", "还没有运行记录，点击“立即运行”");
    setText("macro", "暂无");
    return;
  }
  const params = run.stages && run.stages.params ? ` ｜ 参数 ${run.stages.params.version}` : "";
  setText("run-meta", `#${run.id} ｜ ${run.trading_date} ${run.slot === "manual" ? "手动" : run.slot + " ET"} ｜ ${RUN_STATUS[run.status] || run.status} ｜ ${localTime(run.finished_at || run.started_at)}${params}`);
  const s = run.stages || {};
  const news = s.news || [];
  const newsNote = news.map((r) => `${SOURCE_NAMES[r.source] || r.source}: ${r.status === "ok" ? "+" + r.count : r.status}${r.detail ? "（" + r.detail + "）" : ""}`).join("；");
  const newsOk = news.length > 0 && news.every((r) => r.status !== "failed");
  stages.appendChild(stageBox("新闻采集", !news.length ? "未采集" : newsOk ? "正常" : "部分失败", newsNote, newsOk ? "pos" : "neg"));
  const ibkr = s.ibkr || {};
  stages.appendChild(stageBox("IBKR", { ok: "已连接", unavailable: "未连接", failed: "出错", disabled: "未启用" }[ibkr.status] || "—", ibkr.detail || (ibkr.underlyings ? `报价 ${ibkr.underlyings} 个标的` : ""), ibkr.status === "ok" ? "pos" : "neg"));
  const tagging = s.tagging || {};
  const tagUsage = tagging.usage || {};
  const tagNote = [
    tagging.detail,
    tagging.remaining ? `待分类 ${tagging.remaining} 条` : "",
    tagUsage.calls ? `${tagUsage.calls} 次调用，约 ¥${tagUsage.cost_cny}` : "",
    (tagging.errors || []).join("；"),
  ].filter(Boolean).join("；");
  stages.appendChild(stageBox("新闻分类", tagging.status === "skipped" ? "未启用" : `新分类 ${tagging.tagged || 0} 条`, tagNote, tagging.status === "failed" ? "neg" : "pos"));
  const views = s.views || {};
  const changeCount = (views.changes || []).length;
  stages.appendChild(stageBox("观点账本", `${views.open || 0} 个观点`, `依据 ${views.tagged_news || 0} 条已分类新闻；本次变化 ${changeCount} 个`, "pos"));
  const options = s.options || {};
  const reasonCounts = {};
  for (const reason of Object.values(options.skipped || {})) {
    const key = String(reason).split(":")[0];
    reasonCounts[key] = (reasonCounts[key] || 0) + 1;
  }
  const skippedNote = Object.entries(reasonCounts).map(([reason, count]) => `${reason} ×${count}`).join("；");
  const pricingNote = options.pricing === "reference" ? "休市：按收盘参考价估算（标 *）" : options.pricing === "live" ? "实时买卖价" : "";
  const sources = options.sources || {};
  const sourceNote = sources.cboe || sources.ibkr ? `数据：CBOE ${sources.cboe || 0} / IBKR ${sources.ibkr || 0}` : "";
  const live = options.live_check || {};
  const liveNote = live.status === "ok" ? `IBKR 实时核对 ${live.ideas_checked}/${options.ideas} 个推荐` : live.status === "failed" ? "IBKR 实时核对失败" : "";
  const rules = s.rules || {};
  const ladderNote = (rules.ladder || []).map((x) => `${x.symbol} 回撤 ${percent(x.drawdown, 1)}${x.tier ? `（第 ${x.tier} 级）` : ""}`).join("；");
  const anyTier = (rules.ladder || []).some((x) => x.tier > 0);
  stages.appendChild(stageBox("指数回撤阶梯", anyTier ? "已触发" : "未触发", ladderNote || "无历史数据", anyTier ? "pos" : ""));
  const pos = s.positions || {};
  const risk = pos.risk || {};
  const riskNote = risk.net_liquidation ? `持仓最坏亏损 ${money(risk.open_risk)}（净值 ${percent(risk.open_risk_pct, 0)}），组合上限 ${money(risk.cap)}，剩余 ${money(risk.remaining)}` : "";
  if (pos.groups !== undefined) {
    stages.appendChild(stageBox("持仓", `${pos.groups} 组，${pos.action_alerts || 0} 条需处理`, riskNote, pos.action_alerts ? "neg" : "pos"));
  }
  renderPositionAlerts(run);
  const capNote = options.over_cap ? `${options.over_cap} 个最大亏损超过账户上限 ${money(options.loss_cap)} 已过滤` : "";
  stages.appendChild(stageBox("期权分析", options.status === "ok" ? `${(options.symbols || []).length} 个标的 → ${options.ideas || 0} 个推荐` : options.status === "partial" ? "中途断开" : "未执行", [sourceNote, options.pricing === "reference" && sources.cboe ? "休市：CBOE 收盘报价" : pricingNote, liveNote, capNote, options.detail || skippedNote].filter(Boolean).join("；"), options.status === "ok" ? "pos" : "neg"));
  if (run.error) {
    errorNode.textContent = `错误：${run.error}`;
    errorNode.classList.remove("hidden");
  }
  setText("macro", run.macro_summary || "暂无（模型未运行）");
}

function legText(leg) {
  return `${leg.action}${leg.quantity > 1 ? leg.quantity : ""} ${leg.strike}${leg.right}`;
}

function viewLabel(view) {
  if (!view) return el("span", { text: "—" });
  const text = `${DIRECTION_NAMES[view.direction] || view.direction} ${"★".repeat(view.conviction || 0)}`;
  return el("span", { className: view.direction, text, title: view.rationale || "" });
}

function relationshipMarker(idea) {
  const names = { "替代方案": "替代", "冲突": "冲突", "对冲现有持仓": "对冲", "加大现有暴露": "叠加" };
  const labels = [...new Set((idea.relationships || []).map((item) => names[item.type]).filter(Boolean))];
  return labels.length ? `〔${labels.join("/")}〕` : "";
}

function relationshipText(item) {
  if (item.type === "冲突") {
    return `冲突：与 ${item.other} 方向相反；合并 SPY 等效 Delta ${money(item.net_spy_equivalent_delta)}`;
  }
  if (item.type === "替代方案") return `替代方案：与 ${item.other} 只需选择一个`;
  if (item.type === "对冲现有持仓") return `对冲：${item.other}`;
  if (item.type === "加大现有暴露") return `风险叠加：${item.other}`;
  return "";
}

function renderIdeas() {
  const body = document.querySelector("#ideas-table tbody");
  clear(body);
  const empty = document.getElementById("ideas-empty");
  const detail = document.getElementById("idea-detail");
  detail.classList.add("hidden");
  const run = appState.run;
  renderVrpIdeas(run);
  renderWatchlist(run);
  renderPortfolioQuality(run);
  const ideas = run ? run.ideas || [] : [];
  renderDropped(run && run.stages && run.stages.options ? run.stages.options.dropped || [] : []);
  if (!ideas.length) {
    const options = run && run.stages ? run.stages.options || {} : {};
    empty.textContent = !run ? "暂无数据"
      : options.status === "skipped" ? "本次没有可用的期权数据源（CBOE 未启用且 IBKR 未连接）。新闻和观点已更新。"
      : options.status === "partial" ? "IBKR 在分析途中断开（见上方“期权分析”说明），本次推荐不完整；自动调度会在 15 分钟后重试。"
      : "本次没有满足条件（正期望优势、流动性）的组合。没有观点优势时不推荐交易是正常结果。";
    empty.classList.remove("hidden");
    return;
  }
  empty.classList.add("hidden");
  for (const idea of ideas) {
    const expired = ideaExpired(idea);
    const macroEvents = idea.macro_events || [];
    const expiryTitle = macroEvents.length ? `到期前有：${macroEvents.map(macroEventText).join("、")}` : "";
    const row = el("tr", { className: expired ? "quote-expired" : "" }, [
      el("td", { text: idea.rank }),
      el("td", { className: idea.status === "new" ? "status-new" : "", text: expired ? "报价已过期，需重新运行" : idea.status === "new" ? "新增" : idea.status === "kept" ? "保持" : "—" }),
      el("td", { text: `${idea.symbol}${relationshipMarker(idea)}${idea.indicative ? " *" : ""}${idea.origin === "ladder" ? " ⓡ" : ""}${idea.news_unverified ? " ⚠" : ""}`, title: idea.news_unverified ? "新闻观点未经验证" : idea.origin === "ladder" ? "指数回撤阶梯规则" : idea.indicative ? "收盘参考价" : "" }),
      el("td", {}, [viewLabel(idea.view)]),
      el("td", { text: idea.structure_name }),
      el("td", { text: `${idea.expiry.slice(5)}（${idea.dte}天）${macroEvents.length ? " ⚠" : ""}`, title: expiryTitle }),
      el("td", { className: "legs", text: idea.legs.map(legText).join(" / ") }),
      el("td", { className: "num", text: `${idea.net_premium >= 0 ? "付" : "收"} ${money(Math.abs(idea.net_premium))}` }),
      el("td", { className: "num neg", text: money(idea.max_loss) }),
      el("td", { className: "num pos", text: idea.max_profit === null ? "无上限" : money(idea.max_profit) }),
      el("td", { className: "num", text: idea.reward_risk === null ? "∞" : idea.reward_risk.toFixed(2) }),
      el("td", { text: (idea.breakevens || []).join(" / ") || "—" }),
      el("td", { className: "num", text: `${percent(idea.view_pop)} / ${percent(idea.market_pop)}` }),
      el("td", { className: `num ${idea.edge >= 0 ? "pos" : "neg"}`, text: `${money(idea.edge)}（${percent(idea.return_on_risk, 1)}）` }),
      el("td", { className: `num ${idea.expected_return >= 0 ? "pos" : "neg"}`, text: `${money(idea.expected_return)}（${percent(idea.expected_return_pct, 1)}）` }),
      el("td", { className: "num", text: idea.suggested_quantity === null ? "—" : String(idea.suggested_quantity) }),
    ]);
    row.addEventListener("click", () => selectIdea(idea, row));
    body.appendChild(row);
  }
}

function renderPortfolioQuality(run) {
  const snapshots = run ? run.snapshots || {} : {};
  const payload = snapshots.portfolio_quality || {};
  const summary = payload.summary || {};
  const root = document.getElementById("combined-exposure");
  const stressBody = document.querySelector("#stress-table tbody");
  if (!root || !stressBody) return;
  clear(root);
  clear(stressBody);
  for (const [label, row] of [["主推荐", summary.main], ["现有持仓 + 主推荐", summary.existing_plus_main]]) {
    if (!row) continue;
    const conflicts = (row.conflicts || []).map((item) => `${item.left} ↔ ${item.right}`).join("；") || "无冲突";
    root.appendChild(stageBox(
      label,
      `最大亏损 ${money(row.max_loss)}`,
      `SPY 等效 Delta ${money(row.spy_equivalent_delta)}；Vega ${money(row.vega, 2)}；Theta/天 ${money(row.theta, 2)}；入场成本 ${money(row.entry_costs)}；${conflicts}`,
      (row.conflicts || []).length ? "warn" : "",
    ));
  }
  for (const row of payload.stress || []) {
    stressBody.appendChild(el("tr", {}, [
      el("td", { text: row.scenario }),
      el("td", { className: `num ${(row.positions_pnl || 0) >= 0 ? "pos" : "neg"}`, text: money(row.positions_pnl) }),
      el("td", { className: `num ${(row.ideas_pnl || 0) >= 0 ? "pos" : "neg"}`, text: money(row.ideas_pnl) }),
      el("td", { className: `num ${(row.combined_pnl || 0) >= 0 ? "pos" : "neg"}`, text: money(row.combined_pnl) }),
    ]));
  }
}

function renderVrpIdeas(run) {
  const body = document.querySelector("#vrp-ideas-table tbody");
  const conditions = document.getElementById("vrp-conditions");
  if (!body || !conditions) return;
  clear(body);
  clear(conditions);
  const snapshots = run ? run.snapshots || {} : {};
  const payload = snapshots.vrp_ideas || {};
  const ideas = payload.list || [];
  for (const idea of ideas) appendCompactIdeaRow(body, idea);
  document.getElementById("vrp-ideas-empty").classList.toggle("hidden", ideas.length > 0);
  for (const condition of Object.values(payload.conditions || {})) {
    const ivGap = condition.iv_minus_hv20 === null || condition.iv_minus_hv20 === undefined
      ? "IV−HV —"
      : `IV−HV ${percent(condition.iv_minus_hv20, 1)}`;
    const budget = condition.risk_budget === null || condition.risk_budget === undefined
      ? "净值未知，使用最窄宽度"
      : `预算 ${money(condition.risk_budget)}`;
    conditions.appendChild(el("span", {
      className: `tag ${condition.status === "met" ? "pos" : "warn"}`,
      text: `${condition.symbol}：${condition.status === "met" ? "满足" : condition.reason || "未满足"}；${ivGap}；${budget}`,
    }));
  }
}

function appendCompactIdeaRow(body, idea) {
  const macroEvents = idea.macro_events || [];
  const expired = ideaExpired(idea);
  body.appendChild(el("tr", { className: expired ? "quote-expired" : "" }, [
    el("td", { text: idea.rank }),
    el("td", { text: expired ? "报价已过期，需重新运行" : idea.status === "new" ? "新增" : idea.status === "kept" ? "保持" : "—" }),
    el("td", { text: `${idea.symbol}${relationshipMarker(idea)}` }),
    el("td", {}, [viewLabel(idea.view)]),
    el("td", { text: idea.structure_name }),
    el("td", { text: `${idea.expiry.slice(5)}（${idea.dte}天）${macroEvents.length ? " ⚠" : ""}` }),
    el("td", { className: "legs", text: idea.legs.map(legText).join(" / ") }),
    el("td", { className: "num", text: `${idea.net_premium >= 0 ? "付" : "收"} ${money(Math.abs(idea.net_premium))}` }),
    el("td", { className: "num neg", text: money(idea.max_loss) }),
    el("td", { className: "num pos", text: idea.max_profit === null ? "无上限" : money(idea.max_profit) }),
    el("td", { className: "num", text: idea.reward_risk === null ? "∞" : idea.reward_risk.toFixed(2) }),
    el("td", { text: (idea.breakevens || []).join(" / ") || "—" }),
    el("td", { className: "num", text: `${percent(idea.view_pop)} / ${percent(idea.market_pop)}` }),
    el("td", { className: `num ${idea.edge >= 0 ? "pos" : "neg"}`, text: `${money(idea.edge)}（${percent(idea.return_on_risk, 1)}）` }),
    el("td", { className: `num ${idea.expected_return >= 0 ? "pos" : "neg"}`, text: `${money(idea.expected_return)}（${percent(idea.expected_return_pct, 1)}）` }),
    el("td", { className: "num", text: idea.suggested_quantity === null ? "—" : String(idea.suggested_quantity) }),
  ]));
}

function renderWatchlist(run) {
  const body = document.querySelector("#watchlist-table tbody");
  const empty = document.getElementById("watchlist-empty");
  if (!body || !empty) return;
  clear(body);
  const snapshots = run ? run.snapshots || {} : {};
  const entries = snapshots.watchlist ? snapshots.watchlist.list || [] : [];
  for (const entry of entries) {
    const expired = entry.status === "expired";
    body.appendChild(el("tr", { className: expired ? "quote-expired" : "" }, [
      el("td", { text: entry.symbol }),
      el("td", { text: entry.structure_name || entry.structure }),
      el("td", { className: "legs", text: (entry.legs || []).map(legText).join(" / ") }),
      el("td", { className: "num", text: `${entry.current_net_premium >= 0 ? "付" : "收"} ${money(Math.abs(entry.current_net_premium))}` }),
      el("td", { text: entry.trigger }),
      el("td", { text: entry.invalidation }),
      el("td", { text: entry.watch_until }),
      el("td", { text: expired ? "已失效" : entry.reason === "live_price_invalid" ? "实时价格未通过" : entry.reason === "event_blackout" ? "等待事件" : "等待价格" }),
    ]));
  }
  empty.classList.toggle("hidden", entries.length > 0);
}

function ideaExpired(idea) {
  if (!idea || !idea.valid_until) return false;
  const timestamp = new Date(idea.valid_until).getTime();
  return Number.isFinite(timestamp) && Date.now() > timestamp;
}

async function refreshEventStudy() {
  const studyBody = document.querySelector("#event-study-table tbody");
  const ideaBody = document.querySelector("#event-ideas-table tbody");
  clear(studyBody);
  clear(ideaBody);
  const snapshots = appState.run ? appState.run.snapshots || {} : {};
  const ideas = snapshots.event_ideas ? snapshots.event_ideas.list || [] : [];
  for (const idea of ideas) appendCompactIdeaRow(ideaBody, idea);
  document.getElementById("event-ideas-empty").classList.toggle("hidden", ideas.length > 0);
  let payload = snapshots.event_study ? snapshots.event_study.latest : null;
  let fallback = false;
  if (!payload) {
    try { payload = await getJson(ENDPOINTS.eventStudyLatest); } catch (error) { payload = null; }
    fallback = true;
  }
  if (fallback && payload && payload.event) {
    const eventTime = new Date(payload.event.time).getTime();
    const age = Date.now() - eventTime;
    if (!Number.isFinite(eventTime) || age < 0 || age > 3 * 24 * 60 * 60 * 1000) payload = null;
  }
  if (!payload || !payload.event) {
    setText("event-study-head", "暂无近期重大事件测算。");
    setText("event-study-caveat", "");
    document.getElementById("event-study-caveat").classList.add("hidden");
    setText("event-study-thresholds", "");
    await refreshNextEventLearning();
    return;
  }
  const event = payload.event;
  const prefix = fallback ? `${String(event.time_ny || "").slice(0, 5)} 的测算：` : "";
  setText("event-study-head", `${prefix}美国 ${event.name}（${event.time_ny}）实际 ${event.actual ?? "—"} vs 预期 ${event.forecast ?? "—"}，${event.surprise_text}`);
  const caveat = document.getElementById("event-study-caveat");
  setText("event-study-caveat", event.caveat || "");
  caveat.classList.toggle("hidden", !event.caveat);
  const thresholds = payload.thresholds || {};
  const thresholdText = (
    thresholds.min_events === undefined || thresholds.min_t === undefined
      ? ""
      : `门槛：样本 ≥ ${thresholds.min_events}、|t| ≥ ${thresholds.min_t}、前后两半方向一致；滚动观点 ≥ ${thresholds.min_wf_views}、命中率 ≥ ${percent(thresholds.min_wf_hit_rate, 0)}`
  );
  setText("event-study-thresholds", thresholdText);
  for (const row of payload.rows || []) {
    const expected = numberOrNull(row.expected_after_sigma);
    const expectedPct = numberOrNull(row.expected_after_pct);
    studyBody.appendChild(el("tr", { className: row.generated ? "event-study-generated" : "" }, [
      el("td", { text: row.symbol }),
      el("td", { className: "num", text: row.n ?? "—" }),
      el("td", { className: "num", text: numberOrNull(row.beta_shrunk_gap) === null ? "—" : `${Number(row.beta_shrunk_gap).toFixed(2)}σ` }),
      el("td", { className: "num", text: numberOrNull(row.beta_shrunk_after) === null ? "—" : `${Number(row.beta_shrunk_after).toFixed(2)}σ` }),
      el("td", { className: "num", text: numberOrNull(row.t_after) === null ? "—" : Number(row.t_after).toFixed(1) }),
      el("td", { className: "num", text: numberOrNull(row.t_after_first) === null ? "—" : Number(row.t_after_first).toFixed(1) }),
      el("td", { className: "num", text: numberOrNull(row.t_after_second) === null ? "—" : Number(row.t_after_second).toFixed(1) }),
      el("td", { text: row.stable ? "✓" : "✗" }),
      el("td", { className: "num", text: percent(row.wf_view_hit_rate, 0) }),
      el("td", { text: row.wf_pass ? "通过" : "未通过" }),
      el("td", { className: "num", text: expected === null ? "—" : `${expected.toFixed(2)}σ${expectedPct === null ? "" : ` / ${percent(expectedPct, 2)}`}` }),
      el("td", { className: "num", text: numberOrNull(row.abs_move_ratio) === null ? "—" : Number(row.abs_move_ratio).toFixed(2) }),
      el("td", { text: row.generated ? "是" : "否" }),
    ]));
  }
  await refreshNextEventLearning();
}

async function refreshNextEventLearning() {
  let events = [];
  try { events = await getJson(`${ENDPOINTS.majorMacroEvents}?days=30`); } catch (error) { events = []; }
  if (!events.length) {
    setText("event-study-next", "");
    return;
  }
  const next = events[0];
  let stats = [];
  try { stats = await getJson(`${ENDPOINTS.eventStudy}?category=${encodeURIComponent(next.category)}`); } catch (error) { stats = []; }
  const summaries = ["SPY", "QQQ", "TLT"].map((symbol) => {
    const row = stats.find((item) => item.symbol === symbol);
    const beta = row ? numberOrNull(row.beta_shrunk_after) : null;
    return `${symbol} ${beta === null ? "暂无" : `${beta >= 0 ? "+" : ""}${beta.toFixed(2)}σ/σ${row.wf_view_mean > 0 ? "（滚动通过）" : "（滚动未通过）"}`}`;
  });
  setText("event-study-next", `下次公布时将使用：${next.name}；${summaries.join("，")}`);
}

function renderEventRunBanner(eventRun) {
  const banner = document.getElementById("event-run-banner");
  if (!eventRun) {
    banner.classList.add("hidden");
    return;
  }
  const reason = eventRun.reason || {};
  banner.textContent = `最近事件运行 #${eventRun.id}：${reason.title || "事件触发"}；主榜新增 ${eventRun.main_new_ideas}，事件组合 ${eventRun.event_ideas}`;
  banner.classList.remove("hidden");
}

function notifyEventRun(eventRun) {
  if (!eventRun || !("Notification" in window) || Notification.permission !== "granted") return;
  const key = "copilot-last-event-notification";
  if (localStorage.getItem(key) === String(eventRun.id)) return;
  const reason = eventRun.reason || {};
  new Notification("期权副驾：事件运行完成", {
    body: `${reason.title || "重大事件"}；主榜新增 ${eventRun.main_new_ideas}，事件组合 ${eventRun.event_ideas}`,
  });
  localStorage.setItem(key, String(eventRun.id));
}

async function enableNotifications() {
  const button = document.getElementById("notification-button");
  if (!("Notification" in window)) {
    button.textContent = "浏览器不支持提醒";
    button.disabled = true;
    return;
  }
  const permission = await Notification.requestPermission();
  button.textContent = permission === "granted" ? "事件提醒已开启" : "事件提醒未开启";
}

function renderPositionAlerts(run) {
  const list = document.getElementById("position-alerts");
  clear(list);
  const book = run && run.snapshots && run.snapshots.positions ? run.snapshots.positions.book : null;
  const alerts = [];
  for (const group of (book && book.groups) || []) {
    for (const alert of group.alerts || []) {
      alerts.push(el("li", { className: `alert-${alert.level}`, text: `持仓提醒 ${group.symbol}：${alert.text}` }));
    }
  }
  const risk = book && book.risk;
  if (risk && risk.remaining !== null && risk.remaining <= 0) {
    alerts.unshift(el("li", { className: "alert-action", text: `组合风险已满：现有期权持仓最坏亏损 ${money(risk.open_risk)}，超过上限 ${money(risk.cap)}，新组合建议张数均为 0；先处理现有持仓再开新仓。` }));
  }
  for (const item of alerts) list.appendChild(item);
  list.classList.toggle("hidden", alerts.length === 0);
}

function renderDropped(dropped) {
  const box = document.getElementById("ideas-dropped");
  clear(box);
  if (!dropped.length) {
    box.classList.add("hidden");
    return;
  }
  box.appendChild(el("div", { className: "muted", text: "与上次相比移出的组合：" }));
  box.appendChild(el("ul", {}, dropped.map((d) => el("li", {
    text: `${d.symbol} ${d.structure_name} ${d.expiry || ""} ${d.legs} —— ${d.reason}`,
  }))));
  box.classList.remove("hidden");
}

function selectIdea(idea, row) {
  document.querySelectorAll("#ideas-table tbody tr").forEach((node) => node.classList.remove("selected"));
  row.classList.add("selected");
  const detail = document.getElementById("idea-detail");
  clear(detail);
  const view = idea.view || {};
  const tags = [];
  const expired = ideaExpired(idea);
  if (expired) tags.push(el("span", { className: "tag warn", text: "报价已过期，需重新运行" }));
  if (idea.indicative) tags.push(el("span", { className: "tag warn", text: "收盘参考价（IB 模型价±估算价差），开盘后需核实" }));
  else if (idea.data_source === "cboe") tags.push(el("span", { className: "tag warn", text: "CBOE 延迟报价（交易时段约 15 分钟，休市为收盘价）" }));
  else if (idea.delayed_data) tags.push(el("span", { className: "tag warn", text: "非实时报价" }));
  if (idea.live_net_premium !== undefined && idea.live_net_premium !== null) {
    tags.push(el("span", { className: "tag", text: `IBKR 实时估算成交净权利金 ${money(idea.live_net_premium)}（原报价 ${money(idea.net_premium)}）` }));
  }
  if (idea.opening_recheck) tags.push(el("span", { className: "tag warn", text: "开盘后需用实时价复核" }));
  if (idea.origin === "ladder") tags.push(el("span", { className: "tag", text: "指数回撤阶梯规则" }));
  if (idea.news_unverified) tags.push(el("span", { className: "tag warn", text: `新闻观点未经验证：校准系数 k=${Number(idea.calibration_k).toFixed(2)}（基于 ${(appState.calibration || {}).days || 0} 天）` }));
  if (idea.already_holding) tags.push(el("span", { className: "tag warn", text: "已持有该标的" }));
  if (idea.portfolio_ok === false) tags.push(el("span", { className: "tag warn", text: `超出组合剩余风险额度（剩余 ${money(idea.portfolio_remaining)}）` }));
  if (idea.same_direction) tags.push(el("span", { className: "tag warn", text: "与现有持仓同向，风险叠加" }));
  if (idea.exceeds_budget) tags.push(el("span", { className: "tag warn", text: "1 张即超单笔风险预算" }));
  if ((idea.macro_events || []).length) {
    tags.push(el("span", { className: "tag warn", text: `到期前有：${idea.macro_events.map(macroEventText).join("、")}` }));
  }
  for (const warning of idea.assignment_warnings || []) {
    tags.push(el("span", { className: `tag ${warning.level === "warn" ? "warn" : ""}`, text: warning.text }));
  }
  for (const dividend of idea.dividends || []) {
    tags.push(el("span", {
      className: "tag",
      text: `除息 ${dividend.ex_date || "—"}${dividend.estimated ? "（推算）" : ""}`,
    }));
  }
  const limitAmount = Math.abs(Number(idea.limit_net_price || 0) * Number(idea.multiplier || 100));
  const limitText = Number(idea.limit_net_price) >= 0
    ? `最高可接受净支出 ${money(limitAmount)}/组`
    : `最低可接受净收入 ${money(limitAmount)}/组`;
  const validityText = idea.opening_recheck
    ? `开盘后需用实时价复核；有效至 ${localTime(idea.valid_until)}`
    : `报价有效至 ${localTime(idea.valid_until)}`;
  const priced = idea.priced_in || null;
  const pricedText = !priced
    ? ""
    : priced.status !== "ok"
      ? "消息后价格数据不足，未做已计价调整"
      : `消息后已走 ${sigma(priced.moved_sigma)}（${percent(priced.priced_in, 0)}），IV 变化 ${signed(Number(priced.iv_change || 0) * 100, 1)} 点，剩余预期 ${sigma(priced.remaining_drift_sigma)}`;
  const summaryLines = (idea.card_summary || []).map((item) => el("p", {
    className: "idea-summary-line",
    text: `${item.label}：${item.text || "—"}`,
  }));
  const greeks = idea.greeks || {};
  const relationshipLines = (idea.relationships || [])
    .map(relationshipText)
    .filter(Boolean)
    .map((text) => el("p", { className: "muted", text }));
  const left = el("div", {}, [
    summaryLines.length ? el("div", { className: "idea-summary" }, summaryLines) : null,
    el("h3", { text: `${idea.symbol} ${idea.structure_name} ｜ 现价 ${idea.spot}` }),
    el("p", {}, tags),
    relationshipLines.length ? el("div", { className: "idea-relationships" }, relationshipLines) : null,
    el("p", { text: `观点：${DIRECTION_NAMES[view.direction] || "—"}，信心 ${view.conviction || "—"}/5，证据分 ${signed(view.score)}，预期偏移 ${sigma(view.drift_sigma)}，${view.horizon_days || "—"} 天，${VOL_NAMES[view.volatility] || ""}${view.catalyst ? "，催化剂：" + view.catalyst : ""}` }),
    el("p", { text: view.rationale || "（无理由）" }),
    el("p", { className: "muted", text: `预计净收益（保守）${money(idea.expected_return)}（${percent(idea.expected_return_pct, 1)} 最大亏损）；${limitText}；${validityText}` }),
    pricedText ? el("p", { className: "muted", text: pricedText }) : null,
    el("p", { className: "muted", text: `期望优势 ${money(idea.edge)} = 观点分布下价值 − 市场分布下价值 − 滑点 ${money(idea.slippage)} − 手续费 ${money(idea.fees, 2)}；计算采用的预期涨跌 ${percent(idea.drift, 1)}（模型给出的幅度按波动率封顶）。按估算成交价：观点下期望盈亏 ${money(idea.view_ev)}，市场下 ${money(idea.market_ev)}。ATM IV ${percent(idea.atm_iv, 1)}，观点波动率 ${percent(idea.view_sigma, 1)}。${idea.risk_budget ? `单笔风险预算 ${money(idea.risk_budget)}。` : ""}报价时间 ${localTime(idea.quote_time)}` }),
    el("p", { text: `净 Δ ${numberOrNull(greeks.delta_shares) === null ? "—" : Number(greeks.delta_shares).toFixed(2)} 股 / ${money(greeks.delta_dollars)}；Γ ${numberOrNull(greeks.gamma) === null ? "—" : Number(greeks.gamma).toFixed(4)}；ν ${money(greeks.vega, 2)}/波动率点；θ ${money(greeks.theta, 2)}/天` }),
    el("p", { className: "muted", text: idea.greek_summary || "主要收益来源：—" }),
  ]);
  const legTable = el("table", {}, [
    el("thead", {}, [el("tr", {}, ["方向", "合约", "买价", "卖价", "估算成交", "实时买/卖", "IV", "Delta", "未平仓"].map((t) => el("th", { text: t })))]),
    el("tbody", {}, idea.legs.map((leg) => el("tr", {}, [
      el("td", { text: `${leg.action} ×${leg.quantity}` }),
      el("td", { text: `${idea.symbol} ${leg.expiry} ${leg.strike}${leg.right}` }),
      el("td", { className: "num", text: leg.bid ?? "—" }),
      el("td", { className: "num", text: leg.ask ?? "—" }),
      el("td", { className: "num", text: leg.fill }),
      el("td", { className: "num", text: leg.live_bid ? `${leg.live_bid} / ${leg.live_ask}` : "—" }),
      el("td", { className: "num", text: percent(leg.iv, 1) }),
      el("td", { className: "num", text: leg.delta ?? "—" }),
      el("td", { className: "num", text: leg.open_interest ?? "—" }),
    ]))),
  ]);
  left.appendChild(el("div", { className: "table-wrap" }, [legTable]));
  const scenarios = idea.scenarios || [];
  if (scenarios.length) {
    const scenarioTable = el("table", {}, [
      el("thead", {}, [el("tr", {}, ["情景", "标的价", "估值", "盈亏", "% 最大亏损"].map((text) => el("th", { text })))]),
      el("tbody", {}, scenarios.map((item) => el("tr", {}, [
        el("td", { text: item.scenario }),
        el("td", { className: "num", text: money(item.spot, 2) }),
        el("td", { className: "num", text: money(item.valuation) }),
        el("td", { className: `num ${(item.pnl || 0) >= 0 ? "pos" : "neg"}`, text: money(item.pnl) }),
        el("td", { className: "num", text: percent(item.max_loss_pct, 1) }),
      ]))),
    ]);
    left.appendChild(el("details", {}, [
      el("summary", { text: "关键情景重估" }),
      el("div", { className: "table-wrap" }, [scenarioTable]),
    ]));
  }
  if (idea.calendar_sensitivity) {
    const sensitivity = idea.calendar_sensitivity;
    const sensitivityTable = el("table", {}, [
      el("thead", {}, [el("tr", {}, ["远月波动情景", "估值", "盈亏"].map((text) => el("th", { text })))]),
      el("tbody", {}, (sensitivity.rows || []).map((item) => el("tr", {}, [
        el("td", { text: item.scenario }),
        el("td", { className: "num", text: money(item.valuation) }),
        el("td", { className: `num ${(item.pnl || 0) >= 0 ? "pos" : "neg"}`, text: money(item.pnl) }),
      ]))),
    ]);
    left.appendChild(el("details", {}, [
      el("summary", { text: "远月 IV ±3 点时的估值" }),
      el("p", { className: "muted", text: sensitivity.note }),
      el("div", { className: "table-wrap" }, [sensitivityTable]),
    ]));
  }
  const alternatives = idea.alternatives || [];
  if (alternatives.length) {
    const table = el("table", {}, [
      el("thead", {}, [el("tr", {}, ["结构", "保守预期", "POP", "盈亏平衡 σ", "最大盈利", "Vega", "Theta/天", "所需涨跌", "可采用", "说明"].map((text) => el("th", { text })))]),
      el("tbody", {}, alternatives.map((item) => el("tr", {}, [
        el("td", { text: item.structure }),
        el("td", { className: "num", text: money(item.expected_return) }),
        el("td", { className: "num", text: percent(item.pop, 1) }),
        el("td", { className: "num", text: sigma(item.breakeven_sigma) }),
        el("td", { className: "num", text: item.max_profit === null ? "无上限" : money(item.max_profit) }),
        el("td", { className: "num", text: money(item.vega, 2) }),
        el("td", { className: "num", text: money(item.theta, 2) }),
        el("td", { className: "num", text: percent(item.required_move_pct, 1) }),
        el("td", { text: item.admissible ? "是" : "否" }),
        el("td", { text: item.explanation }),
      ]))),
    ]);
    const details = el("details", {}, [
      el("summary", { text: "同一观点的其他做法" }),
      el("div", { className: "table-wrap" }, [table]),
    ]);
    left.appendChild(details);
  }
  const cited = appState.run && appState.run.cited_news ? appState.run.cited_news : {};
  const items = (view.news_ids || []).map((id) => cited[String(id)]).filter(Boolean);
  const right = el("div", {}, [
    el("h3", { text: "引用的新闻" }),
    items.length
      ? el("ol", { className: "cited" }, items.map(newsLine))
      : el("p", { className: "muted", text: "无引用" }),
  ]);
  detail.appendChild(left);
  detail.appendChild(right);
  detail.classList.remove("hidden");
}

function signed(value, digits = 1) {
  const number = numberOrNull(value);
  return number === null ? "—" : `${number >= 0 ? "+" : ""}${number.toFixed(digits)}`;
}

function sigma(value) {
  const number = numberOrNull(value);
  return number === null ? "—" : `${number.toFixed(2)}σ`;
}

const HORIZON_NAMES = Object.freeze({ intraday: "当天", days: "数天", weeks: "数周" });

function impactChips(tag) {
  if (!tag || !(tag.impacts || []).length) return null;
  return el("div", { className: "impacts" }, tag.impacts.map((impact) => {
    const arrow = impact.direction > 0 ? "↑" : impact.direction < 0 ? "↓" : "";
    const vol = impact.vol === "up" ? " 波动↑" : impact.vol === "down" ? " 波动↓" : "";
    const tone = impact.direction > 0 ? "up" : impact.direction < 0 ? "down" : "vol";
    return el("span", {
      className: `impact ${tone}`,
      text: `${impact.symbol} ${arrow}${"●".repeat(impact.magnitude || 1)}${vol}`,
      title: `力度 ${impact.magnitude}/3，${HORIZON_NAMES[impact.horizon] || impact.horizon}`,
    });
  }));
}

function newsLine(item) {
  const tag = item.tag;
  const metaParts = [localTime(item.published_at), SOURCE_NAMES[item.source] || item.source];
  const meta = el("div", { className: "meta" }, [
    tag ? el("span", { className: "badge", text: tag.category_name || tag.category }) : el("span", { className: "badge", text: "未分类" }),
    tag ? el("span", { className: `badge imp${tag.importance}`, text: `重要性 ${tag.importance}` }) : null,
    el("span", { text: metaParts.join(" ｜ ") }),
  ]);
  const summary = tag && tag.summary ? el("div", { className: "summary", text: tag.summary }) : null;
  const titleText = summary ? item.title : item.title;
  const title = item.url && item.url.startsWith("https://")
    ? Object.assign(el("a", { className: summary ? "original" : "", text: titleText }), { href: item.url, target: "_blank", rel: "noopener noreferrer" })
    : el("span", { className: summary ? "original muted" : "", text: titleText });
  return el("li", {}, [meta, summary, title, impactChips(tag)]);
}

function renderViews() {
  refreshViews();
}

function stateText(state) {
  if (!state) return "无";
  return `${DIRECTION_NAMES[state.direction] || state.direction}${state.conviction ? " " + "★".repeat(state.conviction) : ""}${state.volatility && state.volatility !== "neutral" ? " " + VOL_NAMES[state.volatility] : ""}`;
}

async function refreshViews() {
  let data;
  try { data = await getJson(ENDPOINTS.views); } catch (error) { return; }
  const body = document.querySelector("#views-table tbody");
  clear(body);
  document.getElementById("views-empty").classList.toggle("hidden", (data.book || []).length > 0);
  for (const view of data.book || []) {
    body.appendChild(el("tr", {}, [
      el("td", { text: view.symbol }),
      el("td", { className: view.direction, text: DIRECTION_NAMES[view.direction] || view.direction }),
      el("td", { text: "★".repeat(view.conviction || 0) }),
      el("td", { className: `num ${view.score >= 0 ? "pos" : "neg"}`, text: signed(view.score) }),
      el("td", { className: "num", text: signed(view.vol_score) }),
      el("td", { className: "num", text: sigma(view.drift_sigma) }),
      el("td", { className: "num", text: view.horizon_days }),
      el("td", { text: VOL_NAMES[view.volatility] || view.volatility }),
      el("td", { text: view.catalyst || "—" }),
      el("td", { text: localTime(view.opened_at) }),
      el("td", {}, [el("ol", { className: "contributors" }, (view.contributors || []).slice(0, 3).map((c) => el("li", {
        text: `${c.summary}（${Math.abs(c.value) >= 0.05 ? signed(c.value) : "波动" + signed(c.vol)}）`,
      })))]),
    ]));
  }
  const changes = document.querySelector("#view-changes-table tbody");
  clear(changes);
  for (const change of data.changes || []) {
    changes.appendChild(el("tr", {}, [
      el("td", { text: localTime(change.at) }),
      el("td", { text: change.symbol }),
      el("td", { text: ACTION_NAMES[change.action] || change.action }),
      el("td", { text: `${stateText(change.before)} → ${stateText(change.after)}（证据分 ${signed(change.score)}）` }),
      el("td", { text: (change.reasons || []).map((r) => `${r.summary}（${signed(r.value)}）`).join("；") || "证据衰减" }),
    ]));
  }
  await refreshMarketState();
}

function compactNumber(value) {
  const number = numberOrNull(value);
  if (number === null) return "—";
  if (Math.abs(number) >= 1e9) return `$${(number / 1e9).toFixed(2)}B`;
  if (Math.abs(number) >= 1e6) return `$${(number / 1e6).toFixed(2)}M`;
  return money(number, 0);
}

async function refreshMarketState() {
  let data;
  try { data = await getJson(ENDPOINTS.marketState); } catch (error) { return; }
  const body = document.querySelector("#market-state-table tbody");
  clear(body);
  const rows = data.latest || [];
  document.getElementById("market-state-empty").classList.toggle("hidden", rows.length > 0);
  for (const state of rows) {
    body.appendChild(el("tr", {}, [
      el("td", { text: state.symbol }),
      el("td", { className: "num", text: money(state.spot, 2) }),
      el("td", { className: "num", text: percent(state.iv_30d, 1) }),
      el("td", { className: "num", text: percent(state.iv_90d, 1) }),
      el("td", { className: "num", text: percent(state.term_slope, 1) }),
      el("td", { className: "num", text: percent(state.skew_25d, 1) }),
      el("td", { className: "num", text: percent(state.butterfly_25d, 1) }),
      el("td", { className: "num", text: percent(state.iv_minus_hv20, 1) }),
      el("td", { className: "num", text: compactNumber(state.gex) }),
      el("td", { className: "num", text: state.gex_sign > 0 ? "+" : state.gex_sign < 0 ? "−" : "0" }),
      el("td", { className: "num", text: flowLevel(state.call_wall) }),
      el("td", { className: "num", text: flowLevel(state.put_wall) }),
      el("td", { className: "num", text: flowLevel(state.max_pain) }),
      el("td", { text: state.opex_week ? "OPEX 周" : "—" }),
      el("td", { className: "num", text: state.oi_put_call === null ? "—" : Number(state.oi_put_call).toFixed(2) }),
      el("td", { className: "num", text: `${percent(state.own_iv_percentile, 0)}（${state.history_days || 0} 天）` }),
      el("td", { text: localTime(state.updated_at || state.as_of) }),
    ]));
  }
}

function flowLevel(feature) {
  if (!feature || feature.strike === undefined) return "—";
  const distance = feature.distance_pct === null || feature.distance_pct === undefined
    ? ""
    : ` (${signed(Number(feature.distance_pct) * 100, 1)}%)`;
  return `${Number(feature.strike).toFixed(2)}${distance}`;
}

function renderFlowStats() {
  const root = document.getElementById("flow-study-summary");
  if (!root) return;
  clear(root);
  const stats = appState.flowStats || {};
  for (const [key, label] of [["gex", "GEX 与动量"], ["pinning", "到期钉住"], ["walls", "墙位约束"]]) {
    const row = stats[key] || {};
    root.appendChild(stageBox(label, row.verdict || "样本不足", `样本 ${row.n || 0} 天`, row.verdict === "样本不足" ? "warn" : ""));
  }
}

function trackingValue(row) {
  return row.status === "closed" ? row.exit || {} : row.mark || {};
}

function renderTrackingSummary(stats) {
  const summary = document.getElementById("tracking-summary");
  clear(summary);
  summary.appendChild(stageBox("已平仓", `${stats.closed || 0} 笔`, `胜率 ${percent(stats.win_rate, 1)}`, "pos"));
  summary.appendChild(stageBox("累计结果", money(stats.pnl_total), `累计风险 ${money(stats.risk_total)}；收益风险比 ${percent(stats.return_on_risk, 1)}`, (stats.pnl_total || 0) >= 0 ? "pos" : "neg"));
  summary.appendChild(stageBox("预测 vs 实际", percent(stats.predicted_return_on_risk, 1), `实际 ${percent(stats.return_on_risk, 1)}`, ""));
  summary.appendChild(stageBox("持有中", `${stats.open || 0} 笔`, `浮动盈亏 ${money(stats.open_unrealized)}`, (stats.open_unrealized || 0) >= 0 ? "pos" : "neg"));
  setText("tracking-sample", stats.enough_sample ? "样本已达到最低观察门槛。" : `样本不足（已平仓 ${stats.closed || 0} 笔 < ${stats.min_sample || 30} 笔），结论仅供参考`);
  renderOriginQuality(stats);
}

function renderOriginQuality(stats) {
  const body = document.querySelector("#origin-quality-table tbody");
  if (!body) return;
  clear(body);
  const names = { news: "新闻", ladder: "阶梯", event: "事件", macro: "事件", vrp: "VRP" };
  for (const [origin, row] of Object.entries(stats.origin_quality || {})) {
    const attribution = Object.entries(row.attribution || {}).map(([key, value]) => `${key} ${money(value)}`).join("；");
    const excess = Object.entries(row.benchmark_excess || {}).map(([key, value]) => `${key} ${money(value)}`).join("；");
    const interval = row.ci95 || [];
    body.appendChild(el("tr", {}, [
      el("td", { text: names[origin] || origin }),
      el("td", { text: row.verdict }),
      el("td", { className: "num", text: row.count || 0 }),
      el("td", { className: "num", text: percent(row.mean_return_on_risk, 1) }),
      el("td", { text: `${percent(interval[0], 1)} ～ ${percent(interval[1], 1)}` }),
      el("td", { className: "num", text: percent(row.direction_hit_rate, 1) }),
      el("td", { text: `${attribution || "无归因"}${excess ? `；超额：${excess}` : ""}` }),
    ]));
  }
}

function renderTrackingGroups(stats) {
  const grouping = document.getElementById("tracking-group").value;
  const body = document.querySelector("#tracking-groups-table tbody");
  clear(body);
  for (const [name, metrics] of Object.entries(stats[grouping] || {})) {
    body.appendChild(el("tr", {}, [
      el("td", { text: name }),
      el("td", { className: "num", text: metrics.count }),
      el("td", { className: "num", text: percent(metrics.win_rate, 1) }),
      el("td", { className: `num ${(metrics.pnl_total || 0) >= 0 ? "pos" : "neg"}`, text: money(metrics.pnl_total) }),
      el("td", { className: "num", text: percent(metrics.return_on_risk, 1) }),
      el("td", { className: "num", text: percent(metrics.predicted_return_on_risk, 1) }),
    ]));
  }
}

async function refreshTracking() {
  const status = document.getElementById("tracking-status").value;
  let data;
  try { data = await getJson(`${ENDPOINTS.tracking}?status=${status}&limit=1000`); } catch (error) { return; }
  appState.tracking = data;
  renderTrackingSummary(data.stats || {});
  renderTrackingGroups(data.stats || {});
  const body = document.querySelector("#tracking-table tbody");
  clear(body);
  const rows = data.ideas || [];
  document.getElementById("tracking-empty").classList.toggle("hidden", rows.length > 0);
  for (const row of rows) {
    const result = trackingValue(row);
    const tableRow = el("tr", { className: "clickable" }, [
      el("td", { text: localTime(row.entered_at) }),
      el("td", { text: row.symbol }),
      el("td", { text: row.structure_name }),
      el("td", { className: "legs", text: (row.legs || []).map(legText).join(" / ") }),
      el("td", { text: row.origin === "ladder" ? "ⓡ 阶梯" : row.origin === "vrp" ? "VRP" : row.origin === "macro" ? "事件" : "新闻" }),
      el("td", { className: "num", text: `${(row.net_premium || 0) >= 0 ? "付" : "收"} ${money(Math.abs(row.net_premium || 0))}` }),
      el("td", { className: "num neg", text: money(row.max_loss) }),
      el("td", { className: "num", text: percent(row.predicted_return_on_risk, 1) }),
      el("td", { className: `num ${(result.pnl || 0) >= 0 ? "pos" : "neg"}`, text: money(result.pnl) }),
      el("td", { className: "num", text: percent(result.return_on_risk, 1) }),
      el("td", { text: row.status === "open" ? "持有中" : (row.exit && row.exit.text) || "已平仓" }),
      el("td", { text: row.params_version }),
    ]);
    tableRow.addEventListener("click", () => selectTracking(row.id, tableRow));
    body.appendChild(tableRow);
  }
}

async function selectTracking(id, selectedRow) {
  document.querySelectorAll("#tracking-table tbody tr").forEach((row) => row.classList.remove("selected"));
  selectedRow.classList.add("selected");
  let record;
  try { record = await getJson(`${ENDPOINTS.tracking}/${id}`); } catch (error) { return; }
  const detail = document.getElementById("tracking-detail");
  clear(detail);
  const idea = record.entry.idea;
  const state = record.entry.market_state;
  const marks = el("table", {}, [
    el("thead", {}, [el("tr", {}, ["时间", "现价", "估值", "盈亏", "Delta", "Gamma", "Vega", "Theta/天", "剩余天数"].map((text) => el("th", { text })))]),
    el("tbody", {}, (record.marks || []).map((mark) => el("tr", {}, [
      el("td", { text: localTime(mark.marked_at) }),
      el("td", { className: "num", text: money(mark.spot, 2) }),
      el("td", { className: "num", text: money(mark.value, 2) }),
      el("td", { className: `num ${(mark.pnl || 0) >= 0 ? "pos" : "neg"}`, text: money(mark.pnl, 2) }),
      el("td", { className: "num", text: money((mark.greeks || {}).delta_shares, 2) }),
      el("td", { className: "num", text: money((mark.greeks || {}).gamma, 3) }),
      el("td", { className: "num", text: money((mark.greeks || {}).vega, 2) }),
      el("td", { className: "num", text: money((mark.greeks || {}).theta, 2) }),
      el("td", { className: "num", text: mark.dte }),
    ]))),
  ]);
  detail.appendChild(el("div", {}, [
    el("h3", { text: `${idea.symbol} ${idea.structure_name}` }),
    el("p", { className: "muted", text: `入选运行 #${record.entry_run_id}；累计入选 ${record.times_listed} 次；参数 ${record.params_version}` }),
    state ? el("p", { text: `入场市场状态：IV30 ${percent(state.iv_30d, 1)}，IV90 ${percent(state.iv_90d, 1)}，25Δ偏斜 ${percent(state.skew_25d, 1)}，GEX ${compactNumber(state.gex)}` }) : el("p", { className: "muted", text: "入场时没有可用的市场状态。" }),
    el("div", { className: "table-wrap" }, [marks]),
  ]));
  detail.classList.remove("hidden");
}

function renderGroups(snapshots) {
  const body = document.querySelector("#groups-table tbody");
  clear(body);
  const book = snapshots.positions ? snapshots.positions.book : null;
  const groups = book ? book.groups || [] : [];
  document.getElementById("groups-empty").classList.toggle("hidden", groups.length > 0);
  const risk = book && book.risk;
  setText("risk-meta", risk && risk.net_liquidation
    ? `期权持仓最坏亏损 ${money(risk.open_risk)}（净值 ${percent(risk.open_risk_pct, 0)}）；组合上限 ${money(risk.cap)}；剩余额度 ${money(risk.remaining)}${(risk.undefined || []).length ? `；无法确定风险：${risk.undefined.join(",")}` : ""}`
    : "最近一次运行没有账户数据");
  for (const group of groups) {
    const pnlClass = group.pnl === null ? "" : group.pnl >= 0 ? "pos" : "neg";
    body.appendChild(el("tr", {}, [
      el("td", { text: group.symbol }),
      el("td", { className: "legs", text: group.legs.map((leg) => `${leg.quantity > 0 ? "买" : "卖"}${Math.abs(leg.quantity)} ${leg.strike}${leg.right}`).join(" / ") }),
      el("td", { text: group.nearest_expiry ? `${group.nearest_expiry.slice(5)}（${group.dte}天）` : "—" }),
      el("td", { className: "num", text: `${group.kind === "credit" ? "收" : "付"} ${money(Math.abs(group.entry_cost))}` }),
      el("td", { className: "num", text: group.current_value === null ? "—" : money(group.current_value) }),
      el("td", { className: `num ${pnlClass}`, text: group.pnl === null ? "—" : money(group.pnl) }),
      el("td", { className: "num neg", text: group.max_loss === null ? "无法确定" : money(group.max_loss) }),
      el("td", { className: "num pos", text: group.max_profit === null ? "无上限/不定" : money(group.max_profit) }),
      el("td", {}, [(group.alerts || []).length
        ? el("ul", { className: "alert-list" }, group.alerts.map((a) => el("li", { className: `alert-${a.level}`, text: a.text })))
        : el("span", { className: "muted", text: "无" })]),
    ]));
  }
}

function renderPositions() {
  const body = document.querySelector("#positions-table tbody");
  clear(body);
  const snapshots = appState.run ? appState.run.snapshots || {} : {};
  renderGroups(snapshots);
  const account = snapshots.account ? snapshots.account.summary : null;
  if (!account) {
    setText("account-meta", "最近一次运行没有连上 IBKR");
    return;
  }
  setText("account-meta", `净清算价值 ${money(account.net_liquidation)} ${account.currency} ｜ ${localTime(account.observed_at)}`);
  for (const position of account.positions || []) {
    body.appendChild(el("tr", {}, [
      el("td", { text: position.label }),
      el("td", { text: position.sec_type }),
      el("td", { className: "num", text: position.quantity }),
      el("td", { className: "num", text: money(position.avg_cost, 2) }),
    ]));
  }
}

async function refreshRuns() {
  let runs = [];
  try { runs = await getJson(`${ENDPOINTS.runs}?limit=40`); } catch (error) { return; }
  const body = document.querySelector("#runs-table tbody");
  clear(body);
  for (const run of runs) {
    body.appendChild(el("tr", {}, [
      el("td", { text: run.id }),
      el("td", { text: run.trading_date }),
      el("td", { text: run.slot === "manual" ? "手动" : run.slot }),
      el("td", { text: run.trigger === "schedule" ? "自动" : run.trigger === "event" ? "事件" : "手动" }),
      el("td", { className: run.status === "ok" ? "pos" : run.status === "failed" ? "neg" : "", text: RUN_STATUS[run.status] || run.status }),
      el("td", { text: run.ibkr || "—" }),
      el("td", { text: { ledger: "账本", llm: "模型", reused: "复用", fallback: "兜底" }[run.views] || run.views || "—" }),
      el("td", { className: "num", text: run.idea_count ?? "—" }),
      el("td", { text: localTime(run.started_at) }),
      el("td", { className: "neg", text: run.error || "" }),
    ]));
  }
}

async function loadMeta() {
  if (appState.meta) return;
  try { appState.meta = await getJson(ENDPOINTS.meta); } catch (error) { return; }
  const select = document.getElementById("news-category");
  for (const [key, name] of Object.entries(appState.meta.categories || {})) {
    const option = el("option", { text: name });
    option.value = key;
    select.appendChild(option);
  }
}

async function refreshNews() {
  await loadMeta();
  const params = new URLSearchParams({ hours: "24", limit: "300" });
  const source = document.getElementById("news-source").value;
  const importance = document.getElementById("news-importance").value;
  const category = document.getElementById("news-category").value;
  const symbol = document.getElementById("news-symbol").value.trim().toUpperCase();
  if (source) params.set("source", source);
  if (importance !== "0") params.set("min_importance", importance);
  if (category) params.set("category", category);
  if (/^[A-Z]{1,5}$/.test(symbol)) params.set("symbol", symbol);
  const url = `${ENDPOINTS.news}?${params.toString()}`;
  let items = [];
  try { items = await getJson(url); } catch (error) { return; }
  const list = document.getElementById("news-list");
  clear(list);
  if (!items.length) list.appendChild(el("li", { className: "muted", text: "暂无新闻" }));
  for (const item of items) list.appendChild(newsLine(item));
}

async function refreshEarnings() {
  let events = [];
  try { events = await getJson(ENDPOINTS.earnings); } catch (error) { return; }
  const body = document.querySelector("#earnings-table tbody");
  clear(body);
  for (const event of events) {
    body.appendChild(el("tr", {}, [
      el("td", { text: event.date }),
      el("td", { text: event.symbol }),
      el("td", { text: HOUR_NAMES[event.hour] || "未定" }),
      el("td", { className: "num", text: event.eps_estimate ?? "—" }),
    ]));
  }
}

function macroEventText(event) {
  return `${event.name} ${String(event.date || "").slice(5)}`;
}

async function refreshMajorEvents() {
  const banner = document.getElementById("major-events-banner");
  let events = [];
  try { events = await getJson(`${ENDPOINTS.majorMacroEvents}?days=7`); } catch (error) { events = []; }
  banner.textContent = events.length ? `未来 7 天重大事件：${events.map(macroEventText).join(" · ")}` : "";
  banner.classList.toggle("hidden", events.length === 0);
}

async function refreshMacroEvents() {
  const days = document.getElementById("calendar-days").value;
  const importance = document.getElementById("calendar-importance").value;
  const params = new URLSearchParams({ days });
  if (importance !== "major") params.set("min_importance", importance);
  let events = [];
  try { events = await getJson(`${ENDPOINTS.macroEvents}?${params.toString()}`); } catch (error) { return; }
  if (importance === "major") events = events.filter((event) => event.major);
  const body = document.querySelector("#macro-events-table tbody");
  clear(body);
  document.getElementById("macro-events-empty").classList.toggle("hidden", events.length > 0);
  for (const event of events) {
    body.appendChild(el("tr", { className: event.major ? "macro-major" : "" }, [
      el("td", { text: event.time_beijing }),
      el("td", { text: event.time_ny }),
      el("td", { text: event.name, title: event.title }),
      el("td", { text: event.period || "—" }),
      el("td", { text: "★".repeat(event.importance || 0) || "—" }),
      el("td", { text: event.forecast ?? "—" }),
      el("td", { text: event.previous ?? "—" }),
      el("td", { text: event.actual ?? "—" }),
      el("td", { text: ["SPY", "QQQ", "TLT"].map((symbol) => {
        const ratio = numberOrNull((event.abs_move_ratios || {})[symbol]);
        return `${symbol} ${ratio === null ? "—" : ratio.toFixed(2)}`;
      }).join(" / ") }),
      el("td", { text: SOURCE_NAMES[event.source] || event.source }),
    ]));
  }
  refreshIntradayStatus();
}

async function refreshIntradayStatus() {
  let status;
  try { status = await getJson(ENDPOINTS.intradayStatus); } catch (error) { return; }
  const body = document.querySelector("#intraday-status-table tbody");
  clear(body);
  for (const row of status.sources || []) {
    body.appendChild(el("tr", {}, [
      el("td", { text: SOURCE_NAMES[row.source] || row.source }),
      el("td", { className: "num", text: row.symbols ?? 0 }),
      el("td", { className: "num", text: row.days ?? 0 }),
      el("td", { text: row.last_day || "—" }),
      el("td", { text: localTime(row.last_fetch || row.last_job) }),
      el("td", { className: "neg", text: row.last_error || "" }),
    ]));
  }
}

function reactionSigma(item) {
  const value = numberOrNull(item ? item.sigma : null);
  return value === null ? "—" : `${value >= 0 ? "+" : ""}${value.toFixed(2)}σ`;
}

async function refreshEventReactions() {
  const category = document.getElementById("reaction-category").value;
  const symbol = document.getElementById("reaction-symbol").value;
  const params = new URLSearchParams({ category, symbol, limit: "200" });
  let rows = [];
  let stats = [];
  try {
    [rows, stats] = await Promise.all([
      getJson(`${ENDPOINTS.eventReactions}?${params.toString()}`),
      getJson(`${ENDPOINTS.eventStudy}?category=${encodeURIComponent(category)}`),
    ]);
  } catch (error) { return; }
  const body = document.querySelector("#event-reactions-table tbody");
  clear(body);
  const stat = stats.find((row) => row.symbol === symbol);
  setText("reaction-summary", stat
    ? `过去 ${stat.n} 次（滚动检验 ${stat.wf_n || 0} 次可预测）：命中率 ${percent(stat.wf_view_hit_rate, 0)}，按预测方向平均 ${numberOrNull(stat.wf_view_mean) === null ? "—" : `${Number(stat.wf_view_mean) >= 0 ? "+" : ""}${Number(stat.wf_view_mean).toFixed(2)}σ`}；当前${stat.wf_views >= stat.min_wf_views && stat.wf_view_hit_rate >= stat.min_wf_hit_rate && stat.wf_view_mean > 0 ? "满足" : "不满足"}生成推荐门槛`
    : `过去 ${rows.length} 次；当前没有可用统计。`);
  for (const row of rows) {
    const reaction = row.reaction || {};
    const prediction = row.prediction || {};
    const expected = numberOrNull(prediction.expected_after_sigma);
    const realized = reaction.after ? numberOrNull(reaction.after.sigma) : null;
    const hit = expected === null || realized === null ? "—" : expected * realized > 0 ? "是" : "否";
    const ivChange = row.vol ? numberOrNull(row.vol.change) : null;
    body.appendChild(el("tr", {}, [
      el("td", { text: row.event_date }),
      el("td", { text: `${row.actual ?? "—"} / ${row.forecast ?? "—"}` }),
      el("td", { className: "num", text: numberOrNull(row.z) === null ? "—" : `${Number(row.z) >= 0 ? "+" : ""}${Number(row.z).toFixed(2)}σ` }),
      el("td", { className: "num", text: reactionSigma(reaction.gap) }),
      el("td", { className: "num", text: reaction.after ? `${reaction.after_label || "开盘后"} ${reactionSigma(reaction.after)}` : row.status === "partial" ? "等待次日收盘" : "—" }),
      el("td", { className: "num", text: expected === null ? "—" : `${expected >= 0 ? "+" : ""}${expected.toFixed(2)}σ` }),
      el("td", { text: hit }),
      el("td", { className: "num", text: ivChange === null ? "—" : `${ivChange >= 0 ? "+" : ""}${ivChange.toFixed(3)}` }),
    ]));
  }
}

async function refreshCalendar() {
  await Promise.all([refreshMacroEvents(), refreshEventReactions(), refreshEarnings()]);
}

async function triggerRun() {
  const button = document.getElementById("run-button");
  button.disabled = true;
  button.textContent = "运行中…";
  try {
    await getJson(ENDPOINTS.runs, { method: "POST" });
  } catch (error) {
    if (error.status !== 409) button.disabled = false;
  }
  setTimeout(refreshStatus, 1500);
}

function switchTab(name) {
  document.querySelectorAll("#tabs button").forEach((b) => b.classList.toggle("active", b.dataset.tab === name));
  document.querySelectorAll(".tab").forEach((s) => s.classList.toggle("active", s.id === `tab-${name}`));
  if (name === "news") refreshNews();
  if (name === "views") refreshViews();
  if (name === "earnings") refreshCalendar();
  if (name === "tracking") refreshTracking();
  if (name === "runs") refreshRuns();
}

document.getElementById("run-button").addEventListener("click", triggerRun);
document.getElementById("notification-button").addEventListener("click", enableNotifications);
document.getElementById("news-source").addEventListener("change", refreshNews);
document.getElementById("news-importance").addEventListener("change", refreshNews);
document.getElementById("news-category").addEventListener("change", refreshNews);
document.getElementById("news-symbol").addEventListener("change", refreshNews);
document.getElementById("tracking-status").addEventListener("change", refreshTracking);
document.getElementById("tracking-group").addEventListener("change", () => renderTrackingGroups((appState.tracking || {}).stats || {}));
document.getElementById("calendar-importance").addEventListener("change", refreshMacroEvents);
document.getElementById("calendar-days").addEventListener("change", refreshMacroEvents);
document.getElementById("reaction-category").addEventListener("change", refreshEventReactions);
document.getElementById("reaction-symbol").addEventListener("change", refreshEventReactions);
document.querySelectorAll("#tabs button").forEach((button) => button.addEventListener("click", () => switchTab(button.dataset.tab)));

refreshStatus();
setInterval(refreshStatus, STATUS_REFRESH_MS);
