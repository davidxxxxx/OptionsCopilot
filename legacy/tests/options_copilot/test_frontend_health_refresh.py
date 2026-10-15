from __future__ import annotations

import json
from pathlib import Path
import shutil
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[2]
FRONTEND = ROOT / "options_copilot" / "frontend"


def _run_node(source: str) -> object:
    node = shutil.which("node")
    if node is None:
        pytest.skip("node is not installed")
    result = subprocess.run(
        [node, "--input-type=module", "--eval", source],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=15,
    )
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def test_periodic_health_refresh_publishes_diagnostic_cache_without_control_side_effects() -> None:
    script_path = json.dumps(str((FRONTEND / "app.js").resolve()))
    source = rf'''import fs from "node:fs";
import vm from "node:vm";

const source = fs.readFileSync({script_path}, "utf8")
  .replace(/export\s*\{{[\s\S]*?\}};/, "");
const context = vm.createContext({{
  console,
  Date,
  Intl,
  Map,
  Set,
  AbortController,
  Promise,
  document: {{
    hidden: false,
    addEventListener() {{}},
    getElementById() {{ return null; }},
  }},
  window: {{ setInterval() {{}} }},
}});
vm.runInContext(source, context);

const result = await vm.runInContext(`(async () => {{
  const staleHealth = {{ marker: "initial-unavailable" }};
  const currentHealth = {{
    marker: "current-manifest",
    dependencies: {{ production_scanner: {{ daily_operations: {{
      day_manifest: {{ manifest_hash: "manifest-current" }},
      runs: [{{ operation: "TOP10_REPRICE", status: "PENDING", scheduled_at: "2099-09-09T13:35:00Z" }}],
    }} }} }},
  }};
  const completedHealth = {{
    marker: "completed-slot",
    dependencies: {{ production_scanner: {{ daily_operations: {{
      day_manifest: {{ manifest_hash: "manifest-completed" }},
      runs: [
        {{ operation: "TOP10_REPRICE", status: "COMPLETED", scheduled_at: "2099-09-09T13:35:00Z" }},
        {{ operation: "AFTER_HOURS_DISCOVERY", status: "PENDING", scheduled_at: "2099-09-09T20:30:00Z" }},
      ],
    }} }} }},
  }};
  const healthOutcomes = [currentHealth, completedHealth, new Error("health unavailable")];
  const requests = [];
  const brokerHealthRenders = [];
  const dailyRenders = [];
  const overviewHealthRenders = [];
  const upcomingHealthRenders = [];
  const scanHealthRenders = [];

  appState.healthSnapshot = staleHealth;
  appState.controlContext = {{
    ...appState.controlContext,
    brokerState: "FRESH",
    lastControlPollAtMs: 111,
    lastSuccessfulControlAtMs: 110,
    lastControlPollSucceeded: true,
    controlFailureReason: null,
  }};
  const controlBefore = JSON.stringify(appState.controlContext);

  fetchResearchJson = async (endpoint, options = {{}}) => {{
    requests.push({{ endpoint, method: options.method || "GET" }});
    return {{}};
  }};
  fetchResearchTop10 = async () => ({{}});
  fetchAfterHoursIndicative = async () => ({{}});
  refreshLearningSnapshot = async () => true;
  fetchJson = async (endpoint, options = {{}}) => {{
    requests.push({{ endpoint, method: options.method || "GET" }});
    if (endpoint === ENDPOINTS.scans) return {{ decision: "NO_TRADE" }};
    if (endpoint !== ENDPOINTS.health) throw new Error("unexpected endpoint " + endpoint);
    const outcome = healthOutcomes.shift();
    if (outcome instanceof Error) throw outcome;
    return outcome;
  }};
  renderNews = () => {{}};
  renderCalendar = () => {{}};
  renderResearchTop10 = () => {{}};
  renderResearchTop10Unavailable = () => {{}};
  renderWeeklyBrief = () => {{}};
  renderAfterHoursIndicative = () => {{}};
  renderFundamentals = () => {{}};
  renderNewsBrokerHealth = (health) => brokerHealthRenders.push(health?.marker || "UNAVAILABLE");
  renderOverviewPriority = () => overviewHealthRenders.push(appState.healthSnapshot?.marker || "UNAVAILABLE");
  renderDailyFunnel = (health) => dailyRenders.push({{
    marker: health?.marker || "UNAVAILABLE",
    manifest: health?.dependencies?.production_scanner?.daily_operations?.day_manifest?.manifest_hash || "UNAVAILABLE",
  }});
  renderUpcomingExactSlots = (slots) => upcomingHealthRenders.push({{
    marker: appState.healthSnapshot?.marker || "UNAVAILABLE",
    operations: slots.map((item) => item.operation),
  }});
  renderReadiness = (_readiness, _scan, _ranking, health) => scanHealthRenders.push(health?.marker || "UNAVAILABLE");

  await refreshNewsData();
  const afterCurrent = appState.healthSnapshot?.marker || "UNAVAILABLE";
  await refreshScanSnapshot();
  const afterScan = appState.healthSnapshot?.marker || "UNAVAILABLE";
  await refreshNewsData();
  const afterCompleted = appState.healthSnapshot?.marker || "UNAVAILABLE";
  await refreshNewsData();
  const afterFailure = appState.healthSnapshot?.marker || "UNAVAILABLE";

  return {{
    afterCurrent,
    afterScan,
    afterCompleted,
    afterFailure,
    brokerHealthRenders,
    dailyRenders,
    overviewHealthRenders,
    upcomingHealthRenders,
    scanHealthRenders,
    controlUnchanged: JSON.stringify(appState.controlContext) === controlBefore,
    postCount: requests.filter((item) => item.method !== "GET").length,
    brokerRequestCount: requests.filter((item) => item.endpoint.includes("broker")).length,
    healthRequestCount: requests.filter((item) => item.endpoint === ENDPOINTS.health).length,
  }};
}})()`, context);
console.log(JSON.stringify(result));'''

    assert _run_node(source) == {
        "afterCurrent": "current-manifest",
        "afterScan": "current-manifest",
        "afterCompleted": "completed-slot",
        "afterFailure": "UNAVAILABLE",
        "brokerHealthRenders": [
            "current-manifest",
            "completed-slot",
            "UNAVAILABLE",
        ],
        "dailyRenders": [
            {"marker": "current-manifest", "manifest": "manifest-current"},
            {"marker": "completed-slot", "manifest": "manifest-completed"},
            {"marker": "UNAVAILABLE", "manifest": "UNAVAILABLE"},
        ],
        "overviewHealthRenders": [
            "current-manifest",
            "completed-slot",
            "UNAVAILABLE",
        ],
        "upcomingHealthRenders": [
            {"marker": "current-manifest", "operations": ["TOP10_REPRICE"]},
            {"marker": "completed-slot", "operations": ["AFTER_HOURS_DISCOVERY"]},
            {"marker": "UNAVAILABLE", "operations": []},
        ],
        "scanHealthRenders": ["current-manifest"],
        "controlUnchanged": True,
        "postCount": 0,
        "brokerRequestCount": 0,
        "healthRequestCount": 3,
    }


def test_periodic_health_refresh_updates_real_daily_and_next_window_surfaces() -> None:
    script_path = json.dumps(str((FRONTEND / "app.js").resolve()))
    source = rf'''import fs from "node:fs";
import vm from "node:vm";

const RealDate = Date;
const fixedNow = "2026-09-09T14:30:00Z";
class FixedDate extends RealDate {{
  constructor(...args) {{
    super(...(args.length === 0 ? [fixedNow] : args));
  }}
  static now() {{ return RealDate.parse(fixedNow); }}
  static parse(value) {{ return RealDate.parse(value); }}
  static UTC(...args) {{ return RealDate.UTC(...args); }}
}}

class FakeClassList {{
  add() {{}}
  remove() {{}}
  toggle() {{}}
}}
const nodes = new Map();
function makeNode(tagName = "div", id = "") {{
  return {{
    id,
    tagName,
    textContent: "",
    hidden: false,
    disabled: false,
    dataset: {{}},
    classList: new FakeClassList(),
    children: [],
    append(...children) {{ this.children.push(...children); }},
    replaceChildren(...children) {{ this.children = children; }},
    closest() {{ return {{ dataset: {{}} }}; }},
    querySelector() {{ return null; }},
    setAttribute() {{}},
    removeAttribute() {{}},
  }};
}}
const document = {{
  hidden: false,
  addEventListener() {{}},
  createElement(tagName) {{ return makeNode(tagName); }},
  getElementById(id) {{
    if (!nodes.has(id)) nodes.set(id, makeNode("div", id));
    return nodes.get(id);
  }},
}};
const appSource = fs.readFileSync({script_path}, "utf8")
  .replace(/export\s*\{{[\s\S]*?\}};/, "");
const context = vm.createContext({{
  console,
  Date: FixedDate,
  Intl,
  Map,
  Set,
  AbortController,
  Promise,
  document,
  window: {{ setInterval() {{}} }},
}});
vm.runInContext(appSource, context);

const result = await vm.runInContext(`(async () => {{
  const operations = [
    "RESEARCH_REFRESH",
    "TOP10_FREEZE",
    "ORDINARY_SCAN",
    "TOP10_REPRICE",
    "AFTER_HOURS_DISCOVERY",
    "AFTER_HOURS_REPRICE",
    "NEXT_SESSION_PREPARATION",
    "ORDINARY_SCAN",
    "RESEARCH_REFRESH",
    "TOP10_FREEZE",
    "TOP10_REPRICE",
  ];
  const times = [
    "2026-09-09T12:00:00Z",
    "2026-09-09T12:20:00Z",
    "2026-09-09T12:35:00Z",
    "2026-09-09T13:35:00Z",
    "2026-09-09T15:30:00Z",
    "2026-09-09T15:45:00Z",
    "2026-09-09T16:00:00Z",
    "2026-09-09T16:15:00Z",
    "2026-09-09T16:30:00Z",
    "2026-09-09T16:45:00Z",
    "2026-09-09T17:00:00Z",
  ];
  const makeRuns = (terminalCount) => operations.map((operation, index) => ({{
    slot_key: "2026-09-09:" + index,
    operation,
    scheduled_at: times[index],
    status: index < terminalCount ? "COMPLETED" : "PENDING",
    producer_status: index < terminalCount ? "OPEN_REPRICED" : null,
    producer_written_count: index < terminalCount ? 10 : null,
    reason_codes: [],
  }}));
  const health = (marker, manifestHash, terminalCount) => ({{
    marker,
    dependencies: {{
      production_scanner: {{
        connected: true,
        daily_operations: {{
          day_manifest: {{ manifest_hash: manifestHash }},
          today: {{ market_status: "TRADING_SESSION", next_trading_date: "2026-09-10" }},
          runs: makeRuns(terminalCount),
        }},
      }},
    }},
  }});
  const healthOutcomes = [
    health("four-terminal", "a".repeat(64), 4),
    health("five-terminal", "b".repeat(64), 5),
    new Error("health unavailable"),
  ];
  const requests = [];
  const controlStates = [];

  appState.bootstrapSnapshot = {{
    ibkr: {{
      status: "CURRENT",
      connected: true,
      reconciled: true,
      observed_at: "2026-09-09T14:30:00Z",
    }},
  }};
  appState.controlContext = {{
    ...appState.controlContext,
    brokerState: "FRESH",
    lastControlPollAtMs: 444,
    lastSuccessfulControlAtMs: 443,
    lastControlPollSucceeded: true,
    controlFailureReason: null,
  }};
  const controlBefore = JSON.stringify(appState.controlContext);
  const capture = () => ({{
    marker: appState.healthSnapshot?.marker || "UNAVAILABLE",
    daily: document.getElementById("daily-funnel-status").textContent,
    upcoming: document.getElementById("upcoming-exact-slots").textContent,
    nextState: document.getElementById("overview-next-state").textContent,
    nextSummary: document.getElementById("overview-next-summary").textContent,
  }});
  document.getElementById("daily-funnel-status").textContent = "UNAVAILABLE";
  document.getElementById("upcoming-exact-slots").textContent = "UNAVAILABLE";
  document.getElementById("overview-next-state").textContent = "UNAVAILABLE";
  document.getElementById("overview-next-summary").textContent = "UNAVAILABLE";
  const states = [capture()];

  fetchResearchJson = async (endpoint, options = {{}}) => {{
    requests.push({{ endpoint, method: options.method || "GET" }});
    return {{}};
  }};
  fetchResearchTop10 = async () => ({{}});
  fetchAfterHoursIndicative = async () => ({{}});
  refreshLearningSnapshot = async () => true;
  fetchJson = async (endpoint, options = {{}}) => {{
    requests.push({{ endpoint, method: options.method || "GET" }});
    if (endpoint !== ENDPOINTS.health) throw new Error("unexpected endpoint " + endpoint);
    const outcome = healthOutcomes.shift();
    if (outcome instanceof Error) throw outcome;
    return outcome;
  }};
  renderNews = () => {{}};
  renderCalendar = () => {{}};
  renderResearchTop10 = () => {{}};
  renderResearchTop10Unavailable = () => {{}};
  renderWeeklyBrief = () => {{}};
  renderAfterHoursIndicative = () => {{}};
  renderFundamentals = () => {{}};
  renderNewsBrokerHealth = () => {{}};
  renderReadiness = () => {{ throw new Error("diagnostic refresh called renderReadiness"); }};
  renderBroker = () => {{ throw new Error("diagnostic refresh called renderBroker"); }};
  markControlSnapshotSucceeded = () => {{ throw new Error("diagnostic refresh marked control fresh"); }};
  synchronizeActionControls = () => {{ throw new Error("diagnostic refresh synchronized actions"); }};

  for (let index = 0; index < 3; index += 1) {{
    await refreshNewsData();
    states.push(capture());
    controlStates.push(JSON.stringify(appState.controlContext));
  }}
  publishDiagnosticHealth({{
    marker: "empty-durable-schedule",
    dependencies: {{ production_scanner: {{
      connected: true,
      daily_operations: {{
        day_manifest: {{ manifest_hash: "c".repeat(64) }},
        today: {{ market_status: "TRADING_SESSION", next_trading_date: "2026-09-10" }},
        runs: [],
      }},
    }} }},
  }});
  states.push(capture());
  publishDiagnosticHealth({{
    marker: "non-array-schedule",
    dependencies: {{ production_scanner: {{ connected: true, daily_operations: {{ runs: {{}} }} }} }},
  }});
  states.push(capture());
  publishDiagnosticHealth({{
    marker: "missing-schedule",
    dependencies: {{ production_scanner: {{ connected: true }} }},
  }});
  states.push(capture());
  appState.bootstrapSnapshot = {{
    ibkr: {{ status: "CURRENT", connected: false, reconciled: false, observed_at: "2026-09-09T14:30:00Z" }},
  }};
  publishDiagnosticHealth({{
    marker: "broker-blocked-missing-schedule",
    dependencies: {{ production_scanner: {{ connected: false }} }},
  }});
  states.push(capture());
  return {{
    states,
    controlUnchanged: controlStates.every((item) => item === controlBefore),
    methods: [...new Set(requests.map((item) => item.method))],
    healthRequestCount: requests.filter((item) => item.endpoint === ENDPOINTS.health).length,
  }};
}})()`, context);
console.log(JSON.stringify(result));'''

    payload = _run_node(source)
    states = payload["states"]

    assert states[0] == {
        "marker": "UNAVAILABLE",
        "daily": "UNAVAILABLE",
        "upcoming": "UNAVAILABLE",
        "nextState": "UNAVAILABLE",
        "nextSummary": "UNAVAILABLE",
    }
    assert states[1]["marker"] == "four-terminal"
    assert states[1]["daily"] == "4/11 TERMINAL · 0 FAILED/MISSED · DURABLE"
    assert "23:30" in states[1]["upcoming"]
    assert "23:30" in states[1]["nextState"]
    assert "AFTER_HOURS_DISCOVERY" in states[1]["nextSummary"]
    assert states[2]["marker"] == "five-terminal"
    assert states[2]["daily"] == "5/11 TERMINAL · 0 FAILED/MISSED · DURABLE"
    assert "23:45" in states[2]["upcoming"]
    assert "23:45" in states[2]["nextState"]
    assert "AFTER_HOURS_REPRICE" in states[2]["nextSummary"]
    assert states[3]["marker"] == "UNAVAILABLE"
    assert states[3]["daily"] == "UNAVAILABLE"
    assert "health 未提供未来 PENDING" in states[3]["upcoming"]
    assert states[3]["nextState"] == "日程诊断不可用"
    assert "08:30 ET" not in states[3]["nextSummary"]
    assert "09:35 ET" not in states[3]["nextSummary"]
    assert "下个美股交易日" not in states[3]["nextSummary"]
    assert "AFTER_HOURS_REPRICE" not in states[3]["nextSummary"]
    assert states[4]["daily"] == "0 OPERATIONS DUE · DURABLE"
    assert states[4]["nextState"] == "下个美股交易日"
    assert "08:30 ET" in states[4]["nextSummary"]
    assert states[5]["daily"] == "UNAVAILABLE"
    assert states[5]["nextState"] == "日程诊断不可用"
    assert states[6]["daily"] == "UNAVAILABLE"
    assert states[6]["nextState"] == "日程诊断不可用"
    assert states[7]["nextState"] == "IBKR 未连接"
    assert "只读会话未连接" in states[7]["nextSummary"]
    assert payload["controlUnchanged"] is True
    assert payload["methods"] == ["GET"]
    assert payload["healthRequestCount"] == 3


def test_slow_news_health_cannot_overwrite_newer_published_health() -> None:
    script_path = json.dumps(str((FRONTEND / "app.js").resolve()))
    source = rf'''import fs from "node:fs";
import vm from "node:vm";

const appSource = fs.readFileSync({script_path}, "utf8")
  .replace(/export\s*\{{[\s\S]*?\}};/, "");
const context = vm.createContext({{
  console,
  Date,
  Intl,
  Map,
  Set,
  AbortController,
  Promise,
  document: {{
    hidden: false,
    addEventListener() {{}},
    getElementById() {{ return null; }},
  }},
  window: {{ setInterval() {{}} }},
}});
vm.runInContext(appSource, context);

const result = await vm.runInContext(`(async () => {{
  const deferred = () => {{
    let resolve;
    let reject;
    const promise = new Promise((resolvePromise, rejectPromise) => {{
      resolve = resolvePromise;
      reject = rejectPromise;
    }});
    return {{ promise, resolve, reject }};
  }};
  const healthReads = [];
  const controlsBefore = JSON.stringify(appState.controlContext);

  fetchResearchJson = async () => ({{}});
  fetchResearchTop10 = async () => ({{}});
  fetchAfterHoursIndicative = async () => ({{}});
  refreshLearningSnapshot = async () => true;
  fetchJson = async (endpoint) => {{
    if (endpoint === ENDPOINTS.scans) return {{ decision: "NO_TRADE" }};
    if (endpoint !== ENDPOINTS.health) throw new Error("unexpected endpoint " + endpoint);
    return healthReads.shift().promise;
  }};
  renderNews = () => {{}};
  renderCalendar = () => {{}};
  renderResearchTop10 = () => {{}};
  renderResearchTop10Unavailable = () => {{}};
  renderWeeklyBrief = () => {{}};
  renderAfterHoursIndicative = () => {{}};
  renderFundamentals = () => {{}};
  renderNewsBrokerHealth = () => {{}};
  renderOverviewPriority = () => {{}};
  renderDailyFunnel = () => {{}};
  renderUpcomingExactSlots = () => {{}};
  renderReadiness = (_readiness, _scan, _ranking, health) => {{
    appState.healthSnapshot = health;
  }};

  appState.healthSnapshot = {{ marker: "initial" }};
  const sameReference = deferred();
  healthReads.push(sameReference);
  const sameReferenceRefresh = refreshNewsData();
  await Promise.resolve();
  await refreshScanSnapshot();
  sameReference.resolve({{ marker: "published-after-same-reference-scan" }});
  await sameReferenceRefresh;
  const afterSameReferenceScan = appState.healthSnapshot.marker;

  const oldSuccess = deferred();
  healthReads.push(oldSuccess);
  const successRefresh = refreshNewsData();
  await Promise.resolve();
  publishDiagnosticHealth({{ marker: "newer-success" }});
  oldSuccess.resolve({{ marker: "older-success" }});
  await successRefresh;
  const afterOldSuccess = appState.healthSnapshot.marker;

  const oldFailure = deferred();
  healthReads.push(oldFailure);
  const failureRefresh = refreshNewsData();
  await Promise.resolve();
  publishDiagnosticHealth({{ marker: "newer-failure" }});
  oldFailure.reject(new Error("older health failed"));
  await failureRefresh;
  const afterOldFailure = appState.healthSnapshot.marker;

  return {{
    afterSameReferenceScan,
    afterOldSuccess,
    afterOldFailure,
    controlUnchanged: JSON.stringify(appState.controlContext) === controlsBefore,
  }};
}})()`, context);
console.log(JSON.stringify(result));'''

    assert _run_node(source) == {
        "afterSameReferenceScan": "published-after-same-reference-scan",
        "afterOldSuccess": "newer-success",
        "afterOldFailure": "newer-failure",
        "controlUnchanged": True,
    }
