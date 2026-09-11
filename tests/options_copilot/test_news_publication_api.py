"""Public news acquisition diagnostics stay bounded and non-authoritative."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
import shutil
import subprocess

import pytest

from options_copilot.api.app import OptionsCopilotServices, create_app


ASOF = "2026-09-09T01:14:28+00:00"
OBSERVED = "2026-09-09T01:18:22+00:00"
EVALUATED = "2026-09-09T01:19:00+00:00"


def _public_feed(raw: dict[str, object], key: str = "news") -> dict[str, object]:
    services = OptionsCopilotServices(
        health_provider=lambda: {},
        bootstrap_provider=lambda: {},
        candidates_provider=lambda: [],
        positions_provider=lambda: [],
        learning_provider=lambda: {},
        approval_handler=None,
        approval_status_provider=None,
        **{f"{key}_provider": lambda: raw},
    )
    app = create_app(services)
    endpoint = next(route.endpoint for route in app.routes if route.path == f"/api/{key}")
    return asyncio.run(endpoint())


def _diagnostic_payload(key: str = "news") -> dict[str, object]:
    return {
        key: [],
        "asof": ASOF,
        "source_status_scope": "LIVE_ACQUISITION_DIAGNOSTIC",
        "source_status_observed_at": OBSERVED,
        "source_status_evaluated_at": EVALUATED,
        "read_model_published_at": "2026-09-09T01:17:00+00:00",
        "refresh_progress": {
            "status": "RUNNING",
            "stage": "READ_MODEL_REBUILD",
            "cycle_started_at": "2026-09-09T01:18:13+00:00",
            "cycle_completed_at": None,
            "stage_started_at": OBSERVED,
            "elapsed_ms": 47000.5,
            "stage_elapsed_ms": 38000,
            "stage_durations_ms": {"NEWS_PROVIDERS": 8500.5},
            "read_model_asof": ASOF,
            "api_key": "must-not-leak",
            "order_creation_allowed": True,
        },
    }


@pytest.mark.parametrize("key", ["news", "calendar"])
def test_public_feed_separates_acquisition_clock_from_frozen_model(key: str) -> None:
    result = _public_feed(_diagnostic_payload(key), key)
    assert result["asof"] == ASOF
    assert result["source_status_scope"] == "LIVE_ACQUISITION_DIAGNOSTIC"
    assert result["source_status_observed_at"] == OBSERVED
    assert result["source_status_evaluated_at"] == EVALUATED
    assert result["read_model_published_at"] == "2026-09-09T01:17:00+00:00"
    progress = result["refresh_progress"]
    assert progress["status"] == "RUNNING"
    assert progress["stage"] == "READ_MODEL_REBUILD"
    assert progress["read_model_asof"] == result["asof"]
    assert progress["stage_durations_ms"] == {"NEWS_PROVIDERS": 8500.5}
    assert "must-not-leak" not in json.dumps(result)
    assert "order_creation_allowed" not in progress


def test_public_progress_rejects_unknown_labels_and_invalid_durations() -> None:
    raw = _diagnostic_payload()
    raw["refresh_progress"] = {
        "status": "secret-token",
        "stage": "https://secret.example/api?key=private",
        "elapsed_ms": True,
        "stage_elapsed_ms": float("inf"),
        "stage_durations_ms": {
            "NEWS_PROVIDERS": -1,
            "NEWS_APPEND": float("nan"),
            "READ_MODEL_REBUILD": 1.5,
            "secret-token": 45,
        },
    }
    result = _public_feed(raw)
    assert result["refresh_progress"]["status"] == "UNAVAILABLE"
    assert result["refresh_progress"]["stage"] == "IDLE"
    assert result["refresh_progress"]["elapsed_ms"] is None
    assert result["refresh_progress"]["stage_elapsed_ms"] is None
    assert result["refresh_progress"]["stage_durations_ms"] == {"READ_MODEL_REBUILD": 1.5}
    assert "secret" not in json.dumps(result)


def test_unknown_diagnostic_scope_cannot_launder_authority() -> None:
    raw = _diagnostic_payload()
    raw["source_status_scope"] = "PRODUCTION_VERIFIED"
    result = _public_feed(raw)
    assert "source_status_scope" not in result
    assert "refresh_progress" not in result
    assert "read_model_published_at" not in result


def test_public_progress_rejects_integers_too_large_for_float_conversion() -> None:
    raw = _diagnostic_payload()
    raw["refresh_progress"].update({
        "elapsed_ms": 10**10000,
        "stage_elapsed_ms": 10**10000,
        "stage_durations_ms": {"NEWS_APPEND": 10**10000},
    })
    progress = _public_feed(raw)["refresh_progress"]
    assert progress["elapsed_ms"] is None
    assert progress["stage_elapsed_ms"] is None
    assert progress["stage_durations_ms"] == {}


@pytest.mark.parametrize("reason", ["SOURCE_STATUS_STALE", "SOURCE_STATUS_CLOCK_REGRESSED"])
def test_public_source_health_retains_explicit_freshness_failure(reason: str) -> None:
    raw = _diagnostic_payload()
    raw["source_health"] = [{
        "source": "SEC", "source_kind": "NEWS", "status": "DEGRADED",
        "reason": reason, "success_count": 50, "failure_date_count": 0,
        "asof": ASOF,
    }]
    result = _public_feed(raw)
    assert result["source_health"][0]["reason"] == reason
    assert result["source_health"][0]["decision_authority"] == "SUPPORTING_ONLY"


def test_frontend_publication_diagnostics_show_both_clocks_without_actions() -> None:
    node = shutil.which("node")
    if node is None:
        pytest.skip("node is not installed")
    frontend = Path(__file__).resolve().parents[2] / "options_copilot" / "frontend"
    script_uri = (frontend / "app.js").as_uri()
    source = f'''const nodes = new Map();
globalThis.document = {{
  addEventListener() {{}},
  getElementById(id) {{
    if (!nodes.has(id)) nodes.set(id, {{textContent: ""}});
    return nodes.get(id);
  }},
}};
const {{renderNewsPublication}} = await import("{script_uri}");
renderNewsPublication({json.dumps(_diagnostic_payload())});
const valid = nodes.get("news-publication-status").textContent;
renderNewsPublication({{}});
console.log(JSON.stringify({{valid, unavailable: nodes.get("news-publication-status").textContent}}));'''
    completed = subprocess.run(
        [node, "--input-type=module", "--eval", source],
        check=True, capture_output=True, text=True, encoding="utf-8", timeout=20,
    )
    result = json.loads(completed.stdout)
    assert "最新采集状态" in result["valid"]
    assert "决策数据截止" in result["valid"]
    assert "发布完成" in result["valid"]
    assert "重建读模型" in result["valid"]
    assert "不代表交易就绪" in result["valid"]
    assert "不可用" in result["unavailable"]
    assert "news-publication-status" in (frontend / "index.html").read_text(encoding="utf-8")


def test_frontend_expires_source_success_without_another_network_response() -> None:
    node = shutil.which("node")
    if node is None:
        pytest.skip("node is not installed")
    frontend = Path(__file__).resolve().parents[2] / "options_copilot" / "frontend" / "app.js"
    source = f'''const nodes = new Map();
globalThis.document = {{addEventListener() {{}}, getElementById(id) {{
  if (!nodes.has(id)) nodes.set(id, {{textContent: "", classList: {{remove() {{}}, add() {{}}}}}});
  return nodes.get(id);
}}}};
const {{renderNewsSourceHealth}} = await import("{frontend.as_uri()}");
const health = [{{source: "SEC", source_kind: "NEWS", status: "READY", success_count: 50, failure_date_count: 0}}];
const cadence = [{{source_id: "SEC", source_kind: "NEWS", freshness: "CURRENT", cadence_status: "WAITING", interval_seconds: 90, last_success: "2026-09-09T01:00:00Z", last_attempt: "2026-09-09T01:00:00Z", next_due: "2026-09-09T01:01:30Z"}}];
const at = (time) => {{renderNewsSourceHealth(health, cadence, Date.parse(time)); return nodes.get("sec-source-health").textContent;}};
console.log(JSON.stringify({{
  boundary: at("2026-09-09T01:01:30Z"),
  expired: at("2026-09-09T01:01:30.001Z"),
  backwards: at("2026-09-09T00:59:59Z"),
  original: cadence[0].freshness,
}}));'''
    completed = subprocess.run(
        [node, "--input-type=module", "--eval", source],
        check=True, capture_output=True, text=True, encoding="utf-8", timeout=20,
    )
    result = json.loads(completed.stdout)
    assert "READY" in result["boundary"]
    assert "DEGRADED" in result["expired"] and "STALE/DUE" in result["expired"]
    assert "DEGRADED" in result["backwards"] and "CLOCK_REGRESSED" in result["backwards"]
    assert result["original"] == "CURRENT"


@pytest.mark.parametrize("reason", ["SOURCE_STATUS_STALE", "SOURCE_STATUS_CLOCK_REGRESSED"])
def test_frontend_retains_api_source_freshness_reason(reason: str) -> None:
    node = shutil.which("node")
    if node is None:
        pytest.skip("node is not installed")
    frontend = Path(__file__).resolve().parents[2] / "options_copilot" / "frontend" / "app.js"
    source = f'''const nodes = new Map();
globalThis.document = {{addEventListener() {{}}, getElementById(id) {{
  if (!nodes.has(id)) nodes.set(id, {{textContent: "", classList: {{remove() {{}}, add() {{}}}}}});
  return nodes.get(id);
}}}};
const {{renderNewsSourceHealth}} = await import("{frontend.as_uri()}");
renderNewsSourceHealth([{{source: "SEC", source_kind: "NEWS", status: "DEGRADED", reason: "{reason}", success_count: 50, failure_date_count: 0}}], [], Date.parse("2026-09-09T01:19:00Z"));
console.log(JSON.stringify(nodes.get("sec-source-health").textContent));'''
    completed = subprocess.run(
        [node, "--input-type=module", "--eval", source],
        check=True, capture_output=True, text=True, encoding="utf-8", timeout=20,
    )
    rendered = json.loads(completed.stdout)
    assert reason in rendered
    assert "PROVIDER_DEGRADED" not in rendered


def test_calendar_render_cannot_overwrite_news_publication_clock() -> None:
    node = shutil.which("node")
    if node is None:
        pytest.skip("node is not installed")
    frontend = Path(__file__).resolve().parents[2] / "options_copilot" / "frontend" / "app.js"
    source = f'''import fs from "node:fs";
import vm from "node:vm";
const makeNode = () => ({{textContent: "", dataset: {{}}, style: {{}}, value: "", children: [], childNodes: [], firstChild: {{}},
  classList: {{add() {{}}, remove() {{}}, toggle() {{}}}},
  replaceChildren() {{}}, append() {{}}, appendChild() {{}}, setAttribute() {{}},
  removeAttribute() {{}}, addEventListener() {{}}, querySelectorAll() {{return [];}},
}});
const nodes = new Map();
const context = vm.createContext({{document: {{
  addEventListener() {{}}, getElementById(id) {{
    if (!nodes.has(id)) nodes.set(id, makeNode());
    return nodes.get(id);
  }}, createElement() {{return makeNode();}},
  querySelectorAll() {{return [];}},
}}, console, Date, URL, setTimeout, clearTimeout}});
const script = fs.readFileSync(new URL("{frontend.as_uri()}"), "utf8").replace(/\\nexport \\{{[\\s\\S]*?\\n\\}};/, "");
vm.runInContext(script, context);
vm.runInContext('renderNews(' + {json.dumps(json.dumps(_diagnostic_payload()))} + ')', context);
const published = nodes.get("last-news-refresh").textContent;
vm.runInContext('renderCalendar({{calendar: [], asof: "2026-09-09T01:00:00Z", provider: {{asof: "2026-09-09T00:45:00Z"}}}})', context);
console.log(JSON.stringify({{published, afterCalendar: nodes.get("last-news-refresh").textContent}}));'''
    completed = subprocess.run(
        [node, "--input-type=module", "--eval", source],
        check=False, capture_output=True, text=True, encoding="utf-8", timeout=20,
    )
    assert completed.returncode == 0, completed.stderr
    result = json.loads(completed.stdout)
    assert result["published"] != "--"
    assert result["afterCalendar"] == result["published"]
