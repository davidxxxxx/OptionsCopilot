from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path
import shutil
import subprocess

import pytest
from fastapi import HTTPException

from options_copilot.api.app import _normalise_ranking
from options_copilot.ranking import PortfolioRanker, evaluate_account_capacity


ROOT = Path(__file__).resolve().parents[2]
FRONTEND = ROOT / "options_copilot" / "frontend"


def _candidate(
    candidate_id: str,
    underlying: str,
    expected_value: str,
) -> dict[str, object]:
    return {
        "candidate_id": candidate_id,
        "underlying": underlying,
        "structure": "DEBIT_VERTICAL",
        "eligible": True,
        "after_cost_expected_value": expected_value,
        "max_loss": "100",
        "liquidity_score": "10",
        "open_combinations": 0,
    }


def _ranked_row(
    rank: int,
    candidate_id: str,
    underlying: str,
) -> dict[str, object]:
    return {
        "rank": rank,
        "candidate_id": candidate_id,
        "candidate_body": {
            "candidate_id": candidate_id,
            "underlying": underlying,
            "reasons": ["SUPPORTED_NEWS"],
        },
        "authorizable": True,
        "authority_status": "NORMAL",
    }


def _run_node(source: str) -> dict[str, object]:
    node = shutil.which("node")
    if node is None:
        pytest.skip("node is not installed")
    result = subprocess.run(
        [node, "--input-type=module", "--eval", source],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    return json.loads(result.stdout)


def test_same_underlying_alternatives_survive_ranking_and_are_nested_view_only() -> None:
    ranked = PortfolioRanker().rank(
        (
            _candidate("spy-preferred", "SPY", "30"),
            _candidate("spy-alternative", "SPY", "20"),
            _candidate("qqq-preferred", "QQQ", "10"),
        ),
        limit=10,
    )

    assert [row.candidate_id for row in ranked.candidates] == [
        "spy-preferred",
        "qqq-preferred",
        "spy-alternative",
    ]

    payload = _normalise_ranking(
        {
            "approval_enabled": True,
            "candidates": [
                _ranked_row(1, "spy-preferred", "SPY"),
                _ranked_row(2, "qqq-preferred", "QQQ"),
                _ranked_row(3, "spy-alternative", "SPY"),
            ],
        }
    )

    assert payload["count"] == 2
    assert payload["total_ranked_count"] == 3
    assert [row["candidate_id"] for row in payload["candidates"]] == [
        "spy-preferred",
        "qqq-preferred",
    ]
    spy = payload["candidates"][0]
    assert spy["preferred_for_underlying"] is True
    assert [row["candidate_id"] for row in spy["alternatives"]] == [
        "spy-alternative"
    ]
    assert spy["alternatives"][0]["interaction"] == "VIEW_ONLY"

    with pytest.raises(HTTPException, match="invalid immutable rank"):
        _normalise_ranking(
            {
                "approval_enabled": True,
                "candidates": [
                    _ranked_row(1, "first", "SPY"),
                    _ranked_row(1, "duplicate-rank", "QQQ"),
                ],
            }
        )

    invalid_identifier = _ranked_row(1, "<hostile>", "SPY")
    with pytest.raises(HTTPException, match="invalid immutable candidate identity") as exc:
        _normalise_ranking(
            {"approval_enabled": True, "candidates": [invalid_identifier]}
        )
    assert exc.value.status_code == 502

    mismatched_body = _ranked_row(1, "candidate-a", "SPY")
    mismatched_body["candidate_body"]["candidate_id"] = "candidate-b"
    with pytest.raises(HTTPException, match="invalid immutable candidate identity"):
        _normalise_ranking(
            {"approval_enabled": True, "candidates": [mismatched_body]}
        )

    conflicting_underlying = _ranked_row(1, "candidate-a", "SPY")
    conflicting_underlying["underlying"] = "QQQ"
    with pytest.raises(HTTPException, match="invalid immutable candidate underlying"):
        _normalise_ranking(
            {"approval_enabled": True, "candidates": [conflicting_underlying]}
        )

    conflicting_proposal = _ranked_row(1, "candidate-a", "SPY")
    conflicting_proposal["proposal_body"] = {"underlying": "QQQ"}
    with pytest.raises(HTTPException, match="invalid immutable candidate underlying"):
        _normalise_ranking(
            {"approval_enabled": True, "candidates": [conflicting_proposal]}
        )


def test_a_grade_capacity_accepts_the_full_authorized_fifteen_percent_band() -> None:
    def capacity(risk_fraction: str) -> dict[str, object]:
        maximum_loss = str(int(Decimal("1000") * Decimal(risk_fraction)))
        return evaluate_account_capacity(
            {
                "authority_status": "A_GRADE",
                "authorizable": True,
                "candidate_body": {
                    "strategy_nav_usd": "1000",
                    "max_loss_usd": maximum_loss,
                    "risk_fraction": risk_fraction,
                },
            }
        )

    assert capacity("0.10")["status"] == "READY"
    assert capacity("0.15")["status"] == "READY"
    assert capacity("0.151")["status"] == "BLOCKED"


def test_candidate_reason_taxonomy_is_sanitized_and_complete() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''globalThis.document = {{ addEventListener() {{}} }};
const {{ normalizeCandidateReasonBuckets }} = await import("{script_uri}");
const value = normalizeCandidateReasonBuckets({{
  supported_reasons: ["Official earnings beat", "<script>alert(1)</script>"],
  invalidation_reasons: ["Thesis invalidated"],
  stale_reasons: ["QUOTE_STALE"],
  uncertainty_reasons: ["SOURCE_CONFLICTED"],
  blocked_reasons: ["ACCOUNT_CAPACITY_UNAVAILABLE"],
  source_health: {{ status: "DEGRADED", reason: "PROVIDER_TIMEOUT" }},
  account_capacity: {{ status: "BLOCKED", reason: "STRATEGY_NAV_UNAVAILABLE" }},
}});
console.log(JSON.stringify(value));'''

    rendered = _run_node(source)

    assert rendered == {
        "SUPPORTED": ["Official earnings beat"],
        "INVALIDATED": ["Thesis invalidated"],
        "STALE": ["QUOTE_STALE"],
        "UNCERTAIN": ["SOURCE_CONFLICTED", "SOURCE_HEALTH · DEGRADED · PROVIDER_TIMEOUT"],
        "BLOCKED": [
            "ACCOUNT_CAPACITY_UNAVAILABLE",
            "ACCOUNT_CAPACITY · BLOCKED · STRATEGY_NAV_UNAVAILABLE",
        ],
    }


def test_server_derived_ready_source_and_capacity_render_as_supported() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''globalThis.document = {{ addEventListener() {{}} }};
const {{ normalizeCandidateReasonBuckets }} = await import("{script_uri}");
const value = normalizeCandidateReasonBuckets({{
  source_health: {{
    status: "READY",
    reason: "CANDIDATE_EVIDENCE_PRIMARY_COMPLETE",
    decision_authority: "SUPPORTING_ONLY",
  }},
  account_capacity: {{
    status: "READY",
    reason: "ACCOUNT_CAPACITY_CONFIRMED",
    decision_authority: "SUPPORTING_ONLY",
  }},
}});
console.log(JSON.stringify(value));'''

    rendered = _run_node(source)

    assert rendered["SUPPORTED"] == [
        "SOURCE_HEALTH · READY · CANDIDATE_EVIDENCE_PRIMARY_COMPLETE",
        "ACCOUNT_CAPACITY · READY · ACCOUNT_CAPACITY_CONFIRMED",
    ]
    assert rendered["UNCERTAIN"] == []
    assert rendered["BLOCKED"] == []


def test_deepseek_and_all_five_outcome_horizons_are_supporting_only() -> None:
    html = (FRONTEND / "index.html").read_text(encoding="utf-8")
    script = (FRONTEND / "app.js").read_text(encoding="utf-8")
    for identifier in (
        "deepseek-advisory-status",
        "deepseek-advisory-summary",
        "outcome-horizon-30m",
        "outcome-horizon-session-close",
        "outcome-horizon-1d",
        "outcome-horizon-3d",
        "outcome-horizon-5d",
    ):
        assert f'id="{identifier}"' in html
        assert identifier in script
    assert 'advisory: "/api/advisory"' in script
    assert 'learning: "/api/learning"' in script
    assert "learningOutcomes" not in script
    assert "/api/learning/outcome-horizons" not in script
    assert "renderOutcomeHorizons(" in script
    assert "source.outcome_horizons || {}" in script
    assert "source.outcome_capture || {}" in script

    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''globalThis.document = {{ addEventListener() {{}} }};
const {{ normalizeOutcomeHorizons }} = await import("{script_uri}");
const value = normalizeOutcomeHorizons({{
  status: "READY",
  decision_authority: "SUPPORTING_ONLY",
  selected_challenger: "challenger-a",
  complete_through_sequence: 6001,
  verified_head_sequence: 6001,
  verified_head_hash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  horizons: {{
    "30M": {{ status: "OBSERVED", count: 2, observed_count: 2, blocked_count: 0, uncertain_count: 0 }},
    "SESSION_CLOSE": {{ status: "BLOCKED", count: 1, observed_count: 0, blocked_count: 1, uncertain_count: 0, reason: "MISSING_OBSERVATION" }},
    "1D": {{ status: "OBSERVED", count: 1, observed_count: 1, blocked_count: 0, uncertain_count: 0 }},
    "3D": {{ status: "UNCERTAIN", count: 3, observed_count: 1, blocked_count: 0, uncertain_count: 2 }},
    "5D": {{ status: "OBSERVED", count: 1, observed_count: 1, blocked_count: 0, uncertain_count: 0 }},
  }},
}});
console.log(JSON.stringify(value));'''

    horizons = _run_node(source)

    assert list(horizons) == ["30M", "SESSION_CLOSE", "1D", "3D", "5D"]
    assert all(item["decision_authority"] == "SUPPORTING_ONLY" for item in horizons.values())
    assert horizons["SESSION_CLOSE"]["status"] == "BLOCKED"
    assert horizons["30M"]["count"] == 2
    assert horizons["30M"]["observed_count"] == 2
    assert horizons["3D"]["status"] == "UNCERTAIN"
    assert horizons["3D"]["uncertain_count"] == 2


def test_outcome_horizon_summary_fails_closed_when_unavailable_or_incomplete() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''globalThis.document = {{ addEventListener() {{}} }};
const {{ normalizeOutcomeHorizons }} = await import("{script_uri}");
const unavailable = normalizeOutcomeHorizons({{ status: "UNAVAILABLE" }});
const incomplete = normalizeOutcomeHorizons({{
  status: "READY",
  decision_authority: "SUPPORTING_ONLY",
  selected_challenger: "challenger-a",
  complete_through_sequence: 4999,
  verified_head_sequence: 5000,
  verified_head_hash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  horizons: {{
    "30M": {{ status: "OBSERVED", count: 2, observed_count: 2, blocked_count: 0, uncertain_count: 0 }},
  }},
}});
console.log(JSON.stringify({{ unavailable, incomplete }}));'''

    result = _run_node(source)

    assert result["unavailable"]["SESSION_CLOSE"] == {
        "status": "UNCERTAIN",
        "count": 0,
        "observed_count": 0,
        "blocked_count": 0,
        "uncertain_count": 0,
        "reason": "OUTCOME_HORIZON_SUMMARY_UNAVAILABLE",
        "observed_at": None,
        "decision_authority": "SUPPORTING_ONLY",
    }
    assert all(item["status"] == "UNCERTAIN" for item in result["incomplete"].values())
    assert all(
        item["reason"] == "OUTCOME_HORIZON_SUMMARY_INCOMPLETE"
        for item in result["incomplete"].values()
    )
    assert all(
        item["decision_authority"] == "SUPPORTING_ONLY"
        for horizon_map in result.values()
        for item in horizon_map.values()
    )


def test_broker_state_matrix_and_blocked_controls_emit_zero_post() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''globalThis.document = {{ addEventListener() {{}}, getElementById() {{ return null; }} }};
let posts = 0;
globalThis.fetch = async (_path, options = {{}}) => {{
  if (String(options.method || "GET").toUpperCase() === "POST") posts += 1;
  return {{ ok: true, async json() {{ return {{}}; }} }};
}};
const {{ normalizeBrokerState, actionControlGate, requestRankOneChallenge }} = await import("{script_uri}");
const now = 1720000000000;
const freshContext = {{
  readinessStatus: "READY",
  scanDecision: "CANDIDATES_AVAILABLE",
  approvalEnabled: true,
  strategyNavReady: true,
  brokerState: "FRESH",
  lastControlPollAtMs: now,
  lastSuccessfulControlAtMs: now,
  lastControlPollSucceeded: true,
}};
const states = [
  normalizeBrokerState({{ status: "CURRENT", connected: true, reconciled: true, market_data_status: true }}, {{}}),
  normalizeBrokerState({{ status: "PARTIAL", connected: true }}, {{}}),
  normalizeBrokerState({{ status: "STALE", connected: true }}, {{}}),
  normalizeBrokerState({{ status: "CURRENT", connected: false }}, {{}}),
  normalizeBrokerState(
    {{ status: "PARTIAL", connected: true }},
    {{ warnings: ["UNSUBMITTED_INSTRUCTIONS_UNKNOWN"] }},
  ),
];
const blocked = states.slice(1);
for (const brokerState of blocked) {{
  const approval = {{
    checkbox: {{ checked: true }},
    button: {{ disabled: false }},
    controlContext: {{
      ...freshContext,
      brokerState,
    }},
    sourceHealth: {{ status: "READY" }},
    accountCapacity: {{ status: "READY" }},
  }};
  await requestRankOneChallenge(approval);
}}
console.log(JSON.stringify({{
  states,
  posts,
  freshAllowed: actionControlGate(freshContext, now + 14999),
  staleByAge: actionControlGate(freshContext, now + 15001),
}}));'''

    result = _run_node(source)

    assert result == {
        "states": [
            "FRESH",
            "PARTIAL",
            "STALE",
            "DISCONNECTED",
            "SAVED_INSTRUCTION_UNKNOWN",
        ],
        "posts": 0,
        "freshAllowed": True,
        "staleByAge": False,
    }


def test_control_poll_contract_is_five_seconds_and_fail_closed_immediately() -> None:
    script = (FRONTEND / "app.js").read_text(encoding="utf-8")

    assert "const CONTROL_REFRESH_INTERVAL_MS = 5_000;" in script
    assert "const SCAN_REFRESH_INTERVAL_MS = 30_000;" in script
    assert "const CONTROL_SNAPSHOT_STALE_AFTER_MS = 15_000;" in script
    assert "window.setInterval(refreshControlSnapshot, CONTROL_REFRESH_INTERVAL_MS);" in script
    assert "window.setInterval(refreshScanSnapshot, SCAN_REFRESH_INTERVAL_MS);" in script
    assert 'health: "/api/health/summary"' in script
    assert 'return fetchResearchJson("/health");' in script
    assert "lastControlPollAtMs" in script
    assert "lastSuccessfulControlAtMs" in script
    assert "lastControlPollSucceeded" in script
    assert "failClosedControlSnapshot" in script
    assert "synchronizeActionControls" in script


def test_candidate_level_health_and_capacity_block_challenge_without_post() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''globalThis.document = {{ addEventListener() {{}}, getElementById() {{ return null; }} }};
let posts = 0;
globalThis.fetch = async (_path, options = {{}}) => {{
  if (String(options.method || "GET").toUpperCase() === "POST") posts += 1;
  return {{ ok: true, async json() {{ return {{}}; }} }};
}};
const {{ candidateChallengeGate, requestRankOneChallenge }} = await import("{script_uri}");
const now = 1720000000000;
const controlContext = {{
  readinessStatus: "READY",
  scanDecision: "CANDIDATES_AVAILABLE",
  approvalEnabled: true,
  strategyNavReady: true,
  brokerState: "FRESH",
  lastControlPollAtMs: now,
  lastSuccessfulControlAtMs: now,
  lastControlPollSucceeded: true,
}};
const cases = [
  {{ sourceHealth: {{ status: "DEGRADED" }}, accountCapacity: {{ status: "READY" }} }},
  {{ sourceHealth: {{ status: "READY" }}, accountCapacity: {{ status: "BLOCKED" }} }},
];
for (const candidate of cases) {{
  const approval = {{
    checkbox: {{ checked: true }},
    button: {{ disabled: false }},
    controlContext,
    ...candidate,
  }};
  await requestRankOneChallenge(approval);
}}
console.log(JSON.stringify({{
  posts,
  sourceBlocked: candidateChallengeGate(cases[0], controlContext, now),
  capacityBlocked: candidateChallengeGate(cases[1], controlContext, now),
  ready: candidateChallengeGate({{
    sourceHealth: {{ status: "READY" }},
    accountCapacity: {{ status: "READY" }},
  }}, controlContext, now),
}}));'''

    result = _run_node(source)

    assert result == {
        "posts": 0,
        "sourceBlocked": False,
        "capacityBlocked": False,
        "ready": True,
    }


def test_candidate_identity_uses_canonical_body_despite_hostile_top_level_fields() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''globalThis.document = {{ addEventListener() {{}} }};
const {{ candidateUnderlying, candidateView, groupRankedCandidates }} = await import("{script_uri}");
const hostile = {{
  rank: 1,
  candidate_id: "immutable-spy",
  interaction: "VIEW_ONLY",
  underlying: "QQQ",
  symbol: "QQQ",
  proposal_body: {{ underlying: "QQQ", symbol: "QQQ" }},
  candidate_body: {{
    candidate_id: "immutable-spy",
    underlying: "SPY",
    symbol: "SPY",
    structure: "DEBIT_VERTICAL",
  }},
}};
const view = candidateView(hostile);
const groups = groupRankedCandidates({{ candidates: [hostile] }});
console.log(JSON.stringify({{
  underlying: candidateUnderlying(hostile),
  viewUnderlying: view.underlying,
  viewSymbol: view.symbol,
  groupUnderlying: groups[0]?.candidate_body?.underlying,
}}));'''

    result = _run_node(source)

    assert result == {
        "underlying": "SPY",
        "viewUnderlying": "SPY",
        "viewSymbol": "SPY",
        "groupUnderlying": "SPY",
    }


def test_candidate_selection_and_nested_alternatives_are_keyboard_accessible() -> None:
    html = (FRONTEND / "index.html").read_text(encoding="utf-8")
    script = (FRONTEND / "app.js").read_text(encoding="utf-8")
    candidate_renderer = script.split("function buildCandidateCard", 1)[1].split(
        "function candidateView", 1
    )[0]
    group_renderer = script.split("function buildCandidateGroup", 1)[1].split(
        "function buildAlternativeCandidate", 1
    )[0]

    assert 'data-field="select-candidate"' in html
    assert "selectButton.addEventListener(\"click\"" in candidate_renderer
    assert 'card.addEventListener("click"' not in candidate_renderer
    assert 'document.createElement("details")' in group_renderer
    assert 'document.createElement("summary")' in group_renderer
    assert 'summary.setAttribute("aria-expanded", "false")' in group_renderer
    assert 'groupDisclosure.addEventListener("toggle"' in group_renderer

    alternative_renderer = script.split("function buildAlternativeCandidate", 1)[1].split(
        "function buildCandidateCard", 1
    )[0]
    assert "renderReasonBuckets" in alternative_renderer
    assert "evidenceLines(view)" in alternative_renderer
    assert "exitPlanLines(view)" in alternative_renderer
    assert '"alternative-evidence"' in alternative_renderer
    assert '"alternative-exit-plan"' in alternative_renderer


def test_partial_and_blocked_position_states_are_never_presented_as_current() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''globalThis.document = {{ addEventListener() {{}} }};
const {{ normalizePositionDisplayState }} = await import("{script_uri}");
const states = Object.fromEntries([
  ["FRESH", normalizePositionDisplayState("CURRENT", "FRESH")],
  ["PARTIAL", normalizePositionDisplayState("CURRENT", "PARTIAL")],
  ["STALE", normalizePositionDisplayState("CURRENT", "STALE")],
  ["DISCONNECTED", normalizePositionDisplayState("CURRENT", "DISCONNECTED")],
  ["SAVED_INSTRUCTION_UNKNOWN", normalizePositionDisplayState("CURRENT", "SAVED_INSTRUCTION_UNKNOWN")],
  ["UNAVAILABLE", normalizePositionDisplayState("UNAVAILABLE", "FRESH")],
  ["POSITION_DISCONNECTED", normalizePositionDisplayState("DISCONNECTED", "FRESH")],
  ["POSITION_SAVED_UNKNOWN", normalizePositionDisplayState("SAVED_INSTRUCTION_UNKNOWN", "FRESH")],
  ["MISSING", normalizePositionDisplayState(undefined, "FRESH")],
  ["UNKNOWN", normalizePositionDisplayState("MYSTERY_STATUS", "FRESH")],
]);
console.log(JSON.stringify(states));'''

    states = _run_node(source)

    assert states["FRESH"] == {
        "status": "CURRENT",
        "displayable": True,
        "last_known_only": False,
        "approval_eligible": False,
    }
    for state in ("PARTIAL", "STALE", "DISCONNECTED", "SAVED_INSTRUCTION_UNKNOWN"):
        assert states[state] == {
            "status": state,
            "displayable": True,
            "last_known_only": True,
            "approval_eligible": False,
        }
    assert states["UNAVAILABLE"] == {
        "status": "UNAVAILABLE",
        "displayable": False,
        "last_known_only": False,
        "approval_eligible": False,
    }
    for state, expected_status in (
        ("POSITION_DISCONNECTED", "DISCONNECTED"),
        ("POSITION_SAVED_UNKNOWN", "SAVED_INSTRUCTION_UNKNOWN"),
    ):
        assert states[state] == {
            "status": expected_status,
            "displayable": True,
            "last_known_only": True,
            "approval_eligible": False,
        }
    for state in ("MISSING", "UNKNOWN"):
        assert states[state] == {
            "status": "UNAVAILABLE",
            "displayable": True,
            "last_known_only": True,
            "approval_eligible": False,
        }


def test_position_time_exit_copy_does_not_claim_exchange_trading_days() -> None:
    script = (FRONTEND / "app.js").read_text(encoding="utf-8")

    assert "到期前两交易日" not in script
    assert "仅按周一至周五，未校验交易所休市" in script


def test_fetch_json_aborts_hung_requests_within_the_bounded_timeout() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''globalThis.document = {{ addEventListener() {{}} }};
let aborted = false;
globalThis.fetch = async (_path, options = {{}}) => new Promise((_resolve, reject) => {{
  options.signal.addEventListener("abort", () => {{
    aborted = true;
    reject(new Error("aborted"));
  }}, {{ once: true }});
}});
const {{ fetchJson }} = await import("{script_uri}");
const started = Date.now();
let code = null;
try {{
  await fetchJson("/hung", {{ timeoutMs: 25 }});
}} catch (error) {{
  code = error.code || null;
}}
console.log(JSON.stringify({{ code, aborted, elapsedMs: Date.now() - started }}));'''

    result = _run_node(source)

    assert result["code"] == "FETCH_TIMEOUT"
    assert result["aborted"] is True
    assert 0 <= result["elapsedMs"] < 1000


def test_research_gets_allow_a_bounded_timeout_above_the_control_limit() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''globalThis.document = {{ addEventListener() {{}} }};
const nativeSetTimeout = globalThis.setTimeout;
const scheduled = [];
globalThis.setTimeout = (callback, delay, ...args) => {{
  scheduled.push(delay);
  return nativeSetTimeout(callback, delay, ...args);
}};
globalThis.fetch = async () => ({{
  ok: true,
  status: 200,
  async json() {{ return {{ ok: true }}; }},
}});
const {{ fetchJson, fetchResearchJson }} = await import("{script_uri}");
await fetchJson("/api/health");
await fetchResearchJson("/api/news");
await fetchJson("/api/calendar", {{ timeoutMs: 120000 }});
console.log(JSON.stringify(scheduled));'''

    result = _run_node(source)

    assert result == [4000, 60000, 60000]


def test_large_read_only_surfaces_use_the_research_timeout_tier() -> None:
    script = (FRONTEND / "app.js").read_text(encoding="utf-8")

    assert "fetchResearchJson(ENDPOINTS.news)" in script
    assert "fetchResearchJson(ENDPOINTS.calendar)" in script
    assert "fetchResearchJson(ENDPOINTS.weeklyBrief)" in script
    assert "fetchResearchJson(ENDPOINTS.fundamentals)" in script
    assert "fetchResearchJson(RESEARCH_TOP10_ROUTES.researchTop10)" in script
    assert "fetchResearchJson(RESEARCH_TOP10_ROUTES.afterHoursIndicative)" in script


def test_state_changing_posts_ignore_the_read_timeout_without_aborting() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''globalThis.document = {{ addEventListener() {{}} }};
const nativeSetTimeout = globalThis.setTimeout;
let now = 1720000000000;
Date.now = () => now;
let aborted = false;
let calls = 0;
let timeoutSchedules = 0;
globalThis.setTimeout = (callback, delay, ...args) => {{
  timeoutSchedules += 1;
  return nativeSetTimeout(callback, delay, ...args);
}};
globalThis.fetch = async (_path, options = {{}}) => new Promise((resolve, reject) => {{
  calls += 1;
  now += 5000;
  options.signal.addEventListener("abort", () => {{
    aborted = true;
    reject(new Error("post aborted"));
  }}, {{ once: true }});
  nativeSetTimeout(() => resolve({{
    ok: true,
    status: 200,
    async json() {{ return {{ accepted: true }}; }},
  }}), 5);
}});
const {{ fetchJson }} = await import("{script_uri}");
const started = Date.now();
await fetchJson("/api/rankings/ranking-1/candidates/candidate-1/challenge", {{
  method: "POST",
  body: "{{}}",
}});
await fetchJson("/api/approval-challenges/challenge-1/confirm", {{
  method: "POST",
  body: "{{}}",
}});
console.log(JSON.stringify({{
  aborted,
  calls,
  timeoutSchedules,
  elapsedMs: Date.now() - started,
}}));'''

    result = _run_node(source)

    assert result["aborted"] is False
    assert result["calls"] == 2
    assert result["timeoutSchedules"] == 0
    assert result["elapsedMs"] == 10000


def test_elapsed_control_snapshot_visibly_migrates_to_last_known_only() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''function makeNode() {{
  const classes = new Set();
  return {{
    textContent: "",
    dataset: {{}},
    disabled: false,
    href: "#",
    classList: {{
      add(...values) {{ values.forEach((value) => classes.add(value)); }},
      remove(...values) {{ values.forEach((value) => classes.delete(value)); }},
      contains(value) {{ return classes.has(value); }},
    }},
    setAttribute(name, value) {{ this[name] = String(value); }},
    removeAttribute(name) {{ delete this[name]; }},
  }};
}}
const ids = [
  "readiness-state",
  "readiness-reasons",
  "ibkr-status",
  "ibkr-reconciled",
  "ibkr-market-data",
  "position-count",
  "management-review-action",
  "ibkr-review-link",
];
const nodes = Object.fromEntries(ids.map((id) => [id, makeNode()]));
globalThis.document = {{
  addEventListener() {{}},
  getElementById(id) {{ return nodes[id] || null; }},
}};
const {{ markControlSnapshotSucceeded, synchronizeActionControls }} = await import("{script_uri}");
const now = 1720000000000;
nodes["position-count"].textContent = "2";
markControlSnapshotSucceeded(now);
synchronizeActionControls(now + 15001);
console.log(JSON.stringify({{
  readiness: nodes["readiness-state"].textContent,
  reason: nodes["readiness-reasons"].textContent,
  broker: nodes["ibkr-status"].textContent,
  reconciled: nodes["ibkr-reconciled"].textContent,
  marketData: nodes["ibkr-market-data"].textContent,
  positions: nodes["position-count"].textContent,
}}));'''

    result = _run_node(source)

    assert result["readiness"] == "STALE · NO_TRADE"
    assert "CONTROL_SNAPSHOT_ELAPSED_STALE" in result["reason"]
    assert result["broker"] == "STALE · LAST_KNOWN_ONLY"
    assert result["reconciled"] == "LAST_KNOWN_ONLY"
    assert result["marketData"] == "LAST_KNOWN_ONLY"
    assert result["positions"] == "LAST_KNOWN_ONLY"


def test_second_confirmation_requires_a_new_complete_control_snapshot() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''function makeNode() {{
  const classes = new Set();
  return {{
    textContent: "",
    dataset: {{}},
    style: {{}},
    disabled: false,
    href: "#",
    classList: {{
      add(...values) {{ values.forEach((value) => classes.add(value)); }},
      remove(...values) {{ values.forEach((value) => classes.delete(value)); }},
      contains(value) {{ return classes.has(value); }},
    }},
    setAttribute(name, value) {{ this[name] = String(value); }},
    removeAttribute(name) {{ delete this[name]; }},
    append() {{}},
    replaceChildren() {{}},
    querySelector() {{ return null; }},
    closest() {{ return null; }},
  }};
}}
const ids = [
  "approval-status",
  "readiness-state",
  "readiness-reasons",
  "ibkr-status",
  "ibkr-reconciled",
  "ibkr-market-data",
  "position-count",
  "management-review-action",
  "ibkr-review-link",
];
const nodes = Object.fromEntries(ids.map((id) => [id, makeNode()]));
globalThis.document = {{
  addEventListener() {{}},
  getElementById(id) {{ return nodes[id] || null; }},
  createElement() {{ return makeNode(); }},
}};
let now = 1720000000000;
Date.now = () => now;
const getPaths = [];
const postPaths = [];
globalThis.window = {{
  confirm() {{
    now += 16000;
    return true;
  }},
}};
globalThis.fetch = async (path, options = {{}}) => {{
  const method = String(options.method || "GET").toUpperCase();
  if (method === "POST") {{
    postPaths.push(String(path));
    if (String(path).endsWith("/challenge")) {{
      return {{
        ok: true,
        status: 200,
        async json() {{
          return {{
            status: "PENDING_SECOND_CONFIRMATION",
            ranking_snapshot_id: "ranking-1",
            candidate_id: "candidate-1",
            challenge_id: "challenge-1",
            challenge_response: "0123456789abcdefghijklmnop",
            approval_id: null,
            instruction_id: null,
            ibkr_deep_link: null,
            review_only: true,
            order_submitted: false,
            transmitted_to_broker: false,
          }};
        }},
      }};
    }}
    return {{ ok: true, status: 200, async json() {{ return {{}}; }} }};
  }}
  getPaths.push(String(path));
  const failed = String(path) === "/api/rankings/latest";
  return {{
    ok: !failed,
    status: failed ? 503 : 200,
    async json() {{ return failed ? {{ detail: "ranking unavailable" }} : {{}}; }},
  }};
}};
const {{ requestRankOneChallenge }} = await import("{script_uri}");
const controlContext = {{
  readinessStatus: "READY",
  scanDecision: "CANDIDATES_AVAILABLE",
  approvalEnabled: true,
  strategyNavReady: true,
  brokerState: "FRESH",
  lastControlPollAtMs: now,
  lastSuccessfulControlAtMs: now,
  lastControlPollSucceeded: true,
  controlFailureReason: null,
}};
const approval = {{
  rankingSnapshotId: "ranking-1",
  candidateId: "candidate-1",
  checkbox: {{ checked: true }},
  button: {{ disabled: false, textContent: "challenge", closest() {{ return null; }} }},
  sourceHealth: {{ status: "READY" }},
  accountCapacity: {{ status: "READY" }},
  controlContext,
}};
await requestRankOneChallenge(approval);
console.log(JSON.stringify({{
  getPaths,
  postPaths,
  readiness: nodes["readiness-state"].textContent,
  reason: nodes["readiness-reasons"].textContent,
  broker: nodes["ibkr-status"].textContent,
  positions: nodes["position-count"].textContent,
  approvalStatus: nodes["approval-status"].textContent,
  buttonText: approval.button.textContent,
}}));'''

    result = _run_node(source)

    assert result["getPaths"] == [
        "/api/bootstrap",
        "/api/health/summary",
        "/api/readiness",
        "/api/scans/latest",
        "/api/scans/campaign",
        "/api/rankings/latest",
        "/api/management/current",
        "/api/positions",
    ]
    assert result["postPaths"] == [
        "/api/rankings/ranking-1/candidates/candidate-1/challenge"
    ]
    assert result["readiness"] == "STALE · NO_TRADE"
    assert "CONTROL_POLL_FAILED_RANKINGS" in result["reason"]
    assert "LAST_KNOWN_ONLY" in result["broker"]
    assert result["positions"] == "LAST_KNOWN_ONLY"
    assert "不会创建审批或 IBKR 指令" in result["approvalStatus"]
    assert result["buttonText"] == "控制快照已变化 · 禁止确认"


def test_post_dispatch_transport_failure_locks_unknown_and_reconciles_get_only() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''function makeNode() {{
  const classes = new Set();
  return {{
    textContent: "",
    dataset: {{}},
    style: {{}},
    disabled: false,
    href: "#",
    classList: {{
      add(...values) {{ values.forEach((value) => classes.add(value)); }},
      remove(...values) {{ values.forEach((value) => classes.delete(value)); }},
      contains(value) {{ return classes.has(value); }},
    }},
    setAttribute(name, value) {{ this[name] = String(value); }},
    removeAttribute(name) {{ delete this[name]; }},
    append() {{}},
    replaceChildren() {{}},
    querySelector() {{ return null; }},
    closest() {{ return null; }},
  }};
}}
const ids = [
  "approval-status",
  "readiness-state",
  "readiness-reasons",
  "ibkr-status",
  "ibkr-reconciled",
  "ibkr-market-data",
  "position-count",
  "management-review-action",
  "ibkr-review-link",
];
const nodes = Object.fromEntries(ids.map((id) => [id, makeNode()]));
globalThis.document = {{
  addEventListener() {{}},
  getElementById(id) {{ return nodes[id] || null; }},
  createElement() {{ return makeNode(); }},
}};
let confirmCalls = 0;
globalThis.window = {{ confirm() {{ confirmCalls += 1; return true; }} }};
const getPaths = [];
const postPaths = [];
globalThis.fetch = async (path, options = {{}}) => {{
  const method = String(options.method || "GET").toUpperCase();
  if (method === "POST") {{
    postPaths.push(String(path));
    throw new TypeError("connection reset after dispatch");
  }}
  getPaths.push(String(path));
  const failed = String(path) === "/api/rankings/latest";
  return {{
    ok: !failed,
    status: failed ? 503 : 200,
    async json() {{ return failed ? {{ detail: "ranking unavailable" }} : {{}}; }},
  }};
}};
const {{ requestRankOneChallenge, synchronizeActionControls }} = await import("{script_uri}");
const now = Date.now();
const approval = {{
  rankingSnapshotId: "ranking-1",
  candidateId: "candidate-1",
  checkbox: {{ checked: true, disabled: false }},
  button: {{ disabled: false, textContent: "challenge", closest() {{ return null; }} }},
  sourceHealth: {{ status: "READY" }},
  accountCapacity: {{ status: "READY" }},
  controlContext: {{
    readinessStatus: "READY",
    scanDecision: "CANDIDATES_AVAILABLE",
    approvalEnabled: true,
    strategyNavReady: true,
    brokerState: "FRESH",
    lastControlPollAtMs: now,
    lastSuccessfulControlAtMs: now,
    lastControlPollSucceeded: true,
    controlFailureReason: null,
  }},
}};
await requestRankOneChallenge(approval);
synchronizeActionControls(now + 20000);
console.log(JSON.stringify({{
  getPaths,
  postPaths,
  confirmCalls,
  readiness: nodes["readiness-state"].textContent,
  reason: nodes["readiness-reasons"].textContent,
  broker: nodes["ibkr-status"].textContent,
  positions: nodes["position-count"].textContent,
  approvalStatus: nodes["approval-status"].textContent,
  checkboxDisabled: approval.checkbox.disabled,
  buttonDisabled: approval.button.disabled,
  buttonText: approval.button.textContent,
}}));'''

    result = _run_node(source)

    assert result["postPaths"] == [
        "/api/rankings/ranking-1/candidates/candidate-1/challenge"
    ]
    assert result["getPaths"] == [
        "/api/bootstrap",
        "/api/health/summary",
        "/api/readiness",
        "/api/scans/latest",
        "/api/scans/campaign",
        "/api/rankings/latest",
        "/api/management/current",
        "/api/positions",
    ]
    assert result["confirmCalls"] == 0
    assert result["readiness"] == "STALE · NO_TRADE"
    assert "CHALLENGE_POST_OUTCOME_UNKNOWN" in result["reason"]
    assert "SAVED_INSTRUCTION_UNKNOWN" in result["broker"]
    assert result["positions"] == "LAST_KNOWN_ONLY"
    assert "SAVED_INSTRUCTION_UNKNOWN" in result["approvalStatus"]
    assert result["checkboxDisabled"] is True
    assert result["buttonDisabled"] is True
    assert result["buttonText"] == "结果未知 · 禁止重试"


def test_never_settling_post_stays_single_flight_then_fails_closed_unknown() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''function makeNode() {{
  const classes = new Set();
  return {{
    textContent: "",
    dataset: {{}},
    style: {{}},
    disabled: false,
    href: "#",
    classList: {{
      add(...values) {{ values.forEach((value) => classes.add(value)); }},
      remove(...values) {{ values.forEach((value) => classes.delete(value)); }},
      contains(value) {{ return classes.has(value); }},
    }},
    setAttribute(name, value) {{ this[name] = String(value); }},
    removeAttribute(name) {{ delete this[name]; }},
    append() {{}},
    replaceChildren() {{}},
    querySelector() {{ return null; }},
    closest() {{ return null; }},
  }};
}}
const ids = [
  "approval-status",
  "readiness-state",
  "readiness-reasons",
  "ibkr-status",
  "ibkr-reconciled",
  "ibkr-market-data",
  "position-count",
  "management-review-action",
  "ibkr-review-link",
];
const nodes = Object.fromEntries(ids.map((id) => [id, makeNode()]));
globalThis.document = {{
  addEventListener() {{}},
  getElementById(id) {{ return nodes[id] || null; }},
  createElement() {{ return makeNode(); }},
}};
let confirmCalls = 0;
globalThis.window = {{ confirm() {{ confirmCalls += 1; return true; }} }};
const scheduled = [];
globalThis.setTimeout = (callback, delay) => {{
  scheduled.push({{ callback, delay }});
  return scheduled.length;
}};
globalThis.clearTimeout = () => {{}};
const getPaths = [];
const postPaths = [];
globalThis.fetch = (path, options = {{}}) => {{
  const method = String(options.method || "GET").toUpperCase();
  if (method === "POST") {{
    postPaths.push(String(path));
    return new Promise(() => {{}});
  }}
  getPaths.push(String(path));
  return Promise.resolve({{
    ok: true,
    status: 200,
    async json() {{ return {{}}; }},
  }});
}};
const {{ requestRankOneChallenge, synchronizeActionControls }} = await import("{script_uri}");
const now = Date.now();
const approval = {{
  rankingSnapshotId: "ranking-1",
  candidateId: "candidate-1",
  checkbox: {{ checked: true, disabled: false }},
  button: {{ disabled: false, textContent: "challenge", closest() {{ return null; }} }},
  sourceHealth: {{ status: "READY" }},
  accountCapacity: {{ status: "READY" }},
  controlContext: {{
    readinessStatus: "READY",
    scanDecision: "CANDIDATES_AVAILABLE",
    approvalEnabled: true,
    strategyNavReady: true,
    brokerState: "FRESH",
    lastControlPollAtMs: now,
    lastSuccessfulControlAtMs: now,
    lastControlPollSucceeded: true,
    controlFailureReason: null,
  }},
}};
const first = requestRankOneChallenge(approval);
await Promise.resolve();
synchronizeActionControls(now);
await requestRankOneChallenge(approval);
const postsBeforeWatchdog = [...postPaths];
const disabledWhilePending = approval.button.disabled;
scheduled[0].callback();
await first;
synchronizeActionControls(now + 20_000);
console.log(JSON.stringify({{
  postsBeforeWatchdog,
  postPaths,
  getPaths,
  confirmCalls,
  disabledWhilePending,
  approvalStatus: nodes["approval-status"].textContent,
  checkboxDisabled: approval.checkbox.disabled,
  buttonDisabled: approval.button.disabled,
  buttonText: approval.button.textContent,
}}));'''

    result = _run_node(source)

    challenge_path = "/api/rankings/ranking-1/candidates/candidate-1/challenge"
    assert result["postsBeforeWatchdog"] == [challenge_path]
    assert result["postPaths"] == [challenge_path]
    assert result["getPaths"] == [
        "/api/bootstrap",
        "/api/health/summary",
        "/api/readiness",
        "/api/scans/latest",
        "/api/scans/campaign",
        "/api/rankings/latest",
        "/api/management/current",
        "/api/positions",
    ]
    assert result["confirmCalls"] == 0
    assert result["disabledWhilePending"] is True
    assert "SAVED_INSTRUCTION_UNKNOWN" in result["approvalStatus"]
    assert result["checkboxDisabled"] is True
    assert result["buttonDisabled"] is True
    assert result["buttonText"] == "结果未知 · 禁止重试"


def test_slow_control_refresh_cannot_reopen_the_challenge_workflow() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''function makeNode() {{
  const classes = new Set();
  return {{
    textContent: "",
    dataset: {{}},
    style: {{}},
    disabled: false,
    href: "#",
    classList: {{
      add(...values) {{ values.forEach((value) => classes.add(value)); }},
      remove(...values) {{ values.forEach((value) => classes.delete(value)); }},
      contains(value) {{ return classes.has(value); }},
    }},
    setAttribute(name, value) {{ this[name] = String(value); }},
    removeAttribute(name) {{ delete this[name]; }},
    append() {{}},
    replaceChildren() {{}},
    querySelector() {{ return null; }},
    closest() {{ return null; }},
  }};
}}
const nodes = {{}};
globalThis.document = {{
  addEventListener() {{}},
  getElementById(id) {{ return nodes[id] ||= makeNode(); }},
  createElement() {{ return makeNode(); }},
}};
let confirmCalls = 0;
globalThis.window = {{ confirm() {{ confirmCalls += 1; return true; }} }};
const getPaths = [];
const postPaths = [];
let rejectRanking;
const slowRanking = new Promise((_resolve, reject) => {{ rejectRanking = reject; }});
globalThis.fetch = (path, options = {{}}) => {{
  const method = String(options.method || "GET").toUpperCase();
  if (method === "POST") {{
    postPaths.push(String(path));
    return Promise.resolve({{
      ok: true,
      status: 200,
      async json() {{
        return {{
          status: "PENDING_SECOND_CONFIRMATION",
          ranking_snapshot_id: "ranking-1",
          candidate_id: "candidate-1",
          challenge_id: "challenge-1",
          challenge_response: "0123456789abcdefghijklmnop",
          approval_id: null,
          instruction_id: null,
          ibkr_deep_link: null,
          review_only: true,
          order_submitted: false,
          transmitted_to_broker: false,
        }};
      }},
    }});
  }}
  getPaths.push(String(path));
  if (String(path) === "/api/rankings/latest") return slowRanking;
  return Promise.resolve({{ ok: true, status: 200, async json() {{ return {{}}; }} }});
}};
const {{ requestRankOneChallenge, synchronizeActionControls }} = await import("{script_uri}");
const now = Date.now();
const approval = {{
  rankingSnapshotId: "ranking-1",
  candidateId: "candidate-1",
  checkbox: {{ checked: true, disabled: false }},
  button: {{ disabled: false, textContent: "challenge", closest() {{ return null; }} }},
  sourceHealth: {{ status: "READY" }},
  accountCapacity: {{ status: "READY" }},
  controlContext: {{
    readinessStatus: "READY",
    scanDecision: "CANDIDATES_AVAILABLE",
    approvalEnabled: true,
    strategyNavReady: true,
    brokerState: "FRESH",
    lastControlPollAtMs: now,
    lastSuccessfulControlAtMs: now,
    lastControlPollSucceeded: true,
    controlFailureReason: null,
  }},
}};
const first = requestRankOneChallenge(approval);
for (let index = 0; index < 12; index += 1) await Promise.resolve();
synchronizeActionControls(now + 1000);
await requestRankOneChallenge(approval);
const postsDuringSlowRefresh = [...postPaths];
rejectRanking(new TypeError("slow refresh failed"));
await first;
synchronizeActionControls(now + 2000);
await requestRankOneChallenge(approval);
console.log(JSON.stringify({{
  postsDuringSlowRefresh,
  postPaths,
  getPaths,
  confirmCalls,
  workflowLocked: approval.workflowLocked,
  buttonDisabled: approval.button.disabled,
}}));'''

    result = _run_node(source)

    challenge_path = "/api/rankings/ranking-1/candidates/candidate-1/challenge"
    assert result["postsDuringSlowRefresh"] == [challenge_path]
    assert result["postPaths"] == [challenge_path]
    assert result["getPaths"] == [
        "/api/bootstrap",
        "/api/health/summary",
        "/api/readiness",
        "/api/scans/latest",
        "/api/scans/campaign",
        "/api/rankings/latest",
        "/api/management/current",
        "/api/positions",
    ]
    assert result["confirmCalls"] == 1
    assert result["workflowLocked"] is True
    assert result["buttonDisabled"] is True


def test_successful_handoff_terminal_controls_cannot_be_reenabled() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''globalThis.document = {{ addEventListener() {{}} }};
const {{ updateApprovalState }} = await import("{script_uri}");
const now = Date.now();
const approval = {{
  expiresAt: now + 60000,
  workflowLocked: true,
  checkbox: {{ checked: true, disabled: true }},
  button: {{ disabled: true, textContent: "等待 Codex 重新报价", closest() {{ return null; }} }},
  countdown: {{ textContent: "", classList: {{ add() {{}}, remove() {{}} }} }},
  sourceHealth: {{ status: "READY" }},
  accountCapacity: {{ status: "READY" }},
  controlContext: {{
    readinessStatus: "READY",
    scanDecision: "CANDIDATES_AVAILABLE",
    approvalEnabled: true,
    strategyNavReady: true,
    brokerState: "FRESH",
    lastControlPollAtMs: now,
    lastSuccessfulControlAtMs: now,
    lastControlPollSucceeded: true,
    controlFailureReason: null,
  }},
}};
updateApprovalState(approval, now + 1000);
console.log(JSON.stringify({{
  workflowLocked: approval.workflowLocked,
  checkboxDisabled: approval.checkbox.disabled,
  buttonDisabled: approval.button.disabled,
}}));'''

    result = _run_node(source)

    assert result == {
        "workflowLocked": True,
        "checkboxDisabled": True,
        "buttonDisabled": True,
    }


def test_rerendered_approval_reuses_persisted_identity_workflow_lock() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''globalThis.document = {{ addEventListener() {{}} }};
const {{
  approvalWorkflowLocked,
  applyApprovalWorkflowLock,
  lockApprovalWorkflow,
}} = await import("{script_uri}");
const original = {{
  rankingSnapshotId: "ranking-1",
  candidateId: "candidate-1",
  checkbox: {{ checked: true, disabled: false }},
  button: {{ disabled: false, textContent: "challenge" }},
}};
lockApprovalWorkflow(original);
const replacement = {{
  rankingSnapshotId: "ranking-1",
  candidateId: "candidate-1",
  checkbox: {{ checked: true, disabled: false }},
  button: {{ disabled: false, textContent: "challenge" }},
}};
const applied = applyApprovalWorkflowLock(replacement);
console.log(JSON.stringify({{
  applied,
  originalLocked: approvalWorkflowLocked(original),
  replacementLocked: approvalWorkflowLocked(replacement),
  replacementWorkflowFlag: replacement.workflowLocked,
  checkboxChecked: replacement.checkbox.checked,
  checkboxDisabled: replacement.checkbox.disabled,
  buttonDisabled: replacement.button.disabled,
  buttonText: replacement.button.textContent,
}}));'''

    result = _run_node(source)

    assert result == {
        "applied": True,
        "originalLocked": True,
        "replacementLocked": True,
        "replacementWorkflowFlag": True,
        "checkboxChecked": False,
        "checkboxDisabled": True,
        "buttonDisabled": True,
        "buttonText": "审批流程已冻结 · 禁止重建",
    }
