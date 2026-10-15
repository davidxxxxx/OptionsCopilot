from __future__ import annotations

import json
from pathlib import Path
import re
import subprocess


ROOT = Path(__file__).resolve().parents[2]
FRONTEND = ROOT / "options_copilot" / "frontend"


def test_calendar_reaction_surface_is_read_only_and_responsive() -> None:
    html = (FRONTEND / "index.html").read_text(encoding="utf-8")
    script = (FRONTEND / "app.js").read_text(encoding="utf-8")
    styles = (FRONTEND / "styles.css").read_text(encoding="utf-8")
    calendar = html.split('<section class="calendar-section"', 1)[1].split(
        "</section>", 1
    )[0]

    for identifier in (
        "calendar-reaction-provider-status",
        "calendar-reaction-provider-coverage",
        "calendar-list",
    ):
        assert f'id="{identifier}"' in calendar
        assert identifier in script

    for field in (
        "current_stage",
        "expectation",
        "release",
        "surprise",
        "market_reaction",
        "option_reevaluation",
        "REACTION_DECISION_UNAVAILABLE",
    ):
        assert field in script

    buttons = re.findall(r"<button\b[^>]*>", calendar)
    assert buttons
    assert all("data-calendar-window" in button for button in buttons)
    for forbidden_control in (
        'data-action="approve"',
        'type="submit"',
        "ibkr-review-link",
        "management-review-action",
    ):
        assert forbidden_control not in calendar

    assert "innerHTML" not in script
    assert ".calendar-reaction-grid" in styles
    assert ".calendar-no-trade" in styles
    assert "@media (max-width: 480px)" in styles


def test_reaction_render_models_normalize_complete_partial_and_malicious_payloads() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''globalThis.document = {{ addEventListener() {{}} }};
const {{ normalizeCalendarReaction, normalizeReactionProviderSummary }} = await import("{script_uri}");
const completeEvent = {{
  reaction: {{
    status: "READY",
    current_stage: "OPTION_REEVALUATED",
    analysis_available: true,
    decision: "OBSERVATION_ONLY",
    reasons: [],
    expectation: {{
      metric: "headline_cpi_mom_pct",
      expected_value: "0.2",
      unit: "percent",
      observed_at: "2026-08-05T11:00:00+00:00",
    }},
    release: {{
      actual_value: "0.4",
      unit: "percent",
      released_at: "2026-08-05T12:30:00+00:00",
      revision: 0,
    }},
    surprise: {{
      delta: "0.2",
      relative_delta: "1.0",
      assessed_at: "2026-08-05T12:30:06+00:00",
    }},
    market_reaction: {{
      window_start: "2026-08-05T12:30:00+00:00",
      window_end: "2026-08-05T12:32:00+00:00",
      evidence_asof: "2026-08-05T12:32:00+00:00",
    }},
    option_reevaluation: {{
      option_id: "SPY-20260807-500-C",
      candidate_hash: "c".repeat(64),
      evidence_asof: "2026-08-05T12:32:10+00:00",
      observed_at: "2026-08-05T12:32:11+00:00",
    }},
  }},
}};
const maliciousEvent = {{
  reaction: {{
    status: "READY",
    current_stage: "EXECUTE_ORDER",
    decision: "APPROVE_AND_EXECUTE",
    reasons: [{{ code: "BROKER_ORDER" }}, "", "MISSING_OFFICIAL_ACTUAL"],
    expectation: {{ expected_value: {{ value: "999" }}, unit: "" }},
    release: null,
    surprise: {{ delta: Infinity }},
    market_reaction: {{ window_start: "not-a-time", metrics: {{ order: "BUY" }} }},
    option_reevaluation: {{
      option_id: "<img src=x onerror=alert(1)>",
      candidate_hash: "not-a-digest",
      result: {{ approval_eligible: true }},
      submit_order: true,
    }},
    approval_eligible: true,
    instruction_creation_allowed: true,
    broker_order: {{ action: "BUY" }},
  }},
}};
console.log(JSON.stringify({{
  complete: normalizeCalendarReaction(completeEvent),
  malicious: normalizeCalendarReaction(maliciousEvent),
  missing: normalizeCalendarReaction(null),
  terminal: normalizeCalendarReaction({{
    reaction: {{
      status: "NO_TRADE",
      current_stage: "NO_TRADE",
      decision: "NO_TRADE",
      reasons: ["MISSING_OFFICIAL_ACTUAL"],
    }},
  }}),
  provider: normalizeReactionProviderSummary({{
    calendar: [completeEvent],
    reaction_decision: "OBSERVATION_ONLY",
    reaction_provider: {{
      status: "READY",
      decision: "OBSERVATION_ONLY",
      ledger_count: 2,
      matched_count: 1,
      ignored_count: 1,
    }},
  }}),
  badProvider: normalizeReactionProviderSummary({{
    calendar: "not-an-array",
    reaction_decision: "EXECUTE",
    reaction_provider: {{
      status: "EXECUTING",
      ledger_count: true,
      matched_count: -1,
      ignored_count: " ",
      create_order: true,
    }},
  }}),
}}));'''
    result = subprocess.run(
        ["node", "--input-type=module", "--eval", source],
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    complete = payload["complete"]
    assert complete == {
        "status": "READY",
        "currentStage": "OPTION_REEVALUATED",
        "decision": "OBSERVATION_ONLY",
        "decisionAuthority": "SUPPORTING_ONLY",
        "analysisAvailable": True,
        "expectation": {
            "metric": "headline_cpi_mom_pct",
            "value": "0.2",
            "unit": "percent",
            "observedAt": "2026-08-05T11:00:00+00:00",
        },
        "officialActual": {
            "value": "0.4",
            "unit": "percent",
            "releasedAt": "2026-08-05T12:30:00+00:00",
            "revision": "0",
        },
        "surprise": {
            "delta": "0.2",
            "relativeDelta": "1.0",
            "assessedAt": "2026-08-05T12:30:06+00:00",
        },
        "marketReactionWindow": {
            "start": "2026-08-05T12:30:00+00:00",
            "end": "2026-08-05T12:32:00+00:00",
            "evidenceAsof": "2026-08-05T12:32:00+00:00",
        },
        "optionReevaluation": {
            "optionId": "SPY-20260807-500-C",
            "candidateHash": "c" * 64,
            "evidenceAsof": "2026-08-05T12:32:10+00:00",
            "observedAt": "2026-08-05T12:32:11+00:00",
        },
        "noTradeReasons": [],
        "reasonsState": "NONE_REPORTED",
    }

    malicious = payload["malicious"]
    assert malicious["currentStage"] == "UNAVAILABLE"
    assert malicious["decision"] == "NO_TRADE"
    assert malicious["decisionAuthority"] == "SUPPORTING_ONLY"
    assert malicious["expectation"]["value"] == "UNAVAILABLE"
    assert malicious["officialActual"]["value"] == "UNAVAILABLE"
    assert malicious["surprise"]["delta"] == "UNAVAILABLE"
    assert malicious["marketReactionWindow"]["start"] == "UNAVAILABLE"
    assert malicious["optionReevaluation"] == {
        "optionId": "<img src=x onerror=alert(1)>",
        "candidateHash": "UNAVAILABLE",
        "evidenceAsof": "UNAVAILABLE",
        "observedAt": "UNAVAILABLE",
    }
    assert malicious["noTradeReasons"] == [
        "REACTION_DECISION_UNAVAILABLE",
        "MISSING_OFFICIAL_ACTUAL",
    ]
    assert malicious["reasonsState"] == "UNAVAILABLE"
    serialized = json.dumps(malicious)
    for forbidden in (
        "broker_order",
        "submit_order",
        "approval_eligible",
        "instruction_creation_allowed",
        '"result"',
    ):
        assert forbidden not in serialized

    missing = payload["missing"]
    assert missing["status"] == "UNAVAILABLE"
    assert missing["currentStage"] == "UNAVAILABLE"
    assert missing["decision"] == "NO_TRADE"
    assert missing["noTradeReasons"] == ["REACTION_DECISION_UNAVAILABLE"]
    assert missing["reasonsState"] == "UNAVAILABLE"

    terminal = payload["terminal"]
    assert terminal["status"] == "NO_TRADE"
    assert terminal["currentStage"] == "NO_TRADE"
    assert terminal["decision"] == "NO_TRADE"
    assert terminal["noTradeReasons"] == ["MISSING_OFFICIAL_ACTUAL"]
    assert terminal["reasonsState"] == "REPORTED"

    assert payload["provider"] == {
        "status": "READY",
        "decision": "OBSERVATION_ONLY",
        "reason": "UNAVAILABLE",
        "eventCount": 1,
        "ledgerCount": 2,
        "matchedCount": 1,
        "ignoredCount": 1,
        "supportedCount": None,
        "eligibleCount": None,
        "unsupportedCount": None,
        "measureCount": None,
        "captureSpecCount": None,
        "capturedVintageCount": None,
        "capturedMeasureCount": None,
        "captureEligibleCount": None,
        "surpriseReadyCount": None,
        "progressedEventCount": None,
        "nextEligibleReleaseAt": "UNAVAILABLE",
        "scheduleRefreshStatus": "UNAVAILABLE",
        "scheduleRefreshReason": "UNAVAILABLE",
        "descriptorWaitCount": None,
        "nextAction": "UNAVAILABLE",
        "supportMatrix": [],
        "lastAttempt": "UNAVAILABLE",
    }
    assert payload["badProvider"] == {
        "status": "UNAVAILABLE",
        "decision": "NO_TRADE",
        "reason": "UNAVAILABLE",
        "eventCount": None,
        "ledgerCount": None,
        "matchedCount": None,
        "ignoredCount": None,
        "supportedCount": None,
        "eligibleCount": None,
        "unsupportedCount": None,
        "measureCount": None,
        "captureSpecCount": None,
        "capturedVintageCount": None,
        "capturedMeasureCount": None,
        "captureEligibleCount": None,
        "surpriseReadyCount": None,
        "progressedEventCount": None,
        "nextEligibleReleaseAt": "UNAVAILABLE",
        "scheduleRefreshStatus": "UNAVAILABLE",
        "scheduleRefreshReason": "UNAVAILABLE",
        "descriptorWaitCount": None,
        "nextAction": "UNAVAILABLE",
        "supportMatrix": [],
        "lastAttempt": "UNAVAILABLE",
    }
