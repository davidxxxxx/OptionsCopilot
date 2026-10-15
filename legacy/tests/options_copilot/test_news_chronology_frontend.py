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
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    return json.loads(result.stdout)


def test_news_chronology_surface_is_read_only_complete_and_responsive() -> None:
    html = (FRONTEND / "index.html").read_text(encoding="utf-8")
    script = (FRONTEND / "app.js").read_text(encoding="utf-8")
    styles = (FRONTEND / "styles.css").read_text(encoding="utf-8")
    chronology = html.split('<section id="news-chronology-panel"', 1)[1].split(
        "</section>", 1
    )[0]

    for identifier in (
        "daily-major-news-list",
        "this-week-major-events-list",
        "next-week-outlook-list",
        "news-classifier-summary",
        "company-ir-source-health",
        "finnhub-news-source-health",
        "alpha-vantage-source-health",
        "finnhub-calendar-source-health",
        "calendar-list-summary",
    ):
        assert identifier in html
        assert identifier in script
    for label in ("预计发生", "实际发布/释放", "来源发布时间"):
        assert label in script
    assert "DeepSeek shadow" in html
    assert "生产规则" in html
    for class_name in (
        ".news-chronology-grid",
        ".chronology-card",
        ".impact-critical",
        ".impact-high",
        ".impact-medium",
        ".impact-low",
    ):
        assert class_name in styles
    assert "@media (max-width: 760px)" in styles
    assert ".news-chronology-grid { grid-template-columns: 1fr; }" in styles
    assert "CALENDAR_DETAIL_LIMIT = 50" in script
    assert ".slice(0, CALENDAR_DETAIL_LIMIT)" in script
    assert "SUPPORTING_ONLY" in chronology
    assert "innerHTML" not in script
    for forbidden in (
        'type="submit"',
        'data-action="approve"',
        "ibkr-review-link",
        "management-review-action",
    ):
        assert forbidden not in chronology


def test_daily_major_news_uses_beijing_day_and_descending_impact() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''globalThis.document = {{ addEventListener() {{}} }};
const {{ dailyMajorNews }} = await import("{script_uri}");
const row = (id, score, publishedAt, confidence = 0.5) => ({{
  id,
  confidence,
  scores: {{ event_impact_score: score }},
  times: {{ published_at: publishedAt }},
}});
const rows = dailyMajorNews([
  row("medium", 60, "2026-08-10T08:00:00Z", 0.9),
  row("critical", 90, "2026-08-10T06:00:00Z", 0.7),
  row("next-beijing-day", 99, "2026-08-10T16:30:00Z", 1),
  row("same-score-low-confidence", 90, "2026-08-10T07:00:00Z", 0.6),
], new Date("2026-08-10T10:00:00Z"));
console.log(JSON.stringify(rows.map((item) => item.id)));'''

    assert _run_node(source) == [
        "critical",
        "same-score-low-confidence",
        "medium",
    ]


def test_daily_major_news_defensively_folds_only_exact_stories() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''globalThis.document = {{ addEventListener() {{}} }};
const {{ dailyMajorNews }} = await import("{script_uri}");
const row = (id, score, publishedAt, url) => ({{
  id,
  title: "Exact shared headline",
  source: "Reuters",
  confidence: 0.8,
  scores: {{ event_impact_score: score }},
  times: {{ published_at: publishedAt }},
  evidence: [{{ url }}],
}});
const rows = dailyMajorNews([
  row("duplicate-lower", 90, "2026-08-10T08:00:00Z", "https://example.test/story?b=2&a=1"),
  row("duplicate-winner", 91, "2026-08-10T08:00:00Z", "https://EXAMPLE.test/story?a=1&b=2#fragment"),
  row("different-url", 80, "2026-08-10T08:00:00Z", "https://example.test/other"),
  row("different-time", 70, "2026-08-10T08:01:00Z", "https://example.test/story?a=1&b=2"),
], new Date("2026-08-10T10:00:00Z"));
console.log(JSON.stringify(rows.map((item) => item.id)));'''

    assert _run_node(source) == [
        "duplicate-winner",
        "different-url",
        "different-time",
    ]


def test_daily_major_news_limits_unverified_source_category_domination() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''globalThis.document = {{ addEventListener() {{}} }};
const {{ dailyMajorNews }} = await import("{script_uri}");
const row = (
  id,
  score,
  sourceName,
  category,
  bindingStatus,
  symbols = [],
  affectedReason = null,
) => ({{
  id,
  title: `Distinct ${{id}}`,
  source: sourceName,
  category,
  symbols,
  symbol_binding: {{ status: bindingStatus }},
  intelligence: {{ affected_assets: {{ reason: affectedReason }} }},
  confidence: 0.8,
  scores: {{ event_impact_score: score }},
  times: {{ published_at: `2026-08-10T0${{score % 10}}:00:00Z` }},
}});
const rows = dailyMajorNews([
  row("sec-1", 99, "SEC", "REGULATORY", "SOURCE_DECLARED", [], "SYMBOL_BINDING_UNVERIFIED"),
  row("sec-2", 98, "SEC", "REGULATORY", "SOURCE_DECLARED", [], "SYMBOL_BINDING_UNVERIFIED"),
  row("sec-cross-category", 97, "SEC", "MACRO", "SOURCE_DECLARED", [], "SYMBOL_BINDING_UNVERIFIED"),
  row("sec-legacy", 96, "SEC", "REGULATORY", "UNVERIFIED_PROVIDER_BINDING"),
  row("macro", 80, "JIN10", "MACRO", "UNBOUND"),
  row("verified-a", 75, "SEC", "REGULATORY", "VERIFIED_PROVIDER_RELATED", ["AAA"]),
  row("verified-b", 74, "SEC", "REGULATORY", "VERIFIED_PROVIDER_RELATED", ["BBB"]),
], new Date("2026-08-10T10:00:00Z"));
console.log(JSON.stringify(rows.map((item) => item.id)));'''

    assert _run_node(source) == [
        "sec-1",
        "sec-2",
        "macro",
        "verified-a",
        "verified-b",
    ]


def test_periodic_news_refresh_updates_the_global_refresh_clock() -> None:
    script = (FRONTEND / "app.js").read_text(encoding="utf-8")
    refresh_news = script.split("async function refreshNewsData()", 1)[1].split(
        "async function refreshLearningSnapshot", 1
    )[0]

    assert "function markReadOnlyRefresh(" in script
    assert "markReadOnlyRefresh();" in refresh_news
    assert "finally" in refresh_news


def test_classifier_coverage_separates_production_rules_from_deepseek_shadow() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''globalThis.document = {{ addEventListener() {{}} }};
const {{ classifierCoverage }} = await import("{script_uri}");
console.log(JSON.stringify(classifierCoverage([
  {{ classifier: "DETERMINISTIC_RULES", research_advisory: {{ classifier: "STRUCTURED_LLM" }} }},
  {{ classifier: "DETERMINISTIC_RULES" }},
  {{ classifier: "STRUCTURED_LLM" }},
  {{ classifier: null }},
])));'''

    assert _run_node(source) == {
        "total": 4,
        "deepseekShadow": 1,
        "structuredPrimary": 1,
        "deterministic": 2,
        "comparableShadow": 0,
        "shadowPredictions": 0,
        "undeclared": 1,
    }


def test_deepseek_advisory_summary_is_explicitly_shadow_and_supporting_only() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''globalThis.document = {{ addEventListener() {{}} }};
const {{ deepseekAdvisorySummary }} = await import("{script_uri}");
console.log(JSON.stringify({{
  available: deepseekAdvisorySummary({{
    research_advisory: {{
      classifier: "STRUCTURED_LLM",
      research_priority_score: 91.25,
      classification: {{ direction: "BULLISH", horizon: "DAYS_1_3", confidence: 0.82 }},
    }},
  }}),
  missing: deepseekAdvisorySummary({{ classifier: "DETERMINISTIC_RULES" }}),
}}));'''

    assert _run_node(source) == {
        "available": "BULLISH · DAYS_1_3 · 置信度 82.0 / 100 · 研究优先 91.3 · SUPPORTING_ONLY",
        "missing": "未覆盖",
    }


def test_deepseek_runtime_summary_separates_shadow_output_from_legacy_adapter() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''globalThis.document = {{ addEventListener() {{}} }};
const {{ deepseekRuntimeSummary }} = await import("{script_uri}");
console.log(JSON.stringify(deepseekRuntimeSummary(
  {{ model_state: "FALLBACK", fallback_reason: "MODEL_EVALUATION_PENDING" }},
  {{
    shadow_advisory: {{ status: "READY", advisory_count: 1 }},
    news: [
      {{ classifier: "DETERMINISTIC_RULES", research_advisory: {{ classifier: "STRUCTURED_LLM", shadow_prediction_count: 5 }} }},
      {{ classifier: "DETERMINISTIC_RULES" }},
    ],
  }},
)));'''

    assert _run_node(source) == {
        "shadowStatus": "READY",
        "advisoryCount": 1,
        "rowAdvisoryCount": 1,
        "total": 2,
        "deterministic": 2,
        "shadowPredictions": 5,
        "modelOutputAvailable": True,
        "countMismatch": False,
        "legacyModelState": "FALLBACK",
        "legacyFallbackReason": "MODEL_EVALUATION_PENDING",
        "failureReasons": {},
    }


def test_durable_shadow_counts_are_separate_from_current_news_coverage() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''globalThis.document = {{ addEventListener() {{}} }};
const {{ durableShadowCounts, durableShadowCountText }} = await import("{script_uri}");
const verified = durableShadowCounts({{
  shadow_learning: {{
    record_counts: {{ THESIS: 1, EVIDENCE: 261, PREDICTION: 1560, OUTCOME: 0 }},
    ledger: {{ integrity_verified: true }},
  }},
}});
const missing = durableShadowCounts({{}});
const unverified = durableShadowCounts({{
  shadow_learning: {{
    record_counts: {{ THESIS: 1, EVIDENCE: 2, PREDICTION: 0, OUTCOME: 0 }},
    ledger: {{ integrity_verified: false }},
  }},
}});
console.log(JSON.stringify({{
  verified,
  verifiedPredictions: durableShadowCountText(verified, "predictions"),
  verifiedOutcomes: durableShadowCountText(verified, "outcomes"),
  missingPredictions: durableShadowCountText(missing, "predictions"),
  unverifiedPredictions: durableShadowCountText(unverified, "predictions"),
}}));'''

    assert _run_node(source) == {
        "verified": {
            "available": True,
            "integrityVerified": True,
            "theses": 1,
            "evidence": 261,
            "predictions": 1560,
            "outcomes": 0,
            "challenger": "UNAVAILABLE",
            "legacyExcluded": None,
            "exclusionReasons": {},
        },
        "verifiedPredictions": "1,560",
        "verifiedOutcomes": "0",
        "missingPredictions": "--",
        "unverifiedPredictions": "--",
    }


def test_calendar_digest_and_time_projection_preserve_priority_and_time_roles() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''globalThis.document = {{ addEventListener() {{}} }};
const {{ calendarDigestRows, eventTimeProjection }} = await import("{script_uri}");
const row = (id, importance, precision, eventAt, windows, reaction = null) => ({{
  id,
  importance,
  category: "EARNINGS",
  schedule_precision: precision,
  times: {{ event_at: eventAt, published_at: "2026-08-01T12:00:00Z" }},
  windows,
  reaction,
}});
const rows = [
  row("medium-exact", "MEDIUM", "EXACT", "2026-08-12T12:00:00Z", ["THIS_WEEK"]),
  row("critical-date-only", "CRITICAL", "DATE_ONLY", "2026-08-13T12:00:00Z", ["THIS_WEEK"]),
  row("critical-exact-later", "CRITICAL", "EXACT", "2026-08-14T12:00:00Z", ["THIS_WEEK"]),
  row("critical-exact-earlier", "CRITICAL", "EXACT", "2026-08-11T12:00:00Z", ["THIS_WEEK"], {{
    release: {{ released_at: "2026-08-11T12:00:03Z" }},
  }}),
  row("next-week", "CRITICAL", "EXACT", "2026-08-18T12:00:00Z", ["NEXT_WEEK"]),
];
rows.push({{
  ...row("macro-critical-date-only", "CRITICAL", "DATE_ONLY", "2026-08-15T12:00:00Z", ["THIS_WEEK"]),
  category: "MACRO",
}});
console.log(JSON.stringify({{
  ids: calendarDigestRows(rows, "this-week").map((item) => item.id),
  projection: eventTimeProjection(rows[3]),
}}));'''

    assert _run_node(source) == {
        "ids": [
            "macro-critical-date-only",
            "critical-exact-earlier",
            "critical-exact-later",
            "critical-date-only",
            "medium-exact",
        ],
        "projection": {
            "expected": "2026-08-11T12:00:00Z",
            "actual": "2026-08-11T12:00:03Z",
            "published": "2026-08-01T12:00:00Z",
            "precision": "EXACT",
        },
    }
