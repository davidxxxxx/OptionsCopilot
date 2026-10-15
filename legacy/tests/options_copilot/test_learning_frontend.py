from __future__ import annotations

import json
from pathlib import Path
import re
import subprocess


ROOT = Path(__file__).resolve().parents[2]
FRONTEND = ROOT / "options_copilot" / "frontend"
GOVERNANCE_MESSAGE = (
    "影子学习只到 Discovery；生产治理保持只读锁定。NORMAL 10%，15% A-grade 未解锁，20% 绝对拒绝。"
)


def test_learning_card_is_read_only_and_exposes_governance_contract() -> None:
    html = (FRONTEND / "index.html").read_text(encoding="utf-8")
    script = (FRONTEND / "app.js").read_text(encoding="utf-8")
    styles = (FRONTEND / "styles.css").read_text(encoding="utf-8")
    learning_region = html.split('<section id="learning-region"', 1)[1].split(
        "</section>", 1
    )[0]

    for identifier in (
        "learning-region",
        "learning-gate",
        "learning-stage",
        "learning-samples",
        "learning-record-count",
        "learning-integrity",
        "learning-policy-status",
        "learning-policy-version",
        "learning-policy-hash",
        "learning-policy-marker",
        "learning-authority-head",
        "learning-initial-policy-hash",
        "learning-evaluation-status",
        "learning-evaluation-stage",
        "learning-evaluation-report",
        "learning-evaluation-dataset",
        "learning-evaluation-independence",
        "learning-transition-status",
        "learning-promotion-status",
        "learning-promotion-hash",
        "learning-rollback-status",
        "learning-rollback-hash",
        "learning-rollback-target",
        "learning-a-grade-status",
        "learning-a-grade-proposal",
        "learning-a-grade-marker",
        "learning-a-grade-risk",
        "learning-production-governance",
        "learning-human-signer",
        "learning-risk-contract",
        "learning-risk-authority",
        "learning-creator-transport",
        "learning-governance-reason",
        "learning-summary",
    ):
        assert f'id="{identifier}"' in html
    for identifier in (
        "learning-gate",
        "learning-stage",
        "learning-samples",
        "learning-record-count",
        "learning-integrity",
        "learning-policy-status",
        "learning-evaluation-status",
        "learning-promotion-status",
        "learning-rollback-status",
        "learning-a-grade-status",
        "learning-production-governance",
        "learning-human-signer",
        "learning-risk-contract",
        "learning-creator-transport",
        "learning-summary",
    ):
        assert f'"{identifier}"' in script

    assert GOVERNANCE_MESSAGE in html
    assert GOVERNANCE_MESSAGE in script
    assert "options_copilot.learning.governance.v1" in script
    assert "NO_TRUSTED_HUMAN_SIGNER" in script
    assert "NORMAL 10%" in html
    assert "15% A-grade 未解锁" in html
    assert "20% 绝对拒绝" in html
    assert "CREATOR_TRANSPORT_UNAVAILABLE" in html
    assert "LEARNING_DISCOVERY_SAMPLE_TARGET = 30" in script
    assert ".learning-governance" in styles
    assert '#learning-integrity[data-state="verified"]' in styles
    assert '#learning-integrity[data-state="failed"]' in styles
    assert '#learning-integrity[data-state="unknown"]' in styles

    lowered_region = learning_region.lower()
    for tag in ("button", "input", "select", "textarea", "a"):
        assert re.search(rf"<{tag}(?:\s|>)", lowered_region) is None

    renderer = script.split("function renderLearning", 1)[1].split(
        "function renderModel", 1
    )[0].lower()
    assert ".addeventlistener(" not in renderer
    for authority in ("promote", "unlock", "approval", "bridge", "order"):
        assert authority not in renderer


def test_learning_renderer_handles_current_legacy_and_missing_payloads() -> None:
    script_uri = (FRONTEND / "app.js").resolve().as_uri()
    source = f'''
class ClassList {{
  constructor() {{ this.values = new Set(); }}
  add(...values) {{ values.forEach((value) => this.values.add(value)); }}
  remove(...values) {{ values.forEach((value) => this.values.delete(value)); }}
}}

function simpleNode() {{
  return {{ textContent: "", dataset: {{}}, classList: new ClassList() }};
}}

function modelCard() {{
  const fields = Object.fromEntries(
    ["model-name", "model-ev", "model-calibration", "model-samples"]
      .map((name) => [name, simpleNode()]),
  );
  return {{
    fields,
    querySelector(selector) {{
      const match = selector.match(/^\\[data-field="([^"]+)"\\]$/);
      return match ? fields[match[1]] ?? null : null;
    }},
  }};
}}

const nodes = {{
  "learning-champion": modelCard(),
  "learning-challenger": modelCard(),
  "learning-gate": simpleNode(),
  "learning-stage": simpleNode(),
  "learning-samples": simpleNode(),
  "learning-record-count": simpleNode(),
  "learning-integrity": simpleNode(),
  "learning-policy-status": simpleNode(),
  "learning-policy-version": simpleNode(),
  "learning-policy-hash": simpleNode(),
  "learning-policy-marker": simpleNode(),
  "learning-authority-head": simpleNode(),
  "learning-initial-policy-hash": simpleNode(),
  "learning-evaluation-status": simpleNode(),
  "learning-evaluation-stage": simpleNode(),
  "learning-evaluation-report": simpleNode(),
  "learning-evaluation-dataset": simpleNode(),
  "learning-evaluation-independence": simpleNode(),
  "learning-transition-status": simpleNode(),
  "learning-promotion-status": simpleNode(),
  "learning-promotion-hash": simpleNode(),
  "learning-rollback-status": simpleNode(),
  "learning-rollback-hash": simpleNode(),
  "learning-rollback-target": simpleNode(),
  "learning-a-grade-status": simpleNode(),
  "learning-a-grade-proposal": simpleNode(),
  "learning-a-grade-marker": simpleNode(),
  "learning-a-grade-risk": simpleNode(),
  "learning-production-governance": simpleNode(),
  "learning-human-signer": simpleNode(),
  "learning-risk-contract": simpleNode(),
  "learning-risk-authority": simpleNode(),
  "learning-creator-transport": simpleNode(),
  "learning-governance-reason": simpleNode(),
  "learning-summary": simpleNode(),
}};

globalThis.document = {{
  addEventListener() {{}},
  getElementById(id) {{ return nodes[id] ?? null; }},
}};

const {{ renderLearning }} = await import("{script_uri}");

function snapshot() {{
  return {{
    challengerName: nodes["learning-challenger"].fields["model-name"].textContent,
    challengerSamples: nodes["learning-challenger"].fields["model-samples"].textContent,
    gate: nodes["learning-gate"].textContent,
    stage: nodes["learning-stage"].textContent,
    samples: nodes["learning-samples"].textContent,
    recordCount: nodes["learning-record-count"].textContent,
    integrity: nodes["learning-integrity"].textContent,
    integrityState: nodes["learning-integrity"].dataset.state,
    summary: nodes["learning-summary"].textContent,
  }};
}}

function governanceSnapshot() {{
  return {{
    policyStatus: nodes["learning-policy-status"].textContent,
    policyVersion: nodes["learning-policy-version"].textContent,
    policyHash: nodes["learning-policy-hash"].textContent,
    policyMarker: nodes["learning-policy-marker"].textContent,
    authorityHead: nodes["learning-authority-head"].textContent,
    initialPolicyHash: nodes["learning-initial-policy-hash"].textContent,
    evaluationStatus: nodes["learning-evaluation-status"].textContent,
    evaluationStage: nodes["learning-evaluation-stage"].textContent,
    evaluationReport: nodes["learning-evaluation-report"].textContent,
    evaluationDataset: nodes["learning-evaluation-dataset"].textContent,
    evaluationIndependence: nodes["learning-evaluation-independence"].textContent,
    transitionStatus: nodes["learning-transition-status"].textContent,
    promotionStatus: nodes["learning-promotion-status"].textContent,
    promotionHash: nodes["learning-promotion-hash"].textContent,
    rollbackStatus: nodes["learning-rollback-status"].textContent,
    rollbackHash: nodes["learning-rollback-hash"].textContent,
    rollbackTarget: nodes["learning-rollback-target"].textContent,
    aGradeStatus: nodes["learning-a-grade-status"].textContent,
    aGradeProposal: nodes["learning-a-grade-proposal"].textContent,
    aGradeMarker: nodes["learning-a-grade-marker"].textContent,
    aGradeRisk: nodes["learning-a-grade-risk"].textContent,
    productionGovernance: nodes["learning-production-governance"].textContent,
    humanSigner: nodes["learning-human-signer"].textContent,
    riskContract: nodes["learning-risk-contract"].textContent,
    riskAuthority: nodes["learning-risk-authority"].textContent,
    creatorTransport: nodes["learning-creator-transport"].textContent,
    reason: nodes["learning-governance-reason"].textContent,
  }};
}}

renderLearning({{
  challenger: "challenger-string-v7",
  message: "provider text must not replace governance copy",
  shadow_learning: {{
    stage: "DISCOVERY",
    mode: "PRODUCTION",
    independent_samples: 30,
    minimum_discovery_scenarios: 99,
    record_count: 84,
    ledger: {{ integrity_verified: true }},
    challengers: ["lower-priority-v6"],
  }},
}});
const current = snapshot();

renderLearning({{
  shadow_learning: {{
    stage: "COLLECTING",
    independent_samples: 7,
    record_count: 9,
    ledger: {{ integrity_verified: false }},
    challengers: ["shadow-list-v3"],
  }},
}});
const shadowFallback = snapshot();

renderLearning({{
  models: {{ challenger: {{ name: "legacy-object-v2", independent_samples: 12 }} }},
}});
const legacy = snapshot();

renderLearning({{
  challenger: 27,
  summary: "promote now",
  shadow_learning: {{
    stage: "PROMOTED",
    mode: "LIVE",
    independent_samples: -1,
    record_count: "not-a-count",
    ledger: {{ integrity_verified: "yes" }},
  }},
}});
const missing = snapshot();

const digest = (character) => character.repeat(64);
const readyGovernance = {{
  schema: "options_copilot.learning.governance.v1",
  status: "READY",
  current_policy: {{
    status: "VERIFIED",
    version: "v2",
    hash: digest("a"),
    authority_marker_hash: digest("b"),
    authority_head_hash: digest("c"),
    immutable_initial_policy_hash: digest("d"),
  }},
  evaluation: {{
    status: "AVAILABLE",
    report_hash: digest("e"),
    dataset_hash: digest("f"),
    independence_spec_hash: digest("1"),
    independent_count: 34,
    stage: "DISCOVERY",
  }},
  promotion: {{ status: "APPROVED", authority_hash: digest("2") }},
  rollback: {{
    status: "APPLIED",
    authority_hash: digest("3"),
    target_policy_hash: digest("a"),
  }},
  a_grade: {{
    status: "APPROVED",
    marker_hash: digest("5"),
    proposal_id: "proposal-7",
    proposal_hash: digest("6"),
    candidate_hash: digest("9"),
    ranking_basis_hash: digest("7"),
    current_policy_version: "v2",
    current_policy_hash: digest("a"),
    policy_authority_marker_hash: digest("b"),
    execution_cost_version: "cost-v1",
    execution_cost_hash: digest("0"),
    evaluation_report_hash: digest("e"),
    dataset_hash: digest("f"),
    independence_spec_hash: digest("1"),
    risk_contract_hash: digest("4"),
    max_risk_fraction: 0.15,
  }},
  authority: {{
    human_signer_status: "TRUSTED",
    read_only: true,
    can_sign: false,
    can_auto_promote: false,
    approval_authority: false,
    bridge_authority: false,
    order_authority: false,
  }},
  risk: {{
    normal_max_fraction: 0.10,
    a_grade_max_fraction: 0.15,
    absolute_reject_fraction: 0.20,
    authority_version: "risk-v1",
    authority_marker_hash: digest("8"),
  }},
}};
renderLearning({{
  creator_transport_status: "CREATOR_TRANSPORT_UNAVAILABLE",
  governance: readyGovernance,
}});
const governance = governanceSnapshot();

renderLearning({{
  creator_transport_status: "CREATOR_TRANSPORT_UNAVAILABLE",
  governance: {{
    ...readyGovernance,
    status: "BLOCKED",
    reason: "GOVERNANCE_BLOCKED",
  }},
}});
const failClosed = governanceSnapshot();

renderLearning({{
  creator_transport_status: "CREATOR_TRANSPORT_UNAVAILABLE",
  governance: {{
    ...readyGovernance,
    status: "BLOCKED",
    reason: "NO_TRUSTED_HUMAN_SIGNER",
    authority: {{
      ...readyGovernance.authority,
      human_signer_status: "NO_TRUSTED_HUMAN_SIGNER",
    }},
  }},
}});
const noSigner = governanceSnapshot();

renderLearning({{
  governance: {{
    ...readyGovernance,
    status: "TEST_ONLY",
    reason: "TEST_ONLY_AUTHORITY",
  }},
}});
const testOnly = governanceSnapshot();

renderLearning({{
  governance: {{
    ...readyGovernance,
    authority: {{
      human_signer_status: "TRUSTED",
      read_only: true,
      can_sign: false,
    }},
  }},
}});
const incompleteAuthority = governanceSnapshot();

console.log(JSON.stringify({{ current, shadowFallback, legacy, missing, governance, failClosed, noSigner, testOnly, incompleteAuthority }}));
'''
    result = subprocess.run(
        ["node", "--input-type=module", "--eval", source],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    rendered = json.loads(result.stdout)

    assert rendered["current"] == {
        "challengerName": "challenger-string-v7",
        "challengerSamples": "30",
        "gate": "SHADOW_ONLY · DISCOVERY",
        "stage": "DISCOVERY",
        "samples": "30 / 30",
        "recordCount": "84",
        "integrity": "已验证",
        "integrityState": "verified",
        "summary": GOVERNANCE_MESSAGE,
    }
    assert rendered["shadowFallback"] == {
        "challengerName": "shadow-list-v3",
        "challengerSamples": "7",
        "gate": "SHADOW_ONLY · COMPARISON_AVAILABLE",
        "stage": "COMPARISON_AVAILABLE",
        "samples": "7 / 30",
        "recordCount": "9",
        "integrity": "验证失败",
        "integrityState": "failed",
        "summary": GOVERNANCE_MESSAGE,
    }
    assert rendered["legacy"] == {
        "challengerName": "legacy-object-v2",
        "challengerSamples": "12",
        "gate": "SHADOW_ONLY · COLLECTING",
        "stage": "COLLECTING",
        "samples": "-- / 30",
        "recordCount": "--",
        "integrity": "未提供",
        "integrityState": "unknown",
        "summary": GOVERNANCE_MESSAGE,
    }
    assert rendered["missing"] == {
        "challengerName": "暂无影子模型",
        "challengerSamples": "--",
        "gate": "SHADOW_ONLY · COLLECTING",
        "stage": "COLLECTING",
        "samples": "-- / 30",
        "recordCount": "--",
        "integrity": "未提供",
        "integrityState": "unknown",
        "summary": GOVERNANCE_MESSAGE,
    }
    assert rendered["governance"] == {
        "policyStatus": "VERIFIED",
        "policyVersion": "v2",
        "policyHash": "aaaaaaaa…aaaaaa",
        "policyMarker": "bbbbbbbb…bbbbbb",
        "authorityHead": "cccccccc…cccccc",
        "initialPolicyHash": "dddddddd…dddddd",
        "evaluationStatus": "AVAILABLE",
        "evaluationStage": "DISCOVERY · 34 independent",
        "evaluationReport": "eeeeeeee…eeeeee",
        "evaluationDataset": "ffffffff…ffffff",
        "evaluationIndependence": "11111111…111111",
        "transitionStatus": "AUTHORITY-RECORDED · READ_ONLY",
        "promotionStatus": "APPROVED",
        "promotionHash": "22222222…222222",
        "rollbackStatus": "APPLIED",
        "rollbackHash": "33333333…333333",
        "rollbackTarget": "aaaaaaaa…aaaaaa",
        "aGradeStatus": "APPROVED · PROPOSAL-BOUND",
        "aGradeProposal": "proposal-7 · 66666666…666666",
        "aGradeMarker": "55555555…555555",
        "aGradeRisk": "15.0% PROPOSAL-BOUND",
        "productionGovernance": "LOCKED · READ_ONLY",
        "humanSigner": "TRUSTED",
        "riskContract": "NORMAL 10.0% · A-GRADE 15.0% LOCKED · 20.0% REJECT",
        "riskAuthority": "risk-v1 · 88888888…888888",
        "creatorTransport": "CREATOR_TRANSPORT_UNAVAILABLE",
        "reason": "READY · NO BLOCKER",
    }
    assert rendered["failClosed"] == {
        "policyStatus": "UNAVAILABLE · LOCKED",
        "policyVersion": "UNAVAILABLE",
        "policyHash": "UNAVAILABLE",
        "policyMarker": "UNAVAILABLE",
        "authorityHead": "UNAVAILABLE",
        "initialPolicyHash": "UNAVAILABLE",
        "evaluationStatus": "UNAVAILABLE · LOCKED",
        "evaluationStage": "UNAVAILABLE",
        "evaluationReport": "UNAVAILABLE",
        "evaluationDataset": "UNAVAILABLE",
        "evaluationIndependence": "UNAVAILABLE",
        "transitionStatus": "LOCKED · READ_ONLY",
        "promotionStatus": "UNAVAILABLE · LOCKED",
        "promotionHash": "UNAVAILABLE",
        "rollbackStatus": "UNAVAILABLE · LOCKED",
        "rollbackHash": "UNAVAILABLE",
        "rollbackTarget": "UNAVAILABLE",
        "aGradeStatus": "UNAVAILABLE · LOCKED",
        "aGradeProposal": "UNAVAILABLE",
        "aGradeMarker": "UNAVAILABLE",
        "aGradeRisk": "15% LOCKED",
        "productionGovernance": "LOCKED · READ_ONLY",
        "humanSigner": "TRUSTED",
        "riskContract": "UNAVAILABLE · LOCKED",
        "riskAuthority": "UNAVAILABLE",
        "creatorTransport": "CREATOR_TRANSPORT_UNAVAILABLE",
        "reason": "GOVERNANCE_BLOCKED",
    }
    assert rendered["noSigner"]["policyStatus"] == "VERIFIED"
    assert rendered["noSigner"]["policyVersion"] == "v2"
    assert rendered["noSigner"]["policyHash"] == "aaaaaaaa…aaaaaa"
    assert rendered["noSigner"]["riskContract"] == (
        "NORMAL 10.0% · A-GRADE 15.0% LOCKED · 20.0% REJECT"
    )
    assert rendered["noSigner"]["riskAuthority"] == "risk-v1 · 88888888…888888"
    assert rendered["noSigner"]["promotionStatus"] == "UNAVAILABLE · LOCKED"
    assert rendered["noSigner"]["aGradeStatus"] == "UNAVAILABLE · LOCKED"
    assert rendered["noSigner"]["humanSigner"] == "NO_TRUSTED_HUMAN_SIGNER"
    assert rendered["noSigner"]["reason"] == "NO_TRUSTED_HUMAN_SIGNER"

    for state_name, expected_signer, expected_reason in (
        ("testOnly", "TRUSTED", "TEST_ONLY_AUTHORITY"),
        ("incompleteAuthority", "TRUSTED", "UNAVAILABLE · LOCKED"),
    ):
        state = rendered[state_name]
        assert state["policyStatus"] == "UNAVAILABLE · LOCKED"
        assert state["evaluationStatus"] == "UNAVAILABLE · LOCKED"
        assert state["promotionStatus"] == "UNAVAILABLE · LOCKED"
        assert state["rollbackStatus"] == "UNAVAILABLE · LOCKED"
        assert state["aGradeStatus"] == "UNAVAILABLE · LOCKED"
        assert state["riskContract"] == "UNAVAILABLE · LOCKED"
        assert state["transitionStatus"] == "LOCKED · READ_ONLY"
        assert state["productionGovernance"] == "LOCKED · READ_ONLY"
        assert state["humanSigner"] == expected_signer
        assert state["reason"] == expected_reason
