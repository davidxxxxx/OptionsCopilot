# AI-SPEC — Internal Phase P9: Champion/Challenger Governance

> AI design contract generated with `gsd-ai-integration-phase` for the internal Options Copilot P9 plan. The repository-wide `.planning` Phase 9 is unrelated. This contract is consumed by the Options Copilot implementation and evaluation work.

---

## 1. System Classification

**System Type:** Hybrid — deterministic governance/evaluation pipeline with supporting-only LLM analysis

**Description:**
Options Copilot is a human-gated US-options research agent. It combines deterministic broker, market, risk, cost, policy, and authority checks with optional model-assisted news and event interpretation. Good output is timely, evidence-linked, cost-aware, reproducible, and visibly fails closed when any authoritative input is stale, incomplete, conflicting, or untrusted. The model never signs governance records, unlocks risk, creates approval authority, or submits orders.

**Critical Failure Modes:**
1. Creating a second combination, an unlimited-risk structure, or a proposal whose maximum loss cannot be calculated exactly before instruction creation.
2. Treating model output, UI/API input, TEST_ONLY evidence, or 30 Discovery samples as production promotion, rollback, or proposal-specific A-grade authority.
3. Authorizing against stale or mismatched policy, risk, execution-cost, ranking, broker, quote, evaluation, dataset, or independence evidence.
4. Retrying an uncertain creator result or allowing any component to submit an IBKR order instead of creating a review-only instruction after human double confirmation.
5. Optimizing nominal win rate while hiding spread, slippage, IV crush, tail loss, drawdown, dependence, or data leakage.

---

## 1b. Domain Context

**Industry Vertical:** Financial services — US listed options research and human-gated brokerage workflow

**User Population:** One self-directed retail options trader using IBKR Level 3 permissions. The model is supporting-only; the account owner retains proposal approval, governance signing, review-instruction approval, and final confirmation inside IBKR.

**Stakes Level:** High

**Output Consequence:** Research may lead the account owner to approve creation of a review-only IBKR instruction and later confirm or reject it inside IBKR. A bad output can cause avoidable spread/slippage, the permitted maximum loss, early assignment or exercise consequences, an unintended stock/cash obligation, or promotion of a weaker policy. The system has no automatic order-submission authority.

### What Domain Experts Evaluate Against

Dimension: Defined payoff and account fit
Good (domain expert would accept): Every leg is bound to an authoritative standard US-option contract identity, side, ratio, multiplier, expiry, strike, and right; an independent payoff reconstruction proves exact maximum loss after estimated costs and agrees with the displayed payoff. Naked calls/puts, uncovered ratio legs, unlimited risk, and unquantifiable loss are rejected. New exposure is one open combination, at least 7 DTE (normally 14–35), and is measured against current strategy NAV: NORMAL is at most 10%; only a separately signed, proposal-bound human A-grade decision may permit more than 10% and never more than 15%; 20% is an absolute reject boundary. The output also shows the account permission/position basis and remains review-only through human double confirmation.
Bad (domain expert would flag): A missing or adjusted-contract field, wrong multiplier or leg sign, payoff sampled only at a few prices, ignored fees/assignment obligation, a second combination, sub-7-DTE exposure, or any model/UI claim that relaxes the cap, supplies suitability, signs authority, or creates/submits an order.
Stakes: Critical
Source: OCC/OIC Characteristics and Risks of Standardized Options; FINRA Rule 2360 options disclosure, account-approval, authorization, and suitability standards (directly binding only in their regulated member context); repository P9 risk/authority contract.

Dimension: Executable price and total trading cost
Good (domain expert would accept): Each leg has a current executable bid and ask from an entitled source at approval and bridge time, with source, market-data type, timestamp, size/availability, and contract identity visible. The reviewer sees bid-ask width, realistic adverse-fill/slippage and commission/fee assumptions, cost-adjusted expected value, and sensitivity to a worse fill; a crossed, one-sided, stale, delayed/frozen, incomplete, or economically prohibitive market produces `NO_TRADE`. Discovery data may narrow the universe but cannot price an instruction.
Bad (domain expert would flag): Pricing a combination from last/close/theoretical values or an unlabelled midpoint, treating volume/open interest as a fill guarantee, omitting a wide leg or exchange/commission costs, using a delayed quote as live, or recommending a trade whose apparent edge disappears under a plausible fill.
Stakes: High
Source: OCC/OIC options risk and transaction-cost disclosure; IBKR API market-data subscription/type documentation; established listed-options execution practice.

Dimension: Volatility, event, and contract-lifecycle exposure
Good (domain expert would accept): The thesis names the earnings/news/ex-dividend catalyst and timing, distinguishes price direction from implied-volatility level, term structure/skew, and expected post-event volatility crush, and reports position-level delta, gamma, theta, and vega with adverse price/volatility/time scenarios. Every short American-style leg is reviewed for early assignment, exercise-by-exception, ex-dividend, halt/corporate-action, settlement, and resulting stock/cash obligations, with a feasible exit or manual-handling plan.
Bad (domain expert would flag): A directional story with no IV/crush or Greek exposure, Greeks copied without quote/model time and assumptions, treating a defined-risk spread as eliminating leg assignment, or missing an ex-dividend/earnings date, expiration convention, settlement style, halt, or corporate action that changes the operational outcome.
Stakes: Critical
Source: OIC Volatility & the Greeks; OIC Options Assignment FAQ; OCC/OIC Characteristics and Risks of Standardized Options.

Dimension: Point-in-time evidence and market-data fitness
Good (domain expert would accept): The decision freezes authoritative contract definitions; per-leg quotes and quote type; positions, working orders, NAV, and session times; event `published_at`, `first_seen_at`, and `ingested_at`; provider health; policy/cost/ranking versions and hashes; and all conflict/missing-data flags. Replay uses only facts observable at decision time, official SEC/company material anchors event facts, conflicting sources fail closed, and market-data entitlements, API-use certification, and non-redistribution limits are respected.
Bad (domain expert would flag): A mixed-age “snapshot,” retrospective earnings/news corrections leaking into replay, an aggregator silently overwriting an official conflict, stale policy or broker state, unverifiable source/hash/time, incomplete chain data presented as complete, or unentitled/shared/delayed data represented as execution-grade.
Stakes: Critical
Source: IBKR market-data subscription and data-type documentation; repository point-in-time evidence, freshness, replay, and fail-closed contracts.

Dimension: Forecast reliability and loss containment
Good (domain expert would accept): Champion and Challenger are scored on the same independent, point-in-time, non-duplicated cases and on net-of-spread/slippage/fees outcomes. A stated probability is reliable in plain terms (for example, outcomes labelled near 60% occur near 60% over comparable cases), and the report shows sample size, uncertainty, scenario breakdown, worst/tail loss, maximum drawdown, loss concentration, and stability across time, symbols, strategy families, DTE, and event regimes. Promotion requires material improvement over the Champion and `NO_TRADE` baseline without worsening calibration or downside, plus a credible rollback trigger; 30 Discovery samples alone cannot promote. Promotion/rollback and proposal-specific A-grade are separate, exact-hash-bound human decisions.
Bad (domain expert would flag): Promotion from headline win rate, average P&L, or a few overlapping earnings cases; selection/look-ahead leakage; confidence that materially overstates realized frequency; hidden drawdown, tail loss, dependence, or cost sensitivity; model/UI self-promotion; or stale, mismatched, tampered, or revoked authority accepted after the fact.
Stakes: Critical
Source: Established financial model-validation and champion/challenger practice; repository P8 independence specification and P9 promotion/rollback/A-grade contract.

### Known Failure Modes in This Domain

1. **False bounded-risk structure:** A wrong multiplier, sign, ratio, adjusted contract, or leg pairing makes the displayed maximum loss smaller than the actual obligation or leaves a naked short leg after partial fill, exercise, or assignment.
2. **Paper edge that cannot be executed:** Stale/delayed midpoint pricing, thin size, wide multi-leg markets, commissions, and adverse legging/slippage turn positive research EV into a negative executable trade; quote incompleteness can be mistaken for opportunity.
3. **Event/lifecycle surprise:** Earnings IV crush, a changed event date, ex-dividend early assignment, expiration exercise-by-exception, a trading halt, or a corporate action creates loss or an unintended stock/cash position that the directional thesis did not model.
4. **Unsafe Challenger promotion:** Duplicated/dependent samples, future-data leakage, regime concentration, poor probability calibration, or an ignored tail loss/drawdown makes a Challenger look superior; an automatic or stale-hash promotion then propagates the weaker policy into ranking and approval.

### Regulatory / Compliance Context

- **Current internal boundary:** This is single-user, self-directed research and not legal, tax, or investment advice. The model is supporting-only, may return `NO_TRADE`, and cannot determine suitability, approve an options account, exercise discretion, sign governance authority, create an order, or submit an order. The IBKR review instruction appears only after human double confirmation, and final action remains inside IBKR.
- **Options disclosure and authorization:** OCC's current *Characteristics and Risks of Standardized Options* is the baseline risk disclosure. FINRA Rule 2360 requires member firms, among other things, to deliver the current ODD, approve options accounts, control discretionary authority, and apply options suitability standards when recommendations are made. Those duties are not asserted to apply automatically to this personal internal tool; any broker-dealer, adviser, client-facing, or multi-user deployment requires qualified compliance/legal classification before use.
- **Records:** FINRA Rule 4511 and SEC Rule 17a-4 impose books-and-records duties on covered members, brokers, and dealers, not automatically on this account owner's private tool. P9 nevertheless keeps append-only, hash-bound decision inputs, evaluation reports, human actor/time/signatures, promotion/rollback/A-grade authorities, quote/data provenance, and creator outcomes so a reviewer can reproduce and repudiate neither the evidence nor the human decision. Retention and electronic-record format must be re-mapped before any regulated-firm deployment.
- **Market data:** IBKR API data depends on subscription/entitlement, professional/non-professional classification, API-use acknowledgements/certification, market-data-line limits, and exchange restrictions. Live, frozen, delayed, and delayed-frozen data are not interchangeable. The tool must not redistribute licensed data, infer an entitlement from a successful stale/delayed response, or use non-live/discovery data as execution-grade evidence.
- **AI-specific scope:** No additional AI-specific US financial-services obligation was identified for this strictly internal, human-gated deployment as of 2026-08-06. Reassess before exposing outputs to another person, marketing performance, exercising discretion, or adding any broker execution authority.

### Domain Expert Roles for Evaluation

| Role | Responsibility in Eval |
|------|------------------------|
| Senior listed-options trader / risk reviewer | Calibrate payoff, max-loss, Greeks, event, early-assignment/exercise, ex-dividend, strategy-fit, and `NO_TRADE` rubrics; label reference cases and adjudicate lifecycle edge cases. |
| Options execution / market-microstructure reviewer | Label quote fitness, liquidity, spread/slippage and fee assumptions, realistic fill sensitivity, and market-data failure cases; review sampled live-read/review-instruction outcomes. |
| Independent quant / model-validation reviewer | Verify point-in-time independence and leakage controls; calibrate forecast-reliability, tail-loss, drawdown, concentration, uncertainty, and Champion/Challenger promotion/rollback rubrics. |
| IBKR account owner / broker-operations reviewer | Confirm account permissions, current positions/orders/NAV, entitlement and assignment procedures; remain sole signer for promotion, rollback, proposal A-grade, review-instruction approval, and final IBKR confirmation. |
| Compliance / records reviewer (required before scope expansion) | Determine whether recommendations, adviser/broker status, FINRA/SEC recordkeeping, communications, privacy, or market-data licensing duties attach before client-facing, regulated-firm, or multi-user use. |

### Research Sources

- OCC, *Characteristics and Risks of Standardized Options* landing page: https://www.theocc.com/Company-Information/Documents-and-Archives/Options-Disclosure-Document
- Options Industry Council, ODD Quick Guide: https://www.optionseducation.org/referencelibrary/odd-quick-guide
- Options Industry Council, Options Assignment FAQ: https://www.optionseducation.org/referencelibrary/faq/options-assignment
- Options Industry Council, Volatility & the Greeks: https://www.optionseducation.org/advancedconcepts/volatility-the-greeks
- FINRA Rule 2360, Options: https://www.finra.org/rules-guidance/rulebooks/finra-rules/2360
- FINRA Rule 4511, General Requirements: https://www.finra.org/rules-guidance/rulebooks/finra-rules/4511
- SEC Rule 17a-4 (current eCFR), records preserved by certain exchange members, brokers, and dealers: https://www.ecfr.gov/current/title-17/chapter-II/part-240/section-240.17a-4
- Interactive Brokers, Market Data Subscriptions / API requirements: https://www.interactivebrokers.com/campus/ibkr-api-page/market-data-subscriptions/
- Interactive Brokers, TWS API market-data types (live/frozen/delayed; page notes migration to IBKR Campus): https://interactivebrokers.github.io/tws-api/market_data_type.html
- Project-specific controls: `options_copilot/plans/P9-02-PLAN.md` and `options_copilot/IMPLEMENTATION_RESEARCH.md`.

---

## 2. Framework Decision

**Selected Framework:** Native deterministic Python architecture: Python 3.12, FastAPI 0.135.1, Pydantic 2.12.5, immutable dataclasses/Protocols, SQLite/WAL/FULL; no agent framework

**Version:** Pinned by the repository environment and lock/configuration files; exact deployed versions must be captured in rollout evidence.

**Rationale:**
P9 already has the correct authority shape: deterministic evaluation, immutable typed records, hash-linked durable state, linearizable resolver guards, and a separate human-only governance CLI. Adding an agent framework would create a second checkpoint/state authority and a wider tool surface without improving evaluation. Direct schema-validated model/provider adapters remain outside promotion, rollback, A-grade, ranking, approval, bridge, and creator authority.

**Alternatives Considered:**

| Framework | Ruled Out Because |
|-----------|------------------|
| LangGraph 1.x | Its checkpoint graph would duplicate authoritative state. It may later coordinate non-authoritative research only, while SQLite and the human CLI remain sole governance authority. |
| CrewAI | Role-based autonomy and coarse checkpointing are a poor fit for deterministic, regulated-style fail-closed gates. |
| OpenAI Agents SDK | Provider-native agent state and tool execution add vendor and authority surface; P9 governance must remain model-agnostic. |

**Vendor Lock-In Accepted:** No for governance; optional model/news providers are replaceable and supporting-only.

---

## 3. Framework Quick Reference

P9 deliberately has no agent framework. The "framework" is the existing native Python composition: typed immutable domain records, strict Pydantic API/model boundaries, a FastAPI read surface, and one SQLite authority ledger whose verified head is the only mutable governance state.

### Installation

```powershell
# Reproduce the versions validated in the current Options Copilot environment.
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install `
  "fastapi==0.135.1" `
  "pydantic==2.12.5" `
  "uvicorn==0.41.0" `
  "pytest==9.0.3"

# sqlite3, dataclasses, typing.Protocol, hashlib, and asyncio ship with Python.
.\.venv\Scripts\python.exe -c "import fastapi,pydantic,sqlite3; print(fastapi.__version__, pydantic.__version__, sqlite3.sqlite_version)"
```

OpenTelemetry is optional observability, not a P9 authority dependency, and is not installed in the validated environment. If it is later enabled, pin `opentelemetry-api`, `opentelemetry-sdk`, and one OTLP exporter in the deployment lock before rollout; exporter failure must never alter a governance decision.

### Core Imports

```python
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field

from options_copilot.learning.policy_authority import (
    CurrentPolicyResolver,
    PolicyAuthorityError,
)
```

### Entry Point Pattern

```python
from typing import Annotated

Hash = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
AUTHORITY_DB = Path("data/options_copilot/governance/policy_authority.sqlite3")


class GovernanceStatus(BaseModel):
    """Read model only; it cannot create or sign an authority."""

    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    current_policy_version: str
    current_policy_hash: Hash
    policy_authority_marker_hash: Hash
    effective_at: datetime
    decision_authority: Literal["READ_ONLY"] = "READ_ONLY"


@asynccontextmanager
async def lifespan(app: FastAPI):
    # One owned ledger/resolver per process. Its RLock serializes access to the
    # check_same_thread=False SQLite connection.
    resolver = CurrentPolicyResolver(AUTHORITY_DB)
    app.state.current_policy_resolver = resolver
    try:
        yield
    finally:
        resolver.close()


app = FastAPI(lifespan=lifespan)


@app.get("/api/governance/current", response_model=GovernanceStatus)
def current_governance(request: Request) -> GovernanceStatus:
    # Use def for this synchronous SQLite path: FastAPI executes it in its
    # external thread pool instead of blocking the event loop.
    resolver: CurrentPolicyResolver = request.app.state.current_policy_resolver
    try:
        resolved = resolver.resolve(now=datetime.now(timezone.utc))
        status = resolver.guard_current(
            resolved,
            callback=lambda: GovernanceStatus(
                current_policy_version=resolved.current_policy_version,
                current_policy_hash=resolved.current_policy_hash,
                policy_authority_marker_hash=resolved.policy_authority_marker_hash,
                effective_at=resolved.effective_at,
            ),
        )
    except PolicyAuthorityError as exc:
        raise HTTPException(status_code=503, detail="governance unavailable") from exc
    if not isinstance(status, GovernanceStatus):
        # The head changed between resolution and the guarded projection.
        raise HTTPException(status_code=409, detail="governance head changed; retry read")
    return status
```

The guarded callback must be bounded local work. Never hold the authority lock across a model call, network request, browser/UI interaction, or broker operation.

### Key Abstractions

| Concept | What It Is | When You Use It |
|---------|------------|-----------------|
| Frozen/slotted dataclass | Immutable domain and signed-authority record with validation in `__post_init__`/`from_dict` | Internal evaluation, policy, marker, and evidence values after untrusted input has been parsed |
| `Protocol` port | Structural interface with no framework lifecycle or hidden state | Provider, clock, evaluator, store, signature verifier, and optional model seams |
| Strict Pydantic model | `ConfigDict(extra="forbid", strict=True, frozen=True)` boundary model | FastAPI requests/responses and optional model JSON before conversion to domain records |
| `PolicyAuthorityLedger` | SQLite/WAL/FULL append-only authority chain with explicit immediate write transactions | The sole durable writer for promotion, proposal-bound A-grade, rollback, and rollback revocation |
| `CurrentPolicyResolver` | Reconstructs and verifies the current signed head; `guard_current` rejects a stale resolution | Every scenario, decision, ranking, approval, bridge, creator, and read-model boundary |

### Common Pitfalls

1. **Treating `check_same_thread=False` as synchronization.** It only disables Python's thread-affinity check. Keep one owner, the ledger `RLock`, parameterized SQL, and serialized `BEGIN IMMEDIATE` writes or concurrent use can interleave.
2. **Calling blocking code from an `async def` route.** The current `_invoke` helper calls a synchronous provider before checking whether the result is awaitable. A SQLite scan or DeepSeek call there blocks the event loop; use a FastAPI `def` route or `await asyncio.to_thread(...)`/Starlette threadpool offload.
3. **Assuming a connection context manager starts a transaction.** With `isolation_level=None` the connection is in SQLite autocommit mode; the context manager only commits or rolls back an already-open transaction. P9 writes must explicitly open `BEGIN IMMEDIATE` and commit/rollback in `finally`-safe code.
4. **Relying on WAL/FULL as logical verification.** WAL and `synchronous=FULL` improve durability, but cannot detect a replaced initial policy, a broken sequence, a stale head, a forged signature, or a mismatched payload. Re-verify the canonical hash/signature chain and all current bindings on every authority resolution.
5. **Accepting Pydantic's default coercion or provider `json_object` as schema enforcement.** Default Pydantic mode coerces values and ignores extras unless configured otherwise; JSON mode only promises an object. Use strict validation, `extra="forbid"`, finite/bounded numeric fields, exact enums, and a second domain-level binding check.

### Recommended Project Structure

```text
options_copilot/
├── AI_SPEC.md                       # this frozen design/evaluation contract
├── runtime.py                       # composition; inject one verified resolution
├── api/app.py                       # read-only FastAPI projections
├── governance/
│   ├── contracts.py                 # canonical signed contracts
│   ├── initial_champion_scenario_policy.v1.json
│   └── independence_spec.v1.json
├── learning/
│   ├── evaluation.py                # deterministic Champion/Challenger reports
│   ├── markers.py                   # frozen promotion/A-grade/rollback schemas
│   ├── policy_authority.py          # WAL/FULL ledger + current resolver
│   └── governance_cli.py            # explicit human-only signing/verification
└── news/
    ├── classifier.py                # narrow supporting-only schema gate
    ├── deepseek.py                  # optional isolated provider adapter
    └── composition.py               # enablement, timeout, cache, and cost caps
tests/options_copilot/
├── test_learning_evaluation.py
├── test_policy_authority.py
├── test_api_frontend.py
└── test_a_grade_creator_e2e.py
data/options_copilot/governance/     # runtime artifacts, never package source
```

### Sources

- Python 3.12 `sqlite3` transaction control and connection context manager: https://docs.python.org/3.12/library/sqlite3.html#transaction-control
- Python `asyncio.to_thread`: https://docs.python.org/3.12/library/asyncio-task.html#running-in-threads
- FastAPI concurrency and sync path-operation threadpool behavior: https://fastapi.tiangolo.com/async/
- Pydantic strict mode: https://docs.pydantic.dev/latest/concepts/strict_mode/
- Pydantic `ConfigDict` (`extra`, `frozen`, `strict`): https://docs.pydantic.dev/latest/api/config/
- SQLite transactions and `BEGIN IMMEDIATE`: https://www.sqlite.org/lang_transaction.html
- SQLite WAL: https://www.sqlite.org/wal.html
- SQLite `synchronous` pragma: https://www.sqlite.org/pragma.html#pragma_synchronous
- OpenTelemetry Python SDK/export examples: https://github.com/open-telemetry/opentelemetry-python/tree/main/docs/examples

---

## 4. Implementation Guidance

**Model Configuration:** Governance is model-agnostic and makes zero model calls. The only current optional model path remains the isolated news classifier using repository model identifier `deepseek-v4-flash`, `temperature=0`, `thinking=False`, `stream=False`, `max_completion_tokens=600`, request size at most 65,536 bytes, and a 6.5-second client timeout. It is disabled unless `OPTIONS_COPILOT_NEWS_LLM_ENABLED` is explicitly true and a DPAPI-backed key is available. Its independent limits are 100 Flash calls/day, one Pro call/day, and USD 0.25/day. Transport, cost, schema, or binding failure falls back to deterministic classification and cannot change a Champion, promotion, rollback, A-grade, ranking, approval, bridge intent, instruction, or order.

**Core Pattern:** Resolve once, carry the exact immutable policy identity through pure evaluation, then re-check that same identity inside the serialized commit boundary. A human may later sign a report-bound decision through the CLI; evaluation code never signs it.

```python
from dataclasses import dataclass
from datetime import datetime
from typing import Literal, Protocol

from options_copilot.learning.policy_authority import CurrentPolicyResolver


@dataclass(frozen=True, slots=True)
class EvaluationReport:
    candidate_policy_hash: str
    current_policy_version: str
    current_policy_hash: str
    policy_authority_marker_hash: str
    evaluation_report_hash: str
    recommendation: Literal["KEEP_SHADOW", "ELIGIBLE_FOR_HUMAN_REVIEW"]


class DeterministicEvaluator(Protocol):
    def evaluate(
        self,
        *,
        candidate_policy_hash: str,
        current_policy_version: str,
        current_policy_hash: str,
        policy_authority_marker_hash: str,
    ) -> EvaluationReport: ...


class EvaluationReportStore(Protocol):
    def append(self, report: EvaluationReport) -> str: ...


def evaluate_and_commit(
    *,
    resolver: CurrentPolicyResolver,
    evaluator: DeterministicEvaluator,
    store: EvaluationReportStore,
    candidate_policy_hash: str,
    now: datetime,
) -> str:
    resolved = resolver.resolve(now=now)
    report = evaluator.evaluate(
        candidate_policy_hash=candidate_policy_hash,
        current_policy_version=resolved.current_policy_version,
        current_policy_hash=resolved.current_policy_hash,
        policy_authority_marker_hash=resolved.policy_authority_marker_hash,
    )
    # The callback is a bounded local append only. If the head changed, the
    # resolver returns None and the caller must rebuild the report from scratch.
    report_id = resolver.guard_current(
        resolved,
        callback=lambda: store.append(report),
    )
    if not isinstance(report_id, str):
        raise RuntimeError("current policy changed before report commit")
    return report_id
```

**Tool Use:** There is no agent tool registry and no model tool calling. The model must never receive the governance CLI, filesystem writer, SQLite connection, signer, approval store, bridge, Creator, broker gateway, or order API. Pydantic validates untrusted API/model shapes; frozen dataclasses and canonical hashes enforce domain bindings; SQLite owns authoritative transitions; pytest exercises boundary, tamper, restart, and concurrency matrices. If OpenTelemetry is later installed, emit only redacted metadata such as operation name, outcome, latency, schema version, model identifier, cost, and non-secret content hashes. Never export prompts, evidence text, account identifiers, credentials, governance signatures, raw broker/order payloads, or exception strings that may contain them.

**State Management:** Open one ledger per runtime process with WAL, `synchronous=FULL`, foreign keys, a finite busy timeout, `isolation_level=None`, and the existing `RLock`. Every mutation uses an explicit serialized immediate transaction: verify the complete chain and expected previous head; validate exact/extra-free signed input; append promotion/A-grade/rollback; revoke affected A-grade markers during rollback in the same transaction; re-verify; then commit. Any exception rolls back and removes an uncommitted artifact. Reads may project a verified head, but authorization must use `guard_current`/equivalent linearizable recheck. Restart always reconstructs from the immutable initial policy plus the full signed chain; an in-memory cached "current" value is never authority.

**Context Window Strategy:** P9 is not RAG and sends no ledger transcript to a model. Deterministic evaluation receives typed records directly. Optional supporting analysis receives a canonical, size-bounded snapshot containing only the current policy/candidate/report/dataset/cost/independence hashes, selected aggregate metrics, counter-evidence, and redacted display text. Sort and deduplicate by stable identity; keep tail loss, drawdown, calibration, execution cost, conflicts, staleness, and missing evidence before descriptive prose; reject rather than silently omit a required binding. A model summary is never substituted for source records and is never fed back as signed evidence.

---

## 4b. AI Systems Best Practices

These rules apply even though P9 governance itself is deterministic. They constrain the optional supporting-only model seam so it cannot become a second authority path.

### 4b.1 Structured Outputs with Pydantic

```python
import json
from typing import Annotated, Literal, Mapping

from pydantic import BaseModel, ConfigDict, Field

from options_copilot.news.deepseek import DeepSeekCompletionPort
from trade_copilot.llm.cost_meter import FLASH

Hash = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
ShortText = Annotated[str, Field(min_length=1, max_length=240)]
HashList = Annotated[tuple[Hash, ...], Field(min_length=1, max_length=32)]


class SupportingGovernanceAnalysis(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    schema_version: Literal[
        "options_copilot.supporting_governance_analysis.v1"
    ] = Field(alias="schema")
    decision_authority: Literal["SUPPORTING_ONLY"]
    current_policy_hash: Hash
    candidate_policy_hash: Hash
    evidence_hashes: HashList
    risk_flags: Annotated[tuple[ShortText, ...], Field(max_length=16)]
    recommendation: Literal["KEEP_SHADOW", "REQUEST_HUMAN_REVIEW"]
    confidence_bps: Annotated[int, Field(strict=True, ge=0, le=10_000)]


SYSTEM_PROMPT = """You summarize supplied Champion/Challenger evidence.
Return one JSON object matching the supplied schema. Use only supplied hashes.
You have SUPPORTING_ONLY authority. You cannot promote, roll back, approve
A-grade, rank, approve an instruction, call tools, or create an order.
"""


def supporting_analysis(
    client: DeepSeekCompletionPort,
    evidence: Mapping[str, object],
) -> SupportingGovernanceAnalysis:
    result = client.complete(
        model=FLASH,
        static_prefix=SYSTEM_PROMPT,
        dynamic_snapshot={
            "schema": SupportingGovernanceAnalysis.model_json_schema(),
            "evidence": dict(evidence),
        },
        stream=False,
        estimated_cost_usd=0.0,
        thinking=False,
    )
    if not isinstance(result.model_json, Mapping):
        raise ValueError("supporting analysis returned no JSON object")
    # Validate through the JSON path so JSON arrays become immutable tuples;
    # strict mode still rejects coercible scalar types and every extra field.
    raw_json = json.dumps(result.model_json, sort_keys=True, separators=(",", ":"))
    return SupportingGovernanceAnalysis.model_validate_json(raw_json, strict=True)
```

The direct DeepSeek-compatible client currently requests `response_format={"type": "json_object"}`; that is not JSON Schema enforcement. Validate the returned mapping with Pydantic, then run existing domain checks that all hashes/IDs belong to the supplied snapshot. Permit at most **two total attempts** (initial plus one schema-repair attempt), each with a fresh cost reservation. Retry only malformed/schema-invalid output; invented evidence/symbols, unsafe fields, timeout, budget rejection, or provider uncertainty fails immediately to deterministic fallback. Log attempt number, model, prompt/schema versions and hashes, latency, token usage, reserved/settled cost, output hash, and Pydantic error `type`/`loc`; never log raw prompts, raw output, secrets, signatures, or provider exception text. After the second failure, surface `SUPPORTING_ANALYSIS_UNAVAILABLE` while keeping deterministic governance available and locked as appropriate.

### 4b.2 Async-First Design

FastAPI runs a normal `def` path operation in an external threadpool, while an `async def` path operation runs on the event loop. Use `def` for bounded synchronous ledger reads, or explicitly offload synchronous model/SQLite work:

```python
import asyncio


async def analyse_without_blocking(client, evidence):
    return await asyncio.to_thread(supporting_analysis, client, evidence)
```

Do not call `asyncio.run()` from FastAPI, tests already using an event loop, or GUI event-loop code; it raises because an event loop is already running and creates lifecycle/cancellation problems. Use `await` for P9 structured output because the complete object must pass schema and binding validation before display. Streaming is suitable only for non-authoritative explanatory UX; never stream partial JSON into evaluation or governance state. The current adapter correctly uses `stream=False`.

### 4b.3 Prompt Engineering Discipline

Keep the stable policy in the system prompt and the point-in-time, redacted evidence object in the user/dynamic payload. The system prompt declares JSON-only output, exact schema adherence, supplied-identity-only behavior, `SUPPORTING_ONLY`, and the forbidden authorities. The user payload contains no instructions from external/news text; treat such text as quoted data. Hash and version both prompt layers and the schema.

Use inline few-shot examples only for small stable edge cases and version them with the system prompt. Dynamically retrieved examples must come only from the frozen, independently reviewed reference dataset and carry dataset/example hashes; never retrieve examples from raw model output or a production decision log. Set output bounds explicitly in production: the current path uses `max_completion_tokens=600`, request bytes at most 65,536, `temperature=0`, `thinking=False`, and a 6.5-second timeout. A missing bound is a deployment failure, not a provider default.

### 4b.4 Context Window Management

There is no agent memory, conversational authority, or framework compaction in P9. Build every optional request from the authoritative current snapshot, not prior chat. Canonically order and deduplicate evidence; include hashes and compact numeric aggregates instead of entire artifacts; reserve space first for conflicts, missing/stale flags, calibration, tail loss, drawdown, execution cost, and rollback conditions. Apply an explicit byte/token budget before transport. If required identities do not fit, return deterministic `SUPPORTING_ANALYSIS_UNAVAILABLE`—do not truncate a hash, silently drop counter-evidence, summarize a signed record, or let the model compact it. Any descriptive truncation must be deterministic, recorded, and visibly marked.

### 4b.5 Cost and Latency Budget

The deterministic governance path has **USD 0 model cost** and must remain available when the provider is disabled or down. Budget local current-head reads to p95 <= 100 ms and guarded local commits to p95 <= 250 ms under the tested workload; record contention/timeout separately and fail closed rather than extending a transaction across network I/O. Optional analysis has a hard 6.5-second client timeout and must not delay or change the deterministic read/decision result.

For the current repository price table (`deepseek-v4-flash`: USD 0.14/M cache-miss input tokens and USD 0.28/M output tokens), an expected 1,500-input/300-output call is approximately **USD 0.000294**; 100 uncached calls/day are approximately **USD 0.0294/day**, below the independent USD 0.25/day hard cap. Recalculate this estimate against the provider's current bill before rollout; the meter's worst-case reservation, not the estimate, gates each call.

Cache only successful, schema- and binding-validated output. Use an exact key over model ID, prompt version/hash, schema version/hash, canonical evidence hash, and all current policy/candidate/report/dataset/cost/independence bindings; the existing classifier's bounded LRU default is 2,048 entries. Semantic caching is forbidden for governance recommendations, classifications, rubric results, or any authority-bound value because a near match can cross a policy/proposal/evidence boundary. It may be considered only for separately labelled display prose after exact authoritative evaluation, and a semantic hit must never suppress a model failure, create evidence, or affect promotion/A-grade/rollback.

---

## 5. Evaluation Strategy

P9 is evaluated as a deterministic governance and safety system first and as a forecasting system second. A better forecast never compensates for a failed authority, risk, freshness, human-gate, or creator-call check. Code-based gates are authoritative; calibrated LLM and human review are limited to subjective explanation quality and domain edge cases. Passing these evaluations demonstrates conformance to the declared controls and observed historical evidence. It is **not investment advice and is not a promise of profitability**.

### Dimensions

| Dimension | Rubric (Pass/Fail or 1-5) | Measurement Approach | Priority |
|-----------|--------------------------|----------------------|----------|
| Authority-chain and policy-state transition correctness | **PASS:** The immutable initial-policy source is unchanged; every event has a valid signature, contiguous sequence, canonical content hash, and exact previous-head binding; promotion is monotonic from the current head; rollback targets a prior signed policy; one verified policy/version/marker identity propagates through scenario, decision, ranking, approval, bridge intent, and creator validation after restart and during competing writes. **FAIL:** Any broken/forged chain, split head, stale prior head, non-monotonic promotion, invalid rollback target, identity divergence, or more than one winner from one head becomes authoritative. | Code: ledger/resolver, restart, propagation, and concurrent-write tests; Human: inspect signed transition packet | Critical |
| Human-gated task completion | **PASS:** Predictions and reports remain `SHADOW_ONLY` with all authority flags false; fewer than 30 independent samples is `COLLECTING`, and 30 or more is only `DISCOVERY`; production promotion, proposal-specific `APPROVE_A_GRADE`, and rollback are three separate explicit account-owner decisions through the human CLI, each bound to actor, time, report/evidence hashes, and the current head. Promotion never implies A-grade, and final action remains inside IBKR. **FAIL:** A model, UI, API, runtime, scheduler, test, sample count, or elapsed time signs or implies any authority, or one human decision is reused as another. | Code: negative authority/caller audit and schema tests; Human: review and sign/deny each checkpoint | Critical |
| Exact risk boundaries and defined payoff | **PASS:** Independent payoff reconstruction proves finite exact maximum loss after costs and one permitted combination. With A-grade locked, `10.00%` may pass and `10.01%`, `15.00%`, `15.01%`, `20.00%`, and `20.01%` reject. With a current exact proposal/policy/cost/ranking-bound A-grade marker, `10.01%` and `15.00%` may pass; `15.01%`, `20.00%`, and `20.01%` always reject. All other structure, DTE, account-fit, and permission gates still apply. **FAIL:** Rounding, coercion, model/UI claims, missing payoff fields, adjusted/wrong contract identity, naked/uncovered risk, or a stale/wrong marker crosses any boundary. | Code: Decimal boundary/property/E2E matrix; Human: listed-options reviewer adjudicates payoff/lifecycle fixtures | Critical |
| User safety and review-only creator behavior | **PASS:** Only a fully current proposal after human double confirmation may reserve and make exactly one creator call for one review-only instruction; uncertainty is terminal and is never retried; no component can submit an order. Every rejected, stale, tampered, concurrent, rolled-back, revoked, or wrong-bound case records zero reservations and zero creator calls. **FAIL:** Any unintended call, duplicate/retry, call before confirmation, mutation of a rejection, or broker submission occurs. | Code: `RecordingCreator`, reservation, idempotency, UNKNOWN, and authority audit assertions; Human: live review-instruction and final-IBKR checkpoint | Critical |
| Stale, tampered, missing, and concurrent fail-closed behavior | **PASS:** Stale or mismatched policy, risk, evaluation, dataset, cost, ranking, approval, bridge, broker, quote, time, or independence evidence produces `NO_TRADE`, `A_GRADE_LOCKED`, no append, or an unavailable read model as appropriate; tamper and concurrent head changes are detected inside the serialized boundary. **FAIL:** Any such input reaches an authorizable output, is silently repaired, or is retried against mixed evidence. | Code: mutation, clock, restart, hash, SQLite concurrency, and zero-side-effect tests | Critical |
| Atomic rollback and A-grade revocation | **PASS:** A human-signed rollback verifies the current prior head, restores a prior signed policy, appends the rollback, and revokes every A-grade marker bound to the rolled-back policy in the same committed transaction; restart reconstructs the same head, and old rankings/approvals immediately reject. **FAIL:** Partial commit, orphan artifact accepted as authority, rollback without every required revocation, revoked marker remains active, or old output remains authorizable. | Code: transaction fault injection, chain/revocation verification, restart and E2E stale-output tests; Human: rollback packet review | Critical |
| Cost-adjusted EV, probability calibration, and Brier comparison | **PASS:** Champion, Challenger, and `NO_TRADE` are compared on the same cases with the current signed cost model; Champion probability and EV baselines are complete; Challenger cost-adjusted EV is positive and exceeds Champion by the predeclared human-reviewed materiality margin; Brier score improves; calibration error is no worse than the signed tolerance; all figures are finite, hash-bound, and reported with uncertainty. **FAIL:** Gross/nominal results substitute for after-cost results, a baseline is missing, EV delta or Brier improvement is non-positive, calibration worsens beyond tolerance, or selection hides losing cases. | Code: `EvaluationReport` metrics, bindings, completeness and threshold checks; Human: independent quant approves materiality/calibration tolerances | Critical |
| Tail loss, drawdown, concentration, and regime stability | **PASS:** Tail loss and maximum drawdown use chronological after-cost outcomes and remain within predeclared human-reviewed limits relative to Champion; the report exposes loss concentration and scenario results across time, symbols, strategy families, DTE, event/IV regimes, and adverse-fill sensitivity. **FAIL:** Average P&L or win rate hides a worse tail/drawdown, a loss cluster, an unstable bucket, or an economically adverse fill. | Code: chronological drawdown/tail and stratified report checks; Human: quant and options-risk review of thin/unstable buckets | Critical |
| Independence, point-in-time integrity, and leakage prevention | **PASS:** Scenario and independence keys are unique; correlated/duplicate cases count once; all features, events, quotes, outcomes, labels, and cost versions respect the economic-time cutoff and the signed P8 independence spec; Champion and Challenger use identical frozen cases. **FAIL:** Duplicate/dependent samples inflate count, future facts or retrospective corrections enter replay, exclusions are hidden, UNKNOWN is imputed as success, or datasets differ between policies. | Code: uniqueness, cutoff, exclusion-log, chronological replay, dataset/spec hash tests; Human: independent model-validation audit | Critical |
| Executable market-data and cost fitness | **PASS:** Approval/bridge evidence contains current entitled per-leg bid/ask, type, time, size/availability, contract identity, spread, fees, adverse slippage, and worse-fill sensitivity; crossed, one-sided, delayed/frozen, stale, incomplete, or prohibitive markets return `NO_TRADE`. **FAIL:** Last/close/theoretical/unlabelled midpoint pricing, volume/OI as a fill guarantee, omitted costs, or non-live data is treated as execution-grade. | Code: quote/coherence/freshness/cost-binding tests; Human: execution reviewer labels live and edge fixtures | High |
| Options event and lifecycle coverage | **PASS:** Reference cases expose earnings/news/ex-dividend timing, IV level/skew/crush, position Greeks and adverse scenarios, early assignment/exercise, settlement, halt/corporate action, and resulting stock/cash obligations with a feasible exit/manual plan. **FAIL:** Direction-only reasoning, un-timestamped Greeks, or a defined-risk label hides leg-level assignment, exercise, or cash/stock consequences. | Code: required-field/provenance/scenario checks; Human: listed-options risk reviewer | High |
| News/model authority isolation and tool-use correctness | **PASS:** Deterministic governance makes zero model calls; every news/model artifact is strict-schema, exact-hash-bound, visibly `SUPPORTING_ONLY`, has no signer/ledger/approval/bridge/creator/order tool, and cannot change eligibility, policy, risk, ranking, approval, or creator behavior. Timeout, budget, schema, invented-evidence, and provider failures fall back without authority change. **FAIL:** A model/news value becomes hard evidence or authority, receives a privileged tool, causes an extra governance/creator call, or provider failure changes the deterministic decision. | Code: spy-call counts, import/authority audit, Pydantic/binding/fallback tests | Critical |
| Supporting explanation faithfulness and non-promissory language | **PASS:** Optional prose accurately summarizes only supplied point-in-time aggregates, foregrounds counter-evidence, costs, calibration and downside, preserves `SUPPORTING_ONLY`/`NO_TRADE`, and makes no suitability, certainty, or profitability claim. **FAIL:** It invents facts, suppresses a material risk, implies an authority/guarantee, or conflicts with deterministic fields. | Calibrated LLM Judge for subjective faithfulness/tone only, with correlation >= 0.70 to human labels; Human adjudicates disagreements and all high-stakes samples | Medium |

### Eval Tooling

**Primary Tool:** The existing local-first stack is authoritative: `pytest`, `options_copilot.learning.evaluation`, canonical hash/schema validators, the SQLite `PolicyAuthorityLedger`/`CurrentPolicyResolver`, governance CLI verifiers, authority audits, and zero-call spies. The repository scan found no installed Langfuse, LangSmith, Phoenix, Braintrust, Promptfoo, or RAGAS integration. RAGAS is inapplicable because P9 is not RAG. Prompt/LLM-judge experiments may be added later for supporting prose, but cannot become a release or authority gate until versioned and calibrated.

**Optional Observability:** Arize Phoenix may be used only as a local/self-hosted viewer for **redacted, non-authoritative** OpenTelemetry spans. It is not a production dependency, source of truth, approval system, or substitute for ledger/evidence verification. Exporter/viewer failure must have no effect on a decision. Pin these optional packages before any rollout and do not emit prompt/evidence text, account identifiers, signatures, credentials, broker/order payloads, or raw exception text.

**Setup:**

```powershell
# Authoritative eval path: no platform account and no new runtime dependency.
.\.venv\Scripts\python.exe -m pip install "pytest==9.0.3"

# Optional local redacted tracing sandbox only; pin resolved versions before use.
.\.venv\Scripts\python.exe -m pip install arize-phoenix opentelemetry-sdk
```

```python
# Optional, non-authoritative, redacted local tracing only.
import phoenix as px
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider

px.launch_app()  # http://localhost:6006; local viewer, never an authority service
provider = TracerProvider()
trace.set_tracer_provider(provider)
tracer = trace.get_tracer("options_copilot.p9")
# Native P9 code may create manual spans containing allowlisted metadata only.
```

**CI/CD Integration:**

```powershell
# Non-human, code-first P9 release gates. CI must never invoke a signing verb.
python -m pytest -q `
  tests/options_copilot/test_learning_evaluation.py `
  tests/options_copilot/test_policy_authority.py `
  tests/options_copilot/test_a_grade_marker.py `
  tests/options_copilot/test_proposal_gate.py
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

# Run after P9-02-02 creates its declared E2E harness.
python -m pytest -q `
  tests/options_copilot/test_api_frontend.py `
  tests/options_copilot/test_a_grade_creator_e2e.py `
  -m "not human_authority"
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

python -m compileall -q options_copilot
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
node --check options_copilot/frontend/app.js
exit $LASTEXITCODE
```

The marked `human_authority` E2E suite is a post-decision checkpoint, not ordinary CI. It may run only against the exact human-verified authority database and decision path after a separate promotion/A-grade decision. Tests and fixtures must never call production `sign-promotion`, `sign-a-grade`, or `sign-rollback` verbs.

### Reference Dataset

**Size:** Start during implementation and freeze/version before evaluation. The outcome dataset requires at least **30 truly independent labeled cases** because `MINIMUM_DISCOVERY_SAMPLES = 30`; fewer is `COLLECTING`, and 30 or more reaches **Discovery only**. No sample count grants production authority. A production-promotion packet requires a separately reviewed, exact-hash-bound dataset and an explicit human promotion decision. Proposal-specific A-grade is another independent, exact-proposal human decision and is never inferred from the dataset or promotion.

**Composition:** Run Champion, Challenger, and `NO_TRADE` over the same frozen point-in-time cases. Cover ordinary and adverse outcomes across time, symbols, strategy families, DTE, earnings/news/ex-dividend and IV-crush regimes, liquid and wide/one-sided/crossed markets, cost/slippage sensitivity, wins, ordinary losses, tail losses, and clustered drawdowns. Alongside the 30+ outcome cases, maintain an exhaustive deterministic adversarial fixture matrix for: `10.00/10.01/15.00/15.01/20.00/20.01` in locked and exact A-grade states; payoff/contract/ratio/multiplier errors; stale/missing/conflicting/tampered bindings; duplicate/leaky samples; restart and concurrent promotion/rollback; atomic rollback/revocation; malformed/injected/timeout/budget-exhausted model/news output; and zero-reservation/zero-creator-call assertions.

**Labeling:** Code derives hashes, cutoff/uniqueness, exact Decimal boundaries, costs, Brier/calibration, drawdown/tail, state transitions, and call counts. A senior listed-options/risk reviewer and execution reviewer label payoff, lifecycle, fill, and `NO_TRADE` cases; an independent quant/model validator labels independence, leakage, uncertainty, calibration, downside, and promotion materiality. The IBKR account owner signs or denies promotion, proposal A-grade, rollback, and live review-instruction checkpoints. An LLM judge may score only subjective explanation faithfulness/tone after calibration against at least 20 human-labeled examples with correlation >= 0.70; below that threshold, human review replaces it. Disagreements and all Critical edge cases are human-adjudicated. New production failures are appended as a new dataset version; prior frozen datasets and hashes remain reproducible.

**Decision Rule:** Automated metrics can force `KEEP_SHADOW`, `NO_TRADE`, `A_GRADE_LOCKED`, rollback review, or a blocked checkpoint. They can never emit `PROMOTE`, `APPROVE_A_GRADE`, `ROLLBACK`, create an instruction, or promise future returns.

---

## 6. Guardrails

### Online (Real-Time)

| Guardrail | Trigger | Intervention |
|-----------|---------|--------------|
| Current authority and transition integrity | Every policy/risk resolve, authority mutation, scenario, ranking, approval, bridge, creator, restart, and read projection | Re-verify immutable source, full signed chain, current head, and exact propagated identities. On any exception/mismatch, roll back the mutation, return unavailable/`NO_TRADE`, keep A-grade locked, make zero reservation/calls, and raise a Critical alert. |
| Exact payoff, portfolio, and risk gate | Every candidate and again immediately before reservation/creator call | Reject non-standard/missing legs, unquantifiable or unlimited loss, second combination, invalid DTE/account basis, locked `>10.00%`, exact-A-grade `>15.00%`, and every `20.00%`/`20.01%` case. No rounding across a threshold; zero side effects. |
| Point-in-time market/evidence fitness | Every evaluation, approval, and bridge build/reserve/proof/attempt boundary | Reject stale, future, delayed/frozen-as-live, crossed, one-sided, incomplete, conflicting, unentitled, or hash-mismatched policy/cost/ranking/broker/quote/event/dataset/independence evidence. Preserve the reason and use `NO_TRADE`; never silently fill or refresh only part of a snapshot. |
| Human confirmation and one-attempt creator gate | Every promotion/A-grade/rollback mutation and review-instruction attempt | Permit signing only through the explicit human CLI. Require separate double confirmation for the exact current proposal before one review-only creator call. Rejection or uncertainty makes zero/one terminal call as defined; never retry UNKNOWN and never submit an order. |
| Atomic rollback/revocation | Every rollback transaction | Verify current prior head and historical target, append rollback and all affected A-grade revocations atomically, re-verify, then commit. Any fault rolls back the entire transaction and leaves no artifact accepted as authority. |
| News/model isolation and privacy | Every optional provider call and every model/news projection | Run outside authority locks with strict schema/binding, timeout, byte/token/cost caps, and an allowlisted redacted trace. Force `SUPPORTING_ONLY`; on error use deterministic fallback/unavailable state. Deny all governance, signer, approval, creator, broker, and order tools. |

### Offline (Flywheel)

| Metric | Sampling Strategy | Action on Degradation |
|--------|------------------|-----------------------|
| Cost-adjusted EV, Brier, calibration, coverage, and scenario deltas | Recompute on 100% of resolved frozen outcome cases for each dataset/report version; compare identical Champion/Challenger/`NO_TRADE` cases | Keep Challenger shadow; invalidate the report/checkpoint on missing bindings or coverage; require new independent evidence and human review before another promotion decision. |
| Tail loss, maximum drawdown, loss concentration, and adverse-fill sensitivity | 100% of resolved outcomes in economic-time order, stratified by symbol, strategy, DTE, event/IV regime, and time window | Freeze promotion/A-grade expansion, open rollback review if signed downside limits are breached, and add the failure cluster to the next dataset version. |
| Independence, duplication, leakage, and exclusion integrity | 100% of new/changed dataset rows and every report; independent quant re-audit before each promotion decision | Reject affected rows/report, return to `COLLECTING`/`KEEP_SHADOW`, rebuild from the point-in-time cutoff, and preserve the exclusion/tamper evidence. |
| Authority/gate/creator conformance | 100% of authority events, rejections, reservations, creator attempts, restarts, and rollback/revocation events | Any unauthorized signer, invalid accepted boundary, stale acceptance, missing revocation, unintended creator call, retry, or submission is a Critical incident: lock creation and governance mutation pending human investigation. |
| Execution-cost and market-data drift | 100% of attempted review instructions and resolved outcomes; weekly comparison of predicted versus observed spread/slippage/fees and quote-type/entitlement failures | Mark cost contract stale, force `NO_TRADE` for new exposure, recalibrate with execution review, and require a new signed cost version before evaluation/approval resumes. |
| Supporting explanation quality and model/provider drift | 100% of failures/schema/binding/budget events plus a stratified 10% of successful supporting outputs; at least 20 human-reviewed cases per judge calibration cycle | Disable the LLM judge below 0.70 human correlation; disable optional analysis on authority-language, unsupported-claim, or material-risk omission; keep deterministic governance available. |

---

## 7. Production Monitoring

**Tracing Tool:** Authoritative monitoring is the existing local append-only/hash-bound evidence, evaluation reports, authority ledger, verifier output, and structured counters/logs. Optional Arize Phoenix is a local/self-hosted viewer for allowlisted redacted OpenTelemetry metadata only and remains non-authoritative and non-required. No external trace is evidence for promotion, A-grade, rollback, approval, or creator calls. Exporter failure must not change system state.

**Key Metrics to Track:** Track authority sequence/head/policy identities and integrity verdicts; transition and rollback/revocation counts; resolver/head mismatches; `NO_TRADE`/`A_GRADE_LOCKED` reasons; exact-boundary outcomes; reservation and creator call counts; UNKNOWN/retry/order-submit counts; double-confirmation age; independent/total sample counts and stage; comparison completeness and blocked reasons; after-cost EV and EV delta; Brier/calibration and improvement; coverage; tail loss/drawdown and scenario buckets; quote/freshness/entitlement/cost drift; deterministic governance model-call count; supporting schema/binding/fallback/budget results; and p50/p95/p99 local resolve/commit latency. Metrics are decision aids, not profitability claims.

### Alert Thresholds

| Severity | Threshold | Required Response |
|----------|-----------|-------------------|
| Critical | Any initial-source, signature, chain, sequence, hash, current-head, propagated-identity, restart, or atomic rollback/revocation failure | Immediately disable governance mutations and all new-exposure review-instruction creation; preserve evidence; require human integrity review. |
| Critical | Any acceptance of locked `10.01%+`, exact-A-grade `15.01%+`, `20.00%`, or `20.01%`; any unbounded/unquantified loss or second combination | Force `NO_TRADE`, zero reservation/calls, lock affected path, and investigate the exact fixture/proposal. |
| Critical | Any creator call on a rejected/stale/tampered/wrong-bound/revoked/concurrent case; more than one call for an accepted approval; any UNKNOWN retry; any order-submission call | Stop creator path immediately and require account-owner reconciliation before re-enable. The tolerated count is zero for invalid cases and exactly one review-only call for a valid case. |
| Critical | Any non-human signing caller, model call on deterministic governance, news/model output with authority other than `SUPPORTING_ONLY`, or model/news change to eligibility/risk/ranking/approval/creator state | Disable the offending path and keep deterministic governance locked/available as safe; run authority and call-graph audit. |
| Promotion block | Independent count `<30`, or any count of `30+` presented as more than `DISCOVERY`; `comparison_complete=false`; coverage `<1.0`; Challenger after-cost EV delta `<=0`; Brier improvement `<=0`; calibration/tail/drawdown breaches its predeclared signed tolerance; missing uncertainty/bucket/binding evidence | Emit `KEEP_SHADOW`; no promotion signing packet is complete. Even when all pass, require a separate explicit human promotion decision. |
| A-grade block | Missing/expired/stale/revoked/wrong proposal-policy-cost-ranking marker or any attempt to infer A-grade from promotion/report/sample count | Emit `A_GRADE_LOCKED`; no A-grade creator branch. A separately signed exact-proposal decision is required. |
| Degraded | Current-head read p95 `>100 ms` or guarded local commit p95 `>250 ms` for 15 minutes; optional call exceeds 6.5 seconds, 100 Flash calls/day, 1 Pro call/day, or USD 0.25/day | Alert on local contention or provider drift. Fail closed on authoritative timeout; disable optional analysis at its hard cap without affecting deterministic decisions. |
| Judge disabled | Calibrated LLM-judge correlation with human labels `<0.70` on at least 20 adjudicated cases | Remove judge scores from decision/reporting gates and use human review until recalibrated. |

**Smart Sampling Strategy:** Online Critical guardrails are unsampled and run on every applicable request/transition. Retain and review 100% of authority mutations, promotion/A-grade/rollback decisions, failed validations, reservations, creator attempts, UNKNOWN results, boundary cases, and resolved evaluation outcomes. For optional supporting analysis, retain counters for 100% of calls/failures and review all anomalies plus a stratified 10% of successful redacted outputs, overweighting unusual latency/cost, long inputs, low confidence, adverse events, risk-language, and human disagreement. Each month, humans review at least 10 highest-risk available cases (or all if fewer); before every promotion/A-grade/rollback decision, humans review the complete exact-hash-bound packet. Investigate any divergence between improving eval scores and worsening execution cost, drawdown, operator overrides, or `NO_TRADE`/failure rates.

**Evidence Retention:** Keep the authority SQLite ledger, signed authorities, revocations, frozen datasets/specs, evaluation reports, human decision manifests, verifier stdout/stderr hashes, and creator/reconciliation outcomes append-only for the life of the deployment plus a verified recoverable backup; never overwrite or auto-prune records used by a decision. Keep optional redacted non-authoritative telemetry locally for 30 days, then delete it without affecting authority evidence. Do not collect raw secrets, prompts/evidence text, account identifiers, signatures, or broker/order payloads in telemetry. This internal retention policy does not claim FINRA/SEC recordkeeping compliance; re-map retention and format with qualified compliance/legal review before any regulated-firm, client-facing, or multi-user use.

---

## Checklist

- [x] System type classified
- [x] Critical failure modes identified (at least 3)
- [x] Domain context researched
- [x] Regulatory/compliance context identified
- [x] Domain expert roles defined for evaluation involvement
- [x] Framework selected with rationale documented
- [x] Alternatives considered and ruled out
- [x] Framework quick reference written
- [x] AI systems best practices written
- [x] Evaluation dimensions grounded in domain rubric ingredients
- [x] Each eval dimension has a concrete rubric
- [x] Eval tooling selected
- [x] Reference dataset specification written
- [x] CI/CD eval integration specified
- [x] Online guardrails defined
- [x] Production monitoring specified
