from __future__ import annotations

import json
import os
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from options_copilot.analytics.scenarios import INITIAL_POLICY_HASH, INITIAL_POLICY_VERSION
from options_copilot.execution_cost import EXECUTION_COST_HASH, EXECUTION_COST_VERSION
from options_copilot.news.external_top10_source import (
    EXTERNAL_TOP10_SCHEMA,
    EXTERNAL_TOP10_VERSION,
    ExternalResolvedStructure,
    ExternalTop10Publisher,
    ExternalTop10StructureSource,
    ExternalTop10ValidationError,
    trusted_terminal_scenario_set_hash,
)
from options_copilot.news.models import (
    ConditionalOptionLeg,
    NewsAuthority,
    OptionLegSide,
    OptionRight,
    PreselectionPhase,
    PreselectionTerminalScenario,
)
from options_copilot.news.preselection import strategy_structure_hash
from options_copilot.news.preselection_producer import (
    ResolvedStructure,
    _validate_structures,
)
from options_copilot.storage.canonical import canonical_hash, canonical_json


UTC = timezone.utc
NEW_YORK = ZoneInfo("America/New_York")
SCHEDULED_FOR = datetime(2026, 8, 6, 9, 20, tzinfo=NEW_YORK)
OBSERVED_AT = SCHEDULED_FOR - timedelta(seconds=30)
WRITTEN_AT = SCHEDULED_FOR - timedelta(seconds=20)
NOW = SCHEDULED_FOR + timedelta(seconds=10)
EXPIRY = SCHEDULED_FOR.date() + timedelta(days=15)
STRATEGIES = (
    "LONG_CALL",
    "LONG_PUT",
    "BULL_CALL_VERTICAL",
    "BEAR_CALL_VERTICAL",
    "BULL_PUT_VERTICAL",
    "BEAR_PUT_VERTICAL",
)
DYNAMIC_FIELDS = (
    "bid",
    "ask",
    "quote_asof",
    "quote_batch_id",
    "implied_volatility",
    "delta",
    "gamma",
    "theta",
    "vega",
    "volume",
    "open_interest",
    "dte",
)


def _identity(index: int, strike: int, right: str) -> dict[str, object]:
    right_code = "C" if right == "CALL" else "P"
    return {
        "conId": 100_000 + index,
        "localSymbol": f"T{index:02d}  {EXPIRY:%y%m%d}{right_code}{strike * 1000:08d}",
        "tradingClass": f"T{index:02d}",
        "multiplier": 100,
        "exchange": "SMART",
        "expiry": EXPIRY.isoformat(),
        "strike": str(strike),
        "right": right,
    }


def _leg(
    identity: dict[str, object], *, underlying: str, side: str, quantity: int = 1
) -> dict[str, object]:
    return {
        "underlying": underlying,
        "identity": identity,
        "side": side,
        "ratio": 1,
        "quantity": quantity,
        **{field: None for field in DYNAMIC_FIELDS},
    }


def _raw_legs(index: int, strategy: str) -> list[dict[str, object]]:
    base = index * 10
    underlying = f"T{index:02d}"
    if strategy == "LONG_CALL":
        return [_leg(_identity(base + 1, 100, "CALL"), underlying=underlying, side="BUY")]
    if strategy == "LONG_PUT":
        return [_leg(_identity(base + 1, 100, "PUT"), underlying=underlying, side="BUY")]
    if strategy == "BULL_CALL_VERTICAL":
        return [
            _leg(_identity(base + 1, 100, "CALL"), underlying=underlying, side="BUY"),
            _leg(_identity(base + 2, 105, "CALL"), underlying=underlying, side="SELL"),
        ]
    if strategy == "BEAR_CALL_VERTICAL":
        return [
            _leg(_identity(base + 1, 100, "CALL"), underlying=underlying, side="SELL"),
            _leg(_identity(base + 2, 105, "CALL"), underlying=underlying, side="BUY"),
        ]
    if strategy == "BULL_PUT_VERTICAL":
        return [
            _leg(_identity(base + 1, 95, "PUT"), underlying=underlying, side="BUY"),
            _leg(_identity(base + 2, 100, "PUT"), underlying=underlying, side="SELL"),
        ]
    if strategy == "BEAR_PUT_VERTICAL":
        return [
            _leg(_identity(base + 1, 95, "PUT"), underlying=underlying, side="SELL"),
            _leg(_identity(base + 2, 100, "PUT"), underlying=underlying, side="BUY"),
        ]
    raise AssertionError(strategy)


def _typed_leg(underlying: str, row: dict[str, object]) -> ConditionalOptionLeg:
    identity = row["identity"]
    assert isinstance(identity, dict)
    return ConditionalOptionLeg(
        underlying=underlying,
        con_id=int(identity["conId"]),
        expiry=EXPIRY,
        strike=Decimal(str(identity["strike"])),
        right=OptionRight(str(identity["right"])),
        side=OptionLegSide(str(row["side"])),
        ratio=int(row["ratio"]),
        quantity=int(row["quantity"]),
        bid=None,
        ask=None,
        quote_asof=None,
        quote_batch_id=None,
        implied_volatility=None,
        delta=None,
        gamma=None,
        theta=None,
        vega=None,
        volume=None,
        open_interest=None,
        dte=None,
        local_symbol=str(identity["localSymbol"]),
        trading_class=str(identity["tradingClass"]),
        multiplier=int(identity["multiplier"]),
        exchange=str(identity["exchange"]),
    )


def _structure(index: int) -> dict[str, object]:
    strategy = STRATEGIES[(index - 1) % len(STRATEGIES)]
    underlying = f"T{index:02d}"
    legs = _raw_legs(index, strategy)
    typed = tuple(_typed_leg(underlying, row) for row in legs)
    evidence_hash = canonical_hash({"evidence": index})
    scenarios = (
        PreselectionTerminalScenario(Decimal("90"), Decimal("0.50")),
        PreselectionTerminalScenario(Decimal("110"), Decimal("0.50")),
    )
    preselection_id = f"external-preselection-{index:02d}"
    strategy_hash = strategy_structure_hash(underlying, strategy, typed)
    return {
        "preselection_id": preselection_id,
        "underlying": underlying,
        "strategy_type": strategy,
        "legs": legs,
        "risk_defined": True,
        "maximum_loss_usd": "500.00",
        "estimated_cost_usd": "250.00",
        "cost_after_ev_usd": "25.00",
        "entry_condition": "Open quote remains executable after repricing.",
        "invalidation_condition": "Catalyst thesis or price level invalidates.",
        "profit_target_condition": "Defined research target is reached.",
        "stop_loss_condition": "Defined maximum loss threshold is approached.",
        "evidence_ids": [f"evidence-{index:02d}"],
        "evidence_hashes": [evidence_hash],
        "strategy_hash": strategy_hash,
        "research_summary": "External supporting-only pre-market structure.",
        "scenario_asof": OBSERVED_AT,
        "terminal_scenarios": [item.as_dict() for item in scenarios],
        "scenario_hash": trusted_terminal_scenario_set_hash(
            candidate_id=preselection_id,
            strategy_hash=strategy_hash,
            scenario_asof=OBSERVED_AT,
            scenarios=scenarios,
            current_policy_version=INITIAL_POLICY_VERSION,
            current_policy_hash=INITIAL_POLICY_HASH,
        ),
        "execution_cost_contract_version": EXECUTION_COST_VERSION,
        "execution_cost_contract_hash": EXECUTION_COST_HASH,
        "decision_authority": "SUPPORTING_ONLY",
        "approval_eligible": False,
        "instruction_creation_allowed": False,
        "order_allowed": False,
    }


def _payload(count: int = 10) -> dict[str, object]:
    structures = [_structure(index) for index in range(1, count + 1)]
    evidence_hashes = [
        item
        for structure in structures
        for item in structure["evidence_hashes"]
    ]
    return {
        "batch_id": "external-top10-20260806-0920",
        "scheduled_for": SCHEDULED_FOR,
        "observed_at": OBSERVED_AT,
        "strategy_nav_usd": "100000.00",
        "current_policy_version": INITIAL_POLICY_VERSION,
        "current_policy_hash": INITIAL_POLICY_HASH,
        "normal_risk_fraction": "0.10",
        "hard_risk_fraction": "0.20",
        "a_grade_enabled": False,
        "evidence_hashes": evidence_hashes,
        "decision_authority": "SUPPORTING_ONLY",
        "approval_eligible": False,
        "instruction_creation_allowed": False,
        "order_allowed": False,
        "structures": structures,
    }


def _publish(path: Path, payload: dict[str, object] | None = None):
    return ExternalTop10Publisher(path, clock=lambda: WRITTEN_AT).publish(
        _payload() if payload is None else payload
    )


def _source(path: Path, *, now: datetime = NOW):
    return ExternalTop10StructureSource(path, clock=lambda: now)


def _rewrite(path: Path, mutator) -> None:
    document = json.loads(path.read_text(encoding="utf-8"))
    mutator(document)
    unsigned = dict(document)
    unsigned.pop("content_hash", None)
    document["content_hash"] = canonical_hash(unsigned)
    path.write_text(canonical_json(document) + "\n", encoding="utf-8")


def test_atomic_hash_bound_exact_top10_resolves_supporting_only_structures(
    tmp_path: Path,
) -> None:
    path = tmp_path / "external-top10.json"
    _publish(path)
    document = json.loads(path.read_text(encoding="utf-8"))
    unsigned = dict(document)
    content_hash = unsigned.pop("content_hash")

    assert document["schema"] == EXTERNAL_TOP10_SCHEMA
    assert document["version"] == EXTERNAL_TOP10_VERSION
    assert content_hash == canonical_hash(unsigned)
    resolved = _source(path).resolve_top10(scheduled_for=SCHEDULED_FOR.astimezone(UTC))
    assert len(resolved) == 10
    assert all(isinstance(item, ResolvedStructure) for item in resolved)
    assert all(isinstance(item, ExternalResolvedStructure) for item in resolved)
    assert all(
        item.scenario_set.verify_hash()
        and item.scenario_set.scenario_asof == OBSERVED_AT.astimezone(UTC)
        and item.scenario_set.current_policy_version == INITIAL_POLICY_VERSION
        and item.scenario_set.current_policy_hash == INITIAL_POLICY_HASH
        and item.scenario_set.candidate_id == item.candidate.preselection_id
        and item.scenario_set.strategy_hash == item.candidate.strategy_hash
        for item in resolved
    )
    assert (
        _validate_structures(
            resolved,
            expected_phase=PreselectionPhase.PRE_MARKET,
        )
        is None
    )
    assert all(
        item.candidate.phase is PreselectionPhase.PRE_MARKET
        and item.candidate.decision_authority is NewsAuthority.SUPPORTING_ONLY
        and item.candidate.approval_eligible is False
        and item.candidate.instruction_creation_allowed is False
        for item in resolved
    )
    assert all(
        getattr(leg, field) is None
        for item in resolved
        for leg in item.candidate.legs
        for field in DYNAMIC_FIELDS
    )
    public = {
        name
        for cls in (ExternalTop10Publisher, ExternalTop10StructureSource)
        for name in dir(cls)
        if not name.startswith("_")
    }
    assert not public.intersection(
        {"approve", "create_instruction", "create_order", "place_order", "submit_order"}
    )


def test_publisher_uses_same_directory_temporary_and_replace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "external-top10.json"
    calls: list[tuple[Path, Path]] = []
    real_replace = os.replace

    def capture(source, destination) -> None:
        calls.append((Path(source), Path(destination)))
        real_replace(source, destination)

    monkeypatch.setattr("options_copilot.news.external_top10_source.os.replace", capture)
    _publish(path)

    assert len(calls) == 1
    temporary, destination = calls[0]
    assert temporary.parent == destination.parent == path.parent
    assert destination == path
    assert not temporary.exists()


@pytest.mark.parametrize("count", [9, 11])
def test_exactly_ten_is_required_without_padding_or_truncation(
    tmp_path: Path, count: int
) -> None:
    path = tmp_path / "external-top10.json"
    with pytest.raises(ExternalTop10ValidationError, match="exactly ten"):
        _publish(path, _payload(count))
    assert not path.exists()


@pytest.mark.parametrize(
    ("mutator", "match"),
    [
        (lambda doc: doc.__setitem__("schema", "unsupported"), "schema"),
        (lambda doc: doc.__setitem__("version", 2), "version"),
        (
            lambda doc: doc["structures"][0]["legs"][0]["identity"].pop("right"),
            "identity fields",
        ),
        (
            lambda doc: doc["structures"][1]["legs"][0]["identity"].__setitem__(
                "conId", doc["structures"][0]["legs"][0]["identity"]["conId"]
            ),
            "duplicate conId",
        ),
        (
            lambda doc: doc["structures"][1].__setitem__(
                "preselection_id", doc["structures"][0]["preselection_id"]
            ),
            "duplicate preselection",
        ),
        (
            lambda doc: doc["structures"][1].__setitem__(
                "strategy_hash", doc["structures"][0]["strategy_hash"]
            ),
            "duplicate strategy hash",
        ),
        (
            lambda doc: doc["structures"][0]["legs"][0].__setitem__("bid", "1.00"),
            "dynamic quote",
        ),
        (
            lambda doc: doc["structures"][0].__setitem__(
                "maximum_loss_usd", "10001.00"
            ),
            "normal 10%",
        ),
        (
            lambda doc: doc.__setitem__("a_grade_enabled", True),
            "A-grade",
        ),
        (
            lambda doc: doc["structures"][2]["legs"][0].__setitem__("side", "SELL"),
            "vertical direction",
        ),
        (
            lambda doc: doc["structures"][0].__setitem__("strategy_hash", "0" * 64),
            "strategy hash mismatch",
        ),
        (
            lambda doc: doc.__setitem__("evidence_hashes", ["0" * 64]),
            "evidence hashes",
        ),
    ],
)
def test_reader_rejects_unsafe_partial_or_noncanonical_structures(
    tmp_path: Path, mutator, match: str
) -> None:
    path = tmp_path / "external-top10.json"
    _publish(path)
    _rewrite(path, mutator)
    with pytest.raises(ExternalTop10ValidationError, match=match):
        _source(path).resolve_top10(scheduled_for=SCHEDULED_FOR)


def test_reader_rejects_tamper_stale_observation_wrong_slot_and_wrong_request(
    tmp_path: Path,
) -> None:
    path = tmp_path / "external-top10.json"
    _publish(path)
    document = json.loads(path.read_text(encoding="utf-8"))
    document["strategy_nav_usd"] = "999999.00"
    path.write_text(canonical_json(document) + "\n", encoding="utf-8")
    with pytest.raises(ExternalTop10ValidationError, match="content hash"):
        _source(path).resolve_top10(scheduled_for=SCHEDULED_FOR)

    _publish(path)
    with pytest.raises(ExternalTop10ValidationError, match="five minutes"):
        _source(path, now=OBSERVED_AT + timedelta(minutes=5, microseconds=1)).resolve_top10(
            scheduled_for=SCHEDULED_FOR
        )
    with pytest.raises(ExternalTop10ValidationError, match="requested scheduled_for"):
        _source(path).resolve_top10(scheduled_for=SCHEDULED_FOR + timedelta(days=1))

    invalid = _payload()
    invalid["scheduled_for"] = SCHEDULED_FOR.replace(hour=9, minute=21)
    with pytest.raises(ExternalTop10ValidationError, match="09:20"):
        _publish(tmp_path / "wrong-slot.json", invalid)


@pytest.mark.parametrize(
    ("mutator", "match"),
    (
        (
            lambda doc: doc["structures"][0].__setitem__(
                "scenario_asof", OBSERVED_AT + timedelta(microseconds=1)
            ),
            "future information",
        ),
        (
            lambda doc: doc["structures"][0].pop("scenario_asof"),
            "fields are incomplete",
        ),
        (
            lambda doc: doc["structures"][0].__setitem__("scenario_hash", "f" * 64),
            "scenario-set hash mismatch",
        ),
        (
            lambda doc: doc.__setitem__("current_policy_version", "stale"),
            "risk policy version",
        ),
        (
            lambda doc: doc.__setitem__("current_policy_hash", "f" * 64),
            "risk policy hash",
        ),
        (
            lambda doc: doc["structures"][0]["terminal_scenarios"][0].__setitem__(
                "probability", "0.40"
            ),
            "probabilities must sum to one",
        ),
    ),
)
def test_trusted_scenario_set_and_risk_policy_fail_closed(
    tmp_path: Path,
    mutator,
    match: str,
) -> None:
    path = tmp_path / "external-top10.json"
    _publish(path)
    _rewrite(path, mutator)

    with pytest.raises(ExternalTop10ValidationError, match=match):
        _source(path).resolve_top10(scheduled_for=SCHEDULED_FOR)


@pytest.mark.parametrize("days", [13, 36])
def test_reader_rejects_expiry_outside_normal_14_to_35_dte(
    tmp_path: Path, days: int
) -> None:
    path = tmp_path / "external-top10.json"
    _publish(path)

    def change_expiry(document) -> None:
        expiry = (SCHEDULED_FOR.date() + timedelta(days=days)).isoformat()
        document["structures"][0]["legs"][0]["identity"]["expiry"] = expiry

    _rewrite(path, change_expiry)
    with pytest.raises(ExternalTop10ValidationError, match="14-35 DTE"):
        _source(path).resolve_top10(scheduled_for=SCHEDULED_FOR)


def test_invalid_publish_keeps_last_good_file(tmp_path: Path) -> None:
    path = tmp_path / "external-top10.json"
    _publish(path)
    before = path.read_bytes()
    invalid = deepcopy(_payload())
    invalid["structures"] = invalid["structures"][:9]

    with pytest.raises(ExternalTop10ValidationError):
        _publish(path, invalid)

    assert path.read_bytes() == before
    assert not tuple(path.parent.glob(f".{path.name}.*.tmp"))
