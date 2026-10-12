from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from options_copilot.analytics.scenarios import INITIAL_POLICY_HASH, INITIAL_POLICY_VERSION
from options_copilot.gateway.external_readonly_feed import (
    OPEN_REPRICE_PURPOSE,
    PREMARKET_ACCOUNT_PURPOSE,
)
from options_copilot.news.external_tick_runner import (
    ExternalTickStatus,
    ExternalTop10TickRunner,
)
from options_copilot.news.external_top10_source import (
    ExternalResolvedStructure,
    TrustedTerminalScenarioSet,
)
from options_copilot.news.models import (
    ConditionalOptionLeg,
    ConditionalOptionPreselection,
    NewsAuthority,
    OptionLegSide,
    OptionRight,
    PreselectionPhase,
    PreselectionTerminalScenario,
)
from options_copilot.news.preselection import strategy_structure_hash
from options_copilot.news.preselection_producer import ResolvedStructure


NEW_YORK = ZoneInfo("America/New_York")
MORNING = datetime(2026, 8, 6, 9, 20, tzinfo=NEW_YORK)
OPEN = datetime(2026, 8, 6, 9, 35, tzinfo=NEW_YORK)
EXPIRY = date(2026, 8, 21)


class FakeSessionGate:
    def __init__(self, value: bool | None = True) -> None:
        self.value = value
        self.calls: list[datetime] = []

    def is_trading_session(self, *, scheduled_for: datetime) -> bool | None:
        self.calls.append(scheduled_for)
        return self.value


@dataclass
class FakeBatch:
    purpose: str
    batch_id: str
    completed_at: datetime
    content_hash: str
    secdef_rows: tuple[dict[str, object], ...] = ()
    quote_rows: tuple[dict[str, object], ...] = ()
    hash_valid: bool = True

    def verify_hash(self) -> bool:
        return self.hash_valid


class FakeFeedReader:
    def __init__(self, batch: object) -> None:
        self.batch = batch
        self.calls = 0

    def read(self) -> object:
        self.calls += 1
        if isinstance(self.batch, BaseException):
            raise self.batch
        return self.batch


class FakeStructureSource:
    def __init__(self, structures: object) -> None:
        self.structures = structures
        self.calls: list[datetime] = []

    def resolve_top10(self, *, scheduled_for: datetime):
        self.calls.append(scheduled_for)
        if isinstance(self.structures, BaseException):
            raise self.structures
        return self.structures


def _identity(index: int) -> dict[str, object]:
    return {
        "conId": 100_000 + index,
        "localSymbol": f"T{index:02d}  260821C00100000",
        "tradingClass": f"T{index:02d}",
        "multiplier": 100,
        "exchange": "SMART",
        "expiry": EXPIRY.isoformat(),
        "strike": "100",
        "right": "C",
    }


def _structures() -> tuple[ResolvedStructure, ...]:
    result: list[ResolvedStructure] = []
    for index in range(1, 11):
        symbol = f"T{index:02d}"
        identity = _identity(index)
        leg = ConditionalOptionLeg(
            underlying=symbol,
            con_id=int(identity["conId"]),
            expiry=EXPIRY,
            strike=Decimal("100"),
            right=OptionRight.CALL,
            side=OptionLegSide.BUY,
            ratio=1,
            quantity=1,
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
            multiplier=100,
            exchange="SMART",
        )
        candidate = ConditionalOptionPreselection(
            preselection_id=f"external-{index:02d}",
            underlying=symbol,
            strategy_type="LONG_CALL",
            phase=PreselectionPhase.PRE_MARKET,
            legs=(leg,),
            risk_defined=True,
            maximum_loss_usd=Decimal("500"),
            estimated_cost_usd=Decimal("250"),
            cost_after_ev_usd=Decimal("25"),
            entry_condition="Open quote remains executable.",
            invalidation_condition="Thesis invalidates.",
            profit_target_condition="Target reached.",
            stop_loss_condition="Risk threshold reached.",
            evidence_ids=(f"evidence-{index}",),
            evidence_hashes=(f"{index:064x}",),
            strategy_hash=strategy_structure_hash(symbol, "LONG_CALL", (leg,)),
            research_summary="Hash-bound external supporting-only structure.",
            terminal_scenarios=(
                PreselectionTerminalScenario(Decimal("90"), Decimal("0.50")),
                PreselectionTerminalScenario(Decimal("110"), Decimal("0.50")),
            ),
            scenario_hash="0" * 64,
        )
        scenario_set = TrustedTerminalScenarioSet.create(
            candidate_id=candidate.preselection_id,
            strategy_hash=candidate.strategy_hash,
            scenario_asof=MORNING - timedelta(seconds=30),
            scenarios=candidate.terminal_scenarios,
            current_policy_version=INITIAL_POLICY_VERSION,
            current_policy_hash=INITIAL_POLICY_HASH,
        )
        candidate = replace(candidate, scenario_hash=scenario_set.scenario_hash)
        result.append(ExternalResolvedStructure(candidate, scenario_set))
    return tuple(result)


def _open_batch(
    structures: tuple[ResolvedStructure, ...],
    *,
    completed_at: datetime = OPEN,
) -> FakeBatch:
    identities = tuple(
        _identity(index)
        for index, _structure in enumerate(structures, start=1)
    )
    return FakeBatch(
        purpose=OPEN_REPRICE_PURPOSE,
        batch_id="open-reprice-20260806-0935",
        completed_at=completed_at.astimezone(timezone.utc),
        content_hash="b" * 64,
        secdef_rows=tuple({"identity": dict(item)} for item in identities),
        quote_rows=tuple({"identity": dict(item)} for item in identities),
    )


def _runner(
    *,
    batch: object | None = None,
    structures: object | None = None,
    gate_value: bool | None = True,
):
    checked_structures = _structures() if structures is None else structures
    checked_batch = (
        FakeBatch(
            purpose=PREMARKET_ACCOUNT_PURPOSE,
            batch_id="premarket-account-20260806-0920",
            completed_at=MORNING.astimezone(timezone.utc),
            content_hash="a" * 64,
        )
        if batch is None
        else batch
    )
    gate = FakeSessionGate(gate_value)
    feed = FakeFeedReader(checked_batch)
    source = FakeStructureSource(checked_structures)
    return ExternalTop10TickRunner(
        session_gate=gate,
        feed_reader=feed,
        structure_source=source,
    ), gate, feed, source


def test_0920_requires_premarket_account_and_hash_bound_top10() -> None:
    runner, gate, feed, source = _runner()

    result = runner.tick(scheduled_for=MORNING)

    assert result.status is ExternalTickStatus.READY
    assert result.slot == "PREMARKET_0920"
    assert result.reason_codes == ()
    assert result.external_batch_id == "premarket-account-20260806-0920"
    assert result.external_batch_hash == "a" * 64
    assert len(result.parent_structure_hash) == 64
    assert result.structure_count == 10
    assert result.contract_count == 10
    assert len(result.scenario_contract_hash or "") == 64
    assert result.risk_policy_version == INITIAL_POLICY_VERSION
    assert result.risk_policy_hash == INITIAL_POLICY_HASH
    assert result.decision_authority is NewsAuthority.SUPPORTING_ONLY
    assert result.approval_eligible is False
    assert result.instruction_creation_allowed is False
    assert result.order_allowed is False
    assert gate.calls == [MORNING]
    assert feed.calls == 1
    assert source.calls == [MORNING]


def test_0935_requires_open_feed_for_exact_0920_parent_contract_set() -> None:
    structures = _structures()
    runner, _, feed, source = _runner(structures=structures)
    morning = runner.tick(scheduled_for=MORNING)
    feed.batch = _open_batch(structures)

    opened = runner.tick(scheduled_for=OPEN)

    assert morning.status is opened.status is ExternalTickStatus.READY
    assert opened.slot == "OPEN_REPRICE_0935"
    assert opened.parent_structure_hash == morning.parent_structure_hash
    assert opened.external_batch_id == "open-reprice-20260806-0935"
    assert opened.external_batch_hash == "b" * 64
    assert opened.structure_count == opened.contract_count == 10
    assert opened.scenario_contract_hash == morning.scenario_contract_hash
    assert opened.risk_policy_hash == morning.risk_policy_hash == INITIAL_POLICY_HASH
    assert feed.calls == 2
    assert source.calls == [MORNING]


def test_0935_after_restart_without_parent_binding_is_no_trade() -> None:
    structures = _structures()
    runner, _, feed, source = _runner(batch=_open_batch(structures))

    result = runner.tick(scheduled_for=OPEN)

    assert result.status is ExternalTickStatus.NO_TRADE
    assert result.reason_codes == ("PARENT_0920_BINDING_MISSING",)
    assert feed.calls == 0
    assert source.calls == []


@pytest.mark.parametrize(
    "gate_value,reason",
    [(None, "SESSION_GATE_UNKNOWN"), (False, "SESSION_CLOSED")],
)
def test_session_gate_unknown_or_closed_short_circuits_all_external_reads(
    gate_value: bool | None,
    reason: str,
) -> None:
    runner, gate, feed, source = _runner(gate_value=gate_value)

    result = runner.tick(scheduled_for=MORNING)

    assert result.status is ExternalTickStatus.NO_TRADE
    assert result.reason_codes == (reason,)
    assert gate.calls == [MORNING]
    assert feed.calls == 0
    assert source.calls == []


@pytest.mark.parametrize(
    "batch,reason",
    [
        (
            FakeBatch(
                OPEN_REPRICE_PURPOSE,
                "wrong-purpose",
                MORNING.astimezone(timezone.utc),
                "a" * 64,
            ),
            "EXTERNAL_BATCH_PURPOSE_MISMATCH",
        ),
        (
            FakeBatch(
                PREMARKET_ACCOUNT_PURPOSE,
                "bad-hash",
                MORNING.astimezone(timezone.utc),
                "a" * 64,
                hash_valid=False,
            ),
            "EXTERNAL_BATCH_HASH_INVALID",
        ),
        (RuntimeError("stale external feed"), "EXTERNAL_BATCH_UNAVAILABLE"),
        (SimpleNamespace(purpose=PREMARKET_ACCOUNT_PURPOSE), "EXTERNAL_BATCH_INVALID"),
    ],
)
def test_invalid_stale_or_wrong_purpose_external_batch_is_no_trade(
    batch: object,
    reason: str,
) -> None:
    runner, _, _, source = _runner(batch=batch)

    result = runner.tick(scheduled_for=MORNING)

    assert result.status is ExternalTickStatus.NO_TRADE
    assert result.reason_codes == (reason,)
    assert source.calls == []


@pytest.mark.parametrize(
    "completed_at",
    [MORNING - timedelta(microseconds=1), MORNING + timedelta(minutes=1)],
)
def test_external_batch_must_complete_inside_the_exact_tick_window(
    completed_at: datetime,
) -> None:
    batch = FakeBatch(
        PREMARKET_ACCOUNT_PURPOSE,
        "off-slot",
        completed_at.astimezone(timezone.utc),
        "a" * 64,
    )
    runner, _, _, source = _runner(batch=batch)

    result = runner.tick(scheduled_for=MORNING)

    assert result.status is ExternalTickStatus.NO_TRADE
    assert result.reason_codes == ("EXTERNAL_BATCH_SLOT_MISMATCH",)
    assert source.calls == []


@pytest.mark.parametrize(
    "structures",
    [
        _structures()[:9],
        RuntimeError("external Top-10 content hash mismatch"),
        tuple(SimpleNamespace(candidate=item.candidate) for item in _structures()),
        tuple(ResolvedStructure(item.candidate) for item in _structures()),
    ],
)
def test_missing_or_invalid_hash_bound_top10_is_no_trade(structures: object) -> None:
    runner, _, _, _ = _runner(structures=structures)

    result = runner.tick(scheduled_for=MORNING)

    assert result.status is ExternalTickStatus.NO_TRADE
    assert "EXTERNAL_TOP10_INVALID" in result.reason_codes


@pytest.mark.parametrize("mutation", ("missing", "extra", "strike", "quote"))
def test_0935_contract_identity_must_equal_the_parent_set(mutation: str) -> None:
    structures = _structures()
    runner, _, feed, _ = _runner(structures=structures)
    morning = runner.tick(scheduled_for=MORNING)
    assert morning.status is ExternalTickStatus.READY
    opened = _open_batch(structures)
    secdefs = list(opened.secdef_rows)
    quotes = list(opened.quote_rows)
    if mutation == "missing":
        secdefs.pop()
        quotes.pop()
    elif mutation == "extra":
        extra = _identity(99)
        secdefs.append({"identity": extra})
        quotes.append({"identity": extra})
    elif mutation == "strike":
        changed = dict(secdefs[0]["identity"])
        changed["strike"] = "101"
        secdefs[0] = {"identity": changed}
        quotes[0] = {"identity": changed}
    else:
        changed = dict(quotes[0]["identity"])
        changed["localSymbol"] = "MISMATCH"
        quotes[0] = {"identity": changed}
    feed.batch = replace(
        opened,
        secdef_rows=tuple(secdefs),
        quote_rows=tuple(quotes),
    )

    result = runner.tick(scheduled_for=OPEN)

    assert result.status is ExternalTickStatus.NO_TRADE
    assert result.reason_codes == ("OPEN_PARENT_STRUCTURE_MISMATCH",)


def test_repeated_0920_cannot_replace_an_existing_parent_with_a_new_set() -> None:
    structures = _structures()
    runner, _, _, source = _runner(structures=structures)
    first = runner.tick(scheduled_for=MORNING)
    changed_leg = replace(
        structures[0].candidate.legs[0],
        con_id=999_999,
        local_symbol="CHANGED",
    )
    changed_candidate = replace(
        structures[0].candidate,
        legs=(changed_leg,),
        strategy_hash=strategy_structure_hash(
            structures[0].candidate.underlying,
            "LONG_CALL",
            (changed_leg,),
        ),
    )
    changed_scenario_set = TrustedTerminalScenarioSet.create(
        candidate_id=changed_candidate.preselection_id,
        strategy_hash=changed_candidate.strategy_hash,
        scenario_asof=structures[0].scenario_set.scenario_asof,
        scenarios=changed_candidate.terminal_scenarios,
        current_policy_version=INITIAL_POLICY_VERSION,
        current_policy_hash=INITIAL_POLICY_HASH,
    )
    changed_candidate = replace(
        changed_candidate,
        scenario_hash=changed_scenario_set.scenario_hash,
    )
    source.structures = (
        ExternalResolvedStructure(changed_candidate, changed_scenario_set),
        *structures[1:],
    )

    conflict = runner.tick(scheduled_for=MORNING)

    assert first.status is ExternalTickStatus.READY
    assert conflict.status is ExternalTickStatus.NO_TRADE
    assert conflict.reason_codes == ("PARENT_STRUCTURE_HASH_CONFLICT",)
    assert runner.parent_binding is not None
    assert runner.parent_binding.structure_hash == first.parent_structure_hash


@pytest.mark.parametrize("mutation", ("hash", "future", "policy"))
def test_runner_revalidates_scenario_contract_instead_of_trusting_source(
    mutation: str,
) -> None:
    structures = _structures()
    scenario_set = structures[0].scenario_set
    if mutation == "hash":
        changed = replace(scenario_set, scenario_hash="f" * 64)
    elif mutation == "future":
        changed = TrustedTerminalScenarioSet.create(
            candidate_id=scenario_set.candidate_id,
            strategy_hash=scenario_set.strategy_hash,
            scenario_asof=MORNING + timedelta(microseconds=1),
            scenarios=scenario_set.scenarios,
            current_policy_version=INITIAL_POLICY_VERSION,
            current_policy_hash=INITIAL_POLICY_HASH,
        )
    else:
        changed = TrustedTerminalScenarioSet.create(
            candidate_id=scenario_set.candidate_id,
            strategy_hash=scenario_set.strategy_hash,
            scenario_asof=scenario_set.scenario_asof,
            scenarios=scenario_set.scenarios,
            current_policy_version="stale",
            current_policy_hash="f" * 64,
        )
    changed_candidate = replace(
        structures[0].candidate,
        scenario_hash=changed.scenario_hash,
    )
    unsafe = (
        ExternalResolvedStructure(changed_candidate, changed),
        *structures[1:],
    )
    runner, _, _, _ = _runner(structures=unsafe)

    result = runner.tick(scheduled_for=MORNING)

    assert result.status is ExternalTickStatus.NO_TRADE
    assert result.reason_codes == ("EXTERNAL_TOP10_INVALID",)


def test_runner_has_no_approval_instruction_or_order_surface() -> None:
    public = {name for name in dir(ExternalTop10TickRunner) if not name.startswith("_")}
    assert public.isdisjoint(
        {"approve", "authorize", "create_instruction", "submit_order", "place_order"}
    )
