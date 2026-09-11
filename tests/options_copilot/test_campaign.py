from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from options_copilot.performance.campaign import TenKCampaign, trades_to_target
from options_copilot.performance.nav_ledger import StrategyNavLedger


START = datetime(2026, 8, 3, 0, 0, tzinfo=timezone.utc)
CONTRACT_PATH = (
    Path(__file__).resolve().parents[2]
    / "options_copilot"
    / "governance"
    / "strategy_nav_contract.v1.json"
)


def test_campaign_excludes_external_deposits_from_strategy_nav() -> None:
    campaign = TenKCampaign(start_nlv=Decimal("2012.44"), started_at=START)
    snapshot = campaign.mark(
        actual_nlv=Decimal("10012.44"),
        external_cash_flow=Decimal("8000"),
        asof=START + timedelta(days=1),
    )
    assert snapshot.actual_nlv == Decimal("10012.44")
    assert snapshot.strategy_nav == Decimal("2012.44")
    assert snapshot.progress_fraction == 0
    assert snapshot.next_milestone == Decimal("2500")


def test_campaign_compounds_time_weighted_period_returns() -> None:
    campaign = TenKCampaign(start_nlv=Decimal("2000"), started_at=START)
    first = campaign.mark(actual_nlv=Decimal("2200"), asof=START + timedelta(days=1))
    second = campaign.mark(
        actual_nlv=Decimal("3300"),
        external_cash_flow=Decimal("1000"),
        asof=START + timedelta(days=2),
    )
    assert first.strategy_nav == Decimal("2200")
    assert second.strategy_nav == Decimal("2300.00")


def test_campaign_pauses_at_thirty_percent_drawdown() -> None:
    campaign = TenKCampaign(start_nlv=Decimal("2000"), started_at=START)
    campaign.mark(actual_nlv=Decimal("2500"), asof=START + timedelta(days=1))
    snapshot = campaign.mark(actual_nlv=Decimal("1750"), asof=START + timedelta(days=2))
    assert snapshot.drawdown_fraction == Decimal("0.30")
    assert snapshot.paused is True


def test_trade_count_math_matches_ten_k_campaign_examples() -> None:
    assert trades_to_target(
        start_nlv=Decimal("2012.44"),
        target=Decimal("10000"),
        geometric_net_growth_per_trade=Decimal("0.01"),
    ) == 162
    assert trades_to_target(
        start_nlv=Decimal("2012.44"),
        target=Decimal("10000"),
        geometric_net_growth_per_trade=Decimal("0.02"),
    ) == 81


def test_campaign_can_only_display_a_valid_hash_bound_strategy_nav(
    tmp_path: Path,
) -> None:
    with StrategyNavLedger(
        tmp_path / "strategy-nav.sqlite3",
        contract=CONTRACT_PATH,
    ) as ledger:
        nav = ledger.snapshot(
            asof=START,
            observed_account_nlv=Decimal("50000"),
        )

    campaign = TenKCampaign(
        start_nlv=Decimal("2012.44"),
        started_at=START - timedelta(seconds=1),
    )
    snapshot = campaign.mark_from_strategy_nav(nav)

    assert snapshot.actual_nlv == Decimal("50000")
    assert snapshot.strategy_nav == Decimal("2012.44")
    assert snapshot.progress_fraction == Decimal("0")
    assert snapshot.strategy_nav_authoritative is True
    assert snapshot.strategy_nav_snapshot_hash == nav.content_hash
    assert snapshot.strategy_nav_contract_hash == nav.contract_hash
    assert snapshot.strategy_nav_ledger_head_hash == nav.ledger_head_hash


def test_campaign_rejects_an_invalid_strategy_nav_display_source(tmp_path: Path) -> None:
    with StrategyNavLedger(
        tmp_path / "missing-contract.sqlite3",
        contract=None,
    ) as ledger:
        invalid = ledger.snapshot(asof=START)

    campaign = TenKCampaign(
        start_nlv=Decimal("2012.44"),
        started_at=START - timedelta(seconds=1),
    )
    with pytest.raises(ValueError, match="valid Strategy NAV"):
        campaign.mark_from_strategy_nav(invalid)
