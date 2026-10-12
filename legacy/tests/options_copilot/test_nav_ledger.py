from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import sqlite3
import threading
from pathlib import Path

import pytest

from options_copilot.performance.nav_ledger import (
    NavAttribution,
    NavEventKind,
    STRATEGY_NAV_AUTHORITY_SCHEMA,
    StrategyNavLedger,
    strategy_nav_authority_hash,
)
from options_copilot.storage.canonical import canonical_hash


START = datetime(2026, 8, 3, 14, 23, 12, tzinfo=timezone.utc)
CONTRACT_PATH = (
    Path(__file__).resolve().parents[2]
    / "options_copilot"
    / "governance"
    / "strategy_nav_contract.v1.json"
)


def _ledger(tmp_path: Path) -> StrategyNavLedger:
    return StrategyNavLedger(
        tmp_path / "strategy-nav.sqlite3",
        contract=CONTRACT_PATH,
        clock=lambda: START + timedelta(hours=1),
    )


def _append(
    ledger: StrategyNavLedger,
    event_kind: NavEventKind,
    identifier: str,
    amount: str,
    *,
    at: datetime,
    attribution: NavAttribution = NavAttribution.STRATEGY,
    position_id: str | None = None,
):
    return ledger.append_flow(
        event_kind=event_kind,
        broker_event_identifier=identifier,
        effective_at=at,
        amount=Decimal(amount),
        attribution=attribution,
        position_id=position_id,
    )


def test_anchor_is_valid_strategy_nav_and_nlv_is_reconciliation_only(
    tmp_path: Path,
) -> None:
    with _ledger(tmp_path) as ledger:
        low_nlv = ledger.snapshot(asof=START, observed_account_nlv=Decimal("500"))
        high_nlv = ledger.snapshot(asof=START, observed_account_nlv=Decimal("50000"))

    assert low_nlv.valid is True
    assert low_nlv.strategy_nav == Decimal("2012.44")
    assert high_nlv.strategy_nav == low_nlv.strategy_nav
    assert high_nlv.ledger_head_hash == low_nlv.ledger_head_hash
    assert high_nlv.contract_hash == (
        "536a938ed773c606ac8c80e6c6a2046f68dff3ee41d43ae599743194ef2dbedd"
    )
    assert low_nlv.observed_account_nlv == Decimal("500")
    assert high_nlv.observed_account_nlv == Decimal("50000")
    assert low_nlv.reconciliation_difference == Decimal("-1512.44")
    assert high_nlv.reconciliation_difference == Decimal("47987.56")


def test_authority_hash_is_stable_across_observation_only_changes(
    tmp_path: Path,
) -> None:
    with _ledger(tmp_path) as ledger:
        first = ledger.snapshot(
            asof=START,
            observed_account_nlv=Decimal("500"),
        )
        later = ledger.snapshot(
            asof=START + timedelta(minutes=1),
            observed_account_nlv=Decimal("50000"),
        )

    expected_payload = {
        "schema": STRATEGY_NAV_AUTHORITY_SCHEMA,
        "strategy_nav_usd": Decimal("2012.44"),
        "contract_hash": first.contract_hash,
        "ledger_head_hash": first.ledger_head_hash,
    }
    assert first.content_hash != later.content_hash
    assert first.authority_payload() == expected_payload
    assert later.authority_payload() == expected_payload
    assert first.authority_hash == canonical_hash(expected_payload)
    assert later.authority_hash == first.authority_hash
    assert first.strategy_nav_authority_hash == first.authority_hash
    assert strategy_nav_authority_hash(
        strategy_nav_usd=first.strategy_nav,
        contract_hash=first.contract_hash,
        ledger_head_hash=first.ledger_head_hash,
    ) == first.authority_hash


def test_authority_hash_changes_when_nav_authority_changes(tmp_path: Path) -> None:
    with _ledger(tmp_path) as ledger:
        before = ledger.snapshot(asof=START)
        _append(
            ledger,
            NavEventKind.FEE,
            "authority-hash-fee",
            "1",
            at=START + timedelta(minutes=1),
        )
        after = ledger.snapshot(asof=START + timedelta(minutes=2))

    assert before.strategy_nav == Decimal("2012.44")
    assert after.strategy_nav == Decimal("2011.44")
    assert before.ledger_head_hash != after.ledger_head_hash
    assert before.authority_hash != after.authority_hash


def test_guard_current_blocks_second_writer_and_rejects_old_head(
    tmp_path: Path,
) -> None:
    path = tmp_path / "strategy-nav.sqlite3"
    left = StrategyNavLedger(path, contract=CONTRACT_PATH)
    right = StrategyNavLedger(path, contract=CONTRACT_PATH)
    entered, release = threading.Event(), threading.Event()
    try:
        snapshot = left.snapshot(asof=START)

        def guarded() -> object | None:
            return left.guard_current(
                snapshot,
                callback=lambda: (
                    entered.set(),
                    release.wait(timeout=5),
                    "approved",
                )[-1],
            )

        with ThreadPoolExecutor(max_workers=2) as workers:
            guard_future = workers.submit(guarded)
            assert entered.wait(timeout=5)
            writer = workers.submit(
                _append,
                right,
                NavEventKind.FEE,
                "fee-after-guard",
                "1",
                at=START + timedelta(minutes=1),
            )
            assert not writer.done()
            release.set()
            assert guard_future.result(timeout=5) == "approved"
            assert writer.result(timeout=5).inserted is True

        callbacks: list[str] = []
        assert left.guard_current(
            snapshot,
            callback=lambda: callbacks.append("called"),
        ) is None
        assert callbacks == []
    finally:
        left.close()
        right.close()


def test_guard_current_propagates_callback_failure(tmp_path: Path) -> None:
    with _ledger(tmp_path) as ledger:
        snapshot = ledger.snapshot(asof=START)
        with pytest.raises(RuntimeError, match="callback failed"):
            ledger.guard_current(
                snapshot,
                callback=lambda: (_ for _ in ()).throw(
                    RuntimeError("callback failed")
                ),
            )


def test_signed_cash_flow_equation_and_latest_unrealized_mark(tmp_path: Path) -> None:
    with _ledger(tmp_path) as ledger:
        _append(
            ledger,
            NavEventKind.DEPOSIT,
            "strategy-deposit-1",
            "100",
            at=START + timedelta(minutes=1),
        )
        _append(
            ledger,
            NavEventKind.WITHDRAWAL,
            "strategy-withdrawal-1",
            "20",
            at=START + timedelta(minutes=2),
        )
        _append(
            ledger,
            NavEventKind.REALIZED_PNL,
            "realized-1",
            "-10",
            at=START + timedelta(minutes=3),
        )
        _append(
            ledger,
            NavEventKind.FEE,
            "fee-1",
            "2",
            at=START + timedelta(minutes=4),
        )
        _append(
            ledger,
            NavEventKind.UNREALIZED_PNL,
            "gld-mark-1",
            "30",
            at=START + timedelta(minutes=5),
            position_id="GLD-LEGACY-COMBO",
        )
        _append(
            ledger,
            NavEventKind.UNREALIZED_PNL,
            "gld-mark-2",
            "40",
            at=START + timedelta(minutes=6),
            position_id="GLD-LEGACY-COMBO",
        )
        _append(
            ledger,
            NavEventKind.DEPOSIT,
            "external-deposit-ignored",
            "1000",
            at=START + timedelta(minutes=7),
            attribution=NavAttribution.NON_STRATEGY,
        )
        _append(
            ledger,
            NavEventKind.FILL_PRINCIPAL,
            "fill-principal-zero-delta",
            "500",
            at=START + timedelta(minutes=8),
        )
        snapshot = ledger.snapshot(asof=START + timedelta(minutes=9))

    assert snapshot.valid is True
    assert snapshot.strategy_deposits == Decimal("100")
    assert snapshot.strategy_withdrawals == Decimal("20")
    assert snapshot.realized_pnl == Decimal("-10")
    assert snapshot.open_position_unrealized_pnl == Decimal("40")
    assert snapshot.fees == Decimal("2")
    assert snapshot.signed_corrections == Decimal("0")
    assert snapshot.non_strategy_contribution == Decimal("0")
    assert snapshot.fill_principal_contribution == Decimal("0")
    assert snapshot.strategy_nav == Decimal("2120.44")


def test_duplicate_is_idempotent_but_identity_conflict_invalidates_risk(
    tmp_path: Path,
) -> None:
    with _ledger(tmp_path) as ledger:
        first = _append(
            ledger,
            NavEventKind.REALIZED_PNL,
            "same-broker-event",
            "25",
            at=START + timedelta(minutes=1),
        )
        duplicate = _append(
            ledger,
            NavEventKind.REALIZED_PNL,
            "same-broker-event",
            "25",
            at=START + timedelta(minutes=1),
        )
        conflict = _append(
            ledger,
            NavEventKind.REALIZED_PNL,
            "same-broker-event",
            "26",
            at=START + timedelta(minutes=1),
        )
        snapshot = ledger.snapshot(asof=START + timedelta(minutes=2))

    assert first.inserted is True
    assert duplicate.inserted is False
    assert duplicate.flow_id == first.flow_id
    assert duplicate.content_hash == first.content_hash
    assert conflict.conflict is True
    assert snapshot.valid is False
    assert "FLOW_IDENTITY_CONFLICT" in snapshot.no_trade_reasons


def test_equivalent_decimal_renderings_are_idempotent(tmp_path: Path) -> None:
    with _ledger(tmp_path) as ledger:
        first = _append(
            ledger,
            NavEventKind.REALIZED_PNL,
            "decimal-normalization",
            "25.0",
            at=START + timedelta(minutes=1),
        )
        duplicate = _append(
            ledger,
            NavEventKind.REALIZED_PNL,
            "decimal-normalization",
            "25.000",
            at=START + timedelta(minutes=1),
        )

    assert first.inserted is True
    assert duplicate.inserted is False
    assert duplicate.conflict is False
    assert duplicate.content_hash == first.content_hash


def test_append_only_correction_references_prior_and_applies_signed_delta(
    tmp_path: Path,
) -> None:
    path = tmp_path / "strategy-nav.sqlite3"
    with _ledger(tmp_path) as ledger:
        original = _append(
            ledger,
            NavEventKind.REALIZED_PNL,
            "realized-original",
            "-10",
            at=START + timedelta(minutes=1),
        )
        correction = ledger.correct_flow(
            original.flow_id,
            broker_event_identifier="realized-correction-v2",
            effective_at=START + timedelta(minutes=2),
            amount=Decimal("-5"),
            actor="human:xujie",
            signed_at=START + timedelta(minutes=3),
        )
        snapshot = ledger.snapshot(asof=START + timedelta(minutes=4))

    assert correction.inserted is True
    assert correction.supersedes_flow_id == original.flow_id
    assert correction.version == 2
    assert snapshot.realized_pnl == Decimal("-10")
    assert snapshot.signed_corrections == Decimal("5")
    assert snapshot.strategy_nav == Decimal("2007.44")
    with sqlite3.connect(path) as connection:
        rows = connection.execute(
            "SELECT flow_id, supersedes_flow_id FROM nav_flows ORDER BY sequence"
        ).fetchall()
    assert rows == [
        (original.flow_id, None),
        (correction.flow_id, original.flow_id),
    ]


def test_correction_retry_is_idempotent_and_correction_fork_is_rejected(
    tmp_path: Path,
) -> None:
    with _ledger(tmp_path) as ledger:
        original = _append(
            ledger,
            NavEventKind.REALIZED_PNL,
            "correction-retry-original",
            "-10",
            at=START + timedelta(minutes=1),
        )
        first = ledger.correct_flow(
            original.flow_id,
            broker_event_identifier="correction-retry-v2",
            effective_at=START + timedelta(minutes=2),
            amount=Decimal("-5.0"),
            actor="human:xujie",
            signed_at=START + timedelta(minutes=3),
        )
        retry = ledger.correct_flow(
            original.flow_id,
            broker_event_identifier="correction-retry-v2",
            effective_at=START + timedelta(minutes=2),
            amount=Decimal("-5.000"),
            actor="human:xujie",
            signed_at=START + timedelta(minutes=3),
        )
        with pytest.raises(ValueError, match="latest correction version"):
            ledger.correct_flow(
                original.flow_id,
                broker_event_identifier="correction-fork-v2",
                effective_at=START + timedelta(minutes=4),
                amount=Decimal("-4"),
                actor="human:xujie",
                signed_at=START + timedelta(minutes=5),
            )

    assert first.inserted is True
    assert retry.inserted is False
    assert retry.flow_id == first.flow_id
    assert retry.content_hash == first.content_hash


def test_missing_contract_and_tampered_ledger_fail_closed(tmp_path: Path) -> None:
    missing_path = tmp_path / "missing-contract.sqlite3"
    with StrategyNavLedger(missing_path, contract=None) as ledger:
        missing = ledger.snapshot(asof=START)
    assert missing.valid is False
    assert "MISSING_STRATEGY_NAV_CONTRACT" in missing.no_trade_reasons
    assert "MISSING_LEDGER_HEAD" in missing.no_trade_reasons
    assert missing.strategy_nav is None

    path = tmp_path / "strategy-nav.sqlite3"
    with _ledger(tmp_path) as ledger:
        receipt = _append(
            ledger,
            NavEventKind.FEE,
            "fee-to-tamper",
            "1",
            at=START + timedelta(minutes=1),
        )
    with sqlite3.connect(path) as connection:
        connection.execute("DROP TRIGGER nav_flows_no_update")
        connection.execute(
            "UPDATE nav_flows SET amount = '999' WHERE flow_id = ?",
            (receipt.flow_id,),
        )
        connection.commit()
    with StrategyNavLedger(path, contract=CONTRACT_PATH) as ledger:
        tampered = ledger.snapshot(asof=START + timedelta(minutes=2))
    assert tampered.valid is False
    assert "LEDGER_INTEGRITY_FAILURE" in tampered.no_trade_reasons


def test_flow_amount_and_timestamp_boundaries_are_decimal_and_aware(
    tmp_path: Path,
) -> None:
    with _ledger(tmp_path) as ledger:
        with pytest.raises(TypeError, match="Decimal"):
            ledger.append_flow(
                event_kind=NavEventKind.FEE,
                broker_event_identifier="bad-float",
                effective_at=START,
                amount=1.0,
                attribution=NavAttribution.STRATEGY,
            )
        with pytest.raises(ValueError, match="timezone-aware"):
            ledger.append_flow(
                event_kind=NavEventKind.FEE,
                broker_event_identifier="bad-time",
                effective_at=START.replace(tzinfo=None),
                amount=Decimal("1"),
                attribution=NavAttribution.STRATEGY,
            )
        with pytest.raises(ValueError, match="nonnegative"):
            ledger.append_flow(
                event_kind=NavEventKind.FEE,
                broker_event_identifier="negative-fee",
                effective_at=START,
                amount=Decimal("-1"),
                attribution=NavAttribution.STRATEGY,
            )
