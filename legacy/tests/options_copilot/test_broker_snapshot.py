from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from options_copilot.gateway.broker_snapshot import (
    BrokerSnapshotBuilder,
    BrokerSnapshotStatus,
)
from options_copilot.gateway.ibkr_readonly import (
    BatchedOptionQuote,
    OptionContractRef,
    OptionQuoteBatch,
    OptionSecDefSnapshot,
    QuoteBatchStatus,
)
from options_copilot.storage.canonical import canonical_hash


NOW = datetime(2026, 8, 3, 15, 0, tzinfo=timezone.utc)
_BATCH_OBSERVED_UNSET = object()


def _contracts() -> tuple[OptionContractRef, ...]:
    return (
        OptionContractRef(
            contract_id=101,
            contract_id_ex="101@SMART",
            symbol="SPY",
            local_symbol="SPY   260821C00100000",
            expiration=date(2026, 8, 21),
            strike=Decimal("100"),
            right="C",
            exchange="SMART",
            trading_class="SPY",
            multiplier=100,
        ),
        OptionContractRef(
            contract_id=102,
            contract_id_ex="102@SMART",
            symbol="SPY",
            local_symbol="SPY   260821C00105000",
            expiration=date(2026, 8, 21),
            strike=Decimal("105"),
            right="C",
            exchange="SMART",
            trading_class="SPY",
            multiplier=100,
        ),
    )


def _secdefs() -> tuple[OptionSecDefSnapshot, ...]:
    return tuple(
        OptionSecDefSnapshot(
            contract_id=contract.contract_id,
            local_symbol=contract.local_symbol,
            trading_class=contract.trading_class,
            multiplier=contract.multiplier,
            exchange=contract.exchange,
            expiration=contract.expiration,
            strike=contract.strike,
            right=contract.right,
            security_type="OPT",
            currency="USD",
            standard_contract=True,
            adjusted=False,
            source="IBKR_REQ_CONTRACT_DETAILS",
        )
        for contract in _contracts()
    )


def _batch(
    *,
    first_observed: datetime = NOW - timedelta(seconds=1),
    second_observed: datetime = NOW - timedelta(seconds=1),
    status: QuoteBatchStatus = QuoteBatchStatus.COMPLETE,
    batch_id: str = "batch-1",
    source: str = "IBKR_REQ_TICKERS_READONLY",
    bids: tuple[str | None, str | None] = ("1.00", "0.50"),
    asks: tuple[str | None, str | None] = ("1.10", "0.60"),
    request_ids: tuple[str, str] = ("request-101", "request-102"),
    batch_observed_at: datetime | None | object = _BATCH_OBSERVED_UNSET,
) -> OptionQuoteBatch:
    observed = (first_observed, second_observed)
    requested_at = min(observed) - timedelta(milliseconds=1)
    completed_at = max(observed) + timedelta(milliseconds=1)
    quotes = tuple(
        BatchedOptionQuote(
            contract_id=contract.contract_id,
            batch_id=batch_id,
            request_id=request_ids[index],
            requested_at=requested_at,
            observed_at=observed[index],
            completed_at=completed_at,
            source=source,
            bid=None if bids[index] is None else Decimal(bids[index]),
            ask=None if asks[index] is None else Decimal(asks[index]),
            exchange_time=observed[index],
        )
        for index, contract in enumerate(_contracts())
    )
    resolved_batch_observed_at = (
        first_observed
        if batch_observed_at is _BATCH_OBSERVED_UNSET
        and first_observed == second_observed
        else batch_observed_at
    )
    return OptionQuoteBatch(
        batch_id=batch_id,
        status=status,
        requested_at=requested_at,
        completed_at=completed_at,
        source=source,
        quotes=quotes,
        observed_at=(
            resolved_batch_observed_at
            if isinstance(resolved_batch_observed_at, datetime)
            else None
        ),
    )


class FakeSnapshotSource:
    def __init__(self) -> None:
        self.account_reads = [
            {
                "currency": "USD",
                "connected": True,
                "net_liquidation": Decimal("2012.44"),
            },
            {
                "net_liquidation": Decimal("2012.44"),
                "connected": True,
                "currency": "USD",
            },
        ]
        self.position_reads = [
            (
                {"contract_id": 8, "quantity": Decimal("1")},
                {"contract_id": 7, "quantity": Decimal("-1")},
            ),
            (
                {"quantity": Decimal("-1"), "contract_id": 7},
                {"quantity": Decimal("1"), "contract_id": 8},
            ),
        ]
        self.order_reads = [(), ()]
        self.instruction_reads = [(), ()]
        self.secdef_reads = [_secdefs(), tuple(reversed(_secdefs()))]
        self.quote_batch = _batch()
        self.quote_calls = 0

    def account_snapshot(self):
        result = self.account_reads.pop(0)
        return {**result, "asof": NOW} if hasattr(self, "upstream_health") else result

    def positions(self):
        result = self.position_reads.pop(0)
        return tuple({**row, "asof": NOW} for row in result) if hasattr(self, "upstream_health") else result

    def working_orders(self):
        return self.order_reads.pop(0)

    def unsubmitted_instructions(self):
        return self.instruction_reads.pop(0)

    def option_contract_definitions(self, _contracts):
        return self.secdef_reads.pop(0)

    def option_quote_batch(self, _contracts):
        self.quote_calls += 1
        return self.quote_batch


def _build(source: FakeSnapshotSource | None = None, contracts=None):
    provider = source or FakeSnapshotSource()
    return BrokerSnapshotBuilder(provider, clock=lambda: NOW).build(
        _contracts() if contracts is None else contracts
    )


@pytest.mark.parametrize("recover", [False, True])
def test_snapshot_rejects_upstream_generation_change_during_quote_read(recover):
    class UpstreamSource(FakeSnapshotSource):
        generation = 1
        status = "READY"

        def upstream_health(self):
            return {"status": self.status, "generation": self.generation, "verified_at": NOW.isoformat()}

        def option_quote_batch(self, contracts):
            self.generation += 1
            self.status = "READY" if recover else "LOST"
            return super().option_quote_batch(contracts)

    snapshot = _build(UpstreamSource())
    assert snapshot.status is not BrokerSnapshotStatus.COMPLETE
    assert "BROKER_UPSTREAM_AUTHORITY_CHANGED" in snapshot.reason_codes
    assert snapshot.verify_hash()


def test_snapshot_rejects_loss_after_last_instruction_read():
    class UpstreamSource(FakeSnapshotSource):
        status = "READY"

        def upstream_health(self):
            return {"status": self.status, "generation": 1, "verified_at": NOW.isoformat()}

        def unsubmitted_instructions(self):
            result = super().unsubmitted_instructions()
            if not self.instruction_reads:
                self.status = "LOST"
            return result

    assert _build(UpstreamSource()).status is not BrokerSnapshotStatus.COMPLETE


def test_pre_state_keeps_its_generation_instead_of_recapturing_after_read():
    class UpstreamSource(FakeSnapshotSource):
        health_reads = 0

        def upstream_health(self):
            self.health_reads += 1
            return {
                "status": "READY", "generation": 1 if self.health_reads <= 4 else 3,
                "verified_at": NOW.isoformat(),
            }

    snapshot = _build(UpstreamSource())
    assert snapshot.status is not BrokerSnapshotStatus.COMPLETE
    assert "BROKER_UPSTREAM_AUTHORITY_CHANGED" in snapshot.reason_codes


def test_control_payload_cannot_be_bound_to_a_newer_health_token():
    class UpstreamSource(FakeSnapshotSource):
        def upstream_health(self):
            return {"status": "READY", "generation": 1, "verified_at": NOW.isoformat()}

        def account_snapshot(self):
            return {**super().account_snapshot(), "asof": NOW - timedelta(seconds=1)}

    snapshot = _build(UpstreamSource())
    assert "BROKER_CONTROL_BATCH_TIME_MISMATCH" in snapshot.reason_codes
    assert not snapshot.complete


def test_coherent_snapshot_is_atomic_canonical_and_uses_one_quote_batch() -> None:
    source = FakeSnapshotSource()
    snapshot = _build(source)

    assert snapshot.status is BrokerSnapshotStatus.COMPLETE
    assert snapshot.complete is True
    assert snapshot.reason_codes == ()
    assert source.quote_calls == 1
    assert (
        snapshot.state_evidence["account"].pre_hash
        == snapshot.state_evidence["account"].post_hash
    )
    assert snapshot.state_evidence["positions"].stable is True
    assert snapshot.oldest_quote_age_seconds == Decimal("1")
    assert snapshot.maximum_leg_skew_seconds == Decimal("0")
    assert snapshot.quote_batch_observed_at == NOW - timedelta(seconds=1)
    assert all(item.stable for item in snapshot.secdef_evidence)
    assert all(
        set(item.pre_identity or ())
        == set(item.post_identity or ())
        == {
            "conId",
            "localSymbol",
            "tradingClass",
            "multiplier",
            "exchange",
            "expiry",
            "strike",
            "right",
        }
        for item in snapshot.secdef_evidence
    )
    assert len(snapshot.snapshot_hash) == 64
    assert snapshot.verify_hash() is True


@pytest.mark.parametrize(
    ("component", "replacement"),
    [
        ("position_reads", ({"contract_id": 8, "quantity": Decimal("2")},)),
        ("order_reads", ({"order_id": 1, "status": "Submitted"},)),
        ("instruction_reads", ({"instruction_id": "pending-1"},)),
    ],
)
def test_each_state_component_mutation_fails_closed(
    component: str,
    replacement,
) -> None:
    source = FakeSnapshotSource()
    getattr(source, component)[1] = replacement

    snapshot = _build(source)

    assert snapshot.status is BrokerSnapshotStatus.MUTATED_DURING_BUILD
    assert snapshot.complete is False
    assert any(
        component.removesuffix("_reads").upper() in reason
        for reason in snapshot.reason_codes
    )


def test_mark_to_market_account_values_use_the_post_sample_without_mutation() -> None:
    source = FakeSnapshotSource()
    source.account_reads[1] = {
        **source.account_reads[1],
        "net_liquidation": Decimal("2013.17"),
        "buying_power": Decimal("987.65"),
    }

    snapshot = _build(source)

    account = snapshot.state_evidence["account"]
    assert snapshot.status is BrokerSnapshotStatus.COMPLETE
    assert account.stable is True
    assert account.pre_hash != account.post_hash
    assert account.post_hash == canonical_hash(account.state)
    assert account.state["net_liquidation"] == Decimal("2013.17")
    assert account.state["buying_power"] == Decimal("987.65")


@pytest.mark.parametrize(
    ("field", "value"),
    [("currency", "CAD"), ("connected", False), ("account_id", "DU999")],
)
def test_account_identity_or_connection_mutation_still_fails_closed(
    field: str,
    value,
) -> None:
    source = FakeSnapshotSource()
    source.account_reads[0]["account_id"] = "DU123"
    source.account_reads[1]["account_id"] = "DU123"
    source.account_reads[1][field] = value

    snapshot = _build(source)

    assert snapshot.status is BrokerSnapshotStatus.MUTATED_DURING_BUILD
    assert "ACCOUNT_MUTATED" in snapshot.reason_codes


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("contract_id", 999),
        ("local_symbol", "SPY-CHANGED"),
        ("trading_class", "SPX"),
        ("multiplier", 10),
        ("exchange", "CBOE"),
        ("expiration", date(2026, 8, 28)),
        ("strike", Decimal("101")),
        ("right", "P"),
    ],
)
def test_every_secdef_identity_field_mutation_is_detected(field: str, value) -> None:
    source = FakeSnapshotSource()
    changed = replace(source.secdef_reads[1][0], **{field: value})
    source.secdef_reads[1] = (changed, source.secdef_reads[1][1])

    snapshot = _build(source)

    assert snapshot.status is BrokerSnapshotStatus.MUTATED_DURING_BUILD
    assert "SECDEF_MUTATED" in snapshot.reason_codes


@pytest.mark.parametrize(
    ("age", "expected"),
    [(Decimal("5.000"), True), (Decimal("5.001"), False)],
)
def test_quote_age_boundary_is_exact(age: Decimal, expected: bool) -> None:
    source = FakeSnapshotSource()
    source.quote_batch = _batch(
        first_observed=NOW - timedelta(seconds=float(age)),
        second_observed=NOW - timedelta(seconds=float(age)),
    )

    snapshot = _build(source)

    assert snapshot.complete is expected
    assert (snapshot.status is BrokerSnapshotStatus.QUOTE_INCOHERENT) is not expected


def test_exchange_time_age_is_rechecked_at_atomic_snapshot_completion() -> None:
    source = FakeSnapshotSource()
    batch = _batch(
        first_observed=NOW - timedelta(seconds=1),
        second_observed=NOW - timedelta(seconds=1),
    )
    source.quote_batch = replace(
        batch,
        quotes=tuple(
            replace(item, exchange_time=NOW - timedelta(seconds=5, microseconds=1))
            for item in batch.quotes
        ),
    )

    snapshot = _build(source)

    assert snapshot.status is BrokerSnapshotStatus.QUOTE_INCOHERENT
    assert "STALE_OR_FUTURE_QUOTE" in snapshot.reason_codes
    assert snapshot.oldest_quote_age_seconds == Decimal("5.000001")


def test_future_quote_remains_incoherent() -> None:
    source = FakeSnapshotSource()
    future = NOW + timedelta(microseconds=1)
    source.quote_batch = _batch(
        first_observed=future,
        second_observed=future,
        batch_observed_at=future,
    )

    snapshot = _build(source)

    assert snapshot.status is BrokerSnapshotStatus.QUOTE_INCOHERENT
    assert "STALE_OR_FUTURE_QUOTE" in snapshot.reason_codes


@pytest.mark.parametrize(
    ("skew", "expected"),
    [(Decimal("2.000"), True), (Decimal("2.001"), False)],
)
def test_leg_skew_boundary_is_exact(skew: Decimal, expected: bool) -> None:
    source = FakeSnapshotSource()
    source.quote_batch = _batch(
        first_observed=NOW - timedelta(seconds=float(skew)),
        second_observed=NOW,
    )

    snapshot = _build(source)

    assert snapshot.complete is expected
    assert (snapshot.status is BrokerSnapshotStatus.QUOTE_INCOHERENT) is not expected


def test_exchange_time_leg_skew_cannot_be_hidden_by_shared_observation_time() -> None:
    source = FakeSnapshotSource()
    batch = _batch(
        first_observed=NOW,
        second_observed=NOW,
    )
    source.quote_batch = replace(
        batch,
        quotes=(
            replace(
                batch.quotes[0],
                exchange_time=NOW - timedelta(seconds=2, microseconds=1),
            ),
            batch.quotes[1],
        ),
    )

    snapshot = _build(source)

    assert snapshot.status is BrokerSnapshotStatus.QUOTE_INCOHERENT
    assert "QUOTE_LEG_SKEW_EXCEEDED" in snapshot.reason_codes
    assert snapshot.maximum_leg_skew_seconds == Decimal("2.000001")


def test_missing_batch_observation_remains_explicit_for_strict_consumers() -> None:
    source = FakeSnapshotSource()
    observed_at = NOW - timedelta(seconds=1)
    source.quote_batch = _batch(
        first_observed=observed_at,
        second_observed=observed_at,
        batch_observed_at=None,
    )

    snapshot = _build(source)

    assert snapshot.status is BrokerSnapshotStatus.COMPLETE
    assert snapshot.quote_batch_observed_at is None


def test_explicit_batch_observation_must_match_every_leg() -> None:
    source = FakeSnapshotSource()
    source.quote_batch = _batch(
        first_observed=NOW - timedelta(seconds=1),
        second_observed=NOW,
        batch_observed_at=NOW,
    )

    snapshot = _build(source)

    assert snapshot.status is BrokerSnapshotStatus.QUOTE_INCOHERENT
    assert "QUOTE_BATCH_IDENTITY_MISMATCH" in snapshot.reason_codes


def test_uniform_explicit_batch_observation_preserves_complete_snapshot() -> None:
    source = FakeSnapshotSource()
    observed_at = NOW - timedelta(seconds=1)
    source.quote_batch = _batch(
        first_observed=observed_at,
        second_observed=observed_at,
        batch_observed_at=observed_at,
    )

    snapshot = _build(source)

    assert snapshot.status is BrokerSnapshotStatus.COMPLETE
    assert snapshot.maximum_leg_skew_seconds == Decimal("0")
    assert snapshot.quote_batch_observed_at == observed_at
    assert all(item.stable for item in snapshot.secdef_evidence)


@pytest.mark.parametrize(
    "batch",
    [
        _batch(bids=(None, "0.50")),
        _batch(asks=("1.10", None)),
        _batch(batch_id=""),
        _batch(request_ids=("duplicate", "duplicate")),
        _batch(status=QuoteBatchStatus.CANCELLED),
        _batch(status=QuoteBatchStatus.TIMEOUT),
        _batch(status=QuoteBatchStatus.PARTIAL),
    ],
)
def test_partial_cancelled_timed_out_or_unidentified_batch_is_incoherent(
    batch: OptionQuoteBatch,
) -> None:
    source = FakeSnapshotSource()
    source.quote_batch = batch

    snapshot = _build(source)

    assert snapshot.status is BrokerSnapshotStatus.QUOTE_INCOHERENT
    assert snapshot.complete is False


def test_each_quote_leg_must_bind_the_parent_batch_id() -> None:
    source = FakeSnapshotSource()
    source.quote_batch = replace(
        source.quote_batch,
        quotes=(
            replace(source.quote_batch.quotes[0], batch_id="different-batch"),
            source.quote_batch.quotes[1],
        ),
    )

    snapshot = _build(source)

    assert snapshot.status is BrokerSnapshotStatus.QUOTE_INCOHERENT
    assert "QUOTE_BATCH_IDENTITY_MISMATCH" in snapshot.reason_codes


def test_empty_state_is_known_but_unknown_state_is_incomplete() -> None:
    known_empty = _build()
    source = FakeSnapshotSource()
    source.instruction_reads = [None, None]
    unknown = _build(source)

    assert known_empty.complete is True
    assert known_empty.state_evidence["unsubmitted_instructions"].known is True
    assert known_empty.state_evidence["unsubmitted_instructions"].count == 0
    assert unknown.status is BrokerSnapshotStatus.STATE_INCOMPLETE
    assert unknown.complete is False


def test_duplicate_requested_leg_and_nonstandard_contract_fail_closed() -> None:
    duplicate = _build(contracts=(_contracts()[0], _contracts()[0]))
    source = FakeSnapshotSource()
    source.secdef_reads[0] = (
        replace(source.secdef_reads[0][0], adjusted=True),
        source.secdef_reads[0][1],
    )
    adjusted = _build(source)

    assert duplicate.status is BrokerSnapshotStatus.MUTATED_DURING_BUILD
    assert "DUPLICATE_REQUESTED_CONID" in duplicate.reason_codes
    assert adjusted.status is BrokerSnapshotStatus.MUTATED_DURING_BUILD
    assert "NONSTANDARD_CONTRACT" in adjusted.reason_codes


def test_stable_nonstandard_multiplier_and_requested_identity_mismatch_fail_closed() -> (
    None
):
    nonstandard_source = FakeSnapshotSource()
    nonstandard = replace(
        nonstandard_source.secdef_reads[0][0],
        multiplier=10,
        standard_contract=False,
    )
    nonstandard_source.secdef_reads = [
        (nonstandard, nonstandard_source.secdef_reads[0][1]),
        (nonstandard, nonstandard_source.secdef_reads[1][1]),
    ]
    multiplier_snapshot = _build(nonstandard_source)

    mismatch_source = FakeSnapshotSource()
    mismatched = replace(mismatch_source.secdef_reads[0][0], strike=Decimal("101"))
    mismatch_source.secdef_reads = [
        (mismatched, mismatch_source.secdef_reads[0][1]),
        (mismatched, mismatch_source.secdef_reads[1][1]),
    ]
    mismatch_snapshot = _build(mismatch_source)

    assert multiplier_snapshot.status is BrokerSnapshotStatus.MUTATED_DURING_BUILD
    assert "NONSTANDARD_CONTRACT" in multiplier_snapshot.reason_codes
    assert mismatch_snapshot.status is BrokerSnapshotStatus.MUTATED_DURING_BUILD
    assert "SECDEF_REQUEST_MISMATCH" in mismatch_snapshot.reason_codes


def test_reordering_is_hash_stable_but_quote_metadata_and_values_are_bound() -> None:
    baseline = _build()
    reordered_source = FakeSnapshotSource()
    reordered_source.secdef_reads = [
        tuple(reversed(reordered_source.secdef_reads[0])),
        reordered_source.secdef_reads[1],
    ]
    reordered_source.quote_batch = replace(
        reordered_source.quote_batch,
        quotes=tuple(reversed(reordered_source.quote_batch.quotes)),
    )
    reordered = _build(reordered_source)
    changed_source = FakeSnapshotSource()
    changed_source.quote_batch = _batch(
        bids=("2.00", "1.50"),
        asks=("2.10", "1.60"),
        request_ids=("changed-101", "changed-102"),
        source="IBKR_CHANGED_PROVENANCE",
    )
    changed = _build(changed_source)

    assert reordered.complete is True
    assert reordered.snapshot_hash == baseline.snapshot_hash
    assert changed.complete is True
    assert changed.snapshot_hash != baseline.snapshot_hash


def test_independently_changing_all_prices_can_remain_coherent() -> None:
    source = FakeSnapshotSource()
    source.quote_batch = _batch(
        bids=("3.00", "2.00"),
        asks=("3.25", "2.25"),
    )

    snapshot = _build(source)

    assert snapshot.complete is True
    assert [quote.bid for quote in snapshot.quotes] == [
        Decimal("3.00"),
        Decimal("2.00"),
    ]
