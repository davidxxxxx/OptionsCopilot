"""Actual quote-batch integration keeps partial historical proof non-authoritative."""

from __future__ import annotations

import asyncio
from contextlib import contextmanager, nullcontext
from datetime import date, timedelta
from decimal import Decimal
from types import SimpleNamespace
import time

import pytest

from options_copilot.gateway.broker_snapshot import BrokerSnapshotBuilder, BrokerSnapshotStatus
from options_copilot.gateway.ibkr_readonly import IBKRReadOnlyGateway, QuoteBatchStatus
from tests.options_copilot.test_broker_snapshot import FakeSnapshotSource
from tests.options_copilot.test_ibkr_readonly_gateway import FakeIB, NOW, _config


class EagerHistoricalStreamingIB(FakeIB):
    """SDK-shaped eager Futures; no network or broker is constructed."""

    def __init__(self, *, complete_all: bool, event_age: int) -> None:
        super().__init__()
        self.exchange_time = None
        self.complete_all = complete_all
        self.event_age = event_age
        self.historical_calls: list[int] = []
        self.pending: list[asyncio.Future] = []

    def reqMktData(self, contract, *_args):
        return super().reqTickers(contract)[0]

    def cancelMktData(self, _contract) -> None:
        return None

    def sleep(self, _seconds: float) -> None:
        raise AssertionError("Fixture already has the required streaming fields")

    def reqHistoricalTicksAsync(self, contract, *_args):
        self.historical_calls.append(contract.conId)
        # ib-insync eagerly allocates on the owner's installed loop before run().
        future = asyncio.get_event_loop().create_future()
        if self.complete_all or len(self.historical_calls) == 1:
            future.set_result((SimpleNamespace(
                time=NOW - timedelta(seconds=self.event_age),
                priceBid=1.01,
                priceAsk=1.19,
            ),))
        else:
            self.pending.append(future)
        return future


class ShortFixtureDeadlineGateway(IBKRReadOnlyGateway):
    def _option_quote_batch_owner_timeout_seconds(self, _contract_count: int) -> float:
        # Test-only aggregate budget; production configuration is unchanged.
        return 1.08


def _actual_batch(tmp_path, *, complete_all: bool, event_age: int):
    fake = EagerHistoricalStreamingIB(complete_all=complete_all, event_age=event_age)
    leases = {"active": 0, "peak": 0}

    @contextmanager
    def historical_lease():
        if leases["active"] >= 2:
            yield SimpleNamespace(allowed=False, reason="PACING_CONCURRENCY_LIMIT")
            return
        leases["active"] += 1
        leases["peak"] = max(leases["peak"], leases["active"])
        try:
            yield SimpleNamespace(allowed=True, reason=None)
        finally:
            leases["active"] -= 1

    gateway = ShortFixtureDeadlineGateway(
        _config(tmp_path),
        ib_factory=lambda: fake,
        historical_request_lease_factory=historical_lease,
        historical_request_max_concurrency=2,
        market_data_request_lease_factory=lambda request_class: nullcontext(
            SimpleNamespace(allowed=True, reason=None, request_class=request_class)
        ),
        now=lambda: NOW,
    )
    try:
        gateway.connect()
        contracts = gateway.qualify_option_contracts(
            "SPY", date(2026, 8, 21), [Decimal("625"), Decimal("626"), Decimal("627")],
            rights=("C",),
        )
        definitions = gateway.option_contract_definitions(contracts)
        batch = gateway.option_quote_batch(contracts)
    finally:
        gateway.disconnect()
    assert leases == {"active": 0, "peak": 2}
    assert all(future.cancelled() for future in fake.pending)
    return contracts, definitions, batch, fake.historical_calls


def _snapshot(contracts, definitions, batch, *, built_after_seconds: int):
    source = FakeSnapshotSource()
    source.secdef_reads = [definitions, definitions]
    source.quote_batch = batch
    snapshot = BrokerSnapshotBuilder(
        source, clock=lambda: NOW + timedelta(seconds=built_after_seconds),
    ).build(contracts)
    assert all(item.known and item.stable for item in snapshot.state_evidence.values())
    assert snapshot.verify_hash()
    return snapshot


def test_partial_history_preserves_one_real_quote_but_not_atomic_authority(tmp_path):
    contracts, definitions, batch, sent = _actual_batch(
        tmp_path, complete_all=False, event_age=1,
    )
    ids = [contract.contract_id for contract in contracts]
    assert sent == ids[:2]
    assert batch.status is QuoteBatchStatus.PARTIAL
    first = next(quote for quote in batch.quotes if quote.contract_id == ids[0])
    assert (first.bid, first.ask, first.exchange_time) == (
        Decimal("1.01"), Decimal("1.19"), NOW - timedelta(seconds=1),
    )
    assert f"HISTORICAL_BBO_TIMEOUT:{ids[1]}" in batch.blockers
    assert f"HISTORICAL_BBO_TIMEOUT:{ids[0]}" not in batch.blockers
    assert f"HISTORICAL_BBO_TIMEOUT:{ids[2]}" not in batch.blockers
    assert f"HISTORICAL_BBO_DEADLINE_EXCEEDED:{ids[2]}" in batch.blockers
    snapshot = _snapshot(contracts, definitions, batch, built_after_seconds=0)
    assert snapshot.status is not BrokerSnapshotStatus.COMPLETE
    assert "MISSING_OR_INVALID_QUOTE_EXCHANGE_TIME" in snapshot.reason_codes
    assert "QUOTE_BATCH_PARTIAL" in snapshot.reason_codes


@pytest.mark.parametrize("built_after_seconds, expected_complete", [(0, True), (2, False)])
def test_complete_proofs_still_age_at_actual_atomic_snapshot_build(
    tmp_path, built_after_seconds, expected_complete,
):
    contracts, definitions, batch, sent = _actual_batch(
        tmp_path, complete_all=True, event_age=4,
    )
    assert sent == [contract.contract_id for contract in contracts]
    assert batch.status is QuoteBatchStatus.COMPLETE
    assert all(quote.exchange_time == NOW - timedelta(seconds=4) for quote in batch.quotes)
    snapshot = _snapshot(
        contracts, definitions, batch, built_after_seconds=built_after_seconds,
    )
    assert (snapshot.status is BrokerSnapshotStatus.COMPLETE) is expected_complete
    assert ("STALE_OR_FUTURE_QUOTE" in snapshot.reason_codes) is not expected_complete


@contextmanager
def _wrapper_backed_history(tmp_path):
    from ib_insync.wrapper import Wrapper

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    wrapper = Wrapper(SimpleNamespace())
    requests = []
    futures = []
    leases = {"active": 0}

    @contextmanager
    def historical_lease():
        leases["active"] += 1
        try:
            yield SimpleNamespace(allowed=True, reason=None)
        finally:
            leases["active"] -= 1

    class WrapperBackedIB:
        def reqHistoricalTicksAsync(self, contract, *_args):
            request_id = 900 + len(requests)
            requests.append((request_id, contract.conId))
            future = wrapper.startReq(request_id, contract)
            futures.append(future)
            if contract.conId == 1:
                wrapper.historicalTicksBidAsk(request_id, [SimpleNamespace(
                    time=NOW - timedelta(seconds=1), priceBid=1.01, priceAsk=1.19,
                )], True)
            return future

    gateway = IBKRReadOnlyGateway(
        _config(tmp_path), historical_request_lease_factory=historical_lease,
        historical_request_max_concurrency=2,
    )
    ib = WrapperBackedIB()
    gateway._require_ib = lambda: ib
    specs = tuple(
        (number, SimpleNamespace(conId=number), Decimal("1.0"), Decimal("1.2"))
        for number in (1, 2)
    )
    try:
        yield gateway, wrapper, loop, specs, requests, futures, leases
    finally:
        for future in futures:
            if not future.done():
                future.cancel()
        wrapper.reset()
        asyncio.set_event_loop(None)
        loop.close()


def test_late_sdk_wrapper_callbacks_clean_maps_without_promoting_timeout(tmp_path):
    with _wrapper_backed_history(tmp_path) as fixture:
        gateway, wrapper, loop, specs, requests, futures, leases = fixture
        resolutions, requested = gateway._matching_historical_bbo_times_batch(
            specs, observed_at=NOW, deadline=time.monotonic() + 0.05,
        )
        frozen = dict(resolutions)
        assert requested and requests == [(900, 1), (901, 2)]
        assert resolutions[1][0] == NOW - timedelta(seconds=1)
        assert resolutions[2][3] == ("HISTORICAL_BBO_TIMEOUT",)
        assert futures[0].done() and not futures[0].cancelled()
        assert futures[1].cancelled() and leases["active"] == 0
        # Local cancellation does not claim remote completion: maps remain.
        assert 901 in wrapper._futures and 901 in wrapper._results
        assert 901 in wrapper._reqId2Contract

        later = wrapper.startReq(902, SimpleNamespace(conId=3))
        futures.append(later)
        row = SimpleNamespace(time=NOW, priceBid=9.0, priceAsk=9.1)
        wrapper.historicalTicksBidAsk(901, [row], False)
        assert 901 in wrapper._futures and not later.done()
        wrapper.historicalTicksBidAsk(901, [], True)
        assert 901 not in wrapper._futures and 901 not in wrapper._results
        assert 901 not in wrapper._reqId2Contract
        assert futures[1].cancelled() and not later.done()
        assert resolutions == frozen and not asyncio.all_tasks(loop)
        later.cancel()
        wrapper.historicalTicksBidAsk(902, [], True)
        assert not wrapper._futures and not wrapper._results


def test_actual_collector_cancellation_drains_unresolved_sdk_futures(tmp_path):
    with _wrapper_backed_history(tmp_path) as fixture:
        gateway, wrapper, loop, specs, requests, futures, leases = fixture
        cancelled_collectors = []

        def cancel_collector():
            for task in asyncio.all_tasks(loop):
                if task.get_coro().__name__ == "collect_outcomes":
                    cancelled_collectors.append(task)
                    task.cancel()

        handle = loop.call_later(0.01, cancel_collector)
        try:
            with pytest.raises(asyncio.CancelledError):
                gateway._matching_historical_bbo_times_batch(
                    specs, observed_at=NOW, deadline=time.monotonic() + 0.5,
                )
        finally:
            handle.cancel()
        assert len(cancelled_collectors) == 1
        assert cancelled_collectors[0].cancelled()
        assert requests == [(900, 1), (901, 2)]
        assert futures[0].done() and not futures[0].cancelled()
        assert futures[0].result()[0].priceBid == 1.01
        assert futures[1].cancelled() and leases["active"] == 0
        assert not asyncio.all_tasks(loop)
        # No outcome is returned by the cancelled collector; late data only cleans up.
        wrapper.historicalTicksBidAsk(901, [SimpleNamespace(
            time=NOW, priceBid=9.0, priceAsk=9.1,
        )], True)
        assert futures[1].cancelled()
        assert not wrapper._futures and not wrapper._results
        assert not wrapper._reqId2Contract
