"""Historical BBO batches preserve completed eager-Future outcomes."""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
import time

from options_copilot.config import OptionsCopilotConfig
from options_copilot.gateway.ibkr_readonly import IBKRReadOnlyGateway


NOW = datetime(2026, 9, 9, 8, 0, tzinfo=timezone.utc)


class _ConcurrencyLeaseFactory:
    def __init__(self, limit: int) -> None:
        self.limit = limit
        self.active = 0
        self.peak = 0

    def __call__(self):
        factory = self

        class Lease:
            acquired = False

            def __enter__(self):
                if factory.active >= factory.limit:
                    return SimpleNamespace(
                        allowed=False,
                        reason="HISTORICAL_PACING_CONCURRENCY_LIMIT",
                    )
                self.acquired = True
                factory.active += 1
                factory.peak = max(factory.peak, factory.active)
                return SimpleNamespace(allowed=True, reason=None)

            def __exit__(self, *_args):
                if self.acquired:
                    factory.active -= 1

        return Lease()


class _EagerFutureIB:
    def __init__(self) -> None:
        self.requests: list[int] = []
        self.pending: asyncio.Future[object] | None = None

    def isConnected(self) -> bool:
        return True

    def reqHistoricalTicksAsync(self, contract, *_args, **_kwargs):
        contract_id = int(contract.conId)
        self.requests.append(contract_id)
        loop = asyncio.get_event_loop()
        future = loop.create_future()
        if contract_id == 1:
            future.set_result(
                [
                    SimpleNamespace(
                        time=NOW - timedelta(seconds=1),
                        priceBid=1.0,
                        priceAsk=1.2,
                    )
                ]
            )
        elif contract_id == 2:
            self.pending = future
        else:
            raise AssertionError("deadline-exhausted leg must not be dispatched")
        return future


def test_eager_future_timeout_preserves_completed_and_never_dispatched_legs(
    tmp_path: Path,
) -> None:
    ib = _EagerFutureIB()
    leases = _ConcurrencyLeaseFactory(limit=2)
    gateway = IBKRReadOnlyGateway(
        OptionsCopilotConfig(
            data_dir=tmp_path / "data",
            log_dir=tmp_path / "logs",
        ),
        historical_request_lease_factory=leases,
        historical_request_max_concurrency=2,
        monotonic=time.monotonic,
    )
    gateway._require_ib = lambda: ib  # type: ignore[method-assign]
    specs = tuple(
        (
            contract_id,
            SimpleNamespace(conId=contract_id),
            Decimal("1.0"),
            Decimal("1.2"),
        )
        for contract_id in (1, 2, 3)
    )

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        resolutions, requested = gateway._matching_historical_bbo_times_batch(
            specs,
            observed_at=NOW,
            deadline=time.monotonic() + 0.05,
        )

        assert requested is True
        assert ib.requests == [1, 2]
        assert leases.peak == 2
        assert leases.active == 0
        assert resolutions[1] == (
            NOW - timedelta(seconds=1),
            Decimal("1.0"),
            Decimal("1.2"),
            (),
        )
        assert resolutions[2] == (
            None,
            None,
            None,
            ("HISTORICAL_BBO_TIMEOUT",),
        )
        assert resolutions[3] == (
            None,
            None,
            None,
            ("HISTORICAL_BBO_DEADLINE_EXCEEDED",),
        )
        assert ib.pending is not None and ib.pending.cancelled()
    finally:
        asyncio.set_event_loop(None)
        loop.close()


def test_eager_cancel_exception_and_empty_rows_remain_per_leg_failures(
    tmp_path: Path,
) -> None:
    class FailedEagerIB:
        def __init__(self) -> None:
            self.requests: list[int] = []

        def reqHistoricalTicksAsync(self, contract, *_args, **_kwargs):
            contract_id = int(contract.conId)
            self.requests.append(contract_id)
            future = asyncio.get_event_loop().create_future()
            if contract_id == 1:
                future.cancel()
            elif contract_id == 2:
                future.set_exception(RuntimeError("redacted broker failure"))
            else:
                future.set_result(())
            return future

    ib = FailedEagerIB()
    leases = _ConcurrencyLeaseFactory(limit=3)
    gateway = IBKRReadOnlyGateway(
        OptionsCopilotConfig(
            data_dir=tmp_path / "data",
            log_dir=tmp_path / "logs",
        ),
        historical_request_lease_factory=leases,
        historical_request_max_concurrency=3,
        monotonic=time.monotonic,
    )
    gateway._require_ib = lambda: ib  # type: ignore[method-assign]
    specs = tuple(
        (
            contract_id,
            SimpleNamespace(conId=contract_id),
            Decimal("1.0"),
            Decimal("1.2"),
        )
        for contract_id in (1, 2, 3)
    )
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        resolutions, requested = gateway._matching_historical_bbo_times_batch(
            specs,
            observed_at=NOW,
            deadline=time.monotonic() + 0.5,
        )

        assert requested is True
        assert ib.requests == [1, 2, 3]
        assert leases.peak == 3
        assert leases.active == 0
        assert resolutions[1][3] == ("HISTORICAL_BBO_READ_FAILED",)
        assert resolutions[2][3] == ("HISTORICAL_BBO_READ_FAILED",)
        assert resolutions[3][3] == (
            "HISTORICAL_BBO_NO_FRESH_EXECUTABLE_QUOTE",
        )
    finally:
        asyncio.set_event_loop(None)
        loop.close()
