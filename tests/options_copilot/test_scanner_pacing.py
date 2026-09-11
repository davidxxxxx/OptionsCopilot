from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from options_copilot.operations.capabilities import (
    MarketDataPacingCapability,
    PACING_REQUEST_CLASSES,
)
from options_copilot.scanner.pacing import (
    PACING_CAPABILITY_MISSING,
    RequestBudgetByClass,
    SubscriptionLease,
)


NOW = datetime(2026, 8, 4, 14, 0, tzinfo=timezone.utc)


def _capability(
    *,
    limit: int = 2,
    request_window: float = 60,
    cooldown: float = 1,
) -> MarketDataPacingCapability:
    return MarketDataPacingCapability.create(
        version="market-data-pacing.v1",
        observed_at=NOW - timedelta(minutes=1),
        source="broker_disclosed",
        request_classes={
            name: {
                "max_concurrency": 1,
                "request_window": request_window,
                "max_requests": limit,
                "cooldown": cooldown,
            }
            for name in PACING_REQUEST_CLASSES
        },
        signer="human:xujie",
    )


class _Clock:
    def __init__(self, now: datetime) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)


@pytest.mark.parametrize("request_class", PACING_REQUEST_CLASSES)
def test_each_request_class_has_an_independent_zero_limit_limit_plus_one_boundary(
    request_class: str,
) -> None:
    budget = RequestBudgetByClass(_capability(limit=2), now=NOW)

    zero = budget.usage()[request_class]
    at_one = budget.consume(request_class)
    at_limit = budget.consume(request_class)
    over_limit = budget.consume(request_class)

    assert zero == {"used": 0, "limit": 2}
    assert at_one.allowed is True and at_one.used == 1
    assert at_limit.allowed is True and at_limit.used == 2
    assert over_limit.allowed is False
    assert over_limit.reason == "PACING_COOLDOWN_ACTIVE"
    assert over_limit.used == 2
    for other in set(PACING_REQUEST_CLASSES) - {request_class}:
        assert budget.usage()[other] == {"used": 0, "limit": 2}


def test_missing_capability_has_zero_budget_and_fixed_no_request_reason() -> None:
    budget = RequestBudgetByClass(None, now=NOW)

    assert budget.ready is False
    for request_class in PACING_REQUEST_CLASSES:
        decision = budget.consume(request_class)
        assert decision.allowed is False
        assert decision.reason == PACING_CAPABILITY_MISSING
        assert decision.used == 0
        assert decision.limit == 0


def test_signed_window_cooldown_and_concurrency_are_enforced() -> None:
    clock = _Clock(NOW)
    budget = RequestBudgetByClass(_capability(limit=2), now=NOW, clock=clock)

    first = budget.lease("scanner")
    with first as first_decision:
        assert first_decision.allowed is True
        with budget.lease("scanner") as concurrent:
            assert concurrent.allowed is False
            assert concurrent.reason == "PACING_CONCURRENCY_LIMIT"

    with budget.lease("scanner") as second:
        assert second.allowed is True

    with budget.lease("scanner") as cooling_down:
        assert cooling_down.allowed is False
        assert cooling_down.reason == "PACING_COOLDOWN_ACTIVE"

    clock.advance(1)
    with budget.lease("scanner") as window_exhausted:
        assert window_exhausted.allowed is False
        assert window_exhausted.reason == "PACING_REQUEST_WINDOW_EXHAUSTED"

    clock.advance(59)
    with budget.lease("scanner") as next_window:
        assert next_window.allowed is True


def test_cooldown_remains_active_when_request_window_empties_first() -> None:
    clock = _Clock(NOW)
    budget = RequestBudgetByClass(
        _capability(limit=1, request_window=1, cooldown=5),
        now=NOW,
        clock=clock,
    )

    with budget.lease("scanner") as first:
        assert first.allowed is True

    clock.advance(1)
    with budget.lease("scanner") as cooling_down:
        assert cooling_down.allowed is False
        assert cooling_down.reason == "PACING_COOLDOWN_ACTIVE"

    clock.advance(4)
    with budget.lease("scanner") as after_cooldown:
        assert after_cooldown.allowed is True


def test_cache_provenance_is_fresh_until_ttl_but_not_at_ttl_plus_one_ms() -> None:
    budget = RequestBudgetByClass(_capability(), now=NOW)
    record = budget.cache_record(
        request_key="SPY:20260918:C:500",
        request_class="snapshot_quote",
        source="IBKR_READONLY",
        payload={"bid": "1.00", "ask": "1.05"},
        observed_at=NOW,
        ttl=timedelta(seconds=5),
        entitlement_hash="e" * 64,
    )

    assert record.fresh(
        now=NOW + timedelta(seconds=4, milliseconds=999),
        entitlement_hash="e" * 64,
        capability_hash=budget.capability_hash or "",
    )
    assert not record.fresh(
        now=NOW + timedelta(seconds=5, milliseconds=1),
        entitlement_hash="e" * 64,
        capability_hash=budget.capability_hash or "",
    )
    assert not record.fresh(
        now=NOW + timedelta(seconds=1),
        entitlement_hash="changed",
        capability_hash=budget.capability_hash or "",
    )


@pytest.mark.parametrize("exit_mode", ["success", "exception"])
def test_temporary_subscription_is_cancelled_exactly_once(exit_mode: str) -> None:
    cancelled: list[str] = []
    lease = SubscriptionLease("temporary-1", cancelled.append)

    if exit_mode == "success":
        with lease:
            pass
    else:
        with pytest.raises(RuntimeError):
            with lease:
                raise RuntimeError("fixture parse failure")
    lease.close()

    assert cancelled == ["temporary-1"]


def test_preexisting_subscription_baseline_is_never_cancelled() -> None:
    cancelled: list[str] = []

    with SubscriptionLease("baseline-1", cancelled.append, preexisting=True):
        pass

    assert cancelled == []
