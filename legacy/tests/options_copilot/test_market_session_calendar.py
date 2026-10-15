from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from types import MappingProxyType, SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from options_copilot.gateway import MarketDataPacingError
from options_copilot.gateway.ibkr_readonly import (
    BrokerConnectionError,
    SessionCalendarReadError,
)
from options_copilot.market import (
    CalendarStatus,
    IBKRSessionCalendarProvider,
    UsOptionsSessionCalendar,
)
from options_copilot.market.session_calendar import BrokerTradingSessionGate
from options_copilot.storage.canonical import canonical_hash


SOURCE = "IBKR_REQ_CONTRACT_DETAILS_READONLY"


def _normalize(
    *,
    now: datetime,
    hours: str,
    trading_hours: str | None = None,
    observed_at: datetime | None = None,
    timezone_id: str = "America/New_York",
):
    return UsOptionsSessionCalendar(
        maximum_age_seconds=Decimal("5")
    ).normalize(
        liquid_hours=hours,
        trading_hours=hours if trading_hours is None else trading_hours,
        timezone_id=timezone_id,
        observed_at=now if observed_at is None else observed_at,
        source=SOURCE,
        now=now,
    )


def test_broker_hours_normalize_across_us_dst_with_utc_evidence() -> None:
    now = datetime(2026, 3, 9, 14, 0, tzinfo=timezone.utc)
    snapshot = _normalize(
        now=now,
        hours=(
            "20260306:0930-20260306:1600;"
            "20260307:CLOSED;20260308:CLOSED;"
            "20260309:0930-20260309:1600"
        ),
    )

    friday = snapshot.session_for(date(2026, 3, 6))
    monday = snapshot.session_for(date(2026, 3, 9))
    assert snapshot.status is CalendarStatus.READY
    assert snapshot.entry_eligible is True
    assert friday is not None and monday is not None
    assert friday.open_utc == datetime(2026, 3, 6, 14, 30, tzinfo=timezone.utc)
    assert monday.open_utc == datetime(2026, 3, 9, 13, 30, tzinfo=timezone.utc)
    assert friday.close_utc == datetime(2026, 3, 6, 21, 0, tzinfo=timezone.utc)
    assert monday.close_utc == datetime(2026, 3, 9, 20, 0, tzinfo=timezone.utc)
    assert snapshot.verify_hash() is True


def test_extended_trading_hours_may_cover_regular_liquid_session() -> None:
    now = datetime(2026, 8, 3, 15, 0, tzinfo=timezone.utc)
    snapshot = _normalize(
        now=now,
        hours=(
            "20260803:0930-20260803:1600;"
            "20260804:0930-20260804:1600"
        ),
        trading_hours=(
            "20260803:0400-20260803:2000;"
            "20260804:0400-20260804:2000"
        ),
    )

    assert snapshot.status is CalendarStatus.READY
    assert snapshot.entry_eligible is True
    session = snapshot.session_for(date(2026, 8, 3))
    assert session is not None
    assert session.open_et.hour == 9 and session.open_et.minute == 30
    assert session.close_et.hour == 16


def test_weekend_holiday_and_early_close_are_authoritative() -> None:
    now = datetime(2026, 11, 27, 16, 0, tzinfo=timezone.utc)
    snapshot = _normalize(
        now=now,
        hours=(
            "20261126:CLOSED;20261127:0930-20261127:1300;"
            "20261128:CLOSED;20261129:CLOSED"
        ),
    )

    session = snapshot.session_for(date(2026, 11, 27))
    assert snapshot.status is CalendarStatus.READY
    assert date(2026, 11, 26) in snapshot.closed_dates
    assert date(2026, 11, 28) in snapshot.closed_dates
    assert session is not None and session.early_close is True
    assert session.close_et.hour == 13
    assert snapshot.session_at(
        datetime(2026, 11, 27, 17, 59, tzinfo=timezone.utc)
    ) == session
    assert snapshot.session_at(
        datetime(2026, 11, 27, 18, 0, tzinfo=timezone.utc)
    ) is None
    assert snapshot.session_at(
        datetime(2026, 11, 26, 16, 0, tzinfo=timezone.utc)
    ) is None


def test_shanghai_saturday_maps_to_friday_us_trading_date() -> None:
    instant_shanghai = datetime(
        2026,
        8,
        8,
        2,
        0,
        tzinfo=ZoneInfo("Asia/Shanghai"),
    )
    now = instant_shanghai.astimezone(timezone.utc)
    snapshot = _normalize(
        now=now,
        hours="20260807:0930-20260807:1600;20260808:CLOSED;20260809:CLOSED",
    )

    session = snapshot.session_at(instant_shanghai)
    assert snapshot.status is CalendarStatus.READY
    assert session is not None
    assert session.trading_date == date(2026, 8, 7)
    assert session.contains(instant_shanghai) is True


@pytest.mark.parametrize(
    ("changes", "reason"),
    [
        ({"hours": ""}, "CALENDAR_HOURS_MISSING"),
        ({"timezone_id": "UTC"}, "CALENDAR_TIMEZONE_UNSUPPORTED"),
        ({"observed_offset": Decimal("5.001")}, "CALENDAR_STALE"),
        (
            {"trading_hours": "20260803:CLOSED"},
            "LIQUID_TRADING_HOURS_MISMATCH",
        ),
        (
            {"hours": "20260804:0930-20260804:1600"},
            "CALENDAR_CURRENT_DATE_UNCOVERED",
        ),
    ],
)
def test_missing_stale_or_inconsistent_broker_calendar_is_degraded(
    changes: dict[str, object],
    reason: str,
) -> None:
    now = datetime(2026, 8, 3, 15, 0, tzinfo=timezone.utc)
    hours = str(changes.get("hours", "20260803:0930-20260803:1600"))
    trading_hours = str(changes.get("trading_hours", hours))
    observed_offset = changes.get("observed_offset", Decimal("0"))
    assert isinstance(observed_offset, Decimal)
    observed_at = now - timedelta(seconds=float(observed_offset))
    snapshot = _normalize(
        now=now,
        hours=hours,
        trading_hours=trading_hours,
        observed_at=observed_at,
        timezone_id=str(changes.get("timezone_id", "America/New_York")),
    )

    assert snapshot.status is CalendarStatus.DEGRADED
    assert snapshot.entry_eligible is False
    assert reason in snapshot.reason_codes
    assert snapshot.session_at(now) is None
    assert snapshot.verify_hash() is True


def test_raw_broker_metadata_and_normalized_calendar_have_distinct_hashes() -> None:
    now = datetime(2026, 8, 3, 15, 0, tzinfo=timezone.utc)
    ordinary = _normalize(
        now=now,
        hours="20260803:0930-20260803:1600",
    )
    early = _normalize(
        now=now,
        hours="20260803:0930-20260803:1300",
    )

    assert ordinary.source_hash != early.source_hash
    assert ordinary.calendar_hash != early.calendar_hash
    assert ordinary.observed_at == now
    assert ordinary.source == SOURCE


def test_ibkr_calendar_provider_normalizes_only_broker_published_hours() -> None:
    now = datetime(2026, 8, 3, 15, 0, tzinfo=timezone.utc)

    class _Source:
        def options_session_hours(self, symbol="SPY"):
            return SimpleNamespace(
                observed_at=now,
                source=SOURCE,
                liquid_hours="20260803:0930-20260803:1600",
                trading_hours="20260803:0930-20260803:1600",
                timezone_id="US/Eastern",
            )

    snapshot = IBKRSessionCalendarProvider(_Source()).snapshot(now=now)

    assert snapshot.status is CalendarStatus.READY
    assert snapshot.source == SOURCE
    assert snapshot.session_at(now) is not None


def test_ibkr_calendar_provider_accepts_bounded_request_completion_delay() -> None:
    now = datetime(2026, 8, 3, 15, 0, tzinfo=timezone.utc)

    class _Source:
        def options_session_hours(self, symbol="SPY"):
            return SimpleNamespace(
                observed_at=now + timedelta(seconds=1),
                source=SOURCE,
                liquid_hours="20260803:0930-20260803:1600",
                trading_hours="20260803:0400-20260803:2000",
                timezone_id="US/Eastern",
            )

    snapshot = IBKRSessionCalendarProvider(_Source()).snapshot(now=now)

    assert snapshot.status is CalendarStatus.READY
    assert snapshot.normalized_at == now + timedelta(seconds=1)
    assert snapshot.observed_at == snapshot.normalized_at


def test_ibkr_calendar_provider_reuses_only_one_still_fresh_broker_read() -> None:
    start = datetime(2026, 8, 3, 15, 0, tzinfo=timezone.utc)

    class _Source:
        def __init__(self) -> None:
            self.calls = 0
            self.observed_at = start

        def options_session_hours(self, symbol="SPY"):
            self.calls += 1
            return SimpleNamespace(
                observed_at=self.observed_at,
                source=SOURCE,
                liquid_hours="20260803:0930-20260803:1600",
                trading_hours="20260803:0930-20260803:1600",
                timezone_id="US/Eastern",
            )

    source = _Source()
    provider = IBKRSessionCalendarProvider(source)
    first = provider.snapshot(now=start)
    shared = provider.snapshot(now=start + timedelta(seconds=4))
    source.observed_at = start + timedelta(seconds=6)
    refreshed = provider.snapshot(now=source.observed_at)

    assert shared is first
    assert refreshed is not first
    assert source.calls == 2


def test_ibkr_calendar_provider_failure_returns_degraded_calendar() -> None:
    now = datetime(2026, 8, 3, 15, 0, tzinfo=timezone.utc)

    class _Source:
        def options_session_hours(self, symbol="SPY"):
            raise RuntimeError("fixture unavailable")

    snapshot = IBKRSessionCalendarProvider(_Source()).snapshot(now=now)

    assert snapshot.status is CalendarStatus.DEGRADED
    assert snapshot.entry_eligible is False
    assert "CALENDAR_HOURS_MISSING" in snapshot.reason_codes
    assert "CALENDAR_SOURCE_READ_FAILED" in snapshot.reason_codes


@pytest.mark.parametrize(
    ("error", "reason"),
    (
        (TimeoutError("private broker detail"), "CALENDAR_BROKER_REQUEST_TIMEOUT"),
        (BrokerConnectionError("private broker detail"), "CALENDAR_BROKER_CONNECTION_UNAVAILABLE"),
        (
            SessionCalendarReadError("CALENDAR_CONTRACT_DETAILS_AMBIGUOUS"),
            "CALENDAR_CONTRACT_DETAILS_AMBIGUOUS",
        ),
    ),
)
def test_ibkr_calendar_provider_retains_safe_acquisition_failure(error, reason) -> None:
    now = datetime(2026, 8, 3, 15, 0, tzinfo=timezone.utc)

    class _Source:
        def options_session_hours(self, symbol="SPY"):
            raise error

    snapshot = IBKRSessionCalendarProvider(_Source()).snapshot(now=now)

    assert snapshot.status is CalendarStatus.DEGRADED
    assert reason in snapshot.reason_codes
    assert "private broker detail" not in str(snapshot.as_dict())
    assert snapshot.verify_hash() is True


def test_ibkr_calendar_provider_accepts_immutable_mapping_source() -> None:
    now = datetime(2026, 8, 3, 15, 0, tzinfo=timezone.utc)

    class _Source:
        def options_session_hours(self, symbol="SPY"):
            return MappingProxyType({
                "observed_at": now,
                "source": SOURCE,
                "liquid_hours": "20260803:0930-20260803:1600",
                "trading_hours": "20260803:0400-20260803:2000",
                "timezone_id": "US/Eastern",
            })

    snapshot = IBKRSessionCalendarProvider(_Source()).snapshot(now=now)

    assert snapshot.status is CalendarStatus.READY
    assert snapshot.verify_hash() is True


def test_ibkr_calendar_provider_preserves_wire_level_pacing_reason() -> None:
    now = datetime(2026, 8, 3, 15, 0, tzinfo=timezone.utc)

    class _Source:
        def options_session_hours(self, symbol="SPY"):
            raise MarketDataPacingError(
                "secdef",
                "PACING_REQUEST_WINDOW_EXHAUSTED",
            )

    snapshot = IBKRSessionCalendarProvider(_Source()).snapshot(now=now)

    assert snapshot.status is CalendarStatus.DEGRADED
    assert snapshot.entry_eligible is False
    assert "CALENDAR_PACING_DENIED" in snapshot.reason_codes
    assert (
        "CALENDAR_PACING_REQUEST_WINDOW_EXHAUSTED"
        in snapshot.reason_codes
    )
    assert snapshot.verify_hash() is True


def test_ibkr_calendar_provider_does_not_cache_pacing_denial() -> None:
    now = datetime(2026, 8, 3, 15, 0, tzinfo=timezone.utc)

    class _Source:
        def __init__(self) -> None:
            self.calls = 0

        def options_session_hours(self, symbol="SPY"):
            self.calls += 1
            if self.calls == 1:
                raise MarketDataPacingError(
                    "secdef",
                    "PACING_REQUEST_WINDOW_EXHAUSTED",
                )
            return SimpleNamespace(
                observed_at=now + timedelta(seconds=1),
                source=SOURCE,
                liquid_hours="20260803:0930-20260803:1600",
                trading_hours="20260803:0400-20260803:2000",
                timezone_id="US/Eastern",
            )

    source = _Source()
    provider = IBKRSessionCalendarProvider(source)

    denied = provider.snapshot(now=now)
    recovered = provider.snapshot(now=now + timedelta(seconds=1))

    assert denied.status is CalendarStatus.DEGRADED
    assert "CALENDAR_PACING_DENIED" in denied.reason_codes
    assert recovered.status is CalendarStatus.READY
    assert source.calls == 2


class _SnapshotProvider:
    def __init__(self, factory) -> None:
        self.factory = factory
        self.calls: list[datetime] = []

    def snapshot(self, *, now: datetime):
        self.calls.append(now)
        return self.factory(now)


def _slot_snapshot(
    now: datetime,
    *,
    hours: str = "20260803:0930-20260803:1600",
    observed_at: datetime | None = None,
):
    return _normalize(
        now=now,
        hours=hours,
        observed_at=now if observed_at is None else observed_at,
    )


def _rehash(snapshot, **changes):
    provisional = replace(snapshot, **changes, calendar_hash="0" * 64)
    return replace(
        provisional,
        calendar_hash=canonical_hash(provisional.hash_payload()),
    )


def test_broker_gate_0920_checks_trading_date_not_current_session() -> None:
    scheduled = datetime(2026, 8, 3, 9, 20, tzinfo=ZoneInfo("America/New_York"))
    observed = scheduled.astimezone(timezone.utc) + timedelta(seconds=1)
    provider = _SnapshotProvider(lambda now: _slot_snapshot(now))
    gate = BrokerTradingSessionGate(provider, clock=lambda: observed)

    assert gate.is_trading_session(scheduled_for=scheduled) is True
    assert gate.is_trading_session(scheduled_for=scheduled) is True
    assert provider.calls == [observed, observed]
    snapshot = _slot_snapshot(observed)
    assert snapshot.session_for(scheduled.date()) is not None
    assert snapshot.session_at(scheduled) is None


def test_broker_gate_0935_requires_the_options_session_to_be_open() -> None:
    scheduled = datetime(2026, 8, 3, 9, 35, tzinfo=ZoneInfo("America/New_York"))
    observed = scheduled.astimezone(timezone.utc) + timedelta(seconds=2)
    provider = _SnapshotProvider(lambda now: _slot_snapshot(now))
    gate = BrokerTradingSessionGate(provider, clock=lambda: observed)

    assert gate.is_trading_session(scheduled_for=scheduled) is True
    assert provider.calls == [observed]


def test_broker_gate_verifies_a_live_response_against_post_io_time() -> None:
    scheduled = datetime(2026, 8, 3, 9, 20, tzinfo=ZoneInfo("America/New_York"))
    requested_at = scheduled.astimezone(timezone.utc) + timedelta(seconds=1)
    completed_at = requested_at + timedelta(milliseconds=2)
    readings = iter((requested_at, completed_at))
    provider = _SnapshotProvider(
        lambda _now: _slot_snapshot(completed_at, observed_at=completed_at)
    )
    gate = BrokerTradingSessionGate(provider, clock=lambda: next(readings))

    assert gate.is_trading_session(scheduled_for=scheduled) is True
    assert provider.calls == [requested_at]


def test_broker_gate_returns_false_for_closed_or_uncovered_trading_date() -> None:
    scheduled = datetime(2026, 8, 3, 9, 20, tzinfo=ZoneInfo("America/New_York"))
    observed = scheduled.astimezone(timezone.utc)
    closed = _SnapshotProvider(
        lambda now: _slot_snapshot(now, hours="20260803:CLOSED")
    )
    assert (
        BrokerTradingSessionGate(closed, clock=lambda: observed).is_trading_session(
            scheduled_for=scheduled
        )
        is False
    )

    ready = _slot_snapshot(observed)
    uncovered = _rehash(ready, sessions=(), closed_dates=())
    provider = _SnapshotProvider(lambda _now: uncovered)
    assert (
        BrokerTradingSessionGate(provider, clock=lambda: observed).is_trading_session(
            scheduled_for=scheduled
        )
        is False
    )


@pytest.mark.parametrize(
    "age_seconds,expected",
    [(Decimal("5"), True), (Decimal("5.001"), None)],
)
def test_broker_gate_enforces_the_five_second_observation_boundary(
    age_seconds: Decimal,
    expected: bool | None,
) -> None:
    scheduled = datetime(2026, 8, 3, 9, 35, tzinfo=ZoneInfo("America/New_York"))
    observed = scheduled.astimezone(timezone.utc)
    provider = _SnapshotProvider(
        lambda now: _slot_snapshot(
            now,
            observed_at=now - timedelta(seconds=float(age_seconds)),
        )
    )

    assert (
        BrokerTradingSessionGate(provider, clock=lambda: observed).is_trading_session(
            scheduled_for=scheduled
        )
        is expected
    )


@pytest.mark.parametrize(
    "failure",
    ("degraded", "hash", "future", "normalized_future", "wrong_type", "raise"),
)
def test_broker_gate_returns_unknown_for_untrusted_calendar_evidence(
    failure: str,
) -> None:
    scheduled = datetime(2026, 8, 3, 9, 35, tzinfo=ZoneInfo("America/New_York"))
    observed = scheduled.astimezone(timezone.utc)
    ready = _slot_snapshot(observed)

    def factory(now: datetime):
        if failure == "degraded":
            return _slot_snapshot(
                now,
                observed_at=now - timedelta(seconds=5.001),
            )
        if failure == "hash":
            return replace(ready, calendar_hash="0" * 64)
        if failure == "future":
            return _rehash(ready, observed_at=now + timedelta(microseconds=1))
        if failure == "normalized_future":
            return _rehash(ready, normalized_at=now + timedelta(microseconds=1))
        if failure == "wrong_type":
            return SimpleNamespace(status=CalendarStatus.READY)
        raise RuntimeError("fixture provider failure")

    provider = _SnapshotProvider(factory)
    gate = BrokerTradingSessionGate(provider, clock=lambda: observed)

    assert gate.is_trading_session(scheduled_for=scheduled) is None


@pytest.mark.parametrize(
    "scheduled",
    [
        datetime(2026, 8, 3, 9, 21, tzinfo=ZoneInfo("America/New_York")),
        datetime(2026, 8, 3, 9, 20, 1, tzinfo=ZoneInfo("America/New_York")),
        datetime(2026, 8, 3, 9, 20),
    ],
)
def test_broker_gate_does_not_guess_for_nonexact_or_naive_slots(
    scheduled: datetime,
) -> None:
    observed = datetime(2026, 8, 3, 13, 20, tzinfo=timezone.utc)
    provider = _SnapshotProvider(lambda now: _slot_snapshot(now))
    gate = BrokerTradingSessionGate(provider, clock=lambda: observed)

    assert gate.is_trading_session(scheduled_for=scheduled) is None
    assert provider.calls == []
