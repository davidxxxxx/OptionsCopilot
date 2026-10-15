"""Fake-SDK verification of at-most-once native history wire requests."""

from __future__ import annotations

from contextlib import contextmanager, nullcontext
from datetime import timedelta
from pathlib import Path
import threading
import time
from types import SimpleNamespace

import pytest

from options_copilot.config import OptionsCopilotConfig
from options_copilot.gateway.ibkr_readonly import IBKRReadOnlyGateway
from options_copilot.history_source_contracts import NativeHistoryContractError, build_native_history_request, validate_native_history_result
from test_ibkr_readonly_gateway import FakeIB
from test_native_history_contracts import IDENTITY, NOW, cutoff_document


class NativeIB(FakeIB):
    def __init__(self):
        super().__init__()
        self.native_wire = []
        self.cancelled_history = []
        self.native_mode = "success"
        self.trace = []
        self.native_clock = NOW
        self.extra_bars = []
        self.wrapper.historicalData = lambda *_args: None
        self.wrapper.historicalDataEnd = lambda *_args: None
        self.client.reqHistoricalData = self.raw_history
        self.client.cancelHistoricalData = lambda request_id: self.cancelled_history.append(request_id)

    def qualifyContracts(self, *_contracts):
        raise AssertionError("native preparation/execution must never qualify")

    def reqContractDetails(self, *_contracts):
        raise AssertionError("native preparation/execution must never resolve secdef or calendar")

    def reqHistoricalData(self, *_args, **_kwargs):
        raise AssertionError("SDK convenience results cannot prove response-end completion")

    def raw_history(self, request_id, contract, ending, duration, size, what, rth, format_date, keep, options):
        self.trace.append("wire")
        self.native_wire.append({
            "request_id": request_id, "contract": contract, "ending": ending, "duration": duration,
            "size": size, "what": what, "rth": rth, "format_date": format_date, "keep": keep, "options": options,
        })
        if self.native_mode == "wire_failure":
            raise RuntimeError("private transport credential-like details")
        if self.native_mode == "error":
            for handler in tuple(self.errorEvent.handlers):
                handler(request_id, 162, "private broker message")
            return
        if self.native_mode == "timeout":
            return
        if self.native_mode == "wrong_end":
            self.wrapper.historicalDataEnd(request_id + 99, "20260908", "20260909")
            return
        if self.native_mode != "empty":
            self.wrapper.historicalData(request_id, SimpleNamespace(
                date="20260909", open=100, high=101, low=99, close=0.2 if what == "OPTION_IMPLIED_VOLATILITY" else 100, volume=10,
            ))
            for bar in self.extra_bars:
                self.wrapper.historicalData(request_id, bar)
        if self.native_mode == "partial_timeout":
            return
        if self.native_mode == "unrelated_error":
            for handler in tuple(self.errorEvent.handlers):
                handler(request_id + 99, 162, "unrelated historical request")
        if self.native_mode in {"1100", "1102"}:
            for handler in tuple(self.errorEvent.handlers):
                handler(-1, int(self.native_mode), "upstream state changed")
        self.wrapper.historicalDataEnd(request_id, "20260908", "20260909")


@contextmanager
def gateway_fixture(tmp_path: Path, *, seeded=True, pacing_allowed=True):
    fake = NativeIB()

    @contextmanager
    def lease():
        fake.trace.append("pacing")
        try:
            yield SimpleNamespace(allowed=pacing_allowed)
        finally:
            fake.trace.append("released")

    gateway = IBKRReadOnlyGateway(
        OptionsCopilotConfig(data_dir=tmp_path / "data", log_dir=tmp_path / "logs"),
        ib_factory=lambda: fake, now=lambda: fake.native_clock,
        historical_request_lease_factory=lease,
        market_data_request_lease_factory=lambda _kind: nullcontext(SimpleNamespace(allowed=True)),
    )
    gateway.connect()
    try:
        gateway.account_snapshot()
        if seeded:
            gateway._call_on_owner(lambda: gateway._remember_underlying_identity("SPY", SimpleNamespace(
                conId=IDENTITY["con_id"], symbol="SPY", secType="STK", currency="USD", exchange="SMART", primaryExchange="ARCA",
            )))
        yield fake, gateway
    finally:
        gateway.disconnect()


def prepare(gateway, *, kind="PRICE_HISTORY", incremental=False):
    return gateway.prepare_native_history("SPY", kind=kind, cutoff=cutoff_document(), incremental=incremental)


def read(gateway, prepared, *, before_send=lambda: "claim.1", guard=lambda: True, remaining=lambda: 3.0):
    return gateway.read_native_history(prepared, before_send=before_send, operation_guard=guard, remaining_seconds=remaining)


def test_prepare_uses_only_current_cache_and_missing_identity_never_qualifies(tmp_path):
    with gateway_fixture(tmp_path, seeded=False) as (fake, gateway):
        with pytest.raises(NativeHistoryContractError, match="CACHED_IDENTITY_UNAVAILABLE"):
            prepare(gateway)
        assert fake.native_wire == fake.trace == []


@pytest.mark.parametrize("kind,incremental,ending,duration", [
    ("PRICE_HISTORY", False, "", "1 Y"), ("PRICE_HISTORY", True, "", "7 D"),
    ("IV_HISTORY", False, "20260909 20:40:00 UTC", "2 Y"), ("IV_HISTORY", True, "20260909 20:40:00 UTC", "7 D"),
])
def test_one_claimed_raw_request_preserves_exact_wire_and_owner_identity(tmp_path, kind, incremental, ending, duration):
    with gateway_fixture(tmp_path) as (fake, gateway):
        prepared = prepare(gateway, kind=kind, incremental=incremental)
        assert fake.trace == fake.native_wire == []
        claims = []
        guards = []

        def claim():
            claims.append(threading.get_ident())
            fake.trace.append("claim")
            return "claim.native"

        def guard():
            guards.append(threading.get_ident())
            return True

        original = fake.wrapper.historicalDataEnd
        result = read(gateway, prepared, before_send=claim, guard=guard)
        assert validate_native_history_result(result, prepared_request=prepared) == result
        assert result["status"] == "DELIVERED"
        assert result["claim_id"] == "claim.native"
        assert result["response"]["response_end_received"] is True
        assert result["response"]["epoch_valid"] is True
        assert result["response"]["bars"][0]["date_eligible"] is True
        assert result["production_eligible"] is False
        assert len(fake.native_wire) == len(claims) == 1
        assert len(set((*claims, *guards))) == 1 and claims[0] != threading.get_ident()
        assert fake.native_wire[0]["contract"].conId == IDENTITY["con_id"]
        assert fake.native_wire[0]["ending"] == ending
        assert fake.native_wire[0]["duration"] == duration
        assert fake.native_wire[0]["format_date"] == 1
        assert fake.native_wire[0]["keep"] is False
        assert fake.trace == ["pacing", "claim", "wire", "released"]
        assert fake.wrapper.historicalDataEnd is original
        assert len(fake.errorEvent.handlers) == 1


def test_pacing_denial_claims_and_sends_nothing(tmp_path):
    with gateway_fixture(tmp_path, pacing_allowed=False) as (fake, gateway):
        def forbidden():
            raise AssertionError("claim before pacing")
        result = read(gateway, prepare(gateway), before_send=forbidden)
        assert result["status"] == "NOT_SENT"
        assert result["claim_id"] is None
        assert result["reason_codes"] == ["NATIVE_HISTORY_PACING_DENIED"]
        assert fake.native_wire == []


def test_guard_before_pacing_claim_and_wire_is_enforced(tmp_path):
    with gateway_fixture(tmp_path) as (fake, gateway):
        result = read(gateway, prepare(gateway), guard=lambda: False)
        assert result["status"] == "NOT_SENT"
        assert fake.trace == []


@pytest.mark.parametrize("after_claim", ["guard", "deadline", "identity", "epoch"])
def test_claim_is_consumed_once_but_changed_boundary_still_prevents_wire(tmp_path, after_claim):
    with gateway_fixture(tmp_path) as (fake, gateway):
        prepared = prepare(gateway)
        state = {"guard": True, "remaining": 3.0, "calls": 0}

        def claim():
            state["calls"] += 1
            if after_claim == "guard":
                state["guard"] = False
            elif after_claim == "deadline":
                state["remaining"] = 0.0
            elif after_claim == "identity":
                gateway._underlying_identity_cache["SPY"].conId += 1
            else:
                for handler in tuple(fake.errorEvent.handlers):
                    handler(-1, 1100, "lost before wire")
            return "claim.recorded"

        result = read(gateway, prepared, before_send=claim, guard=lambda: state["guard"], remaining=lambda: state["remaining"])
        assert state["calls"] == 1
        assert result["status"] == "NOT_SENT"
        assert result["claim_id"] == "claim.recorded"
        assert result["response"]["send_state"] == "INTENT_RECORDED"
        assert fake.native_wire == []


def test_claim_storage_failure_sends_nothing_and_is_redacted(tmp_path):
    with gateway_fixture(tmp_path) as (fake, gateway):
        def claim():
            raise RuntimeError("private database content")
        result = read(gateway, prepare(gateway), before_send=claim)
        assert result["status"] == "NOT_SENT"
        assert result["reason_codes"] == ["NATIVE_HISTORY_SEND_CLAIM_FAILED"]
        assert "private" not in str(result)
        assert fake.native_wire == []


@pytest.mark.parametrize("mode", ["timeout", "wrong_end", "error", "empty", "wire_failure"])
def test_end_timeout_error_empty_and_uncertain_wire_are_distinct(tmp_path, mode):
    with gateway_fixture(tmp_path) as (fake, gateway):
        prepared = prepare(gateway)
        fake.native_mode = mode
        began = time.monotonic()
        result = read(gateway, prepared, remaining=lambda: 0.02)
        assert time.monotonic() - began < 0.5
        assert result["status"] == "UNAVAILABLE"
        assert result["claim_id"] == "claim.1"
        assert len(fake.native_wire) == 1
        assert result["response"]["response_end_received"] is (mode == "empty")
        assert result["response"]["timed_out"] is (mode in {"timeout", "wrong_end"})
        assert result["response"]["broker_error_codes"] == ([162] if mode == "error" else [])
        if mode != "empty":
            assert "NATIVE_HISTORY_END_UNVERIFIED" in result["reason_codes"]
            assert len(fake.cancelled_history) == 1
        if mode == "wire_failure":
            assert result["response"]["send_state"] == "DISPATCH_UNCERTAIN"
        assert "private" not in str(result)


@pytest.mark.parametrize("mode", ["1100", "1102"])
def test_upstream_epoch_change_during_response_cannot_deliver_complete(tmp_path, mode):
    with gateway_fixture(tmp_path) as (fake, gateway):
        prepared = prepare(gateway)
        fake.native_mode = mode
        result = read(gateway, prepared)
        assert result["status"] == "PARTIAL"
        assert result["response"]["epoch_valid"] is False
        assert result["response"]["end_generation"] > result["response"]["start_generation"]
        assert "NATIVE_HISTORY_EPOCH_INVALIDATED" in result["reason_codes"]
        assert len(fake.native_wire) == 1


def test_cache_day_rollover_rejects_frozen_old_identity_without_qualification(tmp_path):
    with gateway_fixture(tmp_path) as (fake, gateway):
        prepared = prepare(gateway)
        fake.native_clock += timedelta(days=1)
        result = read(gateway, prepared)
        assert result["status"] == "NOT_SENT"
        assert "NATIVE_HISTORY_CACHED_IDENTITY_CHANGED" in result["reason_codes"]
        assert fake.native_wire == []


def test_future_invalid_and_duplicate_native_rows_remain_partial(tmp_path):
    with gateway_fixture(tmp_path) as (fake, gateway):
        prepared = prepare(gateway)
        fake.extra_bars = [
            SimpleNamespace(date="20260910", close=100), SimpleNamespace(date="bad-date", close=float("nan")),
            SimpleNamespace(date="20260909", close=100),
        ]
        result = read(gateway, prepared)
        assert result["status"] == "PARTIAL"
        assert {"NATIVE_HISTORY_DATE_EXCLUDED", "NATIVE_HISTORY_INVALID_BARS", "NATIVE_HISTORY_DUPLICATE_DATES"}.issubset(result["reason_codes"])
        assert result["response"]["received_bar_count"] == 4


def test_late_end_for_old_request_cannot_complete_later_request(tmp_path):
    with gateway_fixture(tmp_path) as (fake, gateway):
        prepared = prepare(gateway)
        fake.native_mode = "timeout"
        first = read(gateway, prepared, remaining=lambda: 0.01)
        old_id = first["response"]["broker_request_id"]
        original_wire = fake.client.reqHistoricalData

        def second_wire(*args):
            fake.wrapper.historicalDataEnd(old_id, "old", "old")
            original_wire(*args)

        fake.client.reqHistoricalData = second_wire
        second = read(gateway, prepared, before_send=lambda: "claim.new.child", remaining=lambda: 0.01)
        assert second["response"]["broker_request_id"] != old_id
        assert second["response"]["response_end_received"] is False
        assert second["response"]["timed_out"] is True


def test_partial_bars_never_replace_the_missing_end_witness(tmp_path):
    with gateway_fixture(tmp_path) as (fake, gateway):
        fake.native_mode = "partial_timeout"
        result = read(gateway, prepare(gateway), remaining=lambda: 0.01)
        assert result["status"] == "PARTIAL"
        assert len(result["response"]["bars"]) == 1
        assert result["response"]["response_end_received"] is False
        assert result["response"]["timed_out"] is True


def test_foreign_request_errors_do_not_pollute_native_result(tmp_path):
    with gateway_fixture(tmp_path) as (fake, gateway):
        fake.native_mode = "unrelated_error"
        result = read(gateway, prepare(gateway))
        assert result["status"] == "DELIVERED"
        assert result["response"]["broker_error_codes"] == []


def test_per_request_three_second_cap_is_checked_after_raw_dispatch(tmp_path):
    with gateway_fixture(tmp_path) as (fake, gateway):
        prepared = prepare(gateway)
        ticker = {"now": 0.0}
        gateway._monotonic = lambda: ticker["now"]
        original_wire = fake.client.reqHistoricalData

        def slow_wire(*args):
            original_wire(*args)
            ticker["now"] = 3.01

        fake.client.reqHistoricalData = slow_wire
        result = read(gateway, prepared, remaining=lambda: 30.0)
        assert result["status"] == "PARTIAL"
        assert result["response"]["response_end_received"] is True
        assert result["response"]["timed_out"] is True
        assert "NATIVE_HISTORY_TIMEOUT" in result["reason_codes"]


def test_missing_raw_end_protocol_never_consumes_send_permit(tmp_path):
    with gateway_fixture(tmp_path) as (fake, gateway):
        prepared = prepare(gateway)
        del fake.wrapper.historicalDataEnd
        claims = []
        result = read(gateway, prepared, before_send=lambda: claims.append("claim") or "claim.1")
        assert result["status"] == "NOT_SENT"
        assert claims == fake.native_wire == []
        assert len(fake.errorEvent.handlers) == 1


def test_future_preparation_is_rejected_before_pacing_and_claim(tmp_path):
    with gateway_fixture(tmp_path) as (fake, gateway):
        future = NOW + timedelta(seconds=30)
        request = build_native_history_request(contract=IDENTITY, kind="PRICE_HISTORY", cutoff=cutoff_document(now=future),
                                               incremental=False, prepared_at=future)
        with pytest.raises(NativeHistoryContractError, match="PREPARATION_NOT_YET_AVAILABLE"):
            read(gateway, request)
        assert fake.trace == fake.native_wire == []
