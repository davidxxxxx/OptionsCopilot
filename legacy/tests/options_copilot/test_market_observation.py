"""Safety and lifecycle tests for bounded market-only observations."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
import inspect
import json

from options_copilot.gateway.market_observation import (
    MAX_PRICE_TICKS_PER_CONTRACT,
    MAX_OBSERVATION_BYTES,
    OBSERVATION_SCHEMA,
    RESEARCH_INPUT_SCHEMA,
    OptionObservationRequest,
    _Recorder,
    _finish_observation,
    build_research_market_input,
    collect_market_observation,
)
from options_copilot.storage.canonical import canonical_hash, canonical_json


NOW = datetime(2026, 10, 5, 15, 10, tzinfo=timezone.utc)


def _field(value: str, received_at: datetime, *, data_type: int = 1, valid: bool = True) -> dict[str, object]:
    return {
        "tick_type": 1, "tick_variant": "LIVE_FAMILY",
        "value": value, "valid": valid,
        "callback_received_at": received_at.isoformat(),
        "market_data_type": data_type, "source_event_time": None,
        "source_event_time_status": "UNAVAILABLE", "update_count": 1,
    }


def _computation(tick: int, received_at: datetime, *, data_type: int = 1, valid: bool = True) -> dict[str, object]:
    values = {
        "implied_volatility": "0.171", "delta": "0.54", "gamma": "0.017",
        "vega": "0.52", "theta": "-0.45", "option_price": "10.2",
        "underlying_price": "753.2",
    }
    return {
        "tick_type": tick, "tick_variant": "LIVE_FAMILY", "values": values,
        "validity": {name: valid for name in values}, "valid": valid,
        "callback_received_at": received_at.isoformat(),
        "market_data_type": data_type, "source_event_time": None,
        "source_event_time_status": "UNAVAILABLE", "update_count": 1,
    }


def _rows(received_at: datetime = NOW, *, data_type: int = 1, valid: bool = True) -> list[dict[str, object]]:
    rows = []
    for index, security_type in enumerate(("STK", "OPT", "OPT"), start=1):
        prices = {name: _field(value, received_at + timedelta(microseconds=index), data_type=data_type, valid=valid) for name, value in (("bid", "10"), ("ask", "11"), ("last", "10.5"))}
        for tick_type, name in ((1, "bid"), (2, "ask"), (4, "last")):
            prices[name]["tick_type"] = tick_type
        computations = {}
        if security_type == "OPT":
            computations = {name: _computation(tick, received_at + timedelta(microseconds=index + tick), data_type=data_type, valid=valid) for tick, name in ((10, "bid"), (11, "ask"), (12, "last"), (13, "model"))}
        row = {
            "contract": {
                "contract_id": index, "symbol": "QQQ", "security_type": security_type,
                "currency": "USD", "exchange": "SMART", "primary_exchange": None,
                "expiration": "20261016" if security_type == "OPT" else None,
                "strike": "752", "right": None if security_type == "STK" else ("C" if index == 2 else "P"),
                "trading_class": "QQQ", "multiplier": "100" if security_type == "OPT" else None,
            },
            "market_data_type": {"value": data_type, "callback_received_at": received_at.isoformat()},
            "prices": prices, "option_computations": computations,
            "price_tick_history": [dict(value) for value in prices.values()],
            "option_computation_tick_history": [dict(value) for value in computations.values()],
            "history_limits": {"price": 256, "option_computation": 512},
            "history_truncated": {"price": 0, "option_computation": 0},
            "asynchronous_callbacks": True, "atomic_source_snapshot": False,
        }
        rows.append(row)
    return rows


class FakeSession:
    def __init__(self, *, rows=None, failure: Exception | None = None) -> None:
        self.error_codes = []
        self.rows = _rows() if rows is None else rows
        self.failure = failure
        self.calls = []

    async def connect(self) -> None:
        self.calls.append("connect")
        if self.failure:
            raise self.failure

    async def qualify(self, _request):
        self.calls.append("qualify")
        return [object(), object(), object()]

    def subscribe(self, _contracts) -> None:
        self.calls.append("subscribe")

    async def wait(self, _seconds) -> None:
        self.calls.append("wait")

    def snapshot(self, _contracts):
        self.calls.append("snapshot")
        return self.rows

    def cancel_all(self) -> None:
        self.calls.append("cancel")

    def disconnect(self) -> None:
        self.calls.append("disconnect")

    def connection_document(self):
        return {"host": "127.0.0.1", "port": 4001, "client_id": 4818, "readonly": True, "market_data_only": True, "server_version": 176}


def _request() -> OptionObservationRequest:
    return OptionObservationRequest(symbol="qqq", expiration="20261016", strike=Decimal("752"), collection_seconds=1)


def _observation(rows, *, status="OBSERVED", requested_at=None, completed_at=None, rights=("C", "P")):
    body = {
        "schema": OBSERVATION_SCHEMA, "status": status,
        "scope": "BOUNDED_MARKET_DATA_ONLY", "source": "IBKR",
        "requested_at": (requested_at or NOW - timedelta(seconds=8)).isoformat(),
        "completed_at": (completed_at or NOW).isoformat(), "contracts": rows,
        "request": {
            "symbol": "QQQ", "expiration": "20261016", "strike": "752",
            "rights": list(rights), "collection_seconds": 8.0,
        },
        "decision_authority": "OBSERVATION_ONLY", "production_eligible": False,
        "approval_eligible": False, "instruction_creation_allowed": False,
        "source_event_time_available": False, "atomic_source_snapshot": False,
    }
    return {**body, "content_hash": canonical_hash(body)}


def test_success_is_observation_only_hashed_and_always_cleans_up() -> None:
    session = FakeSession()
    times = iter((NOW, NOW + timedelta(seconds=1)))
    result = collect_market_observation(_request(), session_factory=lambda: session, clock=lambda: next(times))

    assert result["schema"] == OBSERVATION_SCHEMA
    assert result["status"] == "OBSERVED"
    assert result["production_eligible"] is False
    assert result["instruction_creation_allowed"] is False
    assert result["source_event_time_available"] is False
    assert result["atomic_source_snapshot"] is False
    assert session.calls == ["connect", "qualify", "subscribe", "wait", "snapshot", "cancel", "disconnect"]
    body = dict(result)
    assert body.pop("content_hash") == canonical_hash(body)
    assert "account" not in repr(result).lower()
    json.dumps({"observation": result, "research_input": build_research_market_input(result, now=NOW + timedelta(seconds=1))}, allow_nan=False)


def test_timeout_and_disconnect_are_redacted_and_cleanup_still_runs() -> None:
    for failure, reason in ((TimeoutError("private timeout"), "IBKR_MARKET_OBSERVATION_TIMEOUT"), (ConnectionError("private endpoint"), "IBKR_MARKET_OBSERVATION_DISCONNECTED")):
        session = FakeSession(failure=failure)
        result = collect_market_observation(_request(), session_factory=lambda: session, clock=lambda: NOW)
        assert result["status"] == "UNAVAILABLE"
        assert result["reason_codes"] == [reason]
        assert session.calls[-2:] == ["cancel", "disconnect"]
        assert "private" not in repr(result)


def test_cleanup_failures_are_stable_codes_not_private_exception_text() -> None:
    class BrokenCleanup(FakeSession):
        def cancel_all(self) -> None:
            raise RuntimeError("private cancel failure")

        def disconnect(self) -> None:
            raise RuntimeError("private disconnect failure")

    result = collect_market_observation(_request(), session_factory=BrokenCleanup, clock=lambda: NOW)
    assert "IBKR_MARKET_OBSERVATION_CANCEL_FAILED" in result["reason_codes"]
    assert "IBKR_MARKET_OBSERVATION_DISCONNECT_FAILED" in result["reason_codes"]
    assert "private" not in repr(result)


def test_repeated_runs_create_and_cleanup_independent_sessions() -> None:
    sessions = [FakeSession(), FakeSession()]
    for session in sessions:
        assert collect_market_observation(_request(), session_factory=lambda session=session: session, clock=lambda: NOW)["status"] == "OBSERVED"
    assert all(session.calls.count("cancel") == session.calls.count("disconnect") == 1 for session in sessions)


def test_recorder_keeps_separate_tick_times_and_rejects_missing_sentinels() -> None:
    recorder = _Recorder(lambda: NOW)
    recorder.price(1, 1, 0, NOW, 1)
    recorder.price(1, 2, float("nan"), NOW + timedelta(microseconds=1), 1)
    recorder.computation(1, 10, {
        "implied_volatility": -1, "delta": -2, "gamma": -2, "vega": -2,
        "theta": -2, "option_price": -1, "underlying_price": -1,
    }, NOW + timedelta(microseconds=2), 1)
    assert recorder.prices[1]["bid"]["valid"] is True
    assert recorder.prices[1]["bid"]["value"] == "0"
    assert recorder.prices[1]["ask"]["value"] is None
    assert recorder.computations[1]["bid"]["valid"] is False
    assert recorder.prices[1]["bid"]["callback_received_at"] != recorder.computations[1]["bid"]["callback_received_at"]


def test_research_projection_exposes_stale_nonlive_missing_and_async_fields() -> None:
    observation = _observation(
        _rows(NOW - timedelta(seconds=30), data_type=3, valid=False),
        requested_at=NOW - timedelta(seconds=40),
    )
    result = build_research_market_input(observation, now=NOW)
    assert result["schema"] == RESEARCH_INPUT_SCHEMA
    assert result["status"] == "RESEARCH_DEGRADED"
    assert result["production_eligible"] is False
    assert "RESEARCH_MARKET_FIELDS_MISSING" in result["reason_codes"]
    # Invalid fields are missing/invalid rather than also being mislabeled as fresh evidence.
    assert all(row["missing_or_invalid_fields"] for row in result["contracts"])
    assert result["contracts"][1]["market_fields"]["option_computations"]["bid"]["valid"] is False
    assert all(row["source_event_time"] is None for row in result["contracts"])


def test_research_projection_marks_valid_but_stale_and_nonlive_separately() -> None:
    observation = _observation(
        _rows(NOW - timedelta(seconds=30), data_type=3),
        requested_at=NOW - timedelta(seconds=40),
    )
    result = build_research_market_input(observation, now=NOW)
    assert "RESEARCH_MARKET_FIELDS_STALE" in result["reason_codes"]
    assert "RESEARCH_MARKET_DATA_NOT_LIVE" in result["reason_codes"]
    assert "RESEARCH_MARKET_CALLBACKS_ASYNCHRONOUS" in result["reason_codes"]


def test_research_projection_carries_whitelisted_values_times_and_source_hash() -> None:
    observation = _observation(_rows(NOW - timedelta(seconds=1)))
    result = build_research_market_input(observation, now=NOW)
    option = result["contracts"][1]
    assert result["observation_hash"] == observation["content_hash"]
    assert option["market_fields"]["prices"]["bid"]["value"] == "10"
    assert option["market_fields"]["option_computations"]["model"]["values"]["implied_volatility"] == "0.171"
    assert option["market_fields"]["option_computations"]["model"]["callback_received_at"]
    assert set(option["market_fields"]["option_computations"]["model"]["values"]) == {
        "implied_volatility", "delta", "gamma", "vega", "theta", "option_price", "underlying_price",
    }


def test_research_projection_never_upgrades_unavailable_or_tampered_source() -> None:
    unavailable = _observation([], status="UNAVAILABLE")
    result = build_research_market_input(unavailable, now=NOW)
    assert result["status"] == "RESEARCH_DEGRADED"
    assert "SOURCE_OBSERVATION_UNAVAILABLE" in result["reason_codes"]

    tampered = dict(_observation(_rows()))
    tampered["status"] = "UNAVAILABLE"
    try:
        build_research_market_input(tampered, now=NOW)
    except ValueError as exc:
        assert str(exc) == "MARKET_OBSERVATION_HASH_MISMATCH"
    else:  # pragma: no cover
        raise AssertionError("tampered observation was accepted")

    future = _observation(_rows(), completed_at=NOW + timedelta(seconds=1))
    try:
        build_research_market_input(future, now=NOW)
    except ValueError as exc:
        assert str(exc) == "MARKET_OBSERVATION_TIME_INVALID"
    else:  # pragma: no cover
        raise AssertionError("future observation was accepted")


def test_invalid_computations_remain_partial_despite_being_received() -> None:
    rows = _rows(valid=False)
    session = FakeSession(rows=rows)
    result = collect_market_observation(_request(), session_factory=lambda: session, clock=lambda: NOW)
    assert result["status"] == "PARTIAL"
    assert "OPTION_COMPUTATION_INCOMPLETE" in result["reason_codes"]
    assert result["contracts"][1]["option_computations"]["bid"]["values"]


def test_delayed_tick_families_keep_original_type_and_history() -> None:
    recorder = _Recorder(lambda: NOW)
    recorder.price(1, 1, 10, NOW, 1)
    recorder.price(1, 66, 9, NOW + timedelta(microseconds=1), 3)
    values = {"implied_volatility": 0.2, "delta": -1, "gamma": 0.1, "vega": 0.2, "theta": -1, "option_price": 0, "underlying_price": 100}
    recorder.computation(1, 10, values, NOW, 1)
    recorder.computation(1, 80, values, NOW + timedelta(microseconds=2), 3)
    assert [row["tick_type"] for row in recorder.price_ticks[1]] == [1, 66]
    assert [row["market_data_type"] for row in recorder.price_ticks[1]] == [1, 3]
    assert [row["tick_type"] for row in recorder.computation_ticks[1]] == [10, 80]
    assert recorder.computations[1]["bid"]["tick_variant"] == "DELAYED"
    assert recorder.computations[1]["bid"]["valid"] is True


def test_cancel_all_attempts_every_subscription_and_reports_failure() -> None:
    from options_copilot.gateway.market_observation import IBMarketObservationSession

    class IB:
        def __init__(self):
            self.calls = []

        def cancelMktData(self, contract):
            self.calls.append(contract)
            if contract == "bad":
                raise RuntimeError("private")

    session = object.__new__(IBMarketObservationSession)
    session.ib = IB()
    session._requested = ["bad", "good"]
    try:
        session.cancel_all()
    except RuntimeError as exc:
        assert str(exc) == "IBKR_MARKET_DATA_CANCEL_FAILED"
    else:  # pragma: no cover
        raise AssertionError("cancellation failure was hidden")
    assert session.ib.calls == ["bad", "good"]
    assert session._requested == []


def test_contract_identity_requires_exact_requested_leg() -> None:
    from types import SimpleNamespace
    from options_copilot.gateway.market_observation import IBMarketObservationSession

    stock = SimpleNamespace(symbol="QQQ", secType="STK", currency="USD", exchange="SMART")
    wrong = SimpleNamespace(conId=1, symbol="SPY", secType="STK", currency="USD", exchange="SMART")
    assert IBMarketObservationSession._identity_matches(stock, wrong) is False
    requested = SimpleNamespace(symbol="QQQ", secType="OPT", currency="USD", exchange="SMART", lastTradeDateOrContractMonth="20261016", strike=752.0, right="C")
    returned = SimpleNamespace(conId=2, symbol="QQQ", secType="OPT", currency="USD", exchange="SMART", lastTradeDateOrContractMonth="20261016", strike=752.0, right="P", multiplier="100")
    assert IBMarketObservationSession._identity_matches(requested, returned) is False


def test_mixed_delayed_then_live_history_is_preserved_and_degraded() -> None:
    rows = _rows(NOW - timedelta(seconds=1))
    delayed = dict(rows[1]["prices"]["bid"])
    delayed.update({
        "tick_type": 66, "tick_variant": "DELAYED", "market_data_type": 3,
        "callback_received_at": (NOW - timedelta(seconds=2)).isoformat(),
    })
    rows[1]["price_tick_history"].insert(0, delayed)
    observation = _observation(rows)
    result = build_research_market_input(observation, now=NOW)
    call = result["contracts"][1]
    assert result["status"] == "RESEARCH_DEGRADED"
    assert "RESEARCH_MARKET_DATA_MIXED" in result["reason_codes"]
    assert call["mixed_market_data_types"] is True
    assert call["market_data_types_observed"] == [1, 3]
    assert call["market_fields"]["price_tick_history"][0]["tick_type"] == 66


def test_projection_requires_exact_requested_contract_topology_and_allows_exact_single_right() -> None:
    only_stock = _observation(_rows()[:1])
    try:
        build_research_market_input(only_stock, now=NOW + timedelta(seconds=1))
    except ValueError as exc:
        assert str(exc) == "MARKET_OBSERVATION_TOPOLOGY_INVALID"
    else:  # pragma: no cover
        raise AssertionError("missing option legs were accepted")

    single = _observation(_rows(NOW - timedelta(seconds=1))[:2], rights=("C",))
    result = build_research_market_input(single, now=NOW)
    assert result["status"] == "RESEARCH_READY"
    assert len(result["contracts"]) == 2


def test_history_is_bounded_retains_latest_and_reports_truncation() -> None:
    recorder = _Recorder(lambda: NOW)
    for index in range(MAX_PRICE_TICKS_PER_CONTRACT + 2):
        recorder.price(1, 1, index, NOW, 1)
    assert len(recorder.price_ticks[1]) == MAX_PRICE_TICKS_PER_CONTRACT
    assert recorder.price_ticks[1][0]["value"] == "2"
    assert recorder.truncated_counts[1]["price"] == 2

    rows = _rows(NOW - timedelta(seconds=1))
    rows[1]["history_truncated"]["price"] = 2
    observation = _observation(rows)
    result = build_research_market_input(observation, now=NOW)
    assert result["status"] == "RESEARCH_DEGRADED"
    assert "RESEARCH_MARKET_HISTORY_TRUNCATED" in result["reason_codes"]
    assert result["contracts"][1]["history_truncated"]["price"] == 2


def test_every_current_and_history_receipt_must_fit_observation_window() -> None:
    for mutate in ("current_after", "history_before", "market_type_after"):
        rows = _rows(NOW - timedelta(seconds=1))
        if mutate == "current_after":
            rows[1]["prices"]["bid"]["callback_received_at"] = (NOW + timedelta(microseconds=1)).isoformat()
        elif mutate == "history_before":
            rows[1]["option_computation_tick_history"][0]["callback_received_at"] = (NOW - timedelta(seconds=9)).isoformat()
        else:
            rows[1]["market_data_type"]["callback_received_at"] = (NOW + timedelta(microseconds=1)).isoformat()
        observation = _observation(rows)
        try:
            build_research_market_input(observation, now=NOW + timedelta(seconds=1))
        except ValueError as exc:
            assert str(exc) == "MARKET_OBSERVATION_FIELD_TIME_INVALID"
        else:  # pragma: no cover
            raise AssertionError(f"out-of-window receipt accepted: {mutate}")


def test_producer_fails_closed_when_rows_errors_or_history_exceed_output_limit() -> None:
    oversized = "x" * (MAX_OBSERVATION_BYTES + 10_000)
    cases = (
        ([{"oversized_row": oversized}], []),
        ([], [{"code": 354, "oversized_error": oversized}]),
        ([{"price_tick_history": [{"value": oversized}]}], []),
    )
    for rows, errors in cases:
        result = _finish_observation(
            _request(), NOW - timedelta(seconds=8), NOW,
            {
                "host": "127.0.0.1", "port": 4001, "client_id": 4818,
                "readonly": True, "market_data_only": True,
            },
            rows, errors, "OBSERVED", [],
        )
        assert result["status"] == "UNAVAILABLE"
        assert result["reason_codes"] == ["MARKET_OBSERVATION_SIZE_LIMIT_EXCEEDED"]
        assert result["contracts"] == []
        assert result["broker_error_codes"] == []
        assert result["discarded_counts"] == {
            "contracts": len(rows), "broker_errors": len(errors),
        }
        assert len(canonical_json(result).encode("utf-8")) <= MAX_OBSERVATION_BYTES


def test_module_has_no_account_order_or_execution_requests() -> None:
    import options_copilot.gateway.market_observation as module

    source = inspect.getsource(module)
    forbidden = ("reqAccount", "reqPositions", "reqOpenOrders", "reqExecutions", "placeOrder", "cancelOrder")
    assert all(name not in source for name in forbidden)
