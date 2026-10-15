"""Native request/result contracts never upgrade diagnostic or model schemas."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone

import pytest

from options_copilot.history_source_contracts import (
    NativeHistoryContractError,
    build_native_history_request,
    make_native_history_result,
    native_history_wire_parameters,
    validate_native_history_request,
    validate_native_history_result,
)
from options_copilot.market.session_calendar import UsOptionsSessionCalendar
from options_copilot.storage.canonical import canonical_hash


NOW = datetime(2026, 9, 9, 20, 40, tzinfo=timezone.utc)
IDENTITY = {"con_id": 756733, "symbol": "SPY", "sec_type": "STK", "currency": "USD", "exchange": "SMART", "primary_exchange": "ARCA"}


def cutoff_document(*, now=NOW, close="1600"):
    calendar = UsOptionsSessionCalendar().normalize(
        liquid_hours=f"20260909:0930-{close}", trading_hours=f"20260909:0930-{close}",
        timezone_id="America/New_York", observed_at=now,
        source="IBKR_REQ_CONTRACT_DETAILS_READONLY", now=now,
    )
    return {"calendar": calendar.as_dict(), "scheduled_for": now.isoformat()}


def prepared_request(*, kind="PRICE_HISTORY", incremental=False):
    return build_native_history_request(
        contract=IDENTITY, kind=kind, cutoff=cutoff_document(), incremental=incremental, prepared_at=NOW,
    )


def success_response(prepared):
    return {
        "broker_request_id": 12, "send_state": "DISPATCHED", "response_end_received": True,
        "response_end": {"start": "20260908", "end": "20260909"},
        "wire_parameters": native_history_wire_parameters(prepared), "start_generation": 2,
        "end_generation": 2, "broker_error_codes": [], "bars": [{
            "raw_date": "20260909", "session_date": "2026-09-09", "open": "1", "high": "1",
            "low": "1", "close": "0.2" if prepared["kind"] == "IV_HISTORY" else "100",
            "volume": "10", "valid_close": True, "date_eligible": True,
        }], "received_bar_count": 1, "timed_out": False, "epoch_valid": True,
    }


@pytest.mark.parametrize("kind,duration", [("PRICE_HISTORY", "1 Y"), ("IV_HISTORY", "2 Y")])
@pytest.mark.parametrize("incremental", [False, True])
def test_exact_native_requests_keep_vintage_format_and_incremental_parameters(kind, duration, incremental):
    request = prepared_request(kind=kind, incremental=incremental)
    assert validate_native_history_request(request) == request
    assert request["request_contract"]["durationStr"] == ("7 D" if incremental else duration)
    assert request["request_contract"]["formatDate"] == 1
    assert request["request_contract"]["endDateTime"] == ("" if kind == "PRICE_HISTORY" else NOW.isoformat())
    assert request["identity_status"] == "CACHE_QUALIFIED_NOT_PERSISTED_SECDEF"
    assert request["basis_contract"]["status"] == "NATIVE_UNRESOLVED"
    assert request["basis_contract"]["historical_session_coverage_verified"] is False
    assert request["production_eligible"] is False


def test_verified_current_close_allows_same_date_without_claiming_old_sessions():
    request = prepared_request()
    response = success_response(request)
    result = make_native_history_result(request, claim_id="claim.1", requested_at=NOW,
                                        available_at=NOW, response=response, reason_codes=[])
    assert validate_native_history_result(result, prepared_request=request) == result
    assert result["status"] == "DELIVERED"
    assert result["point_in_time_verified"] is False
    assert "session_close_at" not in result["response"]["bars"][0]


def test_cutoff_before_current_session_close_is_rejected():
    early = NOW - timedelta(hours=2)
    with pytest.raises(NativeHistoryContractError, match="COMPLETED_SESSION_UNVERIFIED"):
        build_native_history_request(contract=IDENTITY, kind="PRICE_HISTORY", cutoff=cutoff_document(now=early),
                                     incremental=False, prepared_at=early)


def test_published_early_close_is_used_without_a_weekday_guess():
    early = NOW - timedelta(hours=3)
    request = build_native_history_request(contract=IDENTITY, kind="PRICE_HISTORY", cutoff=cutoff_document(now=early, close="1300"),
                                           incremental=False, prepared_at=early)
    assert request["cutoff"]["session_close_at"] == "2026-09-09T17:00:00+00:00"


@pytest.mark.parametrize("field,value", [
    ("formatDate", 2), ("durationStr", "30 D"), ("endDateTime", "2026-09-08T20:00:00+00:00"),
    ("keepUpToDate", True), ("maximum_rows", 10000),
])
def test_self_resealed_request_cannot_change_native_wire_rules(field, value):
    request = prepared_request()
    request["request_contract"][field] = value
    request["request_hash"] = canonical_hash(request["request_contract"])
    request["content_hash"] = canonical_hash({key: value for key, value in request.items() if key != "content_hash"})
    with pytest.raises(NativeHistoryContractError):
        validate_native_history_request(request)


def test_changed_calendar_content_without_real_matching_hash_is_rejected():
    cutoff = cutoff_document()
    cutoff["calendar"]["sessions"][0]["close_utc"] = "2026-09-09T18:00:00+00:00"
    with pytest.raises(NativeHistoryContractError):
        build_native_history_request(contract=IDENTITY, kind="PRICE_HISTORY", cutoff=cutoff, incremental=False, prepared_at=NOW)


def test_calendar_observed_after_preparation_cannot_be_backdated():
    cutoff = cutoff_document(now=NOW + timedelta(seconds=120))
    cutoff["scheduled_for"] = NOW.isoformat()
    with pytest.raises(NativeHistoryContractError, match="CALENDAR_NOT_YET_AVAILABLE"):
        build_native_history_request(contract=IDENTITY, kind="PRICE_HISTORY", cutoff=cutoff, incremental=False, prepared_at=NOW)


def test_self_resealed_preparation_before_calendar_availability_is_rejected():
    later = NOW + timedelta(seconds=120)
    request = build_native_history_request(contract=IDENTITY, kind="PRICE_HISTORY", cutoff=cutoff_document(now=later),
                                           incremental=False, prepared_at=later)
    request["prepared_at"] = NOW.isoformat()
    request["cutoff"]["scheduled_for"] = NOW.isoformat()
    request["request_contract"]["source_cutoff_at"] = NOW.isoformat()
    request["request_hash"] = canonical_hash(request["request_contract"])
    request["content_hash"] = canonical_hash({key: value for key, value in request.items() if key != "content_hash"})
    with pytest.raises(NativeHistoryContractError, match="CALENDAR_NOT_YET_AVAILABLE"):
        validate_native_history_request(request)


@pytest.mark.parametrize("change", ["missing_end", "epoch", "wire", "date", "authority"])
def test_resealed_result_cannot_manufacture_end_epoch_dates_or_authority(change):
    request = prepared_request()
    result = make_native_history_result(request, claim_id="claim.1", requested_at=NOW,
                                        available_at=NOW, response=success_response(request), reason_codes=[])
    if change == "missing_end":
        result["response"]["response_end_received"] = False
        result["response"]["response_end"] = None
    elif change == "epoch":
        result["response"]["end_generation"] += 1
    elif change == "wire":
        result["response"]["wire_parameters"]["durationStr"] = "7 D"
    elif change == "date":
        result["response"]["bars"][0]["raw_date"] = "20260910"
    else:
        result["production_eligible"] = True
    result["content_hash"] = canonical_hash({key: value for key, value in result.items() if key != "content_hash"})
    with pytest.raises(NativeHistoryContractError):
        validate_native_history_result(result, prepared_request=request)


def test_results_are_detached_and_retain_native_partial_rows():
    request = prepared_request()
    response = success_response(request)
    old = deepcopy(response["bars"][0])
    old.update(raw_date="20260908", session_date="2026-09-08")
    response["bars"].append(old)
    response["received_bar_count"] = 2
    result = make_native_history_result(request, claim_id="claim.1", requested_at=NOW,
                                        available_at=NOW, response=response, reason_codes=[])
    response["bars"][0]["close"] = "999"
    assert result["response"]["bars"][0]["close"] == "100"
    assert "session_close_at" not in result["response"]["bars"][1]
