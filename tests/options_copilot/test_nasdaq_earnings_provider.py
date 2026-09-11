from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from urllib.error import HTTPError

import pytest

from options_copilot.providers.events import EarningsEvent
from options_copilot.providers.nasdaq_earnings import (
    MAXIMUM_NASDAQ_RESPONSE_BYTES,
    NASDAQ_EARNINGS_URL,
    NASDAQ_USER_AGENT,
    NasdaqEarningsDayFailure,
    NasdaqEarningsEvent,
    NasdaqEarningsProvider,
    NasdaqHttpsTransport,
    _NoRedirect,
)


NOW = datetime(2026, 8, 6, 12, tzinfo=timezone.utc)


class MutableClock:
    def __init__(self, value: datetime = NOW) -> None:
        self.value = value

    def __call__(self) -> datetime:
        return self.value


def _payload(day: date, rows: list[dict[str, object]]) -> dict[str, object]:
    return {
        "data": {
            "asOf": f"{day.strftime('%a, %b')} {day.day}, {day.year}",
            "headers": {"time": "Time", "symbol": "Symbol"},
            "rows": rows,
        }
    }


def _verified_empty_payload(day: date) -> dict[str, object]:
    return {
        "data": {
            "asOf": f"{day.strftime('%a, %b')} {day.day}, {day.year}",
            "headers": {"time": "Time", "symbol": "Symbol"},
            "rows": None,
        },
        "message": None,
        "status": {
            "rCode": 200,
            "developerMessage": None,
            "bCodeMessage": None,
        },
    }


def _row(
    symbol: str,
    *,
    session: str = "time-pre-market",
    eps: str = "$1.23",
) -> dict[str, object]:
    return {
        "lastYearRptDt": "8/07/2025",
        "lastYearEPS": "$0.64",
        "time": session,
        "symbol": symbol,
        "name": f"{symbol} company body must not be retained",
        "marketCap": "$1,000,000",
        "fiscalQuarterEnding": "Jun/2026",
        "epsForecast": eps,
        "noOfEsts": "3",
    }


def test_two_week_provider_is_structured_supporting_only_and_cached() -> None:
    clock = MutableClock()
    calls: list[tuple[str, dict[str, str], float]] = []

    def transport(url, *, headers, timeout_seconds):
        calls.append((url, dict(headers), timeout_seconds))
        day = date.fromisoformat(url.rsplit("=", 1)[1])
        if day == date(2026, 8, 6):
            return _payload(day, [_row("COP"), _row("NET", session="time-after-hours", eps="($0.03)")])
        return _payload(day, [])

    provider = NasdaqEarningsProvider(
        transport=transport,
        now=clock,
        timeout_seconds=4,
        cache_ttl=timedelta(minutes=15),
    )

    events = provider.earnings_calendar(date(2026, 8, 6), date(2026, 8, 7))
    cached = provider.earnings_calendar(date(2026, 8, 6), date(2026, 8, 7))

    assert events == cached
    assert len(calls) == 2
    assert [item[0] for item in calls] == [
        f"{NASDAQ_EARNINGS_URL}?date=2026-08-06",
        f"{NASDAQ_EARNINGS_URL}?date=2026-08-07",
    ]
    assert all(headers == {"Accept": "application/json"} for _, headers, _ in calls)
    assert all(timeout == 4 for _, _, timeout in calls)
    assert provider.health == "READY"
    assert provider.health_reason is None
    assert all(isinstance(item, EarningsEvent) for item in events)
    assert all(isinstance(item, NasdaqEarningsEvent) for item in events)
    assert [(item.symbol, item.report_session, item.hour) for item in events] == [
        ("COP", "PRE_MARKET", "bmo"),
        ("NET", "AFTER_MARKET", "amc"),
    ]
    assert events[0].report_date == date(2026, 8, 6)
    assert events[0].is_estimated is True
    assert events[0].source_url == f"{NASDAQ_EARNINGS_URL}?date=2026-08-06"
    assert events[0].decision_authority == "SUPPORTING_ONLY"
    assert events[0].provenance == (events[0].source_url,)
    assert events[0].eps_estimate is not None and str(events[0].eps_estimate) == "1.23"
    assert events[1].eps_estimate is not None and str(events[1].eps_estimate) == "-0.03"
    assert not hasattr(events[0], "name")
    assert not hasattr(events[0], "raw_payload")


def test_cache_expiry_refetches_without_faking_prior_first_seen() -> None:
    clock = MutableClock()
    calls = 0

    def transport(url, **_kwargs):
        nonlocal calls
        calls += 1
        day = date.fromisoformat(url.rsplit("=", 1)[1])
        return _payload(day, [_row("NVDA")])

    provider = NasdaqEarningsProvider(
        transport=transport,
        now=clock,
        cache_ttl=timedelta(minutes=5),
    )
    first = provider.earnings_calendar(date(2026, 8, 6), date(2026, 8, 6))[0]
    clock.value += timedelta(minutes=6)
    refreshed = provider.earnings_calendar(date(2026, 8, 6), date(2026, 8, 6))[0]

    assert calls == 2
    assert refreshed.first_seen_at == first.first_seen_at
    assert refreshed.observed_at == clock.value
    assert refreshed.ingested_at == clock.value


def test_explicit_success_envelope_with_null_rows_is_cached_as_verified_empty() -> None:
    calls = 0

    def transport(url, **_kwargs):
        nonlocal calls
        calls += 1
        return _verified_empty_payload(date.fromisoformat(url.rsplit("=", 1)[1]))

    provider = NasdaqEarningsProvider(transport=transport, now=lambda: NOW)

    first = provider.earnings_calendar(date(2026, 8, 8), date(2026, 8, 8))
    cached = provider.earnings_calendar(date(2026, 8, 8), date(2026, 8, 8))

    assert first == cached == ()
    assert calls == 1
    assert provider.health == "READY"
    assert provider.health_reason is None
    assert provider.failures == ()
    assert provider.failed_dates == ()


@pytest.mark.parametrize(
    ("mutation", "expected_reason"),
    (
        ("rows_missing", "ROWS_NOT_ARRAY"),
        ("status_missing", "ROWS_NOT_ARRAY"),
        ("rcode_string", "ROWS_NOT_ARRAY"),
        ("rcode_float", "ROWS_NOT_ARRAY"),
        ("rcode_failure", "ROWS_NOT_ARRAY"),
        ("message_present", "ROWS_NOT_ARRAY"),
        ("developer_message_present", "ROWS_NOT_ARRAY"),
        ("business_code_present", "ROWS_NOT_ARRAY"),
        ("date_mismatch", "DATE_MISMATCH"),
    ),
)
def test_null_rows_fail_closed_without_complete_verified_empty_envelope(
    mutation: str,
    expected_reason: str,
) -> None:
    day = date(2026, 8, 8)
    payload = _verified_empty_payload(day)
    if mutation == "rows_missing":
        del payload["data"]["rows"]  # type: ignore[index]
    elif mutation == "status_missing":
        del payload["status"]
    elif mutation == "rcode_string":
        payload["status"]["rCode"] = "200"  # type: ignore[index]
    elif mutation == "rcode_float":
        payload["status"]["rCode"] = 200.0  # type: ignore[index]
    elif mutation == "rcode_failure":
        payload["status"]["rCode"] = 500  # type: ignore[index]
    elif mutation == "message_present":
        payload["message"] = "remote failure must not be retained"
    elif mutation == "developer_message_present":
        payload["status"]["developerMessage"] = "private detail"  # type: ignore[index]
    elif mutation == "business_code_present":
        payload["status"]["bCodeMessage"] = [{"code": "NO_DATA"}]  # type: ignore[index]
    elif mutation == "date_mismatch":
        payload["data"]["asOf"] = "Sun, Aug 9, 2026"  # type: ignore[index]

    provider = NasdaqEarningsProvider(
        transport=lambda *_args, **_kwargs: payload,
        now=lambda: NOW,
    )

    assert provider.earnings_calendar(day, day) == ()
    assert provider.health == "DEGRADED"
    assert provider.health_reason == "NASDAQ_EARNINGS_UNAVAILABLE"
    assert provider.failed_dates == (day,)
    assert provider.failures[0].reason == expected_reason
    assert "private detail" not in repr(provider.failures)
    assert "remote failure" not in repr(provider.failures)


@pytest.mark.parametrize(
    "start,end",
    [
        (date(2026, 8, 7), date(2026, 8, 6)),
        (date(2026, 8, 6), date(2026, 8, 21)),
        (datetime(2026, 8, 6, tzinfo=timezone.utc), date(2026, 8, 7)),
    ],
)
def test_window_must_be_an_inclusive_maximum_two_week_date_window(start, end) -> None:
    provider = NasdaqEarningsProvider(transport=lambda *_args, **_kwargs: {})
    with pytest.raises((TypeError, ValueError)):
        provider.earnings_calendar(start, end)


@pytest.mark.parametrize(
    ("mutation", "expected_reason"),
    (
        ("transport", "REQUEST_TIMEOUT"),
        ("root", "ROOT_NOT_OBJECT"),
        ("asof", "DATE_MISMATCH"),
        ("rows", "ROWS_NOT_ARRAY"),
        ("row", "UNSUPPORTED_SESSION"),
        ("session", "UNSUPPORTED_SESSION"),
        ("duplicate", "DUPLICATE_SYMBOL_DATE"),
    ),
)
def test_remote_or_schema_fault_retains_successful_days_and_degrades_window(
    mutation: str,
    expected_reason: str,
) -> None:
    calls: list[str] = []

    def transport(url, **_kwargs):
        calls.append(url)
        day = date.fromisoformat(url.rsplit("=", 1)[1])
        if mutation == "transport" and day == date(2026, 8, 7):
            raise TimeoutError("Authorization: Bearer sentinel-provider-secret")
        payload: object = _payload(day, [_row("AAPL")])
        if mutation == "root" and day == date(2026, 8, 7):
            payload = []
        elif mutation == "asof" and day == date(2026, 8, 7):
            payload["data"]["asOf"] = "Sat, Aug 8, 2026"  # type: ignore[index]
        elif mutation == "rows" and day == date(2026, 8, 7):
            payload["data"]["rows"] = {}  # type: ignore[index]
        elif mutation == "row" and day == date(2026, 8, 7):
            payload["data"]["rows"] = [{"symbol": "AAPL"}]  # type: ignore[index]
        elif mutation == "session" and day == date(2026, 8, 7):
            payload["data"]["rows"][0]["time"] = "sometime"  # type: ignore[index]
        elif mutation == "duplicate" and day == date(2026, 8, 7):
            payload["data"]["rows"] = [_row("AAPL"), _row("AAPL")]  # type: ignore[index]
        return payload

    provider = NasdaqEarningsProvider(transport=transport, now=lambda: NOW)

    events = provider.earnings_calendar(date(2026, 8, 6), date(2026, 8, 7))

    assert [(item.report_date, item.symbol) for item in events] == [
        (date(2026, 8, 6), "AAPL")
    ]
    assert provider.health == "DEGRADED"
    assert provider.health_reason == "NASDAQ_EARNINGS_PARTIAL_WINDOW"
    assert provider.failed_dates == (date(2026, 8, 7),)
    assert provider.failures == (
        NasdaqEarningsDayFailure(
            report_date=date(2026, 8, 7),
            reason=expected_reason,
            source_url=f"{NASDAQ_EARNINGS_URL}?date=2026-08-07",
        ),
    )
    assert "sentinel-provider-secret" not in repr(provider.failures)

    # A successful day is cached even when another day in the window fails.
    provider.earnings_calendar(date(2026, 8, 6), date(2026, 8, 6))
    assert calls.count(f"{NASDAQ_EARNINGS_URL}?date=2026-08-06") == 1
    assert provider.health == "READY"
    assert provider.health_reason is None
    assert provider.failures == ()
    assert provider.failed_dates == ()


def test_all_failed_days_are_explicit_and_health_never_looks_ready() -> None:
    def transport(url, **_kwargs):
        day = date.fromisoformat(url.rsplit("=", 1)[1])
        raise OSError(f"Cookie: secret-for-{day.isoformat()}")

    provider = NasdaqEarningsProvider(transport=transport, now=lambda: NOW)

    assert provider.earnings_calendar(date(2026, 8, 8), date(2026, 8, 9)) == ()
    assert provider.health == "DEGRADED"
    assert provider.health_reason == "NASDAQ_EARNINGS_UNAVAILABLE"
    assert provider.failed_dates == (date(2026, 8, 8), date(2026, 8, 9))
    assert [item.reason for item in provider.failures] == [
        "REQUEST_FAILED",
        "REQUEST_FAILED",
    ]
    assert "secret-for" not in repr(provider.failures)


class FakeResponse:
    def __init__(
        self,
        body: bytes,
        *,
        url: str,
        status: int = 200,
        headers: dict[str, str] | None = None,
    ) -> None:
        self._body = body
        self._url = url
        self.status = status
        self.headers = headers or {
            "Content-Type": "application/json; charset=utf-8",
            "Content-Encoding": "identity",
            "Content-Length": str(len(body)),
        }

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def geturl(self) -> str:
        return self._url

    def read(self, limit: int) -> bytes:
        return self._body[:limit]


class FakeOpener:
    def __init__(self, response: FakeResponse) -> None:
        self.response = response
        self.requests: list[tuple[object, float]] = []

    def open(self, request, *, timeout):
        self.requests.append((request, timeout))
        return self.response


def test_https_transport_has_fixed_request_surface_and_bounded_json() -> None:
    url = f"{NASDAQ_EARNINGS_URL}?date=2026-08-06"
    body = json.dumps(_payload(date(2026, 8, 6), [])).encode()
    opener = FakeOpener(FakeResponse(body, url=url))
    transport = NasdaqHttpsTransport(opener=opener)

    result = transport(
        url,
        headers={"Accept": "application/json"},
        timeout_seconds=7,
    )

    assert result == _payload(date(2026, 8, 6), [])
    request, timeout = opener.requests[0]
    assert request.full_url == url
    assert request.get_method() == "GET"
    assert request.get_header("User-agent") == NASDAQ_USER_AGENT
    assert request.get_header("Accept") == "application/json"
    assert request.get_header("Accept-encoding") == "identity"
    assert timeout == 7


@pytest.mark.parametrize(
    "url",
    [
        "http://api.nasdaq.com/api/calendar/earnings?date=2026-08-06",
        "https://www.nasdaq.com/api/calendar/earnings?date=2026-08-06",
        "https://api.nasdaq.com:443/api/calendar/earnings?date=2026-08-06",
        "https://api.nasdaq.com/api/calendar/earnings/extra?date=2026-08-06",
        "https://api.nasdaq.com/api/calendar/earnings?date=2026-08-06&x=1",
        "https://api.nasdaq.com/api/calendar/earnings?date=2026-08-06#fragment",
        "https://user:pass@api.nasdaq.com/api/calendar/earnings?date=2026-08-06",
    ],
)
def test_https_transport_rejects_every_url_outside_exact_allowlist(url: str) -> None:
    transport = NasdaqHttpsTransport(
        opener=FakeOpener(FakeResponse(b"{}", url=url))
    )
    with pytest.raises(ValueError):
        transport(url, headers={"Accept": "application/json"}, timeout_seconds=5)


def test_https_transport_rejects_redirects_oversize_and_duplicate_json_keys() -> None:
    url = f"{NASDAQ_EARNINGS_URL}?date=2026-08-06"
    redirected = FakeOpener(
        FakeResponse(b"{}", url=f"{NASDAQ_EARNINGS_URL}?date=2026-08-07")
    )
    with pytest.raises(ValueError, match="redirect"):
        NasdaqHttpsTransport(opener=redirected)(
            url, headers={"Accept": "application/json"}, timeout_seconds=5
        )

    oversized = FakeOpener(
        FakeResponse(
            b"{}",
            url=url,
            headers={
                "Content-Type": "application/json",
                "Content-Length": str(MAXIMUM_NASDAQ_RESPONSE_BYTES + 1),
            },
        )
    )
    with pytest.raises(ValueError, match="size"):
        NasdaqHttpsTransport(opener=oversized)(
            url, headers={"Accept": "application/json"}, timeout_seconds=5
        )

    duplicate = FakeOpener(
        FakeResponse(b'{"data": {}, "data": {}}', url=url)
    )
    with pytest.raises(ValueError, match="JSON"):
        NasdaqHttpsTransport(opener=duplicate)(
            url, headers={"Accept": "application/json"}, timeout_seconds=5
        )

    with pytest.raises(HTTPError):
        _NoRedirect().redirect_request(
            SimpleNamespace(full_url=url), None, 302, "redirect", {}, url
        )
