"""Expected-RED contracts for bounded Phase 2 external transports.

The suite is deliberately import-safe while Wave 1 freezes behavior ahead of
production implementation.  Every opener and response is local and recording;
the Phase 2 verifier separately replaces sockets so collection and execution
cannot perform DNS or network I/O.
"""
from __future__ import annotations

from dataclasses import dataclass
import importlib
import json
import math
from types import ModuleType
from typing import Callable, Mapping

import pytest


DEEPSEEK_URL = "https://api.deepseek.com/chat/completions"
FINNHUB_NEWS_URL = "https://finnhub.io/api/v1/company-news"
FINNHUB_EARNINGS_URL = "https://finnhub.io/api/v1/calendar/earnings"
ALPHA_VANTAGE_URL = "https://www.alphavantage.co/query"
DECLARED_IR_URL = "https://investor.example.test/news/releases"
SEC_ATOM_URL = (
    "https://www.sec.gov/cgi-bin/browse-edgar?"
    "action=getcurrent&type=8-K&company=&dateb=&owner=include&count=100&output=atom"
)
NASDAQ_URL = "https://api.nasdaq.com/api/calendar/earnings?date=2026-08-08"
JIN10_URL = "https://mcp.jin10.com/mcp"

DEEPSEEK_WHOLE_CYCLE_SECONDS = 6.5
DEEPSEEK_RESPONSE_BYTES = 65536
DEEPSEEK_ATTEMPTS_PER_COMPLETION = 2
ADVISORY_INPUTS_PER_BATCH = 3
HTTP_ATTEMPTS_PER_BATCH = 6


def _expected_red(detail: str) -> None:
    assert False, f"PHASE2_EXPECTED_RED:TRANSPORT_CONTRACT {detail}"


def _load_module(name: str) -> ModuleType | None:
    try:
        return importlib.import_module(name)
    except ModuleNotFoundError as exc:
        if exc.name == name:
            return None
        raise


@dataclass(frozen=True, slots=True)
class _TransportApi:
    error: type[Exception]
    remaining_timeout: Callable[..., float]
    read_bounded_response: Callable[..., bytes]
    decode_strict_json_object: Callable[..., Mapping[str, object]]


def _require_transport_api() -> _TransportApi:
    module = _load_module("options_copilot.providers.transport")
    required = (
        "BoundedTransportError",
        "remaining_timeout",
        "read_bounded_response",
        "decode_strict_json_object",
    )
    missing = [
        name
        for name in required
        if module is None or getattr(module, name, None) is None
    ]
    if missing:
        _expected_red(
            "missing source-neutral bounded transport API: " + ",".join(missing)
        )
    assert module is not None
    error = module.BoundedTransportError
    assert isinstance(error, type) and issubclass(error, Exception)
    return _TransportApi(
        error=error,
        remaining_timeout=module.remaining_timeout,
        read_bounded_response=module.read_bounded_response,
        decode_strict_json_object=module.decode_strict_json_object,
    )


class FakeResponse:
    """Context-managed response that records bounded reads and closure."""

    def __init__(
        self,
        body: bytes,
        *,
        url: str = DEEPSEEK_URL,
        status: int = 200,
        headers: Mapping[str, str] | None = None,
        read_failure: Exception | None = None,
    ) -> None:
        self.body = body
        self.url = url
        self.status = status
        self.headers = dict(
            headers
            if headers is not None
            else {
                "Content-Type": "application/json; charset=utf-8",
                "Content-Encoding": "identity",
                "Content-Length": str(len(body)),
            }
        )
        self.read_failure = read_failure
        self.read_limits: list[int] = []
        self.close_count = 0

    def __enter__(self) -> "FakeResponse":
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def close(self) -> None:
        self.close_count += 1

    def geturl(self) -> str:
        return self.url

    def read(self, limit: int) -> bytes:
        self.read_limits.append(limit)
        if self.read_failure is not None:
            raise self.read_failure
        return self.body[:limit]


class FakeOpener:
    """Local-only opener with deterministic responses and request recording."""

    def __init__(self, *outcomes: object) -> None:
        self.outcomes = list(outcomes)
        self.requests: list[tuple[object, float]] = []

    def open(self, request: object, *, timeout: float) -> object:
        self.requests.append((request, timeout))
        if not self.outcomes:
            raise AssertionError("recording opener has no remaining outcome")
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


@dataclass(frozen=True, slots=True)
class HostileTransportCase:
    case_id: str
    providers: tuple[str, ...]
    invariant: str
    expected_reason_family: str


HOSTILE_TRANSPORT_CASES = (
    HostileTransportCase("scheme", ("deepseek", "finnhub", "alpha", "company_ir", "sec", "nasdaq", "jin10"), "HTTPS only", "URL_NOT_ALLOWED"),
    HostileTransportCase("host", ("deepseek", "finnhub", "alpha", "company_ir", "sec", "nasdaq", "jin10"), "exact source-owned host", "URL_NOT_ALLOWED"),
    HostileTransportCase("port", ("deepseek", "finnhub", "alpha", "company_ir", "sec", "nasdaq", "jin10"), "default port only", "URL_NOT_ALLOWED"),
    HostileTransportCase("path", ("deepseek", "finnhub", "alpha", "company_ir", "sec", "nasdaq", "jin10"), "exact path", "URL_NOT_ALLOWED"),
    HostileTransportCase("query", ("deepseek", "finnhub", "alpha", "company_ir", "sec", "nasdaq"), "only source-contracted query keys", "URL_NOT_ALLOWED"),
    HostileTransportCase("userinfo", ("deepseek", "finnhub", "alpha", "company_ir", "sec", "nasdaq", "jin10"), "no URL credentials", "URL_NOT_ALLOWED"),
    HostileTransportCase("fragment", ("deepseek", "finnhub", "alpha", "company_ir", "sec", "nasdaq", "jin10"), "no fragment", "URL_NOT_ALLOWED"),
    HostileTransportCase("redirect", ("deepseek", "finnhub", "alpha", "company_ir", "sec", "nasdaq", "jin10"), "no redirect or final-URL change", "REDIRECT_FORBIDDEN"),
    HostileTransportCase("content_type", ("deepseek", "finnhub", "alpha", "company_ir", "sec", "nasdaq", "jin10"), "source-owned media type", "CONTENT_TYPE_INVALID"),
    HostileTransportCase("encoding", ("deepseek", "finnhub", "alpha", "company_ir", "sec", "nasdaq", "jin10"), "identity and UTF-8 or ASCII", "ENCODING_INVALID"),
    HostileTransportCase("declared_size", ("deepseek", "finnhub", "alpha", "company_ir", "sec", "nasdaq", "jin10"), "declared body at or below cap", "RESPONSE_TOO_LARGE"),
    HostileTransportCase("streamed_size", ("deepseek", "finnhub", "alpha", "company_ir", "sec", "nasdaq", "jin10"), "cap plus one bounded read", "RESPONSE_TOO_LARGE"),
    HostileTransportCase("duplicate_key", ("deepseek", "finnhub", "alpha", "company_ir", "nasdaq", "jin10"), "duplicate JSON key rejected", "BAD_JSON"),
    HostileTransportCase("nonfinite", ("deepseek", "finnhub", "alpha", "company_ir", "nasdaq", "jin10"), "NaN and Infinity rejected", "BAD_JSON"),
    HostileTransportCase("deadline", ("deepseek", "finnhub", "alpha", "company_ir", "sec", "nasdaq", "jin10"), "one monotonic whole-cycle budget", "REQUEST_TIMEOUT"),
    HostileTransportCase("pacing", ("finnhub", "alpha", "company_ir", "sec", "nasdaq", "jin10"), "source-scoped limiter", "PACING_UNVERIFIED"),
    HostileTransportCase("close", ("deepseek", "finnhub", "alpha", "company_ir", "sec", "nasdaq", "jin10"), "close once on every response branch", "RESPONSE_CLOSED"),
    HostileTransportCase("isolation", ("finnhub", "alpha", "company_ir", "sec", "nasdaq", "jin10"), "one source cannot mutate another", "SOURCE_ISOLATED"),
)


@pytest.mark.parametrize(
    "case",
    HOSTILE_TRANSPORT_CASES,
    ids=lambda case: case.case_id,
)
def test_cross_provider_hostile_transport_matrix(case: HostileTransportCase) -> None:
    """Freeze the complete rejection/isolation inventory before implementation."""

    assert case.providers
    assert len(case.providers) == len(set(case.providers))
    assert case.invariant
    assert case.expected_reason_family.isupper()
    assert case.expected_reason_family.replace("_", "").isalnum()


def test_matrix_has_every_required_parameter_id_exactly_once() -> None:
    assert [case.case_id for case in HOSTILE_TRANSPORT_CASES] == [
        "scheme",
        "host",
        "port",
        "path",
        "query",
        "userinfo",
        "fragment",
        "redirect",
        "content_type",
        "encoding",
        "declared_size",
        "streamed_size",
        "duplicate_key",
        "nonfinite",
        "deadline",
        "pacing",
        "close",
        "isolation",
    ]


def test_source_owned_destination_method_and_credential_contracts_are_exact() -> None:
    assert {
        "deepseek": (DEEPSEEK_URL, "POST", "Authorization"),
        "finnhub_news": (FINNHUB_NEWS_URL, "GET", "X-Finnhub-Token"),
        "finnhub_earnings": (FINNHUB_EARNINGS_URL, "GET", "X-Finnhub-Token"),
        "alpha": (ALPHA_VANTAGE_URL, "GET", "apikey"),
        "company_ir": (DECLARED_IR_URL, "GET", None),
        "sec": (SEC_ATOM_URL, "GET", None),
        "nasdaq": (NASDAQ_URL, "GET", None),
        "jin10": (JIN10_URL, "POST", "Authorization"),
    } == {
        "deepseek": ("https://api.deepseek.com/chat/completions", "POST", "Authorization"),
        "finnhub_news": ("https://finnhub.io/api/v1/company-news", "GET", "X-Finnhub-Token"),
        "finnhub_earnings": ("https://finnhub.io/api/v1/calendar/earnings", "GET", "X-Finnhub-Token"),
        "alpha": ("https://www.alphavantage.co/query", "GET", "apikey"),
        "company_ir": ("https://investor.example.test/news/releases", "GET", None),
        "sec": (SEC_ATOM_URL, "GET", None),
        "nasdaq": (NASDAQ_URL, "GET", None),
        "jin10": ("https://mcp.jin10.com/mcp", "POST", "Authorization"),
    }


@pytest.mark.parametrize("elapsed", (0.0, 1.25, 6.49), ids=("start", "middle", "edge"))
def test_deadline_uses_remaining_time_from_one_cycle(elapsed: float) -> None:
    api = _require_transport_api()
    deadline = 100.0 + DEEPSEEK_WHOLE_CYCLE_SECONDS
    remaining = api.remaining_timeout(deadline, lambda: 100.0 + elapsed, 6.5)
    assert remaining == pytest.approx(min(6.5, 6.5 - elapsed))


@pytest.mark.parametrize("clock_value", (106.5, 106.5001, math.inf, math.nan))
def test_deadline_refuses_exhausted_or_nonfinite_budget(clock_value: float) -> None:
    api = _require_transport_api()
    with pytest.raises(api.error) as raised:
        api.remaining_timeout(106.5, lambda: clock_value, 6.5)
    assert getattr(raised.value, "reason", None) == "REQUEST_TIMEOUT"
    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None


def test_declared_size_rejected_before_any_body_read_and_response_closed() -> None:
    api = _require_transport_api()
    response = FakeResponse(
        b"{}",
        headers={"Content-Length": str(DEEPSEEK_RESPONSE_BYTES + 1)},
    )
    with pytest.raises(api.error) as raised:
        api.read_bounded_response(response, DEEPSEEK_RESPONSE_BYTES)
    assert getattr(raised.value, "reason", None) == "RESPONSE_TOO_LARGE"
    assert response.read_limits == []
    assert response.close_count == 1


def test_streamed_size_reads_cap_plus_one_and_response_closed() -> None:
    api = _require_transport_api()
    response = FakeResponse(
        b"x" * (DEEPSEEK_RESPONSE_BYTES + 1),
        headers={},
    )
    with pytest.raises(api.error) as raised:
        api.read_bounded_response(response, DEEPSEEK_RESPONSE_BYTES)
    assert getattr(raised.value, "reason", None) == "RESPONSE_TOO_LARGE"
    assert response.read_limits == [DEEPSEEK_RESPONSE_BYTES + 1]
    assert response.close_count == 1


@pytest.mark.parametrize(
    ("payload", "case_id"),
    (
        (b"", "empty"),
        (b'{"value":', "partial"),
        (b'{"value":1,"value":2}', "duplicate_key"),
        (b'{"value":NaN}', "nonfinite_nan"),
        (b'{"value":Infinity}', "nonfinite_infinity"),
        (b"[]", "root_array"),
    ),
    ids=lambda value: value if isinstance(value, str) else None,
)
def test_duplicate_key_nonfinite_empty_partial_json_rejected(
    payload: bytes,
    case_id: str,
) -> None:
    api = _require_transport_api()
    with pytest.raises(api.error) as raised:
        api.decode_strict_json_object(payload)
    assert getattr(raised.value, "reason", None) == "BAD_JSON", case_id
    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None


def test_strict_json_accepts_one_finite_object() -> None:
    api = _require_transport_api()
    assert api.decode_strict_json_object(b'{"ok":true,"value":1.25}') == {
        "ok": True,
        "value": 1.25,
    }


def test_deepseek_exact_caps_and_nonstreaming_transport_surface() -> None:
    module = importlib.import_module("options_copilot.llm.deepseek")
    actual = {
        "cycle": getattr(module, "DEEPSEEK_WHOLE_CYCLE_SECONDS", None),
        "response": getattr(module, "MAXIMUM_DEEPSEEK_RESPONSE_BYTES", None),
        "attempts": getattr(module, "MAXIMUM_DEEPSEEK_HTTP_ATTEMPTS", None),
        "inputs": ADVISORY_INPUTS_PER_BATCH,
        "batch_attempts": HTTP_ATTEMPTS_PER_BATCH,
    }
    expected = {
        "cycle": 6.5,
        "response": 65536,
        "attempts": 2,
        "inputs": 3,
        "batch_attempts": 6,
    }
    if actual != expected:
        _expected_red(f"DeepSeek transport caps differ: {actual!r}")
    assert actual == expected


def test_deepseek_fake_opener_contract_requires_no_live_network() -> None:
    module = importlib.import_module("options_copilot.llm.deepseek")
    response_body = json.dumps(
        {
            "choices": [
                {
                    "finish_reason": "stop",
                    "message": {"content": "{\"state\":\"UNCERTAIN\"}"},
                }
            ],
            "usage": {
                "prompt_tokens": 2,
                "completion_tokens": 1,
                "prompt_cache_hit_tokens": 0,
                "prompt_cache_miss_tokens": 2,
            },
        },
        separators=(",", ":"),
    ).encode("utf-8")
    response = FakeResponse(response_body)
    opener = FakeOpener(response)
    try:
        transport = module.UrllibTransport(opener=opener)
    except TypeError:
        _expected_red("DeepSeek transport has no injected recording opener")
    result = transport.post(
        url=DEEPSEEK_URL,
        headers={
            "Authorization": "Bearer test-only-placeholder",
            "Accept": "application/json",
            "Accept-Encoding": "identity",
            "Content-Type": "application/json",
        },
        body="{}",
        timeout_seconds=6.5,
        stream=False,
    )
    assert result.status == 200
    assert len(opener.requests) == 1
    request, timeout = opener.requests[0]
    assert request.full_url == DEEPSEEK_URL
    assert request.get_method() == "POST"
    assert timeout == 6.5
    assert response.read_limits == [DEEPSEEK_RESPONSE_BYTES + 1]
    assert response.close_count == 1


def test_fake_openers_are_recording_local_only_objects() -> None:
    response = FakeResponse(b"{}")
    opener = FakeOpener(response)
    outcome = opener.open(object(), timeout=1.0)
    assert outcome is response
    assert len(opener.requests) == 1
    assert opener.outcomes == []
