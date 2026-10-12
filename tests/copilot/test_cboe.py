from __future__ import annotations

import gzip
import io
import json
from datetime import date
from typing import Any

import pytest

from copilot import http
from copilot.market.cboe import CboeClient, CboeError, parse_chain, parse_occ


def payload(rows: list[dict[str, Any]], price: float = 378.62) -> dict[str, Any]:
    return {
        "timestamp": "2026-10-09 03:57:30",
        "data": {"symbol": "GLD", "current_price": price, "prev_day_close": 375.9, "bid": 378.6,
                 "ask": 378.7, "price_change_percent": 0.72, "iv30": 20.68,
                 "last_trade_time": "2026-10-08T15:59:59", "options": rows},
    }


ROW = {"option": "GLD261120C00385000", "bid": 8.75, "ask": 9.0, "iv": 0.2077, "delta": 0.4531,
       "gamma": 0.01, "vega": 0.6, "theta": -0.1195, "open_interest": 2051.0, "volume": 217.0,
       "last_trade_price": 8.75, "theo": 8.831}


def test_parse_occ() -> None:
    assert parse_occ("GLD261120C00385000") == ("GLD", date(2026, 11, 20), "C", 385.0)
    assert parse_occ("SPY261023P00772500") == ("SPY", date(2026, 10, 23), "P", 772.5)
    assert parse_occ("garbage") is None
    assert parse_occ("GLD261340C00385000") is None  # month 13


def test_parse_chain_maps_fields_and_drops_adjusted_roots() -> None:
    rows = [ROW, {**ROW, "option": "GLD1261120C00385000"}, {**ROW, "option": "GLD261120P00385000", "bid": 0}]
    chain = parse_chain(payload(rows), "GLD")
    assert chain.underlying.price == 378.62
    assert chain.underlying.iv30 == pytest.approx(0.2068)
    assert chain.underlying.change_pct == pytest.approx(0.0072)
    assert chain.as_of.isoformat() == "2026-10-09T03:57:30+00:00"
    assert len(chain.quotes) == 2  # adjusted GLD1 contract dropped
    call = chain.quotes[0]
    assert (call.expiry, call.strike, call.right, call.bid, call.ask) == (date(2026, 11, 20), 385.0, "C", 8.75, 9.0)
    assert (call.open_interest, call.volume, call.model_price, call.source) == (2051, 217, 8.831, "cboe")
    assert chain.quotes[1].bid is None  # a zero bid means no bid
    assert call.market_data_type == 3


def test_parse_chain_rejects_empty_or_malformed() -> None:
    with pytest.raises(CboeError):
        parse_chain(payload([]), "GLD")
    with pytest.raises(CboeError):
        parse_chain({"oops": 1}, "GLD")


def test_client_maps_transport_errors_and_bad_symbols() -> None:
    def fetch(url: str) -> Any:
        raise http.HttpError("HTTP_ERROR", 503)

    with pytest.raises(CboeError, match="503"):
        CboeClient(fetch=fetch).chain("GLD")
    with pytest.raises(CboeError):
        CboeClient(fetch=lambda url: payload([ROW])).chain("../x")
    seen: list[str] = []
    chain = CboeClient(fetch=lambda url: seen.append(url) or payload([ROW])).chain("GLD")
    assert seen == ["https://cdn-api.cboe.com/api/global/delayed_quotes/options/GLD.json"]
    assert chain.quotes


class _Response(io.BytesIO):
    def __init__(self, body: bytes, headers: dict[str, str]) -> None:
        super().__init__(body)
        self.headers = headers


def test_request_json_decompresses_gzip_and_caps_size(monkeypatch) -> None:
    body = gzip.compress(json.dumps({"x": "y" * 1000}).encode())

    class Opener:
        def open(self, request, timeout):
            assert request.headers["Accept-encoding"] == "gzip"
            return _Response(body, {"Content-Encoding": "gzip"})

    monkeypatch.setattr(http, "_OPENER", Opener())
    url = "https://cdn-api.cboe.com/a.json"
    assert http.request_json("GET", url, allowed_hosts=["cdn-api.cboe.com"])["x"]
    with pytest.raises(http.HttpError, match="RESPONSE_TOO_LARGE"):
        http.request_json("GET", url, allowed_hosts=["cdn-api.cboe.com"], max_bytes=500)


def test_redirects_only_to_allowlisted_https_hosts() -> None:
    from urllib.error import HTTPError
    from urllib.request import Request

    handler = http._AllowlistRedirect()
    request = Request("https://cdn.cboe.com/api/x.json")
    request.allowed_hosts = {"cdn.cboe.com", "cdn-api.cboe.com"}
    moved = handler.redirect_request(request, None, 307, "moved", {}, "https://cdn-api.cboe.com/api/x.json")
    assert moved.full_url == "https://cdn-api.cboe.com/api/x.json"
    with pytest.raises(HTTPError):
        handler.redirect_request(request, None, 307, "moved", {}, "https://evil.example.com/x.json")
    with pytest.raises(HTTPError):
        handler.redirect_request(request, None, 307, "moved", {}, "http://cdn-api.cboe.com/x.json")
