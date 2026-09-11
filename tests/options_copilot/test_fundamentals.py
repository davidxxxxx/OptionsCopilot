"""Point-in-time fundamentals remain bounded and supporting-only."""
from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import json
import threading
from urllib.error import HTTPError

import pytest

from options_copilot.api import OptionsCopilotServices, create_app
from options_copilot.fundamentals import (
    FinnhubValuationProvider,
    FundamentalMetric,
    FundamentalObservation,
    FundamentalsService,
    FundamentalsStore,
    SecCompanyFactsProvider,
    SecManagementGuidanceProvider,
    StrictFundamentalsHttpsTransport,
    StrictSecFilingHttpsTransport,
)
from options_copilot.fundamentals.providers import (
    FINNHUB_METRIC_URL,
    FundamentalProviderError,
    SEC_COMPANY_FACTS_PREFIX,
    SEC_SUBMISSIONS_PREFIX,
    SEC_TICKER_URL,
    _windows_system_proxy_opener,
)


NOW = datetime(2026, 8, 13, 12, 0, tzinfo=timezone.utc)


def _observation(
    value: str,
    *,
    observed_at: datetime = NOW,
) -> FundamentalObservation:
    return FundamentalObservation(
        symbol="AAPL",
        cik="0000320193",
        metric=FundamentalMetric.REVENUE,
        value=Decimal(value),
        unit="USD",
        basis="SEC_XBRL_ACTUAL",
        period_end=date(2026, 6, 30),
        fiscal_period="Q3",
        fiscal_year=2026,
        source="SEC_XBRL",
        source_id=f"sec:accession:{value}",
        source_url=f"{SEC_COMPANY_FACTS_PREFIX}0000320193.json",
        source_filed_date=date(2026, 7, 30),
        observed_at=observed_at,
        taxonomy="us-gaap",
        tag="RevenueFromContractWithCustomerExcludingAssessedTax",
        form="10-Q",
    )


def test_revision_ledger_is_append_only_and_point_in_time(tmp_path) -> None:
    store = FundamentalsStore(tmp_path / "fundamentals.sqlite3")
    first = store.append(_observation("100", observed_at=NOW))
    corrected = store.append(
        _observation("105", observed_at=NOW + timedelta(days=2))
    )

    assert first.inserted is True
    assert corrected.record.revision_number == 2
    assert corrected.record.supersedes_hash == first.record.observation.content_hash
    assert [item.observation.value for item in store.current(
        symbols=("AAPL",),
        as_of=NOW + timedelta(days=1),
    )] == [Decimal("100")]
    assert [item.observation.value for item in store.current(
        symbols=("AAPL",),
        as_of=NOW + timedelta(days=3),
    )] == [Decimal("105")]
    assert len(store.revisions(first.record.observation.series_key)) == 2
    store.assert_integrity()
    store.close()


def test_store_serializes_reads_with_an_inflight_append(tmp_path) -> None:
    store = FundamentalsStore(tmp_path / "concurrent-fundamentals.sqlite3")
    store.append(_observation("100"))
    entered = threading.Event()
    release = threading.Event()
    original_stored = store._stored

    def held_stored(row):
        entered.set()
        assert release.wait(timeout=2)
        return original_stored(row)

    store._stored = held_stored  # type: ignore[method-assign]
    result = []
    reader = threading.Thread(
        target=lambda: result.extend(store.revisions(_observation("100").series_key)),
    )
    reader.start()
    assert entered.wait(timeout=2)
    release.set()
    reader.join(timeout=2)

    assert not reader.is_alive()
    assert len(result) == 1
    store._stored = original_stored  # type: ignore[method-assign]
    store.assert_integrity()
    store.close()


def test_supporting_evidence_does_not_leak_revision_after_cutoff(tmp_path) -> None:
    store = FundamentalsStore(tmp_path / "point-in-time.sqlite3")
    store.append(_observation("100", observed_at=NOW))
    store.append(
        _observation("105", observed_at=NOW + timedelta(days=4))
    )
    service = FundamentalsService(store, providers=(), symbols=("AAPL",))

    supporting = service.supporting_evidence(
        "AAPL",
        as_of=NOW + timedelta(days=2),
    )

    assert [row["value"] for row in supporting["payload"]["records"]] == ["100"]
    assert supporting["payload"]["revisions"] == []
    assert supporting["payload"]["revision_count"] == 0
    assert "105" not in json.dumps(supporting, sort_keys=True)
    service.close()


def test_supporting_evidence_fails_closed_when_ledger_is_corrupted(tmp_path) -> None:
    store = FundamentalsStore(tmp_path / "corrupt-supporting.sqlite3")
    store.append(_observation("100", observed_at=NOW))
    with store._lock:
        store._connection.execute(
            "UPDATE fundamental_records SET content_json=? WHERE sequence=1",
            ('{"tampered":true}',),
        )
    service = FundamentalsService(store, providers=(), symbols=("AAPL",))

    supporting = service.supporting_evidence("AAPL", as_of=NOW)

    assert supporting["status"] == "DEGRADED"
    assert supporting["reason_codes"] == ["FUNDAMENTALS_LEDGER_INVALID"]
    assert supporting["source_hash"] is None
    assert supporting["payload"]["records"] == []
    assert "100" not in json.dumps(supporting, sort_keys=True)
    service.close()


class _Headers(dict):
    pass


class _Response:
    def __init__(self, url: str, body: object) -> None:
        self.status = 200
        self._url = url
        self._body = json.dumps(body).encode("utf-8")
        self.headers = _Headers({"Content-Type": "application/json"})
        self.closed = False

    def geturl(self) -> str:
        return self._url

    def read(self, maximum: int) -> bytes:
        return self._body[:maximum]

    def close(self) -> None:
        self.closed = True


class _Opener:
    def __init__(self, responses: dict[str, object]) -> None:
        self.responses = responses

    def open(self, request, *, timeout: float):
        return _Response(request.full_url, self.responses[request.full_url])


class _RawResponse:
    def __init__(
        self,
        url: str,
        body: bytes,
        *,
        content_type: str,
        returned_url: str | None = None,
    ) -> None:
        self.status = 200
        self._url = returned_url or url
        self._body = body
        self.headers = _Headers({"Content-Type": content_type})

    def geturl(self) -> str:
        return self._url

    def read(self, maximum: int) -> bytes:
        return self._body[:maximum]

    def close(self) -> None:
        pass


class _SecOpener:
    def __init__(self, responses: dict[str, tuple[bytes, str]]) -> None:
        self.responses = responses

    def open(self, request, *, timeout: float):
        body, content_type = self.responses[request.full_url]
        return _RawResponse(
            request.full_url,
            body,
            content_type=content_type,
        )


class _FailingOpener:
    def __init__(self) -> None:
        self.calls = 0

    def open(self, request, *, timeout: float):
        self.calls += 1
        raise OSError("not exposed")


def _json_bytes(value: object) -> bytes:
    return json.dumps(value).encode("utf-8")


def _guidance_provider(
    documents: list[tuple[str, str, str]],
) -> SecManagementGuidanceProvider:
    """Build a bounded SEC filing provider from form/date/text triples."""

    cik = "0000320193"
    accessions = [f"0000320193-26-{index:06d}" for index in range(1, len(documents) + 1)]
    submission_url = f"{SEC_SUBMISSIONS_PREFIX}{cik}.json"
    filing_responses: dict[str, tuple[bytes, str]] = {
        submission_url: (
            _json_bytes(
                {
                    "cik": 320193,
                    "filings": {
                        "recent": {
                            "accessionNumber": accessions,
                            "filingDate": [item[1] for item in documents],
                            "reportDate": [item[1] for item in documents],
                            "form": [item[0] for item in documents],
                            "primaryDocument": [
                                f"report-{index}.htm"
                                for index in range(1, len(documents) + 1)
                            ],
                        }
                    },
                }
            ),
            "application/json",
        )
    }
    for index, (_form, _filed, text) in enumerate(documents, start=1):
        accession = accessions[index - 1].replace("-", "")
        base = f"https://www.sec.gov/Archives/edgar/data/320193/{accession}"
        name = f"report-{index}.htm"
        filing_responses[f"{base}/index.json"] = (
            _json_bytes({"directory": {"item": [{"name": name}]}}),
            "application/json",
        )
        filing_responses[f"{base}/{name}"] = (
            f"<html><body>{text}</body></html>".encode("utf-8"),
            "text/html",
        )
    return SecManagementGuidanceProvider(
        ticker_transport=StrictFundamentalsHttpsTransport(
            opener=_Opener(
                {
                    SEC_TICKER_URL: {
                        "fields": ["cik", "name", "ticker", "exchange"],
                        "data": [[320193, "Apple Inc.", "AAPL", "Nasdaq"]],
                    }
                }
            )
        ),
        filing_transport=StrictSecFilingHttpsTransport(
            opener=_SecOpener(filing_responses)
        ),
        clock=lambda: NOW,
        maximum_filings_per_symbol=4,
    )


def _sec_tickers(*symbols: tuple[int, str]) -> dict[str, object]:
    return {
        "fields": ["cik", "name", "ticker", "exchange"],
        "data": [[cik, symbol, symbol, "Nasdaq"] for cik, symbol in symbols],
    }


def _sec_revenue(cik: int, value: int) -> dict[str, object]:
    return {
        "cik": cik,
        "facts": {
            "us-gaap": {
                "Revenues": {
                    "units": {
                        "USD": [
                            {
                                "end": "2026-06-30",
                                "val": value,
                                "accn": f"{cik}-26-000001",
                                "fy": 2026,
                                "fp": "Q2",
                                "form": "10-Q",
                                "filed": "2026-07-30",
                            }
                        ]
                    }
                }
            }
        },
    }


def test_sec_provider_parses_official_actuals_without_guidance_inference() -> None:
    facts_url = f"{SEC_COMPANY_FACTS_PREFIX}0000320193.json"
    opener = _Opener(
        {
            SEC_TICKER_URL: {
                "fields": ["cik", "name", "ticker", "exchange"],
                "data": [[320193, "Apple Inc.", "AAPL", "Nasdaq"]],
            },
            facts_url: {
                "cik": 320193,
                "facts": {
                    "us-gaap": {
                        "RevenueFromContractWithCustomerExcludingAssessedTax": {
                            "units": {
                                "USD": [
                                    {
                                        "end": "2026-06-30",
                                        "val": 100,
                                        "accn": "0000320193-26-000001",
                                        "fy": 2026,
                                        "fp": "Q3",
                                        "form": "10-Q",
                                        "filed": "2026-07-30",
                                    }
                                ]
                            }
                        }
                    }
                },
            },
        }
    )
    provider = SecCompanyFactsProvider(
        transport=StrictFundamentalsHttpsTransport(opener=opener),
        clock=lambda: NOW,
    )

    rows = provider.fetch(("AAPL",))

    assert len(rows) == 1
    assert rows[0].metric is FundamentalMetric.REVENUE
    assert rows[0].value == Decimal("100")
    assert rows[0].observed_at == NOW
    assert all(item.category.value != "GUIDANCE" for item in rows)


def test_sec_provider_emits_original_and_amended_assertions_oldest_first(
    tmp_path,
) -> None:
    facts_url = f"{SEC_COMPANY_FACTS_PREFIX}0000320193.json"
    payload = _sec_revenue(320193, 100)
    values = payload["facts"]["us-gaap"]["Revenues"]["units"]["USD"]
    values.append(
        {
            "end": "2026-06-30",
            "val": 105,
            "accn": "0000320193-26-000002",
            "fy": 2026,
            "fp": "Q2",
            "form": "10-Q/A",
            "filed": "2026-08-01",
        }
    )
    provider = SecCompanyFactsProvider(
        transport=StrictFundamentalsHttpsTransport(
            opener=_Opener(
                {
                    SEC_TICKER_URL: _sec_tickers((320193, "AAPL")),
                    facts_url: payload,
                }
            )
        ),
        clock=lambda: NOW,
    )

    rows = provider.fetch(("AAPL",))

    assert [row.value for row in rows] == [Decimal("100"), Decimal("105")]
    store = FundamentalsStore(tmp_path / "amendment-ledger.sqlite3")
    try:
        results = store.append_many(rows)
        assert [item.record.revision_number for item in results] == [1, 2]
        assert results[1].record.supersedes_hash == rows[0].content_hash
    finally:
        store.close()


def test_sec_guidance_provider_parses_explicit_management_ranges() -> None:
    provider = _guidance_provider(
        [
            (
                "8-K",
                "2026-08-01",
                "For fiscal year 2027, the company expects adjusted EPS of "
                "$5.20 to $5.40. Our fiscal year 2027 outlook projects revenue "
                "to be between $8.0 billion and $8.4 billion.",
            )
        ]
    )

    rows = provider.fetch(("AAPL",))

    assert {row.metric: row.value for row in rows} == {
        FundamentalMetric.GUIDANCE_EPS_LOW: Decimal("5.20"),
        FundamentalMetric.GUIDANCE_EPS_HIGH: Decimal("5.40"),
        FundamentalMetric.GUIDANCE_REVENUE_LOW: Decimal("8000000000.0"),
        FundamentalMetric.GUIDANCE_REVENUE_HIGH: Decimal("8400000000.0"),
    }
    assert all(row.decision_authority == "SUPPORTING_ONLY" for row in rows)


def test_sec_guidance_provider_parses_explicit_quarterly_revenue_range() -> None:
    provider = _guidance_provider(
        [
            (
                "8-K",
                "2026-04-29",
                "CFO Outlook Commentary We expect second quarter 2026 total "
                "revenue to be in the range of $58-61 billion.",
            )
        ]
    )

    rows = provider.fetch(("AAPL",))

    assert {row.metric: row.value for row in rows} == {
        FundamentalMetric.GUIDANCE_REVENUE_LOW: Decimal("58000000000"),
        FundamentalMetric.GUIDANCE_REVENUE_HIGH: Decimal("61000000000"),
    }
    assert {row.fiscal_period for row in rows} == {"Q2-2026"}
    assert {row.period_end for row in rows} == {date(2026, 6, 30)}
    assert {row.basis for row in rows} == {
        "SEC_MANAGEMENT_GUIDANCE_QUARTER_RANGE"
    }


def test_sec_guidance_provider_accepts_explicit_financial_guidance_section() -> None:
    provider = _guidance_provider(
        [
            (
                "8-K",
                "2026-07-30",
                "Third Quarter 2026 Guidance Net sales are expected to be "
                "between $197.0 billion and $202.0 billion.",
            )
        ]
    )

    rows = provider.fetch(("AAPL",))

    assert {row.metric: row.value for row in rows} == {
        FundamentalMetric.GUIDANCE_REVENUE_LOW: Decimal("197000000000.0"),
        FundamentalMetric.GUIDANCE_REVENUE_HIGH: Decimal("202000000000.0"),
    }
    assert {row.fiscal_period for row in rows} == {"Q3-2026"}


def test_sec_guidance_accepts_official_archive_index_json_served_as_html() -> None:
    """SEC Archives labels some exact index.json responses as text/html."""

    provider = _guidance_provider(
        [
            (
                "8-K",
                "2026-08-01",
                "For fiscal year 2027, the company expects adjusted EPS of "
                "$5.20 to $5.40.",
            )
        ]
    )
    opener = provider._filing_transport._opener
    index_url = next(url for url in opener.responses if url.endswith("/index.json"))
    body, _content_type = opener.responses[index_url]
    opener.responses[index_url] = (body, "text/html; charset=UTF-8")

    rows = provider.fetch(("AAPL",))

    assert {row.metric for row in rows} == {
        FundamentalMetric.GUIDANCE_EPS_LOW,
        FundamentalMetric.GUIDANCE_EPS_HIGH,
    }


def test_sec_guidance_accepts_large_equal_length_recent_columns() -> None:
    rows = 20_001
    payload = {
        "cik": 19617,
        "filings": {
            "recent": {
                "accessionNumber": [""] * rows,
                "filingDate": [""] * rows,
                "reportDate": [""] * rows,
                "form": [""] * rows,
                "primaryDocument": [""] * rows,
            }
        },
    }

    from options_copilot.fundamentals.providers import _recent_guidance_filings

    assert _recent_guidance_filings(
        payload,
        expected_cik="0000019617",
        observed_at=NOW,
        limit=3,
    ) == ()


def test_sec_guidance_selects_prefixed_exhibit_99_documents() -> None:
    from options_copilot.fundamentals.providers import _guidance_document_names

    assert _guidance_document_names(
        {
            "directory": {
                "item": [
                    {"name": "issuer-20260729.htm"},
                    {"name": "msft-ex99_1.htm"},
                    {"name": "a8-kex991q32026.htm"},
                    {"name": "unrelated-image.jpg"},
                ]
            }
        },
        primary_document="issuer-20260729.htm",
        limit=4,
    ) == (
        "issuer-20260729.htm",
        "a8-kex991q32026.htm",
        "msft-ex99_1.htm",
    )


@pytest.mark.parametrize(
    "text",
    [
        "For fiscal year 2027, analyst consensus expects EPS of $5.20 to $5.40.",
        "The company expects EPS of $5.20 to $5.40.",
        "For fiscal year 2027, the company expects EPS of approximately $5.30.",
        "For fiscal year 2027, the company described a constructive outlook.",
    ],
)
def test_sec_guidance_provider_rejects_non_management_or_unstructured_claims(
    text: str,
) -> None:
    provider = _guidance_provider([("8-K", "2026-08-01", text)])

    assert provider.fetch(("AAPL",)) == ()


def test_sec_guidance_amendment_is_latest_point_in_time_revision(tmp_path) -> None:
    provider = _guidance_provider(
        [
            (
                "8-K/A",
                "2026-08-03",
                "For fiscal year 2027, the company raises adjusted EPS guidance "
                "from $5.30 to $5.50.",
            ),
            (
                "8-K",
                "2026-08-01",
                "For fiscal year 2027, the company expects adjusted EPS of "
                "$5.20 to $5.40.",
            ),
        ]
    )
    store = FundamentalsStore(tmp_path / "guidance.sqlite3")

    rows = provider.fetch(("AAPL",))
    for row in rows:
        store.append(row)

    current = {
        item.observation.metric: item.observation
        for item in store.current(symbols=("AAPL",), as_of=NOW)
    }
    assert current[FundamentalMetric.GUIDANCE_EPS_LOW].value == Decimal("5.30")
    assert current[FundamentalMetric.GUIDANCE_EPS_HIGH].value == Decimal("5.50")
    assert current[FundamentalMetric.GUIDANCE_EPS_LOW].form == "8-K/A"
    assert len(
        store.revisions(
            current[FundamentalMetric.GUIDANCE_EPS_LOW].series_key
        )
    ) == 2
    store.assert_integrity()
    store.close()


def test_sec_provider_preserves_success_when_later_symbol_request_fails() -> None:
    aapl_url = f"{SEC_COMPANY_FACTS_PREFIX}0000320193.json"
    provider = SecCompanyFactsProvider(
        transport=StrictFundamentalsHttpsTransport(
            opener=_Opener(
                {
                    SEC_TICKER_URL: _sec_tickers(
                        (320193, "AAPL"),
                        (789019, "MSFT"),
                    ),
                    aapl_url: _sec_revenue(320193, 100),
                }
            )
        ),
        clock=lambda: NOW,
    )

    rows = provider.fetch(("AAPL", "MSFT"))

    assert {row.symbol for row in rows} == {"AAPL"}
    assert provider.last_fetch_diagnostics == {
        "requested_count": 2,
        "succeeded_count": 1,
        "failed_count": 1,
        "skipped_count": 0,
        "failed_reason_codes": ["REQUEST_FAILED"],
    }


def test_sec_provider_reports_request_failed_when_all_symbol_requests_fail() -> None:
    provider = SecCompanyFactsProvider(
        transport=StrictFundamentalsHttpsTransport(
            opener=_Opener(
                {
                    SEC_TICKER_URL: _sec_tickers(
                        (320193, "AAPL"),
                        (789019, "MSFT"),
                    )
                }
            )
        ),
        clock=lambda: NOW,
    )

    with pytest.raises(FundamentalProviderError, match="REQUEST_FAILED"):
        provider.fetch(("AAPL", "MSFT"))


class _Secrets:
    def get(self, name: str) -> str | None:
        return "not-logged" if name == "FINNHUB_API_KEY" else None


def test_finnhub_provider_preserves_valid_metric_payload_on_partial_failure() -> None:
    aapl_url = "https://finnhub.io/api/v1/stock/metric?symbol=AAPL&metric=all"
    provider = FinnhubValuationProvider(
        _Secrets(),
        transport=StrictFundamentalsHttpsTransport(
            opener=_Opener(
                {
                    aapl_url: {
                        "metric": {
                            "peTTM": 31.5,
                            "pbAnnual": 8.25,
                            "psTTM": 7.125,
                        }
                    }
                }
            )
        ),
        clock=lambda: NOW,
    )

    rows = provider.fetch(("AAPL", "MSFT"))

    assert {row.metric for row in rows} == {
        FundamentalMetric.PE_TTM,
        FundamentalMetric.PB_ANNUAL,
        FundamentalMetric.PS_TTM,
    }
    assert provider.last_fetch_diagnostics["failed_count"] == 1


class _PartialProvider:
    decision_authority = "SUPPORTING_ONLY"
    last_fetch_diagnostics = {
        "requested_count": 2,
        "succeeded_count": 1,
        "failed_count": 1,
        "skipped_count": 0,
        "failed_reason_codes": ["REQUEST_FAILED"],
    }

    def fetch(self, symbols):
        return (_observation("100"),)


def test_service_records_partial_provider_health_without_dropping_rows(tmp_path) -> None:
    store = FundamentalsStore(tmp_path / "fundamentals.sqlite3")
    service = FundamentalsService(
        store,
        providers=(_PartialProvider(),),
        symbols=("AAPL", "MSFT"),
    )

    payload = service.refresh_once()

    health = payload["provider_health"]["_PartialProvider"]
    assert payload["row_count"] == 1
    assert payload["status"] == "DEGRADED"
    assert "FUNDAMENTALS_CURRENT_REFRESH_DEGRADED" in payload["reason_codes"]
    assert health["status"] == "DEGRADED"
    assert health["reason_code"] == "PARTIAL_SYMBOL_FAILURE"
    assert health["batch_diagnostics"]["failed_count"] == 1
    service.close()


def test_historical_rows_remain_visible_when_all_current_providers_fail(
    tmp_path,
) -> None:
    class _FailingProvider:
        decision_authority = "SUPPORTING_ONLY"

        def fetch(self, _symbols):
            raise FundamentalProviderError("REQUEST_FAILED")

    store = FundamentalsStore(tmp_path / "degraded-current-refresh.sqlite3")
    store.append(_observation("100", observed_at=NOW - timedelta(days=1)))
    service = FundamentalsService(
        store,
        providers=(_FailingProvider(),),
        symbols=("AAPL",),
        clock=lambda: NOW,
    )

    payload = service.refresh_once()
    supporting = service.supporting_evidence("AAPL", as_of=NOW)

    assert payload["status"] == "DEGRADED"
    assert payload["reason_codes"] == [
        "FUNDAMENTALS_CURRENT_REFRESH_DEGRADED"
    ]
    assert payload["row_count"] == 1
    assert payload["rows"][0]["value"] == "100"
    assert supporting["status"] == "DEGRADED"
    assert "FUNDAMENTALS_CURRENT_REFRESH_DEGRADED" in supporting[
        "reason_codes"
    ]
    assert supporting["payload"]["record_count"] == 1
    assert supporting["payload"]["records"][0]["value"] == "100"
    service.close()


def test_service_rotates_broad_research_symbols_without_evicting_core(tmp_path) -> None:
    class _RecordingProvider:
        decision_authority = "SUPPORTING_ONLY"

        def __init__(self) -> None:
            self.calls = []

        def fetch(self, symbols):
            self.calls.append(tuple(symbols))
            return ()

    provider = _RecordingProvider()
    service = FundamentalsService(
        FundamentalsStore(tmp_path / "dynamic-fundamentals.sqlite3"),
        providers=(provider,),
        symbols=("AAPL", "MSFT"),
        maximum_symbols_per_refresh=4,
    )

    active = service.observe_symbols(("XLE", "GLD", "QQQ", "bad symbol!"))
    service.refresh_once()

    assert active[:2] == ("AAPL", "MSFT")
    assert set(active) == {"AAPL", "MSFT", "XLE", "GLD", "QQQ"}
    assert provider.calls[0] == ("AAPL", "MSFT", "XLE", "GLD")
    assert service.payload()["symbols"] == list(active)
    service.close()


def test_supporting_evidence_enqueues_new_candidate_for_next_refresh(tmp_path) -> None:
    service = FundamentalsService(
        FundamentalsStore(tmp_path / "candidate-fundamentals.sqlite3"),
        providers=(),
        symbols=("AAPL",),
    )

    supporting = service.supporting_evidence("IWM", as_of=NOW)

    assert supporting["status"] == "DEGRADED"
    assert service.payload()["symbols"] == ["AAPL", "IWM"]
    service.close()


def test_transport_rejects_non_allowlisted_host_before_network() -> None:
    transport = StrictFundamentalsHttpsTransport(opener=object())

    with pytest.raises(FundamentalProviderError, match="URL_NOT_ALLOWED"):
        transport.get(
            "https://example.com/secrets",
            headers={"Accept": "application/json", "User-Agent": "x"},
            timeout_seconds=8,
            maximum_bytes=1024,
        )


def test_sec_transport_uses_one_bounded_system_proxy_fallback() -> None:
    direct = _FailingOpener()
    proxy = _Opener(
        {
            SEC_TICKER_URL: {
                "fields": ["cik", "name", "ticker", "exchange"],
                "data": [[320193, "Apple Inc.", "AAPL", "Nasdaq"]],
            }
        }
    )
    factory_calls = []
    transport = StrictFundamentalsHttpsTransport(
        opener=direct,
        system_proxy_opener_factory=lambda: factory_calls.append(1) or proxy,
    )

    body = transport.get(
        SEC_TICKER_URL,
        headers={"Accept": "application/json", "User-Agent": "safe"},
        timeout_seconds=8,
        maximum_bytes=1024 * 1024,
    )

    assert json.loads(body)["data"][0][2] == "AAPL"
    assert direct.calls == 1
    assert factory_calls == [1]
    assert dict(transport.last_request_diagnostics) == {
        "status": "READY",
        "route": "SYSTEM_PROXY_FALLBACK",
        "primary_failure_reason": "REQUEST_FAILED",
        "fallback_activated": True,
        "fallback_failure_reason": None,
    }


@pytest.mark.parametrize(
    ("failure_reason", "maximum_bytes"),
    [
        ("CONTENT_TYPE_INVALID", 1024 * 1024),
        ("REDIRECT_FORBIDDEN", 1024 * 1024),
        ("RESPONSE_TOO_LARGE", 8),
    ],
)
def test_sec_proxy_fallback_validation_failure_remains_degraded(
    failure_reason: str,
    maximum_bytes: int,
) -> None:
    response = _Response(
        SEC_TICKER_URL,
        {
            "fields": ["cik", "name", "ticker", "exchange"],
            "data": [[320193, "Apple Inc.", "AAPL", "Nasdaq"]],
        },
    )
    if failure_reason == "CONTENT_TYPE_INVALID":
        response.headers["Content-Type"] = "application/octet-stream"
    elif failure_reason == "REDIRECT_FORBIDDEN":
        response._url = "https://data.sec.gov/submissions/CIK0000320193.json"

    class Proxy:
        def open(self, request, *, timeout: float):
            assert request.full_url == SEC_TICKER_URL
            return response

    transport = StrictFundamentalsHttpsTransport(
        opener=_FailingOpener(),
        system_proxy_opener_factory=Proxy,
    )

    with pytest.raises(FundamentalProviderError, match=f"^{failure_reason}$"):
        transport.get(
            SEC_TICKER_URL,
            headers={"Accept": "application/json", "User-Agent": "safe"},
            timeout_seconds=8,
            maximum_bytes=maximum_bytes,
        )

    assert response.closed is True
    assert dict(transport.last_request_diagnostics) == {
        "status": "DEGRADED",
        "route": "SYSTEM_PROXY_FALLBACK",
        "primary_failure_reason": "REQUEST_FAILED",
        "fallback_activated": True,
        "fallback_failure_reason": failure_reason,
    }


def test_sec_transport_primary_success_never_activates_proxy_fallback() -> None:
    direct = _Opener(
        {
            SEC_TICKER_URL: {
                "fields": ["cik", "name", "ticker", "exchange"],
                "data": [[320193, "Apple Inc.", "AAPL", "Nasdaq"]],
            }
        }
    )
    factory_calls = []
    transport = StrictFundamentalsHttpsTransport(
        opener=direct,
        system_proxy_opener_factory=lambda: factory_calls.append(1),
    )

    transport.get(
        SEC_TICKER_URL,
        headers={"Accept": "application/json", "User-Agent": "safe"},
        timeout_seconds=8,
        maximum_bytes=1024 * 1024,
    )

    assert factory_calls == []
    assert dict(transport.last_request_diagnostics) == {
        "status": "READY",
        "route": "PRIMARY",
        "primary_failure_reason": None,
        "fallback_activated": False,
        "fallback_failure_reason": None,
    }


def test_sec_proxy_fallback_is_request_local_before_finnhub_token_request() -> None:
    finnhub_url = FINNHUB_METRIC_URL + "?symbol=AAPL&metric=all"

    class Direct:
        def __init__(self) -> None:
            self.requests = []

        def open(self, request, *, timeout: float):
            self.requests.append(request)
            if request.full_url == SEC_TICKER_URL:
                raise OSError("direct SEC edge unavailable")
            assert request.full_url == finnhub_url
            return _Response(finnhub_url, {"metric": {}})

    class Proxy:
        def __init__(self) -> None:
            self.requests = []

        def open(self, request, *, timeout: float):
            self.requests.append(request)
            assert request.full_url == SEC_TICKER_URL
            return _Response(
                SEC_TICKER_URL,
                {
                    "fields": ["cik", "name", "ticker", "exchange"],
                    "data": [[320193, "Apple Inc.", "AAPL", "Nasdaq"]],
                },
            )

    direct = Direct()
    proxy = Proxy()
    transport = StrictFundamentalsHttpsTransport(
        opener=direct,
        system_proxy_opener_factory=lambda: proxy,
    )

    transport.get(
        SEC_TICKER_URL,
        headers={"Accept": "application/json", "User-Agent": "safe"},
        timeout_seconds=8,
        maximum_bytes=1024 * 1024,
    )
    fallback_diagnostics = dict(transport.last_request_diagnostics)
    transport.get(
        finnhub_url,
        headers={"Accept": "application/json", "X-Finnhub-Token": "hidden"},
        timeout_seconds=8,
        maximum_bytes=1024,
    )

    assert [request.full_url for request in direct.requests] == [
        SEC_TICKER_URL,
        finnhub_url,
    ]
    assert [request.full_url for request in proxy.requests] == [SEC_TICKER_URL]
    assert direct.requests[-1].get_header("X-finnhub-token") == "hidden"
    assert fallback_diagnostics["fallback_activated"] is True
    assert fallback_diagnostics["primary_failure_reason"] == "REQUEST_FAILED"
    assert dict(transport.last_request_diagnostics) == {
        "status": "READY",
        "route": "PRIMARY",
        "primary_failure_reason": None,
        "fallback_activated": False,
        "fallback_failure_reason": None,
    }


def test_sec_transport_both_paths_fail_with_sanitized_fixed_reason() -> None:
    class BrokenProxy:
        def open(self, *_args, **_kwargs):
            raise OSError("proxy credential secret must not escape")

    transport = StrictFundamentalsHttpsTransport(
        opener=_FailingOpener(),
        system_proxy_opener_factory=BrokenProxy,
    )

    with pytest.raises(
        FundamentalProviderError,
        match="^SEC_PROXY_FALLBACK_FAILED$",
    ):
        transport.get(
            SEC_TICKER_URL,
            headers={"Accept": "application/json", "User-Agent": "safe"},
            timeout_seconds=8,
            maximum_bytes=1024 * 1024,
        )

    diagnostics = dict(transport.last_request_diagnostics)
    assert diagnostics == {
        "status": "DEGRADED",
        "route": "SYSTEM_PROXY_FALLBACK",
        "primary_failure_reason": "REQUEST_FAILED",
        "fallback_activated": True,
        "fallback_failure_reason": "REQUEST_FAILED",
    }
    assert "secret" not in json.dumps(diagnostics).lower()


def test_sec_http_policy_failure_does_not_activate_proxy_fallback() -> None:
    class Forbidden:
        def open(self, request, *, timeout: float):
            raise HTTPError(request.full_url, 403, "secret body", {}, None)

    factory_calls = []
    transport = StrictFundamentalsHttpsTransport(
        opener=Forbidden(),
        system_proxy_opener_factory=lambda: factory_calls.append(1),
    )

    with pytest.raises(FundamentalProviderError, match="^REQUEST_FAILED$"):
        transport.get(
            SEC_TICKER_URL,
            headers={"Accept": "application/json", "User-Agent": "safe"},
            timeout_seconds=8,
            maximum_bytes=1024 * 1024,
        )

    assert factory_calls == []
    assert transport.last_request_diagnostics["fallback_activated"] is False


def test_finnhub_never_uses_windows_system_proxy_fallback() -> None:
    direct = _FailingOpener()
    factory_calls = []
    transport = StrictFundamentalsHttpsTransport(
        opener=direct,
        system_proxy_opener_factory=lambda: factory_calls.append(1),
    )

    with pytest.raises(FundamentalProviderError, match="REQUEST_FAILED"):
        transport.get(
            "https://finnhub.io/api/v1/stock/metric?symbol=AAPL&metric=all",
            headers={"Accept": "application/json", "X-Finnhub-Token": "hidden"},
            timeout_seconds=8,
            maximum_bytes=1024,
        )

    assert factory_calls == []


@pytest.mark.parametrize(
    "proxies",
    [
        {"https": "http://user:secret@proxy.example:8080"},
        {"https": "socks5://proxy.example:1080"},
        {"https": "http://proxy.example"},
        {"http": "http://proxy.example:8080"},
    ],
)
def test_fundamentals_windows_proxy_rejects_unsafe_registry_shapes(
    monkeypatch,
    proxies,
) -> None:
    monkeypatch.setattr(
        "options_copilot.fundamentals.providers.urllib_request.getproxies_registry",
        lambda: proxies,
    )

    assert _windows_system_proxy_opener() is None


@pytest.mark.parametrize(
    "url,document",
    [
        ("https://example.com/Archives/edgar/data/320193/000032019326000001/index.json", False),
        ("https://www.sec.gov/Archives/edgar/data/320193/../secret.txt", True),
        ("https://www.sec.gov/Archives/edgar/data/320193/000032019326000001/report.htm?x=1", True),
    ],
)
def test_sec_filing_transport_rejects_unapproved_urls_before_network(
    url: str,
    document: bool,
) -> None:
    transport = StrictSecFilingHttpsTransport(opener=object())

    with pytest.raises(FundamentalProviderError, match="URL_NOT_ALLOWED"):
        if document:
            transport.get_document(url, maximum_bytes=1024, timeout_seconds=8)
        else:
            transport.get_json(url, maximum_bytes=1024, timeout_seconds=8)


def test_sec_filing_transport_rejects_redirect_and_oversized_body() -> None:
    url = "https://www.sec.gov/Archives/edgar/data/320193/000032019326000001/report.htm"

    class _BadOpener:
        def __init__(self, *, redirect: bool) -> None:
            self.redirect = redirect

        def open(self, request, *, timeout: float):
            return _RawResponse(
                request.full_url,
                b"x" * 1025,
                content_type="text/html",
                returned_url=("https://www.sec.gov/elsewhere.htm" if self.redirect else None),
            )

    with pytest.raises(FundamentalProviderError, match="REDIRECT_FORBIDDEN"):
        StrictSecFilingHttpsTransport(opener=_BadOpener(redirect=True)).get_document(
            url,
            maximum_bytes=1024,
            timeout_seconds=8,
        )
    with pytest.raises(FundamentalProviderError, match="RESPONSE_TOO_LARGE"):
        StrictSecFilingHttpsTransport(opener=_BadOpener(redirect=False)).get_document(
            url,
            maximum_bytes=1024,
            timeout_seconds=8,
        )


def test_service_and_api_project_supporting_only_categories(tmp_path) -> None:
    store = FundamentalsStore(tmp_path / "fundamentals.sqlite3")
    store.append(_observation("100"))
    service = FundamentalsService(store, providers=(), symbols=("AAPL",))
    raw = service.payload()
    services = OptionsCopilotServices(
        health_provider=lambda: {},
        bootstrap_provider=lambda: {},
        candidates_provider=lambda: [],
        positions_provider=lambda: [],
        learning_provider=lambda: {},
        fundamentals_provider=lambda: raw,
    )
    app = create_app(services)
    route = next(item.endpoint for item in app.routes if item.path == "/api/fundamentals")

    payload = asyncio.run(route())

    assert payload["decision_authority"] == "SUPPORTING_ONLY"
    assert payload["approval_eligible"] is False
    assert payload["order_allowed"] is False
    assert payload["row_count"] == 1
    assert payload["categories"]["REVENUE"]["status"] == "AVAILABLE"
    assert payload["categories"]["GUIDANCE"]["status"] == "UNAVAILABLE"
    supporting = service.supporting_evidence("AAPL", as_of=NOW)
    assert supporting["status"] == "DEGRADED"
    assert "FUNDAMENTALS_REFRESH_NOT_OBSERVED" in supporting["reason_codes"]
    assert supporting["payload"]["decision_authority"] == "SUPPORTING_ONLY"
    assert supporting["payload"]["categories"]["REVENUE"]["status"] == (
        "AVAILABLE"
    )
    assert supporting["payload"]["categories"]["GUIDANCE"] == {
        "status": "UNAVAILABLE",
        "record_count": 0,
        "reason_code": "GUIDANCE_STRUCTURED_SOURCE_UNAVAILABLE",
    }
    assert supporting["payload"]["point_in_time_semantics"] == (
        "FIRST_OBSERVED_AT_OR_BEFORE_CUTOFF"
    )
    assert "GUIDANCE_STRUCTURED_SOURCE_UNAVAILABLE" in supporting["reason_codes"]
    service.close()


def test_read_model_excludes_ancient_actual_without_deleting_ledger(tmp_path) -> None:
    store = FundamentalsStore(tmp_path / "stale-actual.sqlite3")
    stale = replace(
        _observation("100"),
        metric=FundamentalMetric.DEBT_CURRENT,
        period_end=date(2013, 3, 31),
        source_id="sec:old-debt",
    )
    store.append(stale)
    service = FundamentalsService(store, providers=(), symbols=("AAPL",))

    supporting = service.supporting_evidence("AAPL", as_of=NOW)

    assert supporting["status"] == "DEGRADED"
    assert supporting["payload"]["records"] == []
    assert len(store.current(symbols=("AAPL",), as_of=NOW)) == 1
    service.close()


def test_api_projects_management_guidance_bounds_and_provider_health(tmp_path) -> None:
    store = FundamentalsStore(tmp_path / "api-guidance.sqlite3")
    provider = _guidance_provider(
        [
            (
                "8-K/A",
                "2026-08-03",
                "For fiscal year 2027, the company raises adjusted EPS guidance "
                "from $5.30 to $5.50.",
            )
        ]
    )
    service = FundamentalsService(
        store,
        providers=(provider,),
        symbols=("AAPL",),
    )
    raw = service.refresh_once()
    app = create_app(
        OptionsCopilotServices(
            health_provider=lambda: {},
            bootstrap_provider=lambda: {},
            candidates_provider=lambda: [],
            positions_provider=lambda: [],
            learning_provider=lambda: {},
            fundamentals_provider=lambda: raw,
        )
    )
    route = next(item.endpoint for item in app.routes if item.path == "/api/fundamentals")

    payload = asyncio.run(route())

    assert {row["metric"] for row in payload["rows"]} == {
        "GUIDANCE_EPS_LOW",
        "GUIDANCE_EPS_HIGH",
    }
    assert payload["categories"]["GUIDANCE"]["status"] == "AVAILABLE"
    assert payload["provider_health"]["SecManagementGuidanceProvider"][
        "status"
    ] == "READY"
    assert payload["decision_authority"] == "SUPPORTING_ONLY"
    assert payload["approval_eligible"] is False
    assert payload["order_allowed"] is False
    service.close()
