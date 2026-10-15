"""Bounded official/credentialed point-in-time fundamental providers."""
from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from html import unescape
import json
import re
from types import MappingProxyType
import urllib.request as urllib_request
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

from options_copilot.storage.canonical import utc_datetime

from .models import FundamentalMetric, FundamentalObservation


SEC_TICKER_URL = "https://www.sec.gov/files/company_tickers_exchange.json"
SEC_COMPANY_FACTS_PREFIX = "https://data.sec.gov/api/xbrl/companyfacts/CIK"
SEC_SUBMISSIONS_PREFIX = "https://data.sec.gov/submissions/CIK"
FINNHUB_METRIC_URL = "https://finnhub.io/api/v1/stock/metric"
SEC_USER_AGENT = "OptionsCopilot/1.0 local-readonly xujie@example.invalid"
MAXIMUM_SEC_TICKER_BYTES = 8 * 1024 * 1024
MAXIMUM_SEC_FACTS_BYTES = 16 * 1024 * 1024
MAXIMUM_FINNHUB_METRIC_BYTES = 1024 * 1024
MAXIMUM_SEC_SUBMISSIONS_BYTES = 8 * 1024 * 1024
MAXIMUM_SEC_FILING_INDEX_BYTES = 2 * 1024 * 1024
MAXIMUM_SEC_FILING_DOCUMENT_BYTES = 4 * 1024 * 1024
_SYMBOL = re.compile(r"^[A-Z][A-Z0-9.\-]{0,14}$")
_CIK = re.compile(r"^[0-9]{10}$")


class FundamentalProviderError(RuntimeError):
    """Fixed-code failure with no untrusted response or credential material."""

    def __init__(self, reason: str) -> None:
        self.reason = str(reason or "PROVIDER_UNAVAILABLE").strip().upper()
        super().__init__(self.reason)


class SecretReader:
    def get(self, name: str) -> str | None: ...


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: ANN001
        return None


class StrictFundamentalsHttpsTransport:
    """Exact-host, no-redirect, bounded JSON transport."""

    def __init__(
        self,
        *,
        opener: object | None = None,
        system_proxy_opener_factory: Callable[[], object | None] | None = None,
    ) -> None:
        self._opener = opener or build_opener(ProxyHandler(), _NoRedirect())
        self._system_proxy_opener_factory = (
            system_proxy_opener_factory
            if system_proxy_opener_factory is not None
            else (None if opener is not None else _windows_system_proxy_opener)
        )
        self.last_request_diagnostics: Mapping[str, object] = MappingProxyType(
            {
                "status": "NOT_REQUESTED",
                "route": "NONE",
                "primary_failure_reason": None,
                "fallback_activated": False,
                "fallback_failure_reason": None,
            }
        )

    def get(
        self,
        url: str,
        *,
        headers: Mapping[str, str],
        timeout_seconds: float,
        maximum_bytes: int,
    ) -> bytes:
        checked_url = _allowed_url(url)
        if (
            isinstance(timeout_seconds, bool)
            or not isinstance(timeout_seconds, (int, float))
            or not 1.0 <= float(timeout_seconds) <= 15.0
        ):
            raise FundamentalProviderError("REQUEST_TIMEOUT_INVALID")
        if (
            isinstance(maximum_bytes, bool)
            or not isinstance(maximum_bytes, int)
            or not 1 <= maximum_bytes <= MAXIMUM_SEC_FACTS_BYTES
        ):
            raise FundamentalProviderError("RESPONSE_LIMIT_INVALID")
        expected_headers = (
            {"Accept", "User-Agent"}
            if urlsplit(checked_url).hostname in {"www.sec.gov", "data.sec.gov"}
            else {"Accept", "X-Finnhub-Token"}
        )
        if not isinstance(headers, Mapping) or set(headers) != expected_headers:
            raise FundamentalProviderError("HEADERS_NOT_ALLOWED")
        if headers.get("Accept") != "application/json":
            raise FundamentalProviderError("HEADERS_NOT_ALLOWED")
        clean_headers = dict(headers)
        clean_headers["Accept-Encoding"] = "identity"
        request = Request(checked_url, headers=clean_headers, method="GET")
        response = _open_with_sec_proxy_fallback(
            self,
            request,
            timeout_seconds=float(timeout_seconds),
            allow_fallback=_system_proxy_sec_route_allowed(checked_url),
        )
        try:
            if int(getattr(response, "status", 200)) != 200:
                raise FundamentalProviderError("HTTP_STATUS_INVALID")
            if str(response.geturl()) != checked_url:
                raise FundamentalProviderError("REDIRECT_FORBIDDEN")
            content_type = str(response.headers.get("Content-Type") or "").split(";", 1)[0].strip().lower()
            if content_type not in {"application/json", "text/json"}:
                raise FundamentalProviderError("CONTENT_TYPE_INVALID")
            encoding = str(response.headers.get("Content-Encoding") or "identity").strip().lower()
            if encoding not in {"", "identity"}:
                raise FundamentalProviderError("CONTENT_ENCODING_INVALID")
            raw_length = response.headers.get("Content-Length")
            if raw_length not in {None, ""}:
                try:
                    declared = int(str(raw_length))
                except (TypeError, ValueError):
                    raise FundamentalProviderError("RESPONSE_LENGTH_INVALID") from None
                if declared < 0 or declared > maximum_bytes:
                    raise FundamentalProviderError("RESPONSE_TOO_LARGE")
            body = response.read(maximum_bytes + 1)
            if not isinstance(body, bytes) or not body:
                raise FundamentalProviderError("RESPONSE_BODY_INVALID")
            if len(body) > maximum_bytes:
                raise FundamentalProviderError("RESPONSE_TOO_LARGE")
            _finish_transport_validation(self)
            return body
        except FundamentalProviderError as exc:
            _finish_transport_validation(self, failure_reason=exc.reason)
            raise
        except Exception:
            _finish_transport_validation(self, failure_reason="RESPONSE_INVALID")
            raise FundamentalProviderError("RESPONSE_INVALID") from None
        finally:
            try:
                response.close()
            except Exception:
                pass


class StrictSecFilingHttpsTransport:
    """Read bounded SEC submissions, filing indexes, and filing documents."""

    def __init__(
        self,
        *,
        opener: object | None = None,
        system_proxy_opener_factory: Callable[[], object | None] | None = None,
    ) -> None:
        self._opener = opener or build_opener(ProxyHandler(), _NoRedirect())
        self._system_proxy_opener_factory = (
            system_proxy_opener_factory
            if system_proxy_opener_factory is not None
            else (None if opener is not None else _windows_system_proxy_opener)
        )
        self.last_request_diagnostics: Mapping[str, object] = MappingProxyType(
            {
                "status": "NOT_REQUESTED",
                "route": "NONE",
                "primary_failure_reason": None,
                "fallback_activated": False,
                "fallback_failure_reason": None,
            }
        )

    def get_json(self, url: str, *, maximum_bytes: int, timeout_seconds: float) -> bytes:
        return self._get(
            url,
            maximum_bytes=maximum_bytes,
            timeout_seconds=timeout_seconds,
            accept="application/json",
            # SEC's exact Archives ``index.json`` endpoint currently serves a
            # JSON object with ``text/html`` on some filings.  The URL is still
            # fixed-host, suffix-validated, redirect-free, size-bounded, and
            # parsed as JSON by the caller; filing documents do not inherit
            # this compatibility allowance.
            content_types={"application/json", "text/json", "text/html"},
            document=False,
        )

    def get_document(
        self,
        url: str,
        *,
        maximum_bytes: int,
        timeout_seconds: float,
    ) -> bytes:
        return self._get(
            url,
            maximum_bytes=maximum_bytes,
            timeout_seconds=timeout_seconds,
            accept="text/html, text/plain;q=0.9",
            content_types={"text/html", "text/plain"},
            document=True,
        )

    def _get(
        self,
        url: str,
        *,
        maximum_bytes: int,
        timeout_seconds: float,
        accept: str,
        content_types: set[str],
        document: bool,
    ) -> bytes:
        checked_url = _allowed_sec_filing_url(url, document=document)
        if (
            isinstance(timeout_seconds, bool)
            or not isinstance(timeout_seconds, (int, float))
            or not 1.0 <= float(timeout_seconds) <= 15.0
        ):
            raise FundamentalProviderError("REQUEST_TIMEOUT_INVALID")
        if (
            isinstance(maximum_bytes, bool)
            or not isinstance(maximum_bytes, int)
            or not 1 <= maximum_bytes <= MAXIMUM_SEC_SUBMISSIONS_BYTES
        ):
            raise FundamentalProviderError("RESPONSE_LIMIT_INVALID")
        request = Request(
            checked_url,
            headers={
                "Accept": accept,
                "Accept-Encoding": "identity",
                "User-Agent": SEC_USER_AGENT,
            },
            method="GET",
        )
        response = _open_with_sec_proxy_fallback(
            self,
            request,
            timeout_seconds=float(timeout_seconds),
            allow_fallback=_system_proxy_sec_route_allowed(checked_url),
        )
        try:
            if int(getattr(response, "status", 200)) != 200:
                raise FundamentalProviderError("HTTP_STATUS_INVALID")
            if str(response.geturl()) != checked_url:
                raise FundamentalProviderError("REDIRECT_FORBIDDEN")
            content_type = str(response.headers.get("Content-Type") or "").split(
                ";", 1
            )[0].strip().lower()
            if content_type not in content_types:
                raise FundamentalProviderError("CONTENT_TYPE_INVALID")
            encoding = str(
                response.headers.get("Content-Encoding") or "identity"
            ).strip().lower()
            if encoding not in {"", "identity"}:
                raise FundamentalProviderError("CONTENT_ENCODING_INVALID")
            body = response.read(maximum_bytes + 1)
            if not isinstance(body, bytes) or not body:
                raise FundamentalProviderError("RESPONSE_BODY_INVALID")
            if len(body) > maximum_bytes:
                raise FundamentalProviderError("RESPONSE_TOO_LARGE")
            _finish_transport_validation(self)
            return body
        except FundamentalProviderError as exc:
            _finish_transport_validation(self, failure_reason=exc.reason)
            raise
        except Exception:
            _finish_transport_validation(self, failure_reason="RESPONSE_INVALID")
            raise FundamentalProviderError("RESPONSE_INVALID") from None
        finally:
            try:
                response.close()
            except Exception:
                pass


def _open_with_sec_proxy_fallback(
    transport: object,
    request: Request,
    *,
    timeout_seconds: float,
    allow_fallback: bool,
) -> object:
    """Retry one fixed SEC request through a credential-free system proxy."""

    try:
        response = transport._opener.open(request, timeout=timeout_seconds)
        _set_transport_diagnostics(
            transport,
            status="DEGRADED",
            route="PRIMARY",
            primary_failure_reason="RESPONSE_VALIDATION_PENDING",
            fallback_activated=False,
            fallback_failure_reason=None,
        )
        return response
    except TimeoutError:
        reason = "REQUEST_TIMEOUT"
        qualified = True
    except HTTPError as exc:
        reason = (
            "RATE_LIMITED"
            if int(getattr(exc, "code", 0)) == 429
            else "REQUEST_FAILED"
        )
        qualified = False
    except (URLError, OSError):
        reason = "REQUEST_FAILED"
        qualified = True
    except Exception:
        reason = "REQUEST_FAILED"
        qualified = False
    factory = getattr(transport, "_system_proxy_opener_factory", None)
    if not allow_fallback or not qualified or not callable(factory):
        _set_transport_diagnostics(
            transport,
            status="DEGRADED",
            route="PRIMARY",
            primary_failure_reason=reason,
            fallback_activated=False,
            fallback_failure_reason=None,
        )
        raise FundamentalProviderError(reason) from None
    opener = factory()
    if opener is None:
        _set_transport_diagnostics(
            transport,
            status="DEGRADED",
            route="PRIMARY",
            primary_failure_reason=reason,
            fallback_activated=False,
            fallback_failure_reason="SYSTEM_PROXY_UNAVAILABLE",
        )
        raise FundamentalProviderError(reason) from None
    try:
        response = opener.open(request, timeout=timeout_seconds)
        _set_transport_diagnostics(
            transport,
            status="DEGRADED",
            route="SYSTEM_PROXY_FALLBACK",
            primary_failure_reason=reason,
            fallback_activated=True,
            fallback_failure_reason="RESPONSE_VALIDATION_PENDING",
        )
        return response
    except TimeoutError:
        fallback_reason = "REQUEST_TIMEOUT"
    except HTTPError as exc:
        code = int(getattr(exc, "code", 0))
        fallback_reason = "RATE_LIMITED" if code == 429 else "REQUEST_FAILED"
    except (URLError, OSError):
        fallback_reason = "REQUEST_FAILED"
    except Exception:
        fallback_reason = "REQUEST_FAILED"
    _set_transport_diagnostics(
        transport,
        status="DEGRADED",
        route="SYSTEM_PROXY_FALLBACK",
        primary_failure_reason=reason,
        fallback_activated=True,
        fallback_failure_reason=fallback_reason,
    )
    raise FundamentalProviderError("SEC_PROXY_FALLBACK_FAILED") from None


def _finish_transport_validation(
    transport: object,
    *,
    failure_reason: str | None = None,
) -> None:
    """Commit READY only after the complete bounded response validation."""

    current = getattr(transport, "last_request_diagnostics", {})
    diagnostics = current if isinstance(current, Mapping) else {}
    route = str(diagnostics.get("route") or "NONE").strip().upper()
    fallback_activated = diagnostics.get("fallback_activated") is True
    primary_failure_reason = _fixed_diagnostic_reason(
        diagnostics.get("primary_failure_reason")
    )
    checked_failure = _fixed_diagnostic_reason(failure_reason)
    if checked_failure is None:
        _set_transport_diagnostics(
            transport,
            status="READY",
            route=route,
            primary_failure_reason=(
                primary_failure_reason if fallback_activated else None
            ),
            fallback_activated=fallback_activated,
            fallback_failure_reason=None,
        )
        return
    _set_transport_diagnostics(
        transport,
        status="DEGRADED",
        route=route,
        primary_failure_reason=(
            primary_failure_reason if fallback_activated else checked_failure
        ),
        fallback_activated=fallback_activated,
        fallback_failure_reason=(checked_failure if fallback_activated else None),
    )


def _fixed_diagnostic_reason(value: object) -> str | None:
    text = str(value or "").strip().upper()
    if not text or len(text) > 64 or not re.fullmatch(r"[A-Z][A-Z0-9_]*", text):
        return None
    return text


def _set_transport_diagnostics(
    transport: object,
    *,
    status: str,
    route: str,
    primary_failure_reason: str | None,
    fallback_activated: bool,
    fallback_failure_reason: str | None,
) -> None:
    transport.last_request_diagnostics = MappingProxyType(
        {
            "status": status,
            "route": route,
            "primary_failure_reason": primary_failure_reason,
            "fallback_activated": fallback_activated,
            "fallback_failure_reason": fallback_failure_reason,
        }
    )


def _system_proxy_sec_route_allowed(url: str) -> bool:
    """Limit fallback to the exact SEC routes already accepted by transports."""

    try:
        parsed = urlsplit(url)
    except (TypeError, ValueError):
        return False
    if parsed.scheme != "https" or parsed.query or parsed.fragment:
        return False
    if url == SEC_TICKER_URL:
        return True
    if parsed.netloc == "data.sec.gov" and (
        re.fullmatch(r"/api/xbrl/companyfacts/CIK[0-9]{10}\.json", parsed.path)
        or re.fullmatch(r"/submissions/CIK[0-9]{10}\.json", parsed.path)
    ):
        return True
    archive_prefix = r"/Archives/edgar/data/[1-9][0-9]{0,9}/[0-9]{18}/"
    if parsed.netloc != "www.sec.gov":
        return False
    return bool(
        re.fullmatch(archive_prefix + r"index\.json", parsed.path)
        or re.fullmatch(
            archive_prefix
            + r"[A-Za-z0-9][A-Za-z0-9_.-]{0,179}\.(?:htm|html|txt)",
            parsed.path,
            re.I,
        )
    )


def _windows_system_proxy_opener() -> object | None:
    """Build one no-credential Windows registry proxy for fixed SEC hosts."""

    reader = getattr(urllib_request, "getproxies_registry", None)
    if not callable(reader):
        return None
    try:
        raw = reader()
    except Exception:
        return None
    if not isinstance(raw, Mapping):
        return None
    proxies: dict[str, str] = {}
    for scheme in ("http", "https"):
        value = str(raw.get(scheme) or "").strip()
        if not value or len(value) > 2048:
            continue
        parsed = urlsplit(value)
        try:
            port = parsed.port
        except ValueError:
            return None
        if (
            parsed.scheme.lower() not in {"http", "https"}
            or not parsed.hostname
            or port is None
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
            or parsed.path not in {"", "/"}
        ):
            return None
        proxies[scheme] = value
    if "https" not in proxies:
        return None
    return build_opener(ProxyHandler(proxies), _NoRedirect())


class SecCompanyFactsProvider:
    """Read actual company fundamentals from official SEC companyfacts."""

    decision_authority = "SUPPORTING_ONLY"

    def __init__(
        self,
        *,
        transport: StrictFundamentalsHttpsTransport | None = None,
        clock: Callable[[], datetime] | None = None,
        timeout_seconds: float = 8.0,
    ) -> None:
        self._transport = transport or StrictFundamentalsHttpsTransport()
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._timeout = timeout_seconds
        self._mapping: dict[str, str] | None = None
        self.last_fetch_diagnostics: Mapping[str, object] = {}

    def fetch(self, symbols: Sequence[str]) -> tuple[FundamentalObservation, ...]:
        observed_at = utc_datetime(self._clock(), field="SEC fundamentals clock")
        requested = _symbols(symbols)
        mapping = self._ticker_mapping()
        rows: list[FundamentalObservation] = []
        succeeded = 0
        failed_reasons: list[str] = []
        unmapped = 0
        for symbol in requested:
            cik = mapping.get(symbol)
            if cik is None:
                unmapped += 1
                continue
            url = f"{SEC_COMPANY_FACTS_PREFIX}{cik}.json"
            try:
                body = self._transport.get(
                    url,
                    headers={"Accept": "application/json", "User-Agent": SEC_USER_AGENT},
                    timeout_seconds=self._timeout,
                    maximum_bytes=MAXIMUM_SEC_FACTS_BYTES,
                )
                payload = _json_object(body)
                observations = _sec_observations(
                    payload,
                    symbol=symbol,
                    expected_cik=cik,
                    source_url=url,
                    observed_at=observed_at,
                )
            except FundamentalProviderError as exc:
                failed_reasons.append(exc.reason)
                continue
            except Exception:
                failed_reasons.append("PROVIDER_UNAVAILABLE")
                continue
            succeeded += 1
            rows.extend(observations)
        self.last_fetch_diagnostics = _fetch_diagnostics(
            requested_count=len(requested),
            succeeded_count=succeeded,
            failed_reasons=failed_reasons,
            skipped_count=unmapped,
        )
        if failed_reasons and succeeded == 0:
            raise FundamentalProviderError("REQUEST_FAILED")
        return tuple(rows)

    def _ticker_mapping(self) -> Mapping[str, str]:
        if self._mapping is not None:
            return self._mapping
        body = self._transport.get(
            SEC_TICKER_URL,
            headers={"Accept": "application/json", "User-Agent": SEC_USER_AGENT},
            timeout_seconds=self._timeout,
            maximum_bytes=MAXIMUM_SEC_TICKER_BYTES,
        )
        self._mapping = _sec_ticker_mapping(_json_object(body))
        return self._mapping


class SecManagementGuidanceProvider:
    """Extract explicit future EPS/revenue ranges from official SEC filings.

    Only current 8-K/8-K/A filings and their bounded HTML/plain-text exhibits
    are considered.  The parser requires management-forward language, an
    explicit future fiscal year, and both ends of a numeric range.  It never
    promotes analyst consensus, estimates, or an unbounded narrative outlook.
    """

    decision_authority = "SUPPORTING_ONLY"

    def __init__(
        self,
        *,
        ticker_transport: StrictFundamentalsHttpsTransport | None = None,
        filing_transport: StrictSecFilingHttpsTransport | None = None,
        clock: Callable[[], datetime] | None = None,
        timeout_seconds: float = 8.0,
        maximum_filings_per_symbol: int = 3,
        maximum_documents_per_filing: int = 4,
    ) -> None:
        self._ticker_transport = ticker_transport or StrictFundamentalsHttpsTransport()
        self._filing_transport = filing_transport or StrictSecFilingHttpsTransport()
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._timeout = timeout_seconds
        if (
            isinstance(maximum_filings_per_symbol, bool)
            or not isinstance(maximum_filings_per_symbol, int)
            or not 1 <= maximum_filings_per_symbol <= 4
        ):
            raise ValueError("maximum_filings_per_symbol must be between 1 and 4")
        if (
            isinstance(maximum_documents_per_filing, bool)
            or not isinstance(maximum_documents_per_filing, int)
            or not 1 <= maximum_documents_per_filing <= 6
        ):
            raise ValueError("maximum_documents_per_filing must be between 1 and 6")
        self._maximum_filings = maximum_filings_per_symbol
        self._maximum_documents = maximum_documents_per_filing
        self._mapping: dict[str, str] | None = None
        self.last_fetch_diagnostics: Mapping[str, object] = {}

    def fetch(self, symbols: Sequence[str]) -> tuple[FundamentalObservation, ...]:
        observed_at = utc_datetime(self._clock(), field="SEC guidance clock")
        requested = _symbols(symbols)
        mapping = self._ticker_mapping()
        rows: list[FundamentalObservation] = []
        succeeded = 0
        failed_reasons: list[str] = []
        unmapped = 0
        for symbol in requested:
            cik = mapping.get(symbol)
            if cik is None:
                unmapped += 1
                continue
            try:
                rows.extend(
                    self._fetch_symbol(
                        symbol=symbol,
                        cik=cik,
                        observed_at=observed_at,
                    )
                )
            except FundamentalProviderError as exc:
                failed_reasons.append(exc.reason)
                continue
            except Exception:
                failed_reasons.append("PROVIDER_UNAVAILABLE")
                continue
            succeeded += 1
        self.last_fetch_diagnostics = _fetch_diagnostics(
            requested_count=len(requested),
            succeeded_count=succeeded,
            failed_reasons=failed_reasons,
            skipped_count=unmapped,
        )
        if failed_reasons and succeeded == 0:
            raise FundamentalProviderError("REQUEST_FAILED")
        return tuple(rows)

    def _fetch_symbol(
        self,
        *,
        symbol: str,
        cik: str,
        observed_at: datetime,
    ) -> tuple[FundamentalObservation, ...]:
        submissions_url = f"{SEC_SUBMISSIONS_PREFIX}{cik}.json"
        submissions = _json_object(
            self._filing_transport.get_json(
                submissions_url,
                maximum_bytes=MAXIMUM_SEC_SUBMISSIONS_BYTES,
                timeout_seconds=self._timeout,
            )
        )
        filings = _recent_guidance_filings(
            submissions,
            expected_cik=cik,
            observed_at=observed_at,
            limit=self._maximum_filings,
        )
        output: list[FundamentalObservation] = []
        seen: set[tuple[FundamentalMetric, Decimal, date]] = set()
        for filing in filings:
            accession = filing["accession"].replace("-", "")
            filing_date = date.fromisoformat(filing["filing_date"])
            base = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession}"
            index_url = f"{base}/index.json"
            index = _json_object(
                self._filing_transport.get_json(
                    index_url,
                    maximum_bytes=MAXIMUM_SEC_FILING_INDEX_BYTES,
                    timeout_seconds=self._timeout,
                )
            )
            documents = _guidance_document_names(
                index,
                primary_document=filing["primary_document"],
                limit=self._maximum_documents,
            )
            for document_name in documents:
                document_url = f"{base}/{document_name}"
                body = self._filing_transport.get_document(
                    document_url,
                    maximum_bytes=MAXIMUM_SEC_FILING_DOCUMENT_BYTES,
                    timeout_seconds=self._timeout,
                )
                text = _filing_text(body)
                for observation in _guidance_observations(
                    text,
                    symbol=symbol,
                    cik=cik,
                    accession=filing["accession"],
                    filing_date=filing_date,
                    form=filing["form"],
                    source_url=document_url,
                    observed_at=observed_at,
                ):
                    key = (
                        observation.metric,
                        observation.value,
                        observation.period_end,
                    )
                    if key not in seen:
                        seen.add(key)
                        output.append(observation)
        return tuple(output)

    def _ticker_mapping(self) -> Mapping[str, str]:
        if self._mapping is not None:
            return self._mapping
        body = self._ticker_transport.get(
            SEC_TICKER_URL,
            headers={"Accept": "application/json", "User-Agent": SEC_USER_AGENT},
            timeout_seconds=self._timeout,
            maximum_bytes=MAXIMUM_SEC_TICKER_BYTES,
        )
        self._mapping = _sec_ticker_mapping(_json_object(body))
        return self._mapping


class FinnhubValuationProvider:
    """Read observed valuation ratios without treating them as hard facts."""

    decision_authority = "SUPPORTING_ONLY"

    def __init__(
        self,
        secrets: SecretReader,
        *,
        transport: StrictFundamentalsHttpsTransport | None = None,
        clock: Callable[[], datetime] | None = None,
        timeout_seconds: float = 8.0,
    ) -> None:
        self._secrets = secrets
        self._transport = transport or StrictFundamentalsHttpsTransport()
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._timeout = timeout_seconds
        self.last_fetch_diagnostics: Mapping[str, object] = {}

    def fetch(self, symbols: Sequence[str]) -> tuple[FundamentalObservation, ...]:
        token = self._secrets.get("FINNHUB_API_KEY")
        if token is None:
            raise FundamentalProviderError("CREDENTIAL_NOT_CONFIGURED")
        observed_at = utc_datetime(self._clock(), field="Finnhub valuation clock")
        rows: list[FundamentalObservation] = []
        requested = _symbols(symbols)
        succeeded = 0
        failed_reasons: list[str] = []
        for symbol in requested:
            url = FINNHUB_METRIC_URL + "?" + urlencode({"symbol": symbol, "metric": "all"})
            try:
                body = self._transport.get(
                    url,
                    headers={"Accept": "application/json", "X-Finnhub-Token": token},
                    timeout_seconds=self._timeout,
                    maximum_bytes=MAXIMUM_FINNHUB_METRIC_BYTES,
                )
                payload = _json_object(body)
            except FundamentalProviderError as exc:
                failed_reasons.append(exc.reason)
                continue
            except Exception:
                failed_reasons.append("PROVIDER_UNAVAILABLE")
                continue
            succeeded += 1
            metric_payload = payload.get("metric")
            if not isinstance(metric_payload, Mapping):
                continue
            for field, metric in (
                ("peTTM", FundamentalMetric.PE_TTM),
                ("pbAnnual", FundamentalMetric.PB_ANNUAL),
                ("psTTM", FundamentalMetric.PS_TTM),
            ):
                value = _decimal_or_none(metric_payload.get(field))
                if value is None:
                    continue
                rows.append(
                    FundamentalObservation(
                        symbol=symbol,
                        metric=metric,
                        value=value,
                        unit="RATIO",
                        basis="PROVIDER_METRIC",
                        period_end=observed_at.date(),
                        fiscal_period="OBSERVED",
                        source="FINNHUB",
                        source_id=f"finnhub:{symbol}:{field}",
                        source_url=url.split("?", 1)[0],
                        source_filed_date=None,
                        observed_at=observed_at,
                        tag=field,
                    )
                )
        self.last_fetch_diagnostics = _fetch_diagnostics(
            requested_count=len(requested),
            succeeded_count=succeeded,
            failed_reasons=failed_reasons,
        )
        if failed_reasons and succeeded == 0:
            raise FundamentalProviderError("REQUEST_FAILED")
        return tuple(rows)


def _fetch_diagnostics(
    *,
    requested_count: int,
    succeeded_count: int,
    failed_reasons: Sequence[str],
    skipped_count: int = 0,
) -> dict[str, object]:
    """Expose bounded fixed-code batch health without response or credential data."""

    return {
        "requested_count": requested_count,
        "succeeded_count": succeeded_count,
        "failed_count": len(failed_reasons),
        "skipped_count": skipped_count,
        "failed_reason_codes": sorted(set(failed_reasons)),
    }


def _sec_ticker_mapping(payload: Mapping[str, object]) -> dict[str, str]:
    if payload.get("fields") != ["cik", "name", "ticker", "exchange"]:
        raise FundamentalProviderError("TICKER_MAPPING_INVALID")
    data = payload.get("data")
    if not isinstance(data, list) or not data or len(data) > 100_000:
        raise FundamentalProviderError("TICKER_MAPPING_INVALID")
    mapping: dict[str, str] = {}
    ambiguous: set[str] = set()
    for row in data:
        if not isinstance(row, list) or len(row) != 4:
            raise FundamentalProviderError("TICKER_MAPPING_INVALID")
        try:
            cik = f"{int(row[0]):010d}"
            symbol = str(row[2]).strip().upper().replace("-", ".")
        except Exception:
            raise FundamentalProviderError("TICKER_MAPPING_INVALID") from None
        if _CIK.fullmatch(cik) is None or _SYMBOL.fullmatch(symbol) is None:
            continue
        if symbol in mapping and mapping[symbol] != cik:
            ambiguous.add(symbol)
        mapping[symbol] = cik
    for symbol in ambiguous:
        mapping.pop(symbol, None)
    return mapping


def _recent_guidance_filings(
    payload: Mapping[str, object],
    *,
    expected_cik: str,
    observed_at: datetime,
    limit: int,
) -> tuple[dict[str, str], ...]:
    try:
        cik = f"{int(payload.get('cik')):010d}"
    except Exception:
        raise FundamentalProviderError("SUBMISSIONS_INVALID") from None
    filings = payload.get("filings")
    recent = filings.get("recent") if isinstance(filings, Mapping) else None
    if cik != expected_cik or not isinstance(recent, Mapping):
        raise FundamentalProviderError("SUBMISSIONS_IDENTITY_MISMATCH")
    required = (
        "accessionNumber",
        "filingDate",
        "reportDate",
        "form",
        "primaryDocument",
    )
    columns = [recent.get(name) for name in required]
    if any(not isinstance(column, list) for column in columns):
        raise FundamentalProviderError("SUBMISSIONS_INVALID")
    lengths = {len(column) for column in columns if isinstance(column, list)}
    if len(lengths) != 1 or not lengths or next(iter(lengths)) > 100_000:
        raise FundamentalProviderError("SUBMISSIONS_INVALID")
    rows: list[dict[str, str]] = []
    for values in zip(*columns):
        accession, filed, report, form, primary = (str(item or "").strip() for item in values)
        if form.upper() not in {"8-K", "8-K/A"}:
            continue
        try:
            filed_date = date.fromisoformat(filed)
        except ValueError:
            continue
        if datetime.combine(filed_date, datetime.min.time(), tzinfo=timezone.utc) > observed_at:
            continue
        if re.fullmatch(r"[0-9]{10}-[0-9]{2}-[0-9]{6}", accession) is None:
            continue
        if re.fullmatch(r"[A-Za-z0-9_.-]{1,180}\.(?:htm|html|txt)", primary, re.I) is None:
            continue
        try:
            report_date = date.fromisoformat(report)
        except ValueError:
            report_date = filed_date
        rows.append(
            {
                "accession": accession,
                "filing_date": filed_date.isoformat(),
                "report_date": report_date.isoformat(),
                "form": form.upper(),
                "primary_document": primary,
            }
        )
        if len(rows) >= limit:
            break
    # SEC recent submissions are newest-first.  Append older assertions before
    # later amendments so the immutable store's supersession chain ends on the
    # most recently filed management guidance.
    rows.sort(key=lambda item: (item["filing_date"], item["accession"]))
    return tuple(rows)


def _guidance_document_names(
    payload: Mapping[str, object],
    *,
    primary_document: str,
    limit: int,
) -> tuple[str, ...]:
    directory = payload.get("directory")
    items = directory.get("item") if isinstance(directory, Mapping) else None
    if not isinstance(items, list) or len(items) > 2_000:
        raise FundamentalProviderError("FILING_INDEX_INVALID")
    candidates: list[tuple[int, str]] = []
    for item in items:
        if not isinstance(item, Mapping):
            continue
        name = str(item.get("name") or "").strip()
        if re.fullmatch(r"[A-Za-z0-9_.-]{1,180}\.(?:htm|html|txt)", name, re.I) is None:
            continue
        lowered = name.lower()
        # Issuers commonly prefix Exhibit 99 filenames (for example,
        # ``msft-ex99_1.htm`` or ``a8-kex991q3.htm``).  Match the exhibit token
        # anywhere in the already bounded SEC index name, while continuing to
        # exclude unrelated attachments.
        is_exhibit_99 = re.search(r"ex(?:hibit)?[-_]?99", lowered) is not None
        priority = 0 if name == primary_document else 1 if is_exhibit_99 else 2
        if priority < 2:
            candidates.append((priority, name))
    return tuple(name for _priority, name in sorted(set(candidates))[:limit])


def _filing_text(body: bytes) -> str:
    if not isinstance(body, bytes) or not body:
        raise FundamentalProviderError("FILING_DOCUMENT_INVALID")
    try:
        text = body.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        text = body.decode("windows-1252", errors="strict")
    text = re.sub(r"(?is)<(?:script|style)\b[^>]*>.*?</(?:script|style)>", " ", text)
    text = re.sub(r"(?s)<[^>]{0,2000}>", " ", text)
    return " ".join(unescape(text).replace("\xa0", " ").split())


_GUIDANCE_FORWARD = re.compile(
    r"\b(?:guidance|outlook|expects?|anticipates?|forecasts?|projects?|raises?|lowers?)\b",
    re.I,
)
_GUIDANCE_MANAGEMENT = re.compile(
    r"\b(?:we|our|management|the\s+company|company)\b.{0,100}"
    r"\b(?:guidance|outlook|expects?|anticipates?|forecasts?|projects?|raises?|lowers?)\b",
    re.I,
)
_GUIDANCE_THIRD_PARTY = re.compile(
    r"\b(?:analysts?|consensus|wall\s+street|street\s+estimate|third[- ]party\s+estimate)\b",
    re.I,
)
_GUIDANCE_YEAR = re.compile(r"\b(?:fiscal(?:\s+year)?|full[- ]year|FY)\s*(20[0-9]{2})\b", re.I)
_GUIDANCE_QUARTER = re.compile(
    r"\b(?:fiscal\s+)?(?:"
    r"(first|second|third|fourth)\s+quarter|Q([1-4])"
    r")\s*(20[0-9]{2})\b",
    re.I,
)
_GUIDANCE_SECTION = re.compile(
    r"\b(?:financial\s+guidance|CFO\s+outlook\s+commentary|"
    r"(?:first|second|third|fourth)\s+quarter\s+20[0-9]{2}\s+guidance)\b",
    re.I,
)
_GUIDANCE_RANGE = re.compile(
    r"(?:of\s+)?(?:between|range\s+of|from|of|to\s+be)\s*"
    r"\$([0-9][0-9,]*(?:\.[0-9]+)?)\s*(million|billion)?\s*"
    r"(?:to|and|through|[-–—])\s*\$?([0-9][0-9,]*(?:\.[0-9]+)?)\s*(million|billion)?",
    re.I,
)


def _guidance_observations(
    text: str,
    *,
    symbol: str,
    cik: str,
    accession: str,
    filing_date: date,
    form: str,
    source_url: str,
    observed_at: datetime,
) -> tuple[FundamentalObservation, ...]:
    output: list[FundamentalObservation] = []
    segments = re.split(r"(?<=[.;])\s+|\s{2,}", text)
    metric_specs = (
        (
            re.compile(r"\b(?:adjusted\s+)?(?:diluted\s+)?(?:earnings\s+per\s+share|EPS)\b", re.I),
            FundamentalMetric.GUIDANCE_EPS_LOW,
            FundamentalMetric.GUIDANCE_EPS_HIGH,
            "USD/share",
        ),
        (
            re.compile(r"\b(?:net\s+)?(?:revenue|revenues|sales)\b", re.I),
            FundamentalMetric.GUIDANCE_REVENUE_LOW,
            FundamentalMetric.GUIDANCE_REVENUE_HIGH,
            "USD",
        ),
    )
    for segment in segments:
        if (
            not 20 <= len(segment) <= 1_200
            or _GUIDANCE_FORWARD.search(segment) is None
            or (
                _GUIDANCE_MANAGEMENT.search(segment) is None
                and _GUIDANCE_SECTION.search(segment) is None
            )
            or _GUIDANCE_THIRD_PARTY.search(segment) is not None
        ):
            continue
        period = _guidance_period(segment, filing_date=filing_date)
        if period is None:
            continue
        fiscal_year, fiscal_period, period_end, basis = period
        for keyword, low_metric, high_metric, unit in metric_specs:
            keyword_match = keyword.search(segment)
            if keyword_match is None:
                continue
            nearby = segment[
                max(0, keyword_match.start() - 80) : min(
                    len(segment), keyword_match.end() + 260
                )
            ]
            range_match = _GUIDANCE_RANGE.search(nearby)
            if range_match is None:
                continue
            low = _decimal_or_none(range_match.group(1).replace(",", ""))
            high = _decimal_or_none(range_match.group(3).replace(",", ""))
            if low is None or high is None or low <= 0 or high < low:
                continue
            scale_low = (range_match.group(2) or range_match.group(4) or "").lower()
            scale_high = (range_match.group(4) or range_match.group(2) or "").lower()
            if unit == "USD":
                if scale_low not in {"million", "billion"} or scale_high != scale_low:
                    continue
                scale = Decimal("1000000") if scale_low == "million" else Decimal("1000000000")
                low *= scale
                high *= scale
            elif scale_low or scale_high:
                continue
            for metric, value, bound in (
                (low_metric, low, "LOW"),
                (high_metric, high, "HIGH"),
            ):
                output.append(
                    FundamentalObservation(
                        symbol=symbol,
                        cik=cik,
                        metric=metric,
                        value=value,
                        unit=unit,
                        basis=basis,
                        period_end=period_end,
                        fiscal_period=fiscal_period,
                        fiscal_year=fiscal_year,
                        source="SEC_FILING",
                        source_id=f"sec:{accession}:{metric.value}:{fiscal_year}:{bound}",
                        source_url=source_url,
                        source_filed_date=filing_date,
                        observed_at=observed_at,
                        taxonomy="SEC_FILING_TEXT",
                        tag=bound,
                        form=form,
                    )
                )
    return tuple(output)


def _guidance_period(
    segment: str,
    *,
    filing_date: date,
) -> tuple[int, str, date, str] | None:
    """Resolve only explicit future annual or quarterly guidance periods."""

    year_match = _GUIDANCE_YEAR.search(segment)
    if year_match is not None:
        fiscal_year = int(year_match.group(1))
        if filing_date.year <= fiscal_year <= filing_date.year + 3:
            return (
                fiscal_year,
                f"FY{fiscal_year}",
                date(fiscal_year, 12, 31),
                "SEC_MANAGEMENT_GUIDANCE_FY_RANGE",
            )
        return None
    quarter_match = _GUIDANCE_QUARTER.search(segment)
    if quarter_match is None:
        return None
    names = {"first": 1, "second": 2, "third": 3, "fourth": 4}
    quarter = (
        names[str(quarter_match.group(1)).lower()]
        if quarter_match.group(1)
        else int(quarter_match.group(2))
    )
    fiscal_year = int(quarter_match.group(3))
    if not filing_date.year <= fiscal_year <= filing_date.year + 2:
        return None
    month = quarter * 3
    day = 31 if month in {3, 12} else 30
    return (
        fiscal_year,
        f"Q{quarter}-{fiscal_year}",
        date(fiscal_year, month, day),
        "SEC_MANAGEMENT_GUIDANCE_QUARTER_RANGE",
    )


_SEC_TAGS: tuple[tuple[FundamentalMetric, str, tuple[str, ...], tuple[str, ...]], ...] = (
    (
        FundamentalMetric.EPS_DILUTED,
        "USD/shares",
        ("EarningsPerShareDiluted",),
        ("USD/shares",),
    ),
    (
        FundamentalMetric.REVENUE,
        "USD",
        (
            "RevenueFromContractWithCustomerExcludingAssessedTax",
            "Revenues",
            "SalesRevenueNet",
        ),
        ("USD",),
    ),
    (
        FundamentalMetric.OPERATING_CASH_FLOW,
        "USD",
        ("NetCashProvidedByUsedInOperatingActivities",),
        ("USD",),
    ),
    (
        FundamentalMetric.DEBT_CURRENT,
        "USD",
        ("LongTermDebtCurrent", "ShortTermBorrowings"),
        ("USD",),
    ),
    (
        FundamentalMetric.DEBT_NONCURRENT,
        "USD",
        ("LongTermDebtNoncurrent",),
        ("USD",),
    ),
)


def _sec_observations(
    payload: Mapping[str, object],
    *,
    symbol: str,
    expected_cik: str,
    source_url: str,
    observed_at: datetime,
) -> tuple[FundamentalObservation, ...]:
    try:
        cik = f"{int(payload.get('cik')):010d}"
    except Exception:
        raise FundamentalProviderError("COMPANY_FACTS_INVALID") from None
    if cik != expected_cik or payload.get("facts") is None:
        raise FundamentalProviderError("COMPANY_FACTS_IDENTITY_MISMATCH")
    facts = payload.get("facts")
    us_gaap = facts.get("us-gaap") if isinstance(facts, Mapping) else None
    if not isinstance(us_gaap, Mapping):
        raise FundamentalProviderError("COMPANY_FACTS_INVALID")
    output: list[FundamentalObservation] = []
    for metric, output_unit, tags, allowed_units in _SEC_TAGS:
        candidates: list[tuple[date, datetime, int, str, str, Mapping[str, object]]] = []
        for tag_priority, tag in enumerate(tags):
            fact = us_gaap.get(tag)
            units = fact.get("units") if isinstance(fact, Mapping) else None
            if not isinstance(units, Mapping):
                continue
            for unit in allowed_units:
                values = units.get(unit)
                if not isinstance(values, list):
                    continue
                for item in values[-160:]:
                    if not isinstance(item, Mapping):
                        continue
                    parsed = _sec_fact_identity(item, observed_at=observed_at)
                    if parsed is None:
                        continue
                    period_end, filed_at, accession, form, fiscal_period = parsed
                    candidates.append(
                        (period_end, filed_at, -tag_priority, tag, unit, item)
                    )
        # Keep several distinct periods.  The last filed assertion for a
        # period becomes the current revision; older corrected values remain
        # in the append-only store once they have been observed.
        selected: dict[
            tuple[date, str, str],
            tuple[date, datetime, int, str, str, Mapping[str, object]],
        ] = {}
        for item in sorted(candidates, key=lambda row: (row[0], row[1], row[2])):
            fiscal_period = str(item[5].get("fp") or "UNKNOWN").strip().upper()
            accession = str(item[5].get("accn") or "").strip()
            # Keep each filed assertion so a first observation containing an
            # original filing plus amendment can seed the append-only revision
            # chain.  Within one accession, prefer the highest-priority tag.
            selected[(item[0], fiscal_period, accession)] = item
        bounded = sorted(
            selected.values(),
            key=lambda row: (row[0], row[1], str(row[5].get("accn") or "")),
        )[-8:]
        for period_end, filed_at, _priority, tag, unit, item in bounded:
            value = _decimal_or_none(item.get("val"))
            if value is None:
                continue
            accession = str(item.get("accn") or "").replace("-", "")
            form = str(item.get("form") or "").strip().upper()
            fiscal_period = str(item.get("fp") or "UNKNOWN").strip().upper()
            fiscal_year = item.get("fy")
            if isinstance(fiscal_year, bool) or not isinstance(fiscal_year, int):
                fiscal_year = None
            output.append(
                FundamentalObservation(
                    symbol=symbol,
                    cik=cik,
                    metric=metric,
                    value=value,
                    unit=output_unit,
                    basis="SEC_XBRL_ACTUAL",
                    period_end=period_end,
                    fiscal_period=fiscal_period,
                    fiscal_year=fiscal_year,
                    source="SEC_XBRL",
                    source_id=f"sec:{accession}:{tag}:{period_end.isoformat()}:{fiscal_period}",
                    source_url=source_url,
                    source_filed_date=filed_at.date(),
                    observed_at=observed_at,
                    taxonomy="us-gaap",
                    tag=tag,
                    form=form,
                )
            )
    return tuple(output)


def _sec_fact_identity(
    item: Mapping[str, object],
    *,
    observed_at: datetime,
) -> tuple[date, datetime, str, str, str] | None:
    try:
        period_end = date.fromisoformat(str(item.get("end")))
        filed_date = date.fromisoformat(str(item.get("filed")))
        filed_at = datetime.combine(filed_date, datetime.min.time(), tzinfo=timezone.utc)
        accession = str(item.get("accn") or "").strip()
        form = str(item.get("form") or "").strip().upper()
        fiscal_period = str(item.get("fp") or "UNKNOWN").strip().upper()
    except (TypeError, ValueError):
        return None
    if (
        filed_at > observed_at
        or form not in {"10-K", "10-K/A", "10-Q", "10-Q/A", "8-K", "8-K/A"}
        or not accession
        or len(accession) > 40
        or not fiscal_period
        or len(fiscal_period) > 24
    ):
        return None
    return period_end, filed_at, accession, form, fiscal_period


def _allowed_url(value: object) -> str:
    text = str(value or "").strip()
    try:
        parsed = urlsplit(text)
        query = parse_qsl(parsed.query, keep_blank_values=True, strict_parsing=True)
    except (TypeError, ValueError):
        raise FundamentalProviderError("URL_NOT_ALLOWED") from None
    base_valid = (
        parsed.scheme == "https"
        and parsed.username is None
        and parsed.password is None
        and parsed.port is None
        and parsed.fragment == ""
    )
    if not base_valid:
        raise FundamentalProviderError("URL_NOT_ALLOWED")
    if parsed.hostname == "www.sec.gov" and parsed.netloc == "www.sec.gov":
        if parsed.path == "/files/company_tickers_exchange.json" and not query:
            return text
    if parsed.hostname == "data.sec.gov" and parsed.netloc == "data.sec.gov":
        suffix = parsed.path.removeprefix("/api/xbrl/companyfacts/CIK")
        if re.fullmatch(r"[0-9]{10}\.json", suffix) and not query:
            return text
    if parsed.hostname == "finnhub.io" and parsed.netloc == "finnhub.io":
        values = dict(query)
        if (
            parsed.path == "/api/v1/stock/metric"
            and len(values) == len(query) == 2
            and tuple(values) == ("symbol", "metric")
            and _SYMBOL.fullmatch(values.get("symbol", ""))
            and values.get("metric") == "all"
        ):
            return text
    raise FundamentalProviderError("URL_NOT_ALLOWED")


def _allowed_sec_filing_url(value: object, *, document: bool) -> str:
    """Allow only exact SEC submission/archive objects without traversal."""

    text = str(value or "").strip()
    try:
        parsed = urlsplit(text)
    except (TypeError, ValueError):
        raise FundamentalProviderError("URL_NOT_ALLOWED") from None
    if (
        parsed.scheme != "https"
        or parsed.username is not None
        or parsed.password is not None
        or parsed.port is not None
        or parsed.query
        or parsed.fragment
        or "%" in parsed.path
        or "\\" in parsed.path
    ):
        raise FundamentalProviderError("URL_NOT_ALLOWED")
    if (
        not document
        and parsed.hostname == "data.sec.gov"
        and parsed.netloc == "data.sec.gov"
        and re.fullmatch(r"/submissions/CIK[0-9]{10}\.json", parsed.path)
    ):
        return text
    archive_prefix = r"/Archives/edgar/data/[1-9][0-9]{0,9}/[0-9]{18}/"
    if parsed.hostname != "www.sec.gov" or parsed.netloc != "www.sec.gov":
        raise FundamentalProviderError("URL_NOT_ALLOWED")
    if not document and re.fullmatch(archive_prefix + r"index\.json", parsed.path):
        return text
    if document:
        match = re.fullmatch(
            archive_prefix + r"([A-Za-z0-9][A-Za-z0-9_.-]{0,179}\.(?:htm|html|txt))",
            parsed.path,
            re.I,
        )
        if match is not None and ".." not in match.group(1):
            return text
    raise FundamentalProviderError("URL_NOT_ALLOWED")


def _json_object(body: bytes) -> Mapping[str, object]:
    try:
        value = json.loads(
            body.decode("utf-8", errors="strict"),
            object_pairs_hook=_object_without_duplicates,
            parse_float=Decimal,
            parse_constant=lambda _value: (_ for _ in ()).throw(ValueError()),
        )
    except Exception:
        raise FundamentalProviderError("BAD_JSON") from None
    if not isinstance(value, Mapping):
        raise FundamentalProviderError("BAD_JSON")
    return value


def _object_without_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
    output: dict[str, object] = {}
    for key, value in pairs:
        if key in output:
            raise ValueError("duplicate key")
        output[key] = value
    return output


def _symbols(values: Sequence[str]) -> tuple[str, ...]:
    if isinstance(values, (str, bytes, bytearray)) or not isinstance(values, Sequence):
        raise TypeError("symbols must be a sequence")
    result = tuple(dict.fromkeys(str(item).strip().upper() for item in values))
    if not result or len(result) > 40 or any(_SYMBOL.fullmatch(item) is None for item in result):
        raise ValueError("symbols are invalid")
    return result


def _decimal_or_none(value: object) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return parsed if parsed.is_finite() else None


__all__ = [
    "FINNHUB_METRIC_URL",
    "FinnhubValuationProvider",
    "FundamentalProviderError",
    "MAXIMUM_FINNHUB_METRIC_BYTES",
    "MAXIMUM_SEC_FACTS_BYTES",
    "MAXIMUM_SEC_FILING_DOCUMENT_BYTES",
    "MAXIMUM_SEC_FILING_INDEX_BYTES",
    "MAXIMUM_SEC_SUBMISSIONS_BYTES",
    "MAXIMUM_SEC_TICKER_BYTES",
    "SEC_COMPANY_FACTS_PREFIX",
    "SEC_SUBMISSIONS_PREFIX",
    "SEC_TICKER_URL",
    "SecCompanyFactsProvider",
    "SecManagementGuidanceProvider",
    "StrictFundamentalsHttpsTransport",
    "StrictSecFilingHttpsTransport",
]
