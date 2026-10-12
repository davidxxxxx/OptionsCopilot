"""Strict read-only discovery from the SEC current 8-K Atom feed.

The adapter copies filing metadata only.  It never copies the Atom summary or
filing body, and every returned event remains ``SUPPORTING_ONLY`` evidence.
"""
from __future__ import annotations

from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
import json
import re
from threading import RLock
from types import MappingProxyType
from typing import Protocol
from urllib.error import HTTPError
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener
import xml.etree.ElementTree as ElementTree

from options_copilot.providers.events import NewsEvent
from options_copilot.providers.sec_identity import sec_filer_entry_identity


SEC_CURRENT_8K_ATOM_URL = (
    "https://www.sec.gov/cgi-bin/browse-edgar?"
    "action=getcurrent&type=8-K&company=&dateb=&owner=include&count=100&output=atom"
)
SEC_COMPANY_TICKERS_EXCHANGE_URL = (
    "https://www.sec.gov/files/company_tickers_exchange.json"
)
SEC_USER_AGENT = (
    "OptionsCopilot/0.1 SEC-current-8K research contact=options-copilot@localhost.invalid"
)
SEC_ATOM_ACCEPT = "application/atom+xml, application/xml;q=0.9"
MAXIMUM_SEC_ATOM_RESPONSE_BYTES = 1024 * 1024
MAXIMUM_SEC_TICKER_RESPONSE_BYTES = 2 * 1024 * 1024
DEFAULT_CACHE_TTL_SECONDS = 60
DEFAULT_TICKER_CACHE_TTL_SECONDS = 6 * 60 * 60

_ATOM_NAMESPACE = "http://www.w3.org/2005/Atom"
_ATOM = f"{{{_ATOM_NAMESPACE}}}"
_ALLOWED_CONTENT_TYPES = frozenset(
    {"application/atom+xml", "application/xml", "text/xml"}
)
_ALLOWED_JSON_CONTENT_TYPES = frozenset({"application/json", "text/json"})
_UNSAFE_XML = re.compile(br"<!\s*(?:DOCTYPE|ENTITY)\b", re.IGNORECASE)
_TITLE = re.compile(
    r"^(?P<form>8-K(?:/A)?)\s*-\s*(?P<company>.+?)\s+"
    r"\((?P<cik>[0-9]{10})\)(?:\s+\([^)]*\))*\s*$",
    re.IGNORECASE,
)


class SecProviderTransportError(RuntimeError):
    """A redacted SEC transport-contract failure."""


class SecProviderTimeout(SecProviderTransportError):
    """The bounded SEC request timed out."""


class SecProviderRateLimited(SecProviderTransportError):
    """The SEC endpoint rejected the bounded request for pacing."""


class SecTickerMappingError(RuntimeError):
    """A redacted or ambiguous SEC CIK-to-ticker mapping failure."""


class AtomTransport(Protocol):
    def __call__(
        self,
        url: str,
        *,
        headers: Mapping[str, str],
        timeout_seconds: float,
    ) -> str | bytes: ...


class TickerResolver(Protocol):
    def __call__(self, company: str, cik: str) -> str | None: ...


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: ANN001
        return None


class SecAtomHttpsTransport:
    """Bounded GET transport for one exact public SEC Atom URL."""

    def __init__(self, *, opener: object | None = None) -> None:
        # ProxyHandler() honors only the environment inherited by this process;
        # it does not mutate user-wide or system-wide proxy settings.
        self._opener = opener or build_opener(ProxyHandler(), _NoRedirect())

    def __call__(
        self,
        url: str,
        *,
        headers: Mapping[str, str],
        timeout_seconds: float,
    ) -> str:
        if url != SEC_CURRENT_8K_ATOM_URL:
            raise SecProviderTransportError("SEC source is not allowlisted")
        timeout = _bounded_seconds(
            timeout_seconds,
            field="timeout_seconds",
            minimum=1.0,
            maximum=30.0,
        )
        if not isinstance(headers, Mapping):
            raise SecProviderTransportError("SEC request headers are invalid")
        allowed_headers = {"Accept", "User-Agent"}
        if set(headers) != allowed_headers:
            raise SecProviderTransportError("SEC request headers are not allowlisted")
        if str(headers.get("Accept") or "") != SEC_ATOM_ACCEPT:
            raise SecProviderTransportError("SEC Accept header is invalid")
        if str(headers.get("User-Agent") or "") != SEC_USER_AGENT:
            raise SecProviderTransportError("SEC User-Agent is invalid")

        request = Request(
            SEC_CURRENT_8K_ATOM_URL,
            data=None,
            headers={
                "Accept": SEC_ATOM_ACCEPT,
                "Accept-Encoding": "identity",
                "User-Agent": SEC_USER_AGENT,
            },
            method="GET",
        )
        failure: SecProviderTransportError | None = None
        response = None
        try:
            response = self._opener.open(request, timeout=timeout)  # type: ignore[attr-defined]
        except TimeoutError:
            failure = SecProviderTimeout("SEC request timed out")
        except HTTPError as exc:
            failure = (
                SecProviderRateLimited("SEC request was rate limited")
                if int(getattr(exc, "code", 0)) == 429
                else SecProviderTransportError("SEC request failed")
            )
        except Exception:
            failure = SecProviderTransportError("SEC request failed")
        if failure is not None:
            # Raise outside the exception handler so the original transport
            # exception (which may contain proxy credentials) is not retained.
            raise failure
        if response is None:
            raise SecProviderTransportError("SEC request failed")

        try:
            with response:
                if int(getattr(response, "status", 200)) != 200:
                    raise SecProviderTransportError("SEC response was not HTTP 200")
                if str(response.geturl()) != SEC_CURRENT_8K_ATOM_URL:
                    raise SecProviderTransportError("SEC redirects are forbidden")
                response_headers = response.headers
                content_encoding = str(
                    response_headers.get("Content-Encoding") or "identity"
                ).strip().lower()
                if content_encoding not in {"", "identity"}:
                    raise SecProviderTransportError(
                        "SEC compressed responses are forbidden"
                    )
                content_type = str(
                    response_headers.get("Content-Type") or ""
                ).split(";", 1)[0].strip().lower()
                if content_type not in _ALLOWED_CONTENT_TYPES:
                    raise SecProviderTransportError(
                        "SEC response content type is invalid"
                    )
                raw_length = response_headers.get("Content-Length")
                if raw_length not in (None, ""):
                    try:
                        declared_length = int(str(raw_length))
                    except (TypeError, ValueError):
                        raise SecProviderTransportError(
                            "SEC response length is invalid"
                        ) from None
                    if (
                        declared_length < 0
                        or declared_length > MAXIMUM_SEC_ATOM_RESPONSE_BYTES
                    ):
                        raise SecProviderTransportError(
                            "SEC response exceeds the size limit"
                        )
                body = response.read(MAXIMUM_SEC_ATOM_RESPONSE_BYTES + 1)
                if not isinstance(body, bytes):
                    raise SecProviderTransportError("SEC response body is invalid")
                if len(body) > MAXIMUM_SEC_ATOM_RESPONSE_BYTES:
                    raise SecProviderTransportError(
                        "SEC response exceeds the size limit"
                    )
                try:
                    return body.decode("utf-8", errors="strict")
                except UnicodeDecodeError:
                    raise SecProviderTransportError(
                        "SEC response encoding is invalid"
                    ) from None
        except SecProviderTransportError:
            raise
        except Exception:
            raise SecProviderTransportError("SEC response validation failed") from None


class SecCompanyTickersHttpsTransport:
    """Bounded JSON GET for the one SEC-maintained company/ticker file."""

    def __init__(self, *, opener: object | None = None) -> None:
        self._opener = opener or build_opener(ProxyHandler(), _NoRedirect())

    def __call__(
        self,
        url: str,
        *,
        headers: Mapping[str, str],
        timeout_seconds: float,
    ) -> str:
        if url != SEC_COMPANY_TICKERS_EXCHANGE_URL:
            raise SecProviderTransportError("SEC ticker source is not allowlisted")
        timeout = _bounded_seconds(
            timeout_seconds,
            field="timeout_seconds",
            minimum=1.0,
            maximum=30.0,
        )
        if not isinstance(headers, Mapping):
            raise SecProviderTransportError("SEC ticker request headers are invalid")
        allowed_headers = {"Accept", "User-Agent"}
        if set(headers) != allowed_headers:
            raise SecProviderTransportError(
                "SEC ticker request headers are not allowlisted"
            )
        if str(headers.get("Accept") or "") != "application/json":
            raise SecProviderTransportError("SEC ticker Accept header is invalid")
        if str(headers.get("User-Agent") or "") != SEC_USER_AGENT:
            raise SecProviderTransportError("SEC ticker User-Agent is invalid")

        request = Request(
            SEC_COMPANY_TICKERS_EXCHANGE_URL,
            data=None,
            headers={
                "Accept": "application/json",
                "Accept-Encoding": "identity",
                "User-Agent": SEC_USER_AGENT,
            },
            method="GET",
        )
        failure: SecProviderTransportError | None = None
        response = None
        try:
            response = self._opener.open(request, timeout=timeout)  # type: ignore[attr-defined]
        except TimeoutError:
            failure = SecProviderTimeout("SEC ticker request timed out")
        except HTTPError as exc:
            failure = (
                SecProviderRateLimited("SEC ticker request was rate limited")
                if int(getattr(exc, "code", 0)) == 429
                else SecProviderTransportError("SEC ticker request failed")
            )
        except Exception:
            failure = SecProviderTransportError("SEC ticker request failed")
        if failure is not None:
            raise failure
        if response is None:
            raise SecProviderTransportError("SEC ticker request failed")

        try:
            with response:
                if int(getattr(response, "status", 200)) != 200:
                    raise SecProviderTransportError(
                        "SEC ticker response was not HTTP 200"
                    )
                if str(response.geturl()) != SEC_COMPANY_TICKERS_EXCHANGE_URL:
                    raise SecProviderTransportError("SEC ticker redirects are forbidden")
                response_headers = response.headers
                content_encoding = str(
                    response_headers.get("Content-Encoding") or "identity"
                ).strip().lower()
                if content_encoding not in {"", "identity"}:
                    raise SecProviderTransportError(
                        "SEC ticker compressed responses are forbidden"
                    )
                content_type = str(
                    response_headers.get("Content-Type") or ""
                ).split(";", 1)[0].strip().lower()
                if content_type not in _ALLOWED_JSON_CONTENT_TYPES:
                    raise SecProviderTransportError(
                        "SEC ticker response content type is invalid"
                    )
                raw_length = response_headers.get("Content-Length")
                if raw_length not in (None, ""):
                    try:
                        declared_length = int(str(raw_length))
                    except (TypeError, ValueError):
                        raise SecProviderTransportError(
                            "SEC ticker response length is invalid"
                        ) from None
                    if (
                        declared_length < 0
                        or declared_length > MAXIMUM_SEC_TICKER_RESPONSE_BYTES
                    ):
                        raise SecProviderTransportError(
                            "SEC ticker response exceeds the size limit"
                        )
                body = response.read(MAXIMUM_SEC_TICKER_RESPONSE_BYTES + 1)
                if not isinstance(body, bytes):
                    raise SecProviderTransportError(
                        "SEC ticker response body is invalid"
                    )
                if len(body) > MAXIMUM_SEC_TICKER_RESPONSE_BYTES:
                    raise SecProviderTransportError(
                        "SEC ticker response exceeds the size limit"
                    )
                try:
                    return body.decode("utf-8", errors="strict")
                except UnicodeDecodeError:
                    raise SecProviderTransportError(
                        "SEC ticker response encoding is invalid"
                    ) from None
        except SecProviderTransportError:
            raise
        except Exception:
            raise SecProviderTransportError(
                "SEC ticker response validation failed"
            ) from None


@dataclass(frozen=True, slots=True)
class _TickerCacheEntry:
    mapping: Mapping[str, str]
    candidate_mapping: Mapping[str, tuple[str, ...]]
    candidate_owner_mapping: Mapping[str, str]
    ambiguous_ciks: frozenset[str]
    observed_at: datetime
    expires_at: datetime
    status: str
    reason: str | None


@dataclass(frozen=True, slots=True)
class _TickerPreferenceSnapshot:
    generation: int
    mapping: Mapping[str, str]
    candidate_mapping: Mapping[str, tuple[str, ...]]
    candidate_owner_mapping: Mapping[str, str]
    expires_at: datetime


class SecCikTickerResolver:
    """Resolve a normalized CIK only when the official mapping is unambiguous."""

    decision_authority = "SUPPORTING_ONLY"
    approval_eligible = False
    instruction_creation_allowed = False
    order_allowed = False

    def __init__(
        self,
        *,
        transport: AtomTransport | None = None,
        now: Callable[[], datetime] | None = None,
        timeout_seconds: float = 8.0,
        cache_ttl_seconds: int = DEFAULT_TICKER_CACHE_TTL_SECONDS,
    ) -> None:
        selected_transport = transport or SecCompanyTickersHttpsTransport()
        if not callable(selected_transport):
            raise TypeError("transport must be callable")
        if now is not None and not callable(now):
            raise TypeError("now must be callable")
        self._timeout = _bounded_seconds(
            timeout_seconds,
            field="timeout_seconds",
            minimum=1.0,
            maximum=30.0,
        )
        if (
            isinstance(cache_ttl_seconds, bool)
            or not isinstance(cache_ttl_seconds, int)
            or not 30 <= cache_ttl_seconds <= 86400
        ):
            raise ValueError("cache_ttl_seconds must be between 30 and 86400")
        self._transport = selected_transport
        self._now = now or (lambda: datetime.now(timezone.utc))
        self._cache_ttl = timedelta(seconds=cache_ttl_seconds)
        self._cache: _TickerCacheEntry | None = None
        self._lock = RLock()
        self.health = "DEGRADED"
        self.health_reason: str | None = "not_fetched"
        self.cache_state = "EMPTY"
        self.last_observed_at: datetime | None = None
        self._attempt_count = 0
        self._resolved_count = 0
        self._missing_count = 0
        self._failure_count = 0
        self._cache_hit_count = 0
        self._cache_miss_count = 0
        self._refresh_count = 0
        self._generation = 0
        self._current_mapping_canonicalized_cik_count = 0
        self._current_mapping_ambiguous_cik_count = 0
        self._failure_counts: Counter[str] = Counter()

    def __call__(self, company: str, cik: str) -> str | None:
        return self.resolve(company, cik)

    def resolve(
        self,
        company: str,
        cik: str,
        *,
        preferred_symbols: Sequence[str] = (),
    ) -> str | None:
        symbol, _generation = self._resolve_with_generation(
            company,
            cik,
            preferred_symbols=preferred_symbols,
        )
        return symbol

    def _resolve_for_owned_provider(
        self,
        company: str,
        cik: str,
        *,
        preferred_symbols: Sequence[str] = (),
    ) -> tuple[str | None, int]:
        return self._resolve_with_generation(
            company,
            cik,
            preferred_symbols=preferred_symbols,
        )

    def _resolve_with_generation(
        self,
        company: str,
        cik: str,
        *,
        preferred_symbols: Sequence[str],
    ) -> tuple[str | None, int]:
        if not isinstance(company, str):
            raise TypeError("company must be a string")
        checked_company = " ".join(company.split())
        if not checked_company or len(checked_company) > 300:
            raise ValueError("company is invalid")
        normalized_cik = _cik(cik)
        preferred = _preferred_symbols(preferred_symbols)
        with self._lock:
            self._attempt_count += 1
            observed_at = _aware(self._now(), field="ticker resolver clock")
            cached = self._cache
            if (
                cached is not None
                and cached.observed_at <= observed_at < cached.expires_at
            ):
                self.cache_state = "HIT"
                self._cache_hit_count += 1
                self.health = cached.status
                self.health_reason = cached.reason
                self.last_observed_at = cached.observed_at
                return (
                    self._resolve_and_count(cached, normalized_cik, preferred),
                    self._generation,
                )

            self.cache_state = "MISS"
            self._cache_miss_count += 1
            # refresh_count records every attempted official-mapping refresh,
            # including transport and parsing failures.  generation advances
            # only after a new mapping has been installed successfully.
            self._refresh_count += 1
            transport_failure: tuple[str, str] | None = None
            try:
                payload = self._transport(
                    SEC_COMPANY_TICKERS_EXCHANGE_URL,
                    headers={
                        "Accept": "application/json",
                        "User-Agent": SEC_USER_AGENT,
                    },
                    timeout_seconds=self._timeout,
                )
            except (SecProviderTimeout, TimeoutError):
                transport_failure = ("TIMEOUT", "request_timeout")
            except SecProviderRateLimited:
                transport_failure = ("RATE_LIMITED", "rate_limited")
            except Exception:
                transport_failure = ("DEGRADED", "request_failed")
            if transport_failure is not None:
                self._set_failure(*transport_failure, observed_at=observed_at)
                self._record_resolution_failure(transport_failure[1])
                raise SecTickerMappingError("SEC ticker mapping unavailable")

            try:
                (
                    mapping,
                    candidate_mapping,
                    ambiguous_ciks,
                    canonicalized_cik_count,
                ) = _ticker_mapping(payload)
            except _InvalidTickerMapping:
                self._set_failure(
                    "BAD_JSON",
                    "invalid_mapping",
                    observed_at=observed_at,
                )
                self._record_resolution_failure("invalid_mapping")
                raise SecTickerMappingError("SEC ticker mapping is invalid") from None
            status = "CONFLICTED" if ambiguous_ciks else "READY"
            reason = (
                "duplicate_or_conflicting_mapping" if ambiguous_ciks else None
            )
            cached = _TickerCacheEntry(
                mapping=MappingProxyType(mapping),
                candidate_mapping=MappingProxyType(candidate_mapping),
                candidate_owner_mapping=MappingProxyType(
                    {
                        ticker: candidate_cik
                        for candidate_cik, tickers in candidate_mapping.items()
                        for ticker in tickers
                    }
                ),
                ambiguous_ciks=ambiguous_ciks,
                observed_at=observed_at,
                expires_at=observed_at + self._cache_ttl,
                status=status,
                reason=reason,
            )
            self._cache = cached
            self._generation += 1
            self.health = status
            self.health_reason = reason
            self.last_observed_at = observed_at
            self._current_mapping_canonicalized_cik_count = canonicalized_cik_count
            self._current_mapping_ambiguous_cik_count = len(ambiguous_ciks)
            return (
                self._resolve_and_count(cached, normalized_cik, preferred),
                self._generation,
            )

    def _preference_snapshot(
        self,
        *,
        observed_at: datetime,
        expected_generation: int,
    ) -> _TickerPreferenceSnapshot | None:
        """Return one immutable, fresh mapping generation for cache relabeling."""

        checked_observed_at = _aware(
            observed_at,
            field="ticker preference observation",
        )
        with self._lock:
            cached = self._cache
            if (
                cached is None
                or self._generation != expected_generation
                or not cached.observed_at
                <= checked_observed_at
                < cached.expires_at
            ):
                return None
            return _TickerPreferenceSnapshot(
                generation=self._generation,
                mapping=cached.mapping,
                candidate_mapping=cached.candidate_mapping,
                candidate_owner_mapping=cached.candidate_owner_mapping,
                expires_at=cached.expires_at,
            )

    def resolution_stats(self) -> dict[str, object]:
        """Return redacted, aggregation-safe resolver counters.

        The snapshot deliberately contains no CIK, company, ticker, response
        body, request header, token, or account field.  A later persistence
        adapter can therefore aggregate these fixed reason codes without
        retaining arbitrary provider text.
        """

        with self._lock:
            return {
                "schema": "options_copilot.sec_ticker_resolution_stats",
                "version": 1,
                "asof": (
                    None
                    if self.last_observed_at is None
                    else self.last_observed_at.isoformat()
                ),
                "attempt_count": self._attempt_count,
                "resolved_count": self._resolved_count,
                "missing_count": self._missing_count,
                "failure_count": self._failure_count,
                "cache_hit_count": self._cache_hit_count,
                "cache_miss_count": self._cache_miss_count,
                "refresh_count": self._refresh_count,
                "generation": self._generation,
                "current_mapping_canonicalized_cik_count": (
                    self._current_mapping_canonicalized_cik_count
                ),
                "current_mapping_ambiguous_cik_count": (
                    self._current_mapping_ambiguous_cik_count
                ),
                "failure_counts": dict(sorted(self._failure_counts.items())),
            }

    def health_snapshot(self) -> dict[str, object]:
        with self._lock:
            return {
                "name": "sec-cik-ticker-resolver",
                "status": self.health,
                "reason": self.health_reason,
                "cache_state": self.cache_state,
                "asof": (
                    None
                    if self.last_observed_at is None
                    else self.last_observed_at.isoformat()
                ),
                "source": "SEC",
                "source_url": SEC_COMPANY_TICKERS_EXCHANGE_URL,
                "decision_authority": "SUPPORTING_ONLY",
                "approval_eligible": False,
                "instruction_creation_allowed": False,
                "order_allowed": False,
                "resolution_stats": self.resolution_stats(),
            }

    def _resolve_and_count(
        self,
        cached: _TickerCacheEntry,
        normalized_cik: str,
        preferred_symbols: tuple[str, ...],
    ) -> str | None:
        try:
            symbol = self._resolve_cached(
                cached,
                normalized_cik,
                preferred_symbols,
            )
        except SecTickerMappingError:
            self._record_resolution_failure("ambiguous_cik")
            raise
        if symbol is None:
            self._missing_count += 1
        else:
            self._resolved_count += 1
        return symbol

    def _record_resolution_failure(self, reason: str) -> None:
        if reason not in {
            "ambiguous_cik",
            "invalid_mapping",
            "rate_limited",
            "request_failed",
            "request_timeout",
        }:
            reason = "request_failed"
        self._failure_count += 1
        self._failure_counts[reason] += 1

    def _resolve_cached(
        self,
        cached: _TickerCacheEntry,
        normalized_cik: str,
        preferred_symbols: tuple[str, ...],
    ) -> str | None:
        if normalized_cik in cached.ambiguous_ciks:
            raise SecTickerMappingError("SEC CIK mapping is ambiguous")
        candidates = cached.candidate_mapping.get(normalized_cik, ())
        for preferred in preferred_symbols:
            if preferred in candidates:
                return preferred
        return cached.mapping.get(normalized_cik)

    def _set_failure(
        self,
        status: str,
        reason: str,
        *,
        observed_at: datetime,
    ) -> None:
        self.health = status
        self.health_reason = reason
        self.last_observed_at = observed_at


@dataclass(frozen=True, slots=True)
class _CacheEntry:
    events: tuple[NewsEvent, ...]
    ticker_resolver_generation: int | None
    status: str
    reason: str | None
    observed_at: datetime
    materialized_at: datetime
    expires_at: datetime


_DEFAULT_TICKER_RESOLVER = object()


def _empty_ticker_resolution_stats() -> dict[str, object]:
    return {
        "schema": "options_copilot.sec_ticker_resolution_stats",
        "version": 1,
        "asof": None,
        "attempt_count": 0,
        "resolved_count": 0,
        "missing_count": 0,
        "failure_count": 0,
        "cache_hit_count": 0,
        "cache_miss_count": 0,
        "refresh_count": 0,
        "generation": 0,
        "current_mapping_canonicalized_cik_count": 0,
        "current_mapping_ambiguous_cik_count": 0,
        "failure_counts": {},
    }


class SecCurrent8KProvider:
    """Cached current-8-K discovery with no trading or approval authority."""

    decision_authority = "SUPPORTING_ONLY"
    approval_eligible = False
    instruction_creation_allowed = False
    order_allowed = False

    def __init__(
        self,
        *,
        transport: AtomTransport | None = None,
        now: Callable[[], datetime] | None = None,
        ticker_resolver: TickerResolver | None | object = _DEFAULT_TICKER_RESOLVER,
        ticker_transport: AtomTransport | None = None,
        timeout_seconds: float = 8.0,
        cache_ttl_seconds: int = DEFAULT_CACHE_TTL_SECONDS,
        ticker_cache_ttl_seconds: int = DEFAULT_TICKER_CACHE_TTL_SECONDS,
    ) -> None:
        selected_transport = transport or SecAtomHttpsTransport()
        if not callable(selected_transport):
            raise TypeError("transport must be callable")
        if now is not None and not callable(now):
            raise TypeError("now must be callable")
        self._timeout = _bounded_seconds(
            timeout_seconds,
            field="timeout_seconds",
            minimum=1.0,
            maximum=30.0,
        )
        if (
            isinstance(cache_ttl_seconds, bool)
            or not isinstance(cache_ttl_seconds, int)
            or not 30 <= cache_ttl_seconds <= 300
        ):
            raise ValueError("cache_ttl_seconds must be between 30 and 300")
        self._transport = selected_transport
        self._now = now or (lambda: datetime.now(timezone.utc))
        if ticker_resolver is _DEFAULT_TICKER_RESOLVER:
            self._ticker_resolver: TickerResolver | None = SecCikTickerResolver(
                transport=ticker_transport,
                now=self._now,
                timeout_seconds=self._timeout,
                cache_ttl_seconds=ticker_cache_ttl_seconds,
            )
            self._owns_default_ticker_resolver = True
        else:
            if ticker_transport is not None:
                raise ValueError(
                    "ticker_transport is accepted only by the default ticker resolver"
                )
            if ticker_resolver is not None and not callable(ticker_resolver):
                raise TypeError("ticker_resolver must be callable or None")
            self._ticker_resolver = ticker_resolver  # type: ignore[assignment]
            self._owns_default_ticker_resolver = False
        self._cache_ttl = timedelta(seconds=cache_ttl_seconds)
        self._cache: _CacheEntry | None = None
        self._lock = RLock()
        self.health = "DEGRADED"
        self.health_reason: str | None = "not_fetched"
        self.cache_state = "EMPTY"
        self.last_observed_at: datetime | None = None
        self._last_clock_at: datetime | None = None

    def news(
        self,
        symbols: Sequence[str],
        *,
        limit: int = 50,
    ) -> tuple[NewsEvent, ...]:
        requested = _symbols(symbols)
        if (
            isinstance(limit, bool)
            or not isinstance(limit, int)
            or not 1 <= limit <= 100
        ):
            raise ValueError("limit must be between 1 and 100")
        with self._lock:
            try:
                request_started_at = self._read_clock()
            except _InvalidProviderClock:
                return self._clock_failed("clock_invalid")
            except _RegressedProviderClock:
                return self._clock_failed("clock_regressed")
            cached = self._cache
            provider_cache_fresh = (
                cached is not None
                and cached.observed_at <= request_started_at < cached.expires_at
            )
            preference_snapshot: _TickerPreferenceSnapshot | None = None
            owned_resolver = self._owned_default_resolver()
            if (
                provider_cache_fresh
                and cached is not None
                and owned_resolver is not None
                and cached.ticker_resolver_generation is not None
            ):
                preference_snapshot = owned_resolver._preference_snapshot(
                    observed_at=request_started_at,
                    expected_generation=cached.ticker_resolver_generation,
                )
                provider_cache_fresh = preference_snapshot is not None
            if provider_cache_fresh and cached is not None:
                events = self._events_for_request(
                    cached.events,
                    requested,
                    preference_snapshot=preference_snapshot,
                )
                try:
                    cache_checked_at = self._read_clock()
                except _InvalidProviderClock:
                    return self._clock_failed("clock_invalid")
                except _RegressedProviderClock:
                    return self._clock_failed("clock_regressed")
                ticker_cache_fresh = (
                    preference_snapshot is None
                    or cache_checked_at < preference_snapshot.expires_at
                )
                if cache_checked_at < cached.expires_at and ticker_cache_fresh:
                    self.health = cached.status
                    self.health_reason = cached.reason
                    self.cache_state = "HIT"
                    self.last_observed_at = cached.materialized_at
                    return _filter_events(events, requested, limit=limit)
                request_started_at = cache_checked_at

            self.cache_state = "MISS"
            try:
                payload = self._transport(
                    SEC_CURRENT_8K_ATOM_URL,
                    headers={
                        "Accept": SEC_ATOM_ACCEPT,
                        "User-Agent": SEC_USER_AGENT,
                    },
                    timeout_seconds=self._timeout,
                )
            except (SecProviderTimeout, TimeoutError):
                return self._failed("TIMEOUT", "request_timeout", request_started_at)
            except SecProviderRateLimited:
                return self._failed("RATE_LIMITED", "rate_limited", request_started_at)
            except Exception:
                return self._failed("DEGRADED", "request_failed", request_started_at)

            try:
                atom_received_at = self._read_clock()
            except _InvalidProviderClock:
                return self._clock_failed("clock_invalid")
            except _RegressedProviderClock:
                return self._clock_failed("clock_regressed")

            try:
                (
                    events,
                    targeted,
                    malformed,
                    resolver_failed,
                    resolver_generation,
                ) = self._parse(
                    payload,
                    publication_cutoff=atom_received_at,
                    preferred_symbols=requested,
                )
            except _UnsafeXml:
                return self._failed("BAD_XML", "unsafe_xml", atom_received_at)
            except _InvalidAtom:
                return self._failed("BAD_XML", "invalid_atom", atom_received_at)

            try:
                materialized_at = self._read_clock()
            except _InvalidProviderClock:
                return self._clock_failed("clock_invalid")
            except _RegressedProviderClock:
                return self._clock_failed("clock_regressed")
            events = tuple(
                replace(
                    event,
                    first_seen_at=materialized_at,
                    ingested_at=materialized_at,
                    observed_at=materialized_at,
                    content_hash=None,
                )
                for event in events
            )

            if targeted and not events and malformed:
                status, reason = "MISSING_FIELDS", "no_usable_records"
            elif malformed:
                status, reason = "DEGRADED", "partial_parse"
            elif resolver_failed:
                status, reason = "DEGRADED", "ticker_resolution_failed"
            else:
                status, reason = "READY", None
            self.health = status
            self.health_reason = reason
            self.last_observed_at = materialized_at
            expires_at = atom_received_at + self._cache_ttl
            if materialized_at < expires_at:
                self._cache = _CacheEntry(
                    events=events,
                    ticker_resolver_generation=resolver_generation,
                    status=status,
                    reason=reason,
                    observed_at=atom_received_at,
                    materialized_at=materialized_at,
                    expires_at=expires_at,
                )
            return _filter_events(events, requested, limit=limit)

    def health_snapshot(self) -> dict[str, object]:
        with self._lock:
            return {
                "name": "sec-current-8k-atom",
                "status": self.health,
                "reason": self.health_reason,
                "cache_state": self.cache_state,
                "asof": (
                    None
                    if self.last_observed_at is None
                    else self.last_observed_at.isoformat()
                ),
                "source": "SEC",
                "source_url": SEC_CURRENT_8K_ATOM_URL,
                "decision_authority": "SUPPORTING_ONLY",
                "approval_eligible": False,
                "instruction_creation_allowed": False,
                "order_allowed": False,
                "ticker_resolution_stats": self.ticker_resolution_stats(),
            }

    def ticker_resolution_stats(self) -> dict[str, object]:
        """Expose only fixed-schema, redacted counters from the default resolver."""

        resolver = self._owned_default_resolver()
        if resolver is not None:
            # Use the exact implementation only for the resolver constructed
            # inside this provider.  Injected callables never receive a health
            # or stats call, even when they subclass SecCikTickerResolver.
            return SecCikTickerResolver.resolution_stats(resolver)
        return _empty_ticker_resolution_stats()

    def _owned_default_resolver(self) -> SecCikTickerResolver | None:
        resolver = self._ticker_resolver
        if (
            self._owns_default_ticker_resolver
            and type(resolver) is SecCikTickerResolver
        ):
            return resolver
        return None

    def _events_for_request(
        self,
        events: tuple[NewsEvent, ...],
        requested: tuple[str, ...],
        *,
        preference_snapshot: _TickerPreferenceSnapshot | None,
    ) -> tuple[NewsEvent, ...]:
        if preference_snapshot is None:
            return events
        remapped: list[NewsEvent] = []
        for event in events:
            symbol = event.symbol
            if symbol is None:
                remapped.append(event)
                continue
            normalized_cik = preference_snapshot.candidate_owner_mapping.get(
                symbol
            )
            candidates = (
                ()
                if normalized_cik is None
                else preference_snapshot.candidate_mapping.get(normalized_cik, ())
            )
            preferred = next(
                (candidate for candidate in requested if candidate in candidates),
                (
                    symbol
                    if normalized_cik is None
                    else preference_snapshot.mapping.get(normalized_cik, symbol)
                ),
            )
            remapped.append(
                event
                if preferred == symbol
                else replace(
                    event,
                    symbol=preferred,
                    entity_id=preferred,
                    content_hash=None,
                )
            )
        return tuple(remapped)

    def _failed(
        self,
        status: str,
        reason: str,
        observed_at: datetime,
    ) -> tuple[NewsEvent, ...]:
        self.health = status
        self.health_reason = reason
        self.last_observed_at = observed_at
        return ()

    def _read_clock(self) -> datetime:
        try:
            observed_at = _aware(self._now(), field="provider clock")
        except Exception:
            raise _InvalidProviderClock from None
        if self._last_clock_at is not None and observed_at < self._last_clock_at:
            raise _RegressedProviderClock
        self._last_clock_at = observed_at
        return observed_at

    def _clock_failed(self, reason: str) -> tuple[NewsEvent, ...]:
        self.health = "DEGRADED"
        self.health_reason = reason
        self.cache_state = "MISS"
        return ()

    def _parse(
        self,
        payload: str | bytes,
        *,
        publication_cutoff: datetime,
        preferred_symbols: tuple[str, ...],
    ) -> tuple[tuple[NewsEvent, ...], int, int, bool, int | None]:
        raw = _payload_bytes(payload)
        if _UNSAFE_XML.search(raw):
            raise _UnsafeXml
        try:
            root = ElementTree.fromstring(raw)
        except (ElementTree.ParseError, ValueError):
            raise _InvalidAtom from None
        if root.tag != f"{_ATOM}feed":
            raise _InvalidAtom

        events: list[NewsEvent] = []
        targeted = 0
        malformed = 0
        resolver_failed = False
        resolver_generations: set[int] = set()
        owned_resolver = self._owned_default_resolver()
        seen_entry_metadata: dict[
            tuple[str, str],
            tuple[str, str, str, datetime, str, str],
        ] = {}
        for entry in root.findall(f"{_ATOM}entry"):
            category = entry.find(f"{_ATOM}category")
            form = str(category.get("term") if category is not None else "").strip().upper()
            if form not in {"8-K", "8-K/A"}:
                continue
            targeted += 1
            try:
                parsed = _entry_metadata(
                    entry,
                    publication_cutoff=publication_cutoff,
                )
            except (TypeError, ValueError):
                malformed += 1
                continue
            company, cik, source_id, published_at, link, normalized_form = parsed
            entry_key = (source_id, cik)
            prior_metadata = seen_entry_metadata.get(entry_key)
            if prior_metadata is not None:
                # SEC can repeat a byte-equivalent filing entry in the current
                # feed. Fold only the same accession-and-filer metadata;
                # conflicting metadata for that exact pair remains invalid.
                if parsed != prior_metadata:
                    malformed += 1
                continue
            seen_entry_metadata[entry_key] = parsed

            symbol: str | None = None
            if self._ticker_resolver is not None:
                try:
                    if owned_resolver is not None:
                        raw_symbol, resolver_generation = (
                            owned_resolver._resolve_for_owned_provider(
                                company,
                                cik,
                                preferred_symbols=preferred_symbols,
                            )
                        )
                        resolver_generations.add(resolver_generation)
                    else:
                        # Preserve the public two-argument resolver protocol for
                        # every custom resolver.
                        raw_symbol = self._ticker_resolver(company, cik)
                    symbol = None if raw_symbol is None else _symbol(raw_symbol)
                except Exception:
                    resolver_failed = True
                    symbol = None
            event_id = sec_filer_entry_identity(source_id, cik, link).event_id
            events.append(
                NewsEvent(
                    event_id=event_id,
                    symbol=symbol,
                    source="SEC",
                    headline=f"{normalized_form} - {company}",
                    summary=(
                        f"Official SEC current {normalized_form} filing metadata for "
                        f"{company}."
                    ),
                    url=link,
                    published_at=published_at,
                    first_seen_at=publication_cutoff,
                    ingested_at=publication_cutoff,
                    observed_at=publication_cutoff,
                    source_rank=1,
                    source_id=source_id,
                    lineage_id=event_id,
                    evidence_ids=(event_id,),
                    provenance=("SEC", "SEC Current 8-K Atom"),
                )
            )
        events.sort(key=lambda event: (event.published_at, event.event_id), reverse=True)
        if len(resolver_generations) > 1:
            resolver_failed = True
        resolver_generation = (
            next(iter(resolver_generations))
            if len(resolver_generations) == 1
            else None
        )
        return (
            tuple(events),
            targeted,
            malformed,
            resolver_failed,
            resolver_generation,
        )


class _InvalidAtom(ValueError):
    pass


class _UnsafeXml(ValueError):
    pass


class _InvalidProviderClock(ValueError):
    pass


class _RegressedProviderClock(ValueError):
    pass


class _InvalidTickerMapping(ValueError):
    pass


def _entry_metadata(
    entry: ElementTree.Element,
    *,
    publication_cutoff: datetime,
) -> tuple[str, str, str, datetime, str, str]:
    title = _element_text(entry, "title", maximum=500)
    title_match = _TITLE.fullmatch(title)
    if title_match is None:
        raise ValueError("SEC entry title is invalid")
    form = title_match.group("form").upper()
    company = " ".join(title_match.group("company").split())
    cik = title_match.group("cik")
    if not company or len(company) > 300:
        raise ValueError("SEC company metadata is invalid")
    source_id = _element_text(entry, "id", maximum=500)
    updated = _timestamp(_element_text(entry, "updated", maximum=80))
    if updated > publication_cutoff:
        raise ValueError("SEC publication time is in the future")

    candidates = [
        child
        for child in entry.findall(f"{_ATOM}link")
        if str(child.get("rel") or "alternate").strip().lower() == "alternate"
    ]
    if len(candidates) != 1:
        raise ValueError("SEC entry alternate link is missing or ambiguous")
    link = sec_filer_entry_identity(
        source_id,
        cik,
        str(candidates[0].get("href") or ""),
    ).url
    return company, cik, source_id, updated, link, form


def _element_text(
    entry: ElementTree.Element,
    name: str,
    *,
    maximum: int,
) -> str:
    node = entry.find(f"{_ATOM}{name}")
    value = "" if node is None or node.text is None else node.text.strip()
    if not value or len(value) > maximum:
        raise ValueError(f"SEC entry {name} is invalid")
    return value


def _timestamp(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise ValueError("SEC entry timestamp is invalid") from None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("SEC entry timestamp is timezone-naive")
    return parsed.astimezone(timezone.utc)


def _payload_bytes(payload: str | bytes) -> bytes:
    if isinstance(payload, str):
        raw = payload.encode("utf-8")
    elif isinstance(payload, bytes):
        raw = payload
    else:
        raise _InvalidAtom
    if not raw or len(raw) > MAXIMUM_SEC_ATOM_RESPONSE_BYTES:
        raise _InvalidAtom
    return raw


def _ticker_mapping(
    payload: str | bytes,
) -> tuple[
    dict[str, str],
    dict[str, tuple[str, ...]],
    frozenset[str],
    int,
]:
    if isinstance(payload, str):
        raw = payload.encode("utf-8")
    elif isinstance(payload, bytes):
        raw = payload
    else:
        raise _InvalidTickerMapping
    if not raw or len(raw) > MAXIMUM_SEC_TICKER_RESPONSE_BYTES:
        raise _InvalidTickerMapping
    try:
        document = json.loads(
            raw.decode("utf-8", errors="strict"),
            object_pairs_hook=_json_object_without_duplicates,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, _InvalidTickerMapping):
        raise _InvalidTickerMapping from None
    if (
        not isinstance(document, dict)
        or set(document) != {"fields", "data"}
        or document.get("fields") != ["cik", "name", "ticker", "exchange"]
    ):
        raise _InvalidTickerMapping
    rows = document.get("data")
    if (
        not isinstance(rows, list)
        or not rows
        or len(rows) > 100000
    ):
        raise _InvalidTickerMapping

    ticker_rows_by_cik: dict[str, list[str]] = {}
    ticker_owners: dict[str, str] = {}
    ambiguous_ciks: set[str] = set()
    for row in rows:
        if not isinstance(row, list) or len(row) != 4:
            raise _InvalidTickerMapping
        try:
            normalized_cik = _cik(row[0])
            company = " ".join(row[1].split()) if isinstance(row[1], str) else ""
        except (TypeError, ValueError):
            raise _InvalidTickerMapping from None
        exchange = row[3]
        if (
            not company
            or len(company) > 300
            or (
                exchange is not None
                and (
                    not isinstance(exchange, str)
                    or len(exchange.strip()) > 120
                )
            )
        ):
            raise _InvalidTickerMapping

        try:
            ticker = _sec_ticker(row[2])
        except (TypeError, ValueError):
            # One unusable SEC ticker must not poison unrelated CIKs, but the
            # affected CIK is permanently excluded from this cache head.
            ambiguous_ciks.add(normalized_cik)
            continue

        cik_tickers = ticker_rows_by_cik.setdefault(normalized_cik, [])
        if ticker in cik_tickers:
            # Exact duplicate rows are not needed to represent multiple share
            # classes and remain a fail-closed integrity error.
            ambiguous_ciks.add(normalized_cik)
        cik_tickers.append(ticker)
        owner = ticker_owners.get(ticker)
        if owner is None:
            ticker_owners[ticker] = normalized_cik
        elif owner != normalized_cik:
            ambiguous_ciks.update((owner, normalized_cik))

    mapping: dict[str, str] = {}
    candidate_mapping: dict[str, tuple[str, ...]] = {}
    canonicalized_cik_count = 0
    for normalized_cik, ticker_rows in ticker_rows_by_cik.items():
        if normalized_cik in ambiguous_ciks:
            continue
        tickers = tuple(
            sorted(frozenset(ticker_rows), key=_canonical_ticker_order)
        )
        candidate_mapping[normalized_cik] = tickers
        mapping[normalized_cik] = tickers[0]
        if len(tickers) > 1:
            # One issuer can legitimately publish several listed share-class,
            # unit, or warrant symbols under the same CIK.  The filing applies
            # at issuer level; choose one stable representative for the
            # single-symbol NewsEvent contract and expose only an aggregate
            # canonicalization count.
            canonicalized_cik_count += 1
    return (
        mapping,
        candidate_mapping,
        frozenset(ambiguous_ciks),
        canonicalized_cik_count,
    )


def _canonical_ticker_order(ticker: str) -> tuple[int, int, str]:
    """Prefer a plain, short issuer symbol with a stable lexical tie-break."""

    return (1 if "." in ticker else 0, len(ticker), ticker)


def _json_object_without_duplicates(
    pairs: list[tuple[str, object]],
) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise _InvalidTickerMapping
        result[key] = value
    return result


def _cik(value: object) -> str:
    if isinstance(value, bool):
        raise TypeError("CIK must not be boolean")
    if isinstance(value, int):
        if value <= 0:
            raise ValueError("CIK must be positive")
        text = str(value)
    elif isinstance(value, str):
        text = value.strip()
        if not text.isdigit():
            raise ValueError("CIK must contain digits only")
        text = text.lstrip("0") or "0"
        if text == "0":
            raise ValueError("CIK must be positive")
    else:
        raise TypeError("CIK must be an integer or digit string")
    if len(text) > 10:
        raise ValueError("CIK exceeds ten digits")
    return text.zfill(10)


def _sec_ticker(value: object) -> str:
    if not isinstance(value, str):
        raise TypeError("SEC ticker must be a string")
    raw = value.strip().upper()
    if (
        not raw
        or raw.startswith((".", "-"))
        or raw.endswith((".", "-"))
        or "--" in raw
        or ".." in raw
        or ".-" in raw
        or "-." in raw
    ):
        raise ValueError("SEC ticker is invalid")
    # SEC encodes class/share separators with '-', while the Options Copilot
    # symbol contract uses '.'.  Normalize before duplicate/conflict checks.
    return _symbol(raw.replace("-", "."))


def _filter_events(
    events: tuple[NewsEvent, ...],
    requested: tuple[str, ...],
    *,
    limit: int,
) -> tuple[NewsEvent, ...]:
    # The SEC current feed is the broad-market discovery source.  Requested
    # symbols are prioritized, not used to discard material official filings.
    requested_rows = tuple(event for event in events if event.symbol in requested)
    other_resolved = tuple(
        event
        for event in events
        if event.symbol is not None and event.symbol not in requested
    )
    company_only = tuple(event for event in events if event.symbol is None)
    return (*requested_rows, *other_resolved, *company_only)[:limit]


def _symbols(values: Sequence[str]) -> tuple[str, ...]:
    if isinstance(values, (str, bytes, bytearray)) or not isinstance(values, Sequence):
        raise TypeError("symbols must be a sequence")
    result = tuple(dict.fromkeys(_symbol(value) for value in values))
    if not result:
        raise ValueError("symbols cannot be empty")
    return result


def _preferred_symbols(values: Sequence[str]) -> tuple[str, ...]:
    if (
        isinstance(values, (str, bytes, bytearray))
        or not isinstance(values, Sequence)
    ):
        raise TypeError("preferred_symbols must be a sequence")
    return tuple(dict.fromkeys(_symbol(value) for value in values))


def _symbol(value: object) -> str:
    if not isinstance(value, str):
        raise TypeError("ticker symbol must be a string")
    symbol = value.strip().upper()
    if (
        not symbol
        or len(symbol) > 12
        or not symbol.replace(".", "").isalnum()
    ):
        raise ValueError("ticker symbol is invalid")
    return symbol


def _aware(value: datetime, *, field: str) -> datetime:
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise ValueError(f"{field} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _bounded_seconds(
    value: object,
    *,
    field: str,
    minimum: float,
    maximum: float,
) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{field} must be numeric")
    checked = float(value)
    if not minimum <= checked <= maximum:
        raise ValueError(f"{field} must be between {minimum} and {maximum}")
    return checked


__all__ = [
    "MAXIMUM_SEC_ATOM_RESPONSE_BYTES",
    "MAXIMUM_SEC_TICKER_RESPONSE_BYTES",
    "SEC_COMPANY_TICKERS_EXCHANGE_URL",
    "SEC_CURRENT_8K_ATOM_URL",
    "SEC_USER_AGENT",
    "SecAtomHttpsTransport",
    "SecCikTickerResolver",
    "SecCompanyTickersHttpsTransport",
    "SecCurrent8KProvider",
    "SecProviderRateLimited",
    "SecProviderTimeout",
    "SecProviderTransportError",
    "SecTickerMappingError",
]
