"""Point-in-time news and earnings provider adapters.

Only metadata, summaries, and source URLs are returned.  Full copyrighted
article bodies are intentionally not copied into the learning ledger.
"""
from __future__ import annotations

import hashlib
import json
import math
import re
from threading import Lock
import time
from urllib.error import HTTPError
import urllib.parse
import urllib.request
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Callable, Iterable, Mapping, Protocol, Sequence

from options_copilot.storage.canonical import canonical_hash, utc_datetime

from .sec_identity import sec_filer_group_identity
from options_copilot.providers.transport import (
    BoundedTransportError,
    read_bounded_response,
    remaining_timeout,
)
from options_copilot.providers.entity_linking import link_news_entity


FINNHUB_COMPANY_NEWS_URL = "https://finnhub.io/api/v1/company-news"
FINNHUB_EARNINGS_CALENDAR_URL = "https://finnhub.io/api/v1/calendar/earnings"
ALPHA_VANTAGE_NEWS_URL = "https://www.alphavantage.co/query"
MAXIMUM_OPTIONAL_PROVIDER_RESPONSE_BYTES = 1024 * 1024
_ALPHA_VANTAGE_MAX_TICKERS_PER_REQUEST = 10

_JSON_CONTENT_TYPES = frozenset({"application/json", "text/json"})
_SAFE_CHARSETS = frozenset({"utf-8", "utf8", "us-ascii", "ascii"})
_REASON_CODE = re.compile(r"^[A-Z][A-Z0-9_]{0,63}$")
_PROVIDER_STORY_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,159}$")
_SYMBOL_BINDING_STATUSES = frozenset(
    {
        "SOURCE_DECLARED",
        "VERIFIED_PROVIDER_RELATED",
        "PROVIDER_RELATED_UNVERIFIED",
        "UNBOUND",
    }
)
class ProviderUnavailable(RuntimeError):
    pass


class ProviderTransportFailure(ProviderUnavailable):
    """A source-owned fixed-code failure with no untrusted exception state."""

    def __init__(self, reason: str) -> None:
        checked = str(reason or "").strip().upper()
        if _REASON_CODE.fullmatch(checked) is None:
            checked = "REQUEST_FAILED"
        self.reason = checked
        super().__init__(checked)


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: ANN001
        return None


class _SourcePacingLimiter:
    """One source-local non-blocking limiter; account quota stays unverified."""

    def __init__(
        self,
        *,
        minimum_interval_seconds: float,
        monotonic_clock: Callable[[], float],
    ) -> None:
        interval = float(minimum_interval_seconds)
        if not math.isfinite(interval) or interval < 0:
            raise ValueError("minimum_interval_seconds must be finite and non-negative")
        self._minimum_interval = interval
        self._clock = monotonic_clock
        self._lock = Lock()
        self._last_request_at: float | None = None

    def acquire(self) -> None:
        try:
            observed = float(self._clock())
        except Exception:
            raise ProviderTransportFailure("PACING_UNVERIFIED") from None
        if not math.isfinite(observed):
            raise ProviderTransportFailure("PACING_UNVERIFIED")
        with self._lock:
            previous = self._last_request_at
            if (
                previous is not None
                and observed >= previous
                and observed - previous < self._minimum_interval
            ):
                raise ProviderTransportFailure("PACING_LIMITED")
            self._last_request_at = observed


class _StrictJsonHttpsTransport:
    """Shared mechanics for one source; subclasses retain endpoint policy."""

    source_reason_prefix = "PROVIDER"

    def __init__(
        self,
        *,
        opener: object | None,
        monotonic_clock: Callable[[], float] | None,
        minimum_interval_seconds: float,
    ) -> None:
        self._opener = opener or urllib.request.build_opener(
            urllib.request.ProxyHandler(),
            _NoRedirect(),
        )
        self._monotonic = monotonic_clock or time.monotonic
        self._pacing = _SourcePacingLimiter(
            minimum_interval_seconds=minimum_interval_seconds,
            monotonic_clock=self._monotonic,
        )

    def _get(
        self,
        url: str,
        *,
        headers: Mapping[str, str],
        timeout_seconds: float,
    ) -> object:
        checked_url = self._allowed_url(url)
        checked_headers = self._headers(headers)
        timeout = _bounded_timeout(timeout_seconds)
        try:
            started = float(self._monotonic())
        except Exception:
            raise ProviderTransportFailure("REQUEST_TIMEOUT") from None
        if not math.isfinite(started):
            raise ProviderTransportFailure("REQUEST_TIMEOUT")
        deadline_at = started + timeout
        self._pacing.acquire()
        request = urllib.request.Request(
            checked_url,
            data=None,
            headers={**checked_headers, "Accept-Encoding": "identity"},
            method="GET",
        )
        response = None
        failure: ProviderTransportFailure | None = None
        try:
            attempt_timeout = remaining_timeout(
                deadline_at,
                self._monotonic,
                timeout,
            )
            response = self._opener.open(request, timeout=attempt_timeout)  # type: ignore[attr-defined]
        except ProviderTransportFailure:
            raise
        except BoundedTransportError as exc:
            failure = ProviderTransportFailure(exc.reason)
        except TimeoutError:
            failure = ProviderTransportFailure("REQUEST_TIMEOUT")
        except HTTPError as exc:
            _close_response(exc)
            failure = ProviderTransportFailure(
                "RATE_LIMITED" if int(getattr(exc, "code", 0)) == 429 else "HTTP_ERROR"
            )
        except Exception:
            failure = ProviderTransportFailure("REQUEST_FAILED")
        if failure is not None:
            raise failure
        if response is None:
            raise ProviderTransportFailure("REQUEST_FAILED")
        try:
            if int(getattr(response, "status", 200)) != 200:
                raise ProviderTransportFailure("HTTP_ERROR")
            final_url = str(response.geturl())
            if self._allowed_url(final_url) != checked_url:
                raise ProviderTransportFailure("REDIRECT_FORBIDDEN")
            response_headers = getattr(response, "headers", None)
            encoding = str(_header(response_headers, "Content-Encoding") or "identity")
            if encoding.strip().lower() not in {"", "identity"}:
                raise ProviderTransportFailure("ENCODING_INVALID")
            content_type = str(_header(response_headers, "Content-Type") or "")
            media_type, charset = _json_media_type(content_type)
            if media_type not in _JSON_CONTENT_TYPES:
                raise ProviderTransportFailure("CONTENT_TYPE_INVALID")
            if charset not in _SAFE_CHARSETS:
                raise ProviderTransportFailure("ENCODING_INVALID")
        except ProviderTransportFailure:
            _close_response(response)
            raise
        except Exception:
            _close_response(response)
            raise ProviderTransportFailure("INVALID_RESPONSE") from None
        try:
            body = read_bounded_response(
                response,
                MAXIMUM_OPTIONAL_PROVIDER_RESPONSE_BYTES,
            )
        except BoundedTransportError as exc:
            raise ProviderTransportFailure(exc.reason) from None
        return _decode_strict_json(body, charset=charset)

    def _allowed_url(self, url: str) -> str:
        raise NotImplementedError

    def _headers(self, headers: Mapping[str, str]) -> dict[str, str]:
        raise NotImplementedError


class FinnhubHttpsTransport(_StrictJsonHttpsTransport):
    """Exact no-redirect JSON GET transport for Finnhub evidence endpoints."""

    def __init__(
        self,
        *,
        opener: object | None = None,
        monotonic_clock: Callable[[], float] | None = None,
    ) -> None:
        super().__init__(
            opener=opener,
            monotonic_clock=monotonic_clock,
            minimum_interval_seconds=1.0,
        )

    def __call__(
        self,
        url: str,
        *,
        headers: Mapping[str, str],
        timeout_seconds: float,
    ) -> object:
        return self._get(url, headers=headers, timeout_seconds=timeout_seconds)

    def _allowed_url(self, url: str) -> str:
        return _allowed_finnhub_url(url)

    def _headers(self, headers: Mapping[str, str]) -> dict[str, str]:
        if not isinstance(headers, Mapping) or set(headers) != {
            "Accept",
            "X-Finnhub-Token",
        }:
            raise ProviderTransportFailure("HEADERS_NOT_ALLOWED")
        token = headers.get("X-Finnhub-Token")
        if (
            headers.get("Accept") != "application/json"
            or not isinstance(token, str)
            or not token
            or token != token.strip()
            or "\x00" in token
        ):
            raise ProviderTransportFailure("HEADERS_NOT_ALLOWED")
        return {"Accept": "application/json", "X-Finnhub-Token": token}


class AlphaVantageHttpsTransport(_StrictJsonHttpsTransport):
    """Exact no-redirect JSON GET transport for Alpha Vantage news."""

    def __init__(
        self,
        *,
        opener: object | None = None,
        monotonic_clock: Callable[[], float] | None = None,
    ) -> None:
        super().__init__(
            opener=opener,
            monotonic_clock=monotonic_clock,
            minimum_interval_seconds=12.0,
        )

    def __call__(
        self,
        url: str,
        *,
        headers: Mapping[str, str],
        timeout_seconds: float,
    ) -> object:
        return self._get(url, headers=headers, timeout_seconds=timeout_seconds)

    def _allowed_url(self, url: str) -> str:
        return _allowed_alpha_vantage_url(url)

    def _headers(self, headers: Mapping[str, str]) -> dict[str, str]:
        if not isinstance(headers, Mapping) or set(headers) != {"Accept"}:
            raise ProviderTransportFailure("HEADERS_NOT_ALLOWED")
        if headers.get("Accept") != "application/json":
            raise ProviderTransportFailure("HEADERS_NOT_ALLOWED")
        return {"Accept": "application/json"}


@dataclass(frozen=True, slots=True)
class SymbolBindingProof:
    """Versioned evidence for one provider-to-issuer symbol binding."""

    schema_version: int
    method: str
    provider_adapter: str
    requested_symbol: str
    provider_symbols: tuple[str, ...]
    corroborating_terms: tuple[str, ...]
    verified: bool

    def __post_init__(self) -> None:
        if self.schema_version != 1:
            raise ValueError("symbol binding proof schema_version must be 1")
        method = str(self.method or "").strip().upper()
        adapter = str(self.provider_adapter or "").strip().upper()
        if _REASON_CODE.fullmatch(method) is None:
            raise ValueError("symbol binding proof method is invalid")
        if _REASON_CODE.fullmatch(adapter) is None:
            raise ValueError("symbol binding proof provider_adapter is invalid")
        requested = _symbol(self.requested_symbol)
        provider_symbols = tuple(
            dict.fromkeys(_symbol(value) for value in self.provider_symbols)
        )
        if requested not in provider_symbols:
            raise ValueError("symbol binding proof does not contain requested_symbol")
        terms = tuple(
            dict.fromkeys(str(value).strip().upper() for value in self.corroborating_terms)
        )
        if any(not value or len(value) > 160 for value in terms):
            raise ValueError("symbol binding proof corroborating_terms are invalid")
        if not isinstance(self.verified, bool):
            raise TypeError("symbol binding proof verified must be a bool")
        object.__setattr__(self, "method", method)
        object.__setattr__(self, "provider_adapter", adapter)
        object.__setattr__(self, "requested_symbol", requested)
        object.__setattr__(self, "provider_symbols", provider_symbols)
        object.__setattr__(self, "corroborating_terms", terms)

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "method": self.method,
            "provider_adapter": self.provider_adapter,
            "requested_symbol": self.requested_symbol,
            "provider_symbols": list(self.provider_symbols),
            "corroborating_terms": list(self.corroborating_terms),
            "verified": self.verified,
        }


@dataclass(frozen=True, slots=True)
class NewsEvent:
    event_id: str
    symbol: str | None
    source: str
    headline: str
    summary: str
    url: str
    published_at: datetime
    first_seen_at: datetime
    ingested_at: datetime
    sentiment_score: Decimal | None = None
    source_rank: int = 2
    observed_at: datetime | None = None
    source_id: str | None = None
    content_hash: str | None = None
    provenance: tuple[str, ...] = ()
    status: str = "ACTIVE"
    decision_authority: str = "SUPPORTING_ONLY"
    entity_id: str | None = None
    source_tier: int | None = None
    lineage_id: str | None = None
    evidence_ids: tuple[str, ...] = ()
    provider_adapter: str | None = None
    symbol_binding_status: str = "SOURCE_DECLARED"
    symbol_binding_proof: SymbolBindingProof | None = None
    provider_story_id: str | None = None

    def __post_init__(self) -> None:
        if self.decision_authority != "SUPPORTING_ONLY":
            raise ValueError("news events are SUPPORTING_ONLY")
        for field in ("published_at", "first_seen_at", "ingested_at"):
            object.__setattr__(self, field, utc_datetime(getattr(self, field), field=field))
        observed = self.observed_at or self.ingested_at
        object.__setattr__(self, "observed_at", utc_datetime(observed, field="observed_at"))
        if self.published_at > self.first_seen_at or self.first_seen_at > self.ingested_at or self.ingested_at > self.observed_at:
            raise ValueError("news event timestamps must be ordered published/first_seen/ingested/observed")
        source_id = self.source_id or self.event_id
        object.__setattr__(self, "source_id", source_id)
        entity_id = str(self.entity_id or self.symbol or "MARKET").strip()
        if not entity_id or len(entity_id) > 160:
            raise ValueError("news event entity_id is invalid")
        object.__setattr__(self, "entity_id", entity_id)
        source_name = " ".join(self.source.upper().split())
        fixed_tier = (
            0
            if source_name in {"SEC", "SEC/XBRL"}
            else 1
            if source_name in {"COMPANY IR", "ISSUER IR"}
            else self.source_rank
            if self.source_tier is None
            else self.source_tier
        )
        if (
            isinstance(fixed_tier, bool)
            or not isinstance(fixed_tier, int)
            or fixed_tier < 0
        ):
            raise ValueError("news event source_tier must be a non-negative integer")
        object.__setattr__(self, "source_tier", fixed_tier)
        lineage_id = str(self.lineage_id or source_id).strip()
        if not lineage_id or len(lineage_id) > 240:
            raise ValueError("news event lineage_id is invalid")
        object.__setattr__(self, "lineage_id", lineage_id)
        evidence_ids = tuple(
            dict.fromkeys(
                str(item).strip()
                for item in (self.evidence_ids or (source_id,))
            )
        )
        if (
            not evidence_ids
            or any(not item or len(item) > 240 for item in evidence_ids)
            or len(evidence_ids) > 64
        ):
            raise ValueError("news event evidence_ids are invalid")
        object.__setattr__(self, "evidence_ids", evidence_ids)
        adapter = str(self.provider_adapter or "").strip().upper() or None
        if adapter is not None and _REASON_CODE.fullmatch(adapter) is None:
            raise ValueError("news event provider_adapter is invalid")
        object.__setattr__(self, "provider_adapter", adapter)
        binding_status = str(self.symbol_binding_status or "").strip().upper()
        if self.symbol is None:
            binding_status = "UNBOUND"
        if binding_status not in _SYMBOL_BINDING_STATUSES:
            raise ValueError("news event symbol_binding_status is invalid")
        if self.symbol is not None and binding_status == "UNBOUND":
            raise ValueError("bound news symbol cannot use UNBOUND status")
        proof = self.symbol_binding_proof
        if proof is not None and not isinstance(proof, SymbolBindingProof):
            raise TypeError("symbol_binding_proof must be SymbolBindingProof or None")
        if proof is not None and (
            self.symbol is None
            or proof.requested_symbol != self.symbol.upper()
            or proof.provider_adapter != adapter
        ):
            raise ValueError("symbol binding proof does not match news event")
        if binding_status == "VERIFIED_PROVIDER_RELATED" and (
            adapter is None or proof is None or proof.verified is not True
        ):
            raise ValueError("verified provider binding requires verified proof")
        object.__setattr__(self, "symbol_binding_status", binding_status)
        provider_story_id = str(self.provider_story_id or "").strip() or None
        if (
            provider_story_id is not None
            and _PROVIDER_STORY_ID.fullmatch(provider_story_id) is None
        ):
            raise ValueError("news event provider_story_id is invalid")
        object.__setattr__(self, "provider_story_id", provider_story_id)
        if not self.provenance:
            object.__setattr__(self, "provenance", (self.source,))
        if self.content_hash is None:
            object.__setattr__(self, "content_hash", canonical_hash({
                "symbol": self.symbol, "headline": self.headline, "summary": self.summary,
                "url": self.url, "published_at": self.published_at,
            }))

    @property
    def identity_key(self) -> tuple[str, str, str]:
        return (
            self.entity_id,
            " ".join(self.headline.lower().split()),
            self.published_at.replace(second=0, microsecond=0).isoformat(),
        )


@dataclass(frozen=True, slots=True)
class EarningsEvent:
    event_id: str
    symbol: str
    report_date: date
    hour: str | None
    eps_estimate: Decimal | None
    revenue_estimate: Decimal | None
    source: str
    first_seen_at: datetime
    ingested_at: datetime
    observed_at: datetime | None = None
    source_id: str | None = None
    content_hash: str | None = None
    provenance: tuple[str, ...] = ()
    status: str = "ACTIVE"
    decision_authority: str = "SUPPORTING_ONLY"
    published_at: datetime | None = None

    def __post_init__(self) -> None:
        if self.decision_authority != "SUPPORTING_ONLY":
            raise ValueError("earnings events are SUPPORTING_ONLY")
        for field in ("first_seen_at", "ingested_at"):
            object.__setattr__(self, field, utc_datetime(getattr(self, field), field=field))
        published = self.published_at or self.first_seen_at
        object.__setattr__(self, "published_at", utc_datetime(published, field="published_at"))
        observed = self.observed_at or self.ingested_at
        object.__setattr__(self, "observed_at", utc_datetime(observed, field="observed_at"))
        if self.published_at > self.first_seen_at or self.first_seen_at > self.ingested_at or self.ingested_at > self.observed_at:
            raise ValueError("earnings timestamps must be ordered")
        object.__setattr__(self, "source_id", self.source_id or self.event_id)
        if not self.provenance:
            object.__setattr__(self, "provenance", (self.source,))
        if self.content_hash is None:
            object.__setattr__(self, "content_hash", canonical_hash({
                "symbol": self.symbol, "report_date": self.report_date, "hour": self.hour,
                "eps_estimate": self.eps_estimate, "revenue_estimate": self.revenue_estimate,
            }))


class JsonTransport(Protocol):
    def __call__(
        self,
        url: str,
        *,
        headers: Mapping[str, str],
        timeout_seconds: float,
    ) -> object: ...


class SecretReader(Protocol):
    def get(self, name: str) -> str | None: ...


class FinnhubEventProvider:
    def __init__(
        self,
        secrets: SecretReader,
        *,
        transport: JsonTransport | None = None,
        now: Callable[[], datetime] | None = None,
        timeout_seconds: float = 8.0,
        max_news_symbols_per_cycle: int | None = None,
    ) -> None:
        if max_news_symbols_per_cycle is not None and (
            isinstance(max_news_symbols_per_cycle, bool)
            or not isinstance(max_news_symbols_per_cycle, int)
            or not 1 <= max_news_symbols_per_cycle <= 1000
        ):
            raise ValueError(
                "max_news_symbols_per_cycle must be between 1 and 1000"
            )
        self._secrets = secrets
        self._transport = transport or FinnhubHttpsTransport()
        self._now = now or (lambda: datetime.now(timezone.utc))
        self._timeout = timeout_seconds
        self._max_news_symbols_per_cycle = max_news_symbols_per_cycle
        self._news_symbol_cursor: int | None = None
        self.health = "READY"
        self.health_reason: str | None = None
        self.pacing = "PACING_UNVERIFIED"
        self.last_success_at: datetime | None = None
        self.last_observed_at: datetime | None = None
        self._requested_symbol_count = 0
        self._queried_symbol_count = 0
        self._coverage_status = "FULL"
        self._coverage_reason: str | None = None

    def news(
        self,
        symbols: Sequence[str],
        *,
        limit: int = 50,
    ) -> tuple[NewsEvent, ...]:
        """Implement the coordinator protocol over Finnhub company-news.

        Finnhub exposes one symbol per request, while ``NewsCoordinator`` owns
        a bounded multi-symbol protocol.  Fan out deterministically, retain a
        degraded state if any symbol request fails, then apply one global
        newest-first limit.
        """

        if (
            not symbols
            or isinstance(limit, bool)
            or not isinstance(limit, int)
            or not 1 <= limit <= 1000
        ):
            raise ValueError("symbols and a limit between 1 and 1000 are required")
        requested_tickers = tuple(dict.fromkeys(_symbol(value) for value in symbols))
        observed_now = _aware(self._now())
        tickers = requested_tickers
        if (
            self._max_news_symbols_per_cycle is not None
            and len(requested_tickers) > self._max_news_symbols_per_cycle
        ):
            if self._news_symbol_cursor is None:
                self._news_symbol_cursor = (
                    int(observed_now.timestamp() // 90) % len(requested_tickers)
                )
            start_index = self._news_symbol_cursor
            tickers = tuple(
                requested_tickers[(start_index + offset) % len(requested_tickers)]
                for offset in range(self._max_news_symbols_per_cycle)
            )
            self._news_symbol_cursor = (
                start_index + self._max_news_symbols_per_cycle
            ) % len(requested_tickers)
        self._requested_symbol_count = len(requested_tickers)
        self._queried_symbol_count = len(tickers)
        self._coverage_status = (
            "BOUNDED" if len(tickers) < len(requested_tickers) else "FULL"
        )
        self._coverage_reason = (
            "PROVIDER_SYMBOL_ROTATION"
            if self._coverage_status == "BOUNDED"
            else None
        )
        end = observed_now.date()
        start = end - timedelta(days=2)
        groups: list[tuple[NewsEvent, ...]] = []
        failure: tuple[str, str | None] | None = None
        for ticker in tickers:
            rows = self.company_news(ticker, start, end)
            groups.append(rows)
            if self.health != "READY" and failure is None:
                failure = (self.health, self.health_reason)
        merged = NewsAggregator.merge(*groups)
        if failure is None:
            self.health, self.health_reason = "READY", None
        else:
            self.health, self.health_reason = failure
        return merged[:limit]

    def company_news(self, symbol: str, start: date, end: date) -> tuple[NewsEvent, ...]:
        token = self._token()
        ticker = _symbol(symbol)
        query = urllib.parse.urlencode(
            {"symbol": ticker, "from": start.isoformat(), "to": end.isoformat()}
        )
        try:
            payload = self._transport(
                f"{FINNHUB_COMPANY_NEWS_URL}?{query}",
                headers={"X-Finnhub-Token": token, "Accept": "application/json"},
                timeout_seconds=self._timeout,
            )
        except Exception as exc:
            self._set_failure(exc)
            return ()
        if not isinstance(payload, list):
            self.health, self.health_reason = "BAD_JSON", "response_not_list"
            return ()
        observed = _aware(self._now())
        events: list[NewsEvent] = []
        binding_rejections = 0
        related_match_count = 0
        unverified_related_count = 0
        invalid_verified_count = 0
        for item in payload:
            if not isinstance(item, dict):
                continue
            related_symbols = _finnhub_related_symbols(item.get("related"))
            if related_symbols is None or ticker not in related_symbols:
                binding_rejections += 1
                continue
            related_match_count += 1
            headline = str(item.get("headline") or "").strip()
            published = _unix_timestamp(item.get("datetime"))
            if not headline or published is None:
                invalid_verified_count += 1
                continue
            source = str(item.get("source") or "Finnhub").strip()
            summary = str(item.get("summary") or "").strip()
            url = str(item.get("url") or "").strip()
            raw_story_id = str(item.get("id") or "").strip()
            provider_story_id = (
                f"FINNHUB:{raw_story_id}"
                if _PROVIDER_STORY_ID.fullmatch(raw_story_id) is not None
                else None
            )
            event_id = (
                _event_id("news", provider_story_id)
                if provider_story_id is not None
                else _event_id(
                    "news",
                    ticker,
                    source,
                    headline,
                    published.isoformat(),
                )
            )
            # Finnhub's ``related`` field is provider metadata, not issuer
            # identity proof.  Require the exact requested ticker to also be
            # present in the supplied headline/summary before the binding may
            # feed symbol-aware research.  Otherwise retain the item visibly
            # but quarantine the requested symbol.
            entity_link = link_news_entity(
                headline,
                summary,
                allowed_symbols=(ticker,),
            )
            entity_verified = entity_link.symbol == ticker and entity_link.method in {
                "CONTROLLED_ALIAS",
                "EXPLICIT_CASHTAG",
                "EXPLICIT_EXCHANGE_TICKER",
            }
            corroborating_terms = (
                (
                    ticker,
                    f"METHOD={entity_link.method}",
                    f"CATALOG_VERSION={entity_link.catalog_version}",
                    f"CATALOG_HASH={entity_link.catalog_hash.upper()}",
                )
                if entity_verified
                else ()
            )
            binding_proof = SymbolBindingProof(
                schema_version=1,
                method="PROVIDER_RELATED_PLUS_ENTITY_LINK",
                provider_adapter="FINNHUB",
                requested_symbol=ticker,
                provider_symbols=related_symbols,
                corroborating_terms=corroborating_terms,
                verified=entity_verified,
            )
            if not binding_proof.verified:
                unverified_related_count += 1
            events.append(
                NewsEvent(
                    event_id=event_id,
                    symbol=ticker,
                    source=source,
                    headline=headline[:500],
                    summary=summary[:2000],
                    url=url[:2000],
                    published_at=published,
                    first_seen_at=observed,
                    ingested_at=observed,
                    source_rank=2,
                    provenance=("FINNHUB", source),
                    provider_adapter="FINNHUB",
                    source_id=provider_story_id or event_id,
                    lineage_id=provider_story_id or event_id,
                    provider_story_id=provider_story_id,
                    symbol_binding_status=(
                        "VERIFIED_PROVIDER_RELATED"
                        if binding_proof.verified
                        else "PROVIDER_RELATED_UNVERIFIED"
                    ),
                    symbol_binding_proof=binding_proof,
                )
            )
        self.last_observed_at = observed
        if payload and not events:
            self.health = "MISSING_FIELDS"
            self.health_reason = (
                "NO_VERIFIED_RELATED_RECORDS"
                if related_match_count == 0 and binding_rejections
                else "NO_USABLE_RECORDS"
            )
        elif binding_rejections:
            self.health = "PARTIAL_PARSE"
            self.health_reason = "SYMBOL_BINDING_REJECTED"
        elif invalid_verified_count:
            self.health = "PARTIAL_PARSE"
            self.health_reason = "VERIFIED_RELATED_RECORD_REJECTED"
        elif unverified_related_count:
            self.health = "PARTIAL_PARSE"
            self.health_reason = "SYMBOL_BINDING_UNVERIFIED"
        else:
            self.health, self.health_reason = "READY", None
        if self.health == "READY":
            self.last_success_at = observed
        return tuple(sorted(events, key=lambda event: event.published_at, reverse=True))

    def earnings_calendar(self, start: date, end: date) -> tuple[EarningsEvent, ...]:
        token = self._token()
        query = urllib.parse.urlencode({"from": start.isoformat(), "to": end.isoformat()})
        try:
            payload = self._transport(
                f"{FINNHUB_EARNINGS_CALENDAR_URL}?{query}",
                headers={"X-Finnhub-Token": token, "Accept": "application/json"},
                timeout_seconds=self._timeout,
            )
        except Exception as exc:
            self._set_failure(exc)
            return ()
        rows = payload.get("earningsCalendar") if isinstance(payload, dict) else None
        if not isinstance(rows, list):
            self.health, self.health_reason = "BAD_JSON", "response_not_object"
            return ()
        observed = _aware(self._now())
        results: list[EarningsEvent] = []
        for item in rows:
            if not isinstance(item, dict):
                continue
            ticker = str(item.get("symbol") or "").strip().upper()
            raw_date = str(item.get("date") or "")
            try:
                report_date = date.fromisoformat(raw_date)
                ticker = _symbol(ticker)
            except ValueError:
                continue
            results.append(
                EarningsEvent(
                    event_id=_event_id("earnings", ticker, report_date.isoformat()),
                    symbol=ticker,
                    report_date=report_date,
                    hour=str(item.get("hour") or "").strip() or None,
                    eps_estimate=_decimal(item.get("epsEstimate")),
                    revenue_estimate=_decimal(item.get("revenueEstimate")),
                    source="Finnhub",
                    first_seen_at=observed,
                    ingested_at=observed,
                    published_at=observed,
                )
            )
        self.last_observed_at = observed
        self.health, self.health_reason = ("MISSING_FIELDS", "NO_USABLE_RECORDS") if rows and not results else ("READY", None)
        if self.health == "READY":
            self.last_success_at = observed
        return tuple(sorted(results, key=lambda event: (event.report_date, event.symbol)))

    def health_snapshot(self) -> dict[str, object]:
        configured = _secret_configured(self._secrets, "FINNHUB_API_KEY")
        observed = self.last_observed_at
        last_success = self.last_success_at
        reason = self.health_reason
        if reason == "SYMBOL_BINDING_UNVERIFIED":
            reason = "PROVIDER_RELATED_ENTITY_PROOF_MISSING"
        return {
            "source_id": "finnhub",
            "configured": configured,
            "readiness": self.health if configured else "NOT_CONFIGURED",
            "status": self.health if configured else "NOT_CONFIGURED",
            "observed_at": None if observed is None else observed.isoformat(),
            "as_of": None if observed is None else observed.isoformat(),
            "last_success_at": (
                None if last_success is None else last_success.isoformat()
            ),
            "freshness_age_seconds": _freshness_age(observed, last_success),
            "provenance": ("finnhub",),
            "pacing": self.pacing,
            "reason": reason if configured else "NOT_CONFIGURED",
            "requested_symbol_count": self._requested_symbol_count,
            "queried_symbol_count": self._queried_symbol_count,
            "coverage_status": self._coverage_status,
            "coverage_reason": self._coverage_reason,
            "decision_authority": "SUPPORTING_ONLY",
        }

    def _token(self) -> str:
        token = self._secrets.get("FINNHUB_API_KEY")
        if not token:
            raise ProviderUnavailable("FINNHUB_API_KEY is not configured")
        return token

    def _set_failure(self, exc: Exception) -> None:
        self.last_observed_at = _aware(self._now())
        reason = _fixed_transport_reason(exc)
        self.health, self.health_reason = _health_for_reason(reason)


class AlphaVantageNewsProvider:
    """Low-frequency cross-check provider (free tier is currently 25 calls/day)."""

    def __init__(
        self,
        secrets: SecretReader,
        *,
        transport: JsonTransport | None = None,
        now: Callable[[], datetime] | None = None,
        timeout_seconds: float = 8.0,
    ) -> None:
        self._secrets = secrets
        self._transport = transport or AlphaVantageHttpsTransport()
        self._now = now or (lambda: datetime.now(timezone.utc))
        self._timeout = timeout_seconds
        self.health = "READY"
        self.health_reason: str | None = None
        self.pacing = "PACING_UNVERIFIED"
        self.last_success_at: datetime | None = None
        self.last_observed_at: datetime | None = None
        self._requested_symbol_count = 0
        self._queried_symbol_count = 0
        self._coverage_status = "FULL"
        self._coverage_reason: str | None = None

    def news(self, symbols: Sequence[str], *, limit: int = 50) -> tuple[NewsEvent, ...]:
        if not symbols or limit <= 0 or limit > 1000:
            raise ValueError("symbols and a limit between 1 and 1000 are required")
        token = self._secrets.get("ALPHA_VANTAGE_API_KEY")
        if not token:
            raise ProviderUnavailable("ALPHA_VANTAGE_API_KEY is not configured")
        requested_symbols = tuple(dict.fromkeys(_symbol(value) for value in symbols))
        queried_symbols = requested_symbols[:_ALPHA_VANTAGE_MAX_TICKERS_PER_REQUEST]
        self._requested_symbol_count = len(requested_symbols)
        self._queried_symbol_count = len(queried_symbols)
        self._coverage_status = (
            "BOUNDED" if len(queried_symbols) < len(requested_symbols) else "FULL"
        )
        self._coverage_reason = (
            "PROVIDER_TICKER_LIMIT" if self._coverage_status == "BOUNDED" else None
        )
        tickers = ",".join(queried_symbols)
        # Alpha Vantage requires the API key in its query contract.  The URL is
        # never logged and any transport error is redacted by this provider.
        query = urllib.parse.urlencode(
            {
                "function": "NEWS_SENTIMENT",
                "tickers": tickers,
                "limit": limit,
                "apikey": token,
            }
        )
        failure: ProviderUnavailable | None = None
        try:
            payload = self._transport(
                f"{ALPHA_VANTAGE_NEWS_URL}?{query}",
                headers={"Accept": "application/json"},
                timeout_seconds=self._timeout,
            )
        except Exception as exc:
            self.last_observed_at = _aware(self._now())
            reason = _fixed_transport_reason(exc)
            self.health, self.health_reason = _health_for_reason(reason)
            # Raise only after leaving the exception handler.  The transport
            # exception can contain the full request URL (and therefore the
            # query-string API key); retaining it as __cause__/__context__
            # would leak the credential through ordinary traceback logging.
            failure = ProviderUnavailable("ALPHA_VANTAGE_REQUEST_FAILED")
        if failure is not None:
            raise failure
        response_failure = _alpha_vantage_response_failure(payload)
        if response_failure is not None:
            observed = _aware(self._now())
            self.last_observed_at = observed
            self.health, self.health_reason = response_failure
            if self.health == "RATE_LIMITED":
                self.pacing = "RATE_LIMITED"
            return ()
        rows = payload.get("feed") if isinstance(payload, dict) else None
        if not isinstance(rows, list):
            self.last_observed_at = _aware(self._now())
            self.health, self.health_reason = "BAD_JSON", "feed_not_list"
            return ()
        observed = _aware(self._now())
        self.pacing = "PACING_UNVERIFIED"
        results: list[NewsEvent] = []
        for item in rows:
            if not isinstance(item, dict):
                continue
            headline = str(item.get("title") or "").strip()
            published = _alpha_timestamp(item.get("time_published"))
            if not headline or published is None:
                continue
            ticker_sentiment = item.get("ticker_sentiment")
            symbol = None
            score = _decimal(item.get("overall_sentiment_score"))
            if isinstance(ticker_sentiment, list):
                matches = [
                    entry
                    for entry in ticker_sentiment
                    if isinstance(entry, dict)
                    and str(entry.get("ticker") or "") in queried_symbols
                ]
                if matches:
                    symbol = str(matches[0].get("ticker") or "").upper() or None
                    score = _decimal(matches[0].get("ticker_sentiment_score")) or score
            binding_proof = (
                SymbolBindingProof(
                    schema_version=1,
                    method="PROVIDER_TICKER_SENTIMENT_EXACT",
                    provider_adapter="ALPHA_VANTAGE",
                    requested_symbol=symbol,
                    provider_symbols=(symbol,),
                    corroborating_terms=("TICKER_SENTIMENT",),
                    verified=True,
                )
                if symbol is not None
                else None
            )
            source = str(item.get("source") or "Alpha Vantage")
            results.append(
                NewsEvent(
                    event_id=_event_id("news", symbol or "MARKET", source, headline, published.isoformat()),
                    symbol=symbol,
                    source=source,
                    headline=headline[:500],
                    summary=str(item.get("summary") or "")[:2000],
                    url=str(item.get("url") or "")[:2000],
                    published_at=published,
                    first_seen_at=observed,
                    ingested_at=observed,
                    sentiment_score=score,
                    source_rank=3,
                    provenance=("ALPHA_VANTAGE", source),
                    provider_adapter="ALPHA_VANTAGE",
                    symbol_binding_status=(
                        "VERIFIED_PROVIDER_RELATED" if symbol else "UNBOUND"
                    ),
                    symbol_binding_proof=binding_proof,
                )
            )
        self.last_observed_at = observed
        self.health, self.health_reason = ("MISSING_FIELDS", "NO_USABLE_RECORDS") if rows and not results else ("READY", None)
        if self.health == "READY":
            self.last_success_at = observed
        return tuple(sorted(results, key=lambda event: event.published_at, reverse=True))

    def health_snapshot(self) -> dict[str, object]:
        configured = _secret_configured(self._secrets, "ALPHA_VANTAGE_API_KEY")
        observed = self.last_observed_at
        last_success = self.last_success_at
        return {
            "source_id": "alpha_vantage",
            "configured": configured,
            "readiness": self.health if configured else "NOT_CONFIGURED",
            "status": self.health if configured else "NOT_CONFIGURED",
            "observed_at": None if observed is None else observed.isoformat(),
            "as_of": None if observed is None else observed.isoformat(),
            "last_success_at": (
                None if last_success is None else last_success.isoformat()
            ),
            "freshness_age_seconds": _freshness_age(observed, last_success),
            "provenance": ("alpha_vantage",),
            "pacing": self.pacing,
            "reason": self.health_reason if configured else "NOT_CONFIGURED",
            "requested_symbol_count": self._requested_symbol_count,
            "queried_symbol_count": self._queried_symbol_count,
            "coverage_status": self._coverage_status,
            "coverage_reason": self._coverage_reason,
            "decision_authority": "SUPPORTING_ONLY",
        }


class NewsAggregator:
    """Deterministically deduplicate provider events without inventing facts."""

    @staticmethod
    def merge(*groups: Iterable[NewsEvent]) -> tuple[NewsEvent, ...]:
        selected: dict[tuple[str, str, str], list[NewsEvent]] = {}
        for event in (item for group in groups for item in group):
            selected.setdefault(event.identity_key, []).append(event)
        merged: list[NewsEvent] = []
        for events in selected.values():
            filer_identities = tuple(
                sec_filer_group_identity(
                    source=event.source,
                    source_id=str(event.source_id),
                    event_id=event.event_id,
                    lineage_id=event.lineage_id,
                    evidence_ids=event.evidence_ids,
                    url=event.url,
                    provider_story_id=event.provider_story_id,
                )
                for event in events
            )
            identity_groups: tuple[list[NewsEvent], ...] = (events,)
            if all(identity is not None for identity in filer_identities):
                by_filer: dict[str, list[NewsEvent]] = {}
                for event, identity in zip(events, filer_identities, strict=True):
                    assert identity is not None
                    by_filer.setdefault(identity, []).append(event)
                identity_groups = tuple(by_filer.values())
            for identity_group in identity_groups:
                merged.extend(_merge_news_identity_group(identity_group))
        return tuple(
            sorted(
                merged,
                key=lambda event: (
                    event.published_at,
                    -int(event.source_tier or 0),
                    event.event_id,
                ),
                reverse=True,
            )
        )


def _merge_news_identity_group(events: list[NewsEvent]) -> list[NewsEvent]:
    lineages: dict[str, list[NewsEvent]] = {}
    for event in events:
        assert event.lineage_id is not None
        lineages.setdefault(event.lineage_id, []).append(event)
    independent: list[NewsEvent] = []
    for lineage_events in lineages.values():
        lineage_hashes = {event.content_hash for event in lineage_events}
        if len(lineage_hashes) != 1:
            independent.extend(
                replace(event, status="CONFLICTED")
                for event in lineage_events
            )
            continue
        winner = min(
            lineage_events,
            key=lambda event: (event.source_tier, event.source, event.event_id),
        )
        independent.append(
            replace(
                winner,
                evidence_ids=_merged_evidence_ids(lineage_events),
                provenance=_merged_provenance(lineage_events),
            )
        )
    hashes = {event.content_hash for event in independent}
    if len(hashes) != 1:
        return [
            replace(event, status="CONFLICTED")
            for event in independent
        ]
    winner = min(
        independent,
        key=lambda event: (event.source_tier, event.source, event.event_id),
    )
    return [
        replace(
            winner,
            evidence_ids=_merged_evidence_ids(independent),
            provenance=_merged_provenance(independent),
        )
    ]


def _merged_evidence_ids(events: Iterable[NewsEvent]) -> tuple[str, ...]:
    return tuple(
        sorted(
            {
                evidence_id
                for event in events
                for evidence_id in event.evidence_ids
            }
        )
    )


def _merged_provenance(events: Iterable[NewsEvent]) -> tuple[str, ...]:
    return tuple(
        sorted(
            {
                source
                for event in events
                for source in event.provenance
            }
        )
    )


def _allowed_finnhub_url(value: object) -> str:
    text = str(value or "").strip()
    try:
        parsed = urllib.parse.urlsplit(text)
        query = urllib.parse.parse_qsl(
            parsed.query,
            keep_blank_values=True,
            strict_parsing=True,
        )
        port = parsed.port
    except (TypeError, ValueError):
        raise ProviderTransportFailure("URL_NOT_ALLOWED") from None
    if (
        parsed.scheme != "https"
        or parsed.hostname != "finnhub.io"
        or parsed.netloc != "finnhub.io"
        or parsed.username is not None
        or parsed.password is not None
        or port is not None
        or parsed.fragment
    ):
        raise ProviderTransportFailure("URL_NOT_ALLOWED")
    values = dict(query)
    if len(values) != len(query):
        raise ProviderTransportFailure("URL_NOT_ALLOWED")
    if parsed.path == "/api/v1/company-news":
        if tuple(values) != ("symbol", "from", "to"):
            raise ProviderTransportFailure("URL_NOT_ALLOWED")
        try:
            ticker = _symbol(values["symbol"])
            start = date.fromisoformat(values["from"])
            end = date.fromisoformat(values["to"])
        except (TypeError, ValueError):
            raise ProviderTransportFailure("URL_NOT_ALLOWED") from None
        if end < start:
            raise ProviderTransportFailure("URL_NOT_ALLOWED")
        canonical = FINNHUB_COMPANY_NEWS_URL + "?" + urllib.parse.urlencode(
            {"symbol": ticker, "from": start.isoformat(), "to": end.isoformat()}
        )
    elif parsed.path == "/api/v1/calendar/earnings":
        if tuple(values) != ("from", "to"):
            raise ProviderTransportFailure("URL_NOT_ALLOWED")
        try:
            start = date.fromisoformat(values["from"])
            end = date.fromisoformat(values["to"])
        except (TypeError, ValueError):
            raise ProviderTransportFailure("URL_NOT_ALLOWED") from None
        if end < start:
            raise ProviderTransportFailure("URL_NOT_ALLOWED")
        canonical = FINNHUB_EARNINGS_CALENDAR_URL + "?" + urllib.parse.urlencode(
            {"from": start.isoformat(), "to": end.isoformat()}
        )
    else:
        raise ProviderTransportFailure("URL_NOT_ALLOWED")
    if text != canonical:
        raise ProviderTransportFailure("URL_NOT_ALLOWED")
    return canonical


def _allowed_alpha_vantage_url(value: object) -> str:
    text = str(value or "").strip()
    try:
        parsed = urllib.parse.urlsplit(text)
        query = urllib.parse.parse_qsl(
            parsed.query,
            keep_blank_values=True,
            strict_parsing=True,
        )
        port = parsed.port
    except (TypeError, ValueError):
        raise ProviderTransportFailure("URL_NOT_ALLOWED") from None
    values = dict(query)
    if (
        parsed.scheme != "https"
        or parsed.hostname != "www.alphavantage.co"
        or parsed.netloc != "www.alphavantage.co"
        or parsed.username is not None
        or parsed.password is not None
        or port is not None
        or parsed.path != "/query"
        or parsed.fragment
        or len(values) != len(query)
        or tuple(values) != ("function", "tickers", "limit", "apikey")
        or values.get("function") != "NEWS_SENTIMENT"
    ):
        raise ProviderTransportFailure("URL_NOT_ALLOWED")
    raw_tickers = values.get("tickers", "")
    try:
        tickers = tuple(_symbol(item) for item in raw_tickers.split(","))
        limit = int(values.get("limit", ""))
    except (TypeError, ValueError):
        raise ProviderTransportFailure("URL_NOT_ALLOWED") from None
    api_key = values.get("apikey")
    if (
        not tickers
        or len(tickers) != len(set(tickers))
        or not 1 <= limit <= 1000
        or not isinstance(api_key, str)
        or not api_key
        or api_key != api_key.strip()
        or len(api_key) > 8192
        or "\x00" in api_key
    ):
        raise ProviderTransportFailure("URL_NOT_ALLOWED")
    canonical = ALPHA_VANTAGE_NEWS_URL + "?" + urllib.parse.urlencode(
        {
            "function": "NEWS_SENTIMENT",
            "tickers": ",".join(tickers),
            "limit": limit,
            "apikey": api_key,
        }
    )
    if text != canonical:
        raise ProviderTransportFailure("URL_NOT_ALLOWED")
    return canonical


def _bounded_timeout(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ProviderTransportFailure("REQUEST_TIMEOUT")
    timeout = float(value)
    if not math.isfinite(timeout) or not 0 < timeout <= 30:
        raise ProviderTransportFailure("REQUEST_TIMEOUT")
    return timeout


def _header(headers: object, name: str) -> object | None:
    getter = getattr(headers, "get", None)
    if callable(getter):
        value = getter(name)
        if value is not None:
            return value
    if isinstance(headers, Mapping):
        wanted = name.lower()
        for key, value in headers.items():
            if str(key).lower() == wanted:
                return value
    return None


def _json_media_type(value: str) -> tuple[str, str]:
    parts = [part.strip() for part in value.split(";")]
    media_type = parts[0].lower() if parts else ""
    charset = "utf-8"
    for part in parts[1:]:
        if part.lower().startswith("charset="):
            charset = part.split("=", 1)[1].strip().strip("\"'").lower()
        elif part:
            raise ProviderTransportFailure("CONTENT_TYPE_INVALID")
    return media_type, charset


def _decode_strict_json(body: bytes, *, charset: str) -> object:
    try:
        payload = json.loads(
            body.decode(charset, errors="strict"),
            object_pairs_hook=_object_without_duplicate_keys,
            parse_constant=_reject_json_constant,
            parse_float=_finite_float,
        )
    except Exception:
        raise ProviderTransportFailure("BAD_JSON") from None
    if not isinstance(payload, (dict, list)):
        raise ProviderTransportFailure("BAD_JSON")
    return payload


def _object_without_duplicate_keys(
    pairs: list[tuple[str, object]],
) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON object key")
        result[key] = value
    return result


def _reject_json_constant(_value: str) -> object:
    raise ValueError("non-finite JSON constant")


def _finite_float(value: str) -> float:
    parsed = float(value)
    if not math.isfinite(parsed):
        raise ValueError("non-finite JSON number")
    return parsed


def _close_response(response: object) -> None:
    try:
        closer = getattr(response, "close", None)
        if callable(closer):
            closer()
    except Exception:
        return


def _fixed_transport_reason(exc: Exception) -> str:
    if isinstance(exc, ProviderTransportFailure):
        return exc.reason
    if isinstance(exc, TimeoutError):
        return "REQUEST_TIMEOUT"
    return "REQUEST_FAILED"


def _health_for_reason(reason: str) -> tuple[str, str]:
    if reason in {"RATE_LIMITED", "PACING_LIMITED", "PACING_UNVERIFIED"}:
        return "LIMITED", "PACING_UNVERIFIED"
    if reason == "REQUEST_TIMEOUT":
        return "TIMEOUT", "REQUEST_TIMEOUT"
    if reason == "BAD_JSON":
        return "BAD_JSON", "BAD_JSON"
    return "DEGRADED", reason


def _freshness_age(
    observed_at: datetime | None,
    last_success_at: datetime | None,
) -> int | None:
    if observed_at is None or last_success_at is None:
        return None
    return max(0, int((observed_at - last_success_at).total_seconds()))


def _secret_configured(secrets: SecretReader, name: str) -> bool:
    try:
        value = secrets.get(name)
    except Exception:
        return False
    return isinstance(value, str) and bool(value)


def _symbol(value: str) -> str:
    cleaned = value.strip().upper()
    if not cleaned or len(cleaned) > 12 or not cleaned.replace(".", "").isalnum():
        raise ValueError("invalid US symbol")
    return cleaned


def _finnhub_related_symbols(value: object) -> tuple[str, ...] | None:
    """Parse Finnhub's string-valued ``related`` field without guessing."""

    if not isinstance(value, str):
        return None
    raw_parts = value.split(",")
    if not raw_parts:
        return None
    try:
        symbols = tuple(dict.fromkeys(_symbol(part) for part in raw_parts))
    except ValueError:
        return None
    return symbols or None


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return value


def _unix_timestamp(value: object) -> datetime | None:
    try:
        return datetime.fromtimestamp(int(value), tz=timezone.utc)
    except (TypeError, ValueError, OverflowError):
        return None


def _alpha_timestamp(value: object) -> datetime | None:
    raw = str(value or "")
    try:
        return datetime.strptime(raw, "%Y%m%dT%H%M%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _alpha_vantage_response_failure(
    payload: object,
) -> tuple[str, str] | None:
    """Classify Alpha Vantage control envelopes without retaining their text."""

    if not isinstance(payload, Mapping):
        return "BAD_JSON", "response_not_object"
    for key, reason in (
        ("Error Message", "PROVIDER_ERROR_RESPONSE"),
        ("Information", "PROVIDER_INFORMATION_RESPONSE"),
        ("Note", "PROVIDER_NOTE_RESPONSE"),
    ):
        raw = payload.get(key)
        if raw is None:
            continue
        text = " ".join(str(raw).lower().split())
        if any(
            marker in text
            for marker in (
                "rate limit",
                "call frequency",
                "calls per minute",
                "requests per minute",
                "requests per day",
                "api request limit",
            )
        ):
            return "RATE_LIMITED", "RATE_LIMITED"
        return "DEGRADED", reason
    return None


def _decimal(value: object) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def _event_id(*parts: str) -> str:
    digest = hashlib.sha256("\x1f".join(parts).encode("utf-8")).hexdigest()
    return f"evt_{digest[:32]}"


__all__ = [
    "ALPHA_VANTAGE_NEWS_URL",
    "FINNHUB_COMPANY_NEWS_URL",
    "FINNHUB_EARNINGS_CALENDAR_URL",
    "MAXIMUM_OPTIONAL_PROVIDER_RESPONSE_BYTES",
    "AlphaVantageHttpsTransport",
    "AlphaVantageNewsProvider",
    "EarningsEvent",
    "FinnhubEventProvider",
    "FinnhubHttpsTransport",
    "JsonTransport",
    "NewsAggregator",
    "NewsEvent",
    "SymbolBindingProof",
    "ProviderTransportFailure",
    "ProviderUnavailable",
    "SecretReader",
]
