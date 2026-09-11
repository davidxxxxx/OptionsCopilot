from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json

from options_copilot.config import DEFAULT_NEWS_CORE_SYMBOLS
from options_copilot.providers.entity_linking import (
    COMPANY_ALIAS_CATALOG,
    ENTITY_LINK_CATALOG_HASH,
    ENTITY_LINK_CATALOG_VERSION,
    link_news_entity,
)
from options_copilot.providers.jin10 import Jin10EventProvider
from options_copilot.providers.jin10_mcp import Jin10McpError, Jin10McpNewsBatch
from options_copilot.news.macro_proxy import (
    MARKET_PROXY_MAPPING_HASH,
    MARKET_PROXY_MAPPING_VERSION,
    bind_market_proxy,
    bind_research_proxy,
)
from options_copilot.news.models import NewsInput


NOW = datetime(2026, 8, 6, 6, 30, tzinfo=timezone.utc)


class _Secrets:
    @staticmethod
    def get(name: str) -> str:
        assert name == "JIN10_MCP_TOKEN"
        return "fixture-token"


def _entity_audit(provenance: tuple[str, ...]) -> dict[str, str]:
    rows = [item for item in provenance if item.startswith("entity_link.audit=")]
    assert len(rows) == 1
    assert len(rows[0]) <= 256
    payload = json.loads(rows[0].removeprefix("entity_link.audit="))
    assert rows[0] == "entity_link.audit=" + json.dumps(
        payload,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    return payload


def test_versioned_catalog_covers_every_default_core_symbol() -> None:
    assert set(COMPANY_ALIAS_CATALOG) == set(DEFAULT_NEWS_CORE_SYMBOLS)
    assert ENTITY_LINK_CATALOG_VERSION == "2026-08-06.1"
    assert ENTITY_LINK_CATALOG_HASH == (
        "59c8cb92e287e61553f33b0bdcf130847dc54cb933612f6bd1bd72a5e99c9c05"
    )


def test_explicit_cashtag_and_exchange_ticker_win_over_aliases() -> None:
    cashtag = link_news_entity(
        "$NVDA. 上调指引，苹果公司也发布消息",
        "",
        allowed_symbols=("NVDA", "AAPL"),
    )
    exchange = link_news_entity(
        "NASDAQ: NVDA raises guidance while Microsoft comments",
        "",
        allowed_symbols=("NVDA", "MSFT"),
    )

    assert (cashtag.symbol, cashtag.method, cashtag.confidence) == (
        "NVDA",
        "EXPLICIT_CASHTAG",
        "1.0000",
    )
    assert (exchange.symbol, exchange.method, exchange.confidence) == (
        "NVDA",
        "EXPLICIT_EXCHANGE_TICKER",
        "0.9900",
    )


def test_alias_linking_is_deterministic_for_english_and_chinese_names() -> None:
    english = link_news_entity(
        "Microsoft announces a new cloud product",
        "",
        allowed_symbols=("MSFT", "NVDA"),
    )
    chinese = link_news_entity(
        "英伟达发布新一代芯片",
        "",
        allowed_symbols=("NVDA", "MSFT"),
    )

    assert (english.symbol, english.method, english.confidence) == (
        "MSFT",
        "CONTROLLED_ALIAS",
        "0.9000",
    )
    assert (chinese.symbol, chinese.method, chinese.confidence) == (
        "NVDA",
        "CONTROLLED_ALIAS",
        "0.9000",
    )


def test_bare_words_and_disallowed_entities_never_become_symbols() -> None:
    generic = link_news_entity(
        "AI helps all cat owners track market sentiment",
        "",
        allowed_symbols=("AI", "ALL", "CAT"),
    )
    disallowed = link_news_entity(
        "$NVDA 发布新芯片",
        "",
        allowed_symbols=("AAPL",),
    )

    assert (generic.symbol, generic.method, generic.confidence) == (
        None,
        "NO_MATCH",
        "0.0000",
    )
    assert (disallowed.symbol, disallowed.method) == (None, "NO_MATCH")


def test_multiple_different_entities_remain_unlinked_and_auditable() -> None:
    explicit = link_news_entity(
        "$NVDA 与 NYSE: JPM 同时发布消息",
        "",
        allowed_symbols=("NVDA", "JPM"),
    )
    aliases = link_news_entity(
        "Microsoft and Amazon.com announce separate products",
        "",
        allowed_symbols=("MSFT", "AMZN"),
    )

    assert (explicit.symbol, explicit.method, explicit.confidence) == (
        None,
        "AMBIGUOUS_EXPLICIT",
        "0.0000",
    )
    assert (aliases.symbol, aliases.method, aliases.confidence) == (
        None,
        "AMBIGUOUS_ALIAS",
        "0.0000",
    )


def test_jin10_provider_links_only_requested_symbols_and_persists_provenance() -> None:
    batch = Jin10McpNewsBatch(
        payloads={
            "list_flash": {
                "status": 200,
                "data": {
                    "items": [
                        {
                            "id": "flash-1",
                            "content": "$NVDA 上调业绩指引",
                            "time": "2026-08-06T14:29:00+08:00",
                        },
                        {
                            "id": "flash-2",
                            "content": "$AAPL 发布新品",
                            "time": "2026-08-06T14:28:00+08:00",
                        },
                    ]
                },
            }
        }
    )

    class Client:
        transport_verified = True

        @staticmethod
        def fetch_news(token: str, *, limit: int) -> Jin10McpNewsBatch:
            assert token == "fixture-token"
            assert limit == 10
            return batch

    provider = Jin10EventProvider(
        _Secrets(), mcp_client=Client(), now=lambda: NOW
    )
    events = provider.news(["NVDA"], limit=10)

    assert [event.symbol for event in events] == ["NVDA", None]
    assert all(event.decision_authority == "SUPPORTING_ONLY" for event in events)
    assert len(events[0].provenance) == 3
    assert _entity_audit(events[0].provenance) == {
        "catalog_hash": ENTITY_LINK_CATALOG_HASH,
        "catalog_version": ENTITY_LINK_CATALOG_VERSION,
        "confidence": "1.0000",
        "method": "EXPLICIT_CASHTAG",
    }
    assert _entity_audit(events[1].provenance)["method"] == "NO_MATCH"


def test_jin10_flash_without_provider_id_gets_stable_content_identity() -> None:
    batch = Jin10McpNewsBatch(
        payloads={
            "list_flash": {
                "status": 200,
                "data": {
                    "items": [
                        {
                            "content": "美国 CPI 数据即将公布",
                            "time": "2026-08-06T14:29:00+08:00",
                            "url": "https://flash.jin10.com/detail/cpi",
                        }
                    ]
                },
            }
        }
    )

    class Client:
        transport_verified = True

        @staticmethod
        def fetch_news(_token: str, *, limit: int) -> Jin10McpNewsBatch:
            assert limit == 10
            return batch

    provider = Jin10EventProvider(
        _Secrets(), mcp_client=Client(), now=lambda: NOW
    )

    first = provider.news(["SPY"], limit=10)
    second = provider.news(["SPY"], limit=10)

    assert len(first) == 1
    assert provider.health == "READY"
    assert first[0].source_id.startswith("evt_")
    assert first[0].source_id == second[0].source_id
    assert first[0].event_id == second[0].event_id


def test_jin10_cpi_gets_versioned_shadow_only_market_proxy() -> None:
    news = NewsInput(
        event_id="jin10-cpi-actual",
        headline="美国 CPI 同比低于预期",
        summary="美国消费者价格指数公布，核心 CPI 同比放缓。",
        source="Jin10",
        source_url="https://flash.jin10.com/detail/cpi",
        published_at=NOW - timedelta(minutes=1),
        first_seen_at=NOW,
        evidence_ids=("evidence-cpi",),
        symbols=(),
    )

    binding = bind_market_proxy(news, allowed_symbols=("SPY", "QQQ"))

    assert binding is not None
    assert binding.as_dict() == {
        "binding_role": "MARKET_PROXY",
        "event_category": "US_INFLATION",
        "source": "JIN10",
        "mapping_version": MARKET_PROXY_MAPPING_VERSION,
        "mapping_hash": MARKET_PROXY_MAPPING_HASH,
        "method": "DETERMINISTIC_KEYWORD_CATEGORY_MAP",
        "proxy_symbol": "SPY",
        "decision_authority": "SUPPORTING_ONLY",
        "approval_eligible": False,
        "instruction_creation_allowed": False,
        "order_allowed": False,
    }


def test_jin10_cpi_gets_separate_deterministic_research_proxy() -> None:
    news = NewsInput(
        event_id="jin10-cpi-research-proxy",
        headline="美国 CPI 同比低于预期",
        summary="美国消费者价格指数公布，核心 CPI 同比放缓。",
        source="Jin10",
        source_url="https://flash.jin10.com/detail/cpi-research",
        published_at=NOW - timedelta(minutes=1),
        first_seen_at=NOW,
        evidence_ids=("evidence-cpi-research",),
        symbols=(),
    )

    binding = bind_research_proxy(news, allowed_symbols=("SPY", "QQQ"))

    assert binding is not None
    assert binding.as_dict() == {
        "binding_role": "DETERMINISTIC_RESEARCH_PROXY",
        "event_category": "US_INFLATION",
        "source": "JIN10",
        "mapping_version": MARKET_PROXY_MAPPING_VERSION,
        "mapping_hash": MARKET_PROXY_MAPPING_HASH,
        "method": "DETERMINISTIC_KEYWORD_CATEGORY_MAP",
        "proxy_symbol": "SPY",
        "influence_scope": "EQUITY_RESEARCH_FACTOR_ONLY",
        "decision_authority": "SUPPORTING_ONLY",
        "eligibility_effect": "NONE",
        "risk_effect": "NONE",
        "approval_eligible": False,
        "instruction_creation_allowed": False,
        "order_allowed": False,
    }


def test_market_proxy_rejects_non_jin10_ambiguous_and_unavailable_mapping() -> None:
    base = dict(
        event_id="macro-event",
        summary="",
        source_url="https://example.test/macro",
        published_at=NOW - timedelta(minutes=1),
        first_seen_at=NOW,
        evidence_ids=("evidence-macro",),
        symbols=(),
    )
    non_jin10 = NewsInput(
        headline="US CPI is released",
        source="Unknown Wire",
        **base,
    )
    ambiguous = NewsInput(
        headline="美联储讨论 CPI 与利率决定",
        source="Jin10",
        **base,
    )
    unavailable = NewsInput(
        headline="美国 CPI 同比低于预期",
        source="Jin10",
        **base,
    )

    assert bind_market_proxy(non_jin10, allowed_symbols=("SPY", "TLT")) is None
    assert bind_market_proxy(ambiguous, allowed_symbols=("SPY", "TLT")) is None
    assert bind_market_proxy(unavailable, allowed_symbols=("QQQ",)) is None


def test_jin10_structured_source_metadata_is_unique_allowlisted_and_conflict_safe() -> None:
    batch = Jin10McpNewsBatch(
        payloads={
            "list_news": {
                "status": 200,
                "data": {
                    "items": [
                        {
                            "id": "metadata-1",
                            "title": "公司上调业绩指引",
                            "introduction": "",
                            "ticker": "NVDA",
                            "time": "2026-08-06T14:29:00+08:00",
                        },
                        {
                            "id": "metadata-2",
                            "title": "$AAPL 发布消息",
                            "introduction": "",
                            "symbol": "NVDA",
                            "time": "2026-08-06T14:28:00+08:00",
                        },
                        {
                            "id": "metadata-3",
                            "title": "两家公司发布消息",
                            "introduction": "",
                            "symbols": ["NVDA", "AAPL"],
                            "time": "2026-08-06T14:27:00+08:00",
                        },
                    ]
                },
            }
        }
    )

    class Client:
        transport_verified = True

        @staticmethod
        def fetch_news(_token: str, *, limit: int) -> Jin10McpNewsBatch:
            return batch

    provider = Jin10EventProvider(
        _Secrets(), mcp_client=Client(), now=lambda: NOW
    )
    events = provider.news(["NVDA", "AAPL"], limit=10)

    assert [event.symbol for event in events] == ["NVDA", None, None]
    assert [_entity_audit(event.provenance)["method"] for event in events] == [
        "SOURCE_METADATA",
        "AMBIGUOUS_SOURCE_METADATA",
        "AMBIGUOUS_SOURCE_METADATA",
    ]


def test_provider_rate_limit_cools_down_for_exactly_five_minutes() -> None:
    clock = [NOW]

    class Client:
        transport_verified = True
        calls = 0

        @classmethod
        def fetch_news(cls, _token: str, *, limit: int) -> Jin10McpNewsBatch:
            assert limit == 10
            cls.calls += 1
            raise Jin10McpError("RATE_LIMITED")

    provider = Jin10EventProvider(
        _Secrets(), mcp_client=Client(), now=lambda: clock[0]
    )

    assert provider.news(["NVDA"], limit=10) == ()
    assert Client.calls == 1
    assert provider.health == "DEGRADED"
    assert provider.health_reason == "rate_limited"

    clock[0] = NOW + timedelta(minutes=4, seconds=59, microseconds=999999)
    assert provider.news(["NVDA"], limit=10) == ()
    assert Client.calls == 1
    assert provider.health == "DEGRADED"
    assert provider.health_reason == "cooldown_active"

    clock[0] = NOW + timedelta(minutes=5)
    assert provider.news(["NVDA"], limit=10) == ()
    assert Client.calls == 2
    assert provider.health == "DEGRADED"
    assert provider.health_reason == "rate_limited"


def test_composed_mcp_provider_without_activated_credential_is_down() -> None:
    class MissingSecrets:
        @staticmethod
        def get(_name: str) -> None:
            return None

    class Client:
        transport_verified = True

        @staticmethod
        def fetch_news(_token: str, *, limit: int) -> Jin10McpNewsBatch:
            raise AssertionError("provider must fail before transport")

    provider = Jin10EventProvider(
        MissingSecrets(), mcp_client=Client(), now=lambda: NOW
    )

    assert provider.news(["NVDA"], limit=10) == ()
    assert (provider.health, provider.health_reason) == (
        "DOWN",
        "credential_not_activated",
    )


def test_authentication_failure_and_unusable_records_are_down() -> None:
    class AuthFailureClient:
        transport_verified = True

        @staticmethod
        def fetch_news(_token: str, *, limit: int) -> Jin10McpNewsBatch:
            raise Jin10McpError("AUTHENTICATION_FAILED")

    auth_provider = Jin10EventProvider(
        _Secrets(), mcp_client=AuthFailureClient(), now=lambda: NOW
    )
    assert auth_provider.news(["NVDA"], limit=10) == ()
    assert (auth_provider.health, auth_provider.health_reason) == (
        "DOWN",
        "authentication_failed",
    )

    class InvalidRecordsClient:
        transport_verified = True

        @staticmethod
        def fetch_news(_token: str, *, limit: int) -> Jin10McpNewsBatch:
            return Jin10McpNewsBatch(
                payloads={
                    "list_flash": {
                        "status": 200,
                        "data": {"items": [{"id": "missing-required-fields"}]},
                    }
                }
            )

    records_provider = Jin10EventProvider(
        _Secrets(), mcp_client=InvalidRecordsClient(), now=lambda: NOW
    )
    assert records_provider.news(["NVDA"], limit=10) == ()
    assert (records_provider.health, records_provider.health_reason) == (
        "DOWN",
        "invalid_records",
    )


def test_timeout_is_transient_degraded_health_with_fixed_reason() -> None:
    class TimeoutClient:
        transport_verified = True

        @staticmethod
        def fetch_news(_token: str, *, limit: int) -> Jin10McpNewsBatch:
            raise Jin10McpError("REQUEST_TIMEOUT")

    provider = Jin10EventProvider(
        _Secrets(), mcp_client=TimeoutClient(), now=lambda: NOW
    )

    assert provider.news(["NVDA"], limit=10) == ()
    assert (provider.health, provider.health_reason) == (
        "DEGRADED",
        "request_timeout",
    )


def test_partial_trusted_tool_result_is_degraded_but_no_result_is_down() -> None:
    class PartialClient:
        transport_verified = True

        @staticmethod
        def fetch_news(_token: str, *, limit: int) -> Jin10McpNewsBatch:
            return Jin10McpNewsBatch(
                payloads={
                    "list_flash": {"status": 200, "data": {"items": []}}
                },
                failures=("list_news:request_timeout",),
            )

    partial_provider = Jin10EventProvider(
        _Secrets(), mcp_client=PartialClient(), now=lambda: NOW
    )
    assert partial_provider.news(["NVDA"], limit=10) == ()
    assert (partial_provider.health, partial_provider.health_reason) == (
        "DEGRADED",
        "partial_tool_failure",
    )

    class NoResultClient:
        transport_verified = True

        @staticmethod
        def fetch_news(_token: str, *, limit: int) -> Jin10McpNewsBatch:
            return Jin10McpNewsBatch(
                payloads={},
                failures=("list_flash:request_timeout",),
            )

    down_provider = Jin10EventProvider(
        _Secrets(), mcp_client=NoResultClient(), now=lambda: NOW
    )
    assert down_provider.news(["NVDA"], limit=10) == ()
    assert (down_provider.health, down_provider.health_reason) == (
        "DOWN",
        "no_usable_records",
    )
