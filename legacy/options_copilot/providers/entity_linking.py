"""Deterministic, allowlisted entity linking for supplemental news.

This module deliberately does not infer tickers from arbitrary bare words.  A
symbol can only be linked when it was supplied by the current provider call and
is supported by either an explicit market notation or this versioned catalog.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import re
from types import MappingProxyType
from typing import Mapping, Sequence
import unicodedata


ENTITY_LINK_CATALOG_VERSION = "2026-08-06.1"

_COMPANY_ALIAS_CATALOG: dict[str, tuple[str, ...]] = {
    "SPY": (
        "SPDR S&P 500 ETF Trust",
        "SPDR S&P 500 ETF",
        "SPDR标普500ETF",
        "标普500ETF",
    ),
    "QQQ": (
        "Invesco QQQ Trust",
        "Invesco QQQ ETF",
        "景顺纳斯达克100ETF",
        "纳斯达克100ETF",
    ),
    "IWM": (
        "iShares Russell 2000 ETF",
        "iShares罗素2000ETF",
        "罗素2000ETF",
    ),
    "DIA": (
        "SPDR Dow Jones Industrial Average ETF",
        "SPDR道琼斯工业平均ETF",
        "道指ETF",
    ),
    "AAPL": (
        "Apple Inc",
        "Apple公司",
        "苹果公司",
    ),
    "MSFT": (
        "Microsoft Corporation",
        "Microsoft",
        "微软公司",
        "微软",
    ),
    "NVDA": (
        "NVIDIA Corporation",
        "NVIDIA",
        "英伟达",
        "辉达",
    ),
    "AMZN": (
        "Amazon.com Inc",
        "Amazon.com",
        "Amazon公司",
        "亚马逊公司",
        "亚马逊",
    ),
    "META": (
        "Meta Platforms Inc",
        "Meta Platforms",
        "脸书母公司Meta",
        "元宇宙平台公司Meta",
    ),
    "GOOGL": (
        "Alphabet Inc",
        "Alphabet公司",
        "Google parent Alphabet",
        "Google",
        "谷歌母公司Alphabet",
        "谷歌",
    ),
    "TSLA": (
        "Tesla Inc",
        "Tesla",
        "特斯拉公司",
        "特斯拉",
    ),
    "AMD": (
        "Advanced Micro Devices",
        "AMD公司",
        "超威半导体",
    ),
    "AVGO": (
        "Broadcom Inc",
        "Broadcom",
        "博通公司",
        "博通",
    ),
    "JPM": (
        "JPMorgan Chase",
        "JPMorgan",
        "摩根大通",
    ),
    "XOM": (
        "Exxon Mobil Corporation",
        "Exxon Mobil",
        "ExxonMobil",
        "埃克森美孚",
    ),
    "GLD": (
        "SPDR Gold Shares",
        "SPDR黄金信托",
        "SPDR黄金ETF",
    ),
    "TLT": (
        "iShares 20+ Year Treasury Bond ETF",
        "iShares 20年期以上美国国债ETF",
        "美国长期国债ETF",
    ),
    "SMH": (
        "VanEck Semiconductor ETF",
        "VanEck半导体ETF",
        "范艾克半导体ETF",
    ),
    "XLK": (
        "Technology Select Sector SPDR Fund",
        "科技精选行业SPDR基金",
        "科技行业SPDR基金",
    ),
    "XLF": (
        "Financial Select Sector SPDR Fund",
        "金融精选行业SPDR基金",
        "金融行业SPDR基金",
    ),
    "XLE": (
        "Energy Select Sector SPDR Fund",
        "能源精选行业SPDR基金",
        "能源行业SPDR基金",
    ),
}

COMPANY_ALIAS_CATALOG: Mapping[str, tuple[str, ...]] = MappingProxyType(
    _COMPANY_ALIAS_CATALOG
)

_CATALOG_BYTES = json.dumps(
    {
        "version": ENTITY_LINK_CATALOG_VERSION,
        "symbols": _COMPANY_ALIAS_CATALOG,
    },
    ensure_ascii=False,
    separators=(",", ":"),
    sort_keys=True,
).encode("utf-8")
ENTITY_LINK_CATALOG_HASH = hashlib.sha256(_CATALOG_BYTES).hexdigest()

_CASHTAG_RE = re.compile(
    r"(?<![\w$])\$([A-Za-z][A-Za-z0-9]{0,10}(?:\.[A-Za-z0-9]{1,10})?)"
    r"(?![\w])"
)
_EXCHANGE_TICKER_RE = re.compile(
    r"(?i)(?<![A-Za-z0-9])(?:NASDAQ|NYSE)\s*[:：]\s*"
    r"([A-Za-z][A-Za-z0-9]{0,10}(?:\.[A-Za-z0-9]{1,10})?)"
    r"(?![A-Za-z0-9])"
)


@dataclass(frozen=True, slots=True)
class EntityLink:
    symbol: str | None
    method: str
    confidence: str
    catalog_version: str = ENTITY_LINK_CATALOG_VERSION
    catalog_hash: str = ENTITY_LINK_CATALOG_HASH

    @property
    def provenance(self) -> tuple[str, ...]:
        audit = "entity_link.audit=" + json.dumps(
            {
                "catalog_hash": self.catalog_hash,
                "catalog_version": self.catalog_version,
                "confidence": self.confidence,
                "method": self.method,
            },
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
        if len(audit) > 256:
            raise ValueError("entity link audit provenance exceeds 256 characters")
        return (audit,)


def link_news_entity(
    headline: str,
    summary: str,
    *,
    allowed_symbols: Sequence[str],
    source_symbols: object = None,
) -> EntityLink:
    """Link one untrusted news item without escaping the call's allowlist."""

    allowed = _normalized_allowed_symbols(allowed_symbols)
    text = unicodedata.normalize("NFKC", f"{headline}\n{summary}")

    cashtags = _explicit_matches(_CASHTAG_RE, text)
    exchange_tickers = _explicit_matches(_EXCHANGE_TICKER_RE, text)
    explicit = cashtags | exchange_tickers

    metadata_present, metadata_symbols = _source_metadata_symbols(source_symbols)
    if metadata_present:
        if len(metadata_symbols) != 1:
            return EntityLink(None, "AMBIGUOUS_SOURCE_METADATA", "0.0000")
        source_symbol = next(iter(metadata_symbols))
        if len(explicit) > 1 or (explicit and source_symbol not in explicit):
            return EntityLink(None, "AMBIGUOUS_SOURCE_METADATA", "0.0000")
        if source_symbol not in allowed:
            return EntityLink(None, "NO_MATCH", "0.0000")
        return EntityLink(source_symbol, "SOURCE_METADATA", "1.0000")

    if len(explicit) > 1:
        return EntityLink(None, "AMBIGUOUS_EXPLICIT", "0.0000")
    if explicit:
        symbol = next(iter(explicit))
        if symbol not in allowed:
            return EntityLink(None, "NO_MATCH", "0.0000")
        if symbol in cashtags:
            return EntityLink(symbol, "EXPLICIT_CASHTAG", "1.0000")
        return EntityLink(symbol, "EXPLICIT_EXCHANGE_TICKER", "0.9900")

    alias_matches = {
        symbol
        for symbol in allowed
        if symbol in COMPANY_ALIAS_CATALOG
        and any(
            _contains_alias(text, alias)
            for alias in COMPANY_ALIAS_CATALOG[symbol]
        )
    }
    if len(alias_matches) > 1:
        return EntityLink(None, "AMBIGUOUS_ALIAS", "0.0000")
    if alias_matches:
        return EntityLink(
            next(iter(alias_matches)), "CONTROLLED_ALIAS", "0.9000"
        )
    return EntityLink(None, "NO_MATCH", "0.0000")


def _normalized_allowed_symbols(symbols: Sequence[str]) -> frozenset[str]:
    normalized: set[str] = set()
    for value in symbols:
        symbol = str(value).strip().upper()
        if (
            symbol
            and len(symbol) <= 12
            and symbol.replace(".", "").isalnum()
        ):
            normalized.add(symbol)
    return frozenset(normalized)


def _explicit_matches(
    pattern: re.Pattern[str],
    text: str,
) -> set[str]:
    return {
        match.group(1).upper()
        for match in pattern.finditer(text)
    }


def _source_metadata_symbols(value: object) -> tuple[bool, frozenset[str]]:
    if value is None:
        return False, frozenset()
    raw_values: list[object] = []
    _flatten_source_metadata(value, raw_values)
    present_values = [raw for raw in raw_values if str(raw).strip()]
    if not present_values:
        return False, frozenset()
    symbols: set[str] = set()
    for raw in present_values:
        for candidate in re.split(r"[,;|\s]+", str(raw).strip()):
            symbol = candidate.strip().upper()
            if (
                symbol
                and len(symbol) <= 12
                and symbol.replace(".", "").isalnum()
            ):
                symbols.add(symbol)
    return True, frozenset(symbols)


def _flatten_source_metadata(value: object, target: list[object]) -> None:
    if value is None:
        return
    if isinstance(value, Sequence) and not isinstance(
        value, (str, bytes, bytearray)
    ):
        for item in value:
            _flatten_source_metadata(item, target)
        return
    target.append(value)


def _contains_alias(text: str, alias: str) -> bool:
    normalized_text = unicodedata.normalize("NFKC", text).casefold()
    normalized_alias = unicodedata.normalize("NFKC", alias).casefold()
    if any(ord(char) > 127 for char in normalized_alias):
        return normalized_alias in normalized_text
    return re.search(
        rf"(?<![0-9a-z]){re.escape(normalized_alias)}(?![0-9a-z])",
        normalized_text,
    ) is not None


__all__ = [
    "COMPANY_ALIAS_CATALOG",
    "ENTITY_LINK_CATALOG_HASH",
    "ENTITY_LINK_CATALOG_VERSION",
    "EntityLink",
    "link_news_entity",
]
