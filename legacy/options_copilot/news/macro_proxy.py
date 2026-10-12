"""Deterministic market-proxy bindings for authority-free macro research.

The mapping never changes the source event's issuer symbols. It creates one
separate, hash-bound underlying so a public macro event can be evaluated against
an observable market proxy. Shadow predictions and deterministic equity research
use distinct binding contracts and neither contract can affect action authority.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import re
from types import MappingProxyType
from typing import Mapping, Sequence
import unicodedata

from options_copilot.storage.canonical import canonical_hash

from .models import AuditJson, NewsInput


MARKET_PROXY_MAPPING_VERSION = "2026-08-14.1"
_MARKET_PROXY_METHOD = "DETERMINISTIC_KEYWORD_CATEGORY_MAP"

_MARKET_PROXY_RULES: dict[str, dict[str, object]] = {
    "US_INFLATION": {
        "proxy_symbol": "SPY",
        "patterns": (
            r"\bCPI\b",
            r"\bPCE\b",
            r"CONSUMER PRICE INDEX",
            r"PERSONAL CONSUMPTION EXPENDITURES",
            r"消费者价格指数",
            r"消费价格指数",
            r"通胀",
        ),
    },
    "US_MONETARY_POLICY": {
        "proxy_symbol": "TLT",
        "patterns": (
            r"\bFOMC\b",
            r"FEDERAL RESERVE",
            r"FED FUNDS",
            r"美联储",
            r"联邦基金利率",
        ),
    },
    "US_LABOR": {
        "proxy_symbol": "SPY",
        "patterns": (
            r"NONFARM PAYROLL",
            r"NON-FARM PAYROLL",
            r"\bNFP\b",
            r"UNEMPLOYMENT RATE",
            r"INITIAL JOBLESS CLAIMS",
            r"非农",
            r"失业率",
            r"初请失业金",
        ),
    },
    "US_GROWTH": {
        "proxy_symbol": "SPY",
        "patterns": (
            r"\bGDP\b",
            r"GROSS DOMESTIC PRODUCT",
            r"RETAIL SALES",
            r"国内生产总值",
            r"零售销售",
        ),
    },
    "GOLD_MACRO": {
        "proxy_symbol": "GLD",
        "patterns": (
            r"\bGOLD\b",
            r"\bXAU(?:USD)?\b",
            r"黄金",
        ),
    },
}

MARKET_PROXY_RULES: Mapping[str, Mapping[str, object]] = MappingProxyType(
    {
        category: MappingProxyType(dict(rule))
        for category, rule in _MARKET_PROXY_RULES.items()
    }
)
MARKET_PROXY_MAPPING_HASH = canonical_hash(
    {
        "mapping_version": MARKET_PROXY_MAPPING_VERSION,
        "method": _MARKET_PROXY_METHOD,
        "sources": ("JIN10",),
        "rules": _MARKET_PROXY_RULES,
    }
)


@dataclass(frozen=True, slots=True)
class MarketProxyBinding(AuditJson):
    """Auditable proof for a shadow evaluation underlying, never an issuer."""

    event_category: str
    source: str
    proxy_symbol: str
    mapping_version: str = MARKET_PROXY_MAPPING_VERSION
    mapping_hash: str = MARKET_PROXY_MAPPING_HASH
    method: str = _MARKET_PROXY_METHOD
    binding_role: str = field(default="MARKET_PROXY", init=False)
    decision_authority: str = field(default="SUPPORTING_ONLY", init=False)
    approval_eligible: bool = field(default=False, init=False)
    instruction_creation_allowed: bool = field(default=False, init=False)
    order_allowed: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        category = str(self.event_category or "").strip().upper()
        source = str(self.source or "").strip().upper()
        symbol = str(self.proxy_symbol or "").strip().upper()
        if category not in MARKET_PROXY_RULES:
            raise ValueError("market proxy event_category is not mapped")
        if source != "JIN10":
            raise ValueError("market proxy source is not allowed")
        if MARKET_PROXY_RULES[category]["proxy_symbol"] != symbol:
            raise ValueError("market proxy symbol does not match category")
        if self.mapping_version != MARKET_PROXY_MAPPING_VERSION:
            raise ValueError("market proxy mapping version is stale")
        if self.mapping_hash != MARKET_PROXY_MAPPING_HASH:
            raise ValueError("market proxy mapping hash is stale")
        if self.method != _MARKET_PROXY_METHOD:
            raise ValueError("market proxy method is invalid")
        object.__setattr__(self, "event_category", category)
        object.__setattr__(self, "source", source)
        object.__setattr__(self, "proxy_symbol", symbol)


@dataclass(frozen=True, slots=True)
class ResearchProxyBinding(AuditJson):
    """Auditable deterministic equity-research proxy, never an issuer binding."""

    event_category: str
    source: str
    proxy_symbol: str
    mapping_version: str = MARKET_PROXY_MAPPING_VERSION
    mapping_hash: str = MARKET_PROXY_MAPPING_HASH
    method: str = _MARKET_PROXY_METHOD
    binding_role: str = field(default="DETERMINISTIC_RESEARCH_PROXY", init=False)
    influence_scope: str = field(default="EQUITY_RESEARCH_FACTOR_ONLY", init=False)
    decision_authority: str = field(default="SUPPORTING_ONLY", init=False)
    eligibility_effect: str = field(default="NONE", init=False)
    risk_effect: str = field(default="NONE", init=False)
    approval_eligible: bool = field(default=False, init=False)
    instruction_creation_allowed: bool = field(default=False, init=False)
    order_allowed: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        shadow_binding = MarketProxyBinding(
            event_category=self.event_category,
            source=self.source,
            proxy_symbol=self.proxy_symbol,
            mapping_version=self.mapping_version,
            mapping_hash=self.mapping_hash,
            method=self.method,
        )
        object.__setattr__(self, "event_category", shadow_binding.event_category)
        object.__setattr__(self, "source", shadow_binding.source)
        object.__setattr__(self, "proxy_symbol", shadow_binding.proxy_symbol)


def bind_market_proxy(
    news: NewsInput,
    *,
    allowed_symbols: Sequence[str],
) -> MarketProxyBinding | None:
    """Return one unambiguous, allowlisted proxy for eligible Jin10 macro news."""

    if not isinstance(news, NewsInput):
        raise TypeError("news must be a NewsInput")
    if news.symbols or _source_name(news.source) != "JIN10":
        return None
    allowed = {
        str(value).strip().upper()
        for value in allowed_symbols
        if str(value).strip()
    }
    text = unicodedata.normalize("NFKC", f"{news.headline}\n{news.summary}")
    matches = [
        category
        for category, rule in MARKET_PROXY_RULES.items()
        if any(
            re.search(str(pattern), text, flags=re.IGNORECASE)
            for pattern in rule["patterns"]  # type: ignore[index]
        )
    ]
    if len(matches) != 1:
        return None
    category = matches[0]
    symbol = str(MARKET_PROXY_RULES[category]["proxy_symbol"])
    if symbol not in allowed:
        return None
    return MarketProxyBinding(
        event_category=category,
        source="JIN10",
        proxy_symbol=symbol,
    )


def bind_research_proxy(
    news: NewsInput,
    *,
    allowed_symbols: Sequence[str],
) -> ResearchProxyBinding | None:
    """Return one fixed, allowlisted proxy for deterministic equity research."""

    binding = bind_market_proxy(news, allowed_symbols=allowed_symbols)
    if binding is None:
        return None
    return ResearchProxyBinding(
        event_category=binding.event_category,
        source=binding.source,
        proxy_symbol=binding.proxy_symbol,
    )


def require_current_market_proxy_binding(
    value: object,
    *,
    symbol: str,
) -> MarketProxyBinding | None:
    """Rebuild a persisted proxy proof against the current immutable mapping."""

    checked_symbol = str(symbol or "").strip().upper()
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise ValueError("market proxy binding must be a mapping or None")
    binding = MarketProxyBinding(
        event_category=str(value.get("event_category") or ""),
        source=str(value.get("source") or ""),
        proxy_symbol=str(value.get("proxy_symbol") or ""),
        mapping_version=str(value.get("mapping_version") or ""),
        mapping_hash=str(value.get("mapping_hash") or ""),
        method=str(value.get("method") or ""),
    )
    if binding.proxy_symbol != checked_symbol:
        raise ValueError("market proxy binding does not match prediction symbol")
    if canonical_hash(dict(value)) != canonical_hash(binding.as_dict()):
        raise ValueError("market proxy binding contains unrecognized fields or values")
    return binding


def require_current_research_proxy_binding(
    value: object,
    *,
    symbol: str,
) -> ResearchProxyBinding | None:
    """Rebuild a persisted deterministic research proxy against current rules."""

    checked_symbol = str(symbol or "").strip().upper()
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise ValueError("research proxy binding must be a mapping or None")
    binding = ResearchProxyBinding(
        event_category=str(value.get("event_category") or ""),
        source=str(value.get("source") or ""),
        proxy_symbol=str(value.get("proxy_symbol") or ""),
        mapping_version=str(value.get("mapping_version") or ""),
        mapping_hash=str(value.get("mapping_hash") or ""),
        method=str(value.get("method") or ""),
    )
    if binding.proxy_symbol != checked_symbol:
        raise ValueError("research proxy binding does not match factor symbol")
    if canonical_hash(dict(value)) != canonical_hash(binding.as_dict()):
        raise ValueError("research proxy binding contains unrecognized fields or values")
    return binding


def _source_name(value: object) -> str:
    normalized = " ".join(str(value or "").strip().upper().split())
    return "JIN10" if normalized in {"JIN10", "JIN10 MCP"} else normalized


__all__ = [
    "MARKET_PROXY_MAPPING_HASH",
    "MARKET_PROXY_MAPPING_VERSION",
    "MARKET_PROXY_RULES",
    "MarketProxyBinding",
    "ResearchProxyBinding",
    "bind_market_proxy",
    "bind_research_proxy",
    "require_current_market_proxy_binding",
    "require_current_research_proxy_binding",
]
