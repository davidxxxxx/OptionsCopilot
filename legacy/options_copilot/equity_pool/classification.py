"""Exact, versioned equity classification with no fuzzy or model fallback."""

from __future__ import annotations

from collections.abc import Mapping

from .models import (
    CanonicalClassification,
    ClassificationSource,
    EquityCategory,
)
from .taxonomy import (
    COMPANY_ALIAS_VALUES,
    ETF_ALIAS_VALUES,
    LOCAL_EXACT_CATEGORY_VALUES,
    MEGA_CAP_TECH_SYMBOL_VALUES,
    TAXONOMY_VERSION,
    taxonomy_hash,
)


_COMPANY_ALIASES = {
    alias: EquityCategory(category)
    for alias, category in COMPANY_ALIAS_VALUES.items()
}
_ETF_ALIASES = {
    alias: EquityCategory(category)
    for alias, category in ETF_ALIAS_VALUES.items()
}

# Deliberately bounded. This table is a transparent fallback, not a claim that
# every IBKR-discoverable security has been classified locally.
LOCAL_EXACT_MAPPING: Mapping[str, EquityCategory] = {
    symbol: EquityCategory(category)
    for symbol, category in LOCAL_EXACT_CATEGORY_VALUES.items()
}

MEGA_CAP_TECH_SYMBOLS = MEGA_CAP_TECH_SYMBOL_VALUES


def classify_security(
    symbol: str,
    *,
    security_type: str,
    ibkr_sector: str | None = None,
    ibkr_category: str | None = None,
    local_mapping: Mapping[str, EquityCategory] = LOCAL_EXACT_MAPPING,
) -> CanonicalClassification:
    """Classify from exact same-scan IBKR metadata, then an exact local map."""

    normalized_symbol = _required_upper(symbol, "symbol")
    normalized_type = _required_upper(security_type, "security_type")
    local_hash = taxonomy_hash(local_mapping)
    category: EquityCategory | None = None
    if normalized_type in {"STK", "STOCK"}:
        category = _exact_alias(ibkr_sector, _COMPANY_ALIASES)
    elif normalized_type == "ETF":
        category = _exact_alias(ibkr_category, _ETF_ALIASES)

    if category is not None:
        return CanonicalClassification(
            symbol=normalized_symbol,
            category=category,
            source=ClassificationSource.IBKR_METADATA,
            mega_cap_tech=normalized_symbol in MEGA_CAP_TECH_SYMBOLS,
            taxonomy_version=TAXONOMY_VERSION,
            taxonomy_hash=local_hash,
        )

    local_category = local_mapping.get(normalized_symbol)
    if local_category is not None:
        if not isinstance(local_category, EquityCategory):
            raise TypeError("local mapping values must be EquityCategory values")
        return CanonicalClassification(
            symbol=normalized_symbol,
            category=local_category,
            source=ClassificationSource.LOCAL_EXACT,
            mega_cap_tech=normalized_symbol in MEGA_CAP_TECH_SYMBOLS,
            taxonomy_version=TAXONOMY_VERSION,
            taxonomy_hash=local_hash,
        )

    return CanonicalClassification(
        symbol=normalized_symbol,
        category=EquityCategory.UNCLASSIFIED,
        source=ClassificationSource.UNCLASSIFIED,
        taxonomy_version=TAXONOMY_VERSION,
        taxonomy_hash=local_hash,
    )


def _exact_alias(
    value: str | None,
    aliases: Mapping[str, EquityCategory],
) -> EquityCategory | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError("IBKR classification metadata must be a string or null")
    normalized = value.strip().upper()
    if not normalized:
        return None
    return aliases.get(normalized)


def _required_upper(value: object, field: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field} must be a string")
    normalized = value.strip().upper()
    if not normalized:
        raise ValueError(f"{field} cannot be blank")
    return normalized


__all__ = [
    "LOCAL_EXACT_MAPPING",
    "MEGA_CAP_TECH_SYMBOLS",
    "classify_security",
]
