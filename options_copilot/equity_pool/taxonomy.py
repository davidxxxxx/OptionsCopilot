"""Versioned exact classification data included in the taxonomy authority hash."""

from __future__ import annotations

from collections.abc import Mapping

from options_copilot.storage.canonical import canonical_hash


TAXONOMY_VERSION = "equity-taxonomy.v2"
TAXONOMY_ALGORITHM = "IBKR_EXACT_THEN_LOCAL_EXACT_THEN_UNCLASSIFIED"

COMPANY_ALIAS_VALUES = {
    "COMMUNICATION SERVICES": "COMMUNICATION_SERVICES",
    "COMMUNICATION_SERVICES": "COMMUNICATION_SERVICES",
    "CONSUMER CYCLICAL": "CONSUMER_DISCRETIONARY",
    "CONSUMER DISCRETIONARY": "CONSUMER_DISCRETIONARY",
    "CONSUMER_DISCRETIONARY": "CONSUMER_DISCRETIONARY",
    "CONSUMER DEFENSIVE": "CONSUMER_STAPLES",
    "CONSUMER STAPLES": "CONSUMER_STAPLES",
    "CONSUMER_STAPLES": "CONSUMER_STAPLES",
    "ENERGY": "ENERGY",
    "FINANCIAL": "FINANCIALS",
    "FINANCIALS": "FINANCIALS",
    "HEALTH CARE": "HEALTH_CARE",
    "HEALTHCARE": "HEALTH_CARE",
    "HEALTH_CARE": "HEALTH_CARE",
    "INDUSTRIAL": "INDUSTRIALS",
    "INDUSTRIALS": "INDUSTRIALS",
    "INFORMATION TECHNOLOGY": "INFORMATION_TECHNOLOGY",
    "INFORMATION_TECHNOLOGY": "INFORMATION_TECHNOLOGY",
    "TECHNOLOGY": "INFORMATION_TECHNOLOGY",
    "MATERIALS": "MATERIALS",
    "BASIC MATERIALS": "MATERIALS",
    "REAL ESTATE": "REAL_ESTATE",
    "REAL_ESTATE": "REAL_ESTATE",
    "UTILITIES": "UTILITIES",
}

ETF_ALIAS_VALUES = {
    "BROAD EQUITY": "BROAD_EQUITY_ETF",
    "BROAD_EQUITY_ETF": "BROAD_EQUITY_ETF",
    "EQUITY INDEX": "BROAD_EQUITY_ETF",
    "SECTOR": "SECTOR_ETF",
    "SECTOR ETF": "SECTOR_ETF",
    "SECTOR_ETF": "SECTOR_ETF",
    "BOND": "RATES_BOND_ETF",
    "FIXED INCOME": "RATES_BOND_ETF",
    "RATES_BOND_ETF": "RATES_BOND_ETF",
    "COMMODITY": "COMMODITY_ETF",
    "COMMODITY_ETF": "COMMODITY_ETF",
    "OTHER ETF": "OTHER_ETF",
    "OTHER_ETF": "OTHER_ETF",
}

LOCAL_EXACT_CATEGORY_VALUES = {
    "AAPL": "INFORMATION_TECHNOLOGY",
    "MSFT": "INFORMATION_TECHNOLOGY",
    "NVDA": "INFORMATION_TECHNOLOGY",
    "AVGO": "INFORMATION_TECHNOLOGY",
    "AMZN": "CONSUMER_DISCRETIONARY",
    "GOOG": "COMMUNICATION_SERVICES",
    "GOOGL": "COMMUNICATION_SERVICES",
    "META": "COMMUNICATION_SERVICES",
    "TSLA": "CONSUMER_DISCRETIONARY",
    "JPM": "FINANCIALS",
    "XOM": "ENERGY",
    "UNH": "HEALTH_CARE",
    "CAT": "INDUSTRIALS",
    "LIN": "MATERIALS",
    "PLD": "REAL_ESTATE",
    "NEE": "UTILITIES",
    "PG": "CONSUMER_STAPLES",
    "SPY": "BROAD_EQUITY_ETF",
    "QQQ": "BROAD_EQUITY_ETF",
    "IWM": "BROAD_EQUITY_ETF",
    "DIA": "BROAD_EQUITY_ETF",
    "XLB": "SECTOR_ETF",
    "XLE": "SECTOR_ETF",
    "XLF": "SECTOR_ETF",
    "XLI": "SECTOR_ETF",
    "XLK": "SECTOR_ETF",
    "XLP": "SECTOR_ETF",
    "XLRE": "SECTOR_ETF",
    "XLU": "SECTOR_ETF",
    "XLV": "SECTOR_ETF",
    "XLY": "SECTOR_ETF",
    "TLT": "RATES_BOND_ETF",
    "IEF": "RATES_BOND_ETF",
    "SHY": "RATES_BOND_ETF",
    "GLD": "COMMODITY_ETF",
    "SLV": "COMMODITY_ETF",
    "USO": "COMMODITY_ETF",
}

MEGA_CAP_TECH_SYMBOL_VALUES = frozenset(
    {"AAPL", "MSFT", "NVDA", "AVGO", "AMZN", "GOOG", "GOOGL", "META", "TSLA"}
)


def taxonomy_hash(local_mapping: Mapping[str, object]) -> str:
    normalized = {
        str(symbol).strip().upper(): str(getattr(category, "value", category)).strip().upper()
        for symbol, category in local_mapping.items()
    }
    return canonical_hash(
        {
            "version": TAXONOMY_VERSION,
            "algorithm": TAXONOMY_ALGORITHM,
            "company_aliases": COMPANY_ALIAS_VALUES,
            "etf_aliases": ETF_ALIAS_VALUES,
            "local_exact_mapping": dict(sorted(normalized.items())),
            "mega_cap_tech_symbols": tuple(sorted(MEGA_CAP_TECH_SYMBOL_VALUES)),
            "fallback": "UNCLASSIFIED",
        }
    )


DEFAULT_TAXONOMY_HASH = taxonomy_hash(LOCAL_EXACT_CATEGORY_VALUES)


__all__ = [
    "COMPANY_ALIAS_VALUES",
    "DEFAULT_TAXONOMY_HASH",
    "ETF_ALIAS_VALUES",
    "LOCAL_EXACT_CATEGORY_VALUES",
    "MEGA_CAP_TECH_SYMBOL_VALUES",
    "TAXONOMY_ALGORITHM",
    "TAXONOMY_VERSION",
    "taxonomy_hash",
]
