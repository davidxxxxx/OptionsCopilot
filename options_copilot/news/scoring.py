"""Transparent three-band research scores with no approval or order authority."""
from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal

from .models import ClassifiedEvent, EventCategory, NewsInput, OptionTradabilityInput, ScoreBand


_HIGH_IMPACT = frozenset({EventCategory.EARNINGS, EventCategory.FOMC, EventCategory.MACRO, EventCategory.GUIDANCE, EventCategory.M_AND_A, EventCategory.REGULATORY})


_ZERO = Decimal("0")
_FORTY = Decimal("40")
_SEVENTY = Decimal("70")
_HUNDRED = Decimal("100")


def score_band(score: Decimal) -> ScoreBand:
    if score >= _SEVENTY:
        return ScoreBand.HIGH
    if score >= _FORTY:
        return ScoreBand.MEDIUM
    return ScoreBand.LOW


def event_impact_score(news: NewsInput, classification: ClassifiedEvent) -> Decimal:
    """Score materiality from disclosed event metadata, never from option data."""
    if not news.is_complete or news.conflicting_evidence_ids:
        return _ZERO
    base = Decimal("65") if classification.category in _HIGH_IMPACT else Decimal("40") if classification.category is not EventCategory.OTHER else Decimal("20")
    return min(_HUNDRED, base + classification.confidence * Decimal("35")).quantize(Decimal("0.01"))


def event_impact(news: NewsInput, classification: ClassifiedEvent) -> ScoreBand:
    return score_band(event_impact_score(news, classification))


def option_tradability_score(data: OptionTradabilityInput | None, *, now: datetime, maximum_age: timedelta = timedelta(seconds=5)) -> Decimal:
    """Use only complete, fresh IBKR quote/liquidity fields passed by the caller."""
    if data is None or not data.complete or now < data.observed_at or now - data.observed_at > maximum_age:
        return _ZERO
    assert data.bid is not None and data.ask is not None and data.volume is not None and data.open_interest is not None
    midpoint = (data.bid + data.ask) / Decimal("2")
    if midpoint <= 0:
        return _ZERO
    spread = (data.ask - data.bid) / midpoint
    raw = min(_HUNDRED, Decimal("50") + min(Decimal("35"), Decimal(data.volume) / Decimal("10")) + min(Decimal("25"), Decimal(data.open_interest) / Decimal("25")) - min(Decimal("50"), spread * Decimal("100")))
    if spread <= Decimal("0.08") and data.volume >= 100 and data.open_interest >= 500:
        return max(_SEVENTY, raw).quantize(Decimal("0.01"))
    if spread <= Decimal("0.15") and data.volume >= 20 and data.open_interest >= 100:
        return min(Decimal("69.99"), max(_FORTY, raw)).quantize(Decimal("0.01"))
    return min(Decimal("39.99"), max(_ZERO, raw)).quantize(Decimal("0.01"))


def option_tradability(data: OptionTradabilityInput | None, *, now: datetime, maximum_age: timedelta = timedelta(seconds=5)) -> ScoreBand:
    return score_band(option_tradability_score(data, now=now, maximum_age=maximum_age))


def combined_opportunity_score(event_score: Decimal, tradability_score: Decimal) -> Decimal:
    """A transparent intersection: a weak component cannot become an opportunity."""
    if event_score < _FORTY or tradability_score < _FORTY:
        return _ZERO
    return (event_score * Decimal("0.60") + tradability_score * Decimal("0.40")).quantize(Decimal("0.01"))


def combined_opportunity(event_score: ScoreBand, tradability_score: ScoreBand) -> ScoreBand:
    band_to_floor = {ScoreBand.LOW: _ZERO, ScoreBand.MEDIUM: _FORTY, ScoreBand.HIGH: _SEVENTY}
    return score_band(combined_opportunity_score(band_to_floor[event_score], band_to_floor[tradability_score]))


def sort_key(score: ScoreBand) -> int:
    return {ScoreBand.LOW: 0, ScoreBand.MEDIUM: 1, ScoreBand.HIGH: 2}[score]
