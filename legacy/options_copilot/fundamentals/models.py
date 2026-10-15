"""Immutable point-in-time fundamental observations.

Fundamentals are descriptive, supporting-only evidence.  They never carry
ranking, approval, instruction, or order authority.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from enum import Enum
import re

from options_copilot.storage.canonical import canonical_hash, utc_datetime


_SYMBOL = re.compile(r"^[A-Z][A-Z0-9.\-]{0,14}$")
_CIK = re.compile(r"^[0-9]{10}$")
_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/\-]{0,239}$")


class FundamentalCategory(str, Enum):
    EPS = "EPS"
    REVENUE = "REVENUE"
    GUIDANCE = "GUIDANCE"
    CASH_FLOW = "CASH_FLOW"
    DEBT = "DEBT"
    VALUATION = "VALUATION"


class FundamentalMetric(str, Enum):
    EPS_DILUTED = "EPS_DILUTED"
    REVENUE = "REVENUE"
    OPERATING_CASH_FLOW = "OPERATING_CASH_FLOW"
    DEBT_CURRENT = "DEBT_CURRENT"
    DEBT_NONCURRENT = "DEBT_NONCURRENT"
    GUIDANCE_EPS = "GUIDANCE_EPS"
    GUIDANCE_REVENUE = "GUIDANCE_REVENUE"
    GUIDANCE_EPS_LOW = "GUIDANCE_EPS_LOW"
    GUIDANCE_EPS_HIGH = "GUIDANCE_EPS_HIGH"
    GUIDANCE_REVENUE_LOW = "GUIDANCE_REVENUE_LOW"
    GUIDANCE_REVENUE_HIGH = "GUIDANCE_REVENUE_HIGH"
    PE_TTM = "PE_TTM"
    PB_ANNUAL = "PB_ANNUAL"
    PS_TTM = "PS_TTM"

    @property
    def category(self) -> FundamentalCategory:
        if self is FundamentalMetric.EPS_DILUTED:
            return FundamentalCategory.EPS
        if self is FundamentalMetric.REVENUE:
            return FundamentalCategory.REVENUE
        if self is FundamentalMetric.OPERATING_CASH_FLOW:
            return FundamentalCategory.CASH_FLOW
        if self in {
            FundamentalMetric.DEBT_CURRENT,
            FundamentalMetric.DEBT_NONCURRENT,
        }:
            return FundamentalCategory.DEBT
        if self in {
            FundamentalMetric.GUIDANCE_EPS,
            FundamentalMetric.GUIDANCE_REVENUE,
            FundamentalMetric.GUIDANCE_EPS_LOW,
            FundamentalMetric.GUIDANCE_EPS_HIGH,
            FundamentalMetric.GUIDANCE_REVENUE_LOW,
            FundamentalMetric.GUIDANCE_REVENUE_HIGH,
        }:
            return FundamentalCategory.GUIDANCE
        return FundamentalCategory.VALUATION


@dataclass(frozen=True, slots=True)
class FundamentalObservation:
    """One value whose availability begins at ``observed_at``.

    A source filing date is provenance, not a substitute for first-seen time.
    This prevents a newly downloaded historical filing from leaking into an
    earlier point-in-time replay.
    """

    symbol: str
    metric: FundamentalMetric
    value: Decimal
    unit: str
    basis: str
    period_end: date
    fiscal_period: str
    source: str
    source_id: str
    source_url: str
    source_filed_date: date | None
    observed_at: datetime
    cik: str | None = None
    fiscal_year: int | None = None
    taxonomy: str | None = None
    tag: str | None = None
    form: str | None = None
    decision_authority: str = "SUPPORTING_ONLY"

    def __post_init__(self) -> None:
        symbol = str(self.symbol or "").strip().upper()
        if _SYMBOL.fullmatch(symbol) is None:
            raise ValueError("fundamental symbol is invalid")
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "metric", FundamentalMetric(self.metric))
        value = _decimal(self.value, field="value")
        object.__setattr__(self, "value", value)
        unit = _label(self.unit, field="unit", maximum=40)
        basis = _label(self.basis, field="basis", maximum=40).upper()
        fiscal_period = _label(
            self.fiscal_period,
            field="fiscal_period",
            maximum=24,
        ).upper()
        object.__setattr__(self, "unit", unit)
        object.__setattr__(self, "basis", basis)
        object.__setattr__(self, "fiscal_period", fiscal_period)
        if not isinstance(self.period_end, date) or isinstance(self.period_end, datetime):
            raise TypeError("period_end must be a date")
        for field in ("source", "source_id"):
            value_text = _identifier(getattr(self, field), field=field)
            object.__setattr__(self, field, value_text)
        source_url = str(self.source_url or "").strip()
        if not source_url.startswith("https://") or len(source_url) > 1000:
            raise ValueError("source_url must be a bounded HTTPS URL")
        object.__setattr__(self, "source_url", source_url)
        object.__setattr__(
            self,
            "observed_at",
            utc_datetime(self.observed_at, field="observed_at"),
        )
        if self.cik is not None:
            cik = str(self.cik).strip()
            if _CIK.fullmatch(cik) is None:
                raise ValueError("cik must contain exactly ten digits")
            object.__setattr__(self, "cik", cik)
        if self.fiscal_year is not None and (
            isinstance(self.fiscal_year, bool)
            or not isinstance(self.fiscal_year, int)
            or not 1900 <= self.fiscal_year <= 2200
        ):
            raise ValueError("fiscal_year is invalid")
        for field in ("taxonomy", "tag", "form"):
            raw = getattr(self, field)
            if raw is not None:
                object.__setattr__(
                    self,
                    field,
                    _label(raw, field=field, maximum=160),
                )
        if self.decision_authority != "SUPPORTING_ONLY":
            raise ValueError("fundamentals are permanently SUPPORTING_ONLY")

    @property
    def category(self) -> FundamentalCategory:
        return self.metric.category

    @property
    def series_key(self) -> str:
        return canonical_hash(
            {
                "symbol": self.symbol,
                "metric": self.metric.value,
                "period_end": self.period_end,
                "fiscal_period": self.fiscal_period,
                "unit": self.unit,
                "basis": self.basis,
            }
        )

    @property
    def content_hash(self) -> str:
        return canonical_hash(self.hash_payload())

    @property
    def semantic_hash(self) -> str:
        """Hash the assertion independently of repeat observation time."""

        payload = self.hash_payload()
        payload.pop("observed_at")
        return canonical_hash(payload)

    def hash_payload(self) -> dict[str, object]:
        return {
            "schema": "options_copilot.fundamental_observation.v1",
            "symbol": self.symbol,
            "cik": self.cik,
            "category": self.category.value,
            "metric": self.metric.value,
            "value": self.value,
            "unit": self.unit,
            "basis": self.basis,
            "period_end": self.period_end,
            "fiscal_period": self.fiscal_period,
            "fiscal_year": self.fiscal_year,
            "source": self.source,
            "source_id": self.source_id,
            "source_url": self.source_url,
            "source_filed_date": self.source_filed_date,
            "observed_at": self.observed_at,
            "taxonomy": self.taxonomy,
            "tag": self.tag,
            "form": self.form,
            "decision_authority": self.decision_authority,
        }

    def as_dict(self) -> dict[str, object]:
        return {
            **self.hash_payload(),
            "value": format(self.value, "f"),
            "period_end": self.period_end.isoformat(),
            "source_filed_date": (
                None
                if self.source_filed_date is None
                else self.source_filed_date.isoformat()
            ),
            "observed_at": self.observed_at.isoformat(),
            "series_key": self.series_key,
            "semantic_hash": self.semantic_hash,
            "content_hash": self.content_hash,
        }


def _decimal(value: object, *, field: str) -> Decimal:
    if isinstance(value, bool):
        raise TypeError(f"{field} must not be boolean")
    try:
        parsed = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        raise ValueError(f"{field} must be a decimal") from None
    if not parsed.is_finite():
        raise ValueError(f"{field} must be finite")
    return parsed


def _label(value: object, *, field: str, maximum: int) -> str:
    text = " ".join(str(value or "").split())
    if not text or len(text) > maximum or any(ord(char) < 32 for char in text):
        raise ValueError(f"{field} is invalid")
    return text


def _identifier(value: object, *, field: str) -> str:
    text = str(value or "").strip()
    if _IDENTIFIER.fullmatch(text) is None:
        raise ValueError(f"{field} is invalid")
    return text


__all__ = [
    "FundamentalCategory",
    "FundamentalMetric",
    "FundamentalObservation",
]
