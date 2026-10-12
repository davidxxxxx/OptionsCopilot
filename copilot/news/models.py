"""News and calendar records shared by every source."""
from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import date, datetime, timezone


@dataclass(frozen=True)
class NewsItem:
    source: str
    source_id: str
    published_at: datetime
    title: str
    body: str = ""
    url: str = ""
    symbols: tuple[str, ...] = ()
    importance: int = 0
    id: int | None = field(default=None, compare=False)

    def __post_init__(self) -> None:
        if self.published_at.tzinfo is None:
            raise ValueError("published_at must be timezone-aware")
        object.__setattr__(self, "published_at", self.published_at.astimezone(timezone.utc))

    @property
    def text(self) -> str:
        if self.body and self.body != self.title:
            return f"{self.title} — {self.body}" if self.title else self.body
        return self.title


@dataclass(frozen=True)
class EarningsEvent:
    symbol: str
    report_date: date
    hour: str = ""  # bmo / amc / dmh / ""
    eps_estimate: float | None = None
    source: str = ""


@dataclass(frozen=True)
class SourceResult:
    source: str
    status: str  # ok / failed / skipped
    count: int = 0
    detail: str = ""


def stable_id(*parts: str) -> str:
    return hashlib.sha256("\x1f".join(parts).encode("utf-8")).hexdigest()[:24]


__all__ = ["EarningsEvent", "NewsItem", "SourceResult", "stable_id"]
