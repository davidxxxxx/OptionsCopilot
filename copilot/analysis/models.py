"""A trading view on one symbol, produced by the view ledger from tagged news."""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date
from typing import Any

DIRECTIONS = ("bullish", "bearish", "neutral")
VOLATILITY_VIEWS = ("expand", "contract", "neutral")


@dataclass(frozen=True)
class View:
    symbol: str
    direction: str
    conviction: int  # 1 (weak) .. 5 (strong)
    expected_move_pct: float  # magnitude of the expected move by the horizon, 0.05 = 5%
    horizon_days: int
    volatility: str = "neutral"
    catalyst: str = ""
    catalyst_date: date | None = None
    rationale: str = ""
    news_ids: tuple[int, ...] = ()
    source: str = "ledger"
    score: float = 0.0  # signed evidence score from tagged news
    vol_score: float = 0.0
    # Expected move in standard deviations; when set the builder uses it instead of
    # expected_move_pct (it scales with each expiry's implied vol).
    drift_sigma: float | None = None
    # Rule views (e.g. the index ladder) can pin the structures and the expiry window.
    structures: tuple[str, ...] = ()
    dte_min: int | None = None
    dte_max: int | None = None

    def __post_init__(self) -> None:
        if self.direction not in DIRECTIONS:
            raise ValueError(f"direction must be one of {DIRECTIONS}")
        if self.volatility not in VOLATILITY_VIEWS:
            raise ValueError(f"volatility must be one of {VOLATILITY_VIEWS}")
        if not 1 <= self.conviction <= 5:
            raise ValueError("conviction must be 1-5")
        if not 0 <= self.expected_move_pct <= 1:
            raise ValueError("expected_move_pct must be a fraction between 0 and 1")
        if not 1 <= self.horizon_days <= 120:
            raise ValueError("horizon_days must be 1-120")

    @property
    def sign(self) -> float:
        return {"bullish": 1.0, "bearish": -1.0}.get(self.direction, 0.0)

    @property
    def signed_drift(self) -> float:
        """Expected fractional move, shrunk by conviction (5 = full, 1 = 20%)."""

        return self.sign * self.expected_move_pct * self.conviction / 5

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "View":
        raw_date = payload.get("catalyst_date")
        return cls(
            symbol=payload["symbol"],
            direction=payload["direction"],
            conviction=int(payload["conviction"]),
            expected_move_pct=float(payload["expected_move_pct"]),
            horizon_days=int(payload["horizon_days"]),
            volatility=payload.get("volatility", "neutral"),
            catalyst=payload.get("catalyst", ""),
            catalyst_date=date.fromisoformat(raw_date) if raw_date else None,
            rationale=payload.get("rationale", ""),
            news_ids=tuple(int(item) for item in payload.get("news_ids") or ()),
            source=payload.get("source", "ledger"),
            score=float(payload.get("score") or 0.0),
            vol_score=float(payload.get("vol_score") or 0.0),
            drift_sigma=payload.get("drift_sigma"),
            structures=tuple(payload.get("structures") or ()),
            dte_min=payload.get("dte_min"),
            dte_max=payload.get("dte_max"),
        )

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["catalyst_date"] = self.catalyst_date.isoformat() if self.catalyst_date else None
        payload["news_ids"] = list(self.news_ids)
        payload["structures"] = list(self.structures)
        return payload


__all__ = ["DIRECTIONS", "VOLATILITY_VIEWS", "View"]
