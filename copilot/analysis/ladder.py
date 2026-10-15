"""Index drawdown ladder, a rule taken from the reference account.

When an index ETF closes 5% / 10% / 15% below its 52-week closing high, buy the dip
in steps: sell put spreads, then buy call spreads, then longer-dated calls. Each tier
becomes a rule view with pinned structures and expiry window; its expected move (a
modest mean reversion, in standard deviations) is a stated assumption, not a forecast.
"""
from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date

from copilot.config import RulesSettings
from .models import View

TIER_PLANS = {
    1: {"structures": ("bull_put_spread",), "dte": (21, 45), "horizon": 30, "label": "卖出看跌价差"},
    2: {"structures": ("bull_call_spread",), "dte": (30, 60), "horizon": 40, "label": "买入看涨价差"},
    3: {"structures": ("bull_call_spread", "long_call"), "dte": (90, 180), "horizon": 120,
        "label": "买入较长期限（90-180 天）看涨/看涨价差"},
}


@dataclass(frozen=True)
class LadderSignal:
    symbol: str
    as_of: date
    close: float
    high_52w: float
    drawdown: float  # positive fraction below the high
    tier: int  # 0 = not triggered

    def to_dict(self) -> dict[str, object]:
        return {"symbol": self.symbol, "as_of": self.as_of.isoformat(), "close": self.close,
                "high_52w": self.high_52w, "drawdown": round(self.drawdown, 4), "tier": self.tier}


def ladder_signal(symbol: str, closes: Sequence[tuple[date, float]], rules: RulesSettings) -> LadderSignal | None:
    if len(closes) < 120:
        return None  # too little history to know the 52-week high
    window = closes[-252:]
    as_of, close = window[-1]
    high = max(value for _, value in window)
    drawdown = max(0.0, 1 - close / high)
    tier = sum(1 for threshold in rules.ladder_tiers if drawdown >= threshold)
    return LadderSignal(symbol, as_of, close, high, drawdown, tier)


def ladder_view(signal: LadderSignal, rules: RulesSettings) -> View | None:
    if signal.tier <= 0:
        return None
    plan = TIER_PLANS[min(signal.tier, 3)]
    low, high = plan["dte"]
    return View(
        symbol=signal.symbol,
        direction="bullish",
        conviction=min(5, 2 + signal.tier),
        expected_move_pct=0.0,
        horizon_days=plan["horizon"],
        rationale=(
            f"规则：{signal.symbol} 收盘 {signal.close:.2f}，距 52 周收盘高点 {signal.high_52w:.2f} "
            f"回撤 {signal.drawdown:.1%}，触发第 {signal.tier} 级：{plan['label']}"
            f"（假设均值回归 {rules.ladder_drift_sigma[signal.tier - 1]:.2f}σ）"
        ),
        source="ladder",
        drift_sigma=rules.ladder_drift_sigma[signal.tier - 1],
        structures=plan["structures"],
        dte_min=low,
        dte_max=high,
    )


__all__ = ["LadderSignal", "TIER_PLANS", "ladder_signal", "ladder_view"]
