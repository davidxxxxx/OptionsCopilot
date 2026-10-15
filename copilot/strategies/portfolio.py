"""Open option positions: grouping, worst-case risk, direction and exit alerts.

Rules come from the reference account's lessons: take profits early on short premium,
cut debit trades at half the premium, do not carry short premium into earnings or the
last weeks before expiry, and keep the total worst-case loss under a share of equity.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date
from typing import Any

from copilot.analysis.models import View
from copilot.config import RulesSettings
from copilot.market.models import OptionQuote, Position
from copilot.news.dividends import DividendEvent
from .payoff import Leg, analyze_payoff

INDEX_FAMILY = frozenset({"SPY", "QQQ", "IWM", "DIA"})
DELTA_THRESHOLD = 10.0  # share-equivalents below which a group counts as direction-neutral
CASH_SETTLED_INDEXES = frozenset({"NDX", "RUT", "SPX", "XSP"})
ROLL_REASSESSMENT = (
    "如考虑展期，请当作新交易重新评估：旧仓已实现盈亏、新仓新增风险与期限，对比直接平仓"
)


@dataclass
class PositionGroup:
    symbol: str
    legs: list[Position]
    multiplier: int
    entry_cost: float  # dollars: positive = paid (debit), negative = received (credit)
    max_loss: float | None  # None = undefined/unbounded risk
    max_profit: float | None  # None = unlimited
    current_value: float | None = None
    pnl: float | None = None
    net_delta: float | None = None
    gamma: float | None = None
    vega: float | None = None
    theta: float | None = None
    spot: float | None = None
    beta_to_spy: float = 1.0
    beta_fallback: bool = True
    spy_equivalent_delta: float | None = None
    market_legs: list[tuple[Position, OptionQuote]] = field(default_factory=list)
    alerts: list[dict[str, str]] = field(default_factory=list)

    @property
    def expiries(self) -> list[date]:
        return sorted({leg.expiry for leg in self.legs if leg.expiry})

    @property
    def kind(self) -> str:
        return "credit" if self.entry_cost < 0 else "debit"

    @property
    def has_short_leg(self) -> bool:
        return any(leg.quantity < 0 for leg in self.legs)

    @property
    def direction(self) -> int:
        if self.net_delta is None or abs(self.net_delta) < DELTA_THRESHOLD:
            return 0
        return 1 if self.net_delta > 0 else -1

    def to_dict(self, today: date) -> dict[str, Any]:
        nearest = self.expiries[0] if self.expiries else None
        return {
            "symbol": self.symbol,
            "legs": [
                {"label": leg.label, "quantity": leg.quantity, "avg_cost": round(leg.avg_cost, 2),
                 "expiry": leg.expiry.isoformat() if leg.expiry else None, "strike": leg.strike, "right": leg.right}
                for leg in self.legs
            ],
            "kind": self.kind,
            "entry_cost": round(self.entry_cost, 2),
            "max_loss": None if self.max_loss is None else round(self.max_loss, 2),
            "max_profit": None if self.max_profit is None else round(self.max_profit, 2),
            "current_value": None if self.current_value is None else round(self.current_value, 2),
            "pnl": None if self.pnl is None else round(self.pnl, 2),
            "net_delta": None if self.net_delta is None else round(self.net_delta, 1),
            "gamma": None if self.gamma is None else round(self.gamma, 4),
            "vega": None if self.vega is None else round(self.vega, 2),
            "theta": None if self.theta is None else round(self.theta, 2),
            "spot": self.spot,
            "beta_to_spy": round(self.beta_to_spy, 4),
            "beta_fallback": self.beta_fallback,
            "spy_equivalent_delta": (
                None if self.spy_equivalent_delta is None
                else round(self.spy_equivalent_delta, 2)
            ),
            "direction": self.direction,
            "nearest_expiry": nearest.isoformat() if nearest else None,
            "dte": (nearest - today).days if nearest else None,
            "alerts": self.alerts,
        }


@dataclass(frozen=True)
class PortfolioRisk:
    net_liquidation: float | None
    open_risk: float  # summed defined worst-case losses
    undefined: tuple[str, ...]  # symbols whose risk is unbounded/undetermined
    cap: float | None
    remaining: float | None
    directions: Mapping[str, int]

    def to_dict(self) -> dict[str, Any]:
        return {
            "net_liquidation": self.net_liquidation,
            "open_risk": round(self.open_risk, 2),
            "open_risk_pct": round(self.open_risk / self.net_liquidation, 4) if self.net_liquidation else None,
            "undefined": list(self.undefined),
            "cap": None if self.cap is None else round(self.cap, 2),
            "remaining": None if self.remaining is None else round(self.remaining, 2),
        }

    def same_direction(self, symbol: str, direction: int) -> bool:
        if direction == 0:
            return False
        family = INDEX_FAMILY if symbol in INDEX_FAMILY else {symbol}
        return any(self.directions.get(name) == direction for name in family)


def group_positions(positions: Sequence[Position]) -> list[PositionGroup]:
    by_symbol: dict[str, list[Position]] = {}
    for position in positions:
        if position.sec_type == "OPT" and position.expiry and position.strike and position.right:
            by_symbol.setdefault(position.symbol, []).append(position)
    groups = []
    for symbol, legs in sorted(by_symbol.items()):
        multiplier = max(1, legs[0].multiplier or 100)
        entry_cost = sum(leg.quantity * leg.avg_cost for leg in legs)  # IBKR avgCost is per contract
        max_loss, max_profit = _risk(legs, multiplier, entry_cost)
        groups.append(PositionGroup(symbol, legs, multiplier, entry_cost, max_loss, max_profit))
    return groups


def _risk(legs: Sequence[Position], multiplier: int, entry_cost: float) -> tuple[float | None, float | None]:
    expiries = {leg.expiry for leg in legs}
    if len(expiries) == 1:
        payoff_legs = [
            Leg(OptionQuote(symbol=leg.symbol, expiry=leg.expiry, strike=leg.strike, right=leg.right,
                            multiplier=multiplier),
                int(leg.quantity), leg.avg_cost / multiplier)
            for leg in legs
        ]
        profile = analyze_payoff(payoff_legs, multiplier=multiplier, commission_per_contract=0.0)
        if profile is None:
            return None, None
        return profile.max_loss, profile.max_profit
    # Calendars/diagonals: risk is the debit only if every short leg is covered by a long
    # leg of the same right expiring no earlier.
    for short in (leg for leg in legs if leg.quantity < 0):
        cover = sum(
            leg.quantity for leg in legs
            if leg.quantity > 0 and leg.right == short.right and leg.expiry >= short.expiry
        )
        if cover < -short.quantity:
            return None, None
    return (entry_cost, None) if entry_cost > 0 else (None, None)


def mark_to_market(
    group: PositionGroup,
    quotes: Mapping[tuple[date, float, str], OptionQuote],
    *,
    spot: float | None = None,
) -> None:
    value, delta, gamma, vega, theta, complete = 0.0, 0.0, 0.0, 0.0, 0.0, True
    group.market_legs = []
    group.spot = spot
    for leg in group.legs:
        quote = quotes.get((leg.expiry, float(leg.strike), leg.right))
        if quote is None or quote.mid is None:
            complete = False
            continue
        value += leg.quantity * quote.mid * group.multiplier
        group.market_legs.append((leg, quote))
        if quote.delta is not None:
            delta += leg.quantity * quote.delta * group.multiplier
        if quote.gamma is not None:
            gamma += leg.quantity * quote.gamma * group.multiplier
        if quote.vega is not None:
            vega += leg.quantity * quote.vega * group.multiplier
        if quote.theta is not None:
            theta += leg.quantity * quote.theta * group.multiplier
    group.net_delta = delta
    group.gamma = gamma
    group.vega = vega
    group.theta = theta
    if complete:
        group.current_value = value
        group.pnl = value - group.entry_cost


def portfolio_risk(
    groups: Sequence[PositionGroup], net_liquidation: float | None, rules: RulesSettings
) -> PortfolioRisk:
    open_risk = sum(group.max_loss for group in groups if group.max_loss is not None)
    undefined = tuple(group.symbol for group in groups if group.max_loss is None)
    cap = net_liquidation * rules.portfolio_risk_cap_pct if net_liquidation else None
    return PortfolioRisk(
        net_liquidation=net_liquidation,
        open_risk=open_risk,
        undefined=undefined,
        cap=cap,
        remaining=None if cap is None else cap - open_risk,
        directions={group.symbol: group.direction for group in groups},
    )


def exit_alerts(
    group: PositionGroup,
    *,
    today: date,
    rules: RulesSettings,
    earnings: Sequence[date] = (),
    macro_events: Sequence[dict] = (),
    dividends: Sequence[DividendEvent | Mapping[str, Any]] = (),
    view: View | None = None,
    net_liquidation: float | None = None,
) -> list[dict[str, str]]:
    alerts: list[dict[str, str]] = []

    def add(level: str, code: str, text: str) -> None:
        alerts.append({"level": level, "code": code, "text": text})

    pnl = group.pnl
    if pnl is not None:
        decision = profit_exit(group.kind, group.entry_cost, pnl, group.max_profit, rules)
        if decision is not None:
            code, text = decision
            add("action", code, text)
    expiries = group.expiries
    if expiries:
        dte = (expiries[0] - today).days
        if dte <= rules.time_exit_dte:
            add("warn", "expiry", f"距最近到期仅 {dte} 天，Gamma 风险高，建议到期前处理")
        elif group.has_short_leg and group.kind == "credit" and dte <= rules.short_premium_time_exit_dte:
            add("warn", "expiry", f"卖方仓位进入最后 {dte} 天，收益/风险变差，考虑提前平仓")
        if group.has_short_leg and any(today <= event <= expiries[0] for event in earnings):
            event = min(event for event in earnings if event >= today)
            add("action", "earnings", f"{event:%m/%d} 财报在空头腿到期前，建议财报前平掉卖方部分")
        relevant = [
            event for event in macro_events
            if today <= date.fromisoformat(event["date"]) <= expiries[0]
        ]
        if group.has_short_leg and relevant:
            text = "、".join(
                f"{date.fromisoformat(event['date']):%m/%d} 美国 {event['name']} 公布"
                for event in relevant
            )
            add("warn", "macro_event", f"{text}，在空头腿到期前，注意跳空风险")
    for warning in position_assignment_warnings(group, dividends, today=today):
        add(warning["level"], warning["code"], warning["text"])
    if view is not None and view.direction != "neutral" and view.conviction >= 3 and group.direction:
        if (view.direction == "bullish") != (group.direction > 0):
            add("warn", "view_conflict", f"观点账本当前为{'看涨' if view.direction == 'bullish' else '看跌'}（{view.conviction}★），与持仓方向相反")
    if group.max_loss is None:
        add("action", "undefined_risk", "无法确定最大亏损（可能有裸卖腿），请核对")
    elif net_liquidation and group.max_loss > net_liquidation * rules.portfolio_risk_cap_pct:
        add("warn", "oversized", f"这一组最坏亏损 ${group.max_loss:,.0f}，占净值 {group.max_loss / net_liquidation:.0%}，超过组合上限 {rules.portfolio_risk_cap_pct:.0%}")
    return alerts


def profit_exit(
    kind: str,
    entry_cost: float,
    pnl: float,
    max_profit: float | None,
    rules: RulesSettings,
) -> tuple[str, str] | None:
    if kind == "credit":
        credit = -entry_cost
        if max_profit and pnl >= rules.take_profit_credit_pct * max_profit:
            return (
                "take_profit",
                _with_roll_note(
                    f"已赚到最大收益的 {pnl / max_profit:.0%}（≥{rules.take_profit_credit_pct:.0%}），建议平仓锁定"
                ),
            )
        if pnl <= -rules.stop_loss_credit_multiple * credit:
            return (
                "stop_loss",
                _with_roll_note(
                    f"亏损 ${-pnl:,.0f} 已达收取权利金的 {rules.stop_loss_credit_multiple:g} 倍，建议止损"
                ),
            )
        return None
    paid = entry_cost
    if max_profit is not None:
        if pnl >= rules.take_profit_debit_pct * max_profit:
            return (
                "take_profit",
                _with_roll_note(
                    f"已实现最大收益的 {pnl / max_profit:.0%}（≥{rules.take_profit_debit_pct:.0%}），建议止盈"
                ),
            )
    elif paid > 0 and pnl >= rules.take_profit_long_option_pct * paid:
        return (
            "take_profit",
            _with_roll_note(
                f"盈利 {pnl / paid:.0%}，已达止盈线（+{rules.take_profit_long_option_pct:.0%}）"
            ),
        )
    if paid > 0 and pnl <= -rules.stop_loss_debit_pct * paid:
        return (
            "stop_loss",
            _with_roll_note(
                f"亏损 {-pnl / paid:.0%} 的权利金（≥{rules.stop_loss_debit_pct:.0%}），建议止损"
            ),
        )
    return None


def idea_assignment_warnings(
    idea: Mapping[str, Any],
    dividends: Sequence[DividendEvent | Mapping[str, Any]],
    *,
    today: date,
) -> list[dict[str, str]]:
    legs = []
    for leg in idea.get("legs") or ():
        bid = _number(leg.get("bid"))
        ask = _number(leg.get("ask"))
        mark = (bid + ask) / 2 if bid is not None and ask is not None else _number(leg.get("fill"))
        legs.append({
            "quantity": int(leg.get("signed_quantity") or 0),
            "expiry": leg.get("expiry"),
            "strike": leg.get("strike"),
            "right": leg.get("right"),
            "mark": mark,
        })
    return _assignment_warnings(
        str(idea.get("symbol") or ""),
        float(idea.get("spot") or 0.0),
        legs,
        dividends,
        today=today,
    )


def position_assignment_warnings(
    group: PositionGroup,
    dividends: Sequence[DividendEvent | Mapping[str, Any]],
    *,
    today: date,
) -> list[dict[str, str]]:
    quote_map = {
        (position.expiry, position.strike, position.right): quote
        for position, quote in group.market_legs
    }
    legs = []
    for position in group.legs:
        quote = quote_map.get((position.expiry, position.strike, position.right))
        legs.append({
            "quantity": int(position.quantity),
            "expiry": position.expiry,
            "strike": position.strike,
            "right": position.right,
            "mark": quote.mid if quote is not None else None,
        })
    return _assignment_warnings(
        group.symbol,
        float(group.spot or 0.0),
        legs,
        dividends,
        today=today,
    )


def _assignment_warnings(
    symbol: str,
    spot: float,
    legs: Sequence[Mapping[str, Any]],
    dividends: Sequence[DividendEvent | Mapping[str, Any]],
    *,
    today: date,
) -> list[dict[str, str]]:
    events = sorted(
        (_dividend_event(item) for item in dividends),
        key=lambda event: event.ex_date,
    )
    warnings: list[dict[str, str]] = []
    seen: set[str] = set()

    def add(level: str, code: str, text: str) -> None:
        if code not in seen:
            warnings.append({"level": level, "code": code, "text": text})
            seen.add(code)

    has_short_leg = any(int(leg.get("quantity") or 0) < 0 for leg in legs)
    if has_short_leg and not events and symbol.upper() not in CASH_SETTLED_INDEXES:
        add("warn", "dividend_unknown", "除息日未知，请自行核对是否可能提前指派")
    if spot <= 0:
        return warnings

    for leg in legs:
        if int(leg.get("quantity") or 0) >= 0:
            continue
        expiry = _date(leg.get("expiry"))
        strike = _number(leg.get("strike"))
        right = str(leg.get("right") or "")
        mark = _number(leg.get("mark"))
        if expiry is None or strike is None or right not in {"C", "P"}:
            continue
        dte = (expiry - today).days
        intrinsic = max(spot - strike, 0.0) if right == "C" else max(strike - spot, 0.0)
        extrinsic = None if mark is None else max(mark - intrinsic, 0.0)
        if right == "C" and strike <= spot * 1.01 and extrinsic is not None:
            dividend = next(
                (
                    event for event in events
                    if today <= event.ex_date <= expiry and event.amount > extrinsic
                ),
                None,
            )
            if dividend is not None:
                qualifier = "（推算）" if dividend.estimated else ""
                add(
                    "warn",
                    "early_assignment_call",
                    f"{dividend.ex_date:%m/%d} 除息{qualifier}前可能被提前指派",
                )
        if (
            right == "P"
            and 0 <= dte <= 7
            and strike >= spot * 1.05
            and extrinsic is not None
            and extrinsic <= 0.10
        ):
            add("warn", "early_assignment_put", "深度价内短 Put 临近到期且时间价值很低，可能被提前指派")
        if 0 <= dte <= 3:
            if symbol.upper() in CASH_SETTLED_INDEXES:
                add("info", "cash_settlement", "现金结算指数期权，不发生股票交割")
            elif abs(strike - spot) / spot <= 0.01:
                add("warn", "pin_risk", "到期交割/钉住风险，建议到期前处理")
    return warnings


def _with_roll_note(text: str) -> str:
    return f"{text}。{ROLL_REASSESSMENT}"


def _dividend_event(value: DividendEvent | Mapping[str, Any]) -> DividendEvent:
    if isinstance(value, DividendEvent):
        return value
    return DividendEvent.from_dict(dict(value))


def _date(value: Any) -> date | None:
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except ValueError:
        return None


def _number(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


__all__ = [
    "PortfolioRisk",
    "PositionGroup",
    "ROLL_REASSESSMENT",
    "exit_alerts",
    "group_positions",
    "idea_assignment_warnings",
    "mark_to_market",
    "position_assignment_warnings",
    "portfolio_risk",
    "profit_exit",
]
