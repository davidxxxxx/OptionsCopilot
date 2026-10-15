"""Portfolio-level analytics for recommendation relationships and decision quality."""
from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from datetime import date
from typing import Any

from copilot.config import RulesSettings, StrategySettings
from copilot.market.models import AccountSnapshot, OptionQuote, Position
from copilot.news.dividends import DividendEvent
from .builder import Candidate
from .decision_quality import conservative_expected_return, is_admissible
from .portfolio import INDEX_FAMILY, PositionGroup
from .pricing import RISK_FREE_RATE, bs_delta, bs_price


STRESS_SCENARIOS = (
    ("市场 −3%", -0.03, 0.0),
    ("市场 −5%", -0.05, 0.0),
    ("市场 +3%", 0.03, 0.0),
    ("波动 +5 点", 0.0, 0.05),
    ("波动 −3 点", 0.0, -0.03),
    ("市场 −5% 且波动 +5 点", -0.05, 0.05),
)
IDEA_SCENARIO_NAMES = (
    "小涨 +0.5σ 但 IV −3 点（5 天后）",
    "方向正确但晚一周（+1σ，在 view 期限 + 7 天）",
    "事件后 IV 快速回落（−5 点，标的不动，1 天后）",
    "跳空 +2σ（1 天后）",
    "跳空 −2σ（1 天后）",
    "原地不动到退出日",
)
CALENDAR_MODEL_NOTE = "最大亏损为模型估计，依赖远月 IV；合约本身只保证借方价差的损失上限"


def beta_to_spy(
    closes: Sequence[tuple[date, float]],
    spy_closes: Sequence[tuple[date, float]],
    *,
    window: int = 60,
) -> float | None:
    values = {day: close for day, close in closes if close > 0}
    spy_values = {day: close for day, close in spy_closes if close > 0}
    days = sorted(set(values) & set(spy_values))
    pairs: list[tuple[float, float]] = []
    for previous, current in zip(days, days[1:]):
        asset_return = math.log(values[current] / values[previous])
        spy_return = math.log(spy_values[current] / spy_values[previous])
        pairs.append((asset_return, spy_return))
    pairs = pairs[-window:]
    if len(pairs) < 20:
        return None
    asset_mean = sum(asset for asset, _spy in pairs) / len(pairs)
    spy_mean = sum(spy for _asset, spy in pairs) / len(pairs)
    variance = sum((spy - spy_mean) ** 2 for _asset, spy in pairs)
    if variance <= 1e-12:
        return None
    covariance = sum(
        (asset - asset_mean) * (spy - spy_mean) for asset, spy in pairs
    )
    return covariance / variance


def option_greeks(
    spot: float,
    strike: float,
    t_years: float,
    sigma: float,
    right: str,
) -> dict[str, float]:
    if spot <= 0 or strike <= 0 or t_years <= 0 or sigma <= 0:
        return {
            "delta": bs_delta(spot, strike, t_years, sigma, right),
            "gamma": 0.0,
            "vega": 0.0,
            "theta": 0.0,
        }
    sqrt_t = math.sqrt(t_years)
    d1 = (
        math.log(spot / strike) + (RISK_FREE_RATE + 0.5 * sigma * sigma) * t_years
    ) / (sigma * sqrt_t)
    d2 = d1 - sigma * sqrt_t
    density = math.exp(-0.5 * d1 * d1) / math.sqrt(2 * math.pi)
    discount = math.exp(-RISK_FREE_RATE * t_years)
    gamma = density / (spot * sigma * sqrt_t)
    vega = spot * density * sqrt_t / 100.0
    if right == "C":
        theta = (
            -(spot * density * sigma) / (2 * sqrt_t)
            - RISK_FREE_RATE * strike * discount * _norm_cdf(d2)
        ) / 365.0
    else:
        theta = (
            -(spot * density * sigma) / (2 * sqrt_t)
            + RISK_FREE_RATE * strike * discount * _norm_cdf(-d2)
        ) / 365.0
    return {
        "delta": bs_delta(spot, strike, t_years, sigma, right),
        "gamma": gamma,
        "vega": vega,
        "theta": theta,
    }


def candidate_greeks(candidate: Candidate) -> dict[str, float]:
    totals = {"delta_shares": 0.0, "gamma": 0.0, "vega": 0.0, "theta": 0.0}
    for leg in candidate.legs:
        quote = leg.quote
        calculated = option_greeks(
            candidate.spot,
            quote.strike,
            max(candidate.dte, 1) / 365,
            quote.iv or candidate.atm_iv,
            quote.right,
        )
        quantity = leg.quantity * candidate.multiplier
        totals["delta_shares"] += quantity * (
            quote.delta if quote.delta is not None else calculated["delta"]
        )
        totals["gamma"] += quantity * (
            quote.gamma if quote.gamma is not None else calculated["gamma"]
        )
        totals["vega"] += quantity * (
            quote.vega if quote.vega is not None else calculated["vega"]
        )
        totals["theta"] += quantity * (
            quote.theta if quote.theta is not None else calculated["theta"]
        )
    totals["delta_dollars"] = totals["delta_shares"] * candidate.spot
    return totals


def greek_summary(idea: Mapping[str, Any]) -> str:
    greeks = idea.get("greeks") or {}
    delta_dollars = _number(greeks.get("delta_dollars"))
    vega = _number(greeks.get("vega"))
    theta = _number(greeks.get("theta"))
    if delta_dollars is None and vega is None and theta is None:
        return "—"
    delta_dollars = delta_dollars or 0.0
    vega = vega or 0.0
    theta = theta or 0.0
    if abs(delta_dollars) >= max(abs(vega) * 5, abs(theta) * 5):
        primary = "方向（Δ）"
    elif abs(vega) >= abs(theta):
        primary = "波动率（ν）"
    else:
        primary = "时间价值（θ）"
    theta_text = (
        f"每天时间损耗 ${abs(theta):,.2f}"
        if theta < 0
        else f"每天时间收益 ${theta:,.2f}"
    )
    if vega > 0:
        iv_text = f"IV 每降 1 点亏 ${vega:,.2f}"
    elif vega < 0:
        iv_text = f"IV 每降 1 点赚 ${abs(vega):,.2f}"
    else:
        iv_text = "IV 变化影响接近 $0"
    return f"主要靠{primary}，{theta_text}，{iv_text}"


def idea_scenarios(
    idea: Mapping[str, Any],
    *,
    today: date,
) -> list[dict[str, Any]]:
    spot = float(idea.get("spot") or 0.0)
    atm_iv = float(idea.get("atm_iv") or 0.0)
    if spot <= 0 or atm_iv <= 0 or not idea.get("legs"):
        return []
    view = idea.get("view") or {}
    horizon = max(int(view.get("horizon_days") or idea.get("dte") or 1), 1)
    horizon_sigma = atm_iv * math.sqrt(horizon / 365)
    direction = -1.0 if view.get("direction") == "bearish" else 1.0
    cases = (
        (IDEA_SCENARIO_NAMES[0], 5, 0.5, -0.03),
        (IDEA_SCENARIO_NAMES[1], horizon + 7, direction, 0.0),
        (IDEA_SCENARIO_NAMES[2], 1, 0.0, -0.05),
        (IDEA_SCENARIO_NAMES[3], 1, 2.0, 0.0),
        (IDEA_SCENARIO_NAMES[4], 1, -2.0, 0.0),
        (IDEA_SCENARIO_NAMES[5], horizon, 0.0, 0.0),
    )
    rows = []
    max_loss = float(idea.get("max_loss") or 0.0)
    for name, days_forward, sigma_units, iv_shift in cases:
        scenario_spot = spot * math.exp(sigma_units * horizon_sigma)
        valuation, pnl = _reprice_idea(
            idea,
            today=today,
            days_forward=days_forward,
            spot=scenario_spot,
            iv_shift=iv_shift,
        )
        rows.append({
            "scenario": name,
            "days_forward": days_forward,
            "spot": round(scenario_spot, 2),
            "iv_shift": iv_shift,
            "valuation": round(valuation, 2),
            "pnl": round(pnl, 2),
            "max_loss_pct": round(pnl / max_loss, 4) if max_loss > 0 else None,
        })
    return rows


def calendar_sensitivity(
    idea: Mapping[str, Any],
    *,
    today: date,
) -> dict[str, Any] | None:
    expiries = sorted({_date(leg.get("expiry")) for leg in idea.get("legs") or ()})
    expiries = [expiry for expiry in expiries if expiry is not None]
    if len(expiries) < 2:
        return None
    far_expiry = expiries[-1]
    rows = []
    for shift in (-0.03, 0.03):
        valuation, pnl = _reprice_idea(
            idea,
            today=today,
            days_forward=0,
            spot=float(idea.get("spot") or 0.0),
            far_expiry=far_expiry,
            far_iv_shift=shift,
        )
        rows.append({
            "scenario": f"远月 IV {'+' if shift > 0 else '−'}3 点时的估值",
            "iv_shift": shift,
            "valuation": round(valuation, 2),
            "pnl": round(pnl, 2),
        })
    return {"rows": rows, "note": CALENDAR_MODEL_NOTE}


def idea_card_summary(
    idea: Mapping[str, Any],
    *,
    origin_quality: Mapping[str, Mapping[str, Any]],
    account: AccountSnapshot | None,
    rules: RulesSettings,
) -> list[dict[str, str]]:
    view = idea.get("view") or {}
    origin = str(idea.get("origin") or "")
    origin_name = {
        "news": "新闻观点",
        "ladder": "回撤阶梯规则",
        "macro": "宏观事件",
        "vrp": "波动率风险溢价规则",
    }.get(origin)
    direction = {"bullish": "看涨", "bearish": "看跌", "neutral": "中性"}.get(view.get("direction"))
    why_parts = [part for part in (origin_name, direction) if part]
    priced = idea.get("priced_in") or {}
    if priced.get("status") == "ok" and _number(priced.get("priced_in")) is not None:
        why_parts.append(f"已反映 {float(priced['priced_in']):.0%}")

    limit = _number(idea.get("limit_net_price"))
    multiplier = int(idea.get("multiplier") or 100)
    if limit is None:
        price_text = "—"
    elif limit >= 0:
        price_text = f"每组净支出不高于 ${abs(limit) * multiplier:,.0f}"
    else:
        price_text = f"每组净收入不低于 ${abs(limit) * multiplier:,.0f}"

    invalidation = []
    if view.get("direction") in {"bullish", "bearish"}:
        invalidation.append("观点关闭或反向")
    if limit is not None:
        invalidation.append("价格越过限价")
    if view.get("catalyst") or idea.get("macro_events"):
        invalidation.append("事件已过且催化未兑现")

    max_loss = _number(idea.get("max_loss"))
    net_liquidation = _number(account.net_liquidation) if account is not None else None
    if max_loss is None:
        loss_text = "—"
    elif net_liquidation:
        loss_text = f"${max_loss:,.0f}（净值 {max_loss / net_liquidation:.1%}）"
    else:
        loss_text = f"${max_loss:,.0f}"

    if not origin:
        exit_text = "—"
    elif origin == "vrp":
        exit_text = "按 VRP 止盈/止损与事件黑窗规则退出"
    else:
        exit_text = (
            f"止盈、亏损达权利金 {rules.stop_loss_debit_pct:.0%} 或剩 {rules.time_exit_dte} 天退出"
        )

    quality = origin_quality.get(origin) or {}
    verdict = str(quality.get("verdict") or "")
    evidence_parts = [verdict] if verdict else []
    if origin == "news":
        if idea.get("calibration_verified") is True:
            evidence_parts.append("校准已验证")
        elif idea.get("news_unverified"):
            evidence_parts.append("校准未验证")

    return [
        {"label": "为什么现在做", "text": "；".join(why_parts) or "—"},
        {"label": "赚什么钱", "text": greek_summary(idea)},
        {"label": "价格以内值得做", "text": price_text},
        {"label": "什么情况说明判断失效", "text": "；".join(invalidation) or "—"},
        {"label": "最多亏多少", "text": loss_text},
        {"label": "何时退出", "text": exit_text},
        {"label": "证据有多充分", "text": "；".join(evidence_parts) or "—"},
    ]


def annotate_phase3_idea(
    idea: dict[str, Any],
    *,
    today: date,
    dividends: Sequence[DividendEvent | Mapping[str, Any]],
    origin_quality: Mapping[str, Mapping[str, Any]],
    account: AccountSnapshot | None,
    rules: RulesSettings,
) -> None:
    from .portfolio import idea_assignment_warnings

    idea["greek_summary"] = greek_summary(idea)
    idea["scenarios"] = idea_scenarios(idea, today=today)
    idea["dividends"] = [
        event.to_dict() if isinstance(event, DividendEvent) else dict(event)
        for event in dividends
    ]
    sensitivity = calendar_sensitivity(idea, today=today)
    if sensitivity is not None:
        idea["calendar_sensitivity"] = sensitivity
    idea["assignment_warnings"] = idea_assignment_warnings(idea, dividends, today=today)
    idea["card_summary"] = idea_card_summary(
        idea,
        origin_quality=origin_quality,
        account=account,
        rules=rules,
    )


def annotate_idea_beta(idea: dict[str, Any], beta: float | None) -> None:
    effective = 1.0 if beta is None else beta
    idea["beta_to_spy"] = round(effective, 4)
    idea["beta_fallback"] = beta is None
    greeks = idea.setdefault("greeks", {})
    greeks["spy_equivalent_delta"] = round(
        float(greeks.get("delta_dollars") or 0.0) * effective, 2
    )


def allocate_joint_budget(
    collections: Sequence[list[dict[str, Any]]],
    *,
    account: AccountSnapshot | None,
    settings: StrategySettings,
    remaining: float | None,
    origin_verified: Mapping[str, bool],
) -> dict[str, Any]:
    available = remaining
    allocated = 0.0
    alternative_losers = _alternative_losers(collections)
    for collection in collections:
        for idea in collection:
            max_loss = float(idea.get("live_max_loss", idea.get("max_loss") or 0.0))
            verified = _idea_verified(idea, origin_verified)
            risk_pct = settings.risk_per_trade_pct
            if not verified:
                risk_pct = float(
                    settings.unverified_risk_per_trade_pct
                    if settings.unverified_risk_per_trade_pct is not None
                    else settings.risk_per_trade_pct
                )
            per_trade = account.net_liquidation * risk_pct if account and account.net_liquidation else None
            idea["risk_verified"] = verified
            if per_trade is not None:
                idea["risk_budget"] = round(per_trade, 2)
            winner_rank = alternative_losers.get(id(idea))
            if winner_rank is not None:
                idea["suggested_quantity"] = 0
                idea["quantity_reason"] = f"与 #{winner_rank} 互为替代方案，只需选一个"
                continue
            if max_loss <= 0 or per_trade is None:
                continue
            previous_quantity = int(idea.get("suggested_quantity") or 0)
            quantity = math.floor(per_trade / max_loss)
            if verified and quantity == 0 and previous_quantity > 0:
                quantity = 1
            if available is not None:
                quantity = min(quantity, math.floor(max(available, 0.0) / max_loss))
            idea["suggested_quantity"] = max(0, quantity)
            idea.pop("quantity_reason", None)
            if quantity <= 0:
                idea["quantity_reason"] = (
                    "超出未验证推荐的单笔预算"
                    if not verified and per_trade < max_loss
                    else "组合预算已被前面的推荐用完"
                )
                continue
            used = quantity * max_loss
            allocated += used
            if available is not None:
                available -= used
    return {
        "starting_budget": None if remaining is None else round(remaining, 2),
        "allocated": round(allocated, 2),
        "remaining": None if available is None else round(available, 2),
    }


def annotate_relationships(
    ideas: Sequence[dict[str, Any]],
    positions: Sequence[PositionGroup],
) -> list[dict[str, Any]]:
    conflicts: list[dict[str, Any]] = []
    for idea in ideas:
        idea["relationships"] = []
    for index, left in enumerate(ideas):
        for right in ideas[index + 1:]:
            if not _same_family(str(left["symbol"]), str(right["symbol"])):
                continue
            left_delta = _spy_delta(left)
            right_delta = _spy_delta(right)
            if left_delta * right_delta < 0:
                relation = "冲突"
                payload = {
                    "type": relation,
                    "other": _idea_label(right),
                    "net_spy_equivalent_delta": round(left_delta + right_delta, 2),
                    "combined_costs": round(_entry_costs(left) + _entry_costs(right), 2),
                }
                left["relationships"].append(payload)
                right["relationships"].append({**payload, "other": _idea_label(left)})
                conflicts.append({
                    "left": _idea_label(left),
                    "right": _idea_label(right),
                    "net_spy_equivalent_delta": payload["net_spy_equivalent_delta"],
                    "combined_costs": payload["combined_costs"],
                })
            elif left_delta * right_delta > 0 and left["symbol"] == right["symbol"]:
                relation = "替代方案"
                left["relationships"].append({"type": relation, "other": _idea_label(right)})
                right["relationships"].append({"type": relation, "other": _idea_label(left)})
    for idea in ideas:
        idea_delta = _spy_delta(idea)
        for group in positions:
            if not _same_family(str(idea["symbol"]), group.symbol) or not group.net_delta:
                continue
            position_delta = float(
                group.spy_equivalent_delta
                if group.spy_equivalent_delta is not None
                else group.net_delta * float(group.spot or idea.get("spot") or 0.0)
            )
            relation = "对冲现有持仓" if idea_delta * position_delta < 0 else "加大现有暴露"
            idea["relationships"].append({"type": relation, "other": f"现有 {group.symbol} 持仓"})
        if not idea["relationships"]:
            idea["relationships"].append({"type": "独立"})
    return conflicts


def combined_exposure(
    ideas: Sequence[Mapping[str, Any]],
    positions: Sequence[PositionGroup] = (),
    *,
    conflicts: Sequence[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    result = {
        "max_loss": 0.0,
        "spy_equivalent_delta": 0.0,
        "vega": 0.0,
        "theta": 0.0,
        "entry_costs": 0.0,
        "conflicts": list(conflicts),
    }
    for idea in ideas:
        quantity = int(idea.get("suggested_quantity") or 0)
        result["max_loss"] += quantity * float(idea.get("live_max_loss", idea.get("max_loss") or 0.0))
        result["spy_equivalent_delta"] += quantity * _spy_delta(idea)
        result["vega"] += quantity * float((idea.get("greeks") or {}).get("vega") or 0.0)
        result["theta"] += quantity * float((idea.get("greeks") or {}).get("theta") or 0.0)
        result["entry_costs"] += quantity * _entry_costs(idea)
    for group in positions:
        result["max_loss"] += float(group.max_loss or 0.0)
        result["spy_equivalent_delta"] += float(getattr(group, "spy_equivalent_delta", 0.0) or 0.0)
        result["vega"] += float(getattr(group, "vega", 0.0) or 0.0)
        result["theta"] += float(getattr(group, "theta", 0.0) or 0.0)
    for key in ("max_loss", "spy_equivalent_delta", "vega", "theta", "entry_costs"):
        result[key] = round(result[key], 2)
    return result


def structure_alternatives(
    listed: Mapping[str, Any],
    candidates: Sequence[Candidate],
    settings: StrategySettings,
) -> list[dict[str, Any]]:
    target_loss = float(listed.get("max_loss") or 0.0)
    horizon = max(int(((listed.get("view") or {}).get("horizon_days") or listed.get("dte") or 1)), 1)
    rows: list[dict[str, Any]] = []
    for candidate in candidates:
        if candidate.symbol != listed.get("symbol") or candidate.profile.max_loss <= 0:
            continue
        scale = target_loss / candidate.profile.max_loss if target_loss > 0 else 1.0
        greeks = candidate_greeks(candidate)
        sigma_move = candidate.atm_iv * math.sqrt(horizon / 365)
        breakeven_distance = min(
            (abs(math.log(level / candidate.spot)) / sigma_move for level in candidate.profile.breakevens if level > 0),
            default=None,
        )
        required_move = min(
            (abs(level / candidate.spot - 1.0) for level in candidate.profile.breakevens),
            default=None,
        )
        rows.append({
            "structure": candidate.structure,
            "expected_return": round(conservative_expected_return(candidate) * scale, 2),
            "pop": round(candidate.view_pop, 4),
            "breakeven_sigma": None if breakeven_distance is None else round(breakeven_distance, 3),
            "max_profit": None if candidate.profile.max_profit is None else round(candidate.profile.max_profit * scale, 2),
            "vega": round(greeks["vega"] * scale, 2),
            "theta": round(greeks["theta"] * scale, 2),
            "required_move_pct": None if required_move is None else round(required_move, 4),
            "admissible": is_admissible(candidate, settings),
            "normalization": round(scale, 4),
            "explanation": _alternative_explanation(candidate.structure, greeks["theta"] * scale),
        })
    best: dict[str, dict[str, Any]] = {}
    for row in rows:
        current = best.get(row["structure"])
        if current is None or row["expected_return"] > current["expected_return"]:
            best[row["structure"]] = row
    return sorted(best.values(), key=lambda row: row["expected_return"], reverse=True)


def stress_test(
    ideas: Sequence[Mapping[str, Any]],
    positions: Sequence[PositionGroup],
    *,
    today: date | None = None,
) -> list[dict[str, Any]]:
    valuation_day = today or date.today()
    output = []
    for name, market_shock, iv_shift in STRESS_SCENARIOS:
        idea_pnl = sum(_stress_idea(idea, market_shock, iv_shift) for idea in ideas)
        position_pnl = sum(
            _stress_position(group, market_shock, iv_shift, valuation_day)
            for group in positions
        )
        output.append({
            "scenario": name,
            "positions_pnl": round(position_pnl, 2),
            "ideas_pnl": round(idea_pnl, 2),
            "combined_pnl": round(position_pnl + idea_pnl, 2),
        })
    return output


def fixed_vertical_benchmark(
    quotes: Sequence[OptionQuote],
    *,
    symbol: str,
    spot: float,
    today: date,
    direction: str,
    max_loss: float,
    fill_ratio: float,
) -> dict[str, Any] | None:
    expiries = sorted({quote.expiry for quote in quotes if quote.expiry > today})
    if not expiries:
        return None
    expiry = min(expiries, key=lambda value: abs((value - today).days - 30))
    right = "C" if direction == "bullish" else "P"
    side = [
        quote for quote in quotes
        if quote.expiry == expiry
        and quote.right == right
        and quote.mid is not None
        and quote.bid is not None
        and quote.ask is not None
    ]
    if len(side) < 2:
        return None
    t_years = max((expiry - today).days, 1) / 365
    with_delta = [
        (
            quote,
            quote.delta if quote.delta is not None else bs_delta(
                spot, quote.strike, t_years, quote.iv or 0.25, right
            ),
        )
        for quote in side
    ]
    long_quote = min(with_delta, key=lambda item: abs(abs(item[1]) - 0.50))[0]
    if direction == "bullish":
        shorts = [item for item in with_delta if item[0].strike > long_quote.strike]
    else:
        shorts = [item for item in with_delta if item[0].strike < long_quote.strike]
    if not shorts:
        return None
    short_quote = min(shorts, key=lambda item: abs(abs(item[1]) - 0.30))[0]
    long_fill = _entry_fill(long_quote, 1, fill_ratio)
    short_fill = _entry_fill(short_quote, -1, fill_ratio)
    debit = long_fill - short_fill
    if debit <= 0:
        return None
    scale = max_loss / (debit * long_quote.multiplier) if max_loss > 0 else 1.0
    return {
        "symbol": symbol,
        "expiry": expiry.isoformat(),
        "right": right,
        "entry_spot": spot,
        "entry_value": round(debit * long_quote.multiplier * scale, 4),
        "scale": scale,
        "legs": [
            _benchmark_leg(long_quote, 1, long_fill),
            _benchmark_leg(short_quote, -1, short_fill),
        ],
    }


def match_open_position(
    idea: Mapping[str, Any], positions: Sequence[Position]
) -> dict[str, Any]:
    matched: list[Position] = []
    for leg in idea.get("legs") or ():
        quantity = int(leg["signed_quantity"])
        position = next((
            position for position in positions
            if position.symbol == idea.get("symbol")
            and position.sec_type == "OPT"
            and position.expiry == date.fromisoformat(leg["expiry"])
            and position.strike is not None
            and abs(position.strike - float(leg["strike"])) < 1e-6
            and position.right == leg["right"]
            and position.quantity * quantity > 0
        ), None)
        if position is None:
            return {"taken": False, "average_cost": None}
        matched.append(position)
    return {
        "taken": True,
        "average_cost": round(sum(abs(position.avg_cost) for position in matched), 2),
    }


def _reprice_idea(
    idea: Mapping[str, Any],
    *,
    today: date,
    days_forward: int,
    spot: float,
    iv_shift: float = 0.0,
    far_expiry: date | None = None,
    far_iv_shift: float = 0.0,
) -> tuple[float, float]:
    multiplier = int(idea.get("multiplier") or 100)
    value = 0.0
    entry = 0.0
    for leg in idea.get("legs") or ():
        expiry = _date(leg.get("expiry"))
        strike = _number(leg.get("strike"))
        fill = _number(leg.get("fill"))
        if expiry is None or strike is None or fill is None:
            continue
        sigma = float(leg.get("iv") or idea.get("atm_iv") or 0.25) + iv_shift
        if far_expiry is not None and expiry == far_expiry:
            sigma += far_iv_shift
        t_years = max((expiry - today).days - days_forward, 0) / 365
        priced = bs_price(max(spot, 0.01), strike, t_years, max(sigma, 0.01), str(leg["right"]))
        quantity = int(leg.get("signed_quantity") or 0)
        value += quantity * priced
        entry += quantity * fill
    valuation = value * multiplier
    pnl = (value - entry) * multiplier - float(idea.get("fees") or 0.0)
    return valuation, pnl


def _stress_idea(idea: Mapping[str, Any], market_shock: float, iv_shift: float) -> float:
    quantity = int(idea.get("suggested_quantity") or 0)
    if quantity <= 0:
        return 0.0
    spot = float(idea.get("spot") or 0.0)
    beta = float(idea.get("beta_to_spy") or 1.0)
    shocked_spot = max(0.01, spot * (1 + beta * market_shock))
    total = 0.0
    for leg in idea.get("legs") or ():
        sigma = max(0.01, float(leg.get("iv") or idea.get("atm_iv") or 0.25) + iv_shift)
        t_years = max(int(idea.get("dte") or 1) - 1, 0) / 365
        value = bs_price(shocked_spot, float(leg["strike"]), t_years, sigma, leg["right"])
        total += int(leg["signed_quantity"]) * (value - float(leg["fill"]))
    return quantity * (
        total * int(idea.get("multiplier") or 100) - float(idea.get("fees") or 0.0)
    )


def _stress_position(
    group: PositionGroup, market_shock: float, iv_shift: float, today: date
) -> float:
    spot = float(getattr(group, "spot", 0.0) or 0.0)
    if spot <= 0:
        return 0.0
    beta = float(getattr(group, "beta_to_spy", 1.0) or 1.0)
    shocked_spot = max(0.01, spot * (1 + beta * market_shock))
    pnl = 0.0
    for position, quote in getattr(group, "market_legs", ()):
        if quote.mid is None or position.expiry is None or position.strike is None or position.right is None:
            continue
        t_years = max((position.expiry - today).days - 1, 0) / 365
        sigma = max(0.01, float(quote.iv or 0.25) + iv_shift)
        stressed = bs_price(shocked_spot, position.strike, t_years, sigma, position.right)
        pnl += position.quantity * (stressed - quote.mid) * group.multiplier
    return pnl


def _idea_verified(idea: Mapping[str, Any], origin_verified: Mapping[str, bool]) -> bool:
    origin = str(idea.get("origin") or "news")
    if origin == "news":
        return not bool(idea.get("news_unverified"))
    return bool(origin_verified.get(origin, False))


def _same_family(left: str, right: str) -> bool:
    return left == right or (left in INDEX_FAMILY and right in INDEX_FAMILY)


def _alternative_losers(
    collections: Sequence[Sequence[Mapping[str, Any]]],
) -> dict[int, int]:
    grouped: dict[
        tuple[str, str],
        list[tuple[int, int, int, Mapping[str, Any]]],
    ] = {}
    order = 0
    for collection_index, collection in enumerate(collections):
        for idea in collection:
            order += 1
            direction = _idea_direction(idea)
            if direction == "neutral":
                continue
            rank = int(idea.get("rank") or order)
            key = (str(idea.get("symbol") or ""), direction)
            grouped.setdefault(key, []).append((collection_index, rank, order, idea))
    losers: dict[int, int] = {}
    for rows in grouped.values():
        if len(rows) < 2:
            continue
        _, winner_rank, _, winner = min(rows, key=lambda row: (row[0], row[1], row[2]))
        for _, _, _, idea in rows:
            if idea is not winner:
                losers[id(idea)] = winner_rank
    return losers


def _idea_direction(idea: Mapping[str, Any]) -> str:
    direction = str((idea.get("view") or {}).get("direction") or "")
    if direction in {"bullish", "bearish"}:
        return direction
    delta = _spy_delta(idea)
    return "bullish" if delta > 0 else "bearish" if delta < 0 else "neutral"


def _spy_delta(idea: Mapping[str, Any]) -> float:
    return float((idea.get("greeks") or {}).get("spy_equivalent_delta") or 0.0)


def _entry_costs(idea: Mapping[str, Any]) -> float:
    return float(idea.get("slippage") or 0.0) + float(idea.get("fees") or 0.0)


def _idea_label(idea: Mapping[str, Any]) -> str:
    return f"{idea.get('symbol')} {idea.get('structure_name', idea.get('structure'))}"


def _alternative_explanation(structure: str, theta: float) -> str:
    if structure in {"bull_call_spread", "bear_put_spread"}:
        return "温和方向性行情、空间有限 → 借方价差；时间损耗受限"
    if structure in {"bull_put_spread", "bear_call_spread"}:
        return "不逆向即可且 IV 较高 → 贷方价差，但尾部风险更集中"
    if structure in {"long_call", "long_put"}:
        return f"需要快速大幅运动 → 买入期权；时间损耗 θ {theta:.2f}/天"
    return "收益形状不同；按盈亏平衡、波动和时间损耗比较"


def _entry_fill(quote: OptionQuote, quantity: int, fill_ratio: float) -> float:
    assert quote.mid is not None and quote.bid is not None and quote.ask is not None
    if quantity > 0:
        return quote.mid + fill_ratio * (quote.ask - quote.mid)
    return quote.mid - fill_ratio * (quote.mid - quote.bid)


def _benchmark_leg(quote: OptionQuote, quantity: int, fill: float) -> dict[str, Any]:
    return {
        "expiry": quote.expiry.isoformat(),
        "strike": quote.strike,
        "right": quote.right,
        "signed_quantity": quantity,
        "fill": fill,
        "iv": quote.iv,
        "multiplier": quote.multiplier,
    }


def _norm_cdf(value: float) -> float:
    return 0.5 * (1.0 + math.erf(value / math.sqrt(2.0)))


def _date(value: Any) -> date | None:
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None


def _number(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


__all__ = [
    "STRESS_SCENARIOS",
    "CALENDAR_MODEL_NOTE",
    "IDEA_SCENARIO_NAMES",
    "allocate_joint_budget",
    "annotate_phase3_idea",
    "annotate_idea_beta",
    "annotate_relationships",
    "beta_to_spy",
    "calendar_sensitivity",
    "candidate_greeks",
    "combined_exposure",
    "fixed_vertical_benchmark",
    "greek_summary",
    "idea_card_summary",
    "idea_scenarios",
    "match_open_position",
    "option_greeks",
    "stress_test",
    "structure_alternatives",
]
