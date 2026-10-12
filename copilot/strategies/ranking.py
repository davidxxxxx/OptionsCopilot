"""Rank candidates into the daily Top-N list and project them for storage/GUI."""
from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from typing import Any

from copilot.analysis.models import View
from copilot.config import StrategySettings
from copilot.market.models import AccountSnapshot
from .builder import STRUCTURE_NAMES, Candidate


def idea_key(idea: Mapping[str, Any]) -> tuple:
    legs = []
    for leg in idea["legs"]:
        quantity = leg.get("signed_quantity")
        if quantity is None:
            quantity = leg["quantity"] if leg.get("action") == "买入" else -leg["quantity"]
        legs.append((leg["expiry"], round(float(leg["strike"]), 4), leg["right"], int(quantity)))
    return idea["symbol"], idea["structure"], tuple(sorted(legs))


def candidate_key(candidate: Candidate) -> tuple:
    legs = tuple(sorted(
        (leg.quote.expiry.isoformat(), round(leg.strike, 4), leg.right, leg.quantity)
        for leg in candidate.legs
    ))
    return candidate.symbol, candidate.structure, legs


def rank_candidates(
    candidates: Sequence[Candidate],
    *,
    views: Mapping[str, View],
    settings: StrategySettings,
    account: AccountSnapshot | None = None,
    previous: Sequence[Mapping[str, Any]] = (),
    rule_views: Mapping[str, View] | None = None,
    portfolio: Any = None,
) -> list[dict[str, Any]]:
    """Positive edge only, best edge-per-dollar-at-risk first, capped per symbol.

    Ideas from the previous list get a ``keep_margin`` head start, so a new idea only
    displaces one of them when it is clearly better (stops the list churning on noise).
    """

    kept = {idea_key(idea) for idea in previous}

    cap = loss_cap(account, settings)
    eligible = [
        item for item in candidates
        if item.edge > 0 and item.profile.max_loss > 0
        and (cap is None or item.profile.max_loss <= cap)
    ]
    def selection(item: Candidate) -> tuple[float, float]:
        score = item.return_on_risk
        if candidate_key(item) in kept and score > 0:
            score *= 1 + settings.keep_margin
        return score, item.view_pop

    eligible.sort(key=selection, reverse=True)
    chosen: list[Candidate] = []
    per_symbol: dict[str, int] = {}
    seen: set[tuple[str, str]] = set()
    for candidate in eligible:
        key = (candidate.symbol, candidate.structure)
        if key in seen or per_symbol.get(candidate.symbol, 0) >= settings.max_ideas_per_symbol:
            continue
        chosen.append(candidate)
        seen.add(key)
        per_symbol[candidate.symbol] = per_symbol.get(candidate.symbol, 0) + 1
        if len(chosen) >= settings.max_ideas:
            break
    rule_views = rule_views or {}
    ideas = [
        to_idea(
            candidate, rank,
            view=(rule_views if candidate.origin == "ladder" else views).get(candidate.symbol),
            settings=settings, account=account, portfolio=portfolio,
        )
        for rank, candidate in enumerate(chosen, start=1)
    ]
    if previous:
        for idea in ideas:
            idea["status"] = "kept" if idea_key(idea) in kept else "new"
    return ideas


def dropped_ideas(
    previous: Sequence[Mapping[str, Any]],
    ideas: Sequence[Mapping[str, Any]],
    candidates: Sequence[Candidate],
    *,
    settings: StrategySettings,
    account: AccountSnapshot | None,
    analyzed: set[str] | None = None,
) -> list[dict[str, Any]]:
    """Previous ideas that left the list, each with the reason."""

    current = {idea_key(idea) for idea in ideas}
    pool = {candidate_key(candidate): candidate for candidate in candidates}
    cap = loss_cap(account, settings)
    dropped = []
    for idea in previous:
        key = idea_key(idea)
        if key in current:
            continue
        candidate = pool.get(key)
        if candidate is None and analyzed is not None and idea["symbol"] not in analyzed:
            reason = "该标的观点已结束或本次未分析"
        elif candidate is None:
            reason = "本次无法重新定价（腿已不在报价中、流动性不足或该标的未分析）"
        elif candidate.edge <= 0:
            reason = "期望优势已不为正（观点或价格变化）"
        elif cap is not None and candidate.profile.max_loss > cap:
            reason = "最大亏损超出账户上限"
        else:
            reason = "被明显更优的组合替换"
        dropped.append({
            "symbol": idea["symbol"],
            "structure_name": idea.get("structure_name", idea["structure"]),
            "expiry": idea.get("expiry"),
            "legs": " / ".join(
                f"{leg.get('action', '')}{leg['strike']:g}{leg['right']}" for leg in idea["legs"]
            ),
            "reason": reason,
        })
    return dropped


def loss_cap(account: AccountSnapshot | None, settings: StrategySettings) -> float | None:
    if not account or not account.net_liquidation or settings.max_loss_cap_pct <= 0:
        return None
    return account.net_liquidation * settings.max_loss_cap_pct


def to_idea(
    candidate: Candidate,
    rank: int,
    *,
    view: View | None,
    settings: StrategySettings,
    account: AccountSnapshot | None,
    portfolio: Any = None,
) -> dict[str, Any]:
    profile = candidate.profile
    budget = (
        account.net_liquidation * settings.risk_per_trade_pct
        if account and account.net_liquidation
        else None
    )
    suggested = math.floor(budget / profile.max_loss) if budget else None
    cap = loss_cap(account, settings)
    if suggested == 0 and cap is not None and profile.max_loss <= cap:
        suggested = 1  # one contract is above the normal budget but within the hard cap
    # Portfolio cap: worst case of open positions plus this idea must stay within budget.
    exceeds_budget = suggested == 0 if suggested is not None else None  # per-trade budget only
    remaining = getattr(portfolio, "remaining", None)
    portfolio_ok = remaining is None or profile.max_loss <= remaining
    if suggested is not None and remaining is not None:
        suggested = max(0, min(suggested, math.floor(max(remaining, 0.0) / profile.max_loss)))
    net_delta = sum(
        leg.quantity * (leg.quote.delta or 0.0) * candidate.multiplier for leg in candidate.legs
    )
    direction = 0 if abs(net_delta) < 10 else (1 if net_delta > 0 else -1)
    same_direction = bool(portfolio is not None and portfolio.same_direction(candidate.symbol, direction))
    quote_times = [leg.quote.quote_time for leg in candidate.legs if leg.quote.quote_time]
    data_types = {leg.quote.market_data_type for leg in candidate.legs if leg.quote.market_data_type}
    return {
        "rank": rank,
        "symbol": candidate.symbol,
        "structure": candidate.structure,
        "structure_name": STRUCTURE_NAMES.get(candidate.structure, candidate.structure),
        "expiry": candidate.expiry.isoformat(),
        "dte": candidate.dte,
        "spot": round(candidate.spot, 2),
        "multiplier": candidate.multiplier,
        "origin": candidate.origin,
        "data_source": "cboe" if any(leg.quote.source == "cboe" for leg in candidate.legs) else "ibkr",
        "legs": [
            {
                "action": "买入" if leg.quantity > 0 else "卖出",
                "quantity": abs(leg.quantity),
                "signed_quantity": leg.quantity,
                "right": leg.right,
                "strike": leg.strike,
                "expiry": leg.quote.expiry.isoformat(),
                "bid": leg.quote.bid,
                "ask": leg.quote.ask,
                "fill": round(leg.price, 3),
                "iv": _round(leg.quote.iv, 4),
                "delta": _round(leg.quote.delta, 3),
                "open_interest": leg.quote.open_interest,
                "volume": leg.quote.volume,
            }
            for leg in candidate.legs
        ],
        "net_debit": round(profile.net_debit, 3),
        "net_premium": round(profile.net_debit * candidate.multiplier, 2),
        "max_loss": round(profile.max_loss, 2),
        "max_profit": None if profile.max_profit is None else round(profile.max_profit, 2),
        "breakevens": list(profile.breakevens),
        "fees": round(profile.fees, 2),
        "reward_risk": _round(candidate.reward_risk, 2),
        "view_ev": round(candidate.view_ev, 2),
        "view_pop": round(candidate.view_pop, 3),
        "market_ev": round(candidate.market_ev, 2),
        "market_pop": round(candidate.market_pop, 3),
        "edge": round(candidate.edge, 2),
        "slippage": round(candidate.slippage, 2),
        "return_on_risk": round(candidate.return_on_risk, 4),
        "spread_cost": round(candidate.spread_cost, 2),
        "atm_iv": round(candidate.atm_iv, 4),
        "view_sigma": round(candidate.view_sigma, 4),
        "drift": round(candidate.drift, 4),
        "suggested_quantity": suggested,
        "risk_budget": round(budget, 2) if budget else None,
        "exceeds_budget": exceeds_budget,
        "portfolio_ok": portfolio_ok,
        "portfolio_remaining": None if remaining is None else round(remaining, 2),
        "net_delta": round(net_delta, 1),
        "same_direction": same_direction,
        "already_holding": bool(account and candidate.symbol in account.held_symbols),
        "quote_time": max(quote_times).isoformat() if quote_times else None,
        "delayed_data": bool(data_types - {1}),
        "indicative": any(leg.quote.indicative for leg in candidate.legs),
        "view": view.to_dict() if view else None,
    }


def _round(value: float | None, digits: int) -> float | None:
    return None if value is None or not math.isfinite(value) else round(value, digits)


__all__ = ["candidate_key", "dropped_ideas", "idea_key", "loss_cap", "rank_candidates", "to_idea"]
