"""Pure paper-tracking calculations for ranked option ideas."""
from __future__ import annotations

import math
import random
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from typing import Any

from copilot.config import RulesSettings, VrpSettings
from copilot.market.models import OptionQuote, valid_price
from copilot.strategies.portfolio import profit_exit
from copilot.strategies.decision_portfolio import option_greeks


def entry_record(
    idea: dict[str, Any],
    *,
    run_id: int,
    slot: str,
    trigger: str,
    pricing: str,
    params_version: str,
    market_state: dict[str, Any] | None,
) -> dict[str, Any]:
    return {
        "idea": idea,
        "run_id": run_id,
        "slot": slot,
        "trigger": trigger,
        "pricing": pricing,
        "params_version": params_version,
        "market_state": market_state,
    }


def leg_exit_price(quote: OptionQuote, quantity: int, fill_ratio: float) -> float | None:
    bid = quote.bid if valid_price(quote.bid) else 0.0
    ask = quote.ask if valid_price(quote.ask) else None
    if quantity > 0:
        if ask is None:
            return bid if bid > 0 else None
        mid = (bid + ask) / 2
        return mid - fill_ratio * (mid - bid)
    if ask is None:
        return None
    mid = (bid + ask) / 2
    return mid + fill_ratio * (ask - mid)


def mark_idea(
    entry: Mapping[str, Any],
    quotes: Mapping[tuple[date, float, str], OptionQuote],
    *,
    spot: float,
    today: date,
    fill_ratio: float,
    marked_at: datetime,
    quote_time: datetime | None,
    previous_mark: Mapping[str, Any] | None = None,
) -> dict[str, Any] | None:
    idea = entry["idea"]
    value = 0.0
    value_mid = 0.0
    expiries: list[date] = []
    net_greeks = {"delta_shares": 0.0, "gamma": 0.0, "vega": 0.0, "theta": 0.0}
    iv_weighted = 0.0
    iv_weight = 0.0
    for leg in idea["legs"]:
        expiry = date.fromisoformat(leg["expiry"])
        quantity = int(leg["signed_quantity"])
        quote = quotes.get((expiry, round(float(leg["strike"]), 4), leg["right"]))
        if quote is None:
            return None
        exit_price = leg_exit_price(quote, quantity, fill_ratio)
        if exit_price is None:
            return None
        bid = quote.bid if valid_price(quote.bid) else 0.0
        ask = quote.ask if valid_price(quote.ask) else None
        midpoint = (bid + ask) / 2 if ask is not None else bid
        value += quantity * exit_price
        value_mid += quantity * midpoint
        expiries.append(expiry)
        calculated = option_greeks(
            spot,
            float(leg["strike"]),
            max((expiry - today).days, 1) / 365,
            float(quote.iv or leg.get("iv") or idea.get("atm_iv") or 0.25),
            leg["right"],
        )
        scale = quantity * int(idea.get("multiplier") or 100)
        net_greeks["delta_shares"] += scale * (
            quote.delta if quote.delta is not None else calculated["delta"]
        )
        net_greeks["gamma"] += scale * (
            quote.gamma if quote.gamma is not None else calculated["gamma"]
        )
        net_greeks["vega"] += scale * (
            quote.vega if quote.vega is not None else calculated["vega"]
        )
        net_greeks["theta"] += scale * (
            quote.theta if quote.theta is not None else calculated["theta"]
        )
        if quote.iv is not None:
            iv_weighted += abs(quantity) * quote.iv
            iv_weight += abs(quantity)
    multiplier = int(idea.get("multiplier") or 100)
    net_debit = float(idea["net_debit"])
    fees = float(idea.get("fees") or 0.0)
    pnl = (value - net_debit) * multiplier - fees
    pnl_mid = (value_mid - net_debit) * multiplier - fees
    max_loss = float(idea.get("max_loss") or 0.0)
    current_iv = iv_weighted / iv_weight if iv_weight else float(idea.get("atm_iv") or 0.25)
    net_greeks["delta_dollars"] = net_greeks["delta_shares"] * spot
    mark = {
        "marked_at": marked_at.isoformat(),
        "quote_time": quote_time.isoformat() if quote_time else None,
        "spot": round(spot, 2),
        "dte": (min(expiries) - today).days,
        "value": round(value, 2),
        "value_mid": round(value_mid, 2),
        "pnl": round(pnl, 2),
        "pnl_mid": round(pnl_mid, 2),
        "return_on_risk": round(pnl / max_loss, 4) if max_loss > 0 else None,
        "iv": round(current_iv, 6),
        "greeks": {key: round(value, 6) for key, value in net_greeks.items()},
    }
    mark["benchmarks"] = _mark_benchmarks(idea, quotes, spot=spot)
    mark["direction_hit"] = _direction_hit(idea, spot)
    mark["attribution"] = _attribution(
        idea, mark, previous_mark, marked_at=marked_at
    )
    return mark


def exit_decision(
    entry: Mapping[str, Any],
    mark: Mapping[str, Any],
    *,
    today: date,
    rules: RulesSettings,
    vrp: VrpSettings | None = None,
) -> tuple[str, str] | None:
    idea = entry["idea"]
    multiplier = int(idea.get("multiplier") or 100)
    net_debit = float(idea["net_debit"])
    kind = "credit" if net_debit < 0 else "debit"
    if idea.get("origin") == "vrp" and vrp is not None:
        pnl_mid = float(mark.get("pnl_mid", mark["pnl"]))
        max_profit = float(idea.get("max_profit") or 0.0)
        credit = -net_debit * multiplier
        if max_profit > 0 and pnl_mid >= max_profit * vrp.take_profit_pct:
            return "take_profit", f"VRP 已获得最大利润的 {vrp.take_profit_pct:.0%}"
        if credit > 0 and pnl_mid <= -credit * vrp.stop_credit_multiple:
            return "stop_loss", f"VRP 中间价亏损达到初始权利金的 {vrp.stop_credit_multiple:g} 倍"
        dte = int(mark["dte"])
        if int(idea["dte"]) > vrp.time_exit_dte and dte <= vrp.time_exit_dte:
            return "time_exit", f"VRP 距到期 {dte} 天，按规则平仓"
        if dte <= 0:
            return "expiry", "到期日按最后报价结算"
        return None
    decision = profit_exit(
        kind,
        net_debit * multiplier,
        float(mark.get("pnl_mid", mark["pnl"])),
        None if idea.get("max_profit") is None else float(idea["max_profit"]),
        rules,
    )
    if decision is not None:
        return decision
    dte = int(mark["dte"])
    if int(idea["dte"]) > rules.time_exit_dte and dte <= rules.time_exit_dte:
        return "time_exit", f"距到期 {dte} 天，按规则平仓"
    if dte <= 0:
        return "expiry", "到期日按最后报价结算"
    return None


def settle_unmarked(
    entry: Mapping[str, Any], last_mark: Mapping[str, Any] | None, *, today: date
) -> dict[str, Any] | None:
    expiry = min(date.fromisoformat(leg["expiry"]) for leg in entry["idea"]["legs"])
    if today <= expiry:
        return None
    payload = {
        "code": "expired_unmarked",
        "text": "到期前未能估值，按最后一次估值结算（近似）",
        "spot": None,
        "value": None,
        "pnl": None,
        "pnl_mid": None,
        "return_on_risk": None,
        "approximate": True,
    }
    if last_mark is not None:
        for key in (
            "spot", "value", "pnl", "pnl_mid", "return_on_risk",
            "attribution", "benchmarks", "direction_hit",
        ):
            payload[key] = last_mark.get(key)
    return payload


def tracking_stats(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    closed = [row for row in rows if row.get("status") == "closed"]
    opened = [row for row in rows if row.get("status") == "open"]
    approximate = [row for row in closed if bool((row.get("exit") or {}).get("approximate"))]
    settled = [
        row for row in closed
        if not bool((row.get("exit") or {}).get("approximate"))
        and _number((row.get("exit") or {}).get("pnl")) is not None
    ]
    core = _metrics(settled)
    result = {
        "closed": len(closed),
        "open": len(opened),
        "unsettled": len(closed) - len(settled),
        "approximate_exits": len(approximate),
        **core,
        "open_unrealized": round(sum(
            _number((row.get("mark") or {}).get("pnl")) or 0.0 for row in opened
        ), 2),
        "exit_reasons": _counts((row.get("exit") or {}).get("code") for row in closed),
        "min_sample": 30,
        "enough_sample": len(settled) >= 30,
    }
    groupings = {
        "by_origin": lambda row: row.get("origin") or "—",
        "by_structure": lambda row: row.get("structure") or "—",
        "by_conviction": lambda row: str(
            (((row.get("entry") or {}).get("idea") or {}).get("view") or {}).get("conviction") or "—"
        ),
        "by_params_version": lambda row: row.get("params_version") or "—",
        "by_edge_bucket": lambda row: _edge_bucket(
            _number(((row.get("entry") or {}).get("idea") or {}).get("return_on_risk"))
        ),
        "by_macro_event": lambda row: (
            "跨越重大事件"
            if (((row.get("entry") or {}).get("idea") or {}).get("macro_events"))
            else "不跨越"
        ),
    }
    for name, key_fn in groupings.items():
        buckets: dict[str, list[Mapping[str, Any]]] = {}
        for row in settled:
            buckets.setdefault(key_fn(row), []).append(row)
        result[name] = {key: _group_metrics(items) for key, items in sorted(buckets.items())}
    result["origin_quality"] = {
        origin: _origin_quality(items)
        for origin, items in _bucket(settled, lambda row: str(row.get("origin") or "news")).items()
    }
    for origin in ("news", "ladder", "macro", "vrp"):
        result["origin_quality"].setdefault(origin, _origin_quality(()))
    result["actual_vs_paper"] = {
        "actual": _metrics([
            row for row in settled
            if bool((((row.get("entry") or {}).get("idea") or {}).get("taken")))
        ]),
        "paper": _metrics([
            row for row in settled
            if not bool((((row.get("entry") or {}).get("idea") or {}).get("taken")))
        ]),
    }
    return result


def _metrics(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    pnls = [_number((row.get("exit") or {}).get("pnl")) or 0.0 for row in rows]
    risks = [_number(((row.get("entry") or {}).get("idea") or {}).get("max_loss")) or 0.0 for row in rows]
    actual_returns = [
        value for row in rows
        if (value := _number((row.get("exit") or {}).get("return_on_risk"))) is not None
    ]
    predicted = [
        value for row in rows
        if (value := _number(((row.get("entry") or {}).get("idea") or {}).get("return_on_risk"))) is not None
    ]
    view_pop = [
        value for row in rows
        if (value := _number(((row.get("entry") or {}).get("idea") or {}).get("view_pop"))) is not None
    ]
    market_pop = [
        value for row in rows
        if (value := _number(((row.get("entry") or {}).get("idea") or {}).get("market_pop"))) is not None
    ]
    pnl_total = sum(pnls)
    risk_total = sum(risks)
    wins = sum(value > 0 for value in pnls)
    return {
        "count": len(rows),
        "wins": wins,
        "win_rate": wins / len(rows) if rows else None,
        "pnl_total": round(pnl_total, 2),
        "risk_total": round(risk_total, 2),
        "return_on_risk": round(pnl_total / risk_total, 4) if risk_total > 0 else None,
        "avg_return_on_risk": _mean(actual_returns),
        "predicted_return_on_risk": _mean(predicted),
        "model_pop": _mean(view_pop),
        "market_pop": _mean(market_pop),
    }


def _group_metrics(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    metrics = _metrics(rows)
    return {
        "count": len(rows),
        "win_rate": metrics["win_rate"],
        "pnl_total": metrics["pnl_total"],
        "return_on_risk": metrics["return_on_risk"],
        "predicted_return_on_risk": metrics["predicted_return_on_risk"],
    }


def _attribution(
    idea: Mapping[str, Any],
    mark: Mapping[str, Any],
    previous_mark: Mapping[str, Any] | None,
    *,
    marked_at: datetime,
) -> dict[str, Any]:
    previous_spot = float(
        (previous_mark or {}).get("spot") or idea.get("spot") or mark.get("spot") or 0.0
    )
    current_spot = float(mark.get("spot") or previous_spot)
    previous_iv = float((previous_mark or {}).get("iv") or idea.get("atm_iv") or mark.get("iv") or 0.25)
    current_iv = float(mark.get("iv") or previous_iv)
    previous_greeks = (previous_mark or {}).get("greeks") or idea.get("greeks") or {}
    delta_s = current_spot - previous_spot
    elapsed_days = 0.0
    if previous_mark and previous_mark.get("marked_at"):
        elapsed_days = max(
            0.0,
            (marked_at - datetime.fromisoformat(str(previous_mark["marked_at"]))).total_seconds()
            / 86400,
        )
    components = {
        "delta": float(previous_greeks.get("delta_shares") or 0.0) * delta_s,
        "gamma": 0.5 * float(previous_greeks.get("gamma") or 0.0) * delta_s * delta_s,
        "vega": float(previous_greeks.get("vega") or 0.0) * (current_iv - previous_iv) * 100,
        "theta": float(previous_greeks.get("theta") or 0.0) * elapsed_days,
        "costs": 0.0,
    }
    if previous_mark is None:
        components["costs"] = -(
            float(idea.get("slippage") or 0.0) + float(idea.get("fees") or 0.0) / 2
        )
    pnl_change = float(mark.get("pnl") or 0.0) - float((previous_mark or {}).get("pnl") or 0.0)
    components["residual"] = pnl_change - sum(components.values())
    previous_total = (previous_mark or {}).get("attribution") or {}
    cumulative = {
        key: float(previous_total.get(key) or 0.0) + value
        for key, value in components.items()
    }
    return {key: round(value, 4) for key, value in cumulative.items()}


def _mark_benchmarks(
    idea: Mapping[str, Any],
    quotes: Mapping[tuple[date, float, str], OptionQuote],
    *,
    spot: float,
) -> dict[str, float]:
    benchmarks = idea.get("benchmarks") or {}
    entry_spot = float(benchmarks.get("entry_spot") or idea.get("spot") or spot)
    underlying = float(benchmarks.get("delta_shares") or 0.0) * (spot - entry_spot)
    vertical = benchmarks.get("vertical")
    vertical_pnl = 0.0
    if isinstance(vertical, Mapping):
        current = 0.0
        complete = True
        for leg in vertical.get("legs") or ():
            quote = quotes.get((
                date.fromisoformat(str(leg["expiry"])),
                round(float(leg["strike"]), 4),
                str(leg["right"]),
            ))
            if quote is None or quote.mid is None:
                complete = False
                break
            current += int(leg["signed_quantity"]) * quote.mid * int(leg.get("multiplier") or 100)
        if complete:
            vertical_pnl = float(vertical.get("scale") or 1.0) * (
                current - float(vertical.get("entry_value") or 0.0)
            )
    return {
        "underlying": round(underlying, 2),
        "vertical": round(vertical_pnl, 2),
        "cash": 0.0,
    }


def _direction_hit(idea: Mapping[str, Any], spot: float) -> bool | None:
    entry_spot = float(idea.get("spot") or 0.0)
    direction = str(((idea.get("view") or {}).get("direction") or "neutral"))
    if entry_spot <= 0 or direction not in {"bullish", "bearish"}:
        return None
    move = spot - entry_spot
    return move > 0 if direction == "bullish" else move < 0


def _origin_quality(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    metrics = _metrics(rows)
    returns = [
        value for row in rows
        if (value := _number((row.get("exit") or {}).get("return_on_risk"))) is not None
    ]
    low, high = _clustered_ci(rows)
    chronological = sorted(
        rows,
        key=lambda row: str(
            (row.get("exit") or {}).get("at") or row.get("entered_at") or ""
        ),
    )
    pnl_values = [
        _number((row.get("exit") or {}).get("pnl")) or 0.0
        for row in chronological
    ]
    cumulative = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for pnl in pnl_values:
        cumulative += pnl
        peak = max(peak, cumulative)
        max_drawdown = max(max_drawdown, peak - cumulative)
    hits = [
        value for row in rows
        if isinstance((value := (row.get("exit") or {}).get("direction_hit")), bool)
    ]
    attribution = _mean_mapping(
        [(row.get("exit") or {}).get("attribution") or {} for row in rows]
    )
    benchmark_excess = _mean_mapping([
        {
            name: (_number((row.get("exit") or {}).get("pnl")) or 0.0) - float(value or 0.0)
            for name, value in ((row.get("exit") or {}).get("benchmarks") or {}).items()
        }
        for row in rows
    ])
    verdict = "未验证"
    if len(rows) >= 30:
        if low is not None and low > 0:
            verdict = "有效"
        elif high is not None and high < 0:
            verdict = "无效"
        else:
            verdict = "不确定"
    return {
        "count": len(rows),
        "win_rate": metrics["win_rate"],
        "mean_return_on_risk": _mean(returns),
        "ci95": [low, high],
        "max_drawdown": round(max_drawdown, 2),
        "worst_5pct": _percentile(returns, 0.05),
        "direction_hit_rate": sum(hits) / len(hits) if hits else None,
        "attribution": attribution,
        "benchmark_excess": benchmark_excess,
        "verdict": verdict,
        "verified": verdict == "有效",
    }


def _clustered_ci(rows: Sequence[Mapping[str, Any]]) -> tuple[float | None, float | None]:
    buckets = _bucket(rows, lambda row: str(row.get("entered_at") or "")[:10])
    if not buckets:
        return None, None
    keys = sorted(buckets)
    rng = random.Random(20261014)
    estimates = []
    for _ in range(1000):
        sample = [row for _key in keys for row in buckets[rng.choice(keys)]]
        values = [
            value for row in sample
            if (value := _number((row.get("exit") or {}).get("return_on_risk"))) is not None
        ]
        if values:
            estimates.append(sum(values) / len(values))
    if not estimates:
        return None, None
    estimates.sort()
    return (
        round(estimates[int(0.025 * (len(estimates) - 1))], 4),
        round(estimates[int(0.975 * (len(estimates) - 1))], 4),
    )


def _bucket(
    rows: Sequence[Mapping[str, Any]], key_fn: Any
) -> dict[str, list[Mapping[str, Any]]]:
    result: dict[str, list[Mapping[str, Any]]] = {}
    for row in rows:
        result.setdefault(key_fn(row), []).append(row)
    return result


def _mean_mapping(rows: Sequence[Mapping[str, Any]]) -> dict[str, float]:
    keys = sorted({key for row in rows for key in row})
    return {
        key: round(sum(float(row.get(key) or 0.0) for row in rows) / len(rows), 4)
        for key in keys
    } if rows else {}


def _percentile(values: Sequence[float], quantile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return round(ordered[int(quantile * (len(ordered) - 1))], 4)


def _edge_bucket(value: float | None) -> str:
    if value is None or value < 0.02:
        return "<2%"
    if value < 0.05:
        return "2–5%"
    if value < 0.10:
        return "5–10%"
    return "≥10%"


def _counts(values: Sequence[Any] | Any) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        if value:
            counts[str(value)] = counts.get(str(value), 0) + 1
    return counts


def _mean(values: Sequence[float]) -> float | None:
    return round(sum(values) / len(values), 4) if values else None


def _number(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    number = float(value)
    return number if math.isfinite(number) else None


__all__ = [
    "entry_record",
    "exit_decision",
    "leg_exit_price",
    "mark_idea",
    "settle_unmarked",
    "tracking_stats",
]
