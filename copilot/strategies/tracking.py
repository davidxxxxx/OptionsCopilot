"""Pure paper-tracking calculations for ranked option ideas."""
from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from typing import Any

from copilot.config import RulesSettings
from copilot.market.models import OptionQuote, valid_price
from copilot.strategies.portfolio import profit_exit


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
) -> dict[str, Any] | None:
    idea = entry["idea"]
    value = 0.0
    value_mid = 0.0
    expiries: list[date] = []
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
    multiplier = int(idea.get("multiplier") or 100)
    net_debit = float(idea["net_debit"])
    fees = float(idea.get("fees") or 0.0)
    pnl = (value - net_debit) * multiplier - fees
    pnl_mid = (value_mid - net_debit) * multiplier - fees
    max_loss = float(idea.get("max_loss") or 0.0)
    return {
        "marked_at": marked_at.isoformat(),
        "quote_time": quote_time.isoformat() if quote_time else None,
        "spot": round(spot, 2),
        "dte": (min(expiries) - today).days,
        "value": round(value, 2),
        "value_mid": round(value_mid, 2),
        "pnl": round(pnl, 2),
        "pnl_mid": round(pnl_mid, 2),
        "return_on_risk": round(pnl / max_loss, 4) if max_loss > 0 else None,
    }


def exit_decision(
    entry: Mapping[str, Any],
    mark: Mapping[str, Any],
    *,
    today: date,
    rules: RulesSettings,
) -> tuple[str, str] | None:
    idea = entry["idea"]
    multiplier = int(idea.get("multiplier") or 100)
    net_debit = float(idea["net_debit"])
    kind = "credit" if net_debit < 0 else "debit"
    decision = profit_exit(
        kind,
        net_debit * multiplier,
        float(mark["pnl"]),
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
        for key in ("spot", "value", "pnl", "pnl_mid", "return_on_risk"):
            payload[key] = last_mark.get(key)
    return payload


def tracking_stats(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    closed = [row for row in rows if row.get("status") == "closed"]
    opened = [row for row in rows if row.get("status") == "open"]
    settled = [row for row in closed if _number((row.get("exit") or {}).get("pnl")) is not None]
    core = _metrics(settled)
    result = {
        "closed": len(closed),
        "open": len(opened),
        "unsettled": len(closed) - len(settled),
        **core,
        "open_unrealized": round(sum(
            _number((row.get("mark") or {}).get("pnl")) or 0.0 for row in opened
        ), 2),
        "exit_reasons": _counts((row.get("exit") or {}).get("code") for row in closed),
        "min_sample": 30,
        "enough_sample": len(closed) >= 30,
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
