"""One end-to-end run: news -> market context -> views -> option analysis -> Top-N ideas.

Every stage degrades on its own: without IBKR the run still stores news and views
(status "partial"), without the LLM it falls back to neutral views. Nothing here
can place, modify or cancel an order.
"""
from __future__ import annotations

import json
import logging
import math
import threading
import time
from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any, Protocol
from zoneinfo import ZoneInfo

from copilot.analysis.models import View
from copilot.analysis.brief import needs_refresh, write_brief
from copilot.analysis.ladder import LadderSignal, ladder_signal, ladder_view
from copilot.analysis.ledger import score_news, update_ledger
from copilot.analysis.tagging import JsonModel, NewsTagger
from copilot.config import Settings
from copilot.market.calendar import is_regular_session, is_trading_day
from copilot.market.cboe import CboeChain
from copilot.market.history import iv_rank_and_percentile, realized_vol, trailing_return
from copilot.market.ibkr import IbkrUnavailable
from copilot.market.models import AccountSnapshot, ChainInfo, OptionChain, OptionQuote, Underlying
from copilot.market.state import market_state
from copilot.news.collect import NewsSources, collect_news
from copilot.params import params_payload, params_version
from copilot.store import Store
from copilot.strategies.builder import (
    MULTI_EXPIRY,
    Candidate,
    back_expiries,
    build_candidates,
    rebuild_candidate,
    select_expiries,
    strike_window,
    structures_for,
)
from copilot.strategies.portfolio import (
    PortfolioRisk,
    PositionGroup,
    exit_alerts,
    group_positions,
    mark_to_market,
    portfolio_risk,
)
from copilot.strategies.ranking import candidate_key, dropped_ideas, idea_key, loss_cap, rank_candidates
from copilot.strategies.tracking import entry_record, exit_decision, mark_idea, settle_unmarked

log = logging.getLogger(__name__)
NEW_YORK = ZoneInfo("America/New_York")


class MarketSource(Protocol):
    warnings: list[str]

    def connect(self) -> None: ...
    def close(self) -> None: ...
    def account(self) -> AccountSnapshot: ...
    def underlyings(self, symbols: Sequence[str]) -> dict[str, Underlying]: ...
    def daily_history(self, symbol: str, what: str) -> list[tuple[str, float]]: ...
    def chain_info(self, symbol: str) -> ChainInfo | None: ...
    def option_quotes(
        self, info: ChainInfo, expiries: Sequence[date], *, spot: float,
        strike_low: float, strike_high: float, max_strikes_per_side: int = 20,
        require_bbo: bool = True,
    ) -> list[OptionQuote]: ...
    def live_quotes(
        self, specs: Sequence[tuple[str, date, float, str]]
    ) -> dict[tuple[str, date, float, str], tuple[float, float]]: ...


class OptionChainSource(Protocol):
    def chain(self, symbol: str) -> CboeChain: ...


@dataclass
class PipelineDeps:
    settings: Settings
    store: Store
    news_sources: NewsSources | None
    llm: JsonModel | None
    market_factory: Callable[[], MarketSource] | None
    clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc)
    option_source: OptionChainSource | None = None
    # Daily closes for the index ladder: (symbol, today) -> [(date, close)], oldest first.
    index_history: Callable[[str, date], list[tuple[date, float]]] | None = None


def run_pipeline(deps: PipelineDeps, *, trigger: str, slot: str) -> int:
    settings, store = deps.settings, deps.store
    now = deps.clock()
    today = now.astimezone(NEW_YORK).date()
    run_id = store.create_run(trading_date=today, slot=slot, trigger=trigger)
    stages: dict[str, Any] = {}
    timings: dict[str, float] = {}
    market: MarketSource | None = None
    macro_summary = ""
    try:
        version = params_version(settings)
        payload = params_payload(settings)
        stages["params"] = {"version": version}
        store.save_param_set(version, payload)
        # 1. News and calendar ------------------------------------------------
        started = time.monotonic()
        upcoming = store.earnings_between(today, today + timedelta(days=settings.news.earnings_days_ahead))
        universe = list(settings.universe)
        company_symbols = _dedupe(
            [*settings.core_symbols, *(e.symbol for e in upcoming if e.symbol in set(universe)), *universe]
        )
        if deps.news_sources is not None:
            results = collect_news(
                settings, store, deps.news_sources, now=now, company_symbols=company_symbols,
            )
            stages["news"] = [asdict(result) for result in results]
        else:
            stages["news"] = []
        timings["news"] = time.monotonic() - started
        store.update_run_stages(run_id, stages)

        # 2. Broker: account and underlying prices ----------------------------
        started = time.monotonic()
        account: AccountSnapshot | None = None
        underlyings: dict[str, Underlying] = {}
        if deps.market_factory is None:
            stages["ibkr"] = {"status": "disabled"}
        else:
            try:
                market = deps.market_factory()
                market.connect()
                account = market.account()
                underlyings = market.underlyings(universe)
                stages["ibkr"] = {"status": "ok", "underlyings": len(underlyings)}
                _save_account(store, run_id, account)
            except IbkrUnavailable as exc:
                stages["ibkr"] = {"status": "unavailable", "detail": str(exc)}
                market = _close(market)
            except Exception as exc:
                log.exception("IBKR stage failed")
                stages["ibkr"] = {"status": "failed", "detail": f"{type(exc).__name__}: {exc}"[:300]}
                market = _close(market)
        timings["ibkr"] = time.monotonic() - started
        _attach_cached_history(store, underlyings, today)
        store.update_run_stages(run_id, stages)

        # 3. News tagging -> view ledger -> macro brief ----------------------------
        started = time.monotonic()
        earnings = store.earnings_between(today, today + timedelta(days=settings.news.earnings_days_ahead))
        llm = deps.llm if settings.llm.enabled else None
        lookback_start = now - timedelta(days=settings.views.lookback_days)
        stages["tagging"] = tag_pending_news(store, llm, settings, since=lookback_start)
        tagged = store.tagged_news(lookback_start)
        evidence = score_news(tagged, now=now, settings=settings.views)
        ledger = update_ledger(
            store.view_book(), evidence, earnings=earnings,
            core=settings.core_symbols, now=now, settings=settings.views,
            max_drift_sigma=settings.strategy.max_drift_sigma,
        )
        store.replace_view_book(ledger.book, ledger.changes, run_id=run_id)
        views = ledger.views
        store.save_views(run_id, (ledger.book.get(s) or view.to_dict() for s, view in views.items()))
        macro_summary, stages["brief"] = _refresh_brief(store, llm, tagged, now)
        stages["views"] = {
            "status": "ledger",
            "count": len(views),
            "open": len(ledger.book),
            "tagged_news": len(tagged),
            "changes": ledger.changes[:40],
        }
        timings["views"] = time.monotonic() - started
        store.update_run_stages(run_id, stages)

        # 3b. Rules: index drawdown ladder and the open-position book ------------
        ladder_signals = _ladder_signals(store, deps, settings, today)
        rule_views = {
            view.symbol: view for signal in ladder_signals if (view := ladder_view(signal, settings.rules))
        }
        stages["rules"] = {"ladder": [signal.to_dict() for signal in ladder_signals]}
        groups = group_positions(account.positions) if account else []

        # 4. Option analysis ---------------------------------------------------
        started = time.monotonic()
        ideas: list[dict[str, Any]] = []
        chains = _ChainCache(deps.option_source) if deps.option_source is not None else None
        event_dates: dict[str, list[date]] = {}
        for event in earnings:
            event_dates.setdefault(event.symbol, []).append(event.report_date)
        _mark_groups(groups, chains)
        risk = portfolio_risk(groups, account.net_liquidation if account else None, settings.rules) if account else None
        if market is None and chains is None:
            stages["options"] = {"status": "skipped", "detail": "IBKR 不可用且未启用 CBOE，未分析期权"}
        else:
            previous = store.previous_ideas(run_id, now - timedelta(hours=36))
            symbols = _analysis_symbols(
                views, underlyings, settings, need_price=chains is None,
                keep=[idea["symbol"] for idea in previous],
            )
            live_session = is_regular_session(deps.clock())
            history = _History(market, store, today, settings.ibkr.history_timeout_seconds)
            previous_by_symbol: dict[str, list[dict[str, Any]]] = {}
            for idea in previous:
                previous_by_symbol.setdefault(idea["symbol"], []).append(idea)
            candidates: list[Candidate] = []
            failures: dict[str, str] = {}
            used: dict[str, str] = {}
            ibkr_ok = market is not None
            ibkr_analyses = 0
            consecutive_timeouts = 0
            pause = getattr(market, "pause", None)
            for symbol in symbols:
                view = views[symbol]
                events = event_dates.get(symbol, ())
                found: list[Candidate] | None = None
                reason = ""
                if chains is not None:
                    try:
                        found, underlyings[symbol] = _analyze_chain(
                            chains, symbol, view, settings, history, today=today, event_dates=events,
                            previous=[i for i in previous_by_symbol.get(symbol, ()) if i.get("origin") != "ladder"],
                        )
                        used[symbol] = "cboe"
                    except _Skip as exc:
                        failures[symbol] = str(exc)
                        continue
                    except Exception as exc:  # CBOE down or changed: try IBKR for this symbol
                        log.warning("CBOE chain for %s failed: %s", symbol, exc)
                        reason = str(exc)[:160]
                underlying = underlyings.get(symbol)
                if found is None and ibkr_ok and underlying is not None and underlying.price:
                    if pause is not None and ibkr_analyses:
                        pause(settings.ibkr.symbol_pause_seconds)
                    ibkr_analyses += 1
                    try:
                        underlyings[symbol] = history.enrich(underlying)
                        found = _analyze_symbol(
                            market, underlyings[symbol], view, settings,
                            today=today, event_dates=events, live_session=live_session,
                        )
                        used[symbol] = "ibkr"
                        consecutive_timeouts = 0
                    except _Skip as exc:
                        reason = str(exc)
                    except IbkrUnavailable as exc:
                        reason = f"IBKR 连接中断: {exc}"[:160]
                        ibkr_ok = False
                    except TimeoutError:
                        reason = "IBKR 请求超时"
                        consecutive_timeouts += 1
                        if consecutive_timeouts >= 2:
                            ibkr_ok = False
                    except Exception as exc:
                        log.warning("analysis of %s failed: %s", symbol, exc)
                        reason = f"{type(exc).__name__}: {exc}"[:160]
                if found is None:
                    failures[symbol] = reason or "没有期权数据"
                elif not found:
                    failures[symbol] = "没有满足流动性/正期望的组合"
                else:
                    candidates.extend(found)
            # Rule views (index ladder) are analysed on their own, labelled by origin.
            for symbol, rule_view in rule_views.items() if chains is not None else ():
                try:
                    found, _underlying = _analyze_chain(
                        chains, symbol, rule_view, settings, history, today=today,
                        event_dates=event_dates.get(symbol, ()),
                        previous=[i for i in previous_by_symbol.get(symbol, ()) if i.get("origin") == "ladder"],
                    )
                    candidates.extend(found)
                    used.setdefault(symbol, "cboe")
                except Exception as exc:
                    failures[f"{symbol}（阶梯规则）"] = str(exc)[:160]
            ideas = rank_candidates(
                candidates, views=views, settings=settings.strategy, account=account, previous=previous,
                rule_views=rule_views, portfolio=risk,
            )
            dropped = dropped_ideas(
                previous, ideas, candidates, settings=settings.strategy, account=account, analyzed=set(used),
            )
            live_check: dict[str, Any] = {"status": "skipped"}
            if (
                ideas and live_session and ibkr_ok and market is not None
                and settings.options.live_refresh
                and any(idea["delayed_data"] for idea in ideas)
            ):
                try:
                    live_check = _refresh_live_quotes(market, ideas)
                except Exception as exc:
                    log.warning("live re-quote failed: %s", exc)
                    live_check = {"status": "failed", "detail": f"{type(exc).__name__}: {exc}"[:200]}
            store.save_ideas(run_id, ideas)
            states: dict[str, dict[str, Any]] = {}
            try:
                if not is_trading_day(today):
                    stages["market_state"] = {"status": "skipped", "detail": "非交易日，不留存"}
                else:
                    for symbol, chain in chains.fetched().items() if chains is not None else ():
                        underlying = underlyings.get(symbol)
                        state = market_state(
                            chain.quotes,
                            spot=chain.underlying.price or 0.0,
                            today=today,
                            as_of=chain.as_of,
                            hv20=underlying.hv20 if underlying else None,
                            vendor_iv30=chain.underlying.iv30,
                        )
                        if state is not None:
                            store.save_market_state(symbol, today, state)
                            states[symbol] = state
                    stages["market_state"] = {"status": "ok", "symbols": len(states)}
            except Exception as exc:
                log.exception("market-state stage failed")
                stages["market_state"] = {
                    "status": "failed",
                    "symbols": len(states),
                    "detail": f"{type(exc).__name__}: {exc}"[:300],
                }
            try:
                stages["tracking"] = _track(
                    store,
                    run_id=run_id,
                    ideas=ideas,
                    chains=chains,
                    today=today,
                    now=now,
                    slot=slot,
                    trigger=trigger,
                    pricing="live" if live_session else "reference",
                    params_version=version,
                    market_states=states,
                    settings=settings,
                )
            except Exception as exc:
                log.exception("tracking stage failed")
                stages["tracking"] = {
                    "status": "failed",
                    "detail": f"{type(exc).__name__}: {exc}"[:300],
                }
            cap = loss_cap(account, settings.strategy)
            over_cap = sum(1 for c in candidates if c.edge > 0 and cap is not None and c.profile.max_loss > cap)
            # Partial only when the broker dropped and that actually cost us symbols.
            connection_lost = market is not None and not ibkr_ok
            stages["options"] = {
                "status": "partial" if connection_lost and (
                    chains is None or any("IBKR" in reason for reason in failures.values())
                ) else "ok",
                "pricing": "live" if live_session else "reference",
                "sources": {name: list(used.values()).count(name) for name in ("cboe", "ibkr")},
                "symbols": symbols,
                "candidates": len(candidates),
                "ideas": len(ideas),
                "loss_cap": round(cap, 2) if cap is not None else None,
                "over_cap": over_cap,
                "live_check": live_check,
                "previous_ideas": len(previous),
                "dropped": dropped,
                "history": "unavailable" if history.disabled else "ok",
                "missing_contracts": getattr(market, "missing_contracts", 0),
                "skipped": failures,
                "warnings": market.warnings[:30] if market is not None else [],
            }
        for symbol, underlying in underlyings.items():
            store.save_snapshot(run_id, "underlying", symbol, _underlying_dict(underlying))
        if account is not None:
            for group in groups:
                group.alerts = exit_alerts(
                    group, today=today, rules=settings.rules, earnings=event_dates.get(group.symbol, ()),
                    view=views.get(group.symbol), net_liquidation=account.net_liquidation,
                )
            store.save_snapshot(run_id, "positions", "book", {
                "risk": risk.to_dict() if risk else None,
                "groups": [group.to_dict(today) for group in groups],
            })
            stages["positions"] = {
                "groups": len(groups),
                "action_alerts": sum(1 for g in groups for a in g.alerts if a["level"] == "action"),
                "warn_alerts": sum(1 for g in groups for a in g.alerts if a["level"] == "warn"),
                "risk": risk.to_dict() if risk else None,
            }
        timings["options"] = time.monotonic() - started

        stages["timings"] = {key: round(value, 1) for key, value in timings.items()}
        degraded = (
            stages["ibkr"].get("status") != "ok"
            or stages["options"].get("status") != "ok"
            or stages["tagging"].get("status") == "failed"
        )
        store.finish_run(
            run_id, status="partial" if degraded else "ok", stages=stages,
            macro_summary=macro_summary,
        )
    except Exception as exc:
        log.exception("run %s failed", run_id)
        stages["timings"] = {key: round(value, 1) for key, value in timings.items()}
        store.finish_run(
            run_id, status="failed", stages=stages,
            error=f"{type(exc).__name__}: {exc}"[:500], macro_summary=macro_summary,
        )
    finally:
        _close(market)
    return run_id


_TAG_LOCK = threading.Lock()


def tag_pending_news(store: Store, llm: JsonModel | None, settings: Settings, *, since: datetime) -> dict[str, Any]:
    """Classify not-yet-tagged news (newest first). Shared by runs and the news poll."""

    if llm is None:
        return {"status": "skipped", "detail": "LLM 未启用或未配置 key", "remaining": store.count_untagged(since)}
    with _TAG_LOCK:  # the scheduler's news poll and a GUI run must not tag the same items twice
        pending = store.untagged_news(since, limit=settings.llm.max_tags_per_run)
        if not pending:
            return {"status": "ok", "tagged": 0, "failed": 0, "remaining": 0}
        tagger = NewsTagger(
            llm, settings.universe,
            batch_size=settings.llm.tag_batch_size, workers=settings.llm.tag_workers,
        )
        result = tagger.tag(pending)
        store.save_news_tags(result.tags, model=str(getattr(llm, "model", "")))
        if result.failed_ids:
            store.record_tag_failures(result.failed_ids, "; ".join(result.errors))
    return {
        "status": "ok" if result.tags else "failed",
        "tagged": len(result.tags),
        "failed": len(result.failed_ids),
        "remaining": store.count_untagged(since),
        "usage": result.usage(),
        "errors": result.errors[:3],
    }


def _refresh_brief(
    store: Store, llm: JsonModel | None, tagged: Sequence[Any], now: datetime
) -> tuple[str, dict[str, Any]]:
    state = store.get_kv("macro_brief")
    important = [
        (item, tag) for item, tag in tagged
        if tag.importance >= 2 and now - item.published_at <= timedelta(hours=36)
    ]
    if llm is not None and needs_refresh(state, important):
        try:
            state, usage = write_brief(llm, important, now=now)
            store.set_kv("macro_brief", state)
            return state["text"], {"status": "updated", "items": len(important), "usage": usage}
        except Exception as exc:
            log.warning("macro brief failed: %s", exc)
            return (state or {}).get("text", ""), {"status": "failed", "detail": str(exc)[:200]}
    return (state or {}).get("text", ""), {"status": "reused" if state else "none"}


def _analysis_symbols(
    views: dict[str, View], underlyings: dict[str, Underlying], settings: Settings,
    *, need_price: bool = True, keep: Sequence[str] = (),
) -> list[str]:
    def priority(view: View) -> tuple[int, float, int]:
        active = view.direction != "neutral" or view.volatility != "neutral"
        return (0 if active else 1, -abs(view.score) - abs(view.vol_score), -view.conviction)

    ordered = [view.symbol for view in sorted(views.values(), key=priority)]
    # Core first, then symbols on the previous list (so those ideas can be re-priced and
    # kept) while their view is still open, then the strongest remaining views.
    held = [s for s in keep if s in views and views[s].direction != "neutral"]
    ordered = _dedupe([*(s for s in ordered if s in settings.core_symbols), *held, *ordered])
    if need_price:  # IBKR-only analysis needs the broker's underlying price
        ordered = [symbol for symbol in ordered if (u := underlyings.get(symbol)) and u.price]
    return ordered[: settings.strategy.max_symbols_per_run]


class _Skip(Exception):
    """Nothing to analyse for this symbol (no expiry in range, ...); no fallback helps."""


def _analyze_chain(
    chains: OptionChainSource,
    symbol: str,
    view: View,
    settings: Settings,
    history: "_History",
    *,
    today: date,
    event_dates: Sequence[date],
    previous: Sequence[dict[str, Any]] = (),
) -> tuple[list[Candidate], Underlying]:
    chain = chains.chain(symbol)
    underlying = history.enrich(chain.underlying, keep_iv30=True)
    expiries = select_expiries(sorted({q.expiry for q in chain.quotes}), today, view, settings.strategy)
    if not expiries:
        raise _Skip("DTE 窗口内没有到期日")
    sigma = underlying.iv30 or underlying.hv20 or 0.35
    t_years = max((expiries[-1] - today).days, 1) / 365
    low, high = strike_window(underlying.price, sigma, t_years, view)
    wanted = set(expiries)
    if any(name in MULTI_EXPIRY for name in structures_for(view)):
        all_expiries = sorted({q.expiry for q in chain.quotes})
        for front in expiries:
            wanted.update(back_expiries(all_expiries, front))
    # Keep the legs of last time's ideas in the chain so they can be re-scored as-is.
    kept_legs = {
        (date.fromisoformat(leg["expiry"]), float(leg["strike"]), leg["right"])
        for idea in previous for leg in idea["legs"]
    }
    quotes = tuple(
        q for q in chain.quotes
        if (q.expiry in wanted and low <= q.strike <= high) or (q.expiry, q.strike, q.right) in kept_legs
    )
    option_chain = OptionChain(underlying=underlying, quotes=quotes)
    # CBOE carries real bid/ask even when the market is closed (the closing snapshot).
    found = build_candidates(
        view, option_chain, settings.strategy, today=today, event_dates=event_dates, expiries=expiries,
    )
    keys = {candidate_key(candidate) for candidate in found}
    for idea in previous:
        rebuilt = rebuild_candidate(
            view, option_chain, settings.strategy, today=today, structure=idea["structure"],
            legs=[
                (date.fromisoformat(leg["expiry"]), float(leg["strike"]), leg["right"],
                 int(leg.get("signed_quantity") or (leg["quantity"] if leg.get("action") == "买入" else -leg["quantity"])))
                for leg in idea["legs"]
            ],
            event_dates=event_dates,
        )
        if rebuilt is not None and candidate_key(rebuilt) not in keys:
            found.append(rebuilt)
            keys.add(candidate_key(rebuilt))
    return found, underlying


class _ChainCache:
    """One CBOE request per symbol per run, shared by views, ladder rules and positions."""

    def __init__(self, source: OptionChainSource) -> None:
        self.source = source
        self._cache: dict[str, CboeChain | Exception] = {}

    def chain(self, symbol: str) -> CboeChain:
        if symbol not in self._cache:
            try:
                self._cache[symbol] = self.source.chain(symbol)
            except Exception as exc:
                self._cache[symbol] = exc
        cached = self._cache[symbol]
        if isinstance(cached, Exception):
            raise cached
        return cached

    def fetched(self) -> dict[str, CboeChain]:
        return {
            symbol: cached for symbol, cached in self._cache.items()
            if isinstance(cached, CboeChain)
        }


def _track(
    store: Store,
    *,
    run_id: int,
    ideas: Sequence[dict[str, Any]],
    chains: _ChainCache | None,
    today: date,
    now: datetime,
    slot: str,
    trigger: str,
    pricing: str,
    params_version: str,
    market_states: dict[str, dict[str, Any]],
    settings: Settings,
) -> dict[str, Any]:
    opened = listed_again = marked = closed = skipped_indicative = 0
    skipped: dict[str, int] = {}

    def skip(reason: str) -> None:
        skipped[reason] = skipped.get(reason, 0) + 1

    for row in store.open_tracked():
        entry = row["entry"]
        symbol = row["symbol"]
        if chains is None:
            skip("CBOE 未启用，无法估值")
            continue
        chain_failed = False
        try:
            chain = chains.chain(symbol)
            quote_map = {
                (quote.expiry, round(quote.strike, 4), quote.right): quote
                for quote in chain.quotes
            }
            mark = mark_idea(
                entry,
                quote_map,
                spot=chain.underlying.price or 0.0,
                today=today,
                fill_ratio=settings.strategy.fill_ratio,
                marked_at=now,
                quote_time=chain.as_of,
            )
        except Exception as exc:
            mark = None
            chain_failed = True
            skip(f"{symbol}: {type(exc).__name__}")
        if mark is None:
            settled = settle_unmarked(entry, row.get("mark"), today=today)
            if settled is None:
                if not chain_failed:
                    skip(f"{symbol}: 缺少可估值报价")
                continue
            exit_payload = _tracking_exit_payload(
                row, settled, run_id=run_id, now=now, entered_at=row["entered_at"]
            )
            store.close_tracked(row["id"], exit_payload)
            closed += 1
            continue
        store.add_tracked_mark(row["id"], run_id, mark["marked_at"], mark)
        marked += 1
        decision = exit_decision(entry, mark, today=today, rules=settings.rules)
        if decision is not None:
            code, text = decision
            exit_payload = _tracking_exit_payload(
                row,
                {**mark, "code": code, "text": text, "approximate": False},
                run_id=run_id,
                now=now,
                entered_at=row["entered_at"],
            )
            store.close_tracked(row["id"], exit_payload)
            closed += 1

    for idea in ideas:
        if idea.get("indicative"):
            skipped_indicative += 1
            continue
        key = json.dumps(idea_key(idea), ensure_ascii=False, separators=(",", ":"))
        existing = store.find_open_tracked(key)
        if existing is not None:
            store.touch_tracked(existing["id"], run_id)
            listed_again += 1
            continue
        store.add_tracked(
            idea_key=key,
            symbol=idea["symbol"],
            structure=idea["structure"],
            origin=idea.get("origin") or "news",
            params_version=params_version,
            entry_run_id=run_id,
            entered_at=now.isoformat(),
            entry=entry_record(
                idea,
                run_id=run_id,
                slot=slot,
                trigger=trigger,
                pricing=pricing,
                params_version=params_version,
                market_state=market_states.get(idea["symbol"]),
            ),
        )
        opened += 1
    return {
        "status": "ok" if chains is not None else "skipped",
        "opened": opened,
        "listed_again": listed_again,
        "marked": marked,
        "closed": closed,
        "open": len(store.open_tracked()),
        "skipped": skipped,
        "skipped_indicative": skipped_indicative,
    }


def _tracking_exit_payload(
    row: dict[str, Any],
    payload: dict[str, Any],
    *,
    run_id: int,
    now: datetime,
    entered_at: str,
) -> dict[str, Any]:
    entered = datetime.fromisoformat(entered_at)
    if entered.tzinfo is None:
        entered = entered.replace(tzinfo=timezone.utc)
    return {
        "code": payload["code"],
        "text": payload["text"],
        "run_id": run_id,
        "at": now.isoformat(),
        "spot": payload.get("spot"),
        "value": payload.get("value"),
        "pnl": payload.get("pnl"),
        "pnl_mid": payload.get("pnl_mid"),
        "return_on_risk": payload.get("return_on_risk"),
        "days_held": max(0, (now.date() - entered.date()).days),
        "approximate": bool(payload.get("approximate")),
    }


def _ladder_signals(store: Store, deps: PipelineDeps, settings: Settings, today: date) -> list[LadderSignal]:
    signals = []
    for symbol in settings.rules.ladder_symbols:
        closes = store.get_daily_cache(symbol, "NASDAQ_CLOSE", today)
        if closes is None and deps.index_history is not None:
            try:
                fetched = deps.index_history(symbol, today)
            except Exception as exc:
                log.warning("index history for %s failed: %s", symbol, exc)
                fetched = []
            closes = [[day.isoformat(), close] for day, close in fetched]
            if closes:
                store.put_daily_cache(symbol, "NASDAQ_CLOSE", today, closes)
        parsed = [(date.fromisoformat(day), float(close)) for day, close in closes or ()]
        signal = ladder_signal(symbol, parsed, settings.rules)
        if signal is not None:
            signals.append(signal)
    return signals


def _mark_groups(groups: Sequence[PositionGroup], chains: "_ChainCache | None") -> None:
    """Mark open positions with CBOE quotes (also gives their deltas, hence direction)."""

    for group in groups:
        if chains is None:
            continue
        try:
            chain = chains.chain(group.symbol)
        except Exception as exc:
            log.warning("could not mark %s positions: %s", group.symbol, exc)
            continue
        mark_to_market(group, {(q.expiry, q.strike, q.right): q for q in chain.quotes})


def _refresh_live_quotes(market: MarketSource, ideas: list[dict[str, Any]]) -> dict[str, Any]:
    """Attach IBKR real-time bid/ask to every leg of the delayed-data ideas."""

    def key(idea: dict[str, Any], leg: dict[str, Any]) -> tuple[str, date, float, str]:
        return idea["symbol"], date.fromisoformat(leg["expiry"]), float(leg["strike"]), leg["right"]

    specs = sorted({key(idea, leg) for idea in ideas if idea["delayed_data"] for leg in idea["legs"]})
    quotes = market.live_quotes(specs)
    checked = 0
    for idea in ideas:
        net, complete = 0.0, True
        for leg in idea["legs"]:
            live = quotes.get(key(idea, leg))
            if live is None:
                complete = False
                continue
            leg["live_bid"], leg["live_ask"] = live
            net += leg["signed_quantity"] * (live[0] + live[1]) / 2
        if complete:
            idea["live_net_premium"] = round(net * idea.get("multiplier", 100), 2)
            idea["live_checked_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
            checked += 1
    return {"status": "ok", "legs": len(specs), "quoted": len(quotes), "ideas_checked": checked}


def _analyze_symbol(
    market: MarketSource,
    underlying: Underlying,
    view: View,
    settings: Settings,
    *,
    today: date,
    event_dates: Sequence[date],
    live_session: bool,
) -> list[Candidate]:
    info = market.chain_info(underlying.symbol)
    if info is None:
        raise _Skip("IBKR 没有该标的的期权链")
    expiries = select_expiries(info.expiries, today, view, settings.strategy)
    if not expiries:
        raise _Skip("DTE 窗口内没有到期日")
    sigma = underlying.iv30 or underlying.hv20 or 0.35
    t_years = max((expiries[-1] - today).days, 1) / 365
    low, high = strike_window(underlying.price, sigma, t_years, view)
    quotes = market.option_quotes(
        info, expiries, spot=underlying.price, strike_low=low, strike_high=high,
        require_bbo=live_session,
    )
    chain = OptionChain(underlying=underlying, quotes=tuple(quotes))
    return build_candidates(
        view, chain, settings.strategy, today=today, event_dates=event_dates,
        indicative_prices=not live_session,
    )


class _History:
    """Daily bars from the per-day cache or IBKR.

    History only refines vol estimates, so after one IBKR history timeout (its historical
    farm can be down for hours) the rest of the run uses the cache only.
    """

    def __init__(
        self, market: MarketSource | None, store: Store, today: date, timeout_seconds: float
    ) -> None:
        self.market = market
        self.store = store
        self.today = today
        self.timeout_seconds = timeout_seconds
        self.disabled = market is None

    def enrich(self, underlying: Underlying, *, keep_iv30: bool = False) -> Underlying:
        values: dict[str, list[float]] = {}
        for what in ("TRADES", "OPTION_IMPLIED_VOLATILITY"):
            bars = self.store.get_daily_cache(underlying.symbol, what, self.today)
            if bars is None and not self.disabled:
                started = time.monotonic()
                try:
                    bars = self.market.daily_history(underlying.symbol, what)
                except (TimeoutError, IbkrUnavailable):
                    bars = None
                if bars:
                    self.store.put_daily_cache(underlying.symbol, what, self.today, bars)
                elif time.monotonic() - started >= self.timeout_seconds - 0.5:
                    log.warning("IBKR history timed out (%s %s); skipping history for this run",
                                underlying.symbol, what)
                    self.disabled = True
            values[what] = [close for _, close in bars or ()]
        enriched = _with_history(underlying, values)
        if keep_iv30 and underlying.iv30:
            enriched = enriched.with_history(iv30=underlying.iv30)
        return enriched


def _attach_cached_history(store: Store, underlyings: dict[str, Underlying], today: date) -> None:
    for symbol, underlying in list(underlyings.items()):
        values = {
            what: [close for _, close in (store.get_daily_cache(symbol, what, today) or ())]
            for what in ("TRADES", "OPTION_IMPLIED_VOLATILITY")
        }
        if any(values.values()):
            underlyings[symbol] = _with_history(underlying, values)


def _with_history(underlying: Underlying, values: dict[str, list[float]]) -> Underlying:
    closes = values.get("TRADES") or []
    iv_now, iv_rank, iv_percentile = iv_rank_and_percentile(values.get("OPTION_IMPLIED_VOLATILITY") or [])
    return underlying.with_history(
        iv30=iv_now if iv_now and math.isfinite(iv_now) else underlying.iv30,
        iv_rank=iv_rank,
        iv_percentile=iv_percentile,
        hv20=realized_vol(closes, 20),
        ret_5d=trailing_return(closes, 5),
        ret_20d=trailing_return(closes, 20),
    )


def _save_account(store: Store, run_id: int, account: AccountSnapshot) -> None:
    store.save_snapshot(run_id, "account", "summary", {
        "observed_at": account.observed_at.isoformat(),
        "net_liquidation": account.net_liquidation,
        "currency": account.currency,
        "positions": [
            {
                "label": position.label,
                "symbol": position.symbol,
                "sec_type": position.sec_type,
                "quantity": position.quantity,
                "avg_cost": position.avg_cost,
                "expiry": position.expiry.isoformat() if position.expiry else None,
                "strike": position.strike,
                "right": position.right,
                "multiplier": position.multiplier,
            }
            for position in account.positions
        ],
    })


def _underlying_dict(underlying: Underlying) -> dict[str, Any]:
    payload = asdict(underlying)
    payload["quote_time"] = underlying.quote_time.isoformat() if underlying.quote_time else None
    return payload


def _close(market: MarketSource | None) -> None:
    if market is not None:
        try:
            market.close()
        except Exception:
            log.debug("closing market source failed", exc_info=True)
    return None


def _dedupe(values: Sequence[str]) -> list[str]:
    return list(dict.fromkeys(values))


__all__ = ["MarketSource", "OptionChainSource", "PipelineDeps", "run_pipeline", "tag_pending_news"]
