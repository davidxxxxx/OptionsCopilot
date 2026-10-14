"""Local GUI/API. Loopback only: there is no authentication layer."""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from fastapi import FastAPI, HTTPException, Path as ApiPath, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from copilot.analysis.ledger import ACTION_NAMES
from copilot.analysis.news_calibration import current_calibration
from copilot.analysis.tagging import CATEGORIES
from copilot.market.state import with_own_rank
from copilot.news.macro_calendar import CATEGORY_NAMES, MAJOR_CATEGORIES, MacroEvent, group_major
from copilot.runtime import Runtime
from copilot.strategies.tracking import tracking_stats

STATIC_DIR = Path(__file__).resolve().parent / "static"
NEW_YORK = ZoneInfo("America/New_York")
BEIJING = ZoneInfo("Asia/Shanghai")


def create_app(runtime: Runtime) -> FastAPI:
    app = FastAPI(title="OptionsCopilot", docs_url=None, redoc_url=None)
    store = runtime.store
    settings = runtime.settings

    @app.get("/api/status")
    def status() -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        runs = store.list_runs(limit=1)
        latest = runs[0] if runs else None
        latest_event = store.latest_run_by_trigger("event")
        latest_event_detail = store.get_run(latest_event["id"]) if latest_event else None
        return {
            "now": now.isoformat(timespec="seconds"),
            "new_york": now.astimezone(NEW_YORK).strftime("%Y-%m-%d %H:%M"),
            "beijing": now.astimezone(BEIJING).strftime("%Y-%m-%d %H:%M"),
            "schedule": runtime.scheduler.status() if runtime.scheduler else {"enabled": False},
            "running": runtime.runs.current,
            "latest_run": _run_summary(latest) if latest else None,
            "latest_event_run": (
                {
                    "id": latest_event["id"],
                    "reason": (latest_event.get("stages") or {}).get("event"),
                    "status": latest_event["status"],
                    "finished_at": latest_event["finished_at"],
                    "main_new_ideas": sum(
                        idea.get("status") != "kept"
                        for idea in (latest_event_detail or {}).get("ideas", ())
                    ),
                    "event_ideas": len(
                        (((latest_event_detail or {}).get("snapshots") or {}).get("event_ideas") or {}).get("list", ())
                    ),
                }
                if latest_event
                else None
            ),
            "secrets": runtime.secrets.configured(),
            "ibkr": {"host": settings.ibkr.host, "port": settings.ibkr.port},
            "llm": {"enabled": settings.llm.enabled, "model": settings.llm.model},
            "news_24h": store.news_counts(now - timedelta(hours=24)),
        }

    @app.get("/api/runs")
    def runs(limit: int = Query(30, ge=1, le=200)) -> list[dict[str, Any]]:
        return [_run_summary(run) for run in store.list_runs(limit)]

    @app.get("/api/runs/latest")
    def latest_run() -> dict[str, Any]:
        run_id = store.latest_run_id()
        if run_id is None:
            raise HTTPException(404, "还没有完成的运行")
        return _run_detail(runtime, run_id)

    @app.get("/api/runs/{run_id}")
    def run_detail(run_id: int) -> dict[str, Any]:
        return _run_detail(runtime, run_id)

    @app.post("/api/runs", status_code=202)
    def trigger_run() -> dict[str, Any]:
        if not runtime.runs.start_background("manual", "manual"):
            raise HTTPException(409, "已有运行进行中")
        return {"started": True}

    @app.get("/api/news")
    def news(
        hours: int = Query(24, ge=1, le=168),
        limit: int = Query(200, ge=1, le=1000),
        source: str | None = None,
        min_importance: int = Query(0, ge=0, le=3),
        category: str | None = None,
        symbol: str | None = Query(None, pattern=r"^[A-Za-z]{1,5}$"),
    ) -> list[dict[str, Any]]:
        since = datetime.now(timezone.utc) - timedelta(hours=hours)
        feed = store.news_feed(
            since, limit=limit, source=source, min_importance=min_importance,
            category=category or None, symbol=symbol.upper() if symbol else None,
        )
        return [{**_news_dict(row["item"]), "tag": row["tag"]} for row in feed]

    @app.get("/api/views")
    def views() -> dict[str, Any]:
        book = sorted(
            store.view_book().values(),
            key=lambda v: abs(v.get("score") or 0) + abs(v.get("vol_score") or 0),
            reverse=True,
        )
        return {"book": book, "changes": store.view_changes(limit=120)}

    @app.get("/api/meta")
    def meta() -> dict[str, Any]:
        return {"categories": CATEGORIES, "actions": ACTION_NAMES}

    @app.get("/api/earnings")
    def earnings(days: int = Query(10, ge=1, le=30)) -> list[dict[str, Any]]:
        today = datetime.now(NEW_YORK).date()
        universe = set(settings.universe)
        return [
            {"symbol": e.symbol, "date": e.report_date.isoformat(), "hour": e.hour,
             "eps_estimate": e.eps_estimate, "in_universe": e.symbol in universe}
            for e in store.earnings_between(today, today + timedelta(days=days))
            if e.symbol in universe
        ]

    @app.get("/api/macro-events")
    def macro_events(
        days: int = Query(14, ge=1, le=120),
        min_importance: int = Query(0, ge=0, le=3),
    ) -> list[dict[str, Any]]:
        today = datetime.now(NEW_YORK).date()
        start, end = _date_window(today, days)
        rows = store.macro_events_between(start, end, min_importance=min_importance)
        alternate_fomc_dates = {
            event.event_time.astimezone(NEW_YORK).date()
            for event in rows
            if event.source != "fed" and event.category == "fomc_decision"
        }
        ratios = {
            (row["category"], row["symbol"]): row.get("abs_move_ratio")
            for row in store.event_stats()
            if row.get("symbol") in {"SPY", "QQQ", "TLT"}
        }
        return [
            {
                **_macro_event_dict(event),
                "abs_move_ratios": {
                    symbol: ratios.get((event.category, symbol))
                    for symbol in ("SPY", "QQQ", "TLT")
                },
            }
            for event in rows
            if not (
                event.source == "fed"
                and event.category == "fomc_decision"
                and event.event_time.astimezone(NEW_YORK).date() in alternate_fomc_dates
            )
        ]

    @app.get("/api/event-study")
    def event_study(category: str | None = None) -> list[dict[str, Any]]:
        return store.event_stats(category or None)

    @app.get("/api/news-calibration")
    def news_calibration() -> dict[str, Any]:
        payload = current_calibration(
            store.get_kv("news_calibration"), settings.news_calibration
        )
        return {
            **payload,
            "enabled": settings.news_calibration.enabled,
            "min_days_verified": settings.news_calibration.min_days_verified,
            "max_sd_verified": settings.news_calibration.max_sd_verified,
        }

    @app.get("/api/event-reactions")
    def event_reactions(
        category: str | None = None,
        symbol: str | None = None,
        limit: int = Query(100, ge=1, le=1000),
    ) -> list[dict[str, Any]]:
        return store.event_reactions(
            category=category or None,
            symbol=symbol.upper() if symbol else None,
            limit=limit,
        )

    @app.get("/api/event-study/latest")
    def latest_event_study() -> dict[str, Any]:
        payload = store.latest_snapshot("event_study", "latest")
        if payload is None:
            raise HTTPException(404, "还没有可用的事件测算")
        return payload

    @app.get("/api/macro-events/major")
    def major_macro_events(days: int = Query(7, ge=1, le=30)) -> list[dict[str, Any]]:
        now = datetime.now(NEW_YORK)
        today = now.date()
        start, end = _date_window(today, days)
        return group_major(
            [event for event in store.macro_events_between(start, end) if event.event_time > now],
            start=today,
            end=today + timedelta(days=days),
        )

    @app.get("/api/tracking")
    def tracking(
        status: str = Query("all", pattern=r"^(all|open|closed)$"),
        limit: int = Query(300, ge=1, le=1000),
    ) -> dict[str, Any]:
        all_rows = store.tracked_ideas(limit=None)
        rows = all_rows if status == "all" else [row for row in all_rows if row["status"] == status]
        return {
            "stats": tracking_stats(all_rows),
            "ideas": [_tracking_summary(row) for row in rows[:limit]],
        }

    @app.get("/api/tracking/{tracked_id}")
    def tracking_detail(tracked_id: int) -> dict[str, Any]:
        row = store.tracked_idea(tracked_id)
        if row is None:
            raise HTTPException(404, "跟踪记录不存在")
        return row

    @app.get("/api/market-state")
    def market_states() -> dict[str, Any]:
        latest = []
        for row in store.latest_market_states():
            history = store.market_state_history(row["symbol"], 252)
            latest.append(with_own_rank(row, history))
        return {"latest": latest}

    @app.get("/api/market-state/{symbol}")
    def market_state_history(
        symbol: str = ApiPath(pattern=r"^[A-Za-z]{1,5}$"),
        days: int = Query(252, ge=1, le=400),
    ) -> list[dict[str, Any]]:
        return store.market_state_history(symbol.upper(), days)

    @app.get("/api/intraday/status")
    def intraday_status() -> dict[str, Any]:
        if runtime.intraday is not None:
            return runtime.intraday.status()
        stored = {row["source"]: row for row in store.intraday_status()}
        return {
            "enabled": settings.intraday.enabled,
            "active": None,
            "sources": [stored.get(source, {
                "source": source,
                "symbols": 0,
                "days": 0,
                "last_day": None,
                "rows": 0,
                "last_fetch": None,
                "last_error": None,
                "last_job": None,
                "job_status": None,
            }) for source in ("nasdaq", "jin10", "ibkr")],
        }

    @app.get("/api/intraday/{source}/{symbol}")
    def intraday_day(
        source: str = ApiPath(pattern=r"^(nasdaq|jin10|ibkr)$"),
        symbol: str = ApiPath(pattern=r"^[A-Za-z0-9]{1,10}$"),
        day: date = Query(),
    ) -> dict[str, Any]:
        return {
            "source": source,
            "symbol": symbol.upper(),
            "day": day.isoformat(),
            "bars": store.intraday_day(source, symbol, day),
        }

    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

    @app.get("/")
    def index() -> FileResponse:
        return FileResponse(STATIC_DIR / "index.html")

    return app


def _run_summary(run: dict[str, Any]) -> dict[str, Any]:
    stages = run.get("stages") or {}
    return {
        "id": run["id"],
        "trading_date": run["trading_date"],
        "slot": run["slot"],
        "trigger": run["trigger"],
        "status": run["status"],
        "started_at": run["started_at"],
        "finished_at": run["finished_at"],
        "error": run["error"],
        "idea_count": run.get("idea_count"),
        "ibkr": (stages.get("ibkr") or {}).get("status"),
        "views": (stages.get("views") or {}).get("status"),
    }


def _run_detail(runtime: Runtime, run_id: int) -> dict[str, Any]:
    run = runtime.store.get_run(run_id)
    if run is None:
        raise HTTPException(404, "运行不存在")
    cited: set[int] = set()
    for view in run["views"]:
        cited.update(view.get("news_ids") or ())
    run["cited_news"] = {
        str(item.id): _news_dict(item) for item in runtime.store.news_by_ids(sorted(cited))
    }
    return run


def _news_dict(item: Any) -> dict[str, Any]:
    return {
        "id": item.id,
        "source": item.source,
        "published_at": item.published_at.isoformat(),
        "title": item.title,
        "body": item.body,
        "url": item.url,
        "symbols": list(item.symbols),
    }


def _date_window(today: date, days: int) -> tuple[datetime, datetime]:
    start = datetime.combine(today, datetime.min.time(), NEW_YORK).astimezone(timezone.utc)
    end_day = today + timedelta(days=days + 1)
    end = datetime.combine(end_day, datetime.min.time(), NEW_YORK).astimezone(timezone.utc)
    return start, end


def _macro_event_dict(event: MacroEvent) -> dict[str, Any]:
    local_ny = event.event_time.astimezone(NEW_YORK)
    local_beijing = event.event_time.astimezone(BEIJING)
    return {
        "time": event.event_time.isoformat(),
        "time_ny": local_ny.strftime("%Y-%m-%d %H:%M"),
        "time_beijing": local_beijing.strftime("%Y-%m-%d %H:%M"),
        "title": event.title,
        "name": (
            event.title
            if event.category == "other"
            else f"{CATEGORY_NAMES[event.category]} · {event.title}"
        ),
        "category": event.category,
        "importance": event.importance,
        "period": event.period,
        "actual": event.actual,
        "forecast": event.forecast,
        "previous": event.previous,
        "source": event.source,
        "major": event.category in MAJOR_CATEGORIES,
    }


def _tracking_summary(row: dict[str, Any]) -> dict[str, Any]:
    idea = row["entry"]["idea"]
    view = idea.get("view") or {}
    net_delta = idea.get("net_delta") or 0
    return {
        "id": row["id"],
        "symbol": row["symbol"],
        "structure": row["structure"],
        "structure_name": idea.get("structure_name", row["structure"]),
        "origin": row["origin"],
        "params_version": row["params_version"],
        "status": row["status"],
        "entered_at": row["entered_at"],
        "entry_run_id": row["entry_run_id"],
        "times_listed": row["times_listed"],
        "expiry": idea.get("expiry"),
        "entry_dte": idea.get("dte"),
        "legs": [
            {
                "action": leg.get("action"),
                "quantity": leg.get("quantity"),
                "strike": leg.get("strike"),
                "right": leg.get("right"),
                "expiry": leg.get("expiry"),
            }
            for leg in idea.get("legs", [])
        ],
        "net_premium": idea.get("net_premium"),
        "max_loss": idea.get("max_loss"),
        "max_profit": idea.get("max_profit"),
        "predicted_return_on_risk": idea.get("return_on_risk"),
        "view_pop": idea.get("view_pop"),
        "direction": 1 if net_delta > 0 else -1 if net_delta < 0 else 0,
        "conviction": view.get("conviction"),
        "mark": row.get("mark"),
        "exit": row.get("exit"),
    }


__all__ = ["create_app"]
