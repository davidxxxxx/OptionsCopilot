"""Local GUI/API. Loopback only: there is no authentication layer."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from fastapi import FastAPI, HTTPException, Path as ApiPath, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from copilot.analysis.ledger import ACTION_NAMES
from copilot.analysis.tagging import CATEGORIES
from copilot.market.state import with_own_rank
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
        return {
            "now": now.isoformat(timespec="seconds"),
            "new_york": now.astimezone(NEW_YORK).strftime("%Y-%m-%d %H:%M"),
            "beijing": now.astimezone(BEIJING).strftime("%Y-%m-%d %H:%M"),
            "schedule": runtime.scheduler.status() if runtime.scheduler else {"enabled": False},
            "running": runtime.runs.current,
            "latest_run": _run_summary(latest) if latest else None,
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
