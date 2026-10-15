"""Single SQLite database for news, runs, views, ideas and small caches."""
from __future__ import annotations

import json
import sqlite3
import threading
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from copilot.analysis.tagging import NewsTag
from copilot.news.macro_calendar import MacroEvent
from copilot.news.models import EarningsEvent, NewsItem

SCHEMA_VERSION = 7
NEW_YORK = ZoneInfo("America/New_York")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS news(
    id INTEGER PRIMARY KEY,
    source TEXT NOT NULL,
    source_id TEXT NOT NULL,
    published_at TEXT NOT NULL,
    fetched_at TEXT NOT NULL,
    title TEXT NOT NULL,
    body TEXT NOT NULL DEFAULT '',
    url TEXT NOT NULL DEFAULT '',
    symbols TEXT NOT NULL DEFAULT '[]',
    importance INTEGER NOT NULL DEFAULT 0,
    UNIQUE(source, source_id)
);
CREATE INDEX IF NOT EXISTS news_published ON news(published_at);
CREATE TABLE IF NOT EXISTS earnings(
    symbol TEXT NOT NULL,
    report_date TEXT NOT NULL,
    hour TEXT NOT NULL DEFAULT '',
    eps_estimate REAL,
    source TEXT NOT NULL DEFAULT '',
    fetched_at TEXT NOT NULL,
    PRIMARY KEY(symbol, report_date)
);
CREATE TABLE IF NOT EXISTS runs(
    id INTEGER PRIMARY KEY,
    trading_date TEXT NOT NULL,
    slot TEXT NOT NULL,
    trigger TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    status TEXT NOT NULL,
    stages TEXT NOT NULL DEFAULT '{}',
    error TEXT,
    macro_summary TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS runs_slot ON runs(trading_date, slot);
CREATE TABLE IF NOT EXISTS views(
    run_id INTEGER NOT NULL REFERENCES runs(id),
    symbol TEXT NOT NULL,
    payload TEXT NOT NULL,
    PRIMARY KEY(run_id, symbol)
);
CREATE TABLE IF NOT EXISTS ideas(
    run_id INTEGER NOT NULL REFERENCES runs(id),
    rank INTEGER NOT NULL,
    symbol TEXT NOT NULL,
    structure TEXT NOT NULL,
    payload TEXT NOT NULL,
    PRIMARY KEY(run_id, rank)
);
CREATE TABLE IF NOT EXISTS snapshots(
    run_id INTEGER NOT NULL REFERENCES runs(id),
    kind TEXT NOT NULL,
    key TEXT NOT NULL,
    payload TEXT NOT NULL,
    PRIMARY KEY(run_id, kind, key)
);
CREATE TABLE IF NOT EXISTS daily_cache(
    symbol TEXT NOT NULL,
    kind TEXT NOT NULL,
    as_of TEXT NOT NULL,
    payload TEXT NOT NULL,
    PRIMARY KEY(symbol, kind, as_of)
);
CREATE TABLE IF NOT EXISTS news_tags(
    news_id INTEGER PRIMARY KEY REFERENCES news(id),
    category TEXT NOT NULL,
    importance INTEGER NOT NULL,
    payload TEXT NOT NULL,
    model TEXT NOT NULL DEFAULT '',
    tagged_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS news_tags_importance ON news_tags(importance);
CREATE TABLE IF NOT EXISTS news_tag_failures(
    news_id INTEGER PRIMARY KEY REFERENCES news(id),
    attempts INTEGER NOT NULL,
    last_error TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS view_book(
    symbol TEXT PRIMARY KEY,
    payload TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS view_changes(
    id INTEGER PRIMARY KEY,
    symbol TEXT NOT NULL,
    run_id INTEGER,
    action TEXT NOT NULL,
    payload TEXT NOT NULL,
    changed_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS view_changes_symbol ON view_changes(symbol, id);
CREATE TABLE IF NOT EXISTS kv(
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS param_sets(
    version TEXT PRIMARY KEY,
    payload TEXT NOT NULL,
    first_seen TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS tracked_ideas(
    id INTEGER PRIMARY KEY,
    idea_key TEXT NOT NULL,
    symbol TEXT NOT NULL,
    structure TEXT NOT NULL,
    origin TEXT NOT NULL,
    params_version TEXT NOT NULL,
    entry_run_id INTEGER NOT NULL,
    entered_at TEXT NOT NULL,
    last_listed_run_id INTEGER NOT NULL,
    times_listed INTEGER NOT NULL DEFAULT 1,
    status TEXT NOT NULL,
    entry TEXT NOT NULL,
    mark TEXT,
    exit TEXT,
    updated_at TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS tracked_open_key
ON tracked_ideas(idea_key) WHERE status = 'open';
CREATE INDEX IF NOT EXISTS tracked_status ON tracked_ideas(status, symbol);
CREATE TRIGGER IF NOT EXISTS tracked_entry_immutable
BEFORE UPDATE OF idea_key, symbol, structure, origin, params_version, entry_run_id, entered_at, entry
ON tracked_ideas BEGIN SELECT RAISE(ABORT, 'tracked idea entry is immutable'); END;
CREATE TABLE IF NOT EXISTS tracked_marks(
    tracked_id INTEGER NOT NULL REFERENCES tracked_ideas(id),
    run_id INTEGER NOT NULL,
    marked_at TEXT NOT NULL,
    payload TEXT NOT NULL,
    PRIMARY KEY(tracked_id, run_id)
);
CREATE TABLE IF NOT EXISTS market_state(
    symbol TEXT NOT NULL,
    as_of TEXT NOT NULL,
    payload TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(symbol, as_of)
);
CREATE TABLE IF NOT EXISTS macro_events(
    source TEXT NOT NULL,
    source_id TEXT NOT NULL,
    event_time TEXT NOT NULL,
    country TEXT NOT NULL,
    category TEXT NOT NULL,
    importance INTEGER NOT NULL,
    title TEXT NOT NULL,
    payload TEXT NOT NULL,
    fetched_at TEXT NOT NULL,
    PRIMARY KEY(source, source_id)
);
CREATE INDEX IF NOT EXISTS macro_events_time ON macro_events(event_time);
CREATE TABLE IF NOT EXISTS event_stats(
    category TEXT NOT NULL,
    symbol TEXT NOT NULL,
    payload TEXT NOT NULL,
    computed_at TEXT NOT NULL,
    PRIMARY KEY(category, symbol)
);
CREATE TABLE IF NOT EXISTS event_reactions(
    event_date TEXT NOT NULL,
    category TEXT NOT NULL,
    symbol TEXT NOT NULL,
    event_time TEXT NOT NULL,
    title TEXT NOT NULL,
    actual REAL,
    forecast REAL,
    surprise REAL,
    z REAL,
    prediction TEXT,
    reaction TEXT,
    vol TEXT,
    status TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(event_date, category, symbol)
);
CREATE INDEX IF NOT EXISTS event_reactions_latest
ON event_reactions(category, symbol, event_date DESC);
CREATE TABLE IF NOT EXISTS intraday_series(
    source TEXT NOT NULL,
    symbol TEXT NOT NULL,
    day TEXT NOT NULL,
    bars TEXT NOT NULL,
    fetched_at TEXT NOT NULL,
    PRIMARY KEY(source, symbol, day)
);
"""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)


class Store:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self._lock = threading.RLock()
        self._db = sqlite3.connect(path, check_same_thread=False, timeout=30)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA foreign_keys=ON")
        self._db.executescript(_SCHEMA)
        self._db.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
        self._db.commit()

    def close(self) -> None:
        with self._lock:
            self._db.close()

    @contextmanager
    def _tx(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            try:
                yield self._db
                self._db.commit()
            except BaseException:
                self._db.rollback()
                raise

    def _rows(self, sql: str, params: Iterable[Any] = ()) -> list[sqlite3.Row]:
        with self._lock:
            return list(self._db.execute(sql, tuple(params)))

    # ---- news -------------------------------------------------------------
    def add_news(self, items: Iterable[NewsItem]) -> int:
        fetched = _now()
        added = 0
        with self._tx() as db:
            for item in items:
                cursor = db.execute(
                    "INSERT OR IGNORE INTO news(source, source_id, published_at, fetched_at,"
                    " title, body, url, symbols, importance) VALUES (?,?,?,?,?,?,?,?,?)",
                    (
                        item.source, item.source_id, item.published_at.isoformat(), fetched,
                        item.title, item.body, item.url, _dumps(list(item.symbols)), item.importance,
                    ),
                )
                added += cursor.rowcount
        return added

    def recent_news(
        self, since: datetime, *, limit: int = 500, sources: Iterable[str] | None = None
    ) -> list[NewsItem]:
        sql = "SELECT * FROM news WHERE published_at >= ?"
        params: list[Any] = [since.astimezone(timezone.utc).isoformat()]
        if sources:
            names = list(sources)
            sql += f" AND source IN ({','.join('?' * len(names))})"
            params.extend(names)
        sql += " ORDER BY published_at DESC LIMIT ?"
        params.append(limit)
        return [_news_row(row) for row in self._rows(sql, params)]

    def latest_news_time(self, source: str) -> datetime | None:
        rows = self._rows("SELECT MAX(published_at) AS t FROM news WHERE source = ?", (source,))
        value = rows[0]["t"] if rows else None
        return datetime.fromisoformat(value) if value else None

    def news_counts(self, since: datetime) -> dict[str, int]:
        rows = self._rows(
            "SELECT source, COUNT(*) AS n FROM news WHERE published_at >= ? GROUP BY source",
            (since.astimezone(timezone.utc).isoformat(),),
        )
        return {row["source"]: row["n"] for row in rows}

    def news_count_since(self, since: datetime) -> int:
        rows = self._rows(
            "SELECT COUNT(*) AS n FROM news WHERE published_at > ?",
            (since.astimezone(timezone.utc).isoformat(),),
        )
        return int(rows[0]["n"])

    def news_by_ids(self, ids: Iterable[int]) -> list[NewsItem]:
        wanted = [int(value) for value in ids]
        if not wanted:
            return []
        rows = self._rows(
            f"SELECT * FROM news WHERE id IN ({','.join('?' * len(wanted))})", wanted
        )
        return [_news_row(row) for row in rows]

    # ---- earnings ---------------------------------------------------------
    def upsert_earnings(self, events: Iterable[EarningsEvent]) -> int:
        fetched = _now()
        count = 0
        with self._tx() as db:
            for event in events:
                db.execute(
                    "INSERT INTO earnings(symbol, report_date, hour, eps_estimate, source, fetched_at)"
                    " VALUES (?,?,?,?,?,?) ON CONFLICT(symbol, report_date) DO UPDATE SET"
                    " hour=excluded.hour, eps_estimate=excluded.eps_estimate,"
                    " source=excluded.source, fetched_at=excluded.fetched_at",
                    (event.symbol, event.report_date.isoformat(), event.hour,
                     event.eps_estimate, event.source, fetched),
                )
                count += 1
        return count

    def earnings_between(self, start: date, end: date) -> list[EarningsEvent]:
        rows = self._rows(
            "SELECT * FROM earnings WHERE report_date BETWEEN ? AND ? ORDER BY report_date, symbol",
            (start.isoformat(), end.isoformat()),
        )
        return [
            EarningsEvent(
                symbol=row["symbol"], report_date=date.fromisoformat(row["report_date"]),
                hour=row["hour"], eps_estimate=row["eps_estimate"], source=row["source"],
            )
            for row in rows
        ]

    # ---- runs -------------------------------------------------------------
    def create_run(self, *, trading_date: date, slot: str, trigger: str) -> int:
        with self._tx() as db:
            cursor = db.execute(
                "INSERT INTO runs(trading_date, slot, trigger, started_at, status)"
                " VALUES (?,?,?,?, 'running')",
                (trading_date.isoformat(), slot, trigger, _now()),
            )
            return int(cursor.lastrowid)

    def update_run_stages(self, run_id: int, stages: dict[str, Any]) -> None:
        with self._tx() as db:
            db.execute("UPDATE runs SET stages = ? WHERE id = ?", (_dumps(stages), run_id))

    def finish_run(
        self,
        run_id: int,
        *,
        status: str,
        stages: dict[str, Any],
        error: str | None = None,
        macro_summary: str = "",
    ) -> None:
        with self._tx() as db:
            db.execute(
                "UPDATE runs SET finished_at=?, status=?, stages=?, error=?, macro_summary=?"
                " WHERE id=?",
                (_now(), status, _dumps(stages), error, macro_summary, run_id),
            )

    def fail_interrupted_runs(self) -> int:
        """Runs left 'running' by a crash or shutdown are closed as interrupted."""

        with self._tx() as db:
            cursor = db.execute(
                "UPDATE runs SET status='failed', error='interrupted', finished_at=?"
                " WHERE status='running'",
                (_now(),),
            )
            return cursor.rowcount

    def slot_runs(self, trading_date: date, slot: str) -> list[dict[str, Any]]:
        return [
            _run_row(row)
            for row in self._rows(
                "SELECT * FROM runs WHERE trading_date=? AND slot=? ORDER BY id",
                (trading_date.isoformat(), slot),
            )
        ]

    def list_runs(self, limit: int = 30) -> list[dict[str, Any]]:
        rows = self._rows("SELECT * FROM runs ORDER BY id DESC LIMIT ?", (limit,))
        result = []
        for row in rows:
            run = _run_row(row)
            run["idea_count"] = self._rows(
                "SELECT COUNT(*) AS n FROM ideas WHERE run_id=?", (run["id"],)
            )[0]["n"]
            result.append(run)
        return result

    def get_run(self, run_id: int) -> dict[str, Any] | None:
        rows = self._rows("SELECT * FROM runs WHERE id=?", (run_id,))
        if not rows:
            return None
        run = _run_row(rows[0])
        run["views"] = [
            json.loads(row["payload"])
            for row in self._rows("SELECT payload FROM views WHERE run_id=? ORDER BY symbol", (run_id,))
        ]
        run["ideas"] = [
            json.loads(row["payload"])
            for row in self._rows("SELECT payload FROM ideas WHERE run_id=? ORDER BY rank", (run_id,))
        ]
        run["snapshots"] = {}
        for row in self._rows("SELECT kind, key, payload FROM snapshots WHERE run_id=?", (run_id,)):
            run["snapshots"].setdefault(row["kind"], {})[row["key"]] = json.loads(row["payload"])
        return run

    def latest_run_id(self, *, finished_only: bool = True) -> int | None:
        sql = "SELECT id FROM runs"
        if finished_only:
            sql += " WHERE status IN ('ok', 'partial')"
        rows = self._rows(sql + " ORDER BY id DESC LIMIT 1")
        return int(rows[0]["id"]) if rows else None

    def latest_run_by_trigger(self, trigger: str) -> dict[str, Any] | None:
        rows = self._rows(
            "SELECT * FROM runs WHERE trigger=? AND status!='running' ORDER BY id DESC LIMIT 1",
            (trigger,),
        )
        return _run_row(rows[0]) if rows else None

    def latest_model_views(self, *, search_runs: int = 30) -> dict[str, Any] | None:
        """Views most recently produced by the LLM: origin run, generation time, payloads."""

        for row in self._rows("SELECT id, stages FROM runs ORDER BY id DESC LIMIT ?", (search_runs,)):
            views_stage = json.loads(row["stages"] or "{}").get("views") or {}
            if views_stage.get("status") not in ("llm", "reused") or not views_stage.get("generated_at"):
                continue
            origin = int(views_stage.get("source_run_id") or row["id"])
            origin_rows = self._rows("SELECT macro_summary FROM runs WHERE id=?", (origin,))
            return {
                "run_id": origin,
                "generated_at": datetime.fromisoformat(views_stage["generated_at"]),
                "macro_summary": origin_rows[0]["macro_summary"] if origin_rows else "",
                "views": [
                    json.loads(view["payload"])
                    for view in self._rows("SELECT payload FROM views WHERE run_id=?", (origin,))
                ],
            }
        return None

    def save_views(self, run_id: int, views: Iterable[dict[str, Any]]) -> None:
        with self._tx() as db:
            for view in views:
                db.execute(
                    "INSERT OR REPLACE INTO views(run_id, symbol, payload) VALUES (?,?,?)",
                    (run_id, view["symbol"], _dumps(view)),
                )

    def save_ideas(self, run_id: int, ideas: Iterable[dict[str, Any]]) -> None:
        with self._tx() as db:
            for idea in ideas:
                db.execute(
                    "INSERT OR REPLACE INTO ideas(run_id, rank, symbol, structure, payload)"
                    " VALUES (?,?,?,?,?)",
                    (run_id, idea["rank"], idea["symbol"], idea["structure"], _dumps(idea)),
                )

    def save_snapshot(self, run_id: int, kind: str, key: str, payload: Any) -> None:
        with self._tx() as db:
            db.execute(
                "INSERT OR REPLACE INTO snapshots(run_id, kind, key, payload) VALUES (?,?,?,?)",
                (run_id, kind, key, _dumps(payload)),
            )

    def latest_snapshot(self, kind: str, key: str) -> dict[str, Any] | list[Any] | None:
        rows = self._rows(
            "SELECT s.payload FROM snapshots s JOIN runs r ON r.id=s.run_id"
            " WHERE s.kind=? AND s.key=? AND r.status IN ('ok','partial')"
            " ORDER BY s.run_id DESC LIMIT 1",
            (kind, key),
        )
        return json.loads(rows[0]["payload"]) if rows else None

    def latest_underlying_before(self, symbol: str, moment: datetime) -> dict[str, Any] | None:
        rows = self._rows(
            "SELECT s.payload FROM snapshots s JOIN runs r ON r.id=s.run_id"
            " WHERE s.kind='underlying' AND s.key=? AND r.started_at<=?"
            " AND r.status IN ('ok','partial') ORDER BY r.started_at DESC, r.id DESC LIMIT 1",
            (symbol.upper(), moment.isoformat()),
        )
        return json.loads(rows[0]["payload"]) if rows else None

    # ---- parameter versions and recommendation tracking ------------------
    def save_param_set(self, version: str, payload: dict[str, Any]) -> None:
        with self._tx() as db:
            db.execute(
                "INSERT OR IGNORE INTO param_sets(version, payload, first_seen) VALUES (?,?,?)",
                (version, _dumps(payload), _now()),
            )

    def param_set(self, version: str) -> dict[str, Any] | None:
        rows = self._rows("SELECT payload FROM param_sets WHERE version=?", (version,))
        return json.loads(rows[0]["payload"]) if rows else None

    def open_tracked(self) -> list[dict[str, Any]]:
        return self.tracked_ideas(status="open", limit=None)

    def find_open_tracked(self, idea_key: str) -> dict[str, Any] | None:
        rows = self._rows(
            "SELECT * FROM tracked_ideas WHERE idea_key=? AND status='open' LIMIT 1",
            (idea_key,),
        )
        return _tracked_row(rows[0]) if rows else None

    def add_tracked(
        self,
        *,
        idea_key: str,
        symbol: str,
        structure: str,
        origin: str,
        params_version: str,
        entry_run_id: int,
        entered_at: str,
        entry: dict[str, Any],
    ) -> int:
        with self._tx() as db:
            cursor = db.execute(
                "INSERT INTO tracked_ideas(idea_key, symbol, structure, origin, params_version,"
                " entry_run_id, entered_at, last_listed_run_id, status, entry, updated_at)"
                " VALUES (?,?,?,?,?,?,?,?, 'open', ?,?)",
                (
                    idea_key, symbol, structure, origin, params_version, entry_run_id,
                    entered_at, entry_run_id, _dumps(entry), _now(),
                ),
            )
            return int(cursor.lastrowid)

    def touch_tracked(self, tracked_id: int, run_id: int) -> None:
        with self._tx() as db:
            db.execute(
                "UPDATE tracked_ideas SET last_listed_run_id=?, times_listed=times_listed+1,"
                " updated_at=? WHERE id=? AND status='open'",
                (run_id, _now(), tracked_id),
            )

    def add_tracked_mark(
        self, tracked_id: int, run_id: int, marked_at: str, payload: dict[str, Any]
    ) -> None:
        encoded = _dumps(payload)
        with self._tx() as db:
            db.execute(
                "INSERT OR REPLACE INTO tracked_marks(tracked_id, run_id, marked_at, payload)"
                " VALUES (?,?,?,?)",
                (tracked_id, run_id, marked_at, encoded),
            )
            db.execute(
                "UPDATE tracked_ideas SET mark=?, updated_at=? WHERE id=?",
                (encoded, _now(), tracked_id),
            )

    def close_tracked(self, tracked_id: int, exit_payload: dict[str, Any]) -> None:
        with self._tx() as db:
            db.execute(
                "UPDATE tracked_ideas SET status='closed', exit=?, updated_at=?"
                " WHERE id=? AND status='open'",
                (_dumps(exit_payload), _now(), tracked_id),
            )

    def tracked_ideas(
        self, status: str | None = None, limit: int | None = 300
    ) -> list[dict[str, Any]]:
        sql = "SELECT * FROM tracked_ideas"
        params: list[Any] = []
        if status is not None:
            sql += " WHERE status=?"
            params.append(status)
        sql += " ORDER BY id DESC"
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        return [_tracked_row(row) for row in self._rows(sql, params)]

    def tracked_idea(self, tracked_id: int) -> dict[str, Any] | None:
        rows = self._rows("SELECT * FROM tracked_ideas WHERE id=?", (tracked_id,))
        if not rows:
            return None
        result = _tracked_row(rows[0])
        result["marks"] = [
            json.loads(row["payload"])
            for row in self._rows(
                "SELECT payload FROM tracked_marks WHERE tracked_id=? ORDER BY marked_at, run_id",
                (tracked_id,),
            )
        ]
        return result

    def replace_macro_events(
        self,
        source: str,
        start: datetime,
        end: datetime,
        events: Iterable[MacroEvent],
    ) -> int:
        fetched = _now()
        rows = list(events)
        with self._tx() as db:
            db.execute(
                "DELETE FROM macro_events WHERE source=? AND event_time>=? AND event_time<?",
                (source, start.astimezone(timezone.utc).isoformat(), end.astimezone(timezone.utc).isoformat()),
            )
            for event in rows:
                db.execute(
                    "INSERT OR REPLACE INTO macro_events(source, source_id, event_time, country,"
                    " category, importance, title, payload, fetched_at) VALUES (?,?,?,?,?,?,?,?,?)",
                    (
                        source,
                        event.source_id,
                        event.event_time.isoformat(),
                        event.country,
                        event.category,
                        event.importance,
                        event.title,
                        _dumps({
                            "period": event.period,
                            "actual": event.actual,
                            "forecast": event.forecast,
                            "previous": event.previous,
                        }),
                        fetched,
                    ),
                )
        return len(rows)

    def news_calibration_candidates(self) -> list[dict[str, Any]]:
        """Return views from each trading day's last finished pre-close run."""

        selected: dict[str, tuple[datetime, int]] = {}
        for row in self._rows(
            "SELECT id, trading_date, started_at FROM runs"
            " WHERE status IN ('ok','partial') AND finished_at IS NOT NULL"
            " ORDER BY started_at, id"
        ):
            started = datetime.fromisoformat(row["started_at"])
            if started.tzinfo is None:
                started = started.replace(tzinfo=timezone.utc)
            local = started.astimezone(NEW_YORK)
            if local.time() >= datetime.strptime("16:00", "%H:%M").time():
                continue
            selected[row["trading_date"]] = (started, int(row["id"]))

        result: list[dict[str, Any]] = []
        for trading_date, (_started, run_id) in sorted(selected.items()):
            underlyings = {
                row["key"]: json.loads(row["payload"])
                for row in self._rows(
                    "SELECT key, payload FROM snapshots WHERE run_id=? AND kind='underlying'",
                    (run_id,),
                )
            }
            for row in self._rows(
                "SELECT symbol, payload FROM views WHERE run_id=? ORDER BY symbol", (run_id,)
            ):
                symbol = row["symbol"]
                state = self.market_state_on(symbol, date.fromisoformat(trading_date))
                result.append({
                    "run_id": run_id,
                    "trading_date": trading_date,
                    "symbol": symbol,
                    "view": json.loads(row["payload"]),
                    "underlying": underlyings.get(symbol),
                    "market_state": state,
                })
        return result

    def macro_events_between(
        self,
        start: datetime,
        end: datetime,
        *,
        min_importance: int = 0,
    ) -> list[MacroEvent]:
        rows = self._rows(
            "SELECT * FROM macro_events WHERE event_time>=? AND event_time<? AND importance>=?"
            " ORDER BY event_time, source, source_id",
            (
                start.astimezone(timezone.utc).isoformat(),
                end.astimezone(timezone.utc).isoformat(),
                min_importance,
            ),
        )
        result = []
        for row in rows:
            payload = json.loads(row["payload"])
            result.append(MacroEvent(
                source=row["source"],
                source_id=row["source_id"],
                event_time=datetime.fromisoformat(row["event_time"]),
                country=row["country"],
                title=row["title"],
                category=row["category"],
                importance=row["importance"],
                period=payload.get("period") or "",
                actual=payload.get("actual"),
                forecast=payload.get("forecast"),
                previous=payload.get("previous"),
            ))
        return result

    def replace_event_stats(self, rows: Iterable[dict[str, Any]]) -> int:
        values = list(rows)
        stamp = _now()
        with self._tx() as db:
            db.execute("DELETE FROM event_stats")
            for payload in values:
                db.execute(
                    "INSERT OR REPLACE INTO event_stats(category, symbol, payload, computed_at)"
                    " VALUES (?,?,?,?)",
                    (payload["category"], payload["symbol"], _dumps(payload), stamp),
                )
        return len(values)

    def event_stats(self, category: str | None = None) -> list[dict[str, Any]]:
        sql = "SELECT payload, computed_at FROM event_stats"
        params: tuple[Any, ...] = ()
        if category:
            sql += " WHERE category=?"
            params = (category,)
        sql += " ORDER BY category, symbol"
        return [
            {**json.loads(row["payload"]), "computed_at": row["computed_at"]}
            for row in self._rows(sql, params)
        ]

    def event_stats_are_current(self, stats_version: int) -> bool:
        rows = self._rows("SELECT payload FROM event_stats")
        return bool(rows) and all(
            json.loads(row["payload"]).get("stats_version") == stats_version for row in rows
        )

    def upsert_event_reaction(self, payload: dict[str, Any]) -> None:
        """Save one event occurrence while preserving its first prediction and z-score."""

        stamp = _now()
        with self._tx() as db:
            db.execute(
                "INSERT INTO event_reactions("
                " event_date, category, symbol, event_time, title, actual, forecast, surprise, z,"
                " prediction, reaction, vol, status, updated_at)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
                " ON CONFLICT(event_date, category, symbol) DO UPDATE SET"
                " event_time=excluded.event_time, title=excluded.title,"
                " actual=excluded.actual, forecast=excluded.forecast, surprise=excluded.surprise,"
                " z=event_reactions.z, prediction=event_reactions.prediction,"
                " reaction=COALESCE(excluded.reaction, event_reactions.reaction),"
                " vol=COALESCE(excluded.vol, event_reactions.vol),"
                " status=CASE"
                "  WHEN event_reactions.status='complete' THEN 'complete'"
                "  WHEN excluded.status='complete' THEN 'complete'"
                "  WHEN event_reactions.status='partial' OR excluded.status='partial' THEN 'partial'"
                "  ELSE 'predicted' END,"
                " updated_at=excluded.updated_at",
                (
                    str(payload["event_date"]), payload["category"], payload["symbol"],
                    str(payload["event_time"]), payload["title"], payload.get("actual"),
                    payload.get("forecast"), payload.get("surprise"), payload.get("z"),
                    _dumps(payload["prediction"]) if payload.get("prediction") is not None else None,
                    _dumps(payload["reaction"]) if payload.get("reaction") is not None else None,
                    _dumps(payload["vol"]) if payload.get("vol") is not None else None,
                    payload["status"], stamp,
                ),
            )

    def event_reactions(
        self,
        *,
        category: str | None = None,
        symbol: str | None = None,
        limit: int = 200,
        incomplete_only: bool = False,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if category:
            clauses.append("category=?")
            params.append(category)
        if symbol:
            clauses.append("symbol=?")
            params.append(symbol)
        if incomplete_only:
            clauses.append("status!='complete'")
        sql = "SELECT * FROM event_reactions"
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY event_date DESC, category, symbol LIMIT ?"
        params.append(limit)
        return [_event_reaction_row(row) for row in self._rows(sql, params)]

    def event_reactions_empty(self) -> bool:
        rows = self._rows("SELECT 1 FROM event_reactions LIMIT 1")
        return not rows

    def tagged_news_after_id(self, news_id: int) -> list[tuple[NewsItem, NewsTag]]:
        rows = self._rows(
            "SELECT n.*, t.payload AS tag_payload FROM news n JOIN news_tags t ON t.news_id=n.id"
            " WHERE n.id>? ORDER BY n.id",
            (news_id,),
        )
        return [
            (_news_row(row), NewsTag.from_row(row["id"], json.loads(row["tag_payload"])))
            for row in rows
        ]

    def save_market_state(self, symbol: str, as_of: date, payload: dict[str, Any]) -> None:
        with self._tx() as db:
            db.execute(
                "INSERT OR REPLACE INTO market_state(symbol, as_of, payload, updated_at)"
                " VALUES (?,?,?,?)",
                (symbol, as_of.isoformat(), _dumps(payload), _now()),
            )

    def latest_market_states(self) -> list[dict[str, Any]]:
        rows = self._rows(
            "SELECT m.symbol, m.as_of, m.payload, m.updated_at FROM market_state m"
            " JOIN (SELECT symbol, MAX(as_of) AS as_of FROM market_state GROUP BY symbol) latest"
            " ON latest.symbol=m.symbol AND latest.as_of=m.as_of ORDER BY m.symbol"
        )
        return [_market_state_row(row) for row in rows]

    def market_state_history(self, symbol: str, limit: int = 252) -> list[dict[str, Any]]:
        rows = self._rows(
            "SELECT symbol, as_of, payload, updated_at FROM ("
            " SELECT * FROM market_state WHERE symbol=? ORDER BY as_of DESC LIMIT ?"
            ") ORDER BY as_of",
            (symbol, limit),
        )
        return [_market_state_row(row) for row in rows]

    def market_state_on(self, symbol: str, as_of: date) -> dict[str, Any] | None:
        rows = self._rows(
            "SELECT symbol, as_of, payload, updated_at FROM market_state"
            " WHERE symbol=? AND as_of=?",
            (symbol, as_of.isoformat()),
        )
        return _market_state_row(rows[0]) if rows else None

    def latest_market_state_before(self, symbol: str, as_of: date) -> dict[str, Any] | None:
        rows = self._rows(
            "SELECT symbol, as_of, payload, updated_at FROM market_state"
            " WHERE symbol=? AND as_of<? ORDER BY as_of DESC LIMIT 1",
            (symbol, as_of.isoformat()),
        )
        return _market_state_row(rows[0]) if rows else None

    def previous_ideas(self, before_run_id: int, since: datetime) -> list[dict[str, Any]]:
        """Ideas of the most recent finished run before ``before_run_id`` that had any."""

        rows = self._rows(
            "SELECT r.id FROM runs r WHERE r.id < ? AND r.started_at >= ?"
            " AND r.status IN ('ok', 'partial')"
            " AND EXISTS (SELECT 1 FROM ideas i WHERE i.run_id = r.id) ORDER BY r.id DESC LIMIT 1",
            (before_run_id, since.astimezone(timezone.utc).isoformat()),
        )
        if not rows:
            return []
        return [
            json.loads(row["payload"])
            for row in self._rows("SELECT payload FROM ideas WHERE run_id=? ORDER BY rank", (rows[0]["id"],))
        ]

    # ---- news tags ----------------------------------------------------------
    def untagged_news(self, since: datetime, *, limit: int, max_attempts: int = 2) -> list[NewsItem]:
        """Newest untagged items first; items that failed ``max_attempts`` times are skipped."""

        rows = self._rows(
            "SELECT n.* FROM news n LEFT JOIN news_tags t ON t.news_id = n.id"
            " LEFT JOIN news_tag_failures f ON f.news_id = n.id"
            " WHERE t.news_id IS NULL AND n.published_at >= ? AND COALESCE(f.attempts, 0) < ?"
            " ORDER BY n.published_at DESC LIMIT ?",
            (since.astimezone(timezone.utc).isoformat(), max_attempts, limit),
        )
        return [_news_row(row) for row in rows]

    def count_untagged(self, since: datetime, *, max_attempts: int = 2) -> int:
        rows = self._rows(
            "SELECT COUNT(*) AS n FROM news n LEFT JOIN news_tags t ON t.news_id = n.id"
            " LEFT JOIN news_tag_failures f ON f.news_id = n.id"
            " WHERE t.news_id IS NULL AND n.published_at >= ? AND COALESCE(f.attempts, 0) < ?",
            (since.astimezone(timezone.utc).isoformat(), max_attempts),
        )
        return int(rows[0]["n"])

    def save_news_tags(self, tags: Iterable[NewsTag], *, model: str = "") -> int:
        stamp = _now()
        count = 0
        with self._tx() as db:
            for tag in tags:
                db.execute(
                    "INSERT OR REPLACE INTO news_tags(news_id, category, importance, payload, model, tagged_at)"
                    " VALUES (?,?,?,?,?,?)",
                    (tag.news_id, tag.category, tag.importance, _dumps(tag.to_dict()), model, stamp),
                )
                db.execute("DELETE FROM news_tag_failures WHERE news_id=?", (tag.news_id,))
                count += 1
        return count

    def record_tag_failures(self, news_ids: Iterable[int], error: str = "") -> None:
        stamp = _now()
        with self._tx() as db:
            for news_id in news_ids:
                db.execute(
                    "INSERT INTO news_tag_failures(news_id, attempts, last_error, updated_at) VALUES (?,1,?,?)"
                    " ON CONFLICT(news_id) DO UPDATE SET attempts=attempts+1,"
                    " last_error=excluded.last_error, updated_at=excluded.updated_at",
                    (news_id, error[:200], stamp),
                )

    def tagged_news(self, since: datetime, *, min_importance: int = 1) -> list[tuple[NewsItem, NewsTag]]:
        rows = self._rows(
            "SELECT n.*, t.payload AS tag_payload FROM news n JOIN news_tags t ON t.news_id = n.id"
            " WHERE n.published_at >= ? AND t.importance >= ? ORDER BY n.published_at DESC",
            (since.astimezone(timezone.utc).isoformat(), min_importance),
        )
        return [
            (_news_row(row), NewsTag.from_row(row["id"], json.loads(row["tag_payload"])))
            for row in rows
        ]

    def news_feed(
        self,
        since: datetime,
        *,
        limit: int,
        source: str | None = None,
        min_importance: int = 0,
        category: str | None = None,
        symbol: str | None = None,
    ) -> list[dict[str, Any]]:
        sql = (
            "SELECT n.*, t.payload AS tag_payload, t.importance AS tag_importance FROM news n"
            " LEFT JOIN news_tags t ON t.news_id = n.id WHERE n.published_at >= ?"
        )
        params: list[Any] = [since.astimezone(timezone.utc).isoformat()]
        if source:
            sql += " AND n.source = ?"
            params.append(source)
        if min_importance > 0:
            sql += " AND t.importance >= ?"
            params.append(min_importance)
        if category:
            sql += " AND t.category = ?"
            params.append(category)
        if symbol:
            # Coarse SQL filter on the JSON text, then an exact check below.
            sql += " AND t.payload LIKE ?"
            params.append(f'%"symbol":"{symbol}"%')
        sql += " ORDER BY n.published_at DESC LIMIT ?"
        params.append(limit)
        feed = []
        for row in self._rows(sql, params):
            item = _news_row(row)
            tag = json.loads(row["tag_payload"]) if row["tag_payload"] else None
            if symbol and tag and not any(i["symbol"] == symbol for i in tag.get("impacts", ())):
                continue
            feed.append({"item": item, "tag": tag})
        return feed

    # ---- view ledger ---------------------------------------------------------
    def view_book(self) -> dict[str, dict[str, Any]]:
        return {row["symbol"]: json.loads(row["payload"]) for row in self._rows("SELECT * FROM view_book")}

    def replace_view_book(self, book: dict[str, dict[str, Any]], changes: Iterable[dict[str, Any]],
                          *, run_id: int | None) -> None:
        stamp = _now()
        with self._tx() as db:
            db.execute("DELETE FROM view_book")
            for symbol, payload in book.items():
                db.execute("INSERT INTO view_book(symbol, payload, updated_at) VALUES (?,?,?)",
                           (symbol, _dumps(payload), stamp))
            for change in changes:
                db.execute(
                    "INSERT INTO view_changes(symbol, run_id, action, payload, changed_at) VALUES (?,?,?,?,?)",
                    (change["symbol"], run_id, change["action"], _dumps(change), change.get("at") or stamp),
                )

    def view_changes(self, *, limit: int = 100, symbol: str | None = None) -> list[dict[str, Any]]:
        sql = "SELECT * FROM view_changes"
        params: list[Any] = []
        if symbol:
            sql += " WHERE symbol = ?"
            params.append(symbol)
        sql += " ORDER BY id DESC LIMIT ?"
        params.append(limit)
        return [
            {**json.loads(row["payload"]), "run_id": row["run_id"]} for row in self._rows(sql, params)
        ]

    # ---- caches -----------------------------------------------------------
    def get_daily_cache(self, symbol: str, kind: str, as_of: date) -> Any | None:
        rows = self._rows(
            "SELECT payload FROM daily_cache WHERE symbol=? AND kind=? AND as_of=?",
            (symbol, kind, as_of.isoformat()),
        )
        return json.loads(rows[0]["payload"]) if rows else None

    def latest_daily_cache(self, symbol: str, kind: str) -> Any | None:
        rows = self._rows(
            "SELECT payload FROM daily_cache WHERE symbol=? AND kind=?"
            " ORDER BY as_of DESC LIMIT 1",
            (symbol, kind),
        )
        return json.loads(rows[0]["payload"]) if rows else None

    def put_daily_cache(self, symbol: str, kind: str, as_of: date, payload: Any) -> None:
        with self._tx() as db:
            db.execute(
                "INSERT OR REPLACE INTO daily_cache(symbol, kind, as_of, payload) VALUES (?,?,?,?)",
                (symbol, kind, as_of.isoformat(), _dumps(payload)),
            )

    def merge_intraday(self, source: str, symbol: str, bars: Iterable[Iterable[Any]]) -> int:
        """Merge timestamp-keyed bars into New York trading-day archive rows."""

        grouped: dict[str, dict[int, list[Any]]] = {}
        for raw in bars:
            values = list(raw)
            if len(values) != 6:
                continue
            try:
                timestamp = int(values[0])
                open_, high, low, close = (float(value) for value in values[1:5])
                volume = None if values[5] is None else float(values[5])
                day = datetime.fromtimestamp(timestamp, timezone.utc).astimezone(NEW_YORK).date()
            except (OSError, OverflowError, TypeError, ValueError):
                continue
            grouped.setdefault(day.isoformat(), {})[timestamp] = [
                timestamp, open_, high, low, close, volume,
            ]
        if not grouped:
            return 0
        source = source.strip().lower()
        symbol = symbol.strip().upper()
        stamp = _now()
        merged_count = 0
        with self._tx() as db:
            for day, incoming in grouped.items():
                row = db.execute(
                    "SELECT bars FROM intraday_series WHERE source=? AND symbol=? AND day=?",
                    (source, symbol, day),
                ).fetchone()
                existing = {
                    int(bar[0]): list(bar)
                    for bar in (json.loads(row["bars"]) if row else [])
                    if isinstance(bar, list) and len(bar) == 6
                }
                existing.update(incoming)
                encoded = [existing[key] for key in sorted(existing)]
                db.execute(
                    "INSERT INTO intraday_series(source, symbol, day, bars, fetched_at)"
                    " VALUES (?,?,?,?,?) ON CONFLICT(source, symbol, day) DO UPDATE SET"
                    " bars=excluded.bars, fetched_at=excluded.fetched_at",
                    (source, symbol, day, _dumps(encoded), stamp),
                )
                merged_count += len(incoming)
        return merged_count

    def intraday_day(self, source: str, symbol: str, day: date | str) -> list[list[Any]]:
        rows = self._rows(
            "SELECT bars FROM intraday_series WHERE source=? AND symbol=? AND day=?",
            (source.lower(), symbol.upper(), str(day)),
        )
        return json.loads(rows[0]["bars"]) if rows else []

    def intraday_status(self) -> list[dict[str, Any]]:
        rows = self._rows(
            "SELECT source, symbol, day, bars, fetched_at FROM intraday_series"
            " ORDER BY source, symbol, day"
        )
        summaries: dict[str, dict[str, Any]] = {}
        for row in rows:
            summary = summaries.setdefault(row["source"], {
                "source": row["source"],
                "symbols": set(),
                "days": set(),
                "last_day": None,
                "rows": 0,
                "last_fetch": None,
            })
            summary["symbols"].add(row["symbol"])
            summary["days"].add(row["day"])
            summary["last_day"] = max(summary["last_day"] or row["day"], row["day"])
            summary["rows"] += len(json.loads(row["bars"]))
            summary["last_fetch"] = max(
                summary["last_fetch"] or row["fetched_at"], row["fetched_at"]
            )
        return [
            {
                **summary,
                "symbols": len(summary["symbols"]),
                "days": len(summary["days"]),
            }
            for summary in summaries.values()
        ]

    def set_kv(self, key: str, value: Any) -> None:
        with self._tx() as db:
            db.execute(
                "INSERT OR REPLACE INTO kv(key, value, updated_at) VALUES (?,?,?)",
                (key, _dumps(value), _now()),
            )

    def get_kv(self, key: str) -> Any | None:
        rows = self._rows("SELECT value FROM kv WHERE key=?", (key,))
        return json.loads(rows[0]["value"]) if rows else None


def _news_row(row: sqlite3.Row) -> NewsItem:
    return NewsItem(
        id=row["id"],
        source=row["source"],
        source_id=row["source_id"],
        published_at=datetime.fromisoformat(row["published_at"]),
        title=row["title"],
        body=row["body"],
        url=row["url"],
        symbols=tuple(json.loads(row["symbols"])),
        importance=row["importance"],
    )


def _run_row(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "id": row["id"],
        "trading_date": row["trading_date"],
        "slot": row["slot"],
        "trigger": row["trigger"],
        "started_at": row["started_at"],
        "finished_at": row["finished_at"],
        "status": row["status"],
        "stages": json.loads(row["stages"] or "{}"),
        "error": row["error"],
        "macro_summary": row["macro_summary"],
    }


def _tracked_row(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "id": row["id"],
        "idea_key": row["idea_key"],
        "symbol": row["symbol"],
        "structure": row["structure"],
        "origin": row["origin"],
        "params_version": row["params_version"],
        "entry_run_id": row["entry_run_id"],
        "entered_at": row["entered_at"],
        "last_listed_run_id": row["last_listed_run_id"],
        "times_listed": row["times_listed"],
        "status": row["status"],
        "entry": json.loads(row["entry"]),
        "mark": json.loads(row["mark"]) if row["mark"] else None,
        "exit": json.loads(row["exit"]) if row["exit"] else None,
        "updated_at": row["updated_at"],
    }


def _market_state_row(row: sqlite3.Row) -> dict[str, Any]:
    return {
        **json.loads(row["payload"]),
        "symbol": row["symbol"],
        "as_of": row["as_of"],
        "updated_at": row["updated_at"],
    }


def _event_reaction_row(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "event_date": row["event_date"],
        "category": row["category"],
        "symbol": row["symbol"],
        "event_time": row["event_time"],
        "title": row["title"],
        "actual": row["actual"],
        "forecast": row["forecast"],
        "surprise": row["surprise"],
        "z": row["z"],
        "prediction": json.loads(row["prediction"]) if row["prediction"] else None,
        "reaction": json.loads(row["reaction"]) if row["reaction"] else None,
        "vol": json.loads(row["vol"]) if row["vol"] else None,
        "status": row["status"],
        "updated_at": row["updated_at"],
    }


__all__ = ["SCHEMA_VERSION", "Store"]
