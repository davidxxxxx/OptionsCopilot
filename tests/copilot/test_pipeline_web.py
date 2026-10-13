from __future__ import annotations

import ast
import json
import time
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from fastapi.testclient import TestClient

from copilot.news.collect import NewsSources
from copilot.market.models import Position
from copilot.news.macro_calendar import MacroEvent
from copilot.news.models import NewsItem
from copilot.pipeline import PipelineDeps, run_pipeline
from copilot.runtime import Runtime
from copilot.scheduler import RunManager
from copilot.secrets import Secrets
from copilot.store import Store
from copilot.web.app import create_app

from .conftest import TODAY, FakeLlm, FakeMarket

NOW = datetime(2026, 10, 9, 14, 0, tzinfo=timezone.utc)
SPOTS = {"SPY": 600.0, "QQQ": 500.0, "NVDA": 180.0, "JPM": 300.0}
RULES = {
    "英伟达": {
        "category": "company", "importance": 3, "summary": "英伟达发布新芯片",
        "impacts": [
            {"symbol": "NVDA", "direction": 1, "magnitude": 3, "horizon": "weeks", "vol": "none"},
            {"symbol": "SPY", "direction": 1, "magnitude": 1, "horizon": "days", "vol": "none"},
        ],
    },
}


def seed_news(store) -> None:
    store.add_news([NewsItem(source="jin10", source_id="a", published_at=NOW, title="英伟达发布新芯片")])


def deps(settings, store, *, market: FakeMarket | None, model) -> PipelineDeps:
    return PipelineDeps(
        settings=settings, store=store, news_sources=None, llm=model,
        market_factory=(lambda: market) if market else None, clock=lambda: NOW,
    )


def test_full_run_produces_ranked_ideas(settings, store) -> None:
    seed_news(store)
    market = FakeMarket(SPOTS)
    run_id = run_pipeline(deps(settings, store, market=market, model=FakeLlm(RULES)), trigger="test", slot="manual")
    run = store.get_run(run_id)
    assert run["status"] == "ok", run["stages"]
    assert run["macro_summary"] == "降息预期支撑风险资产"
    assert run["ideas"], run["stages"]["options"]
    assert run["ideas"][0]["edge"] > 0
    assert {idea["symbol"] for idea in run["ideas"]} <= {"NVDA", "SPY", "QQQ"}
    nvda = next(view for view in run["views"] if view["symbol"] == "NVDA")
    assert nvda["news_ids"] and nvda["direction"] == "bullish" and nvda["conviction"] == 5
    assert run["stages"]["tagging"]["tagged"] == 1
    assert [c["symbol"] for c in run["stages"]["views"]["changes"]] == ["NVDA", "SPY"]
    assert run["snapshots"]["account"]["summary"]["net_liquidation"] == 50_000.0
    assert run["snapshots"]["underlying"]["NVDA"]["hv20"] is not None
    assert market.closed
    # Daily history is cached: a second run does not need fresh bars.
    assert store.get_daily_cache("NVDA", "TRADES", date(2026, 10, 9))


def test_ibkr_down_still_stores_views_and_is_partial(settings, store) -> None:
    seed_news(store)
    run_id = run_pipeline(
        deps(settings, store, market=FakeMarket(SPOTS, unavailable=True), model=FakeLlm(RULES)),
        trigger="test", slot="manual",
    )
    run = store.get_run(run_id)
    assert run["status"] == "partial"
    assert run["stages"]["ibkr"]["status"] == "unavailable"
    assert run["stages"]["options"]["status"] == "skipped"
    assert {view["symbol"] for view in run["views"]} >= {"NVDA", "SPY", "QQQ"}
    assert run["ideas"] == []


def test_repeated_timeouts_stop_the_option_stage_early(settings, store) -> None:
    market = FakeMarket(SPOTS, hang_chains=True)
    run_id = run_pipeline(deps(settings, store, market=market, model=FakeLlm(RULES)), trigger="test", slot="manual")
    run = store.get_run(run_id)
    assert run["status"] == "partial"
    assert run["stages"]["options"]["status"] == "partial"
    assert market.chain_calls == 2  # gave up after two consecutive timeouts, not one per symbol


def test_upstream_loss_fails_fast() -> None:
    from copilot.config import IbkrSettings
    from copilot.market.ibkr import IbkrMarket, IbkrUnavailable
    import pytest

    market = IbkrMarket(IbkrSettings())
    market._ib = type("Connected", (), {"isConnected": lambda self: True})()
    market._on_error(-1, 1100, "Connectivity between IBKR and Trader Workstation has been lost.")
    with pytest.raises(IbkrUnavailable, match="1100"):
        market.chain_info("SPY")
    market._on_error(-1, 1102, "restored")
    assert not market.upstream_lost
    market._ib = None


def test_llm_failure_marks_partial_and_retries_tagging_later(settings, store) -> None:
    seed_news(store)
    run_id = run_pipeline(
        deps(settings, store, market=FakeMarket(SPOTS), model=FakeLlm(fail=RuntimeError("402"))),
        trigger="test", slot="manual",
    )
    run = store.get_run(run_id)
    assert run["status"] == "partial"
    assert run["stages"]["tagging"]["status"] == "failed"
    assert {v["symbol"] for v in run["views"]} == {"SPY", "QQQ"}  # core symbols, neutral
    assert all(v["direction"] == "neutral" for v in run["views"])
    # The item is retried on the next run (one failed attempt so far).
    llm = FakeLlm(RULES)
    run_pipeline(deps(settings, store, market=FakeMarket(SPOTS), model=llm), trigger="test", slot="manual")
    assert llm.calls["tag"] == 1


def test_web_api_serves_latest_run_and_triggers(settings, store) -> None:
    seed_news(store)
    pipeline_deps = deps(settings, store, market=FakeMarket(SPOTS), model=FakeLlm(RULES))
    runs = RunManager(lambda trigger, slot: run_pipeline(pipeline_deps, trigger=trigger, slot=slot))
    runtime = Runtime(settings, Secrets({}), store, pipeline_deps, runs, None)
    client = TestClient(create_app(runtime))
    assert client.get("/api/runs/latest").status_code == 404
    assert runs.run_blocking("manual", "manual") is not None
    status = client.get("/api/status").json()
    assert status["latest_run"]["status"] == "ok"
    latest = client.get("/api/runs/latest").json()
    assert latest["ideas"] and "cited_news" in latest
    news = client.get("/api/news?hours=168").json()
    assert news[0]["title"] == "英伟达发布新芯片" and news[0]["tag"]["importance"] == 3
    assert client.get("/api/news?hours=168&min_importance=3&symbol=nvda").json()
    assert client.get("/api/news?hours=168&symbol=JPM").json() == []
    book = client.get("/api/views").json()
    assert book["book"][0]["symbol"] == "NVDA" and book["changes"]
    assert "earnings" in client.get("/api/meta").json()["categories"]
    assert client.get("/").status_code == 200
    assert client.post("/api/runs").status_code == 202
    deadline = time.monotonic() + 20
    while runs.busy and time.monotonic() < deadline:
        time.sleep(0.05)
    assert not runs.busy


def test_copilot_never_calls_order_apis() -> None:
    """The broker adapter is read-only by construction."""

    forbidden = {
        "placeOrder", "cancelOrder", "reqGlobalCancel", "bracketOrder", "oneCancelsAll",
        "exerciseOptions", "whatIfOrder", "reqAutoOpenOrders",
    }
    package = Path(__file__).resolve().parents[2] / "copilot"
    for path in package.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        names = {node.attr for node in ast.walk(tree) if isinstance(node, ast.Attribute)}
        names |= {node.id for node in ast.walk(tree) if isinstance(node, ast.Name)}
        assert not names & forbidden, f"{path} references {names & forbidden}"
    source = (package / "market" / "ibkr.py").read_text(encoding="utf-8")
    assert "readonly=True" in source


def _clocked_deps(settings, store, model, clock, market=None):
    return PipelineDeps(
        settings=settings, store=store, news_sources=None, llm=model,
        market_factory=lambda: market or FakeMarket(SPOTS), clock=lambda: clock[0],
    )


def test_same_news_gives_the_same_views_and_is_tagged_once(settings, store) -> None:
    from datetime import timedelta

    seed_news(store)
    clock = [NOW]
    llm = FakeLlm(RULES)
    first = store.get_run(run_pipeline(_clocked_deps(settings, store, llm, clock), trigger="t", slot="manual"))
    clock[0] = NOW + timedelta(minutes=20)
    second = store.get_run(run_pipeline(_clocked_deps(settings, store, llm, clock), trigger="t", slot="manual"))
    assert llm.calls == {"tag": 1, "brief": 1}  # nothing re-tagged, brief reused
    def summary(run):
        return {v["symbol"]: (v["direction"], v["conviction"], v["volatility"]) for v in run["views"]}
    assert summary(first) == summary(second)
    assert second["stages"]["views"]["changes"] == []
    assert second["macro_summary"] == first["macro_summary"]


def test_ideas_are_sticky_between_runs(settings, store) -> None:
    from datetime import timedelta

    seed_news(store)
    clock = [NOW]
    llm = FakeLlm(RULES)
    first = store.get_run(run_pipeline(_clocked_deps(settings, store, llm, clock), trigger="t", slot="manual"))
    assert first["ideas"] and all("status" not in idea for idea in first["ideas"])
    clock[0] = NOW + timedelta(minutes=20)
    second = store.get_run(run_pipeline(_clocked_deps(settings, store, llm, clock), trigger="t", slot="manual"))
    assert [i["structure"] + i["symbol"] for i in second["ideas"]] == [i["structure"] + i["symbol"] for i in first["ideas"]]
    assert all(idea["status"] == "kept" for idea in second["ideas"])
    assert second["stages"]["options"]["dropped"] == []


def test_option_quotes_request_sampled_strikes_without_chain_details() -> None:
    from datetime import date as day
    from types import SimpleNamespace

    from copilot.config import IbkrSettings
    from copilot.market.ibkr import IbkrMarket
    from copilot.market.models import ChainInfo

    requested = []

    class FakeIB:
        def isConnected(self):
            return True

        def reqContractDetails(self, *args, **kwargs):
            raise AssertionError("whole-chain contract details must not be requested")

        def reqMktData(self, contract, ticks, snapshot, regulatory):
            requested.append(contract)
            listed = contract.strike % 5 == 0  # weekly expiry lists only $5 strikes here
            greeks = SimpleNamespace(impliedVol=0.2, delta=0.5, gamma=0.01, theta=-0.1, vega=0.2, optPrice=3.0)
            return SimpleNamespace(
                contract=contract, bid=float("nan"), ask=float("nan"), last=2.9 if listed else float("nan"),
                modelGreeks=greeks if listed else None, callOpenInterest=500, putOpenInterest=400,
                volume=10, time=None, marketDataType=2,
            )

        def cancelMktData(self, contract):
            pass

        def sleep(self, seconds):
            pass

    market = IbkrMarket(IbkrSettings(quote_wait_seconds=0.01))
    market._ib = FakeIB()
    info = ChainInfo("SPY", "SPY", 100, "SMART", (day(2026, 10, 30),), tuple(float(s) for s in range(550, 651)))
    quotes = market.option_quotes(info, [day(2026, 10, 30)], spot=600.0, strike_low=570, strike_high=630,
                                  max_strikes_per_side=12, require_bbo=False)
    assert requested and len(requested) <= 24
    assert market.settings.option_batch_size == 20
    assert all(c.tradingClass == "SPY" and c.exchange == "SMART" and c.right in "CP" for c in requested)
    assert quotes and all(q.strike % 5 == 0 for q in quotes)  # unlisted strikes dropped
    assert all(q.model_price == 3.0 for q in quotes)
    market._ib = None


def test_ibkr_connect_works_from_a_worker_thread_in_a_fresh_process() -> None:
    """Regression: the GUI runs the pipeline on a thread; ib_insync must import there."""

    import subprocess
    import sys
    from pathlib import Path

    script = (
        "import threading\n"
        "from copilot.config import IbkrSettings\n"
        "from copilot.market.ibkr import IbkrMarket, IbkrUnavailable\n"
        "result = []\n"
        "def work():\n"
        "    market = IbkrMarket(IbkrSettings(port=1, connect_timeout_seconds=2))\n"
        "    try:\n"
        "        market.connect()\n"
        "        result.append('connected')\n"
        "    except IbkrUnavailable:\n"
        "        result.append('unavailable')\n"
        "    except Exception as exc:\n"
        "        result.append(type(exc).__name__)\n"
        "    finally:\n"
        "        market.close()\n"
        "thread = threading.Thread(target=work, name='copilot-run')\n"
        "thread.start(); thread.join(30)\n"
        "print(result[0] if result else 'hung')\n"
    )
    root = Path(__file__).resolve().parents[2]
    output = subprocess.run(
        [sys.executable, "-c", script], cwd=root, capture_output=True, text=True, timeout=60,
    )
    assert output.stdout.strip().splitlines()[-1] == "unavailable", output.stdout + output.stderr


def _cboe_deps(settings, store, *, market, cboe, model=None, index_history=None):
    return PipelineDeps(
        settings=settings, store=store, news_sources=None, llm=model or FakeLlm(RULES),
        market_factory=(lambda: market) if market else None, clock=lambda: NOW, option_source=cboe,
        index_history=index_history,
    )


def test_cboe_produces_ideas_while_ibkr_is_down(settings, store) -> None:
    from .conftest import FakeCboe

    seed_news(store)
    run_id = run_pipeline(
        _cboe_deps(settings, store, market=FakeMarket(SPOTS, unavailable=True), cboe=FakeCboe(SPOTS)),
        trigger="test", slot="manual",
    )
    run = store.get_run(run_id)
    assert run["status"] == "partial"  # no broker: no account/sizing, but ideas exist
    assert run["stages"]["options"]["status"] == "ok"
    assert run["stages"]["options"]["sources"]["cboe"] >= 3
    assert run["ideas"] and all(i["data_source"] == "cboe" and i["delayed_data"] for i in run["ideas"])
    assert run["ideas"][0]["suggested_quantity"] is None  # net liquidation unknown


def test_nasdaq_closes_fill_hv20_without_ibkr_and_share_ladder_cache(settings, store) -> None:
    from .conftest import FakeCboe

    seed_news(store)
    calls = []

    def history(symbol: str, today: date):
        calls.append(symbol)
        start = today - timedelta(days=80)
        return [
            (start + timedelta(days=index), 100.0 * (1.004 if index % 2 else 0.996) ** index)
            for index in range(70)
        ]

    run = store.get_run(run_pipeline(
        _cboe_deps(
            settings, store, market=FakeMarket(SPOTS, unavailable=True), cboe=FakeCboe(SPOTS),
            index_history=history,
        ),
        trigger="test", slot="manual",
    ))
    assert run["snapshots"]["underlying"]["NVDA"]["hv20"] is not None
    assert run["stages"]["options"]["closes"] in {"nasdaq", "cache"}
    assert calls.count("SPY") == 1 and calls.count("QQQ") == 1


def test_macro_events_flow_to_ideas_tracking_stage_and_position_alerts(settings, store) -> None:
    from .conftest import FakeCboe

    seed_news(store)
    event_day = TODAY + timedelta(days=2)
    macro = MacroEvent(
        source="tradingview", source_id="cpi", event_time=datetime.combine(
            event_day, datetime.min.time(), timezone.utc
        ) + timedelta(hours=12, minutes=30),
        country="US", title="Inflation Rate YoY", category="cpi", importance=3,
    )
    store.replace_macro_events(
        "tradingview", NOW - timedelta(days=1), NOW + timedelta(days=30), [macro]
    )
    expiry = TODAY + timedelta(days=21)
    position = Position(
        symbol="SPY", sec_type="OPT", quantity=-1, avg_cost=500.0, multiplier=100,
        expiry=expiry, strike=600.0, right="C",
    )
    run = store.get_run(run_pipeline(
        _cboe_deps(
            settings, store,
            market=FakeMarket(SPOTS, positions=(position,)),
            cboe=FakeCboe(SPOTS),
        ),
        trigger="test", slot="manual",
    ))
    assert run["stages"]["calendar"]["major_next_days"][0]["category"] == "cpi"
    tagged = [idea for idea in run["ideas"] if idea.get("macro_events")]
    assert tagged and tagged[0]["macro_events"][0]["name"] == "CPI"
    tracked = store.tracked_ideas(limit=None)
    assert tracked and any(row["entry"]["idea"].get("macro_events") for row in tracked)
    alerts = run["snapshots"]["positions"]["book"]["groups"][0]["alerts"]
    assert "macro_event" in {alert["code"] for alert in alerts}


def _run_same_day_macro_case(settings, store, monkeypatch, now: datetime):
    import copilot.pipeline as pipeline_module
    from .conftest import FakeCboe

    seed_news(store)
    event_time = datetime(TODAY.year, TODAY.month, TODAY.day, 12, 30, tzinfo=timezone.utc)
    store.replace_macro_events(
        "tradingview", event_time - timedelta(days=1), event_time + timedelta(days=1),
        [MacroEvent(
            "tradingview", "same-day-cpi", event_time, "US",
            "Inflation Rate YoY", "cpi", 3,
        )],
    )
    position = Position(
        symbol="SPY", sec_type="OPT", quantity=-1, avg_cost=500.0, multiplier=100,
        expiry=TODAY + timedelta(days=21), strike=600.0, right="C",
    )
    catalyst_events = []
    original_macro_dates = pipeline_module._macro_dates

    def capture_macro_dates(events):
        catalyst_events.extend(events)
        return original_macro_dates(events)

    monkeypatch.setattr(pipeline_module, "_macro_dates", capture_macro_dates)
    pipeline_deps = PipelineDeps(
        settings=settings,
        store=store,
        news_sources=None,
        llm=FakeLlm(RULES),
        market_factory=lambda: FakeMarket(SPOTS, positions=(position,)),
        clock=lambda: now,
        option_source=FakeCboe(SPOTS),
    )
    run = store.get_run(run_pipeline(pipeline_deps, trigger="test", slot="manual"))
    return run, catalyst_events


def test_released_same_day_macro_event_is_excluded_from_forward_looking_outputs(
    settings, store, monkeypatch,
) -> None:
    now = datetime(TODAY.year, TODAY.month, TODAY.day, 16, 30, tzinfo=timezone.utc)
    run, catalyst_events = _run_same_day_macro_case(settings, store, monkeypatch, now)
    assert catalyst_events == []
    assert run["stages"]["calendar"]["major_next_days"] == []
    assert all(not idea.get("macro_events") for idea in run["ideas"])
    alerts = run["snapshots"]["positions"]["book"]["groups"][0]["alerts"]
    assert "macro_event" not in {alert["code"] for alert in alerts}


def test_unreleased_same_day_macro_event_is_included_in_forward_looking_outputs(
    settings, store, monkeypatch,
) -> None:
    now = datetime(TODAY.year, TODAY.month, TODAY.day, 12, 0, tzinfo=timezone.utc)
    run, catalyst_events = _run_same_day_macro_case(settings, store, monkeypatch, now)
    assert any(event["category"] == "cpi" for event in catalyst_events)
    assert run["stages"]["calendar"]["major_next_days"][0]["category"] == "cpi"
    assert any(idea.get("macro_events") for idea in run["ideas"])
    alerts = run["snapshots"]["positions"]["book"]["groups"][0]["alerts"]
    assert "macro_event" in {alert["code"] for alert in alerts}


def test_macro_event_web_endpoints_shapes_and_validation(settings, store) -> None:
    from zoneinfo import ZoneInfo

    today = datetime.now(ZoneInfo("America/New_York")).date()
    moment = datetime.combine(today + timedelta(days=1), datetime.min.time(), timezone.utc) + timedelta(hours=13)
    store.replace_macro_events(
        "tradingview", moment - timedelta(days=1), moment + timedelta(days=1),
        [
            MacroEvent("tradingview", "web", moment, "US", "Inflation Rate YoY", "cpi", 3, "Sep", None, "2.8%", "2.7%"),
            MacroEvent("tradingview", "other", moment + timedelta(hours=1), "US", "Existing Home Sales", "other", 3),
        ],
    )
    pipeline_deps = deps(settings, store, market=None, model=None)
    runtime = Runtime(settings, Secrets({}), store, pipeline_deps, RunManager(lambda trigger, slot: 0), None)
    client = TestClient(create_app(runtime))
    response = client.get("/api/macro-events?days=7&min_importance=3")
    assert response.status_code == 200
    rows = {row["category"]: row for row in response.json()}
    row = rows["cpi"]
    assert {"time", "time_ny", "time_beijing", "name", "major", "source"} <= row.keys()
    assert row["name"] == "CPI · Inflation Rate YoY" and row["major"] is True
    assert rows["other"]["name"] == "Existing Home Sales"
    major = client.get("/api/macro-events/major?days=7")
    assert major.status_code == 200 and major.json()[0]["category"] == "cpi"
    assert client.get("/api/macro-events?days=0").status_code == 422
    assert client.get("/api/macro-events?min_importance=4").status_code == 422
    assert client.get("/api/macro-events/major?days=31").status_code == 422


def test_major_macro_endpoint_excludes_released_events_but_full_table_keeps_them(
    settings, store, monkeypatch,
) -> None:
    import copilot.web.app as web_app

    fixed_now = datetime(2026, 10, 13, 16, 30, tzinfo=timezone.utc)

    class FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed_now if tz is None else fixed_now.astimezone(tz)

    monkeypatch.setattr(web_app, "datetime", FixedDatetime)
    event_time = datetime(2026, 10, 13, 12, 30, tzinfo=timezone.utc)
    store.replace_macro_events(
        "tradingview", event_time - timedelta(days=1), event_time + timedelta(days=1),
        [MacroEvent(
            "tradingview", "released", event_time, "US",
            "Inflation Rate YoY", "cpi", 3,
        )],
    )
    pipeline_deps = deps(settings, store, market=None, model=None)
    runtime = Runtime(settings, Secrets({}), store, pipeline_deps, RunManager(lambda trigger, slot: 0), None)
    client = TestClient(create_app(runtime))
    assert client.get("/api/macro-events?days=7").json()[0]["source"] == "tradingview"
    assert client.get("/api/macro-events/major?days=7").json() == []


def test_macro_event_table_deduplicates_configured_fomc_when_provider_row_exists(
    settings, store,
) -> None:
    from zoneinfo import ZoneInfo

    today = datetime.now(ZoneInfo("America/New_York")).date()
    duplicate_day = today + timedelta(days=1)
    fed_only_day = today + timedelta(days=2)
    duplicate_time = datetime.combine(duplicate_day, datetime.min.time(), timezone.utc) + timedelta(hours=18)
    fed_only_time = datetime.combine(fed_only_day, datetime.min.time(), timezone.utc) + timedelta(hours=18)
    store.replace_macro_events(
        "fed", duplicate_time - timedelta(days=1), fed_only_time + timedelta(days=1),
        [
            MacroEvent("fed", "configured-duplicate", duplicate_time, "US", "FOMC Rate Decision", "fomc_decision", 3),
            MacroEvent("fed", "configured-only", fed_only_time, "US", "FOMC Rate Decision", "fomc_decision", 3),
        ],
    )
    store.replace_macro_events(
        "tradingview", duplicate_time - timedelta(days=1), duplicate_time + timedelta(days=1),
        [MacroEvent(
            "tradingview", "provider", duplicate_time, "US",
            "Fed Interest Rate Decision", "fomc_decision", 3,
        )],
    )
    pipeline_deps = deps(settings, store, market=None, model=None)
    runtime = Runtime(settings, Secrets({}), store, pipeline_deps, RunManager(lambda trigger, slot: 0), None)
    rows = TestClient(create_app(runtime)).get("/api/macro-events?days=7").json()
    assert {(row["source"], row["title"]) for row in rows} == {
        ("tradingview", "Fed Interest Rate Decision"),
        ("fed", "FOMC Rate Decision"),
    }


def test_failing_cboe_symbol_falls_back_to_ibkr(settings, store) -> None:
    from .conftest import FakeCboe

    seed_news(store)
    market = FakeMarket(SPOTS)
    cboe = FakeCboe(SPOTS, failing={"NVDA"})
    run_id = run_pipeline(_cboe_deps(settings, store, market=market, cboe=cboe), trigger="test", slot="manual")
    run = store.get_run(run_id)
    sources = run["stages"]["options"]["sources"]
    assert sources["ibkr"] == 1 and sources["cboe"] >= 2
    assert market.chain_calls == 1  # only the failing symbol touched IBKR option data
    assert any(i["symbol"] == "NVDA" and i["data_source"] == "ibkr" for i in run["ideas"])


def test_live_session_requotes_final_legs_from_ibkr(settings, store) -> None:
    from .conftest import FakeCboe

    seed_news(store)
    market = FakeMarket(SPOTS)
    market.live_requests = []
    run_id = run_pipeline(_cboe_deps(settings, store, market=market, cboe=FakeCboe(SPOTS)), trigger="test", slot="manual")
    run = store.get_run(run_id)
    live = run["stages"]["options"]["live_check"]
    assert live["status"] == "ok" and live["ideas_checked"] == len(run["ideas"])
    assert len(market.live_requests) == 1  # one request covering all final legs
    first = run["ideas"][0]
    assert first["live_net_premium"] is not None
    assert all("live_bid" in leg for leg in first["legs"])


def test_history_timeout_disables_history_for_the_rest_of_the_run(tmp_path) -> None:
    import time as _time

    from copilot.config import load_settings
    from .conftest import FakeCboe

    settings = load_settings(local_path=tmp_path / "none.toml", overrides={
        "paths": {"data_dir": str(tmp_path / "d"), "secrets_file": str(tmp_path / "k.json")},
        "universe": {"core": ["SPY", "QQQ"], "watchlist": ["SPY", "QQQ", "NVDA", "JPM"]},
        "ibkr": {"history_timeout_seconds": 0.6},
    })
    store = Store(settings.database_path)
    seed_news(store)
    market = FakeMarket(SPOTS)
    calls = []

    def slow_history(symbol, what):
        calls.append(symbol)
        _time.sleep(0.15)
        return []

    market.daily_history = slow_history
    try:
        run_id = run_pipeline(_cboe_deps(settings, store, market=market, cboe=FakeCboe(SPOTS)), trigger="t", slot="manual")
        run = store.get_run(run_id)
        assert len(calls) == 1
        assert run["stages"]["options"]["history"] == "unavailable"
        assert run["ideas"]
    finally:
        store.close()


def test_previous_list_symbols_are_analysed_first() -> None:
    from copilot.analysis.models import View
    from copilot.config import load_settings
    from copilot.pipeline import _analysis_symbols

    settings = load_settings(overrides={"strategy": {"max_symbols_per_run": 3}})
    views = {
        symbol: View(symbol=symbol, direction="bullish", conviction=3, expected_move_pct=0.0,
                     horizon_days=10, score=score)
        for symbol, score in (("AAA", 9.0), ("BBB", 8.0), ("CCC", 2.0), ("SPY", 1.6))
    }
    assert _analysis_symbols(views, {}, settings, need_price=False) == ["SPY", "AAA", "BBB"]
    assert _analysis_symbols(views, {}, settings, need_price=False, keep=["CCC"]) == ["SPY", "CCC", "AAA"]


def test_tracking_market_state_and_web_endpoints(settings, store, monkeypatch) -> None:
    import copilot.pipeline as pipeline
    from .conftest import FakeCboe

    seed_news(store)
    spots = dict(SPOTS)
    cboe = FakeCboe(spots)
    clock = [NOW]
    pipeline_deps = PipelineDeps(
        settings=settings, store=store, news_sources=None, llm=FakeLlm(RULES),
        market_factory=lambda: FakeMarket(spots, unavailable=True), clock=lambda: clock[0],
        option_source=cboe,
    )
    run1 = store.get_run(run_pipeline(pipeline_deps, trigger="test", slot="manual"))
    tracked1 = store.tracked_ideas(limit=1000)
    expected = [idea for idea in run1["ideas"] if not idea["indicative"]]
    assert len(tracked1) == len(expected)
    assert run1["stages"]["tracking"]["opened"] == len(expected)
    assert run1["stages"]["params"]["version"]
    assert {row["symbol"] for row in store.latest_market_states()} >= {"NVDA", "SPY"}

    clock[0] += timedelta(minutes=20)
    run2 = store.get_run(run_pipeline(pipeline_deps, trigger="test", slot="manual"))
    tracked2 = store.tracked_ideas(limit=1000)
    assert len(tracked2) == len(tracked1)
    assert all(row["times_listed"] == 2 for row in tracked2)
    assert run2["stages"]["tracking"]["marked"] == len(tracked1)
    assert all(row["mark"] is not None and row["mark"]["pnl"] <= 0 for row in tracked2)

    spots["NVDA"] *= 1.15
    clock[0] += timedelta(minutes=20)
    run_pipeline(pipeline_deps, trigger="test", slot="manual")
    nvda_closed = [row for row in store.tracked_ideas(status="closed", limit=1000) if row["symbol"] == "NVDA"]
    assert nvda_closed and all(row["exit"]["code"] == "take_profit" for row in nvda_closed)

    runs = RunManager(lambda trigger, slot: 0)
    runtime = Runtime(settings, Secrets({}), store, pipeline_deps, runs, None)
    client = TestClient(create_app(runtime))
    tracking = client.get("/api/tracking")
    assert tracking.status_code == 200 and tracking.json()["ideas"]
    tracked_id = tracking.json()["ideas"][0]["id"]
    assert client.get(f"/api/tracking/{tracked_id}").status_code == 200
    assert client.get("/api/tracking/999999").status_code == 404
    assert client.get("/api/market-state").json()["latest"]
    assert client.get("/api/market-state/SPY").status_code == 200

    original_status = store.get_run(store.latest_run_id())["status"]
    monkeypatch.setattr(pipeline, "_track", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("boom")))
    clock[0] += timedelta(minutes=20)
    failed_tracking = store.get_run(run_pipeline(pipeline_deps, trigger="test", slot="manual"))
    assert failed_tracking["status"] == original_status
    assert failed_tracking["stages"]["tracking"]["status"] == "failed"


def test_weekend_pipeline_skips_market_state_but_runs_tracking(settings, store) -> None:
    from .conftest import FakeCboe

    seed_news(store)
    saturday = datetime(2026, 10, 10, 14, 0, tzinfo=timezone.utc)
    pipeline_deps = PipelineDeps(
        settings=settings,
        store=store,
        news_sources=None,
        llm=FakeLlm(RULES),
        market_factory=lambda: FakeMarket(SPOTS, unavailable=True),
        clock=lambda: saturday,
        option_source=FakeCboe(SPOTS),
    )
    run = store.get_run(run_pipeline(pipeline_deps, trigger="test", slot="manual"))
    assert store.latest_market_states() == []
    assert run["stages"]["market_state"] == {"status": "skipped", "detail": "非交易日，不留存"}
    assert run["stages"]["tracking"]["status"] == "ok"
    assert run["stages"]["tracking"]["opened"] > 0
    assert store.tracked_ideas(limit=None)


def test_tracking_api_limit_does_not_cap_stats(settings, store) -> None:
    run_id = store.create_run(trading_date=NOW.date(), slot="manual", trigger="test")
    for index, pnl in enumerate((10.0, -5.0), start=1):
        tracked_id = store.add_tracked(
            idea_key=f"key-{index}",
            symbol="SPY",
            structure="vertical",
            origin="news",
            params_version="v1",
            entry_run_id=run_id,
            entered_at=NOW.isoformat(),
            entry={
                "idea": {
                    "symbol": "SPY",
                    "structure": "vertical",
                    "legs": [],
                    "max_loss": 100.0,
                    "return_on_risk": 0.05,
                }
            },
        )
        store.close_tracked(tracked_id, {
            "code": "take_profit" if pnl > 0 else "stop_loss",
            "pnl": pnl,
            "return_on_risk": pnl / 100.0,
        })
    runtime = Runtime(
        settings,
        Secrets({}),
        store,
        deps(settings, store, market=None, model=None),
        RunManager(lambda trigger, slot: 0),
        None,
    )
    payload = TestClient(create_app(runtime)).get("/api/tracking?limit=1").json()
    assert len(payload["ideas"]) == 1
    assert payload["stats"]["closed"] == 2
    assert payload["stats"]["pnl_total"] == 5.0


def test_param_failure_finishes_run_as_failed(settings, store, monkeypatch) -> None:
    import copilot.pipeline as pipeline

    monkeypatch.setattr(pipeline, "params_version", lambda _settings: (_ for _ in ()).throw(RuntimeError("bad params")))
    run_id = run_pipeline(deps(settings, store, market=None, model=None), trigger="test", slot="manual")
    run = store.get_run(run_id)
    assert run["status"] == "failed"
    assert run["finished_at"] is not None
    assert run["error"] == "RuntimeError: bad params"


def test_tracking_chain_failure_counts_one_skip_reason(settings, store) -> None:
    from copilot.pipeline import _ChainCache, _track

    from .conftest import FakeCboe

    run_id = store.create_run(trading_date=NOW.date(), slot="manual", trigger="test")
    store.add_tracked(
        idea_key="failing-chain",
        symbol="SPY",
        structure="vertical",
        origin="news",
        params_version="v1",
        entry_run_id=run_id,
        entered_at=NOW.isoformat(),
        entry={
            "idea": {
                "symbol": "SPY",
                "legs": [{
                    "expiry": (NOW.date() + timedelta(days=30)).isoformat(),
                    "strike": 600.0,
                    "right": "C",
                    "quantity": 1,
                }],
            }
        },
    )
    stage = _track(
        store,
        run_id=run_id,
        ideas=[],
        chains=_ChainCache(FakeCboe(SPOTS, failing={"SPY"})),
        today=NOW.date(),
        now=NOW,
        slot="manual",
        trigger="test",
        pricing="reference",
        params_version="v1",
        market_states={},
        settings=settings,
    )
    assert stage["skipped"] == {"SPY: CboeError": 1}


def test_tracking_keeps_identical_news_and_macro_ideas_separate(settings, store) -> None:
    from copilot.pipeline import _track

    run_id = store.create_run(trading_date=NOW.date(), slot="event", trigger="event")
    base = {
        "symbol": "SPY",
        "structure": "bull_call_spread",
        "legs": [
            {"expiry": (NOW.date() + timedelta(days=30)).isoformat(), "strike": 600.0,
             "right": "C", "signed_quantity": 1},
            {"expiry": (NOW.date() + timedelta(days=30)).isoformat(), "strike": 610.0,
             "right": "C", "signed_quantity": -1},
        ],
    }

    stage = _track(
        store,
        run_id=run_id,
        ideas=[{**base, "origin": "news"}, {**base, "origin": "macro"}],
        chains=None,
        today=NOW.date(),
        now=NOW,
        slot="event",
        trigger="event",
        pricing="reference",
        params_version="v1",
        market_states={},
        settings=settings,
    )

    rows = store.open_tracked()
    assert stage["opened"] == 2
    assert {row["origin"] for row in rows} == {"news", "macro"}
    keys = {row["origin"]: row["idea_key"] for row in rows}
    assert keys["news"].startswith("[")
    assert keys["macro"].startswith("macro|[")


def _seed_event_study(store: Store) -> None:
    events = []
    surprises = (-6, -5, -4, -3, -2, -1, 1, 2, 3, 4, 5, 6)
    for index, surprise in enumerate(surprises):
        event_time = NOW - timedelta(days=(len(surprises) - 1 - index) * 28)
        if index == len(surprises) - 1:
            event_time = NOW - timedelta(minutes=30)
        events.append(MacroEvent(
            source="tradingview",
            source_id=f"cpi-history-{index}",
            event_time=event_time,
            country="US",
            title="Core Inflation Rate MoM",
            category="cpi",
            importance=3,
            actual=str(surprise),
            forecast="0",
        ))
    store.replace_macro_events(
        "tradingview",
        NOW - timedelta(days=400),
        NOW + timedelta(days=1),
        events,
    )
    store.replace_event_stats([{
        "category": "cpi",
        "symbol": "SPY",
        "n": 18,
        "last_event_date": NOW.date().isoformat(),
        "event_abs_move": 0.018,
        "normal_abs_move": 0.009,
        "abs_move_ratio": 2.0,
        "surprise_std": 3.0,
        "after_label": "开盘后",
        "mean_trailing_vol": 0.01,
        "beta_gap": 0.2,
        "se_gap": 0.05,
        "t_gap": 4.0,
        "beta_shrunk_gap": 0.15,
        "beta_after": 1.2,
        "se_after": 0.3,
        "t_after": 4.0,
        "beta_shrunk_after": 0.9,
    }])


def test_event_study_is_separate_and_main_top10_is_identical(settings, tmp_path) -> None:
    from .conftest import FakeCboe

    enabled_settings = replace(settings, data_dir=tmp_path / "enabled")
    disabled_settings = replace(
        settings,
        data_dir=tmp_path / "disabled",
        events=replace(settings.events, enabled=False),
    )
    enabled_store = Store(enabled_settings.database_path)
    disabled_store = Store(disabled_settings.database_path)
    try:
        for target in (enabled_store, disabled_store):
            seed_news(target)
            _seed_event_study(target)
        reason = {
            "kind": "release",
            "title": "美国 CPI 公布",
            "category": "cpi",
            "news_ids": [],
            "at": NOW.isoformat(),
        }
        enabled_run = enabled_store.get_run(run_pipeline(
            _cboe_deps(
                enabled_settings,
                enabled_store,
                market=FakeMarket(SPOTS, unavailable=True),
                cboe=FakeCboe(SPOTS),
            ),
            trigger="event",
            slot="event",
            reason=reason,
        ))
        disabled_run = disabled_store.get_run(run_pipeline(
            _cboe_deps(
                disabled_settings,
                disabled_store,
                market=FakeMarket(SPOTS, unavailable=True),
                cboe=FakeCboe(SPOTS),
            ),
            trigger="event",
            slot="event",
            reason=reason,
        ))

        assert json.dumps(enabled_run["ideas"], sort_keys=True) == json.dumps(
            disabled_run["ideas"], sort_keys=True
        )
        event_ideas = enabled_run["snapshots"]["event_ideas"]["list"]
        assert event_ideas
        assert all(idea["origin"] == "macro" for idea in event_ideas)
        assert enabled_run["stages"]["event"] == reason
        assert enabled_run["stages"]["event_study"]["ideas"] == len(event_ideas)
        tracked = enabled_store.tracked_ideas(limit=None)
        assert any(row["origin"] == "macro" for row in tracked)

        runtime = Runtime(
            enabled_settings,
            Secrets({}),
            enabled_store,
            _cboe_deps(enabled_settings, enabled_store, market=None, cboe=FakeCboe(SPOTS)),
            RunManager(lambda trigger, slot: 0),
            None,
        )
        client = TestClient(create_app(runtime))
        assert client.get("/api/event-study?category=cpi").json()[0]["symbol"] == "SPY"
        latest = client.get("/api/event-study/latest")
        assert latest.status_code == 200 and latest.json()["event"]["category"] == "cpi"
        event_status = client.get("/api/status").json()["latest_event_run"]
        assert event_status["id"] == enabled_run["id"]
        assert event_status["reason"] == reason
        assert event_status["event_ideas"] == len(event_ideas)
    finally:
        enabled_store.close()
        disabled_store.close()
