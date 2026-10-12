"""Composition root: build settings, store, pipeline dependencies, runner and scheduler."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler

from copilot.analysis.deepseek import DeepSeekClient
from copilot.config import Settings, load_settings
from copilot.market.cboe import CboeClient
from copilot.market.ibkr import IbkrMarket
from copilot.market.nasdaq_history import daily_closes
from copilot.news.collect import NewsSources, build_sources, collect_news
from copilot.pipeline import PipelineDeps, run_pipeline, tag_pending_news
from copilot.scheduler import RunManager, Scheduler
from copilot.secrets import Secrets, load_secrets
from copilot.store import Store

log = logging.getLogger(__name__)


@dataclass
class Runtime:
    settings: Settings
    secrets: Secrets
    store: Store
    deps: PipelineDeps
    runs: RunManager
    scheduler: Scheduler | None

    def poll_news(self) -> None:
        if self.deps.news_sources is None:
            return
        now = datetime.now(timezone.utc)
        collect_news(self.settings, self.store, self.deps.news_sources, now=now, include_calendar=False)
        # Classify as news arrives so the feed shows tags and runs have little backlog.
        if self.settings.llm.enabled and self.deps.llm is not None:
            tag_pending_news(
                self.store, self.deps.llm, self.settings,
                since=now - timedelta(days=self.settings.views.lookback_days),
            )

    def close(self) -> None:
        if self.scheduler is not None:
            self.scheduler.stop()
        self.store.close()


def make_deps(
    settings: Settings,
    secrets: Secrets,
    store: Store,
    *,
    use_ibkr: bool = True,
    use_llm: bool = True,
    use_news: bool = True,
) -> PipelineDeps:
    key = secrets.get("deepseek_api_key")
    model = (
        DeepSeekClient(key, model=settings.llm.model, timeout=settings.llm.timeout_seconds)
        if use_llm and settings.llm.enabled and key
        else None
    )
    sources: NewsSources | None = build_sources(secrets) if use_news else None
    return PipelineDeps(
        settings=settings,
        store=store,
        news_sources=sources,
        llm=model,
        market_factory=(lambda: IbkrMarket(settings.ibkr)) if use_ibkr else None,
        option_source=(
            CboeClient(timeout=settings.options.cboe_timeout_seconds)
            if settings.options.source == "cboe" else None
        ),
        index_history=lambda symbol, today: daily_closes(symbol, today=today),
    )


def build_runtime(
    settings: Settings | None = None,
    *,
    with_scheduler: bool = True,
    use_ibkr: bool = True,
    use_llm: bool = True,
) -> Runtime:
    settings = settings or load_settings()
    configure_logging(settings)
    secrets = load_secrets(settings.secrets_file)
    store = Store(settings.database_path)
    interrupted = store.fail_interrupted_runs()
    if interrupted:
        log.warning("closed %s interrupted run(s)", interrupted)
    if use_ibkr:
        # Import ib_insync once on the main thread so its import-time event-loop lookup
        # never happens first on a run thread (scheduler or the GUI "run now" button).
        import ib_insync  # noqa: F401
    deps = make_deps(settings, secrets, store, use_ibkr=use_ibkr, use_llm=use_llm)
    runs = RunManager(lambda trigger, slot: run_pipeline(deps, trigger=trigger, slot=slot))
    runtime = Runtime(settings, secrets, store, deps, runs, None)
    if with_scheduler:
        runtime.scheduler = Scheduler(settings.schedule, store, runs, poll_news=runtime.poll_news)
    return runtime


def configure_logging(settings: Settings) -> None:
    root = logging.getLogger()
    if any(getattr(handler, "_copilot", False) for handler in root.handlers):
        return
    settings.log_dir.mkdir(parents=True, exist_ok=True)
    handler = RotatingFileHandler(
        settings.log_dir / "copilot.log", maxBytes=5_000_000, backupCount=5, encoding="utf-8"
    )
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    handler._copilot = True  # type: ignore[attr-defined]
    root.addHandler(handler)
    root.setLevel(logging.INFO)


__all__ = ["Runtime", "build_runtime", "configure_logging", "make_deps"]
