"""Settings loaded from config/copilot.toml with an optional local override file."""
from __future__ import annotations

import ipaddress
import os
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "copilot.toml"
LOCAL_CONFIG_PATH = PROJECT_ROOT / "config" / "copilot.local.toml"


@dataclass(frozen=True)
class IbkrSettings:
    host: str = "127.0.0.1"
    port: int = 4001
    client_id: int = 31
    connect_timeout_seconds: float = 15.0
    market_data_type: int = 4
    quote_wait_seconds: float = 6.0
    max_concurrent_quotes: int = 80
    # Option subscriptions per batch. With the market closed IB answers only part of a
    # large batch (80 at once: 0-11 of 80 got greeks; batches of 20: 56 of 80).
    option_batch_size: int = 20
    # Courtesy pause between symbols so bursts never pile up inside IB Gateway.
    symbol_pause_seconds: float = 1.0
    # IB's historical farm can be down for hours (error 2105); give up quickly.
    history_timeout_seconds: float = 12.0


@dataclass(frozen=True)
class NewsSettings:
    lookback_hours: int = 24
    jin10_max_items: int = 200
    finnhub_company_news_symbols: int = 30
    finnhub_items_per_symbol: int = 5
    earnings_days_ahead: int = 10


@dataclass(frozen=True)
class OptionsSettings:
    # "cboe": free CBOE delayed chains (about 15 min delayed in session, closing quotes
    # otherwise), falling back to IBKR per symbol; "ibkr": IB Gateway quotes only.
    source: str = "cboe"
    cboe_timeout_seconds: float = 30.0
    # During regular hours, re-quote the final ideas' legs from IBKR in real time.
    live_refresh: bool = True


@dataclass(frozen=True)
class LlmSettings:
    enabled: bool = True
    model: str = "deepseek-v4-flash"
    max_news_items: int = 220
    timeout_seconds: float = 150.0
    max_output_tokens: int = 8000
    # News classification: items per model call and parallel calls.
    tag_batch_size: int = 40
    tag_workers: int = 4
    max_tags_per_run: int = 600


@dataclass(frozen=True)
class ViewsSettings:
    """View ledger: per-symbol evidence scores from tagged news, with hysteresis."""

    lookback_days: int = 10
    # |score| needed to open (or reverse) a directional view, and below which it closes.
    open_threshold: float = 1.5
    close_threshold: float = 0.75
    vol_open_threshold: float = 3.0
    vol_close_threshold: float = 1.5
    # Expected move in standard deviations per score point (the builder caps it).
    sigma_per_point: float = 0.08
    importance_weights: tuple[float, ...] = (0.0, 0.5, 1.0, 2.0)
    # Evidence half-life in days by the horizon the tag gave.
    half_life_intraday: float = 0.5
    half_life_days: float = 3.0
    half_life_weeks: float = 10.0
    max_open_views: int = 30


@dataclass(frozen=True)
class StrategySettings:
    min_dte: int = 5
    max_dte: int = 60
    expiries_per_symbol: int = 2
    max_symbols_per_run: int = 15
    min_open_interest: int = 50
    max_leg_spread_pct: float = 0.30
    fill_ratio: float = 0.5
    commission_per_contract: float = 0.70
    max_ideas: int = 10
    max_ideas_per_symbol: int = 2
    risk_per_trade_pct: float = 0.10
    # A new idea replaces one kept from the previous list only if it scores this much better.
    keep_margin: float = 0.25
    # Ideas whose max loss exceeds this share of net liquidation are dropped (0 = keep all).
    max_loss_cap_pct: float = 0.20
    # A view may shift the expected price by at most this many standard deviations of the
    # move over its horizon; keeps an over-confident model from inflating the edge.
    max_drift_sigma: float = 0.4
    # Never open a short-premium structure (credit spreads, condors, calendars, diagonals)
    # whose short leg spans an earnings date - the reference account's worst week.
    no_short_premium_through_earnings: bool = True
    tail_weight: float = 0.15
    tail_vol_multiple: float = 1.6
    core_vol_multiple: float = 0.88


@dataclass(frozen=True)
class RulesSettings:
    """Rules learned from the reference account: index ladder, portfolio cap, exits."""

    # Index drawdown ladder: drawdown from the 52-week closing high.
    ladder_symbols: tuple[str, ...] = ("SPY", "QQQ", "IWM")
    ladder_tiers: tuple[float, ...] = (0.05, 0.10, 0.15)
    # Mean reversion assumed at each tier, in standard deviations of the horizon move.
    ladder_drift_sigma: tuple[float, ...] = (0.10, 0.20, 0.30)
    # Worst-case loss of open option positions plus a new idea, as a share of net liquidation.
    portfolio_risk_cap_pct: float = 0.20
    # Exit alerts.
    take_profit_credit_pct: float = 0.50
    take_profit_debit_pct: float = 0.75
    take_profit_long_option_pct: float = 1.00
    stop_loss_debit_pct: float = 0.50
    stop_loss_credit_multiple: float = 2.0
    time_exit_dte: int = 7
    short_premium_time_exit_dte: int = 21


@dataclass(frozen=True)
class ScheduleSettings:
    enabled: bool = True
    run_times_et: tuple[str, ...] = ("09:00", "09:45", "12:30", "15:00")
    retry_minutes: int = 15
    max_attempts_per_slot: int = 4


@dataclass(frozen=True)
class WebSettings:
    host: str = "127.0.0.1"
    port: int = 8891


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    secrets_file: Path
    core_symbols: tuple[str, ...]
    watchlist: tuple[str, ...]
    ibkr: IbkrSettings
    options: OptionsSettings
    news: NewsSettings
    llm: LlmSettings
    views: ViewsSettings
    strategy: StrategySettings
    rules: RulesSettings
    schedule: ScheduleSettings
    web: WebSettings

    @property
    def database_path(self) -> Path:
        return self.data_dir / "copilot.sqlite3"

    @property
    def log_dir(self) -> Path:
        return self.data_dir / "logs"

    @property
    def universe(self) -> tuple[str, ...]:
        seen: dict[str, None] = {}
        for symbol in (*self.core_symbols, *self.watchlist):
            seen.setdefault(symbol, None)
        return tuple(seen)


def load_settings(
    path: Path | None = None,
    *,
    local_path: Path | None = None,
    overrides: dict[str, Any] | None = None,
) -> Settings:
    """Load defaults, merge the local override file, then explicit overrides."""

    raw = _read_toml(path or DEFAULT_CONFIG_PATH)
    local = local_path if local_path is not None else LOCAL_CONFIG_PATH
    if local.exists():
        raw = _merge(raw, _read_toml(local))
    if overrides:
        raw = _merge(raw, overrides)
    env_data_dir = os.environ.get("COPILOT_DATA_DIR")
    if env_data_dir:
        raw = _merge(raw, {"paths": {"data_dir": env_data_dir}})
    return _build(raw)


def _build(raw: dict[str, Any]) -> Settings:
    paths = raw.get("paths", {})
    universe = raw.get("universe", {})
    schedule = dict(raw.get("schedule", {}))
    if "run_times_et" in schedule:
        schedule["run_times_et"] = tuple(schedule["run_times_et"])
    settings = Settings(
        data_dir=_resolve(paths.get("data_dir", "data/copilot")),
        secrets_file=_resolve(paths.get("secrets_file", "secrets/api_keys.local.json")),
        core_symbols=_symbols(universe.get("core", ("SPY", "QQQ", "IWM"))),
        watchlist=_symbols(universe.get("watchlist", ())),
        ibkr=IbkrSettings(**raw.get("ibkr", {})),
        options=OptionsSettings(**raw.get("options", {})),
        news=NewsSettings(**raw.get("news", {})),
        llm=LlmSettings(**raw.get("llm", {})),
        views=_views_settings(raw.get("views", {})),
        strategy=StrategySettings(**raw.get("strategy", {})),
        rules=_rules_settings(raw.get("rules", {})),
        schedule=ScheduleSettings(**schedule),
        web=WebSettings(**raw.get("web", {})),
    )
    _validate(settings)
    return settings


def _validate(settings: Settings) -> None:
    # The GUI/API has no authentication, so it must never listen beyond loopback.
    if not ipaddress.ip_address(settings.web.host).is_loopback:
        raise ValueError("web.host must be a loopback address")
    if settings.options.source not in ("cboe", "ibkr"):
        raise ValueError("options.source must be cboe or ibkr")
    if settings.ibkr.market_data_type not in (1, 2, 3, 4):
        raise ValueError("ibkr.market_data_type must be 1-4")
    if not settings.core_symbols:
        raise ValueError("universe.core must not be empty")
    strategy = settings.strategy
    if not 0 < strategy.min_dte <= strategy.max_dte:
        raise ValueError("strategy DTE window is invalid")
    if not 0 <= strategy.fill_ratio <= 1:
        raise ValueError("strategy.fill_ratio must be between 0 and 1")
    rules = settings.rules
    if len(rules.ladder_tiers) != len(rules.ladder_drift_sigma) or list(rules.ladder_tiers) != sorted(rules.ladder_tiers):
        raise ValueError("rules: ladder_tiers must be ascending and match ladder_drift_sigma")
    views = settings.views
    if not 0 < views.close_threshold < views.open_threshold:
        raise ValueError("views: need 0 < close_threshold < open_threshold")
    if len(views.importance_weights) != 4:
        raise ValueError("views.importance_weights needs 4 values (importance 0-3)")
    if not 0 <= strategy.tail_weight < 1:
        raise ValueError("strategy.tail_weight must be in [0, 1)")
    for moment in settings.schedule.run_times_et:
        hour, minute = (int(part) for part in moment.split(":"))
        if not (0 <= hour < 24 and 0 <= minute < 60):
            raise ValueError(f"invalid schedule time: {moment}")


def _rules_settings(raw: dict[str, Any]) -> RulesSettings:
    values = dict(raw)
    if "ladder_symbols" in values:
        values["ladder_symbols"] = _symbols(values["ladder_symbols"])
    for key in ("ladder_tiers", "ladder_drift_sigma"):
        if key in values:
            values[key] = tuple(float(v) for v in values[key])
    return RulesSettings(**values)


def _views_settings(raw: dict[str, Any]) -> ViewsSettings:
    values = dict(raw)
    if "importance_weights" in values:
        values["importance_weights"] = tuple(float(w) for w in values["importance_weights"])
    return ViewsSettings(**values)


def _symbols(values: Any) -> tuple[str, ...]:
    result: dict[str, None] = {}
    for value in values:
        symbol = str(value).strip().upper()
        if symbol:
            result.setdefault(symbol, None)
    return tuple(result)


def _resolve(value: str | os.PathLike[str]) -> Path:
    path = Path(value)
    return path if path.is_absolute() else PROJECT_ROOT / path


def _read_toml(path: Path) -> dict[str, Any]:
    with path.open("rb") as handle:
        return tomllib.load(handle)


def _merge(base: dict[str, Any], extra: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in extra.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _merge(merged[key], value)
        else:
            merged[key] = value
    return merged


__all__ = [
    "IbkrSettings",
    "LlmSettings",
    "NewsSettings",
    "OptionsSettings",
    "RulesSettings",
    "ScheduleSettings",
    "Settings",
    "StrategySettings",
    "ViewsSettings",
    "WebSettings",
    "load_settings",
]
