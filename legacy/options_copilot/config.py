"""Configuration for the isolated Options Copilot runtime.

Secret values intentionally do not appear in this module.  Optional provider
credentials are read from the fixed local-only JSON store at provider
boundaries; the legacy DPAPI path remains available to credential-admin tools.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

DEFAULT_NEWS_CORE_SYMBOLS = (
    "SPY",
    "QQQ",
    "IWM",
    "DIA",
    "AAPL",
    "MSFT",
    "NVDA",
    "AMZN",
    "META",
    "GOOGL",
    "TSLA",
    "AMD",
    "AVGO",
    "JPM",
    "XOM",
    "GLD",
    "TLT",
    "SMH",
    "XLK",
    "XLF",
    "XLE",
)


def _env_int(name: str, default: int, *, minimum: int, maximum: int) -> int:
    raw = os.getenv(name)
    value = default if raw is None or not raw.strip() else int(raw)
    if not minimum <= value <= maximum:
        raise ValueError(f"{name} must be between {minimum} and {maximum}")
    return value


def _env_float(name: str, default: float, *, minimum: float, maximum: float) -> float:
    raw = os.getenv(name)
    value = default if raw is None or not raw.strip() else float(raw)
    if not minimum <= value <= maximum:
        raise ValueError(f"{name} must be between {minimum} and {maximum}")
    return value


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be a boolean")


def _env_path(name: str, default: Path) -> Path:
    raw = os.getenv(name)
    return Path(raw).expanduser().resolve() if raw and raw.strip() else default.resolve()


def _env_optional_path(name: str) -> Path | None:
    raw = os.getenv(name)
    return Path(raw).expanduser().resolve() if raw and raw.strip() else None


def _unique_pacing_authority_dir(data_dir: Path) -> Path | None:
    """Auto-load one installed pacing authority; never guess an ambiguous head."""

    root = data_dir / "evidence" / "checkpoints" / "P0" / "market-data-pacing"
    try:
        candidates = tuple(
            item.resolve()
            for item in root.iterdir()
            if item.is_dir()
            and (item / "capability.json").is_file()
            and (item / "approval.json").is_file()
        )
    except OSError:
        return None
    return candidates[0] if len(candidates) == 1 else None


def _installed_pacing_authority_keyring(data_dir: Path) -> Path | None:
    """Select only the fixed operator-installed public-key trust anchor."""

    path = data_dir / "governance" / "pacing_authority_keyring.json"
    try:
        return path.resolve() if path.is_file() else None
    except OSError:
        return None


@dataclass(frozen=True, slots=True)
class OptionsCopilotConfig:
    data_dir: Path
    log_dir: Path
    host: str = "127.0.0.1"
    port: int = 8891
    ibkr_host: str = "127.0.0.1"
    # IB Gateway live-trading port.  The gateway adapter is permanently
    # read-only even when this points at a live account session.
    ibkr_port: int = 4001
    ibkr_client_id: int = 4817
    ibkr_readonly: bool = True
    ibkr_timeout_seconds: float = 8.0
    control_snapshot_refresh_seconds: float = 5.0
    control_snapshot_stale_seconds: float = 15.0
    quote_fresh_seconds: float = 5.0
    approval_ttl_seconds: int = 300
    adverse_reprice_tolerance_usd: float = 5.0
    normal_risk_fraction: float = 0.10
    a_grade_risk_fraction: float = 0.15
    hard_risk_fraction: float = 0.20
    max_open_combinations: int = 1
    max_hold_trading_days: int = 5
    min_dte: int = 7
    normal_min_dte: int = 14
    normal_max_dte: int = 35
    live_instruction_enabled: bool = False
    news_refresh_seconds: int = 90
    news_llm_enabled: bool = False
    news_core_symbols: tuple[str, ...] = DEFAULT_NEWS_CORE_SYMBOLS
    pacing_authority_dir: Path | None = None
    pacing_authority_keyring_path: Path | None = None
    broker_acquisition_mode: str = "DIRECT"
    external_readonly_feed_path: Path | None = None
    external_top10_path: Path | None = None
    external_session_calendar_path: Path | None = None

    @classmethod
    def from_env(cls) -> "OptionsCopilotConfig":
        data_dir = _env_path(
            "OPTIONS_COPILOT_DATA_DIR",
            ROOT / "data" / "options_copilot",
        )
        log_dir = _env_path(
            "OPTIONS_COPILOT_LOG_DIR",
            ROOT / "logs" / "options_copilot",
        )
        pacing_authority_dir = _env_optional_path(
            "OPTIONS_COPILOT_PACING_AUTHORITY_DIR"
        ) or _unique_pacing_authority_dir(data_dir)
        external_readonly_feed_path = _env_optional_path(
            "OPTIONS_COPILOT_EXTERNAL_READONLY_FEED_PATH"
        )
        external_top10_path = _env_optional_path(
            "OPTIONS_COPILOT_EXTERNAL_TOP10_PATH"
        )
        external_session_calendar_path = _env_optional_path(
            "OPTIONS_COPILOT_EXTERNAL_SESSION_CALENDAR_PATH"
        )
        return cls(
            data_dir=data_dir,
            log_dir=log_dir,
            host=os.getenv("OPTIONS_COPILOT_HOST", "127.0.0.1").strip(),
            port=_env_int("OPTIONS_COPILOT_PORT", 8891, minimum=1024, maximum=65535),
            ibkr_host=os.getenv("OPTIONS_COPILOT_IBKR_HOST", "127.0.0.1").strip(),
            ibkr_port=_env_int(
                "OPTIONS_COPILOT_IBKR_PORT", 4001, minimum=1, maximum=65535
            ),
            ibkr_client_id=_env_int(
                "OPTIONS_COPILOT_IBKR_CLIENT_ID", 4817, minimum=1, maximum=999999
            ),
            # This is a protocol constant, not an environment-controlled
            # switch.  Orders remain outside this gateway's authority.
            ibkr_readonly=True,
            # These are protocol constants, not tuning knobs.  One bounded
            # eight-second IBKR call is the outer limit for each supervisor
            # connection attempt.  Control state refreshes every five seconds
            # and becomes stale at fifteen; executable quotes retain their
            # independent hard five-second gate below.
            ibkr_timeout_seconds=8.0,
            control_snapshot_refresh_seconds=5.0,
            control_snapshot_stale_seconds=15.0,
            quote_fresh_seconds=5.0,
            approval_ttl_seconds=300,
            adverse_reprice_tolerance_usd=5.0,
            live_instruction_enabled=False,
            news_refresh_seconds=_env_int(
                "OPTIONS_COPILOT_NEWS_REFRESH_SECONDS",
                90,
                minimum=60,
                maximum=120,
            ),
            news_llm_enabled=_env_bool(
                "OPTIONS_COPILOT_NEWS_LLM_ENABLED",
                False,
            ),
            pacing_authority_dir=pacing_authority_dir,
            pacing_authority_keyring_path=_installed_pacing_authority_keyring(
                data_dir
            ),
            broker_acquisition_mode=os.getenv(
                "OPTIONS_COPILOT_BROKER_ACQUISITION_MODE",
                "DIRECT",
            ).strip().upper(),
            external_readonly_feed_path=external_readonly_feed_path,
            external_top10_path=external_top10_path,
            external_session_calendar_path=external_session_calendar_path,
        )

    @property
    def database_path(self) -> Path:
        return self.data_dir / "options_copilot.sqlite3"

    @property
    def secrets_path(self) -> Path:
        return self.data_dir / "secrets.dpapi.json"

    @property
    def news_evidence_path(self) -> Path:
        return self.data_dir / "news_evidence.sqlite3"

    @property
    def news_cadence_path(self) -> Path:
        """Local atomic state for per-source refresh lanes."""

        return self.data_dir / "news_source_cadence.json"

    def ensure_runtime_directories(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)

    def validate(self) -> None:
        if not self.host or not self.ibkr_host:
            raise ValueError("host values cannot be blank")
        if self.ibkr_readonly is not True:
            raise ValueError("IBKR Gateway access is permanently read-only")
        if self.ibkr_timeout_seconds != 8.0:
            raise ValueError("IBKR connection timeout is locked to eight seconds")
        if self.control_snapshot_refresh_seconds != 5.0:
            raise ValueError("control snapshot refresh is locked to five seconds")
        if self.control_snapshot_stale_seconds != 15.0:
            raise ValueError("control snapshot staleness is locked to fifteen seconds")
        if not (
            0 < self.normal_risk_fraction
            <= self.a_grade_risk_fraction
            < self.hard_risk_fraction
            <= 0.20
        ):
            raise ValueError("risk fractions must preserve 10/15/20 ceiling order")
        if self.max_open_combinations != 1:
            raise ValueError("the locked portfolio policy permits exactly one combination")
        if self.min_dte < 7 or self.normal_min_dte < self.min_dte:
            raise ValueError("0-3 DTE and sub-7-DTE structures are prohibited")
        if self.normal_max_dte < self.normal_min_dte:
            raise ValueError("invalid normal DTE range")
        if self.live_instruction_enabled:
            raise ValueError(
                "live instruction mode cannot be enabled from environment configuration"
            )
        if self.quote_fresh_seconds != 5.0:
            raise ValueError("quote freshness is locked to five seconds")
        if self.approval_ttl_seconds != 300:
            raise ValueError("proposal approvals are locked to five minutes")
        if self.adverse_reprice_tolerance_usd != 5.0:
            raise ValueError("adverse repricing tolerance is locked to 5 USD")
        if isinstance(self.news_refresh_seconds, bool) or not 60 <= self.news_refresh_seconds <= 120:
            raise ValueError("news refresh must remain between 60 and 120 seconds")
        if not isinstance(self.news_llm_enabled, bool):
            raise TypeError("news_llm_enabled must be a bool")
        if self.pacing_authority_dir is not None and not isinstance(
            self.pacing_authority_dir, Path
        ):
            raise TypeError("pacing_authority_dir must be a pathlib.Path or None")
        if self.pacing_authority_keyring_path is not None and not isinstance(
            self.pacing_authority_keyring_path,
            Path,
        ):
            raise TypeError(
                "pacing_authority_keyring_path must be a pathlib.Path or None"
            )
        if self.broker_acquisition_mode not in {"DIRECT", "EXTERNAL"}:
            raise ValueError("broker acquisition mode must be DIRECT or EXTERNAL")
        if (self.external_readonly_feed_path is None) != (
            self.external_top10_path is None
        ):
            raise ValueError(
                "external read-only feed and Top-10 paths must be configured together"
            )
        for name, value in (
            ("external_readonly_feed_path", self.external_readonly_feed_path),
            ("external_top10_path", self.external_top10_path),
            (
                "external_session_calendar_path",
                self.external_session_calendar_path,
            ),
        ):
            if value is not None and not isinstance(value, Path):
                raise TypeError(f"{name} must be a pathlib.Path or None")
        external_paths_configured = self.external_readonly_feed_path is not None
        if self.broker_acquisition_mode == "EXTERNAL" and not external_paths_configured:
            raise ValueError("EXTERNAL acquisition requires both external paths")
        if (
            self.external_session_calendar_path is not None
            and not external_paths_configured
        ):
            raise ValueError(
                "external session calendar requires the external feed and Top-10 pair"
            )
        if self.broker_acquisition_mode == "DIRECT" and (
            external_paths_configured
            or self.external_session_calendar_path is not None
        ):
            raise ValueError(
                "external paths cannot be configured while DIRECT acquisition is active"
            )
        if not self.news_core_symbols or len(set(self.news_core_symbols)) != len(
            self.news_core_symbols
        ):
            raise ValueError("news core symbols must be nonempty and unique")
        for symbol in self.news_core_symbols:
            if (
                not isinstance(symbol, str)
                or not symbol
                or symbol != symbol.upper()
                or len(symbol) > 12
                or not symbol.replace(".", "").isalnum()
            ):
                raise ValueError("news core symbols contain an invalid symbol")
