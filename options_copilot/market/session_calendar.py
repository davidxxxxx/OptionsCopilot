"""Authoritative normalization of broker-published US option sessions."""
from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
from enum import Enum
import re
import threading
from typing import Protocol
from zoneinfo import ZoneInfo

from options_copilot.gateway import MarketDataPacingError
from options_copilot.gateway.ibkr_readonly import (
    BrokerConnectionError,
    SessionCalendarReadError,
)
from options_copilot.storage.canonical import canonical_hash, datetime_text, utc_datetime


US_OPTIONS_TIMEZONE = ZoneInfo("America/New_York")
DEFAULT_MAXIMUM_CALENDAR_AGE_SECONDS = Decimal("5")
_TIMEZONE_ALIASES = frozenset(
    {"America/New_York", "US/Eastern", "EST5EDT"}
)
_TRUSTED_SOURCES = frozenset({"IBKR_REQ_CONTRACT_DETAILS_READONLY"})
_INTERVAL_RE = re.compile(
    r"(?:(?P<start_date>[0-9]{8}):)?(?P<start>[0-9]{4})-"
    r"(?:(?P<end_date>[0-9]{8}):)?(?P<end>[0-9]{4})\Z"
)


class CalendarStatus(str, Enum):
    READY = "READY"
    DEGRADED = "DEGRADED"


@dataclass(frozen=True, slots=True)
class UsOptionsSession:
    trading_date: date
    open_et: datetime
    close_et: datetime
    open_utc: datetime
    close_utc: datetime
    early_close: bool

    def __post_init__(self) -> None:
        for name in ("open_et", "close_et", "open_utc", "close_utc"):
            value = getattr(self, name)
            if not isinstance(value, datetime) or value.tzinfo is None:
                raise TypeError(f"{name} must be timezone-aware")
        if self.open_et.tzinfo != US_OPTIONS_TIMEZONE:
            raise ValueError("open_et must use America/New_York")
        if self.close_et.tzinfo != US_OPTIONS_TIMEZONE:
            raise ValueError("close_et must use America/New_York")
        if self.open_et.date() != self.trading_date:
            raise ValueError("session open date does not match trading_date")
        if self.close_et.date() != self.trading_date:
            raise ValueError("US option session must close on its trading date")
        if self.close_et <= self.open_et:
            raise ValueError("session close must follow open")
        if self.open_utc != self.open_et.astimezone(timezone.utc):
            raise ValueError("open UTC evidence does not match ET session")
        if self.close_utc != self.close_et.astimezone(timezone.utc):
            raise ValueError("close UTC evidence does not match ET session")
        expected_early = self.close_et.timetz().replace(tzinfo=None) == time(13, 0)
        if self.early_close is not expected_early:
            raise ValueError("early_close does not match the published close")

    def contains(self, instant: datetime) -> bool:
        checked = utc_datetime(instant, field="instant")
        return self.open_utc <= checked < self.close_utc

    def as_dict(self) -> dict[str, object]:
        return {
            "trading_date": self.trading_date,
            "open_et": self.open_et,
            "close_et": self.close_et,
            "open_utc": self.open_utc,
            "close_utc": self.close_utc,
            "early_close": self.early_close,
        }


@dataclass(frozen=True, slots=True)
class UsOptionsCalendarSnapshot:
    status: CalendarStatus
    reason_codes: tuple[str, ...]
    observed_at: datetime
    normalized_at: datetime
    source: str
    broker_timezone_id: str
    normalized_timezone_id: str
    liquid_hours: str
    trading_hours: str
    source_hash: str
    sessions: tuple[UsOptionsSession, ...]
    closed_dates: tuple[date, ...]
    calendar_hash: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "observed_at",
            utc_datetime(self.observed_at, field="observed_at"),
        )
        object.__setattr__(
            self,
            "normalized_at",
            utc_datetime(self.normalized_at, field="normalized_at"),
        )
        object.__setattr__(self, "reason_codes", tuple(sorted(set(self.reason_codes))))
        object.__setattr__(
            self,
            "sessions",
            tuple(sorted(self.sessions, key=lambda item: item.trading_date)),
        )
        object.__setattr__(self, "closed_dates", tuple(sorted(set(self.closed_dates))))

    @property
    def entry_eligible(self) -> bool:
        return bool(
            self.status is CalendarStatus.READY
            and self.session_at(self.observed_at) is not None
        )

    def session_for(self, trading_date: date) -> UsOptionsSession | None:
        if not isinstance(trading_date, date) or isinstance(trading_date, datetime):
            raise TypeError("trading_date must be a date")
        return next(
            (item for item in self.sessions if item.trading_date == trading_date),
            None,
        )

    def session_at(self, instant: datetime) -> UsOptionsSession | None:
        checked = utc_datetime(instant, field="instant")
        if self.status is not CalendarStatus.READY:
            return None
        return next((item for item in self.sessions if item.contains(checked)), None)

    def hash_payload(self) -> dict[str, object]:
        return {
            "status": self.status.value,
            "reason_codes": self.reason_codes,
            "observed_at": self.observed_at,
            "normalized_at": self.normalized_at,
            "source": self.source,
            "broker_timezone_id": self.broker_timezone_id,
            "normalized_timezone_id": self.normalized_timezone_id,
            "liquid_hours": self.liquid_hours,
            "trading_hours": self.trading_hours,
            "source_hash": self.source_hash,
            "sessions": [item.as_dict() for item in self.sessions],
            "closed_dates": self.closed_dates,
        }

    def verify_hash(self) -> bool:
        return canonical_hash(self.hash_payload()) == self.calendar_hash

    def as_dict(self) -> dict[str, object]:
        return {
            **self.hash_payload(),
            "observed_at": datetime_text(self.observed_at),
            "normalized_at": datetime_text(self.normalized_at),
            "sessions": [
                {
                    **item.as_dict(),
                    "trading_date": item.trading_date.isoformat(),
                    "open_et": item.open_et.isoformat(),
                    "close_et": item.close_et.isoformat(),
                    "open_utc": datetime_text(item.open_utc),
                    "close_utc": datetime_text(item.close_utc),
                }
                for item in self.sessions
            ],
            "closed_dates": [item.isoformat() for item in self.closed_dates],
            "calendar_hash": self.calendar_hash,
            "entry_eligible": self.entry_eligible,
        }


class UsOptionsSessionCalendar:
    """Convert one fresh IBKR hours document into immutable ET/UTC evidence."""

    def __init__(
        self,
        *,
        maximum_age_seconds: Decimal = DEFAULT_MAXIMUM_CALENDAR_AGE_SECONDS,
    ) -> None:
        self.maximum_age_seconds = _nonnegative_decimal(
            maximum_age_seconds,
            "maximum_age_seconds",
        )

    def normalize(
        self,
        *,
        liquid_hours: object,
        trading_hours: object,
        timezone_id: object,
        observed_at: object,
        source: object,
        now: datetime,
    ) -> UsOptionsCalendarSnapshot:
        checked_now = utc_datetime(now, field="now")
        observed_invalid = False
        try:
            checked_observed = utc_datetime(observed_at, field="observed_at")
        except (TypeError, ValueError):
            checked_observed = checked_now
            observed_invalid = True
        liquid_text = liquid_hours.strip() if isinstance(liquid_hours, str) else ""
        trading_text = trading_hours.strip() if isinstance(trading_hours, str) else ""
        timezone_text = timezone_id.strip() if isinstance(timezone_id, str) else ""
        source_text = source.strip() if isinstance(source, str) else ""
        raw_payload = {
            "schema": "options_copilot.market.ibkr_session_source.v1",
            "liquid_hours": liquid_text,
            "trading_hours": trading_text,
            "timezone_id": timezone_text,
            "observed_at": None if observed_invalid else checked_observed,
            "source": source_text,
        }
        source_hash = canonical_hash(raw_payload)
        reasons: set[str] = set()
        if observed_invalid:
            reasons.add("CALENDAR_OBSERVED_AT_INVALID")
        if not liquid_text or not trading_text:
            reasons.add("CALENDAR_HOURS_MISSING")
        if timezone_text not in _TIMEZONE_ALIASES:
            reasons.add("CALENDAR_TIMEZONE_UNSUPPORTED")
        if not source_text:
            reasons.add("CALENDAR_SOURCE_MISSING")
        elif source_text not in _TRUSTED_SOURCES:
            reasons.add("CALENDAR_SOURCE_UNTRUSTED")
        age = Decimal(str((checked_now - checked_observed).total_seconds()))
        if age < 0:
            reasons.add("CALENDAR_OBSERVED_IN_FUTURE")
        elif age > self.maximum_age_seconds:
            reasons.add("CALENDAR_STALE")

        liquid: dict[date, tuple[datetime, datetime] | None] = {}
        trading: dict[date, tuple[datetime, datetime] | None] = {}
        if liquid_text and timezone_text in _TIMEZONE_ALIASES:
            liquid, liquid_reasons = _parse_hours(liquid_text)
            reasons.update(liquid_reasons)
        if trading_text and timezone_text in _TIMEZONE_ALIASES:
            trading, trading_reasons = _parse_hours(trading_text)
            reasons.update(trading_reasons)
        if (
            liquid
            and trading
            and not _trading_hours_cover_liquid_hours(liquid, trading)
        ):
            reasons.add("LIQUID_TRADING_HOURS_MISMATCH")

        current_et_date = checked_now.astimezone(US_OPTIONS_TIMEZONE).date()
        if current_et_date not in liquid or current_et_date not in trading:
            reasons.add("CALENDAR_CURRENT_DATE_UNCOVERED")

        sessions: list[UsOptionsSession] = []
        closed_dates: list[date] = []
        for trading_date, interval in sorted(liquid.items()):
            if interval is None:
                closed_dates.append(trading_date)
                continue
            opened, closed = interval
            if (
                opened.date() != trading_date
                or closed.date() != trading_date
                or opened.timetz().replace(tzinfo=None) != time(9, 30)
                or closed.timetz().replace(tzinfo=None) not in {time(13, 0), time(16, 0)}
                or closed <= opened
            ):
                reasons.add("CALENDAR_SESSION_SHAPE_INVALID")
                continue
            sessions.append(
                UsOptionsSession(
                    trading_date=trading_date,
                    open_et=opened,
                    close_et=closed,
                    open_utc=opened.astimezone(timezone.utc),
                    close_utc=closed.astimezone(timezone.utc),
                    early_close=closed.timetz().replace(tzinfo=None) == time(13, 0),
                )
            )

        status = CalendarStatus.READY if not reasons else CalendarStatus.DEGRADED
        provisional = UsOptionsCalendarSnapshot(
            status=status,
            reason_codes=tuple(reasons),
            observed_at=checked_observed,
            normalized_at=checked_now,
            source=source_text,
            broker_timezone_id=timezone_text,
            normalized_timezone_id="America/New_York",
            liquid_hours=liquid_text,
            trading_hours=trading_text,
            source_hash=source_hash,
            sessions=tuple(sessions),
            closed_dates=tuple(closed_dates),
            calendar_hash="0" * 64,
        )
        return replace(
            provisional,
            calendar_hash=canonical_hash(provisional.hash_payload()),
        )


class BrokerSessionHoursSource(Protocol):
    def options_session_hours(self, symbol: str = "SPY") -> object:
        raise NotImplementedError


class IBKRSessionCalendarProvider:
    """Normalize one fresh broker-hours read without inventing a fallback."""

    def __init__(
        self,
        source: BrokerSessionHoursSource,
        *,
        symbol: str = "SPY",
        normalizer: UsOptionsSessionCalendar | None = None,
    ) -> None:
        if not callable(getattr(source, "options_session_hours", None)):
            raise TypeError("source must expose options_session_hours")
        checked_symbol = str(symbol).strip().upper()
        if not checked_symbol:
            raise ValueError("calendar symbol cannot be blank")
        self.source = source
        self.symbol = checked_symbol
        self.normalizer = normalizer or UsOptionsSessionCalendar()
        self._lock = threading.RLock()
        self._cached_snapshot: UsOptionsCalendarSnapshot | None = None

    def snapshot(self, *, now: datetime) -> UsOptionsCalendarSnapshot:
        checked_now = utc_datetime(now, field="now")
        with self._lock:
            cached = self._cached_snapshot
            if cached is not None and self._cache_is_fresh(cached, checked_now):
                return cached
            values = {
                "liquid_hours": "",
                "trading_hours": "",
                "timezone_id": "",
                "observed_at": checked_now,
                "source": "",
            }
            acquisition_reasons: tuple[str, ...] = ()
            try:
                raw = self.source.options_session_hours(self.symbol)
                values = {
                    name: (
                        raw.get(name)
                        if isinstance(raw, Mapping)
                        else getattr(raw, name)
                    )
                    for name in (
                        "liquid_hours",
                        "trading_hours",
                        "timezone_id",
                        "observed_at",
                        "source",
                    )
                }
            except MarketDataPacingError as exc:
                acquisition_reasons = (
                    "CALENDAR_PACING_DENIED",
                    f"CALENDAR_{exc.reason_code}",
                )
            except SessionCalendarReadError as exc:
                acquisition_reasons = (exc.reason_code,)
            except TimeoutError:
                acquisition_reasons = ("CALENDAR_BROKER_REQUEST_TIMEOUT",)
            except BrokerConnectionError:
                acquisition_reasons = ("CALENDAR_BROKER_CONNECTION_UNAVAILABLE",)
            except Exception:
                acquisition_reasons = ("CALENDAR_SOURCE_READ_FAILED",)
            normalization_now = checked_now
            try:
                source_observed = utc_datetime(
                    values["observed_at"],
                    field="observed_at",
                )
                io_delay = Decimal(
                    str((source_observed - checked_now).total_seconds())
                )
                if Decimal("0") <= io_delay <= self.normalizer.maximum_age_seconds:
                    normalization_now = source_observed
            except (TypeError, ValueError):
                pass
            snapshot = self.normalizer.normalize(now=normalization_now, **values)
            if acquisition_reasons:
                provisional = replace(
                    snapshot,
                    reason_codes=(*snapshot.reason_codes, *acquisition_reasons),
                    calendar_hash="0" * 64,
                )
                snapshot = replace(
                    provisional,
                    calendar_hash=canonical_hash(provisional.hash_payload()),
                )
            self._cached_snapshot = snapshot
            return snapshot

    def _cache_is_fresh(
        self,
        snapshot: UsOptionsCalendarSnapshot,
        checked_now: datetime,
    ) -> bool:
        # A degraded read is evidence of an unsuccessful attempt, not reusable
        # broker authority.  In particular, caching a pacing denial prevents a
        # later authorized caller from recovering even when a lease is already
        # available.
        if snapshot.status is not CalendarStatus.READY or snapshot.reason_codes:
            return False
        if snapshot.verify_hash() is not True:
            return False
        for timestamp in (snapshot.observed_at, snapshot.normalized_at):
            age = Decimal(str((checked_now - timestamp).total_seconds()))
            if age < 0 or age > self.normalizer.maximum_age_seconds:
                return False
        return True


class TradingSessionCalendarProvider(Protocol):
    def snapshot(self, *, now: datetime) -> UsOptionsCalendarSnapshot:
        raise NotImplementedError


class BrokerTradingSessionGate:
    """Fail-closed 09:20/09:35 decision over a fresh IBKR calendar read.

    At 09:20 ET the option session has not opened, so production may only ask
    whether the broker published a session for that trading date.  At 09:35 ET
    the exact scheduled instant must already be inside that published session.
    The provider may share one still-fresh broker snapshot with the scheduler;
    this gate never manufactures a weekday fallback or accepts stale evidence.
    """

    _PREMARKET_SLOT = time(9, 20)
    _OPEN_SLOT = time(9, 35)
    _SLOT_WINDOW = timedelta(minutes=1)

    def __init__(
        self,
        provider: TradingSessionCalendarProvider,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if not callable(getattr(provider, "snapshot", None)):
            raise TypeError("provider must expose snapshot(now=...)")
        if clock is not None and not callable(clock):
            raise TypeError("clock must be callable")
        self._provider = provider
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    def is_trading_session(self, *, scheduled_for: datetime) -> bool | None:
        try:
            scheduled_utc = utc_datetime(scheduled_for, field="scheduled_for")
            scheduled_et = scheduled_utc.astimezone(US_OPTIONS_TIMEZONE)
            scheduled_time = scheduled_et.timetz().replace(tzinfo=None)
            if (
                scheduled_time not in {self._PREMARKET_SLOT, self._OPEN_SLOT}
                or scheduled_et.second != 0
                or scheduled_et.microsecond != 0
            ):
                return None

            checked_now = utc_datetime(self._clock(), field="clock result")
            if not (
                scheduled_utc
                <= checked_now
                < scheduled_utc + self._SLOT_WINDOW
            ):
                return None
            snapshot = self._provider.snapshot(now=checked_now)
            if (
                not isinstance(snapshot, UsOptionsCalendarSnapshot)
                or snapshot.status is not CalendarStatus.READY
            ):
                return None
            if snapshot.verify_hash() is not True:
                return None
            # A live IBKR request can complete a few milliseconds after the
            # timestamp supplied to ``snapshot``.  Verify freshness against a
            # clock read taken after that I/O instead of misclassifying the
            # broker's response timestamp as future evidence.
            verified_now = utc_datetime(self._clock(), field="clock result")
            if not (
                scheduled_utc
                <= verified_now
                < scheduled_utc + self._SLOT_WINDOW
            ):
                return None
            for timestamp in (snapshot.observed_at, snapshot.normalized_at):
                age = Decimal(str((verified_now - timestamp).total_seconds()))
                if age < 0 or age > DEFAULT_MAXIMUM_CALENDAR_AGE_SECONDS:
                    return None

            session = snapshot.session_for(scheduled_et.date())
            if session is None:
                return False
            if scheduled_time == self._PREMARKET_SLOT:
                return True
            return snapshot.session_at(scheduled_utc) is not None
        except Exception:
            return None


def _parse_hours(
    value: str,
) -> tuple[dict[date, tuple[datetime, datetime] | None], set[str]]:
    result: dict[date, tuple[datetime, datetime] | None] = {}
    reasons: set[str] = set()
    for raw_entry in value.split(";"):
        entry = raw_entry.strip()
        if not entry:
            continue
        if ":" not in entry:
            reasons.add("CALENDAR_HOURS_INVALID")
            continue
        day_text, body = entry.split(":", 1)
        try:
            trading_date = datetime.strptime(day_text, "%Y%m%d").date()
        except ValueError:
            reasons.add("CALENDAR_HOURS_INVALID")
            continue
        if trading_date in result:
            reasons.add("CALENDAR_DUPLICATE_DATE")
            continue
        if body == "CLOSED":
            result[trading_date] = None
            continue
        if "," in body:
            reasons.add("CALENDAR_MULTIPLE_INTERVALS_UNSUPPORTED")
            continue
        match = _INTERVAL_RE.fullmatch(body)
        if match is None:
            reasons.add("CALENDAR_HOURS_INVALID")
            continue
        try:
            start_date = _compact_date(match.group("start_date")) or trading_date
            end_date = _compact_date(match.group("end_date")) or trading_date
            opened = datetime.combine(
                start_date,
                _compact_time(match.group("start")),
                tzinfo=US_OPTIONS_TIMEZONE,
            )
            closed = datetime.combine(
                end_date,
                _compact_time(match.group("end")),
                tzinfo=US_OPTIONS_TIMEZONE,
            )
        except ValueError:
            reasons.add("CALENDAR_HOURS_INVALID")
            continue
        result[trading_date] = (opened, closed)
    if not result:
        reasons.add("CALENDAR_HOURS_INVALID")
    return result, reasons


def _trading_hours_cover_liquid_hours(
    liquid: dict[date, tuple[datetime, datetime] | None],
    trading: dict[date, tuple[datetime, datetime] | None],
) -> bool:
    """Accept IBKR extended hours only when they contain every liquid session."""

    for trading_date, liquid_interval in liquid.items():
        if trading_date not in trading:
            return False
        trading_interval = trading[trading_date]
        if liquid_interval is None:
            if trading_interval is not None:
                return False
            continue
        if trading_interval is None:
            return False
        liquid_open, liquid_close = liquid_interval
        trading_open, trading_close = trading_interval
        if trading_open > liquid_open or trading_close < liquid_close:
            return False
    return True


def _compact_date(value: str | None) -> date | None:
    if value is None:
        return None
    return datetime.strptime(value, "%Y%m%d").date()


def _compact_time(value: str) -> time:
    return time(int(value[:2]), int(value[2:]))


def _nonnegative_decimal(value: object, field: str) -> Decimal:
    if isinstance(value, bool):
        raise TypeError(f"{field} must be a Decimal-compatible number")
    try:
        parsed = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise TypeError(f"{field} must be a Decimal-compatible number") from exc
    if not parsed.is_finite() or parsed < 0:
        raise ValueError(f"{field} must be finite and nonnegative")
    return parsed


__all__ = [
    "BrokerTradingSessionGate",
    "CalendarStatus",
    "DEFAULT_MAXIMUM_CALENDAR_AGE_SECONDS",
    "US_OPTIONS_TIMEZONE",
    "UsOptionsCalendarSnapshot",
    "UsOptionsSession",
    "UsOptionsSessionCalendar",
]
