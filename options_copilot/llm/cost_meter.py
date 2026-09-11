"""Daily model-specific call and spend caps."""
from __future__ import annotations

import threading
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
import json
import math
import os
from pathlib import Path
from typing import Callable
from zoneinfo import ZoneInfo


FLASH = "deepseek-v4-flash"
PRO = "deepseek-v4-pro"
MODELS = {FLASH, PRO}


@dataclass(frozen=True, slots=True)
class CostPolicy:
    flash_call_cap: int = 500
    pro_call_cap: int = 30
    daily_spend_cap_usd: float = 2.0
    timezone_name: str = "Asia/Shanghai"

    def __post_init__(self) -> None:
        if self.flash_call_cap <= 0 or self.pro_call_cap <= 0:
            raise ValueError("daily call caps must be positive")
        if not math.isfinite(self.daily_spend_cap_usd):
            raise ValueError("daily spend cap must be finite")
        if self.daily_spend_cap_usd <= 0:
            raise ValueError("daily spend cap must be positive")
        ZoneInfo(self.timezone_name)


@dataclass(frozen=True, slots=True)
class DailyCostStatus:
    local_date: date
    flash_calls: int
    pro_calls: int
    spend_usd: float


@dataclass(frozen=True, slots=True)
class CostReservation:
    model: str
    local_date: date
    estimated_cost_usd: float
    token: int


@dataclass(frozen=True, slots=True)
class CostRejection:
    reason: str
    next_eligible_at: datetime


_PRICING_PER_MILLION = {
    FLASH: {"cache_hit": 0.0028, "cache_miss": 0.14, "output": 0.28},
    PRO: {"cache_hit": 0.003625, "cache_miss": 0.435, "output": 0.87},
}


class CostMeter:
    def __init__(
        self,
        policy: CostPolicy | None = None,
        *,
        clock: Callable[[], datetime] | None = None,
        state_path: str | Path | None = None,
    ) -> None:
        self.policy = policy or CostPolicy()
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._zone = ZoneInfo(self.policy.timezone_name)
        self._state_path = Path(state_path) if state_path is not None else None
        self._date: date | None = None
        self._calls = {FLASH: 0, PRO: 0}
        self._spend_usd = 0.0
        self._reservations: dict[int, CostReservation] = {}
        self._next_reservation_token = 0
        self._lock = threading.Lock()

    def authorize(self, model: str, *, estimated_cost_usd: float) -> str | None:
        self._validate_model(model)
        if not math.isfinite(estimated_cost_usd):
            raise ValueError("estimated cost must be finite")
        if estimated_cost_usd < 0:
            raise ValueError("estimated cost cannot be negative")
        with self._lock:
            self._roll_day(self._clock())
            return self._cap_reason(model, estimated_cost_usd)

    def reserve(
        self,
        model: str,
        *,
        estimated_cost_usd: float,
    ) -> tuple[CostReservation | None, CostRejection | None]:
        self._validate_model(model)
        if not math.isfinite(estimated_cost_usd):
            raise ValueError("estimated cost must be finite")
        if estimated_cost_usd < 0:
            raise ValueError("estimated cost cannot be negative")
        with self._lock:
            self._roll_day(self._clock())
            cap_reason = self._cap_reason(model, estimated_cost_usd)
            if cap_reason:
                return None, CostRejection(
                    reason=cap_reason,
                    next_eligible_at=self._next_eligible_at_locked(),
                )
            assert self._date is not None
            self._next_reservation_token += 1
            reservation = CostReservation(
                model=model,
                local_date=self._date,
                estimated_cost_usd=estimated_cost_usd,
                token=self._next_reservation_token,
            )
            self._reservations[reservation.token] = reservation
            self._calls[model] += 1
            self._spend_usd += estimated_cost_usd
            self._persist()
            return reservation, None

    def settle(
        self,
        reservation: CostReservation,
        usage: Mapping[str, int],
        *,
        now: datetime | None = None,
    ) -> float:
        cost = calculate_cost_usd(reservation.model, usage)
        with self._lock:
            active = self._reservations.pop(reservation.token, None)
            if active != reservation:
                raise ValueError("unknown cost reservation")
            self._roll_day(now or self._clock())
            if reservation.local_date == self._date:
                self._spend_usd += cost - reservation.estimated_cost_usd
                self._persist()
        return cost

    def commit_failure(
        self,
        reservation: CostReservation,
        *,
        now: datetime | None = None,
    ) -> None:
        """Keep the reserved call and worst-case spend after a transport attempt."""
        with self._lock:
            active = self._reservations.pop(reservation.token, None)
            if active != reservation:
                return
            self._roll_day(now or self._clock())
            if reservation.local_date == self._date:
                self._persist()

    def cancel(self, reservation: CostReservation, *, now: datetime | None = None) -> None:
        with self._lock:
            active = self._reservations.pop(reservation.token, None)
            if active != reservation:
                return
            self._roll_day(now or self._clock())
            if reservation.local_date == self._date:
                self._calls[reservation.model] -= 1
                self._spend_usd -= reservation.estimated_cost_usd
                self._persist()

    def record(
        self,
        model: str,
        usage: Mapping[str, int],
        *,
        now: datetime | None = None,
    ) -> float:
        self._validate_model(model)
        cost = calculate_cost_usd(model, usage)
        with self._lock:
            self._roll_day(now or self._clock())
            self._calls[model] += 1
            self._spend_usd += cost
            self._persist()
        return cost

    def status(self) -> DailyCostStatus:
        with self._lock:
            self._roll_day(self._clock())
            assert self._date is not None
            return DailyCostStatus(
                local_date=self._date,
                flash_calls=self._calls[FLASH],
                pro_calls=self._calls[PRO],
                spend_usd=self._spend_usd,
            )

    def next_eligible_at(self) -> datetime:
        """Return the next reset boundary from this meter's clock and timezone."""
        with self._lock:
            self._roll_day(self._clock())
            return self._next_eligible_at_locked()

    def _next_eligible_at_locked(self) -> datetime:
        assert self._date is not None
        local_midnight = datetime.combine(
            self._date + timedelta(days=1),
            time.min,
            tzinfo=self._zone,
        )
        return local_midnight.astimezone(timezone.utc)

    def _roll_day(self, now: datetime) -> None:
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("cost meter clock must be timezone-aware")
        local_date = now.astimezone(self._zone).date()
        if local_date != self._date:
            self._date = local_date
            self._calls, self._spend_usd = self._load(local_date)

    def _load(self, local_date: date) -> tuple[dict[str, int], float]:
        if self._state_path is None or not self._state_path.exists():
            return {FLASH: 0, PRO: 0}, 0.0
        try:
            payload = json.loads(self._state_path.read_text(encoding="utf-8"))
            if payload.get("local_date") != local_date.isoformat():
                return {FLASH: 0, PRO: 0}, 0.0
            flash_calls = max(0, int(payload.get("flash_calls", 0)))
            pro_calls = max(0, int(payload.get("pro_calls", 0)))
            spend = float(payload.get("spend_usd", 0.0))
            if not math.isfinite(spend) or spend < 0:
                raise ValueError("invalid persisted spend")
            return {FLASH: flash_calls, PRO: pro_calls}, spend
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            return (
                {
                    FLASH: self.policy.flash_call_cap,
                    PRO: self.policy.pro_call_cap,
                },
                self.policy.daily_spend_cap_usd,
            )

    def _persist(self) -> None:
        if self._state_path is None or self._date is None:
            return
        self._state_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "local_date": self._date.isoformat(),
            "flash_calls": self._calls[FLASH],
            "pro_calls": self._calls[PRO],
            "spend_usd": self._spend_usd,
        }
        temporary = self._state_path.with_name(
            f"{self._state_path.name}.{os.getpid()}.{threading.get_ident()}.tmp"
        )
        temporary.write_text(
            json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n",
            encoding="utf-8",
        )
        temporary.replace(self._state_path)

    def _cap_reason(self, model: str, estimated_cost_usd: float) -> str | None:
        cap = self.policy.flash_call_cap if model == FLASH else self.policy.pro_call_cap
        if self._calls[model] >= cap:
            prefix = "FLASH" if model == FLASH else "PRO"
            return f"{prefix}_DAILY_CALL_CAP"
        if self._spend_usd + estimated_cost_usd > self.policy.daily_spend_cap_usd:
            return "DAILY_SPEND_CAP"
        return None

    @staticmethod
    def _validate_model(model: str) -> None:
        if model not in MODELS:
            raise ValueError(f"unsupported DeepSeek model: {model}")


def calculate_cost_usd(model: str, usage: Mapping[str, int]) -> float:
    if model not in MODELS:
        raise ValueError(f"unsupported DeepSeek model: {model}")
    prompt = max(0, int(usage.get("prompt_tokens", 0)))
    hit = max(0, int(usage.get("prompt_cache_hit_tokens", 0)))
    miss = max(0, int(usage.get("prompt_cache_miss_tokens", prompt - hit)))
    if hit + miss < prompt:
        miss += prompt - hit - miss
    output = max(0, int(usage.get("completion_tokens", 0)))
    price = _PRICING_PER_MILLION[model]
    return (
        hit * price["cache_hit"]
        + miss * price["cache_miss"]
        + output * price["output"]
    ) / 1_000_000.0
