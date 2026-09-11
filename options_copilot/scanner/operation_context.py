"""Live parent-lease and shared-deadline fence for scheduled observations."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from math import isfinite
import threading
from time import monotonic
from typing import Callable

from options_copilot.market.session_calendar import US_OPTIONS_TIMEZONE
from .scheduler import DAILY_OPERATION_PIPELINES, ScanRunStore


@dataclass(frozen=True, slots=True)
class ScheduledOperationContext:
    """Prove a current preparation lease, never trading or model authority.

    The scheduler creates this only after its real durable acquisition. A
    caller-supplied token is not a substitute, and every use rechecks the row.
    Both wall-clock and monotonic budgets belong to that original invocation.
    """

    scan_run_id: str
    owner: str
    operation: str
    pipeline_version: str
    trading_date: date
    slot_at: datetime
    deadline_at: datetime
    cancel_event: threading.Event = field(repr=False, compare=False)
    _store: ScanRunStore = field(repr=False, compare=False)
    _clock: Callable[[], datetime] = field(repr=False, compare=False)
    _closing_event: threading.Event = field(repr=False, compare=False)
    _monotonic_deadline: float = field(repr=False, compare=False)
    _monotonic_clock: Callable[[], float] = field(
        default=monotonic, repr=False, compare=False,
    )

    def _remaining_at(self, instant: datetime) -> float:
        if self.cancel_event.is_set() or self._closing_event.is_set():
            return 0.0
        if any(
            not isinstance(value, datetime)
            or value.tzinfo is None
            or value.utcoffset() is None
            for value in (instant, self.slot_at, self.deadline_at)
        ):
            return 0.0
        if instant < self.slot_at:
            return 0.0
        wall_remaining = (self.deadline_at - instant).total_seconds()
        monotonic_remaining = self._monotonic_deadline - self._monotonic_clock()
        if not isfinite(wall_remaining) or not isfinite(monotonic_remaining):
            return 0.0
        return max(0.0, min(wall_remaining, monotonic_remaining))

    def remaining_seconds(self) -> float:
        """Return the original local budget, without resetting nested timeouts."""

        try:
            return self._remaining_at(self._clock())
        except Exception:
            return 0.0

    def is_active(self) -> bool:
        """Fail closed unless the exact parent remains durably and locally live."""

        try:
            instant = self._clock()
            if self._remaining_at(instant) <= 0:
                return False
            if (
                self.operation != "NEXT_SESSION_PREPARATION"
                or self.pipeline_version != DAILY_OPERATION_PIPELINES[
                    "NEXT_SESSION_PREPARATION"
                ]
                or self.owner != "daily-operation.next_session_preparation"
                or self.slot_at.astimezone(US_OPTIONS_TIMEZONE).date()
                != self.trading_date
            ):
                return False
            run = self._store.get(self.scan_run_id)
            # The read may wait behind SQLite work; do not validate its lease
            # against the earlier pre-read instant.
            verified_at = self._clock()
            return bool(
                run.scan_run_id == self.scan_run_id
                and run.status == "LEASED"
                and run.owner == self.owner
                and run.pipeline_version == self.pipeline_version
                and run.trading_date == self.trading_date
                and run.slot_at.astimezone(timezone.utc)
                == self.slot_at.astimezone(timezone.utc)
                and run.lease_expires_at is not None
                and run.lease_expires_at > verified_at
                # Store access can itself consume the remaining budget.
                and self._remaining_at(verified_at) > 0
            )
        except Exception:
            return False

    def require_active(self) -> None:
        """Reject a side effect when its scheduled observation parent is lost."""

        if not self.is_active():
            raise RuntimeError("SCHEDULED_OPERATION_NOT_ACTIVE")


__all__ = ["ScheduledOperationContext"]
