"""Capability-bound request accounting for read-only market-data research."""
from __future__ import annotations

from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import threading
from typing import Mapping

from options_copilot.operations.capabilities import (
    CapabilityStatus,
    DEFAULT_PACING_MAX_AGE,
    MarketDataPacingCapability,
    PACING_REQUEST_CLASSES,
)


PACING_CAPABILITY_MISSING = "PACING_CAPABILITY_MISSING"

@dataclass(frozen=True, slots=True)
class BudgetDecision:
    allowed: bool
    request_class: str
    reason: str | None
    used: int
    limit: int

@dataclass(frozen=True, slots=True)
class CacheRecord:
    request_key: str; request_class: str; source: str; observed_at: datetime; expires_at: datetime
    entitlement_hash: str; capability_hash: str; payload_hash: str; payload: object
    def fresh(self, *, now: datetime, entitlement_hash: str, capability_hash: str) -> bool:
        return now < self.expires_at and self.entitlement_hash == entitlement_hash and self.capability_hash == capability_hash


class RequestBudgetLease:
    """One signed pacing reservation released after the broker call returns."""

    def __init__(
        self,
        budget: "RequestBudgetByClass",
        decision: BudgetDecision,
        *,
        reserved: bool,
    ) -> None:
        self._budget = budget
        self.decision = decision
        self._reserved = reserved
        self._closed = False

    def __enter__(self) -> BudgetDecision:
        return self.decision

    def __exit__(self, *_: object) -> None:
        if self._closed:
            return
        self._closed = True
        if self._reserved:
            self._budget._release(self.decision.request_class)

class RequestBudgetByClass:
    """No valid signed capability means zero usable request budget."""
    def __init__(
        self,
        capability: MarketDataPacingCapability | None,
        *,
        now: datetime,
        expected_version: str | None = None,
        clock: Callable[[], datetime] | None = None,
        validation_max_age: timedelta = DEFAULT_PACING_MAX_AGE,
    ) -> None:
        if not isinstance(validation_max_age, timedelta) or validation_max_age <= timedelta(0):
            raise ValueError("validation_max_age must be a positive timedelta")
        self.capability = capability
        self._expected_version = expected_version
        self._clock = clock or (lambda: now)
        self._validation_max_age = validation_max_age
        self._lock = threading.RLock()
        self.reason: str | None = self._validate(capability, now, expected_version)
        self.capability_hash = capability.content_hash if self.reason is None and capability else None
        self._limits = {name: 0 for name in PACING_REQUEST_CLASSES}
        self._windows = {name: deque() for name in PACING_REQUEST_CLASSES}
        self._in_flight = {name: 0 for name in PACING_REQUEST_CLASSES}
        self._cooldown_until: dict[str, datetime | None] = {
            name: None for name in PACING_REQUEST_CLASSES
        }
        if self.reason is None and capability is not None:
            self._limits = {name: int(capability.request_classes[name]["max_requests"]) for name in PACING_REQUEST_CLASSES}

    @property
    def ready(self) -> bool: return self.reason is None
    def decision(self, request_class: str) -> BudgetDecision:
        with self.lease(request_class) as decision:
            return decision
    consume = decision

    def lease(self, request_class: str) -> RequestBudgetLease:
        self._check_class(request_class)
        now = self._now()
        with self._lock:
            self._prune(request_class, now)
            used = len(self._windows[request_class])
            limit = self._limits[request_class]
            reason = self._validate(
                self.capability,
                now,
                self._expected_version,
            )
            if reason is not None:
                decision = BudgetDecision(
                    False,
                    request_class,
                    PACING_CAPABILITY_MISSING,
                    used,
                    limit,
                )
                return RequestBudgetLease(self, decision, reserved=False)
            limits = self._class_limits(request_class)
            if self._in_flight[request_class] >= limits["max_concurrency"]:
                decision = BudgetDecision(
                    False,
                    request_class,
                    "PACING_CONCURRENCY_LIMIT",
                    used,
                    limit,
                )
                return RequestBudgetLease(self, decision, reserved=False)
            cooldown_until = self._cooldown_until[request_class]
            if cooldown_until is not None and now < cooldown_until:
                decision = BudgetDecision(
                    False,
                    request_class,
                    "PACING_COOLDOWN_ACTIVE",
                    used,
                    limit,
                )
                return RequestBudgetLease(self, decision, reserved=False)
            if used >= limit:
                decision = BudgetDecision(
                    False,
                    request_class,
                    "PACING_REQUEST_WINDOW_EXHAUSTED",
                    used,
                    limit,
                )
                return RequestBudgetLease(self, decision, reserved=False)
            self._windows[request_class].append(now)
            self._in_flight[request_class] += 1
            used += 1
            if used >= limit:
                self._cooldown_until[request_class] = now + timedelta(
                    seconds=limits["cooldown"]
                )
            decision = BudgetDecision(True, request_class, None, used, limit)
            return RequestBudgetLease(self, decision, reserved=True)

    def denied_lease(
        self,
        request_class: str,
        reason: str,
    ) -> RequestBudgetLease:
        self._check_class(request_class)
        with self._lock:
            used = len(self._windows[request_class])
            decision = BudgetDecision(
                False,
                request_class,
                reason,
                used,
                self._limits[request_class],
            )
            return RequestBudgetLease(self, decision, reserved=False)

    def usage(self) -> dict[str, dict[str, int]]:
        now = self._now()
        with self._lock:
            for request_class in PACING_REQUEST_CLASSES:
                self._prune(request_class, now)
            return {
                key: {
                    "used": len(self._windows[key]),
                    "limit": self._limits[key],
                }
                for key in PACING_REQUEST_CLASSES
            }
    def cache_record(self, *, request_key: str, request_class: str, source: str, payload: object, observed_at: datetime, ttl: timedelta, entitlement_hash: str) -> CacheRecord:
        self._check_class(request_class)
        if self.reason or self.capability_hash is None: raise RuntimeError(PACING_CAPABILITY_MISSING)
        if ttl <= timedelta(0): raise ValueError("ttl must be positive")
        return CacheRecord(request_key, request_class, source, observed_at, observed_at + ttl, entitlement_hash, self.capability_hash, sha256(repr(payload).encode()).hexdigest(), payload)
    def _validate(self, capability: MarketDataPacingCapability | None, now: datetime, expected_version: str | None) -> str | None:
        if capability is None or not isinstance(capability, MarketDataPacingCapability) or not capability.signer: return PACING_CAPABILITY_MISSING
        report = capability.validate(
            now=now,
            max_age=self._validation_max_age,
        )
        if report.status is not CapabilityStatus.READY_FOR_REVIEW: return PACING_CAPABILITY_MISSING
        if expected_version is not None and capability.version != expected_version: return PACING_CAPABILITY_MISSING
        if set(capability.request_classes) != set(PACING_REQUEST_CLASSES): return PACING_CAPABILITY_MISSING
        return None
    def _check_class(self, request_class: str) -> None:
        if request_class not in PACING_REQUEST_CLASSES: raise ValueError("unsupported pacing request class")

    def _class_limits(self, request_class: str) -> dict[str, int | float]:
        assert self.capability is not None
        raw = self.capability.request_classes[request_class]
        return {
            "max_concurrency": int(raw["max_concurrency"]),
            "request_window": float(raw["request_window"]),
            "cooldown": float(raw["cooldown"]),
        }

    def _now(self) -> datetime:
        value = self._clock()
        if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("pacing clock must return a timezone-aware datetime")
        return value.astimezone(timezone.utc)

    def _prune(self, request_class: str, now: datetime) -> None:
        if self.capability is None:
            return
        window = timedelta(seconds=self._class_limits(request_class)["request_window"])
        requests = self._windows[request_class]
        while requests and now - requests[0] >= window:
            requests.popleft()
        cooldown_until = self._cooldown_until[request_class]
        if cooldown_until is not None and now >= cooldown_until:
            self._cooldown_until[request_class] = None

    def _release(self, request_class: str) -> None:
        with self._lock:
            if self._in_flight[request_class] <= 0:
                raise RuntimeError("pacing reservation is not active")
            self._in_flight[request_class] -= 1

class SubscriptionLease:
    """Cancels only the temporary subscription, exactly once."""
    def __init__(self, subscription_id: str, cancel: callable, *, preexisting: bool = False) -> None:
        self.subscription_id, self._cancel, self._preexisting, self._closed = subscription_id, cancel, preexisting, False
    def close(self) -> None:
        if not self._closed:
            self._closed = True
            if not self._preexisting: self._cancel(self.subscription_id)
    def __enter__(self) -> "SubscriptionLease": return self
    def __exit__(self, *_: object) -> None: self.close()
