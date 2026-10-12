"""Prospective market-window and all-Gates option-reprice evidence contracts."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from types import MappingProxyType
from typing import Mapping

from options_copilot.storage.canonical import canonical_hash, freeze_json, utc_datetime


SUPPORTING_ONLY = "SUPPORTING_ONLY"
WINDOW_DURATION = timedelta(minutes=5)


@dataclass(frozen=True, slots=True)
class MarketSample:
    observed_at: datetime
    values: Mapping[str, Decimal]

    def __post_init__(self) -> None:
        object.__setattr__(self, "observed_at", utc_datetime(self.observed_at, field="observed_at"))
        normalized = {str(key): value for key, value in self.values.items() if isinstance(value, Decimal) and value.is_finite()}
        if not normalized:
            raise ValueError("market sample requires finite Decimal values")
        object.__setattr__(self, "values", MappingProxyType(normalized))


@dataclass(frozen=True, slots=True)
class ProspectiveMarketWindow:
    event_hash: str
    release_hash: str
    release_at: datetime
    armed_at: datetime
    samples: tuple[MarketSample, ...] = ()
    baseline: MarketSample | None = None

    def __post_init__(self) -> None:
        release = utc_datetime(self.release_at, field="release_at")
        armed = utc_datetime(self.armed_at, field="armed_at")
        if armed > release:
            raise ValueError("market window cannot be armed after release")
        object.__setattr__(self, "release_at", release)
        object.__setattr__(self, "armed_at", armed)
        if self.baseline is not None:
            if self.baseline.observed_at > release or self.baseline.observed_at < armed:
                raise ValueError("market baseline must be captured prospectively after arming and no later than release")
        previous: datetime | None = None
        for sample in self.samples:
            if sample.observed_at < release or sample.observed_at > release + WINDOW_DURATION + timedelta(seconds=5):
                raise ValueError("historical or out-of-window market samples are forbidden")
            if previous is not None and sample.observed_at <= previous:
                raise ValueError("market samples must be strictly prospective and ordered")
            previous = sample.observed_at

    def append(self, sample: MarketSample) -> "ProspectiveMarketWindow":
        return ProspectiveMarketWindow(self.event_hash, self.release_hash, self.release_at, self.armed_at, (*self.samples, sample), self.baseline)

    def with_baseline(self, sample: MarketSample) -> "ProspectiveMarketWindow":
        if self.baseline is not None:
            raise ValueError("market baseline is immutable once captured")
        return ProspectiveMarketWindow(self.event_hash, self.release_hash, self.release_at, self.armed_at, self.samples, sample)

    def projection(self, *, now: datetime) -> Mapping[str, object]:
        checked = utc_datetime(now, field="now")
        endpoint = next((sample for sample in self.samples if sample.observed_at >= self.release_at + WINDOW_DURATION), None)
        complete = checked >= self.release_at + WINDOW_DURATION and self.baseline is not None and endpoint is not None
        if checked < self.release_at + WINDOW_DURATION:
            reason = "WAITING_PROSPECTIVE_FIVE_MINUTE_ENDPOINT"
        elif self.baseline is None:
            reason = "WAITING_PROSPECTIVE_PRE_RELEASE_BASELINE"
        elif endpoint is None:
            reason = "WAITING_PROSPECTIVE_FIVE_MINUTE_ENDPOINT"
        else:
            reason = None
        document = {"schema": "options_copilot.prospective_market_window.v2", "event_hash": self.event_hash, "release_hash": self.release_hash, "window_start": self.release_at.isoformat(), "window_end": (self.release_at + WINDOW_DURATION).isoformat(), "armed_at": self.armed_at.isoformat(), "baseline": None if self.baseline is None else {"observed_at": self.baseline.observed_at.isoformat(), "values": {key: str(value) for key, value in self.baseline.values.items()}}, "sample_count": len(self.samples), "samples": [{"observed_at": sample.observed_at.isoformat(), "values": {key: str(value) for key, value in sample.values.items()}} for sample in self.samples], "complete": complete, "reason": reason, "decision_authority": SUPPORTING_ONLY, "approval_eligible": False, "instruction_creation_allowed": False, "order_creation_allowed": False}
        return freeze_json({**document, "content_hash": canonical_hash(document)})


REQUIRED_OPTION_GATES = (
    "GATE_1_AUTHORITY_DATA",
    "GATE_2_MARKET_CREDIT_REGIME",
    "GATE_3_UNDERLYING_EVENT",
    "GATE_4_OPTION_EDGE_LIQUIDITY",
    "GATE_5_STRUCTURE_ACCOUNT_RISK",
    "GATE_6_RANKING_REVIEWABILITY",
)


@dataclass(frozen=True, slots=True)
class OptionRepriceBundle:
    event_hash: str
    release_hash: str
    market_window_hash: str
    candidate_hash: str | None
    gate_bundle_hash: str | None
    gate_results: Mapping[str, bool]
    evidence_hashes: tuple[str, ...]
    observed_at: datetime

    def __post_init__(self) -> None:
        object.__setattr__(self, "observed_at", utc_datetime(self.observed_at, field="observed_at"))
        normalized = {name: self.gate_results.get(name) is True for name in REQUIRED_OPTION_GATES}
        object.__setattr__(self, "gate_results", MappingProxyType(normalized))

    @property
    def blockers(self) -> tuple[str, ...]:
        blockers = tuple(f"OPTION_{name}_FAILED" for name, passed in self.gate_results.items() if not passed)
        if self.candidate_hash is None:
            blockers += ("OPTION_CANDIDATE_HASH_UNAVAILABLE",)
        if self.gate_bundle_hash is None:
            blockers += ("OPTION_GATE_BUNDLE_HASH_UNAVAILABLE",)
        return blockers

    def as_dict(self) -> Mapping[str, object]:
        ready = not self.blockers
        document = {"schema": "options_copilot.option_reprice_supporting_bundle.v1", "event_hash": self.event_hash, "release_hash": self.release_hash, "market_window_hash": self.market_window_hash, "candidate_hash": self.candidate_hash, "gate_bundle_hash": self.gate_bundle_hash, "gate_results": dict(self.gate_results), "evidence_hashes": list(self.evidence_hashes), "observed_at": self.observed_at.isoformat(), "status": "AVAILABLE" if ready else "WAIT", "next_action": "NONE_SUPPORTING_COMPLETE" if ready else "WAIT_FOR_ALL_OPTION_GATES", "blockers": list(self.blockers), "decision_authority": SUPPORTING_ONLY, "approval_eligible": False, "instruction_creation_allowed": False, "order_creation_allowed": False}
        return freeze_json({**document, "content_hash": canonical_hash(document)})


__all__ = ["MarketSample", "OptionRepriceBundle", "ProspectiveMarketWindow", "REQUIRED_OPTION_GATES", "WINDOW_DURATION"]
