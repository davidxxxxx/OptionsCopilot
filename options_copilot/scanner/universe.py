"""Auditable, fixed-order candidate funnel for read-only options research."""
from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from hashlib import sha256
import json

from .pacing import RequestBudgetByClass, PACING_CAPABILITY_MISSING

FUNNEL_LEVELS = (
    "positions",
    "core_etfs",
    "event_pool",
    "scanner",
    "coarse_contracts",
    "finalists",
)


@dataclass(frozen=True, slots=True)
class FunnelEvidence:
    level: str
    item: Mapping[str, object]
    included: bool
    reason_code: str
    evidence_hash: str


@dataclass(frozen=True, slots=True)
class UniverseResult:
    status: str
    mode: str
    research: tuple[Mapping[str, object], ...]
    finalists: tuple[Mapping[str, object], ...]
    evidence: tuple[FunnelEvidence, ...]
    usage: Mapping[str, Mapping[str, int]]
    reasons: tuple[str, ...]
    funnel_trace: Mapping[str, object]


class UniverseFunnel:
    """Each input flows through every named level; no level is bypassable."""

    def __init__(self, pacing: RequestBudgetByClass) -> None:
        self.pacing = pacing

    def run(
        self,
        *,
        positions: Iterable[Mapping[str, object]],
        core_etfs: Iterable[Mapping[str, object]],
        event_pool: Iterable[Mapping[str, object]],
        scanner: Iterable[Mapping[str, object]],
        coarse_contracts: Iterable[Mapping[str, object]],
        signed_dte_exception: bool = False,
        funnel_trace: Mapping[str, object] | None = None,
    ) -> UniverseResult:
        sources = {
            "positions": tuple(positions),
            "core_etfs": tuple(core_etfs),
            "event_pool": tuple(event_pool),
            "scanner": tuple(scanner),
            "coarse_contracts": tuple(coarse_contracts),
        }
        evidence: list[FunnelEvidence] = []
        position_symbols = {
            str(item.get("symbol") or item.get("underlying") or "").upper()
            for item in sources["positions"]
            if _open_position(item)
        }
        mode = "POSITION_MANAGEMENT_ONLY" if position_symbols else "ENTRY_RESEARCH"
        for item in sources["positions"]:
            is_open = _open_position(item)
            evidence.append(
                _evidence(
                    "positions",
                    item,
                    is_open,
                    "POSITION_MANAGEMENT_SEED" if is_open else "POSITION_CLOSED",
                )
            )
        if not self.pacing.ready:
            return UniverseResult(
                "NO_TRADE",
                mode,
                (),
                (),
                tuple(evidence),
                self.pacing.usage(),
                (PACING_CAPABILITY_MISSING,),
                dict(funnel_trace or {}),
            )
        # Retain a transparent Top-10 research list. Action candidates are
        # hard-filtered but never forced to fill ten slots.
        research_pool = (
            list(sources["core_etfs"])
            + list(sources["event_pool"])
            + list(sources["scanner"])
        )
        for level in ("core_etfs", "event_pool", "scanner"):
            for item in sources[level]:
                evidence.append(_evidence(level, item, True, "RESEARCH_INCLUDED"))
        research = tuple(
            sorted(
                research_pool,
                key=lambda item: float(item.get("score", 0)),
                reverse=True,
            )[:10]
        )
        finalists: list[Mapping[str, object]] = []
        for item in sources["coarse_contracts"]:
            dte = item.get("dte")
            reason = "ELIGIBLE"
            allowed = mode == "ENTRY_RESEARCH"
            if mode != "ENTRY_RESEARCH":
                reason = "POSITION_MANAGEMENT_ONLY"
                allowed = False
            elif not isinstance(dte, int):
                reason = "DTE_EVIDENCE_MISSING"
                allowed = False
            elif dte < 7:
                reason = "DTE_BELOW_PERMANENT_FLOOR"
                allowed = False
            elif dte < 14 and not signed_dte_exception:
                reason = "DTE_EXCEPTION_REQUIRED"
                allowed = False
            elif dte > 35:
                reason = "DTE_ABOVE_NORMAL_MAXIMUM"
                allowed = False
            evidence.append(_evidence("coarse_contracts", item, allowed, reason))
            if allowed:
                finalists.append(item)
        for item in finalists[:10]:
            evidence.append(_evidence("finalists", item, True, "HARD_DATA_FINALIST"))
        for item in finalists[10:]:
            evidence.append(_evidence("finalists", item, False, "TOP10_ACTION_LIMIT"))
        result_status = (
            "PARTIAL"
            if any(
                not item.included
                for item in evidence
                if item.level == "coarse_contracts"
            )
            else "READY"
        )
        if mode != "ENTRY_RESEARCH":
            result_status = "NO_TRADE"
        return UniverseResult(
            result_status,
            mode,
            research,
            tuple(finalists[:10]) if mode == "ENTRY_RESEARCH" else (),
            tuple(evidence),
            self.pacing.usage(),
            () if mode == "ENTRY_RESEARCH" else ("POSITION_MANAGEMENT_ONLY",),
            dict(funnel_trace or {}),
        )

    __call__ = run


def _open_position(item: Mapping[str, object]) -> bool:
    value = item.get("position", item.get("quantity", 0))
    try:
        return float(value) != 0
    except (TypeError, ValueError):
        return False


def _evidence(level: str, item: Mapping[str, object], included: bool, reason: str) -> FunnelEvidence:
    digest = sha256(
        json.dumps(
            dict(item),
            sort_keys=True,
            default=str,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    return FunnelEvidence(level, item, included, reason, digest)
