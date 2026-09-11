"""Cash-flow-adjusted accounting for the locked 10K Campaign."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, ROUND_HALF_EVEN

from .nav_ledger import StrategyNavSnapshot


TARGET = Decimal("10000")
_CENT = Decimal("0.01")
MILESTONES = (
    Decimal("2500"),
    Decimal("3500"),
    Decimal("5000"),
    Decimal("7500"),
    TARGET,
)


@dataclass(frozen=True, slots=True)
class CampaignSnapshot:
    asof: datetime
    actual_nlv: Decimal
    strategy_nav: Decimal
    external_cash_flow_total: Decimal
    target: Decimal
    progress_fraction: Decimal
    next_milestone: Decimal | None
    drawdown_fraction: Decimal
    peak_strategy_nav: Decimal
    paused: bool
    strategy_nav_authoritative: bool = False
    strategy_nav_snapshot_hash: str | None = None
    strategy_nav_contract_hash: str | None = None
    strategy_nav_ledger_head_hash: str | None = None


class TenKCampaign:
    """Present 10K progress without granting any risk authority.

    ``mark`` remains as a compatibility-only time-weighted research view.
    Production displays bind to an immutable :class:`StrategyNavSnapshot`
    through :meth:`mark_from_strategy_nav`; neither view can change a risk cap.
    """

    def __init__(
        self,
        *,
        start_nlv: Decimal,
        started_at: datetime,
        target: Decimal = TARGET,
        drawdown_circuit_breaker: Decimal = Decimal("0.30"),
    ) -> None:
        _aware(started_at)
        if start_nlv <= 0 or target <= start_nlv:
            raise ValueError("campaign requires positive start NLV below target")
        if not Decimal("0") < drawdown_circuit_breaker < Decimal("1"):
            raise ValueError("drawdown circuit breaker must be between zero and one")
        self.start_nlv = start_nlv
        self.target = target
        self.started_at = started_at
        self.drawdown_circuit_breaker = drawdown_circuit_breaker
        self._actual_nlv = start_nlv
        self._strategy_nav = start_nlv
        self._peak = start_nlv
        self._external_flows = Decimal("0")
        self._last_asof = started_at
        self._paused = False
        self._strategy_nav_snapshot_hash: str | None = None
        self._strategy_nav_contract_hash: str | None = None
        self._strategy_nav_ledger_head_hash: str | None = None

    def mark(
        self,
        *,
        actual_nlv: Decimal,
        asof: datetime,
        external_cash_flow: Decimal = Decimal("0"),
    ) -> CampaignSnapshot:
        _aware(asof)
        if asof <= self._last_asof:
            raise ValueError("campaign marks must be strictly time ordered")
        if actual_nlv < 0:
            raise ValueError("actual NLV cannot be negative")
        if self._actual_nlv <= 0:
            raise ValueError("cannot calculate a return after account depletion")
        investable_end = actual_nlv - external_cash_flow
        if investable_end < 0:
            raise ValueError("external cash flow exceeds ending NLV")
        period_factor = investable_end / self._actual_nlv
        self._strategy_nav = (self._strategy_nav * period_factor).quantize(
            _CENT, rounding=ROUND_HALF_EVEN
        )
        self._actual_nlv = actual_nlv
        self._external_flows += external_cash_flow
        self._last_asof = asof
        self._clear_strategy_nav_binding()
        self._peak = max(self._peak, self._strategy_nav)
        if self._drawdown() >= self.drawdown_circuit_breaker:
            self._paused = True
        return self.snapshot()

    def mark_from_strategy_nav(
        self,
        nav_snapshot: StrategyNavSnapshot,
    ) -> CampaignSnapshot:
        """Display one valid, hash-bound ledger snapshot as campaign progress."""

        if not isinstance(nav_snapshot, StrategyNavSnapshot):
            raise TypeError("nav_snapshot must be a StrategyNavSnapshot")
        if (
            not nav_snapshot.valid
            or nav_snapshot.strategy_nav is None
            or nav_snapshot.contract_hash is None
            or nav_snapshot.ledger_head_hash is None
            or not _hash(nav_snapshot.content_hash)
            or not _hash(nav_snapshot.contract_hash)
            or not _hash(nav_snapshot.ledger_head_hash)
        ):
            raise ValueError("campaign display requires a valid Strategy NAV snapshot")
        if nav_snapshot.asof <= self._last_asof:
            raise ValueError("campaign marks must be strictly time ordered")

        self._strategy_nav = nav_snapshot.strategy_nav
        if nav_snapshot.observed_account_nlv is not None:
            self._actual_nlv = nav_snapshot.observed_account_nlv
        self._last_asof = nav_snapshot.asof
        self._strategy_nav_snapshot_hash = nav_snapshot.content_hash
        self._strategy_nav_contract_hash = nav_snapshot.contract_hash
        self._strategy_nav_ledger_head_hash = nav_snapshot.ledger_head_hash
        self._peak = max(self._peak, self._strategy_nav)
        if self._drawdown() >= self.drawdown_circuit_breaker:
            self._paused = True
        return self.snapshot()

    observe_strategy_nav = mark_from_strategy_nav

    def snapshot(self) -> CampaignSnapshot:
        progress = (self._strategy_nav - self.start_nlv) / (self.target - self.start_nlv)
        progress = min(Decimal("1"), max(Decimal("0"), progress))
        next_milestone = next(
            (level for level in MILESTONES if level > self._strategy_nav),
            None,
        )
        return CampaignSnapshot(
            asof=self._last_asof,
            actual_nlv=self._actual_nlv,
            strategy_nav=self._strategy_nav,
            external_cash_flow_total=self._external_flows,
            target=self.target,
            progress_fraction=progress,
            next_milestone=next_milestone,
            drawdown_fraction=self._drawdown(),
            peak_strategy_nav=self._peak,
            paused=self._paused,
            strategy_nav_authoritative=self._strategy_nav_snapshot_hash is not None,
            strategy_nav_snapshot_hash=self._strategy_nav_snapshot_hash,
            strategy_nav_contract_hash=self._strategy_nav_contract_hash,
            strategy_nav_ledger_head_hash=self._strategy_nav_ledger_head_hash,
        )

    def _drawdown(self) -> Decimal:
        if self._peak <= 0:
            return Decimal("0")
        return max(Decimal("0"), (self._peak - self._strategy_nav) / self._peak)

    def _clear_strategy_nav_binding(self) -> None:
        self._strategy_nav_snapshot_hash = None
        self._strategy_nav_contract_hash = None
        self._strategy_nav_ledger_head_hash = None


def trades_to_target(
    *,
    start_nlv: Decimal,
    target: Decimal,
    geometric_net_growth_per_trade: Decimal,
) -> int:
    if start_nlv <= 0 or target <= start_nlv:
        raise ValueError("target must exceed positive starting NLV")
    if geometric_net_growth_per_trade <= 0:
        raise ValueError("growth per trade must be positive")
    count = 0
    value = start_nlv
    factor = Decimal("1") + geometric_net_growth_per_trade
    while value < target:
        value *= factor
        count += 1
        if count > 1_000_000:
            raise ValueError("growth rate is too small for bounded calculation")
    return count


def _aware(value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")


def _hash(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )
