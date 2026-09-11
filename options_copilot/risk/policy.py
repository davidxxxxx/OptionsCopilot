"""Locked 10/15/20 account-risk and single-combination policy."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, localcontext

from options_copilot.domain import CandidateRiskTier, StrategyCandidate
from options_copilot.performance.nav_ledger import StrategyNavSnapshot
from options_copilot.storage.canonical import canonical_hash

from .authorization import RiskTierAuthority

from .payoff import (
    ExpirationPayoff,
    RiskRejection,
    analyze_expiration_payoff,
)


ZERO = Decimal("0")


def _positive_decimal(value: object, field_name: str) -> Decimal:
    if not isinstance(value, Decimal):
        raise TypeError(f"{field_name} must be a Decimal")
    if not value.is_finite() or value <= ZERO:
        raise ValueError(f"{field_name} must be finite and positive")
    return value


@dataclass(frozen=True, slots=True)
class RiskPolicy:
    normal_risk_fraction: Decimal = Decimal("0.10")
    validated_risk_fraction: Decimal = Decimal("0.15")
    hard_reject_fraction: Decimal = Decimal("0.20")
    max_open_combinations: int = 1

    def __post_init__(self) -> None:
        for field_name in (
            "normal_risk_fraction",
            "validated_risk_fraction",
            "hard_reject_fraction",
        ):
            _positive_decimal(getattr(self, field_name), field_name)
        if not (
            self.normal_risk_fraction <= Decimal("0.10")
            and self.normal_risk_fraction
            <= self.validated_risk_fraction
            <= Decimal("0.15")
            and self.validated_risk_fraction < self.hard_reject_fraction
            <= Decimal("0.20")
        ):
            raise ValueError("risk policy cannot loosen the locked 10/15/20 ceilings")
        if isinstance(self.max_open_combinations, bool) or not isinstance(
            self.max_open_combinations, int
        ):
            raise TypeError("max_open_combinations must be an integer")
        if self.max_open_combinations != 1:
            raise ValueError("the locked policy permits at most one open combination")


@dataclass(frozen=True, slots=True)
class RiskAssessment:
    candidate: StrategyCandidate
    payoff: ExpirationPayoff
    approved: bool
    account_equity: Decimal
    risk_fraction: Decimal | None
    allowed_risk_fraction: Decimal
    rejections: tuple[RiskRejection, ...]
    production_bound: bool = False
    strategy_nav_snapshot_hash: str | None = None
    strategy_nav_contract_hash: str | None = None
    ledger_head_hash: str | None = None
    risk_authority_version: str | None = None
    risk_authority_marker_hash: str | None = None

    @property
    def risk_base(self) -> Decimal:
        """Capital base used by this assessment.

        In production-bound mode this is always signed Strategy NAV.  The
        historical ``account_equity`` field remains for research compatibility.
        """

        return self.account_equity


class RiskEngine:
    def __init__(
        self,
        policy: RiskPolicy | None = None,
        *,
        strategy_nav: StrategyNavSnapshot | None = None,
        expected_contract_hash: str | None = None,
        expected_ledger_head_hash: str | None = None,
        risk_tier_authority: RiskTierAuthority | None = None,
    ) -> None:
        self.policy = policy or RiskPolicy()
        self.strategy_nav = strategy_nav
        self.risk_tier_authority = risk_tier_authority
        self._production_bound = strategy_nav is not None
        production_arguments = (
            expected_contract_hash,
            expected_ledger_head_hash,
            risk_tier_authority,
        )
        if strategy_nav is None:
            if any(value is not None for value in production_arguments):
                raise ValueError(
                    "production risk bindings require a StrategyNavSnapshot"
                )
            return
        if not isinstance(strategy_nav, StrategyNavSnapshot):
            raise TypeError("strategy_nav must be a StrategyNavSnapshot")
        if not strategy_nav.valid or strategy_nav.strategy_nav is None:
            raise ValueError("production risk requires a valid Strategy NAV snapshot")
        _positive_decimal(strategy_nav.strategy_nav, "strategy_nav.strategy_nav")
        if canonical_hash(strategy_nav.hash_payload()) != strategy_nav.content_hash:
            raise ValueError("Strategy NAV snapshot content hash mismatch")
        expected_contract = _hash(expected_contract_hash, "expected_contract_hash")
        expected_head = _hash(
            expected_ledger_head_hash,
            "expected_ledger_head_hash",
        )
        if strategy_nav.contract_hash != expected_contract:
            raise ValueError("Strategy NAV contract hash does not match risk binding")
        if strategy_nav.ledger_head_hash != expected_head:
            raise ValueError("Strategy NAV ledger head does not match risk binding")
        authority = risk_tier_authority or RiskTierAuthority.normal(expected_contract)
        if not isinstance(authority, RiskTierAuthority):
            raise TypeError("risk_tier_authority must be RiskTierAuthority")
        if authority.risk_contract_hash != expected_contract:
            raise ValueError("risk authority contract hash does not match Strategy NAV")
        self.risk_tier_authority = authority

    @property
    def production_bound(self) -> bool:
        return self._production_bound

    @property
    def a_grade_approved(self) -> bool:
        return bool(
            self._production_bound
            and self.risk_tier_authority is not None
            and self.risk_tier_authority.a_grade_approved
        )

    def assess(
        self,
        candidate: StrategyCandidate,
        *,
        account_equity: Decimal | None = None,
        open_combinations: int = 0,
    ) -> RiskAssessment:
        if not isinstance(candidate, StrategyCandidate):
            raise TypeError("candidate must be a StrategyCandidate")
        if self._production_bound:
            if account_equity is not None:
                raise ValueError(
                    "account_equity is not a production risk input; use Strategy NAV"
                )
            assert self.strategy_nav is not None
            assert self.strategy_nav.strategy_nav is not None
            equity = self.strategy_nav.strategy_nav
        else:
            if account_equity is None:
                raise ValueError(
                    "research risk assessment requires explicit account_equity"
                )
            equity = _positive_decimal(account_equity, "account_equity")
        if isinstance(open_combinations, bool) or not isinstance(open_combinations, int):
            raise TypeError("open_combinations must be an integer")
        if open_combinations < 0:
            raise ValueError("open_combinations cannot be negative")

        payoff = analyze_expiration_payoff(candidate)
        a_grade_authorized = (
            candidate.risk_tier is CandidateRiskTier.VALIDATED_A_GRADE
            and (
                (
                    self._production_bound
                    and self.risk_tier_authority is not None
                    and self.risk_tier_authority.a_grade_approved
                )
                or not self._production_bound
            )
        )
        allowed = (
            self.policy.validated_risk_fraction
            if a_grade_authorized
            else self.policy.normal_risk_fraction
        )
        rejection_set = set(payoff.rejections)
        risk_fraction: Decimal | None = None
        if payoff.max_loss is None:
            if not rejection_set:
                rejection_set.add(RiskRejection.UNKNOWN_MAX_LOSS)
        else:
            with localcontext() as context:
                context.prec = 50
                risk_fraction = payoff.max_loss / equity
            if risk_fraction >= self.policy.hard_reject_fraction:
                rejection_set.add(RiskRejection.HARD_RISK_LIMIT_REACHED)
            elif risk_fraction > allowed:
                rejection_set.add(
                    RiskRejection.VALIDATED_RISK_LIMIT_EXCEEDED
                    if a_grade_authorized
                    else RiskRejection.NORMAL_RISK_LIMIT_EXCEEDED
                )
        if open_combinations >= self.policy.max_open_combinations:
            rejection_set.add(RiskRejection.MAX_OPEN_COMBINATIONS)

        rejections = tuple(sorted(rejection_set, key=lambda item: item.value))
        return RiskAssessment(
            candidate=candidate,
            payoff=payoff,
            approved=not rejections,
            account_equity=equity,
            risk_fraction=risk_fraction,
            allowed_risk_fraction=allowed,
            rejections=rejections,
            production_bound=self._production_bound,
            strategy_nav_snapshot_hash=(
                None if self.strategy_nav is None else self.strategy_nav.content_hash
            ),
            strategy_nav_contract_hash=(
                None if self.strategy_nav is None else self.strategy_nav.contract_hash
            ),
            ledger_head_hash=(
                None if self.strategy_nav is None else self.strategy_nav.ledger_head_hash
            ),
            risk_authority_version=(
                None
                if self.risk_tier_authority is None
                else self.risk_tier_authority.version
            ),
            risk_authority_marker_hash=(
                None
                if self.risk_tier_authority is None
                else self.risk_tier_authority.risk_authority_marker_hash
            ),
        )


def _hash(value: object, field_name: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError(f"{field_name} must be a lowercase SHA-256 hash")
    return value


__all__ = ["RiskAssessment", "RiskEngine", "RiskPolicy"]
