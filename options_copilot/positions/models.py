"""Immutable contracts for close/reduce-only option position management.

The broker snapshot remains the authority for the before state.  These models
only carry a normalized view and a canonical proof of a proposed transition;
they do not grant order or instruction authority.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum
import re

from options_copilot.storage.canonical import canonical_hash, utc_datetime


ZERO = Decimal("0")
PROOF_SCHEMA = "options_copilot.position_transition.v1"
_DIGEST_RE = re.compile(r"[0-9a-f]{64}")
_INTEGER_RE = re.compile(r"[+-]?(?:0|[1-9][0-9]*)")


def _strict_integer(value: object, field: str) -> int:
    if isinstance(value, bool) or isinstance(value, float):
        raise TypeError(f"{field} must be an exact integer")
    if isinstance(value, int):
        return value
    if isinstance(value, Decimal):
        if not value.is_finite() or value != value.to_integral_value():
            raise ValueError(f"{field} must be a finite integer")
        return int(value)
    if isinstance(value, str) and _INTEGER_RE.fullmatch(value) is not None:
        return int(value)
    raise TypeError(f"{field} must be an int, integer Decimal, or strict integer string")


def _positive_contract_id(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError("contract_id must be an integer")
    if value <= 0:
        raise ValueError("contract_id must be positive")
    return value


def _nonblank(value: object, field: str, *, uppercase: bool = False) -> str:
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise ValueError(f"{field} must be a nonblank trimmed string")
    return value.upper() if uppercase else value


def _digest(value: object, field: str) -> str:
    if not isinstance(value, str) or _DIGEST_RE.fullmatch(value) is None:
        raise ValueError(f"{field} must be a lowercase 64-character SHA-256 hash")
    return value


def _nonnegative_decimal(value: object, field: str) -> Decimal:
    if not isinstance(value, Decimal):
        raise TypeError(f"{field} must be a Decimal")
    if not value.is_finite() or value < ZERO:
        raise ValueError(f"{field} must be finite and nonnegative")
    return value


class PositionManagementKind(str, Enum):
    """The complete management authority surface.

    There is deliberately no OPEN, ROLL, REVERSE, or generic ADJUST member.
    """

    CLOSE_ALL = "CLOSE_ALL"
    REDUCE_RISK = "REDUCE_RISK"

    @classmethod
    def parse(cls, value: "PositionManagementKind | str") -> "PositionManagementKind":
        if isinstance(value, cls):
            return value
        if not isinstance(value, str):
            raise TypeError("management kind must be a PositionManagementKind or string")
        try:
            return cls(value.strip().upper())
        except ValueError as exc:
            raise ValueError("management kind must be CLOSE_ALL or REDUCE_RISK") from exc


@dataclass(frozen=True, slots=True)
class AuthoritativePosition:
    """One normalized signed option position bound to a P1 snapshot."""

    contract_id: int
    symbol: str
    local_symbol: str
    security_type: str
    currency: str
    exchange: str
    signed_quantity: int
    observed_at: datetime

    def __post_init__(self) -> None:
        object.__setattr__(self, "contract_id", _positive_contract_id(self.contract_id))
        object.__setattr__(self, "symbol", _nonblank(self.symbol, "symbol", uppercase=True))
        object.__setattr__(self, "local_symbol", _nonblank(self.local_symbol, "local_symbol"))
        object.__setattr__(
            self,
            "security_type",
            _nonblank(self.security_type, "security_type", uppercase=True),
        )
        object.__setattr__(
            self,
            "currency",
            _nonblank(self.currency, "currency", uppercase=True),
        )
        object.__setattr__(self, "exchange", _nonblank(self.exchange, "exchange", uppercase=True))
        object.__setattr__(
            self,
            "signed_quantity",
            _strict_integer(self.signed_quantity, "signed_quantity"),
        )
        object.__setattr__(
            self,
            "observed_at",
            utc_datetime(self.observed_at, field="observed_at"),
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "contract_id": self.contract_id,
            "symbol": self.symbol,
            "local_symbol": self.local_symbol,
            "security_type": self.security_type,
            "currency": self.currency,
            "exchange": self.exchange,
            "signed_quantity": self.signed_quantity,
            "observed_at": self.observed_at,
        }


@dataclass(frozen=True, slots=True)
class PositionDelta:
    """Exact signed execution delta for one existing conId."""

    contract_id: int
    signed_quantity_delta: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "contract_id", _positive_contract_id(self.contract_id))
        value = _strict_integer(self.signed_quantity_delta, "signed_quantity_delta")
        if value == 0:
            raise ValueError("signed_quantity_delta cannot be zero")
        object.__setattr__(self, "signed_quantity_delta", value)

    @property
    def quantity_delta(self) -> int:
        return self.signed_quantity_delta

    def as_dict(self) -> dict[str, int]:
        return {
            "contract_id": self.contract_id,
            "signed_quantity_delta": self.signed_quantity_delta,
        }


@dataclass(frozen=True, slots=True)
class PositionRiskMetrics:
    """Exact monetary risk measures supplied by the deterministic risk layer."""

    max_loss_usd: Decimal
    exposure_usd: Decimal
    capital_usage_usd: Decimal

    def __post_init__(self) -> None:
        _nonnegative_decimal(self.max_loss_usd, "max_loss_usd")
        _nonnegative_decimal(self.exposure_usd, "exposure_usd")
        _nonnegative_decimal(self.capital_usage_usd, "capital_usage_usd")

    def as_dict(self) -> dict[str, Decimal]:
        return {
            "max_loss_usd": self.max_loss_usd,
            "exposure_usd": self.exposure_usd,
            "capital_usage_usd": self.capital_usage_usd,
        }


@dataclass(frozen=True, slots=True)
class SecDefBinding:
    contract_id: int
    pre_identity_hash: str
    post_identity_hash: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "contract_id", _positive_contract_id(self.contract_id))
        _digest(self.pre_identity_hash, "pre_identity_hash")
        _digest(self.post_identity_hash, "post_identity_hash")
        if self.pre_identity_hash != self.post_identity_hash:
            raise ValueError("secdef pre/post identity hashes must match")

    @property
    def identity_hash(self) -> str:
        return self.post_identity_hash

    def as_dict(self) -> dict[str, object]:
        return {
            "contract_id": self.contract_id,
            "pre_identity_hash": self.pre_identity_hash,
            "post_identity_hash": self.post_identity_hash,
        }


@dataclass(frozen=True, slots=True)
class CapitalUsageProof:
    """Quantity and monetary invariants for a projected transition."""

    before_capital_usage_usd: Decimal
    after_capital_usage_usd: Decimal
    capital_change_usd: Decimal
    before_gross_contracts: int
    after_gross_contracts: int
    before_net_short_contracts: int
    after_net_short_contracts: int

    def __post_init__(self) -> None:
        _nonnegative_decimal(self.before_capital_usage_usd, "before_capital_usage_usd")
        _nonnegative_decimal(self.after_capital_usage_usd, "after_capital_usage_usd")
        if not isinstance(self.capital_change_usd, Decimal) or not self.capital_change_usd.is_finite():
            raise ValueError("capital_change_usd must be a finite Decimal")
        expected_change = self.after_capital_usage_usd - self.before_capital_usage_usd
        if self.capital_change_usd != expected_change:
            raise ValueError("capital_change_usd does not match before/after values")
        for field in (
            "before_gross_contracts",
            "after_gross_contracts",
            "before_net_short_contracts",
            "after_net_short_contracts",
        ):
            value = getattr(self, field)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{field} must be a nonnegative integer")

    @property
    def non_increasing(self) -> bool:
        return (
            self.after_capital_usage_usd <= self.before_capital_usage_usd
            and self.after_gross_contracts <= self.before_gross_contracts
            and self.after_net_short_contracts <= self.before_net_short_contracts
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "before_capital_usage_usd": self.before_capital_usage_usd,
            "after_capital_usage_usd": self.after_capital_usage_usd,
            "capital_change_usd": self.capital_change_usd,
            "before_gross_contracts": self.before_gross_contracts,
            "after_gross_contracts": self.after_gross_contracts,
            "before_net_short_contracts": self.before_net_short_contracts,
            "after_net_short_contracts": self.after_net_short_contracts,
        }


def _positions_payload(values: tuple[AuthoritativePosition, ...]) -> list[dict[str, object]]:
    return [item.as_dict() for item in sorted(values, key=lambda item: item.contract_id)]


@dataclass(frozen=True, slots=True)
class PositionTransitionProof:
    """Hash-bound proof that a proposal only closes or strictly reduces risk."""

    management_kind: PositionManagementKind
    before_positions: tuple[AuthoritativePosition, ...]
    deltas: tuple[PositionDelta, ...]
    after_positions: tuple[AuthoritativePosition, ...]
    before_risk: PositionRiskMetrics
    after_risk: PositionRiskMetrics
    capital_usage: CapitalUsageProof
    broker_snapshot_hash: str
    positions_state_hash: str
    secdef_bindings: tuple[SecDefBinding, ...]
    quote_batch_id: str
    quote_batch_hash: str
    exit_contract_hash: str
    execution_cost_contract_version: str
    execution_cost_contract_hash: str
    verified_at: datetime
    before_positions_hash: str
    after_positions_hash: str
    proof_hash: str
    schema: str = PROOF_SCHEMA

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "management_kind",
            PositionManagementKind.parse(self.management_kind),
        )
        object.__setattr__(self, "before_positions", tuple(self.before_positions))
        object.__setattr__(self, "deltas", tuple(self.deltas))
        object.__setattr__(self, "after_positions", tuple(self.after_positions))
        object.__setattr__(self, "secdef_bindings", tuple(self.secdef_bindings))
        if not all(isinstance(item, AuthoritativePosition) for item in self.before_positions):
            raise TypeError("before_positions must contain AuthoritativePosition values")
        if not all(isinstance(item, PositionDelta) for item in self.deltas):
            raise TypeError("deltas must contain PositionDelta values")
        if not all(isinstance(item, AuthoritativePosition) for item in self.after_positions):
            raise TypeError("after_positions must contain AuthoritativePosition values")
        if not isinstance(self.before_risk, PositionRiskMetrics) or not isinstance(
            self.after_risk, PositionRiskMetrics
        ):
            raise TypeError("before_risk and after_risk must be PositionRiskMetrics")
        if not isinstance(self.capital_usage, CapitalUsageProof):
            raise TypeError("capital_usage must be a CapitalUsageProof")
        if not all(isinstance(item, SecDefBinding) for item in self.secdef_bindings):
            raise TypeError("secdef_bindings must contain SecDefBinding values")
        for field in (
            "broker_snapshot_hash",
            "positions_state_hash",
            "quote_batch_hash",
            "exit_contract_hash",
            "execution_cost_contract_hash",
            "before_positions_hash",
            "after_positions_hash",
            "proof_hash",
        ):
            _digest(getattr(self, field), field)
        _nonblank(self.quote_batch_id, "quote_batch_id")
        _nonblank(self.execution_cost_contract_version, "execution_cost_contract_version")
        object.__setattr__(
            self,
            "verified_at",
            utc_datetime(self.verified_at, field="verified_at"),
        )
        if self.schema != PROOF_SCHEMA:
            raise ValueError(f"schema must be {PROOF_SCHEMA!r}")
        if self.before_positions_hash != canonical_hash(
            _positions_payload(self.before_positions)
        ):
            raise ValueError("before_positions_hash does not match before_positions")
        if self.after_positions_hash != canonical_hash(
            _positions_payload(self.after_positions)
        ):
            raise ValueError("after_positions_hash does not match after_positions")
        if self.capital_usage.before_capital_usage_usd != self.before_risk.capital_usage_usd:
            raise ValueError("capital proof before value does not match before_risk")
        if self.capital_usage.after_capital_usage_usd != self.after_risk.capital_usage_usd:
            raise ValueError("capital proof after value does not match after_risk")

    @property
    def kind(self) -> PositionManagementKind:
        return self.management_kind

    def hash_payload(self) -> dict[str, object]:
        return {
            "schema": self.schema,
            "management_kind": self.management_kind.value,
            "before_positions": _positions_payload(self.before_positions),
            "deltas": [item.as_dict() for item in self.deltas],
            "after_positions": _positions_payload(self.after_positions),
            "before_risk": self.before_risk.as_dict(),
            "after_risk": self.after_risk.as_dict(),
            "capital_usage": self.capital_usage.as_dict(),
            "broker_snapshot_hash": self.broker_snapshot_hash,
            "positions_state_hash": self.positions_state_hash,
            "secdef_bindings": [item.as_dict() for item in self.secdef_bindings],
            "quote_batch_id": self.quote_batch_id,
            "quote_batch_hash": self.quote_batch_hash,
            "exit_contract_hash": self.exit_contract_hash,
            "execution_cost_contract_version": self.execution_cost_contract_version,
            "execution_cost_contract_hash": self.execution_cost_contract_hash,
            "verified_at": self.verified_at,
            "before_positions_hash": self.before_positions_hash,
            "after_positions_hash": self.after_positions_hash,
        }

    def verify_hash(self) -> bool:
        try:
            return canonical_hash(self.hash_payload()) == self.proof_hash
        except (TypeError, ValueError):
            return False


__all__ = [
    "AuthoritativePosition",
    "CapitalUsageProof",
    "PROOF_SCHEMA",
    "PositionDelta",
    "PositionManagementKind",
    "PositionRiskMetrics",
    "PositionTransitionProof",
    "SecDefBinding",
]
