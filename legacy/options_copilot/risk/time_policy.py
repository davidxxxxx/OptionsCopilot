"""Locked US-options entry DTE and position-management time policy."""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
import hmac
import re

from options_copilot.market import (
    CalendarStatus,
    DEFAULT_MAXIMUM_CALENDAR_AGE_SECONDS,
    US_OPTIONS_TIMEZONE,
    UsOptionsCalendarSnapshot,
)
from options_copilot.storage.canonical import canonical_hash, datetime_text, utc_datetime


DTE_EXCEPTION_SCHEMA = "options_copilot.risk.dte_exception.v1"
TIME_POLICY_VERSION = "v1"
PERMANENT_MINIMUM_ENTRY_DTE = 7
NORMAL_MINIMUM_ENTRY_DTE = 14
NORMAL_MAXIMUM_ENTRY_DTE = 35
_HASH_RE = re.compile(r"[0-9a-f]{64}\Z")


@dataclass(frozen=True, slots=True)
class DteEntryExceptionAuthority:
    approved: bool
    rejection_reasons: tuple[str, ...]
    version: str | None = None
    sequence: int | None = None
    actor: str | None = None
    reason: str | None = None
    proposal_id: str | None = None
    expiration: date | None = None
    policy_hash: str | None = None
    approved_at: datetime | None = None
    expires_at: datetime | None = None
    content_hash: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "rejection_reasons",
            tuple(sorted(set(self.rejection_reasons))),
        )
        for name in ("approved_at", "expires_at"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, utc_datetime(value, field=name))
        if self.approved:
            required = (
                self.version,
                self.sequence,
                self.actor,
                self.reason,
                self.proposal_id,
                self.expiration,
                self.policy_hash,
                self.approved_at,
                self.expires_at,
                self.content_hash,
            )
            if any(value is None for value in required):
                raise ValueError("approved DTE exception lacks immutable bindings")
            if self.rejection_reasons:
                raise ValueError("approved DTE exception cannot have rejection reasons")

    @classmethod
    def resolve(
        cls,
        *,
        marker: Mapping[str, object] | None,
        expected_policy_hash: str,
        expected_proposal_id: str,
        expected_expiration: date,
        asof: datetime,
    ) -> "DteEntryExceptionAuthority":
        checked_at = utc_datetime(asof, field="asof")
        expected_policy = _digest(expected_policy_hash, "expected_policy_hash")
        proposal_id = _nonblank(expected_proposal_id, "expected_proposal_id")
        if not isinstance(expected_expiration, date) or isinstance(
            expected_expiration, datetime
        ):
            raise TypeError("expected_expiration must be a date")
        if not isinstance(marker, Mapping):
            return cls(False, ("DTE_EXCEPTION_MISSING",))

        reasons: set[str] = set()
        required_fields = {
            "schema",
            "version",
            "sequence",
            "append_only",
            "decision",
            "actor",
            "reason",
            "proposal_id",
            "expiration",
            "policy_hash",
            "approved_at",
            "expires_at",
            "revoked",
            "previous_marker_hash",
            "governance_signature",
            "content_hash",
        }
        if set(marker) != required_fields:
            reasons.add("DTE_EXCEPTION_FIELDS_INVALID")
        version = marker.get("version")
        if version != TIME_POLICY_VERSION:
            reasons.add("DTE_EXCEPTION_VERSION_INVALID")
            version = None
        sequence = marker.get("sequence")
        if isinstance(sequence, bool) or not isinstance(sequence, int) or sequence < 1:
            reasons.add("DTE_EXCEPTION_SEQUENCE_INVALID")
            sequence = None
        if marker.get("schema") != DTE_EXCEPTION_SCHEMA:
            reasons.add("DTE_EXCEPTION_SCHEMA_INVALID")
        if marker.get("append_only") is not True:
            reasons.add("DTE_EXCEPTION_NOT_APPEND_ONLY")
        if marker.get("decision") != "APPROVE_7_13_DTE_ENTRY":
            reasons.add("DTE_EXCEPTION_DECISION_INVALID")
        actor = marker.get("actor")
        if not isinstance(actor, str) or not actor.startswith("human:"):
            reasons.add("DTE_EXCEPTION_HUMAN_ACTOR_REQUIRED")
            actor = None
        reason = marker.get("reason")
        if not isinstance(reason, str) or not reason.strip():
            reasons.add("DTE_EXCEPTION_REASON_REQUIRED")
            reason = None
        marker_proposal = marker.get("proposal_id")
        if marker_proposal != proposal_id:
            reasons.add("DTE_EXCEPTION_PROPOSAL_MISMATCH")
        try:
            marker_expiration = _date(marker.get("expiration"), "expiration")
        except (TypeError, ValueError):
            marker_expiration = None
            reasons.add("DTE_EXCEPTION_EXPIRATION_INVALID")
        if marker_expiration != expected_expiration:
            reasons.add("DTE_EXCEPTION_EXPIRATION_MISMATCH")
        marker_policy = marker.get("policy_hash")
        if marker_policy != expected_policy:
            reasons.add("DTE_EXCEPTION_POLICY_MISMATCH")
        if marker.get("revoked") is not False:
            reasons.add("DTE_EXCEPTION_REVOKED")
        previous_hash = marker.get("previous_marker_hash")
        if previous_hash is not None and (
            not isinstance(previous_hash, str) or _HASH_RE.fullmatch(previous_hash) is None
        ):
            reasons.add("DTE_EXCEPTION_PREVIOUS_HASH_INVALID")
        if sequence == 1 and previous_hash is not None:
            reasons.add("DTE_EXCEPTION_CHAIN_INVALID")
        if isinstance(sequence, int) and sequence > 1 and previous_hash is None:
            reasons.add("DTE_EXCEPTION_CHAIN_INVALID")
        try:
            approved_at = _timestamp(marker.get("approved_at"), "approved_at")
            expires_at = _timestamp(marker.get("expires_at"), "expires_at")
        except (TypeError, ValueError):
            approved_at = None
            expires_at = None
            reasons.add("DTE_EXCEPTION_TIME_INVALID")
        if approved_at is not None and approved_at > checked_at:
            reasons.add("DTE_EXCEPTION_NOT_YET_APPROVED")
        if expires_at is not None and checked_at >= expires_at:
            reasons.add("DTE_EXCEPTION_EXPIRED")
        if approved_at is not None and expires_at is not None and expires_at <= approved_at:
            reasons.add("DTE_EXCEPTION_TIME_ORDER_INVALID")

        marker_dict = dict(marker)
        signature = marker_dict.pop("governance_signature", None)
        content_hash = marker_dict.pop("content_hash", None)
        if not isinstance(signature, str) or not hmac.compare_digest(
            signature,
            canonical_hash(marker_dict),
        ):
            reasons.add("DTE_EXCEPTION_SIGNATURE_INVALID")
        marker_with_signature = dict(marker_dict)
        marker_with_signature["governance_signature"] = signature
        if not isinstance(content_hash, str) or not hmac.compare_digest(
            content_hash,
            canonical_hash(marker_with_signature),
        ):
            reasons.add("DTE_EXCEPTION_CONTENT_HASH_INVALID")

        if reasons:
            return cls(False, tuple(reasons))
        assert isinstance(version, str)
        assert isinstance(sequence, int)
        assert isinstance(actor, str)
        assert isinstance(reason, str)
        assert marker_expiration is not None
        assert approved_at is not None and expires_at is not None
        assert isinstance(content_hash, str)
        return cls(
            approved=True,
            rejection_reasons=(),
            version=version,
            sequence=sequence,
            actor=actor,
            reason=reason.strip(),
            proposal_id=proposal_id,
            expiration=marker_expiration,
            policy_hash=expected_policy,
            approved_at=approved_at,
            expires_at=expires_at,
            content_hash=content_hash,
        )


@dataclass(frozen=True, slots=True)
class P2ManagementTransitionProof:
    valid: bool
    rejection_reasons: tuple[str, ...]
    position_key: str | None = None
    action: str | None = None
    current_quantity: Decimal | None = None
    proposed_quantity: Decimal | None = None
    current_max_loss_usd: Decimal | None = None
    proposed_max_loss_usd: Decimal | None = None
    current_capital_at_risk_usd: Decimal | None = None
    proposed_capital_at_risk_usd: Decimal | None = None
    verified_at: datetime | None = None
    content_hash: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "rejection_reasons",
            tuple(sorted(set(self.rejection_reasons))),
        )
        if self.verified_at is not None:
            object.__setattr__(
                self,
                "verified_at",
                utc_datetime(self.verified_at, field="verified_at"),
            )

    @classmethod
    def resolve(cls, payload: Mapping[str, object]) -> "P2ManagementTransitionProof":
        if not isinstance(payload, Mapping):
            return cls(False, ("P2_TRANSITION_NOT_AN_OBJECT",))
        reasons: set[str] = set()
        expected_fields = {
            "schema",
            "position_key",
            "action",
            "current_quantity",
            "proposed_quantity",
            "current_max_loss_usd",
            "proposed_max_loss_usd",
            "current_capital_at_risk_usd",
            "proposed_capital_at_risk_usd",
            "verified_at",
            "content_hash",
        }
        if set(payload) != expected_fields:
            reasons.add("P2_TRANSITION_FIELDS_INVALID")
        if payload.get("schema") != "options_copilot.p2.management_transition.v1":
            reasons.add("P2_TRANSITION_SCHEMA_INVALID")
        try:
            position_key = _nonblank(payload.get("position_key"), "position_key")
        except (TypeError, ValueError):
            position_key = None
            reasons.add("P2_TRANSITION_POSITION_INVALID")
        action = payload.get("action")
        if action not in {"CLOSE", "REDUCE"}:
            action = None
            reasons.add("P2_TRANSITION_ACTION_INVALID")
        try:
            current_quantity = _decimal(payload.get("current_quantity"), "current_quantity")
            proposed_quantity = _decimal(
                payload.get("proposed_quantity"),
                "proposed_quantity",
            )
            current_max_loss = _nonnegative_decimal(
                payload.get("current_max_loss_usd"),
                "current_max_loss_usd",
            )
            proposed_max_loss = _nonnegative_decimal(
                payload.get("proposed_max_loss_usd"),
                "proposed_max_loss_usd",
            )
            current_capital = _nonnegative_decimal(
                payload.get("current_capital_at_risk_usd"),
                "current_capital_at_risk_usd",
            )
            proposed_capital = _nonnegative_decimal(
                payload.get("proposed_capital_at_risk_usd"),
                "proposed_capital_at_risk_usd",
            )
        except (TypeError, ValueError):
            current_quantity = None
            proposed_quantity = None
            current_max_loss = None
            proposed_max_loss = None
            current_capital = None
            proposed_capital = None
            reasons.add("P2_TRANSITION_NUMERIC_INVALID")
        try:
            verified_at = _timestamp(payload.get("verified_at"), "verified_at")
        except (TypeError, ValueError):
            verified_at = None
            reasons.add("P2_TRANSITION_TIME_INVALID")

        if current_quantity is not None and proposed_quantity is not None:
            if current_quantity == 0:
                reasons.add("P2_TRANSITION_CURRENT_POSITION_ZERO")
            if current_quantity * proposed_quantity < 0:
                reasons.add("P2_TRANSITION_CROSSES_ZERO")
            if abs(proposed_quantity) >= abs(current_quantity):
                reasons.add("P2_TRANSITION_NOT_STRICT_REDUCTION")
            expected_action = "CLOSE" if proposed_quantity == 0 else "REDUCE"
            if action is not None and action != expected_action:
                reasons.add("P2_TRANSITION_ACTION_MISMATCH")
        if current_max_loss is not None and proposed_max_loss is not None:
            if proposed_max_loss > current_max_loss:
                reasons.add("P2_TRANSITION_MAX_LOSS_INCREASED")
        if current_capital is not None and proposed_capital is not None:
            if proposed_capital > current_capital:
                reasons.add("P2_TRANSITION_CAPITAL_INCREASED")

        content_hash = payload.get("content_hash")
        unsigned = dict(payload)
        unsigned.pop("content_hash", None)
        if not isinstance(content_hash, str) or not hmac.compare_digest(
            content_hash,
            canonical_hash(unsigned),
        ):
            reasons.add("P2_TRANSITION_CONTENT_HASH_INVALID")
        if reasons:
            return cls(False, tuple(reasons))
        assert position_key is not None
        assert action is not None
        assert current_quantity is not None and proposed_quantity is not None
        assert current_max_loss is not None and proposed_max_loss is not None
        assert current_capital is not None and proposed_capital is not None
        assert verified_at is not None
        assert isinstance(content_hash, str)
        return cls(
            valid=True,
            rejection_reasons=(),
            position_key=position_key,
            action=action,
            current_quantity=current_quantity,
            proposed_quantity=proposed_quantity,
            current_max_loss_usd=current_max_loss,
            proposed_max_loss_usd=proposed_max_loss,
            current_capital_at_risk_usd=current_capital,
            proposed_capital_at_risk_usd=proposed_capital,
            verified_at=verified_at,
            content_hash=content_hash,
        )


@dataclass(frozen=True, slots=True)
class TimePolicyDecision:
    mode: str
    allowed: bool
    reason_codes: tuple[str, ...]
    evaluated_at_utc: datetime
    et_trading_date: date
    expiration: date
    dte: int
    policy_version: str
    policy_hash: str
    calendar_hash: str | None = None
    calendar_source_hash: str | None = None
    exception_hash: str | None = None
    transition_proof_hash: str | None = None
    session_open_utc: datetime | None = None
    session_close_utc: datetime | None = None
    decision_hash: str = "0" * 64

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "evaluated_at_utc",
            utc_datetime(self.evaluated_at_utc, field="evaluated_at_utc"),
        )
        object.__setattr__(
            self,
            "reason_codes",
            tuple(sorted(set(self.reason_codes))),
        )
        for name in ("session_open_utc", "session_close_utc"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, utc_datetime(value, field=name))
        for name in (
            "policy_hash",
            "calendar_hash",
            "calendar_source_hash",
            "exception_hash",
            "transition_proof_hash",
            "decision_hash",
        ):
            value = getattr(self, name)
            if value is not None:
                _digest(value, name)
        if self.allowed and self.reason_codes:
            raise ValueError("allowed time-policy decision cannot have rejection reasons")
        if not self.allowed and not self.reason_codes:
            raise ValueError("rejected time-policy decision requires reason codes")

    def hash_payload(self) -> dict[str, object]:
        return {
            "mode": self.mode,
            "allowed": self.allowed,
            "reason_codes": self.reason_codes,
            "evaluated_at_utc": self.evaluated_at_utc,
            "et_trading_date": self.et_trading_date,
            "expiration": self.expiration,
            "dte": self.dte,
            "policy_version": self.policy_version,
            "policy_hash": self.policy_hash,
            "calendar_hash": self.calendar_hash,
            "calendar_source_hash": self.calendar_source_hash,
            "exception_hash": self.exception_hash,
            "transition_proof_hash": self.transition_proof_hash,
            "session_open_utc": self.session_open_utc,
            "session_close_utc": self.session_close_utc,
        }

    def verify_hash(self) -> bool:
        return canonical_hash(self.hash_payload()) == self.decision_hash

    def as_dict(self) -> dict[str, object]:
        return {
            **self.hash_payload(),
            "evaluated_at_utc": datetime_text(self.evaluated_at_utc),
            "et_trading_date": self.et_trading_date.isoformat(),
            "expiration": self.expiration.isoformat(),
            "session_open_utc": (
                None
                if self.session_open_utc is None
                else datetime_text(self.session_open_utc)
            ),
            "session_close_utc": (
                None
                if self.session_close_utc is None
                else datetime_text(self.session_close_utc)
            ),
            "decision_hash": self.decision_hash,
        }

    @property
    def rejection_message(self) -> str:
        if self.allowed:
            return ""
        if "DTE_BELOW_PERMANENT_FLOOR" in self.reason_codes:
            return f"{self.dte} DTE; permanent minimum is 7"
        if "DTE_EXCEPTION_REQUIRED" in self.reason_codes:
            return f"{self.dte} DTE; signed 7-13 DTE exception is required"
        if "DTE_ABOVE_NORMAL_MAXIMUM" in self.reason_codes:
            return f"{self.dte} DTE; normal maximum is 35"
        return ", ".join(self.reason_codes)


class OptionTimePolicy:
    """Non-configurable 7/14/35 entry policy plus P2 management gate."""

    def __init__(self) -> None:
        self.version = TIME_POLICY_VERSION
        self.policy_hash = canonical_hash(
            {
                "schema": "options_copilot.risk.time_policy.v1",
                "version": self.version,
                "entry": {
                    "permanent_minimum_dte": PERMANENT_MINIMUM_ENTRY_DTE,
                    "normal_minimum_dte": NORMAL_MINIMUM_ENTRY_DTE,
                    "normal_maximum_dte": NORMAL_MAXIMUM_ENTRY_DTE,
                    "exception_band": [7, 13],
                    "exception_authority": "append_only_human_signed",
                },
                "management": {
                    "below_entry_floor": "P2_transition_proof_required",
                    "cross_zero": False,
                    "exposure_increase": False,
                    "risk_increase": False,
                    "capital_increase": False,
                },
            }
        )

    def evaluate_dte(
        self,
        *,
        proposal_id: str,
        expiration: date,
        now: datetime,
        exception_authority: DteEntryExceptionAuthority | None = None,
    ) -> TimePolicyDecision:
        checked_now = utc_datetime(now, field="now")
        proposal = _nonblank(proposal_id, "proposal_id")
        if not isinstance(expiration, date) or isinstance(expiration, datetime):
            raise TypeError("expiration must be a date")
        et_date = checked_now.astimezone(US_OPTIONS_TIMEZONE).date()
        dte = (expiration - et_date).days
        reasons: set[str] = set()
        exception_hash: str | None = None
        if dte < PERMANENT_MINIMUM_ENTRY_DTE:
            reasons.add("DTE_BELOW_PERMANENT_FLOOR")
        elif dte < NORMAL_MINIMUM_ENTRY_DTE:
            if (
                not isinstance(exception_authority, DteEntryExceptionAuthority)
                or not exception_authority.approved
                or exception_authority.policy_hash != self.policy_hash
                or exception_authority.proposal_id != proposal
                or exception_authority.expiration != expiration
                or exception_authority.approved_at is None
                or exception_authority.expires_at is None
                or not (
                    exception_authority.approved_at
                    <= checked_now
                    < exception_authority.expires_at
                )
                or exception_authority.content_hash is None
            ):
                reasons.add("DTE_EXCEPTION_REQUIRED")
            else:
                exception_hash = exception_authority.content_hash
        elif dte > NORMAL_MAXIMUM_ENTRY_DTE:
            reasons.add("DTE_ABOVE_NORMAL_MAXIMUM")
        return _decision(
            mode="ENTRY_DTE",
            allowed=not reasons,
            reasons=reasons,
            evaluated_at=checked_now,
            et_trading_date=et_date,
            expiration=expiration,
            dte=dte,
            policy=self,
            exception_hash=exception_hash,
        )

    def evaluate_entry(
        self,
        *,
        proposal_id: str,
        expiration: date,
        now: datetime,
        calendar: UsOptionsCalendarSnapshot,
        exception_authority: DteEntryExceptionAuthority | None = None,
    ) -> TimePolicyDecision:
        base = self.evaluate_dte(
            proposal_id=proposal_id,
            expiration=expiration,
            now=now,
            exception_authority=exception_authority,
        )
        reasons = set(base.reason_codes)
        session = None
        calendar_hash = None
        source_hash = None
        if not isinstance(calendar, UsOptionsCalendarSnapshot):
            reasons.add("CALENDAR_EVIDENCE_MISSING")
        else:
            calendar_hash = calendar.calendar_hash
            source_hash = calendar.source_hash
            if not calendar.verify_hash():
                reasons.add("CALENDAR_HASH_INVALID")
            elif calendar.status is not CalendarStatus.READY:
                reasons.add("CALENDAR_DEGRADED")
            else:
                calendar_age = Decimal(
                    str(
                        (
                            base.evaluated_at_utc - calendar.observed_at
                        ).total_seconds()
                    )
                )
                if calendar_age < 0:
                    reasons.add("CALENDAR_OBSERVED_IN_FUTURE")
                elif calendar_age > DEFAULT_MAXIMUM_CALENDAR_AGE_SECONDS:
                    reasons.add("CALENDAR_STALE")
                else:
                    session = calendar.session_at(base.evaluated_at_utc)
                    if session is None:
                        reasons.add("US_OPTIONS_SESSION_CLOSED")
        et_date = (
            base.et_trading_date if session is None else session.trading_date
        )
        return _decision(
            mode="ENTRY",
            allowed=not reasons,
            reasons=reasons,
            evaluated_at=base.evaluated_at_utc,
            et_trading_date=et_date,
            expiration=expiration,
            dte=base.dte,
            policy=self,
            calendar_hash=calendar_hash,
            calendar_source_hash=source_hash,
            exception_hash=base.exception_hash,
            session_open_utc=None if session is None else session.open_utc,
            session_close_utc=None if session is None else session.close_utc,
        )

    def evaluate_management(
        self,
        *,
        position_key: str,
        expiration: date,
        now: datetime,
        current_quantity: Decimal,
        proposed_quantity: Decimal,
        current_max_loss_usd: Decimal,
        proposed_max_loss_usd: Decimal,
        current_capital_at_risk_usd: Decimal,
        proposed_capital_at_risk_usd: Decimal,
        transition_proof: P2ManagementTransitionProof | None = None,
    ) -> TimePolicyDecision:
        checked_now = utc_datetime(now, field="now")
        key = _nonblank(position_key, "position_key")
        if not isinstance(expiration, date) or isinstance(expiration, datetime):
            raise TypeError("expiration must be a date")
        current = _decimal(current_quantity, "current_quantity")
        proposed = _decimal(proposed_quantity, "proposed_quantity")
        current_loss = _nonnegative_decimal(current_max_loss_usd, "current_max_loss_usd")
        proposed_loss = _nonnegative_decimal(
            proposed_max_loss_usd,
            "proposed_max_loss_usd",
        )
        current_capital = _nonnegative_decimal(
            current_capital_at_risk_usd,
            "current_capital_at_risk_usd",
        )
        proposed_capital = _nonnegative_decimal(
            proposed_capital_at_risk_usd,
            "proposed_capital_at_risk_usd",
        )
        et_date = checked_now.astimezone(US_OPTIONS_TIMEZONE).date()
        dte = (expiration - et_date).days
        reasons: set[str] = set()
        if current == 0:
            reasons.add("MANAGEMENT_CURRENT_POSITION_ZERO")
        if current * proposed < 0:
            reasons.add("MANAGEMENT_CROSSES_ZERO")
        if abs(proposed) >= abs(current):
            reasons.add("MANAGEMENT_NOT_STRICT_REDUCTION")
        if proposed_loss > current_loss:
            reasons.add("MANAGEMENT_MAX_LOSS_INCREASED")
        if proposed_capital > current_capital:
            reasons.add("MANAGEMENT_CAPITAL_INCREASED")
        proof_hash: str | None = None
        if dte < NORMAL_MINIMUM_ENTRY_DTE:
            if (
                not isinstance(transition_proof, P2ManagementTransitionProof)
                or not transition_proof.valid
                or transition_proof.position_key != key
                or transition_proof.current_quantity != current
                or transition_proof.proposed_quantity != proposed
                or transition_proof.current_max_loss_usd != current_loss
                or transition_proof.proposed_max_loss_usd != proposed_loss
                or transition_proof.current_capital_at_risk_usd != current_capital
                or transition_proof.proposed_capital_at_risk_usd != proposed_capital
                or transition_proof.verified_at is None
                or transition_proof.verified_at > checked_now
                or Decimal(
                    str(
                        (checked_now - transition_proof.verified_at).total_seconds()
                    )
                )
                > Decimal("5")
                or transition_proof.content_hash is None
            ):
                reasons.add("P2_MANAGEMENT_TRANSITION_PROOF_REQUIRED")
            else:
                proof_hash = transition_proof.content_hash
        return _decision(
            mode="MANAGEMENT",
            allowed=not reasons,
            reasons=reasons,
            evaluated_at=checked_now,
            et_trading_date=et_date,
            expiration=expiration,
            dte=dte,
            policy=self,
            transition_proof_hash=proof_hash,
        )


def _decision(
    *,
    mode: str,
    allowed: bool,
    reasons: object,
    evaluated_at: datetime,
    et_trading_date: date,
    expiration: date,
    dte: int,
    policy: OptionTimePolicy,
    calendar_hash: str | None = None,
    calendar_source_hash: str | None = None,
    exception_hash: str | None = None,
    transition_proof_hash: str | None = None,
    session_open_utc: datetime | None = None,
    session_close_utc: datetime | None = None,
) -> TimePolicyDecision:
    provisional = TimePolicyDecision(
        mode=mode,
        allowed=allowed,
        reason_codes=tuple(str(reason) for reason in reasons),
        evaluated_at_utc=evaluated_at,
        et_trading_date=et_trading_date,
        expiration=expiration,
        dte=dte,
        policy_version=policy.version,
        policy_hash=policy.policy_hash,
        calendar_hash=calendar_hash,
        calendar_source_hash=calendar_source_hash,
        exception_hash=exception_hash,
        transition_proof_hash=transition_proof_hash,
        session_open_utc=session_open_utc,
        session_close_utc=session_close_utc,
    )
    return replace(
        provisional,
        decision_hash=canonical_hash(provisional.hash_payload()),
    )


def _digest(value: object, field: str) -> str:
    if not isinstance(value, str) or _HASH_RE.fullmatch(value) is None:
        raise ValueError(f"{field} must be lowercase SHA-256 hex")
    return value


def _nonblank(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a nonblank string")
    return value.strip()


def _date(value: object, field: str) -> date:
    if isinstance(value, datetime):
        raise TypeError(f"{field} must be a date")
    if isinstance(value, date):
        return value
    if not isinstance(value, str):
        raise TypeError(f"{field} must be an ISO date")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{field} must be an ISO date") from exc


def _timestamp(value: object, field: str) -> datetime:
    if isinstance(value, datetime):
        return utc_datetime(value, field=field)
    if not isinstance(value, str) or not value.strip():
        raise TypeError(f"{field} must be an ISO datetime")
    text = value.strip()
    try:
        parsed = datetime.fromisoformat(
            text[:-1] + "+00:00" if text.endswith(("Z", "z")) else text
        )
    except ValueError as exc:
        raise ValueError(f"{field} must be an ISO datetime") from exc
    return utc_datetime(parsed, field=field)


def _decimal(value: object, field: str) -> Decimal:
    if isinstance(value, bool):
        raise TypeError(f"{field} must be a finite decimal")
    try:
        parsed = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise TypeError(f"{field} must be a finite decimal") from exc
    if not parsed.is_finite():
        raise ValueError(f"{field} must be a finite decimal")
    return parsed


def _nonnegative_decimal(value: object, field: str) -> Decimal:
    parsed = _decimal(value, field)
    if parsed < 0:
        raise ValueError(f"{field} must be nonnegative")
    return parsed


__all__ = [
    "DTE_EXCEPTION_SCHEMA",
    "DteEntryExceptionAuthority",
    "NORMAL_MAXIMUM_ENTRY_DTE",
    "NORMAL_MINIMUM_ENTRY_DTE",
    "OptionTimePolicy",
    "P2ManagementTransitionProof",
    "PERMANENT_MINIMUM_ENTRY_DTE",
    "TIME_POLICY_VERSION",
    "TimePolicyDecision",
]
