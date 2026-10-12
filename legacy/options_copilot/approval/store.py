"""Durable, single-use GUI approvals bound to immutable option proposals."""
from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
from pathlib import Path
import re
import secrets
import sqlite3
import threading

from options_copilot.ranking.store import FrozenRankOneAuthorization, RankingStore
from options_copilot.approval.proofs import (
    BROKER_PROOF_SCHEMA,
    STRATEGY_NAV_PROOF_SCHEMA,
    ApprovalProofError,
    normalize_broker_proof,
    normalize_strategy_nav_proof,
)
from options_copilot.storage.canonical import (
    canonical_hash,
    canonical_json,
    datetime_text,
    freeze_json,
    thaw_json,
    utc_datetime,
)


APPROVAL_TTL_SECONDS = 300
APPROVAL_CONFIRMATION_TOKEN = "CREATE_IBKR_REVIEW_ONLY"
DEFAULT_ADVERSE_TOLERANCE_USD = Decimal("5.00")
MAX_ADVERSE_TOLERANCE_USD = Decimal("5.00")
SCHEMA_VERSION = 3
_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,159}\Z")
_REPRICE_ONLY_KEYS = frozenset(
    {
        "ask",
        "bid",
        "estimated_cost_usd",
        "estimated_fill_usd",
        "executable_price",
        "implied_volatility",
        "iv",
        "last",
        "mark",
        "market_price",
        "mid",
        "net_credit_usd",
        "net_debit_usd",
        "open_interest",
        "observed_at",
        "price",
        "quote",
        "quote_asof",
        "quote_snapshot_id",
        "quote_time",
        "quoted_at",
        "snapshot_id",
        "timestamp",
        "volume",
        # These economics must be recomputed from the new executable quotes by
        # the proposal/risk gate.  Structural risk fields such as defined_risk,
        # tier, contracts, ratios, and expirations remain hash-bound.
        "all_in_executable_cost_usd",
        "breakevens",
        "break_even_points",
        "expected_value_before_costs_usd",
        "expected_value_usd",
        "max_loss_usd",
        "max_profit_usd",
        "maximum_loss_usd",
        "maximum_profit_usd",
        "reference_cost_usd",
        "risk_fraction",
    }
)
_CHALLENGE_INSERT_FIELDS = (
    "challenge_id",
    "challenge_response_hash",
    "ranking_snapshot_id",
    "scan_run_id",
    "candidate_id",
    "proposal_hash",
    "candidate_hash",
    "ranking_basis_hash",
    "row_hash",
    "snapshot_hash",
    "current_policy_version",
    "current_policy_hash",
    "policy_authority_marker_hash",
    "cost_version",
    "cost_hash",
    "risk_contract_hash",
    "risk_authority_version",
    "risk_authority_marker_hash",
    "expected_hashes_json",
    "candidate_body_json",
    "proposal_body_json",
    "reference_cost_usd",
    "created_at",
    "expires_at",
    "challenge_hash",
)
_AUTHORITY_BINDING_INSERT_FIELDS = (
    "approval_id",
    "challenge_id",
    "ranking_snapshot_id",
    "scan_run_id",
    "candidate_id",
    "proposal_hash",
    "candidate_hash",
    "ranking_basis_hash",
    "row_hash",
    "snapshot_hash",
    "current_policy_version",
    "current_policy_hash",
    "policy_authority_marker_hash",
    "cost_version",
    "cost_hash",
    "risk_contract_hash",
    "risk_authority_version",
    "risk_authority_marker_hash",
    "expected_hashes_json",
    "candidate_body_json",
    "proposal_body_json",
    "reference_cost_usd",
    "bound_at",
    "binding_hash",
)
_CHALLENGE_PROOF_INSERT_FIELDS = (
    "challenge_id",
    "broker_proof_json",
    "broker_proof_hash",
    "strategy_nav_proof_json",
    "strategy_nav_proof_hash",
    "proof_binding_hash",
)
_AUTHORITY_PROOF_INSERT_FIELDS = (
    "approval_id",
    "challenge_broker_proof_json",
    "challenge_broker_proof_hash",
    "confirm_broker_proof_json",
    "confirm_broker_proof_hash",
    "strategy_nav_proof_json",
    "strategy_nav_proof_hash",
    "proof_binding_hash",
)


class ApprovalError(RuntimeError):
    pass


class NonceReplayError(ApprovalError):
    pass


class ApprovalIdentityConflict(ApprovalError):
    pass


class ActiveApprovalExists(ApprovalError):
    pass


class ApprovalChallengeRejected(ApprovalError):
    pass


class ApprovalAuthorityConflict(ApprovalChallengeRejected):
    pass


@dataclass(frozen=True, slots=True)
class ProposalHashes:
    proposal_hash: str
    material_hash: str
    legs_hash: str
    risk_hash: str


@dataclass(frozen=True, slots=True)
class ApprovalRecord:
    approval_id: str
    sequence: int
    proposal_id: str
    proposal_hash: str
    material_hash: str
    legs_hash: str
    risk_hash: str
    approved_by: str
    approved_at: datetime
    expires_at: datetime
    nonce_hash: str
    adverse_tolerance_usd: Decimal
    reference_cost_usd: Decimal | None
    proposal: Mapping[str, object]

    def __post_init__(self) -> None:
        object.__setattr__(self, "approved_at", utc_datetime(self.approved_at))
        object.__setattr__(self, "expires_at", utc_datetime(self.expires_at))
        frozen = freeze_json(self.proposal)
        assert isinstance(frozen, Mapping)
        object.__setattr__(self, "proposal", frozen)

    def as_dict(self) -> dict[str, object]:
        return {
            "approval_id": self.approval_id,
            "sequence": self.sequence,
            "proposal_id": self.proposal_id,
            "proposal_hash": self.proposal_hash,
            "material_hash": self.material_hash,
            "legs_hash": self.legs_hash,
            "risk_hash": self.risk_hash,
            "approved_by": self.approved_by,
            "approved_at": datetime_text(self.approved_at),
            "expires_at": datetime_text(self.expires_at),
            "nonce_hash": self.nonce_hash,
            "adverse_tolerance_usd": format(self.adverse_tolerance_usd, "f"),
            "reference_cost_usd": (
                None
                if self.reference_cost_usd is None
                else format(self.reference_cost_usd, "f")
            ),
            "proposal": thaw_json(self.proposal),
        }


@dataclass(frozen=True, slots=True)
class ApprovalValidation:
    valid: bool
    reasons: tuple[str, ...]
    checked_at: datetime
    approval: ApprovalRecord | None
    adverse_change_usd: Decimal | None

    @property
    def reason(self) -> str | None:
        return None if not self.reasons else self.reasons[0]


@dataclass(frozen=True, slots=True)
class ApprovalConsumption:
    approval_id: str
    consumed_at: datetime
    execution_hash: str


@dataclass(frozen=True, slots=True)
class ApprovalChallengeRecord:
    challenge_id: str
    sequence: int
    challenge_response_hash: str
    ranking_snapshot_id: str
    scan_run_id: str
    candidate_id: str
    proposal_hash: str
    candidate_hash: str
    ranking_basis_hash: str
    row_hash: str
    snapshot_hash: str
    current_policy_version: str
    current_policy_hash: str
    policy_authority_marker_hash: str
    cost_version: str
    cost_hash: str
    risk_contract_hash: str
    risk_authority_version: str
    risk_authority_marker_hash: str
    expected_hashes: Mapping[str, object]
    candidate_body: Mapping[str, object]
    proposal_body: Mapping[str, object]
    broker_proof: Mapping[str, object]
    strategy_nav_proof: Mapping[str, object]
    reference_cost_usd: Decimal
    created_at: datetime
    expires_at: datetime
    challenge_hash: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "created_at", utc_datetime(self.created_at))
        object.__setattr__(self, "expires_at", utc_datetime(self.expires_at))
        for name in (
            "expected_hashes",
            "candidate_body",
            "proposal_body",
            "broker_proof",
            "strategy_nav_proof",
        ):
            frozen = freeze_json(getattr(self, name))
            if not isinstance(frozen, Mapping):
                raise TypeError(f"{name} must be a mapping")
            object.__setattr__(self, name, frozen)

    def as_dict(self) -> dict[str, object]:
        return {
            "challenge_id": self.challenge_id,
            "ranking_snapshot_id": self.ranking_snapshot_id,
            "candidate_id": self.candidate_id,
            "proposal_hash": self.proposal_hash,
            "candidate_hash": self.candidate_hash,
            "ranking_basis_hash": self.ranking_basis_hash,
            "row_hash": self.row_hash,
            "snapshot_hash": self.snapshot_hash,
            "current_policy_version": self.current_policy_version,
            "current_policy_hash": self.current_policy_hash,
            "policy_authority_marker_hash": self.policy_authority_marker_hash,
            "cost_version": self.cost_version,
            "cost_hash": self.cost_hash,
            "risk_contract_hash": self.risk_contract_hash,
            "risk_authority_version": self.risk_authority_version,
            "risk_authority_marker_hash": self.risk_authority_marker_hash,
            "broker_proof": thaw_json(self.broker_proof),
            "strategy_nav_proof": thaw_json(self.strategy_nav_proof),
            "reference_cost_usd": format(self.reference_cost_usd, "f"),
            "created_at": datetime_text(self.created_at),
            "expires_at": datetime_text(self.expires_at),
            "challenge_hash": self.challenge_hash,
        }


@dataclass(frozen=True, slots=True)
class IssuedApprovalChallenge:
    challenge: ApprovalChallengeRecord
    challenge_response: str

    def as_dict(self) -> dict[str, object]:
        return {**self.challenge.as_dict(), "challenge_response": self.challenge_response}


@dataclass(frozen=True, slots=True)
class ApprovalChallengeConsumption:
    sequence: int
    challenge_id: str
    approval_id: str
    consumed_at: datetime
    confirmation_hash: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "consumed_at", utc_datetime(self.consumed_at))


@dataclass(frozen=True, slots=True)
class ApprovalAuthorityBinding:
    approval_id: str
    challenge_id: str
    ranking_snapshot_id: str
    scan_run_id: str
    candidate_id: str
    proposal_hash: str
    candidate_hash: str
    ranking_basis_hash: str
    row_hash: str
    snapshot_hash: str
    current_policy_version: str
    current_policy_hash: str
    policy_authority_marker_hash: str
    cost_version: str
    cost_hash: str
    risk_contract_hash: str
    risk_authority_version: str
    risk_authority_marker_hash: str
    expected_hashes: Mapping[str, object]
    candidate_body: Mapping[str, object]
    proposal_body: Mapping[str, object]
    challenge_broker_proof: Mapping[str, object]
    confirm_broker_proof: Mapping[str, object]
    strategy_nav_proof: Mapping[str, object]
    reference_cost_usd: Decimal
    bound_at: datetime
    binding_hash: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "bound_at", utc_datetime(self.bound_at))
        for name in (
            "expected_hashes",
            "candidate_body",
            "proposal_body",
            "challenge_broker_proof",
            "confirm_broker_proof",
            "strategy_nav_proof",
        ):
            frozen = freeze_json(getattr(self, name))
            if not isinstance(frozen, Mapping):
                raise TypeError(f"{name} must be a mapping")
            object.__setattr__(self, name, frozen)

    def as_dict(self) -> dict[str, object]:
        return {
            "approval_id": self.approval_id,
            "challenge_id": self.challenge_id,
            "ranking_snapshot_id": self.ranking_snapshot_id,
            "scan_run_id": self.scan_run_id,
            "candidate_id": self.candidate_id,
            "proposal_hash": self.proposal_hash,
            "candidate_hash": self.candidate_hash,
            "ranking_basis_hash": self.ranking_basis_hash,
            "row_hash": self.row_hash,
            "snapshot_hash": self.snapshot_hash,
            "current_policy_version": self.current_policy_version,
            "current_policy_hash": self.current_policy_hash,
            "policy_authority_marker_hash": self.policy_authority_marker_hash,
            "cost_version": self.cost_version,
            "cost_hash": self.cost_hash,
            "risk_contract_hash": self.risk_contract_hash,
            "risk_authority_version": self.risk_authority_version,
            "risk_authority_marker_hash": self.risk_authority_marker_hash,
            "challenge_broker_proof": thaw_json(self.challenge_broker_proof),
            "confirm_broker_proof": thaw_json(self.confirm_broker_proof),
            "strategy_nav_proof": thaw_json(self.strategy_nav_proof),
            "reference_cost_usd": format(self.reference_cost_usd, "f"),
            "bound_at": datetime_text(self.bound_at),
            "binding_hash": self.binding_hash,
        }


@dataclass(frozen=True, slots=True)
class ApprovalConfirmation:
    approval: ApprovalRecord
    consumption: ApprovalChallengeConsumption
    authority_binding: ApprovalAuthorityBinding


class ProposalApprovalStore:
    """SQLite-backed five-minute approval and anti-replay boundary."""

    def __init__(
        self,
        path: str | Path,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._lock = threading.RLock()
        self._closed = False
        self._connection = sqlite3.connect(
            self.path,
            timeout=10.0,
            isolation_level=None,
            check_same_thread=False,
        )
        self._connection.row_factory = sqlite3.Row
        try:
            self._journal_mode = str(
                self._connection.execute("PRAGMA journal_mode=WAL").fetchone()[0]
            ).lower()
            self._connection.execute("PRAGMA synchronous=FULL")
            synchronous = int(
                self._connection.execute("PRAGMA synchronous").fetchone()[0]
            )
            self._synchronous = {0: "off", 1: "normal", 2: "full", 3: "extra"}.get(
                synchronous, str(synchronous)
            )
            self._migrate()
        except BaseException:
            self._connection.close()
            self._closed = True
            raise

    def __enter__(self) -> "ProposalApprovalStore":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    @property
    def journal_mode(self) -> str:
        return self._journal_mode

    @property
    def synchronous(self) -> str:
        return self._synchronous

    @property
    def schema_version(self) -> int:
        self._ensure_open()
        return int(self._connection.execute("PRAGMA user_version").fetchone()[0])

    def close(self) -> None:
        with self._lock:
            if not self._closed:
                self._connection.close()
                self._closed = True

    def record_counts(self) -> dict[str, int]:
        self._ensure_open()
        with self._lock:
            return {
                table: int(
                    self._connection.execute(
                        f'SELECT COUNT(*) FROM "{table}"'
                    ).fetchone()[0]
                )
                for table in (
                    "proposal_approvals",
                    "approval_consumptions",
                    "approval_challenges",
                    "approval_challenge_consumptions",
                    "approval_authority_bindings",
                    "approval_challenge_proofs",
                    "approval_authority_proofs",
                )
            }

    def get_challenge(self, challenge_id: str) -> ApprovalChallengeRecord | None:
        _identifier("challenge_id", challenge_id)
        self._ensure_open()
        with self._lock:
            row = self._connection.execute(
                """
                SELECT challenges.*, proofs.broker_proof_json,
                       proofs.broker_proof_hash,
                       proofs.strategy_nav_proof_json,
                       proofs.strategy_nav_proof_hash,
                       proofs.proof_binding_hash
                FROM approval_challenges AS challenges
                JOIN approval_challenge_proofs AS proofs
                  ON proofs.challenge_id = challenges.challenge_id
                WHERE challenges.challenge_id=?
                """,
                (challenge_id,),
            ).fetchone()
        return None if row is None else _row_to_challenge(row)

    def get_authority_binding(
        self, approval_id: str
    ) -> ApprovalAuthorityBinding | None:
        _identifier("approval_id", approval_id)
        self._ensure_open()
        with self._lock:
            row = self._connection.execute(
                """
                SELECT bindings.*, proofs.challenge_broker_proof_json,
                       proofs.challenge_broker_proof_hash,
                       proofs.confirm_broker_proof_json,
                       proofs.confirm_broker_proof_hash,
                       proofs.strategy_nav_proof_json,
                       proofs.strategy_nav_proof_hash,
                       proofs.proof_binding_hash
                FROM approval_authority_bindings AS bindings
                JOIN approval_authority_proofs AS proofs
                  ON proofs.approval_id = bindings.approval_id
                WHERE bindings.approval_id=?
                """,
                (approval_id,),
            ).fetchone()
        return None if row is None else _row_to_authority_binding(row)

    def create_challenge(
        self,
        ranking_snapshot_id: str,
        candidate_id: str,
        *,
        ranking_store: RankingStore,
        policy_resolver: object,
        risk_authority_resolver: object,
        execution_cost_contract: object,
        broker_proof: Mapping[str, object],
        strategy_nav_proof: Mapping[str, object],
        strategy_nav_source: object,
        strategy_nav_snapshot: object,
        now: datetime | None = None,
    ) -> IssuedApprovalChallenge:
        """Persist a one-time challenge from server-side rank-one truth only."""

        _identifier("ranking_snapshot_id", ranking_snapshot_id)
        _identifier("candidate_id", candidate_id)
        at = self._trusted_now(now, field="now")

        def persist_under_nav(
            authorization: FrozenRankOneAuthorization,
        ) -> IssuedApprovalChallenge:
            try:
                hashes, frozen_proposal = proposal_hashes(
                    authorization.proposal_body
                )
                reference = _executable_cost(frozen_proposal)
            except (TypeError, ValueError) as exc:
                raise ApprovalAuthorityConflict(
                    "frozen rank-one proposal is not approval-ready"
                ) from exc
            if (
                hashes.proposal_hash != authorization.proposal_hash
                or reference is None
                or _proposal_identity(authorization.proposal_body)
                != authorization.candidate_id
            ):
                raise ApprovalAuthorityConflict(
                    "frozen rank-one proposal binding is invalid"
                )
            challenge_id = f"challenge-{secrets.token_urlsafe(18)}"
            challenge_response = secrets.token_urlsafe(32)
            response_hash = hashlib.sha256(
                challenge_response.encode("utf-8")
            ).hexdigest()
            expires_at = at + timedelta(seconds=APPROVAL_TTL_SECONDS)
            values = _challenge_values(
                challenge_id=challenge_id,
                challenge_response_hash=response_hash,
                authorization=authorization,
                reference_cost_usd=reference,
                created_at=at,
                expires_at=expires_at,
            )
            with self._transaction():
                proof_checked_at = self._trusted_now(
                    None, field="broker_proof_checked_at"
                )
                try:
                    normalized_broker_proof = normalize_broker_proof(
                        broker_proof,
                        ranking_snapshot_id=authorization.ranking_snapshot_id,
                        candidate_id=authorization.candidate_id,
                        proposal_hash=authorization.proposal_hash,
                        candidate_body=authorization.candidate_body,
                        checked_at=proof_checked_at,
                    )
                    normalized_nav_proof = normalize_strategy_nav_proof(
                        strategy_nav_proof,
                        candidate_body=authorization.candidate_body,
                        broker_proof=normalized_broker_proof,
                        checked_at=proof_checked_at,
                    )
                except ApprovalProofError as exc:
                    raise ApprovalChallengeRejected(str(exc)) from exc
                proof_values = _challenge_proof_values(
                    challenge_id=challenge_id,
                    broker_proof=normalized_broker_proof,
                    strategy_nav_proof=normalized_nav_proof,
                )
                self._connection.execute(
                    """
                    INSERT INTO approval_challenges(
                        challenge_id, challenge_response_hash,
                        ranking_snapshot_id, scan_run_id, candidate_id,
                        proposal_hash, candidate_hash, ranking_basis_hash,
                        row_hash, snapshot_hash, current_policy_version,
                        current_policy_hash, policy_authority_marker_hash,
                        cost_version, cost_hash, risk_contract_hash,
                        risk_authority_version, risk_authority_marker_hash,
                        expected_hashes_json, candidate_body_json,
                        proposal_body_json, reference_cost_usd, created_at,
                        expires_at, challenge_hash
                    ) VALUES (
                        ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
                    )
                    """,
                    tuple(values[name] for name in _CHALLENGE_INSERT_FIELDS),
                )
                self._connection.execute(
                    """
                    INSERT INTO approval_challenge_proofs(
                        challenge_id, broker_proof_json, broker_proof_hash,
                        strategy_nav_proof_json, strategy_nav_proof_hash,
                        proof_binding_hash
                    ) VALUES (?,?,?,?,?,?)
                    """,
                    tuple(
                        proof_values[name]
                        for name in _CHALLENGE_PROOF_INSERT_FIELDS
                    ),
                )
                row = self._connection.execute(
                    """
                    SELECT challenges.*, proofs.broker_proof_json,
                           proofs.broker_proof_hash,
                           proofs.strategy_nav_proof_json,
                           proofs.strategy_nav_proof_hash,
                           proofs.proof_binding_hash
                    FROM approval_challenges AS challenges
                    JOIN approval_challenge_proofs AS proofs
                      ON proofs.challenge_id = challenges.challenge_id
                    WHERE challenges.challenge_id=?
                    """,
                    (challenge_id,),
                ).fetchone()
                if row is None:
                    raise ApprovalError("inserted approval challenge is missing")
                challenge = _row_to_challenge(row)
            return IssuedApprovalChallenge(
                challenge=challenge,
                challenge_response=challenge_response,
            )

        def persist(
            authorization: FrozenRankOneAuthorization,
        ) -> IssuedApprovalChallenge:
            result = _guard_strategy_nav_current(
                strategy_nav_source,
                strategy_nav_snapshot,
                callback=lambda: persist_under_nav(authorization),
            )
            if result is None:
                raise ApprovalAuthorityConflict(
                    "current Strategy NAV authorization is unavailable"
                )
            return result

        result = ranking_store.guard_frozen_rank_one(
            ranking_snapshot_id,
            candidate_id,
            policy_resolver=policy_resolver,
            risk_authority_resolver=risk_authority_resolver,
            execution_cost_contract=execution_cost_contract,
            callback=persist,
            now=at,
        )
        if result is None:
            raise ApprovalAuthorityConflict(
                "current rank-one authorization is unavailable"
            )
        return result

    def confirm_challenge(
        self,
        challenge_id: str,
        *,
        challenge_response: str,
        risk_acknowledged: bool,
        second_confirmation: bool,
        confirmation_token: str,
        ranking_store: RankingStore,
        policy_resolver: object,
        risk_authority_resolver: object,
        execution_cost_contract: object,
        broker_proof: Mapping[str, object],
        strategy_nav_proof: Mapping[str, object],
        strategy_nav_source: object,
        strategy_nav_snapshot: object,
        approved_by: str = "options_copilot_gui",
        adverse_tolerance_usd: Decimal | int | float | str = DEFAULT_ADVERSE_TOLERANCE_USD,
        now: datetime | None = None,
    ) -> ApprovalConfirmation:
        """Consume one challenge and append its bound approval atomically."""

        _identifier("challenge_id", challenge_id)
        _identifier("approved_by", approved_by)
        if (
            risk_acknowledged is not True
            or second_confirmation is not True
            or confirmation_token != APPROVAL_CONFIRMATION_TOKEN
        ):
            raise ApprovalChallengeRejected(
                "explicit review-only confirmation is required"
            )
        if not isinstance(challenge_response, str) or not 24 <= len(
            challenge_response
        ) <= 256:
            raise ApprovalChallengeRejected("challenge response is invalid")
        tolerance = _money("adverse_tolerance_usd", adverse_tolerance_usd)
        if tolerance <= 0 or tolerance > MAX_ADVERSE_TOLERANCE_USD:
            raise ValueError(
                "adverse_tolerance_usd must be positive and at most 5.00"
            )
        at = self._trusted_now(now, field="now")
        challenge = self.get_challenge(challenge_id)
        if challenge is None:
            raise ApprovalChallengeRejected("approval challenge was not found")

        def confirm_under_nav(
            authorization: FrozenRankOneAuthorization,
        ) -> ApprovalConfirmation:
            with self._transaction():
                proof_checked_at = self._trusted_now(
                    None, field="broker_proof_checked_at"
                )
                row = self._connection.execute(
                    """
                    SELECT challenges.*, proofs.broker_proof_json,
                           proofs.broker_proof_hash,
                           proofs.strategy_nav_proof_json,
                           proofs.strategy_nav_proof_hash,
                           proofs.proof_binding_hash
                    FROM approval_challenges AS challenges
                    JOIN approval_challenge_proofs AS proofs
                      ON proofs.challenge_id = challenges.challenge_id
                    WHERE challenges.challenge_id=?
                    """,
                    (challenge_id,),
                ).fetchone()
                if row is None:
                    raise ApprovalChallengeRejected(
                        "approval challenge was not found"
                    )
                current = _row_to_challenge(row)
                latest = self._connection.execute(
                    """
                    SELECT challenge_id FROM approval_challenges
                    WHERE ranking_snapshot_id=? AND candidate_id=?
                    ORDER BY sequence DESC LIMIT 1
                    """,
                    (current.ranking_snapshot_id, current.candidate_id),
                ).fetchone()
                if latest is None or str(latest[0]) != challenge_id:
                    raise ApprovalChallengeRejected(
                        "approval challenge was superseded"
                    )
                if at < current.created_at or at >= current.expires_at:
                    raise ApprovalChallengeRejected("approval challenge expired")
                supplied_hash = hashlib.sha256(
                    challenge_response.encode("utf-8")
                ).hexdigest()
                if not secrets.compare_digest(
                    supplied_hash, current.challenge_response_hash
                ):
                    raise ApprovalChallengeRejected(
                        "challenge response does not match"
                    )
                consumed = self._connection.execute(
                    "SELECT 1 FROM approval_challenge_consumptions "
                    "WHERE challenge_id=?",
                    (challenge_id,),
                ).fetchone()
                if consumed is not None:
                    raise ApprovalChallengeRejected(
                        "approval challenge was already consumed"
                    )
                if not _challenge_matches_authorization(current, authorization):
                    raise ApprovalAuthorityConflict(
                        "approval challenge authority is stale"
                    )
                hashes, frozen_proposal = proposal_hashes(
                    authorization.proposal_body
                )
                reference = _executable_cost(frozen_proposal)
                if (
                    reference is None
                    or reference != current.reference_cost_usd
                    or hashes.proposal_hash != current.proposal_hash
                ):
                    raise ApprovalAuthorityConflict(
                        "current executable proposal cost changed"
                    )
                try:
                    normalized_broker_proof = normalize_broker_proof(
                        broker_proof,
                        ranking_snapshot_id=authorization.ranking_snapshot_id,
                        candidate_id=authorization.candidate_id,
                        proposal_hash=authorization.proposal_hash,
                        candidate_body=authorization.candidate_body,
                        checked_at=proof_checked_at,
                    )
                    normalized_nav_proof = normalize_strategy_nav_proof(
                        strategy_nav_proof,
                        candidate_body=authorization.candidate_body,
                        broker_proof=normalized_broker_proof,
                        checked_at=proof_checked_at,
                    )
                except ApprovalProofError as exc:
                    raise ApprovalChallengeRejected(str(exc)) from exc
                stable_nav_fields = (
                    "content_hash",
                    "authority_hash",
                    "contract_hash",
                    "ledger_head_hash",
                    "strategy_nav_usd",
                    "observed_account_nlv",
                    "reconciliation_difference",
                    "asof",
                )
                if any(
                    normalized_nav_proof.get(field)
                    != current.strategy_nav_proof.get(field)
                    for field in stable_nav_fields
                ):
                    raise ApprovalChallengeRejected(
                        "Strategy NAV authority changed after challenge creation"
                    )
                active = self._connection.execute(
                    """
                    SELECT approvals.approval_id
                    FROM proposal_approvals AS approvals
                    LEFT JOIN approval_consumptions AS consumptions
                      ON consumptions.approval_id = approvals.approval_id
                    WHERE approvals.approved_at <= ?
                      AND approvals.expires_at > ?
                      AND consumptions.approval_id IS NULL
                    LIMIT 1
                    """,
                    (datetime_text(at), datetime_text(at)),
                ).fetchone()
                if active is not None:
                    raise ActiveApprovalExists(
                        "another unexpired options approval is active"
                    )

                approval_id = f"approval-{secrets.token_urlsafe(18)}"
                nonce_hash = hashlib.sha256(
                    f"challenge:{challenge_id}:{current.challenge_response_hash}".encode(
                        "utf-8"
                    )
                ).hexdigest()
                approval_expires = at + timedelta(seconds=APPROVAL_TTL_SECONDS)
                self._connection.execute(
                    """
                    INSERT INTO proposal_approvals(
                        approval_id, proposal_id, proposal_hash, material_hash,
                        legs_hash, risk_hash, approved_by, approved_at,
                        expires_at, nonce_hash, adverse_tolerance_usd,
                        reference_cost_usd, proposal_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        approval_id,
                        authorization.candidate_id,
                        hashes.proposal_hash,
                        hashes.material_hash,
                        hashes.legs_hash,
                        hashes.risk_hash,
                        approved_by,
                        datetime_text(at),
                        datetime_text(approval_expires),
                        nonce_hash,
                        format(tolerance, "f"),
                        format(reference, "f"),
                        canonical_json(frozen_proposal),
                    ),
                )
                binding_values = _authority_binding_values(
                    approval_id=approval_id,
                    challenge=current,
                    authorization=authorization,
                    bound_at=at,
                )
                self._connection.execute(
                    """
                    INSERT INTO approval_authority_bindings(
                        approval_id, challenge_id, ranking_snapshot_id,
                        scan_run_id, candidate_id, proposal_hash,
                        candidate_hash, ranking_basis_hash, row_hash,
                        snapshot_hash, current_policy_version,
                        current_policy_hash, policy_authority_marker_hash,
                        cost_version, cost_hash, risk_contract_hash,
                        risk_authority_version, risk_authority_marker_hash,
                        expected_hashes_json, candidate_body_json,
                        proposal_body_json, reference_cost_usd, bound_at,
                        binding_hash
                    ) VALUES (
                        ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
                    )
                    """,
                    tuple(
                        binding_values[name]
                        for name in _AUTHORITY_BINDING_INSERT_FIELDS
                    ),
                )
                authority_proof_values = _authority_proof_values(
                    approval_id=approval_id,
                    challenge_broker_proof=current.broker_proof,
                    confirm_broker_proof=normalized_broker_proof,
                    strategy_nav_proof=normalized_nav_proof,
                )
                self._connection.execute(
                    """
                    INSERT INTO approval_authority_proofs(
                        approval_id, challenge_broker_proof_json,
                        challenge_broker_proof_hash,
                        confirm_broker_proof_json,
                        confirm_broker_proof_hash,
                        strategy_nav_proof_json,
                        strategy_nav_proof_hash,
                        proof_binding_hash
                    ) VALUES (?,?,?,?,?,?,?,?)
                    """,
                    tuple(
                        authority_proof_values[name]
                        for name in _AUTHORITY_PROOF_INSERT_FIELDS
                    ),
                )
                confirmation_hash = canonical_hash(
                    {
                        "challenge_id": challenge_id,
                        "challenge_response_hash": current.challenge_response_hash,
                        "risk_acknowledged": True,
                        "second_confirmation": True,
                        "confirmation_token": APPROVAL_CONFIRMATION_TOKEN,
                    }
                )
                self._connection.execute(
                    """
                    INSERT INTO approval_challenge_consumptions(
                        challenge_id, approval_id, consumed_at,
                        confirmation_hash
                    ) VALUES (?, ?, ?, ?)
                    """,
                    (
                        challenge_id,
                        approval_id,
                        datetime_text(at),
                        confirmation_hash,
                    ),
                )
                approval_row = self._connection.execute(
                    "SELECT * FROM proposal_approvals WHERE approval_id=?",
                    (approval_id,),
                ).fetchone()
                binding_row = self._connection.execute(
                    """
                    SELECT bindings.*, proofs.challenge_broker_proof_json,
                           proofs.challenge_broker_proof_hash,
                           proofs.confirm_broker_proof_json,
                           proofs.confirm_broker_proof_hash,
                           proofs.strategy_nav_proof_json,
                           proofs.strategy_nav_proof_hash,
                           proofs.proof_binding_hash
                    FROM approval_authority_bindings AS bindings
                    JOIN approval_authority_proofs AS proofs
                      ON proofs.approval_id = bindings.approval_id
                    WHERE bindings.approval_id=?
                    """,
                    (approval_id,),
                ).fetchone()
                consumption_row = self._connection.execute(
                    "SELECT * FROM approval_challenge_consumptions "
                    "WHERE challenge_id=?",
                    (challenge_id,),
                ).fetchone()
                if (
                    approval_row is None
                    or binding_row is None
                    or consumption_row is None
                ):
                    raise ApprovalError("confirmed approval batch is incomplete")
                return ApprovalConfirmation(
                    approval=_row_to_approval(approval_row),
                    authority_binding=_row_to_authority_binding(binding_row),
                    consumption=_row_to_challenge_consumption(consumption_row),
                )

        def confirm(
            authorization: FrozenRankOneAuthorization,
        ) -> ApprovalConfirmation:
            result = _guard_strategy_nav_current(
                strategy_nav_source,
                strategy_nav_snapshot,
                callback=lambda: confirm_under_nav(authorization),
            )
            if result is None:
                raise ApprovalAuthorityConflict(
                    "current Strategy NAV authorization is unavailable"
                )
            return result

        result = ranking_store.guard_frozen_rank_one(
            challenge.ranking_snapshot_id,
            challenge.candidate_id,
            policy_resolver=policy_resolver,
            risk_authority_resolver=risk_authority_resolver,
            execution_cost_contract=execution_cost_contract,
            callback=confirm,
            now=at,
        )
        if result is None:
            raise ApprovalAuthorityConflict(
                "current rank-one authorization is unavailable"
            )
        return result

    def approve(
        self,
        proposal_id: str,
        proposal: Mapping[str, object],
        *,
        nonce: str,
        approved_by: str,
        approved_at: datetime | None = None,
        approval_id: str | None = None,
        adverse_tolerance_usd: Decimal | int | float | str = DEFAULT_ADVERSE_TOLERANCE_USD,
        reference_cost_usd: Decimal | int | float | str | None = None,
        exclusive_active: bool = False,
    ) -> ApprovalRecord:
        _identifier("proposal_id", proposal_id)
        _identifier("approved_by", approved_by)
        if approval_id is None:
            approval_id = f"approval-{secrets.token_urlsafe(18)}"
        _identifier("approval_id", approval_id)
        if not isinstance(nonce, str) or not 16 <= len(nonce) <= 256:
            raise ValueError("nonce must contain 16-256 characters")
        if not isinstance(exclusive_active, bool):
            raise TypeError("exclusive_active must be a boolean")
        nonce_hash = hashlib.sha256(nonce.encode("utf-8")).hexdigest()
        tolerance = _money("adverse_tolerance_usd", adverse_tolerance_usd)
        if tolerance <= 0 or tolerance > MAX_ADVERSE_TOLERANCE_USD:
            raise ValueError("adverse_tolerance_usd must be positive and at most 5.00")
        hashes, frozen_proposal = proposal_hashes(proposal)
        reference = _executable_cost(frozen_proposal)
        if reference is None:
            raise ValueError(
                "proposal requires executable ask for every buy/long leg and bid "
                "for every sell/short leg"
            )
        if reference_cost_usd is not None:
            supplied_reference = _money("reference_cost_usd", reference_cost_usd)
            if supplied_reference != reference:
                raise ValueError("reference_cost_usd must match independently priced legs")
        at = self._trusted_now(approved_at, field="approved_at")
        expires_at = at + timedelta(seconds=APPROVAL_TTL_SECONDS)
        with self._transaction():
            if exclusive_active:
                active = self._connection.execute(
                    """
                    SELECT approvals.approval_id
                    FROM proposal_approvals AS approvals
                    LEFT JOIN approval_consumptions AS consumptions
                      ON consumptions.approval_id = approvals.approval_id
                    WHERE approvals.approved_at <= ?
                      AND approvals.expires_at > ?
                      AND consumptions.approval_id IS NULL
                    LIMIT 1
                    """,
                    (datetime_text(at), datetime_text(at)),
                ).fetchone()
                if active is not None:
                    raise ActiveApprovalExists(
                        "another unexpired options approval is active"
                    )
            replay = self._connection.execute(
                "SELECT approval_id FROM proposal_approvals WHERE nonce_hash = ?",
                (nonce_hash,),
            ).fetchone()
            if replay is not None:
                raise NonceReplayError(
                    f"nonce was already consumed by approval {replay['approval_id']}"
                )
            existing = self._connection.execute(
                "SELECT * FROM proposal_approvals WHERE approval_id = ?", (approval_id,)
            ).fetchone()
            if existing is not None:
                raise ApprovalIdentityConflict("approval_id is already in use")
            self._connection.execute(
                """
                INSERT INTO proposal_approvals(
                    approval_id, proposal_id, proposal_hash, material_hash,
                    legs_hash, risk_hash, approved_by, approved_at, expires_at,
                    nonce_hash, adverse_tolerance_usd, reference_cost_usd,
                    proposal_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    approval_id,
                    proposal_id,
                    hashes.proposal_hash,
                    hashes.material_hash,
                    hashes.legs_hash,
                    hashes.risk_hash,
                    approved_by,
                    datetime_text(at),
                    datetime_text(expires_at),
                    nonce_hash,
                    format(tolerance, "f"),
                    None if reference is None else format(reference, "f"),
                    canonical_json(frozen_proposal),
                ),
            )
            row = self._connection.execute(
                "SELECT * FROM proposal_approvals WHERE approval_id = ?", (approval_id,)
            ).fetchone()
            if row is None:
                raise ApprovalError("inserted approval is missing")
            return _row_to_approval(row)

    create_approval = approve

    def get(self, approval_id: str) -> ApprovalRecord | None:
        _identifier("approval_id", approval_id)
        self._ensure_open()
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM proposal_approvals WHERE approval_id = ?", (approval_id,)
            ).fetchone()
        return None if row is None else _row_to_approval(row)

    def validate(
        self,
        approval_id: str,
        current_proposal: Mapping[str, object],
        *,
        checked_at: datetime | None = None,
        adverse_change_usd: Decimal | int | float | str | None = None,
        current_cost_usd: Decimal | int | float | str | None = None,
        require_unconsumed: bool = True,
        require_authority_binding: bool = False,
    ) -> ApprovalValidation:
        _identifier("approval_id", approval_id)
        if not isinstance(require_authority_binding, bool):
            raise TypeError("require_authority_binding must be a boolean")
        at = self._trusted_now(checked_at, field="checked_at")
        self._ensure_open()
        with self._lock:
            return self._validate_locked(
                approval_id,
                current_proposal,
                checked_at=at,
                adverse_change_usd=adverse_change_usd,
                current_cost_usd=current_cost_usd,
                require_unconsumed=require_unconsumed,
                require_authority_binding=require_authority_binding,
            )

    validate_approval = validate

    def consume(
        self,
        approval_id: str,
        current_proposal: Mapping[str, object],
        *,
        execution: Mapping[str, object],
        consumed_at: datetime | None = None,
        adverse_change_usd: Decimal | int | float | str | None = None,
        current_cost_usd: Decimal | int | float | str | None = None,
        require_authority_binding: bool = False,
    ) -> tuple[ApprovalValidation, ApprovalConsumption | None]:
        _identifier("approval_id", approval_id)
        if not isinstance(require_authority_binding, bool):
            raise TypeError("require_authority_binding must be a boolean")
        if not isinstance(execution, Mapping) or not execution:
            raise ValueError("execution must be a nonempty mapping")
        execution_hash = canonical_hash(execution)
        at = self._trusted_now(consumed_at, field="consumed_at")
        with self._transaction():
            validation = self._validate_locked(
                approval_id,
                current_proposal,
                checked_at=at,
                adverse_change_usd=adverse_change_usd,
                current_cost_usd=current_cost_usd,
                require_unconsumed=True,
                require_authority_binding=require_authority_binding,
            )
            if not validation.valid:
                return validation, None
            self._connection.execute(
                """
                INSERT INTO approval_consumptions(approval_id, consumed_at, execution_hash)
                VALUES (?, ?, ?)
                """,
                (approval_id, datetime_text(at), execution_hash),
            )
            return validation, ApprovalConsumption(
                approval_id=approval_id,
                consumed_at=at,
                execution_hash=execution_hash,
            )

    consume_approval = consume

    def query(
        self,
        *,
        proposal_id: str | None = None,
        after_sequence: int = 0,
        limit: int = 500,
    ) -> tuple[ApprovalRecord, ...]:
        if not isinstance(after_sequence, int) or isinstance(after_sequence, bool):
            raise TypeError("after_sequence must be an integer")
        if after_sequence < 0:
            raise ValueError("after_sequence cannot be negative")
        if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 5000:
            raise ValueError("limit must be between 1 and 5000")
        clauses = ["sequence > ?"]
        params: list[object] = [after_sequence]
        if proposal_id is not None:
            _identifier("proposal_id", proposal_id)
            clauses.append("proposal_id = ?")
            params.append(proposal_id)
        params.append(limit)
        self._ensure_open()
        with self._lock:
            rows = self._connection.execute(
                "SELECT * FROM proposal_approvals WHERE "
                + " AND ".join(clauses)
                + " ORDER BY sequence LIMIT ?",
                params,
            ).fetchall()
        return tuple(_row_to_approval(row) for row in rows)

    def list_active(self) -> tuple[ApprovalRecord, ...]:
        """Return every unexpired, unconsumed approval using the store clock.

        This intentionally has no caller-supplied limit.  Safety gates must not
        scan only the oldest page of an append-only ledger, because a current
        approval would eventually sit beyond that page after enough history.
        """

        now = self._trusted_now(None, field="checked_at")
        at = datetime_text(now)
        self._ensure_open()
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT approvals.*
                FROM proposal_approvals AS approvals
                LEFT JOIN approval_consumptions AS consumptions
                  ON consumptions.approval_id = approvals.approval_id
                WHERE approvals.approved_at <= ?
                  AND approvals.expires_at > ?
                  AND consumptions.approval_id IS NULL
                ORDER BY approvals.sequence
                """,
                (at, at),
            ).fetchall()
        return tuple(_row_to_approval(row) for row in rows)

    def export_jsonl(
        self,
        destination: str | Path,
        *,
        proposal_id: str | None = None,
    ) -> int:
        path = Path(destination)
        path.parent.mkdir(parents=True, exist_ok=True)
        after = 0
        exported = 0
        with path.open("w", encoding="utf-8", newline="\n") as handle:
            while rows := self.query(
                proposal_id=proposal_id, after_sequence=after, limit=1000
            ):
                for row in rows:
                    handle.write(canonical_json(row.as_dict()) + "\n")
                exported += len(rows)
                after = rows[-1].sequence
        return exported

    export = export_jsonl

    def _validate_locked(
        self,
        approval_id: str,
        current_proposal: Mapping[str, object],
        *,
        checked_at: datetime,
        adverse_change_usd: Decimal | int | float | str | None,
        current_cost_usd: Decimal | int | float | str | None,
        require_unconsumed: bool,
        require_authority_binding: bool,
    ) -> ApprovalValidation:
        row = self._connection.execute(
            "SELECT * FROM proposal_approvals WHERE approval_id = ?", (approval_id,)
        ).fetchone()
        if row is None:
            return ApprovalValidation(
                valid=False,
                reasons=("approval_not_found",),
                checked_at=checked_at,
                approval=None,
                adverse_change_usd=None,
            )
        approval = _row_to_approval(row)
        reasons: list[str] = []
        binding_row = self._connection.execute(
            """
            SELECT bindings.*, proofs.challenge_broker_proof_json,
                   proofs.challenge_broker_proof_hash,
                   proofs.confirm_broker_proof_json,
                   proofs.confirm_broker_proof_hash,
                   proofs.strategy_nav_proof_json,
                   proofs.strategy_nav_proof_hash,
                   proofs.proof_binding_hash
            FROM approval_authority_bindings AS bindings
            JOIN approval_authority_proofs AS proofs
              ON proofs.approval_id = bindings.approval_id
            WHERE bindings.approval_id=?
            """,
            (approval_id,),
        ).fetchone()
        base_binding = self._connection.execute(
            "SELECT 1 FROM approval_authority_bindings WHERE approval_id=?",
            (approval_id,),
        ).fetchone()
        legacy_unbound = self._connection.execute(
            "SELECT 1 FROM approval_legacy_unbound WHERE approval_id=?",
            (approval_id,),
        ).fetchone()
        if binding_row is None and (
            require_authority_binding or legacy_unbound is not None
        ):
            reasons.append("approval_authority_unbound")
        elif binding_row is None and base_binding is not None:
            # A v2 authority row remains readable for non-bridge compatibility,
            # but can never satisfy require_authority_binding without v3 proofs.
            pass
        elif binding_row is not None:
            binding = _row_to_authority_binding(binding_row)
            if (
                binding.proposal_hash != approval.proposal_hash
                or canonical_hash(binding.proposal_body) != approval.proposal_hash
                or binding.candidate_id != approval.proposal_id
            ):
                reasons.append("approval_authority_binding_mismatch")
        if checked_at >= approval.expires_at:
            reasons.append("approval_expired")
        if checked_at < approval.approved_at:
            reasons.append("approval_not_yet_valid")
        if require_unconsumed:
            consumed = self._connection.execute(
                "SELECT 1 FROM approval_consumptions WHERE approval_id = ?", (approval_id,)
            ).fetchone()
            if consumed is not None:
                reasons.append("approval_already_consumed")

        hashes, frozen_current = proposal_hashes(current_proposal)
        changed = hashes.proposal_hash != approval.proposal_hash
        if hashes.legs_hash != approval.legs_hash:
            reasons.append("proposal_legs_changed")
        if hashes.risk_hash != approval.risk_hash:
            reasons.append("proposal_risk_changed")
        if hashes.material_hash != approval.material_hash:
            reasons.append("proposal_materially_changed")

        adverse: Decimal | None = None
        current_reference = _executable_cost(frozen_current)
        if current_reference is not None and approval.reference_cost_usd is not None:
            # This independently derived leg-side price is authoritative.  Any
            # caller-supplied value may only make the check more conservative.
            adverse_candidates = [current_reference - approval.reference_cost_usd]
            if current_cost_usd is not None:
                adverse_candidates.append(
                    _money("current_cost_usd", current_cost_usd)
                    - approval.reference_cost_usd
                )
            if adverse_change_usd is not None:
                adverse_candidates.append(
                    _money("adverse_change_usd", adverse_change_usd)
                )
            adverse = max(adverse_candidates)
        elif not changed:
            adverse = Decimal("0")
        if adverse is None:
            reasons.append("adverse_change_unavailable")
        elif adverse > approval.adverse_tolerance_usd:
            reasons.append("adverse_tolerance_exceeded")

        return ApprovalValidation(
            valid=not reasons,
            reasons=tuple(dict.fromkeys(reasons)),
            checked_at=checked_at,
            approval=approval,
            adverse_change_usd=adverse,
        )

    def _trusted_now(self, supplied: datetime | None, *, field: str) -> datetime:
        """Use only the composition-owned clock; reject caller backdating."""

        trusted = utc_datetime(self._clock(), field="clock result")
        if supplied is not None:
            proposed = utc_datetime(supplied, field=field)
            # Composition layers often capture ``now`` immediately before the
            # call.  Permit only transport-scale skew, then use the store's own
            # value; material backdating never controls TTL evaluation.
            if abs((proposed - trusted).total_seconds()) > 5:
                raise ValueError(f"{field} is controlled by the approval store clock")
        return trusted

    def _migrate(self) -> None:
        version = int(self._connection.execute("PRAGMA user_version").fetchone()[0])
        if version > SCHEMA_VERSION:
            raise RuntimeError(f"approval schema {version} is newer than supported")
        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                self._create_v3_schema_in_transaction(version)
                self._install_immutable_triggers_in_transaction()
                self._connection.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
                self._connection.execute("COMMIT")
            except BaseException:
                if self._connection.in_transaction:
                    self._connection.execute("ROLLBACK")
                raise
        self._connection.execute("PRAGMA foreign_keys=ON")

    def _create_v2_schema_in_transaction(self, source_version: int) -> None:
        statements = (
            """
            CREATE TABLE IF NOT EXISTS proposal_approvals (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                approval_id TEXT NOT NULL UNIQUE,
                proposal_id TEXT NOT NULL,
                proposal_hash TEXT NOT NULL,
                material_hash TEXT NOT NULL,
                legs_hash TEXT NOT NULL,
                risk_hash TEXT NOT NULL,
                approved_by TEXT NOT NULL,
                approved_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                nonce_hash TEXT NOT NULL UNIQUE,
                adverse_tolerance_usd TEXT NOT NULL,
                reference_cost_usd TEXT,
                proposal_json TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS approval_consumptions (
                approval_id TEXT PRIMARY KEY,
                consumed_at TEXT NOT NULL,
                execution_hash TEXT NOT NULL,
                FOREIGN KEY(approval_id) REFERENCES proposal_approvals(approval_id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS approval_challenges (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                challenge_id TEXT NOT NULL UNIQUE,
                challenge_response_hash TEXT NOT NULL UNIQUE,
                ranking_snapshot_id TEXT NOT NULL,
                scan_run_id TEXT NOT NULL,
                candidate_id TEXT NOT NULL,
                proposal_hash TEXT NOT NULL,
                candidate_hash TEXT NOT NULL,
                ranking_basis_hash TEXT NOT NULL,
                row_hash TEXT NOT NULL,
                snapshot_hash TEXT NOT NULL,
                current_policy_version TEXT NOT NULL,
                current_policy_hash TEXT NOT NULL,
                policy_authority_marker_hash TEXT NOT NULL,
                cost_version TEXT NOT NULL,
                cost_hash TEXT NOT NULL,
                risk_contract_hash TEXT NOT NULL,
                risk_authority_version TEXT NOT NULL,
                risk_authority_marker_hash TEXT NOT NULL,
                expected_hashes_json TEXT NOT NULL,
                candidate_body_json TEXT NOT NULL,
                proposal_body_json TEXT NOT NULL,
                reference_cost_usd TEXT NOT NULL,
                created_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                challenge_hash TEXT NOT NULL UNIQUE
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS approval_challenge_consumptions (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                challenge_id TEXT NOT NULL UNIQUE,
                approval_id TEXT NOT NULL UNIQUE,
                consumed_at TEXT NOT NULL,
                confirmation_hash TEXT NOT NULL UNIQUE,
                FOREIGN KEY(challenge_id)
                    REFERENCES approval_challenges(challenge_id),
                FOREIGN KEY(approval_id)
                    REFERENCES proposal_approvals(approval_id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS approval_authority_bindings (
                approval_id TEXT PRIMARY KEY,
                challenge_id TEXT NOT NULL UNIQUE,
                ranking_snapshot_id TEXT NOT NULL,
                scan_run_id TEXT NOT NULL,
                candidate_id TEXT NOT NULL,
                proposal_hash TEXT NOT NULL,
                candidate_hash TEXT NOT NULL,
                ranking_basis_hash TEXT NOT NULL,
                row_hash TEXT NOT NULL,
                snapshot_hash TEXT NOT NULL,
                current_policy_version TEXT NOT NULL,
                current_policy_hash TEXT NOT NULL,
                policy_authority_marker_hash TEXT NOT NULL,
                cost_version TEXT NOT NULL,
                cost_hash TEXT NOT NULL,
                risk_contract_hash TEXT NOT NULL,
                risk_authority_version TEXT NOT NULL,
                risk_authority_marker_hash TEXT NOT NULL,
                expected_hashes_json TEXT NOT NULL,
                candidate_body_json TEXT NOT NULL,
                proposal_body_json TEXT NOT NULL,
                reference_cost_usd TEXT NOT NULL,
                bound_at TEXT NOT NULL,
                binding_hash TEXT NOT NULL UNIQUE,
                FOREIGN KEY(approval_id)
                    REFERENCES proposal_approvals(approval_id),
                FOREIGN KEY(challenge_id)
                    REFERENCES approval_challenges(challenge_id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS approval_legacy_unbound (
                approval_id TEXT PRIMARY KEY,
                migrated_at TEXT NOT NULL,
                FOREIGN KEY(approval_id)
                    REFERENCES proposal_approvals(approval_id)
            )
            """,
            "CREATE INDEX IF NOT EXISTS proposal_approvals_proposal_idx "
            "ON proposal_approvals(proposal_id, sequence)",
            "CREATE INDEX IF NOT EXISTS approval_challenges_rank_idx "
            "ON approval_challenges(ranking_snapshot_id,candidate_id,sequence)",
        )
        for statement in statements:
            self._connection.execute(statement)
        if source_version < 2:
            migrated_at = datetime_text(
                utc_datetime(self._clock(), field="clock result")
            )
            self._connection.execute(
                """
                INSERT OR IGNORE INTO approval_legacy_unbound(
                    approval_id, migrated_at
                )
                SELECT approval_id, ? FROM proposal_approvals
                WHERE approval_id NOT IN (
                    SELECT approval_id FROM approval_authority_bindings
                )
                """,
                (migrated_at,),
            )

    def _create_v3_schema_in_transaction(self, source_version: int) -> None:
        self._create_v2_schema_in_transaction(source_version)
        statements = (
            """
            CREATE TABLE IF NOT EXISTS approval_challenge_proofs (
                challenge_id TEXT PRIMARY KEY,
                broker_proof_json TEXT NOT NULL,
                broker_proof_hash TEXT NOT NULL,
                strategy_nav_proof_json TEXT NOT NULL,
                strategy_nav_proof_hash TEXT NOT NULL,
                proof_binding_hash TEXT NOT NULL UNIQUE,
                FOREIGN KEY(challenge_id)
                    REFERENCES approval_challenges(challenge_id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS approval_authority_proofs (
                approval_id TEXT PRIMARY KEY,
                challenge_broker_proof_json TEXT NOT NULL,
                challenge_broker_proof_hash TEXT NOT NULL,
                confirm_broker_proof_json TEXT NOT NULL,
                confirm_broker_proof_hash TEXT NOT NULL,
                strategy_nav_proof_json TEXT NOT NULL,
                strategy_nav_proof_hash TEXT NOT NULL,
                proof_binding_hash TEXT NOT NULL UNIQUE,
                FOREIGN KEY(approval_id)
                    REFERENCES approval_authority_bindings(approval_id)
            )
            """,
        )
        for statement in statements:
            self._connection.execute(statement)

    def _install_immutable_triggers_in_transaction(self) -> None:
        messages = {
            "proposal_approvals": "immutable proposal approval",
            "approval_consumptions": "immutable approval consumption",
            "approval_challenges": "immutable approval challenge",
            "approval_challenge_consumptions": (
                "immutable approval challenge consumption"
            ),
            "approval_authority_bindings": "immutable approval authority binding",
            "approval_challenge_proofs": "immutable approval challenge proof",
            "approval_authority_proofs": "immutable approval authority proof",
            "approval_legacy_unbound": "immutable legacy approval marker",
        }
        for table, message in messages.items():
            for operation in ("update", "delete"):
                self._connection.execute(
                    f"CREATE TRIGGER IF NOT EXISTS {table}_no_{operation} "
                    f"BEFORE {operation.upper()} ON {table} "
                    f"BEGIN SELECT RAISE(ABORT, '{message}: {operation} forbidden'); END"
                )
        self._connection.execute(
            """
            CREATE TRIGGER IF NOT EXISTS approval_legacy_unbound_no_insert
            BEFORE INSERT ON approval_legacy_unbound
            BEGIN
                SELECT RAISE(ABORT, 'immutable legacy approval marker: insert forbidden');
            END
            """
        )

    class _Transaction:
        def __init__(self, store: "ProposalApprovalStore") -> None:
            self.store = store

        def __enter__(self) -> None:
            self.store._ensure_open()
            self.store._lock.acquire()
            try:
                self.store._connection.execute("BEGIN IMMEDIATE")
            except BaseException:
                self.store._lock.release()
                raise

        def __exit__(self, exc_type: object, *_: object) -> None:
            try:
                self.store._connection.execute("ROLLBACK" if exc_type else "COMMIT")
            finally:
                self.store._lock.release()

    def _transaction(self) -> "ProposalApprovalStore._Transaction":
        return self._Transaction(self)

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("proposal approval store is closed")


def _guard_strategy_nav_current(
    source: object,
    snapshot: object,
    *,
    callback: Callable[[], object],
) -> object | None:
    """Invoke approval writes only under the trusted NAV source's guard."""

    guard = getattr(source, "guard_current", None)
    if not callable(guard) or not callable(callback):
        return None
    return guard(snapshot, callback=callback)


def proposal_hashes(
    proposal: Mapping[str, object],
) -> tuple[ProposalHashes, Mapping[str, object]]:
    if not isinstance(proposal, Mapping):
        raise TypeError("proposal must be a mapping")
    frozen = freeze_json(proposal)
    assert isinstance(frozen, Mapping)
    legs = frozen.get("legs")
    if not isinstance(legs, tuple) or not legs:
        raise ValueError("proposal must contain at least one option leg")
    if any(not isinstance(leg, Mapping) for leg in legs):
        raise ValueError("every proposal leg must be an object")
    risk = frozen.get("risk")
    if not isinstance(risk, Mapping) or not risk:
        raise ValueError("proposal must contain a nonempty risk object")
    plain = thaw_json(frozen)
    assert isinstance(plain, dict)
    material = _strip_repricing(plain)
    hashes = ProposalHashes(
        proposal_hash=canonical_hash(plain),
        material_hash=canonical_hash(material),
        legs_hash=canonical_hash(_strip_repricing(plain["legs"])),
        risk_hash=canonical_hash(_strip_repricing(plain["risk"])),
    )
    return hashes, frozen


def hash_proposal(proposal: Mapping[str, object]) -> str:
    return proposal_hashes(proposal)[0].proposal_hash


def _strip_repricing(value: object) -> object:
    if isinstance(value, dict):
        return {
            key: _strip_repricing(item)
            for key, item in value.items()
            if key not in _REPRICE_ONLY_KEYS
        }
    if isinstance(value, list):
        return [_strip_repricing(item) for item in value]
    return value


def _executable_cost(proposal: Mapping[str, object]) -> Decimal | None:
    plain = thaw_json(proposal)
    assert isinstance(plain, dict)
    legs = plain.get("legs")
    if not isinstance(legs, list) or not legs:
        return None
    try:
        total = Decimal("0")
        for item in legs:
            if not isinstance(item, dict):
                return None
            leg_spec = item.get("leg") if isinstance(item.get("leg"), dict) else item
            assert isinstance(leg_spec, dict)
            quote = item.get("quote") if isinstance(item.get("quote"), dict) else item
            assert isinstance(quote, dict)
            side_raw = leg_spec.get("side", leg_spec.get("position_side"))
            side = str(side_raw).strip().upper()
            quantity_raw = leg_spec.get("quantity", 1)
            if isinstance(quantity_raw, bool):
                return None
            quantity = Decimal(str(quantity_raw))
            if quantity <= 0 or quantity != quantity.to_integral_value():
                return None
            contract = (
                leg_spec.get("contract")
                if isinstance(leg_spec.get("contract"), dict)
                else {}
            )
            multiplier_raw = leg_spec.get("multiplier", contract.get("multiplier", 100))
            multiplier = _money("multiplier", multiplier_raw)
            if multiplier <= 0:
                return None
            if side in {"BUY", "BOT", "LONG"}:
                price = _money("ask", quote.get("ask"))
                sign = Decimal("1")
            elif side in {"SELL", "SLD", "SHORT"}:
                price = _money("bid", quote.get("bid"))
                sign = Decimal("-1")
            else:
                return None
            if price < 0:
                return None
            total += sign * price * quantity * multiplier
        return total
    except (InvalidOperation, TypeError, ValueError):
        return None


def _money(field: str, value: Decimal | int | float | str | object) -> Decimal:
    if isinstance(value, bool):
        raise TypeError(f"{field} must be numeric")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field} must be finite numeric money") from exc
    if not result.is_finite():
        raise ValueError(f"{field} must be finite numeric money")
    return result


def _identifier(field: str, value: str) -> None:
    if not isinstance(value, str) or _ID_RE.fullmatch(value) is None:
        raise ValueError(f"{field} is not a valid approval identifier")


def _row_to_approval(row: sqlite3.Row) -> ApprovalRecord:
    reference = row["reference_cost_usd"]
    return ApprovalRecord(
        approval_id=str(row["approval_id"]),
        sequence=int(row["sequence"]),
        proposal_id=str(row["proposal_id"]),
        proposal_hash=str(row["proposal_hash"]),
        material_hash=str(row["material_hash"]),
        legs_hash=str(row["legs_hash"]),
        risk_hash=str(row["risk_hash"]),
        approved_by=str(row["approved_by"]),
        approved_at=datetime.fromisoformat(str(row["approved_at"])),
        expires_at=datetime.fromisoformat(str(row["expires_at"])),
        nonce_hash=str(row["nonce_hash"]),
        adverse_tolerance_usd=Decimal(str(row["adverse_tolerance_usd"])),
        reference_cost_usd=None if reference is None else Decimal(str(reference)),
        proposal=json.loads(str(row["proposal_json"])),
    )


def _proposal_identity(proposal: Mapping[str, object]) -> str | None:
    value = proposal.get("proposal_id", proposal.get("candidate_id"))
    return value.strip() if isinstance(value, str) and value.strip() else None


def _challenge_values(
    *,
    challenge_id: str,
    challenge_response_hash: str,
    authorization: FrozenRankOneAuthorization,
    reference_cost_usd: Decimal,
    created_at: datetime,
    expires_at: datetime,
) -> dict[str, str]:
    values = {
        "challenge_id": challenge_id,
        "challenge_response_hash": challenge_response_hash,
        "ranking_snapshot_id": authorization.ranking_snapshot_id,
        "scan_run_id": authorization.scan_run_id,
        "candidate_id": authorization.candidate_id,
        "proposal_hash": authorization.proposal_hash,
        "candidate_hash": authorization.candidate_hash,
        "ranking_basis_hash": authorization.ranking_basis_hash,
        "row_hash": authorization.row_hash,
        "snapshot_hash": authorization.snapshot_hash,
        "current_policy_version": authorization.current_policy_version,
        "current_policy_hash": authorization.current_policy_hash,
        "policy_authority_marker_hash": (
            authorization.policy_authority_marker_hash
        ),
        "cost_version": authorization.cost_version,
        "cost_hash": authorization.cost_hash,
        "risk_contract_hash": authorization.risk_contract_hash,
        "risk_authority_version": authorization.risk_authority_version,
        "risk_authority_marker_hash": (
            authorization.risk_authority_marker_hash
        ),
        "expected_hashes_json": canonical_json(authorization.expected_hashes),
        "candidate_body_json": canonical_json(authorization.candidate_body),
        "proposal_body_json": canonical_json(authorization.proposal_body),
        "reference_cost_usd": format(reference_cost_usd, "f"),
        "created_at": datetime_text(created_at),
        "expires_at": datetime_text(expires_at),
    }
    values["challenge_hash"] = canonical_hash(
        _stored_binding_payload(values)
    )
    return values


def _authority_binding_values(
    *,
    approval_id: str,
    challenge: ApprovalChallengeRecord,
    authorization: FrozenRankOneAuthorization,
    bound_at: datetime,
) -> dict[str, str]:
    if not _challenge_matches_authorization(challenge, authorization):
        raise ApprovalAuthorityConflict("approval authority changed before binding")
    values = {
        "approval_id": approval_id,
        "challenge_id": challenge.challenge_id,
        "ranking_snapshot_id": authorization.ranking_snapshot_id,
        "scan_run_id": authorization.scan_run_id,
        "candidate_id": authorization.candidate_id,
        "proposal_hash": authorization.proposal_hash,
        "candidate_hash": authorization.candidate_hash,
        "ranking_basis_hash": authorization.ranking_basis_hash,
        "row_hash": authorization.row_hash,
        "snapshot_hash": authorization.snapshot_hash,
        "current_policy_version": authorization.current_policy_version,
        "current_policy_hash": authorization.current_policy_hash,
        "policy_authority_marker_hash": (
            authorization.policy_authority_marker_hash
        ),
        "cost_version": authorization.cost_version,
        "cost_hash": authorization.cost_hash,
        "risk_contract_hash": authorization.risk_contract_hash,
        "risk_authority_version": authorization.risk_authority_version,
        "risk_authority_marker_hash": (
            authorization.risk_authority_marker_hash
        ),
        "expected_hashes_json": canonical_json(authorization.expected_hashes),
        "candidate_body_json": canonical_json(authorization.candidate_body),
        "proposal_body_json": canonical_json(authorization.proposal_body),
        "reference_cost_usd": format(challenge.reference_cost_usd, "f"),
        "bound_at": datetime_text(bound_at),
    }
    values["binding_hash"] = canonical_hash(_stored_binding_payload(values))
    return values


def _challenge_proof_values(
    *,
    challenge_id: str,
    broker_proof: Mapping[str, object],
    strategy_nav_proof: Mapping[str, object],
) -> dict[str, str]:
    values = {
        "challenge_id": challenge_id,
        "broker_proof_json": canonical_json(broker_proof),
        "broker_proof_hash": canonical_hash(broker_proof),
        "strategy_nav_proof_json": canonical_json(strategy_nav_proof),
        "strategy_nav_proof_hash": canonical_hash(strategy_nav_proof),
    }
    values["proof_binding_hash"] = canonical_hash(
        _stored_binding_payload(values)
    )
    return values


def _authority_proof_values(
    *,
    approval_id: str,
    challenge_broker_proof: Mapping[str, object],
    confirm_broker_proof: Mapping[str, object],
    strategy_nav_proof: Mapping[str, object],
) -> dict[str, str]:
    values = {
        "approval_id": approval_id,
        "challenge_broker_proof_json": canonical_json(challenge_broker_proof),
        "challenge_broker_proof_hash": canonical_hash(challenge_broker_proof),
        "confirm_broker_proof_json": canonical_json(confirm_broker_proof),
        "confirm_broker_proof_hash": canonical_hash(confirm_broker_proof),
        "strategy_nav_proof_json": canonical_json(strategy_nav_proof),
        "strategy_nav_proof_hash": canonical_hash(strategy_nav_proof),
    }
    values["proof_binding_hash"] = canonical_hash(
        _stored_binding_payload(values)
    )
    return values


def _stored_binding_payload(values: Mapping[str, object]) -> dict[str, object]:
    payload: dict[str, object] = {}
    for key, value in values.items():
        if key in {"challenge_hash", "binding_hash", "proof_binding_hash"}:
            continue
        if key.endswith("_json"):
            payload[key.removesuffix("_json")] = json.loads(str(value))
        else:
            payload[key] = value
    return payload


def _row_to_challenge(row: sqlite3.Row) -> ApprovalChallengeRecord:
    stored = {name: str(row[name]) for name in _CHALLENGE_INSERT_FIELDS}
    expected_hash = canonical_hash(_stored_binding_payload(stored))
    if not secrets.compare_digest(stored["challenge_hash"], expected_hash):
        raise ApprovalError("approval challenge binding is corrupt")
    broker_proof, strategy_nav_proof = _challenge_proofs_from_row(row)
    return ApprovalChallengeRecord(
        challenge_id=stored["challenge_id"],
        sequence=int(row["sequence"]),
        challenge_response_hash=stored["challenge_response_hash"],
        ranking_snapshot_id=stored["ranking_snapshot_id"],
        scan_run_id=stored["scan_run_id"],
        candidate_id=stored["candidate_id"],
        proposal_hash=stored["proposal_hash"],
        candidate_hash=stored["candidate_hash"],
        ranking_basis_hash=stored["ranking_basis_hash"],
        row_hash=stored["row_hash"],
        snapshot_hash=stored["snapshot_hash"],
        current_policy_version=stored["current_policy_version"],
        current_policy_hash=stored["current_policy_hash"],
        policy_authority_marker_hash=stored["policy_authority_marker_hash"],
        cost_version=stored["cost_version"],
        cost_hash=stored["cost_hash"],
        risk_contract_hash=stored["risk_contract_hash"],
        risk_authority_version=stored["risk_authority_version"],
        risk_authority_marker_hash=stored["risk_authority_marker_hash"],
        expected_hashes=json.loads(stored["expected_hashes_json"]),
        candidate_body=json.loads(stored["candidate_body_json"]),
        proposal_body=json.loads(stored["proposal_body_json"]),
        broker_proof=broker_proof,
        strategy_nav_proof=strategy_nav_proof,
        reference_cost_usd=Decimal(stored["reference_cost_usd"]),
        created_at=datetime.fromisoformat(stored["created_at"]),
        expires_at=datetime.fromisoformat(stored["expires_at"]),
        challenge_hash=stored["challenge_hash"],
    )


def _row_to_authority_binding(row: sqlite3.Row) -> ApprovalAuthorityBinding:
    stored = {name: str(row[name]) for name in _AUTHORITY_BINDING_INSERT_FIELDS}
    expected_hash = canonical_hash(_stored_binding_payload(stored))
    if not secrets.compare_digest(stored["binding_hash"], expected_hash):
        raise ApprovalError("approval authority binding is corrupt")
    (
        challenge_broker_proof,
        confirm_broker_proof,
        strategy_nav_proof,
    ) = _authority_proofs_from_row(row)
    return ApprovalAuthorityBinding(
        approval_id=stored["approval_id"],
        challenge_id=stored["challenge_id"],
        ranking_snapshot_id=stored["ranking_snapshot_id"],
        scan_run_id=stored["scan_run_id"],
        candidate_id=stored["candidate_id"],
        proposal_hash=stored["proposal_hash"],
        candidate_hash=stored["candidate_hash"],
        ranking_basis_hash=stored["ranking_basis_hash"],
        row_hash=stored["row_hash"],
        snapshot_hash=stored["snapshot_hash"],
        current_policy_version=stored["current_policy_version"],
        current_policy_hash=stored["current_policy_hash"],
        policy_authority_marker_hash=stored["policy_authority_marker_hash"],
        cost_version=stored["cost_version"],
        cost_hash=stored["cost_hash"],
        risk_contract_hash=stored["risk_contract_hash"],
        risk_authority_version=stored["risk_authority_version"],
        risk_authority_marker_hash=stored["risk_authority_marker_hash"],
        expected_hashes=json.loads(stored["expected_hashes_json"]),
        candidate_body=json.loads(stored["candidate_body_json"]),
        proposal_body=json.loads(stored["proposal_body_json"]),
        challenge_broker_proof=challenge_broker_proof,
        confirm_broker_proof=confirm_broker_proof,
        strategy_nav_proof=strategy_nav_proof,
        reference_cost_usd=Decimal(stored["reference_cost_usd"]),
        bound_at=datetime.fromisoformat(stored["bound_at"]),
        binding_hash=stored["binding_hash"],
    )


def _challenge_proofs_from_row(
    row: sqlite3.Row,
) -> tuple[Mapping[str, object], Mapping[str, object]]:
    stored = {name: str(row[name]) for name in _CHALLENGE_PROOF_INSERT_FIELDS}
    _verify_proof_binding(stored, label="approval challenge proof")
    broker = _verified_proof_document(
        stored["broker_proof_json"],
        stored["broker_proof_hash"],
        label="approval challenge broker proof",
    )
    nav = _verified_proof_document(
        stored["strategy_nav_proof_json"],
        stored["strategy_nav_proof_hash"],
        label="approval challenge Strategy NAV proof",
    )
    return broker, nav


def _authority_proofs_from_row(
    row: sqlite3.Row,
) -> tuple[Mapping[str, object], Mapping[str, object], Mapping[str, object]]:
    stored = {name: str(row[name]) for name in _AUTHORITY_PROOF_INSERT_FIELDS}
    _verify_proof_binding(stored, label="approval authority proof")
    challenge_broker = _verified_proof_document(
        stored["challenge_broker_proof_json"],
        stored["challenge_broker_proof_hash"],
        label="approval authority challenge broker proof",
    )
    confirm_broker = _verified_proof_document(
        stored["confirm_broker_proof_json"],
        stored["confirm_broker_proof_hash"],
        label="approval authority confirmation broker proof",
    )
    nav = _verified_proof_document(
        stored["strategy_nav_proof_json"],
        stored["strategy_nav_proof_hash"],
        label="approval authority Strategy NAV proof",
    )
    return challenge_broker, confirm_broker, nav


def _verify_proof_binding(values: Mapping[str, str], *, label: str) -> None:
    try:
        expected = canonical_hash(_stored_binding_payload(values))
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ApprovalError(f"{label} binding is corrupt") from exc
    if not secrets.compare_digest(values["proof_binding_hash"], expected):
        raise ApprovalError(f"{label} binding is corrupt")


def _verified_proof_document(
    stored_json: str, stored_hash: str, *, label: str
) -> Mapping[str, object]:
    try:
        document = json.loads(stored_json)
        if not isinstance(document, Mapping):
            raise TypeError("proof document must be a mapping")
        canonical = canonical_json(document)
        expected_hash = canonical_hash(document)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ApprovalError(f"{label} is corrupt") from exc
    if canonical != stored_json or not secrets.compare_digest(
        stored_hash, expected_hash
    ):
        raise ApprovalError(f"{label} is corrupt")
    frozen = freeze_json(document)
    if not isinstance(frozen, Mapping):
        raise ApprovalError(f"{label} is corrupt")
    return frozen


def _row_to_challenge_consumption(
    row: sqlite3.Row,
) -> ApprovalChallengeConsumption:
    return ApprovalChallengeConsumption(
        sequence=int(row["sequence"]),
        challenge_id=str(row["challenge_id"]),
        approval_id=str(row["approval_id"]),
        consumed_at=datetime.fromisoformat(str(row["consumed_at"])),
        confirmation_hash=str(row["confirmation_hash"]),
    )


def _challenge_matches_authorization(
    challenge: ApprovalChallengeRecord,
    authorization: FrozenRankOneAuthorization,
) -> bool:
    scalar_names = (
        "ranking_snapshot_id",
        "scan_run_id",
        "candidate_id",
        "proposal_hash",
        "candidate_hash",
        "ranking_basis_hash",
        "row_hash",
        "snapshot_hash",
        "current_policy_version",
        "current_policy_hash",
        "policy_authority_marker_hash",
        "cost_version",
        "cost_hash",
        "risk_contract_hash",
        "risk_authority_version",
        "risk_authority_marker_hash",
    )
    return (
        all(
            getattr(challenge, name) == getattr(authorization, name)
            for name in scalar_names
        )
        and canonical_json(challenge.expected_hashes)
        == canonical_json(authorization.expected_hashes)
        and canonical_json(challenge.candidate_body)
        == canonical_json(authorization.candidate_body)
        and canonical_json(challenge.proposal_body)
        == canonical_json(authorization.proposal_body)
    )


__all__ = [
    "APPROVAL_CONFIRMATION_TOKEN",
    "APPROVAL_TTL_SECONDS",
    "BROKER_PROOF_SCHEMA",
    "STRATEGY_NAV_PROOF_SCHEMA",
    "ApprovalAuthorityBinding",
    "ApprovalAuthorityConflict",
    "ApprovalChallengeConsumption",
    "ApprovalChallengeRecord",
    "ApprovalChallengeRejected",
    "ApprovalConfirmation",
    "ApprovalConsumption",
    "ApprovalError",
    "ApprovalIdentityConflict",
    "ApprovalRecord",
    "ApprovalValidation",
    "DEFAULT_ADVERSE_TOLERANCE_USD",
    "MAX_ADVERSE_TOLERANCE_USD",
    "NonceReplayError",
    "ProposalApprovalStore",
    "ProposalHashes",
    "IssuedApprovalChallenge",
    "hash_proposal",
    "proposal_hashes",
]
