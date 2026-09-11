"""Durable append-only P9 policy and proposal-risk authority ledger."""
from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import threading
from typing import Callable

from options_copilot.analytics.scenarios import ResolvedPolicy
from options_copilot.governance.contracts import (
    ContractError,
    ContractKind,
    SignedContract,
    load_contract,
    verify_contract,
)
from options_copilot.storage.canonical import (
    canonical_hash,
    canonical_json,
    freeze_json,
    utc_datetime,
)

from .markers import (
    AGradeAuthority,
    AuthoritySignatureVerifier,
    PromotionAuthority,
    RollbackAuthority,
    verify_a_grade_authority,
    verify_promotion_authority,
    verify_rollback_authority,
    validate_authority_verifier,
)


SCHEMA_VERSION = 1


class PolicyAuthorityError(RuntimeError):
    pass


class PolicyAuthorityTampered(PolicyAuthorityError):
    pass


class AuthorityConflict(PolicyAuthorityError):
    pass


class AuthorityNotFound(PolicyAuthorityError):
    pass


class PolicyAuthorityLedger:
    """SQLite/WAL/FULL authority ledger with serialized immediate writes."""

    def __init__(
        self,
        path: str | Path,
        *,
        initial_policy_path: str | Path | None = None,
        timeout_seconds: float = 10.0,
        signature_verifier: AuthoritySignatureVerifier | None = None,
        allow_test_authority: bool = False,
    ) -> None:
        self.path = Path(path)
        self.initial_policy_path = (
            Path(initial_policy_path)
            if initial_policy_path is not None
            else Path(__file__).parents[1] / "governance" / "initial_champion_scenario_policy.v1.json"
        )
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(
            self.path,
            timeout=timeout_seconds,
            isolation_level=None,
            check_same_thread=False,
        )
        self.connection.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        validate_authority_verifier(
            signature_verifier,
            allow_test_authority=allow_test_authority,
        )
        self.signature_verifier = signature_verifier
        self.allow_test_authority = allow_test_authority
        self._before_commit: Callable[[], None] = lambda: None
        self._after_active_head_read: Callable[[], None] = lambda: None
        self._configure()
        self._create_schema()
        contract, source_hash = self._load_initial_policy()
        self._initial_contract = contract
        self._initial_source_hash = source_hash
        self._initial_marker_hash = _initial_marker_hash(contract, source_hash)

    def __enter__(self) -> "PolicyAuthorityLedger":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def close(self) -> None:
        self.connection.close()

    @property
    def journal_mode(self) -> str:
        return str(self.connection.execute("PRAGMA journal_mode").fetchone()[0]).lower()

    @property
    def synchronous(self) -> str:
        value = int(self.connection.execute("PRAGMA synchronous").fetchone()[0])
        return {0: "off", 1: "normal", 2: "full", 3: "extra"}.get(value, str(value))

    @property
    def initial_source_hash(self) -> str:
        return self._initial_source_hash

    @property
    def initial_marker_hash(self) -> str:
        return self._initial_marker_hash

    def append_promotion(
        self,
        authority: PromotionAuthority | Mapping[str, object],
        *,
        policy_payload: Mapping[str, object],
        artifact_output: str | Path | None = None,
    ) -> PromotionAuthority:
        marker = verify_promotion_authority(
            authority,
            signature_verifier=self.signature_verifier,
            allow_test_authority=self.allow_test_authority,
        )
        payload = _verified_policy_payload(
            policy_payload,
            expected_version=marker.current_policy_version,
            expected_hash=marker.current_policy_hash,
        )
        created: Path | None = None
        try:
            with self._write_transaction() as cursor:
                state = self._verify_chain_locked(cursor)
                self._require_next(marker.sequence, marker.previous_authority_hash, state)
                if marker.initial_policy_source_hash != self._initial_source_hash:
                    raise AuthorityConflict("promotion initial-policy source hash mismatch")
                if (marker.prior_policy_version, marker.prior_policy_hash) != (
                    state.policy_version,
                    state.policy_hash,
                ):
                    raise AuthorityConflict("promotion prior policy is not the current head")
                if _version_parts(marker.current_policy_version) <= _version_parts(state.policy_version):
                    raise AuthorityConflict("promotion version is not monotonic")
                self._insert_event(cursor, "PROMOTION", marker.to_dict(), payload)
                if artifact_output is not None:
                    created = _write_authority_artifact(artifact_output, marker.to_dict())
                self._before_commit()
        except Exception:
            _remove_new_artifact(created)
            raise
        return marker

    def append_a_grade(
        self,
        authority: AGradeAuthority | Mapping[str, object],
        *,
        artifact_output: str | Path | None = None,
    ) -> AGradeAuthority:
        marker = verify_a_grade_authority(
            authority,
            signature_verifier=self.signature_verifier,
            allow_test_authority=self.allow_test_authority,
        )
        created: Path | None = None
        try:
            with self._write_transaction() as cursor:
                state = self._verify_chain_locked(cursor)
                self._require_next(marker.sequence, marker.previous_authority_hash, state)
                if marker.revoked or marker.rolled_back:
                    raise AuthorityConflict("inactive A-grade authority cannot be appended")
                if (
                    marker.current_policy_version != state.policy_version
                    or marker.current_policy_hash != state.policy_hash
                    or marker.policy_authority_marker_hash != state.policy_marker_hash
                ):
                    raise AuthorityConflict("A-grade authority is not bound to the current policy head")
                self._insert_event(cursor, "A_GRADE", marker.to_dict(), None)
                if artifact_output is not None:
                    created = _write_authority_artifact(artifact_output, marker.to_dict())
                self._before_commit()
        except Exception:
            _remove_new_artifact(created)
            raise
        return marker

    def append_rollback(
        self,
        authority: RollbackAuthority | Mapping[str, object],
        *,
        artifact_output: str | Path | None = None,
    ) -> RollbackAuthority:
        marker = verify_rollback_authority(
            authority,
            signature_verifier=self.signature_verifier,
            allow_test_authority=self.allow_test_authority,
        )
        created: Path | None = None
        try:
            with self._write_transaction() as cursor:
                state = self._verify_chain_locked(cursor)
                self._require_next(marker.sequence, marker.previous_authority_hash, state)
                if marker.initial_policy_source_hash != self._initial_source_hash:
                    raise AuthorityConflict("rollback initial-policy source hash mismatch")
                if (marker.prior_policy_version, marker.prior_policy_hash) != (
                    state.policy_version,
                    state.policy_hash,
                ):
                    raise AuthorityConflict("rollback prior policy is not the current head")
                history = self._policy_history_locked(cursor)
                target = (marker.current_policy_version, marker.current_policy_hash)
                if target not in history:
                    raise AuthorityConflict("rollback target is not a prior signed policy")
                self._insert_event(cursor, "ROLLBACK", marker.to_dict(), None)
                active_rows = cursor.execute(
                    """
                    SELECT sequence, content_hash, authority_json
                    FROM authority_events
                    WHERE kind='A_GRADE'
                    ORDER BY sequence
                    """
                ).fetchall()
                for row in active_rows:
                    a_grade = verify_a_grade_authority(
                        json.loads(str(row["authority_json"])),
                        signature_verifier=self.signature_verifier,
                        allow_test_authority=self.allow_test_authority,
                    )
                    if a_grade.current_policy_hash != marker.prior_policy_hash:
                        continue
                    already = cursor.execute(
                        "SELECT 1 FROM a_grade_revocations WHERE marker_hash=?",
                        (a_grade.content_hash,),
                    ).fetchone()
                    if already is None:
                        revoked_at = marker.signed_at.isoformat(timespec="microseconds")
                        revocation = {
                            "schema": "options_copilot.learning.a_grade_revocation.v1",
                            "marker_hash": a_grade.content_hash,
                            "policy_hash": a_grade.current_policy_hash,
                            "rollback_authority_hash": marker.content_hash,
                            "revoked_at": revoked_at,
                            "reason": "POLICY_ROLLED_BACK",
                        }
                        cursor.execute(
                            """
                            INSERT INTO a_grade_revocations(
                                marker_hash, policy_hash, rollback_authority_hash,
                                revoked_at, reason, row_hash
                            ) VALUES(?,?,?,?,?,?)
                            """,
                            (
                                a_grade.content_hash,
                                a_grade.current_policy_hash,
                                marker.content_hash,
                                revoked_at,
                                "POLICY_ROLLED_BACK",
                                canonical_hash(revocation),
                            ),
                        )
                if artifact_output is not None:
                    created = _write_authority_artifact(artifact_output, marker.to_dict())
                self._before_commit()
        except Exception:
            _remove_new_artifact(created)
            raise
        return marker

    def is_a_grade_active(
        self,
        marker_hash: str,
        *,
        asof: datetime | None = None,
    ) -> bool:
        with self._write_transaction() as cursor:
            state = self._verify_chain_locked(cursor)
            self._after_active_head_read()
            row = cursor.execute(
                "SELECT authority_json FROM authority_events WHERE kind='A_GRADE' AND content_hash=?",
                (marker_hash,),
            ).fetchone()
            if row is None:
                return False
            marker = verify_a_grade_authority(
                json.loads(str(row["authority_json"])),
                signature_verifier=self.signature_verifier,
                allow_test_authority=self.allow_test_authority,
            )
            if marker.revoked or marker.rolled_back:
                return False
            checked_at = utc_datetime(asof or datetime.now(timezone.utc), field="asof")
            if marker.signed_at > checked_at or marker.expires_at <= checked_at:
                return False
            if (
                marker.current_policy_version != state.policy_version
                or marker.current_policy_hash != state.policy_hash
                or marker.policy_authority_marker_hash != state.policy_marker_hash
            ):
                return False
            if cursor.execute(
                "SELECT 1 FROM a_grade_revocations WHERE marker_hash=?",
                (marker_hash,),
            ).fetchone() is not None:
                return False
            final_state = self._verify_chain_locked(cursor)
            return (
                final_state.sequence == state.sequence
                and final_state.authority_head_hash == state.authority_head_hash
                and final_state.policy_version == state.policy_version
                and final_state.policy_hash == state.policy_hash
                and final_state.policy_marker_hash == state.policy_marker_hash
            )

    def verify_integrity(self) -> bool:
        with self._lock:
            self._verify_chain_locked(self.connection.cursor())
        return True

    def current_state(self) -> "_ChainState":
        with self._lock:
            return self._verify_chain_locked(self.connection.cursor())

    def current_a_grade_marker(self) -> Mapping[str, object] | None:
        """Return only an A-grade marker that is the verified ledger head.

        NORMAL risk authority is represented by ``None``.  An older A-grade
        event must never become current again after a promotion or rollback,
        so this reader intentionally inspects only the complete authority
        head rather than searching backward for an apparently usable marker.
        """

        with self._lock:
            cursor = self.connection.cursor()
            state = self._verify_chain_locked(cursor)
            if state.sequence == 0:
                return None
            row = cursor.execute(
                """
                SELECT kind, authority_json, content_hash
                FROM authority_events
                WHERE sequence=?
                """,
                (state.sequence,),
            ).fetchone()
            if row is None:
                raise PolicyAuthorityTampered("current authority head is missing")
            if str(row["content_hash"]) != state.authority_head_hash:
                raise PolicyAuthorityTampered("current authority head hash differs")
            if str(row["kind"]) != "A_GRADE":
                return None
            marker = verify_a_grade_authority(
                json.loads(str(row["authority_json"])),
                signature_verifier=self.signature_verifier,
                allow_test_authority=self.allow_test_authority,
            )
            if (
                marker.current_policy_version != state.policy_version
                or marker.current_policy_hash != state.policy_hash
                or marker.policy_authority_marker_hash != state.policy_marker_hash
            ):
                raise PolicyAuthorityTampered(
                    "current A-grade marker differs from the policy head"
                )
            result = freeze_json(marker.to_dict())
            assert isinstance(result, Mapping)
            return result

    def guard_read(self, callback: Callable[[], object]) -> object | None:
        """Hold the authority write lock while a restricted callback commits."""

        if not callable(callback):
            raise TypeError("callback must be callable")
        with self._lock:
            nested = self.connection.in_transaction
            if not nested:
                self.connection.execute("BEGIN IMMEDIATE")
            try:
                before = self._verify_chain_locked(self.connection.cursor())
                result = callback()
                after = self._verify_chain_locked(self.connection.cursor())
                if (
                    after.sequence != before.sequence
                    or after.authority_head_hash != before.authority_head_hash
                    or after.policy_version != before.policy_version
                    or after.policy_hash != before.policy_hash
                    or after.policy_marker_hash != before.policy_marker_hash
                ):
                    if not nested:
                        self.connection.rollback()
                    return None
                if not nested:
                    self.connection.commit()
                return result
            except BaseException:
                if not nested:
                    self.connection.rollback()
                raise

    def current_authority_signed_at(self) -> datetime:
        with self._lock:
            state = self._verify_chain_locked(self.connection.cursor())
            if state.sequence == 0:
                return self._initial_contract.signed_at
            row = self.connection.execute(
                "SELECT kind, authority_json FROM authority_events WHERE sequence=?",
                (state.sequence,),
            ).fetchone()
            if row is None:
                raise PolicyAuthorityTampered("current authority head is missing")
            document = json.loads(str(row["authority_json"]))
            kind = str(row["kind"])
            if kind == "PROMOTION":
                return verify_promotion_authority(
                    document,
                    signature_verifier=self.signature_verifier,
                    allow_test_authority=self.allow_test_authority,
                ).signed_at
            if kind == "ROLLBACK":
                return verify_rollback_authority(
                    document,
                    signature_verifier=self.signature_verifier,
                    allow_test_authority=self.allow_test_authority,
                ).signed_at
            return verify_a_grade_authority(
                document,
                signature_verifier=self.signature_verifier,
                allow_test_authority=self.allow_test_authority,
            ).signed_at

    def policy_payload(self, version: str, policy_hash: str) -> Mapping[str, object]:
        if version == self._initial_contract.version and policy_hash == self._initial_contract.contract_hash:
            return self._initial_contract.payload
        with self._lock:
            self._verify_chain_locked(self.connection.cursor())
            rows = self.connection.execute(
                """
                SELECT authority_json, policy_payload_json FROM authority_events
                WHERE kind='PROMOTION'
                ORDER BY sequence DESC
                """
            ).fetchall()
            for item in rows:
                raw = item["policy_payload_json"]
                if raw is None:
                    continue
                payload = json.loads(str(raw))
                authority = verify_promotion_authority(
                    json.loads(str(item["authority_json"])),
                    signature_verifier=self.signature_verifier,
                    allow_test_authority=self.allow_test_authority,
                )
                if authority.current_policy_version == version and authority.current_policy_hash == policy_hash:
                    verified = _verified_policy_payload(
                        payload,
                        expected_version=version,
                        expected_hash=policy_hash,
                    )
                    if "contract_hash" in verified:
                        contract = verify_contract(verified)
                        return contract.payload
                    return freeze_json(verified)  # type: ignore[return-value]
        raise AuthorityNotFound("current policy payload is unavailable")

    def current_policy_contract_document(
        self,
        version: str,
        policy_hash: str,
    ) -> Mapping[str, object] | None:
        """Return only a complete signed contract bound to the current head.

        Historical raw policy payloads remain valid inputs for evaluation and
        resolver replay, but they are not production candidate authority and
        therefore return ``None`` here.
        """

        with self._lock:
            state = self._verify_chain_locked(self.connection.cursor())
            if (state.policy_version, state.policy_hash) != (version, policy_hash):
                return None
            if (
                version == self._initial_contract.version
                and policy_hash == self._initial_contract.contract_hash
            ):
                contract = self._verify_initial_unchanged()
                return contract.to_dict()
            rows = self.connection.execute(
                """
                SELECT authority_json, policy_payload_json FROM authority_events
                WHERE kind='PROMOTION'
                ORDER BY sequence DESC
                """
            ).fetchall()
            for item in rows:
                raw = item["policy_payload_json"]
                if raw is None:
                    continue
                authority = verify_promotion_authority(
                    json.loads(str(item["authority_json"])),
                    signature_verifier=self.signature_verifier,
                    allow_test_authority=self.allow_test_authority,
                )
                if (
                    authority.current_policy_version != version
                    or authority.current_policy_hash != policy_hash
                ):
                    continue
                try:
                    payload = json.loads(str(raw))
                    contract = verify_contract(
                        payload,
                        expected_kind=(
                            ContractKind.INITIAL_CHAMPION_SCENARIO_POLICY
                        ),
                        expected_version=version,
                        expected_hash=policy_hash,
                    )
                except (ContractError, TypeError, ValueError):
                    return None
                return contract.to_dict()
        return None

    def _configure(self) -> None:
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=FULL")
        self.connection.execute("PRAGMA foreign_keys=ON")

    def _create_schema(self) -> None:
        with self.connection:
            self.connection.execute(
                "CREATE TABLE IF NOT EXISTS authority_schema(version INTEGER NOT NULL)"
            )
            rows = self.connection.execute("SELECT version FROM authority_schema").fetchall()
            if not rows:
                self.connection.execute("INSERT INTO authority_schema(version) VALUES(?)", (SCHEMA_VERSION,))
            elif len(rows) != 1 or int(rows[0][0]) != SCHEMA_VERSION:
                raise PolicyAuthorityTampered("unsupported authority schema version")
            self.connection.execute(
                """
                CREATE TABLE IF NOT EXISTS authority_events(
                    sequence INTEGER PRIMARY KEY,
                    kind TEXT NOT NULL CHECK(kind IN ('PROMOTION','A_GRADE','ROLLBACK')),
                    authority_json TEXT NOT NULL,
                    content_hash TEXT NOT NULL UNIQUE,
                    previous_authority_hash TEXT NOT NULL,
                    policy_payload_json TEXT,
                    policy_payload_hash TEXT,
                    appended_at TEXT NOT NULL
                )
                """
            )
            self.connection.execute(
                """
                CREATE TABLE IF NOT EXISTS a_grade_revocations(
                    marker_hash TEXT PRIMARY KEY,
                    policy_hash TEXT NOT NULL,
                    rollback_authority_hash TEXT NOT NULL,
                    revoked_at TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    row_hash TEXT NOT NULL
                )
                """
            )

    def _load_initial_policy(self) -> tuple[SignedContract, str]:
        raw = self.initial_policy_path.read_bytes()
        source_hash = hashlib.sha256(raw).hexdigest()
        contract = load_contract(
            self.initial_policy_path,
            expected_kind=ContractKind.INITIAL_CHAMPION_SCENARIO_POLICY,
        )
        return verify_contract(contract), source_hash

    def _verify_initial_unchanged(self) -> SignedContract:
        contract, source_hash = self._load_initial_policy()
        if source_hash != self._initial_source_hash or contract != self._initial_contract:
            raise PolicyAuthorityTampered("immutable initial policy source changed")
        return contract

    def _verify_chain_locked(self, cursor: sqlite3.Cursor) -> "_ChainState":
        initial = self._verify_initial_unchanged()
        state = _ChainState(
            sequence=0,
            authority_head_hash=self._initial_marker_hash,
            policy_version=initial.version,
            policy_hash=initial.contract_hash,
            policy_marker_hash=self._initial_marker_hash,
        )
        policy_history = {(initial.version, initial.contract_hash)}
        rows = cursor.execute(
            "SELECT * FROM authority_events ORDER BY sequence"
        ).fetchall()
        for expected_sequence, row in enumerate(rows, start=1):
            if int(row["sequence"]) != expected_sequence:
                raise PolicyAuthorityTampered("authority sequence is not contiguous")
            try:
                document = json.loads(str(row["authority_json"]))
            except json.JSONDecodeError as exc:
                raise PolicyAuthorityTampered("authority JSON is corrupt") from exc
            kind = str(row["kind"])
            try:
                if kind == "PROMOTION":
                    marker: object = verify_promotion_authority(
                        document,
                        signature_verifier=self.signature_verifier,
                        allow_test_authority=self.allow_test_authority,
                    )
                elif kind == "ROLLBACK":
                    marker = verify_rollback_authority(
                        document,
                        signature_verifier=self.signature_verifier,
                        allow_test_authority=self.allow_test_authority,
                    )
                elif kind == "A_GRADE":
                    marker = verify_a_grade_authority(
                        document,
                        signature_verifier=self.signature_verifier,
                        allow_test_authority=self.allow_test_authority,
                    )
                else:
                    raise PolicyAuthorityTampered("authority kind is invalid")
            except ValueError as exc:
                raise PolicyAuthorityTampered(str(exc)) from exc
            sequence = int(getattr(marker, "sequence"))
            content_hash = str(getattr(marker, "content_hash"))
            previous = str(getattr(marker, "previous_authority_hash"))
            if sequence != expected_sequence:
                raise PolicyAuthorityTampered("document sequence differs from ledger sequence")
            if content_hash != row["content_hash"]:
                raise PolicyAuthorityTampered("authority content hash differs from ledger row")
            if previous != row["previous_authority_hash"] or previous != state.authority_head_hash:
                raise PolicyAuthorityTampered("authority hash chain is broken")
            payload_json = row["policy_payload_json"]
            payload_hash = row["policy_payload_hash"]
            if kind == "PROMOTION":
                if payload_json is None or payload_hash is None:
                    raise PolicyAuthorityTampered("promotion policy payload is missing")
                if canonical_hash(json.loads(str(payload_json))) != payload_hash:
                    raise PolicyAuthorityTampered("promotion policy payload was tampered")
                try:
                    _verified_policy_payload(
                        json.loads(str(payload_json)),
                        expected_version=str(getattr(marker, "current_policy_version")),
                        expected_hash=str(getattr(marker, "current_policy_hash")),
                    )
                except (AuthorityConflict, TypeError, ValueError) as exc:
                    raise PolicyAuthorityTampered(
                        "promotion payload does not match its authority"
                    ) from exc
            elif payload_json is not None or payload_hash is not None:
                raise PolicyAuthorityTampered("non-promotion event carries a policy payload")
            if kind in {"PROMOTION", "ROLLBACK"}:
                if getattr(marker, "initial_policy_source_hash") != self._initial_source_hash:
                    raise PolicyAuthorityTampered("policy authority binds the wrong initial source")
                if (
                    getattr(marker, "prior_policy_version") != state.policy_version
                    or getattr(marker, "prior_policy_hash") != state.policy_hash
                ):
                    raise PolicyAuthorityTampered("policy transition does not bind the prior head")
                target = (
                    str(getattr(marker, "current_policy_version")),
                    str(getattr(marker, "current_policy_hash")),
                )
                if kind == "PROMOTION":
                    if _version_parts(target[0]) <= _version_parts(state.policy_version):
                        raise PolicyAuthorityTampered("promotion policy version is not monotonic")
                    policy_history.add(target)
                elif target not in policy_history:
                    raise PolicyAuthorityTampered("rollback target is not in signed policy history")
                state = _ChainState(
                    expected_sequence,
                    content_hash,
                    str(getattr(marker, "current_policy_version")),
                    str(getattr(marker, "current_policy_hash")),
                    content_hash,
                )
            else:
                if (
                    getattr(marker, "revoked")
                    or getattr(marker, "rolled_back")
                    or getattr(marker, "current_policy_version") != state.policy_version
                    or getattr(marker, "current_policy_hash") != state.policy_hash
                    or getattr(marker, "policy_authority_marker_hash") != state.policy_marker_hash
                ):
                    raise PolicyAuthorityTampered("A-grade authority is not active on its append head")
                state = _ChainState(
                    expected_sequence,
                    content_hash,
                    state.policy_version,
                    state.policy_hash,
                    state.policy_marker_hash,
                )
        self._verify_revocations_locked(cursor, rows)
        return state

    def _verify_revocations_locked(self, cursor: sqlite3.Cursor, events: list[sqlite3.Row]) -> None:
        event_hashes = {str(row["content_hash"]): str(row["kind"]) for row in events}
        event_sequences = {str(row["content_hash"]): int(row["sequence"]) for row in events}
        revocations: dict[str, tuple[str, str]] = {}
        for row in cursor.execute("SELECT * FROM a_grade_revocations ORDER BY marker_hash"):
            document = {
                "schema": "options_copilot.learning.a_grade_revocation.v1",
                "marker_hash": row["marker_hash"], "policy_hash": row["policy_hash"],
                "rollback_authority_hash": row["rollback_authority_hash"],
                "revoked_at": row["revoked_at"], "reason": row["reason"],
            }
            if canonical_hash(document) != row["row_hash"]:
                raise PolicyAuthorityTampered("A-grade revocation row was tampered")
            if event_hashes.get(str(row["marker_hash"])) != "A_GRADE":
                raise PolicyAuthorityTampered("revocation does not reference an A-grade authority")
            if event_hashes.get(str(row["rollback_authority_hash"])) != "ROLLBACK":
                raise PolicyAuthorityTampered("revocation does not reference a rollback authority")
            if event_sequences[str(row["marker_hash"])] >= event_sequences[str(row["rollback_authority_hash"])]:
                raise PolicyAuthorityTampered("revocation marker does not predate rollback")
            revocations[str(row["marker_hash"])] = (
                str(row["rollback_authority_hash"]),
                str(row["policy_hash"]),
            )
        a_grades: list[AGradeAuthority] = []
        for event in events:
            document = json.loads(str(event["authority_json"]))
            if event["kind"] == "A_GRADE":
                a_grades.append(
                    verify_a_grade_authority(
                        document,
                        signature_verifier=self.signature_verifier,
                        allow_test_authority=self.allow_test_authority,
                    )
                )
            elif event["kind"] == "ROLLBACK":
                rollback = verify_rollback_authority(
                    document,
                    signature_verifier=self.signature_verifier,
                    allow_test_authority=self.allow_test_authority,
                )
                remaining: list[AGradeAuthority] = []
                for a_grade in a_grades:
                    if a_grade.current_policy_hash != rollback.prior_policy_hash:
                        remaining.append(a_grade)
                        continue
                    recorded = revocations.get(a_grade.content_hash)
                    if recorded != (rollback.content_hash, a_grade.current_policy_hash):
                        raise PolicyAuthorityTampered(
                            "rollback is missing an atomic A-grade revocation"
                        )
                a_grades = remaining

    def _policy_history_locked(self, cursor: sqlite3.Cursor) -> set[tuple[str, str]]:
        history = {(self._initial_contract.version, self._initial_contract.contract_hash)}
        for row in cursor.execute("SELECT kind, authority_json FROM authority_events ORDER BY sequence"):
            if row["kind"] == "PROMOTION":
                marker = verify_promotion_authority(
                    json.loads(str(row["authority_json"])),
                    signature_verifier=self.signature_verifier,
                    allow_test_authority=self.allow_test_authority,
                )
                history.add((marker.current_policy_version, marker.current_policy_hash))
        return history

    def _require_next(self, sequence: int, previous: str | None, state: "_ChainState") -> None:
        if sequence != state.sequence + 1:
            raise AuthorityConflict("authority sequence is stale or non-monotonic")
        if previous != state.authority_head_hash:
            raise AuthorityConflict("authority previous hash is not the current authority head")

    def _insert_event(
        self,
        cursor: sqlite3.Cursor,
        kind: str,
        document: Mapping[str, object],
        policy_payload: Mapping[str, object] | None,
    ) -> None:
        payload_json = None if policy_payload is None else canonical_json(policy_payload)
        payload_hash = None if policy_payload is None else canonical_hash(policy_payload)
        cursor.execute(
            """
            INSERT INTO authority_events(
                sequence, kind, authority_json, content_hash,
                previous_authority_hash, policy_payload_json,
                policy_payload_hash, appended_at
            ) VALUES(?,?,?,?,?,?,?,?)
            """,
            (
                document["sequence"], kind, canonical_json(document),
                document["content_hash"], document["previous_authority_hash"],
                payload_json, payload_hash, datetime.now(timezone.utc).isoformat(timespec="microseconds"),
            ),
        )

    class _ImmediateTransaction:
        def __init__(self, outer: "PolicyAuthorityLedger") -> None:
            self.outer = outer
            self.cursor: sqlite3.Cursor | None = None

        def __enter__(self) -> sqlite3.Cursor:
            self.outer._lock.acquire()
            try:
                self.outer.connection.execute("BEGIN IMMEDIATE")
                self.cursor = self.outer.connection.cursor()
                return self.cursor
            except Exception:
                self.outer._lock.release()
                raise

        def __exit__(self, exc_type: object, exc: object, traceback: object) -> bool:
            try:
                if exc_type is None:
                    self.outer.connection.commit()
                else:
                    self.outer.connection.rollback()
            finally:
                self.outer._lock.release()
            return False

    def _write_transaction(self) -> "PolicyAuthorityLedger._ImmediateTransaction":
        return PolicyAuthorityLedger._ImmediateTransaction(self)


class CurrentPolicyResolver:
    """Resolve only the verified durable head or immutable initial fallback."""

    def __init__(
        self,
        ledger: PolicyAuthorityLedger | str | Path,
        *,
        initial_policy_path: str | Path | None = None,
        signature_verifier: AuthoritySignatureVerifier | None = None,
        maximum_initial_age: timedelta | None = None,
        allow_test_authority: bool = False,
    ) -> None:
        if isinstance(ledger, PolicyAuthorityLedger):
            if initial_policy_path is not None:
                raise ValueError("initial_policy_path belongs to an owned ledger only")
            self.ledger = ledger
            self._owns_ledger = False
        else:
            self.ledger = PolicyAuthorityLedger(
                ledger,
                initial_policy_path=initial_policy_path,
                signature_verifier=signature_verifier,
                allow_test_authority=allow_test_authority,
            )
            self._owns_ledger = True
        if maximum_initial_age is not None and (
            not isinstance(maximum_initial_age, timedelta)
            or maximum_initial_age <= timedelta(0)
        ):
            raise ValueError(
                "maximum_initial_age must be a positive timedelta or None"
            )
        self.maximum_initial_age = maximum_initial_age

    def close(self) -> None:
        if self._owns_ledger:
            self.ledger.close()

    def __enter__(self) -> "CurrentPolicyResolver":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def resolve(self, *, now: datetime) -> ResolvedPolicy:
        observation_time = utc_datetime(now, field="now")
        state = self.ledger.current_state()
        initial = self.ledger._verify_initial_unchanged()
        head_signed_at = self.ledger.current_authority_signed_at()
        if head_signed_at > observation_time:
            raise PolicyAuthorityError("current authority signature is in the future")
        if state.policy_version == initial.version and state.policy_hash == initial.contract_hash:
            payload = initial.payload
            effective_at = initial.effective_at
            if initial.effective_at > observation_time or initial.signed_at > observation_time:
                raise PolicyAuthorityError("initial policy is not yet effective")
            if (
                self.maximum_initial_age is not None
                and observation_time - initial.effective_at
                > self.maximum_initial_age
            ):
                raise PolicyAuthorityError("initial policy freshness window expired")
        else:
            payload = self.ledger.policy_payload(state.policy_version, state.policy_hash)
            effective_at = _policy_effective_time(self.ledger, state.policy_marker_hash)
            if effective_at > observation_time:
                raise PolicyAuthorityError("current policy authority is in the future")
        calibration = payload.get("calibration", {}) if isinstance(payload, Mapping) else {}
        return ResolvedPolicy(
            current_policy_version=state.policy_version,
            current_policy_hash=state.policy_hash,
            policy_authority_marker_hash=state.policy_marker_hash,
            effective_at=effective_at,
            payload=freeze_json(payload),
            calibration_provenance=freeze_json(calibration),
        )

    def is_current(self, resolution: ResolvedPolicy) -> bool:
        if not isinstance(resolution, ResolvedPolicy):
            return False
        try:
            current = self.resolve(now=datetime.now(timezone.utc))
        except Exception:
            return False
        return current == resolution

    def guard_current(
        self,
        resolution: ResolvedPolicy,
        *,
        callback: Callable[[], object],
    ) -> object | None:
        if not isinstance(resolution, ResolvedPolicy) or not callable(callback):
            return None

        def guarded() -> object | None:
            if not self.is_current(resolution):
                return None
            return callback()

        return self.ledger.guard_read(guarded)

    def policy_contract_document(
        self,
        resolution: ResolvedPolicy,
    ) -> Mapping[str, object] | None:
        """Read one production contract document under the authority lease."""

        if not isinstance(resolution, ResolvedPolicy):
            return None

        def read_current_contract() -> Mapping[str, object] | None:
            return self.ledger.current_policy_contract_document(
                resolution.current_policy_version,
                resolution.current_policy_hash,
            )

        result = self.guard_current(
            resolution,
            callback=read_current_contract,
        )
        return result if isinstance(result, Mapping) else None


class _ChainState:
    __slots__ = ("sequence", "authority_head_hash", "policy_version", "policy_hash", "policy_marker_hash")

    def __init__(self, sequence: int, authority_head_hash: str, policy_version: str, policy_hash: str, policy_marker_hash: str) -> None:
        self.sequence = sequence
        self.authority_head_hash = authority_head_hash
        self.policy_version = policy_version
        self.policy_hash = policy_hash
        self.policy_marker_hash = policy_marker_hash


def _initial_marker_hash(contract: SignedContract, source_hash: str) -> str:
    return canonical_hash({
        "schema": "options_copilot.learning.initial_policy_authority.v1",
        "source_hash": source_hash,
        "current_policy_version": contract.version,
        "current_policy_hash": contract.contract_hash,
        "immutable": True,
    })


def _policy_effective_time(ledger: PolicyAuthorityLedger, marker_hash: str) -> datetime:
    row = ledger.connection.execute(
        "SELECT authority_json FROM authority_events WHERE content_hash=?",
        (marker_hash,),
    ).fetchone()
    if row is None:
        raise PolicyAuthorityTampered("policy authority marker is missing")
    document = json.loads(str(row[0]))
    if document.get("schema", "").endswith("promotion_authority.v1"):
        return verify_promotion_authority(
            document,
            signature_verifier=ledger.signature_verifier,
            allow_test_authority=ledger.allow_test_authority,
        ).signed_at
    return verify_rollback_authority(
        document,
        signature_verifier=ledger.signature_verifier,
        allow_test_authority=ledger.allow_test_authority,
    ).signed_at


def _mapping(value: object, *, field: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or not value:
        raise ValueError(f"{field} must be a nonempty mapping")
    canonical_json(value)
    return value


def _verified_policy_payload(
    value: object,
    *,
    expected_version: str,
    expected_hash: str,
) -> Mapping[str, object]:
    document = _mapping(value, field="policy_payload")
    if {
        "schema", "contract_kind", "version", "effective_at", "provenance",
        "payload", "actor", "signed_at", "supersedes_version",
        "supersedes_hash", "contract_hash",
    } <= set(document):
        contract = verify_contract(
            document,
            expected_kind=ContractKind.INITIAL_CHAMPION_SCENARIO_POLICY,
            expected_version=expected_version,
            expected_hash=expected_hash,
        )
        return document
    if canonical_hash(document) != expected_hash:
        raise AuthorityConflict("promoted policy payload does not match current_policy_hash")
    return document


def _version_parts(value: str) -> tuple[int, ...]:
    return tuple(int(part) for part in value[1:].split("."))


def _write_authority_artifact(
    path: str | Path,
    document: Mapping[str, object],
) -> Path:
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    rendered = json.dumps(
        document,
        ensure_ascii=False,
        allow_nan=False,
        indent=2,
        sort_keys=True,
    ) + "\n"
    with output.open("x", encoding="utf-8", newline="\n") as stream:
        stream.write(rendered)
        stream.flush()
        os.fsync(stream.fileno())
    return output


def _remove_new_artifact(path: Path | None) -> None:
    if path is None:
        return
    try:
        path.unlink(missing_ok=True)
    except OSError:
        # The ledger transaction has already rolled back.  A leftover file is
        # never authoritative because verifier commands also require its exact
        # content hash to exist in the verified SQLite chain.
        pass


__all__ = [
    "AuthorityConflict", "AuthorityNotFound", "CurrentPolicyResolver",
    "PolicyAuthorityError", "PolicyAuthorityLedger", "PolicyAuthorityTampered",
    "SCHEMA_VERSION",
]
