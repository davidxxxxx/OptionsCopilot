"""Append-only Strategy NAV authority for Options Copilot.

The Strategy NAV contract is the sole capital base used by production risk
authorization.  Account NLV is deliberately accepted only as a reconciliation
observation and can never substitute for a missing or invalid ledger.
"""
from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_EVEN
from enum import Enum
import json
from pathlib import Path
import sqlite3
import threading

from options_copilot.governance.contracts import (
    ContractKind,
    ContractValidationError,
    SignedContract,
    load_contract,
    verify_contract,
)
from options_copilot.storage.canonical import (
    canonical_hash,
    canonical_json,
    datetime_text,
    utc_datetime,
)


SCHEMA_VERSION = 1
STRATEGY_NAV_AUTHORITY_SCHEMA = "options_copilot.strategy_nav_authority.v1"
GENESIS_HASH = "0" * 64
_CENT = Decimal("0.01")
_ZERO = Decimal("0")


class NavAttribution(str, Enum):
    STRATEGY = "STRATEGY"
    NON_STRATEGY = "NON_STRATEGY"


class NavEventKind(str, Enum):
    DEPOSIT = "DEPOSIT"
    WITHDRAWAL = "WITHDRAWAL"
    REALIZED_PNL = "REALIZED_PNL"
    UNREALIZED_PNL = "UNREALIZED_PNL"
    FEE = "FEE"
    FILL_PRINCIPAL = "FILL_PRINCIPAL"
    CORRECTION = "CORRECTION"


class StrategyNavLedgerError(RuntimeError):
    """Base error for Strategy NAV ledger operations."""


class StrategyNavUnavailable(StrategyNavLedgerError):
    """The signed contract or immutable anchor is unavailable for writing."""


class StrategyNavLedgerCorruption(StrategyNavLedgerError):
    """Stored rows no longer satisfy the immutable hash-chain contract."""


@dataclass(frozen=True, slots=True)
class NavFlowReceipt:
    sequence: int
    flow_id: str
    content_hash: str
    chain_hash: str
    inserted: bool
    conflict: bool
    event_kind: NavEventKind
    version: int
    supersedes_flow_id: str | None
    supersedes_content_hash: str | None


@dataclass(frozen=True, slots=True)
class StrategyNavSnapshot:
    asof: datetime
    strategy_nav: Decimal | None
    strategy_deposits: Decimal
    strategy_withdrawals: Decimal
    realized_pnl: Decimal
    open_position_unrealized_pnl: Decimal
    fees: Decimal
    signed_corrections: Decimal
    non_strategy_contribution: Decimal
    fill_principal_contribution: Decimal
    observed_account_nlv: Decimal | None
    reconciliation_difference: Decimal | None
    contract_version: str | None
    contract_hash: str | None
    ledger_head_hash: str | None
    valid: bool
    no_trade_reasons: tuple[str, ...]
    content_hash: str

    def hash_payload(self) -> dict[str, object]:
        return {
            "asof": self.asof,
            "strategy_nav": self.strategy_nav,
            "strategy_deposits": self.strategy_deposits,
            "strategy_withdrawals": self.strategy_withdrawals,
            "realized_pnl": self.realized_pnl,
            "open_position_unrealized_pnl": self.open_position_unrealized_pnl,
            "fees": self.fees,
            "signed_corrections": self.signed_corrections,
            "non_strategy_contribution": self.non_strategy_contribution,
            "fill_principal_contribution": self.fill_principal_contribution,
            "observed_account_nlv": self.observed_account_nlv,
            "reconciliation_difference": self.reconciliation_difference,
            "contract_version": self.contract_version,
            "contract_hash": self.contract_hash,
            "ledger_head_hash": self.ledger_head_hash,
            "valid": self.valid,
            "no_trade_reasons": self.no_trade_reasons,
        }

    def authority_payload(self) -> dict[str, object]:
        """Return the stable risk-authority identity for this NAV state.

        Observation time, account NLV, and reconciliation fields intentionally
        remain bound only by ``content_hash``.  They must not make an unchanged
        signed ledger authority appear stale between ranking and approval.
        """

        return _strategy_nav_authority_payload(
            strategy_nav_usd=self.strategy_nav,
            contract_hash=self.contract_hash,
            ledger_head_hash=self.ledger_head_hash,
        )

    @property
    def authority_hash(self) -> str:
        return strategy_nav_authority_hash(
            strategy_nav_usd=self.strategy_nav,
            contract_hash=self.contract_hash,
            ledger_head_hash=self.ledger_head_hash,
        )

    @property
    def strategy_nav_authority_hash(self) -> str:
        """Compatibility alias with a domain-qualified public name."""

        return self.authority_hash


@dataclass(frozen=True, slots=True)
class _ContractAuthority:
    contract: SignedContract
    account_reference: str
    currency: str
    anchor_amount: Decimal
    anchor_effective_at: datetime

    def anchor_document(self) -> dict[str, object]:
        return {
            "contract_version": self.contract.version,
            "contract_hash": self.contract.contract_hash,
            "account_reference": self.account_reference,
            "currency": self.currency,
            "anchor_amount": self.anchor_amount,
            "anchor_effective_at": self.anchor_effective_at,
        }


class StrategyNavLedger:
    """SQLite WAL/FULL Strategy NAV ledger with immutable hash chaining."""

    def __init__(
        self,
        path: str | Path,
        *,
        contract: str | Path | SignedContract | Mapping[str, object] | None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._lock = threading.RLock()
        self._closed = False
        self._contract_path = (
            Path(contract) if isinstance(contract, (str, Path)) else None
        )
        self._authority: _ContractAuthority | None = None
        self._contract_reason: str | None = None
        self._anchor_reason: str | None = None
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
            self._connection.execute("PRAGMA foreign_keys=ON")
            if self._journal_mode != "wal" or self._synchronous != "full":
                raise StrategyNavLedgerError("Strategy NAV ledger durability is unavailable")
            if int(self._connection.execute("PRAGMA foreign_keys").fetchone()[0]) != 1:
                raise StrategyNavLedgerError("Strategy NAV foreign keys are disabled")
            self._migrate()
            self._authority, self._contract_reason = _resolve_contract(contract)
            if self._authority is not None:
                self._ensure_anchor()
        except BaseException:
            self._connection.close()
            self._closed = True
            raise

    def __enter__(self) -> "StrategyNavLedger":
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
    def contract_hash(self) -> str | None:
        return None if self._authority is None else self._authority.contract.contract_hash

    def close(self) -> None:
        with self._lock:
            if not self._closed:
                self._connection.close()
                self._closed = True

    def append_flow(
        self,
        *,
        event_kind: NavEventKind | str,
        broker_event_identifier: str,
        effective_at: datetime,
        amount: Decimal,
        attribution: NavAttribution | str,
        position_id: str | None = None,
    ) -> NavFlowReceipt:
        authority = self._require_authority()
        kind = _event_kind(event_kind, allow_correction=False)
        attributed = _attribution(attribution)
        event_time = utc_datetime(effective_at, field="effective_at")
        value = _amount(kind, amount)
        identifier = _identifier("broker_event_identifier", broker_event_identifier)
        normalized_position = _position(kind, position_id)
        document = _event_document(
            authority=authority,
            event_kind=kind,
            broker_event_identifier=identifier,
            effective_at=event_time,
            amount=value,
            attribution=attributed,
            position_id=normalized_position,
            version=1,
            supersedes_flow_id=None,
            supersedes_content_hash=None,
            correction_delta=None,
            actor=None,
            signed_at=None,
        )
        return self._append_document(document)

    def correct_flow(
        self,
        supersedes_flow_id: str,
        *,
        broker_event_identifier: str,
        effective_at: datetime,
        amount: Decimal,
        actor: str,
        signed_at: datetime,
    ) -> NavFlowReceipt:
        authority = self._require_authority()
        target_id = _hash_text("supersedes_flow_id", supersedes_flow_id)
        identifier = _identifier("broker_event_identifier", broker_event_identifier)
        event_time = utc_datetime(effective_at, field="effective_at")
        signature_time = utc_datetime(signed_at, field="signed_at")
        signer = _identifier("actor", actor)
        if signature_time < event_time:
            raise ValueError("signed_at cannot precede correction effective_at")

        with self._lock:
            self._ensure_open()
            target = self._connection.execute(
                """
                SELECT * FROM nav_flows
                WHERE flow_id = ? AND status = 'APPLIED'
                ORDER BY sequence DESC LIMIT 1
                """,
                (target_id,),
            ).fetchone()
            if target is None:
                raise ValueError("superseded flow does not exist")

        target_kind = NavEventKind(str(target["economic_event_kind"]))
        target_amount = Decimal(str(target["amount"]))
        new_amount = _amount(target_kind, amount)
        if event_time < datetime.fromisoformat(str(target["effective_at"])):
            raise ValueError("correction effective_at cannot precede the prior version")
        document = _event_document(
            authority=authority,
            event_kind=NavEventKind.CORRECTION,
            economic_event_kind=target_kind,
            broker_event_identifier=identifier,
            effective_at=event_time,
            amount=new_amount,
            attribution=NavAttribution(str(target["attribution"])),
            position_id=(
                None if target["position_id"] is None else str(target["position_id"])
            ),
            version=int(target["version"]) + 1,
            supersedes_flow_id=target_id,
            supersedes_content_hash=str(target["content_hash"]),
            correction_delta=new_amount - target_amount,
            actor=signer,
            signed_at=signature_time,
        )
        content_hash = canonical_hash(document)
        flow_id = _flow_id(document)
        with self._lock:
            branch = self._connection.execute(
                """
                SELECT * FROM nav_flows
                WHERE supersedes_flow_id = ? AND status = 'APPLIED'
                ORDER BY sequence LIMIT 1
                """,
                (target_id,),
            ).fetchone()
            if branch is not None:
                if (
                    str(branch["flow_id"]) == flow_id
                    and str(branch["content_hash"]) == content_hash
                ):
                    return _receipt(branch, inserted=False)
                raise ValueError("superseded flow is not the latest correction version")
        try:
            return self._append_document(document)
        except sqlite3.IntegrityError as exc:
            raise ValueError(
                "superseded flow is not the latest correction version"
            ) from exc

    def snapshot(
        self,
        *,
        asof: datetime,
        observed_account_nlv: Decimal | None = None,
    ) -> StrategyNavSnapshot:
        observation_time = utc_datetime(asof, field="asof")
        observed = _optional_nonnegative_decimal(
            observed_account_nlv, "observed_account_nlv"
        )
        reasons: set[str] = set()
        if self._contract_reason is not None:
            reasons.add(self._contract_reason)
        if self._authority is None:
            reasons.add("MISSING_LEDGER_HEAD")
            return _snapshot(
                asof=observation_time,
                observed_account_nlv=observed,
                reasons=reasons,
            )
        if self._anchor_reason is not None:
            reasons.add(self._anchor_reason)

        try:
            self.assert_integrity()
        except (
            StrategyNavLedgerCorruption,
            StrategyNavUnavailable,
            ContractValidationError,
            OSError,
            sqlite3.DatabaseError,
            KeyError,
            TypeError,
            ValueError,
        ):
            reasons.add("LEDGER_INTEGRITY_FAILURE")

        authority = self._authority
        if observation_time < authority.anchor_effective_at:
            reasons.add("SNAPSHOT_BEFORE_STRATEGY_NAV_ANCHOR")
        with self._lock:
            self._ensure_open()
            anchor = self._connection.execute(
                "SELECT * FROM nav_anchor WHERE singleton = 1"
            ).fetchone()
            rows = self._connection.execute(
                """
                SELECT * FROM nav_flows
                WHERE effective_at <= ?
                ORDER BY effective_at, sequence
                """,
                (datetime_text(observation_time),),
            ).fetchall()
            conflict = self._connection.execute(
                "SELECT 1 FROM nav_flows WHERE status = 'CONFLICT' LIMIT 1"
            ).fetchone()
            tail = self._connection.execute(
                "SELECT chain_hash FROM nav_flows ORDER BY sequence DESC LIMIT 1"
            ).fetchone()
        if anchor is None:
            reasons.add("MISSING_LEDGER_HEAD")
            head_hash = None
        else:
            head_hash = str(anchor["chain_hash"]) if tail is None else str(tail[0])
        if conflict is not None:
            reasons.add("FLOW_IDENTITY_CONFLICT")

        if reasons:
            return _snapshot(
                asof=observation_time,
                observed_account_nlv=observed,
                contract_version=authority.contract.version,
                contract_hash=authority.contract.contract_hash,
                ledger_head_hash=head_hash,
                reasons=reasons,
            )

        deposits = _ZERO
        withdrawals = _ZERO
        realized = _ZERO
        fees = _ZERO
        corrections = _ZERO
        latest_marks: dict[str, tuple[datetime, int, Decimal]] = {}
        for row in rows:
            if str(row["status"]) != "APPLIED":
                continue
            if NavAttribution(str(row["attribution"])) is not NavAttribution.STRATEGY:
                continue
            kind = NavEventKind(str(row["event_kind"]))
            economic_kind = NavEventKind(str(row["economic_event_kind"]))
            value = Decimal(str(row["amount"]))
            if kind is NavEventKind.CORRECTION:
                corrections += Decimal(str(row["correction_delta"]))
            elif economic_kind is NavEventKind.DEPOSIT:
                deposits += value
            elif economic_kind is NavEventKind.WITHDRAWAL:
                withdrawals += value
            elif economic_kind is NavEventKind.REALIZED_PNL:
                realized += value
            elif economic_kind is NavEventKind.FEE:
                fees += value
            elif economic_kind is NavEventKind.UNREALIZED_PNL:
                position_id = str(row["position_id"])
                marker = (
                    datetime.fromisoformat(str(row["effective_at"])),
                    int(row["sequence"]),
                    value,
                )
                if position_id not in latest_marks or marker[:2] > latest_marks[position_id][:2]:
                    latest_marks[position_id] = marker

        unrealized = sum((item[2] for item in latest_marks.values()), _ZERO)
        nav = (
            authority.anchor_amount
            + deposits
            - withdrawals
            + realized
            + unrealized
            - fees
            + corrections
        ).quantize(_CENT, rounding=ROUND_HALF_EVEN)
        reconciliation = (
            None
            if observed is None
            else (observed - nav).quantize(_CENT, rounding=ROUND_HALF_EVEN)
        )
        return _snapshot(
            asof=observation_time,
            strategy_nav=nav,
            strategy_deposits=deposits,
            strategy_withdrawals=withdrawals,
            realized_pnl=realized,
            open_position_unrealized_pnl=unrealized,
            fees=fees,
            signed_corrections=corrections,
            observed_account_nlv=observed,
            reconciliation_difference=reconciliation,
            contract_version=authority.contract.version,
            contract_hash=authority.contract.contract_hash,
            ledger_head_hash=head_hash,
            reasons=(),
        )

    def guard_current(
        self,
        snapshot: StrategyNavSnapshot,
        *,
        callback: Callable[[], object],
    ) -> object | None:
        """Linearize one restricted callback against an exact NAV head.

        Lock order for approval is ranking -> policy -> risk -> cost -> NAV ->
        approval.  ``BEGIN IMMEDIATE`` prevents a second ledger connection from
        appending a flow until the callback has finished.  The exact snapshot
        is recomputed before and after the callback so a caller cannot supply a
        detached or stale NAV document.
        """

        if not isinstance(snapshot, StrategyNavSnapshot):
            return None
        if not callable(callback):
            raise TypeError("callback must be callable")
        if not snapshot.valid or snapshot.strategy_nav is None:
            return None

        callback_started = False
        try:
            with self._transaction():
                before = self.snapshot(
                    asof=snapshot.asof,
                    observed_account_nlv=snapshot.observed_account_nlv,
                )
                if before != snapshot:
                    return None
                callback_started = True
                result = callback()
                after = self.snapshot(
                    asof=snapshot.asof,
                    observed_account_nlv=snapshot.observed_account_nlv,
                )
                if after != snapshot:
                    raise StrategyNavLedgerCorruption(
                        "Strategy NAV head changed during guarded callback"
                    )
                return result
        except (
            StrategyNavLedgerError,
            ContractValidationError,
            OSError,
            sqlite3.DatabaseError,
            KeyError,
            TypeError,
            ValueError,
        ):
            if callback_started:
                raise
            return None

    def assert_integrity(self) -> None:
        authority = self._require_authority(check_integrity=False)
        self._verify_contract_source()
        self._ensure_open()
        with self._lock:
            anchor = self._connection.execute(
                "SELECT * FROM nav_anchor WHERE singleton = 1"
            ).fetchone()
            rows = self._connection.execute(
                "SELECT * FROM nav_flows ORDER BY sequence"
            ).fetchall()
        if anchor is None:
            raise StrategyNavLedgerCorruption("immutable NAV anchor is missing")
        anchor_document = _anchor_document_from_row(anchor)
        if anchor_document != authority.anchor_document():
            raise StrategyNavLedgerCorruption("NAV anchor differs from signed contract")
        anchor_json = canonical_json(anchor_document)
        if anchor_json != str(anchor["immutable_json"]):
            raise StrategyNavLedgerCorruption("NAV anchor immutable document mismatch")
        anchor_content = canonical_hash(anchor_document)
        if anchor_content != str(anchor["content_hash"]):
            raise StrategyNavLedgerCorruption("NAV anchor content hash mismatch")
        expected_previous = GENESIS_HASH
        expected_chain = _chain_hash(
            0, expected_previous, anchor_content, "ANCHOR", authority.contract.contract_hash
        )
        if (
            str(anchor["previous_hash"]) != expected_previous
            or str(anchor["chain_hash"]) != expected_chain
        ):
            raise StrategyNavLedgerCorruption("NAV anchor chain hash mismatch")

        expected_sequence = 1
        accepted_by_id: dict[str, str] = {}
        accepted_rows: dict[str, sqlite3.Row] = {}
        expected_previous = expected_chain
        for row in rows:
            sequence = int(row["sequence"])
            if sequence != expected_sequence:
                raise StrategyNavLedgerCorruption("NAV flow sequence contains a gap")
            document = _event_document_from_row(row)
            immutable_json = canonical_json(document)
            if immutable_json != str(row["immutable_json"]):
                raise StrategyNavLedgerCorruption(
                    f"NAV flow immutable document mismatch at sequence {sequence}"
                )
            content_hash = canonical_hash(document)
            if content_hash != str(row["content_hash"]):
                raise StrategyNavLedgerCorruption(
                    f"NAV flow content hash mismatch at sequence {sequence}"
                )
            flow_id = _flow_id(document)
            if flow_id != str(row["flow_id"]):
                raise StrategyNavLedgerCorruption(
                    f"NAV flow identity mismatch at sequence {sequence}"
                )
            status = str(row["status"])
            if status == "APPLIED":
                if flow_id in accepted_by_id:
                    raise StrategyNavLedgerCorruption("duplicate applied NAV flow identity")
                accepted_by_id[flow_id] = content_hash
                accepted_rows[flow_id] = row
            elif status == "CONFLICT":
                if flow_id not in accepted_by_id or accepted_by_id[flow_id] == content_hash:
                    raise StrategyNavLedgerCorruption("invalid NAV conflict record")
            else:
                raise StrategyNavLedgerCorruption("unknown NAV flow status")
            supersedes = row["supersedes_flow_id"]
            if supersedes is not None:
                prior = accepted_rows.get(str(supersedes))
                if prior is None:
                    raise StrategyNavLedgerCorruption("correction predecessor is missing")
                if str(row["supersedes_content_hash"]) != str(prior["content_hash"]):
                    raise StrategyNavLedgerCorruption("correction predecessor hash mismatch")
                if int(row["version"]) != int(prior["version"]) + 1:
                    raise StrategyNavLedgerCorruption("correction version is not monotonic")
            chain_hash = _chain_hash(
                sequence, expected_previous, content_hash, status, flow_id
            )
            if (
                str(row["previous_hash"]) != expected_previous
                or str(row["chain_hash"]) != chain_hash
            ):
                raise StrategyNavLedgerCorruption(
                    f"NAV flow chain mismatch at sequence {sequence}"
                )
            expected_previous = chain_hash
            expected_sequence += 1

    def _append_document(self, document: dict[str, object]) -> NavFlowReceipt:
        self._require_authority()
        immutable_json = canonical_json(document)
        content_hash = canonical_hash(document)
        flow_id = _flow_id(document)
        recorded_at = utc_datetime(self._clock(), field="clock result")
        with self._transaction():
            exact = self._connection.execute(
                """
                SELECT * FROM nav_flows
                WHERE flow_id = ? AND content_hash = ?
                ORDER BY sequence LIMIT 1
                """,
                (flow_id, content_hash),
            ).fetchone()
            if exact is not None:
                return _receipt(exact, inserted=False)
            prior = self._connection.execute(
                """
                SELECT * FROM nav_flows
                WHERE flow_id = ? AND status = 'APPLIED'
                ORDER BY sequence LIMIT 1
                """,
                (flow_id,),
            ).fetchone()
            status = "CONFLICT" if prior is not None else "APPLIED"
            tail = self._connection.execute(
                "SELECT sequence, chain_hash FROM nav_flows ORDER BY sequence DESC LIMIT 1"
            ).fetchone()
            if tail is None:
                anchor = self._connection.execute(
                    "SELECT chain_hash FROM nav_anchor WHERE singleton = 1"
                ).fetchone()
                if anchor is None:
                    raise StrategyNavUnavailable("MISSING_LEDGER_HEAD")
                sequence = 1
                previous_hash = str(anchor[0])
            else:
                sequence = int(tail["sequence"]) + 1
                previous_hash = str(tail["chain_hash"])
            chain_hash = _chain_hash(
                sequence, previous_hash, content_hash, status, flow_id
            )
            self._connection.execute(
                """
                INSERT INTO nav_flows(
                    sequence, flow_id, status, event_kind, economic_event_kind,
                    broker_event_identifier, effective_at, amount, attribution,
                    position_id, version, supersedes_flow_id,
                    supersedes_content_hash, correction_delta, actor, signed_at,
                    contract_hash, immutable_json, content_hash, previous_hash,
                    chain_hash, recorded_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    sequence,
                    flow_id,
                    status,
                    str(document["event_kind"]),
                    str(document["economic_event_kind"]),
                    str(document["broker_event_identifier"]),
                    str(document["effective_at"]),
                    str(document["amount"]),
                    str(document["attribution"]),
                    document["position_id"],
                    int(document["version"]),
                    document["supersedes_flow_id"],
                    document["supersedes_content_hash"],
                    (
                        None
                        if document["correction_delta"] is None
                        else str(document["correction_delta"])
                    ),
                    document["actor"],
                    document["signed_at"],
                    str(document["contract_hash"]),
                    immutable_json,
                    content_hash,
                    previous_hash,
                    chain_hash,
                    datetime_text(recorded_at),
                ),
            )
            row = self._connection.execute(
                "SELECT * FROM nav_flows WHERE sequence = ?", (sequence,)
            ).fetchone()
            if row is None:
                raise StrategyNavLedgerCorruption("inserted NAV flow is missing")
            return _receipt(row, inserted=True)

    def _ensure_anchor(self) -> None:
        assert self._authority is not None
        authority = self._authority
        document = authority.anchor_document()
        immutable_json = canonical_json(document)
        content_hash = canonical_hash(document)
        chain_hash = _chain_hash(
            0, GENESIS_HASH, content_hash, "ANCHOR", authority.contract.contract_hash
        )
        recorded_at = utc_datetime(self._clock(), field="clock result")
        with self._transaction():
            existing = self._connection.execute(
                "SELECT * FROM nav_anchor WHERE singleton = 1"
            ).fetchone()
            if existing is None:
                self._connection.execute(
                    """
                    INSERT INTO nav_anchor(
                        singleton, contract_version, contract_hash,
                        account_reference, currency, anchor_amount,
                        anchor_effective_at, immutable_json, content_hash,
                        previous_hash, chain_hash, recorded_at
                    ) VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        authority.contract.version,
                        authority.contract.contract_hash,
                        authority.account_reference,
                        authority.currency,
                        str(authority.anchor_amount),
                        datetime_text(authority.anchor_effective_at),
                        immutable_json,
                        content_hash,
                        GENESIS_HASH,
                        chain_hash,
                        datetime_text(recorded_at),
                    ),
                )
            elif (
                str(existing["immutable_json"]) != immutable_json
                or str(existing["content_hash"]) != content_hash
                or str(existing["chain_hash"]) != chain_hash
            ):
                self._anchor_reason = "STRATEGY_NAV_CONTRACT_LEDGER_MISMATCH"

    def _verify_contract_source(self) -> None:
        if self._contract_path is None or self._authority is None:
            return
        current = load_contract(
            self._contract_path,
            expected_kind=ContractKind.STRATEGY_NAV,
            expected_version=self._authority.contract.version,
            expected_hash=self._authority.contract.contract_hash,
            expected_signer=self._authority.contract.actor,
            expected_effective_at=self._authority.contract.effective_at,
        )
        _authority_from_contract(current)

    def _require_authority(
        self, *, check_integrity: bool = True
    ) -> _ContractAuthority:
        self._ensure_open()
        if self._authority is None:
            raise StrategyNavUnavailable(
                self._contract_reason or "MISSING_STRATEGY_NAV_CONTRACT"
            )
        if self._anchor_reason is not None:
            raise StrategyNavUnavailable(self._anchor_reason)
        self._verify_contract_source()
        if check_integrity:
            self.assert_integrity()
        return self._authority

    def _migrate(self) -> None:
        version = int(self._connection.execute("PRAGMA user_version").fetchone()[0])
        if version > SCHEMA_VERSION:
            raise StrategyNavLedgerError(
                f"Strategy NAV schema {version} is newer than supported"
            )
        with self._lock:
            self._connection.executescript(
                f"""
                BEGIN IMMEDIATE;
                CREATE TABLE IF NOT EXISTS nav_anchor (
                    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                    contract_version TEXT NOT NULL,
                    contract_hash TEXT NOT NULL,
                    account_reference TEXT NOT NULL,
                    currency TEXT NOT NULL,
                    anchor_amount TEXT NOT NULL,
                    anchor_effective_at TEXT NOT NULL,
                    immutable_json TEXT NOT NULL,
                    content_hash TEXT NOT NULL,
                    previous_hash TEXT NOT NULL,
                    chain_hash TEXT NOT NULL,
                    recorded_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS nav_flows (
                    sequence INTEGER PRIMARY KEY,
                    flow_id TEXT NOT NULL,
                    status TEXT NOT NULL CHECK(status IN ('APPLIED', 'CONFLICT')),
                    event_kind TEXT NOT NULL,
                    economic_event_kind TEXT NOT NULL,
                    broker_event_identifier TEXT NOT NULL,
                    effective_at TEXT NOT NULL,
                    amount TEXT NOT NULL,
                    attribution TEXT NOT NULL,
                    position_id TEXT,
                    version INTEGER NOT NULL,
                    supersedes_flow_id TEXT,
                    supersedes_content_hash TEXT,
                    correction_delta TEXT,
                    actor TEXT,
                    signed_at TEXT,
                    contract_hash TEXT NOT NULL,
                    immutable_json TEXT NOT NULL,
                    content_hash TEXT NOT NULL,
                    previous_hash TEXT NOT NULL,
                    chain_hash TEXT NOT NULL UNIQUE,
                    recorded_at TEXT NOT NULL,
                    UNIQUE(flow_id, content_hash)
                );
                CREATE INDEX IF NOT EXISTS nav_flows_identity_idx
                    ON nav_flows(flow_id, sequence);
                CREATE INDEX IF NOT EXISTS nav_flows_effective_idx
                    ON nav_flows(effective_at, sequence);
                CREATE UNIQUE INDEX IF NOT EXISTS nav_flows_one_applied_correction_idx
                    ON nav_flows(supersedes_flow_id)
                    WHERE supersedes_flow_id IS NOT NULL AND status = 'APPLIED';
                CREATE TRIGGER IF NOT EXISTS nav_anchor_no_update
                BEFORE UPDATE ON nav_anchor BEGIN
                    SELECT RAISE(ABORT, 'immutable Strategy NAV anchor: update forbidden');
                END;
                CREATE TRIGGER IF NOT EXISTS nav_anchor_no_delete
                BEFORE DELETE ON nav_anchor BEGIN
                    SELECT RAISE(ABORT, 'immutable Strategy NAV anchor: delete forbidden');
                END;
                CREATE TRIGGER IF NOT EXISTS nav_flows_no_update
                BEFORE UPDATE ON nav_flows BEGIN
                    SELECT RAISE(ABORT, 'immutable Strategy NAV ledger: update forbidden');
                END;
                CREATE TRIGGER IF NOT EXISTS nav_flows_no_delete
                BEFORE DELETE ON nav_flows BEGIN
                    SELECT RAISE(ABORT, 'immutable Strategy NAV ledger: delete forbidden');
                END;
                PRAGMA user_version={SCHEMA_VERSION};
                COMMIT;
                """
            )

    class _Transaction:
        def __init__(self, ledger: "StrategyNavLedger") -> None:
            self.ledger = ledger

        def __enter__(self) -> None:
            self.ledger._ensure_open()
            self.ledger._lock.acquire()
            try:
                self.ledger._connection.execute("BEGIN IMMEDIATE")
            except BaseException:
                self.ledger._lock.release()
                raise

        def __exit__(self, exc_type: object, *_: object) -> None:
            try:
                self.ledger._connection.execute("ROLLBACK" if exc_type else "COMMIT")
            finally:
                self.ledger._lock.release()

    def _transaction(self) -> "StrategyNavLedger._Transaction":
        return self._Transaction(self)

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("Strategy NAV ledger is closed")


def _resolve_contract(
    source: str | Path | SignedContract | Mapping[str, object] | None,
) -> tuple[_ContractAuthority | None, str | None]:
    if source is None:
        return None, "MISSING_STRATEGY_NAV_CONTRACT"
    try:
        if isinstance(source, (str, Path)):
            contract = load_contract(source, expected_kind=ContractKind.STRATEGY_NAV)
        else:
            contract = verify_contract(source, expected_kind=ContractKind.STRATEGY_NAV)
        return _authority_from_contract(contract), None
    except FileNotFoundError:
        return None, "MISSING_STRATEGY_NAV_CONTRACT"
    except (ContractValidationError, KeyError, TypeError, ValueError):
        return None, "INVALID_STRATEGY_NAV_CONTRACT"


def _authority_from_contract(contract: SignedContract) -> _ContractAuthority:
    payload = contract.payload
    account_reference = _identifier("account_reference", payload["account_reference"])
    currency = payload["currency"]
    if currency != "USD":
        raise ContractValidationError("Strategy NAV currency must be USD")
    anchor = payload["anchor"]
    if not isinstance(anchor, Mapping):
        raise ContractValidationError("Strategy NAV anchor must be an object")
    anchor_amount = Decimal(str(anchor["amount"]))
    if not anchor_amount.is_finite() or anchor_amount <= _ZERO:
        raise ContractValidationError("Strategy NAV anchor amount must be positive")
    anchor_currency = anchor["currency"]
    if anchor_currency != currency:
        raise ContractValidationError("Strategy NAV anchor currency mismatch")
    anchor_effective_at = utc_datetime(
        datetime.fromisoformat(str(anchor["effective_at"])),
        field="anchor.effective_at",
    )
    if anchor_effective_at != contract.effective_at:
        raise ContractValidationError("Strategy NAV anchor effective time mismatch")

    nlv_policy = payload["account_nlv_policy"]
    equation = payload["decimal_equation"]
    idempotency = payload["idempotency"]
    correction = payload["correction_policy"]
    conflict = payload["conflict_policy"]
    policies = (nlv_policy, equation, idempotency, correction, conflict)
    if not all(isinstance(item, Mapping) for item in policies):
        raise ContractValidationError("Strategy NAV contract policies must be objects")
    required = (
        nlv_policy.get("risk_input") is False,
        nlv_policy.get("fallback_when_contract_or_ledger_invalid") is False,
        equation.get("account_nlv_counted") is False,
        equation.get("fill_principal_counted") is False,
        equation.get("non_strategy_contribution") == "0",
        equation.get("quantum") == "0.01",
        equation.get("rounding") == "ROUND_HALF_EVEN",
        idempotency.get("same_flow_id_different_content_hash") == "CONFLICT",
        correction.get("mode") == "APPEND_ONLY",
        correction.get("overwrite_prior") is False,
        correction.get("delete_prior") is False,
        conflict.get("nlv_fallback") == "PROHIBITED",
        conflict.get("risk_authorization") == "BLOCKED",
    )
    if not all(required):
        raise ContractValidationError("Strategy NAV contract weakens a locked policy")
    return _ContractAuthority(
        contract=contract,
        account_reference=account_reference,
        currency=str(currency),
        anchor_amount=anchor_amount,
        anchor_effective_at=anchor_effective_at,
    )


def _event_document(
    *,
    authority: _ContractAuthority,
    event_kind: NavEventKind,
    broker_event_identifier: str,
    effective_at: datetime,
    amount: Decimal,
    attribution: NavAttribution,
    position_id: str | None,
    version: int,
    supersedes_flow_id: str | None,
    supersedes_content_hash: str | None,
    correction_delta: Decimal | None,
    actor: str | None,
    signed_at: datetime | None,
    economic_event_kind: NavEventKind | None = None,
) -> dict[str, object]:
    economic = economic_event_kind or event_kind
    return {
        "account_reference": authority.account_reference,
        "broker_event_kind": event_kind.value,
        "broker_event_identifier": broker_event_identifier,
        "effective_at": datetime_text(effective_at),
        "event_kind": event_kind.value,
        "economic_event_kind": economic.value,
        "amount": _decimal_text(amount),
        "attribution": attribution.value,
        "position_id": position_id,
        "version": version,
        "supersedes_flow_id": supersedes_flow_id,
        "supersedes_content_hash": supersedes_content_hash,
        "correction_delta": (
            None if correction_delta is None else _decimal_text(correction_delta)
        ),
        "actor": actor,
        "signed_at": None if signed_at is None else datetime_text(signed_at),
        "contract_hash": authority.contract.contract_hash,
    }


def _event_document_from_row(row: sqlite3.Row) -> dict[str, object]:
    immutable = json.loads(str(row["immutable_json"]))
    if not isinstance(immutable, dict):
        raise StrategyNavLedgerCorruption("NAV immutable event is not an object")
    # Rebuild from physical columns so a mutation cannot hide behind immutable_json.
    return {
        "account_reference": immutable.get("account_reference"),
        "broker_event_kind": str(row["event_kind"]),
        "broker_event_identifier": str(row["broker_event_identifier"]),
        "effective_at": str(row["effective_at"]),
        "event_kind": str(row["event_kind"]),
        "economic_event_kind": str(row["economic_event_kind"]),
        "amount": str(row["amount"]),
        "attribution": str(row["attribution"]),
        "position_id": None if row["position_id"] is None else str(row["position_id"]),
        "version": int(row["version"]),
        "supersedes_flow_id": (
            None if row["supersedes_flow_id"] is None else str(row["supersedes_flow_id"])
        ),
        "supersedes_content_hash": (
            None
            if row["supersedes_content_hash"] is None
            else str(row["supersedes_content_hash"])
        ),
        "correction_delta": (
            None if row["correction_delta"] is None else str(row["correction_delta"])
        ),
        "actor": None if row["actor"] is None else str(row["actor"]),
        "signed_at": None if row["signed_at"] is None else str(row["signed_at"]),
        "contract_hash": str(row["contract_hash"]),
    }


def _anchor_document_from_row(row: sqlite3.Row) -> dict[str, object]:
    return {
        "contract_version": str(row["contract_version"]),
        "contract_hash": str(row["contract_hash"]),
        "account_reference": str(row["account_reference"]),
        "currency": str(row["currency"]),
        "anchor_amount": Decimal(str(row["anchor_amount"])),
        "anchor_effective_at": datetime.fromisoformat(str(row["anchor_effective_at"])),
    }


def _flow_id(document: Mapping[str, object]) -> str:
    return canonical_hash(
        {
            "account_reference": document["account_reference"],
            "broker_event_kind": document["broker_event_kind"],
            "broker_event_identifier": document["broker_event_identifier"],
            "effective_at": document["effective_at"],
        }
    )


def _chain_hash(
    sequence: int,
    previous_hash: str,
    content_hash: str,
    status: str,
    identity: str,
) -> str:
    return canonical_hash(
        {
            "sequence": sequence,
            "previous_hash": previous_hash,
            "content_hash": content_hash,
            "status": status,
            "identity": identity,
        }
    )


def _receipt(row: sqlite3.Row, *, inserted: bool) -> NavFlowReceipt:
    return NavFlowReceipt(
        sequence=int(row["sequence"]),
        flow_id=str(row["flow_id"]),
        content_hash=str(row["content_hash"]),
        chain_hash=str(row["chain_hash"]),
        inserted=inserted,
        conflict=str(row["status"]) == "CONFLICT",
        event_kind=NavEventKind(str(row["event_kind"])),
        version=int(row["version"]),
        supersedes_flow_id=(
            None if row["supersedes_flow_id"] is None else str(row["supersedes_flow_id"])
        ),
        supersedes_content_hash=(
            None
            if row["supersedes_content_hash"] is None
            else str(row["supersedes_content_hash"])
        ),
    )


def _snapshot(
    *,
    asof: datetime,
    strategy_nav: Decimal | None = None,
    strategy_deposits: Decimal = _ZERO,
    strategy_withdrawals: Decimal = _ZERO,
    realized_pnl: Decimal = _ZERO,
    open_position_unrealized_pnl: Decimal = _ZERO,
    fees: Decimal = _ZERO,
    signed_corrections: Decimal = _ZERO,
    observed_account_nlv: Decimal | None = None,
    reconciliation_difference: Decimal | None = None,
    contract_version: str | None = None,
    contract_hash: str | None = None,
    ledger_head_hash: str | None = None,
    reasons: object = (),
) -> StrategyNavSnapshot:
    reason_tuple = tuple(sorted(str(item) for item in reasons))
    fields = dict(
        asof=asof,
        strategy_nav=strategy_nav,
        strategy_deposits=strategy_deposits,
        strategy_withdrawals=strategy_withdrawals,
        realized_pnl=realized_pnl,
        open_position_unrealized_pnl=open_position_unrealized_pnl,
        fees=fees,
        signed_corrections=signed_corrections,
        non_strategy_contribution=_ZERO,
        fill_principal_contribution=_ZERO,
        observed_account_nlv=observed_account_nlv,
        reconciliation_difference=reconciliation_difference,
        contract_version=contract_version,
        contract_hash=contract_hash,
        ledger_head_hash=ledger_head_hash,
        valid=not reason_tuple,
        no_trade_reasons=reason_tuple,
    )
    return StrategyNavSnapshot(**fields, content_hash=canonical_hash(fields))


def strategy_nav_authority_hash(
    *,
    strategy_nav_usd: Decimal | None,
    contract_hash: str | None,
    ledger_head_hash: str | None,
) -> str:
    """Hash the stable signed NAV authority independently of one observation."""

    return canonical_hash(
        _strategy_nav_authority_payload(
            strategy_nav_usd=strategy_nav_usd,
            contract_hash=contract_hash,
            ledger_head_hash=ledger_head_hash,
        )
    )


def _strategy_nav_authority_payload(
    *,
    strategy_nav_usd: Decimal | None,
    contract_hash: str | None,
    ledger_head_hash: str | None,
) -> dict[str, object]:
    return {
        "schema": STRATEGY_NAV_AUTHORITY_SCHEMA,
        "strategy_nav_usd": strategy_nav_usd,
        "contract_hash": contract_hash,
        "ledger_head_hash": ledger_head_hash,
    }


def _event_kind(value: NavEventKind | str, *, allow_correction: bool) -> NavEventKind:
    if isinstance(value, NavEventKind):
        kind = value
    elif isinstance(value, str):
        try:
            kind = NavEventKind(value.strip().upper())
        except ValueError as exc:
            raise ValueError(f"unsupported NAV event kind: {value}") from exc
    else:
        raise TypeError("event_kind must be NavEventKind or string")
    if kind is NavEventKind.CORRECTION and not allow_correction:
        raise ValueError("corrections must use correct_flow")
    return kind


def _attribution(value: NavAttribution | str) -> NavAttribution:
    if isinstance(value, NavAttribution):
        return value
    if not isinstance(value, str):
        raise TypeError("attribution must be NavAttribution or string")
    try:
        return NavAttribution(value.strip().upper())
    except ValueError as exc:
        raise ValueError(f"unsupported NAV attribution: {value}") from exc


def _amount(kind: NavEventKind, value: Decimal) -> Decimal:
    if not isinstance(value, Decimal):
        raise TypeError("amount must be a Decimal")
    if not value.is_finite():
        raise ValueError("amount must be finite")
    if kind in (NavEventKind.DEPOSIT, NavEventKind.WITHDRAWAL, NavEventKind.FEE) and value < _ZERO:
        raise ValueError(f"{kind.value.lower()} amount must be nonnegative")
    return value


def _optional_nonnegative_decimal(value: Decimal | None, field: str) -> Decimal | None:
    if value is None:
        return None
    if not isinstance(value, Decimal):
        raise TypeError(f"{field} must be a Decimal")
    if not value.is_finite() or value < _ZERO:
        raise ValueError(f"{field} must be finite and nonnegative")
    return value


def _decimal_text(value: Decimal) -> str:
    normalized = value.normalize()
    return "0" if not normalized else format(normalized, "f")


def _position(kind: NavEventKind, value: str | None) -> str | None:
    if kind is NavEventKind.UNREALIZED_PNL:
        if value is None:
            raise ValueError("UNREALIZED_PNL requires position_id")
        return _identifier("position_id", value)
    if value is not None:
        return _identifier("position_id", value)
    return None


def _identifier(field: str, value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a nonblank string")
    normalized = value.strip()
    if len(normalized) > 240:
        raise ValueError(f"{field} is too long")
    return normalized


def _hash_text(field: str, value: object) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError(f"{field} must be a lowercase SHA-256 hash")
    return value


__all__ = [
    "NavAttribution",
    "NavEventKind",
    "NavFlowReceipt",
    "STRATEGY_NAV_AUTHORITY_SCHEMA",
    "StrategyNavLedger",
    "StrategyNavLedgerCorruption",
    "StrategyNavLedgerError",
    "StrategyNavSnapshot",
    "StrategyNavUnavailable",
    "strategy_nav_authority_hash",
]
