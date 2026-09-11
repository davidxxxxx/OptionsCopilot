"""Versioned, hash-bound external IBKR read-only batch exchange.

The external connector publishes one complete account/order/secdef/quote batch
to a same-directory temporary file.  :func:`os.replace` is the only operation
that makes a new batch visible.  Readers accept either the old complete file or
the new complete file and fail closed on stale, partial, mixed, or tampered
content.

This module intentionally has no instruction-creation or order-submission API.
"""
from __future__ import annotations

import json
import os
import re
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from types import MappingProxyType
from typing import Callable

from options_copilot.storage.canonical import canonical_hash, canonical_json

from .ibkr_readonly import (
    AccountSnapshot,
    BatchedOptionQuote,
    OptionContractRef,
    OptionQuoteBatch,
    OptionSecDefSnapshot,
    PositionSnapshot,
    QuoteBatchStatus,
)


EXTERNAL_READONLY_FEED_SCHEMA = "options_copilot.external_ibkr_readonly_batch"
EXTERNAL_READONLY_FEED_VERSION = 1
PREMARKET_ACCOUNT_PURPOSE = "PREMARKET_ACCOUNT"
OPEN_REPRICE_PURPOSE = "OPEN_REPRICE"
_PURPOSES = frozenset({PREMARKET_ACCOUNT_PURPOSE, OPEN_REPRICE_PURPOSE})
MAXIMUM_BATCH_AGE_SECONDS = Decimal("5")
MAXIMUM_DOCUMENT_BYTES = 16 * 1024 * 1024

_BATCH_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_IDENTITY_FIELDS = {
    "conId",
    "localSymbol",
    "tradingClass",
    "multiplier",
    "exchange",
    "expiry",
    "strike",
    "right",
}
_PAYLOAD_FIELDS = {
    "purpose",
    "batch_id",
    "requested_at",
    "completed_at",
    "account",
    "nav",
    "positions",
    "working_orders",
    "unsubmitted_instructions",
    "secdefs",
    "quotes",
}
_DOCUMENT_FIELDS = {
    "schema",
    "version",
    "written_at",
    "content_hash",
    *_PAYLOAD_FIELDS,
}
_ACCOUNT_FIELDS = {
    "asof",
    "currency",
    "net_liquidation",
    "equity_with_loan_value",
    "available_funds",
    "buying_power",
    "initial_margin",
    "maintenance_margin",
    "excess_liquidity",
    "day_trades_remaining",
    "connected",
}
_NAV_FIELDS = {"asof", "currency", "strategy_nav", "source"}
_POSITION_FIELDS = {
    "asof",
    "contract_id",
    "symbol",
    "local_symbol",
    "security_type",
    "currency",
    "exchange",
    "quantity",
    "average_cost",
    "market_price",
    "market_value",
    "unrealized_pnl",
    "realized_pnl",
    "identity",
}
_SECDEF_FIELDS = {
    "batch_id",
    "requested_at",
    "observed_at",
    "completed_at",
    "identity",
    "security_type",
    "currency",
    "standard_contract",
    "adjusted",
    "source",
}
_QUOTE_FIELDS = {
    "batch_id",
    "request_id",
    "requested_at",
    "observed_at",
    "completed_at",
    "identity",
    "source",
    "bid",
    "ask",
    "last",
    "close",
    "volume",
    "open_interest",
    "implied_volatility",
    "delta",
    "gamma",
    "theta",
    "vega",
    "market_data_type",
}


class ExternalFeedValidationError(ValueError):
    """The external read-only feed cannot be trusted as one current batch."""


@dataclass(frozen=True, slots=True)
class ExternalReadonlyBatch:
    """Immutable, hash-verified projection of one external connector batch."""

    purpose: str
    batch_id: str
    requested_at: datetime
    completed_at: datetime
    written_at: datetime
    account_data: Mapping[str, object]
    nav_data: Mapping[str, object]
    position_rows: tuple[Mapping[str, object], ...]
    working_order_rows: tuple[Mapping[str, object], ...]
    instruction_rows: tuple[Mapping[str, object], ...]
    secdef_rows: tuple[Mapping[str, object], ...]
    quote_rows: tuple[Mapping[str, object], ...]
    content_hash: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "account_data", _freeze_mapping(self.account_data))
        object.__setattr__(self, "nav_data", _freeze_mapping(self.nav_data))
        for name in (
            "position_rows",
            "working_order_rows",
            "instruction_rows",
            "secdef_rows",
            "quote_rows",
        ):
            rows = tuple(_freeze_mapping(row) for row in getattr(self, name))
            object.__setattr__(self, name, rows)

    def hash_payload(self) -> dict[str, object]:
        return {
            "schema": EXTERNAL_READONLY_FEED_SCHEMA,
            "version": EXTERNAL_READONLY_FEED_VERSION,
            "purpose": self.purpose,
            "batch_id": self.batch_id,
            "requested_at": self.requested_at,
            "completed_at": self.completed_at,
            "written_at": self.written_at,
            "account": self.account_data,
            "nav": self.nav_data,
            "positions": self.position_rows,
            "working_orders": self.working_order_rows,
            "unsubmitted_instructions": self.instruction_rows,
            "secdefs": self.secdef_rows,
            "quotes": self.quote_rows,
        }

    def verify_hash(self) -> bool:
        return canonical_hash(self.hash_payload()) == self.content_hash

    def account_snapshot(self) -> AccountSnapshot:
        row = self.account_data
        return AccountSnapshot(
            asof=_timestamp(row["asof"], "account.asof"),
            currency=str(row["currency"]),
            net_liquidation=_decimal(row["net_liquidation"], "net_liquidation"),
            equity_with_loan_value=_decimal(
                row["equity_with_loan_value"], "equity_with_loan_value"
            ),
            available_funds=_decimal(row["available_funds"], "available_funds"),
            buying_power=_decimal(row["buying_power"], "buying_power"),
            initial_margin=_decimal(row["initial_margin"], "initial_margin"),
            maintenance_margin=_decimal(
                row["maintenance_margin"], "maintenance_margin"
            ),
            excess_liquidity=_decimal(
                row["excess_liquidity"], "excess_liquidity"
            ),
            day_trades_remaining=_integer_or_none(
                row["day_trades_remaining"], "day_trades_remaining"
            ),
            connected=True,
        )

    def strategy_nav(self) -> dict[str, object]:
        return _detached_mapping(self.nav_data)

    def positions(self) -> tuple[PositionSnapshot, ...]:
        values: list[PositionSnapshot] = []
        for row in self.position_rows:
            identity = row["identity"]
            option_identity = identity if isinstance(identity, Mapping) else None
            values.append(
                PositionSnapshot(
                    asof=_timestamp(row["asof"], "position.asof"),
                    contract_id=_positive_integer(
                        row["contract_id"], "position.contract_id"
                    ),
                    symbol=str(row["symbol"]),
                    local_symbol=str(row["local_symbol"]),
                    security_type=str(row["security_type"]),
                    currency=str(row["currency"]),
                    exchange=str(row["exchange"]),
                    quantity=_decimal(row["quantity"], "position.quantity"),
                    average_cost=_decimal(
                        row["average_cost"], "position.average_cost"
                    ),
                    market_price=_decimal(
                        row["market_price"], "position.market_price"
                    ),
                    market_value=_decimal(
                        row["market_value"], "position.market_value"
                    ),
                    unrealized_pnl=_decimal(
                        row["unrealized_pnl"], "position.unrealized_pnl"
                    ),
                    realized_pnl=_decimal(
                        row["realized_pnl"], "position.realized_pnl"
                    ),
                    expiration=(
                        _date(option_identity["expiry"], "position.identity.expiry")
                        if option_identity is not None
                        else None
                    ),
                    strike=(
                        _decimal(option_identity["strike"], "position.identity.strike")
                        if option_identity is not None
                        else None
                    ),
                    right=(
                        str(option_identity["right"])
                        if option_identity is not None
                        else None
                    ),
                    trading_class=(
                        str(option_identity["tradingClass"])
                        if option_identity is not None
                        else None
                    ),
                    multiplier=(
                        int(option_identity["multiplier"])
                        if option_identity is not None
                        else None
                    ),
                )
            )
        return tuple(values)

    def working_orders(self) -> tuple[dict[str, object], ...]:
        return tuple(_detached_mapping(row) for row in self.working_order_rows)

    def unsubmitted_instructions(self) -> tuple[dict[str, object], ...]:
        return tuple(_detached_mapping(row) for row in self.instruction_rows)

    def option_contract_definitions(
        self, contracts: Sequence[OptionContractRef]
    ) -> tuple[OptionSecDefSnapshot, ...]:
        requested = _requested_contracts(contracts)
        by_id = {
            _positive_integer(row["identity"]["conId"], "secdef identity conId"): row
            for row in self.secdef_rows
        }
        _require_requested_identity(requested, by_id)
        values: list[OptionSecDefSnapshot] = []
        for contract in requested:
            row = by_id[contract.contract_id]
            identity = row["identity"]
            assert isinstance(identity, Mapping)
            values.append(
                OptionSecDefSnapshot(
                    contract_id=contract.contract_id,
                    local_symbol=str(identity["localSymbol"]),
                    trading_class=str(identity["tradingClass"]),
                    multiplier=int(identity["multiplier"]),
                    exchange=str(identity["exchange"]),
                    expiration=_date(identity["expiry"], "secdef.identity.expiry"),
                    strike=_decimal(identity["strike"], "secdef.identity.strike"),
                    right=str(identity["right"]),  # type: ignore[arg-type]
                    security_type="OPT",
                    currency="USD",
                    standard_contract=True,
                    adjusted=False,
                    source=str(row["source"]),
                )
            )
        return tuple(values)

    def option_quote_batch(
        self, contracts: Sequence[OptionContractRef]
    ) -> OptionQuoteBatch:
        requested = _requested_contracts(contracts)
        by_id = {
            _positive_integer(row["identity"]["conId"], "quote identity conId"): row
            for row in self.quote_rows
        }
        _require_requested_identity(requested, by_id)
        values: list[BatchedOptionQuote] = []
        for contract in requested:
            row = by_id[contract.contract_id]
            values.append(
                BatchedOptionQuote(
                    contract_id=contract.contract_id,
                    batch_id=self.batch_id,
                    request_id=str(row["request_id"]),
                    requested_at=self.requested_at,
                    observed_at=_timestamp(row["observed_at"], "quote.observed_at"),
                    completed_at=self.completed_at,
                    source=str(row["source"]),
                    bid=_decimal(row["bid"], "quote.bid"),
                    ask=_decimal(row["ask"], "quote.ask"),
                    last=_decimal(row["last"], "quote.last"),
                    close=_decimal(row["close"], "quote.close"),
                    exchange_time=_timestamp(
                        row["observed_at"], "quote.observed_at"
                    ),
                    volume=_nonnegative_integer(row["volume"], "quote.volume"),
                    open_interest=_nonnegative_integer(
                        row["open_interest"], "quote.open_interest"
                    ),
                    implied_volatility=_decimal(
                        row["implied_volatility"], "quote.implied_volatility"
                    ),
                    delta=_decimal(row["delta"], "quote.delta"),
                    gamma=_decimal(row["gamma"], "quote.gamma"),
                    theta=_decimal(row["theta"], "quote.theta"),
                    vega=_decimal(row["vega"], "quote.vega"),
                    market_data_type=1,
                )
            )
        source = str(self.quote_rows[0]["source"])
        observed_at = _timestamp(self.quote_rows[0]["observed_at"], "quote.observed_at")
        return OptionQuoteBatch(
            batch_id=self.batch_id,
            status=QuoteBatchStatus.COMPLETE,
            requested_at=self.requested_at,
            completed_at=self.completed_at,
            source=source,
            quotes=tuple(values),
            observed_at=observed_at,
        )


class ExternalReadonlyFeedPublisher:
    """Validate and atomically publish one connector-owned read-only batch."""

    def __init__(
        self,
        path: Path | str,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.path = Path(path)
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    def publish(self, payload: Mapping[str, object]) -> ExternalReadonlyBatch:
        if not isinstance(payload, Mapping) or set(payload) != _PAYLOAD_FIELDS:
            raise ExternalFeedValidationError("external feed payload fields are incomplete")
        written_at = _aware_utc(self._clock(), "publisher clock")
        try:
            normalized = json.loads(canonical_json(payload), object_pairs_hook=_unique_object)
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ExternalFeedValidationError("external feed payload is not canonical") from exc
        document: dict[str, object] = {
            "schema": EXTERNAL_READONLY_FEED_SCHEMA,
            "version": EXTERNAL_READONLY_FEED_VERSION,
            **normalized,
            "written_at": written_at.isoformat(timespec="microseconds"),
        }
        document["content_hash"] = canonical_hash(document)
        batch = _parse_document(document, now=written_at)
        rendered = (canonical_json(document) + "\n").encode("utf-8")
        if len(rendered) > MAXIMUM_DOCUMENT_BYTES:
            raise ExternalFeedValidationError("external feed document is too large")
        _atomic_replace(self.path, rendered)
        return batch


class ExternalReadonlyFeedReader:
    """Read and verify one current external batch without mutating its file."""

    def __init__(
        self,
        path: Path | str,
        *,
        clock: Callable[[], datetime] | None = None,
        maximum_age_seconds: Decimal = MAXIMUM_BATCH_AGE_SECONDS,
    ) -> None:
        self.path = Path(path)
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self.maximum_age_seconds = _decimal(
            maximum_age_seconds, "maximum_age_seconds"
        )
        if self.maximum_age_seconds != MAXIMUM_BATCH_AGE_SECONDS:
            raise ValueError("external feed freshness is locked to five seconds")

    def read(self) -> ExternalReadonlyBatch:
        try:
            size = self.path.stat().st_size
            if size <= 0 or size > MAXIMUM_DOCUMENT_BYTES:
                raise ExternalFeedValidationError(
                    "external feed document size is invalid"
                )
            payload = self.path.read_bytes()
        except ExternalFeedValidationError:
            raise
        except OSError as exc:
            raise ExternalFeedValidationError("external feed is unavailable") from exc
        try:
            document = json.loads(payload.decode("utf-8"), object_pairs_hook=_unique_object)
        except (UnicodeDecodeError, json.JSONDecodeError, ExternalFeedValidationError) as exc:
            raise ExternalFeedValidationError("external feed JSON is invalid") from exc
        return _parse_document(
            document,
            now=_aware_utc(self._clock(), "reader clock"),
        )


def _parse_document(document: object, *, now: datetime) -> ExternalReadonlyBatch:
    if not isinstance(document, Mapping) or set(document) != _DOCUMENT_FIELDS:
        raise ExternalFeedValidationError("external feed top-level fields are incomplete")
    if document["schema"] != EXTERNAL_READONLY_FEED_SCHEMA:
        raise ExternalFeedValidationError("external feed schema is unsupported")
    version = document["version"]
    if isinstance(version, bool) or version != EXTERNAL_READONLY_FEED_VERSION:
        raise ExternalFeedValidationError("external feed version is unsupported")
    content_hash = document["content_hash"]
    if not isinstance(content_hash, str) or _SHA256.fullmatch(content_hash) is None:
        raise ExternalFeedValidationError("external feed content hash is invalid")
    unsigned = dict(document)
    unsigned.pop("content_hash")
    if canonical_hash(unsigned) != content_hash:
        raise ExternalFeedValidationError("external feed content hash mismatch")

    batch_id = document["batch_id"]
    if not isinstance(batch_id, str) or _BATCH_ID.fullmatch(batch_id) is None:
        raise ExternalFeedValidationError("external feed batch_id is invalid")
    requested_at = _timestamp(document["requested_at"], "requested_at")
    completed_at = _timestamp(document["completed_at"], "completed_at")
    written_at = _timestamp(document["written_at"], "written_at")
    now = _aware_utc(now, "reader clock")
    if not requested_at <= completed_at <= written_at <= now:
        raise ExternalFeedValidationError("external feed timestamps are invalid")
    age = Decimal(str((now - completed_at).total_seconds()))
    if age > MAXIMUM_BATCH_AGE_SECONDS:
        raise ExternalFeedValidationError("external feed is older than five seconds")

    account = _mapping(document["account"], "account")
    nav = _mapping(document["nav"], "nav")
    _validate_account(account, completed_at)
    _validate_nav(nav, completed_at)
    positions = _known_array(document["positions"], "positions")
    working_orders = _known_array(document["working_orders"], "working_orders")
    instructions = _known_array(
        document["unsubmitted_instructions"], "unsubmitted_instructions"
    )
    secdefs = _known_array(document["secdefs"], "secdefs")
    quotes = _known_array(document["quotes"], "quotes")
    purpose = document["purpose"]
    if purpose not in _PURPOSES:
        raise ExternalFeedValidationError("external feed purpose is unsupported")
    if purpose == OPEN_REPRICE_PURPOSE and (not secdefs or not quotes):
        raise ExternalFeedValidationError("secdefs and quotes must be complete arrays")
    if purpose == PREMARKET_ACCOUNT_PURPOSE and (secdefs or quotes):
        raise ExternalFeedValidationError(
            "premarket account feed cannot contain option quote evidence"
        )

    identities: dict[int, str] = {}
    _validate_positions(positions, completed_at, identities)
    _validate_optional_state_identities(working_orders, identities, "working_orders")
    _validate_optional_state_identities(instructions, identities, "unsubmitted_instructions")
    if purpose == OPEN_REPRICE_PURPOSE:
        secdef_ids = _validate_secdefs(
            secdefs,
            batch_id=batch_id,
            requested_at=requested_at,
            completed_at=completed_at,
            identities=identities,
        )
        quote_ids = _validate_quotes(
            quotes,
            batch_id=batch_id,
            requested_at=requested_at,
            completed_at=completed_at,
            identities=identities,
        )
        if secdef_ids != quote_ids:
            raise ExternalFeedValidationError(
                "secdef and quote contract sets are incomplete or mixed"
            )

    return ExternalReadonlyBatch(
        purpose=purpose,
        batch_id=batch_id,
        requested_at=requested_at,
        completed_at=completed_at,
        written_at=written_at,
        account_data=account,
        nav_data=nav,
        position_rows=positions,
        working_order_rows=working_orders,
        instruction_rows=instructions,
        secdef_rows=secdefs,
        quote_rows=quotes,
        content_hash=content_hash,
    )


def _validate_account(row: Mapping[str, object], completed_at: datetime) -> None:
    if set(row) != _ACCOUNT_FIELDS:
        raise ExternalFeedValidationError("account fields are incomplete")
    if _timestamp(row["asof"], "account.asof") != completed_at:
        raise ExternalFeedValidationError("account contains a mixed timestamp")
    if row["currency"] != "USD" or row["connected"] is not True:
        raise ExternalFeedValidationError("account is unknown or disconnected")
    for field in _ACCOUNT_FIELDS - {
        "asof",
        "currency",
        "connected",
        "day_trades_remaining",
    }:
        _decimal(row[field], f"account.{field}")
    value = row["day_trades_remaining"]
    if value is not None:
        _nonnegative_integer(value, "account.day_trades_remaining")


def _validate_nav(row: Mapping[str, object], completed_at: datetime) -> None:
    if set(row) != _NAV_FIELDS:
        raise ExternalFeedValidationError("nav fields are incomplete")
    if _timestamp(row["asof"], "nav.asof") != completed_at:
        raise ExternalFeedValidationError("nav contains a mixed timestamp")
    if row["currency"] != "USD" or not _nonblank(row["source"]):
        raise ExternalFeedValidationError("nav is unknown")
    if _decimal(row["strategy_nav"], "nav.strategy_nav") <= 0:
        raise ExternalFeedValidationError("nav.strategy_nav must be positive")


def _validate_positions(
    rows: tuple[Mapping[str, object], ...],
    completed_at: datetime,
    identities: dict[int, str],
) -> None:
    seen: set[int] = set()
    for index, row in enumerate(rows):
        if set(row) != _POSITION_FIELDS:
            raise ExternalFeedValidationError("position fields are incomplete")
        if _timestamp(row["asof"], f"positions[{index}].asof") != completed_at:
            raise ExternalFeedValidationError("positions contain a mixed timestamp")
        contract_id = _positive_integer(
            row["contract_id"], f"positions[{index}].contract_id"
        )
        if contract_id in seen:
            raise ExternalFeedValidationError("duplicate contract in positions")
        seen.add(contract_id)
        for field in ("symbol", "local_symbol", "security_type", "currency", "exchange"):
            if not _nonblank(row[field]):
                raise ExternalFeedValidationError(f"positions[{index}].{field} is unknown")
        for field in (
            "quantity",
            "average_cost",
            "market_price",
            "market_value",
            "unrealized_pnl",
            "realized_pnl",
        ):
            _decimal(row[field], f"positions[{index}].{field}")
        if row["security_type"] == "OPT":
            identity = _identity(row["identity"], f"positions[{index}].identity")
            if identity["conId"] != contract_id:
                raise ExternalFeedValidationError("position option identity mismatch")
            if identity["localSymbol"] != row["local_symbol"]:
                raise ExternalFeedValidationError("position option identity mismatch")
            _register_identity(identity, identities)
        elif row["identity"] is not None:
            raise ExternalFeedValidationError("non-option position identity must be null")


def _validate_optional_state_identities(
    rows: tuple[Mapping[str, object], ...],
    identities: dict[int, str],
    name: str,
) -> None:
    for index, row in enumerate(rows):
        if not row or any(value is None for value in row.values()):
            raise ExternalFeedValidationError(f"{name}[{index}] is partial or unknown")
        if str(row.get("security_type", "")).upper() == "OPT":
            identity = _identity(row.get("identity"), f"{name}[{index}].identity")
            _register_identity(identity, identities)


def _validate_secdefs(
    rows: tuple[Mapping[str, object], ...],
    *,
    batch_id: str,
    requested_at: datetime,
    completed_at: datetime,
    identities: dict[int, str],
) -> set[int]:
    seen: set[int] = set()
    for index, row in enumerate(rows):
        if set(row) != _SECDEF_FIELDS:
            raise ExternalFeedValidationError("secdef fields are incomplete")
        _same_batch_times(row, batch_id, requested_at, completed_at, f"secdefs[{index}]")
        identity = _identity(row["identity"], f"secdefs[{index}].identity")
        contract_id = int(identity["conId"])
        if contract_id in seen:
            raise ExternalFeedValidationError("duplicate contract in secdefs")
        seen.add(contract_id)
        _register_identity(identity, identities)
        if (
            row["security_type"] != "OPT"
            or row["currency"] != "USD"
            or row["standard_contract"] is not True
            or row["adjusted"] is not False
            or not _nonblank(row["source"])
        ):
            raise ExternalFeedValidationError("secdef is nonstandard or unknown")
    return seen


def _validate_quotes(
    rows: tuple[Mapping[str, object], ...],
    *,
    batch_id: str,
    requested_at: datetime,
    completed_at: datetime,
    identities: dict[int, str],
) -> set[int]:
    seen: set[int] = set()
    request_ids: set[str] = set()
    for index, row in enumerate(rows):
        if set(row) != _QUOTE_FIELDS:
            raise ExternalFeedValidationError("quote fields are incomplete")
        _same_batch_times(row, batch_id, requested_at, completed_at, f"quotes[{index}]")
        identity = _identity(row["identity"], f"quotes[{index}].identity")
        contract_id = int(identity["conId"])
        if contract_id in seen:
            raise ExternalFeedValidationError("duplicate contract in quotes")
        seen.add(contract_id)
        _register_identity(identity, identities)
        request_id = row["request_id"]
        if not _nonblank(request_id) or str(request_id) in request_ids:
            raise ExternalFeedValidationError("quote request_id is invalid or duplicate")
        request_ids.add(str(request_id))
        if not _nonblank(row["source"]):
            raise ExternalFeedValidationError("quote source is unknown")
        bid = _nonnegative_decimal(row["bid"], f"quotes[{index}].bid")
        ask = _nonnegative_decimal(row["ask"], f"quotes[{index}].ask")
        if ask < bid:
            raise ExternalFeedValidationError("quote executable market is crossed")
        _nonnegative_decimal(row["last"], f"quotes[{index}].last")
        _nonnegative_decimal(row["close"], f"quotes[{index}].close")
        _nonnegative_integer(row["volume"], f"quotes[{index}].volume")
        _nonnegative_integer(
            row["open_interest"], f"quotes[{index}].open_interest"
        )
        _nonnegative_decimal(
            row["implied_volatility"], f"quotes[{index}].implied_volatility"
        )
        delta = _decimal(row["delta"], f"quotes[{index}].delta")
        if delta < -1 or delta > 1:
            raise ExternalFeedValidationError("quote delta must be between -1 and 1")
        _nonnegative_decimal(row["gamma"], f"quotes[{index}].gamma")
        _decimal(row["theta"], f"quotes[{index}].theta")
        _nonnegative_decimal(row["vega"], f"quotes[{index}].vega")
        market_data_type = row["market_data_type"]
        if isinstance(market_data_type, bool) or market_data_type != 1:
            raise ExternalFeedValidationError("quote market_data_type must be integer 1")
    return seen


def _same_batch_times(
    row: Mapping[str, object],
    batch_id: str,
    requested_at: datetime,
    completed_at: datetime,
    name: str,
) -> None:
    if row["batch_id"] != batch_id:
        raise ExternalFeedValidationError(f"{name} contains a mixed batch")
    if (
        _timestamp(row["requested_at"], f"{name}.requested_at") != requested_at
        or _timestamp(row["completed_at"], f"{name}.completed_at") != completed_at
        or _timestamp(row["observed_at"], f"{name}.observed_at") != completed_at
    ):
        raise ExternalFeedValidationError(f"{name} contains a mixed timestamp")


def _identity(value: object, field: str) -> dict[str, object]:
    row = _mapping(value, field)
    if set(row) != _IDENTITY_FIELDS:
        raise ExternalFeedValidationError(f"{field} identity fields are incomplete")
    contract_id = _positive_integer(row["conId"], f"{field}.conId")
    local_symbol = row["localSymbol"]
    trading_class = row["tradingClass"]
    exchange = row["exchange"]
    if not all(_nonblank(item) for item in (local_symbol, trading_class, exchange)):
        raise ExternalFeedValidationError(f"{field} contains an unknown identity")
    multiplier = row["multiplier"]
    if isinstance(multiplier, bool) or multiplier != 100:
        raise ExternalFeedValidationError(f"{field}.multiplier must be integer 100")
    expiration = _date(row["expiry"], f"{field}.expiry")
    strike = _decimal(row["strike"], f"{field}.strike")
    right = row["right"]
    if strike <= 0 or right not in {"C", "P"}:
        raise ExternalFeedValidationError(f"{field} contains an invalid option identity")
    return {
        "conId": contract_id,
        "localSymbol": str(local_symbol),
        "tradingClass": str(trading_class),
        "multiplier": 100,
        "exchange": str(exchange),
        "expiry": expiration.isoformat(),
        "strike": strike,
        "right": str(right),
    }


def _register_identity(identity: Mapping[str, object], values: dict[int, str]) -> None:
    contract_id = int(identity["conId"])
    identity_hash = canonical_hash(identity)
    previous = values.get(contract_id)
    if previous is not None and previous != identity_hash:
        raise ExternalFeedValidationError("contract identity mismatch across batch")
    values[contract_id] = identity_hash


def _requested_contracts(
    contracts: Sequence[OptionContractRef],
) -> tuple[OptionContractRef, ...]:
    if isinstance(contracts, (str, bytes, bytearray, memoryview)):
        raise ExternalFeedValidationError("requested contracts are invalid")
    try:
        requested = tuple(contracts)
    except TypeError as exc:
        raise ExternalFeedValidationError("requested contracts are invalid") from exc
    if not requested or not all(isinstance(item, OptionContractRef) for item in requested):
        raise ExternalFeedValidationError("requested contracts are invalid")
    ids = [item.contract_id for item in requested]
    if len(ids) != len(set(ids)):
        raise ExternalFeedValidationError("requested contracts contain duplicates")
    return requested


def _require_requested_identity(
    requested: tuple[OptionContractRef, ...],
    rows: Mapping[int, Mapping[str, object]],
) -> None:
    for contract in requested:
        row = rows.get(contract.contract_id)
        if row is None:
            raise ExternalFeedValidationError("requested contract is missing from batch")
        identity = row["identity"]
        assert isinstance(identity, Mapping)
        if (
            identity["localSymbol"] != contract.local_symbol
            or _date(identity["expiry"], "identity.expiry") != contract.expiration
            or _decimal(identity["strike"], "identity.strike") != contract.strike
            or identity["right"] != contract.right
            or identity["tradingClass"] != contract.trading_class
            or identity["multiplier"] != contract.multiplier
            or identity["exchange"] != contract.exchange
        ):
            raise ExternalFeedValidationError("requested contract identity mismatch")


def _known_array(value: object, name: str) -> tuple[Mapping[str, object], ...]:
    if isinstance(value, (str, bytes, bytearray, memoryview)) or not isinstance(
        value, Sequence
    ):
        raise ExternalFeedValidationError(f"{name} must be a known array")
    rows: list[Mapping[str, object]] = []
    for index, item in enumerate(value):
        rows.append(_mapping(item, f"{name}[{index}]"))
    return tuple(rows)


def _mapping(value: object, name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise ExternalFeedValidationError(f"{name} must be a known object")
    if not all(isinstance(key, str) for key in value):
        raise ExternalFeedValidationError(f"{name} contains an invalid key")
    return value


def _timestamp(value: object, name: str) -> datetime:
    if isinstance(value, datetime):
        return _aware_utc(value, name)
    if not isinstance(value, str):
        raise ExternalFeedValidationError(f"{name} must be a timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ExternalFeedValidationError(f"{name} must be a timestamp") from exc
    return _aware_utc(parsed, name)


def _aware_utc(value: datetime, name: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ExternalFeedValidationError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _date(value: object, name: str) -> date:
    if isinstance(value, datetime) or not isinstance(value, str):
        raise ExternalFeedValidationError(f"{name} must be an ISO date")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ExternalFeedValidationError(f"{name} must be an ISO date") from exc


def _decimal(value: object, name: str) -> Decimal:
    raw = value
    if isinstance(value, Mapping) and set(value) == {"$decimal"}:
        raw = value["$decimal"]
    if isinstance(raw, bool) or raw is None or isinstance(raw, float):
        raise ExternalFeedValidationError(f"{name} must be an exact decimal")
    try:
        result = Decimal(str(raw))
    except (InvalidOperation, ValueError) as exc:
        raise ExternalFeedValidationError(f"{name} must be an exact decimal") from exc
    if not result.is_finite():
        raise ExternalFeedValidationError(f"{name} must be a finite decimal")
    return result


def _nonnegative_decimal(value: object, name: str) -> Decimal:
    result = _decimal(value, name)
    if result < 0:
        raise ExternalFeedValidationError(f"{name} must be nonnegative")
    return result


def _positive_integer(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ExternalFeedValidationError(f"{name} must be a positive integer")
    return value


def _nonnegative_integer(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ExternalFeedValidationError(f"{name} must be a nonnegative integer")
    return value


def _integer_or_none(value: object, name: str) -> int | None:
    if value is None:
        return None
    return _nonnegative_integer(value, name)


def _nonblank(value: object) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ExternalFeedValidationError(
                f"external feed JSON contains duplicate key {key!r}"
            )
        result[key] = value
    return result


def _freeze_mapping(value: Mapping[str, object]) -> Mapping[str, object]:
    return MappingProxyType(
        {str(key): _freeze_value(item) for key, item in value.items()}
    )


def _freeze_value(value: object) -> object:
    if isinstance(value, Mapping):
        return _freeze_mapping(value)
    if isinstance(value, Sequence) and not isinstance(
        value, (str, bytes, bytearray, memoryview)
    ):
        return tuple(_freeze_value(item) for item in value)
    return value


def _detached_mapping(value: Mapping[str, object]) -> dict[str, object]:
    return json.loads(canonical_json(value), object_pairs_hook=_unique_object)


def _atomic_replace(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = -1
    temporary: Path | None = None
    try:
        descriptor, raw_path = tempfile.mkstemp(
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
        )
        temporary = Path(raw_path)
        with os.fdopen(descriptor, "wb") as handle:
            descriptor = -1
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        temporary = None
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if temporary is not None:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass


__all__ = [
    "EXTERNAL_READONLY_FEED_SCHEMA",
    "EXTERNAL_READONLY_FEED_VERSION",
    "OPEN_REPRICE_PURPOSE",
    "PREMARKET_ACCOUNT_PURPOSE",
    "ExternalFeedValidationError",
    "ExternalReadonlyBatch",
    "ExternalReadonlyFeedPublisher",
    "ExternalReadonlyFeedReader",
]
