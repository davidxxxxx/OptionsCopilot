"""Durable hash-bound PIT cache for captured underlying research evidence."""

from __future__ import annotations

import json
import sqlite3
import threading
from typing import Callable
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

from options_copilot.storage.canonical import canonical_hash, canonical_json, datetime_text, freeze_json, thaw_json, utc_datetime

from .models import FactorEvidence, FactorKind, FactorStatus, LiquidityEvidence


GENESIS_HASH = "0" * 64
_IMMUTABLE_TRIGGER_OPERATIONS = {
    "underlying_evidence_no_update": "UPDATE",
    "underlying_evidence_no_delete": "DELETE",
}


def _quote_trend_measurement(
    *,
    bid: object,
    ask: object,
    last: object,
    close: object,
) -> tuple[Decimal, Decimal] | None:
    """Separate measured direction strength from quote-quality confidence."""

    values = (bid, ask, last, close)
    if any(
        not isinstance(value, Decimal)
        or not value.is_finite()
        or value <= 0
        for value in values
    ):
        return None
    assert isinstance(bid, Decimal)
    assert isinstance(ask, Decimal)
    assert isinstance(last, Decimal)
    assert isinstance(close, Decimal)
    if ask < bid:
        return None
    mid = (bid + ask) / Decimal("2")
    spread_rate = (ask - bid) / mid
    change = (last - close) / close
    signal = max(
        Decimal("-1"),
        min(Decimal("1"), change * Decimal("20")),
    )
    # The move magnitude already belongs in ``signal``.  Reusing it as
    # confidence squared small but genuine moves and starved the uncertainty
    # calculation.  A fresh, hash-bound bid/ask spread measures the quality of
    # the observed price change without manufacturing direction.
    confidence = max(
        Decimal("0"),
        min(Decimal("1"), Decimal("1") - spread_rate),
    )
    return signal, confidence


class UnderlyingEvidenceCacheCorruption(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class UnderlyingEvidenceRecord:
    symbol: str
    observed_at: datetime
    effective_at: datetime
    valid_until: datetime
    bid: Decimal
    ask: Decimal
    last: Decimal | None
    close: Decimal | None
    source_hashes: tuple[str, ...]
    trend: FactorEvidence
    regime: FactorEvidence
    positioning: FactorEvidence
    liquidity: LiquidityEvidence

    def __post_init__(self) -> None:
        symbol = self.symbol.strip().upper() if isinstance(self.symbol, str) else ""
        if not symbol:
            raise ValueError("symbol is required")
        object.__setattr__(self, "symbol", symbol)
        for field in ("observed_at", "effective_at", "valid_until"):
            object.__setattr__(self, field, utc_datetime(getattr(self, field), field=field))
        if self.effective_at > self.observed_at or self.valid_until < self.observed_at:
            raise ValueError("underlying evidence timing is invalid")
        for field in ("bid", "ask"):
            value = getattr(self, field)
            if not isinstance(value, Decimal) or not value.is_finite() or value <= 0:
                raise ValueError(f"{field} must be positive finite Decimal")
        if self.ask < self.bid:
            raise ValueError("ask cannot be below bid")
        for field in ("last", "close"):
            value = getattr(self, field)
            if value is not None and (not isinstance(value, Decimal) or not value.is_finite() or value <= 0):
                raise ValueError(f"{field} must be positive finite Decimal or null")
        hashes = tuple(self.source_hashes)
        if not hashes or any(not isinstance(item, str) or len(item) != 64 for item in hashes):
            raise ValueError("source_hashes are invalid")
        object.__setattr__(self, "source_hashes", hashes)
        expected = {
            FactorKind.TREND_VOLATILITY: self.trend,
            FactorKind.REGIME: self.regime,
            FactorKind.POSITIONING: self.positioning,
        }
        if any(
            not isinstance(value, FactorEvidence) or value.factor is not kind
            for kind, value in expected.items()
        ):
            raise ValueError("factor evidence kinds are invalid")
        if not isinstance(self.liquidity, LiquidityEvidence):
            raise TypeError("liquidity must be LiquidityEvidence")

    @property
    def record_hash(self) -> str:
        return canonical_hash(self.as_dict())

    def as_dict(self) -> dict[str, object]:
        return {
            "schema": "options_copilot.underlying_evidence_record.v1",
            "symbol": self.symbol, "observed_at": self.observed_at,
            "effective_at": self.effective_at, "valid_until": self.valid_until,
            "bid": self.bid, "ask": self.ask, "last": self.last, "close": self.close,
            "source_hashes": self.source_hashes, "trend": self.trend.as_dict(),
            "regime": self.regime.as_dict(), "positioning": self.positioning.as_dict(),
            "liquidity": self.liquidity.as_dict(),
        }


@dataclass(frozen=True, slots=True)
class UnderlyingEvidenceReadView:
    """One immutable PIT read view created afresh for one equity-pool build."""

    as_of: datetime
    records: tuple[tuple[str, UnderlyingEvidenceRecord], ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "as_of", utc_datetime(self.as_of, field="read view as_of"))
        rows = tuple(self.records)
        if len({symbol for symbol, _record in rows}) != len(rows):
            raise ValueError("read view contains duplicate symbols")
        if any(symbol != record.symbol for symbol, record in rows):
            raise ValueError("read view symbol mismatch")
        object.__setattr__(self, "records", rows)

    def _record(self, symbol: str) -> UnderlyingEvidenceRecord | None:
        normalized = str(symbol).strip().upper()
        return next(
            (record for value, record in self.records if value == normalized),
            None,
        )

    def read_factor(self, symbol: str, kind: FactorKind) -> FactorEvidence | None:
        record = self._record(symbol)
        if record is None:
            return None
        return {
            FactorKind.REGIME: record.regime,
            FactorKind.TREND_VOLATILITY: record.trend,
            FactorKind.POSITIONING: record.positioning,
        }.get(kind)

    def read_liquidity(self, symbol: str) -> LiquidityEvidence | None:
        record = self._record(symbol)
        return None if record is None else record.liquidity


class UnderlyingEvidenceCache:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._connection = sqlite3.connect(self.path, isolation_level=None, check_same_thread=False)
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA journal_mode=WAL")
        self._connection.execute("PRAGMA synchronous=FULL")
        self._migrate()

    def close(self) -> None:
        self._connection.close()

    def append(
        self,
        record: UnderlyingEvidenceRecord,
        *,
        commit_guard: Callable[[], bool] | None = None,
    ) -> str:
        if not isinstance(record, UnderlyingEvidenceRecord):
            raise TypeError("record must be UnderlyingEvidenceRecord")
        body = canonical_json(record.as_dict())
        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                self.assert_integrity()
                existing = self._connection.execute(
                    "SELECT record_hash FROM underlying_evidence WHERE record_hash=?", (record.record_hash,)
                ).fetchone()
                if existing is not None:
                    if commit_guard is not None and not commit_guard():
                        raise TimeoutError("underlying evidence commit cancelled")
                    self._connection.execute("COMMIT")
                    return record.record_hash
                previous = self._connection.execute(
                    "SELECT chain_hash FROM underlying_evidence ORDER BY sequence DESC LIMIT 1"
                ).fetchone()
                previous_hash = GENESIS_HASH if previous is None else str(previous[0])
                chain_hash = canonical_hash({"previous": previous_hash, "record_hash": record.record_hash})
                self._connection.execute(
                    "INSERT INTO underlying_evidence(symbol, observed_at, valid_until, record_json, record_hash, previous_chain_hash, chain_hash) VALUES(?,?,?,?,?,?,?)",
                    (record.symbol, datetime_text(record.observed_at), datetime_text(record.valid_until), body, record.record_hash, previous_hash, chain_hash),
                )
                if commit_guard is not None and not commit_guard():
                    raise TimeoutError("underlying evidence commit cancelled")
                self._connection.execute("COMMIT")
                return record.record_hash
            except BaseException:
                self._connection.execute("ROLLBACK")
                raise

    def latest(self, symbol: str, *, as_of: datetime) -> UnderlyingEvidenceRecord | None:
        cutoff = utc_datetime(as_of, field="cache cutoff")
        self.assert_integrity()
        rows = self._connection.execute(
            "SELECT symbol, observed_at, valid_until, record_json "
            "FROM underlying_evidence WHERE symbol=? AND observed_at<=? "
            "AND valid_until>=? ORDER BY observed_at DESC, record_hash DESC",
            (symbol.strip().upper(), datetime_text(cutoff), datetime_text(cutoff)),
        ).fetchall()
        return None if not rows else _record_from_authority_row(rows[0], cutoff=cutoff)

    def snapshot_for(
        self,
        symbols: tuple[str, ...] | list[str],
        as_of: datetime,
    ) -> UnderlyingEvidenceReadView:
        """Verify once and materialize one bounded latest-record PIT snapshot."""

        cutoff = utc_datetime(as_of, field="cache snapshot cutoff")
        normalized = tuple(dict.fromkeys(
            str(symbol).strip().upper()
            for symbol in symbols
            if str(symbol).strip()
        ))
        if len(normalized) > 150:
            raise ValueError("cache snapshot symbol limit exceeded")
        with self._lock:
            self.assert_integrity()
            if not normalized:
                return UnderlyingEvidenceReadView(cutoff, ())
            placeholders = ",".join("?" for _ in normalized)
            rows = self._connection.execute(
                "SELECT symbol, observed_at, valid_until, record_json FROM ("
                "SELECT symbol, observed_at, valid_until, record_json, "
                "ROW_NUMBER() OVER (PARTITION BY symbol "
                "ORDER BY observed_at DESC, record_hash DESC) AS row_number "
                f"FROM underlying_evidence WHERE symbol IN ({placeholders}) "
                "AND observed_at<=? AND valid_until>=?"
                ") WHERE row_number=1 ORDER BY symbol",
                (*normalized, datetime_text(cutoff), datetime_text(cutoff)),
            ).fetchall()
            latest: dict[str, UnderlyingEvidenceRecord] = {}
            for row in rows:
                record = _record_from_authority_row(row, cutoff=cutoff)
                if record.symbol in latest:
                    raise UnderlyingEvidenceCacheCorruption(
                        "underlying evidence snapshot duplicate symbol"
                    )
                latest[record.symbol] = record
            return UnderlyingEvidenceReadView(
                cutoff,
                tuple((symbol, latest[symbol]) for symbol in normalized if symbol in latest),
            )

    def read_factor(self, symbol: str, kind: FactorKind, as_of: datetime) -> FactorEvidence | None:
        record = self.latest(symbol, as_of=as_of)
        if record is None:
            return None
        return {FactorKind.REGIME: record.regime, FactorKind.TREND_VOLATILITY: record.trend, FactorKind.POSITIONING: record.positioning}.get(kind)

    def read_liquidity(self, symbol: str, as_of: datetime) -> LiquidityEvidence | None:
        record = self.latest(symbol, as_of=as_of)
        return None if record is None else record.liquidity

    def replay(self, record_hash: str) -> UnderlyingEvidenceRecord:
        self.assert_integrity()
        row = self._connection.execute(
            "SELECT record_json FROM underlying_evidence WHERE record_hash=?", (record_hash,)
        ).fetchone()
        if row is None:
            raise KeyError(record_hash)
        record = _record_from_json(str(row[0]))
        if record.record_hash != record_hash:
            raise UnderlyingEvidenceCacheCorruption("underlying evidence replay hash mismatch")
        return record

    def assert_integrity(self) -> None:
        with self._lock:
            triggers = {row[0]: row[1] for row in self._connection.execute("SELECT name, sql FROM sqlite_master WHERE type='trigger'")}
            for name, operation in _IMMUTABLE_TRIGGER_OPERATIONS.items():
                actual = _normalize_trigger_sql(str(triggers.get(name) or ""))
                expected = _normalize_trigger_sql(
                    _immutable_trigger_sql(name, operation)
                )
                if actual != expected:
                    raise UnderlyingEvidenceCacheCorruption("underlying evidence immutable trigger invalid")
            previous = GENESIS_HASH
            for row in self._connection.execute("SELECT * FROM underlying_evidence ORDER BY sequence"):
                record = _record_from_authority_row(row)
                if record.record_hash != str(row["record_hash"]):
                    raise UnderlyingEvidenceCacheCorruption("underlying evidence record hash mismatch")
                if str(row["previous_chain_hash"]) != previous:
                    raise UnderlyingEvidenceCacheCorruption("underlying evidence predecessor mismatch")
                expected = canonical_hash({"previous": previous, "record_hash": record.record_hash})
                if str(row["chain_hash"]) != expected:
                    raise UnderlyingEvidenceCacheCorruption("underlying evidence chain hash mismatch")
                previous = expected

    def _migrate(self) -> None:
        self._connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS underlying_evidence(
                sequence INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT NOT NULL,
                observed_at TEXT NOT NULL, valid_until TEXT NOT NULL,
                record_json TEXT NOT NULL, record_hash TEXT NOT NULL UNIQUE,
                previous_chain_hash TEXT NOT NULL, chain_hash TEXT NOT NULL UNIQUE
            );
            CREATE INDEX IF NOT EXISTS underlying_evidence_symbol_time ON underlying_evidence(symbol, observed_at);
            """
        )
        for name, operation in _IMMUTABLE_TRIGGER_OPERATIONS.items():
            self._connection.execute(
                _immutable_trigger_sql(name, operation, if_not_exists=True)
            )
        self.assert_integrity()


def captured_record_from_quote(quote: object, *, captured_at: datetime) -> UnderlyingEvidenceRecord:
    observed = utc_datetime(getattr(quote, "observed_at"), field="quote observed_at")
    captured = utc_datetime(captured_at, field="captured_at")
    if observed > captured or captured - observed > timedelta(minutes=15):
        raise ValueError("captured quote is future or stale")
    symbol = str(getattr(quote, "symbol", "")).strip().upper()
    bid = getattr(quote, "bid", None)
    ask = getattr(quote, "ask", None)
    last = getattr(quote, "last", None)
    close = getattr(quote, "close", None)
    if (
        not isinstance(bid, Decimal)
        or not bid.is_finite()
        or bid <= 0
        or not isinstance(ask, Decimal)
        or not ask.is_finite()
        or ask <= 0
        or ask < bid
    ):
        raise ValueError("captured quote requires Decimal bid/ask")
    source_hash = canonical_hash({
        "symbol": symbol, "contract_id": getattr(quote, "contract_id", None),
        "exchange": getattr(quote, "exchange", None), "observed_at": observed,
        "bid": bid, "ask": ask, "last": last, "close": close,
        "source": getattr(quote, "source", None),
    })
    valid_until = observed + timedelta(minutes=15)
    def missing(kind: FactorKind, reason: str) -> FactorEvidence:
        return FactorEvidence(
            factor=kind, status=FactorStatus.MISSING, signed_signal=None, confidence=None,
            horizon="INTRADAY", observed_at=observed, effective_at=observed,
            valid_until=valid_until, source_hashes=(source_hash,), reasons=(reason,),
            payload_hash=canonical_hash({"source_hash": source_hash, "reason": reason}),
        )
    mid = (bid + ask) / Decimal("2")
    spread = (ask - bid) / mid
    trend_measurement = _quote_trend_measurement(
        bid=bid,
        ask=ask,
        last=last,
        close=close,
    )
    if trend_measurement is None:
        trend = missing(
            FactorKind.TREND_VOLATILITY,
            "UNDERLYING_LAST_OR_CLOSE_UNAVAILABLE",
        )
    else:
        signal, confidence = trend_measurement
        trend = FactorEvidence(
            factor=FactorKind.TREND_VOLATILITY,
            status=FactorStatus.AVAILABLE,
            signed_signal=signal,
            confidence=confidence,
            horizon="INTRADAY",
            observed_at=observed,
            effective_at=observed,
            valid_until=valid_until,
            source_hashes=(source_hash,),
            reasons=(
                "CAPTURED_UNDERLYING_PRICE_TREND",
                "CONFIDENCE_FROM_MEASURED_BID_ASK_QUALITY",
            ),
            payload_hash=canonical_hash(
                {
                    "source_hash": source_hash,
                    "signal": signal,
                    "confidence": confidence,
                }
            ),
        )
    liquidity = LiquidityEvidence(
        status=FactorStatus.AVAILABLE,
        score=max(Decimal("0"), min(Decimal("100"), Decimal("100") * (Decimal("1") - spread))),
        observed_at=observed, source_hashes=(source_hash,),
        reasons=("CAPTURED_MEASURED_UNDERLYING_BID_ASK",),
        payload_hash=canonical_hash({"source_hash": source_hash, "spread": spread}),
    )
    return UnderlyingEvidenceRecord(
        symbol=symbol, observed_at=observed, effective_at=observed,
        valid_until=valid_until, bid=bid, ask=ask, last=last, close=close,
        source_hashes=(source_hash,), trend=trend,
        regime=missing(FactorKind.REGIME, "MARKET_REGIME_CACHE_MISSING"),
        positioning=missing(FactorKind.POSITIONING, "POSITIONING_CACHE_MISSING"),
        liquidity=liquidity,
    )


def captured_records_from_quotes(
    quotes: tuple[object, ...] | list[object],
    *,
    captured_at: datetime,
) -> tuple[UnderlyingEvidenceRecord, ...]:
    """Normalize one real broker quote batch and bind its breadth regime."""

    records = tuple(
        captured_record_from_quote(quote, captured_at=captured_at)
        for quote in quotes
    )
    directional = tuple(
        Decimal("1") if record.last is not None and record.close is not None and record.last > record.close
        else Decimal("-1") if record.last is not None and record.close is not None and record.last < record.close
        else Decimal("0")
        for record in records
    )
    if not records or not directional:
        return records
    signal = sum(directional, Decimal("0")) / Decimal(len(directional))
    confidence = Decimal(len(tuple(item for item in directional if item != 0))) / Decimal(len(directional))
    batch_hash = canonical_hash(tuple(record.record_hash for record in records))
    enriched: list[UnderlyingEvidenceRecord] = []
    for record in records:
        regime = FactorEvidence(
            factor=FactorKind.REGIME, status=FactorStatus.AVAILABLE,
            signed_signal=signal, confidence=confidence, horizon="INTRADAY",
            observed_at=record.observed_at, effective_at=record.effective_at,
            valid_until=record.valid_until, source_hashes=(batch_hash, *record.source_hashes),
            reasons=("CAPTURED_UNDERLYING_BATCH_BREADTH_REGIME",),
            payload_hash=canonical_hash({"batch_hash": batch_hash, "signal": signal, "confidence": confidence}),
        )
        enriched.append(replace(record, regime=regime))
    return tuple(enriched)


def _record_from_json(value: str) -> UnderlyingEvidenceRecord:
    raw = thaw_json(freeze_json(json.loads(value)))
    if not isinstance(raw, dict) or raw.get("schema") != "options_copilot.underlying_evidence_record.v1":
        raise UnderlyingEvidenceCacheCorruption("underlying evidence record invalid")
    return UnderlyingEvidenceRecord(
        symbol=str(raw["symbol"]), observed_at=datetime.fromisoformat(str(raw["observed_at"])),
        effective_at=datetime.fromisoformat(str(raw["effective_at"])), valid_until=datetime.fromisoformat(str(raw["valid_until"])),
        bid=raw["bid"], ask=raw["ask"], last=raw.get("last"), close=raw.get("close"),
        source_hashes=tuple(raw["source_hashes"]), trend=FactorEvidence.from_dict(raw["trend"]),
        regime=FactorEvidence.from_dict(raw["regime"]), positioning=FactorEvidence.from_dict(raw["positioning"]),
        liquidity=LiquidityEvidence.from_dict(raw["liquidity"]),
    )


def _record_from_authority_row(
    row: sqlite3.Row,
    *,
    cutoff: datetime | None = None,
) -> UnderlyingEvidenceRecord:
    """Bind query authority columns to the immutable hashed record body."""

    record = _record_from_json(str(row["record_json"]))
    if (
        str(row["symbol"]) != record.symbol
        or str(row["observed_at"]) != datetime_text(record.observed_at)
        or str(row["valid_until"]) != datetime_text(record.valid_until)
    ):
        raise UnderlyingEvidenceCacheCorruption(
            "underlying evidence authority column mismatch"
        )
    if cutoff is not None and not (
        record.observed_at <= cutoff <= record.valid_until
    ):
        raise UnderlyingEvidenceCacheCorruption(
            "underlying evidence snapshot cutoff mismatch"
        )
    return record


def _immutable_trigger_sql(
    name: str,
    operation: str,
    *,
    if_not_exists: bool = False,
) -> str:
    qualifier = " IF NOT EXISTS" if if_not_exists else ""
    return (
        f"CREATE TRIGGER{qualifier} {name} BEFORE {operation} "
        "ON underlying_evidence BEGIN SELECT RAISE(ABORT, "
        "'underlying_evidence immutable'); END"
    )


def _normalize_trigger_sql(value: str) -> str:
    return " ".join(value.replace("IF NOT EXISTS", "").split()).upper()


__all__ = ["UnderlyingEvidenceCache", "UnderlyingEvidenceCacheCorruption", "UnderlyingEvidenceReadView", "UnderlyingEvidenceRecord", "captured_record_from_quote", "captured_records_from_quotes"]
