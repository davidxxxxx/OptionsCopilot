"""Immutable broker-derived volatility and liquidity evidence.

This module intentionally has no notion of news or positioning eligibility.
Those inputs are permanently supporting-only, even when they are supplied in
the same input document as a complete IBKR quote surface.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum
from typing import Any

from options_copilot.storage.canonical import canonical_hash, freeze_json, utc_datetime


class EvidenceRole(str, Enum):
    HARD = "HARD"
    SUPPORTING_ONLY = "SUPPORTING_ONLY"


class EvidenceClass(str, Enum):
    MARKET = "MARKET"
    VOLATILITY = "VOLATILITY"
    LIQUIDITY = "LIQUIDITY"
    NEWS = "NEWS"
    EARNINGS = "EARNINGS"
    POSITIONING = "POSITIONING"


SUPPORTING_ONLY_KINDS = frozenset({"NEWS", "EARNINGS", "MAX_PAIN", "WALLS", "PCR", "GEX", "POSITIONING"})


@dataclass(frozen=True, slots=True)
class VolatilityEvidence:
    evidence_class: EvidenceClass
    role: EvidenceRole
    eligible: bool
    reasons: tuple[str, ...]
    observed_at: datetime | None
    input_hash: str
    secdef_hash: str | None
    quote_hash: str | None
    evidence_hash: str
    iv_percentile: Decimal | None = None
    iv_rank: Decimal | None = None
    term_structure: Decimal | None = None
    skew: Decimal | None = None
    expected_move_fraction: Decimal | None = None
    earnings_iv_crush: Decimal | None = None
    features: object | None = None


class VolatilityEngine:
    """Build fail-closed hard evidence from one complete IBKR option surface."""

    max_age = timedelta(seconds=5)

    @staticmethod
    def role_for(source: object) -> EvidenceRole:
        return EvidenceRole.SUPPORTING_ONLY if str(source).strip().upper() in SUPPORTING_ONLY_KINDS else EvidenceRole.HARD

    def evaluate(self, raw: Mapping[str, Any] | object, *, now: datetime | None = None) -> VolatilityEvidence:
        document = _mapping(raw)
        now_utc = utc_datetime(now or datetime.now(timezone.utc), field="now")
        batch = document.get("volatility_by_underlying")
        if isinstance(batch, Mapping) and batch:
            return self._evaluate_batch(batch, now=now_utc)
        source = str(document.get("source", "")).upper()
        role = self.role_for(source)
        reasons: list[str] = []
        observed = _datetime(document.get("observed_at"))
        secdef_hash = _hash_or_none(document.get("secdef_hash"))
        quote_hash = _hash_or_none(document.get("quote_hash"))
        if role is EvidenceRole.SUPPORTING_ONLY:
            reasons.append("SUPPORTING_ONLY_SOURCE")
        if source != "IBKR":
            reasons.append("NON_IBKR_SOURCE")
        if observed is None:
            reasons.append("MISSING_TIMESTAMP")
        elif observed > now_utc or now_utc - observed > self.max_age:
            reasons.append("STALE_OR_FUTURE_INPUT")
        if not secdef_hash:
            reasons.append("MISSING_SECDEF_HASH")
        if not quote_hash:
            reasons.append("MISSING_QUOTE_HASH")
        if bool(document.get("conflicted", False)):
            reasons.append("CONFLICTED_INPUT")
        quotes = tuple(document.get("quotes", ()))
        if not quotes:
            reasons.append("MISSING_OPTION_QUOTES")
        else:
            for quote in quotes:
                row = _mapping(quote)
                bid, ask, iv = _decimal(row.get("bid")), _decimal(row.get("ask")), _decimal(row.get("iv", row.get("implied_volatility")))
                if bid is None or ask is None or bid <= 0 or ask <= bid:
                    reasons.append("NON_EXECUTABLE_QUOTE")
                    break
                if iv is None or iv <= 0:
                    reasons.append("MISSING_IV")
                    break
                if _integer(row.get("volume")) is None or _integer(row.get("open_interest")) is None:
                    reasons.append("MISSING_LIQUIDITY_FIELDS")
                    break
        history = tuple(value for value in (_decimal(x) for x in document.get("iv_history", ())) if value is not None)
        atm_iv = _decimal(document.get("atm_iv")) or (_decimal(_mapping(quotes[0]).get("iv", _mapping(quotes[0]).get("implied_volatility"))) if quotes else None)
        if atm_iv is None or not history:
            reasons.append("MISSING_IV_HISTORY")
        input_hash = (
            _hash_or_none(document.get("input_hash"))
            or _hash_or_none(document.get("evidence_hash"))
            or canonical_hash(document)
        )
        percentile = rank = term = skew = expected = crush = None
        if atm_iv is not None and history:
            percentile = Decimal(sum(item <= atm_iv for item in history)) / Decimal(len(history))
            low, high = min(history), max(history)
            rank = Decimal("0") if high == low else (atm_iv - low) / (high - low)
            near, nxt = _decimal(document.get("near_atm_iv")), _decimal(document.get("next_atm_iv"))
            if near is not None and nxt is not None and nxt > 0:
                term = (near - nxt) / nxt
            put, call = _decimal(document.get("put_25_delta_iv")), _decimal(document.get("call_25_delta_iv"))
            if put is not None and call is not None and atm_iv > 0:
                skew = abs(put - call) / atm_iv
            days = _decimal(document.get("calendar_days", document.get("dte")))
            if days is not None and days >= 0:
                expected = max(Decimal("0.005"), atm_iv * (days / Decimal("365")).sqrt())
            before, after = _decimal(document.get("pre_earnings_iv")), _decimal(document.get("post_earnings_iv"))
            if before is not None and after is not None and before > 0:
                crush = (before - after) / before
        eligible = not reasons
        features = freeze_json({"iv_percentile": percentile, "iv_rank": rank, "term_structure": term, "skew": skew, "expected_move_fraction": expected, "earnings_iv_crush": crush})
        evidence_hash = canonical_hash({"class": EvidenceClass.VOLATILITY.value, "role": role.value, "eligible": eligible, "reasons": sorted(set(reasons)), "input_hash": input_hash, "secdef_hash": secdef_hash, "quote_hash": quote_hash, "observed_at": observed, "features": features})
        return VolatilityEvidence(EvidenceClass.VOLATILITY, role, eligible, tuple(sorted(set(reasons))), observed, input_hash, secdef_hash, quote_hash, evidence_hash, percentile, rank, term, skew, expected, crush, features)

    def _evaluate_batch(
        self,
        batch: Mapping[object, object],
        *,
        now: datetime,
    ) -> VolatilityEvidence:
        rows: dict[str, Mapping[str, object]] = {}
        for raw_symbol, raw_value in sorted(
            batch.items(),
            key=lambda item: str(item[0]).strip().upper(),
        ):
            symbol = str(raw_symbol).strip().upper()
            if not symbol or not isinstance(raw_value, Mapping):
                continue
            evidence = self.evaluate(raw_value, now=now)
            rows[symbol] = {
                "eligible": evidence.eligible,
                "reasons": evidence.reasons,
                "observed_at": evidence.observed_at,
                "input_hash": evidence.input_hash,
                "secdef_hash": evidence.secdef_hash,
                "quote_hash": evidence.quote_hash,
                "evidence_hash": evidence.evidence_hash,
                "iv_percentile": evidence.iv_percentile,
                "iv_rank": evidence.iv_rank,
                "term_structure": evidence.term_structure,
                "skew": evidence.skew,
                "expected_move_fraction": evidence.expected_move_fraction,
                "earnings_iv_crush": evidence.earnings_iv_crush,
                "features": evidence.features,
            }
        eligible = any(bool(row.get("eligible")) for row in rows.values())
        reasons = () if eligible else ("NO_ELIGIBLE_UNDERLYING_VOLATILITY",)
        observed_values = tuple(
            value
            for row in rows.values()
            if isinstance((value := row.get("observed_at")), datetime)
        )
        observed = min(observed_values) if observed_values else None
        input_hash = canonical_hash(
            tuple((symbol, row["input_hash"]) for symbol, row in rows.items())
        )
        secdef_hash = canonical_hash(
            tuple((symbol, row["secdef_hash"]) for symbol, row in rows.items())
        )
        quote_hash = canonical_hash(
            tuple((symbol, row["quote_hash"]) for symbol, row in rows.items())
        )
        features = freeze_json({"by_underlying": rows})
        evidence_hash = canonical_hash(
            {
                "class": EvidenceClass.VOLATILITY.value,
                "role": EvidenceRole.HARD.value,
                "eligible": eligible,
                "reasons": reasons,
                "input_hash": input_hash,
                "secdef_hash": secdef_hash,
                "quote_hash": quote_hash,
                "observed_at": observed,
                "features": features,
            }
        )
        return VolatilityEvidence(
            EvidenceClass.VOLATILITY,
            EvidenceRole.HARD,
            eligible,
            reasons,
            observed,
            input_hash,
            secdef_hash,
            quote_hash,
            evidence_hash,
            features=features,
        )

    build = evaluate


def _mapping(value: object) -> Mapping[str, Any]:
    if isinstance(value, Mapping): return value
    if hasattr(value, "__dict__"): return vars(value)
    return {name: getattr(value, name) for name in dir(value) if not name.startswith("_") and not callable(getattr(value, name))}

def _decimal(value: object) -> Decimal | None:
    if isinstance(value, Decimal) and value.is_finite(): return value
    return None

def _integer(value: object) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else None

def _datetime(value: object) -> datetime | None:
    try: return utc_datetime(value, field="observed_at") if isinstance(value, datetime) else None
    except ValueError: return None

def _hash_or_none(value: object) -> str | None:
    return value if isinstance(value, str) and len(value) == 64 and all(c in "0123456789abcdef" for c in value) else None


__all__ = ["EvidenceClass", "EvidenceRole", "SUPPORTING_ONLY_KINDS", "VolatilityEngine", "VolatilityEvidence"]
