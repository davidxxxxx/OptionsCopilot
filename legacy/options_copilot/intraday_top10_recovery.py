"""Create one honest, supporting-only Top-10 after missed scheduled slots."""

from __future__ import annotations

import argparse
from collections.abc import Mapping, Sequence
from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
import json
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

from options_copilot.config import OptionsCopilotConfig
from options_copilot.gateway.ibkr_readonly import IBKRReadOnlyGateway
from options_copilot.news.models import OptionRight
from options_copilot.news.research_top10 import (
    INTRADAY_RECOVERY,
    RESEARCH_TOP10_DIRECT_SOURCE,
    RESEARCH_TOP10_SCHEMA,
    RESEARCH_TOP10_VERSION,
    import_research_top10,
    read_research_top10,
)
from options_copilot.performance.nav_ledger import StrategyNavLedger
from options_copilot.operations.pacing_authority import (
    PACING_EXPECTED_ACTOR,
    load_pacing_authority_verifier,
)
from options_copilot.production_runtime import (
    DirectTop10StructureSource,
    GuardedRequestBudget,
    PacingAuthorityGuard,
)
from options_copilot.runtime import STRATEGY_NAV_CONTRACT_PATH
from options_copilot.storage.canonical import canonical_hash


NEW_YORK = ZoneInfo("America/New_York")
RECOVERY_COST_CAP_USD = Decimal("20.00")
RECOVERY_CORE_LIMIT = 14
_AUTHORITY = {
    "decision_authority": "SUPPORTING_ONLY",
    "approval_eligible": False,
    "instruction_creation_allowed": False,
    "order_allowed": False,
    "action_pool_eligible": False,
}
_UNQUOTED_LEG_BLOCKERS = (
    "QUOTE_UNAVAILABLE",
    "QUOTE_EXCHANGE_TIMESTAMP_UNAVAILABLE",
    "IMPLIED_VOLATILITY_UNAVAILABLE",
    "DELTA_UNAVAILABLE",
    "GAMMA_UNAVAILABLE",
    "THETA_UNAVAILABLE",
    "VEGA_UNAVAILABLE",
    "VOLUME_UNAVAILABLE",
    "OPEN_INTEREST_UNAVAILABLE",
    "MARKET_DATA_TYPE_UNVERIFIED",
)
_UNVERIFIED_CANDIDATE_BLOCKERS = (
    "ASSUMED_MULTIPLIER_USED",
    "ENTRY_DEBIT_UNVERIFIED",
    "MAXIMUM_LOSS_UNVERIFIED",
    "AFTER_COST_EV_UNVERIFIED",
    "RISK_CAP_UNVERIFIED",
    "QUOTE_UNAVAILABLE",
    "EXPECTED_PAYOFF_UNAVAILABLE",
    "INTRADAY_RECOVERY_AFTER_MISSED_SLOTS",
)


def build_intraday_recovery_envelope(
    structures: Sequence[object],
    *,
    observed_at: datetime,
    strategy_nav_usd: Decimal,
) -> dict[str, object]:
    """Build a point-in-time exact-identity watchlist with no action authority."""

    instant = _aware(observed_at)
    observed_et = instant.astimezone(NEW_YORK)
    if observed_et.hour < 9 or (
        observed_et.hour == 9 and observed_et.minute < 30
    ):
        raise ValueError("intraday recovery is available only after 09:30 ET")
    if (
        not isinstance(strategy_nav_usd, Decimal)
        or not strategy_nav_usd.is_finite()
        or strategy_nav_usd <= 0
    ):
        raise ValueError("strategy NAV must be a positive Decimal")
    rows = tuple(structures)[:10]
    if not rows:
        raise ValueError("intraday recovery found no exact structures")

    candidates = [
        _candidate_document(
            item,
            rank=rank,
            trading_date=observed_et.date(),
            observed_at=instant,
        )
        for rank, item in enumerate(rows, start=1)
    ]
    blockers = ["INTRADAY_RECOVERY_AFTER_MISSED_SLOTS"]
    if len(candidates) < 10:
        blockers.append("TOP10_RESEARCH_SHORTFALL")
    return {
        "schema": RESEARCH_TOP10_SCHEMA,
        "version": RESEARCH_TOP10_VERSION,
        "batch_id": f"intraday-recovery-{instant.strftime('%Y%m%dT%H%M%S%fZ')}",
        "phase": INTRADAY_RECOVERY,
        "trading_date": observed_et.date().isoformat(),
        "observed_at": instant.isoformat(timespec="microseconds"),
        "source": RESEARCH_TOP10_DIRECT_SOURCE,
        "strategy_nav_usd": format(strategy_nav_usd, ".2f"),
        "normal_risk_fraction": "0.10",
        "target_count": 10,
        "parent_content_hash": None,
        "session": {
            "liquid_hours": None,
            "trading_hours": None,
            "timezone_id": None,
            "observed_at": None,
            "source": None,
            "blockers": ["BROKER_SESSION_HOURS_UNAVAILABLE"],
        },
        "blockers": blockers,
        "candidates": candidates,
        **_AUTHORITY,
    }


def generate_intraday_recovery(
    config: OptionsCopilotConfig,
    *,
    store_path: str | Path | None = None,
    clock: object | None = None,
    gateway: IBKRReadOnlyGateway | None = None,
) -> Mapping[str, object]:
    """Acquire exact conIds from IBKR and import one immutable recovery view."""

    now = clock if callable(clock) else (lambda: datetime.now(timezone.utc))
    started_at = _aware(now())
    guard = PacingAuthorityGuard(
        config.pacing_authority_dir,
        expected_actor=PACING_EXPECTED_ACTOR,
        clock=now,
        signature_verifier=load_pacing_authority_verifier(
            config.pacing_authority_keyring_path
        ),
    )
    pacing = GuardedRequestBudget(guard, now=started_at)
    if not pacing.ready:
        raise RuntimeError("PACING_CAPABILITY_MISSING")

    owned_gateway = gateway is None
    broker = gateway or IBKRReadOnlyGateway(
        config,
        historical_request_lease_factory=lambda: pacing.lease("historical"),
        market_data_request_lease_factory=pacing.lease,
        now=now,
    )
    if not broker.market_data_pacing_enabled:
        raise RuntimeError("WIRE_LEVEL_PACING_UNAVAILABLE")
    nav_ledger: StrategyNavLedger | None = None
    try:
        if owned_gateway:
            broker.connect()
        account = broker.account_snapshot()
        nav_ledger = StrategyNavLedger(
            config.data_dir / "strategy_nav.sqlite3",
            contract=STRATEGY_NAV_CONTRACT_PATH,
            clock=now,
        )
        nav = nav_ledger.snapshot(
            asof=account.asof,
            observed_account_nlv=account.net_liquidation,
        )
        if not nav.valid or nav.strategy_nav is None:
            raise RuntimeError("STRATEGY_NAV_RECONCILIATION_UNAVAILABLE")
        source = DirectTop10StructureSource(
            broker,
            pacing,
            clock=now,
            core_symbols=config.news_core_symbols[:RECOVERY_CORE_LIMIT],
            include_scanner=False,
        )
        structures = source.resolve_top10(scheduled_for=_aware(now()))
        observed_at = _aware(now())
        envelope = build_intraday_recovery_envelope(
            structures,
            observed_at=observed_at,
            strategy_nav_usd=nav.strategy_nav,
        )
        import_research_top10(envelope, store_path=store_path)
        return read_research_top10(store_path=store_path)
    finally:
        if nav_ledger is not None:
            nav_ledger.close()
        if owned_gateway:
            broker.disconnect()


def _candidate_document(
    resolved: object,
    *,
    rank: int,
    trading_date: object,
    observed_at: datetime,
) -> dict[str, object]:
    candidate = getattr(resolved, "candidate", None)
    legs = tuple(getattr(candidate, "legs", ()))
    if candidate is None or len(legs) != 2:
        raise ValueError("resolved recovery structure is invalid")
    right = getattr(legs[0], "right", None)
    if any(getattr(leg, "right", None) != right for leg in legs):
        raise ValueError("resolved recovery rights are inconsistent")
    strategy = (
        "BULL_CALL_VERTICAL"
        if right is OptionRight.CALL
        else "BEAR_PUT_VERTICAL"
    )
    expiration = getattr(legs[0], "expiry", None)
    if expiration is None or any(getattr(leg, "expiry", None) != expiration for leg in legs):
        raise ValueError("resolved recovery expiration is inconsistent")
    identity_legs = [
        _leg_document(leg, observed_at=observed_at)
        for leg in legs
    ]
    evidence_ids = list(getattr(candidate, "evidence_ids", ()))
    evidence_hashes = list(getattr(candidate, "evidence_hashes", ()))
    return {
        "research_id": str(getattr(candidate, "preselection_id")),
        "rank": rank,
        "underlying": str(getattr(candidate, "underlying")),
        "strategy_type": strategy,
        "expiration": expiration.isoformat(),
        "dte": (expiration - trading_date).days,
        "quantity": 1,
        "entry_debit_usd": None,
        "indicative_entry_debit_usd": None,
        "execution_cost_cap_usd": format(RECOVERY_COST_CAP_USD, ".2f"),
        "maximum_loss_usd": None,
        "indicative_maximum_loss_usd": None,
        "cost_after_ev_usd": None,
        "indicative_cost_after_ev_usd": None,
        "assumed_multiplier": 100,
        "risk_cap_verified": False,
        "trade_status": "NO_TRADE",
        "entry_condition": (
            "Wait for a fresh atomic IBKR reprice and every hard Gate before review."
        ),
        "invalidation_condition": (
            "Discard when direction, event evidence, contract identity, or liquidity changes."
        ),
        "profit_target_condition": "No profit target is active before formal repricing.",
        "stop_loss_condition": "No order exists; the human retains exclusive control.",
        "research_summary": (
            "Intraday exact-identity recovery after the scheduled Top-10 windows were "
            "missed; structure-only, supporting-only, and not executable."
        ),
        "evidence_ids": evidence_ids,
        "evidence_hashes": evidence_hashes,
        "blockers": list(_UNVERIFIED_CANDIDATE_BLOCKERS),
        "legs": identity_legs,
        **_AUTHORITY,
    }


def _leg_document(leg: object, *, observed_at: datetime) -> dict[str, object]:
    right = getattr(leg, "right", None)
    contract_id = int(getattr(leg, "con_id"))
    exchange = str(getattr(leg, "exchange"))
    identity = {
        "contract_id": contract_id,
        "local_symbol": str(getattr(leg, "local_symbol")),
        "trading_class": str(getattr(leg, "trading_class")),
        "multiplier": int(getattr(leg, "multiplier")),
        "exchange": exchange,
        "expiration": getattr(leg, "expiry").isoformat(),
        "strike": str(getattr(leg, "strike")),
        "right": "C" if right is OptionRight.CALL else "P",
    }
    quote_evidence = {
        "contract_id": contract_id,
        "observed_at": observed_at,
        "status": "UNAVAILABLE",
        "reason": "INTRADAY_STRUCTURE_ONLY_RECOVERY",
    }
    return {
        "contract_id": contract_id,
        "contract_id_ex": f"{contract_id}@{exchange}",
        "side": getattr(leg, "side").value,
        "right": identity["right"],
        "strike": identity["strike"],
        "expiration": identity["expiration"],
        "exchange": exchange,
        "trading_class": identity["trading_class"],
        "local_symbol": identity["local_symbol"],
        "multiplier": identity["multiplier"],
        "standard_or_adjusted": "STANDARD",
        "bid": None,
        "ask": None,
        "collected_at": observed_at.isoformat(timespec="microseconds"),
        "quote_asof": None,
        "implied_volatility": None,
        "delta": None,
        "gamma": None,
        "theta": None,
        "vega": None,
        "volume": None,
        "open_interest": None,
        "market_data_type": None,
        "identity_evidence_hash": canonical_hash(identity),
        "quote_evidence_hash": canonical_hash(quote_evidence),
        "blockers": list(_UNQUOTED_LEG_BLOCKERS),
    }


def _aware(value: object) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("clock must return a timezone-aware datetime")
    return value.astimezone(timezone.utc)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate one read-only intraday Top-10 recovery snapshot"
    )
    parser.add_argument("--store", type=Path)
    parser.add_argument("--client-id", type=int)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(list(argv) if argv is not None else None)
    try:
        config = OptionsCopilotConfig.from_env()
        client_id = args.client_id or min(config.ibkr_client_id + 100, 999999)
        config = replace(config, ibkr_client_id=client_id)
        payload = generate_intraday_recovery(config, store_path=args.store)
    except Exception:
        sys.stderr.write("NO_TRADE: INTRADAY_TOP10_RECOVERY_FAILED\n")
        return 2
    sys.stdout.write(
        json.dumps(
            {
                "status": payload.get("status"),
                "phase": payload.get("phase"),
                "available_count": payload.get("available_count"),
                "target_count": payload.get("target_count"),
                "observed_at": payload.get("observed_at"),
                "symbols": [
                    item.get("underlying")
                    for item in payload.get("candidates", ())
                    if isinstance(item, Mapping)
                ],
                "decision_authority": "SUPPORTING_ONLY",
                "approval_eligible": False,
                "instruction_creation_allowed": False,
                "order_allowed": False,
            },
            sort_keys=True,
        )
        + "\n"
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())


__all__ = [
    "build_intraday_recovery_envelope",
    "generate_intraday_recovery",
    "main",
]
