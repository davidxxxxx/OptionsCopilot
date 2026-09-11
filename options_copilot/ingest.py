"""Validated ingestion boundary for managed-connector snapshots."""
from __future__ import annotations

import argparse
import json
from collections.abc import Mapping, Sequence
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

from options_copilot.config import OptionsCopilotConfig
from options_copilot.state import ManagedSnapshotStore, RuntimeSnapshot


_FORBIDDEN_KEY_PARTS = ("token", "secret", "password", "api_key", "apikey")
_MILESTONES = (Decimal("2500"), Decimal("3500"), Decimal("5000"), Decimal("7500"), Decimal("10000"))


def ingest_payload(
    payload: Mapping[str, object],
    *,
    store: ManagedSnapshotStore,
) -> RuntimeSnapshot:
    _reject_secrets(payload)
    observed_at = datetime.fromisoformat(str(payload.get("observed_at") or ""))
    if observed_at.tzinfo is None or observed_at.utcoffset() is None:
        raise ValueError("observed_at must be timezone-aware")
    source = str(payload.get("source") or "managed_ibkr_connector")
    account = _mapping(payload.get("account"), "account")
    positions = _mapping_sequence(payload.get("positions", []), "positions")
    candidates = _mapping_sequence(payload.get("candidates", []), "candidates")
    if len(candidates) > 3:
        raise ValueError("at most three candidates may be ingested")
    warnings_raw = payload.get("warnings", [])
    if not isinstance(warnings_raw, Sequence) or isinstance(
        warnings_raw, (str, bytes, bytearray)
    ):
        raise ValueError("warnings must be a sequence")
    nlv = _money(account.get("net_liquidation"), "account.net_liquidation")
    if nlv <= 0:
        raise ValueError("net liquidation must be positive")
    previous = store.read()
    campaign_raw = payload.get("campaign")
    campaign = dict(campaign_raw) if isinstance(campaign_raw, Mapping) else {}
    if previous is not None:
        previous_campaign = dict(previous.campaign)
    else:
        previous_campaign = {}
    start_nlv = _money(
        campaign.get("start_nlv_usd")
        or previous_campaign.get("start_nlv_usd")
        or nlv,
        "campaign.start_nlv_usd",
    )
    strategy_nav = _money(
        campaign.get("strategy_nav_usd")
        or previous_campaign.get("strategy_nav_usd")
        or start_nlv,
        "campaign.strategy_nav_usd",
    )
    target = Decimal("10000")
    progress = max(
        Decimal("0"),
        min(Decimal("1"), (strategy_nav - start_nlv) / (target - start_nlv)),
    )
    next_milestone = next((value for value in _MILESTONES if value > strategy_nav), None)
    campaign.update(
        {
            "start_nlv_usd": float(start_nlv),
            "strategy_nav_usd": float(strategy_nav),
            "target_nlv_usd": float(target),
            "progress_fraction": float(progress),
            "next_milestone_usd": None if next_milestone is None else float(next_milestone),
            "external_cash_flows_excluded": True,
        }
    )
    snapshot = RuntimeSnapshot(
        observed_at=observed_at,
        source=source,
        account=account,
        positions=positions,
        candidates=candidates,
        warnings=tuple(str(item) for item in warnings_raw),
        campaign=campaign,
        broker_snapshot_complete=payload.get("broker_snapshot_complete", False),
        working_order_count=payload.get("working_order_count"),
        unsubmitted_instruction_count=payload.get("unsubmitted_instruction_count"),
    )
    store.write(snapshot)
    return snapshot


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Ingest a sanitized Options Copilot snapshot")
    parser.add_argument("--input", required=True, type=Path)
    args = parser.parse_args(argv)
    config = OptionsCopilotConfig.from_env()
    config.validate()
    config.ensure_runtime_directories()
    try:
        payload = json.loads(args.input.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SystemExit("Snapshot input is unreadable") from exc
    if not isinstance(payload, Mapping):
        raise SystemExit("Snapshot input must be a JSON object")
    snapshot = ingest_payload(
        payload,
        store=ManagedSnapshotStore(config.data_dir / "runtime_snapshot.json"),
    )
    print(
        json.dumps(
            {
                "status": "INGESTED",
                "observed_at": snapshot.observed_at.isoformat(),
                "positions": len(snapshot.positions),
                "candidates": len(snapshot.candidates),
            },
            separators=(",", ":"),
        )
    )
    return 0


def _mapping(value: object, field: str) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{field} must be a mapping")
    return {str(key): item for key, item in value.items()}


def _mapping_sequence(value: object, field: str) -> tuple[dict[str, object], ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise ValueError(f"{field} must be a sequence")
    return tuple(_mapping(item, field) for item in value)


def _money(value: object, field: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field} must be a finite number") from exc
    if not result.is_finite():
        raise ValueError(f"{field} must be a finite number")
    return result


def _reject_secrets(value: object, path: str = "snapshot") -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            lowered = str(key).lower()
            if any(part in lowered for part in _FORBIDDEN_KEY_PARTS):
                raise ValueError(f"secret-like field is prohibited at {path}.{key}")
            _reject_secrets(item, f"{path}.{key}")
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for index, item in enumerate(value):
            _reject_secrets(item, f"{path}[{index}]")


if __name__ == "__main__":
    raise SystemExit(main())
