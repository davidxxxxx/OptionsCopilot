"""Read-only command line surface for the durable scan scheduler.

The CLI consumes an explicit broker-calendar fixture and a detached decision
payload.  It has no broker client, approval, bridge, creator, instruction, or
order dependency.  Production wiring injects its own ``DecisionPipelinePort``
through :mod:`options_copilot.runtime`.
"""

from __future__ import annotations

import argparse
from collections.abc import Mapping, Sequence
from datetime import date, datetime, timezone
import json
from pathlib import Path
import sys
import time

from options_copilot.market.session_calendar import (
    UsOptionsCalendarSnapshot,
    UsOptionsSessionCalendar,
)

from .scheduler import ScanRunStore, ScanSlot
from .service import DecisionPipelinePort, ScanSchedulerService


class _DetachedDecisionPipeline(DecisionPipelinePort):
    """A fixed read model used only by this offline CLI."""

    def __init__(self, payload: Mapping[str, object]) -> None:
        self._payload = dict(payload)

    def run_slot(self, scan_run_id: str, slot_at: datetime) -> Mapping[str, object]:
        return {
            **self._payload,
            "scan_run_id": scan_run_id,
            "slot_at": slot_at.isoformat(),
            "review_only": True,
            "direct_order_submission": False,
        }


class _CalendarFileProvider:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def snapshot(self, *, now: datetime) -> UsOptionsCalendarSnapshot:
        raw = _read_object(self.path)
        return UsOptionsSessionCalendar().normalize(
            liquid_hours=_required_text(raw, "liquid_hours"),
            trading_hours=_required_text(raw, "trading_hours"),
            timezone_id=_required_text(raw, "timezone_id"),
            observed_at=_timestamp(raw.get("observed_at"), "observed_at"),
            source=_required_text(raw, "source"),
            now=now,
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Inspect or run the read-only Options Copilot scan scheduler.",
    )
    parser.add_argument("--db", required=True, help="Scan-run SQLite database")
    parser.add_argument(
        "--pipeline-version",
        required=True,
        help="Immutable decision-pipeline version bound to each slot",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    tick = subparsers.add_parser("tick", help="Evaluate one calendar-gated tick")
    _add_tick_arguments(tick)

    serve = subparsers.add_parser(
        "serve",
        help="Run calendar-gated read-only ticks until interrupted",
    )
    _add_tick_arguments(serve)
    serve.add_argument(
        "--heartbeat-seconds",
        type=float,
        default=30.0,
        help="Positive loop interval (default: 30)",
    )

    inspect_slot = subparsers.add_parser(
        "inspect-slot",
        help="Inspect the durable record for one exact slot",
    )
    inspect_slot.add_argument("--trading-date", required=True, help="YYYY-MM-DD")
    inspect_slot.add_argument(
        "--slot-at",
        required=True,
        help="Timezone-aware ISO-8601 timestamp",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        with ScanRunStore(args.db) as store:
            if args.command == "inspect-slot":
                return _inspect_slot(store, args)
            provider = _CalendarFileProvider(args.calendar)
            pipeline = _DetachedDecisionPipeline(_decision_payload(args.decision))
            service = ScanSchedulerService(
                store,
                pipeline,
                pipeline_version=args.pipeline_version,
                owner=args.owner,
            )
            if args.command == "tick":
                now = _optional_timestamp(args.now) or datetime.now(timezone.utc)
                result = service.tick(provider.snapshot(now=now), now=now)
                _emit(result.__dict__ if hasattr(result, "__dict__") else {
                    "scan_run_id": result.scan_run_id,
                    "duplicate_reason": result.duplicate_reason,
                    "result_hash": result.result_hash,
                    "status": result.status,
                })
                return 0
            return _serve(service, provider, args)
    except (OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
        sys.stderr.write(f"NO_TRADE: SCANNER_CLI_INPUT_INVALID: {exc}\n")
        return 2


def _add_tick_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--calendar", required=True, help="Broker-calendar JSON")
    parser.add_argument(
        "--decision",
        help="Detached decision JSON; default is explicit NO_TRADE",
    )
    parser.add_argument("--owner", help="Durable lease owner")
    parser.add_argument("--now", help="Timezone-aware ISO-8601 evaluation time")


def _serve(
    service: ScanSchedulerService,
    provider: _CalendarFileProvider,
    args: argparse.Namespace,
) -> int:
    heartbeat = float(args.heartbeat_seconds)
    if heartbeat <= 0:
        raise ValueError("heartbeat-seconds must be positive")
    fixed_now = _optional_timestamp(args.now)
    try:
        while True:
            now = fixed_now or datetime.now(timezone.utc)
            result = service.tick(provider.snapshot(now=now), now=now)
            _emit(
                {
                    "scan_run_id": result.scan_run_id,
                    "duplicate_reason": result.duplicate_reason,
                    "result_hash": result.result_hash,
                    "status": result.status,
                }
            )
            time.sleep(heartbeat)
    except KeyboardInterrupt:
        return 0


def _inspect_slot(store: ScanRunStore, args: argparse.Namespace) -> int:
    trading_date = date.fromisoformat(args.trading_date)
    slot_at = _timestamp(args.slot_at, "slot_at")
    runs = store.runs_for_slot(
        ScanSlot(trading_date=trading_date, slot_at=slot_at),
        pipeline_version=args.pipeline_version,
    )
    _emit(
        {
            "status": "FOUND" if runs else "NOT_FOUND",
            "trading_date": trading_date.isoformat(),
            "slot_at": slot_at.isoformat(),
            "pipeline_version": args.pipeline_version,
            "runs": [
                {
                    "scan_run_id": run.scan_run_id,
                    "status": run.status,
                    "owner": run.owner,
                    "duplicate_reason": run.duplicate_reason,
                    "result_hash": run.result_hash,
                    "failure_reason": run.failure_reason,
                }
                for run in runs
            ],
            "review_only": True,
            "direct_order_submission": False,
        }
    )
    return 0


def _decision_payload(path: str | None) -> Mapping[str, object]:
    if path is None:
        return {
            "decision": "NO_TRADE",
            "reason": "CLI_PIPELINE_NOT_CONFIGURED",
        }
    return _read_object(path)


def _read_object(path: str | Path) -> Mapping[str, object]:
    value = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(value, Mapping):
        raise ValueError(f"{path} must contain a JSON object")
    return value


def _required_text(value: Mapping[str, object], field: str) -> str:
    if field not in value:
        raise ValueError(f"{field} is required")
    result = str(value[field]).strip()
    if not result:
        raise ValueError(f"{field} cannot be blank")
    return result


def _optional_timestamp(value: object) -> datetime | None:
    return None if value is None else _timestamp(value, "now")


def _timestamp(value: object, field: str) -> datetime:
    if isinstance(value, datetime):
        result = value
    elif isinstance(value, str):
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    else:
        raise ValueError(f"{field} must be a timestamp")
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return result


def _emit(payload: Mapping[str, object]) -> None:
    sys.stdout.write(
        json.dumps(
            dict(payload),
            ensure_ascii=False,
            sort_keys=True,
            allow_nan=False,
            default=str,
        )
        + "\n"
    )


if __name__ == "__main__":  # pragma: no cover - exercised through python -m
    raise SystemExit(main())


__all__ = ["build_parser", "main"]
