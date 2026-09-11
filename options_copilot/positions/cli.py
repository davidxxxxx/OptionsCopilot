"""Offline dry-run CLI for defined-risk management previews.

The command only reads explicit JSON/contract files and writes a detached
preview.  It intentionally has no broker, approval, bridge, creator,
instruction, or order dependency.
"""

from __future__ import annotations

import argparse
from collections.abc import Mapping, Sequence
from datetime import datetime
from decimal import Decimal
import json
from pathlib import Path
import sys

from options_copilot.gateway.broker_snapshot import (
    AtomicBrokerSnapshot,
    BrokerSnapshotStatus,
    SecDefEvidence,
    StateComponentEvidence,
)
from options_copilot.gateway.ibkr_readonly import BatchedOptionQuote, QuoteBatchStatus
from options_copilot.governance.contracts import SignedContract, load_contract
from options_copilot.storage.canonical import freeze_json, thaw_json

from .generator import ManagementCandidateGenerator


def run_dry_run(
    generator: ManagementCandidateGenerator,
    snapshot: AtomicBrokerSnapshot,
    exit_contract: Mapping[str, object],
    cost_contract: SignedContract | Mapping[str, object],
) -> dict[str, object]:
    """Generate one detached preview with explicit zero side-effect counters."""

    if not isinstance(generator, ManagementCandidateGenerator):
        raise TypeError("generator must be a ManagementCandidateGenerator")
    result = generator.generate(snapshot, exit_contract, cost_contract)
    return {
        **result.as_dict(),
        "mode": "DRY_RUN_PREVIEW_ONLY",
        "review_only": True,
        "approval_enabled": False,
        "direct_order_submission": False,
        "external_attempt_count": 0,
        "instruction_count": 0,
    }


def write_dry_run_preview(
    preview: Mapping[str, object],
    output_path: str | Path,
) -> Path:
    """Create a new preview file without overwriting an existing artifact."""

    if not isinstance(preview, Mapping):
        raise TypeError("preview must be a mapping")
    path = Path(output_path)
    if not path.parent.exists() or not path.parent.is_dir():
        raise ValueError("preview output directory does not exist")
    rendered = json.dumps(
        dict(preview),
        ensure_ascii=False,
        sort_keys=True,
        indent=2,
        allow_nan=False,
    )
    with path.open("x", encoding="utf-8", newline="\n") as handle:
        handle.write(rendered)
        handle.write("\n")
    return path


def load_snapshot_json(path: str | Path) -> AtomicBrokerSnapshot:
    """Rebuild an AtomicBrokerSnapshot from its JSON hash payload plus hash."""

    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    # JSON arrays are mutable lists while thaw_json intentionally traverses
    # immutable tuples.  Freeze first so tagged Decimal/date values nested in
    # arrays are decoded as well.
    value = thaw_json(freeze_json(raw))
    if not isinstance(value, Mapping):
        raise ValueError("snapshot JSON must be an object")
    states_raw = _mapping(value.get("state_evidence"), "state_evidence")
    state_evidence = {
        str(name): StateComponentEvidence(
            name=str(_required(row, "name")),
            known=_boolean(_required(row, "known"), f"{name}.known"),
            count=_optional_integer(row.get("count"), f"{name}.count"),
            pre_hash=_optional_string(row.get("pre_hash")),
            post_hash=_optional_string(row.get("post_hash")),
            stable=_boolean(_required(row, "stable"), f"{name}.stable"),
            state=row.get("state"),
        )
        for name, item in states_raw.items()
        for row in (_mapping(item, f"state_evidence.{name}"),)
    }
    secdefs = tuple(
        SecDefEvidence(
            contract_id=_integer(_required(row, "contract_id"), "contract_id"),
            pre_identity=_optional_mapping(row.get("pre_identity")),
            post_identity=_optional_mapping(row.get("post_identity")),
            pre_hash=_optional_string(row.get("pre_hash")),
            post_hash=_optional_string(row.get("post_hash")),
            stable=_boolean(_required(row, "stable"), "secdef.stable"),
            standard_contract=_boolean(
                _required(row, "standard_contract"),
                "secdef.standard_contract",
            ),
            adjusted=_boolean(_required(row, "adjusted"), "secdef.adjusted"),
            pre_source=_optional_string(row.get("pre_source")),
            post_source=_optional_string(row.get("post_source")),
        )
        for item in _sequence(value.get("secdef_evidence"), "secdef_evidence")
        for row in (_mapping(item, "secdef"),)
    )
    quotes = tuple(
        BatchedOptionQuote(
            contract_id=_integer(_required(row, "contract_id"), "quote.contract_id"),
            batch_id=str(_required(row, "batch_id")),
            request_id=str(_required(row, "request_id")),
            requested_at=_timestamp(_required(row, "requested_at"), "quote.requested_at"),
            observed_at=_timestamp(_required(row, "observed_at"), "quote.observed_at"),
            completed_at=_timestamp(_required(row, "completed_at"), "quote.completed_at"),
            source=str(_required(row, "source")),
            bid=_optional_decimal(row.get("bid"), "quote.bid"),
            ask=_optional_decimal(row.get("ask"), "quote.ask"),
            last=_optional_decimal(row.get("last"), "quote.last"),
            close=_optional_decimal(row.get("close"), "quote.close"),
            exchange_time=(
                None
                if row.get("exchange_time") is None
                else _timestamp(row["exchange_time"], "quote.exchange_time")
            ),
            volume=_optional_integer(row.get("volume"), "quote.volume"),
            open_interest=_optional_integer(
                row.get("open_interest"),
                "quote.open_interest",
            ),
            implied_volatility=_optional_decimal(
                row.get("implied_volatility"),
                "quote.implied_volatility",
            ),
            delta=_optional_decimal(row.get("delta"), "quote.delta"),
            gamma=_optional_decimal(row.get("gamma"), "quote.gamma"),
            theta=_optional_decimal(row.get("theta"), "quote.theta"),
            vega=_optional_decimal(row.get("vega"), "quote.vega"),
            market_data_type=_optional_integer(
                row.get("market_data_type"),
                "quote.market_data_type",
            ),
        )
        for item in _sequence(value.get("quotes"), "quotes")
        for row in (_mapping(item, "quote"),)
    )
    quote_status = value.get("quote_batch_status")
    return AtomicBrokerSnapshot(
        built_at=_timestamp(_required(value, "built_at"), "built_at"),
        status=BrokerSnapshotStatus(str(_required(value, "status"))),
        reason_codes=tuple(
            str(item)
            for item in _sequence(value.get("reason_codes"), "reason_codes")
        ),
        state_evidence=state_evidence,
        secdef_evidence=secdefs,
        quote_batch_id=_optional_string(value.get("quote_batch_id")),
        quote_batch_status=(
            None if quote_status is None else QuoteBatchStatus(str(quote_status))
        ),
        quote_batch_source=_optional_string(value.get("quote_batch_source")),
        quote_batch_requested_at=(
            None
            if value.get("quote_batch_requested_at") is None
            else _timestamp(value["quote_batch_requested_at"], "quote_batch_requested_at")
        ),
        quote_batch_completed_at=(
            None
            if value.get("quote_batch_completed_at") is None
            else _timestamp(value["quote_batch_completed_at"], "quote_batch_completed_at")
        ),
        quotes=quotes,
        oldest_quote_age_seconds=_optional_decimal(
            value.get("oldest_quote_age_seconds"),
            "oldest_quote_age_seconds",
        ),
        maximum_leg_skew_seconds=_optional_decimal(
            value.get("maximum_leg_skew_seconds"),
            "maximum_leg_skew_seconds",
        ),
        snapshot_hash=str(_required(value, "snapshot_hash")),
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Generate an offline, preview-only defined-risk management report."
        ),
    )
    parser.add_argument("--snapshot", required=True, help="Atomic snapshot JSON")
    parser.add_argument("--exit-contract", required=True, help="Exit contract JSON")
    parser.add_argument("--cost-contract", required=True, help="Signed cost contract JSON")
    parser.add_argument(
        "--output",
        default="-",
        help="New preview path, or '-' for stdout (default)",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        snapshot = load_snapshot_json(args.snapshot)
        exit_contract = json.loads(Path(args.exit_contract).read_text(encoding="utf-8"))
        if not isinstance(exit_contract, Mapping):
            raise ValueError("exit contract JSON must be an object")
        cost_contract = load_contract(args.cost_contract, as_of=snapshot.built_at)
        preview = run_dry_run(
            ManagementCandidateGenerator(),
            snapshot,
            exit_contract,
            cost_contract,
        )
        if args.output == "-":
            sys.stdout.write(
                json.dumps(
                    preview,
                    ensure_ascii=False,
                    sort_keys=True,
                    indent=2,
                    allow_nan=False,
                )
                + "\n"
            )
        else:
            write_dry_run_preview(preview, args.output)
        return 0
    except (OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
        sys.stderr.write(f"NO_TRADE: DRY_RUN_INPUT_INVALID: {exc}\n")
        return 2


def _mapping(value: object, field: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{field} must be an object")
    return value


def _optional_mapping(value: object) -> Mapping[str, object] | None:
    return None if value is None else _mapping(value, "optional mapping")


def _sequence(value: object, field: str) -> tuple[object, ...]:
    if not isinstance(value, Sequence) or isinstance(
        value,
        (str, bytes, bytearray, memoryview),
    ):
        raise ValueError(f"{field} must be an array")
    return tuple(value)


def _required(value: Mapping[str, object], field: str) -> object:
    if field not in value:
        raise ValueError(f"{field} is required")
    return value[field]


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


def _integer(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field} must be an integer")
    return value


def _optional_integer(value: object, field: str) -> int | None:
    return None if value is None else _integer(value, field)


def _boolean(value: object, field: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{field} must be boolean")
    return value


def _optional_string(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("optional string field is invalid")
    return value


def _optional_decimal(value: object, field: str) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        result = value
    elif isinstance(value, (int, str)) and not isinstance(value, bool):
        result = Decimal(str(value))
    else:
        raise ValueError(f"{field} must be a Decimal-compatible value")
    if not result.is_finite():
        raise ValueError(f"{field} must be finite")
    return result


if __name__ == "__main__":  # pragma: no cover - exercised via python -m
    raise SystemExit(main())


__all__ = [
    "load_snapshot_json",
    "main",
    "run_dry_run",
    "write_dry_run_preview",
]
