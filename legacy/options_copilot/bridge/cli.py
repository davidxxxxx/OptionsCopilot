"""JSON stdin/stdout CLI for the local Codex bridge state machine."""
from __future__ import annotations

import argparse
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
import json
from pathlib import Path
import sys
from typing import TextIO

from options_copilot.approval import ProposalApprovalStore

from .coordinator import LocalCodexBridgeCoordinator
from .store import BridgeRecord, CodexBridgeStore


def execute_request(
    coordinator: LocalCodexBridgeCoordinator,
    request: Mapping[str, object],
) -> dict[str, object]:
    """Execute one already-decoded request without reading environment secrets."""

    if not isinstance(request, Mapping):
        raise ValueError("JSON request must be an object")
    raw_command = request.get("command")
    if not isinstance(raw_command, str) or not raw_command.strip():
        raise ValueError("command must be a nonblank string")
    command = raw_command.strip().lower().replace("-", "_")
    approval_id = _required_text(request, "approval_id")

    if command == "claim":
        token = coordinator.claim(approval_id)
        return {
            "approval_id": approval_id,
            "status": "CLAIMED",
            "token": token,
            "automatic_retry_allowed": False,
        }
    if command in {"authorize", "authorize_and_reserve"}:
        token = _required_text(request, "token")
        snapshot = _required_mapping(request, "broker_snapshot")
        intent = _required_mapping(request, "instruction_intent")
        record = coordinator.authorize(
            approval_id,
            token,
            snapshot,
            intent,
        )
        # Commit the one-shot reservation before returning any payload an
        # external Codex process could use.  A crash before this point returns
        # no payload and therefore cannot legitimately cross the boundary.
        record = coordinator.reserve_external_call(approval_id, token)
        payload = _record_payload(record)
        payload.update(
            {
                "idempotency_key": approval_id,
                "review_only": True,
                "proposal": record.proposal,
                "instruction_intent": record.instruction_intent,
            }
        )
        return payload
    if command == "complete":
        token = _required_text(request, "token")
        result = _required_mapping(request, "execution_result")
        return _record_payload(coordinator.complete(approval_id, token, result))
    if command == "fail":
        token = _required_text(request, "token")
        reason = _required_text(request, "reason")
        unknown = request.get("unknown_outcome", False)
        if not isinstance(unknown, bool):
            raise ValueError("unknown_outcome must be a boolean")
        return _record_payload(
            coordinator.fail(
                approval_id,
                token,
                reason,
                unknown_outcome=unknown,
            )
        )
    if command == "expire_stranded_claim":
        return _record_payload(coordinator.expire_stranded_claim(approval_id))
    if command == "status":
        status = coordinator.status(approval_id)
        if status is None:
            return {
                "approval_id": approval_id,
                "status": "NOT_FOUND",
                "automatic_retry_allowed": False,
            }
        return dict(status)
    raise ValueError(f"unsupported command: {command}")


def main(
    argv: Sequence[str] | None = None,
    *,
    stdin: TextIO | None = None,
    stdout: TextIO | None = None,
) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m options_copilot.bridge",
        description="Local review-only Codex bridge (JSON stdin/stdout)",
    )
    parser.add_argument("--approval-db", required=True, type=Path)
    parser.add_argument("--bridge-db", required=True, type=Path)
    args = parser.parse_args(list(argv) if argv is not None else None)
    input_stream = stdin or sys.stdin
    output_stream = stdout or sys.stdout

    approvals: ProposalApprovalStore | None = None
    bridge: CodexBridgeStore | None = None
    try:
        raw = input_stream.read()
        request = json.loads(raw, parse_float=Decimal)
        if not isinstance(request, Mapping):
            raise ValueError("JSON request must be an object")
        approvals = ProposalApprovalStore(args.approval_db)
        bridge = CodexBridgeStore(args.bridge_db, approvals)
        coordinator = LocalCodexBridgeCoordinator(bridge)
        response: dict[str, object] = {
            "ok": True,
            "result": execute_request(coordinator, request),
        }
        exit_code = 0
    except Exception as exc:
        response = {
            "ok": False,
            "error": {
                "type": type(exc).__name__,
                "message": str(exc),
            },
        }
        exit_code = 2
    finally:
        if bridge is not None:
            bridge.close()
        if approvals is not None:
            approvals.close()

    output_stream.write(
        json.dumps(
            _json_safe(response),
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    )
    output_stream.flush()
    return exit_code


def _record_payload(record: BridgeRecord) -> dict[str, object]:
    return {
        "approval_id": record.approval_id,
        "status": record.status.value,
        "record": record.as_dict(),
        "automatic_retry_allowed": False,
    }


def _required_text(mapping: Mapping[str, object], key: str) -> str:
    value = mapping.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{key} must be a nonblank string")
    return value.strip()


def _required_mapping(
    mapping: Mapping[str, object], key: str
) -> Mapping[str, object]:
    value = mapping.get(key)
    if not isinstance(value, Mapping):
        raise ValueError(f"{key} must be an object")
    return value


def _json_safe(value: object) -> object:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime):
        return value.isoformat(timespec="microseconds")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Enum):
        return _json_safe(value.value)
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(
        value, (str, bytes, bytearray, memoryview)
    ):
        return [_json_safe(item) for item in value]
    raise TypeError(f"cannot encode JSON value of type {type(value).__name__}")


if __name__ == "__main__":  # pragma: no cover - exercised through package entrypoint
    raise SystemExit(main())


__all__ = ["execute_request", "main"]
