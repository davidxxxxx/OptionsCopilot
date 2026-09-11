"""Atomic cache for connector-ingested account, position, and quote state."""
from __future__ import annotations

import json
import os
import tempfile
import threading
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from types import MappingProxyType


@dataclass(frozen=True, slots=True)
class RuntimeSnapshot:
    observed_at: datetime
    source: str
    account: Mapping[str, object]
    positions: tuple[Mapping[str, object], ...]
    candidates: tuple[Mapping[str, object], ...]
    warnings: tuple[str, ...]
    campaign: Mapping[str, object]
    broker_snapshot_complete: bool = False
    working_order_count: int | None = None
    unsubmitted_instruction_count: int | None = None

    def __post_init__(self) -> None:
        if self.observed_at.tzinfo is None or self.observed_at.utcoffset() is None:
            raise ValueError("observed_at must be timezone-aware")
        if not self.source.strip():
            raise ValueError("snapshot source is required")
        if not isinstance(self.broker_snapshot_complete, bool):
            raise ValueError("broker_snapshot_complete must be a boolean")
        for field, value in (
            ("working_order_count", self.working_order_count),
            ("unsubmitted_instruction_count", self.unsubmitted_instruction_count),
        ):
            if value is not None and (
                isinstance(value, bool) or not isinstance(value, int) or value < 0
            ):
                raise ValueError(f"{field} must be a nonnegative integer or null")
        object.__setattr__(self, "account", MappingProxyType(dict(self.account)))
        object.__setattr__(
            self,
            "positions",
            tuple(MappingProxyType(dict(item)) for item in self.positions),
        )
        object.__setattr__(
            self,
            "candidates",
            tuple(MappingProxyType(dict(item)) for item in self.candidates[:10]),
        )
        object.__setattr__(self, "warnings", tuple(str(item) for item in self.warnings))
        object.__setattr__(self, "campaign", MappingProxyType(dict(self.campaign)))

    def as_dict(self) -> dict[str, object]:
        return {
            "version": 1,
            "observed_at": self.observed_at.isoformat(),
            "source": self.source,
            "account": dict(self.account),
            "positions": [dict(item) for item in self.positions],
            "candidates": [dict(item) for item in self.candidates],
            "warnings": list(self.warnings),
            "campaign": dict(self.campaign),
            "broker_snapshot_complete": self.broker_snapshot_complete,
            "working_order_count": self.working_order_count,
            "unsubmitted_instruction_count": self.unsubmitted_instruction_count,
        }


class ManagedSnapshotStore:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._lock = threading.RLock()

    def read(self) -> RuntimeSnapshot | None:
        with self._lock:
            if not self.path.exists():
                return None
            try:
                payload = json.loads(self.path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                raise RuntimeError("runtime snapshot is unreadable") from exc
        if not isinstance(payload, dict) or payload.get("version") != 1:
            raise RuntimeError("unsupported runtime snapshot format")
        try:
            observed_at = datetime.fromisoformat(str(payload["observed_at"]))
            account = _mapping(payload.get("account"), "account")
            campaign = _mapping(payload.get("campaign"), "campaign")
            positions = _mapping_sequence(payload.get("positions"), "positions")
            candidates = _mapping_sequence(payload.get("candidates"), "candidates")
            warnings_raw = payload.get("warnings", [])
            if not isinstance(warnings_raw, list):
                raise TypeError("warnings must be a list")
            return RuntimeSnapshot(
                observed_at=observed_at,
                source=str(payload["source"]),
                account=account,
                positions=positions,
                candidates=candidates,
                warnings=tuple(str(value) for value in warnings_raw),
                campaign=campaign,
                broker_snapshot_complete=payload.get("broker_snapshot_complete", False),
                working_order_count=payload.get("working_order_count"),
                unsubmitted_instruction_count=payload.get(
                    "unsubmitted_instruction_count"
                ),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise RuntimeError("runtime snapshot has invalid content") from exc

    def write(self, snapshot: RuntimeSnapshot) -> None:
        body = json.dumps(
            snapshot.as_dict(),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
        with self._lock:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            handle, temporary = tempfile.mkstemp(
                prefix=f".{self.path.name}.", suffix=".tmp", dir=self.path.parent
            )
            try:
                with os.fdopen(handle, "w", encoding="utf-8", newline="\n") as stream:
                    stream.write(body)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(temporary, self.path)
            finally:
                if os.path.exists(temporary):
                    os.unlink(temporary)


def _mapping(value: object, field: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{field} must be a mapping")
    return {str(key): item for key, item in value.items()}


def _mapping_sequence(value: object, field: str) -> tuple[Mapping[str, object], ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise TypeError(f"{field} must be a sequence")
    return tuple(_mapping(item, field) for item in value)
