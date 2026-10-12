"""Validated immutable reference to one durable equity-pool snapshot."""

from __future__ import annotations

from collections.abc import Mapping

from options_copilot.storage.canonical import freeze_json


_HASH_FIELDS = (
    "snapshot_id", "snapshot_hash", "input_manifest_hash", "rows_hash",
    "policy_hash", "taxonomy_hash", "scoring_hash",
)


def normalize_equity_pool_reference(value: object) -> Mapping[str, object] | None:
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise TypeError("equity_pool_reference must be a mapping")
    frozen = freeze_json(value)
    if not isinstance(frozen, Mapping):
        raise TypeError("equity_pool_reference must be canonical")
    if frozen.get("schema") != "options_copilot.equity_pool_reference.v1":
        raise ValueError("equity_pool_reference schema is invalid")
    for field in _HASH_FIELDS:
        digest = frozen.get(field)
        if not isinstance(digest, str) or len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
            raise ValueError(f"equity_pool_reference {field} is invalid")
    selected = frozen.get("selected_symbols")
    if not isinstance(selected, tuple) or any(not isinstance(item, str) or not item for item in selected):
        raise ValueError("equity_pool_reference selected_symbols is invalid")
    for field in ("discovery_count", "selected_count", "excluded_count"):
        number = frozen.get(field)
        if isinstance(number, bool) or not isinstance(number, int) or number < 0:
            raise ValueError(f"equity_pool_reference {field} is invalid")
    if frozen["selected_count"] != len(selected):
        raise ValueError("equity_pool_reference selected count mismatch")
    discovered = frozen.get("discovered_symbols", selected)
    if (
        not isinstance(discovered, tuple)
        or any(not isinstance(item, str) or not item for item in discovered)
        or len(set(discovered)) != len(discovered)
        or any(item not in discovered for item in selected)
    ):
        raise ValueError("equity_pool_reference discovered_symbols is invalid")
    if frozen["discovery_count"] != len(discovered):
        raise ValueError("equity_pool_reference discovery count mismatch")
    stats = frozen.get("exclusion_stats")
    if not isinstance(stats, Mapping):
        raise ValueError("equity_pool_reference exclusion_stats is invalid")
    return frozen


__all__ = ["normalize_equity_pool_reference"]
