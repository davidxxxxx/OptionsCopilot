"""Read-only provider probe with one canonical, redacted checkpoint.

The probe calls only the providers' existing observation methods. It records
fixed source states, counts, freshness, conflicts, and bounded byte counters;
it never serializes request URLs, headers, credentials, article text, or raw
transport exceptions. Jin10 remains uncalled unless both the fixed transport
contract and the current credential generation are verified.
"""

from __future__ import annotations

import argparse
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
import json
import os
from pathlib import Path
import re
import stat
import sys

from options_copilot.config import OptionsCopilotConfig
from options_copilot.security.dpapi import DPAPISecretStore
from options_copilot.security.jin10_credentials import (
    jin10_rotation_evidence_dir,
    resolve_jin10_credential,
)
from options_copilot.security.local_api_keys import (
    LocalApiKeyStore,
    LocalJin10EnvelopeReader,
    local_api_key_path,
)
from options_copilot.storage.canonical import canonical_hash, canonical_json, utc_datetime

from .events import AlphaVantageNewsProvider, FinnhubEventProvider
from .jin10 import Jin10EventProvider
from .jin10_mcp import Jin10McpHttpClient
from .nasdaq_earnings import NasdaqEarningsProvider
from .official import CompanyIrEventProvider
from .sec_current import SecCurrent8KProvider


PROVIDER_NAMES = (
    "sec",
    "nasdaq",
    "company_ir",
    "finnhub",
    "alpha_vantage",
    "jin10",
)
_NORMALIZED_STATUS_VALUES = frozenset(
    {"READY", "DEGRADED", "UNAVAILABLE", "UNCONFIGURED", "LIMITED", "STALE"}
)
_FORBIDDEN_KEY = re.compile(
    r"(?:authorization|cookie|headers?|password|secret|token|api[_-]?key|"
    r"credential|account|broker|position|creator|raw[_-]?(?:body|error)|"
    r"exception|redirect|(?:^|_)url(?:$|_)|(?:^|_)path(?:$|_)|query|tool|"
    r"order|instruction)",
    re.IGNORECASE,
)
_FORBIDDEN_VALUE = re.compile(
    r"(?:bearer\s|apikey=|api_key=|x-finnhub-token|authorization:|"
    r"https?://[^\s?]+\?|[a-z]:[\\/]|sentinel-provider-(?:secret|private)|"
    r"raw[_-]?(?:body|error)|redirect[_-]?target)",
    re.IGNORECASE,
)
_SAFE_AUTHORITY_KEYS = frozenset(
    {
        "approval_eligible",
        "instruction_creation_allowed",
        "order_submission_allowed",
    }
)
_REQUEST_METHODS = {
    "sec": ("GET",),
    "nasdaq": ("GET",),
    "company_ir": ("GET",),
    "finnhub": ("GET",),
    "alpha_vantage": ("GET",),
    "jin10": ("POST", "DELETE"),
}


class ProviderProbeError(RuntimeError):
    """The local probe contract failed before safe evidence was produced."""


@dataclass(frozen=True, slots=True)
class _ProviderBinding:
    provider: object
    configuration_status: str
    call_enabled: bool = True


def run_probe(
    *,
    provider_names: Sequence[str],
    symbols: Sequence[str],
    evidence_dir: str | Path,
    limit: int = 50,
    provider_map: Mapping[str, object] | None = None,
    clock: Callable[[], datetime] | None = None,
) -> dict[str, object]:
    """Probe bounded read-only provider methods and write one checkpoint."""

    names = _provider_names(provider_names)
    tickers = _symbols(symbols)
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 200:
        raise ProviderProbeError("provider limit is invalid")
    now = utc_datetime(
        (clock or (lambda: datetime.now(timezone.utc)))(),
        field="provider probe clock",
    )
    config = OptionsCopilotConfig.from_env()
    config.validate()
    directory, evidence_root = _resolve_evidence_directory(
        config=config,
        requested=evidence_dir,
    )
    bindings = (
        _default_provider_map(config=config, clock=clock)
        if provider_map is None
        else {
            name: _ProviderBinding(provider_map[name], "INJECTED")
            for name in PROVIDER_NAMES
            if name in provider_map
        }
    )

    source_rows: list[dict[str, object]] = []
    conflicts: list[dict[str, object]] = []
    for name in PROVIDER_NAMES:
        binding = bindings.get(name)
        row, conflict_row = _probe_one(
            name,
            binding,
            requested=name in names,
            symbols=tickers,
            limit=limit,
            observed_at=now,
        )
        source_rows.append(row)
        if conflict_row is not None:
            conflicts.append(conflict_row)

    degraded = any(row["status"] != "READY" for row in source_rows)
    checkpoint: dict[str, object] = {
        "schema": "options_copilot.provider_probe_checkpoint.v2",
        "generated_at": now.isoformat(),
        "status": "DEGRADED" if degraded else "READY",
        "decision": "NO_TRADE" if degraded else "OBSERVATION_ONLY",
        "decision_authority": "SUPPORTING_ONLY",
        "read_only": True,
        "instruction_creation_allowed": False,
        "approval_eligible": False,
        "order_submission_allowed": False,
        "contract": {
            "provider_names": list(PROVIDER_NAMES),
            "requested_symbols": list(tickers),
            "limit": limit,
            "read_only": True,
        },
        "sources": source_rows,
        "conflicts": conflicts,
        "redaction": {"status": "PASS", "finding_count": 0},
    }
    _assert_redacted(checkpoint)
    checkpoint["canonical_sha256"] = canonical_hash(checkpoint)
    _assert_redacted(checkpoint)
    if checkpoint["canonical_sha256"] != canonical_hash(
        {key: value for key, value in checkpoint.items() if key != "canonical_sha256"}
    ):
        raise ProviderProbeError("provider checkpoint canonical hash failed")
    _write_checkpoint_exclusive(
        directory,
        evidence_root=evidence_root,
        document=checkpoint,
    )
    return checkpoint


def _default_provider_map(
    *,
    config: OptionsCopilotConfig,
    clock: Callable[[], datetime] | None,
) -> dict[str, _ProviderBinding]:
    secrets = LocalApiKeyStore(local_api_key_path(config.data_dir))
    jin10_resolution = resolve_jin10_credential(
        LocalJin10EnvelopeReader(
            secrets,
            DPAPISecretStore(config.secrets_path),
        ),
        jin10_rotation_evidence_dir(config.data_dir),
    )

    def configured(name: str) -> str:
        try:
            return "CONFIGURED" if secrets.contains(name) else "NOT_CONFIGURED"
        except Exception:
            return "UNKNOWN"

    return {
        "sec": _ProviderBinding(SecCurrent8KProvider(now=clock), "PUBLIC_SOURCE"),
        "nasdaq": _ProviderBinding(
            NasdaqEarningsProvider(now=clock),
            "PUBLIC_SOURCE",
        ),
        "company_ir": _ProviderBinding(
            CompanyIrEventProvider(now=clock),
            "UNCONFIGURED",
        ),
        "finnhub": _ProviderBinding(
            FinnhubEventProvider(secrets, now=clock),
            configured("FINNHUB_API_KEY"),
        ),
        "alpha_vantage": _ProviderBinding(
            AlphaVantageNewsProvider(secrets, now=clock),
            configured("ALPHA_VANTAGE_API_KEY"),
        ),
        "jin10": _ProviderBinding(
            Jin10EventProvider(
                jin10_resolution.secret_store or secrets,
                mcp_client=Jin10McpHttpClient(),
                now=clock,
            ),
            jin10_resolution.status,
            call_enabled=jin10_resolution.secret_store is not None,
        ),
    }


def _probe_one(
    name: str,
    binding: _ProviderBinding | None,
    *,
    requested: bool,
    symbols: tuple[str, ...],
    limit: int,
    observed_at: datetime,
) -> tuple[dict[str, object], dict[str, object] | None]:
    events: tuple[object, ...] = ()
    snapshot = _health_snapshot(binding)
    configured = _configured(binding, snapshot)
    verified = binding is not None and (
        name != "jin10"
        or getattr(binding.provider, "transport_verified", False) is True
    )
    raw_status = _snapshot_value(snapshot, "status", "readiness")
    initial_status = _normalise_status(
        raw_status if raw_status is not None else getattr(
            None if binding is None else binding.provider,
            "health",
            None,
        )
    )
    request_bytes = _nonnegative_count(snapshot.get("request_bytes"))
    response_bytes = _nonnegative_count(snapshot.get("response_bytes"))
    call_succeeded = False
    if not requested:
        status, reason = "UNAVAILABLE", "NOT_REQUESTED"
    elif binding is None:
        status, reason = "UNAVAILABLE", "PROVIDER_NOT_COMPOSED"
    elif name == "jin10" and not (
        getattr(binding.provider, "transport_verified", False) is True
    ):
        status, reason = "UNAVAILABLE", "TRANSPORT_UNVERIFIED"
    elif name == "jin10" and not binding.call_enabled:
        status, reason = "UNCONFIGURED", "CREDENTIAL_NOT_ACTIVATED"
    elif name != "jin10" and (
        initial_status == "UNCONFIGURED" or not configured
    ):
        status = "UNCONFIGURED"
        reason = "UNCONFIGURED" if name == "company_ir" else "NOT_CONFIGURED"
    else:
        try:
            if name == "nasdaq" and callable(
                getattr(binding.provider, "calendar_payload", None)
            ):
                reader = getattr(binding.provider, "calendar_payload", None)
                assert callable(reader)
                payload = reader()
                if not isinstance(payload, Mapping):
                    raise ProviderProbeError("provider payload is invalid")
                raw_events = payload.get("events", ())
                events = _items(raw_events)
                status = _normalise_status(
                    payload.get("status", getattr(binding.provider, "health", None))
                )
            elif name == "nasdaq":
                reader = getattr(binding.provider, "earnings_calendar", None)
                if not callable(reader):
                    raise ProviderProbeError("provider contract is unavailable")
                day = observed_at.date()
                events = _items(reader(day, day))
                status = _normalise_status(getattr(binding.provider, "health", None))
            else:
                reader = getattr(binding.provider, "news", None)
                if not callable(reader):
                    raise ProviderProbeError("provider contract is unavailable")
                events = _items(reader(symbols, limit=limit))
                status = _normalise_status(getattr(binding.provider, "health", None))
            call_succeeded = status == "READY"
            snapshot = _health_snapshot(binding)
            snapshot_status = _snapshot_value(snapshot, "status", "readiness")
            if snapshot_status is not None:
                status = _normalise_status(snapshot_status)
            reason = _reason_code(name, status, snapshot=snapshot)
            request_bytes = _nonnegative_count(snapshot.get("request_bytes"))
            response_bytes = _nonnegative_count(snapshot.get("response_bytes"))
        except Exception:
            # Raw provider/transport exceptions may contain complete request
            # URLs or managed credentials.  Never preserve or render them.
            status, reason, events = "DEGRADED", "PROBE_FAILED", ()

    statuses = tuple(str(_field(item, "status") or "ACTIVE").upper() for item in events)
    conflict_hashes = tuple(
        sorted(
            {
                _identity_hash(item)
                for item, event_status in zip(events, statuses, strict=True)
                if event_status == "CONFLICTED"
            }
        )
    )
    freshness = sorted(
        value
        for item in events
        if (value := _event_time(item)) is not None
    )
    last_success_at = _safe_time(snapshot.get("last_success_at"))
    if last_success_at is None and call_succeeded:
        last_success_at = observed_at.isoformat()
    as_of = _safe_time(_snapshot_value(snapshot, "as_of", "asof"))
    if as_of is None and freshness:
        as_of = freshness[-1]
    observed = _safe_time(_snapshot_value(snapshot, "observed_at", "asof"))
    row = {
        "source_id": name,
        "configured": configured,
        "readiness": status,
        "status": status,
        "reason": reason,
        "observed_at": observed or observed_at.isoformat(),
        "as_of": as_of,
        "last_success_at": last_success_at,
        "freshness_age_seconds": _freshness_age_seconds(
            observed_at,
            last_success_at,
        ),
        "provenance": _provenance(name, events, snapshot),
        "pacing": _pacing(snapshot),
        "request_bytes": request_bytes,
        "response_bytes": response_bytes,
        "event_count": len(events),
        "active_count": sum(value == "ACTIVE" for value in statuses),
        "conflicted_count": sum(value == "CONFLICTED" for value in statuses),
        "request_methods": list(_REQUEST_METHODS[name]),
        "transport_verified": verified,
        "read_only": True,
        "decision_authority": "SUPPORTING_ONLY",
    }
    conflict = None
    if conflict_hashes:
        conflict = {
            "source_id": name,
            "identity_hashes": list(conflict_hashes),
        }
    return row, conflict


def _normalise_status(value: object) -> str:
    status = str(value or "DEGRADED").strip().upper()
    if status in _NORMALIZED_STATUS_VALUES:
        return status
    if status in {"NOT_CONFIGURED", "DISABLED"}:
        return "UNCONFIGURED"
    if status in {"RATE_LIMITED", "PACING_LIMITED", "PACING_UNVERIFIED"}:
        return "LIMITED"
    if status in {"DOWN", "FAILED", "TIMEOUT"}:
        return "UNAVAILABLE"
    return "DEGRADED"


def _reason_code(
    name: str,
    status: str,
    *,
    snapshot: Mapping[str, object],
) -> str | None:
    if status == "READY":
        return None
    raw = _snapshot_value(snapshot, "reason", "reason_code")
    checked = str(raw or "").strip().upper()
    allowlisted = {
        "BAD_JSON",
        "CREDENTIAL_NOT_ACTIVATED",
        "NOT_CONFIGURED",
        "NOT_OBSERVED",
        "PACING_LIMITED",
        "PACING_UNVERIFIED",
        "PROBE_FAILED",
        "RATE_LIMITED",
        "REQUEST_FAILED",
        "REQUEST_TIMEOUT",
        "TRANSPORT_UNVERIFIED",
        "UNCONFIGURED",
    }
    if checked in allowlisted:
        return checked
    if status == "UNCONFIGURED":
        return "UNCONFIGURED" if name == "company_ir" else "NOT_CONFIGURED"
    if status == "LIMITED":
        return "PACING_UNVERIFIED"
    return f"{name.upper()}_{status}"


def _health_snapshot(binding: _ProviderBinding | None) -> Mapping[str, object]:
    if binding is None:
        return {}
    reader = getattr(binding.provider, "health_snapshot", None)
    if not callable(reader):
        return {}
    try:
        value = reader()
    except Exception:
        return {}
    return value if isinstance(value, Mapping) else {}


def _snapshot_value(
    snapshot: Mapping[str, object],
    *names: str,
) -> object | None:
    for name in names:
        if name in snapshot:
            return snapshot.get(name)
    return None


def _configured(
    binding: _ProviderBinding | None,
    snapshot: Mapping[str, object],
) -> bool:
    if binding is None:
        return False
    declared = snapshot.get("configured")
    if isinstance(declared, bool):
        return declared
    state = str(
        _snapshot_value(snapshot, "status", "readiness")
        or getattr(binding.provider, "health", "")
    ).strip().upper()
    if state in {"UNCONFIGURED", "NOT_CONFIGURED"}:
        return False
    return binding.configuration_status not in {
        "NOT_CONFIGURED",
        "UNAVAILABLE",
        "UNCONFIGURED",
    }


def _nonnegative_count(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value


def _safe_time(value: object) -> str | None:
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            return None
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if not isinstance(value, str) or not value.strip() or len(value) > 64:
        return None
    text = value.strip()
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        try:
            return date.fromisoformat(text).isoformat()
        except ValueError:
            return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(timezone.utc).isoformat()


def _freshness_age_seconds(
    observed_at: datetime,
    last_success_at: str | None,
) -> int | None:
    if last_success_at is None:
        return None
    try:
        parsed = datetime.fromisoformat(last_success_at.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return max(0, int((observed_at - parsed.astimezone(timezone.utc)).total_seconds()))


def _provenance(
    name: str,
    events: Sequence[object],
    snapshot: Mapping[str, object],
) -> list[str]:
    identities = {f"source:{name}"}
    for event in events:
        try:
            identities.add(f"event:{_identity_hash(event)}")
        except (TypeError, ValueError):
            continue
    raw = snapshot.get("provenance")
    if isinstance(raw, Sequence) and not isinstance(
        raw,
        (str, bytes, bytearray, memoryview),
    ):
        for item in raw:
            if isinstance(item, str) and item.strip() and len(item) <= 240:
                identities.add(f"source-evidence:{canonical_hash(item.strip())}")
    return sorted(identities)


def _pacing(snapshot: Mapping[str, object]) -> str:
    value = str(snapshot.get("pacing") or "PACING_UNVERIFIED").strip().upper()
    allowed = {"READY", "PACING_VERIFIED", "PACING_UNVERIFIED"}
    return value if value in allowed else "PACING_UNVERIFIED"


def _field(value: object, name: str) -> object | None:
    if isinstance(value, Mapping):
        return value.get(name)
    return getattr(value, name, None)


def _event_time(value: object) -> str | None:
    for name in ("published_at", "scheduled_at", "event_date", "first_seen_at"):
        raw = _field(value, name)
        if isinstance(raw, datetime):
            if raw.tzinfo is None or raw.utcoffset() is None:
                return None
            return raw.astimezone(timezone.utc).isoformat()
        if isinstance(raw, date):
            return raw.isoformat()
        if isinstance(raw, str) and raw.strip():
            try:
                parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            except ValueError:
                try:
                    return date.fromisoformat(raw).isoformat()
                except ValueError:
                    continue
            if parsed.tzinfo is not None and parsed.utcoffset() is not None:
                return parsed.astimezone(timezone.utc).isoformat()
    return None


def _identity_hash(value: object) -> str:
    identity = _field(value, "identity_key")
    if identity is None:
        identity = (
            _field(value, "event_id"),
            _field(value, "source_id"),
            _field(value, "content_hash"),
        )
    return canonical_hash(identity)


def _items(value: object) -> tuple[object, ...]:
    if isinstance(value, Sequence) and not isinstance(
        value, (str, bytes, bytearray, memoryview)
    ):
        return tuple(value)
    raise ProviderProbeError("provider collection is invalid")


def _provider_names(values: Sequence[str]) -> tuple[str, ...]:
    result = tuple(str(value).strip().lower() for value in values)
    if not result or any(value not in PROVIDER_NAMES for value in result):
        raise ProviderProbeError("provider selection is invalid")
    if len(set(result)) != len(result):
        raise ProviderProbeError("provider selection contains duplicates")
    return result


def _symbols(values: Sequence[str]) -> tuple[str, ...]:
    result = tuple(dict.fromkeys(str(value).strip().upper() for value in values))
    if not result or len(result) > 40 or any(
        not re.fullmatch(r"[A-Z][A-Z0-9.\-]{0,9}", value) for value in result
    ):
        raise ProviderProbeError("symbol selection is invalid")
    return result


def _assert_redacted(value: object, *, path: str = "evidence") -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            if not isinstance(key, str):
                raise ProviderProbeError("provider evidence redaction failed")
            if _FORBIDDEN_KEY.search(key):
                if key not in _SAFE_AUTHORITY_KEYS or item is not False:
                    raise ProviderProbeError("provider evidence redaction failed")
            _assert_redacted(item, path=f"{path}.{key}")
    elif isinstance(value, Sequence) and not isinstance(
        value, (str, bytes, bytearray, memoryview)
    ):
        for index, item in enumerate(value):
            _assert_redacted(item, path=f"{path}[{index}]")
    elif isinstance(value, str):
        if len(value) > 500 or _FORBIDDEN_VALUE.search(value):
            raise ProviderProbeError("provider evidence redaction failed")


def _resolve_evidence_directory(
    *,
    config: OptionsCopilotConfig,
    requested: str | Path,
) -> tuple[Path, Path]:
    repository_root = Path(__file__).resolve().parents[2]
    raw = Path(requested)
    if any(part in {".", ".."} for part in raw.parts):
        raise ProviderProbeError("provider evidence path is invalid")
    directory = Path(
        os.path.abspath(raw if raw.is_absolute() else repository_root / raw)
    )
    data_dir = Path(os.path.abspath(config.data_dir))
    evidence_root = data_dir / "evidence"
    for candidate in (repository_root, data_dir, evidence_root, directory):
        if candidate.drive.upper() != "G:":
            raise ProviderProbeError("provider evidence must remain on G drive")
    _require_within(data_dir, repository_root)
    _require_within(evidence_root, repository_root)
    _require_within(directory, evidence_root)
    _assert_no_reparse_ancestors(repository_root, directory)
    resolved_root = evidence_root.resolve(strict=False)
    resolved_directory = directory.resolve(strict=False)
    _require_within(resolved_directory, resolved_root)
    if directory.exists() and not directory.is_dir():
        raise ProviderProbeError("provider evidence directory is invalid")
    return directory, evidence_root


def _require_within(path: Path, root: Path) -> None:
    try:
        path.relative_to(root)
    except ValueError:
        raise ProviderProbeError("provider evidence path escapes its root") from None


def _assert_no_reparse_ancestors(root: Path, path: Path) -> None:
    try:
        relative = path.relative_to(root)
    except ValueError:
        raise ProviderProbeError("provider evidence path escapes its root") from None
    candidates = [root]
    cursor = root
    for part in relative.parts:
        cursor /= part
        candidates.append(cursor)
    reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    for candidate in candidates:
        try:
            attributes = os.lstat(candidate)
        except FileNotFoundError:
            continue
        except OSError:
            raise ProviderProbeError("provider evidence path inspection failed") from None
        if (
            stat.S_ISLNK(attributes.st_mode)
            or getattr(attributes, "st_file_attributes", 0) & reparse_flag
        ):
            raise ProviderProbeError("provider evidence path contains a reparse point")


def _write_checkpoint_exclusive(
    directory: Path,
    *,
    evidence_root: Path,
    document: Mapping[str, object],
) -> None:
    digest = canonical_hash(document)
    filename = f"provider_probe_checkpoint.{digest}.json"
    destination = directory / filename
    rendered = canonical_json(document) + "\n"
    try:
        parsed = json.loads(rendered)
    except json.JSONDecodeError:
        raise ProviderProbeError("provider checkpoint canonicalization failed") from None
    if canonical_json(parsed) + "\n" != rendered or canonical_hash(parsed) != digest:
        raise ProviderProbeError("provider checkpoint canonicalization failed")
    directory.mkdir(parents=True, exist_ok=True)
    _assert_no_reparse_ancestors(evidence_root, directory)
    _require_within(directory.resolve(strict=False), evidence_root.resolve(strict=False))
    created = False
    try:
        with destination.open("x", encoding="utf-8", newline="\n") as handle:
            created = True
            handle.write(rendered)
            handle.flush()
            os.fsync(handle.fileno())
        if destination.read_text(encoding="utf-8") != rendered:
            raise ProviderProbeError("provider checkpoint integrity failed")
    except FileExistsError:
        raise ProviderProbeError("provider checkpoint already exists") from None
    except Exception:
        if created:
            try:
                destination.unlink(missing_ok=True)
            except OSError:
                pass
        raise ProviderProbeError("provider checkpoint write failed") from None


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Probe read-only evidence providers")
    subparsers = parser.add_subparsers(dest="command", required=True)
    probe = subparsers.add_parser("probe", help="write redacted provider evidence")
    probe.add_argument("--providers", required=True)
    probe.add_argument("--symbols")
    probe.add_argument("--limit", type=int, default=50)
    probe.add_argument("--evidence-dir", type=Path, required=True)
    probe.add_argument("--json", action="store_true")
    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    provider_map: Mapping[str, object] | None = None,
    clock: Callable[[], datetime] | None = None,
) -> int:
    args = _parser().parse_args(argv)
    try:
        names = tuple(item.strip() for item in args.providers.split(","))
        if args.symbols:
            symbols = tuple(item.strip() for item in args.symbols.split(","))
        else:
            config = OptionsCopilotConfig.from_env()
            config.validate()
            symbols = config.news_core_symbols
        result = run_probe(
            provider_names=names,
            symbols=symbols,
            evidence_dir=args.evidence_dir,
            limit=args.limit,
            provider_map=provider_map,
            clock=clock,
        )
    except Exception:
        # Never render exception text: network exceptions may embed a complete
        # credential-bearing URL.
        result = {
            "schema": "options_copilot.provider_probe_error.v1",
            "status": "DEGRADED",
            "decision": "NO_TRADE",
            "reason_code": "PROVIDER_PROBE_FAILED",
            "read_only": True,
        }
        if args.json:
            print(json.dumps(result, sort_keys=True))
        else:
            print("NO_TRADE: PROVIDER_PROBE_FAILED", file=sys.stderr)
        return 2
    if args.json:
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    else:
        print(f"{result['status']}: {result['decision']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["PROVIDER_NAMES", "ProviderProbeError", "main", "run_probe"]
