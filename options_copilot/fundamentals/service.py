"""Read-only fundamentals refresh and deterministic public projection."""
from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timedelta, timezone
import threading
from typing import Protocol

from options_copilot.storage.canonical import canonical_hash, utc_datetime

from .models import FundamentalCategory, FundamentalMetric, FundamentalObservation
from .providers import FundamentalProviderError
from .store import FundamentalsStore, StoredFundamental


MAXIMUM_ACTUAL_PERIOD_AGE = timedelta(days=550)


class FundamentalProvider(Protocol):
    decision_authority: str

    def fetch(self, symbols: Sequence[str]) -> tuple[FundamentalObservation, ...]: ...


class FundamentalsService:
    """Refresh immutable facts without blocking API reads or trading paths."""

    def __init__(
        self,
        store: FundamentalsStore,
        *,
        providers: Sequence[FundamentalProvider],
        symbols: Sequence[str],
        clock: Callable[[], datetime] | None = None,
        refresh_seconds: int = 15 * 60,
        maximum_symbols_per_refresh: int = 4,
    ) -> None:
        if not isinstance(store, FundamentalsStore):
            raise TypeError("store must be FundamentalsStore")
        if (
            isinstance(refresh_seconds, bool)
            or not isinstance(refresh_seconds, int)
            or not 15 * 60 <= refresh_seconds <= 24 * 60 * 60
        ):
            raise ValueError("refresh_seconds must be between 15 minutes and 24 hours")
        self.store = store
        self._providers = tuple(providers)
        self._core_symbols = tuple(
            dict.fromkeys(str(item).strip().upper() for item in symbols)
        )
        if not self._core_symbols or len(self._core_symbols) > 40:
            raise ValueError("symbols must contain between 1 and 40 items")
        self._observed_symbols: list[str] = []
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._refresh_seconds = refresh_seconds
        if (
            isinstance(maximum_symbols_per_refresh, bool)
            or not isinstance(maximum_symbols_per_refresh, int)
            or not 1 <= maximum_symbols_per_refresh <= 8
        ):
            raise ValueError("maximum_symbols_per_refresh must be between 1 and 8")
        self._maximum_symbols_per_refresh = maximum_symbols_per_refresh
        self._symbol_cursor = 0
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._provider_health: dict[str, dict[str, object]] = {
            type(provider).__name__: {
                "status": "DEGRADED",
                "reason_code": "NOT_OBSERVED",
                "as_of": None,
                "observation_count": 0,
            }
            for provider in self._providers
        }
        self._last_refresh_at: datetime | None = None

    def observe_symbols(self, symbols: Sequence[str]) -> tuple[str, ...]:
        """Prioritize bounded candidate symbols for subsequent provider refreshes.

        Core symbols are never evicted.  Recently observed scan/research symbols
        rotate through the remaining capacity so a broad market funnel does not
        silently collapse fundamentals back to a fixed mega-cap list.
        """

        checked: list[str] = []
        for value in symbols:
            symbol = str(value).strip().upper()
            if (
                not symbol
                or len(symbol) > 12
                or not symbol.replace(".", "").isalnum()
            ):
                continue
            checked.append(symbol)
        dynamic_capacity = 40 - len(self._core_symbols)
        with self._lock:
            for symbol in dict.fromkeys(checked):
                if symbol in self._core_symbols:
                    continue
                if symbol in self._observed_symbols:
                    self._observed_symbols.remove(symbol)
                self._observed_symbols.append(symbol)
            if dynamic_capacity <= 0:
                self._observed_symbols.clear()
            elif len(self._observed_symbols) > dynamic_capacity:
                self._observed_symbols = self._observed_symbols[-dynamic_capacity:]
            active = self._active_symbols_unlocked()
            self._symbol_cursor %= len(active)
            return active

    def _active_symbols_unlocked(self) -> tuple[str, ...]:
        return (*self._core_symbols, *self._observed_symbols)

    def start(self) -> None:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._stop.clear()
            self._thread = threading.Thread(
                target=self._run,
                name="options-copilot-fundamentals",
                daemon=True,
            )
            self._thread.start()

    def close(self) -> bool:
        self._stop.set()
        thread = self._thread
        if thread is not None:
            thread.join(timeout=10.0)
            if thread.is_alive():
                return False
        self.store.close()
        return True

    def refresh_once(self) -> Mapping[str, object]:
        now = utc_datetime(self._clock(), field="fundamentals refresh clock")
        with self._lock:
            active_symbols = self._active_symbols_unlocked()
            start = self._symbol_cursor
            stop = start + self._maximum_symbols_per_refresh
            doubled = active_symbols + active_symbols
            refresh_symbols = doubled[start:stop]
            self._symbol_cursor = stop % len(active_symbols)
        for provider in self._providers:
            name = type(provider).__name__
            status = "READY"
            reason: str | None = None
            inserted = 0
            count = 0
            try:
                if getattr(provider, "decision_authority", None) != "SUPPORTING_ONLY":
                    raise FundamentalProviderError("AUTHORITY_INVALID")
                observations = provider.fetch(refresh_symbols)
                for observation in observations:
                    result = self.store.append(observation)
                    count += 1
                    inserted += int(result.inserted)
                diagnostics = getattr(provider, "last_fetch_diagnostics", None)
                partial_failure = (
                    isinstance(diagnostics, Mapping)
                    and isinstance(diagnostics.get("failed_count"), int)
                    and diagnostics["failed_count"] > 0
                )
                if observations and partial_failure:
                    status = "DEGRADED"
                    reason = "PARTIAL_SYMBOL_FAILURE"
                elif not observations:
                    status = "DEGRADED"
                    reason = "NO_USABLE_RECORDS"
            except FundamentalProviderError as exc:
                status = "DEGRADED"
                reason = exc.reason
            except Exception:
                status = "DEGRADED"
                reason = "PROVIDER_UNAVAILABLE"
            with self._lock:
                health: dict[str, object] = {
                    "status": status,
                    "reason_code": reason,
                    "as_of": now.isoformat(),
                    "observation_count": count,
                    "inserted_count": inserted,
                    "requested_symbols": list(refresh_symbols),
                }
                diagnostics = getattr(provider, "last_fetch_diagnostics", None)
                if isinstance(diagnostics, Mapping):
                    health["batch_diagnostics"] = dict(diagnostics)
                self._provider_health[name] = health
        with self._lock:
            self._last_refresh_at = now
        return self.payload()

    def payload(self) -> dict[str, object]:
        with self._lock:
            active_symbols = self._active_symbols_unlocked()
        try:
            self.store.assert_integrity()
            records = self.store.current(symbols=active_symbols, limit=500)
        except Exception:
            return unavailable_fundamentals_payload("FUNDAMENTALS_LEDGER_INVALID")
        with self._lock:
            provider_health = {
                key: dict(value) for key, value in self._provider_health.items()
            }
            last_refresh = self._last_refresh_at
        rows = _latest_metric_rows(records, as_of=last_refresh)
        categories = _category_status(rows)
        revisions = _revision_projection(self.store, rows, as_of=last_refresh)
        refresh_reason = _current_refresh_reason(
            provider_health,
            last_refresh_at=last_refresh,
        )
        status = "READY" if rows and refresh_reason is None else "DEGRADED"
        reason_codes = list(
            dict.fromkeys(
                (
                    *(() if rows else ("FUNDAMENTALS_NOT_OBSERVED",)),
                    *(() if refresh_reason is None else (refresh_reason,)),
                )
            )
        )
        content_hash = canonical_hash(
            {
                "schema": "options_copilot.fundamentals_read_model.v1",
                "as_of": last_refresh,
                "rows": rows,
                "revisions": revisions,
                "provider_health": provider_health,
                "status": status,
                "reason_codes": reason_codes,
                "decision_authority": "SUPPORTING_ONLY",
            }
        )
        return {
            "schema": "options_copilot.fundamentals_read_model.v1",
            "status": status,
            "reason_codes": reason_codes,
            "as_of": None if last_refresh is None else last_refresh.isoformat(),
            "symbols": list(active_symbols),
            "rows": rows,
            "row_count": len(rows),
            "revisions": revisions,
            "revision_count": len(revisions),
            "categories": categories,
            "provider_health": provider_health,
            "content_hash": content_hash,
            "point_in_time_semantics": "FIRST_OBSERVED_AT_OR_BEFORE_CUTOFF",
            "correction_policy": "APPEND_ONLY_SUPERSEDES_HASH",
            "decision_authority": "SUPPORTING_ONLY",
            "approval_eligible": False,
            "instruction_creation_allowed": False,
            "order_allowed": False,
        }

    def supporting_evidence(self, symbol: str, *, as_of: datetime) -> dict[str, object]:
        cutoff = utc_datetime(as_of, field="fundamental cutoff")
        checked_symbol = str(symbol).strip().upper()
        self.observe_symbols((checked_symbol,))
        try:
            records = self.store.verified_current(
                symbols=(checked_symbol,),
                as_of=cutoff,
                limit=64,
            )
        except Exception:
            return {
                "status": "DEGRADED",
                "reason_codes": ["FUNDAMENTALS_LEDGER_INVALID"],
                "source_hash": None,
                "payload": {
                    "symbol": checked_symbol,
                    "as_of": cutoff.isoformat(),
                    "record_count": 0,
                    "records": [],
                    "categories": {},
                    "revisions": [],
                    "revision_count": 0,
                    "point_in_time_semantics": "FIRST_OBSERVED_AT_OR_BEFORE_CUTOFF",
                    "decision_authority": "SUPPORTING_ONLY",
                },
            }
        rows = _latest_metric_rows(records, as_of=cutoff)
        categories = _category_status(rows)
        revisions = _revision_projection(self.store, rows, as_of=cutoff)
        with self._lock:
            provider_health = {
                key: dict(value) for key, value in self._provider_health.items()
            }
            last_refresh = self._last_refresh_at
        refresh_reason = _current_refresh_reason(
            provider_health,
            last_refresh_at=last_refresh,
        )
        partial_reasons = tuple(
            dict.fromkeys(
                str(value.get("reason_code"))
                for value in categories.values()
                if isinstance(value, Mapping) and value.get("reason_code")
            )
        )
        payload = {
            "symbol": checked_symbol,
            "as_of": cutoff.isoformat(),
            "record_count": len(rows),
            "records": rows,
            "categories": categories,
            "revisions": revisions,
            "revision_count": len(revisions),
            "provider_health": provider_health,
            "last_refresh_at": (
                None if last_refresh is None else last_refresh.isoformat()
            ),
            "point_in_time_semantics": "FIRST_OBSERVED_AT_OR_BEFORE_CUTOFF",
            "decision_authority": "SUPPORTING_ONLY",
        }
        source_hash = fundamental_supporting_source_hash(payload)
        if source_hash is None:
            raise RuntimeError("fundamental supporting source hash basis invalid")
        return {
            "status": (
                "AVAILABLE"
                if rows and refresh_reason is None
                else "DEGRADED"
            ),
            "reason_codes": (
                list(
                    dict.fromkeys(
                        (
                            *(
                                ("SUPPORTING_ONLY_NO_HARD_AUTHORITY",)
                                if rows
                                else ("FUNDAMENTALS_UNAVAILABLE",)
                            ),
                            *(
                                ()
                                if refresh_reason is None
                                else (refresh_reason,)
                            ),
                            *partial_reasons,
                        )
                    )
                )
            ),
            "source_hash": source_hash,
            "payload": payload,
        }

    def _run(self) -> None:
        while not self._stop.is_set():
            self.refresh_once()
            if self._stop.wait(self._refresh_seconds):
                return


def _current_refresh_reason(
    provider_health: Mapping[str, Mapping[str, object]],
    *,
    last_refresh_at: datetime | None,
) -> str | None:
    """Return one bounded reason when current provider evidence is absent.

    Historical PIT rows remain useful supporting evidence, but they cannot
    turn a failed or never-run current refresh into a READY projection.
    """

    if last_refresh_at is None:
        return "FUNDAMENTALS_REFRESH_NOT_OBSERVED"
    if not provider_health:
        return "FUNDAMENTALS_PROVIDER_UNCONFIGURED"
    if any(
        str(health.get("status") or "").upper() == "READY"
        for health in provider_health.values()
    ):
        return None
    return "FUNDAMENTALS_CURRENT_REFRESH_DEGRADED"


def fundamental_supporting_source_hash(payload: object) -> str | None:
    """Return the exact producer digest for one public supporting payload."""

    if not isinstance(payload, Mapping):
        return None
    symbol = str(payload.get("symbol") or "").strip().upper()
    if not symbol or payload.get("decision_authority") != "SUPPORTING_ONLY":
        return None
    try:
        cutoff = utc_datetime(
            datetime.fromisoformat(str(payload.get("as_of") or "")),
            field="fundamental supporting as_of",
        )
    except (TypeError, ValueError):
        return None
    records = payload.get("records")
    revisions = payload.get("revisions")
    categories = payload.get("categories")
    provider_health = payload.get("provider_health")
    if (
        not isinstance(records, Sequence)
        or isinstance(records, (str, bytes, bytearray))
        or not isinstance(revisions, Sequence)
        or isinstance(revisions, (str, bytes, bytearray))
        or not isinstance(categories, Mapping)
        or not isinstance(provider_health, Mapping)
    ):
        return None
    record_hashes: list[str] = []
    for row in records:
        if not isinstance(row, Mapping):
            return None
        content_hash = row.get("content_hash")
        if (
            not isinstance(content_hash, str)
            or len(content_hash) != 64
            or any(character not in "0123456789abcdef" for character in content_hash)
        ):
            return None
        record_hashes.append(content_hash)
    health: dict[str, Mapping[str, object]] = {}
    for key, value in provider_health.items():
        if not isinstance(value, Mapping):
            return None
        health[str(key)] = value
    last_refresh_raw = payload.get("last_refresh_at")
    if last_refresh_raw is None:
        last_refresh = None
    else:
        try:
            last_refresh = utc_datetime(
                datetime.fromisoformat(str(last_refresh_raw)),
                field="fundamental supporting last_refresh_at",
            )
        except (TypeError, ValueError):
            return None
    return canonical_hash(
        {
            "schema": "options_copilot.fundamental_supporting_input.v1",
            "symbol": symbol,
            "as_of": cutoff,
            "record_hashes": tuple(record_hashes),
            "categories": categories,
            "revisions": revisions,
            "refresh_reason": _current_refresh_reason(
                health,
                last_refresh_at=last_refresh,
            ),
            "decision_authority": "SUPPORTING_ONLY",
        }
    )


def unavailable_fundamentals_payload(reason: str) -> dict[str, object]:
    now = datetime.now(timezone.utc)
    categories = {
        category.value: {
            "status": "UNAVAILABLE",
            "record_count": 0,
            "reason_code": str(reason).upper(),
        }
        for category in FundamentalCategory
    }
    return {
        "schema": "options_copilot.fundamentals_read_model.v1",
        "status": "UNAVAILABLE",
        "reason_codes": [str(reason).upper()],
        "as_of": now.isoformat(),
        "symbols": [],
        "rows": [],
        "row_count": 0,
        "revisions": [],
        "revision_count": 0,
        "categories": categories,
        "provider_health": {},
        "content_hash": None,
        "point_in_time_semantics": "FIRST_OBSERVED_AT_OR_BEFORE_CUTOFF",
        "correction_policy": "APPEND_ONLY_SUPERSEDES_HASH",
        "decision_authority": "SUPPORTING_ONLY",
        "approval_eligible": False,
        "instruction_creation_allowed": False,
        "order_allowed": False,
    }


def _latest_metric_rows(
    records: Sequence[StoredFundamental],
    *,
    as_of: datetime | None = None,
) -> list[dict[str, object]]:
    cutoff = None if as_of is None else utc_datetime(as_of, field="fundamental cutoff")
    latest: dict[tuple[str, FundamentalMetric], StoredFundamental] = {}
    for record in records:
        observation = record.observation
        if (
            cutoff is not None
            and observation.basis == "SEC_XBRL_ACTUAL"
            and cutoff.date() - observation.period_end > MAXIMUM_ACTUAL_PERIOD_AGE
        ):
            # Historical filings stay in the immutable ledger, but an ancient
            # value must not be projected as the company's current fundamental.
            continue
        key = (record.observation.symbol, record.observation.metric)
        previous = latest.get(key)
        if previous is None or (
            record.observation.period_end,
            record.sequence,
        ) > (
            previous.observation.period_end,
            previous.sequence,
        ):
            latest[key] = record
    return [
        record.as_dict()
        for record in sorted(
            latest.values(),
            key=lambda item: (item.observation.symbol, item.observation.metric.value),
        )
    ]


def _category_status(rows: Sequence[Mapping[str, object]]) -> dict[str, object]:
    output: dict[str, object] = {}
    for category in FundamentalCategory:
        matching = [row for row in rows if row.get("category") == category.value]
        reason = None
        if not matching:
            reason = (
                "GUIDANCE_STRUCTURED_SOURCE_UNAVAILABLE"
                if category is FundamentalCategory.GUIDANCE
                else "FUNDAMENTAL_CATEGORY_UNAVAILABLE"
            )
        output[category.value] = {
            "status": "AVAILABLE" if matching else "UNAVAILABLE",
            "record_count": len(matching),
            "reason_code": reason,
        }
    return output


def _revision_projection(
    store: FundamentalsStore,
    rows: Sequence[Mapping[str, object]],
    *,
    as_of: datetime | None = None,
) -> list[dict[str, object]]:
    output: list[dict[str, object]] = []
    for row in rows:
        series_key = row.get("series_key")
        if not isinstance(series_key, str):
            continue
        history = store.revisions(series_key, as_of=as_of, limit=4)
        if len(history) < 2:
            continue
        current, previous = history[0], history[1]
        output.append(
            {
                "symbol": current.observation.symbol,
                "metric": current.observation.metric.value,
                "period_end": current.observation.period_end.isoformat(),
                "revision_number": current.revision_number,
                "previous_value": format(previous.observation.value, "f"),
                "current_value": format(current.observation.value, "f"),
                "delta": format(
                    current.observation.value - previous.observation.value,
                    "f",
                ),
                "supersedes_hash": current.supersedes_hash,
                "observed_at": current.observation.observed_at.isoformat(),
                "decision_authority": "SUPPORTING_ONLY",
            }
        )
    return output


__all__ = ["FundamentalsService", "unavailable_fundamentals_payload"]
