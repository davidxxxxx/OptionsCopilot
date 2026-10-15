"""One leased daily child for native history observations, never model authority."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import re
import threading

from options_copilot.market.session_calendar import (
    CalendarStatus,
    US_OPTIONS_TIMEZONE,
    UsOptionsCalendarSnapshot,
)
from options_copilot.scanner.operation_context import ScheduledOperationContext
from options_copilot.scanner.scheduler import DAILY_OPERATION_PIPELINES
from options_copilot.storage.canonical import canonical_hash


_KINDS = ("PRICE_HISTORY", "IV_HISTORY")
_MAX_SYMBOLS = 3


def _delivered_history(row: Mapping[str, object]) -> bool:
    fragment = row.get("fragment")
    return bool(
        isinstance(fragment, Mapping)
        and fragment.get("status") == "DELIVERED"
        and isinstance(fragment.get("response"), Mapping)
        and fragment["response"].get("response_end_received") is True
    )


def _baseline_rows(row: Mapping[str, object]) -> int:
    return len({
        bar["session_date"] for bar in row["fragment"]["response"].get("bars", ())
        if bar.get("valid_close") is True and bar.get("date_eligible") is True
        and isinstance(bar.get("session_date"), str)
    })


def _result(status: str, reasons: Sequence[str], **values: object) -> dict[str, object]:
    body = {
        "schema": "options_copilot.scheduled_native_history.v1",
        "status": status,
        "reason_codes": list(dict.fromkeys(reasons)),
        **values,
        "decision_authority": "OBSERVATION_ONLY",
        "model_input_complete": False,
        "production_eligible": False,
        "affects_eligibility": False,
        "instruction_creation_allowed": False,
        "order_allowed": False,
    }
    return {**body, "content_hash": canonical_hash(body)}


class ScheduledHistoryProducer:
    """Uses the existing parent slot and gateway; GET never enters this lane."""

    def __init__(
        self,
        gateway: object,
        store: object,
        *,
        closing: Callable[[], bool],
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.gateway = gateway
        self.store = store
        self._closing = closing
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._run_lock = threading.Lock()
        self._state_lock = threading.Lock()
        self._latest = _result("WIRED_NOT_RUN", ("FEATURE_HISTORY_SCHEDULED_SLOT_NOT_RUN",))

    def status(self) -> dict[str, object]:
        with self._state_lock:
            latest = deepcopy(self._latest)
        try:
            durable = self.store.status()
        except Exception:
            durable = {"status": "UNAVAILABLE", "reason_codes": ["HISTORY_SOURCE_STORE_UNAVAILABLE"]}
        status, reasons = latest["status"], latest["reason_codes"]
        if durable.get("status") != "VERIFIED":
            status, reasons = "UNAVAILABLE", ["HISTORY_SOURCE_STORE_UNAVAILABLE"]
        elif status == "WIRED_NOT_RUN" and durable.get("manifest_count", 0) > 0:
            status, reasons = "HISTORICAL_EVIDENCE_RESTORED", ["FEATURE_HISTORY_CURRENT_SLOT_NOT_RUN"]
        return {
            "status": status,
            "latest_runtime_run": latest,
            "durable": durable,
            "reason_codes": reasons,
            "decision_authority": "OBSERVATION_ONLY",
            "model_input_complete": False,
            "production_eligible": False,
        }

    def run(
        self,
        calendar: UsOptionsCalendarSnapshot,
        scheduled_for: datetime,
        *,
        operation_context: ScheduledOperationContext | None,
        symbols: Sequence[str],
    ) -> dict[str, object]:
        if not self._run_lock.acquire(blocking=False):
            return _result("NOT_RUN", ("FEATURE_HISTORY_PRODUCER_BUSY",))
        try:
            try:
                outcome = self._run(calendar, scheduled_for, operation_context, symbols)
            except Exception:
                # Provider/store exception text can carry private SDK state.
                outcome = _result("FAILED", ("FEATURE_HISTORY_CHILD_FAILED",))
            with self._state_lock:
                self._latest = deepcopy(outcome)
            return outcome
        finally:
            self._run_lock.release()

    def _run(
        self,
        calendar: UsOptionsCalendarSnapshot,
        scheduled_for: datetime,
        context: ScheduledOperationContext | None,
        symbols: Sequence[str],
    ) -> dict[str, object]:
        if not isinstance(context, ScheduledOperationContext):
            return _result("NOT_RUN", ("FEATURE_HISTORY_PARENT_LEASE_REQUIRED",))

        def guard() -> bool:
            return not self._closing() and context.is_active()

        session = calendar.session_for(context.trading_date)
        if not (
            guard()
            and context.operation == "NEXT_SESSION_PREPARATION"
            and context.owner == "daily-operation.next_session_preparation"
            and context.pipeline_version == DAILY_OPERATION_PIPELINES[context.operation]
            and calendar.status is CalendarStatus.READY
            and calendar.verify_hash()
            and session is not None
            and context.slot_at == scheduled_for
            and scheduled_for == session.close_utc + timedelta(minutes=40)
            and context.trading_date == scheduled_for.astimezone(US_OPTIONS_TIMEZONE).date()
        ):
            return _result("NOT_RUN", ("FEATURE_HISTORY_PARENT_LEASE_OR_CALENDAR_INVALID",))
        parent = {
            "parent_run_id": context.scan_run_id,
            "owner": context.owner,
            "operation": context.operation,
            "session_date": context.trading_date.isoformat(),
            "scheduled_for": scheduled_for.astimezone(timezone.utc).isoformat(),
            "deadline_at": context.deadline_at.astimezone(timezone.utc).isoformat(),
            "calendar_hash": calendar.calendar_hash,
        }
        cutoff = {"calendar": calendar.as_dict(), "scheduled_for": parent["scheduled_for"]}
        if not isinstance(symbols, Sequence) or isinstance(symbols, (str, bytes, bytearray)):
            return _result("NOT_RUN", ("FEATURE_HISTORY_TARGETS_INVALID",), parent=parent)
        targets = tuple(dict.fromkeys(
            symbol for symbol in symbols[:256]
            if isinstance(symbol, str) and re.fullmatch(r"[A-Z0-9][A-Z0-9.\-]{0,31}", symbol)
        ))
        prepared: list[dict[str, object]] = []
        skipped: list[dict[str, str]] = []
        prepared_symbols: list[str] = []
        for symbol in targets:
            if len(prepared_symbols) >= _MAX_SYMBOLS or not guard():
                break
            per_symbol: list[dict[str, object]] = []
            for kind in _KINDS:
                if not guard():
                    break
                try:
                    # Preparation is cache-only. It cannot silently qualify a
                    # missing identity or spend an unclaimed broker request.
                    request = self.gateway.prepare_native_history(
                        symbol, kind=kind, cutoff=cutoff, incremental=False,
                    )
                    page_reader = getattr(self.store, "find_fragments_page", None)
                    if callable(page_reader):
                        existing = page_reader(
                            symbol=symbol, con_id=request["con_id"], cutoff=self._clock(), limit=128,
                        )["fragments"]
                    else:
                        existing = self.store.find_fragments(
                            symbol=symbol, con_id=request["con_id"], cutoff=self._clock(),
                        )
                    same_kind = [
                        row for row in existing
                        if row["prepared_request"]["kind"] == kind
                    ]
                    # A long gap is a new provider vintage, not a fabricated
                    # contiguous seven-day update. Old fragments stay intact.
                    baseline = any(
                        _delivered_history(row)
                        and row["prepared_request"]["request_contract"].get("incremental") is False
                        and row["prepared_request"]["basis_hash"] == request["basis_hash"]
                        # Acquisition sufficiency only, not a model calendar,
                        # approved EMA window or comparable 252-session IV.
                        and _baseline_rows(row) >= (60 if kind == "PRICE_HISTORY" else 252)
                        for row in same_kind
                    )
                    recent = baseline and any(
                        _delivered_history(row)
                        and row["prepared_request"]["basis_hash"] == request["basis_hash"]
                        and timedelta(0) <= self._clock() - datetime.fromisoformat(
                            row["reference"]["first_seen_at"]
                        ) <= timedelta(days=6)
                        for row in same_kind
                    )
                    if recent:
                        request = self.gateway.prepare_native_history(
                            symbol, kind=kind, cutoff=cutoff, incremental=True,
                        )
                    per_symbol.append(request)
                except Exception:
                    skipped.append({"symbol": symbol, "kind": kind, "reason": "HISTORY_PREPARATION_UNAVAILABLE"})
            if per_symbol:
                prepared_symbols.append(symbol)
                prepared.extend(per_symbol)
        if not guard():
            return _result("NOT_RUN", ("FEATURE_HISTORY_PARENT_EXPIRED_OR_CANCELLED",), parent=parent)
        if not prepared:
            return _result("NOT_RUN", ("FEATURE_HISTORY_PREPARED_IDENTITY_UNAVAILABLE",), parent=parent, skipped=skipped)
        manifest = self.store.freeze_manifest(parent, prepared, guard=guard)
        results: list[dict[str, object]] = []
        attempts: list[dict[str, object]] = []
        reasons: list[str] = []
        for request in prepared:
            if not guard():
                reasons.append("FEATURE_HISTORY_PARENT_EXPIRED_OR_CANCELLED")
                break
            permits: list[object] = []

            def before_send(request=request, permits=permits) -> str:
                if not guard():
                    raise RuntimeError("FEATURE_HISTORY_PARENT_EXPIRED_OR_CANCELLED")
                claim = self.store.claim_for_send(
                    manifest["manifest_id"], request["request_hash"], request["basis_hash"], guard=guard,
                )
                if claim.permit is None:
                    raise RuntimeError("FEATURE_HISTORY_REQUEST_ALREADY_CLAIMED")
                permits.append(claim.permit)
                return claim.permit.claim_id

            try:
                fragment = self.gateway.read_native_history(
                    request, before_send=before_send, operation_guard=guard,
                    remaining_seconds=context.remaining_seconds,
                )
                attempts.append({
                    "symbol": request["symbol"], "kind": request["kind"],
                    "source_status": fragment.get("status"),
                    "reason_codes": [
                        code for code in fragment.get("reason_codes", ())
                        if isinstance(code, str) and re.fullmatch(r"[A-Z0-9_:]{1,160}", code)
                    ][:96],
                })
                if len(permits) != 1:
                    raise RuntimeError("FEATURE_HISTORY_SEND_INTENT_REQUIRED")
                reference = self.store.complete(permits[0], fragment, guard=guard)
                results.append({
                    "symbol": request["symbol"], "kind": request["kind"],
                    "request_hash": request["request_hash"], "basis_hash": request["basis_hash"],
                    "source_status": fragment.get("status"), "reference": reference,
                })
                if fragment.get("status") != "DELIVERED":
                    reasons.append("FEATURE_HISTORY_RESPONSE_INCOMPLETE")
            except Exception:
                reasons.append("FEATURE_HISTORY_REQUEST_UNAVAILABLE_OR_UNCERTAIN")
                # Keep a lost response/failed commit's durable intent uncertain.
                # A local exception is not proof that nothing reached the wire.
        deferred = [symbol for symbol in targets if symbol not in prepared_symbols and not any(row["symbol"] == symbol for row in skipped)]
        if deferred:
            reasons.append("FEATURE_HISTORY_TARGET_BUDGET_EXHAUSTED")
        status = "OBSERVATIONS_PERSISTED" if results and not reasons and not skipped else "PARTIAL" if results else "FAILED"
        return _result(
            status, reasons or (["FEATURE_HISTORY_TARGETS_PARTIALLY_UNAVAILABLE"] if skipped else []),
            parent=parent, manifest=manifest, target_symbols=prepared_symbols,
            planned_request_count=len(prepared), persisted_response_count=len(results),
            results=results, attempts=attempts, skipped=skipped, deferred_symbols=deferred,
            native_fragments_only=True, historical_session_coverage_verified=False,
            adjustment_vintages_mergeable=False,
        )


__all__ = ["ScheduledHistoryProducer"]
