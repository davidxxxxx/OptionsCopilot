"""Durable shadow evaluation collection without production authority."""
from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import json
from pathlib import Path
import sqlite3
import threading

from options_copilot.learning_shadow import (
    OutcomeRecord,
    PredictionRecord,
    ShadowLearningLedger,
)
from options_copilot.news.shadow_prediction import NEWS_SHADOW_PREDICTION_SCHEMA
from options_copilot.storage.canonical import (
    canonical_hash,
    canonical_json,
    datetime_text,
    utc_datetime,
)


GENESIS_HASH = "0" * 64
MAX_WRITER_LOCK_WAIT_MS = 250


def _stop_reason(
    cancel_event: threading.Event | None,
    deadline_at: datetime | None,
    clock: Callable[[], datetime] | None,
) -> str | None:
    if cancel_event is not None and cancel_event.is_set():
        return "OUTCOME_PROCESSING_CANCELLED"
    if deadline_at is None:
        return None
    deadline = utc_datetime(deadline_at, field="deadline_at")
    now = utc_datetime(
        (clock or (lambda: datetime.now(timezone.utc)))(),
        field="clock result",
    )
    return (
        "SHADOW_EVALUATION_DEFERRED_DEADLINE"
        if now >= deadline
        else None
    )


class ShadowEvaluationCorruption(RuntimeError):
    pass


class ShadowEvaluationStore:
    """Append immutable dataset/report heads derived from verified shadow rows."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._closed = False
        self._connection = sqlite3.connect(
            self.path, isolation_level=None, check_same_thread=False, timeout=10
        )
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA journal_mode=WAL")
        self._connection.execute("PRAGMA synchronous=FULL")
        self._migrate()
        self.assert_integrity()

    def close(self) -> None:
        with self._lock:
            if not self._closed:
                self._connection.close()
                self._closed = True

    def refresh(
        self,
        ledger: ShadowLearningLedger,
        *,
        generated_at: datetime | None = None,
        cancel_event: threading.Event | None = None,
        deadline_at: datetime | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> Mapping[str, object]:
        stop_reason = _stop_reason(cancel_event, deadline_at, clock)
        if stop_reason is not None:
            return self._cancelled_projection(stop_reason)
        snapshot = ledger.verified_replay_snapshot()
        predictions = {
            row.prediction_id: row
            for row in snapshot.records
            if isinstance(row, PredictionRecord)
            and row.prediction.get("schema") == NEWS_SHADOW_PREDICTION_SCHEMA
        }
        rows: list[dict[str, object]] = []
        for outcome in snapshot.records:
            stop_reason = _stop_reason(cancel_event, deadline_at, clock)
            if stop_reason is not None:
                return self._cancelled_projection(stop_reason)
            if not isinstance(outcome, OutcomeRecord):
                continue
            prediction = predictions.get(outcome.prediction_id)
            if prediction is None:
                continue
            rows.append(_evaluation_row(prediction, outcome))
        rows.sort(key=lambda item: (str(item["observed_at"]), str(item["prediction_id"])))
        dataset_hash = canonical_hash({
            "schema": "options_copilot.shadow_evaluation_dataset.v1",
            "rows": rows,
        })
        eligible_by_key: dict[str, Mapping[str, object]] = {}
        exclusion_reason_counts: dict[str, int] = {}
        for row in rows:
            stop_reason = _stop_reason(cancel_event, deadline_at, clock)
            if stop_reason is not None:
                return self._cancelled_projection(stop_reason)
            if row["evaluation_eligible"] is True:
                independence_key = str(row["independence_key"])
                if independence_key in eligible_by_key:
                    reason = "DUPLICATE_INDEPENDENCE_KEY"
                    exclusion_reason_counts[reason] = (
                        exclusion_reason_counts.get(reason, 0) + 1
                    )
                else:
                    eligible_by_key[independence_key] = row
            else:
                reason = str(row.get("exclusion_reason") or "EVALUATION_INELIGIBLE")
                exclusion_reason_counts[reason] = (
                    exclusion_reason_counts.get(reason, 0) + 1
                )
        independent_rows = tuple(eligible_by_key[key] for key in sorted(eligible_by_key))
        independence_keys = tuple(sorted(eligible_by_key))
        independence_hash = canonical_hash({
            "schema": "options_copilot.shadow_evaluation_independence.v1",
            "keys": independence_keys,
        })
        challenger_hash = canonical_hash({
            "schema": "options_copilot.shadow_challenger_identity.v1",
            "versions": sorted({str(row["challenger_version"]) for row in rows}),
            "model_snapshot_hashes": sorted({
                str(row["model_visible_snapshot_hash"])
                for row in rows
                if row.get("model_visible_snapshot_hash") is not None
            }),
        })
        at = utc_datetime(generated_at or datetime.now(timezone.utc), field="generated_at")
        comparison_complete = bool(independent_rows)
        reason = None
        if not comparison_complete:
            reason = "ZERO_INDEPENDENT_SAMPLES" if not rows else sorted(
                exclusion_reason_counts,
                key=lambda item: (-exclusion_reason_counts[item], item),
            )[0]
        champion_correct = sum(bool(row["champion_correct"]) for row in independent_rows)
        challenger_correct = sum(bool(row["challenger_correct"]) for row in independent_rows)
        champion_brier = _mean_decimal(
            tuple(Decimal(str(row["champion_brier"])) for row in independent_rows)
        )
        challenger_brier = _mean_decimal(
            tuple(Decimal(str(row["challenger_brier"])) for row in independent_rows)
        )
        report = {
            "schema": "options_copilot.shadow_evaluation_collection.v1",
            "status": "AVAILABLE" if comparison_complete else "COLLECTING",
            "reason": reason,
            "generated_at": datetime_text(at),
            "independent_count": len(independence_keys),
            "total_sample_count": len(rows),
            "excluded_sample_count": len(rows) - len(independent_rows),
            "exclusion_reason_counts": dict(sorted(exclusion_reason_counts.items())),
            "dataset_hash": dataset_hash,
            "independence_spec_hash": independence_hash,
            "challenger_hash": challenger_hash,
            "comparison_complete": comparison_complete,
            "champion_accuracy": (
                None
                if not independent_rows
                else Decimal(champion_correct) / Decimal(len(independent_rows))
            ),
            "challenger_accuracy": (
                None
                if not independent_rows
                else Decimal(challenger_correct) / Decimal(len(independent_rows))
            ),
            "challenger_accuracy_delta": (
                None
                if not independent_rows
                else Decimal(challenger_correct - champion_correct)
                / Decimal(len(independent_rows))
            ),
            "champion_brier_score": champion_brier,
            "challenger_brier_score": challenger_brier,
            "challenger_brier_improvement": (
                None
                if champion_brier is None or challenger_brier is None
                else champion_brier - challenger_brier
            ),
            "decision_authority": "SUPPORTING_ONLY",
            "can_change_production_weights": False,
            "can_change_production_rules": False,
            "can_change_ranking": False,
            "can_change_risk": False,
            "approval_eligible": False,
            "instruction_creation_allowed": False,
            "order_allowed": False,
        }
        report_hash = canonical_hash(report)
        projected = {**report, "report_hash": report_hash}
        with self._lock:
            stop_reason = _stop_reason(cancel_event, deadline_at, clock)
            if stop_reason is not None:
                return self._cancelled_projection(stop_reason)
            self.assert_integrity()
            stop_reason = _stop_reason(cancel_event, deadline_at, clock)
            if stop_reason is not None:
                return self._cancelled_projection(stop_reason)

            original_busy_timeout_ms = int(
                self._connection.execute("PRAGMA busy_timeout").fetchone()[0]
            )
            lock_wait_ms = original_busy_timeout_ms
            deadline_limited = False
            busy_timeout_changed = False
            if deadline_at is not None:
                deadline = utc_datetime(deadline_at, field="deadline_at")
                now = utc_datetime(
                    (clock or (lambda: datetime.now(timezone.utc)))(),
                    field="clock result",
                )
                remaining_ms = max(
                    0,
                    int((deadline - now).total_seconds() * 1000),
                )
                lock_wait_ms = min(MAX_WRITER_LOCK_WAIT_MS, remaining_ms)
                deadline_limited = remaining_ms <= MAX_WRITER_LOCK_WAIT_MS
                self._connection.execute(f"PRAGMA busy_timeout={lock_wait_ms}")
                busy_timeout_changed = True
            elif cancel_event is not None:
                lock_wait_ms = min(
                    MAX_WRITER_LOCK_WAIT_MS,
                    original_busy_timeout_ms,
                )
                self._connection.execute(f"PRAGMA busy_timeout={lock_wait_ms}")
                busy_timeout_changed = True

            try:
                try:
                    self._connection.execute("BEGIN IMMEDIATE")
                except sqlite3.OperationalError as exc:
                    if "locked" not in str(exc).lower() and "busy" not in str(
                        exc
                    ).lower():
                        raise
                    stop_reason = _stop_reason(cancel_event, deadline_at, clock)
                    if stop_reason is None:
                        stop_reason = (
                            "SHADOW_EVALUATION_DEFERRED_DEADLINE"
                            if deadline_limited
                            else "SHADOW_EVALUATION_DEFERRED_CONTENTION"
                        )
                    return self._cancelled_projection(stop_reason)

                try:
                    stop_reason = _stop_reason(cancel_event, deadline_at, clock)
                    if stop_reason is not None:
                        self._connection.execute("ROLLBACK")
                        return self._cancelled_projection(stop_reason)
                    latest = self._latest_locked()
                    if (
                        latest is not None
                        and latest.get("dataset_hash") == dataset_hash
                    ):
                        self._connection.execute("ROLLBACK")
                        return latest
                    tail = self._connection.execute(
                        "SELECT sequence, chain_hash FROM shadow_evaluation_reports "
                        "ORDER BY sequence DESC LIMIT 1"
                    ).fetchone()
                    sequence = 1 if tail is None else int(tail["sequence"]) + 1
                    previous = (
                        GENESIS_HASH if tail is None else str(tail["chain_hash"])
                    )
                    chain_hash = canonical_hash({
                        "schema": "options_copilot.shadow_evaluation_chain.v1",
                        "sequence": sequence,
                        "previous_hash": previous,
                        "report_hash": report_hash,
                    })
                    stop_reason = _stop_reason(cancel_event, deadline_at, clock)
                    if stop_reason is not None:
                        self._connection.execute("ROLLBACK")
                        return self._cancelled_projection(stop_reason)
                    self._connection.execute(
                        "INSERT INTO shadow_evaluation_reports VALUES(?,?,?,?,?,?)",
                        (
                            sequence,
                            canonical_json(projected),
                            report_hash,
                            previous,
                            chain_hash,
                            datetime_text(at),
                        ),
                    )
                    stop_reason = _stop_reason(cancel_event, deadline_at, clock)
                    if stop_reason is not None:
                        self._connection.execute("ROLLBACK")
                        return self._cancelled_projection(stop_reason)
                    self._connection.execute("COMMIT")
                except BaseException:
                    if self._connection.in_transaction:
                        self._connection.execute("ROLLBACK")
                    raise
            finally:
                if busy_timeout_changed:
                    self._connection.execute(
                        f"PRAGMA busy_timeout={original_busy_timeout_ms}"
                    )
        return projected

    def _cancelled_projection(self, reason: str) -> Mapping[str, object]:
        return {
            "schema": "options_copilot.shadow_evaluation_collection.v1",
            "status": "CANCELLED",
            "reason": reason,
            "decision_authority": "SUPPORTING_ONLY",
            "persisted": False,
        }

    def latest(self) -> Mapping[str, object] | None:
        with self._lock:
            self.assert_integrity()
            return self._latest_locked()

    def assert_integrity(self) -> None:
        if self._closed:
            raise RuntimeError("shadow evaluation store is closed")
        triggers = {
            str(row[0])
            for row in self._connection.execute(
                "SELECT name FROM sqlite_master WHERE type='trigger'"
            ).fetchall()
        }
        if not {
            "shadow_evaluation_no_update",
            "shadow_evaluation_no_delete",
        }.issubset(triggers):
            raise ShadowEvaluationCorruption(
                "shadow evaluation append-only trigger is unavailable"
            )
        previous = GENESIS_HASH
        rows = self._connection.execute(
            "SELECT * FROM shadow_evaluation_reports ORDER BY sequence"
        ).fetchall()
        for expected, row in enumerate(rows, start=1):
            if int(row["sequence"]) != expected or str(row["previous_hash"]) != previous:
                raise ShadowEvaluationCorruption("shadow evaluation chain is broken")
            payload = json.loads(str(row["report_json"]))
            supplied = payload.pop("report_hash", None)
            if (
                payload.get("schema")
                != "options_copilot.shadow_evaluation_collection.v1"
                or payload.get("decision_authority") != "SUPPORTING_ONLY"
                or payload.get("can_change_production_weights") is not False
                or payload.get("can_change_production_rules") is not False
                or payload.get("can_change_ranking") is not False
                or payload.get("can_change_risk") is not False
                or payload.get("approval_eligible") is not False
                or payload.get("instruction_creation_allowed") is not False
                or payload.get("order_allowed") is not False
            ):
                raise ShadowEvaluationCorruption(
                    "shadow evaluation schema or authority is invalid"
                )
            report_hash = canonical_hash(payload)
            if supplied != report_hash or report_hash != str(row["report_hash"]):
                raise ShadowEvaluationCorruption("shadow evaluation report hash mismatch")
            chain_hash = canonical_hash({
                "schema": "options_copilot.shadow_evaluation_chain.v1",
                "sequence": expected,
                "previous_hash": previous,
                "report_hash": report_hash,
            })
            if chain_hash != str(row["chain_hash"]):
                raise ShadowEvaluationCorruption("shadow evaluation chain hash mismatch")
            previous = chain_hash

    def _latest_locked(self) -> Mapping[str, object] | None:
        row = self._connection.execute(
            "SELECT report_json FROM shadow_evaluation_reports ORDER BY sequence DESC LIMIT 1"
        ).fetchone()
        return None if row is None else json.loads(str(row["report_json"]))

    def _migrate(self) -> None:
        self._connection.execute(
            "CREATE TABLE IF NOT EXISTS shadow_evaluation_reports("
            "sequence INTEGER PRIMARY KEY, report_json TEXT NOT NULL, "
            "report_hash TEXT NOT NULL, previous_hash TEXT NOT NULL, "
            "chain_hash TEXT NOT NULL UNIQUE, recorded_at TEXT NOT NULL)"
        )
        self._connection.execute(
            "CREATE TRIGGER IF NOT EXISTS shadow_evaluation_no_update "
            "BEFORE UPDATE ON shadow_evaluation_reports "
            "BEGIN SELECT RAISE(ABORT, 'shadow evaluation is append-only'); END"
        )
        self._connection.execute(
            "CREATE TRIGGER IF NOT EXISTS shadow_evaluation_no_delete "
            "BEFORE DELETE ON shadow_evaluation_reports "
            "BEGIN SELECT RAISE(ABORT, 'shadow evaluation is append-only'); END"
        )


def _evaluation_row(
    prediction: PredictionRecord,
    outcome: OutcomeRecord,
) -> dict[str, object]:
    challenger = prediction.prediction.get("classification")
    baseline = prediction.prediction.get("champion_baseline")
    reason = None
    champion = None
    if not isinstance(challenger, Mapping):
        reason = "CHALLENGER_CLASSIFICATION_INVALID"
    elif not isinstance(baseline, Mapping):
        reason = "CHAMPION_BASELINE_UNAVAILABLE"
    else:
        body = {key: value for key, value in baseline.items() if key != "baseline_hash"}
        if (
            baseline.get("schema")
            != "options_copilot.deterministic_champion_baseline.v1"
            or baseline.get("champion_version") != prediction.champion_version
            or baseline.get("baseline_hash") != canonical_hash(body)
            or not isinstance(baseline.get("classification"), Mapping)
            or baseline.get("classification_hash")
            != canonical_hash(baseline["classification"])
        ):
            reason = "CHAMPION_BASELINE_INVALID"
        else:
            champion = baseline["classification"]
    actual = _actual_bullish(outcome.outcome)
    if actual is None and reason is None:
        reason = "MARKET_DIRECTION_UNAVAILABLE"
    champion_probability = _bullish_probability(champion)
    challenger_probability = _bullish_probability(challenger)
    if reason is None and (
        champion_probability is None or challenger_probability is None
    ):
        reason = "CLASSIFICATION_PROBABILITY_INVALID"
    champion_correct = (
        None if actual is None or champion is None else _direction_correct(champion, actual)
    )
    challenger_correct = (
        None if actual is None or not isinstance(challenger, Mapping)
        else _direction_correct(challenger, actual)
    )
    return {
        "prediction_id": prediction.prediction_id,
        "prediction_hash": prediction.content_hash,
        "outcome_hash": outcome.content_hash,
        "independence_key": prediction.independence_key,
        "champion_version": prediction.champion_version,
        "challenger_version": prediction.challenger_version,
        "model_visible_snapshot_hash": prediction.prediction.get(
            "model_visible_snapshot_hash"
        ),
        "champion_baseline_hash": (
            None if not isinstance(baseline, Mapping) else baseline.get("baseline_hash")
        ),
        "actual_bullish": actual,
        "champion_probability": champion_probability,
        "challenger_probability": challenger_probability,
        "champion_correct": champion_correct,
        "challenger_correct": challenger_correct,
        "champion_brier": (
            None if actual is None or champion_probability is None
            else (champion_probability - actual) ** 2
        ),
        "challenger_brier": (
            None if actual is None or challenger_probability is None
            else (challenger_probability - actual) ** 2
        ),
        "evaluation_eligible": reason is None,
        "exclusion_reason": reason,
        "observed_at": datetime_text(outcome.observed_at),
    }


def _actual_bullish(outcome: Mapping[str, object]) -> Decimal | None:
    underlying = outcome.get("underlying")
    if not isinstance(underlying, Mapping):
        return None
    try:
        baseline = Decimal(str(underlying.get("baseline_price")))
        current = Decimal(str(underlying.get("price")))
    except (InvalidOperation, TypeError, ValueError):
        return None
    if not baseline.is_finite() or not current.is_finite() or current == baseline:
        return None
    return Decimal("1") if current > baseline else Decimal("0")


def _bullish_probability(classification: object) -> Decimal | None:
    if not isinstance(classification, Mapping):
        return None
    direction = str(classification.get("direction") or "").upper()
    try:
        confidence = Decimal(str(classification.get("confidence")))
    except (InvalidOperation, TypeError, ValueError):
        return None
    if not confidence.is_finite() or not Decimal("0") <= confidence <= Decimal("1"):
        return None
    if direction == "BULLISH":
        return confidence
    if direction == "BEARISH":
        return Decimal("1") - confidence
    if direction in {"NEUTRAL", "MIXED"}:
        return Decimal("0.5")
    return None


def _direction_correct(classification: Mapping[str, object], actual: Decimal) -> bool:
    direction = str(classification.get("direction") or "").upper()
    return (direction == "BULLISH" and actual == 1) or (
        direction == "BEARISH" and actual == 0
    )


def _mean_decimal(values: tuple[Decimal, ...]) -> Decimal | None:
    return None if not values else sum(values, Decimal("0")) / Decimal(len(values))


__all__ = ["ShadowEvaluationCorruption", "ShadowEvaluationStore"]
