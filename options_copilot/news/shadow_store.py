"""Durable, research-only persistence for DeepSeek news advisories.

This adapter writes only to the append-only shadow-learning ledger.  It has no
broker, approval, bridge, creator, ranking, or order dependency.  Five horizon
predictions share one event-level independence key, so an event can contribute
at most one independent sample even after every horizon is resolved.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from collections.abc import Iterable
from typing import Mapping

from options_copilot.learning_shadow import (
    ShadowLearningLedger,
    UnknownRecordError,
)
from options_copilot.storage.canonical import canonical_hash, utc_datetime

from .models import ClassifiedEvent, NewsInput
from .deepseek import deepseek_news_snapshot_hash
from .macro_proxy import require_current_market_proxy_binding
from .shadow_research import ResearchAdvisoryProjection
from .shadow_prediction import (
    LEGACY_NEWS_SHADOW_PREDICTION_SCHEMA,
    NEWS_SHADOW_PREDICTION_SCHEMA,
    NEWS_SHADOW_PREDICTION_TARGETS_BY_SLUG,
    news_shadow_prediction_id,
    shadow_prediction_exclusion_reason,
)


LEGACY_THESIS_ID = "news-deepseek-shadow-v1"
THESIS_ID = "news-deepseek-shadow-v2"
CHAMPION_VERSION = "deterministic-news-research-v1"
CHALLENGER_VERSION = "deepseek-news-advisory-v2"
_EVIDENCE_SCHEMA = "options_copilot.news_shadow_evidence.v2"


@dataclass(frozen=True, slots=True)
class ShadowWriteResult:
    advisory_id: str
    evidence_id: str
    prediction_ids: tuple[str, ...]
    appended_predictions: int
    decision_authority: str = "SUPPORTING_ONLY"
    approval_eligible: bool = False
    instruction_creation_allowed: bool = False
    order_allowed: bool = False


def _immutable_prediction_payloads(
    advisory: ResearchAdvisoryProjection,
    *,
    predicted_at: datetime,
    model_visible_snapshot_hash: str,
    prediction_baseline_hash: str,
    champion_baseline: Mapping[str, object] | None,
) -> tuple[Mapping[str, object], ...]:
    payloads: list[Mapping[str, object]] = []
    for spec in advisory.prediction_specs:
        slug = spec.prediction_id.rsplit(":", 1)[-1]
        if NEWS_SHADOW_PREDICTION_TARGETS_BY_SLUG.get(slug) != (
            spec.horizon,
            spec.target_rule,
        ):
            raise ValueError("advisory prediction target is incompatible")
        prediction_id = news_shadow_prediction_id(advisory.advisory_id, slug)
        payloads.append(
            {
                "prediction_id": prediction_id,
                "predicted_at": predicted_at.isoformat(),
                "horizon_at": None,
                "independence_key": spec.independence_key,
                "challenger_version": CHALLENGER_VERSION,
                "tags": (
                    "news",
                    "deepseek",
                    "shadow",
                    f"symbol:{advisory.symbol}",
                    f"horizon:{spec.horizon}",
                ),
                "prediction": {
                    "schema": NEWS_SHADOW_PREDICTION_SCHEMA,
                    "advisory_id": advisory.advisory_id,
                    "event_id": advisory.event_id,
                    "symbol": advisory.symbol,
                    "horizon": spec.horizon,
                    "target_rule": spec.target_rule,
                    "research_priority_score": str(
                        advisory.research_priority_score
                    ),
                    "classification": advisory.classification.as_dict(),
                    "champion_baseline": champion_baseline,
                    "symbol_binding": (
                        None
                        if advisory.symbol_binding is None
                        else advisory.symbol_binding.as_dict()
                    ),
                    "model_visible_snapshot_hash": model_visible_snapshot_hash,
                    "prediction_set_predicted_at": predicted_at.isoformat(),
                    "prediction_baseline_hash": prediction_baseline_hash,
                    "decision_authority": "SUPPORTING_ONLY",
                    "approval_eligible": False,
                    "instruction_creation_allowed": False,
                    "order_allowed": False,
                },
            }
        )
    return tuple(payloads)


def _verified_symbol_binding(
    value: object,
    *,
    symbol: str,
) -> Mapping[str, object] | None | bool:
    """Return a canonical current proxy binding, or False when it is invalid."""

    if value is None:
        return None
    try:
        binding = require_current_market_proxy_binding(
            value,
            symbol=symbol,
        )
    except (TypeError, ValueError):
        return False
    if binding is None:
        return None
    canonical = binding.as_dict()
    return canonical


class NewsShadowLearningWriter:
    """Idempotently bind one advisory to one evidence row and five predictions."""

    def __init__(
        self,
        ledger: ShadowLearningLedger,
        *,
        enabled_at: datetime,
    ) -> None:
        if not isinstance(ledger, ShadowLearningLedger):
            raise TypeError("ledger must be a ShadowLearningLedger")
        self._ledger = ledger
        self._enabled_at = utc_datetime(enabled_at, field="enabled_at")
        self._enabled_at = self._ensure_thesis()

    @property
    def enabled_at(self) -> datetime:
        """Return the immutable first activation time for this shadow lane."""

        return self._enabled_at

    def record(
        self,
        advisory: ResearchAdvisoryProjection,
        news: NewsInput,
        *,
        recorded_at: datetime,
        champion_classification: ClassifiedEvent | None = None,
    ) -> ShadowWriteResult:
        if not isinstance(advisory, ResearchAdvisoryProjection):
            raise TypeError("advisory must be a ResearchAdvisoryProjection")
        if not isinstance(news, NewsInput):
            raise TypeError("news must be a NewsInput")
        checked_at = utc_datetime(recorded_at, field="recorded_at")
        if champion_classification is not None and not isinstance(
            champion_classification, ClassifiedEvent
        ):
            raise TypeError("champion_classification must be a ClassifiedEvent")
        if advisory.event_id != news.event_id or advisory.symbol not in news.symbols:
            raise ValueError("advisory does not match the supplied news input")
        if news.first_seen_at <= self._enabled_at:
            raise ValueError("pre-enable news cannot enter shadow learning")
        if checked_at < news.first_seen_at:
            raise ValueError("advisory cannot be recorded before first_seen_at")

        model_visible_snapshot_hash = deepseek_news_snapshot_hash(news)
        supplied_champion_baseline = None
        if champion_classification is not None:
            champion_body = champion_classification.as_dict()
            champion_baseline_body = {
                "schema": "options_copilot.deterministic_champion_baseline.v1",
                "champion_version": CHAMPION_VERSION,
                "analysis_cutoff_at": checked_at.isoformat(),
                "classification": champion_body,
                "classification_hash": canonical_hash(champion_body),
            }
            supplied_champion_baseline = {
                **champion_baseline_body,
                "baseline_hash": canonical_hash(champion_baseline_body),
            }
        evidence_id = "news-shadow-evidence-v2:" + canonical_hash(
            {
                "event_id": news.event_id,
                "symbol": advisory.symbol,
                "evidence_ids": news.evidence_ids,
                "published_at": news.published_at,
                "first_seen_at": news.first_seen_at,
                "model_visible_snapshot_hash": model_visible_snapshot_hash,
            }
        )
        try:
            evidence_record = self._ledger.get_evidence(evidence_id)
        except UnknownRecordError:
            prediction_set_predicted_at = checked_at
            prediction_baseline_hash = canonical_hash(
                {
                    "schema": "options_copilot.news_prediction_baseline.v2",
                    "advisory_id": advisory.advisory_id,
                    "event_id": news.event_id,
                    "symbol": advisory.symbol,
                    "model_visible_snapshot_hash": model_visible_snapshot_hash,
                    "prediction_set_predicted_at": prediction_set_predicted_at,
                }
            )
            prediction_payloads = _immutable_prediction_payloads(
                advisory,
                predicted_at=prediction_set_predicted_at,
                model_visible_snapshot_hash=model_visible_snapshot_hash,
                prediction_baseline_hash=prediction_baseline_hash,
                champion_baseline=supplied_champion_baseline,
            )
            evidence_record = self._ledger.record_evidence(
                evidence_id,
                THESIS_ID,
                source="OPTIONS_COPILOT_NEWS",
                evidence={
                    "schema": _EVIDENCE_SCHEMA,
                    "event_id": news.event_id,
                    "symbol": advisory.symbol,
                    "news_input_hash": canonical_hash(news.as_dict()),
                    "model_visible_snapshot_schema": (
                        "options_copilot.deepseek_public_news_snapshot.v1"
                    ),
                    "model_visible_snapshot_hash": model_visible_snapshot_hash,
                    "prediction_set_predicted_at": (
                        prediction_set_predicted_at.isoformat()
                    ),
                    "prediction_baseline_hash": prediction_baseline_hash,
                    "prediction_payloads": prediction_payloads,
                    "prediction_payloads_hash": canonical_hash(
                        prediction_payloads
                    ),
                    "champion_baseline": supplied_champion_baseline,
                    "source": news.source,
                    "authority": news.authority.value,
                    "source_evidence_ids": list(news.evidence_ids),
                    "symbol_binding": (
                        None
                        if advisory.symbol_binding is None
                        else advisory.symbol_binding.as_dict()
                    ),
                    "decision_authority": "SUPPORTING_ONLY",
                    "approval_eligible": False,
                    "instruction_creation_allowed": False,
                    "order_allowed": False,
                },
                published_at=news.published_at,
                first_seen_at=news.first_seen_at,
                tags=("news", "deepseek", "shadow", f"symbol:{advisory.symbol}"),
            )
        evidence_payload = evidence_record.evidence
        prediction_set_predicted_at = _prediction_set_time(evidence_payload)
        prediction_baseline_hash = str(
            evidence_payload.get("prediction_baseline_hash") or ""
        ).strip().lower()
        expected_baseline_hash = canonical_hash(
            {
                "schema": "options_copilot.news_prediction_baseline.v2",
                "advisory_id": advisory.advisory_id,
                "event_id": news.event_id,
                "symbol": advisory.symbol,
                "model_visible_snapshot_hash": model_visible_snapshot_hash,
                "prediction_set_predicted_at": prediction_set_predicted_at,
            }
        )
        if (
            evidence_payload.get("schema") != _EVIDENCE_SCHEMA
            or prediction_baseline_hash != expected_baseline_hash
        ):
            raise ValueError("news shadow prediction baseline is incompatible")
        champion_baseline = evidence_payload.get("champion_baseline")
        if champion_baseline is not None:
            if not isinstance(champion_baseline, Mapping):
                raise ValueError("news shadow champion baseline is incompatible")
            baseline_body = {
                key: value
                for key, value in champion_baseline.items()
                if key != "baseline_hash"
            }
            if (
                champion_baseline.get("schema")
                != "options_copilot.deterministic_champion_baseline.v1"
                or champion_baseline.get("champion_version") != CHAMPION_VERSION
                or champion_baseline.get("analysis_cutoff_at")
                != prediction_set_predicted_at.isoformat()
                or champion_baseline.get("baseline_hash")
                != canonical_hash(baseline_body)
                or not isinstance(
                    champion_baseline.get("classification"), Mapping
                )
                or champion_baseline.get("classification_hash")
                != canonical_hash(champion_baseline["classification"])
            ):
                raise ValueError("news shadow champion baseline is incompatible")

        prediction_payloads = evidence_payload.get("prediction_payloads")
        if (
            not isinstance(prediction_payloads, (tuple, list))
            or len(prediction_payloads) != len(
                NEWS_SHADOW_PREDICTION_TARGETS_BY_SLUG
            )
            or evidence_payload.get("prediction_payloads_hash")
            != canonical_hash(prediction_payloads)
        ):
            raise ValueError("news shadow prediction payload set is incompatible")

        appended = 0
        prediction_ids: list[str] = []
        for stored_payload in prediction_payloads:
            if not isinstance(stored_payload, Mapping):
                raise ValueError("news shadow prediction payload is incompatible")
            prediction_id = str(stored_payload.get("prediction_id") or "")
            prediction = stored_payload.get("prediction")
            independence_key = str(stored_payload.get("independence_key") or "")
            tags = stored_payload.get("tags")
            if (
                not prediction_id
                or not isinstance(prediction, Mapping)
                or not isinstance(tags, (tuple, list))
                or stored_payload.get("predicted_at")
                != prediction_set_predicted_at.isoformat()
                or stored_payload.get("challenger_version") != CHALLENGER_VERSION
                or shadow_prediction_exclusion_reason(
                    prediction,
                    prediction_id=prediction_id,
                    independence_key=independence_key,
                    predicted_at=prediction_set_predicted_at,
                )
                is not None
            ):
                raise ValueError("news shadow prediction payload is incompatible")
            prediction_ids.append(prediction_id)
            try:
                existing_prediction = self._ledger.get_prediction(prediction_id)
            except UnknownRecordError:
                self._ledger.record_prediction(
                    prediction_id,
                    THESIS_ID,
                    evidence_ids=(evidence_id,),
                    prediction=prediction,
                    predicted_at=prediction_set_predicted_at,
                    horizon_at=None,
                    independence_key=independence_key,
                    challenger_version=CHALLENGER_VERSION,
                    tags=tuple(str(item) for item in tags),
                )
                appended += 1
            else:
                if (
                    existing_prediction.predicted_at
                    != prediction_set_predicted_at
                    or existing_prediction.prediction.get(
                        "prediction_baseline_hash"
                    )
                    != prediction_baseline_hash
                ):
                    raise ValueError(
                        "partial prediction set does not share one immutable baseline"
                    )

        return ShadowWriteResult(
            advisory_id=advisory.advisory_id,
            evidence_id=evidence_id,
            prediction_ids=tuple(prediction_ids),
            appended_predictions=appended,
        )

    def advisory_projection(self, event_id: str) -> Mapping[str, object] | None:
        """Restore one persisted advisory without invoking the classifier."""

        checked_event_id = str(event_id).strip()
        if not checked_event_id:
            raise ValueError("event_id cannot be blank")
        return self.advisory_projections((checked_event_id,)).get(checked_event_id)

    def advisory_projections(
        self,
        event_ids: Iterable[str],
        *,
        expected_advisory_ids: Mapping[str, str] | None = None,
    ) -> Mapping[str, Mapping[str, object]]:
        """Restore a bounded event set with one ledger integrity pass."""

        checked_ids = tuple(
            dict.fromkeys(str(value).strip() for value in event_ids if str(value).strip())
        )
        if len(checked_ids) > 500:
            raise ValueError("at most 500 event_ids may be restored")
        wanted = set(checked_ids)
        targeted = expected_advisory_ids is not None
        expected = {
            str(event_id).strip(): str(advisory_id).strip()
            for event_id, advisory_id in (expected_advisory_ids or {}).items()
            if str(event_id).strip() in wanted and str(advisory_id).strip()
        }
        if targeted and not expected:
            return {}
        grouped: dict[tuple[str, str], dict[str, object]] = {}

        def accumulate(record: object) -> None:
            prediction = record.prediction
            event_id = str(prediction.get("event_id") or "")
            advisory_id = str(prediction.get("advisory_id") or "")
            if (
                prediction.get("schema") != NEWS_SHADOW_PREDICTION_SCHEMA
                or event_id not in wanted
                or not advisory_id
            ):
                return
            prediction_id = record.prediction_id
            expected_target = next(
                (
                    target
                    for slug, target in NEWS_SHADOW_PREDICTION_TARGETS_BY_SLUG.items()
                    if prediction_id == news_shadow_prediction_id(advisory_id, slug)
                ),
                None,
            )
            exclusion = shadow_prediction_exclusion_reason(
                prediction,
                prediction_id=prediction_id,
                independence_key=record.independence_key,
                predicted_at=record.predicted_at,
            )
            if exclusion is not None or expected_target is None or (
                prediction.get("horizon"),
                prediction.get("target_rule"),
            ) != expected_target:
                return
            classification = prediction.get("classification")
            symbol = str(prediction.get("symbol") or "").strip().upper()
            symbol_binding = _verified_symbol_binding(
                prediction.get("symbol_binding"),
                symbol=symbol,
            )
            model_visible_snapshot_hash = str(
                prediction.get("model_visible_snapshot_hash") or ""
            ).strip().lower()
            independence_key = str(record.independence_key or "").strip()
            predicted_at = record.predicted_at.isoformat()
            prediction_baseline_hash = str(
                prediction.get("prediction_baseline_hash") or ""
            ).strip().lower()
            if (
                not isinstance(classification, Mapping)
                or not symbol
                or symbol_binding is False
                or len(model_visible_snapshot_hash) != 64
                or any(
                    character not in "0123456789abcdef"
                    for character in model_visible_snapshot_hash
                )
                or not independence_key
                or prediction.get("decision_authority") != "SUPPORTING_ONLY"
                or prediction.get("approval_eligible") is not False
                or prediction.get("instruction_creation_allowed") is not False
                or prediction.get("order_allowed") is not False
            ):
                return
            key = (event_id, advisory_id)
            group = grouped.setdefault(
                key,
                {
                    "latest_sequence": 0,
                    "verified_prediction_ids": set(),
                    "payload_signatures": set(),
                    "classification": None,
                    "research_priority_score": None,
                    "symbol": None,
                    "symbol_binding": None,
                },
            )
            group["latest_sequence"] = max(
                int(group["latest_sequence"]),
                record.sequence,
            )
            checked_classification = dict(classification)
            group["classification"] = checked_classification
            group["research_priority_score"] = prediction.get(
                "research_priority_score"
            )
            group["symbol"] = symbol
            group["symbol_binding"] = symbol_binding
            signatures = group["payload_signatures"]
            assert isinstance(signatures, set)
            signatures.add(
                canonical_hash(
                    {
                        "classification": checked_classification,
                        "research_priority_score": prediction.get(
                            "research_priority_score"
                        ),
                        "symbol": symbol,
                        "symbol_binding": symbol_binding,
                        "model_visible_snapshot_hash": model_visible_snapshot_hash,
                        "independence_key": independence_key,
                        "predicted_at": predicted_at,
                        "prediction_baseline_hash": prediction_baseline_hash,
                        "decision_authority": "SUPPORTING_ONLY",
                        "approval_eligible": False,
                        "instruction_creation_allowed": False,
                        "order_allowed": False,
                    }
                )
            )
            verified_ids = group["verified_prediction_ids"]
            assert isinstance(verified_ids, set)
            verified_ids.add(prediction_id)

        if targeted:
            target_prediction_ids = tuple(
                news_shadow_prediction_id(advisory_id, slug)
                for advisory_id in expected.values()
                for slug in NEWS_SHADOW_PREDICTION_TARGETS_BY_SLUG
            )
            for record in self._ledger.query_predictions_by_ids(
                target_prediction_ids,
                challenger_version=CHALLENGER_VERSION,
            ):
                accumulate(record)
        else:
            after_sequence = 0
            while True:
                page = self._ledger.query_replays(
                    challenger_version=CHALLENGER_VERSION,
                    limit=5000,
                    after_sequence=after_sequence,
                )
                if not page:
                    break
                for replay in page:
                    accumulate(replay.prediction)
                after_sequence = page[-1].prediction.sequence
                if len(page) < 5000:
                    break

        restored: dict[str, Mapping[str, object]] = {}
        latest_by_event: dict[str, tuple[int, Mapping[str, object]]] = {}
        for (event_id, advisory_id), group in grouped.items():
            expected_id = expected.get(event_id)
            if expected_id is not None and advisory_id != expected_id:
                continue
            verified_ids = group["verified_prediction_ids"]
            signatures = group["payload_signatures"]
            assert isinstance(verified_ids, set)
            assert isinstance(signatures, set)
            expected_ids = {
                news_shadow_prediction_id(advisory_id, slug)
                for slug in NEWS_SHADOW_PREDICTION_TARGETS_BY_SLUG
            }
            projection = {
                "advisory_id": advisory_id,
                "research_priority_score": group["research_priority_score"],
                "classification": group["classification"],
                "symbol": group["symbol"],
                "symbol_binding": group["symbol_binding"],
                "shadow_prediction_count": len(verified_ids),
                "prediction_set_complete": (
                    verified_ids == expected_ids and len(signatures) == 1
                ),
                "decision_authority": "SUPPORTING_ONLY",
                "approval_eligible": False,
                "instruction_creation_allowed": False,
                "order_allowed": False,
            }
            sequence = int(group["latest_sequence"])
            current = latest_by_event.get(event_id)
            if current is None or sequence > current[0]:
                latest_by_event[event_id] = (sequence, projection)
        restored.update(
            {event_id: projection for event_id, (_, projection) in latest_by_event.items()}
        )
        return restored

    def _ensure_thesis(self) -> datetime:
        try:
            thesis = self._ledger.get_thesis(THESIS_ID)
        except UnknownRecordError:
            activation_fence = self._enabled_at
            try:
                legacy = self._ledger.get_thesis(LEGACY_THESIS_ID)
            except UnknownRecordError:
                pass
            else:
                if (
                    legacy.thesis.get("schema")
                    == "options_copilot.news_shadow_thesis.v1"
                ):
                    activation_fence = min(activation_fence, legacy.created_at)
            thesis = self._ledger.record_thesis(
                THESIS_ID,
                champion_version=CHAMPION_VERSION,
                challenger_version=CHALLENGER_VERSION,
                thesis={
                    "schema": "options_copilot.news_shadow_thesis.v2",
                    "purpose": "Evaluate DeepSeek news research priority only",
                    "decision_authority": "SUPPORTING_ONLY",
                    "can_change_production_weights": False,
                    "can_change_production_rules": False,
                    "can_unlock_a_grade": False,
                    "approval_eligible": False,
                    "instruction_creation_allowed": False,
                    "order_allowed": False,
                },
                created_at=activation_fence,
                tags=("news", "deepseek", "shadow"),
            )
        if (
            thesis.champion_version != CHAMPION_VERSION
            or thesis.challenger_version != CHALLENGER_VERSION
        ):
            raise ValueError("existing news shadow thesis version is incompatible")
        # The thesis timestamp is the durable activation fence. Reusing it
        # across process restarts prevents the current research pool from
        # becoming permanently ineligible while still excluding every item
        # first seen before the model lane was originally enabled.
        return thesis.created_at


def _prediction_set_time(value: Mapping[str, object]) -> datetime:
    raw = value.get("prediction_set_predicted_at")
    if not isinstance(raw, str) or not raw.strip():
        raise ValueError("prediction_set_predicted_at is missing")
    try:
        parsed = datetime.fromisoformat(raw.strip().replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("prediction_set_predicted_at is invalid") from exc
    return utc_datetime(parsed, field="prediction_set_predicted_at")


__all__ = [
    "CHALLENGER_VERSION",
    "CHAMPION_VERSION",
    "LEGACY_THESIS_ID",
    "NewsShadowLearningWriter",
    "ShadowWriteResult",
    "THESIS_ID",
]
