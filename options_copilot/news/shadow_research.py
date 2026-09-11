"""Read-only, supporting-only research advisories for eligible news events.

This module deliberately has no runtime, market-data, storage, approval, bridge,
or order dependencies.  The caller owns pre-model eligibility and may persist
the returned prediction specifications elsewhere.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Iterable, Protocol

from options_copilot.storage.canonical import canonical_hash, utc_datetime
from options_copilot.llm.deepseek import DeepSeekError

from .models import AuditJson, ClassifiedEvent, NewsAuthority, NewsInput
from .macro_proxy import MarketProxyBinding
from .scoring import event_impact_score
from .shadow_prediction import news_shadow_prediction_id


_STRUCTURED_LLM = "STRUCTURED_LLM"
_ADVISORY_INPUT_SCHEMA = "options_copilot.news_advisory_input.v2"
_MAXIMUM_MODEL_ATTEMPTS = 3
_PREDICTION_TARGETS = (
    ("30M", "PREDICTED_AT_PLUS_30_MINUTES", "30m"),
    ("SESSION_CLOSE", "NEXT_ELIGIBLE_SESSION_CLOSE", "session-close"),
    ("1D", "SESSION_CLOSE_PLUS_1_TRADING_DAY", "1d"),
    ("3D", "SESSION_CLOSE_PLUS_3_TRADING_DAYS", "3d"),
    ("5D", "SESSION_CLOSE_PLUS_5_TRADING_DAYS", "5d"),
)


class AdvisoryClassifier(Protocol):
    """Minimal classifier boundary; implementations may be local or remote."""

    def classify(self, news: NewsInput) -> ClassifiedEvent:
        ...


@dataclass(frozen=True, slots=True)
class ResearchAdvisoryInput(AuditJson):
    """An immutable news event plus the caller's deterministic eligibility."""

    news: NewsInput
    eligible: bool
    pre_model_priority_rank: int | None
    symbol_binding: MarketProxyBinding | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.news, NewsInput):
            raise TypeError("news must be a NewsInput")
        if not isinstance(self.eligible, bool):
            raise TypeError("eligible must be a bool")
        rank = self.pre_model_priority_rank
        if rank is not None and (
            isinstance(rank, bool) or not isinstance(rank, int) or rank < 1
        ):
            raise ValueError("pre_model_priority_rank must be a positive integer or None")
        binding = self.symbol_binding
        if binding is not None and (
            not isinstance(binding, MarketProxyBinding)
            or self.news.symbols != (binding.proxy_symbol,)
        ):
            raise ValueError("symbol_binding must match the single shadow symbol")


@dataclass(frozen=True, slots=True)
class PredictionSpec(AuditJson):
    """A stable evaluation target; it does not calculate or fetch an outcome."""

    prediction_id: str
    advisory_id: str
    event_id: str
    symbol: str
    horizon: str
    target_rule: str
    independence_key: str
    decision_authority: NewsAuthority = field(
        default=NewsAuthority.SUPPORTING_ONLY,
        init=False,
    )
    approval_eligible: bool = field(default=False, init=False)
    instruction_creation_allowed: bool = field(default=False, init=False)
    order_allowed: bool = field(default=False, init=False)


@dataclass(frozen=True, slots=True)
class ResearchAdvisoryProjection(AuditJson):
    """Supporting research projection that cannot enter an execution path."""

    advisory_id: str
    event_id: str
    symbol: str
    classification: ClassifiedEvent
    research_priority_score: Decimal
    prediction_specs: tuple[PredictionSpec, ...]
    symbol_binding: MarketProxyBinding | None = None
    decision_authority: NewsAuthority = field(
        default=NewsAuthority.SUPPORTING_ONLY,
        init=False,
    )
    approval_eligible: bool = field(default=False, init=False)
    instruction_creation_allowed: bool = field(default=False, init=False)
    order_allowed: bool = field(default=False, init=False)


@dataclass(frozen=True, slots=True)
class AdvisoryFailure(AuditJson):
    """Sanitized classifier failure; exception text is never retained."""

    failure_id: str
    event_id: str
    reason_code: str
    retryable: bool
    decision_authority: NewsAuthority = field(
        default=NewsAuthority.SUPPORTING_ONLY,
        init=False,
    )
    approval_eligible: bool = field(default=False, init=False)
    instruction_creation_allowed: bool = field(default=False, init=False)
    order_allowed: bool = field(default=False, init=False)


@dataclass(frozen=True, slots=True)
class AdvisoryBatch(AuditJson):
    advisories: tuple[ResearchAdvisoryProjection, ...]
    failures: tuple[AdvisoryFailure, ...]
    attempted_count: int
    deferred_count: int
    skipped_count: int
    skipped_reasons: tuple[tuple[str, int], ...]
    decision_authority: NewsAuthority = field(
        default=NewsAuthority.SUPPORTING_ONLY,
        init=False,
    )
    approval_eligible: bool = field(default=False, init=False)
    instruction_creation_allowed: bool = field(default=False, init=False)
    order_allowed: bool = field(default=False, init=False)


_CachedResult = ResearchAdvisoryProjection | AdvisoryFailure


class ShadowResearchAdvisory:
    """Apply a strict post-enable gate and produce advisory-only projections."""

    def __init__(
        self,
        *,
        classifier: AdvisoryClassifier,
        enabled_at: datetime,
        maximum_batch_size: int = _MAXIMUM_MODEL_ATTEMPTS,
    ) -> None:
        if (
            isinstance(maximum_batch_size, bool)
            or not isinstance(maximum_batch_size, int)
            or not 1 <= maximum_batch_size <= _MAXIMUM_MODEL_ATTEMPTS
        ):
            raise ValueError("maximum_batch_size must be between 1 and 3")
        self._classifier = classifier
        self._enabled_at = utc_datetime(enabled_at, field="enabled_at")
        self._maximum_batch_size = maximum_batch_size
        self._completed: dict[str, _CachedResult] = {}

    def process(self, inputs: Iterable[ResearchAdvisoryInput]) -> AdvisoryBatch:
        candidates, skipped_reasons = self._eligible_candidates(inputs)
        advisories: list[ResearchAdvisoryProjection] = []
        failures: list[AdvisoryFailure] = []
        attempted_count = 0
        deferred_count = 0

        for input_hash, candidate in candidates:
            cached = self._completed.get(input_hash)
            if cached is not None:
                self._append(cached, advisories=advisories, failures=failures)
                continue
            if attempted_count >= self._maximum_batch_size:
                deferred_count += 1
                continue

            attempted_count += 1
            result, cache_result = self._attempt(candidate, input_hash=input_hash)
            self._append(result, advisories=advisories, failures=failures)
            if cache_result:
                self._completed[input_hash] = result

        return AdvisoryBatch(
            advisories=tuple(advisories),
            failures=tuple(failures),
            attempted_count=attempted_count,
            deferred_count=deferred_count,
            skipped_count=sum(skipped_reasons.values()),
            skipped_reasons=tuple(sorted(skipped_reasons.items())),
        )

    def _eligible_candidates(
        self,
        inputs: Iterable[ResearchAdvisoryInput],
    ) -> tuple[
        tuple[tuple[str, ResearchAdvisoryInput], ...],
        dict[str, int],
    ]:
        eligible: list[ResearchAdvisoryInput] = []
        skipped_reasons: dict[str, int] = {}
        for candidate in inputs:
            if not isinstance(candidate, ResearchAdvisoryInput):
                raise TypeError("inputs must contain ResearchAdvisoryInput values")
            news = candidate.news
            reason = self._skip_reason(candidate)
            if reason is not None:
                skipped_reasons[reason] = skipped_reasons.get(reason, 0) + 1
                continue
            eligible.append(candidate)

        eligible.sort(
            key=lambda item: (
                item.pre_model_priority_rank,
                item.news.first_seen_at,
                item.news.event_id,
            )
        )
        deduplicated: list[tuple[str, ResearchAdvisoryInput]] = []
        seen_hashes: set[str] = set()
        for candidate in eligible:
            input_hash = self._input_hash(candidate)
            if input_hash in seen_hashes:
                reason = "SHADOW_INPUT_DUPLICATE"
                skipped_reasons[reason] = skipped_reasons.get(reason, 0) + 1
                continue
            seen_hashes.add(input_hash)
            deduplicated.append((input_hash, candidate))
        return tuple(deduplicated), skipped_reasons

    def _skip_reason(self, candidate: ResearchAdvisoryInput) -> str | None:
        news = candidate.news
        if candidate.eligible is not True:
            return "SHADOW_INPUT_NOT_ELIGIBLE"
        if candidate.pre_model_priority_rank is None:
            return "SHADOW_INPUT_PRIORITY_RANK_MISSING"
        if news.first_seen_at <= self._enabled_at:
            return "SHADOW_INPUT_BEFORE_ENABLEMENT"
        if len(news.symbols) != 1:
            return "SHADOW_INPUT_SYMBOL_COUNT_INVALID"
        if news.is_complete is not True:
            return "SHADOW_INPUT_INCOMPLETE"
        if news.conflicting_evidence_ids:
            return "SHADOW_INPUT_CONFLICTED"
        return None

    def _attempt(
        self,
        candidate: ResearchAdvisoryInput,
        *,
        input_hash: str,
    ) -> tuple[_CachedResult, bool]:
        try:
            classification = self._classifier.classify(candidate.news)
        except ValueError:
            return self._failure(candidate, input_hash, "CLASSIFIER_VALUE_ERROR", False), True
        except TypeError:
            return self._failure(candidate, input_hash, "CLASSIFIER_TYPE_ERROR", False), True
        except DeepSeekError as exc:
            return self._failure(
                candidate,
                input_hash,
                f"DEEPSEEK_{exc.reason}",
                exc.reason
                in {
                    "BAD_JSON",
                    "CONNECT_ERROR",
                    "DAILY_SPEND_CAP",
                    "EMPTY_RESPONSE",
                    "FLASH_DAILY_CALL_CAP",
                    "INCOMPLETE_FINISH",
                    "PRO_DAILY_CALL_CAP",
                    "RATE_LIMITED",
                    "REMOTE_UNAVAILABLE",
                    "REQUEST_TIMEOUT",
                    "TLS_ERROR",
                },
            ), False
        except Exception:
            return self._failure(candidate, input_hash, "CLASSIFIER_TRANSIENT_FAILURE", True), False

        rejection = self._rejection_reason(candidate.news, classification)
        if rejection is not None:
            reason_code, retryable = rejection
            return self._failure(candidate, input_hash, reason_code, retryable), not retryable
        assert isinstance(classification, ClassifiedEvent)
        return self._projection(candidate, input_hash, classification), True

    @staticmethod
    def _rejection_reason(
        news: NewsInput,
        classification: object,
    ) -> tuple[str, bool] | None:
        if not isinstance(classification, ClassifiedEvent):
            return "CLASSIFIER_RESULT_TYPE_INVALID", False
        if not set(classification.symbols).issubset(news.symbols):
            return "CLASSIFIER_INVENTED_SYMBOL", False
        if not set(classification.evidence_ids).issubset(news.evidence_ids):
            return "CLASSIFIER_INVENTED_EVIDENCE", False
        if classification.classifier.strip().upper() != _STRUCTURED_LLM:
            return "CLASSIFIER_NOT_STRUCTURED_LLM", True
        return None

    @staticmethod
    def _input_hash(candidate: ResearchAdvisoryInput) -> str:
        return research_advisory_input_hash(candidate)

    @staticmethod
    def _failure(
        candidate: ResearchAdvisoryInput,
        input_hash: str,
        reason_code: str,
        retryable: bool,
    ) -> AdvisoryFailure:
        return AdvisoryFailure(
            failure_id=f"news-advisory-failure:{input_hash}:{reason_code.lower()}",
            event_id=candidate.news.event_id,
            reason_code=reason_code,
            retryable=retryable,
        )

    @staticmethod
    def _projection(
        candidate: ResearchAdvisoryInput,
        input_hash: str,
        classification: ClassifiedEvent,
    ) -> ResearchAdvisoryProjection:
        news = candidate.news
        advisory_id = f"news-advisory:{input_hash}"
        independence_key = "news-event:" + canonical_hash(
            {
                "event_id": news.event_id,
                "symbol": news.symbols[0],
                "evidence_ids": news.evidence_ids,
            }
        )
        prediction_specs = tuple(
            PredictionSpec(
                prediction_id=news_shadow_prediction_id(advisory_id, slug),
                advisory_id=advisory_id,
                event_id=news.event_id,
                symbol=news.symbols[0],
                horizon=horizon,
                target_rule=target_rule,
                independence_key=independence_key,
            )
            for horizon, target_rule, slug in _PREDICTION_TARGETS
        )
        return ResearchAdvisoryProjection(
            advisory_id=advisory_id,
            event_id=news.event_id,
            symbol=news.symbols[0],
            classification=classification,
            research_priority_score=event_impact_score(news, classification),
            prediction_specs=prediction_specs,
            symbol_binding=candidate.symbol_binding,
        )

    @staticmethod
    def _append(
        result: _CachedResult,
        *,
        advisories: list[ResearchAdvisoryProjection],
        failures: list[AdvisoryFailure],
    ) -> None:
        if isinstance(result, ResearchAdvisoryProjection):
            advisories.append(result)
        else:
            failures.append(result)


def research_advisory_input_hash(candidate: ResearchAdvisoryInput) -> str:
    """Return the stable model-input identity; scheduling rank is not model input."""

    if not isinstance(candidate, ResearchAdvisoryInput):
        raise TypeError("candidate must be ResearchAdvisoryInput")
    document: dict[str, object] = {
        "schema": _ADVISORY_INPUT_SCHEMA,
        "news": candidate.news.as_dict(),
    }
    if candidate.symbol_binding is not None:
        document["symbol_binding"] = candidate.symbol_binding.as_dict()
    return canonical_hash(document)


__all__ = [
    "AdvisoryBatch",
    "AdvisoryClassifier",
    "AdvisoryFailure",
    "PredictionSpec",
    "ResearchAdvisoryInput",
    "ResearchAdvisoryProjection",
    "ShadowResearchAdvisory",
    "research_advisory_input_hash",
]
