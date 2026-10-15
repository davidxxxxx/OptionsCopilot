"""Constrained event classifiers.  They classify facts and never select options."""
from __future__ import annotations

import re
from collections import OrderedDict
from decimal import Decimal, InvalidOperation
from threading import RLock
from typing import Callable, Mapping, Protocol, Sequence

from .models import (
    ClassifiedEvent,
    EventCategory,
    ImpactDirection,
    ImpactHorizon,
    LLM_CLASSIFICATION_JSON_SCHEMA,
    NewsInput,
)


class NewsClassifier(Protocol):
    def classify(self, news: NewsInput) -> ClassifiedEvent: ...


class StructuredLlmAdapter:
    """Validates a deliberately narrow structured response from an LLM adapter."""

    def __init__(self, invoke: Callable[[Mapping[str, object]], Mapping[str, object]]) -> None:
        self._invoke = invoke

    @property
    def json_schema(self) -> Mapping[str, object]:
        return LLM_CLASSIFICATION_JSON_SCHEMA

    def classify(self, news: NewsInput) -> ClassifiedEvent:
        request: dict[str, object] = {
            "schema": LLM_CLASSIFICATION_JSON_SCHEMA,
            "event_id": news.event_id,
            "headline": news.headline,
            "summary": news.summary,
            "symbols": list(news.symbols),
            "evidence_ids": list(news.evidence_ids),
        }
        payload = self._invoke(request)
        if not isinstance(payload, Mapping):
            raise ValueError("structured LLM response must be an object")
        allowed = set(LLM_CLASSIFICATION_JSON_SCHEMA["properties"])
        unexpected = set(payload) - allowed
        if unexpected:
            raise ValueError(f"structured LLM fields are not permitted: {sorted(unexpected)!r}")
        required = set(LLM_CLASSIFICATION_JSON_SCHEMA["required"])
        missing = required - set(payload)
        if missing:
            raise ValueError(f"structured LLM response missing fields: {sorted(missing)!r}")
        for name in ("symbols", "counter_evidence", "evidence_ids"):
            if not isinstance(payload[name], Sequence) or isinstance(payload[name], (str, bytes)):
                raise ValueError(f"structured LLM {name} must be an array")
        try:
            confidence = Decimal(str(payload["confidence"]))
        except (InvalidOperation, ValueError) as exc:
            raise ValueError("structured LLM confidence must be decimal text") from exc
        provided_evidence = tuple(str(item) for item in payload["evidence_ids"])
        unknown_evidence = set(provided_evidence) - set(news.evidence_ids)
        if unknown_evidence:
            raise ValueError("LLM cannot invent evidence identifiers")
        classified_symbols = tuple(str(item) for item in payload["symbols"])
        unknown_symbols = set(symbol.upper() for symbol in classified_symbols) - set(news.symbols)
        if unknown_symbols:
            raise ValueError("LLM cannot invent symbols")
        return ClassifiedEvent(
            category=EventCategory(str(payload["category"]).upper()),
            symbols=classified_symbols,
            direction=ImpactDirection(str(payload["direction"]).upper()),
            horizon=ImpactHorizon(str(payload["horizon"]).upper()),
            confidence=confidence,
            counter_evidence=tuple(str(item) for item in payload["counter_evidence"]),
            evidence_ids=provided_evidence,
            classifier="STRUCTURED_LLM",
        )


class DeterministicNewsClassifier:
    """Predictable downgrade used whenever no constrained LLM adapter is configured."""

    contract_version = "4"

    _CATEGORY_KEYWORDS: tuple[tuple[EventCategory, tuple[str, ...]], ...] = (
        (
            EventCategory.FOMC,
            (
                "fomc",
                "federal reserve",
                "fed decision",
                "美联储",
                "联邦公开市场委员会",
            ),
        ),
        (
            EventCategory.EARNINGS,
            (
                "earnings",
                "eps",
                "quarterly results",
                "财报",
                "季度业绩",
                "季度营收",
                "净利润",
            ),
        ),
        (
            EventCategory.MACRO,
            (
                "cpi",
                "inflation",
                "nonfarm",
                "payroll",
                "gdp",
                "消费者价格指数",
                "通胀",
                "非农",
                "国内生产总值",
            ),
        ),
        (
            EventCategory.GUIDANCE,
            (
                "guidance",
                "outlook",
                "forecast",
                "业绩指引",
                "营收指引",
                "收入指引",
                "销售额指引",
                "销售额增长指引",
                "盈利指引",
                "财年展望",
            ),
        ),
        (
            EventCategory.M_AND_A,
            ("acquire", "acquisition", "merger", "takeover", "收购", "并购"),
        ),
        (
            EventCategory.REGULATORY,
            (
                "sec",
                "doj",
                "investigation",
                "regulator",
                "regulatory",
                "监管",
                "反垄断",
                "诉讼",
            ),
        ),
        (EventCategory.PRODUCT, ("launch", "product", "approval", "新品", "获批")),
        (
            EventCategory.ANALYST,
            (
                "upgrade",
                "downgrade",
                "price target",
                "valuation",
                "sector median",
                "上调评级",
                "下调评级",
                "目标价",
                "估值",
            ),
        ),
    )
    _BULLISH = ("raises", "raise", "beat", "strong", "approval", "upgrade", "record")
    _BEARISH = ("cuts", "cut", "miss", "weak", "investigation", "downgrade", "recall")
    _CORPORATE_BULLISH = (
        "超预期",
        "超过市场预期",
        "上调",
        "强劲",
        "创新高",
        "由跌转涨",
    )
    _CORPORATE_BEARISH = (
        "不及预期",
        "低于预期",
        "下调",
        "疲软",
        "供应限制",
        "供应受限",
        "由涨转跌",
    )
    _CORPORATE_BULLISH_PATTERNS = (
        re.compile(
            r"(?:营收|收入|销售额|净利润|利润|盈利)"
            r"(?:同比|环比)?(?:大幅|显著|持续)?"
            r"(?:增长|上升|增加)"
            r"(?!(?:[ \t]|明显|显著|大幅|持续|进一步|幅度){0,4}(?:放缓|减速))"
        ),
        re.compile(
            r"(?:成本|费用|支出)(?:同比|环比)?(?:大幅|显著)?"
            r"(?:下降|降低|减少)"
        ),
        re.compile(r"(?:净?亏损)(?:同比|环比)?(?:收窄|减少|下降)"),
    )
    _CORPORATE_BEARISH_PATTERNS = (
        re.compile(
            r"(?:营收|收入|销售额|净利润|利润|盈利)"
            r"(?:同比|环比)?(?:大幅|显著)?(?:下降|减少|下滑)"
        ),
        re.compile(
            r"(?:营收|收入|销售额|净利润|利润|盈利)"
            r"(?:同比|环比)?(?:增长|上升|增加)"
            r"(?:[ \t]|明显|显著|大幅|持续|进一步|幅度){0,4}"
            r"(?:放缓|减速)"
        ),
        re.compile(
            r"(?:成本|费用|支出)(?:同比|环比)?(?:大幅|显著)?"
            r"(?:增长|上升|增加)"
        ),
        re.compile(
            r"(?:净?亏损)(?:同比|环比)?(?:大幅|显著)?"
            r"(?:扩大|增长|增加|上升)"
        ),
        re.compile(r"(?:出现|录得|转为)(?:净)?亏损"),
    )
    _MACRO_BULLISH = (
        "cpi below expectations",
        "inflation below expectations",
        "inflation cooled",
        "通胀回落",
        "通胀降温",
        "cpi低于预期",
    )
    _MACRO_BEARISH = (
        "cpi above expectations",
        "inflation above expectations",
        "inflation accelerated",
        "通胀回升",
        "通胀加速",
        "cpi高于预期",
    )
    _MACRO_BULLISH_PATTERNS = (
        re.compile(
            r"(?:cpi|消费者价格指数)[^，。；;]{0,16}?"
            r"(?:低于|不及)(?:市场)?预期"
        ),
    )
    _MACRO_BEARISH_PATTERNS = (
        re.compile(
            r"(?:cpi|消费者价格指数)[^，。；;]{0,16}?"
            r"(?:高于|超过)(?:市场)?预期"
        ),
    )
    _CHINESE_GENERAL_BULLISH = ("获批", "上调评级")
    _CHINESE_GENERAL_BEARISH = ("召回", "下调评级", "接受调查")

    def classify(self, news: NewsInput) -> ClassifiedEvent:
        text = f"{news.headline} {news.summary}".lower()
        category = next(
            (
                candidate
                for candidate, words in self._CATEGORY_KEYWORDS
                if any(self._matches_keyword(text, word) for word in words)
            ),
            EventCategory.OTHER,
        )
        bullish = sum(word in text for word in self._BULLISH)
        bearish = sum(word in text for word in self._BEARISH)
        if category in {EventCategory.EARNINGS, EventCategory.GUIDANCE}:
            bullish += sum(word in text for word in self._CORPORATE_BULLISH)
            bearish += sum(word in text for word in self._CORPORATE_BEARISH)
            bullish += sum(
                pattern.search(text) is not None
                for pattern in self._CORPORATE_BULLISH_PATTERNS
            )
            bearish += sum(
                pattern.search(text) is not None
                for pattern in self._CORPORATE_BEARISH_PATTERNS
            )
        elif category in {EventCategory.FOMC, EventCategory.MACRO}:
            bullish += sum(word in text for word in self._MACRO_BULLISH)
            bearish += sum(word in text for word in self._MACRO_BEARISH)
            bullish += sum(
                pattern.search(text) is not None
                for pattern in self._MACRO_BULLISH_PATTERNS
            )
            bearish += sum(
                pattern.search(text) is not None
                for pattern in self._MACRO_BEARISH_PATTERNS
            )
        else:
            bullish += sum(word in text for word in self._CHINESE_GENERAL_BULLISH)
            bearish += sum(word in text for word in self._CHINESE_GENERAL_BEARISH)
        direction = (
            ImpactDirection.BULLISH if bullish > bearish else
            ImpactDirection.BEARISH if bearish > bullish else
            ImpactDirection.NEUTRAL if bullish == bearish == 0 else
            ImpactDirection.MIXED
        )
        horizon = ImpactHorizon.INTRADAY if category in {EventCategory.FOMC, EventCategory.MACRO} else ImpactHorizon.DAYS_1_3
        confidence = Decimal("0.65") if category is not EventCategory.OTHER else Decimal("0.35")
        return ClassifiedEvent(
            category=category,
            symbols=news.symbols,
            direction=direction,
            horizon=horizon,
            confidence=confidence,
            counter_evidence=("Deterministic fallback; model corroboration unavailable",),
            evidence_ids=news.evidence_ids,
            classifier="DETERMINISTIC_RULES",
        )

    @staticmethod
    def _matches_keyword(text: str, keyword: str) -> bool:
        if keyword.isalpha() and len(keyword) <= 4:
            return re.search(
                rf"(?<![a-z0-9]){re.escape(keyword)}(?![a-z0-9])",
                text,
            ) is not None
        return keyword in text


class FailSafeNewsClassifier:
    """Use a constrained model classifier without making it a runtime dependency.

    Model transport, schema, or provider failures are deliberately collapsed
    into the deterministic classifier.  Exception text is never copied into
    the read model because it can contain credentials or provider internals.
    """

    def __init__(
        self,
        primary: NewsClassifier,
        *,
        fallback: NewsClassifier | None = None,
    ) -> None:
        if not callable(getattr(primary, "classify", None)):
            raise TypeError("primary news classifier must implement classify")
        selected_fallback = fallback or DeterministicNewsClassifier()
        if not callable(getattr(selected_fallback, "classify", None)):
            raise TypeError("fallback news classifier must implement classify")
        self._primary = primary
        self._fallback = selected_fallback

    @property
    def primary(self) -> NewsClassifier:
        """Return the strict classifier for an isolated advisory-only lane."""

        return self._primary

    def classify(self, news: NewsInput) -> ClassifiedEvent:
        try:
            result = self._primary.classify(news)
            if not isinstance(result, ClassifiedEvent):
                raise TypeError("primary classifier returned an invalid result")
            return result
        except Exception:
            result = self._fallback.classify(news)
            if not isinstance(result, ClassifiedEvent):
                raise TypeError("fallback classifier returned an invalid result")
            return result


class CachedNewsClassifier:
    """Bound repeat model calls for immutable point-in-time news inputs.

    Only successful, schema-validated classifications are cached.  Failures
    escape to a surrounding :class:`FailSafeNewsClassifier`, allowing a later
    refresh to retry instead of persisting a transient fallback forever.
    """

    def __init__(self, classifier: NewsClassifier, *, maximum_entries: int = 2048) -> None:
        if not callable(getattr(classifier, "classify", None)):
            raise TypeError("cached news classifier must implement classify")
        if isinstance(maximum_entries, bool) or not isinstance(maximum_entries, int):
            raise TypeError("maximum_entries must be an integer")
        if not 1 <= maximum_entries <= 10_000:
            raise ValueError("maximum_entries must be between 1 and 10000")
        self._classifier = classifier
        self._maximum_entries = maximum_entries
        self._cache: OrderedDict[NewsInput, ClassifiedEvent] = OrderedDict()
        self._lock = RLock()

    def classify(self, news: NewsInput) -> ClassifiedEvent:
        with self._lock:
            cached = self._cache.get(news)
            if cached is not None:
                self._cache.move_to_end(news)
                return cached
        result = self._classifier.classify(news)
        if not isinstance(result, ClassifiedEvent):
            raise TypeError("cached classifier returned an invalid result")
        with self._lock:
            self._cache[news] = result
            self._cache.move_to_end(news)
            while len(self._cache) > self._maximum_entries:
                self._cache.popitem(last=False)
        return result
