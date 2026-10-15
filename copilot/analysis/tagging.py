"""News classification: category, importance, Chinese summary and per-symbol impacts.

Each news item is classified once and the tag is stored. The view ledger is computed
from stored tags by fixed rules, so the same news can never produce two different
views. Only public news text and the symbol universe are sent to the model.
"""
from __future__ import annotations

import logging
import re
from collections.abc import Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Protocol
from zoneinfo import ZoneInfo

from copilot.news.models import NewsItem

log = logging.getLogger(__name__)
BEIJING = ZoneInfo("Asia/Shanghai")

CATEGORIES: dict[str, str] = {
    "macro_data": "宏观数据",
    "central_bank": "央行/利率",
    "policy": "财政/产业政策",
    "trade": "贸易/关税",
    "geopolitics": "地缘政治",
    "commodity": "大宗商品/能源",
    "earnings": "财报/业绩",
    "company": "公司经营/产品",
    "deal": "并购/融资/回购",
    "analyst": "机构评级/观点",
    "regulation": "监管/诉讼",
    "flows": "资金流/市场情绪",
    "crypto": "加密货币",
    "other": "其他/噪音",
}
HORIZONS = ("intraday", "days", "weeks")
VOL_EFFECTS = ("up", "down", "none")
MAX_IMPACTS = 5
_TICKER = re.compile(r"^[A-Z]{1,5}$")

_PROMPT = """你是美股期权研究助理，负责给新闻逐条分类并标注它对美股标的的影响。
新闻来自金十快讯（中文）和 Finnhub（英文）。只依据新闻本身判断，不臆测。

对每条新闻输出：
- category：{categories}
- importance：0-3 的整数
  0 = 与美股/美股期权无关或没有增量信息（A股/港股个股、币价波动、重复报道、无实质内容）
  1 = 相关但影响很小
  2 = 可能影响相关标的未来 1-5 天走势
  3 = 可能显著影响大盘或个股（央行决议、CPI/非农等明显超预期、重大财报或指引、重大政策/制裁/战争）
- summary：中文一句话摘要（不超过 40 字）
- impacts：受影响的标的列表（最多 {max_impacts} 个，importance 为 0 时为空），每个：
  symbol（只能来自下面的允许列表）、direction（1 利多 / -1 利空 / 0 只影响波动不定方向）、
  magnitude（1 小 / 2 中 / 3 大）、horizon（intraday 当天 / days 数天 / weeks 数周）、
  vol（up 预计波动加大 / down 波动减小 / none）

规则：
1. 宏观、利率、政策、地缘新闻要映射到受影响的 ETF：大盘 SPY/QQQ/IWM/DIA，长债 TLT，黄金 GLD，白银 SLV，
   原油 USO，天然气 UNG，行业 XLE 能源、XLF 金融、XLK 科技、XLV 医疗、XLI 工业、XLY 可选消费、XLP 必选消费、
   XLU 公用事业、SMH 半导体、KRE 地区银行、XBI 生物科技、XHB 住宅建筑、XRT 零售，中国相关 FXI/KWEB，新兴市场 EEM。
2. 公司新闻标注公司本身；只有明确的产业链关系才标注同行或上下游。
3. 只标注有把握的方向；只是"值得关注"的写 importance 1 且 impacts 为空。
4. 每条新闻都要返回，id 与输入一致。

只输出 JSON：{{"items": [{{"id": 1, "category": "...", "importance": 2, "summary": "...",
"impacts": [{{"symbol": "SPY", "direction": 1, "magnitude": 2, "horizon": "days", "vol": "none"}}]}}]}}

允许的标的：{universe}"""


class JsonModel(Protocol):
    def complete_json(
        self, system: str, user: str, *, max_tokens: int
    ) -> tuple[dict[str, Any], dict[str, Any]]: ...


@dataclass(frozen=True)
class Impact:
    symbol: str
    direction: int  # 1 bullish, -1 bearish, 0 volatility only
    magnitude: int  # 1-3
    horizon: str  # intraday / days / weeks
    vol: str  # up / down / none

    def to_dict(self) -> dict[str, Any]:
        return {"symbol": self.symbol, "direction": self.direction, "magnitude": self.magnitude,
                "horizon": self.horizon, "vol": self.vol}


@dataclass(frozen=True)
class NewsTag:
    news_id: int
    category: str
    importance: int
    summary: str
    impacts: tuple[Impact, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {"category": self.category, "category_name": CATEGORIES.get(self.category, self.category),
                "importance": self.importance, "summary": self.summary,
                "impacts": [impact.to_dict() for impact in self.impacts]}

    @classmethod
    def from_row(cls, news_id: int, payload: Mapping[str, Any]) -> "NewsTag":
        return cls(
            news_id=news_id,
            category=str(payload.get("category") or "other"),
            importance=int(payload.get("importance") or 0),
            summary=str(payload.get("summary") or ""),
            impacts=tuple(Impact(**impact) for impact in payload.get("impacts") or ()),
        )


@dataclass
class TaggingResult:
    tags: list[NewsTag] = field(default_factory=list)
    failed_ids: list[int] = field(default_factory=list)
    calls: int = 0
    prompt_tokens: int = 0
    completion_tokens: int = 0
    cost_cny: float = 0.0
    errors: list[str] = field(default_factory=list)

    def usage(self) -> dict[str, Any]:
        return {"calls": self.calls, "prompt_tokens": self.prompt_tokens,
                "completion_tokens": self.completion_tokens, "cost_cny": round(self.cost_cny, 4)}


class NewsTagger:
    def __init__(
        self,
        model: JsonModel,
        universe: Sequence[str],
        *,
        batch_size: int = 40,
        workers: int = 4,
        max_tokens: int = 6000,
    ) -> None:
        self.model = model
        self.universe = tuple(universe)
        self.batch_size = batch_size
        self.workers = workers
        self.max_tokens = max_tokens
        # Constant prefix: DeepSeek bills repeated prefixes at the cheap cache-hit rate.
        self.system = _PROMPT.format(
            categories=" / ".join(f"{key}（{name}）" for key, name in CATEGORIES.items()),
            max_impacts=MAX_IMPACTS,
            universe=", ".join(self.universe),
        )

    def tag(self, items: Sequence[NewsItem]) -> TaggingResult:
        items = [item for item in items if item.id is not None]
        batches = [items[i:i + self.batch_size] for i in range(0, len(items), self.batch_size)]
        result = TaggingResult()
        if not batches:
            return result
        with ThreadPoolExecutor(max_workers=max(1, min(self.workers, len(batches)))) as pool:
            for tags, failed, usage, error in pool.map(self._tag_batch, batches):
                result.tags.extend(tags)
                result.failed_ids.extend(failed)
                result.calls += 1
                result.prompt_tokens += int(usage.get("prompt_tokens") or 0)
                result.completion_tokens += int(usage.get("completion_tokens") or 0)
                result.cost_cny += float(usage.get("cost_cny") or 0.0)
                if error:
                    result.errors.append(error)
        return result

    def _tag_batch(
        self, batch: Sequence[NewsItem]
    ) -> tuple[list[NewsTag], list[int], dict[str, Any], str | None]:
        numbers = {number: item.id for number, item in enumerate(batch, start=1)}
        lines = []
        for number, item in enumerate(batch, start=1):
            stamp = item.published_at.astimezone(BEIJING).strftime("%m-%d %H:%M")
            text = re.sub(r"\s+", " ", item.text)[:300]
            hint = f" [{','.join(item.symbols[:3])}]" if item.symbols else ""
            lines.append(f"[{number}] {stamp} | {item.source}{hint} | {text}")
        try:
            payload, usage = self.model.complete_json(
                self.system, "\n".join(lines), max_tokens=self.max_tokens
            )
        except Exception as exc:
            log.warning("news tagging batch failed: %s", exc)
            return [], [item.id for item in batch], {}, str(exc)[:200]
        tags = parse_tags(payload, numbers, self.universe)
        tagged = {tag.news_id for tag in tags}
        return tags, [news_id for news_id in numbers.values() if news_id not in tagged], usage, None


def parse_tags(
    payload: Mapping[str, Any], numbers: Mapping[int, int], universe: Sequence[str]
) -> list[NewsTag]:
    """Validate model output strictly; malformed fields are clamped or dropped, never guessed."""

    allowed = set(universe)
    rows = payload.get("items") if isinstance(payload, Mapping) else None
    tags: dict[int, NewsTag] = {}
    for row in rows if isinstance(rows, list) else ():
        if not isinstance(row, Mapping):
            continue
        try:
            news_id = numbers[int(row.get("id"))]
        except (KeyError, TypeError, ValueError):
            continue
        category = str(row.get("category") or "other").strip()
        importance = _clamp(row.get("importance"), 0, 3, 0)
        impacts: list[Impact] = []
        seen: set[str] = set()
        for raw in row.get("impacts") or () if importance > 0 else ():
            if not isinstance(raw, Mapping):
                continue
            symbol = str(raw.get("symbol") or "").strip().upper()
            if not _TICKER.match(symbol) or symbol not in allowed or symbol in seen:
                continue
            direction = _clamp(raw.get("direction"), -1, 1, 0)
            vol = str(raw.get("vol") or "none").lower()
            vol = vol if vol in VOL_EFFECTS else "none"
            if direction == 0 and vol == "none":
                continue  # an impact with neither direction nor volatility says nothing
            horizon = str(raw.get("horizon") or "days").lower()
            impacts.append(Impact(
                symbol=symbol,
                direction=direction,
                magnitude=_clamp(raw.get("magnitude"), 1, 3, 1),
                horizon=horizon if horizon in HORIZONS else "days",
                vol=vol,
            ))
            seen.add(symbol)
            if len(impacts) >= MAX_IMPACTS:
                break
        tags[news_id] = NewsTag(
            news_id=news_id,
            category=category if category in CATEGORIES else "other",
            importance=importance,
            summary=str(row.get("summary") or "").strip()[:80],
            impacts=tuple(impacts),
        )
    return list(tags.values())


def _clamp(value: Any, low: int, high: int, default: int) -> int:
    try:
        number = int(round(float(value)))
    except (TypeError, ValueError):
        return default
    return max(low, min(high, number))


__all__ = [
    "CATEGORIES",
    "Impact",
    "JsonModel",
    "NewsTag",
    "NewsTagger",
    "TaggingResult",
    "parse_tags",
]
