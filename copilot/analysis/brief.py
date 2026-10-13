"""Macro brief: a short Chinese summary written only when new important news arrives."""
from __future__ import annotations

import logging
from collections.abc import Sequence
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from copilot.news.models import NewsItem
from .tagging import CATEGORIES, JsonModel, NewsTag

log = logging.getLogger(__name__)
BEIJING = ZoneInfo("Asia/Shanghai")
MAX_ITEMS = 40

SYSTEM_PROMPT = """你是美股期权研究助理。下面是已经分类、按重要性排序的近期新闻摘要，末尾可能附有未来重大宏观事件。
请用中文写 3-6 句话，总结当前宏观/政策/市场主线，以及对美股大盘、利率、主要板块的含义；可以提及给出的未来重大宏观事件。
只依据给出的新闻和事件，不编造数据，不给交易指令。只输出 JSON：{"macro_summary": "..."}"""


def needs_refresh(state: dict[str, Any] | None, important: Sequence[tuple[NewsItem, NewsTag]]) -> bool:
    """Regenerate only if an important item arrived after the last brief."""

    if not important:
        return False
    newest = max(item.id or 0 for item, _ in important)
    return state is None or newest > int(state.get("max_news_id") or 0)


def write_brief(
    model: JsonModel,
    important: Sequence[tuple[NewsItem, NewsTag]],
    *,
    now: datetime,
    upcoming: Sequence[dict[str, Any]] = (),
) -> tuple[dict[str, Any], dict[str, Any]]:
    ranked = sorted(important, key=lambda pair: (pair[1].importance, pair[0].published_at), reverse=True)
    lines = []
    for item, tag in ranked[:MAX_ITEMS]:
        stamp = item.published_at.astimezone(BEIJING).strftime("%m-%d %H:%M")
        impacts = "，".join(
            f"{i.symbol}{'↑' if i.direction > 0 else '↓' if i.direction < 0 else '~'}" for i in tag.impacts
        )
        lines.append(
            f"- {stamp} [{CATEGORIES.get(tag.category, tag.category)}/重要性{tag.importance}] "
            f"{tag.summary or item.title[:80]}{'（' + impacts + '）' if impacts else ''}"
        )
    if upcoming:
        lines.append("\n未来 7 天重大数据/会议：")
        lines.extend(
            f"- {event['date']} {event['time_ny']}（纽约）{event['name']}"
            for event in upcoming
        )
    payload, usage = model.complete_json(SYSTEM_PROMPT, "\n".join(lines), max_tokens=800)
    text = str(payload.get("macro_summary") or "").strip()[:1500]
    state = {
        "text": text,
        "generated_at": now.isoformat(timespec="seconds"),
        "max_news_id": max(item.id or 0 for item, _ in important),
    }
    return state, usage


__all__ = ["needs_refresh", "write_brief"]
