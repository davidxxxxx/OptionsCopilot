"""DeepSeek chat-completions client returning one JSON object."""
from __future__ import annotations

import json
import re
from datetime import datetime, time, timezone
from typing import Any
from zoneinfo import ZoneInfo

from copilot.http import HttpError, request_json

URL = "https://api.deepseek.com/chat/completions"
HOSTS = ("api.deepseek.com",)
BEIJING = ZoneInfo("Asia/Shanghai")
# CNY per million tokens at peak: (input cache hit, input cache miss, output). Off-peak is
# half. Source: api-docs.deepseek.com/zh-cn/quick_start/pricing (checked 2026-10-09).
PEAK_PRICES_CNY = {
    "deepseek-flash": (0.04, 2.0, 8.0),
    "deepseek-v4-flash": (0.04, 2.0, 8.0),  # legacy name, billed as Flash
    "deepseek-v4-pro": (0.30, 9.0, 27.0),
}
_PEAK_WINDOWS = ((time(9, 0), time(12, 0)), (time(14, 0), time(18, 0)))


class LlmError(RuntimeError):
    pass


class DeepSeekClient:
    def __init__(self, api_key: str, *, model: str, timeout: float = 150.0) -> None:
        if not api_key:
            raise LlmError("DeepSeek API key missing")
        self._key = api_key
        self.model = model
        self._timeout = timeout

    def complete_json(self, system: str, user: str, *, max_tokens: int) -> tuple[dict[str, Any], dict[str, Any]]:
        body = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "response_format": {"type": "json_object"},
            "thinking": {"type": "disabled"},
            "temperature": 0.2,
            "max_tokens": max_tokens,
            "stream": False,
        }
        try:
            response = request_json(
                "POST", URL, allowed_hosts=HOSTS,
                headers={"Authorization": f"Bearer {self._key}"},
                body=body, timeout=self._timeout, max_bytes=2 * 1024 * 1024,
            )
        except HttpError as exc:
            raise LlmError(f"DeepSeek 请求失败: {exc}") from None
        try:
            content = response["choices"][0]["message"]["content"]
        except (KeyError, IndexError, TypeError):
            raise LlmError("DeepSeek 返回格式异常") from None
        usage = response.get("usage") if isinstance(response.get("usage"), dict) else {}
        return parse_json_object(content), _usage(self.model, usage)


def parse_json_object(text: str) -> dict[str, Any]:
    cleaned = re.sub(r"^```(?:json)?\s*|\s*```$", "", str(text or "").strip())
    try:
        value = json.loads(cleaned)
    except json.JSONDecodeError:
        start, end = cleaned.find("{"), cleaned.rfind("}")
        if start < 0 or end <= start:
            raise LlmError("模型输出不是 JSON") from None
        try:
            value = json.loads(cleaned[start:end + 1])
        except json.JSONDecodeError:
            raise LlmError("模型输出不是 JSON") from None
    if not isinstance(value, dict):
        raise LlmError("模型输出不是 JSON 对象")
    return value


def is_peak(moment: datetime) -> bool:
    """DeepSeek peak hours: Beijing weekdays 09-12 and 14-18 (public holidays not modelled)."""

    local = moment.astimezone(BEIJING)
    return local.weekday() < 5 and any(start <= local.time() < end for start, end in _PEAK_WINDOWS)


def call_cost_cny(
    model: str, *, cache_hit: int, cache_miss: int, completion: int, moment: datetime
) -> float:
    hit_price, miss_price, output_price = PEAK_PRICES_CNY.get(model, (0.0, 0.0, 0.0))
    factor = 1.0 if is_peak(moment) else 0.5
    return (cache_hit * hit_price + cache_miss * miss_price + completion * output_price) / 1_000_000 * factor


def _usage(model: str, usage: dict[str, Any]) -> dict[str, Any]:
    prompt = int(usage.get("prompt_tokens") or 0)
    completion = int(usage.get("completion_tokens") or 0)
    cache_hit = int(usage.get("prompt_cache_hit_tokens") or 0)
    cache_miss = int(usage.get("prompt_cache_miss_tokens") or max(0, prompt - cache_hit))
    now = datetime.now(timezone.utc)
    return {
        "model": model,
        "prompt_tokens": prompt,
        "completion_tokens": completion,
        "cache_hit_tokens": cache_hit,
        "peak": is_peak(now),
        "cost_cny": round(
            call_cost_cny(model, cache_hit=cache_hit, cache_miss=cache_miss,
                          completion=completion, moment=now),
            4,
        ),
    }


__all__ = ["DeepSeekClient", "LlmError", "call_cost_cny", "is_peak", "parse_json_object"]
