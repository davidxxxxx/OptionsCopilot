"""Provider API keys from a git-ignored local JSON file, overridable by environment."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path

ENVIRONMENT_NAMES = {
    "jin10_mcp_token": "JIN10_MCP_TOKEN",
    "finnhub_api_key": "FINNHUB_API_KEY",
    "deepseek_api_key": "DEEPSEEK_API_KEY",
    "alpha_vantage_api_key": "ALPHA_VANTAGE_API_KEY",
}


@dataclass(frozen=True)
class Secrets:
    """Holds key values; repr never shows them."""

    _values: dict[str, str] = field(default_factory=dict, repr=False)

    def get(self, name: str) -> str | None:
        value = self._values.get(name)
        return value or None

    def configured(self) -> dict[str, bool]:
        return {name: bool(self._values.get(name)) for name in ENVIRONMENT_NAMES}

    def __repr__(self) -> str:
        names = ", ".join(sorted(name for name, ok in self.configured().items() if ok))
        return f"Secrets(configured=[{names}])"


def load_secrets(path: Path) -> Secrets:
    values: dict[str, str] = {}
    if path.exists():
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("secrets file must contain a JSON object")
        for name in ENVIRONMENT_NAMES:
            value = payload.get(name)
            if isinstance(value, str) and value.strip():
                values[name] = value.strip()
    for name, env_name in ENVIRONMENT_NAMES.items():
        value = os.environ.get(env_name)
        if value and value.strip():
            values[name] = value.strip()
    return Secrets(values)


__all__ = ["ENVIRONMENT_NAMES", "Secrets", "load_secrets"]
