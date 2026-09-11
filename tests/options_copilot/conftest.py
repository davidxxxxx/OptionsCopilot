"""Options Copilot pytest marker registration."""

from __future__ import annotations


def pytest_configure(config: object) -> None:
    add_marker = getattr(config, "addinivalue_line")
    add_marker(
        "markers",
        "human_authority: requires an externally verified explicit human P9 authority decision",
    )
