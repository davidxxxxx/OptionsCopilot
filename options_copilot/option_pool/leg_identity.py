"""Normalize equivalent broker/domain leg vocabulary without changing evidence."""
from __future__ import annotations


def normalise_option_right(value: object) -> str:
    token = value.strip().upper() if isinstance(value, str) else ""
    if token in {"C", "CALL"}:
        return "CALL"
    if token in {"P", "PUT"}:
        return "PUT"
    raise ValueError("unknown option right")


def normalise_option_side(value: object) -> str:
    token = value.strip().upper() if isinstance(value, str) else ""
    if token in {"BUY", "LONG"}:
        return "LONG"
    if token in {"SELL", "SHORT"}:
        return "SHORT"
    raise ValueError("unknown option side")


__all__ = ["normalise_option_right", "normalise_option_side"]
