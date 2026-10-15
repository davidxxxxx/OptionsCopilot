"""Fail-safe composition for the optional news classification model."""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal, Mapping, Protocol

from options_copilot.llm.cost_meter import CostMeter, CostPolicy
from options_copilot.llm.deepseek import DeepSeekClient

from .classifier import CachedNewsClassifier, FailSafeNewsClassifier, NewsClassifier
from .deepseek import DeepSeekNewsClassifier


_MAXIMUM_REQUEST_BYTES = 65_536


class SecretReader(Protocol):
    def get(self, name: str) -> str | None: ...


def build_optional_news_classifier(
    *,
    enabled: bool,
    secrets: SecretReader,
    cost_state_path: str | Path,
    client_factory: Callable[..., object] = DeepSeekClient,
) -> NewsClassifier | None:
    """Build a cost-bounded classifier only after explicit local enablement.

    Missing or unreadable credentials leave the deterministic runtime fallback
    active.  The model is capped independently from Trade Copilot and cannot
    become a production or trading dependency.
    """

    if not isinstance(enabled, bool):
        raise TypeError("news model enablement must be a bool")
    if not enabled:
        return None
    try:
        api_key = secrets.get("DEEPSEEK_API_KEY")
    except Exception:
        return None
    if not _valid_secret(api_key):
        return None
    meter = CostMeter(
        CostPolicy(
            flash_call_cap=100,
            pro_call_cap=1,
            daily_spend_cap_usd=0.25,
        ),
        state_path=Path(cost_state_path),
    )
    client = client_factory(
        api_key=api_key,
        cost_meter=meter,
        timeout_seconds=6.5,
        max_completion_tokens=600,
        max_request_bytes=_MAXIMUM_REQUEST_BYTES,
    )
    primary = CachedNewsClassifier(DeepSeekNewsClassifier(client))  # type: ignore[arg-type]
    return FailSafeNewsClassifier(primary)


def build_optional_shadow_news_classifier(
    *,
    enabled: bool,
    secrets: SecretReader,
    cost_state_path: str | Path,
    client_factory: Callable[..., object] = DeepSeekClient,
) -> NewsClassifier | None:
    """Build the strict classifier used only by the independent shadow lane.

    The production news read model deliberately uses deterministic rules.  A
    deterministic fallback inside this shadow lane would look like a model
    response and hide the fixed DeepSeek failure code, so failures remain
    visible to the bounded shadow coordinator instead.  Successful immutable
    inputs are still cached and every caller remains ``SUPPORTING_ONLY``.
    """

    classifier = build_optional_news_classifier(
        enabled=enabled,
        secrets=secrets,
        cost_state_path=cost_state_path,
        client_factory=client_factory,
    )
    if classifier is None:
        return None
    if not isinstance(classifier, FailSafeNewsClassifier):
        raise TypeError("optional news classifier composition is invalid")
    return classifier.primary


@dataclass(frozen=True, slots=True)
class Phase2AdvisoryComposition:
    """Fail-closed optional composition state with no action authority."""

    adapter: object | None
    fallback_reason: Literal["MODEL_DISABLED", "MODEL_EVALUATION_PENDING"]
    decision_authority: Literal["SUPPORTING_ONLY"] = field(
        default="SUPPORTING_ONLY",
        init=False,
    )
    approval_eligible: Literal[False] = field(default=False, init=False)
    instruction_creation_allowed: Literal[False] = field(default=False, init=False)
    order_allowed: Literal[False] = field(default=False, init=False)


def build_optional_phase2_advisory(
    *,
    enabled: bool,
    secrets: SecretReader,
    cost_state_path: str | Path,
    readiness_evidence: Mapping[str, object] | None,
    client_factory: Callable[..., object] = DeepSeekClient,
) -> Phase2AdvisoryComposition:
    """Stop before all secret, cost, client, and transport work in Phase 2.

    Candidate manifests, actor labels, and self-hashes cannot establish the
    independently verified human, listed-options, compatibility, or current
    pricing evidence required to construct a client. No such verified evidence
    type is created in Phase 2, so requested enablement remains pending.
    """

    if not isinstance(enabled, bool):
        raise TypeError("advisory model enablement must be a bool")
    if not enabled:
        return Phase2AdvisoryComposition(
            adapter=None,
            fallback_reason="MODEL_DISABLED",
        )
    # These values intentionally remain untouched until a later phase provides
    # a separately verified readiness authority rather than a caller mapping.
    _ = secrets, cost_state_path, readiness_evidence, client_factory
    return Phase2AdvisoryComposition(
        adapter=None,
        fallback_reason="MODEL_EVALUATION_PENDING",
    )


def _valid_secret(value: object) -> bool:
    return (
        isinstance(value, str)
        and value == value.strip()
        and 1 <= len(value) <= 4_096
        and all(32 <= ord(character) < 127 for character in value)
    )


__all__ = [
    "Phase2AdvisoryComposition",
    "SecretReader",
    "build_optional_news_classifier",
    "build_optional_shadow_news_classifier",
    "build_optional_phase2_advisory",
]
