"""Closed, finite-risk option strategy construction.

This package deliberately contains no broker, order, or LLM integration.
"""

from .generator import (
    GenerationResult,
    GeneratedStrategyCandidate,
    StrategyCandidateGenerator,
    StrategyGenerationRequest,
    option_quote_liquidity_assessment,
)
from .templates import (
    ExitPlan,
    StrategyKind,
    StrategyTemplate,
    StrategyTemplateRegistry,
    TemplateLeg,
    TemplateValidationError,
)

__all__ = [
    "ExitPlan",
    "GeneratedStrategyCandidate",
    "GenerationResult",
    "StrategyCandidateGenerator",
    "StrategyGenerationRequest",
    "option_quote_liquidity_assessment",
    "StrategyKind",
    "StrategyTemplate",
    "StrategyTemplateRegistry",
    "TemplateLeg",
    "TemplateValidationError",
]
