"""Read-only, auditable news classification, calendar, and ranking contracts."""

from .advisory_adapter import Phase2AdvisoryAdapter, build_fallback_advisory
from .advisory_models import (
    AdvisoryFallbackReason,
    AdvisorySlice,
    ModelAdvisoryPayload,
    NormalizedAdvisory,
    ObservedFact,
)
from .composition import Phase2AdvisoryComposition, build_optional_phase2_advisory
from .models import (
    AnalyzedNews,
    CalendarEvent,
    EventCategory,
    ImpactDirection,
    NewsAuthority,
    NewsInput,
    OptionTradabilityInput,
    ScoreBand,
)
from .service import NewsAnalysisService
from .weekly_brief import (
    SourceHealthStatus,
    WeeklyBrief,
    WeeklyBriefEvidenceItem,
    WeeklyBriefSlotDecision,
    WeeklyBriefSlotStatus,
    WeeklyBriefSourceHealth,
    evaluate_weekly_brief_slot,
    weekly_brief_source_bundle_hash,
)
from .weekly_brief_store import (
    StoredWeeklyBrief,
    WeeklyBriefAppendResult,
    WeeklyBriefStore,
    WeeklyBriefStoreConflict,
    WeeklyBriefStoreCorruption,
)
from .weekly_brief_runtime import (
    WEEKLY_BRIEF_READ_MODEL_SCHEMA,
    WeeklyBriefRuntime,
)
from .research_session_calendar import (
    BoundedResearchSessionCalendar,
    ResearchSessionCalendarSnapshot,
)
from .weekly_brief_builder import WeeklyBriefInputs, build_weekly_brief_inputs
from .reaction_specs import (
    EventFamily,
    EventRole,
    FAMILY_SPECS,
    MeasureIdentity,
    ParentEventIdentity,
    SupportState,
    assess_support,
    classify_event_family,
)

__all__ = [
    "AdvisoryFallbackReason",
    "AdvisorySlice",
    "AnalyzedNews",
    "CalendarEvent",
    "EventCategory",
    "EventFamily",
    "EventRole",
    "FAMILY_SPECS",
    "ImpactDirection",
    "ModelAdvisoryPayload",
    "MeasureIdentity",
    "NewsAnalysisService",
    "NewsAuthority",
    "NewsInput",
    "NormalizedAdvisory",
    "ObservedFact",
    "OptionTradabilityInput",
    "Phase2AdvisoryAdapter",
    "Phase2AdvisoryComposition",
    "ParentEventIdentity",
    "ScoreBand",
    "SourceHealthStatus",
    "SupportState",
    "BoundedResearchSessionCalendar",
    "StoredWeeklyBrief",
    "ResearchSessionCalendarSnapshot",
    "WeeklyBrief",
    "WeeklyBriefEvidenceItem",
    "WeeklyBriefSlotDecision",
    "WeeklyBriefSlotStatus",
    "WeeklyBriefSourceHealth",
    "WEEKLY_BRIEF_READ_MODEL_SCHEMA",
    "WeeklyBriefAppendResult",
    "WeeklyBriefInputs",
    "WeeklyBriefStore",
    "WeeklyBriefStoreConflict",
    "WeeklyBriefStoreCorruption",
    "WeeklyBriefRuntime",
    "build_fallback_advisory",
    "build_optional_phase2_advisory",
    "assess_support",
    "classify_event_family",
    "evaluate_weekly_brief_slot",
    "weekly_brief_source_bundle_hash",
    "build_weekly_brief_inputs",
]
