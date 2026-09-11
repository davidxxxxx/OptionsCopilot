from __future__ import annotations

import asyncio

from options_copilot.api.app import (
    OptionsCopilotServices,
    _normalise_reaction_provider,
    create_app,
)


def test_reaction_failure_preserves_independently_valid_schedule_health() -> None:
    result = _normalise_reaction_provider(
        {
            "status": "READY",
            "decision": "OBSERVATION_ONLY",
            "reason": None,
            "ledger_count": 0,
            "matched_count": 0,
            "ignored_count": 0,
            "supported_event_ids": ["future-cpi"],
            "supported_count": 1,
            "eligible_event_ids": ["future-cpi"],
            "eligible_count": 1,
            "unsupported_count": 0,
            "last_attempt": "2026-08-22T03:00:00+00:00",
            "schedule_refresh_status": "READY",
            "schedule_refresh_reason": "LATEST_GLOBAL_HEALTH",
            "schedule_hash": "d" * 64,
        }
    )

    assert result["status"] == "UNAVAILABLE"
    assert result["reason"] == "REACTION_LEDGER_COVERAGE_INCOMPLETE"
    assert result["schedule_refresh_status"] == "READY"
    assert result["schedule_refresh_reason"] == "LATEST_GLOBAL_HEALTH"
    assert result["schedule_hash"] == "d" * 64


def test_official_calendar_projection_preserves_time_and_provenance_without_authority() -> None:
    digest = "a" * 64
    reaction_hash = "f" * 64
    provider = lambda: {
        "calendar": [
            {
                "id": "fomc-2026-09",
                "title": "FOMC rate decision",
                "category": "FOMC",
                "event_at": "2026-08-11T14:00:00-04:00",
                "event_date": "2026-08-11",
                "event_timezone": "America/New_York",
                "schedule_precision": "EXACT",
                "windows": ["NEXT_WEEK", "FUTURE_TWO_WEEKS"],
                "source": "Federal Reserve",
                "status": "CONFIRMED",
                "importance": "HIGH",
                "country": "US",
                "first_seen_at": "2026-08-04T20:00:00+00:00",
                "observed_at": "2026-08-04T20:00:01+00:00",
                "content_hash": digest,
                "record_hash": "b" * 64,
                "calendar_origin": "OFFICIAL",
                "reaction_identity_hash": reaction_hash,
                "reaction_identity_provenance": {
                    "calendar_record_hash": "must-remain-internal",
                    "observed_at": "must-remain-internal",
                },
                "reaction": {
                    "status": "READY",
                    "current_stage": "OPTION_REEVALUATED",
                    "analysis_available": True,
                    "event_id": "fomc-2026-09",
                    "event_hash": reaction_hash,
                    "asof": "2026-08-11T18:05:11+00:00",
                    "head_hash": "9" * 64,
                    "transition_count": 6,
                    "expectation": {
                        "content_hash": "1" * 64,
                        "metric": "federal_funds_target_pct",
                        "expected_value": "5.25",
                        "unit": "percent",
                        "observed_at": "2026-08-11T17:55:00+00:00",
                        "vintage": "2026-08-11T17:55:00Z",
                        "provider": "must-not-cross-the-api",
                        "api_key": "must-not-cross-the-api",
                    },
                    "release": {
                        "content_hash": "2" * 64,
                        "actual_value": "5.00",
                        "unit": "percent",
                        "released_at": "2026-08-11T18:00:00+00:00",
                        "vintage_at": "2026-08-11T18:00:00+00:00",
                        "captured_at": "2026-08-11T18:00:05+00:00",
                        "revision": 0,
                        "supersedes_hash": None,
                        "release_chain_hashes": ["2" * 64],
                        "official_source": "must-not-cross-the-api",
                    },
                    "surprise": {
                        "content_hash": "3" * 64,
                        "expectation_hash": "1" * 64,
                        "release_hash": "2" * 64,
                        "delta": "-0.25",
                        "relative_delta": "-0.047619",
                        "assessed_at": "2026-08-11T18:00:06+00:00",
                        "supporting_evidence_hashes": ["7" * 64],
                        "llm_prompt": "must-not-cross-the-api",
                    },
                    "market_reaction": {
                        "content_hash": "4" * 64,
                        "release_hash": "2" * 64,
                        "window_start": "2026-08-11T18:00:00+00:00",
                        "window_end": "2026-08-11T18:05:00+00:00",
                        "evidence_asof": "2026-08-11T18:05:00+00:00",
                        "observed_at": "2026-08-11T18:05:01+00:00",
                        "metrics": {"order": "must-not-cross-the-api"},
                    },
                    "option_reevaluation": {
                        "content_hash": "5" * 64,
                        "market_reaction_hash": "4" * 64,
                        "option_id": "SPY-20260811-500-C",
                        "candidate_hash": "6" * 64,
                        "evidence_asof": "2026-08-11T18:05:10+00:00",
                        "observed_at": "2026-08-11T18:05:11+00:00",
                        "input_evidence_hashes": ["8" * 64],
                        "result": {"approval_eligible": True},
                        "submit_order": True,
                    },
                    "decision": "OBSERVATION_ONLY",
                    "reasons": [],
                    "decision_authority": "EXECUTION",
                    "approval_eligible": True,
                    "instruction_creation_allowed": True,
                    "order_creation_allowed": True,
                    "credential": "must-not-cross-the-api",
                },
                "provenance": [
                    {
                        "source": "Federal Reserve",
                        "source_id": "fomc-2026-09",
                        "source_url": "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm",
                        "first_seen_at": "2026-08-04T20:00:00+00:00",
                        "observed_at": "2026-08-04T20:00:01+00:00",
                        "content_hash": digest,
                        "decision_authority": "EXECUTION",
                    }
                ],
                "decision_authority": "EXECUTION",
                "approval_eligible": True,
            }
        ],
        "provider": {"name": "official-calendar", "status": "DEGRADED"},
        "window_start": "2026-08-04T20:00:00+00:00",
        "window_end": "2026-08-18T20:00:00+00:00",
        "decision": "NO_TRADE",
        "decision_authority": "EXECUTION",
        "reasons": ["OFFICIAL_SOURCE_DEGRADED"],
        "sources": [
            {
                "source": "Federal Reserve",
                "source_url": "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm?tracking=drop#fragment",
                "status": "DEGRADED",
                "reason": "REQUEST_TIMEOUT",
                "observed_at": "2026-08-04T20:00:01+00:00",
                "content_hash": "c" * 64,
                "event_count": 1,
                "duplicate_count": 2,
                "warnings": ["IDENTICAL_DUPLICATES_FOLDED"],
                "audit_hash": "e" * 64,
                "duplicate_record_hashes": ["must-not-cross-the-api"],
            }
        ],
        "snapshot_hash": "d" * 64,
        "reaction_provider": {
            "name": "untrusted-provider-name",
            "status": "READY",
            "decision": "OBSERVATION_ONLY",
            "reason": None,
            "ledger_count": 1,
            "matched_count": 1,
            "ignored_count": 2,
            "supported_event_ids": ["fomc-2026-09"],
            "supported_count": 1,
            "eligible_event_ids": ["fomc-2026-09"],
            "eligible_count": 1,
            "unsupported_count": 0,
            "last_attempt": "2026-08-11T18:05:11+00:00",
            "authorization": "must-not-cross-the-api",
            "order_submission_allowed": True,
        },
        "reaction_decision": "OBSERVATION_ONLY",
        "approval_eligible": True,
        "instruction_creation_allowed": True,
        "order_creation_allowed": True,
    }
    app = create_app(
        OptionsCopilotServices(
            health_provider=lambda: {},
            bootstrap_provider=lambda: {},
            candidates_provider=lambda: [],
            positions_provider=lambda: [],
            learning_provider=lambda: {},
            calendar_provider=provider,
        )
    )

    result = asyncio.run(_route(app, "/api/calendar")())

    assert result["decision"] == "NO_TRADE"
    assert result["decision_authority"] == "SUPPORTING_ONLY"
    assert result["approval_eligible"] is False
    assert result["instruction_creation_allowed"] is False
    assert result["order_creation_allowed"] is False
    assert result["window_start"] == "2026-08-04T20:00:00+00:00"
    assert result["window_end"] == "2026-08-18T20:00:00+00:00"
    assert result["snapshot_hash"] == "d" * 64
    assert result["reasons"] == ["OFFICIAL_SOURCE_DEGRADED"]
    assert result["reaction_decision"] == "OBSERVATION_ONLY"
    assert result["reaction_provider"] == {
        "name": "event-reaction-provider",
        "status": "READY",
        "decision": "OBSERVATION_ONLY",
        "reason": None,
        "ledger_count": 1,
        "matched_count": 1,
        "ignored_count": 2,
        "supported_event_ids": ["fomc-2026-09"],
        "supported_count": 1,
        "eligible_event_ids": ["fomc-2026-09"],
        "eligible_count": 1,
        "unsupported_count": 0,
        "last_attempt": "2026-08-11T18:05:11+00:00",
        "event_count": 1,
        "measure_count": 0,
        "capture_spec_count": 0,
        "captured_release_vintage_count": 0,
        "captured_measure_count": 0,
        "capture_eligible_count": 0,
        "surprise_ready_count": 0,
        "progressed_event_count": 0,
        "next_eligible_release_at": "NEXT_ELIGIBLE_RELEASE_UNKNOWN",
        "schedule_refresh_status": "UNKNOWN",
        "schedule_refresh_reason": None,
        "schedule_hash": None,
        "descriptor_wait_count": 0,
        "next_action": "OBSERVE_CURRENT_ELIGIBLE_EVENTS",
        "support_matrix": [],
            "worker_health": {},
            "lifecycle_supersessions": {},
            "decision_authority": "SUPPORTING_ONLY",
        "approval_eligible": False,
        "instruction_creation_allowed": False,
        "order_creation_allowed": False,
    }
    assert result["sources"][0] == {
        "source": "Federal Reserve",
        "source_url": "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm",
        "status": "DEGRADED",
        "reason": "REQUEST_TIMEOUT",
        "observed_at": "2026-08-04T20:00:01+00:00",
        "content_hash": "c" * 64,
        "event_count": 1,
        "duplicate_count": 2,
        "warnings": ["IDENTICAL_DUPLICATES_FOLDED"],
        "audit_hash": "e" * 64,
        "decision_authority": "SUPPORTING_ONLY",
    }
    assert "duplicate_record_hashes" not in result["sources"][0]
    event = result["calendar"][0]
    assert event["event_date"] == "2026-08-11"
    assert event["event_timezone"] == "America/New_York"
    assert event["schedule_precision"] == "EXACT"
    assert event["windows"] == ["NEXT_WEEK", "FUTURE_TWO_WEEKS"]
    assert event["content_hash"] == digest
    assert event["record_hash"] == "b" * 64
    assert event["calendar_origin"] == "OFFICIAL"
    assert event["reaction_identity_hash"] == reaction_hash
    assert "reaction_identity_provenance" not in event
    assert event["reaction"]["current_stage"] == "OPTION_REEVALUATED"
    assert event["reaction"]["event_hash"] == reaction_hash
    assert event["reaction"]["expectation"] == {
        "content_hash": "1" * 64,
        "metric": "federal_funds_target_pct",
        "expected_value": "5.25",
        "unit": "percent",
        "observed_at": "2026-08-11T17:55:00+00:00",
        "vintage": "2026-08-11T17:55:00Z",
        "decision_authority": "SUPPORTING_ONLY",
    }
    assert event["reaction"]["release"]["revision"] == 0
    assert event["reaction"]["surprise"]["delta"] == "-0.25"
    assert event["reaction"]["market_reaction"]["window_end"] == (
        "2026-08-11T18:05:00+00:00"
    )
    assert event["reaction"]["option_reevaluation"]["candidate_hash"] == "6" * 64
    assert set(event["reaction"]["option_reevaluation"]) == {
        "content_hash",
        "market_reaction_hash",
        "option_id",
        "candidate_hash",
        "evidence_asof",
        "observed_at",
        "input_evidence_hashes",
        "decision_authority",
    }
    assert event["reaction"]["decision_authority"] == "SUPPORTING_ONLY"
    assert event["reaction"]["approval_eligible"] is False
    assert event["reaction"]["instruction_creation_allowed"] is False
    assert event["reaction"]["order_creation_allowed"] is False
    assert event["decision_authority"] == "SUPPORTING_ONLY"
    assert event["provenance"][0]["decision_authority"] == "SUPPORTING_ONLY"
    rendered = repr(result).lower()
    assert "instruction_creation_allowed': true" not in rendered
    assert "approval_eligible': true" not in rendered
    assert "order_creation_allowed': true" not in rendered
    assert "must-not-cross-the-api" not in rendered


def test_reaction_projection_rejects_misbound_malformed_and_execution_payloads() -> None:
    provider = lambda: {
        "calendar": [
            {
                "id": "cpi-2026-08",
                "title": "Consumer Price Index",
                "source": "Bureau of Labor Statistics",
                "event_at": "2026-08-12T12:30:00+00:00",
                "calendar_origin": "OFFICIAL",
                "reaction_identity_hash": "a" * 64,
                "reaction": {
                    "status": "READY",
                    "current_stage": "OPTION_REEVALUATED",
                    "event_id": "cpi-2026-08",
                    "event_hash": "b" * 64,
                    "decision": "OBSERVATION_ONLY",
                    "approval_eligible": True,
                    "instruction_creation_allowed": True,
                    "order_creation_allowed": True,
                    "broker_order": {"action": "BUY"},
                    "api_token": "must-not-cross-the-api",
                },
                "reaction_identity_provenance": {
                    "credential": "must-not-cross-the-api"
                },
                "execution_instruction": "must-not-cross-the-api",
            },
            {
                "id": "jobs-2026-08",
                "title": "Employment Situation",
                "source": "Bureau of Labor Statistics",
                "event_at": "2026-08-14T12:30:00+00:00",
                "calendar_origin": "OFFICIAL",
                "reaction_identity_hash": "not-a-digest",
                "reaction": {
                    "status": "READY",
                    "event_id": "jobs-2026-08",
                    "event_hash": "c" * 64,
                    "decision": "APPROVE_AND_EXECUTE",
                },
                "password": "must-not-cross-the-api",
            },
        ],
        "provider": {"name": "calendar", "status": "READY"},
        "decision": "OBSERVATION_ONLY",
        "reaction_provider": {
            "status": "READY",
            "decision": "OBSERVATION_ONLY",
            "ledger_count": 2,
            "matched_count": 2,
            "ignored_count": 0,
            "supported_event_ids": ["cpi-2026-08", "jobs-2026-08"],
            "supported_count": 2,
            "eligible_event_ids": ["cpi-2026-08", "jobs-2026-08"],
            "eligible_count": 2,
            "unsupported_count": 0,
            "last_attempt": "2026-08-11T18:05:11+00:00",
            "client_secret": "must-not-cross-the-api",
            "create_order": True,
        },
        "reaction_decision": "EXECUTE",
        "approval_eligible": True,
        "instruction_creation_allowed": True,
        "order_creation_allowed": True,
    }
    app = create_app(
        OptionsCopilotServices(
            health_provider=lambda: {},
            bootstrap_provider=lambda: {},
            candidates_provider=lambda: [],
            positions_provider=lambda: [],
            learning_provider=lambda: {},
            calendar_provider=provider,
        )
    )

    result = asyncio.run(_route(app, "/api/calendar")())

    assert result["reaction_decision"] == "NO_TRADE"
    assert result["reaction_provider"] == {
        "name": "event-reaction-provider",
        "status": "CONFLICTED",
        "decision": "NO_TRADE",
        "reason": "REACTION_API_BINDING_MISMATCH",
        "ledger_count": 2,
        "matched_count": 0,
        "ignored_count": 0,
        "supported_event_ids": ["cpi-2026-08", "jobs-2026-08"],
        "supported_count": 2,
        "eligible_event_ids": ["cpi-2026-08", "jobs-2026-08"],
        "eligible_count": 2,
        "unsupported_count": 0,
        "last_attempt": "2026-08-11T18:05:11+00:00",
        "event_count": 0,
        "measure_count": 0,
        "capture_spec_count": 0,
        "captured_release_vintage_count": 0,
        "captured_measure_count": 0,
        "capture_eligible_count": 0,
        "surprise_ready_count": 0,
        "progressed_event_count": 0,
        "next_eligible_release_at": "NEXT_ELIGIBLE_RELEASE_UNKNOWN",
        "schedule_refresh_status": "UNAVAILABLE",
        "schedule_refresh_reason": "REACTION_SCHEDULE_REFRESH_UNAVAILABLE",
        "schedule_hash": None,
        "descriptor_wait_count": 0,
        "next_action": "WAIT_NEXT_ELIGIBLE_RELEASE",
        "support_matrix": [],
            "worker_health": {},
            "lifecycle_supersessions": {},
            "decision_authority": "SUPPORTING_ONLY",
        "approval_eligible": False,
        "instruction_creation_allowed": False,
        "order_creation_allowed": False,
    }
    conflicted, unavailable = result["calendar"]
    assert conflicted["reaction"]["status"] == "CONFLICTED"
    assert conflicted["reaction"]["decision"] == "NO_TRADE"
    assert conflicted["reaction"]["reasons"] == [
        "REACTION_API_BINDING_MISMATCH"
    ]
    assert conflicted["reaction"]["expectation"] is None
    assert unavailable["reaction_identity_hash"] is None
    assert unavailable["reaction"]["status"] == "UNAVAILABLE"
    assert unavailable["reaction"]["decision"] == "NO_TRADE"
    assert unavailable["reaction"]["reasons"] == [
        "REACTION_EVENT_IDENTITY_UNAVAILABLE"
    ]
    assert result["approval_eligible"] is False
    assert result["instruction_creation_allowed"] is False
    assert result["order_creation_allowed"] is False
    rendered = repr(result).lower()
    for forbidden in (
        "must-not-cross-the-api",
        "broker_order",
        "execution_instruction",
        "api_token",
        "client_secret",
        "create_order",
        "password",
    ):
        assert forbidden not in rendered


def test_official_snapshot_unavailable_reason_survives_api_normalisation() -> None:
    event_hash = "a" * 64
    event_id = "bls-cpi-2026-08"
    reason = "OFFICIAL_CALENDAR_SNAPSHOT_UNAVAILABLE"
    provider = lambda: {
        "calendar": [
            {
                "id": event_id,
                "title": "Consumer Price Index",
                "source": "Bureau of Labor Statistics",
                "event_at": "2026-08-12T12:30:00+00:00",
                "calendar_origin": "OFFICIAL",
                "reaction_identity_hash": event_hash,
                "reaction": {
                    "status": "UNAVAILABLE",
                    "event_id": event_id,
                    "event_hash": event_hash,
                    "decision": "NO_TRADE",
                    "reasons": [reason],
                },
            }
        ],
        "reaction_provider": {
            "status": "UNAVAILABLE",
            "decision": "NO_TRADE",
            "reason": reason,
            "ledger_count": 0,
            "matched_count": 0,
            "ignored_count": 0,
        },
        "reaction_decision": "NO_TRADE",
    }
    app = create_app(
        OptionsCopilotServices(
            health_provider=lambda: {},
            bootstrap_provider=lambda: {},
            candidates_provider=lambda: [],
            positions_provider=lambda: [],
            learning_provider=lambda: {},
            calendar_provider=provider,
        )
    )

    result = asyncio.run(_route(app, "/api/calendar")())

    assert result["reaction_provider"]["reason"] == reason
    reaction = result["calendar"][0]["reaction"]
    assert reaction["status"] == "UNAVAILABLE"
    assert reaction["decision"] == "NO_TRADE"
    assert reaction["reasons"] == [reason]
    assert reaction["expectation"] is None
    assert reaction["release"] is None
    assert reaction["surprise"] is None
    assert reaction["market_reaction"] is None
    assert reaction["option_reevaluation"] is None
    assert reaction["approval_eligible"] is False
    assert reaction["instruction_creation_allowed"] is False
    assert reaction["order_creation_allowed"] is False


def test_supported_future_release_wait_survives_api_normalisation() -> None:
    event_hash = "b" * 64
    event_id = "bea-gdp-2026-08"
    reason = "WAIT_FOR_DECLARED_RELEASE_TIME"
    provider = lambda: {
        "calendar": [
            {
                "id": event_id,
                "event_id": event_id,
                "title": "Gross Domestic Product",
                "source": "Bureau of Economic Analysis",
                "event_at": "2026-08-26T12:30:00+00:00",
                "scheduled_at": "2026-08-26T12:30:00+00:00",
                "category": "MACRO",
                "schedule_precision": "EXACT",
                "calendar_origin": "OFFICIAL",
                "reaction_identity_hash": event_hash,
                "reaction": {
                    "status": "UNAVAILABLE",
                    "current_stage": None,
                    "analysis_available": False,
                    "event_id": event_id,
                    "event_hash": event_hash,
                    "asof": None,
                    "head_hash": None,
                    "transition_count": 0,
                    "expectation": None,
                    "release": None,
                    "surprise": None,
                    "market_reaction": None,
                    "option_reevaluation": None,
                    "decision": "OBSERVATION_ONLY",
                    "reasons": ["WAITING_DECLARED_RELEASE_TIME"],
                    "coverage": {
                        "family": "GDP",
                        "support_state": "SUPPORTED",
                        "supported": True,
                        "capture_eligible": False,
                        "surprise_eligible": False,
                        "progressed": False,
                        "next_action": reason,
                        "measure_count": 1,
                        "capture_count": 0,
                        "document_stage": "SCHEDULED",
                        "next_eligible_release_at": "2026-08-26T12:30:00+00:00",
                    },
                    "document_progression": {
                        "status": "SCHEDULED",
                        "capture_count": 0,
                        "next_action": reason,
                        "decision_authority": "SUPPORTING_ONLY",
                    },
                    "numeric_surprise": {
                        "status": "UNAVAILABLE",
                        "reason": "NUMERIC_SURPRISE_UNSUPPORTED",
                        "decision_authority": "SUPPORTING_ONLY",
                    },
                    "decision_authority": "SUPPORTING_ONLY",
                    "approval_eligible": False,
                    "instruction_creation_allowed": False,
                    "order_creation_allowed": False,
                },
            }
        ],
        "asof": "2026-08-22T04:42:40+00:00",
        "decision": "OBSERVATION_ONLY",
        "reaction_provider": {
            "status": "READY",
            "decision": "OBSERVATION_ONLY",
            "reason": "NO_ELIGIBLE_REACTION_EVENTS",
            "ledger_count": 0,
            "matched_count": 0,
            "ignored_count": 0,
            "supported_event_ids": [event_id],
            "supported_count": 1,
            "eligible_event_ids": [],
            "eligible_count": 0,
            "unsupported_count": 0,
        },
        "reaction_decision": "OBSERVATION_ONLY",
    }
    app = create_app(
        OptionsCopilotServices(
            health_provider=lambda: {},
            bootstrap_provider=lambda: {},
            candidates_provider=lambda: [],
            positions_provider=lambda: [],
            learning_provider=lambda: {},
            calendar_provider=provider,
        )
    )

    result = asyncio.run(_route(app, "/api/calendar")())

    reaction = result["calendar"][0]["reaction"]
    assert reaction["status"] == "UNAVAILABLE"
    assert reaction["decision"] == "OBSERVATION_ONLY"
    assert reaction["reasons"] == [reason]
    assert reaction["coverage"]["support_state"] == "SUPPORTED"
    assert reaction["coverage"]["next_action"] == reason
    assert result["reaction_decision"] == "OBSERVATION_ONLY"
    assert result["reaction_provider"]["status"] == "READY"


def test_due_supported_release_without_ledger_remains_fail_closed() -> None:
    event_hash = "c" * 64
    event_id = "bea-gdp-due-without-ledger"
    reason = "WAIT_FOR_DECLARED_RELEASE_TIME"
    provider = lambda: {
        "calendar": [
            {
                "id": event_id,
                "event_id": event_id,
                "title": "Gross Domestic Product",
                "source": "Bureau of Economic Analysis",
                "event_at": "2026-08-21T12:30:00+00:00",
                "scheduled_at": "2026-08-21T12:30:00+00:00",
                "category": "MACRO",
                "schedule_precision": "EXACT",
                "calendar_origin": "OFFICIAL",
                "reaction_identity_hash": event_hash,
                "reaction": {
                    "status": "UNAVAILABLE",
                    "current_stage": None,
                    "analysis_available": False,
                    "event_id": event_id,
                    "event_hash": event_hash,
                    "asof": None,
                    "head_hash": None,
                    "transition_count": 0,
                    "expectation": None,
                    "release": None,
                    "surprise": None,
                    "market_reaction": None,
                    "option_reevaluation": None,
                    "decision": "OBSERVATION_ONLY",
                    "reasons": ["WAITING_DECLARED_RELEASE_TIME"],
                    "coverage": {
                        "family": "GDP",
                        "support_state": "SUPPORTED",
                        "supported": True,
                        "capture_eligible": False,
                        "surprise_eligible": False,
                        "progressed": False,
                        "next_action": reason,
                        "measure_count": 1,
                        "capture_count": 0,
                        "document_stage": "SCHEDULED",
                        "next_eligible_release_at": "2026-08-21T12:30:00+00:00",
                    },
                    "document_progression": {
                        "status": "SCHEDULED",
                        "capture_count": 0,
                        "next_action": reason,
                        "decision_authority": "SUPPORTING_ONLY",
                    },
                    "numeric_surprise": {
                        "status": "UNAVAILABLE",
                        "reason": "NUMERIC_SURPRISE_UNSUPPORTED",
                        "decision_authority": "SUPPORTING_ONLY",
                    },
                    "decision_authority": "SUPPORTING_ONLY",
                    "approval_eligible": False,
                    "instruction_creation_allowed": False,
                    "order_creation_allowed": False,
                },
            }
        ],
        "asof": "2026-08-22T04:42:40+00:00",
        "decision": "OBSERVATION_ONLY",
        "reaction_provider": {
            "status": "READY",
            "decision": "OBSERVATION_ONLY",
            "reason": "NO_ELIGIBLE_REACTION_EVENTS",
            "ledger_count": 0,
            "matched_count": 0,
            "ignored_count": 0,
            "supported_event_ids": [event_id],
            "supported_count": 1,
            "eligible_event_ids": [],
            "eligible_count": 0,
            "unsupported_count": 0,
        },
        "reaction_decision": "OBSERVATION_ONLY",
    }
    app = create_app(
        OptionsCopilotServices(
            health_provider=lambda: {},
            bootstrap_provider=lambda: {},
            candidates_provider=lambda: [],
            positions_provider=lambda: [],
            learning_provider=lambda: {},
            calendar_provider=provider,
        )
    )

    result = asyncio.run(_route(app, "/api/calendar")())

    reaction = result["calendar"][0]["reaction"]
    assert reaction["decision"] == "NO_TRADE"
    assert reaction["reasons"] == ["REACTION_API_READ_MODEL_INVALID"]
    assert result["reaction_decision"] == "NO_TRADE"
    assert result["reaction_provider"]["status"] == "UNAVAILABLE"


def test_reaction_projection_preserves_healthy_zero_eligible_scope() -> None:
    event_hash = "a" * 64
    attempted_at = "2026-08-21T01:02:03+00:00"
    provider = lambda: {
        "calendar": [
            {
                "id": "fomc-unsupported",
                "title": "FOMC rate decision",
                "source": "Federal Reserve",
                "event_at": "2026-08-21T18:00:00+00:00",
                "calendar_origin": "OFFICIAL",
                "reaction_identity_hash": event_hash,
                "reaction": {
                    "status": "UNAVAILABLE",
                    "event_id": "fomc-unsupported",
                    "event_hash": event_hash,
                    "decision": "OBSERVATION_ONLY",
                    "reasons": ["REACTION_EVENT_UNSUPPORTED"],
                },
            }
        ],
        "provider": {"name": "calendar", "status": "READY"},
        "decision": "OBSERVATION_ONLY",
        "reaction_provider": {
            "status": "READY",
            "decision": "OBSERVATION_ONLY",
            "reason": "NO_ELIGIBLE_REACTION_EVENTS",
            "ledger_count": 0,
            "matched_count": 0,
            "ignored_count": 0,
            "supported_event_ids": [],
            "supported_count": 0,
            "eligible_event_ids": [],
            "eligible_count": 0,
            "unsupported_count": 1,
            "last_attempt": attempted_at,
        },
        "source_runtime": [
            {
                "source_id": "REACTION",
                "source_kind": "REACTION",
                "configured": True,
                "cadence_status": "WAITING",
                "interval_seconds": 90,
                "last_attempt": attempted_at,
                "last_success": attempted_at,
                "next_due": "2026-08-21T01:03:33+00:00",
                "freshness": "CURRENT",
                "failure_code": None,
                "attempt_count": 1,
                "success_count": 1,
                "skip_count": 0,
            }
        ],
        "reaction_decision": "OBSERVATION_ONLY",
    }
    app = create_app(
        OptionsCopilotServices(
            health_provider=lambda: {},
            bootstrap_provider=lambda: {},
            candidates_provider=lambda: [],
            positions_provider=lambda: [],
            learning_provider=lambda: {},
            calendar_provider=provider,
        )
    )

    result = asyncio.run(_route(app, "/api/calendar")())

    reaction_provider = result["reaction_provider"]
    assert result["reaction_decision"] == "OBSERVATION_ONLY"
    assert reaction_provider["status"] == "READY"
    assert reaction_provider["reason"] == "NO_ELIGIBLE_REACTION_EVENTS"
    assert reaction_provider["eligible_count"] == 0
    assert reaction_provider["unsupported_count"] == 1
    assert reaction_provider["last_attempt"] == attempted_at
    assert result["source_runtime"][0]["source_kind"] == "REACTION"
    assert result["source_runtime"][0]["interval_seconds"] == 90
    reaction = result["calendar"][0]["reaction"]
    assert reaction["decision"] == "OBSERVATION_ONLY"
    assert reaction["reasons"] == ["REACTION_EVENT_UNSUPPORTED"]


def test_inactive_old_versions_do_not_poison_zero_eligible_current_summary() -> None:
    attempted_at = "2026-08-21T02:03:04+00:00"
    current_rows = []
    for index in range(15):
        event_id = f"current-unsupported-{index:02d}"
        event_hash = f"{index + 1:064x}"
        current_rows.append(
            {
                "id": event_id,
                "title": f"Current unsupported official event {index}",
                "source": "Federal Reserve",
                "event_at": "2026-08-22T18:00:00+00:00",
                "calendar_origin": "OFFICIAL",
                "reaction_identity_hash": event_hash,
                "reaction": {
                    "status": "UNAVAILABLE",
                    "event_id": event_id,
                    "event_hash": event_hash,
                    "decision": "OBSERVATION_ONLY",
                    "reasons": ["REACTION_EVENT_UNSUPPORTED"],
                },
            }
        )
    inactive_rows = []
    for index in range(10):
        event_id = f"inactive-old-version-{index:02d}"
        event_hash = f"{index + 101:064x}"
        inactive_rows.append(
            {
                "id": event_id,
                "title": f"Inactive old official version {index}",
                "source": "Bureau of Labor Statistics",
                "event_at": "2026-08-01T12:30:00+00:00",
                "calendar_origin": "OFFICIAL",
                "reaction_identity_hash": event_hash,
                "reaction": {
                    "status": "UNAVAILABLE",
                    "event_id": event_id,
                    "event_hash": event_hash,
                    "decision": "NO_TRADE",
                    "reasons": [
                        "REACTION_EVENT_NOT_IN_CURRENT_OFFICIAL_SNAPSHOT"
                    ],
                },
            }
        )
    provider = lambda: {
        "calendar": [*current_rows, *inactive_rows],
        "provider": {"name": "calendar", "status": "READY"},
        "decision": "OBSERVATION_ONLY",
        "reaction_provider": {
            "status": "READY",
            "decision": "OBSERVATION_ONLY",
            "reason": "NO_ELIGIBLE_REACTION_EVENTS",
            "ledger_count": 0,
            "matched_count": 0,
            "ignored_count": 0,
            "supported_event_ids": [],
            "supported_count": 0,
            "eligible_event_ids": [],
            "eligible_count": 0,
            "unsupported_count": 15,
            "last_attempt": attempted_at,
        },
        "reaction_decision": "OBSERVATION_ONLY",
    }
    app = create_app(
        OptionsCopilotServices(
            health_provider=lambda: {},
            bootstrap_provider=lambda: {},
            candidates_provider=lambda: [],
            positions_provider=lambda: [],
            learning_provider=lambda: {},
            calendar_provider=provider,
        )
    )

    result = asyncio.run(_route(app, "/api/calendar")())

    assert result["count"] == 25
    assert result["reaction_decision"] == "OBSERVATION_ONLY"
    assert result["reaction_provider"] == {
        "name": "event-reaction-provider",
        "status": "READY",
        "decision": "OBSERVATION_ONLY",
        "reason": "NO_ELIGIBLE_REACTION_EVENTS",
        "ledger_count": 0,
        "matched_count": 0,
        "ignored_count": 0,
        "supported_event_ids": [],
        "supported_count": 0,
        "eligible_event_ids": [],
        "eligible_count": 0,
        "unsupported_count": 15,
        "last_attempt": attempted_at,
        "event_count": 15,
        "measure_count": 0,
        "capture_spec_count": 0,
        "captured_release_vintage_count": 0,
        "captured_measure_count": 0,
        "capture_eligible_count": 0,
        "surprise_ready_count": 0,
        "progressed_event_count": 0,
        "next_eligible_release_at": "NEXT_ELIGIBLE_RELEASE_UNKNOWN",
        "schedule_refresh_status": "UNKNOWN",
        "schedule_refresh_reason": None,
        "schedule_hash": None,
        "descriptor_wait_count": 0,
        "next_action": "WAIT_NEXT_ELIGIBLE_RELEASE",
        "support_matrix": [],
            "worker_health": {},
            "lifecycle_supersessions": {},
            "decision_authority": "SUPPORTING_ONLY",
        "approval_eligible": False,
        "instruction_creation_allowed": False,
        "order_creation_allowed": False,
    }
    assert result["calendar"][0]["reaction"]["reasons"] == [
        "REACTION_EVENT_UNSUPPORTED"
    ]
    assert result["calendar"][-1]["reaction"]["reasons"] == [
        "REACTION_EVENT_NOT_IN_CURRENT_OFFICIAL_SNAPSHOT"
    ]
    assert result["calendar"][-1]["reaction"]["decision"] == "NO_TRADE"


def _route(app, path: str):
    return next(route.endpoint for route in app.routes if route.path == path)
