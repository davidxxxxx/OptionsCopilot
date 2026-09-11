from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import datetime, timedelta, timezone
import importlib
import importlib.util

import pytest


NOW = datetime(2026, 8, 3, 4, 0, tzinfo=timezone.utc)
REQUEST_CLASSES = (
    "scanner",
    "secdef",
    "snapshot_quote",
    "streaming_quote",
    "historical",
)


def _contracts():
    spec = importlib.util.find_spec("options_copilot.operations.capabilities")
    assert spec is not None, "readiness capability contracts are not implemented"
    return importlib.import_module("options_copilot.operations.capabilities")


def _limits() -> dict[str, dict[str, int | float]]:
    return {
        name: {
            "max_concurrency": index + 1,
            "request_window": 60.0,
            "max_requests": (index + 1) * 10,
            "cooldown": 1.0,
        }
        for index, name in enumerate(REQUEST_CLASSES)
    }


def _capability(*, source: str = "observed", signer: str | None = "operator"):
    contracts = _contracts()
    return contracts.MarketDataPacingCapability.create(
        version="market-data-pacing.v1",
        observed_at=NOW,
        source=source,
        request_classes=_limits(),
        signer=signer,
    )


@pytest.mark.parametrize(
    ("source", "signer"),
    [
        ("broker_disclosed", None),
        ("observed", None),
        ("conservative_default", "human:options-operator"),
    ],
)
def test_all_allowed_sources_and_five_request_classes_validate(
    source: str,
    signer: str | None,
) -> None:
    contracts = _contracts()
    capability = _capability(source=source, signer=signer)

    result = capability.validate(now=NOW + timedelta(minutes=1))

    assert result.status is contracts.CapabilityStatus.READY_FOR_REVIEW
    assert result.reason_codes == ()
    assert tuple(capability.request_classes) == REQUEST_CLASSES
    assert len(capability.content_hash) == 64
    assert capability.as_dict()["content_hash"] == capability.content_hash


def test_capability_contract_is_deeply_immutable() -> None:
    capability = _capability()

    with pytest.raises(FrozenInstanceError):
        capability.source = "broker_disclosed"  # type: ignore[misc]
    with pytest.raises(TypeError):
        capability.request_classes["scanner"]["max_requests"] = 999  # type: ignore[index]


def test_missing_request_class_fails_closed_with_fixed_reason() -> None:
    contracts = _contracts()
    payload = _capability().as_dict()
    payload["request_classes"].pop("historical")

    result = contracts.MarketDataPacingCapability.inspect(
        payload,
        now=NOW + timedelta(minutes=1),
    )

    assert result.status is contracts.CapabilityStatus.MISSING
    assert "PACING_REQUEST_CLASS_MISSING" in result.reason_codes
    assert "PACING_CAPABILITY_MISSING" in result.reason_codes


def test_non_positive_limit_fails_closed_before_hash_validation() -> None:
    contracts = _contracts()
    payload = _capability().as_dict()
    payload["request_classes"]["scanner"]["max_concurrency"] = 0

    result = contracts.MarketDataPacingCapability.inspect(
        payload,
        now=NOW + timedelta(minutes=1),
    )

    assert result.status is contracts.CapabilityStatus.DEGRADED
    assert "PACING_LIMIT_NON_POSITIVE" in result.reason_codes
    assert "PACING_CAPABILITY_MISSING" in result.reason_codes


def test_stale_observation_is_never_ready() -> None:
    contracts = _contracts()
    capability = _capability()

    result = capability.validate(
        now=NOW + timedelta(days=2),
        max_age=timedelta(hours=24),
    )

    assert result.status is contracts.CapabilityStatus.STALE
    assert "PACING_OBSERVATION_STALE" in result.reason_codes


def test_changed_hash_is_detected() -> None:
    contracts = _contracts()
    payload = _capability().as_dict()
    payload["content_hash"] = "0" * 64

    result = contracts.MarketDataPacingCapability.inspect(
        payload,
        now=NOW + timedelta(minutes=1),
    )

    assert result.status is contracts.CapabilityStatus.FORBIDDEN
    assert "PACING_CONTENT_HASH_MISMATCH" in result.reason_codes


def test_unsigned_conservative_default_is_forbidden() -> None:
    contracts = _contracts()
    capability = _capability(source="conservative_default", signer=None)

    result = capability.validate(now=NOW + timedelta(minutes=1))

    assert result.status is contracts.CapabilityStatus.FORBIDDEN
    assert "PACING_CONSERVATIVE_DEFAULT_UNSIGNED" in result.reason_codes


@pytest.mark.parametrize(
    "mutate",
    [
        lambda payload: payload.__setitem__("guessed_limit", 12),
        lambda payload: payload["request_classes"]["scanner"].__setitem__(
            "burst", 3
        ),
    ],
)
def test_unknown_fields_are_forbidden(mutate) -> None:
    contracts = _contracts()
    payload = _capability().as_dict()
    mutate(payload)

    result = contracts.MarketDataPacingCapability.inspect(
        payload,
        now=NOW + timedelta(minutes=1),
    )

    assert result.status is contracts.CapabilityStatus.FORBIDDEN
    assert any(code.endswith("UNKNOWN_FIELD") for code in result.reason_codes)


def test_readiness_report_cannot_upgrade_any_fail_closed_record() -> None:
    contracts = _contracts()
    for status in (
        contracts.CapabilityStatus.MISSING,
        contracts.CapabilityStatus.STALE,
        contracts.CapabilityStatus.DEGRADED,
        contracts.CapabilityStatus.FORBIDDEN,
    ):
        record = contracts.CapabilityRecord(
            name="market_data_pacing",
            status=status,
            observed_at=NOW,
            reason_codes=("PACING_CAPABILITY_MISSING",),
            details={},
        )
        report = contracts.ReadinessReport(observed_at=NOW, records=(record,))

        assert report.status is not contracts.CapabilityStatus.READY_FOR_REVIEW
        assert report.as_dict()["review_only"] is True
        assert report.as_dict()["direct_order_submission"] is False


def test_contract_module_and_gateway_expose_no_broker_write_authority() -> None:
    contracts = _contracts()
    gateway = importlib.import_module("options_copilot.gateway.ibkr_readonly")
    forbidden = ("placeOrder", "submit_order", "transmit", "cancelOrder")

    assert all(not hasattr(contracts, name) for name in forbidden)
    assert all(not hasattr(gateway.IBKRReadOnlyGateway, name) for name in forbidden)
