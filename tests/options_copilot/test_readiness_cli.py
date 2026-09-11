from __future__ import annotations

import asyncio
from dataclasses import fields
from datetime import datetime, timedelta, timezone
import hashlib
import importlib
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace
import pytest

from options_copilot.api import OptionsCopilotServices, create_app
from options_copilot.config import OptionsCopilotConfig
from options_copilot.gateway.ibkr_readonly import IBKRReadOnlyGateway
from options_copilot.runtime import RuntimeServices


NOW = datetime(2026, 8, 3, 5, 0, tzinfo=timezone.utc)
ROOT = Path(__file__).resolve().parents[2]


def _readiness():
    spec = importlib.util.find_spec("options_copilot.operations.readiness")
    assert spec is not None, "shared readiness probes are not implemented"
    return importlib.import_module("options_copilot.operations.readiness")


def _create_windows_junction(link: Path, target: Path) -> None:
    completed = subprocess.run(
        ["cmd.exe", "/d", "/c", "mklink", "/J", str(link), str(target)],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr


def _capability() -> dict[str, object]:
    contracts = importlib.import_module("options_copilot.operations.capabilities")
    request_classes = {
        name: {
            "max_concurrency": index + 1,
            "request_window": 60.0,
            "max_requests": (index + 1) * 10,
            "cooldown": 1.0,
        }
        for index, name in enumerate(contracts.PACING_REQUEST_CLASSES)
    }
    return contracts.MarketDataPacingCapability.create(
        version="market-data-pacing.v1",
        observed_at=NOW,
        source="broker_disclosed",
        request_classes=request_classes,
        signer=None,
    ).as_dict()


def _ready_observations() -> dict[str, object]:
    return {
        "market_data_pacing": _capability(),
        "broker_session": {
            "connected": True,
            "read_only": True,
            "observed_at": NOW,
        },
        "market_data_entitlements": {
            "entitled": True,
            "quote_mode": "live",
            "observed_at": NOW,
        },
        "providers": {
            "available": True,
            "conflicted": False,
            "observed_at": NOW,
        },
        "creator_transport": {
            "available": True,
            "review_only": True,
            "direct_order_submission": False,
            "observed_at": NOW,
        },
        "scanner_heartbeat": {
            "available": True,
            "stale": False,
            "observed_at": NOW,
        },
        "learning_governance": {
            "available": True,
            "human_promotion_required": True,
            "automatic_promotion": False,
            "observed_at": NOW,
        },
        "process_ports": {
            "listener_known": True,
            "protected_ports_unchanged": True,
            "observed_at": NOW,
        },
        "tailscale_route": {
            "remote_route_present": True,
            "protected_root_unchanged": True,
            "observed_at": NOW,
        },
    }


def _environment_identity(**updates: object) -> dict[str, object]:
    identity: dict[str, object] = {
        "interpreter_path": str((ROOT / ".venv" / "Scripts" / "python.exe").resolve()),
        "python_version": ".".join(str(part) for part in sys.version_info[:3]),
        "lock_filename": "requirements-dev.lock",
        "lock_type": "development",
        "lock_sha256": hashlib.sha256(
            (ROOT / "requirements-dev.lock").read_bytes()
        ).hexdigest(),
        "dependency_check": "EXACT_LOCK_MATCH",
    }
    identity.update(updates)
    return identity


def test_cli_help_lists_market_data_pacing() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "options_copilot.operations.readiness",
            "--help",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "market_data_pacing" in result.stdout
    assert "--evidence-dir" in result.stdout


def test_fixture_identical_cli_and_api_reports_have_same_content_hash(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    readiness = _readiness()
    inputs = readiness.ReadinessProbeInputs.from_mapping(_ready_observations())

    cli_payload = readiness.run_cli(
        [
            "--probe",
            "market_data_pacing",
            "--json",
            "--evidence-dir",
            str(tmp_path / "evidence"),
        ],
        inputs=inputs,
        now=lambda: NOW + timedelta(minutes=1),
    )
    stdout_payload = json.loads(capsys.readouterr().out)

    app = create_app(
        _services(readiness_provider=lambda: dict(cli_payload))
    )
    api_payload = asyncio.run(_route(app, "/api/readiness")())

    assert cli_payload == stdout_payload == api_payload
    assert cli_payload["content_hash"] == api_payload["content_hash"]
    assert cli_payload["review_only"] is True
    assert cli_payload["direct_order_submission"] is False
    artifacts = list((tmp_path / "evidence").rglob("readiness.json"))
    assert len(artifacts) == 1
    assert json.loads(artifacts[0].read_text(encoding="utf-8")) == cli_payload


def test_environment_identity_is_identical_and_sanitized_through_cli_and_api(
    capsys: pytest.CaptureFixture[str],
) -> None:
    readiness = _readiness()
    identity = _environment_identity()
    inputs = readiness.ReadinessProbeInputs(environment_identity=identity)

    cli_payload = readiness.run_cli(
        ["--probe", "environment_identity", "--json"],
        inputs=inputs,
        now=NOW,
    )
    stdout_payload = json.loads(capsys.readouterr().out)
    app = create_app(_services(readiness_provider=lambda: dict(cli_payload)))
    api_payload = asyncio.run(_route(app, "/api/readiness")())

    assert cli_payload == stdout_payload == api_payload
    assert cli_payload["content_hash"] == api_payload["content_hash"]
    assert cli_payload["status"] == "READY_FOR_REVIEW"
    assert cli_payload["review_only"] is True
    assert cli_payload["direct_order_submission"] is False
    record = cli_payload["records"][0]
    assert record["name"] == "environment_identity"
    assert record["reason_codes"] == []
    assert record["details"] == identity
    assert set(record["details"]) == {
        "interpreter_path",
        "python_version",
        "lock_filename",
        "lock_type",
        "lock_sha256",
        "dependency_check",
    }


@pytest.mark.parametrize(
    ("identity", "reason"),
    (
        (
            _environment_identity(
                interpreter_path="",
                dependency_check="INTERPRETER_MISSING",
            ),
            "ENVIRONMENT_INTERPRETER_MISSING",
        ),
        (
            _environment_identity(
                lock_filename="",
                lock_sha256="",
                dependency_check="LOCK_MISSING",
            ),
            "ENVIRONMENT_LOCK_MISSING",
        ),
        (
            _environment_identity(dependency_check="VERIFIER_MISSING"),
            "ENVIRONMENT_VERIFIER_MISSING",
        ),
        (
            _environment_identity(interpreter_path="C:\\Python312\\python.exe"),
            "ENVIRONMENT_IDENTITY_ATTESTATION_MISMATCH",
        ),
        (
            _environment_identity(python_version="3.11.9"),
            "ENVIRONMENT_IDENTITY_ATTESTATION_MISMATCH",
        ),
        (
            _environment_identity(lock_sha256="not-a-sha256"),
            "ENVIRONMENT_IDENTITY_ATTESTATION_MISMATCH",
        ),
        (
            _environment_identity(dependency_check="LOCK_HASH_MISMATCH"),
            "ENVIRONMENT_LOCK_HASH_MISMATCH",
        ),
        (
            _environment_identity(dependency_check="INVENTORY_MISMATCH"),
            "ENVIRONMENT_INVENTORY_MISMATCH",
        ),
        (
            _environment_identity(dependency_check="PIP_CHECK_FAILED"),
            "ENVIRONMENT_PIP_CHECK_FAILED",
        ),
    ),
)
def test_invalid_environment_identity_never_upgrades_readiness(
    identity: dict[str, object],
    reason: str,
) -> None:
    readiness = _readiness()

    report = readiness.build_readiness_report(
        readiness.ReadinessProbeInputs(environment_identity=identity),
        probes=("environment_identity",),
        now=NOW,
    )
    payload = report.as_dict()

    assert payload["status"] != "READY_FOR_REVIEW"
    assert reason in payload["reason_codes"]
    assert payload["review_only"] is True
    assert payload["direct_order_submission"] is False
    details = payload["records"][0]["details"]
    assert set(details) <= {
        "interpreter_path",
        "python_version",
        "lock_filename",
        "lock_type",
        "lock_sha256",
        "dependency_check",
    }


def test_caller_claims_cannot_fabricate_a_ready_environment_identity() -> None:
    readiness = _readiness()
    fabricated = _environment_identity(
        python_version="99.0.0",
        lock_sha256="0" * 64,
        dependency_check="EXACT_LOCK_MATCH",
    )

    report = readiness.build_readiness_report(
        readiness.ReadinessProbeInputs(environment_identity=fabricated),
        probes=("environment_identity",),
        now=NOW,
    )

    assert report.status.value == "FORBIDDEN"
    assert "ENVIRONMENT_IDENTITY_ATTESTATION_MISMATCH" in report.reason_codes
    assert report.records[0].details["python_version"] != "99.0.0"
    assert report.records[0].details["lock_sha256"] != "0" * 64


@pytest.mark.parametrize(
    "private_field",
    (
        "credential",
        "provider_key",
        "account_id",
        "broker_state",
        "position",
        "order",
        "instruction",
        "environment",
    ),
)
def test_environment_identity_rejects_private_or_broad_fields(
    private_field: str,
) -> None:
    readiness = _readiness()
    identity = _environment_identity(**{private_field: "must-not-cross-boundary"})

    if private_field == "credential":
        with pytest.raises(readiness.SecretLikeFieldError):
            readiness.ReadinessProbeInputs(environment_identity=identity)
        return

    inputs = readiness.ReadinessProbeInputs(environment_identity=identity)
    with pytest.raises(readiness.ReadinessInputError):
        readiness.build_readiness_report(
            inputs,
            probes=("environment_identity",),
            now=NOW,
        )


def test_environment_identity_rejects_nested_private_values() -> None:
    readiness = _readiness()
    identity = _environment_identity(
        interpreter_path={"account_id": "must-not-cross-boundary"},
    )
    inputs = readiness.ReadinessProbeInputs(environment_identity=identity)

    with pytest.raises(readiness.ReadinessInputError):
        readiness.build_readiness_report(
            inputs,
            probes=("environment_identity",),
            now=NOW,
        )


def test_default_environment_observation_is_exact_and_side_effect_free() -> None:
    readiness = _readiness()

    inputs = readiness.load_default_probe_inputs()
    report = readiness.build_readiness_report(
        inputs,
        probes=("environment_identity",),
        now=NOW,
    )
    payload = report.as_dict()

    assert payload["status"] == "READY_FOR_REVIEW"
    record = payload["records"][0]
    assert record["details"]["dependency_check"] == "EXACT_LOCK_MATCH"
    assert Path(record["details"]["interpreter_path"]) == (
        ROOT / ".venv" / "Scripts" / "python.exe"
    )
    assert record["details"]["lock_filename"] == "requirements-dev.lock"
    assert record["details"]["lock_type"] == "development"
    assert len(record["details"]["lock_sha256"]) == 64


@pytest.mark.skipif(sys.platform != "win32", reason="Windows junctions are required")
def test_default_environment_rejects_junctioned_project_venv(
    tmp_path: Path,
) -> None:
    project_root = tmp_path / "junction-project"
    module_path = project_root / "options_copilot" / "operations" / "readiness.py"
    verifier_path = project_root / "scripts" / "verify_locked_environment.py"
    module_path.parent.mkdir(parents=True)
    verifier_path.parent.mkdir(parents=True)
    module_path.write_bytes(
        (ROOT / "options_copilot" / "operations" / "readiness.py").read_bytes()
    )
    verifier_path.write_bytes(
        (ROOT / "scripts" / "verify_locked_environment.py").read_bytes()
    )
    (project_root / "requirements-dev.lock").write_bytes(
        (ROOT / "requirements-dev.lock").read_bytes()
    )
    project_venv = project_root / ".venv"
    trusted_venv = Path(sys.executable).resolve().parents[1]
    _create_windows_junction(project_venv, trusted_venv)
    module_name = "_options_copilot_junction_readiness_regression"

    try:
        spec = importlib.util.spec_from_file_location(module_name, module_path)
        assert spec is not None and spec.loader is not None
        readiness = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = readiness
        spec.loader.exec_module(readiness)

        inputs = readiness.load_default_probe_inputs()
        report = readiness.build_readiness_report(
            inputs,
            probes=("environment_identity",),
            now=NOW,
        )

        assert inputs.environment_identity["dependency_check"] == (
            "PROJECT_VENV_REPARSE_POINT"
        )
        assert inputs.environment_identity["dependency_check"] != "EXACT_LOCK_MATCH"
        assert report.status.value != "READY_FOR_REVIEW"
        assert "ENVIRONMENT_PROJECT_VENV_REPARSE_POINT" in report.reason_codes
    finally:
        sys.modules.pop(module_name, None)
        if project_venv.is_junction():
            project_venv.rmdir()


@pytest.mark.parametrize(
    ("successful_calls", "dependency_check", "reason"),
    (
        (1, "INVENTORY_MISMATCH", "ENVIRONMENT_INVENTORY_MISMATCH"),
        (2, "PIP_CHECK_FAILED", "ENVIRONMENT_PIP_CHECK_FAILED"),
    ),
)
def test_default_environment_subprocess_timeout_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
    successful_calls: int,
    dependency_check: str,
    reason: str,
) -> None:
    readiness = _readiness()
    calls = 0

    def bounded_run(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        nonlocal calls
        calls += 1
        assert kwargs["timeout"] == 30
        assert kwargs["check"] is False
        if calls == 1:
            return subprocess.CompletedProcess(
                args[0],
                0,
                json.dumps(
                    {
                        "implementation": "CPython",
                        "version": ".".join(
                            str(part) for part in sys.version_info[:3]
                        ),
                        "executable": sys.executable,
                    }
                ),
                "",
            )
        if calls <= successful_calls:
            return subprocess.CompletedProcess(args[0], 0, "", "")
        raise subprocess.TimeoutExpired(args[0], kwargs["timeout"])

    monkeypatch.setattr(readiness.subprocess, "run", bounded_run)

    inputs = readiness.load_default_probe_inputs()
    assert inputs.environment_identity["dependency_check"] == dependency_check
    report = readiness.build_readiness_report(
        inputs,
        probes=("environment_identity",),
        now=NOW,
    )

    assert report.status.value != "READY_FOR_REVIEW"
    assert reason in report.reason_codes


def test_runtime_readiness_provider_returns_valid_fail_closed_api_payload() -> None:
    runtime_services = RuntimeServices(
        **{field.name: None for field in fields(RuntimeServices)}
    )
    app = create_app(
        _services(readiness_provider=runtime_services.readiness)
    )

    payload = asyncio.run(_route(app, "/api/readiness")())

    assert payload["status"] == "DEGRADED"
    assert payload["decision"] == "NO_TRADE"
    assert payload["approval_enabled"] is False
    assert payload["review_only"] is True
    assert payload["direct_order_submission"] is False
    assert len(payload["content_hash"]) == 64


@pytest.mark.parametrize(
    ("probe", "updates", "reason"),
    [
        (
            "market_data_entitlements",
            {"entitled": False, "quote_mode": "missing"},
            "MARKET_DATA_ENTITLEMENT_MISSING",
        ),
        (
            "market_data_entitlements",
            {"entitled": True, "quote_mode": "delayed"},
            "MARKET_DATA_DELAYED",
        ),
        (
            "process_ports",
            {"listener_known": False, "protected_ports_unchanged": True},
            "PROCESS_LISTENER_UNKNOWN",
        ),
        (
            "creator_transport",
            {
                "available": False,
                "review_only": True,
                "direct_order_submission": False,
            },
            "CREATOR_TRANSPORT_MISSING",
        ),
        (
            "tailscale_route",
            {
                "remote_route_present": False,
                "protected_root_unchanged": True,
            },
            "REMOTE_ROUTE_MISSING",
        ),
    ],
)
def test_missing_delayed_or_unknown_observations_never_become_ready(
    probe: str,
    updates: dict[str, object],
    reason: str,
) -> None:
    readiness = _readiness()
    observations = _ready_observations()
    observations[probe] = {"observed_at": NOW, **updates}

    report = readiness.build_readiness_report(
        readiness.ReadinessProbeInputs.from_mapping(observations),
        probes=(probe,),
        now=NOW,
    )

    assert report.status.value != "READY_FOR_REVIEW"
    assert reason in report.reason_codes


@pytest.mark.parametrize(
    ("observed_at", "reason", "status"),
    (
        (None, "BROKER_SESSION_OBSERVATION_MISSING", "MISSING"),
        ("not-a-timestamp", "BROKER_SESSION_OBSERVATION_INVALID", "FORBIDDEN"),
        (
            NOW + timedelta(minutes=2),
            "BROKER_SESSION_OBSERVATION_FUTURE",
            "FORBIDDEN",
        ),
        (
            NOW - timedelta(hours=25),
            "BROKER_SESSION_STALE",
            "STALE",
        ),
    ),
)
def test_freshness_bearing_observations_fail_closed_without_a_current_timestamp(
    observed_at: object,
    reason: str,
    status: str,
) -> None:
    readiness = _readiness()
    observation: dict[str, object] = {"connected": True, "read_only": True}
    if observed_at is not None:
        observation["observed_at"] = observed_at

    report = readiness.build_readiness_report(
        readiness.ReadinessProbeInputs(broker_session=observation),
        probes=("broker_session",),
        now=NOW + timedelta(minutes=1),
    )

    record = report.records[0]
    assert record.status.value == status
    assert reason in record.reason_codes
    if isinstance(observed_at, datetime):
        assert record.observed_at == observed_at


def test_ready_observation_retains_the_source_timestamp() -> None:
    readiness = _readiness()

    report = readiness.build_readiness_report(
        readiness.ReadinessProbeInputs(
            broker_session={
                "connected": True,
                "read_only": True,
                "observed_at": NOW,
            }
        ),
        probes=("broker_session",),
        now=NOW + timedelta(seconds=15),
    )

    assert report.status.value == "READY_FOR_REVIEW"
    assert report.records[0].observed_at == NOW


@pytest.mark.parametrize(
    "probe",
    (
        "broker_session",
        "creator_transport",
        "scanner_heartbeat",
        "process_ports",
        "tailscale_route",
    ),
)
@pytest.mark.parametrize(
    ("age", "expected_status"),
    (
        (timedelta(seconds=15), "READY_FOR_REVIEW"),
        (timedelta(seconds=15, microseconds=1), "STALE"),
    ),
)
def test_operational_observation_freshness_has_a_fifteen_second_boundary(
    probe: str,
    age: timedelta,
    expected_status: str,
) -> None:
    readiness = _readiness()
    observations = _ready_observations()

    report = readiness.build_readiness_report(
        readiness.ReadinessProbeInputs.from_mapping(observations),
        probes=(probe,),
        now=NOW + age,
    )

    assert report.status.value == expected_status


def test_minute_old_broker_session_is_stale() -> None:
    readiness = _readiness()

    report = readiness.build_readiness_report(
        readiness.ReadinessProbeInputs(
            broker_session={
                "connected": True,
                "read_only": True,
                "observed_at": NOW,
            }
        ),
        probes=("broker_session",),
        now=NOW + timedelta(minutes=1),
    )

    assert report.status.value == "STALE"
    assert "BROKER_SESSION_STALE" in report.reason_codes


@pytest.mark.parametrize(
    ("age", "expected_status"),
    (
        (timedelta(seconds=5), "READY_FOR_REVIEW"),
        (timedelta(seconds=5, microseconds=1), "STALE"),
    ),
)
def test_live_quote_mode_freshness_retains_the_five_second_boundary(
    age: timedelta,
    expected_status: str,
) -> None:
    readiness = _readiness()

    report = readiness.build_readiness_report(
        readiness.ReadinessProbeInputs(
            market_data_entitlements={
                "entitled": True,
                "quote_mode": "live",
                "observed_at": NOW,
            }
        ),
        probes=("market_data_entitlements",),
        now=NOW + age,
    )

    assert report.status.value == expected_status


def test_missing_later_modules_are_reported_missing_not_import_errors() -> None:
    readiness = _readiness()

    report = readiness.build_readiness_report(
        readiness.ReadinessProbeInputs(),
        now=NOW,
    )

    assert report.status.value == "MISSING"
    assert {record.name for record in report.records} == set(readiness.PROBE_NAMES)
    assert all(record.status.value == "MISSING" for record in report.records)


@pytest.mark.parametrize(
    "unsafe",
    [
        {"providers": {"available": True, "api_token": "must-not-leak"}},
        {"providers": {"available": True, "note": "Bearer raw-credential"}},
        {"providers": {"available": True, "password": "correct horse"}},
    ],
)
def test_probe_inputs_and_api_reject_secret_like_fields(
    unsafe: dict[str, object],
) -> None:
    readiness = _readiness()
    with pytest.raises(readiness.SecretLikeFieldError):
        readiness.ReadinessProbeInputs.from_mapping(unsafe)

    app = create_app(_services(readiness_provider=lambda: unsafe))
    with pytest.raises(Exception) as exc_info:
        asyncio.run(_route(app, "/api/readiness")())
    assert getattr(exc_info.value, "status_code", None) == 502


def test_gateway_pacing_observer_is_injected_and_session_stays_read_only(
    tmp_path: Path,
) -> None:
    fake = _FakeIB()
    observed = {
        "version": "market-data-pacing.v1",
        "observed_at": NOW,
        "source": "observed",
        "request_classes": _capability()["request_classes"],
        "signer": None,
    }
    gateway = IBKRReadOnlyGateway(
        OptionsCopilotConfig(
            data_dir=tmp_path / "data",
            log_dir=tmp_path / "logs",
        ),
        ib_factory=lambda: fake,
        pacing_observer=lambda ib: observed if ib is fake else None,
        now=lambda: NOW,
    )

    gateway.connect()
    result = gateway.market_data_pacing_observation()

    assert fake.connect_kwargs["readonly"] is True
    assert result == {
        **observed,
        "observed_at": "2026-08-03T05:00:00.000000+00:00",
    }
    assert not hasattr(gateway, "placeOrder")
    assert not hasattr(gateway, "submit_order")


class _ControlEvent:
    def __init__(self) -> None:
        self.handlers = []

    def __iadd__(self, handler):
        self.handlers.append(handler)
        return self

    def __isub__(self, handler):
        self.handlers.remove(handler)
        return self

    def emit(self):
        for handler in tuple(self.handlers):
            handler()


class _FakeIB:
    def __init__(self) -> None:
        self.connected = False
        self.connect_kwargs: dict[str, object] = {}
        self.errorEvent = _ControlEvent()
        self.connectedEvent = _ControlEvent()
        self.disconnectedEvent = _ControlEvent()
        self.wrapper = SimpleNamespace(**{
            name: lambda *_args: None
            for name in (
                "accountSummary", "accountSummaryEnd", "position", "positionEnd",
                "openOrder", "openOrderEnd",
            )
        })
        self.client = self

    def connect(self, host: str, port: int, **kwargs: object) -> None:
        self.connected = True
        self.connect_kwargs = {"host": host, "port": port, **kwargs}
        self.client.reqPositions()
        self.connectedEvent.emit()

    def isConnected(self) -> bool:
        return self.connected

    def disconnect(self) -> None:
        self.connected = False
        self.disconnectedEvent.emit()

    def reqPositions(self):
        self.wrapper.positionEnd()

    def reqOpenOrders(self):
        self.wrapper.openOrderEnd()

    def reqAllOpenOrders(self):
        self.wrapper.openOrderEnd()


def _services(**overrides: object) -> OptionsCopilotServices:
    values: dict[str, object] = {
        "health_provider": lambda: {},
        "bootstrap_provider": lambda: {},
        "candidates_provider": lambda: [],
        "positions_provider": lambda: [],
        "learning_provider": lambda: {},
        "approval_handler": None,
        "approval_status_provider": None,
    }
    values.update(overrides)
    return OptionsCopilotServices(**values)  # type: ignore[arg-type]


def _route(app, path: str):
    return next(route.endpoint for route in app.routes if route.path == path)
