from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path

import pytest

from options_copilot.config import OptionsCopilotConfig
from options_copilot.security import cli as security_cli
from options_copilot.security.dpapi import DPAPISecretStore
from options_copilot.security.jin10_credentials import (
    JIN10_CREDENTIAL_SCHEMA,
    resolve_jin10_credential,
)
from options_copilot.security.local_api_keys import (
    JIN10_BINDING_SECRET_NAME,
    LocalApiKeyStore,
    LocalJin10EnvelopeReader,
    local_api_key_path,
)
from options_copilot.security.revocation import (
    JIN10_SECRET_NAME,
    REVOCATION_SCHEMA,
    REVOCATION_VERSION,
    RevocationAttestation,
    RevocationValidationError,
    RevocationWriteError,
    load_revocation_attestation,
    write_revocation_attestation,
)
from options_copilot.storage.canonical import canonical_hash, canonical_json


SIGNED_AT = datetime(2026, 8, 5, 1, 2, 3, 456789, tzinfo=timezone.utc)


class _FakeSecretStore:
    values: dict[str, str] = {}

    def __init__(self, _path: Path) -> None:
        pass

    def names(self) -> tuple[str, ...]:
        return tuple(sorted(self.values))

    def contains(self, name: str) -> bool:
        return name in self.values

    def get(self, name: str) -> str | None:
        return self.values.get(name)

    def set(self, name: str, value: str) -> None:
        self.values[name] = value

    def delete(self, name: str) -> bool:
        return self.values.pop(name, None) is not None


def _revocation_document() -> dict[str, object]:
    return RevocationAttestation.create(
        name=JIN10_SECRET_NAME,
        actor="human:test-operator",
        signed_at=SIGNED_AT,
    ).to_dict()


def _rehash(document: dict[str, object]) -> None:
    signable = dict(document)
    signable.pop("canonical_hash", None)
    document["canonical_hash"] = canonical_hash(signable)


def _install_fake_secret_store(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _FakeSecretStore.values = {}
    monkeypatch.setattr(security_cli, "DPAPISecretStore", _FakeSecretStore)
    monkeypatch.setenv("OPTIONS_COPILOT_DATA_DIR", str(tmp_path / "runtime"))
    monkeypatch.setenv("OPTIONS_COPILOT_LOG_DIR", str(tmp_path / "logs"))


def test_config_uses_isolated_g_drive_runtime_paths_and_locked_risk(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("OPTIONS_COPILOT_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("OPTIONS_COPILOT_LOG_DIR", str(tmp_path / "logs"))
    config = OptionsCopilotConfig.from_env()
    config.validate()
    config.ensure_runtime_directories()

    assert config.database_path.parent == (tmp_path / "data").resolve()
    assert config.normal_risk_fraction == 0.10
    assert config.a_grade_risk_fraction == 0.15
    assert config.hard_risk_fraction == 0.20
    assert config.max_open_combinations == 1
    assert config.live_instruction_enabled is False
    assert config.data_dir.is_dir() and config.log_dir.is_dir()


def test_config_auto_loads_one_installed_pacing_authority_on_every_start(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / "data"
    authority_dir = (
        data_dir
        / "evidence"
        / "checkpoints"
        / "P0"
        / "market-data-pacing"
        / "installed-policy"
    )
    authority_dir.mkdir(parents=True)
    (authority_dir / "capability.json").write_text("{}", encoding="utf-8")
    (authority_dir / "approval.json").write_text("{}", encoding="utf-8")
    keyring = data_dir / "governance" / "pacing_authority_keyring.json"
    keyring.parent.mkdir(parents=True)
    keyring.write_text("{}", encoding="utf-8")
    monkeypatch.setenv("OPTIONS_COPILOT_DATA_DIR", str(data_dir))
    monkeypatch.setenv("OPTIONS_COPILOT_LOG_DIR", str(tmp_path / "logs"))
    monkeypatch.delenv("OPTIONS_COPILOT_PACING_AUTHORITY_DIR", raising=False)

    first = OptionsCopilotConfig.from_env()
    second = OptionsCopilotConfig.from_env()

    assert first.pacing_authority_dir == authority_dir.resolve()
    assert second.pacing_authority_dir == authority_dir.resolve()
    assert first.pacing_authority_keyring_path == keyring.resolve()
    assert second.pacing_authority_keyring_path == keyring.resolve()


def test_live_instruction_cannot_be_enabled_by_environment(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("OPTIONS_COPILOT_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("OPTIONS_COPILOT_LIVE_INSTRUCTION_ENABLED", "true")
    config = OptionsCopilotConfig.from_env()
    assert config.live_instruction_enabled is False


def test_environment_cannot_loosen_quote_approval_or_reprice_gates(monkeypatch) -> None:
    monkeypatch.setenv("OPTIONS_COPILOT_QUOTE_FRESH_SECONDS", "30")
    monkeypatch.setenv("OPTIONS_COPILOT_APPROVAL_TTL_SECONDS", "900")
    monkeypatch.setenv("OPTIONS_COPILOT_ADVERSE_TOLERANCE_USD", "25")

    config = OptionsCopilotConfig.from_env()

    assert config.quote_fresh_seconds == 5.0
    assert config.approval_ttl_seconds == 300
    assert config.adverse_reprice_tolerance_usd == 5.0


def test_external_acquisition_requires_an_explicit_path_pair(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    feed = tmp_path / "external-readonly.json"
    top10 = tmp_path / "external-top10.json"
    session = tmp_path / "external-session.json"
    monkeypatch.setenv("OPTIONS_COPILOT_BROKER_ACQUISITION_MODE", "EXTERNAL")
    monkeypatch.setenv("OPTIONS_COPILOT_EXTERNAL_READONLY_FEED_PATH", str(feed))
    monkeypatch.setenv("OPTIONS_COPILOT_EXTERNAL_TOP10_PATH", str(top10))
    monkeypatch.setenv(
        "OPTIONS_COPILOT_EXTERNAL_SESSION_CALENDAR_PATH",
        str(session),
    )

    config = OptionsCopilotConfig.from_env()
    config.validate()

    assert config.broker_acquisition_mode == "EXTERNAL"
    assert config.external_readonly_feed_path == feed.resolve()
    assert config.external_top10_path == top10.resolve()
    assert config.external_session_calendar_path == session.resolve()


@pytest.mark.parametrize("missing", ["feed", "top10"])
def test_external_acquisition_rejects_a_partial_path_pair(
    tmp_path: Path,
    missing: str,
) -> None:
    config = OptionsCopilotConfig(
        data_dir=tmp_path / "data",
        log_dir=tmp_path / "logs",
        broker_acquisition_mode="EXTERNAL",
        external_readonly_feed_path=(
            None if missing == "feed" else tmp_path / "external-readonly.json"
        ),
        external_top10_path=(
            None if missing == "top10" else tmp_path / "external-top10.json"
        ),
    )

    with pytest.raises(ValueError, match="configured together"):
        config.validate()


def test_direct_acquisition_rejects_external_paths(tmp_path: Path) -> None:
    config = OptionsCopilotConfig(
        data_dir=tmp_path / "data",
        log_dir=tmp_path / "logs",
        broker_acquisition_mode="DIRECT",
        external_readonly_feed_path=tmp_path / "external-readonly.json",
        external_top10_path=tmp_path / "external-top10.json",
    )

    with pytest.raises(ValueError, match="DIRECT acquisition"):
        config.validate()


def test_news_refresh_and_core_universe_are_bounded(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("OPTIONS_COPILOT_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("OPTIONS_COPILOT_NEWS_REFRESH_SECONDS", "120")

    config = OptionsCopilotConfig.from_env()
    config.validate()

    assert config.news_refresh_seconds == 120
    assert 60 <= config.news_refresh_seconds <= 120
    assert {"SPY", "QQQ", "AAPL", "MSFT", "NVDA", "GLD"} <= set(
        config.news_core_symbols
    )
    assert config.news_evidence_path == tmp_path.resolve() / "news_evidence.sqlite3"


def test_news_model_requires_explicit_boolean_enablement(monkeypatch) -> None:
    assert OptionsCopilotConfig.from_env().news_llm_enabled is False

    monkeypatch.setenv("OPTIONS_COPILOT_NEWS_LLM_ENABLED", "true")
    assert OptionsCopilotConfig.from_env().news_llm_enabled is True

    monkeypatch.setenv("OPTIONS_COPILOT_NEWS_LLM_ENABLED", "sometimes")
    with pytest.raises(ValueError, match="must be a boolean"):
        OptionsCopilotConfig.from_env()


@pytest.mark.parametrize("seconds", [59, 121])
def test_news_refresh_outside_locked_range_is_rejected(
    tmp_path: Path, seconds: int
) -> None:
    config = OptionsCopilotConfig(
        data_dir=tmp_path / "data",
        log_dir=tmp_path / "logs",
        news_refresh_seconds=seconds,
    )

    with pytest.raises(ValueError, match="news refresh"):
        config.validate()


@pytest.mark.skipif(__import__("os").name != "nt", reason="Windows DPAPI only")
def test_dpapi_store_never_persists_plaintext(tmp_path: Path) -> None:
    path = tmp_path / "secrets.dpapi.json"
    store = DPAPISecretStore(path)
    secret = "test-secret-value-that-must-not-appear"

    store.set("FINNHUB_API_KEY", secret)

    assert store.get("FINNHUB_API_KEY") == secret
    assert store.names() == ("FINNHUB_API_KEY",)
    raw = path.read_text(encoding="utf-8")
    assert secret not in raw
    assert json.loads(raw)["version"] == 1
    assert store.delete("FINNHUB_API_KEY") is True
    assert store.get("FINNHUB_API_KEY") is None


def test_dpapi_store_rejects_unsafe_names(tmp_path: Path) -> None:
    store = DPAPISecretStore(tmp_path / "secrets.json")
    with pytest.raises(ValueError):
        store.set("jin10-token", "value")


def test_revocation_attestation_is_canonical_human_only_and_append_only(
    tmp_path: Path,
) -> None:
    attestation = RevocationAttestation.create(
        name=JIN10_SECRET_NAME,
        actor="human:test-operator",
        signed_at=SIGNED_AT,
    )

    path = write_revocation_attestation(attestation, tmp_path)
    loaded = load_revocation_attestation(path)
    document = json.loads(path.read_text(encoding="utf-8"))

    assert loaded == attestation
    assert document == attestation.to_dict()
    assert document["schema"] == REVOCATION_SCHEMA
    assert document["version"] == REVOCATION_VERSION
    assert document["old_token_revoked"] is True
    assert document["actor"] == document["signer"] == "human:test-operator"
    assert document["signed_at"].endswith("+00:00")
    assert document["canonical_hash"] == canonical_hash(attestation.signable_dict())
    assert not {
        "ciphertext",
        "fingerprint",
        "authorization",
        "authorization_header",
        "headers",
        "secret_value",
        "secret_fragment",
        "token_value",
        "token_fragment",
    }.intersection(document)

    with pytest.raises(RevocationWriteError, match="cannot be overwritten"):
        write_revocation_attestation(attestation, tmp_path)


@pytest.mark.parametrize(
    "fault",
    ["false", "tampered", "wrong-name", "token-material"],
)
def test_jin10_setter_rejects_invalid_revocation_before_hidden_prompt(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fault: str,
) -> None:
    _install_fake_secret_store(monkeypatch, tmp_path)
    document = _revocation_document()
    if fault == "false":
        document["old_token_revoked"] = False
        _rehash(document)
    elif fault == "tampered":
        document["actor"] = "human:different-operator"
    elif fault == "wrong-name":
        document["name"] = "FINNHUB_API_KEY"
        _rehash(document)
    else:
        document["ciphertext"] = "x"
        _rehash(document)
    path = tmp_path / f"invalid-{fault}.json"
    path.write_text(canonical_json(document) + "\n", encoding="utf-8")

    def unexpected_prompt(_prompt: str) -> str:
        raise AssertionError("invalid revocation evidence must fail before prompting")

    monkeypatch.setattr(security_cli.getpass, "getpass", unexpected_prompt)
    with pytest.raises(SystemExit):
        security_cli.main(
            [
                "set",
                JIN10_SECRET_NAME,
                "--revocation-attestation",
                str(path),
            ]
        )
    assert _FakeSecretStore.values == {}


def test_jin10_ordinary_set_is_refused_before_hidden_prompt(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _install_fake_secret_store(monkeypatch, tmp_path)

    def unexpected_prompt(_prompt: str) -> str:
        raise AssertionError("missing attestation must fail before prompting")

    monkeypatch.setattr(security_cli.getpass, "getpass", unexpected_prompt)
    with pytest.raises(SystemExit, match="refuses ordinary set"):
        security_cli.main(["set", JIN10_SECRET_NAME])
    assert _FakeSecretStore.values == {}


def test_non_jin10_set_keeps_hidden_double_input_without_attestation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _install_fake_secret_store(monkeypatch, tmp_path)
    entered_value = "held-in-memory-only"
    answers = iter((entered_value, entered_value))
    prompts: list[str] = []

    def hidden_prompt(prompt: str) -> str:
        prompts.append(prompt)
        return next(answers)

    monkeypatch.setattr(security_cli.getpass, "getpass", hidden_prompt)
    assert security_cli.main(["set", "DEEPSEEK_API_KEY"]) == 0

    output = capsys.readouterr().out
    assert len(prompts) == 2
    assert _FakeSecretStore.values == {"DEEPSEEK_API_KEY": entered_value}
    assert entered_value not in output


def test_valid_jin10_attestation_is_consumed_once_and_status_is_zero_material(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _install_fake_secret_store(monkeypatch, tmp_path)
    evidence_dir = tmp_path / "evidence"
    attestation = RevocationAttestation.create(
        name=JIN10_SECRET_NAME,
        actor="human:test-operator",
        signed_at=SIGNED_AT,
    )
    attestation_path = write_revocation_attestation(attestation, evidence_dir)
    entered_value = "held-in-memory-only"
    answers = iter((entered_value, entered_value))
    monkeypatch.setattr(
        security_cli.getpass, "getpass", lambda _prompt: next(answers)
    )

    assert (
        security_cli.main(
            [
                "set",
                JIN10_SECRET_NAME,
                "--revocation-attestation",
                str(attestation_path),
            ]
        )
        == 0
    )
    set_output = capsys.readouterr().out
    assert entered_value not in set_output
    stored_envelope = json.loads(_FakeSecretStore.values[JIN10_SECRET_NAME])
    assert stored_envelope["schema"] == JIN10_CREDENTIAL_SCHEMA
    assert stored_envelope["name"] == JIN10_SECRET_NAME
    assert stored_envelope["token"] == entered_value
    assert len(stored_envelope["credential_generation"]) == 32

    manifests = tuple(evidence_dir.glob("rotation-*.json"))
    assert len(manifests) == 1
    manifest = json.loads(manifests[0].read_text(encoding="utf-8"))
    assert set(manifest) == {"name", "attestation_hash", "rotated_at"}
    assert manifest["name"] == JIN10_SECRET_NAME
    assert manifest["attestation_hash"] == attestation.canonical_hash
    assert manifest["rotated_at"].endswith("+00:00")
    assert entered_value not in manifests[0].read_text(encoding="utf-8")
    activations = tuple(evidence_dir.glob("activation-*.json"))
    assert len(activations) == 1
    activation_text = activations[0].read_text(encoding="utf-8")
    assert entered_value not in activation_text
    assert stored_envelope["credential_generation"] in activation_text

    resolution = resolve_jin10_credential(
        _FakeSecretStore(tmp_path / "unused.json"), evidence_dir
    )
    assert resolution.status == "ACTIVATED"
    assert resolution.secret_store is not None
    assert resolution.secret_store.get(JIN10_SECRET_NAME) == entered_value

    assert (
        security_cli.main(
            ["verify-revocation", "--path", str(attestation_path), "--json"]
        )
        == 0
    )
    verified = json.loads(capsys.readouterr().out)
    assert verified["ok"] is True
    assert verified["canonical_hash"] == attestation.canonical_hash

    assert (
        security_cli.main(
            [
                "secret-status",
                "--name",
                JIN10_SECRET_NAME,
                "--evidence-dir",
                str(evidence_dir),
                "--json",
            ]
        )
        == 0
    )
    status = json.loads(capsys.readouterr().out)
    assert status["configured"] is True
    assert status["backend"] == "WINDOWS_DPAPI"
    assert status["rotation_attested"] is True
    assert status["activation_status"] == "ACTIVATED"
    assert status["attestation_hash"] == attestation.canonical_hash
    assert entered_value not in canonical_json(status)

    def unexpected_prompt(_prompt: str) -> str:
        raise AssertionError("a replay must fail before prompting")

    monkeypatch.setattr(security_cli.getpass, "getpass", unexpected_prompt)
    with pytest.raises(SystemExit, match="already consumed"):
        security_cli.main(
            [
                "set",
                JIN10_SECRET_NAME,
                "--revocation-attestation",
                str(attestation_path),
            ]
        )


def test_local_jin10_activation_consumes_revocation_without_exposing_token(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _install_fake_secret_store(monkeypatch, tmp_path)
    data_dir = tmp_path / "runtime"
    evidence_dir = tmp_path / "evidence"
    monkeypatch.setenv("OPTIONS_COPILOT_DATA_DIR", str(data_dir))
    monkeypatch.setenv("OPTIONS_COPILOT_LOG_DIR", str(tmp_path / "logs"))
    key_path = local_api_key_path(data_dir)
    key_path.parent.mkdir(parents=True, exist_ok=True)
    replacement = "replacement-token-must-never-be-printed"
    key_path.write_text(
        json.dumps(
            {
                "jin10_mcp_token": replacement,
                "finnhub_api_key": "",
                "alpha_vantage_api_key": "",
                "deepseek_api_key": "",
            }
        ),
        encoding="utf-8",
    )
    attestation = RevocationAttestation.create(
        name=JIN10_SECRET_NAME,
        actor="human:test-operator",
        signed_at=SIGNED_AT,
    )
    attestation_path = write_revocation_attestation(attestation, evidence_dir)

    assert security_cli.main(
        [
            "activate-local-jin10",
            "--revocation-attestation",
            str(attestation_path),
        ]
    ) == 0

    output = capsys.readouterr().out
    assert replacement not in output
    resolution = resolve_jin10_credential(
        LocalJin10EnvelopeReader(
            LocalApiKeyStore(key_path),
            _FakeSecretStore(tmp_path / "unused-binding.json"),
        ),
        evidence_dir,
    )
    assert resolution.status == "ACTIVATED"
    assert resolution.secret_store is not None
    assert resolution.secret_store.get(JIN10_SECRET_NAME) == replacement
    public_artifacts = "\n".join(
        path.read_text(encoding="utf-8")
        for path in evidence_dir.glob("*.json")
    )
    binding_payload = json.loads(_FakeSecretStore.values[JIN10_BINDING_SECRET_NAME])
    assert replacement not in public_artifacts
    assert binding_payload["token_digest"] not in public_artifacts


def test_jin10_activation_failure_leaves_new_dpapi_generation_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _install_fake_secret_store(monkeypatch, tmp_path)
    evidence_dir = tmp_path / "evidence"
    attestation = RevocationAttestation.create(
        name=JIN10_SECRET_NAME,
        actor="human:test-operator",
        signed_at=SIGNED_AT,
    )
    attestation_path = write_revocation_attestation(attestation, evidence_dir)
    entered_value = "must-not-appear-in-failure"
    answers = iter((entered_value, entered_value))
    monkeypatch.setattr(
        security_cli.getpass,
        "getpass",
        lambda _prompt: next(answers),
    )

    def fail_activation(*_args, **_kwargs):
        raise security_cli.Jin10CredentialError("Jin10 activation unavailable")

    monkeypatch.setattr(
        security_cli,
        "activate_jin10_credential",
        fail_activation,
    )

    with pytest.raises(SystemExit) as captured:
        security_cli.main(
            [
                "set",
                JIN10_SECRET_NAME,
                "--revocation-attestation",
                str(attestation_path),
            ]
        )

    output = capsys.readouterr()
    assert entered_value not in str(captured.value)
    assert entered_value not in output.out
    assert entered_value not in output.err
    assert tuple(evidence_dir.glob("rotation-*.json")) == ()
    resolution = resolve_jin10_credential(
        _FakeSecretStore(tmp_path / "unused.json"),
        evidence_dir,
    )
    assert resolution.secret_store is None
    assert resolution.status == "ROTATION_NOT_ATTESTED"


def test_powershell_setter_requires_jin10_attestation_and_never_reads_value() -> None:
    script = (
        Path(__file__).resolve().parents[2]
        / "scripts"
        / "set_options_copilot_secret.ps1"
    ).read_text(encoding="utf-8")

    assert '"DEEPSEEK_API_KEY"' in script
    assert '$Name -eq "JIN10_MCP_TOKEN" -and -not $hasRevocationAttestation' in script
    assert '"--revocation-attestation"' in script
    assert "Resolve-Path -LiteralPath $RevocationAttestation" in script
    assert "Read-Host" not in script
    assert "ConvertFrom-SecureString" not in script
    assert "$Secret" not in script
