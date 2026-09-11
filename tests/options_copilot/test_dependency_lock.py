"""Contracts for the hermetic, exact-hash dependency environments."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import shutil
import subprocess
import sys
from pathlib import Path
import uuid

import pytest


ROOT = Path(__file__).resolve().parents[2]
LOCKS = (ROOT / "requirements.lock", ROOT / "requirements-dev.lock")
SETUP_SCRIPT = ROOT / "scripts" / "setup_options_copilot_env.ps1"
VERIFIER_PATH = ROOT / "scripts" / "verify_locked_environment.py"
README_PATH = ROOT / "README.md"


def _ps_quote(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _setup_function_loader(*names: str) -> str:
    rendered_names = ", ".join(_ps_quote(name) for name in names)
    return f"""
$tokens = $null
$errors = $null
$ast = [System.Management.Automation.Language.Parser]::ParseFile(
    {_ps_quote(SETUP_SCRIPT)},
    [ref]$tokens,
    [ref]$errors
)
foreach ($name in @({rendered_names})) {{
    $definition = $ast.Find({{
        param($node)
        $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and
            $node.Name -eq $name
    }}, $true)
    Invoke-Expression $definition.Extent.Text
}}
$projectRoot = {_ps_quote(ROOT)}
"""


def _load_verifier():
    spec = importlib.util.spec_from_file_location("verify_locked_environment", VERIFIER_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_readme_describes_index_capable_verification_without_offline_claim() -> None:
    verification_section = README_PATH.read_text(encoding="utf-8").split(
        "## Verification",
        maxsplit=1,
    )[1].split("## Migrated local state", maxsplit=1)[0]

    assert "do not contact IBKR or create instructions" in verification_section
    assert "may access the configured package index" in verification_section
    assert "offline" not in verification_section.casefold()


@pytest.mark.parametrize("lock_path", LOCKS)
def test_locks_are_utf8_self_contained_exact_hash_closures(lock_path: Path) -> None:
    verifier = _load_verifier()
    text = lock_path.read_text(encoding="utf-8", errors="strict")

    assert "--hash=sha256:" in text
    inventory = verifier.parse_lock_text(text, source=str(lock_path))
    assert inventory
    assert all(version and "*" not in version for version in inventory.values())


@pytest.mark.parametrize(
    "unsafe_entry",
    (
        "-e .",
        "--editable .",
        "package @ git+https://example.invalid/repository.git",
        "git+https://example.invalid/repository.git",
        "package @ file:///G:/local/package",
        "G:/local/package",
        "package @ https://example.invalid/package.whl",
        "package==* --hash=sha256:" + "0" * 64,
        "package>=1 --hash=sha256:" + "0" * 64,
        "package==1.0",
        "-r requirements.lock",
        "--requirement requirements.lock",
        "-c constraints.txt",
        "--constraint constraints.txt",
    ),
)
def test_lock_parser_fails_closed_on_non_exact_or_external_entries(
    unsafe_entry: str,
) -> None:
    verifier = _load_verifier()

    with pytest.raises(verifier.LockContractError):
        verifier.parse_lock_text(unsafe_entry + "\n", source="unsafe.lock")


def test_development_lock_contains_the_exact_production_closure() -> None:
    verifier = _load_verifier()
    production = verifier.parse_lock_file(LOCKS[0])
    development = verifier.parse_lock_file(LOCKS[1])

    verifier.assert_lock_pair(production, development)
    assert production.items() <= development.items()
    assert {"pywin32", "tzdata"} <= production.keys()


@pytest.mark.parametrize(
    ("development", "expected_fragment"),
    (
        ({"beta": "2.0"}, "missing_from_development"),
        ({"alpha": "1.1", "beta": "2.0"}, "version_mismatches"),
    ),
)
def test_lock_pair_validation_rejects_incomplete_or_drifted_production_closure(
    development: dict[str, str],
    expected_fragment: str,
) -> None:
    verifier = _load_verifier()

    with pytest.raises(verifier.LockContractError, match=expected_fragment):
        verifier.assert_lock_pair(
            {"alpha": "1.0", "beta": "2.0"},
            development,
        )


def test_lock_pair_cli_validates_without_consulting_installed_inventory() -> None:
    completed = subprocess.run(
        [
            sys.executable,
            str(VERIFIER_PATH),
            "--production-lock",
            str(LOCKS[0]),
            "--development-lock",
            str(LOCKS[1]),
        ],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    payload = json.loads(completed.stdout)

    assert payload["status"] == "LOCK_PAIR_VALID"
    assert payload["production_locked"].items() <= payload[
        "development_locked"
    ].items()
    assert "installed" not in payload


@pytest.mark.parametrize(
    ("installed", "expected_fragment"),
    (
        ({"alpha": "1.0"}, "missing"),
        ({"alpha": "1.0", "beta": "2.0", "extra": "9"}, "surplus"),
        ({"alpha": "1.0", "beta": "2.1"}, "version_mismatches"),
    ),
)
def test_inventory_equality_rejects_missing_surplus_and_version_drift(
    installed: dict[str, str],
    expected_fragment: str,
) -> None:
    verifier = _load_verifier()
    expected = {"alpha": "1.0", "beta": "2.0"}

    with pytest.raises(verifier.InventoryMismatchError, match=expected_fragment):
        verifier.assert_inventory_equal(expected, installed, allowed_bootstrap=())


def test_inventory_equality_allows_only_documented_interpreter_bootstrap_names() -> None:
    verifier = _load_verifier()
    expected = {"alpha": "1.0"}
    installed = {"alpha": "1.0", "pip": "25.0", "setuptools": "80.0"}

    verifier.assert_inventory_equal(
        expected,
        installed,
        allowed_bootstrap=("pip", "setuptools"),
    )
    with pytest.raises(verifier.LockContractError, match="unsupported bootstrap"):
        verifier.assert_inventory_equal(
            expected,
            {**installed, "wheel": "0.45"},
            allowed_bootstrap=("pip", "setuptools", "wheel"),
        )


def test_installed_inventory_rejects_duplicate_same_version_distributions(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    verifier = _load_verifier()
    first_location = tmp_path / "first-site-packages"
    second_location = tmp_path / "second-site-packages"

    class FakeDistribution:
        metadata = {"Name": "Duplicate_Project"}
        version = "1.0.0"

        def __init__(self, location: Path) -> None:
            self.location = location

        def locate_file(self, relative: str) -> Path:
            return self.location / relative

    monkeypatch.setattr(
        verifier.importlib.metadata,
        "distributions",
        lambda: (
            FakeDistribution(first_location),
            FakeDistribution(second_location),
        ),
    )

    with pytest.raises(verifier.InventoryMismatchError) as exc_info:
        verifier.installed_inventory()

    message = str(exc_info.value)
    assert "duplicate-project" in message
    assert str(first_location) in message
    assert str(second_location) in message


def test_project_interpreter_exactly_matches_development_lock() -> None:
    completed = subprocess.run(
        [
            sys.executable,
            str(VERIFIER_PATH),
            "--lock",
            str(ROOT / "requirements-dev.lock"),
            "--allow-bootstrap",
            "pip",
            "--allow-bootstrap",
            "setuptools",
        ],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    payload = json.loads(completed.stdout)

    assert Path(payload["interpreter"]).resolve() == Path(sys.executable).resolve()
    assert payload["status"] == "EXACT"
    assert payload["allowed_bootstrap"] == ["pip", "setuptools"]
    assert "pip-tools" not in payload["installed"]
    assert "options-copilot-local" not in payload["installed"]


def test_setup_script_separates_bootstrap_target_and_production_environments() -> None:
    script = SETUP_SCRIPT.read_text(encoding="utf-8")

    assert 'ValidateSet("RegenerateAndCreate", "Verify")' in script
    assert '"pip-tools==7.6.0"' in script
    assert "requirements.lock" in script
    assert "requirements-dev.lock" in script
    assert "--resolver=backtracking" in script
    assert "--generate-hashes" in script
    assert "--strip-extras" in script
    assert "--extra=dev" in script
    assert "--require-hashes" in script
    assert "--no-deps" in script
    assert "verify_locked_environment.py" in script
    assert script.count("-m pip check") >= 2
    assert "bootstrap-" in script
    assert "production-verification-" in script
    assert "lock-generation-" in script
    assert "Remove-OwnedScratch" in script


def test_setup_script_validates_and_transactionally_publishes_generated_lock_pair() -> None:
    script = SETUP_SCRIPT.read_text(encoding="utf-8")

    assert '"--output-file=$generatedProductionLock"' in script
    assert '"--output-file=$generatedDevelopmentLock"' in script
    assert '"--output-file=requirements.lock"' not in script
    assert '"--output-file=requirements-dev.lock"' not in script
    pair_validation = script.index(
        '"--production-lock", $generatedProductionLock',
    )
    pair_publication = script.index("Publish-ValidatedLockPair `", pair_validation)
    target_creation = script.index(
        'Invoke-Checked -FilePath $pyLauncher -Arguments @("-3.12", "-m", "venv", $target)',
    )
    assert pair_validation < pair_publication < target_creation
    assert "Local\\OptionsCopilot-LockPairPublication-v1" in script
    assert "[System.IO.FileMode]::CreateNew" in script
    assert "[System.IO.FileShare]::None" in script
    assert script.count("Flush($true)") >= 3
    assert "Write-DurableLockTransaction" in script
    assert "Recover-LockPairTransaction" in script
    assert "new_production_sha256" in script
    assert "old_development_sha256" in script
    assert "-ValidateFinalPair $validatePublishedLocks" in script
    assert "requirements.lock.backup" in script
    assert "requirements-dev.lock.backup" in script
    maintenance_acquire = script.index(
        "$environmentMaintenanceGuard = Enter-EnvironmentMaintenanceTransaction",
    )
    target_absence_check = script.index(
        'if (Test-Path -LiteralPath $target)',
        maintenance_acquire,
    )
    final_audit = script.index('$audit = [ordered]@{', target_absence_check)
    maintenance_release = script.rindex(
        "Exit-EnvironmentMaintenanceTransaction",
    )
    assert maintenance_acquire < target_absence_check < final_audit < maintenance_release
    assert 'Join-Path $projectRoot "scripts\\start_options_copilot.ps1"' in script


@pytest.mark.skipif(shutil.which("pwsh") is None, reason="PowerShell 7 is unavailable")
def test_lock_pair_publication_restores_production_when_second_replace_fails() -> None:
    scratch = (
        ROOT
        / "data"
        / "options_copilot"
        / f"lock-publication-test-{uuid.uuid4().hex}"
    )
    scratch.mkdir(parents=True)
    generated = scratch / "generated.lock"
    production = scratch / "requirements.lock"
    development = scratch / "requirements-dev.lock"
    transaction_dir = scratch / "active-transaction"
    generated.write_text("new-production", encoding="utf-8")
    production.write_text("old-production", encoding="utf-8")
    development.write_text("old-development", encoding="utf-8")
    command = _setup_function_loader(
        "Assert-ProjectContainedPath",
        "New-OwnedDirectory",
        "Get-LockFileSha256",
        "Copy-DurableLockFile",
        "Invoke-AtomicLockReplace",
        "Write-DurableLockTransaction",
        "Enter-LockPairPublicationGuard",
        "Exit-LockPairPublicationGuard",
        "Resolve-LockPairTransactionPaths",
        "Assert-LockPairTransactionBackups",
        "Remove-LockPairTransactionArtifacts",
        "Restore-LockPairFromTransaction",
        "Recover-LockPairTransaction",
        "Publish-ValidatedLockPair",
    ) + f"""
$message = $null
try {{
    Publish-ValidatedLockPair `
        -GeneratedProductionLock {_ps_quote(generated)} `
        -GeneratedDevelopmentLock {_ps_quote(generated)} `
        -ProductionLock {_ps_quote(production)} `
        -DevelopmentLock {_ps_quote(development)} `
        -PublicationLockPath {_ps_quote(scratch / 'publication.lock')} `
        -TransactionPath {_ps_quote(transaction_dir / 'transaction.json')} `
        -ValidateFinalPair {{}}
}}
catch {{
    $message = $_.Exception.Message
}}
[pscustomobject]@{{
    message = $message
    production = [System.IO.File]::ReadAllText({_ps_quote(production)})
    development = [System.IO.File]::ReadAllText({_ps_quote(development)})
    production_backup_exists = Test-Path -LiteralPath (
        Join-Path {_ps_quote(transaction_dir)} 'requirements.lock.backup'
    )
    development_backup_exists = Test-Path -LiteralPath (
        Join-Path {_ps_quote(transaction_dir)} 'requirements-dev.lock.backup'
    )
    transaction_exists = Test-Path -LiteralPath {_ps_quote(transaction_dir / 'transaction.json')}
    publication_lock_exists = Test-Path -LiteralPath {_ps_quote(scratch / 'publication.lock')}
}} | ConvertTo-Json -Compress
"""
    try:
        completed = subprocess.run(
            ["pwsh", "-NoProfile", "-Command", command],
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        payload = json.loads(completed.stdout.strip().splitlines()[-1])

        assert payload["message"]
        assert payload["production"] == "old-production", payload
        assert payload["development"] == "old-development"
        assert payload["production_backup_exists"] is False
        assert payload["development_backup_exists"] is False
        assert payload["transaction_exists"] is False
        assert payload["publication_lock_exists"] is False
    finally:
        shutil.rmtree(scratch, ignore_errors=True)


@pytest.mark.skipif(shutil.which("pwsh") is None, reason="PowerShell 7 is unavailable")
def test_lock_pair_transaction_recovers_mixed_pair_after_process_loss() -> None:
    scratch = (
        ROOT
        / "data"
        / "options_copilot"
        / f"lock-recovery-test-{uuid.uuid4().hex}"
    )
    scratch.mkdir(parents=True)
    transaction_dir = scratch / "active-transaction"
    transaction_dir.mkdir()
    production = scratch / "requirements.lock"
    development = scratch / "requirements-dev.lock"
    production_backup = transaction_dir / "requirements.lock.backup"
    development_backup = transaction_dir / "requirements-dev.lock.backup"
    transaction = transaction_dir / "transaction.json"
    old_production = b"old-production"
    old_development = b"old-development"
    new_production = b"new-production"
    new_development = b"new-development"
    production.write_bytes(new_production)
    development.write_bytes(old_development)
    production_backup.write_bytes(old_production)
    development_backup.write_bytes(old_development)
    transaction.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "transaction_id": uuid.uuid4().hex,
                "production_lock": str(production),
                "development_lock": str(development),
                "production_backup": str(production_backup),
                "development_backup": str(development_backup),
                "old_production_sha256": hashlib.sha256(old_production).hexdigest(),
                "old_development_sha256": hashlib.sha256(old_development).hexdigest(),
                "new_production_sha256": hashlib.sha256(new_production).hexdigest(),
                "new_development_sha256": hashlib.sha256(new_development).hexdigest(),
            }
        ),
        encoding="utf-8",
    )
    command = _setup_function_loader(
        "Assert-ProjectContainedPath",
        "Get-LockFileSha256",
        "Copy-DurableLockFile",
        "Invoke-AtomicLockReplace",
        "Resolve-LockPairTransactionPaths",
        "Assert-LockPairTransactionBackups",
        "Remove-LockPairTransactionArtifacts",
        "Restore-LockPairFromTransaction",
        "Recover-LockPairTransaction",
    ) + f"""
Recover-LockPairTransaction `
    -TransactionPath {_ps_quote(transaction)} `
    -ProductionLock {_ps_quote(production)} `
    -DevelopmentLock {_ps_quote(development)}
"""
    try:
        subprocess.run(
            ["pwsh", "-NoProfile", "-Command", command],
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )

        assert production.read_bytes() == old_production
        assert development.read_bytes() == old_development
        assert not transaction.exists()
        assert not production_backup.exists()
        assert not development_backup.exists()
    finally:
        shutil.rmtree(scratch, ignore_errors=True)


@pytest.mark.skipif(shutil.which("pwsh") is None, reason="PowerShell 7 is unavailable")
def test_lock_pair_publish_recovers_durable_transaction_before_republishing() -> None:
    scratch = (
        ROOT
        / "data"
        / "options_copilot"
        / f"lock-publish-retry-test-{uuid.uuid4().hex}"
    )
    transaction_dir = scratch / "active-transaction"
    transaction_dir.mkdir(parents=True)
    generated_production = scratch / "generated-requirements.lock"
    generated_development = scratch / "generated-requirements-dev.lock"
    production = scratch / "requirements.lock"
    development = scratch / "requirements-dev.lock"
    production_backup = transaction_dir / "requirements.lock.backup"
    development_backup = transaction_dir / "requirements-dev.lock.backup"
    transaction = transaction_dir / "transaction.json"
    old_production = b"old-production"
    old_development = b"old-development"
    interrupted_production = b"interrupted-production"
    interrupted_development = b"interrupted-development"
    next_production = b"next-production"
    next_development = b"next-development"
    generated_production.write_bytes(next_production)
    generated_development.write_bytes(next_development)
    production.write_bytes(interrupted_production)
    development.write_bytes(old_development)
    production_backup.write_bytes(old_production)
    development_backup.write_bytes(old_development)
    transaction.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "transaction_id": uuid.uuid4().hex,
                "production_lock": str(production),
                "development_lock": str(development),
                "production_backup": str(production_backup),
                "development_backup": str(development_backup),
                "old_production_sha256": hashlib.sha256(old_production).hexdigest(),
                "old_development_sha256": hashlib.sha256(old_development).hexdigest(),
                "new_production_sha256": hashlib.sha256(
                    interrupted_production
                ).hexdigest(),
                "new_development_sha256": hashlib.sha256(
                    interrupted_development
                ).hexdigest(),
            }
        ),
        encoding="utf-8",
    )
    command = _setup_function_loader(
        "Assert-ProjectContainedPath",
        "New-OwnedDirectory",
        "Get-LockFileSha256",
        "Copy-DurableLockFile",
        "Invoke-AtomicLockReplace",
        "Write-DurableLockTransaction",
        "Enter-LockPairPublicationGuard",
        "Exit-LockPairPublicationGuard",
        "Resolve-LockPairTransactionPaths",
        "Assert-LockPairTransactionBackups",
        "Remove-LockPairTransactionArtifacts",
        "Restore-LockPairFromTransaction",
        "Recover-LockPairTransaction",
        "Publish-ValidatedLockPair",
    ) + f"""
Publish-ValidatedLockPair `
    -GeneratedProductionLock {_ps_quote(generated_production)} `
    -GeneratedDevelopmentLock {_ps_quote(generated_development)} `
    -ProductionLock {_ps_quote(production)} `
    -DevelopmentLock {_ps_quote(development)} `
    -PublicationLockPath {_ps_quote(scratch / 'publication.lock')} `
    -TransactionPath {_ps_quote(transaction)} `
    -ValidateFinalPair {{
        if (
            [System.IO.File]::ReadAllText({_ps_quote(production)}) -cne 'next-production' `
                -or [System.IO.File]::ReadAllText({_ps_quote(development)}) -cne 'next-development'
        ) {{
            throw 'published pair did not match generated inputs'
        }}
    }}
"""
    try:
        subprocess.run(
            ["pwsh", "-NoProfile", "-Command", command],
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )

        assert production.read_bytes() == next_production
        assert development.read_bytes() == next_development
        assert not transaction.exists()
        assert not production_backup.exists()
        assert not development_backup.exists()
        assert not (scratch / "publication.lock").exists()
    finally:
        shutil.rmtree(scratch, ignore_errors=True)


@pytest.mark.skipif(shutil.which("pwsh") is None, reason="PowerShell 7 is unavailable")
def test_setup_entry_recovers_transaction_before_verify_requires_target_python() -> None:
    scratch_root = (
        ROOT
        / "data"
        / "options_copilot"
        / f"setup-entry-recovery-test-{uuid.uuid4().hex}"
    )
    scripts_dir = scratch_root / "scripts"
    scripts_dir.mkdir(parents=True)
    setup_copy = scripts_dir / SETUP_SCRIPT.name
    start_copy = scripts_dir / "start_options_copilot.ps1"
    verifier_copy = scripts_dir / VERIFIER_PATH.name
    production = scratch_root / "requirements.lock"
    development = scratch_root / "requirements-dev.lock"
    setup_text = SETUP_SCRIPT.read_text(encoding="utf-8")
    setup_text = setup_text.replace(
        '$requiredProjectRoot = [System.IO.Path]::GetFullPath("G:\\OptionsCopilot")',
        "$requiredProjectRoot = [System.IO.Path]::GetFullPath(" +
        _ps_quote(scratch_root) + ")",
        1,
    )
    setup_copy.write_text(setup_text, encoding="utf-8")
    shutil.copy2(ROOT / "scripts" / start_copy.name, start_copy)
    shutil.copy2(VERIFIER_PATH, verifier_copy)
    shutil.copy2(LOCKS[0], production)
    shutil.copy2(LOCKS[1], development)
    transaction_dir = (
        scratch_root
        / "data"
        / "options_copilot"
        / "hermetic"
        / "lock-publication"
        / "active-transaction"
    )
    transaction = transaction_dir / "transaction.json"
    production_backup = transaction_dir / "requirements.lock.backup"
    development_backup = transaction_dir / "requirements-dev.lock.backup"
    old_production = production.read_bytes()
    old_development = development.read_bytes()
    transaction_id = uuid.uuid4().hex
    transaction_dir.mkdir(parents=True, exist_ok=False)
    production_backup.write_bytes(old_production)
    development_backup.write_bytes(old_development)
    transaction.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "transaction_id": transaction_id,
                "production_lock": str(production),
                "development_lock": str(development),
                "production_backup": str(production_backup),
                "development_backup": str(development_backup),
                "old_production_sha256": hashlib.sha256(old_production).hexdigest(),
                "old_development_sha256": hashlib.sha256(old_development).hexdigest(),
                "new_production_sha256": "0" * 64,
                "new_development_sha256": "1" * 64,
            }
        ),
        encoding="utf-8",
    )
    try:
        completed = subprocess.run(
            [
                "pwsh",
                "-NoProfile",
                "-File",
                str(setup_copy),
                "-Mode",
                "Verify",
            ],
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )

        assert completed.returncode != 0
        assert "Required hermetic artifact is missing" in (
            completed.stdout + completed.stderr
        )
        assert production.read_bytes() == old_production
        assert development.read_bytes() == old_development
        assert not transaction.exists()
        assert not production_backup.exists()
        assert not development_backup.exists()
    finally:
        shutil.rmtree(scratch_root, ignore_errors=True)


@pytest.mark.skipif(shutil.which("pwsh") is None, reason="PowerShell 7 is unavailable")
def test_lock_pair_failed_restore_preserves_durable_evidence_for_retry() -> None:
    scratch = (
        ROOT
        / "data"
        / "options_copilot"
        / f"lock-restore-retry-test-{uuid.uuid4().hex}"
    )
    invocation_scratch = scratch / "lock-generation-invocation"
    transaction_dir = scratch / "lock-publication" / "active-transaction"
    invocation_scratch.mkdir(parents=True)
    transaction_dir.mkdir(parents=True)
    production = scratch / "requirements.lock"
    development = scratch / "requirements-dev.lock"
    production_backup = transaction_dir / "requirements.lock.backup"
    development_backup = transaction_dir / "requirements-dev.lock.backup"
    transaction = transaction_dir / "transaction.json"
    old_production = b"old-production"
    old_development = b"old-development"
    new_production = b"new-production"
    new_development = b"new-development"
    production.write_bytes(new_production)
    development.write_bytes(new_development)
    production_backup.write_bytes(old_production)
    development_backup.write_bytes(old_development)
    transaction.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "transaction_id": uuid.uuid4().hex,
                "production_lock": str(production),
                "development_lock": str(development),
                "production_backup": str(production_backup),
                "development_backup": str(development_backup),
                "old_production_sha256": hashlib.sha256(old_production).hexdigest(),
                "old_development_sha256": hashlib.sha256(old_development).hexdigest(),
                "new_production_sha256": hashlib.sha256(new_production).hexdigest(),
                "new_development_sha256": hashlib.sha256(new_development).hexdigest(),
            }
        ),
        encoding="utf-8",
    )
    loader = _setup_function_loader(
        "Assert-ProjectContainedPath",
        "Get-LockFileSha256",
        "Copy-DurableLockFile",
        "Invoke-AtomicLockReplace",
        "Resolve-LockPairTransactionPaths",
        "Assert-LockPairTransactionBackups",
        "Remove-LockPairTransactionArtifacts",
        "Restore-LockPairFromTransaction",
        "Recover-LockPairTransaction",
    )
    failed_restore_command = loader + f"""
$script:replaceAttempt = 0
function Invoke-AtomicLockReplace {{
    param(
        [Parameter(Mandatory)][string]$Source,
        [Parameter(Mandatory)][string]$Destination,
        [Parameter(Mandatory)][string]$DiscardedDestination
    )
    $script:replaceAttempt += 1
    if ($script:replaceAttempt -eq 2) {{
        throw 'induced second restore failure'
    }}
    [System.IO.File]::Replace($Source, $Destination, $DiscardedDestination)
    if (Test-Path -LiteralPath $DiscardedDestination) {{
        Remove-Item -LiteralPath $DiscardedDestination -Force
    }}
}}
$message = $null
try {{
    Recover-LockPairTransaction `
        -TransactionPath {_ps_quote(transaction)} `
        -ProductionLock {_ps_quote(production)} `
        -DevelopmentLock {_ps_quote(development)}
}}
catch {{
    $message = $_.Exception.Message
}}
[pscustomobject]@{{
    message = $message
    production = [System.IO.File]::ReadAllText({_ps_quote(production)})
    development = [System.IO.File]::ReadAllText({_ps_quote(development)})
    journal = Test-Path -LiteralPath {_ps_quote(transaction)}
    production_backup = Test-Path -LiteralPath {_ps_quote(production_backup)}
    development_backup = Test-Path -LiteralPath {_ps_quote(development_backup)}
}} | ConvertTo-Json -Compress
"""
    retry_command = loader + f"""
Recover-LockPairTransaction `
    -TransactionPath {_ps_quote(transaction)} `
    -ProductionLock {_ps_quote(production)} `
    -DevelopmentLock {_ps_quote(development)}
"""
    try:
        failed = subprocess.run(
            ["pwsh", "-NoProfile", "-Command", failed_restore_command],
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        payload = json.loads(failed.stdout.strip().splitlines()[-1])

        assert "Manual recovery is required" in payload["message"]
        assert payload["production"] == "old-production"
        assert payload["development"] == "new-development"
        assert payload["journal"] is True
        assert payload["production_backup"] is True
        assert payload["development_backup"] is True

        shutil.rmtree(invocation_scratch)
        assert transaction.exists()
        assert production_backup.exists()
        assert development_backup.exists()

        subprocess.run(
            ["pwsh", "-NoProfile", "-Command", retry_command],
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        assert production.read_bytes() == old_production
        assert development.read_bytes() == old_development
        assert not transaction.exists()
        assert not production_backup.exists()
        assert not development_backup.exists()
    finally:
        shutil.rmtree(scratch, ignore_errors=True)


@pytest.mark.skipif(shutil.which("pwsh") is None, reason="PowerShell 7 is unavailable")
def test_lock_pair_recovery_rejects_journal_backup_path_authority() -> None:
    scratch = (
        ROOT
        / "data"
        / "options_copilot"
        / f"lock-path-authority-test-{uuid.uuid4().hex}"
    )
    transaction_dir = scratch / "active-transaction"
    transaction_dir.mkdir(parents=True)
    production = scratch / "requirements.lock"
    development = scratch / "requirements-dev.lock"
    unrelated_production = scratch / "unrelated-production.txt"
    unrelated_development = scratch / "unrelated-development.txt"
    transaction = transaction_dir / "transaction.json"
    current_production = b"current-production"
    current_development = b"current-development"
    production.write_bytes(current_production)
    development.write_bytes(current_development)
    unrelated_production.write_text("preserve-production", encoding="utf-8")
    unrelated_development.write_text("preserve-development", encoding="utf-8")
    transaction.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "transaction_id": uuid.uuid4().hex,
                "production_lock": str(production),
                "development_lock": str(development),
                "production_backup": str(unrelated_production),
                "development_backup": str(unrelated_development),
                "old_production_sha256": "0" * 64,
                "old_development_sha256": "1" * 64,
                "new_production_sha256": hashlib.sha256(current_production).hexdigest(),
                "new_development_sha256": hashlib.sha256(current_development).hexdigest(),
            }
        ),
        encoding="utf-8",
    )
    command = _setup_function_loader(
        "Assert-ProjectContainedPath",
        "Get-LockFileSha256",
        "Copy-DurableLockFile",
        "Invoke-AtomicLockReplace",
        "Resolve-LockPairTransactionPaths",
        "Assert-LockPairTransactionBackups",
        "Remove-LockPairTransactionArtifacts",
        "Restore-LockPairFromTransaction",
        "Recover-LockPairTransaction",
    ) + f"""
$message = $null
try {{
    Recover-LockPairTransaction `
        -TransactionPath {_ps_quote(transaction)} `
        -ProductionLock {_ps_quote(production)} `
        -DevelopmentLock {_ps_quote(development)} `
        -ValidateFinalPair {{}}
}}
catch {{
    $message = $_.Exception.Message
}}
[pscustomobject]@{{
    message = $message
    unrelated_production = [System.IO.File]::ReadAllText({_ps_quote(unrelated_production)})
    unrelated_development = [System.IO.File]::ReadAllText({_ps_quote(unrelated_development)})
    transaction_exists = Test-Path -LiteralPath {_ps_quote(transaction)}
}} | ConvertTo-Json -Compress
"""
    try:
        completed = subprocess.run(
            ["pwsh", "-NoProfile", "-Command", command],
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        payload = json.loads(completed.stdout.strip().splitlines()[-1])

        assert "backup paths do not match" in payload["message"]
        assert payload["unrelated_production"] == "preserve-production"
        assert payload["unrelated_development"] == "preserve-development"
        assert payload["transaction_exists"] is True
    finally:
        shutil.rmtree(scratch, ignore_errors=True)


@pytest.mark.skipif(shutil.which("pwsh") is None, reason="PowerShell 7 is unavailable")
def test_lock_pair_publication_guard_serializes_across_processes() -> None:
    scratch = (
        ROOT
        / "data"
        / "options_copilot"
        / f"lock-serialization-test-{uuid.uuid4().hex}"
    )
    scratch.mkdir(parents=True)
    lock_path = scratch / "publication.lock"
    loader = _setup_function_loader(
        "Assert-ProjectContainedPath",
        "Enter-LockPairPublicationGuard",
        "Exit-LockPairPublicationGuard",
    )
    holder_command = loader + f"""
$guard = Enter-LockPairPublicationGuard -LockPath {_ps_quote(lock_path)}
Write-Output 'LOCKED'
Start-Sleep -Seconds 3
Exit-LockPairPublicationGuard -Guard $guard
"""
    contender_command = loader + f"""
$message = $null
try {{
    $guard = Enter-LockPairPublicationGuard `
        -LockPath {_ps_quote(lock_path)} -WaitMilliseconds 100
    Exit-LockPairPublicationGuard -Guard $guard
}}
catch {{
    $message = $_.Exception.Message
}}
[pscustomobject]@{{ message = $message }} | ConvertTo-Json -Compress
"""
    holder = subprocess.Popen(
        ["pwsh", "-NoProfile", "-Command", holder_command],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
    )
    try:
        assert holder.stdout is not None
        assert holder.stdout.readline().strip() == "LOCKED"
        contender = subprocess.run(
            ["pwsh", "-NoProfile", "-Command", contender_command],
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        payload = json.loads(contender.stdout.strip().splitlines()[-1])
        assert "Timed out waiting" in payload["message"]
        stdout, stderr = holder.communicate(timeout=10)
        assert holder.returncode == 0, stdout + stderr
        assert not lock_path.exists()
    finally:
        if holder.poll() is None:
            holder.kill()
            holder.communicate()
        shutil.rmtree(scratch, ignore_errors=True)


@pytest.mark.skipif(shutil.which("pwsh") is None, reason="PowerShell 7 is unavailable")
def test_environment_maintenance_transaction_serializes_target_absence_check() -> None:
    scratch = (
        ROOT
        / "data"
        / "options_copilot"
        / f"environment-maintenance-test-{uuid.uuid4().hex}"
    )
    scratch.mkdir(parents=True)
    lock_path = scratch / "start_options_copilot.ps1"
    target = scratch / ".venv"
    lock_path.write_text("test lock anchor\n", encoding="utf-8")
    loader = _setup_function_loader(
        "Assert-ProjectContainedPath",
        "Enter-EnvironmentMaintenanceTransaction",
        "Exit-EnvironmentMaintenanceTransaction",
    )
    holder_command = loader + f"""
$guard = Enter-EnvironmentMaintenanceTransaction -LockPath {_ps_quote(lock_path)}
try {{
    if (Test-Path -LiteralPath {_ps_quote(target)}) {{
        throw 'holder unexpectedly observed an existing target'
    }}
    Write-Output 'ABSENCE_CHECKED'
    Start-Sleep -Seconds 2
    New-Item -ItemType Directory -Path {_ps_quote(target)} | Out-Null
}}
finally {{
    Exit-EnvironmentMaintenanceTransaction -Guard $guard
}}
"""
    contender_command = loader + f"""
$guard = Enter-EnvironmentMaintenanceTransaction -LockPath {_ps_quote(lock_path)}
try {{
    $targetWasAbsent = -not (Test-Path -LiteralPath {_ps_quote(target)})
}}
finally {{
    Exit-EnvironmentMaintenanceTransaction -Guard $guard
}}
[pscustomobject]@{{ target_was_absent = $targetWasAbsent }} | ConvertTo-Json -Compress
"""
    holder = subprocess.Popen(
        ["pwsh", "-NoProfile", "-Command", holder_command],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
    )
    try:
        assert holder.stdout is not None
        assert holder.stdout.readline().strip() == "ABSENCE_CHECKED"
        contender = subprocess.run(
            ["pwsh", "-NoProfile", "-Command", contender_command],
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=10,
        )
        payload = json.loads(contender.stdout.strip().splitlines()[-1])
        stdout, stderr = holder.communicate(timeout=10)

        assert holder.returncode == 0, stdout + stderr
        assert payload["target_was_absent"] is False
        assert target.is_dir()
        assert lock_path.read_text(encoding="utf-8") == "test lock anchor\n"
    finally:
        if holder.poll() is None:
            holder.kill()
            holder.communicate()
        shutil.rmtree(scratch, ignore_errors=True)


def test_setup_script_scopes_and_restores_g_drive_cache_and_temp_before_python() -> None:
    script = SETUP_SCRIPT.read_text(encoding="utf-8")
    lowered = script.lower()

    for name in ("PIP_CACHE_DIR", "PIP_NO_CACHE_DIR", "TEMP", "TMP"):
        assert name in script
    assert "$previousEnvironment[$name]" in script
    assert "data\\options_copilot\\hermetic" in script
    assert "finally" in script
    assert "[System.EnvironmentVariableTarget]::Process" in script
    assert "--no-cache-dir" in script
    assert '"G:\\OptionsCopilot"' in script
    assert "c:\\" not in lowered
    assert script.index("$env:PIP_CACHE_DIR") < script.index("Get-Command py.exe")


def test_setup_script_emits_sanitized_contained_path_audit() -> None:
    script = SETUP_SCRIPT.read_text(encoding="utf-8")

    for field in (
        "target",
        "bootstrap",
        "production_verification",
        "pip_cache",
        "temp",
        "tmp",
    ):
        assert field in script
    assert "ConvertTo-Json -Compress" in script
    assert "Assert-ProjectContainedPath" in script
    assert "GetFullPath" in script


def test_setup_script_rejects_reparse_point_artifact_ancestors() -> None:
    if shutil.which("pwsh") is None:
        pytest.skip("PowerShell 7 is unavailable")
    unique = uuid.uuid4().hex
    project = ROOT / "data" / "options_copilot" / f"setup-reparse-test-{unique}"
    outside_target = (
        ROOT / "data" / "options_copilot" / f"setup-reparse-target-{unique}"
    )
    scripts = project / "scripts"
    junction = project / ".venv"
    scripts.mkdir(parents=True)
    outside_target.mkdir(parents=True)
    script_text = SETUP_SCRIPT.read_text(encoding="utf-8").replace(
        r"G:\OptionsCopilot",
        str(project),
    )
    candidate = scripts / SETUP_SCRIPT.name
    candidate.write_text(script_text, encoding="utf-8")
    try:
        created = subprocess.run(
            [
                "pwsh",
                "-NoProfile",
                "-Command",
                (
                    "New-Item -ItemType Junction -Path "
                    f"'{junction}' -Target '{outside_target}' | Out-Null"
                ),
            ],
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        assert created.returncode == 0, created.stderr

        completed = subprocess.run(
            ["pwsh", "-NoProfile", "-File", str(candidate), "-Mode", "Verify"],
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )

        assert completed.returncode != 0
        assert "reparse-point ancestor" in (completed.stdout + completed.stderr)
        assert not (project / "data" / "options_copilot" / "hermetic").exists()
    finally:
        if junction.exists():
            junction.rmdir()
        shutil.rmtree(project, ignore_errors=True)
        shutil.rmtree(outside_target, ignore_errors=True)


def test_parser_only_tests_do_not_consult_a_global_interpreter() -> None:
    verifier = _load_verifier()
    inventory = verifier.parse_lock_text(
        "alpha==1.0 --hash=sha256:" + "a" * 64 + "\n",
        source="offline.lock",
    )

    assert inventory == {"alpha": "1.0"}
    assert "subprocess" not in verifier.parse_lock_text.__code__.co_names
