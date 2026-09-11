from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import socket
import subprocess
import sys
import time
import uuid

import pytest


ROOT = Path(__file__).resolve().parents[2]


def _ps_quote(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


@pytest.fixture
def approved_runtime_dirs(request: pytest.FixtureRequest) -> tuple[Path, Path]:
    unique = uuid.uuid4().hex
    data_dir = ROOT / "data" / "options_copilot" / f"launcher-test-{unique}"
    log_dir = ROOT / "logs" / "options_copilot" / f"launcher-test-{unique}"

    def cleanup() -> None:
        shutil.rmtree(data_dir, ignore_errors=True)
        shutil.rmtree(log_dir, ignore_errors=True)

    request.addfinalizer(cleanup)
    return data_dir, log_dir


def _copy_launcher_project(project: Path) -> Path:
    scripts = project / "scripts"
    scripts.mkdir(parents=True)
    shutil.copy2(ROOT / "scripts" / "start_options_copilot.ps1", scripts)
    shutil.copy2(ROOT / "scripts" / "verify_locked_environment.py", scripts)
    shutil.copy2(ROOT / "requirements-dev.lock", project)
    return scripts / "start_options_copilot.ps1"


def _copy_project_python(project: Path, *, source_python: Path | None = None) -> None:
    project_source = ROOT / ".venv" / "Scripts" / "python.exe"
    source = source_python or (
        project_source if project_source.is_file() else Path(sys.executable)
    )
    scripts = project / ".venv" / "Scripts"
    scripts.mkdir(parents=True)
    shutil.copy2(source, scripts / "python.exe")
    source_config = source.parent.parent / "pyvenv.cfg"
    if source_config.exists():
        shutil.copy2(source_config, project / ".venv" / "pyvenv.cfg")


def _run_launcher_harness(
    script: Path,
    data_dir: Path,
    log_dir: Path,
    *,
    port: int = 18891,
    extra_pythonpath: Path | None = None,
) -> dict[str, object]:
    command = f"""
function global:Start-Process {{
    param(
        [string]$FilePath,
        [object]$ArgumentList,
        [string]$WorkingDirectory,
        [object]$WindowStyle,
        [switch]$PassThru,
        [string]$RedirectStandardOutput,
        [string]$RedirectStandardError
    )
    $global:ChildStarted = $true
    $global:CapturedFilePath = $FilePath
    [pscustomobject]@{{ Id = 424242; HasExited = $true }}
}}
$env:OPTIONS_COPILOT_HOST = 'SENTINEL_HOST'
$global:ChildStarted = $false
$global:CapturedFilePath = $null
$message = $null
$scriptOutput = @()
try {{
    & {_ps_quote(script)} -Port {port} `
        -DataDir {_ps_quote(data_dir)} `
        -LogDir {_ps_quote(log_dir)} -Background |
        ForEach-Object {{ $scriptOutput += [string]$_ }}
}}
catch {{
    $message = $_.Exception.Message
}}
[pscustomobject]@{{
    message = $message
    output = $scriptOutput
    child_started = $global:ChildStarted
    captured_file_path = $global:CapturedFilePath
    restored_host = $env:OPTIONS_COPILOT_HOST
}} | ConvertTo-Json -Compress
"""
    environment = os.environ.copy()
    project_site_packages = ROOT / ".venv" / "Lib" / "site-packages"
    source_site_packages = (
        Path(sys.executable).resolve().parent.parent / "Lib" / "site-packages"
    )
    python_paths = [
        str(
            project_site_packages
            if project_site_packages.is_dir()
            else source_site_packages
        )
    ]
    if extra_pythonpath is not None:
        python_paths.insert(0, str(extra_pythonpath))
    environment["PYTHONPATH"] = os.pathsep.join(python_paths)
    completed = subprocess.run(
        ["pwsh", "-NoProfile", "-Command", command],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
        env=environment,
    )
    return json.loads(completed.stdout.strip().splitlines()[-1])


def _run_launcher_process_safety_harness(body: str) -> dict[str, object]:
    script = ROOT / "scripts" / "start_options_copilot.ps1"
    command = f"""
$pathComparison = [System.StringComparison]::OrdinalIgnoreCase
$tokens = $null
$parseErrors = $null
$ast = [System.Management.Automation.Language.Parser]::ParseFile(
    {_ps_quote(script)},
    [ref]$tokens,
    [ref]$parseErrors
)
if ($parseErrors.Count -ne 0) {{ throw $parseErrors[0].Message }}
foreach ($name in @(
    'Get-OwnedProcessTree',
    'Test-ExactProcessIdentity',
    'Stop-OwnedProcessTree'
)) {{
    $functionAst = $ast.Find({{
        param($node)
        $node -is [System.Management.Automation.Language.FunctionDefinitionAst] `
            -and $node.Name -eq $name
    }}, $true)
    if ($null -ne $functionAst) {{
        Invoke-Expression $functionAst.Extent.Text
    }}
}}
$global:Records = @()
$global:StoppedProcessIds = @()
function global:Get-CimInstance {{
    [CmdletBinding()]
    param(
        [string]$ClassName,
        [string]$Filter
    )
    if ($Filter -match 'ProcessId=(\\d+)') {{
        $expectedId = [int]$Matches[1]
        return @($global:Records | Where-Object {{
            [int]$_.ProcessId -eq $expectedId
        }})
    }}
    return @($global:Records)
}}
function global:Stop-Process {{
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)]
        [int]$Id,
        [switch]$Force
    )
    $global:StoppedProcessIds += $Id
    $global:Records = @($global:Records | Where-Object {{
        [int]$_.ProcessId -ne $Id
    }})
}}
function global:Start-Sleep {{
    param([int]$Milliseconds)
}}
{body}
"""
    completed = subprocess.run(
        ["pwsh", "-NoProfile", "-Command", command],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    return json.loads(completed.stdout.strip().splitlines()[-1])


def test_launcher_is_isolated_loopback_and_uses_runtime_factory() -> None:
    script = (ROOT / "scripts" / "start_options_copilot.ps1").read_text(encoding="utf-8")
    assert '"127.0.0.1"' in script
    assert script.count(
        "Get-NetTCPConnection -State Listen -LocalAddress $listenAddress"
    ) == 2
    assert "options_copilot.runtime:create_runtime_app" in script
    assert '"--factory"' in script
    assert "trade_copilot" not in script
    assert "paper_server" not in script
    assert "rithmic" not in script.lower()
    assert "Port $Port is already listening" in script


def test_background_startup_wait_is_bounded_for_slow_runtime() -> None:
    script = (ROOT / "scripts" / "start_options_copilot.ps1").read_text(
        encoding="utf-8",
    )

    match = re.search(
        r"\$backgroundStartupTimeoutSeconds\s*=\s*(\d+)",
        script,
    )
    assert match is not None
    timeout_seconds = int(match.group(1))
    assert 60 < timeout_seconds <= 90
    assert (
        "$deadline = [DateTime]::UtcNow.AddSeconds("
        "$backgroundStartupTimeoutSeconds)"
    ) in script


def test_launcher_uses_one_normalized_project_interpreter_after_preflight() -> None:
    script = (ROOT / "scripts" / "start_options_copilot.ps1").read_text(encoding="utf-8")

    assert '".venv\\Scripts\\python.exe"' in script
    assert "Start-Process -FilePath $projectPython" in script
    assert "& $projectPython @arguments" in script
    assert 'Start-Process -FilePath "python"' not in script
    assert "& python" not in script
    assert "Get-Command python" not in script
    assert "scripts\\verify_locked_environment.py" in script
    assert '"--lock", $dependencyLock' in script
    assert '"--allow-bootstrap", "pip"' in script
    assert '"--allow-bootstrap", "setuptools"' in script
    assert '"-m", "pip", "check"' in script
    assert "Get-FileHash -LiteralPath $dependencyLock -Algorithm SHA256" in script
    assert "Resolve-TrustedEnvironmentPath" in script
    assert "Assert-EnvironmentTrustPaths" in script
    assert script.count("$null = Assert-EnvironmentTrustPaths") >= 9
    assert "os.path.realpath(sys.executable)" in script
    assert "reported an unapproved executable identity" in script
    maintenance_acquire = script.index(
        "$environmentMaintenanceGuard = Enter-EnvironmentMaintenanceTransaction",
    )
    inventory_check = script.index("$inventoryOutput =", maintenance_acquire)
    child_start = script.index(
        "Start-Process -FilePath $projectPython",
        inventory_check,
    )
    maintenance_release = script.rindex(
        "Exit-EnvironmentMaintenanceTransaction",
    )
    assert maintenance_acquire < inventory_check < child_start < maintenance_release
    assert "$environmentMaintenanceLock = [System.IO.Path]::GetFullPath($PSCommandPath)" in script

    preflight_end = script.index("dependency_check=EXACT_LOCK_MATCH")
    assert preflight_end < script.index("New-Item -ItemType Directory")
    assert preflight_end < script.index("$env:OPTIONS_COPILOT_HOST")
    assert preflight_end < script.index("Start-Process -FilePath $projectPython")


@pytest.mark.skipif(shutil.which("pwsh") is None, reason="PowerShell 7 is unavailable")
def test_launcher_missing_interpreter_fails_before_any_side_effect(tmp_path: Path) -> None:
    script = _copy_launcher_project(tmp_path / "project")
    data_dir = tmp_path / "data"
    log_dir = tmp_path / "logs"

    payload = _run_launcher_harness(script, data_dir, log_dir)

    assert payload["message"] == "Project Python interpreter is unavailable."
    assert payload["output"] == []
    assert payload["child_started"] is False
    assert payload["captured_file_path"] is None
    assert payload["restored_host"] == "SENTINEL_HOST"
    assert not data_dir.exists()
    assert not log_dir.exists()


@pytest.mark.skipif(shutil.which("pwsh") is None, reason="PowerShell 7 is unavailable")
@pytest.mark.parametrize("missing_name", ("requirements-dev.lock", "verify_locked_environment.py"))
def test_launcher_missing_lock_or_verifier_fails_before_any_side_effect(
    tmp_path: Path,
    missing_name: str,
) -> None:
    project = tmp_path / "project"
    script = _copy_launcher_project(project)
    _copy_project_python(project)
    missing_path = (
        project / "requirements-dev.lock"
        if missing_name == "requirements-dev.lock"
        else project / "scripts" / missing_name
    )
    missing_path.unlink()
    data_dir = tmp_path / "data"
    log_dir = tmp_path / "logs"

    payload = _run_launcher_harness(script, data_dir, log_dir)

    assert payload["message"] in {
        "Development dependency lock is unavailable.",
        "Exact inventory verifier is unavailable.",
    }
    assert payload["output"] == []
    assert payload["child_started"] is False
    assert payload["restored_host"] == "SENTINEL_HOST"
    assert not data_dir.exists()
    assert not log_dir.exists()


@pytest.mark.skipif(shutil.which("pwsh") is None, reason="PowerShell 7 is unavailable")
def test_launcher_rejects_interpreter_resolving_outside_project_venv(
    tmp_path: Path,
) -> None:
    project = tmp_path / "project"
    script = _copy_launcher_project(project)
    subprocess.run(
        [
            "pwsh",
            "-NoProfile",
            "-Command",
            (
                "New-Item -ItemType Junction -Path "
                f"{_ps_quote(project / '.venv')} -Target "
                f"{_ps_quote(ROOT / '.venv')} | Out-Null"
            ),
        ],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    data_dir = tmp_path / "data"
    log_dir = tmp_path / "logs"

    payload = _run_launcher_harness(script, data_dir, log_dir)

    assert "reparse-point ancestor" in str(payload["message"])
    assert payload["child_started"] is False
    assert payload["restored_host"] == "SENTINEL_HOST"
    assert not data_dir.exists()
    assert not log_dir.exists()


@pytest.mark.skipif(shutil.which("pwsh") is None, reason="PowerShell 7 is unavailable")
@pytest.mark.parametrize(
    "junction_kind",
    ("venv_scripts", "scripts", "dependency_lock"),
)
def test_launcher_rejects_every_environment_trust_path_reparse_ancestor(
    tmp_path: Path,
    junction_kind: str,
) -> None:
    project = tmp_path / "project"
    outside = tmp_path / f"outside-{junction_kind}"
    outside.mkdir(parents=True)

    if junction_kind == "scripts":
        outside_scripts = outside / "scripts"
        outside_scripts.mkdir()
        shutil.copy2(
            ROOT / "scripts" / "start_options_copilot.ps1",
            outside_scripts,
        )
        shutil.copy2(
            ROOT / "scripts" / "verify_locked_environment.py",
            outside_scripts,
        )
        project.mkdir()
        shutil.copy2(ROOT / "requirements-dev.lock", project)
        _copy_project_python(project)
        link = project / "scripts"
        target = outside_scripts
        script = link / "start_options_copilot.ps1"
        item_type = "Junction"
    elif junction_kind == "venv_scripts":
        script = _copy_launcher_project(project)
        project_venv = project / ".venv"
        project_venv.mkdir()
        shutil.copy2(ROOT / ".venv" / "pyvenv.cfg", project_venv)
        outside_scripts = outside / "Scripts"
        outside_scripts.mkdir()
        shutil.copy2(
            ROOT / ".venv" / "Scripts" / "python.exe",
            outside_scripts,
        )
        link = project_venv / "Scripts"
        target = outside_scripts
        item_type = "Junction"
    else:
        script = _copy_launcher_project(project)
        _copy_project_python(project)
        outside_lock = outside / "requirements-dev.lock"
        shutil.copy2(ROOT / "requirements-dev.lock", outside_lock)
        link = project / "requirements-dev.lock"
        link.unlink()
        target = outside_lock
        item_type = "SymbolicLink"

    created = subprocess.run(
        [
            "pwsh",
            "-NoProfile",
            "-Command",
            (
                f"New-Item -ItemType {item_type} -Path "
                f"{_ps_quote(link)} -Target {_ps_quote(target)} | Out-Null"
            ),
        ],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    if created.returncode != 0 and junction_kind == "dependency_lock":
        pytest.skip("File symbolic links are unavailable on this Windows host")
    assert created.returncode == 0, created.stderr

    payload = _run_launcher_harness(
        script,
        tmp_path / "data",
        tmp_path / "logs",
    )

    assert "reparse-point ancestor" in str(payload["message"])
    assert payload["child_started"] is False
    assert not (tmp_path / "data").exists()
    assert not (tmp_path / "logs").exists()


@pytest.mark.skipif(shutil.which("pwsh") is None, reason="PowerShell 7 is unavailable")
def test_launcher_rejects_python_below_312_before_any_side_effect(tmp_path: Path) -> None:
    older = subprocess.run(
        ["py", "-3.10", "-c", "import sys; print(sys.executable)"],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    if older.returncode != 0:
        pytest.skip("A Python interpreter below 3.12 is unavailable")
    project = tmp_path / "project"
    script = _copy_launcher_project(project)
    _copy_project_python(project, source_python=Path(older.stdout.strip()))
    data_dir = tmp_path / "data"
    log_dir = tmp_path / "logs"

    payload = _run_launcher_harness(script, data_dir, log_dir)

    assert payload["message"] == "Project Python must be CPython 3.12 or newer."
    assert payload["child_started"] is False
    assert payload["restored_host"] == "SENTINEL_HOST"
    assert not data_dir.exists()
    assert not log_dir.exists()


@pytest.mark.skipif(shutil.which("pwsh") is None, reason="PowerShell 7 is unavailable")
@pytest.mark.parametrize(
    "lock_mutation",
    (
        "missing_locked_distribution",
        "extra_installed_distribution",
        "version_drift",
    ),
)
def test_launcher_inventory_drift_wins_even_when_pip_check_is_healthy(
    tmp_path: Path,
    lock_mutation: str,
) -> None:
    project = tmp_path / "project"
    script = _copy_launcher_project(project)
    _copy_project_python(project)
    lock = project / "requirements-dev.lock"
    lock_text = lock.read_text(encoding="utf-8")
    if lock_mutation == "missing_locked_distribution":
        lock_text += (
            "\nlauncher-missing-package==1.0.0 \\\n"
            "    --hash=sha256:" + "0" * 64 + "\n"
        )
    elif lock_mutation == "extra_installed_distribution":
        lock_text = re.sub(
            r"(?ms)^annotated-doc==.*?(?=^annotated-types==)",
            "",
            lock_text,
            count=1,
        )
    else:
        lock_text = lock_text.replace("annotated-doc==0.0.5", "annotated-doc==0.0.4", 1)
    lock.write_text(lock_text, encoding="utf-8")
    environment = os.environ.copy()
    project_site_packages = ROOT / ".venv" / "Lib" / "site-packages"
    environment["PYTHONPATH"] = str(
        project_site_packages
        if project_site_packages.is_dir()
        else Path(sys.executable).resolve().parent.parent / "Lib" / "site-packages"
    )
    healthy = subprocess.run(
        [str(project / ".venv" / "Scripts" / "python.exe"), "-m", "pip", "check"],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        env=environment,
    )
    assert healthy.returncode == 0, healthy.stderr
    data_dir = tmp_path / "data"
    log_dir = tmp_path / "logs"

    payload = _run_launcher_harness(script, data_dir, log_dir)

    assert payload["message"] == "Installed dependency inventory does not match the development lock."
    assert payload["output"] == []
    assert payload["child_started"] is False
    assert payload["restored_host"] == "SENTINEL_HOST"
    assert not data_dir.exists()
    assert not log_dir.exists()


@pytest.mark.skipif(shutil.which("pwsh") is None, reason="PowerShell 7 is unavailable")
def test_launcher_failed_pip_check_fails_before_any_side_effect(tmp_path: Path) -> None:
    project = tmp_path / "project"
    script = _copy_launcher_project(project)
    _copy_project_python(project)
    shadow = tmp_path / "shadow"
    metadata = shadow / "launcher_failing_package-1.0.0.dist-info" / "METADATA"
    metadata.parent.mkdir(parents=True)
    metadata.write_text(
        "Metadata-Version: 2.1\n"
        "Name: launcher-failing-package\n"
        "Version: 1.0.0\n"
        "Requires-Dist: launcher-impossible-dependency>=1\n",
        encoding="utf-8",
    )
    lock = project / "requirements-dev.lock"
    with lock.open("a", encoding="utf-8") as handle:
        handle.write(
            "\nlauncher-failing-package==1.0.0 \\\n"
            "    --hash=sha256:" + "0" * 64 + "\n"
        )
    data_dir = tmp_path / "data"
    log_dir = tmp_path / "logs"

    payload = _run_launcher_harness(
        script,
        data_dir,
        log_dir,
        extra_pythonpath=shadow,
    )

    assert payload["message"] == "Project dependency check failed."
    assert payload["output"] == []
    assert payload["child_started"] is False
    assert payload["restored_host"] == "SENTINEL_HOST"
    assert not data_dir.exists()
    assert not log_dir.exists()


@pytest.mark.skipif(shutil.which("pwsh") is None, reason="PowerShell 7 is unavailable")
def test_launcher_refuses_occupied_loopback_port_without_replacement(tmp_path: Path) -> None:
    project = tmp_path / "project"
    script = _copy_launcher_project(project)
    _copy_project_python(project)
    data_dir = tmp_path / "data"
    log_dir = tmp_path / "logs"
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        port = listener.getsockname()[1]
        payload = _run_launcher_harness(
            script,
            data_dir,
            log_dir,
            port=port,
        )

    assert f"Port {port} is already listening" in str(payload["message"])
    assert payload["child_started"] is False
    assert payload["restored_host"] == "SENTINEL_HOST"
    assert not data_dir.exists()
    assert not log_dir.exists()


@pytest.mark.skipif(shutil.which("pwsh") is None, reason="PowerShell 7 is unavailable")
def test_launcher_valid_preflight_emits_only_sanitized_identity_and_project_python(
    approved_runtime_dirs: tuple[Path, Path],
) -> None:
    script = ROOT / "scripts" / "start_options_copilot.ps1"
    data_dir, log_dir = approved_runtime_dirs
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]

    payload = _run_launcher_harness(
        script,
        data_dir.relative_to(ROOT),
        log_dir.relative_to(ROOT),
        port=port,
    )

    assert "did not begin listening" in str(payload["message"])
    assert payload["child_started"] is True
    assert Path(str(payload["captured_file_path"])) == (
        ROOT / ".venv" / "Scripts" / "python.exe"
    )
    assert payload["restored_host"] == "SENTINEL_HOST"
    identity_lines = list(payload["output"])
    assert len(identity_lines) == 1
    assert re.fullmatch(
        r"OptionsCopilot environment_identity "
        r"interpreter_path=.+; python_version=3\.12\.\d+; "
        r"lock_filename=requirements-dev\.lock; lock_type=development; "
        r"lock_sha256=[0-9a-f]{64}; dependency_check=EXACT_LOCK_MATCH",
        identity_lines[0],
    )
    lowered = identity_lines[0].lower()
    for forbidden in (
        "package",
        "credential",
        "provider",
        "account",
        "broker",
        "position",
        "order",
        "instruction",
    ):
        assert forbidden not in lowered


@pytest.mark.skipif(shutil.which("pwsh") is None, reason="PowerShell 7 is unavailable")
def test_background_launcher_accepts_only_verified_owned_listener_descendant(
    approved_runtime_dirs: tuple[Path, Path],
) -> None:
    script = ROOT / "scripts" / "start_options_copilot.ps1"
    data_dir, log_dir = approved_runtime_dirs
    shadow = data_dir / "launcher-process-tree-shadow"
    uvicorn_package = shadow / "uvicorn"
    uvicorn_package.mkdir(parents=True)
    (uvicorn_package / "__init__.py").write_text("", encoding="utf-8")
    (uvicorn_package / "__main__.py").write_text(
        """from __future__ import annotations

import os
from pathlib import Path
import socket
import sys
import time


def _argument(name: str) -> str:
    index = sys.argv.index(name)
    return sys.argv[index + 1]


pid_path = Path(os.environ["OPTIONS_COPILOT_LAUNCHER_TEST_PID_PATH"])
pid_path.write_text(str(os.getpid()), encoding="ascii")
time.sleep(16)
with socket.socket() as listener:
    listener.bind((_argument("--host"), int(_argument("--port"))))
    listener.listen()
    time.sleep(120)
""",
        encoding="utf-8",
    )
    pid_path = data_dir / "launcher-process-tree-child.pid"
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    environment = os.environ.copy()
    environment["PYTHONPATH"] = os.pathsep.join(
        part
        for part in (str(shadow), environment.get("PYTHONPATH", ""))
        if part
    )
    environment["OPTIONS_COPILOT_LAUNCHER_TEST_PID_PATH"] = str(pid_path)
    child_pid: int | None = None
    try:
        launcher_stdout = data_dir / "launcher.stdout.log"
        launcher_stderr = data_dir / "launcher.stderr.log"
        command = [
            "pwsh",
            "-NoProfile",
            "-File",
            str(script),
            "-Port",
            str(port),
            "-DataDir",
            str(data_dir),
            "-LogDir",
            str(log_dir),
            "-Background",
        ]
        with launcher_stdout.open("w", encoding="utf-8") as stdout_handle, (
            launcher_stderr.open("w", encoding="utf-8")
        ) as stderr_handle:
            launcher = subprocess.Popen(
                command,
                stdout=stdout_handle,
                stderr=stderr_handle,
                text=True,
                encoding="utf-8",
                env=environment,
            )
            returncode = launcher.wait(timeout=75)
        completed = subprocess.CompletedProcess(
            command,
            returncode,
            launcher_stdout.read_text(encoding="utf-8"),
            launcher_stderr.read_text(encoding="utf-8"),
        )
        if pid_path.is_file():
            child_pid = int(pid_path.read_text(encoding="ascii"))

        assert completed.returncode == 0, completed.stderr
        match = re.search(r"Options Copilot PID=(\d+)", completed.stdout)
        assert match is not None, completed.stdout
        reported_pid = int(match.group(1))
        assert child_pid == reported_pid

        evidence = subprocess.run(
            [
                "pwsh",
                "-NoProfile",
                "-Command",
                (
                    f"$p=Get-CimInstance Win32_Process -Filter 'ProcessId={reported_pid}'; "
                    "$parent=Get-CimInstance Win32_Process -Filter "
                    "\"ProcessId=$($p.ParentProcessId)\"; "
                    f"$l=@(Get-NetTCPConnection -State Listen -LocalAddress '127.0.0.1' -LocalPort {port}); "
                    "[pscustomobject]@{process_id=[int]$p.ProcessId; "
                    "executable_path=[string]$p.ExecutablePath; "
                    "command_line=[string]$p.CommandLine; "
                    "parent_process_id=[int]$p.ParentProcessId; "
                    "parent_executable_path=[string]$parent.ExecutablePath; "
                    "listener_count=$l.Count; listener_owner=[int]$l[0].OwningProcess} "
                    "| ConvertTo-Json -Compress"
                ),
            ],
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        identity = json.loads(evidence.stdout.strip())
        assert identity["process_id"] == reported_pid
        assert identity["listener_count"] == 1
        assert identity["listener_owner"] == reported_pid
        base_executable = subprocess.run(
            [
                str(ROOT / ".venv" / "Scripts" / "python.exe"),
                "-c",
                "import sys; print(sys._base_executable)",
            ],
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
        ).stdout.strip()
        assert Path(identity["executable_path"]) == Path(base_executable)
        assert Path(identity["parent_executable_path"]) == (
            ROOT / ".venv" / "Scripts" / "python.exe"
        )
        assert "-m uvicorn options_copilot.runtime:create_runtime_app" in identity[
            "command_line"
        ]
    finally:
        if child_pid is None and pid_path.is_file():
            child_pid = int(pid_path.read_text(encoding="ascii"))
        if child_pid is not None:
            subprocess.run(
                [
                    "pwsh",
                    "-NoProfile",
                    "-Command",
                    f"Stop-Process -Id {child_pid} -Force -ErrorAction SilentlyContinue",
                ],
                check=False,
                capture_output=True,
                text=True,
                encoding="utf-8",
            )


@pytest.mark.skipif(shutil.which("pwsh") is None, reason="PowerShell 7 is unavailable")
def test_owned_process_tree_is_empty_when_exact_root_is_absent() -> None:
    payload = _run_launcher_process_safety_harness(
        r"""
$created = [DateTime]::Parse('2026-08-09T09:00:00Z').ToUniversalTime()
$rootIdentity = [pscustomobject]@{
    ProcessId = 111
    ParentProcessId = 10
    CreationDate = $created
    ExecutablePath = 'G:\OptionsCopilot\.venv\Scripts\python.exe'
    CommandLine = 'expected-root-command'
    Depth = 0
}
$global:Records = @(
    [pscustomobject]@{
        ProcessId = 222
        ParentProcessId = 111
        CreationDate = $created.AddMilliseconds(10)
        ExecutablePath = 'C:\Python312\python.exe'
        CommandLine = 'expected-child-command'
    }
)
if ((Get-Command Get-OwnedProcessTree).Parameters.ContainsKey('RootIdentity')) {
    $tree = @(Get-OwnedProcessTree -RootIdentity $rootIdentity)
}
else {
    $tree = @(
        Get-OwnedProcessTree -RootProcessId 111 `
            -NotCreatedBeforeUtc $created.AddSeconds(-1)
    )
}
[pscustomobject]@{
    tree_count = $tree.Count
    process_ids = @($tree | ForEach-Object { [int]$_.ProcessId })
} | ConvertTo-Json -Compress
"""
    )

    assert payload == {"tree_count": 0, "process_ids": []}


@pytest.mark.skipif(shutil.which("pwsh") is None, reason="PowerShell 7 is unavailable")
def test_cleanup_skips_reused_or_mismatched_root_identity() -> None:
    payload = _run_launcher_process_safety_harness(
        r"""
$expectedCreated = [DateTime]::Parse(
    '2026-08-09T09:00:00Z'
).ToUniversalTime()
$rootIdentity = [pscustomobject]@{
    ProcessId = 111
    ParentProcessId = 10
    CreationDate = $expectedCreated
    ExecutablePath = 'G:\OptionsCopilot\.venv\Scripts\python.exe'
    CommandLine = 'expected-root-command'
    Depth = 0
}
$global:Records = @(
    [pscustomobject]@{
        ProcessId = 111
        ParentProcessId = 999
        CreationDate = $expectedCreated.AddSeconds(1)
        ExecutablePath = 'C:\Unrelated\python.exe'
        CommandLine = 'unrelated-command'
    },
    [pscustomobject]@{
        ProcessId = 333
        ParentProcessId = 111
        CreationDate = $expectedCreated.AddSeconds(2)
        ExecutablePath = 'C:\Unrelated\worker.exe'
        CommandLine = 'unrelated-child-command'
    }
)
if ((Get-Command Stop-OwnedProcessTree).Parameters.ContainsKey('RootIdentity')) {
    Stop-OwnedProcessTree -RootIdentity $rootIdentity
}
else {
    Stop-OwnedProcessTree -RootProcessId 111 `
        -NotCreatedBeforeUtc $expectedCreated.AddSeconds(-1)
}
[pscustomobject]@{
    stopped_count = $global:StoppedProcessIds.Count
    stopped_process_ids = @($global:StoppedProcessIds)
} | ConvertTo-Json -Compress
"""
    )

    assert payload == {"stopped_count": 0, "stopped_process_ids": []}


@pytest.mark.skipif(shutil.which("pwsh") is None, reason="PowerShell 7 is unavailable")
def test_launcher_attestation_blocks_lock_replacement_until_child_start() -> None:
    project = (
        ROOT
        / "data"
        / "options_copilot"
        / f"launcher-attestation-race-{uuid.uuid4().hex}"
    )
    script = _copy_launcher_project(project)
    _copy_project_python(project, source_python=Path(sys.executable))
    scripts = project / "scripts"
    verifier = scripts / "verify_locked_environment.py"
    real_verifier = scripts / "verify_locked_environment_real.py"
    verifier.replace(real_verifier)
    inventory_signal = project / "inventory-complete.signal"
    child_signal = project / "child-started.signal"
    mutation_signal = project / "lock-mutated.signal"
    maintenance_lock = script
    dependency_lock = project / "requirements-dev.lock"
    original_lock_sha256 = hashlib.sha256(dependency_lock.read_bytes()).hexdigest()
    verifier.write_text(
        "from __future__ import annotations\n"
        "import importlib.util\n"
        "import json\n"
        "from pathlib import Path\n"
        "import sys\n"
        "import time\n"
        f"real_path = Path({str(real_verifier)!r})\n"
        "spec = importlib.util.spec_from_file_location('real_verifier', real_path)\n"
        "assert spec is not None and spec.loader is not None\n"
        "module = importlib.util.module_from_spec(spec)\n"
        "sys.modules[spec.name] = module\n"
        "spec.loader.exec_module(module)\n"
        "payload = module.run()\n"
        f"Path({str(inventory_signal)!r}).write_text('complete', encoding='utf-8')\n"
        "time.sleep(2)\n"
        "print(json.dumps(payload, ensure_ascii=False, sort_keys=True))\n",
        encoding="utf-8",
    )
    data_dir = project / "data" / "options_copilot" / "runtime"
    log_dir = project / "logs" / "options_copilot" / "runtime"
    project_python = project / ".venv" / "Scripts" / "python.exe"
    launcher_command = f"""
function global:Start-Process {{
    param(
        [string]$FilePath,
        [object]$ArgumentList,
        [string]$WorkingDirectory,
        [object]$WindowStyle,
        [switch]$PassThru,
        [string]$RedirectStandardOutput,
        [string]$RedirectStandardError
    )
    $global:MutationSeenAtChildStart = Test-Path -LiteralPath {_ps_quote(mutation_signal)}
    [System.IO.File]::WriteAllText({_ps_quote(child_signal)}, 'started')
    [pscustomobject]@{{ Id = 424242; HasExited = $true }}
}}
$message = $null
$scriptOutput = @()
try {{
    & {_ps_quote(script)} -Port 18892 `
        -DataDir {_ps_quote(data_dir)} `
        -LogDir {_ps_quote(log_dir)} -Background |
        ForEach-Object {{ $scriptOutput += [string]$_ }}
}}
catch {{
    $message = $_.Exception.Message
}}
[pscustomobject]@{{
    message = $message
    output = $scriptOutput
    mutation_seen_at_child_start = $global:MutationSeenAtChildStart
}} | ConvertTo-Json -Compress
"""
    mutator_command = f"""
$deadline = [DateTime]::UtcNow.AddSeconds(15)
$stream = $null
while ($null -eq $stream -and [DateTime]::UtcNow -lt $deadline) {{
    try {{
        $stream = [System.IO.File]::Open(
            {_ps_quote(maintenance_lock)},
            [System.IO.FileMode]::Open,
            [System.IO.FileAccess]::Read,
            [System.IO.FileShare]::None
        )
    }}
    catch [System.IO.IOException] {{
        Start-Sleep -Milliseconds 25
    }}
}}
if ($null -eq $stream) {{ throw 'mutator timed out waiting for maintenance lock' }}
try {{
    [System.IO.File]::AppendAllText({_ps_quote(dependency_lock)}, "`n# race mutation`n")
    [System.IO.File]::WriteAllText({_ps_quote(mutation_signal)}, 'mutated')
}}
finally {{
    $stream.Dispose()
}}
"""
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(
        Path(sys.executable).resolve().parent.parent / "Lib" / "site-packages"
    )
    launcher = subprocess.Popen(
        ["pwsh", "-NoProfile", "-Command", launcher_command],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        env=environment,
    )
    mutator: subprocess.Popen[str] | None = None
    try:
        deadline = time.monotonic() + 15
        while not inventory_signal.exists() and time.monotonic() < deadline:
            time.sleep(0.025)
        assert inventory_signal.exists(), "launcher inventory verifier did not complete"
        mutator = subprocess.Popen(
            ["pwsh", "-NoProfile", "-Command", mutator_command],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
        )
        launcher_stdout, launcher_stderr = launcher.communicate(timeout=30)
        mutator_stdout, mutator_stderr = mutator.communicate(timeout=30)
        assert launcher.returncode == 0, launcher_stdout + launcher_stderr
        assert mutator.returncode == 0, mutator_stdout + mutator_stderr
        payload = json.loads(launcher_stdout.strip().splitlines()[-1])

        identity_lines = list(payload["output"])
        assert payload["mutation_seen_at_child_start"] is False
        assert child_signal.exists()
        assert mutation_signal.exists()
        assert len(identity_lines) == 1
        assert f"lock_sha256={original_lock_sha256}" in identity_lines[0]
        assert Path(str(project_python)).is_file()
    finally:
        if launcher.poll() is None:
            launcher.kill()
            launcher.communicate()
        if mutator is not None and mutator.poll() is None:
            mutator.kill()
            mutator.communicate()
        shutil.rmtree(project, ignore_errors=True)


def test_launcher_supports_an_explicit_fail_closed_acceptance_instance() -> None:
    script = (ROOT / "scripts" / "start_options_copilot.ps1").read_text(encoding="utf-8")
    assert "[string]$DataDir" in script
    assert "[string]$LogDir" in script
    assert "[switch]$DisablePacingAuthority" in script
    assert '$disabledPacingPath = Join-Path $dataDir ".pacing-authority-disabled"' in script
    assert "Test-Path -LiteralPath $disabledPacingPath" in script
    assert (
        "$env:OPTIONS_COPILOT_PACING_AUTHORITY_DIR = $disabledPacingPath"
        in script
    )
    assert "Remove-Item Env:OPTIONS_COPILOT_PACING_AUTHORITY_DIR" in script
    assert "$env:OPTIONS_COPILOT_DATA_DIR = $dataDir" in script
    assert "$env:OPTIONS_COPILOT_LOG_DIR = $logDir" in script
    assert "DataDir and LogDir must be different directories" in script
    assert "Resolve-ApprovedRuntimeDirectory" in script
    assert '"G:\\"' in script
    assert "reparse-point ancestor" in script


def test_launcher_supports_explicit_external_readonly_inputs() -> None:
    script = (ROOT / "scripts" / "start_options_copilot.ps1").read_text(encoding="utf-8")
    assert '[ValidateSet("DIRECT", "EXTERNAL")]' in script
    assert '[string]$BrokerAcquisitionMode = "DIRECT"' in script
    assert "[string]$ExternalReadonlyFeedPath" in script
    assert "[string]$ExternalTop10Path" in script
    assert "[string]$ExternalSessionCalendarPath" in script
    assert (
        '$env:OPTIONS_COPILOT_BROKER_ACQUISITION_MODE = $BrokerAcquisitionMode'
        in script
    )
    assert (
        '$env:OPTIONS_COPILOT_EXTERNAL_READONLY_FEED_PATH = $externalReadonlyFeedPath'
        in script
    )
    assert '$env:OPTIONS_COPILOT_EXTERNAL_TOP10_PATH = $externalTop10Path' in script
    assert (
        '$env:OPTIONS_COPILOT_EXTERNAL_SESSION_CALENDAR_PATH = '
        '$externalSessionCalendarPath'
        in script
    )
    assert "EXTERNAL acquisition requires all three external input paths" in script
    assert "[System.StringComparer]::OrdinalIgnoreCase" in script


def test_direct_launcher_clears_inherited_external_input_environment() -> None:
    script = (ROOT / "scripts" / "start_options_copilot.ps1").read_text(encoding="utf-8")
    assert "Remove-Item Env:OPTIONS_COPILOT_EXTERNAL_READONLY_FEED_PATH" in script
    assert "Remove-Item Env:OPTIONS_COPILOT_EXTERNAL_TOP10_PATH" in script
    assert "Remove-Item Env:OPTIONS_COPILOT_EXTERNAL_SESSION_CALENDAR_PATH" in script
    assert "$previousEnvironment" in script
    assert "[System.EnvironmentVariableTarget]::Process" in script
    assert "finally" in script
    assert "OPTIONS_COPILOT_PACING_AUTHORITY_DIR" in script
    assert "Get-OwnedProcessTree" in script
    assert "Test-OptionsCopilotRuntimeProcessIdentity" in script
    assert "Stop-OwnedProcessTree" in script
    assert 'Write-Output "Options Copilot PID=$($listener.OwningProcess)' in script


def test_launcher_exposes_explicit_inherited_provider_proxy_switch() -> None:
    script = (ROOT / "scripts" / "start_options_copilot.ps1").read_text(encoding="utf-8")
    assert "[switch]$UseInheritedProviderProxy" in script


def test_launcher_exposes_scoped_news_llm_enablement() -> None:
    script = (ROOT / "scripts" / "start_options_copilot.ps1").read_text(encoding="utf-8")
    assert "[switch]$EnableNewsLlm" in script
    assert '"OPTIONS_COPILOT_NEWS_LLM_ENABLED"' in script
    assert (
        '$env:OPTIONS_COPILOT_NEWS_LLM_ENABLED = if ($EnableNewsLlm) '
        '{ "true" } else { "false" }'
        in script
    )
    assert "[Console]::OutputEncoding = $utf8NoBom" in script


@pytest.mark.skipif(shutil.which("pwsh") is None, reason="PowerShell 7 is unavailable")
@pytest.mark.parametrize(
    ("proxy_switch", "expected_proxy", "expected_no_proxy"),
    (
        ("", None, "127.0.0.1,localhost"),
        (
            "-UseInheritedProviderProxy",
            "SENTINEL_PROXY",
            "SENTINEL_NO_PROXY,127.0.0.1,localhost",
        ),
    ),
)
def test_launcher_scopes_environment_to_the_child_process(
    approved_runtime_dirs: tuple[Path, Path],
    proxy_switch: str,
    expected_proxy: str | None,
    expected_no_proxy: str,
) -> None:
    script = ROOT / "scripts" / "start_options_copilot.ps1"
    data_dir, log_dir = approved_runtime_dirs
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]

    def ps_quote(value: object) -> str:
        return "'" + str(value).replace("'", "''") + "'"

    command = f"""
function global:Start-Process {{
    param(
        [string]$FilePath,
        [object]$ArgumentList,
        [string]$WorkingDirectory,
        [object]$WindowStyle,
        [switch]$PassThru,
        [string]$RedirectStandardOutput,
        [string]$RedirectStandardError
    )
    $global:CapturedMode = $env:OPTIONS_COPILOT_BROKER_ACQUISITION_MODE
    $global:CapturedFeed = $env:OPTIONS_COPILOT_EXTERNAL_READONLY_FEED_PATH
    $global:CapturedTop10 = $env:OPTIONS_COPILOT_EXTERNAL_TOP10_PATH
    $global:CapturedCalendar = $env:OPTIONS_COPILOT_EXTERNAL_SESSION_CALENDAR_PATH
    $global:CapturedPacing = $env:OPTIONS_COPILOT_PACING_AUTHORITY_DIR
    $global:CapturedHttpProxy = $env:HTTP_PROXY
    $global:CapturedHttpsProxy = $env:HTTPS_PROXY
    $global:CapturedAllProxy = $env:ALL_PROXY
    $global:CapturedLowerHttpProxy = $env:http_proxy
    $global:CapturedLowerHttpsProxy = $env:https_proxy
    $global:CapturedLowerAllProxy = $env:all_proxy
    $global:CapturedNoProxy = $env:NO_PROXY
    $global:CapturedLowerNoProxy = $env:no_proxy
    [pscustomobject]@{{ Id = 424242; HasExited = $true }}
}}
$env:OPTIONS_COPILOT_BROKER_ACQUISITION_MODE = 'SENTINEL_MODE'
$env:OPTIONS_COPILOT_EXTERNAL_READONLY_FEED_PATH = 'SENTINEL_FEED'
$env:OPTIONS_COPILOT_EXTERNAL_TOP10_PATH = 'SENTINEL_TOP10'
$env:OPTIONS_COPILOT_EXTERNAL_SESSION_CALENDAR_PATH = 'SENTINEL_CALENDAR'
$env:OPTIONS_COPILOT_PACING_AUTHORITY_DIR = 'SENTINEL_PACING'
$env:HTTP_PROXY = 'SENTINEL_PROXY'
$env:HTTPS_PROXY = 'SENTINEL_PROXY'
$env:ALL_PROXY = 'SENTINEL_PROXY'
$env:http_proxy = 'SENTINEL_PROXY'
$env:https_proxy = 'SENTINEL_PROXY'
$env:all_proxy = 'SENTINEL_PROXY'
$env:NO_PROXY = 'SENTINEL_NO_PROXY'
$env:no_proxy = 'SENTINEL_NO_PROXY'
$message = $null
try {{
    & {ps_quote(script)} -Port {port} `
        -DataDir {ps_quote(data_dir)} `
        -LogDir {ps_quote(log_dir)} `
        -BrokerAcquisitionMode DIRECT {proxy_switch} -Background
}}
catch {{
    $message = $_.Exception.Message
}}
[pscustomobject]@{{
    message = $message
    captured_mode = $global:CapturedMode
    captured_feed = $global:CapturedFeed
    captured_top10 = $global:CapturedTop10
    captured_calendar = $global:CapturedCalendar
    captured_pacing = $global:CapturedPacing
    captured_http_proxy = $global:CapturedHttpProxy
    captured_https_proxy = $global:CapturedHttpsProxy
    captured_all_proxy = $global:CapturedAllProxy
    captured_lower_http_proxy = $global:CapturedLowerHttpProxy
    captured_lower_https_proxy = $global:CapturedLowerHttpsProxy
    captured_lower_all_proxy = $global:CapturedLowerAllProxy
    captured_no_proxy = $global:CapturedNoProxy
    captured_lower_no_proxy = $global:CapturedLowerNoProxy
    restored_mode = $env:OPTIONS_COPILOT_BROKER_ACQUISITION_MODE
    restored_feed = $env:OPTIONS_COPILOT_EXTERNAL_READONLY_FEED_PATH
    restored_top10 = $env:OPTIONS_COPILOT_EXTERNAL_TOP10_PATH
    restored_calendar = $env:OPTIONS_COPILOT_EXTERNAL_SESSION_CALENDAR_PATH
    restored_pacing = $env:OPTIONS_COPILOT_PACING_AUTHORITY_DIR
    restored_http_proxy = $env:HTTP_PROXY
    restored_https_proxy = $env:HTTPS_PROXY
    restored_all_proxy = $env:ALL_PROXY
    restored_lower_http_proxy = $env:http_proxy
    restored_lower_https_proxy = $env:https_proxy
    restored_lower_all_proxy = $env:all_proxy
    restored_no_proxy = $env:NO_PROXY
    restored_lower_no_proxy = $env:no_proxy
}} | ConvertTo-Json -Compress
"""
    completed = subprocess.run(
        ["pwsh", "-NoProfile", "-Command", command],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    payload = json.loads(completed.stdout.strip().splitlines()[-1])

    assert "did not begin listening" in payload["message"]
    assert payload["captured_mode"] == "DIRECT"
    assert payload["captured_feed"] is None
    assert payload["captured_top10"] is None
    assert payload["captured_calendar"] is None
    assert payload["captured_pacing"] is None
    assert payload["captured_http_proxy"] == expected_proxy
    assert payload["captured_https_proxy"] == expected_proxy
    assert payload["captured_all_proxy"] == expected_proxy
    assert payload["captured_lower_http_proxy"] == expected_proxy
    assert payload["captured_lower_https_proxy"] == expected_proxy
    assert payload["captured_lower_all_proxy"] == expected_proxy
    assert payload["captured_no_proxy"] == expected_no_proxy
    assert payload["captured_lower_no_proxy"] == expected_no_proxy
    assert payload["restored_mode"] == "SENTINEL_MODE"
    assert payload["restored_feed"] == "SENTINEL_FEED"
    assert payload["restored_top10"] == "SENTINEL_TOP10"
    assert payload["restored_calendar"] == "SENTINEL_CALENDAR"
    assert payload["restored_pacing"] == "SENTINEL_PACING"
    assert payload["restored_http_proxy"] == "SENTINEL_PROXY"
    assert payload["restored_https_proxy"] == "SENTINEL_PROXY"
    assert payload["restored_all_proxy"] == "SENTINEL_PROXY"
    assert payload["restored_lower_http_proxy"] == "SENTINEL_PROXY"
    assert payload["restored_lower_https_proxy"] == "SENTINEL_PROXY"
    assert payload["restored_lower_all_proxy"] == "SENTINEL_PROXY"
    assert payload["restored_no_proxy"] == "SENTINEL_NO_PROXY"
    assert payload["restored_lower_no_proxy"] == "SENTINEL_NO_PROXY"


@pytest.mark.skipif(shutil.which("pwsh") is None, reason="PowerShell 7 is unavailable")
def test_external_launcher_rejects_case_only_duplicate_windows_paths(
    tmp_path: Path,
    approved_runtime_dirs: tuple[Path, Path],
) -> None:
    script = ROOT / "scripts" / "start_options_copilot.ps1"
    data_dir, log_dir = approved_runtime_dirs
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    feed = tmp_path / "inputs" / "external-feed.json"
    completed = subprocess.run(
        [
            "pwsh",
            "-NoProfile",
            "-File",
            str(script),
            "-Port",
            str(port),
            "-DataDir",
            str(data_dir),
            "-LogDir",
            str(log_dir),
            "-BrokerAcquisitionMode",
            "EXTERNAL",
            "-ExternalReadonlyFeedPath",
            str(feed),
            "-ExternalTop10Path",
            str(feed).upper(),
            "-ExternalSessionCalendarPath",
            str(tmp_path / "inputs" / "external-calendar.json"),
            "-Background",
        ],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )

    assert completed.returncode != 0
    assert "External input paths must identify three different files" in (
        completed.stderr + completed.stdout
    )
    assert not data_dir.exists()
    assert not log_dir.exists()


@pytest.mark.skipif(shutil.which("pwsh") is None, reason="PowerShell 7 is unavailable")
@pytest.mark.parametrize("invalid_kind", ("c_drive", "unc", "relative_escape"))
def test_launcher_rejects_unapproved_runtime_paths_before_mutation(
    tmp_path: Path,
    approved_runtime_dirs: tuple[Path, Path],
    invalid_kind: str,
) -> None:
    script = ROOT / "scripts" / "start_options_copilot.ps1"
    approved_data, approved_log = approved_runtime_dirs
    unique = uuid.uuid4().hex
    escaped = ROOT / f"launcher-relative-escape-{unique}"
    if invalid_kind == "c_drive":
        invalid_data: object = Path("C:/") / f"options-copilot-forbidden-{unique}"
        expected = "DataDir must remain on G:."
    elif invalid_kind == "unc":
        invalid_data = rf"\\127.0.0.1\options-copilot-missing\{unique}"
        expected = "DataDir must remain on G:."
    else:
        invalid_data = rf"data\options_copilot\..\..\{escaped.name}"
        expected = "DataDir must remain under its approved project runtime root."
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]

    payload = _run_launcher_harness(
        script,
        invalid_data,  # type: ignore[arg-type]
        approved_log,
        port=port,
    )

    assert payload["message"] == expected
    assert payload["child_started"] is False
    assert not approved_data.exists()
    assert not approved_log.exists()
    assert not (tmp_path / "data").exists()
    assert not escaped.exists()
    if invalid_kind == "c_drive":
        assert not invalid_data.exists()


@pytest.mark.skipif(shutil.which("pwsh") is None, reason="PowerShell 7 is unavailable")
def test_launcher_rejects_runtime_junction_before_creating_directories(
    tmp_path: Path,
    approved_runtime_dirs: tuple[Path, Path],
) -> None:
    script = ROOT / "scripts" / "start_options_copilot.ps1"
    approved_data, approved_log = approved_runtime_dirs
    junction = approved_data.parent / f"launcher-junction-{uuid.uuid4().hex}"
    outside_target = tmp_path / "outside-target"
    outside_target.mkdir()
    created = subprocess.run(
        [
            "pwsh",
            "-NoProfile",
            "-Command",
            (
                "New-Item -ItemType Junction -Path "
                f"{_ps_quote(junction)} -Target {_ps_quote(outside_target)} | Out-Null"
            ),
        ],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    assert created.returncode == 0, created.stderr
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    try:
        payload = _run_launcher_harness(
            script,
            junction / "escaped-data",
            approved_log,
            port=port,
        )

        assert "DataDir path contains a reparse-point ancestor" in str(payload["message"])
        assert payload["child_started"] is False
        assert not (outside_target / "escaped-data").exists()
        assert not approved_log.exists()
    finally:
        if junction.exists():
            junction.rmdir()
