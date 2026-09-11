[CmdletBinding()]
param(
    [ValidateRange(1024, 65535)]
    [int]$Port = 8891,
    [string]$DataDir,
    [string]$LogDir,
    [ValidateSet("DIRECT", "EXTERNAL")]
    [string]$BrokerAcquisitionMode = "DIRECT",
    [string]$ExternalReadonlyFeedPath,
    [string]$ExternalTop10Path,
    [string]$ExternalSessionCalendarPath,
    [switch]$DisablePacingAuthority,
    [switch]$EnableNewsLlm,
    [switch]$UseInheritedProviderProxy,
    [switch]$Background,
    [switch]$OpenBrowser
)

$ErrorActionPreference = "Stop"
$utf8NoBom = [System.Text.UTF8Encoding]::new($false)
[Console]::InputEncoding = $utf8NoBom
[Console]::OutputEncoding = $utf8NoBom
$OutputEncoding = $utf8NoBom
$projectRoot = [System.IO.Path]::GetFullPath(
    (Split-Path -Parent $PSScriptRoot)
)
$projectVenv = [System.IO.Path]::GetFullPath(
    (Join-Path $projectRoot ".venv")
)
$projectPython = [System.IO.Path]::GetFullPath(
    (Join-Path $projectRoot ".venv\Scripts\python.exe")
)
$dependencyLock = [System.IO.Path]::GetFullPath(
    (Join-Path $projectRoot "requirements-dev.lock")
)
$inventoryVerifier = [System.IO.Path]::GetFullPath(
    (Join-Path $projectRoot "scripts\verify_locked_environment.py")
)
$environmentMaintenanceLock = [System.IO.Path]::GetFullPath($PSCommandPath)
$pathComparison = [System.StringComparison]::OrdinalIgnoreCase
$directorySeparator = [System.IO.Path]::DirectorySeparatorChar
$backgroundStartupTimeoutSeconds = 90

function Add-LoopbackNoProxy {
    param(
        [AllowNull()]
        [string]$Value
    )

    $entries = [System.Collections.Generic.List[string]]::new()
    $seen = [System.Collections.Generic.HashSet[string]]::new(
        [System.StringComparer]::OrdinalIgnoreCase
    )
    foreach ($entry in @($Value -split ",")) {
        $normalized = [string]$entry
        $normalized = $normalized.Trim()
        if ($normalized -and $seen.Add($normalized)) {
            $entries.Add($normalized)
        }
    }
    foreach ($loopback in @("127.0.0.1", "localhost")) {
        if ($seen.Add($loopback)) {
            $entries.Add($loopback)
        }
    }
    return $entries -join ","
}

function Resolve-ApprovedRuntimeDirectory {
    param(
        [Parameter(Mandatory)]
        [string]$Path,
        [Parameter(Mandatory)]
        [string]$ApprovedRoot,
        [Parameter(Mandatory)]
        [string]$Label
    )

    $fullPath = if ([System.IO.Path]::IsPathFullyQualified($Path)) {
        [System.IO.Path]::GetFullPath($Path)
    }
    else {
        [System.IO.Path]::GetFullPath((Join-Path $projectRoot $Path))
    }
    $fullApprovedRoot = [System.IO.Path]::GetFullPath($ApprovedRoot)
    $approvedBoundary = $fullApprovedRoot.TrimEnd(
        [System.IO.Path]::DirectorySeparatorChar,
        [System.IO.Path]::AltDirectorySeparatorChar
    ) + $directorySeparator
    if (-not ([System.IO.Path]::GetPathRoot($fullPath)).Equals(
        "G:\",
        $pathComparison
    )) {
        throw "$Label must remain on G:."
    }
    if (
        -not $fullPath.Equals($fullApprovedRoot, $pathComparison) `
            -and -not $fullPath.StartsWith($approvedBoundary, $pathComparison)
    ) {
        throw "$Label must remain under its approved project runtime root."
    }

    $cursor = $fullPath
    while ($true) {
        if (Test-Path -LiteralPath $cursor) {
            $item = Get-Item -LiteralPath $cursor -Force
            if (($item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
                throw "$Label path contains a reparse-point ancestor: $cursor"
            }
        }
        if ($cursor.Equals($projectRoot, $pathComparison)) {
            break
        }
        $parent = [System.IO.Directory]::GetParent($cursor)
        if ($null -eq $parent) {
            throw "$Label path cannot be traced to the project root."
        }
        $cursor = $parent.FullName
    }
    if (
        (Test-Path -LiteralPath $fullPath) `
            -and -not (Test-Path -LiteralPath $fullPath -PathType Container)
    ) {
        throw "$Label must identify a directory."
    }
    return $fullPath
}

function Resolve-TrustedEnvironmentPath {
    param(
        [Parameter(Mandatory)]
        [string]$Path,
        [Parameter(Mandatory)]
        [string]$ApprovedRoot,
        [Parameter(Mandatory)]
        [string]$Label,
        [Parameter(Mandatory)]
        [ValidateSet("Leaf", "Container")]
        [string]$PathType
    )

    $fullPath = [System.IO.Path]::GetFullPath($Path)
    $fullApprovedRoot = [System.IO.Path]::GetFullPath($ApprovedRoot)
    $approvedBoundary = $fullApprovedRoot.TrimEnd(
        [System.IO.Path]::DirectorySeparatorChar,
        [System.IO.Path]::AltDirectorySeparatorChar
    ) + $directorySeparator
    if (
        -not $fullPath.Equals($fullApprovedRoot, $pathComparison) `
            -and -not $fullPath.StartsWith($approvedBoundary, $pathComparison)
    ) {
        throw "$Label resolves outside its approved project boundary."
    }
    if (-not (Test-Path -LiteralPath $fullPath -PathType $PathType)) {
        throw "$Label is unavailable."
    }

    $cursor = $fullPath
    while ($true) {
        $item = Get-Item -LiteralPath $cursor -Force
        if (($item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
            throw "$Label path contains a reparse-point ancestor: $cursor"
        }
        if ($cursor.Equals($projectRoot, $pathComparison)) {
            break
        }
        $parent = [System.IO.Directory]::GetParent($cursor)
        if ($null -eq $parent) {
            throw "$Label cannot be traced to the project root."
        }
        $cursor = $parent.FullName
    }

    $resolved = Resolve-Path -LiteralPath $fullPath -ErrorAction Stop
    $canonicalPath = [System.IO.Path]::GetFullPath($resolved.Path)
    if (-not $canonicalPath.Equals($fullPath, $pathComparison)) {
        throw "$Label canonical path changed outside the approved identity."
    }
    return $canonicalPath
}

function Assert-EnvironmentTrustPaths {
    $approvedVenv = Resolve-TrustedEnvironmentPath -Path $projectVenv `
        -ApprovedRoot $projectVenv -Label "Project .venv" -PathType Container
    $approvedPython = Resolve-TrustedEnvironmentPath -Path $projectPython `
        -ApprovedRoot $projectVenv -Label "Project Python interpreter" -PathType Leaf
    $approvedLock = Resolve-TrustedEnvironmentPath -Path $dependencyLock `
        -ApprovedRoot $projectRoot -Label "Development dependency lock" -PathType Leaf
    $approvedVerifier = Resolve-TrustedEnvironmentPath -Path $inventoryVerifier `
        -ApprovedRoot $projectRoot -Label "Exact inventory verifier" -PathType Leaf
    $approvedMaintenanceLock = Resolve-TrustedEnvironmentPath `
        -Path $environmentMaintenanceLock -ApprovedRoot $projectRoot `
        -Label "Environment maintenance lock anchor" -PathType Leaf
    if (
        -not $approvedVenv.Equals($projectVenv, $pathComparison) `
            -or -not $approvedPython.Equals($projectPython, $pathComparison) `
            -or -not $approvedLock.Equals($dependencyLock, $pathComparison) `
            -or -not $approvedVerifier.Equals($inventoryVerifier, $pathComparison) `
            -or -not $approvedMaintenanceLock.Equals(
                $environmentMaintenanceLock,
                $pathComparison
            )
    ) {
        throw "Environment trust-path identity changed during launcher preflight."
    }
}

function Test-ExactProcessIdentity {
    param(
        [Parameter(Mandatory)]
        [psobject]$Expected,
        [Parameter(Mandatory)]
        [psobject]$Actual
    )

    try {
        $expectedCreated = ([DateTime]$Expected.CreationDate).ToUniversalTime()
        $actualCreated = ([DateTime]$Actual.CreationDate).ToUniversalTime()
        $expectedExecutable = [System.IO.Path]::GetFullPath(
            [string]$Expected.ExecutablePath
        )
        $actualExecutable = [System.IO.Path]::GetFullPath(
            [string]$Actual.ExecutablePath
        )
        $expectedCommand = [string]$Expected.CommandLine
        $actualCommand = [string]$Actual.CommandLine
    }
    catch {
        return $false
    }
    return (
        [int]$Expected.ProcessId -eq [int]$Actual.ProcessId `
            -and [int]$Expected.ParentProcessId -eq [int]$Actual.ParentProcessId `
            -and $expectedCreated.Ticks -eq $actualCreated.Ticks `
            -and $expectedExecutable.Equals($actualExecutable, $pathComparison) `
            -and $expectedCommand.Equals($actualCommand, $pathComparison)
    )
}

function Get-OwnedProcessTree {
    param(
        [Parameter(Mandatory)]
        [psobject]$RootIdentity
    )

    $records = @(Get-CimInstance Win32_Process -ErrorAction SilentlyContinue)
    $rootProcessId = [int]$RootIdentity.ProcessId
    $rootRecord = $records | Where-Object {
        [int]$_.ProcessId -eq $rootProcessId
    } | Select-Object -First 1
    if ($null -eq $rootRecord) {
        return @()
    }
    $currentRoot = [pscustomobject]@{
        ProcessId = [int]$rootRecord.ProcessId
        ParentProcessId = [int]$rootRecord.ParentProcessId
        CreationDate = [DateTime]$rootRecord.CreationDate
        ExecutablePath = [string]$rootRecord.ExecutablePath
        CommandLine = [string]$rootRecord.CommandLine
        Depth = 0
    }
    if (
        -not (Test-ExactProcessIdentity `
            -Expected $RootIdentity -Actual $currentRoot)
    ) {
        return @()
    }

    $ownedIds = [System.Collections.Generic.HashSet[int]]::new()
    $pending = [System.Collections.Generic.Queue[object]]::new()
    $owned = [System.Collections.Generic.List[object]]::new()
    $null = $ownedIds.Add($rootProcessId)
    $owned.Add($currentRoot)
    $pending.Enqueue(
        [pscustomobject]@{
            ProcessId = $rootProcessId
            CreationDate = [DateTime]$currentRoot.CreationDate
            Depth = 0
        }
    )
    while ($pending.Count -gt 0) {
        $parent = $pending.Dequeue()
        foreach ($record in $records) {
            $processId = [int]$record.ProcessId
            $isChild = [int]$record.ParentProcessId -eq [int]$parent.ProcessId
            if (-not $isChild) {
                continue
            }
            $createdAtUtc = ([DateTime]$record.CreationDate).ToUniversalTime()
            $parentCreatedAtUtc = (
                [DateTime]$parent.CreationDate
            ).ToUniversalTime()
            if ($createdAtUtc -lt $parentCreatedAtUtc) {
                continue
            }
            if (-not $ownedIds.Add($processId)) {
                continue
            }
            $depth = [int]$parent.Depth + 1
            $owned.Add(
                [pscustomobject]@{
                    ProcessId = $processId
                    ParentProcessId = [int]$record.ParentProcessId
                    CreationDate = [DateTime]$record.CreationDate
                    ExecutablePath = [string]$record.ExecutablePath
                    CommandLine = [string]$record.CommandLine
                    Depth = $depth
                }
            )
            $pending.Enqueue(
                [pscustomobject]@{
                    ProcessId = $processId
                    CreationDate = [DateTime]$record.CreationDate
                    Depth = $depth
                }
            )
        }
    }
    return $owned.ToArray()
}

function Test-OptionsCopilotRuntimeProcessIdentity {
    param(
        [Parameter(Mandatory)]
        [psobject]$ProcessRecord,
        [Parameter(Mandatory)]
        [object[]]$OwnedProcesses,
        [Parameter(Mandatory)]
        [string]$ExpectedProjectExecutable,
        [Parameter(Mandatory)]
        [string]$ExpectedBaseExecutable,
        [Parameter(Mandatory)]
        [object[]]$ExpectedArguments
    )

    try {
        $executable = [System.IO.Path]::GetFullPath(
            [string]$ProcessRecord.ExecutablePath
        )
    }
    catch {
        return $false
    }
    $usesProjectExecutable = $executable.Equals(
        $ExpectedProjectExecutable,
        $pathComparison
    )
    $usesBaseExecutable = $executable.Equals(
        $ExpectedBaseExecutable,
        $pathComparison
    )
    if (-not $usesProjectExecutable -and -not $usesBaseExecutable) {
        return $false
    }
    if ($usesBaseExecutable) {
        $ancestorId = [int]$ProcessRecord.ParentProcessId
        $hasProjectRedirectorAncestor = $false
        while ($ancestorId -gt 0) {
            $ancestor = $OwnedProcesses | Where-Object {
                [int]$_.ProcessId -eq $ancestorId
            } | Select-Object -First 1
            if ($null -eq $ancestor) {
                break
            }
            try {
                $ancestorExecutable = [System.IO.Path]::GetFullPath(
                    [string]$ancestor.ExecutablePath
                )
            }
            catch {
                return $false
            }
            if (
                $ancestorExecutable.Equals(
                    $ExpectedProjectExecutable,
                    $pathComparison
                )
            ) {
                $hasProjectRedirectorAncestor = $true
                break
            }
            $ancestorId = [int]$ancestor.ParentProcessId
        }
        if (-not $hasProjectRedirectorAncestor) {
            return $false
        }
    }
    $argumentText = $ExpectedArguments -join " "
    $plainCommand = "$executable $argumentText"
    $quotedCommand = '"' + $executable + '" ' + $argumentText
    $commandLine = [string]$ProcessRecord.CommandLine
    return (
        $commandLine.Equals($plainCommand, $pathComparison) `
            -or $commandLine.Equals($quotedCommand, $pathComparison)
    )
}

function Stop-OwnedProcessTree {
    param(
        [Parameter(Mandatory)]
        [psobject]$RootIdentity
    )

    for ($attempt = 0; $attempt -lt 20; $attempt++) {
        $owned = @(Get-OwnedProcessTree -RootIdentity $RootIdentity)
        if ($owned.Count -eq 0) {
            return
        }
        foreach ($record in @($owned | Sort-Object Depth -Descending)) {
            $freshOwned = @(Get-OwnedProcessTree -RootIdentity $RootIdentity)
            $freshRecord = $freshOwned | Where-Object {
                [int]$_.ProcessId -eq [int]$record.ProcessId
            } | Select-Object -First 1
            if (
                $null -eq $freshRecord `
                    -or -not (Test-ExactProcessIdentity `
                        -Expected $record -Actual $freshRecord)
            ) {
                continue
            }
            Stop-Process -Id ([int]$freshRecord.ProcessId) -Force `
                -ErrorAction SilentlyContinue
        }
        Start-Sleep -Milliseconds 100
    }
    $remaining = @(Get-OwnedProcessTree -RootIdentity $RootIdentity)
    if ($remaining.Count -gt 0) {
        throw "Owned Options Copilot process tree could not be fully terminated."
    }
}

function Enter-EnvironmentMaintenanceTransaction {
    param(
        [Parameter(Mandatory)]
        [string]$LockPath,
        [int]$WaitMilliseconds = 30000
    )

    $fullLockPath = Resolve-TrustedEnvironmentPath -Path $LockPath `
        -ApprovedRoot $projectRoot -Label "Environment maintenance lock anchor" `
        -PathType Leaf
    $deadline = [DateTime]::UtcNow.AddMilliseconds($WaitMilliseconds)
    while ($true) {
        try {
            $stream = [System.IO.File]::Open(
                $fullLockPath,
                [System.IO.FileMode]::Open,
                [System.IO.FileAccess]::Read,
                [System.IO.FileShare]::None
            )
            return [pscustomobject]@{
                Stream = $stream
                LockPath = $fullLockPath
            }
        }
        catch [System.IO.IOException] {
            if ([DateTime]::UtcNow -ge $deadline) {
                throw "Timed out waiting for the project environment maintenance transaction."
            }
            Start-Sleep -Milliseconds 25
        }
    }
}

function Exit-EnvironmentMaintenanceTransaction {
    param(
        [Parameter(Mandatory)]
        [psobject]$Guard
    )

    $Guard.Stream.Dispose()
}

$environmentMaintenanceGuard = Enter-EnvironmentMaintenanceTransaction `
    -LockPath $environmentMaintenanceLock
try {
if (-not (Test-Path -LiteralPath $projectPython -PathType Leaf)) {
    throw "Project Python interpreter is unavailable."
}
if (-not (Test-Path -LiteralPath $dependencyLock -PathType Leaf)) {
    throw "Development dependency lock is unavailable."
}
if (-not (Test-Path -LiteralPath $inventoryVerifier -PathType Leaf)) {
    throw "Exact inventory verifier is unavailable."
}
$null = Assert-EnvironmentTrustPaths
$pythonVersionOutput = @(& $projectPython --version 2>&1)
$pythonVersionExitCode = $LASTEXITCODE
$pythonVersionText = ($pythonVersionOutput -join "").Trim()
if (
    $pythonVersionExitCode -ne 0 `
        -or $pythonVersionText -notmatch '^Python (?<major>\d+)\.(?<minor>\d+)\.(?<patch>\d+)$'
) {
    throw "Project Python version could not be verified."
}
$pythonMajor = [int]$Matches.major
$pythonMinor = [int]$Matches.minor
$pythonPatch = [int]$Matches.patch
if ($pythonMajor -lt 3 -or ($pythonMajor -eq 3 -and $pythonMinor -lt 12)) {
    throw "Project Python must be CPython 3.12 or newer."
}
$pythonVersion = "$pythonMajor.$pythonMinor.$pythonPatch"
$null = Assert-EnvironmentTrustPaths
$implementationOutput = @(
    & $projectPython -c "import platform; print(platform.python_implementation())" 2>&1
)
$implementationExitCode = $LASTEXITCODE
if (
    $implementationExitCode -ne 0 `
        -or ($implementationOutput -join "").Trim() -cne "CPython"
) {
    throw "Project Python must be CPython 3.12 or newer."
}
$null = Assert-EnvironmentTrustPaths
$reportedExecutableOutput = @(
    & $projectPython -c "import os, sys; print(os.path.realpath(sys.executable))" 2>&1
)
$reportedExecutableExitCode = $LASTEXITCODE
if ($reportedExecutableExitCode -ne 0 -or $reportedExecutableOutput.Count -ne 1) {
    throw "Project Python executable identity could not be verified."
}
$reportedExecutable = [System.IO.Path]::GetFullPath(
    ($reportedExecutableOutput -join "").Trim()
)
$null = Assert-EnvironmentTrustPaths
if (-not $reportedExecutable.Equals($projectPython, $pathComparison)) {
    throw "Project Python reported an unapproved executable identity."
}
$null = Assert-EnvironmentTrustPaths
$reportedBaseExecutableOutput = @(
    & $projectPython -c `
        "import os, sys; print(os.path.realpath(sys._base_executable))" 2>&1
)
$reportedBaseExecutableExitCode = $LASTEXITCODE
if (
    $reportedBaseExecutableExitCode -ne 0 `
        -or $reportedBaseExecutableOutput.Count -ne 1
) {
    throw "Project Python base executable identity could not be verified."
}
$projectBasePython = [System.IO.Path]::GetFullPath(
    ($reportedBaseExecutableOutput -join "").Trim()
)
if (-not (Test-Path -LiteralPath $projectBasePython -PathType Leaf)) {
    throw "Project Python base executable identity could not be verified."
}

$inventoryArguments = @(
    $inventoryVerifier,
    "--lock", $dependencyLock,
    "--allow-bootstrap", "pip",
    "--allow-bootstrap", "setuptools"
)
$null = Assert-EnvironmentTrustPaths
$inventoryOutput = @(& $projectPython @inventoryArguments 2>&1)
$inventoryExitCode = $LASTEXITCODE
if ($inventoryExitCode -ne 0) {
    throw "Installed dependency inventory does not match the development lock."
}
$pipCheckArguments = @("-m", "pip", "check")
$null = Assert-EnvironmentTrustPaths
$pipCheckOutput = @(& $projectPython @pipCheckArguments 2>&1)
$pipCheckExitCode = $LASTEXITCODE
if ($pipCheckExitCode -ne 0) {
    throw "Project dependency check failed."
}
$null = Assert-EnvironmentTrustPaths
$lockSha256 = (Get-FileHash -LiteralPath $dependencyLock -Algorithm SHA256).Hash.ToLowerInvariant()
$null = Assert-EnvironmentTrustPaths
$environmentIdentity = (
    "OptionsCopilot environment_identity " +
    "interpreter_path=$projectPython; python_version=$pythonVersion; " +
    "lock_filename=requirements-dev.lock; lock_type=development; " +
    "lock_sha256=$lockSha256; dependency_check=EXACT_LOCK_MATCH"
)
Write-Output $environmentIdentity

$listenAddress = "127.0.0.1"
$existing = Get-NetTCPConnection -State Listen -LocalAddress $listenAddress `
    -LocalPort $Port -ErrorAction SilentlyContinue
if ($existing) {
    throw "Port $Port is already listening; refusing to replace an existing service."
}

$approvedDataRoot = Join-Path $projectRoot "data\options_copilot"
$approvedLogRoot = Join-Path $projectRoot "logs\options_copilot"
$dataCandidate = if ([string]::IsNullOrWhiteSpace($DataDir)) {
    $approvedDataRoot
}
else {
    $DataDir
}
$logCandidate = if ([string]::IsNullOrWhiteSpace($LogDir)) {
    $approvedLogRoot
}
else {
    $LogDir
}
$dataDir = Resolve-ApprovedRuntimeDirectory -Path $dataCandidate `
    -ApprovedRoot $approvedDataRoot -Label "DataDir"
$logDir = Resolve-ApprovedRuntimeDirectory -Path $logCandidate `
    -ApprovedRoot $approvedLogRoot -Label "LogDir"
if ($dataDir -eq $logDir) {
    throw "DataDir and LogDir must be different directories."
}
if ($BrokerAcquisitionMode -eq "EXTERNAL") {
    $externalInputs = @(
        $ExternalReadonlyFeedPath,
        $ExternalTop10Path,
        $ExternalSessionCalendarPath
    )
    if ($externalInputs.Where({ [string]::IsNullOrWhiteSpace($_) }).Count -ne 0) {
        throw "EXTERNAL acquisition requires all three external input paths."
    }
    $externalReadonlyFeedPath = [System.IO.Path]::GetFullPath(
        $ExternalReadonlyFeedPath
    )
    $externalTop10Path = [System.IO.Path]::GetFullPath($ExternalTop10Path)
    $externalSessionCalendarPath = [System.IO.Path]::GetFullPath(
        $ExternalSessionCalendarPath
    )
    $resolvedExternalPaths = @(
        $externalReadonlyFeedPath,
        $externalTop10Path,
        $externalSessionCalendarPath
    )
    $uniqueExternalPaths = [System.Collections.Generic.HashSet[string]]::new(
        [System.StringComparer]::OrdinalIgnoreCase
    )
    foreach ($path in $resolvedExternalPaths) {
        $null = $uniqueExternalPaths.Add($path)
    }
    if ($uniqueExternalPaths.Count -ne 3) {
        throw "External input paths must identify three different files."
    }
}
if ($DisablePacingAuthority) {
    $disabledPacingPath = Join-Path $dataDir ".pacing-authority-disabled"
    if (Test-Path -LiteralPath $disabledPacingPath) {
        throw "Fail-closed pacing authority path must not exist: $disabledPacingPath"
    }
}
New-Item -ItemType Directory -Force -Path $dataDir, $logDir | Out-Null
$dataDir = Resolve-ApprovedRuntimeDirectory -Path $dataDir `
    -ApprovedRoot $approvedDataRoot -Label "DataDir"
$logDir = Resolve-ApprovedRuntimeDirectory -Path $logDir `
    -ApprovedRoot $approvedLogRoot -Label "LogDir"

$arguments = @(
    "-m", "uvicorn",
    "options_copilot.runtime:create_runtime_app",
    "--factory",
    "--host", $listenAddress,
    "--port", [string]$Port,
    "--log-level", "info"
)

$scopedEnvironmentNames = @(
    "OPTIONS_COPILOT_HOST",
    "OPTIONS_COPILOT_PORT",
    "OPTIONS_COPILOT_DATA_DIR",
    "OPTIONS_COPILOT_LOG_DIR",
    "OPTIONS_COPILOT_BROKER_ACQUISITION_MODE",
    "OPTIONS_COPILOT_EXTERNAL_READONLY_FEED_PATH",
    "OPTIONS_COPILOT_EXTERNAL_TOP10_PATH",
    "OPTIONS_COPILOT_EXTERNAL_SESSION_CALENDAR_PATH",
    "OPTIONS_COPILOT_PACING_AUTHORITY_DIR",
    "OPTIONS_COPILOT_NEWS_LLM_ENABLED",
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
    "NO_PROXY",
    "no_proxy"
)
$previousEnvironment = @{}
foreach ($name in $scopedEnvironmentNames) {
    $previousEnvironment[$name] = [System.Environment]::GetEnvironmentVariable(
        $name,
        [System.EnvironmentVariableTarget]::Process
    )
}

try {
    $env:OPTIONS_COPILOT_HOST = $listenAddress
    $env:OPTIONS_COPILOT_PORT = [string]$Port
    $env:OPTIONS_COPILOT_DATA_DIR = $dataDir
    $env:OPTIONS_COPILOT_LOG_DIR = $logDir
    $env:OPTIONS_COPILOT_BROKER_ACQUISITION_MODE = $BrokerAcquisitionMode
    $env:OPTIONS_COPILOT_NEWS_LLM_ENABLED = if ($EnableNewsLlm) { "true" } else { "false" }
    if (-not $UseInheritedProviderProxy) {
        foreach ($proxyName in @(
            "HTTP_PROXY",
            "HTTPS_PROXY",
            "ALL_PROXY",
            "http_proxy",
            "https_proxy",
            "all_proxy"
        )) {
            Remove-Item "Env:$proxyName" -ErrorAction SilentlyContinue
        }
        $env:NO_PROXY = "127.0.0.1,localhost"
        $env:no_proxy = "127.0.0.1,localhost"
    }
    else {
        $env:NO_PROXY = Add-LoopbackNoProxy -Value $env:NO_PROXY
        $env:no_proxy = Add-LoopbackNoProxy -Value $env:no_proxy
    }
    if ($BrokerAcquisitionMode -eq "EXTERNAL") {
        $env:OPTIONS_COPILOT_EXTERNAL_READONLY_FEED_PATH = $externalReadonlyFeedPath
        $env:OPTIONS_COPILOT_EXTERNAL_TOP10_PATH = $externalTop10Path
        $env:OPTIONS_COPILOT_EXTERNAL_SESSION_CALENDAR_PATH = $externalSessionCalendarPath
    }
    else {
        Remove-Item Env:OPTIONS_COPILOT_EXTERNAL_READONLY_FEED_PATH `
            -ErrorAction SilentlyContinue
        Remove-Item Env:OPTIONS_COPILOT_EXTERNAL_TOP10_PATH `
            -ErrorAction SilentlyContinue
        Remove-Item Env:OPTIONS_COPILOT_EXTERNAL_SESSION_CALENDAR_PATH `
            -ErrorAction SilentlyContinue
    }
    if ($DisablePacingAuthority) {
        $env:OPTIONS_COPILOT_PACING_AUTHORITY_DIR = $disabledPacingPath
    }
    else {
        Remove-Item Env:OPTIONS_COPILOT_PACING_AUTHORITY_DIR `
            -ErrorAction SilentlyContinue
    }

    if ($Background) {
        $stdout = Join-Path $logDir "server.stdout.log"
        $stderr = Join-Path $logDir "server.stderr.log"
        $null = Assert-EnvironmentTrustPaths
        $launchStartedAtUtc = [DateTime]::UtcNow.AddSeconds(-1)
        $process = Start-Process -FilePath $projectPython -ArgumentList $arguments `
            -WorkingDirectory $projectRoot -WindowStyle Hidden -PassThru `
            -RedirectStandardOutput $stdout -RedirectStandardError $stderr
        $rootIdentity = $null
        $rootIdentityDeadline = [DateTime]::UtcNow.AddSeconds(2)
        do {
            $rootRecord = Get-CimInstance Win32_Process `
                -Filter "ProcessId=$($process.Id)" -ErrorAction SilentlyContinue
            if ($null -ne $rootRecord) {
                $candidateRootIdentity = [pscustomobject]@{
                    ProcessId = [int]$rootRecord.ProcessId
                    ParentProcessId = [int]$rootRecord.ParentProcessId
                    CreationDate = [DateTime]$rootRecord.CreationDate
                    ExecutablePath = [string]$rootRecord.ExecutablePath
                    CommandLine = [string]$rootRecord.CommandLine
                    Depth = 0
                }
                $rootCreatedAtUtc = (
                    [DateTime]$candidateRootIdentity.CreationDate
                ).ToUniversalTime()
                if (
                    $rootCreatedAtUtc -lt $launchStartedAtUtc `
                        -or -not (Test-OptionsCopilotRuntimeProcessIdentity `
                            -ProcessRecord $candidateRootIdentity `
                            -OwnedProcesses @($candidateRootIdentity) `
                            -ExpectedProjectExecutable $projectPython `
                            -ExpectedBaseExecutable $projectBasePython `
                            -ExpectedArguments $arguments)
                ) {
                    break
                }
                $rootIdentity = $candidateRootIdentity
                break
            }
            if ($process.HasExited) {
                break
            }
            Start-Sleep -Milliseconds 50
        } while ([DateTime]::UtcNow -lt $rootIdentityDeadline)
        if ($null -eq $rootIdentity) {
            throw "Options Copilot did not begin listening. Inspect $stderr"
        }
        $deadline = [DateTime]::UtcNow.AddSeconds($backgroundStartupTimeoutSeconds)
        $listener = $null
        do {
            Start-Sleep -Milliseconds 250
            $ownedProcesses = @(
                Get-OwnedProcessTree -RootIdentity $rootIdentity
            )
            $observedListeners = @(
                Get-NetTCPConnection -State Listen -LocalAddress $listenAddress `
                    -LocalPort $Port -ErrorAction SilentlyContinue
            )
            if ($observedListeners.Count -eq 1) {
                $candidateListener = $observedListeners[0]
                $listenerProcess = $ownedProcesses | Where-Object {
                    [int]$_.ProcessId -eq [int]$candidateListener.OwningProcess
                } | Select-Object -First 1
                if (
                    $null -ne $listenerProcess `
                        -and (Test-OptionsCopilotRuntimeProcessIdentity `
                            -ProcessRecord $listenerProcess `
                            -OwnedProcesses $ownedProcesses `
                            -ExpectedProjectExecutable $projectPython `
                            -ExpectedBaseExecutable $projectBasePython `
                            -ExpectedArguments $arguments)
                ) {
                    $listener = $candidateListener
                }
            }
            $ownedProcessCount = $ownedProcesses.Count
        } while (
            -not $listener `
                -and [DateTime]::UtcNow -lt $deadline `
                -and (-not $process.HasExited -or $ownedProcessCount -gt 0)
        )
        if (-not $listener) {
            Stop-OwnedProcessTree -RootIdentity $rootIdentity
            throw "Options Copilot did not begin listening. Inspect $stderr"
        }
        Write-Output "Options Copilot PID=$($listener.OwningProcess) URL=http://127.0.0.1:$Port/"
        if ($OpenBrowser) {
            Start-Process "http://127.0.0.1:$Port/"
        }
        return
    }

    if ($OpenBrowser) {
        Start-Process "http://127.0.0.1:$Port/"
    }
    Push-Location $projectRoot
    try {
        $null = Assert-EnvironmentTrustPaths
        & $projectPython @arguments
    }
    finally {
        Pop-Location
    }
}
finally {
    foreach ($name in $scopedEnvironmentNames) {
        [System.Environment]::SetEnvironmentVariable(
            $name,
            $previousEnvironment[$name],
            [System.EnvironmentVariableTarget]::Process
        )
    }
}
}
finally {
    Exit-EnvironmentMaintenanceTransaction -Guard $environmentMaintenanceGuard
}
