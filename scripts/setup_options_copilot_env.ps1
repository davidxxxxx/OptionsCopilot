[CmdletBinding()]
param(
    [ValidateSet("RegenerateAndCreate", "Verify")]
    [string]$Mode = "RegenerateAndCreate"
)

$ErrorActionPreference = "Stop"
$utf8NoBom = [System.Text.UTF8Encoding]::new($false)
[Console]::InputEncoding = $utf8NoBom
[Console]::OutputEncoding = $utf8NoBom
$OutputEncoding = $utf8NoBom

$projectRoot = [System.IO.Path]::GetFullPath((Split-Path -Parent $PSScriptRoot))
$requiredProjectRoot = [System.IO.Path]::GetFullPath("G:\OptionsCopilot")
if (-not $projectRoot.Equals(
    $requiredProjectRoot,
    [System.StringComparison]::OrdinalIgnoreCase
)) {
    throw "Hermetic setup is supported only from G:\OptionsCopilot."
}

function Assert-ProjectContainedPath {
    param(
        [Parameter(Mandatory)]
        [string]$Path
    )

    $fullPath = [System.IO.Path]::GetFullPath($Path)
    $rootWithSeparator = $projectRoot.TrimEnd("\", "/") + [System.IO.Path]::DirectorySeparatorChar
    if (-not $fullPath.StartsWith(
        $rootWithSeparator,
        [System.StringComparison]::OrdinalIgnoreCase
    )) {
        throw "Artifact path escapes the project root: $fullPath"
    }
    if (-not ([System.IO.Path]::GetPathRoot($fullPath)).Equals(
        "G:\",
        [System.StringComparison]::OrdinalIgnoreCase
    )) {
        throw "Artifact path must remain on G:."
    }
    $cursor = $fullPath
    while ($true) {
        if (Test-Path -LiteralPath $cursor) {
            $item = Get-Item -LiteralPath $cursor -Force
            if (
                ($item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) `
                    -ne 0
            ) {
                throw "Artifact path contains a reparse-point ancestor: $cursor"
            }
        }
        if ($cursor.Equals(
            $projectRoot,
            [System.StringComparison]::OrdinalIgnoreCase
        )) {
            break
        }
        $parent = [System.IO.Directory]::GetParent($cursor)
        if ($null -eq $parent) {
            throw "Artifact path cannot be traced to the project root: $fullPath"
        }
        $cursor = $parent.FullName
    }
    return $fullPath
}

function Enter-EnvironmentMaintenanceTransaction {
    param(
        [Parameter(Mandatory)]
        [string]$LockPath,
        [int]$WaitMilliseconds = 30000
    )

    $fullLockPath = Assert-ProjectContainedPath -Path $LockPath
    if (-not (Test-Path -LiteralPath $fullLockPath -PathType Leaf)) {
        throw "Environment maintenance lock anchor is unavailable."
    }
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

function New-OwnedDirectory {
    param(
        [Parameter(Mandatory)]
        [string]$Path,
        [switch]$AllowExisting
    )

    $fullPath = Assert-ProjectContainedPath -Path $Path
    if (Test-Path -LiteralPath $fullPath) {
        if (-not $AllowExisting) {
            throw "Invocation-owned directory already exists: $fullPath"
        }
    }
    else {
        New-Item -ItemType Directory -Path $fullPath | Out-Null
    }
    $fullPath = Assert-ProjectContainedPath -Path $fullPath
    if (-not (Test-Path -LiteralPath $fullPath -PathType Container)) {
        throw "Invocation-owned path is not a directory: $fullPath"
    }
    return $fullPath
}

function Invoke-Checked {
    param(
        [Parameter(Mandatory)]
        [string]$FilePath,
        [Parameter(Mandatory)]
        [string[]]$Arguments
    )

    & $FilePath @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "Command failed with exit code ${LASTEXITCODE}: $FilePath"
    }
}

function Get-LockFileSha256 {
    param(
        [Parameter(Mandatory)]
        [string]$Path
    )

    $fullPath = Assert-ProjectContainedPath -Path $Path
    if (-not (Test-Path -LiteralPath $fullPath -PathType Leaf)) {
        throw "Lock transaction file is missing: $fullPath"
    }
    return (Get-FileHash -LiteralPath $fullPath -Algorithm SHA256).Hash.ToLowerInvariant()
}

function Copy-DurableLockFile {
    param(
        [Parameter(Mandatory)]
        [string]$Source,
        [Parameter(Mandatory)]
        [string]$Destination
    )

    $sourcePath = Assert-ProjectContainedPath -Path $Source
    $destinationPath = Assert-ProjectContainedPath -Path $Destination
    [System.IO.File]::Copy($sourcePath, $destinationPath, $false)
    $stream = [System.IO.File]::Open(
        $destinationPath,
        [System.IO.FileMode]::Open,
        [System.IO.FileAccess]::ReadWrite,
        [System.IO.FileShare]::Read
    )
    try {
        $stream.Flush($true)
    }
    finally {
        $stream.Dispose()
    }
}

function Invoke-AtomicLockReplace {
    param(
        [Parameter(Mandatory)]
        [string]$Source,
        [Parameter(Mandatory)]
        [string]$Destination,
        [Parameter(Mandatory)]
        [string]$DiscardedDestination
    )

    $sourcePath = Assert-ProjectContainedPath -Path $Source
    $destinationPath = Assert-ProjectContainedPath -Path $Destination
    $discardPath = Assert-ProjectContainedPath -Path $DiscardedDestination
    if (Test-Path -LiteralPath $discardPath) {
        Remove-Item -LiteralPath $discardPath -Force
    }
    [System.IO.File]::Replace($sourcePath, $destinationPath, $discardPath)
    if (Test-Path -LiteralPath $discardPath) {
        Remove-Item -LiteralPath $discardPath -Force
    }
}

function Write-DurableLockTransaction {
    param(
        [Parameter(Mandatory)]
        [string]$Path,
        [Parameter(Mandatory)]
        [System.Collections.IDictionary]$Payload
    )

    $fullPath = Assert-ProjectContainedPath -Path $Path
    $bytes = [System.Text.UTF8Encoding]::new($false).GetBytes(
        ($Payload | ConvertTo-Json -Compress)
    )
    $stream = [System.IO.File]::Open(
        $fullPath,
        [System.IO.FileMode]::CreateNew,
        [System.IO.FileAccess]::Write,
        [System.IO.FileShare]::None
    )
    try {
        $stream.Write($bytes, 0, $bytes.Length)
        $stream.Flush($true)
    }
    finally {
        $stream.Dispose()
    }
}

function Enter-LockPairPublicationGuard {
    param(
        [Parameter(Mandatory)]
        [string]$LockPath,
        [int]$WaitMilliseconds = 30000
    )

    $fullLockPath = Assert-ProjectContainedPath -Path $LockPath
    $mutex = [System.Threading.Mutex]::new(
        $false,
        "Local\OptionsCopilot-LockPairPublication-v1"
    )
    $ownsMutex = $false
    try {
        try {
            $ownsMutex = $mutex.WaitOne($WaitMilliseconds)
        }
        catch [System.Threading.AbandonedMutexException] {
            $ownsMutex = $true
        }
        if (-not $ownsMutex) {
            throw "Timed out waiting for lock-pair publication serialization."
        }
        if (Test-Path -LiteralPath $fullLockPath) {
            Remove-Item -LiteralPath $fullLockPath -Force
        }
        $stream = [System.IO.File]::Open(
            $fullLockPath,
            [System.IO.FileMode]::CreateNew,
            [System.IO.FileAccess]::Write,
            [System.IO.FileShare]::None
        )
        $ownerBytes = [System.Text.UTF8Encoding]::new($false).GetBytes(
            ([ordered]@{
                schema_version = 1
                process_id = $PID
                acquired_at_utc = [DateTime]::UtcNow.ToString("o")
            } | ConvertTo-Json -Compress)
        )
        $stream.Write($ownerBytes, 0, $ownerBytes.Length)
        $stream.Flush($true)
        return [pscustomobject]@{
            Mutex = $mutex
            OwnsMutex = $ownsMutex
            Stream = $stream
            LockPath = $fullLockPath
        }
    }
    catch {
        if ($ownsMutex) {
            $mutex.ReleaseMutex()
        }
        $mutex.Dispose()
        throw
    }
}

function Exit-LockPairPublicationGuard {
    param(
        [Parameter(Mandatory)]
        [psobject]$Guard
    )

    try {
        $Guard.Stream.Dispose()
        if (Test-Path -LiteralPath $Guard.LockPath) {
            Remove-Item -LiteralPath $Guard.LockPath -Force
        }
    }
    finally {
        if ($Guard.OwnsMutex) {
            $Guard.Mutex.ReleaseMutex()
        }
        $Guard.Mutex.Dispose()
    }
}

function Resolve-LockPairTransactionPaths {
    param(
        [Parameter(Mandatory)]
        [string]$TransactionPath
    )

    $journalPath = Assert-ProjectContainedPath -Path $TransactionPath
    if ([System.IO.Path]::GetFileName($journalPath) -cne "transaction.json") {
        throw "Lock-pair transaction journal must use the fixed transaction.json basename."
    }
    $transactionDirectory = Assert-ProjectContainedPath -Path (
        Split-Path -Parent $journalPath
    )
    if ([System.IO.Path]::GetFileName($transactionDirectory) -cne "active-transaction") {
        throw "Lock-pair transaction directory must use the fixed active-transaction basename."
    }
    return [pscustomobject]@{
        TransactionDirectory = $transactionDirectory
        Journal = $journalPath
        ProductionBackup = Assert-ProjectContainedPath -Path (
            Join-Path $transactionDirectory "requirements.lock.backup"
        )
        DevelopmentBackup = Assert-ProjectContainedPath -Path (
            Join-Path $transactionDirectory "requirements-dev.lock.backup"
        )
    }
}

function Assert-LockPairTransactionBackups {
    param(
        [Parameter(Mandatory)]
        [psobject]$Transaction,
        [Parameter(Mandatory)]
        [psobject]$TransactionPaths
    )

    $records = @(
        [pscustomobject]@{
            Backup = $TransactionPaths.ProductionBackup
            ExpectedSha256 = [string]$Transaction.old_production_sha256
        },
        [pscustomobject]@{
            Backup = $TransactionPaths.DevelopmentBackup
            ExpectedSha256 = [string]$Transaction.old_development_sha256
        }
    )
    foreach ($record in $records) {
        if ((Get-LockFileSha256 -Path $record.Backup) -cne $record.ExpectedSha256) {
            throw "Lock transaction backup hash is invalid: $($record.Backup)"
        }
    }
}

function Remove-LockPairTransactionArtifacts {
    param(
        [Parameter(Mandatory)]
        [string]$TransactionPath
    )

    $transactionPaths = Resolve-LockPairTransactionPaths -TransactionPath $TransactionPath
    foreach ($fullPath in @(
        $transactionPaths.ProductionBackup,
        $transactionPaths.DevelopmentBackup,
        $transactionPaths.Journal
    )) {
        if (Test-Path -LiteralPath $fullPath) {
            Remove-Item -LiteralPath $fullPath -Force
        }
    }
}

function Restore-LockPairFromTransaction {
    param(
        [Parameter(Mandatory)]
        [psobject]$Transaction,
        [Parameter(Mandatory)]
        [string]$TransactionPath,
        [Parameter(Mandatory)]
        [string]$ProductionLock,
        [Parameter(Mandatory)]
        [string]$DevelopmentLock
    )

    $transactionPaths = Resolve-LockPairTransactionPaths -TransactionPath $TransactionPath
    $records = @(
        [pscustomobject]@{
            Backup = $transactionPaths.ProductionBackup
            Final = $ProductionLock
            ExpectedSha256 = [string]$Transaction.old_production_sha256
        },
        [pscustomobject]@{
            Backup = $transactionPaths.DevelopmentBackup
            Final = $DevelopmentLock
            ExpectedSha256 = [string]$Transaction.old_development_sha256
        }
    )
    foreach ($record in $records) {
        $backup = Assert-ProjectContainedPath -Path $record.Backup
        $final = Assert-ProjectContainedPath -Path $record.Final
        if ((Get-LockFileSha256 -Path $backup) -cne $record.ExpectedSha256) {
            throw "Lock transaction backup hash is invalid: $backup"
        }
        $restore = Assert-ProjectContainedPath -Path (
            "$backup.restore-$($Transaction.transaction_id)"
        )
        if (Test-Path -LiteralPath $restore) {
            Remove-Item -LiteralPath $restore -Force
        }
        Copy-DurableLockFile -Source $backup -Destination $restore
        Invoke-AtomicLockReplace -Source $restore -Destination $final `
            -DiscardedDestination "$backup.displaced-$($Transaction.transaction_id)"
    }
    if (
        (Get-LockFileSha256 -Path $ProductionLock) `
            -cne [string]$Transaction.old_production_sha256 `
            -or (Get-LockFileSha256 -Path $DevelopmentLock) `
            -cne [string]$Transaction.old_development_sha256
    ) {
        throw "Recovered lock pair does not match the journaled old authority."
    }
}

function Recover-LockPairTransaction {
    param(
        [Parameter(Mandatory)]
        [string]$TransactionPath,
        [Parameter(Mandatory)]
        [string]$ProductionLock,
        [Parameter(Mandatory)]
        [string]$DevelopmentLock,
        [scriptblock]$ValidateFinalPair,
        [switch]$ForceRollback
    )

    $transactionPaths = Resolve-LockPairTransactionPaths -TransactionPath $TransactionPath
    $journalPath = $transactionPaths.Journal
    if (-not (Test-Path -LiteralPath $journalPath -PathType Leaf)) {
        return
    }
    try {
        $transaction = [System.IO.File]::ReadAllText($journalPath) |
            ConvertFrom-Json -ErrorAction Stop
    }
    catch {
        throw "Lock-pair transaction journal is unreadable or invalid."
    }
    $expectedFields = @(
        "schema_version",
        "transaction_id",
        "production_lock",
        "development_lock",
        "production_backup",
        "development_backup",
        "old_production_sha256",
        "old_development_sha256",
        "new_production_sha256",
        "new_development_sha256"
    ) | Sort-Object
    $observedFields = @($transaction.PSObject.Properties.Name) | Sort-Object
    if (@(Compare-Object $expectedFields $observedFields).Count -ne 0) {
        throw "Lock-pair transaction journal fields are not exact."
    }
    if (
        $transaction.schema_version -ne 1 `
            -or [string]$transaction.transaction_id -notmatch '^[0-9a-f]{32}$'
    ) {
        throw "Lock-pair transaction journal identity is invalid."
    }
    $finalProduction = Assert-ProjectContainedPath -Path $ProductionLock
    $finalDevelopment = Assert-ProjectContainedPath -Path $DevelopmentLock
    $journalAuthorityPaths = @(
        @([string]$transaction.production_lock, $finalProduction),
        @([string]$transaction.development_lock, $finalDevelopment)
    )
    foreach ($journalPathRecord in $journalAuthorityPaths) {
        if (-not ([string]$journalPathRecord[0]).Equals(
            [string]$journalPathRecord[1],
            [System.StringComparison]::Ordinal
        )) {
            throw "Lock-pair transaction journal targets different authority files."
        }
    }
    $journalBackupPaths = @(
        @([string]$transaction.production_backup, $transactionPaths.ProductionBackup),
        @([string]$transaction.development_backup, $transactionPaths.DevelopmentBackup)
    )
    foreach ($journalPathRecord in $journalBackupPaths) {
        if (-not ([string]$journalPathRecord[0]).Equals(
            [string]$journalPathRecord[1],
            [System.StringComparison]::Ordinal
        )) {
            throw "Lock-pair transaction journal backup paths do not match code-derived paths."
        }
    }
    foreach ($digest in @(
        [string]$transaction.old_production_sha256,
        [string]$transaction.old_development_sha256,
        [string]$transaction.new_production_sha256,
        [string]$transaction.new_development_sha256
    )) {
        if ($digest -notmatch '^[0-9a-f]{64}$') {
            throw "Lock-pair transaction journal contains an invalid SHA-256."
        }
    }
    Assert-LockPairTransactionBackups `
        -Transaction $transaction -TransactionPaths $transactionPaths
    $productionHash = Get-LockFileSha256 -Path $finalProduction
    $developmentHash = Get-LockFileSha256 -Path $finalDevelopment
    $pairIsNew = (
        $productionHash -ceq [string]$transaction.new_production_sha256 `
            -and $developmentHash -ceq [string]$transaction.new_development_sha256
    )
    if ($pairIsNew -and -not $ForceRollback -and $null -ne $ValidateFinalPair) {
        try {
            $null = & $ValidateFinalPair
            Remove-LockPairTransactionArtifacts -TransactionPath $journalPath
            return
        }
        catch {
            $ForceRollback = $true
        }
    }
    try {
        Restore-LockPairFromTransaction -Transaction $transaction `
            -TransactionPath $journalPath `
            -ProductionLock $finalProduction -DevelopmentLock $finalDevelopment
    }
    catch {
        throw (
            "Lock-pair recovery failed; the durable journal and both backups were " +
            "preserved for retry. Manual recovery is required before environment " +
            "maintenance can continue. Cause: $($_.Exception.Message)"
        )
    }
    Remove-LockPairTransactionArtifacts -TransactionPath $journalPath
}

function Publish-ValidatedLockPair {
    param(
        [Parameter(Mandatory)]
        [string]$GeneratedProductionLock,
        [Parameter(Mandatory)]
        [string]$GeneratedDevelopmentLock,
        [Parameter(Mandatory)]
        [string]$ProductionLock,
        [Parameter(Mandatory)]
        [string]$DevelopmentLock,
        [Parameter(Mandatory)]
        [string]$PublicationLockPath,
        [Parameter(Mandatory)]
        [string]$TransactionPath,
        [Parameter(Mandatory)]
        [scriptblock]$ValidateFinalPair
    )

    $generatedProduction = Assert-ProjectContainedPath -Path $GeneratedProductionLock
    $generatedDevelopment = Assert-ProjectContainedPath -Path $GeneratedDevelopmentLock
    $finalProduction = Assert-ProjectContainedPath -Path $ProductionLock
    $finalDevelopment = Assert-ProjectContainedPath -Path $DevelopmentLock
    $transactionPaths = Resolve-LockPairTransactionPaths -TransactionPath $TransactionPath
    $productionBackup = $transactionPaths.ProductionBackup
    $developmentBackup = $transactionPaths.DevelopmentBackup

    $guard = Enter-LockPairPublicationGuard -LockPath $PublicationLockPath
    try {
        $null = Recover-LockPairTransaction `
            -TransactionPath $TransactionPath `
            -ProductionLock $finalProduction `
            -DevelopmentLock $finalDevelopment `
            -ValidateFinalPair $ValidateFinalPair
        foreach ($requiredFile in @(
            $generatedProduction,
            $generatedDevelopment,
            $finalProduction,
            $finalDevelopment
        )) {
            if (-not (Test-Path -LiteralPath $requiredFile -PathType Leaf)) {
                throw "Lock publication input is missing: $requiredFile"
            }
        }
        foreach ($backup in @(
            $productionBackup,
            $developmentBackup
        )) {
            if (Test-Path -LiteralPath $backup) {
                throw "Lock publication backup already exists after recovery: $backup"
            }
        }
        $null = New-OwnedDirectory `
            -Path $transactionPaths.TransactionDirectory -AllowExisting
        Copy-DurableLockFile -Source $finalProduction -Destination $productionBackup
        Copy-DurableLockFile -Source $finalDevelopment -Destination $developmentBackup
        $transaction = [ordered]@{
            schema_version = 1
            transaction_id = [System.Guid]::NewGuid().ToString("N")
            production_lock = $finalProduction
            development_lock = $finalDevelopment
            production_backup = $productionBackup
            development_backup = $developmentBackup
            old_production_sha256 = Get-LockFileSha256 -Path $productionBackup
            old_development_sha256 = Get-LockFileSha256 -Path $developmentBackup
            new_production_sha256 = Get-LockFileSha256 -Path $generatedProduction
            new_development_sha256 = Get-LockFileSha256 -Path $generatedDevelopment
        }
        Write-DurableLockTransaction -Path $TransactionPath -Payload $transaction
        Invoke-AtomicLockReplace -Source $generatedProduction `
            -Destination $finalProduction `
            -DiscardedDestination (
                Join-Path $transactionPaths.TransactionDirectory "requirements.lock.displaced"
            )
        Invoke-AtomicLockReplace -Source $generatedDevelopment `
            -Destination $finalDevelopment `
            -DiscardedDestination (
                Join-Path $transactionPaths.TransactionDirectory "requirements-dev.lock.displaced"
            )
        $null = & $ValidateFinalPair
        if (
            (Get-LockFileSha256 -Path $finalProduction) `
                -cne [string]$transaction.new_production_sha256 `
                -or (Get-LockFileSha256 -Path $finalDevelopment) `
                -cne [string]$transaction.new_development_sha256
        ) {
            throw "Published lock pair changed before final validation completed."
        }
        Remove-LockPairTransactionArtifacts -TransactionPath $TransactionPath
    }
    catch {
        $null = Recover-LockPairTransaction `
            -TransactionPath $TransactionPath `
            -ProductionLock $finalProduction `
            -DevelopmentLock $finalDevelopment `
            -ForceRollback
        throw
    }
    finally {
        Exit-LockPairPublicationGuard -Guard $guard
    }
}

function Remove-OwnedScratch {
    param(
        [Parameter(Mandatory)]
        [string]$Path,
        [Parameter(Mandatory)]
        [string]$HermeticRoot,
        [Parameter(Mandatory)]
        [string]$InvocationId
    )

    if (-not (Test-Path -LiteralPath $Path)) {
        return
    }
    $fullPath = Assert-ProjectContainedPath -Path $Path
    $fullHermeticRoot = Assert-ProjectContainedPath -Path $HermeticRoot
    $hermeticPrefix = $fullHermeticRoot.TrimEnd("\", "/") + [System.IO.Path]::DirectorySeparatorChar
    if (-not $fullPath.StartsWith(
        $hermeticPrefix,
        [System.StringComparison]::OrdinalIgnoreCase
    )) {
        throw "Refusing to remove non-hermetic path: $fullPath"
    }
    $leafName = [System.IO.Path]::GetFileName($fullPath)
    if (-not $leafName.EndsWith(
        "-$InvocationId",
        [System.StringComparison]::OrdinalIgnoreCase
    )) {
        throw "Refusing to remove a path not owned by this invocation: $fullPath"
    }
    $reparseDescendant = Get-ChildItem -LiteralPath $fullPath -Recurse -Force |
        Where-Object {
            ($_.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0
        } |
        Select-Object -First 1
    if ($null -ne $reparseDescendant) {
        throw "Refusing to remove scratch containing a reparse point: $($reparseDescendant.FullName)"
    }
    Remove-Item -LiteralPath $fullPath -Recurse -Force
}

$target = Assert-ProjectContainedPath -Path (Join-Path $projectRoot ".venv")
$targetPython = Assert-ProjectContainedPath -Path (Join-Path $target "Scripts\python.exe")
$productionLock = Assert-ProjectContainedPath -Path (Join-Path $projectRoot "requirements.lock")
$developmentLock = Assert-ProjectContainedPath -Path (Join-Path $projectRoot "requirements-dev.lock")
$verifier = Assert-ProjectContainedPath -Path (
    Join-Path $projectRoot "scripts\verify_locked_environment.py"
)
$environmentMaintenanceLock = Assert-ProjectContainedPath -Path (
    Join-Path $projectRoot "scripts\start_options_copilot.ps1"
)
$hermeticRoot = Assert-ProjectContainedPath -Path (
    Join-Path $projectRoot "data\options_copilot\hermetic"
)
$invocationId = [System.Guid]::NewGuid().ToString("N")
$bootstrap = Assert-ProjectContainedPath -Path (
    Join-Path $hermeticRoot "bootstrap-$invocationId"
)
$productionVerification = Assert-ProjectContainedPath -Path (
    Join-Path $hermeticRoot "production-verification-$invocationId"
)
$pipCache = Assert-ProjectContainedPath -Path (
    Join-Path $hermeticRoot "pip-cache-$invocationId"
)
$temp = Assert-ProjectContainedPath -Path (Join-Path $hermeticRoot "TEMP-$invocationId")
$tmp = Assert-ProjectContainedPath -Path (Join-Path $hermeticRoot "TMP-$invocationId")
$lockGeneration = Assert-ProjectContainedPath -Path (
    Join-Path $hermeticRoot "lock-generation-$invocationId"
)
$generatedProductionLock = Assert-ProjectContainedPath -Path (
    Join-Path $lockGeneration "requirements.lock"
)
$generatedDevelopmentLock = Assert-ProjectContainedPath -Path (
    Join-Path $lockGeneration "requirements-dev.lock"
)
$lockPublicationRoot = Assert-ProjectContainedPath -Path (
    Join-Path $hermeticRoot "lock-publication"
)
$lockPublicationGuard = Assert-ProjectContainedPath -Path (
    Join-Path $lockPublicationRoot "publication.lock"
)
$lockTransactionDirectory = Assert-ProjectContainedPath -Path (
    Join-Path $lockPublicationRoot "active-transaction"
)
$lockTransactionJournal = Assert-ProjectContainedPath -Path (
    Join-Path $lockTransactionDirectory "transaction.json"
)

$scopedEnvironmentNames = @("PIP_CACHE_DIR", "PIP_NO_CACHE_DIR", "TEMP", "TMP")
$previousEnvironment = @{}
foreach ($name in $scopedEnvironmentNames) {
    $previousEnvironment[$name] = [System.Environment]::GetEnvironmentVariable(
        $name,
        [System.EnvironmentVariableTarget]::Process
    )
}
$createdScratch = [System.Collections.Generic.List[string]]::new()
$environmentMaintenanceGuard = $null

try {
    $environmentMaintenanceGuard = Enter-EnvironmentMaintenanceTransaction `
        -LockPath $environmentMaintenanceLock
    $hermeticRoot = New-OwnedDirectory -Path $hermeticRoot -AllowExisting
    $lockPublicationRoot = New-OwnedDirectory -Path $lockPublicationRoot -AllowExisting
    foreach ($path in @($pipCache, $temp, $tmp)) {
        $createdScratch.Add($path)
        $null = New-OwnedDirectory -Path $path
    }
    $env:PIP_CACHE_DIR = $pipCache
    $env:PIP_NO_CACHE_DIR = "1"
    $env:TEMP = $temp
    $env:TMP = $tmp

    $startupRecoveryGuard = Enter-LockPairPublicationGuard `
        -LockPath $lockPublicationGuard
    try {
        $null = Recover-LockPairTransaction `
            -TransactionPath $lockTransactionJournal `
            -ProductionLock $productionLock `
            -DevelopmentLock $developmentLock
    }
    finally {
        Exit-LockPairPublicationGuard -Guard $startupRecoveryGuard
    }

    if ($Mode -eq "RegenerateAndCreate") {
        if (Test-Path -LiteralPath $target) {
            throw "Target environment already exists; refusing to delete or rewrite it: $target"
        }
        $pyLauncher = (Get-Command py.exe -ErrorAction Stop).Source
        $createdScratch.Add($lockGeneration)
        $null = New-OwnedDirectory -Path $lockGeneration
        $createdScratch.Add($bootstrap)
        Invoke-Checked -FilePath $pyLauncher -Arguments @("-3.12", "-m", "venv", $bootstrap)
        $bootstrapPython = Assert-ProjectContainedPath -Path (
            Join-Path $bootstrap "Scripts\python.exe"
        )
        Invoke-Checked -FilePath $bootstrapPython -Arguments @(
            "-m", "pip", "install", "--disable-pip-version-check", "--no-cache-dir",
            "pip-tools==7.6.0"
        )

        Push-Location $projectRoot
        try {
            Invoke-Checked -FilePath $bootstrapPython -Arguments @(
                "-m", "piptools", "compile", "--resolver=backtracking", "--generate-hashes",
                "--strip-extras", "--no-emit-index-url", "--no-emit-trusted-host",
                "--output-file=$generatedProductionLock", "pyproject.toml"
            )
            Invoke-Checked -FilePath $bootstrapPython -Arguments @(
                "-m", "piptools", "compile", "--resolver=backtracking", "--generate-hashes",
                "--strip-extras", "--no-emit-index-url", "--no-emit-trusted-host", "--extra=dev",
                "--output-file=$generatedDevelopmentLock", "pyproject.toml"
            )
        }
        finally {
            Pop-Location
        }

        Invoke-Checked -FilePath $bootstrapPython -Arguments @(
            $verifier,
            "--production-lock", $generatedProductionLock,
            "--development-lock", $generatedDevelopmentLock
        )
        $validatePublishedLocks = {
            Invoke-Checked -FilePath $bootstrapPython -Arguments @(
                $verifier,
                "--production-lock", $productionLock,
                "--development-lock", $developmentLock
            )
        }
        Publish-ValidatedLockPair `
            -GeneratedProductionLock $generatedProductionLock `
            -GeneratedDevelopmentLock $generatedDevelopmentLock `
            -ProductionLock $productionLock `
            -DevelopmentLock $developmentLock `
            -PublicationLockPath $lockPublicationGuard `
            -TransactionPath $lockTransactionJournal `
            -ValidateFinalPair $validatePublishedLocks

        Invoke-Checked -FilePath $pyLauncher -Arguments @("-3.12", "-m", "venv", $target)
        $targetPython = Assert-ProjectContainedPath -Path $targetPython
        Invoke-Checked -FilePath $targetPython -Arguments @(
            "-m", "pip", "install", "--disable-pip-version-check", "--no-cache-dir",
            "--require-hashes", "--no-deps", "-r", $developmentLock
        )
    }
    foreach ($requiredPath in @($targetPython, $productionLock, $developmentLock, $verifier)) {
        if (-not (Test-Path -LiteralPath $requiredPath -PathType Leaf)) {
            throw "Required hermetic artifact is missing: $requiredPath"
        }
    }
    if ($Mode -eq "Verify") {
        $validateExistingLocks = {
            Invoke-Checked -FilePath $targetPython -Arguments @(
                $verifier,
                "--production-lock", $productionLock,
                "--development-lock", $developmentLock
            )
        }
        $guard = Enter-LockPairPublicationGuard -LockPath $lockPublicationGuard
        try {
            $null = Recover-LockPairTransaction `
                -TransactionPath $lockTransactionJournal `
                -ProductionLock $productionLock `
                -DevelopmentLock $developmentLock `
                -ValidateFinalPair $validateExistingLocks
            $null = & $validateExistingLocks
        }
        finally {
            Exit-LockPairPublicationGuard -Guard $guard
        }
    }
    Invoke-Checked -FilePath $targetPython -Arguments @(
        $verifier, "--lock", $developmentLock,
        "--allow-bootstrap", "pip", "--allow-bootstrap", "setuptools"
    )
    & $targetPython -m pip check
    if ($LASTEXITCODE -ne 0) {
        throw "Target pip check failed with exit code ${LASTEXITCODE}."
    }

    $createdScratch.Add($productionVerification)
    Invoke-Checked -FilePath $targetPython -Arguments @(
        "-m", "venv", $productionVerification
    )
    $productionPython = Assert-ProjectContainedPath -Path (
        Join-Path $productionVerification "Scripts\python.exe"
    )
    Invoke-Checked -FilePath $productionPython -Arguments @(
        "-m", "pip", "install", "--disable-pip-version-check", "--no-cache-dir",
        "--require-hashes", "--no-deps", "-r", $productionLock
    )
    Invoke-Checked -FilePath $productionPython -Arguments @(
        $verifier, "--lock", $productionLock,
        "--allow-bootstrap", "pip", "--allow-bootstrap", "setuptools"
    )
    & $productionPython -m pip check
    if ($LASTEXITCODE -ne 0) {
        throw "Production verification pip check failed with exit code ${LASTEXITCODE}."
    }

    $audit = [ordered]@{
        status = "VERIFIED"
        mode = $Mode
        project_root = $projectRoot
        target = $target
        bootstrap = $bootstrap
        bootstrap_used = ($Mode -eq "RegenerateAndCreate")
        production_verification = $productionVerification
        pip_cache = $pipCache
        temp = $temp
        tmp = $tmp
        lock_generation = $lockGeneration
        all_paths_contained = $true
        artifact_drive = "G:"
    }
    $audit | ConvertTo-Json -Compress
}
finally {
    try {
        for ($index = $createdScratch.Count - 1; $index -ge 0; $index--) {
            Remove-OwnedScratch -Path $createdScratch[$index] `
                -HermeticRoot $hermeticRoot -InvocationId $invocationId
        }
    }
    finally {
        try {
            if ($null -ne $environmentMaintenanceGuard) {
                Exit-EnvironmentMaintenanceTransaction `
                    -Guard $environmentMaintenanceGuard
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
}
