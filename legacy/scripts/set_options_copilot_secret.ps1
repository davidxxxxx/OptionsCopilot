[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("FINNHUB_API_KEY", "ALPHA_VANTAGE_API_KEY", "JIN10_MCP_TOKEN", "DEEPSEEK_API_KEY")]
    [string]$Name,

    [Parameter(Mandatory = $false)]
    [ValidateNotNullOrEmpty()]
    [string]$RevocationAttestation
)

$ErrorActionPreference = "Stop"
$hasRevocationAttestation = $PSBoundParameters.ContainsKey("RevocationAttestation")
if ($Name -eq "JIN10_MCP_TOKEN" -and -not $hasRevocationAttestation) {
    throw "JIN10_MCP_TOKEN requires -RevocationAttestation after the old token is revoked."
}
if ($Name -ne "JIN10_MCP_TOKEN" -and $hasRevocationAttestation) {
    throw "-RevocationAttestation is accepted only for JIN10_MCP_TOKEN."
}

$pythonArguments = @("-m", "options_copilot.security.cli", "set", $Name)
if ($hasRevocationAttestation) {
    $resolvedAttestation = Resolve-Path -LiteralPath $RevocationAttestation -ErrorAction Stop
    if (-not (Test-Path -LiteralPath $resolvedAttestation.Path -PathType Leaf)) {
        throw "Revocation attestation must be an existing file."
    }
    $pythonArguments += @("--revocation-attestation", $resolvedAttestation.Path)
}

$projectRoot = Split-Path -Parent $PSScriptRoot
Push-Location $projectRoot
try {
    # The replacement value is read twice by Python getpass.  It is never a
    # PowerShell parameter, process argument, environment variable, or output.
    & python @pythonArguments
    if ($LASTEXITCODE -ne 0) {
        throw "Options Copilot secret setup failed."
    }
}
finally {
    Pop-Location
}
