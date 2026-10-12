[CmdletBinding()]
param(
    [ValidateRange(0, 65535)]
    [int]$Port = 0,
    [switch]$Background,
    [switch]$NoScheduler
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$python = Join-Path $root ".venv\Scripts\python.exe"
$pythonw = Join-Path $root ".venv\Scripts\pythonw.exe"
if (-not (Test-Path -LiteralPath $python)) {
    throw "未找到项目虚拟环境：$python"
}

if ($Port -eq 0) {
    $Port = [int](& $python -c "import sys; sys.path.insert(0, r'$root'); from copilot.config import load_settings; print(load_settings().web.port)")
}
if (Get-NetTCPConnection -State Listen -LocalAddress 127.0.0.1 -LocalPort $Port -ErrorAction SilentlyContinue) {
    Write-Output "端口 $Port 已在监听，OptionsCopilot 可能已经在运行：http://127.0.0.1:$Port"
    exit 0
}

$arguments = @("-m", "copilot", "serve", "--port", "$Port")
if ($NoScheduler) {
    $arguments += "--no-scheduler"
}

Push-Location -LiteralPath $root
try {
    if ($Background) {
        # pythonw has no console; copilot redirects its output to data\copilot\logs\console.log.
        Start-Process -FilePath $pythonw -ArgumentList $arguments -WorkingDirectory $root -WindowStyle Hidden
        Write-Output "OptionsCopilot 已在后台启动：http://127.0.0.1:$Port"
    }
    else {
        & $python @arguments
    }
}
finally {
    Pop-Location
}
