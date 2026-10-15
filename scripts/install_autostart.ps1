[CmdletBinding()]
param(
    # Beijing/local time for the daily safety start, shortly before the 09:00 New York slot.
    [string]$DailyAt = "20:30",
    [switch]$Remove
)

$ErrorActionPreference = "Stop"
$taskName = "OptionsCopilot"
$root = Split-Path -Parent $PSScriptRoot
$pythonw = Join-Path $root ".venv\Scripts\pythonw.exe"

if ($Remove) {
    Unregister-ScheduledTask -TaskName $taskName -Confirm:$false -ErrorAction SilentlyContinue
    Write-Output "已删除计划任务 $taskName"
    return
}
if (-not (Test-Path -LiteralPath $pythonw)) {
    throw "未找到 $pythonw"
}

$action = New-ScheduledTaskAction -Execute $pythonw -Argument "-m copilot serve" -WorkingDirectory $root
$triggers = @(
    (New-ScheduledTaskTrigger -AtLogOn -User $env:USERNAME),
    (New-ScheduledTaskTrigger -Daily -At $DailyAt)
)
# One instance only: if it is already running the daily trigger is a no-op; if it died,
# the daily trigger (or a logon) starts it again. WakeToRun wakes a sleeping PC.
$settings = New-ScheduledTaskSettingsSet `
    -MultipleInstances IgnoreNew `
    -StartWhenAvailable `
    -WakeToRun `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -ExecutionTimeLimit ([TimeSpan]::Zero) `
    -RestartCount 3 `
    -RestartInterval (New-TimeSpan -Minutes 5)
$principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Limited

Register-ScheduledTask `
    -TaskName $taskName `
    -Action $action `
    -Trigger $triggers `
    -Settings $settings `
    -Principal $principal `
    -Description "OptionsCopilot 期权副驾：本地网页 127.0.0.1 + 自动调度（只读 IBKR，不下单）" `
    -Force | Out-Null

Write-Output "已注册计划任务 $taskName：登录时启动，并每天 $DailyAt 检查一次（已运行则跳过）。"
Write-Output "立即启动：Start-ScheduledTask -TaskName $taskName"
Write-Output "删除：pwsh -File `"$PSCommandPath`" -Remove"
