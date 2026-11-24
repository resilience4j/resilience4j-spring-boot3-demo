# Launches the demo script for each of the payment-orchestration-service's downstream circuit breakers,
# each in its own PowerShell window, so all of them generate traffic at the same time.
# Every breaker has its own script, which can also be run on its own:
#
#   ledger-demo-script.ps1               core-banking-ledger   slow calls during a batch window
#   fraud-scoring-demo-script.ps1        fraud-scoring         timeouts under load
#   swift-gateway-demo-script.ps1        swift-gateway         random error bursts, outbox replay
#   notification-demo-script.ps1         notification-service  fails often, 80% threshold, drop on failure
#   customer-profile-demo-script.ps1     customer-profile      healthy baseline, cache fallback
#   sanctions-screening-demo-script.ps1  sanctions-screening   retry + strict breaker, fail closed
#   fx-rates-demo-script.ps1             fx-rates              bad data (stale/off-market quotes) counts as failure
#
# (The original backendA/B/C demo is in fault-tolerance-demo-script.ps1.)
# Run from PowerShell:  .\scripts\payment-dependencies-demo-script.ps1 [-PauseSeconds 2] [-DurationHours 3]
# Pass -Only to start a subset, e.g.  -Only ledger,fx-rates
# Close a window, or press Ctrl+C in it, to stop that script.

param(
    [double]$PauseSeconds = 2,
    [double]$DurationHours = 3,
    [string]$GatewayUrl = "http://localhost:9080",
    [string[]]$Only = @()
)

$scripts = [ordered]@{
    "ledger"              = "ledger-demo-script.ps1"
    "fraud-scoring"       = "fraud-scoring-demo-script.ps1"
    "swift-gateway"       = "swift-gateway-demo-script.ps1"
    "notification"        = "notification-demo-script.ps1"
    "customer-profile"    = "customer-profile-demo-script.ps1"
    "sanctions-screening" = "sanctions-screening-demo-script.ps1"
    "fx-rates"            = "fx-rates-demo-script.ps1"
}

$unknown = @($Only | Where-Object { -not $scripts.Contains($_) })
if ($unknown.Count -gt 0) {
    Write-Host "Unknown name(s) in -Only: $($unknown -join ', '). Use: $($scripts.Keys -join ', ')" -ForegroundColor Yellow
    return
}

# Start each script with the same PowerShell (Windows PowerShell or pwsh) that is running this launcher
$shell = (Get-Process -Id $PID).Path

foreach ($name in $scripts.Keys) {
    if ($Only.Count -gt 0 -and $Only -notcontains $name) { continue }
    $file = Join-Path $PSScriptRoot $scripts[$name]
    Start-Process -FilePath $shell -ArgumentList @(
        "-NoExit", "-ExecutionPolicy", "Bypass", "-File", "`"$file`"",
        "-PauseSeconds", $PauseSeconds, "-DurationHours", $DurationHours, "-GatewayUrl", $GatewayUrl
    )
    Write-Host "Started $name ($($scripts[$name]))"
}
