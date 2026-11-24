# customer-profile circuit breaker demo
# Reads one customer profile (/customers/profile) every few seconds.
# This is the healthy baseline: the breaker stays CLOSED unless you switch the simulation with
# PUT /customers/mode/DEGRADED (or DOWN) to show an incident on demand.
#   Gray       - profile from the profile service
#   DarkYellow - service error: cached profile served (stale)
#   Yellow     - breaker OPEN: cached profile served (stale)
#   DarkCyan   - no usable cached copy: fail-safe default profile (caller applies conservative limits)
# Inspect or force the simulation:  GET /customers/status   PUT /customers/mode/{HEALTHY|DEGRADED|DOWN}
# Run from PowerShell:  .\scripts\customer-profile-demo-script.ps1 [-PauseSeconds 2] [-DurationHours 3]
# Stop early with Ctrl+C.

param(
    [double]$PauseSeconds = 2,
    [double]$DurationHours = 3,
    [string]$GatewayUrl = "http://localhost:9080"
)

$path  = "customers/profile"
$stats = @{ Ok = 0; Error = 0; FromCache = 0; Defaults = 0; CircuitOpen = 0 }

$end = (Get-Date).AddHours($DurationHours)
Write-Host "customer-profile: one profile read every $PauseSeconds s until $($end.ToString('HH:mm:ss'))"
Write-Host "Check the simulation with GET $GatewayUrl/customers/status`n"

while ((Get-Date) -lt $end) {
    $time = (Get-Date).ToString('HH:mm:ss')
    try {
        $response = Invoke-WebRequest -Uri "$GatewayUrl/$path" -Method Get -UseBasicParsing -TimeoutSec 10
        $stats.Ok++
        $content = $response.Content
        $color = "Gray"
        $isCircuitOpen = $content -match 'reason=customer-circuit-open'
        if ($isCircuitOpen) { $stats.CircuitOpen++ }
        if ($content -match 'source=default') {
            # No usable cached copy: fail-safe default profile, caller applies conservative limits
            $stats.Defaults++
            $color = "DarkCyan"
        } elseif ($content -match 'source=cache') {
            $stats.FromCache++
            $color = if ($isCircuitOpen) { "Yellow" } else { "DarkYellow" }
        }
        Write-Host "$time  $($response.StatusCode)  /$path  $content" -ForegroundColor $color
    } catch {
        $stats.Error++
        $status = if ($_.Exception.Response) { [int]$_.Exception.Response.StatusCode } else { "ERR" }
        Write-Host "$time  $status  /$path  $($_.Exception.Message)" -ForegroundColor Red
    }
    Start-Sleep -Seconds $PauseSeconds
}

Write-Host "`nDone."
Write-Host ("{0,-20} ok: {1,6}   error: {2,6}   from cache: {3,6}   defaults: {4,6}   circuit-open: {5,6}" -f "/$path",
    $stats.Ok, $stats.Error, $stats.FromCache, $stats.Defaults, $stats.CircuitOpen)
