# core-banking-ledger circuit breaker demo
# Sends one balance inquiry or posting to /ledger every few seconds.
# The ledger never errors; it gets slow during its batch window (last 3 minutes of every 10), so its
# circuit breaker trips on its slow-call rate.
#   Gray   - fast call answered by the ledger
#   Cyan   - slow call (>= slowCallDurationThreshold), still answered by the ledger
#   Yellow - breaker OPEN: fallback answered (stale cached balance, or posting queued as PENDING)
# Inspect or force the simulation:  GET /ledger/status   PUT /ledger/mode/{HEALTHY|DEGRADED|SCHEDULED}
# Run from PowerShell:  .\scripts\ledger-demo-script.ps1 [-PauseSeconds 2] [-DurationHours 3]
# Stop early with Ctrl+C.

param(
    [double]$PauseSeconds = 2,
    [double]$DurationHours = 3,
    [string]$GatewayUrl = "http://localhost:9080"
)

$requests = @(
    @{ Path = "ledger/balance";  Method = "Get"  }
    @{ Path = "ledger/postings"; Method = "Post" }
)
# Must match slowCallDurationThreshold for core-banking-ledger in application.yml
$slowMs = 1500

$stats = [ordered]@{}
foreach ($req in $requests) { $stats[$req.Path] = @{ Ok = 0; Error = 0; Slow = 0; Fallback = 0 } }

$end = (Get-Date).AddHours($DurationHours)
Write-Host "core-banking-ledger: one balance or posting every $PauseSeconds s until $($end.ToString('HH:mm:ss'))"
Write-Host "Check the simulation with GET $GatewayUrl/ledger/status`n"

while ((Get-Date) -lt $end) {
    $req  = Get-Random -InputObject $requests
    $path = $req.Path
    $time = (Get-Date).ToString('HH:mm:ss')
    try {
        $response = Invoke-WebRequest -Uri "$GatewayUrl/$path" -Method $req.Method -UseBasicParsing -TimeoutSec 10
        $stats[$path].Ok++
        $content = $response.Content
        $color = "Gray"
        if ($content -match 'source=cache|PENDING|BALANCE_UNAVAILABLE') {
            # Breaker OPEN: the fallback answered instead of the ledger
            $stats[$path].Fallback++
            $color = "Yellow"
        } elseif ($content -match 'latencyMs=(\d+)' -and [int]$Matches[1] -ge $slowMs) {
            $stats[$path].Slow++
            $color = "Cyan"
        }
        Write-Host "$time  $($response.StatusCode)  /$path  $content" -ForegroundColor $color
    } catch {
        $stats[$path].Error++
        $status = if ($_.Exception.Response) { [int]$_.Exception.Response.StatusCode } else { "ERR" }
        Write-Host "$time  $status  /$path  $($_.Exception.Message)" -ForegroundColor Red
    }
    Start-Sleep -Seconds $PauseSeconds
}

Write-Host "`nDone."
foreach ($path in $stats.Keys) {
    Write-Host ("{0,-20} ok: {1,6}   error: {2,6}   slow: {3,6}   fallback: {4,6}" -f "/$path",
        $stats[$path].Ok, $stats[$path].Error, $stats[$path].Slow, $stats[$path].Fallback)
}
