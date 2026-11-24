# swift-gateway circuit breaker demo
# Sends one cross-border payment to the SWIFT gateway (/swift/payments) every few seconds.
# The gateway has random error bursts (HTTP 503/502, dropped connections), so the breaker trips on its
# failure rate. Failed payments go to an outbox that is replayed while the breaker is CLOSED.
#   Gray       - ACK from the gateway
#   DarkYellow - gateway error: recorded as a failure, payment queued in the outbox
#   Yellow     - breaker OPEN: gateway not called, payment queued in the outbox
#   Magenta    - validation reject (HTTP 422): IGNORED by the breaker
# Inspect or force the simulation:  GET /swift/status   PUT /swift/mode/{HEALTHY|BURSTY|OUTAGE}
# Run from PowerShell:  .\scripts\swift-gateway-demo-script.ps1 [-PauseSeconds 2] [-DurationHours 3]
# Stop early with Ctrl+C.

param(
    [double]$PauseSeconds = 2,
    [double]$DurationHours = 3,
    [string]$GatewayUrl = "http://localhost:9080"
)

$path  = "swift/payments"
$stats = @{ Ok = 0; Error = 0; Queued = 0; CircuitOpen = 0; Rejected = 0 }

$end = (Get-Date).AddHours($DurationHours)
Write-Host "swift-gateway: one payment every $PauseSeconds s until $($end.ToString('HH:mm:ss'))"
Write-Host "Check bursts and the outbox with GET $GatewayUrl/swift/status`n"

while ((Get-Date) -lt $end) {
    $time = (Get-Date).ToString('HH:mm:ss')
    try {
        $response = Invoke-WebRequest -Uri "$GatewayUrl/$path" -Method Post -UseBasicParsing -TimeoutSec 10
        $stats.Ok++
        $content = $response.Content
        $color = "Gray"
        if ($content -match 'reason=swift-circuit-open') {
            # Breaker OPEN: the gateway was not called, payment went straight to the outbox
            $stats.CircuitOpen++
            $color = "Yellow"
        } elseif ($content -match '^QUEUED') {
            # Gateway error: recorded as a failure by the breaker, payment queued in the outbox
            $stats.Queued++
            $color = "DarkYellow"
        }
        Write-Host "$time  $($response.StatusCode)  /$path  $content" -ForegroundColor $color
    } catch {
        $status = if ($_.Exception.Response) { [int]$_.Exception.Response.StatusCode } else { "ERR" }
        if ($status -eq 422) {
            # Validation reject: ignored by the breaker, so it doesn't count against the gateway
            $stats.Rejected++
            Write-Host "$time  $status  /$path  rejected by gateway validation" -ForegroundColor Magenta
        } else {
            $stats.Error++
            Write-Host "$time  $status  /$path  $($_.Exception.Message)" -ForegroundColor Red
        }
    }
    Start-Sleep -Seconds $PauseSeconds
}

Write-Host "`nDone."
Write-Host ("{0,-20} ok: {1,6}   error: {2,6}   queued: {3,6}   circuit-open: {4,6}   rejected: {5,6}" -f "/$path",
    $stats.Ok, $stats.Error, $stats.Queued, $stats.CircuitOpen, $stats.Rejected)
