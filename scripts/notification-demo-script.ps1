# notification-service circuit breaker demo
# Fans out one customer notification to SMS, EMAIL and PUSH (/notifications) every few seconds; each channel
# is a separate call through the breaker. The hub fails 25-45% of the time on a normal day, but the breaker's
# threshold is 80%, so it only opens during a random hub-wide outage.
#   Gray       - every channel sent
#   DarkYellow - at least one channel dropped (rate limited, provider error, connection error)
#   Yellow     - at least one channel dropped because the breaker is OPEN
# Inspect or force the simulation:  GET /notifications/status   PUT /notifications/mode/{HEALTHY|FLAKY|OUTAGE}
# Run from PowerShell:  .\scripts\notification-demo-script.ps1 [-PauseSeconds 2] [-DurationHours 3]
# Stop early with Ctrl+C.

param(
    [double]$PauseSeconds = 2,
    [double]$DurationHours = 3,
    [string]$GatewayUrl = "http://localhost:9080"
)

$path = "notifications"
# Sent/Dropped/CircuitOpen count channels (3 per request), not requests
$stats = @{ Ok = 0; Error = 0; Sent = 0; Dropped = 0; CircuitOpen = 0 }

$end = (Get-Date).AddHours($DurationHours)
Write-Host "notification-service: one fan-out (SMS, EMAIL, PUSH) every $PauseSeconds s until $($end.ToString('HH:mm:ss'))"
Write-Host "Check outages with GET $GatewayUrl/notifications/status`n"

while ((Get-Date) -lt $end) {
    $time = (Get-Date).ToString('HH:mm:ss')
    try {
        $response = Invoke-WebRequest -Uri "$GatewayUrl/$path" -Method Post -UseBasicParsing -TimeoutSec 10
        $stats.Ok++
        $content = $response.Content
        $sent        = ([regex]::Matches($content, '=SENT')).Count
        $circuitOpen = ([regex]::Matches($content, '=DROPPED:notification-circuit-open')).Count
        $dropped     = ([regex]::Matches($content, '=DROPPED:')).Count - $circuitOpen
        $stats.Sent        += $sent
        $stats.Dropped     += $dropped
        $stats.CircuitOpen += $circuitOpen
        $color = if ($circuitOpen -gt 0) { "Yellow" } elseif ($dropped -gt 0) { "DarkYellow" } else { "Gray" }
        Write-Host "$time  $($response.StatusCode)  /$path  $content" -ForegroundColor $color
    } catch {
        $stats.Error++
        $status = if ($_.Exception.Response) { [int]$_.Exception.Response.StatusCode } else { "ERR" }
        Write-Host "$time  $status  /$path  $($_.Exception.Message)" -ForegroundColor Red
    }
    Start-Sleep -Seconds $PauseSeconds
}

Write-Host "`nDone."
Write-Host ("{0,-20} ok: {1,6}   error: {2,6}   channels sent: {3,6}   dropped: {4,6}   circuit-open: {5,6}" -f "/$path",
    $stats.Ok, $stats.Error, $stats.Sent, $stats.Dropped, $stats.CircuitOpen)
