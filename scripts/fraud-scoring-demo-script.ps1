# fraud-scoring circuit breaker demo
# Scores one payment with the fraud model (/fraud/score) every few seconds.
# The model slows down as load rises during a recurring traffic peak (last 4 minutes of every 12); past the
# 1s TimeLimiter the call fails with a TimeoutException, so the breaker trips on its failure rate.
#   Gray       - scored by the model
#   DarkYellow - model timed out: recorded as a failure, decided by the rules fallback
#   Yellow     - breaker OPEN: model not called, decided by the rules fallback
# Inspect or force the simulation:  GET /fraud/status   PUT /fraud/mode/{NORMAL|PEAK|SCHEDULED}
# Run from PowerShell:  .\scripts\fraud-scoring-demo-script.ps1 [-PauseSeconds 2] [-DurationHours 3]
# Stop early with Ctrl+C.

param(
    [double]$PauseSeconds = 2,
    [double]$DurationHours = 3,
    [string]$GatewayUrl = "http://localhost:9080"
)

$path  = "fraud/score"
$stats = @{ Ok = 0; Error = 0; Timeout = 0; CircuitOpen = 0 }

$end = (Get-Date).AddHours($DurationHours)
Write-Host "fraud-scoring: one payment score every $PauseSeconds s until $($end.ToString('HH:mm:ss'))"
Write-Host "Check the simulation with GET $GatewayUrl/fraud/status`n"

while ((Get-Date) -lt $end) {
    $time = (Get-Date).ToString('HH:mm:ss')
    try {
        $response = Invoke-WebRequest -Uri "$GatewayUrl/$path" -Method Post -UseBasicParsing -TimeoutSec 10
        $stats.Ok++
        $content = $response.Content
        $color = "Gray"
        if ($content -match 'reason=model-timeout') {
            # TimeLimiter fired: recorded as a failure by the breaker, then decided by rules
            $stats.Timeout++
            $color = "DarkYellow"
        } elseif ($content -match 'reason=fraud-circuit-open') {
            # Breaker OPEN: the model was not called at all
            $stats.CircuitOpen++
            $color = "Yellow"
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
Write-Host ("{0,-20} ok: {1,6}   error: {2,6}   timeout: {3,6}   circuit-open: {4,6}" -f "/$path",
    $stats.Ok, $stats.Error, $stats.Timeout, $stats.CircuitOpen)
