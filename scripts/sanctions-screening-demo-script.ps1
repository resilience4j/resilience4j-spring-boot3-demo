# sanctions-screening circuit breaker demo
# Screens one payment's beneficiary (/sanctions/screen) every few seconds.
# The screening engine fails during watchlist reload windows (last 2 minutes of every 15). Each call gets
# two quick retries, then the breaker trips at 25% failures. FAIL CLOSED: unscreened payments are held.
#   Gray       - screened and cleared on the first attempt
#   Cyan       - cleared, but only after the Retry recovered from a transient error
#   Magenta    - potential watchlist match: held for an analyst (a business outcome, not a failure)
#   DarkYellow - engine unavailable after the retries: payment HELD for compliance review
#   Yellow     - breaker OPEN: payment HELD for compliance review without calling the engine
# Inspect or force the simulation:  GET /sanctions/status   PUT /sanctions/mode/{HEALTHY|SCHEDULED|DOWN}
# Run from PowerShell:  .\scripts\sanctions-screening-demo-script.ps1 [-PauseSeconds 2] [-DurationHours 3]
# Stop early with Ctrl+C.

param(
    [double]$PauseSeconds = 2,
    [double]$DurationHours = 3,
    [string]$GatewayUrl = "http://localhost:9080"
)

$path  = "sanctions/screen"
$stats = @{ Ok = 0; Error = 0; Cleared = 0; Retried = 0; PotentialMatches = 0; Held = 0; CircuitOpen = 0 }

$end = (Get-Date).AddHours($DurationHours)
Write-Host "sanctions-screening: one beneficiary screen every $PauseSeconds s until $($end.ToString('HH:mm:ss'))"
Write-Host "Check reload windows with GET $GatewayUrl/sanctions/status`n"

while ((Get-Date) -lt $end) {
    $time = (Get-Date).ToString('HH:mm:ss')
    try {
        $response = Invoke-WebRequest -Uri "$GatewayUrl/$path" -Method Post -UseBasicParsing -TimeoutSec 10
        $stats.Ok++
        $content = $response.Content
        $color = "Gray"
        if ($content -match '^HELD') {
            # FAIL CLOSED: the payment could not be screened, so it is held, never released
            if ($content -match 'reason=sanctions-circuit-open') {
                $stats.CircuitOpen++
                $color = "Yellow"
            } else {
                $stats.Held++
                $color = "DarkYellow"
            }
        } elseif ($content -match 'POTENTIAL_MATCH') {
            # Business outcome, not a failure: held for an analyst
            $stats.PotentialMatches++
            $color = "Magenta"
        } else {
            $stats.Cleared++
            if ($content -match 'attempts=(\d+)' -and [int]$Matches[1] -gt 1) {
                # Cleared, but only after the Retry recovered from a transient error
                $stats.Retried++
                $color = "Cyan"
            }
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
Write-Host ("{0,-20} ok: {1,6}   error: {2,6}   cleared: {3,6} (after retry: {4,4})   matches: {5,4}   held: {6,6}   circuit-open: {7,6}" -f "/$path",
    $stats.Ok, $stats.Error, $stats.Cleared, $stats.Retried, $stats.PotentialMatches, $stats.Held, $stats.CircuitOpen)
