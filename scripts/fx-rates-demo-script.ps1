# fx-rates circuit breaker demo
# Requests one FX quote (/fx/quote) every few seconds.
# The market data provider has occasional HTTP errors, plus bad data: during random feed freezes its quotes
# go stale, and now and then it sends an off-market rate. Bad quotes are recorded as failures too.
#   Gray        - live, validated quote from the provider
#   DarkYellow  - provider error: cached rate used (indicative, wider spread)
#   DarkMagenta - stale or off-market quote: cached rate used (indicative, wider spread)
#   Yellow      - breaker OPEN: cached rate used (indicative, wider spread)
#   DarkCyan    - no cached rate young enough: quote refused, FX payment deferred
# Inspect or force the simulation:  GET /fx/status   PUT /fx/mode/{HEALTHY|FLAKY|FROZEN|DOWN}
# Run from PowerShell:  .\scripts\fx-rates-demo-script.ps1 [-PauseSeconds 2] [-DurationHours 3]
# Stop early with Ctrl+C.

param(
    [double]$PauseSeconds = 2,
    [double]$DurationHours = 3,
    [string]$GatewayUrl = "http://localhost:9080"
)

$path  = "fx/quote"
$stats = @{ Ok = 0; Error = 0; Live = 0; Cached = 0; BadData = 0; Refused = 0; CircuitOpen = 0 }

$end = (Get-Date).AddHours($DurationHours)
Write-Host "fx-rates: one quote every $PauseSeconds s until $($end.ToString('HH:mm:ss'))"
Write-Host "Check feed freezes with GET $GatewayUrl/fx/status`n"

while ((Get-Date) -lt $end) {
    $time = (Get-Date).ToString('HH:mm:ss')
    try {
        $response = Invoke-WebRequest -Uri "$GatewayUrl/$path" -Method Get -UseBasicParsing -TimeoutSec 10
        $stats.Ok++
        $content = $response.Content
        $color = "Gray"
        $isCircuitOpen = $content -match 'reason=fx-circuit-open'
        $isBadData     = $content -match 'reason=(stale-quote|off-market-rate)'
        if ($isCircuitOpen) { $stats.CircuitOpen++ }
        if ($isBadData)     { $stats.BadData++ }
        if ($content -match '^RATE_UNAVAILABLE') {
            # No cached rate young enough: refuse rather than misprice, FX payment deferred
            $stats.Refused++
            $color = "DarkCyan"
        } elseif ($content -match 'source=cache') {
            $stats.Cached++
            $color = if ($isCircuitOpen) { "Yellow" } elseif ($isBadData) { "DarkMagenta" } else { "DarkYellow" }
        } else {
            $stats.Live++
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
Write-Host ("{0,-20} ok: {1,6}   error: {2,6}   live: {3,6}   cached: {4,6}   bad data: {5,4}   refused: {6,4}   circuit-open: {7,6}" -f "/$path",
    $stats.Ok, $stats.Error, $stats.Live, $stats.Cached, $stats.BadData, $stats.Refused, $stats.CircuitOpen)
