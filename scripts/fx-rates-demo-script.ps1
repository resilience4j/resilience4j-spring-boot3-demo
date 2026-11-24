# fx-rates circuit breaker demo - multi-datacenter
# Requests one FX quote (/fx/quote) every few seconds from two instances of the app, one per datacenter:
#   prod-us-east-1   http://localhost:9080
#   prod-us-east-2   http://localhost:8080
# Each instance has its own fx-rates circuit breaker and its own random feed-freeze schedule, so the two
# regions degrade independently.
#
# The market data provider has occasional HTTP errors, plus bad data: during random feed freezes its quotes
# go stale, and now and then it sends an off-market rate. Bad quotes are recorded as failures too.
#
# Routing (-Routing):
#   Failover (default) - every quote goes to the primary region first. If the primary can't give a LIVE quote
#                        (it fell back to a cached rate, refused, or is unreachable), the same quote is re-requested
#                        from the other region. A quote is a read, so retrying it in another region is safe.
#                        Order of preference: live local quote > live remote quote > cached rate > refuse.
#   Both               - every quote is sent to both regions, to compare their breakers side by side.
#
# Colors:
#   Gray        - live, validated quote from the provider
#   DarkYellow  - provider error: cached rate used (indicative, wider spread)
#   DarkMagenta - stale or off-market quote: cached rate used (indicative, wider spread)
#   Yellow      - breaker OPEN: cached rate used (indicative, wider spread)
#   DarkCyan    - no cached rate young enough: quote refused, FX payment deferred
#   Red         - region unreachable or HTTP error
#   Green       - failover rescued the quote: the other region returned a live quote
#
# Inspect or force each region:  GET /fx/status   PUT /fx/mode/{HEALTHY|FLAKY|FROZEN|DOWN}  on either port, e.g.
#   Invoke-WebRequest -Method Put http://localhost:9080/fx/mode/FROZEN    (east-1 feed freezes; watch failover to east-2)
# Run from PowerShell:  .\scripts\fx-rates-demo-script.ps1 [-Routing Failover|Both] [-Primary prod-us-east-2]
#                                                        [-PauseSeconds 2] [-DurationHours 3]
# Stop early with Ctrl+C.

param(
    [double]$PauseSeconds = 2,
    [double]$DurationHours = 3,
    [Alias("GatewayUrl")]
    [string]$EastUrl = "http://localhost:9080",
    [string]$East2Url = "http://localhost:8080",
    [ValidateSet("Failover", "Both")]
    [string]$Routing = "Failover",
    [ValidateSet("prod-us-east-1", "prod-us-east-2")]
    [string]$Primary = "prod-us-east-1"
)

$path = "fx/quote"
$currencies = @("EUR", "GBP", "JPY", "CAD", "MXN", "INR", "SGD")

$regions = [ordered]@{
    "prod-us-east-1" = $EastUrl
    "prod-us-east-2" = $East2Url
}
$secondary = @($regions.Keys | Where-Object { $_ -ne $Primary })[0]

# Per-region outcome counts
$stats = [ordered]@{}
foreach ($name in $regions.Keys) {
    $stats[$name] = @{ Requests = 0; Live = 0; Cached = 0; BadData = 0; Refused = 0; CircuitOpen = 0; Error = 0 }
}
# Routing outcome counts (Failover mode)
$routingStats = @{ Quotes = 0; LiveFromPrimary = 0; FailedOver = 0; RescuedBySecondary = 0; EndedCached = 0; EndedRefused = 0; EndedError = 0 }

# Sends one quote request to a region, records the outcome, prints it, and returns the outcome kind:
# Live, Cached, Refused or Error.
function Request-Quote([string]$region, [string]$query, [string]$prefix, [bool]$isFailover) {
    $s = $stats[$region]
    $s.Requests++
    $time = (Get-Date).ToString('HH:mm:ss')
    $label = "{0}{1,-15}" -f $prefix, "[$region]"
    try {
        $response = Invoke-WebRequest -Uri "$($regions[$region])/$path$query" -Method Get -UseBasicParsing -TimeoutSec 10
        $content = $response.Content
        $isCircuitOpen = $content -match 'reason=fx-circuit-open'
        $isBadData     = $content -match 'reason=(stale-quote|off-market-rate)'
        if ($isCircuitOpen) { $s.CircuitOpen++ }
        if ($isBadData)     { $s.BadData++ }
        if ($content -match '^RATE_UNAVAILABLE') {
            # No cached rate young enough: refuse rather than misprice, FX payment deferred
            $s.Refused++
            $kind = "Refused"
            $color = "DarkCyan"
        } elseif ($content -match 'source=cache') {
            $s.Cached++
            $kind = "Cached"
            $color = if ($isCircuitOpen) { "Yellow" } elseif ($isBadData) { "DarkMagenta" } else { "DarkYellow" }
        } else {
            $s.Live++
            $kind = "Live"
            $color = if ($isFailover) { "Green" } else { "Gray" }
        }
        Write-Host "$time  $label $($response.StatusCode)  /$path  $content" -ForegroundColor $color
        return $kind
    } catch {
        $s.Error++
        $status = if ($_.Exception.Response) { [int]$_.Exception.Response.StatusCode } else { "ERR" }
        Write-Host "$time  $label $status  /$path  $($_.Exception.Message)" -ForegroundColor Red
        return "Error"
    }
}

$end = (Get-Date).AddHours($DurationHours)
Write-Host "fx-rates multi-datacenter demo: one quote every $PauseSeconds s until $($end.ToString('HH:mm:ss'))"
foreach ($name in $regions.Keys) { Write-Host ("  {0,-15} {1}" -f $name, $regions[$name]) }
if ($Routing -eq "Failover") {
    Write-Host "Routing: Failover - primary $Primary, fails over to $secondary when $Primary can't give a live quote"
} else {
    Write-Host "Routing: Both - every quote goes to both regions"
}
Write-Host "Check each region with GET <url>/fx/status`n"

while ((Get-Date) -lt $end) {
    # Pick the quote here, so a failover asks the other region for exactly the same quote
    $currency = Get-Random -InputObject $currencies
    $amount   = "{0:F2}" -f (Get-Random -Minimum 100.0 -Maximum 50000.0)
    $query    = "?currency=$currency&amount=$amount"

    if ($Routing -eq "Both") {
        foreach ($name in $regions.Keys) { [void](Request-Quote $name $query "" $false) }
    } else {
        $routingStats.Quotes++
        $primaryKind = Request-Quote $Primary $query "" $false
        if ($primaryKind -eq "Live") {
            $routingStats.LiveFromPrimary++
        } else {
            # Primary couldn't give a live quote: ask the other region before settling for cache or refusal
            $routingStats.FailedOver++
            $secondaryKind = Request-Quote $secondary $query "  -> failover " $true
            switch ($secondaryKind) {
                "Live"    { $routingStats.RescuedBySecondary++ }
                default {
                    # Neither region had a live quote: the best remaining answer wins (cached > refused > error)
                    $best = @($primaryKind, $secondaryKind) | Sort-Object { @{ Cached = 0; Refused = 1; Error = 2 }[$_] } | Select-Object -First 1
                    switch ($best) {
                        "Cached"  { $routingStats.EndedCached++ }
                        "Refused" { $routingStats.EndedRefused++ }
                        default   { $routingStats.EndedError++ }
                    }
                }
            }
        }
    }
    Start-Sleep -Seconds $PauseSeconds
}

Write-Host "`nDone."
foreach ($name in $stats.Keys) {
    $s = $stats[$name]
    Write-Host ("{0,-15} requests: {1,6}   live: {2,6}   cached: {3,6}   bad data: {4,4}   refused: {5,4}   circuit-open: {6,6}   error: {7,4}" -f $name,
        $s.Requests, $s.Live, $s.Cached, $s.BadData, $s.Refused, $s.CircuitOpen, $s.Error)
}
if ($Routing -eq "Failover") {
    $r = $routingStats
    Write-Host ("`nQuotes: {0}   live from {1}: {2}   failed over to {3}: {4} (rescued with a live quote: {5}, ended cached: {6}, refused: {7}, error: {8})" -f
        $r.Quotes, $Primary, $r.LiveFromPrimary, $secondary, $r.FailedOver, $r.RescuedBySecondary, $r.EndedCached, $r.EndedRefused, $r.EndedError)
}
