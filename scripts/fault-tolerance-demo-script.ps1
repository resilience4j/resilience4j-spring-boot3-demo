# Sends a request every 2 seconds for 3 hours.
# At startup you pick the backend that receives failure requests and the percentage of requests that should fail.
# Failure requests hit a random failure endpoint on that backend; every other request hits a random
# success endpoint on one of the two backends you did not pick.
# Run from PowerShell:  .\scripts\fault-tolerance-demo-script.ps1
# Stop early with Ctrl+C.

$gatewayUrl = "http://localhost:9080"
$duration = [TimeSpan]::FromHours(3)
$pause    = 2

# Endpoints expected to return 2xx
$success_endpoints = @(
    "success"
    "ignore"
    "monoSuccess"
    "fluxSuccess"
    "fallback"
)

# Endpoints that fail, time out or throw on purpose
$failure_endpoints = @(
    "failure"
    "futureFailure"
    "futureTimeout"
    "successException"
    "monoFailure"
    "fluxFailure"
)

# Ask which backend should receive the failure requests
$backends = @("backendA", "backendB", "backendC")
$failureBackend = $null
while (-not $failureBackend) {
    Write-Host "Which backend should receive the failure requests?"
    for ($i = 0; $i -lt $backends.Count; $i++) {
        Write-Host ("  {0}) {1}" -f ($i + 1), $backends[$i])
    }
    $choice = (Read-Host "Enter 1-$($backends.Count) or a backend name").Trim()
    if ($choice -match '^\d+$' -and [int]$choice -ge 1 -and [int]$choice -le $backends.Count) {
        $failureBackend = $backends[[int]$choice - 1]
    } else {
        $failureBackend = $backends | Where-Object { $_ -ieq $choice } | Select-Object -First 1
        if (-not $failureBackend) { Write-Host "'$choice' is not a valid option.`n" -ForegroundColor Yellow }
    }
}

# Ask how often the chosen backend should hit a failure endpoint
$failurePercent = $null
while ($null -eq $failurePercent) {
    $answer = (Read-Host "`nWhat percentage (0-100) of requests should hit a failure endpoint on ${failureBackend}?").Trim().TrimEnd('%').Trim()
    $parsed = 0.0
    if ([double]::TryParse($answer, [ref]$parsed) -and $parsed -ge 0 -and $parsed -le 100) {
        $failurePercent = $parsed
    } else {
        Write-Host "'$answer' is not a number between 0 and 100." -ForegroundColor Yellow
    }
}

# The two backends the user did not pick receive all of the success requests
$successBackends = @($backends | Where-Object { $_ -ne $failureBackend })

# Pre-create stat rows so the summary lists every possible path
$stats = [ordered]@{}
foreach ($name in $failure_endpoints) { $stats["$failureBackend/$name"] = @{ Ok = 0; Error = 0 } }
foreach ($backend in $successBackends) {
    foreach ($name in $success_endpoints) { $stats["$backend/$name"] = @{ Ok = 0; Error = 0 } }
}

$end = (Get-Date) + $duration
Write-Host ""
Write-Host ("{0}% of requests -> failure endpoints on {1}" -f $failurePercent, $failureBackend)
Write-Host ("{0}% of requests -> success endpoints on {1} (picked at random)" -f (100 - $failurePercent), ($successBackends -join ' / '))
Write-Host "Sending a request every $pause s until $($end.ToString('HH:mm:ss'))`n"

while ((Get-Date) -lt $end) {
    # Roll against the failure percentage. A failure goes to the chosen backend with a random
    # failure endpoint; a success goes to one of the other two backends with a random success endpoint.
    $isFailure = (Get-Random -Minimum 0.0 -Maximum 100.0) -lt $failurePercent
    if ($isFailure) {
        $backend = $failureBackend
        $name    = Get-Random -InputObject $failure_endpoints
    } else {
        $backend = Get-Random -InputObject $successBackends
        $name    = Get-Random -InputObject $success_endpoints
    }
    $path = "$backend/$name"
    $url  = "$gatewayUrl/$path"
    $time = (Get-Date).ToString('HH:mm:ss')
    try {
        $response = Invoke-WebRequest -Uri $url -UseBasicParsing -TimeoutSec 10
        $stats[$path].Ok++
        Write-Host "$time  $($response.StatusCode)  /$path  $($response.Content)"
    } catch {
        $stats[$path].Error++
        # Failure endpoints return 4xx/5xx on purpose, so show the status code
        $status = if ($_.Exception.Response) { [int]$_.Exception.Response.StatusCode } else { "ERR" }
        Write-Host "$time  $status  /$path  $($_.Exception.Message)" -ForegroundColor Red
    }
    Start-Sleep -Seconds $pause
}

Write-Host "`nDone."
foreach ($path in $stats.Keys) {
    Write-Host ("{0,-28} ok: {1,6}   error: {2,6}" -f "/$path", $stats[$path].Ok, $stats[$path].Error)
}
