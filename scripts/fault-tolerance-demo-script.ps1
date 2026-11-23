# Sends a request every 2 seconds for 3 hours.
# At startup you pick the backend that receives failure requests and the percentage of requests that should fail.
# Failure requests hit a random failure endpoint on that backend; every other request hits a random
# success endpoint on one of the two backends you did not pick.
# Each loop also sends one request to the core banking ledger (/ledger), a balance inquiry or a posting.
# The ledger never errors; it gets slow during its batch window, so its circuit breaker trips on slow calls.
# Ledger responses served by a fallback (breaker OPEN) are shown in yellow, slow ledger calls in cyan.
# Each loop also scores one payment with the fraud model (/fraud/score). The model times out under load,
# so its circuit breaker trips on its failure rate. Rules-fallback decisions are shown in dark yellow
# (model timed out) or yellow (breaker OPEN).
# Each loop also sends one cross-border payment to the SWIFT gateway (/swift/payments). The gateway has random
# error bursts, so its circuit breaker trips on its failure rate. Payments queued in the outbox are shown in
# dark yellow (gateway error) or yellow (breaker OPEN); validation rejects (422, ignored by the breaker) in magenta.
# Each loop also fans out one customer notification to SMS, EMAIL and PUSH (/notifications). The hub fails often
# but its breaker threshold is 80%, so it only opens during a hub outage. Any dropped channel is shown in dark
# yellow, or yellow when the breaker is OPEN.
# Run from PowerShell:  .\scripts\fault-tolerance-demo-script.ps1
# Stop early with Ctrl+C.

$gatewayUrl = "http://localhost:9080"
$duration = [TimeSpan]::FromHours(3)
$pause    = 2

# Core banking ledger traffic (set to $false to leave it out)
$includeLedger = $true
$ledgerRequests = @(
    @{ Path = "ledger/balance";  Method = "Get"  }
    @{ Path = "ledger/postings"; Method = "Post" }
)
# Must match slowCallDurationThreshold for core-banking-ledger in application.yml
$ledgerSlowMs = 1500

# Fraud scoring traffic (set to $false to leave it out)
$includeFraud = $true
$fraudPath = "fraud/score"

# SWIFT gateway traffic (set to $false to leave it out)
$includeSwift = $true
$swiftPath = "swift/payments"

# Notification traffic (set to $false to leave it out)
$includeNotifications = $true
$notificationPath = "notifications"

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
if ($includeLedger) {
    foreach ($req in $ledgerRequests) { $stats[$req.Path] = @{ Ok = 0; Error = 0; Slow = 0; Fallback = 0 } }
}
if ($includeFraud) {
    $stats[$fraudPath] = @{ Ok = 0; Error = 0; Timeout = 0; Fallback = 0 }
}
if ($includeSwift) {
    $stats[$swiftPath] = @{ Ok = 0; Error = 0; Queued = 0; CircuitOpen = 0; Rejected = 0 }
}
if ($includeNotifications) {
    # Sent/Dropped/CircuitOpen count channels (3 per request), not requests
    $stats[$notificationPath] = @{ Ok = 0; Error = 0; Sent = 0; Dropped = 0; CircuitOpen = 0 }
}

$end = (Get-Date) + $duration
Write-Host ""
Write-Host ("{0}% of requests -> failure endpoints on {1}" -f $failurePercent, $failureBackend)
Write-Host ("{0}% of requests -> success endpoints on {1} (picked at random)" -f (100 - $failurePercent), ($successBackends -join ' / '))
if ($includeLedger) {
    Write-Host "Plus one core banking ledger request per loop (balance or posting); check mode with GET $gatewayUrl/ledger/status"
}
if ($includeFraud) {
    Write-Host "Plus one fraud scoring request per loop; check load with GET $gatewayUrl/fraud/status"
}
if ($includeSwift) {
    Write-Host "Plus one SWIFT payment per loop; check bursts and the outbox with GET $gatewayUrl/swift/status"
}
if ($includeNotifications) {
    Write-Host "Plus one notification fan-out (SMS, EMAIL, PUSH) per loop; check outages with GET $gatewayUrl/notifications/status"
}
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

    if ($includeLedger) {
        $req  = Get-Random -InputObject $ledgerRequests
        $path = $req.Path
        $time = (Get-Date).ToString('HH:mm:ss')
        try {
            $response = Invoke-WebRequest -Uri "$gatewayUrl/$path" -Method $req.Method -UseBasicParsing -TimeoutSec 10
            $stats[$path].Ok++
            $content = $response.Content
            $color = "Gray"
            if ($content -match 'source=cache|PENDING|BALANCE_UNAVAILABLE') {
                # Breaker OPEN: the fallback answered instead of the ledger
                $stats[$path].Fallback++
                $color = "Yellow"
            } elseif ($content -match 'latencyMs=(\d+)' -and [int]$Matches[1] -ge $ledgerSlowMs) {
                $stats[$path].Slow++
                $color = "Cyan"
            }
            Write-Host "$time  $($response.StatusCode)  /$path  $content" -ForegroundColor $color
        } catch {
            $stats[$path].Error++
            $status = if ($_.Exception.Response) { [int]$_.Exception.Response.StatusCode } else { "ERR" }
            Write-Host "$time  $status  /$path  $($_.Exception.Message)" -ForegroundColor Red
        }
    }

    if ($includeFraud) {
        $path = $fraudPath
        $time = (Get-Date).ToString('HH:mm:ss')
        try {
            $response = Invoke-WebRequest -Uri "$gatewayUrl/$path" -Method Post -UseBasicParsing -TimeoutSec 10
            $stats[$path].Ok++
            $content = $response.Content
            $color = "Gray"
            if ($content -match 'reason=model-timeout') {
                # TimeLimiter fired: recorded as a failure by the breaker, then decided by rules
                $stats[$path].Timeout++
                $color = "DarkYellow"
            } elseif ($content -match 'reason=fraud-circuit-open') {
                # Breaker OPEN: the model was not called at all
                $stats[$path].Fallback++
                $color = "Yellow"
            }
            Write-Host "$time  $($response.StatusCode)  /$path  $content" -ForegroundColor $color
        } catch {
            $stats[$path].Error++
            $status = if ($_.Exception.Response) { [int]$_.Exception.Response.StatusCode } else { "ERR" }
            Write-Host "$time  $status  /$path  $($_.Exception.Message)" -ForegroundColor Red
        }
    }

    if ($includeSwift) {
        $path = $swiftPath
        $time = (Get-Date).ToString('HH:mm:ss')
        try {
            $response = Invoke-WebRequest -Uri "$gatewayUrl/$path" -Method Post -UseBasicParsing -TimeoutSec 10
            $stats[$path].Ok++
            $content = $response.Content
            $color = "Gray"
            if ($content -match 'reason=swift-circuit-open') {
                # Breaker OPEN: the gateway was not called, payment went straight to the outbox
                $stats[$path].CircuitOpen++
                $color = "Yellow"
            } elseif ($content -match '^QUEUED') {
                # Gateway error: recorded as a failure by the breaker, payment queued in the outbox
                $stats[$path].Queued++
                $color = "DarkYellow"
            }
            Write-Host "$time  $($response.StatusCode)  /$path  $content" -ForegroundColor $color
        } catch {
            $status = if ($_.Exception.Response) { [int]$_.Exception.Response.StatusCode } else { "ERR" }
            if ($status -eq 422) {
                # Validation reject: ignored by the breaker, so it doesn't count against the gateway
                $stats[$path].Rejected++
                Write-Host "$time  $status  /$path  rejected by gateway validation" -ForegroundColor Magenta
            } else {
                $stats[$path].Error++
                Write-Host "$time  $status  /$path  $($_.Exception.Message)" -ForegroundColor Red
            }
        }
    }

    if ($includeNotifications) {
        $path = $notificationPath
        $time = (Get-Date).ToString('HH:mm:ss')
        try {
            $response = Invoke-WebRequest -Uri "$gatewayUrl/$path" -Method Post -UseBasicParsing -TimeoutSec 10
            $stats[$path].Ok++
            $content = $response.Content
            $sent        = ([regex]::Matches($content, '=SENT')).Count
            $circuitOpen = ([regex]::Matches($content, '=DROPPED:notification-circuit-open')).Count
            $dropped     = ([regex]::Matches($content, '=DROPPED:')).Count - $circuitOpen
            $stats[$path].Sent        += $sent
            $stats[$path].Dropped     += $dropped
            $stats[$path].CircuitOpen += $circuitOpen
            $color = if ($circuitOpen -gt 0) { "Yellow" } elseif ($dropped -gt 0) { "DarkYellow" } else { "Gray" }
            Write-Host "$time  $($response.StatusCode)  /$path  $content" -ForegroundColor $color
        } catch {
            $stats[$path].Error++
            $status = if ($_.Exception.Response) { [int]$_.Exception.Response.StatusCode } else { "ERR" }
            Write-Host "$time  $status  /$path  $($_.Exception.Message)" -ForegroundColor Red
        }
    }

    Start-Sleep -Seconds $pause
}

Write-Host "`nDone."
foreach ($path in $stats.Keys) {
    $line = "{0,-28} ok: {1,6}   error: {2,6}" -f "/$path", $stats[$path].Ok, $stats[$path].Error
    if ($stats[$path].ContainsKey('Sent')) {
        $line += "   channels sent: {0,6}   dropped: {1,6}   circuit-open: {2,6}" -f $stats[$path].Sent, $stats[$path].Dropped, $stats[$path].CircuitOpen
    } elseif ($stats[$path].ContainsKey('Rejected')) {
        $line += "   queued: {0,6}   circuit-open: {1,6}   rejected: {2,6}" -f $stats[$path].Queued, $stats[$path].CircuitOpen, $stats[$path].Rejected
    } elseif ($stats[$path].ContainsKey('Fallback')) {
        if ($stats[$path].ContainsKey('Timeout')) {
            $line += "   timeout: {0,6}   circuit-open: {1,6}" -f $stats[$path].Timeout, $stats[$path].Fallback
        } else {
            $line += "   slow: {0,6}   fallback: {1,6}" -f $stats[$path].Slow, $stats[$path].Fallback
        }
    }
    Write-Host $line
}
