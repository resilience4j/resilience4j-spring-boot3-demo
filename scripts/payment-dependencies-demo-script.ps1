# Drives traffic through the payment-orchestration-service's five downstream circuit breakers.
# Each loop sends one request to every enabled dependency, then pauses 2 seconds; runs for 3 hours.
# Each simulation changes its own failure mode on a schedule, so the breakers open and close on their own.
# (The original backendA/B/C demo is in fault-tolerance-demo-script.ps1.)
#
#   core-banking-ledger   /ledger            Gets slow during its batch window; trips on slow calls.
#                                            Fallback answers (breaker OPEN) in yellow, slow calls in cyan.
#   fraud-scoring         /fraud/score       Times out under load; trips on its failure rate.
#                                            Rules-fallback decisions in dark yellow (model timed out) or yellow (breaker OPEN).
#   swift-gateway         /swift/payments    Random error bursts; trips on its failure rate.
#                                            Outbox-queued payments in dark yellow (gateway error) or yellow (breaker OPEN);
#                                            validation rejects (422, ignored by the breaker) in magenta.
#   notification-service  /notifications    Fails often, but its threshold is 80%, so it only opens during a hub outage.
#                                            Dropped channels in dark yellow, or yellow when the breaker is OPEN.
#   customer-profile      /customers/profile Healthy baseline; stays CLOSED unless you PUT /customers/mode/DEGRADED.
#                                            Cached profiles in dark yellow (service error) or yellow (breaker OPEN);
#                                            fail-safe defaults (no cached copy) in dark cyan.
#
# Each dependency has GET /<path>/status and PUT /<path>/mode/{mode} endpoints to inspect or force its state.
# Run from PowerShell:  .\scripts\payment-dependencies-demo-script.ps1
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

# Customer profile traffic (set to $false to leave it out)
$includeCustomerProfile = $true
$customerProfilePath = "customers/profile"

# Pre-create stat rows so the summary lists every path
$stats = [ordered]@{}
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
if ($includeCustomerProfile) {
    $stats[$customerProfilePath] = @{ Ok = 0; Error = 0; FromCache = 0; Defaults = 0; CircuitOpen = 0 }
}

if ($stats.Count -eq 0) {
    Write-Host "Every dependency is turned off; set at least one `$include... variable to `$true." -ForegroundColor Yellow
    return
}

$end = (Get-Date) + $duration
Write-Host ""
if ($includeLedger) {
    Write-Host "Core banking ledger: one balance or posting per loop; check mode with GET $gatewayUrl/ledger/status"
}
if ($includeFraud) {
    Write-Host "Fraud scoring: one payment score per loop; check load with GET $gatewayUrl/fraud/status"
}
if ($includeSwift) {
    Write-Host "SWIFT gateway: one payment per loop; check bursts and the outbox with GET $gatewayUrl/swift/status"
}
if ($includeNotifications) {
    Write-Host "Notifications: one fan-out (SMS, EMAIL, PUSH) per loop; check outages with GET $gatewayUrl/notifications/status"
}
if ($includeCustomerProfile) {
    Write-Host "Customer profile: one read per loop (healthy baseline); check it with GET $gatewayUrl/customers/status"
}
Write-Host "Pausing $pause s between loops until $($end.ToString('HH:mm:ss'))`n"

while ((Get-Date) -lt $end) {

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

    if ($includeCustomerProfile) {
        $path = $customerProfilePath
        $time = (Get-Date).ToString('HH:mm:ss')
        try {
            $response = Invoke-WebRequest -Uri "$gatewayUrl/$path" -Method Get -UseBasicParsing -TimeoutSec 10
            $stats[$path].Ok++
            $content = $response.Content
            $color = "Gray"
            if ($content -match 'reason=customer-circuit-open') {
                $stats[$path].CircuitOpen++
            }
            if ($content -match 'source=default') {
                # No usable cached copy: fail-safe default profile, caller applies conservative limits
                $stats[$path].Defaults++
                $color = "DarkCyan"
            } elseif ($content -match 'source=cache') {
                $stats[$path].FromCache++
                $color = if ($content -match 'reason=customer-circuit-open') { "Yellow" } else { "DarkYellow" }
            }
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
    if ($stats[$path].ContainsKey('Defaults')) {
        $line += "   from cache: {0,6}   defaults: {1,6}   circuit-open: {2,6}" -f $stats[$path].FromCache, $stats[$path].Defaults, $stats[$path].CircuitOpen
    } elseif ($stats[$path].ContainsKey('Sent')) {
        $line += "   channels sent: {0,6}   dropped: {1,6}   circuit-open: {2,6}" -f $stats[$path].Sent, $stats[$path].Dropped, $stats[$path].CircuitOpen
    } elseif ($stats[$path].ContainsKey('Rejected')) {
        $line += "   queued: {0,6}   circuit-open: {1,6}   rejected: {2,6}" -f $stats[$path].Queued, $stats[$path].CircuitOpen, $stats[$path].Rejected
    } elseif ($stats[$path].ContainsKey('Timeout')) {
        $line += "   timeout: {0,6}   circuit-open: {1,6}" -f $stats[$path].Timeout, $stats[$path].Fallback
    } elseif ($stats[$path].ContainsKey('Slow')) {
        $line += "   slow: {0,6}   fallback: {1,6}" -f $stats[$path].Slow, $stats[$path].Fallback
    }
    Write-Host $line
}
