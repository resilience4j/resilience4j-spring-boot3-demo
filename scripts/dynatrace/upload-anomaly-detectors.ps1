# Uploads the circuit breaker anomaly detectors (circuit-breaker-anomaly-detectors.json) to Dynatrace
# through the Settings 2.0 API, schema builtin:davis.anomaly-detectors.
#
# DQL-based anomaly detectors only work on the platform endpoint ({env}.apps.dynatrace.com), not the classic
# {env}.live.dynatrace.com endpoint, and need a platform token or OAuth bearer token with:
#   settings:objects:write, settings:objects:read, settings:schemas:read,
#   storage:metrics:read (Grail metrics the queries read),
#   davis:analyzers:execute (the detectors run without a service user as actor)
#
# Usage:
#   .\dynatrace\upload-anomaly-detectors.ps1 -EnvironmentId abc12345 -Token $env:DT_PLATFORM_TOKEN -ValidateOnly
#   .\dynatrace\upload-anomaly-detectors.ps1 -EnvironmentId abc12345 -Token $env:DT_PLATFORM_TOKEN
# -ValidateOnly asks Dynatrace to check the payload without creating anything. Run it first.

param(
    [Parameter(Mandatory = $true)]
    [string]$EnvironmentId,
    [Parameter(Mandatory = $true)]
    [string]$Token,
    [string]$File = (Join-Path $PSScriptRoot "circuit-breaker-anomaly-detectors.json"),
    [switch]$ValidateOnly
)

$uri = "https://$EnvironmentId.apps.dynatrace.com/platform/classic/environment-api/v2/settings/objects"
if ($ValidateOnly) { $uri += "?validateOnly=true" }

$body = Get-Content -Raw -Encoding UTF8 -Path $File
$titles = (ConvertFrom-Json $body) | ForEach-Object { $_.value.title }

Write-Host ("{0} {1} anomaly detector(s) to {2}" -f ($(if ($ValidateOnly) { "Validating" } else { "Uploading" })), $titles.Count, $uri)

try {
    $response = Invoke-RestMethod -Method Post -Uri $uri -Body ([System.Text.Encoding]::UTF8.GetBytes($body)) `
        -ContentType "application/json; charset=utf-8" -Headers @{ Authorization = "Bearer $Token" }
    for ($i = 0; $i -lt $titles.Count; $i++) {
        $result = if ($response -is [array]) { $response[$i] } else { $null }
        $id = if ($result -and $result.objectId) { $result.objectId } else { "ok" }
        Write-Host ("  {0,-60} {1}" -f $titles[$i], $id) -ForegroundColor Green
    }
} catch {
    Write-Host "Request failed: $($_.Exception.Message)" -ForegroundColor Red
    # The Settings API returns per-object validation errors in the response body
    if ($_.ErrorDetails -and $_.ErrorDetails.Message) {
        Write-Host $_.ErrorDetails.Message -ForegroundColor Red
    }
    exit 1
}
