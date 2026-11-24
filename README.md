# payment-orchestration-service

[![standard-readme compliant](https://img.shields.io/badge/readme%20style-standard-brightgreen.svg?style=flat-square)](https://github.com/RichardLitt/standard-readme)

Downstream Dependency Failure Simulator 

A Spring Boot payment orchestration service that calls seven simulated downstream dependencies of a bank's payment
flow, each protected by its own Resilience4j circuit breaker. Every dependency fails in a different, realistic way and
has a fallback suited to how critical it is. Metrics go to Dynatrace, where a notebook and Davis anomaly detectors
show how each breaker behaves, including across two datacenters.

## Table of Contents

- [Background](#background)
- [Install](#install)
- [Usage](#usage)
  - [Run the service](#run-the-service)
  - [Generate traffic](#generate-traffic)
  - [Force a failure](#force-a-failure)
  - [Import the Dynatrace notebook](#import-the-dynatrace-notebook)
- [Dynatrace anomaly detection](#dynatrace-anomaly-detection)
- [API](#api)
- [Maintainers](#maintainers)
- [Contributing](#contributing)
- [License](#license)

## Background

A payment service is only as reliable as the systems it depends on. This project models the dependencies a payment
orchestration service at a large bank typically calls, and shows how a circuit breaker for each one keeps a failing
dependency from taking the whole payment flow down with it.

Each simulation changes its own failure mode on a schedule or at random, so the breakers open and close on their own
while the demo scripts run:

| Circuit breaker | Dependency | Failure mode | Trips on | Threshold | Fallback |
|---|---|---|---|---|---|
| `core-banking-ledger` | Mainframe system of record | Slow calls during a batch window (last 3 min of every 10) | Slow-call rate | 50% of calls over 1.5 s | Stale cached balance; postings queued as pending |
| `fraud-scoring` | Real-time fraud model | Timeouts under load during a traffic peak (last 4 min of every 12) | Failure rate (1 s TimeLimiter) | 50% | Rules engine: small payments approved, the rest to manual review |
| `swift-gateway` | SWIFT cross-border gateway | Random bursts of HTTP 503/502 and dropped connections | Failure rate | 40% | Outbox, replayed while the breaker is closed |
| `notification-service` | SMS, email and push providers | Fails 25-45% every day, plus rare outages | Failure rate | 80% | Drop the notification |
| `customer-profile` | Customer master / KYC | None by default: the healthy baseline | Failure rate | 50% | Cached profile, then fail-safe defaults |
| `sanctions-screening` | Watchlist screening engine | Errors during watchlist reloads (last 2 min of every 15) | Failure rate, after 2 retries | 25% | Fail closed: payment held for compliance review |
| `fx-rates` | FX market data provider | HTTP errors plus bad data: stale quotes during feed freezes, off-market rates | Failure rate | 50% | Other datacenter, then cached rate with a wider spread, else refuse |

A few design points the simulations demonstrate:

- **Thresholds follow criticality.** The notification hub tolerates 80% failures because a dropped notification costs
  nothing; sanctions screening opens at 25% because it is a compliance control.
- **Not every exception is a failure.** SWIFT validation rejects are ignored by the breaker, and HTTP 429 rate limits
  have to be listed explicitly for the notification breaker to count them.
- **A 200 OK isn't always a success.** A stale or off-market FX quote is recorded as a failure.
- **Retry and circuit breaker work together.** Sanctions screening retries transient errors, and an open breaker stops
  the retries from adding load.

All names, accounts, watchlist entries and rates in the simulations are fictional.

The original Resilience4j demo backends (`backendA`, `backendB`, `backendC`) are still in the project, with their own
demo script.

## Install

Requirements:

- Java 17 (the Gradle toolchain is set to 17; the Gradle wrapper is included)
- PowerShell, for the demo scripts
- A Dynatrace environment, for the notebook and anomaly detectors

Clone the repository and build:

```powershell
.\gradlew build
```

### Sending metrics to Dynatrace

Metrics are exported with the Micrometer Dynatrace registry (v2), configured under `management.dynatrace.metrics.export`
in `application.yml`. With no `uri` set, the registry sends metrics to the local OneAgent, so install OneAgent on the
machine that runs the service. Without OneAgent, set the Metrics API endpoint and an API token with the
`metrics.ingest` scope instead:

```powershell
.\gradlew bootRun --args="--management.dynatrace.metrics.export.uri=https://<environment-id>.live.dynatrace.com/api/v2/metrics/ingest --management.dynatrace.metrics.export.api-token=<token>"
```

Every metric carries an `application` dimension (`payment-orchestration-service`) and a `datacenter` dimension
(see [Run the service](#run-the-service)).

## Usage

### Run the service

Run two instances to simulate two datacenters. The `datacenter` property becomes the `datacenter` dimension on every
metric:

```powershell
.\gradlew bootRun                                                         # prod-us-east-1 on port 9080 (default)
.\gradlew bootRun --args="--server.port=8080 --datacenter=prod-us-east-2"  # prod-us-east-2 on port 8080
```

One instance on port 9080 is enough for everything except the multi-datacenter FX demo. You can also set the
datacenter with the `DATACENTER` environment variable.

Each instance exposes all Actuator endpoints, including `/actuator/health` (with circuit breaker health) and
`/actuator/circuitbreakers`.

### Generate traffic

Each circuit breaker has its own demo script in `scripts/`. Each sends a request about every 2 seconds for 3 hours,
color-codes every response (live answer, fallback, open breaker), and prints a summary at the end. The comments at the
top of each script explain its colors.

| Script | Circuit breaker |
|---|---|
| `ledger-demo-script.ps1` | `core-banking-ledger` |
| `fraud-scoring-demo-script.ps1` | `fraud-scoring` |
| `swift-gateway-demo-script.ps1` | `swift-gateway` |
| `notification-demo-script.ps1` | `notification-service` |
| `customer-profile-demo-script.ps1` | `customer-profile` |
| `sanctions-screening-demo-script.ps1` | `sanctions-screening` |
| `fx-rates-demo-script.ps1` | `fx-rates`, against both datacenters |
| `fault-tolerance-demo-script.ps1` | The original `backendA`/`backendB`/`backendC` demo |

Run one script:

```powershell
.\scripts\ledger-demo-script.ps1
.\scripts\ledger-demo-script.ps1 -PauseSeconds 1 -DurationHours 1
```

Or start every circuit breaker script at once, each in its own window, with the launcher:

```powershell
.\scripts\payment-dependencies-demo-script.ps1
.\scripts\payment-dependencies-demo-script.ps1 -Only ledger,fx-rates
```

The FX script sends each quote to `prod-us-east-1` (9080) first and fails over to `prod-us-east-2` (8080) when the
primary can't return a live quote. Use `-Routing Both` to send every quote to both datacenters instead, or
`-Primary prod-us-east-2` to swap the primary.

The breakers that open on a schedule need up to 10-15 minutes to show their first cycle, so let the scripts run for at
least 30 minutes before you look at the results in Dynatrace.

### Force a failure

Every simulation has a status endpoint and a mode switch, so you can show an incident on demand instead of waiting for
the schedule:

```powershell
Invoke-RestMethod http://localhost:9080/sanctions/status
Invoke-WebRequest -Method Put http://localhost:9080/sanctions/mode/DOWN
Invoke-WebRequest -Method Put http://localhost:9080/sanctions/mode/SCHEDULED   # back to normal
```

The modes for each simulation are listed under [API](#api). The default modes are also set in `application.yml`,
under each simulation's `*.simulation` section.

### Import the Dynatrace notebook

`dynatrace/payment-orchestration-circuit-breakers.notebook.json` is a Dynatrace notebook with DQL aggregations of the
circuit breaker metrics: current state and time spent open for every breaker, an outcome scorecard, failures and
shed calls, one section per failure pattern, and the FX breaker split by datacenter.

1. In Dynatrace, open the **Notebooks** app.
2. Choose **Upload** and select the JSON file.
3. Set the timeframe to start after both instances were started with the `datacenter` tag.

## Dynatrace anomaly detection

The `dynatrace` folder contains one Davis anomaly detector for each circuit breaker simulation, defined in
`circuit-breaker-anomaly-detectors.json`, and a script that uploads them through the Dynatrace Settings 2.0 API
(schema `builtin:davis.anomaly-detectors`). Upload them once per Dynatrace environment.

| Detector | Fires when | Event type |
|---|---|---|
| `core-banking-ledger` | Slow-call rate above 50% in 3 of 5 minutes | Performance |
| `fraud-scoring` | More than 5 TimeLimiter timeouts per minute in 3 of 5 minutes | Performance |
| `swift-gateway` | Circuit breaker open in 2 of 5 minutes | Availability |
| `notification-service` | Circuit breaker open in 3 of 5 minutes (everyday failures are not alerted on) | Custom alert |
| `customer-profile` | Failure rate above 10% in 3 of 5 minutes (early warning before the breaker opens at 50%) | Error |
| `sanctions-screening` | Circuit breaker open in any minute (payments are held for compliance) | Error |
| `fx-rates` | Circuit breaker open in 2 of 5 minutes, one event per datacenter | Availability |

Every detector is split by the `datacenter` metric dimension, so each datacenter alerts on its own.

### Prerequisites

- The application is exporting metrics to Dynatrace with the `datacenter` tag. It defaults to `prod-us-east-1`;
  start a second instance with `--server.port=8080 --datacenter=prod-us-east-2`. Metrics recorded before the tag
  was added are not evaluated.
- A Dynatrace **platform token** (or OAuth client bearer token) with these permissions:
  - `settings:objects:write`, `settings:objects:read`, `settings:schemas:read`
  - `storage:metrics:read`
  - `davis:analyzers:execute`

DQL-based anomaly detectors only work on the platform endpoint (`https://{environment-id}.apps.dynatrace.com`),
not the classic `https://{environment-id}.live.dynatrace.com` endpoint. The script uses the platform endpoint.

### Upload the detectors

From the project root in PowerShell, validate first (nothing is created), then upload:

```powershell
$env:DT_PLATFORM_TOKEN = "<your platform token>"
.\dynatrace\upload-anomaly-detectors.ps1 -EnvironmentId <your-environment-id> -Token $env:DT_PLATFORM_TOKEN -ValidateOnly
.\dynatrace\upload-anomaly-detectors.ps1 -EnvironmentId <your-environment-id> -Token $env:DT_PLATFORM_TOKEN
```

The script prints each detector's title and the settings object ID Dynatrace created for it. Running the upload
again creates duplicates, so delete or edit existing detectors in Dynatrace instead of re-uploading.

### Verify

Open the **Anomaly Detection** app in Dynatrace. The seven detectors are listed with the source
`payment-orchestration-service circuit breaker demo`. Run the demo scripts in `scripts/` (or force a failure, for
example `Invoke-WebRequest -Method Put http://localhost:9080/sanctions/mode/DOWN`) and the matching Davis events
and problems appear within a few minutes.

### Troubleshooting

- **401 or 403:** the token is missing one of the permissions above, or it is a classic API token rather than a
  platform token.
- **Validation error on `executionSettings.actor`:** your environment requires detectors to run as a service user.
  Create one, then add `"actor": "<service user UUID>"` to each detector's `executionSettings` in
  `circuit-breaker-anomaly-detectors.json` and upload again.
- **No events:** check that the metrics carry the `datacenter` dimension, and that the thresholds suit your traffic.
  They are tuned for the demo scripts, at about one request every 2 seconds per breaker.

## API

All endpoints are on both instances (ports 9080 and 8080). Request parameters are optional; random demo values are
used for anything left out.

| Circuit breaker | Endpoints | Modes |
|---|---|---|
| `core-banking-ledger` | `GET /ledger/balance?accountId=`<br>`POST /ledger/postings?accountId=&amount=` | `HEALTHY`, `DEGRADED`, `SCHEDULED` (default) |
| `fraud-scoring` | `POST /fraud/score?accountId=&amount=&channel=` | `NORMAL`, `PEAK`, `SCHEDULED` (default) |
| `swift-gateway` | `POST /swift/payments?accountId=&amount=&currency=&beneficiaryBic=` | `HEALTHY`, `BURSTY` (default), `OUTAGE` |
| `notification-service` | `POST /notifications?accountId=&template=` (SMS, email and push)<br>`POST /notifications/{SMS\|EMAIL\|PUSH}` | `HEALTHY`, `FLAKY` (default), `OUTAGE` |
| `customer-profile` | `GET /customers/profile?customerId=` | `HEALTHY` (default), `DEGRADED`, `DOWN` |
| `sanctions-screening` | `POST /sanctions/screen?beneficiaryName=&country=` | `HEALTHY`, `SCHEDULED` (default), `DOWN` |
| `fx-rates` | `GET /fx/quote?currency=&amount=` | `HEALTHY`, `FLAKY` (default), `FROZEN`, `DOWN` |

Each simulation also has these, under the same path prefix (`/ledger`, `/fraud`, `/swift`, `/notifications`,
`/customers`, `/sanctions`, `/fx`):

- `GET <prefix>/status` shows the simulation mode, its schedule (for example, seconds until the next batch window or
  burst), and counts of how requests were answered. All except `/ledger` and `/fraud` also show the circuit breaker
  state; for every breaker, `/actuator/circuitbreakers` has it.
- `PUT <prefix>/mode/{mode}` changes the mode while the service runs.

Useful examples:

- `POST /swift/payments?beneficiaryBic=BAD` returns a validation reject (HTTP 422) that the breaker ignores.
- `POST /sanctions/screen?beneficiaryName=OBSIDIAN%20HARBOUR%20TRADING%20LTD` returns a potential watchlist match.
- `PUT /fx/mode/FROZEN` freezes the FX feed; quotes go stale and the breaker opens on bad data within seconds.

## Maintainers

[@Nick Montana](https://github.com/Nick Montana)

## Contributing



Small note: If editing the README, please conform to the
[standard-readme](https://github.com/RichardLitt/standard-readme) specification.

## License

MIT © 2026 Nick Montana
