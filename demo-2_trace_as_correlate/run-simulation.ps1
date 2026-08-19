<#
.SYNOPSIS
  One-shot launcher for the ping-pong distributed-tracing simulation (Windows).

  Design:
    - Infra (Kafka + Grafana LGTM) runs in DOCKER (docker compose up -d kafka otel-lgtm).
    - The three Spring Boot services run LOCALLY via the Gradle wrapper (bootRun), which uses the
      project's JDK 25 toolchain. They are launched headless (no popup windows) and each service's
      console output is captured to run_<module>.log / run_<module>.err.log so you can inspect
      startup, Kafka consumption, and telemetry export after the fact.
    - Launched PIDs are recorded to .sim_pids so stop-simulation.ps1 can shut them down cleanly.

  Steps:
    1. Start Kafka + Grafana LGTM containers and wait until healthy.
    2. Launch service_ping / service_pong / service_bang (background, logged to files).
    3. Wait until each reports UP on /actuator/health.
    4. Open Grafana and run trigger_ping in CONTINUOUS mode (Ctrl+C to stop).

  The trigger drives BOTH endpoints concurrently: POST /api/ping and POST /api/ping/fail at a
  -PingPerFail : 1 mix (default 4:1), so happy-path and failure-path telemetry appear together.

.EXAMPLE
  ./run-simulation.ps1
  ./run-simulation.ps1 -Concurrency 25 -ThinkMs 100
  ./run-simulation.ps1 -DurationSec 120
  ./run-simulation.ps1 -PingPerFail 9        # fewer faults (9:1)
#>
param(
    [int]$Concurrency = 12,
    [int]$ThinkMs = 200,
    [int]$ReportSec = 5,
    [int]$DurationSec = 0,    # 0 = run until Ctrl+C
    [int]$PingPerFail = 4     # happy:fail endpoint mix (4:1 by default)
)

$ErrorActionPreference = 'Stop'
# Docker Compose writes normal progress to stderr; on PowerShell 7.3+ that can otherwise be treated
# as a terminating error and abort this script. Keep native-command stderr from stopping us.
$PSNativeCommandUseErrorActionPreference = $false
$demo = $PSScriptRoot
$KAFKA = 'localhost:29092'          # Kafka EXTERNAL listener (from docker-compose) for local apps
$OTLP  = 'http://localhost:4318'    # OTLP HTTP endpoint exposed by the otel-lgtm container
$gradlew = Join-Path $demo 'gradlew.bat'
$pidsFile = Join-Path $demo '.sim_pids'

function Wait-Http($url, $timeoutSec) {
    $deadline = (Get-Date).AddSeconds($timeoutSec)
    while ((Get-Date) -lt $deadline) {
        try {
            $r = Invoke-WebRequest -UseBasicParsing $url -TimeoutSec 5
            if ($r.StatusCode -eq 200) { return $true }
        } catch { Start-Sleep -Seconds 2 }
    }
    return $false
}

function Wait-Tcp($tcpHost, $port, $timeoutSec) {
    $deadline = (Get-Date).AddSeconds($timeoutSec)
    while ((Get-Date) -lt $deadline) {
        try {
            $c = New-Object Net.Sockets.TcpClient
            $c.Connect($tcpHost, $port); $c.Close(); return $true
        } catch { Start-Sleep -Seconds 2 }
    }
    return $false
}

# Grafana readiness is probed with curl.exe hitting /api/health (returns 200 only once Grafana's
# HTTP server is actually listening). We deliberately do NOT use:
#   - the container Docker HEALTHCHECK -> it reports "healthy" BEFORE Grafana's HTTP binds (false OK),
#   - PowerShell's Invoke-WebRequest -> it reports "connection closed" against Grafana (false FAIL).
# Grafana in this otel-lgtm build (Grafana 13) can take several MINUTES to start on a busy machine,
# so this is treated as best-effort: the pipeline only needs Kafka + the OTLP collector.
function Test-GrafanaReady {
    $code = (curl.exe -s -o NUL -w "%{http_code}" --max-time 4 http://localhost:3000/api/health 2>$null)
    return ($code -eq '200')
}
function Wait-Grafana($timeoutSec) {
    $deadline = (Get-Date).AddSeconds($timeoutSec)
    while ((Get-Date) -lt $deadline) {
        if (Test-GrafanaReady) { return $true }
        Start-Sleep -Seconds 3
    }
    return $false
}

$services = @(
    @{ name = 'service_ping'; port = 8080 },
    @{ name = 'service_pong'; port = 8081 },
    @{ name = 'service_bang'; port = 8082 }
)

Write-Host '== 1/4  Starting Kafka + Grafana LGTM containers (Docker) ==' -ForegroundColor Cyan
docker compose -f "$demo/docker-compose.yml" up -d kafka otel-lgtm | Out-Host

# The services only need two things to run and emit telemetry: Kafka (messaging) and the OTLP
# collector endpoint (traces/metrics/logs). Both come up within seconds. The Grafana UI is only for
# *viewing* signals and can take minutes to start (Grafana 13), so it must NOT block startup.
Write-Host '   waiting for Kafka (localhost:29092) ...'
if (-not (Wait-Tcp 'localhost' 29092 90)) { Write-Error 'Kafka did not become reachable.'; exit 1 }
Write-Host '   waiting for OTLP collector (localhost:4318) ...'
if (-not (Wait-Tcp 'localhost' 4318 90)) { Write-Error 'OTLP collector did not become reachable.'; exit 1 }
Write-Host '   infra ready (Kafka + OTLP collector).' -ForegroundColor Green

Write-Host '   checking Grafana UI (best-effort, non-blocking) ...'
if (Wait-Grafana 20) {
    Write-Host '   Grafana UI is up: http://localhost:3000' -ForegroundColor Green
} else {
    Write-Host '   Grafana UI is still starting (first boot of Grafana 13 can take a few minutes).' -ForegroundColor Yellow
    Write-Host '   Continuing anyway - open http://localhost:3000 once it finishes; the pipeline works meanwhile.' -ForegroundColor Yellow
}

Write-Host '== 2/4  Launching the three services (Gradle bootRun, background, logged to file) ==' -ForegroundColor Cyan
# Env vars are inherited by the child processes; all three point at the containerized infra.
$env:KAFKA_BOOTSTRAP = $KAFKA
$env:OTLP_ENDPOINT   = $OTLP
Remove-Item $pidsFile -ErrorAction SilentlyContinue | Out-Null

foreach ($s in $services) {
    $log    = Join-Path $demo "run_$($s.name).log"
    $errLog = Join-Path $demo "run_$($s.name).err.log"
    Remove-Item $log, $errLog -ErrorAction SilentlyContinue | Out-Null

    $proc = Start-Process -FilePath $gradlew `
        -ArgumentList @(":$($s.name):bootRun", '--console=plain') `
        -WorkingDirectory $demo `
        -RedirectStandardOutput $log `
        -RedirectStandardError $errLog `
        -WindowStyle Hidden -PassThru
    $proc.Id | Out-File -FilePath $pidsFile -Append -Encoding ascii
    Write-Host "   started $($s.name) (PID $($proc.Id), port $($s.port)) -> run_$($s.name).log"
}

Write-Host '== 3/4  Waiting for services to report UP ==' -ForegroundColor Cyan
foreach ($s in $services) {
    Write-Host "   waiting for $($s.name) on http://localhost:$($s.port)/actuator/health ..."
    if (-not (Wait-Http "http://localhost:$($s.port)/actuator/health" 180)) {
        Write-Error "$($s.name) did not become healthy. Last lines of run_$($s.name).log:"
        Get-Content (Join-Path $demo "run_$($s.name).log") -Tail 40 -ErrorAction SilentlyContinue | Out-Host
        exit 1
    }
    Write-Host "   $($s.name) is UP." -ForegroundColor Green
}

Write-Host '== 4/4  Starting CONTINUOUS trigger (Ctrl+C to stop) ==' -ForegroundColor Cyan
Write-Host "   Grafana: http://localhost:3000  (Explore -> Tempo / Loki / Prometheus)" -ForegroundColor Yellow
Write-Host "   Happy path : POST /api/ping       (Ping -> Pong -> Bang, one continuous trace)"
Write-Host "   Failure path: POST /api/ping/fail  (mixed in at ${PingPerFail}:1, produces ERROR logs)"
Write-Host "   Service logs: run_service_ping.log / run_service_pong.log / run_service_bang.log"
# Only pop the browser if Grafana is actually serving; otherwise a blank 'page isn't working' tab.
if (Test-GrafanaReady) { try { Start-Process 'http://localhost:3000' } catch { } }
else { Write-Host "   (Grafana not up yet - open http://localhost:3000 in a minute or two.)" -ForegroundColor Yellow }
Write-Host ''

$triggerArgs = @('Trigger.java',
    '--concurrency', "$Concurrency",
    '--think-ms', "$ThinkMs",
    '--report-sec', "$ReportSec",
    '--ping-per-fail', "$PingPerFail")
if ($DurationSec -gt 0) { $triggerArgs += @('--duration-sec', "$DurationSec") }

Push-Location "$demo/trigger_ping"
try {
    & java @triggerArgs
} finally {
    Pop-Location
    Write-Host ''
    Write-Host 'Trigger stopped. The services + containers are still running.' -ForegroundColor Yellow
    Write-Host 'Run ./stop-simulation.ps1 to shut everything down.' -ForegroundColor Yellow
}
