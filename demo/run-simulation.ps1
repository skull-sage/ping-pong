<#
.SYNOPSIS
  One-shot launcher for the ping-pong distributed-tracing simulation.

  Steps:
    1. Starts Kafka + Grafana LGTM in Docker containers.
    2. Launches service_ping, service_pong, service_bang locally via the Gradle wrapper
       (each in its own window), pointed at the containerized Kafka + OTLP endpoint.
    3. Waits until everything is healthy.
    4. Runs trigger_ping in CONTINUOUS mode to mimic real-time user traffic (Ctrl+C to stop).

.EXAMPLE
  ./run-simulation.ps1
  ./run-simulation.ps1 -Concurrency 25 -ThinkMs 100
  ./run-simulation.ps1 -DurationSec 120
#>
param(
    [int]$Concurrency = 12,
    [int]$ThinkMs = 200,
    [int]$ReportSec = 5,
    [int]$DurationSec = 0   # 0 = run until Ctrl+C
)

$ErrorActionPreference = 'Stop'
$demo = $PSScriptRoot
$KAFKA = 'localhost:29092'
$OTLP  = 'http://localhost:4318'

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

Write-Host '== 1/4  Starting Kafka + Grafana LGTM containers ==' -ForegroundColor Cyan
docker compose -f "$demo/docker-compose.yml" up -d kafka otel-lgtm | Out-Host

Write-Host '   waiting for Kafka (localhost:29092) ...'
if (-not (Wait-Tcp 'localhost' 29092 90)) { Write-Error 'Kafka did not become reachable.'; exit 1 }
Write-Host '   waiting for Grafana (http://localhost:3000) ...'
if (-not (Wait-Http 'http://localhost:3000/api/health' 90)) { Write-Error 'Grafana did not become healthy.'; exit 1 }
Write-Host '   infra ready.' -ForegroundColor Green

Write-Host '== 2/4  Launching the three services (Gradle wrapper, separate windows) ==' -ForegroundColor Cyan
$services = @(
    @{ name = 'service_ping';  port = 8080 },
    @{ name = 'service_pong'; port = 8081 },
    @{ name = 'service_bang'; port = 8082 }
)
foreach ($s in $services) {
    $inner = "`$host.UI.RawUI.WindowTitle='$($s.name)'; " +
             "`$env:KAFKA_BOOTSTRAP='$KAFKA'; `$env:OTLP_ENDPOINT='$OTLP'; " +
             "Set-Location '$demo'; .\gradlew :$($s.name):bootRun --console=plain"
    Start-Process powershell -ArgumentList '-NoExit', '-Command', $inner | Out-Null
    Write-Host "   started $($s.name) (port $($s.port))"
}

Write-Host '== 3/4  Waiting for services to report UP ==' -ForegroundColor Cyan
foreach ($s in $services) {
    Write-Host "   waiting for $($s.name) on http://localhost:$($s.port)/actuator/health ..."
    if (-not (Wait-Http "http://localhost:$($s.port)/actuator/health" 180)) {
        Write-Error "$($s.name) did not become healthy. Check its window for errors."
        exit 1
    }
    Write-Host "   $($s.name) is UP." -ForegroundColor Green
}

Write-Host '== 4/4  Starting CONTINUOUS trigger (Ctrl+C to stop) ==' -ForegroundColor Cyan
Write-Host "   Grafana: http://localhost:3000  (Explore -> Tempo / Loki / Prometheus)" -ForegroundColor Yellow
Write-Host ''
$triggerArgs = @('Trigger.java', '--concurrency', "$Concurrency", '--think-ms', "$ThinkMs", '--report-sec', "$ReportSec")
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
