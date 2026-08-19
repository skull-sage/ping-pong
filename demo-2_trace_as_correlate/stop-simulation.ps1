<#
.SYNOPSIS
  Stops the ping-pong simulation started by run-simulation.ps1:
  kills the local service processes (recorded in .sim_pids, plus any stray bootRun / Spring Boot
  app JVMs) and stops the Docker containers.
#>
$ErrorActionPreference = 'SilentlyContinue'
$demo = $PSScriptRoot
$pidsFile = Join-Path $demo '.sim_pids'

Write-Host 'Stopping local services (from .sim_pids) ...' -ForegroundColor Cyan
if (Test-Path $pidsFile) {
    Get-Content $pidsFile | Where-Object { $_ -match '^\d+$' } | ForEach-Object {
        Write-Host "   killing process tree PID $_"
        # /T also terminates the child JVM that Gradle bootRun forked for the app.
        taskkill /PID $_ /T /F 2>$null | Out-Null
    }
    Remove-Item $pidsFile -ErrorAction SilentlyContinue
}

Write-Host 'Sweeping up any stray Gradle bootRun / Spring Boot app JVMs ...' -ForegroundColor Cyan
# Fallback: the Spring Boot app JVMs are forked by the Gradle daemon and may not be children of the
# recorded PIDs, so also match them by command line.
Get-CimInstance Win32_Process |
    Where-Object { $_.CommandLine -and ($_.CommandLine -match 'bootRun' -or $_.CommandLine -match 'com\.pingpong\.') } |
    ForEach-Object {
        Write-Host "   killing PID $($_.ProcessId)"
        Stop-Process -Id $_.ProcessId -Force
    }

Write-Host 'Stopping Kafka + Grafana LGTM containers ...' -ForegroundColor Cyan
docker compose -f "$demo/docker-compose.yml" down | Out-Host

Write-Host 'Done. (Use "docker compose ... down -v" to also wipe volumes.)' -ForegroundColor Green
