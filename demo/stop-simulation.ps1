<#
.SYNOPSIS
  Stops the ping-pong simulation started by run-simulation.ps1:
  kills the local Gradle bootRun service processes and stops the Docker containers.
#>
$ErrorActionPreference = 'SilentlyContinue'
$demo = $PSScriptRoot

Write-Host 'Stopping local services (Gradle bootRun / Spring Boot) ...' -ForegroundColor Cyan
# Kill the bootRun Gradle daemons and the forked Spring Boot app JVMs for this project.
Get-CimInstance Win32_Process |
    Where-Object { $_.CommandLine -and ($_.CommandLine -match 'bootRun' -or $_.CommandLine -match 'com\.pingpong\.') } |
    ForEach-Object {
        Write-Host "   killing PID $($_.ProcessId)"
        Stop-Process -Id $_.ProcessId -Force
    }

Write-Host 'Stopping Kafka + Grafana LGTM containers ...' -ForegroundColor Cyan
docker compose -f "$demo/docker-compose.yml" down | Out-Host

Write-Host 'Done. (Use "docker compose ... down -v" to also wipe volumes.)' -ForegroundColor Green
