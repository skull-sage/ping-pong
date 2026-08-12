#!/usr/bin/env bash
#
# Stops the ping-pong simulation started by run-simulation.sh:
# kills the local Gradle/Spring Boot service processes and stops the Docker containers.
#
set -uo pipefail

DEMO="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PIDS_FILE="$DEMO/.sim_pids"

if docker compose version >/dev/null 2>&1; then DC="docker compose"; else DC="docker-compose"; fi

echo "Stopping local services (Gradle bootRun / Spring Boot) ..."
# Kill recorded launcher PIDs (the Gradle wrapper processes) ...
if [ -f "$PIDS_FILE" ]; then
  while read -r pid; do
    [ -n "$pid" ] && kill "$pid" 2>/dev/null || true
  done < "$PIDS_FILE"
  rm -f "$PIDS_FILE"
fi
# ... and any forked Spring Boot app JVMs / gradle daemons for this project.
pkill -f 'bootRun' 2>/dev/null || true
pkill -f 'com.pingpong.' 2>/dev/null || true

echo "Stopping Kafka + Grafana LGTM containers ..."
$DC -f "$DEMO/docker-compose.yml" down

echo "Done. (Use '$DC -f \"$DEMO/docker-compose.yml\" down -v' to also wipe volumes.)"
