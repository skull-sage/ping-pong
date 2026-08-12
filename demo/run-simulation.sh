#!/usr/bin/env bash
#
# One-shot launcher for the ping-pong distributed-tracing simulation (macOS / Linux).
#
#   1. Starts Kafka + Grafana LGTM in Docker containers.
#   2. Launches service_ping, service_pong, service_bang locally via the Gradle wrapper
#      (in the background, logging to run_<name>.log), wired to Kafka + the OTLP endpoint.
#   3. Waits until everything is healthy and opens the Grafana dashboard in your browser.
#   4. Runs trigger_ping in CONTINUOUS mode to mimic real-time traffic (Ctrl+C to stop).
#
# Usage:
#   ./run-simulation.sh
#   ./run-simulation.sh --concurrency 25 --think-ms 100
#   ./run-simulation.sh --duration-sec 120
#
set -euo pipefail

# --- defaults (overridable via flags) ---
CONCURRENCY=12
THINK_MS=200
REPORT_SEC=5
DURATION_SEC=0

while [ $# -gt 0 ]; do
  case "$1" in
    --concurrency)  CONCURRENCY="$2"; shift 2 ;;
    --think-ms)     THINK_MS="$2";    shift 2 ;;
    --report-sec)   REPORT_SEC="$2";  shift 2 ;;
    --duration-sec) DURATION_SEC="$2"; shift 2 ;;
    *) echo "unknown option: $1"; exit 1 ;;
  esac
done

DEMO="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KAFKA="localhost:29092"
OTLP="http://localhost:4318"
PIDS_FILE="$DEMO/.sim_pids"

# docker compose v2 (plugin) or legacy docker-compose
if docker compose version >/dev/null 2>&1; then DC="docker compose"; else DC="docker-compose"; fi

wait_tcp() {  # host port timeout
  local host="$1" port="$2" deadline=$(( $(date +%s) + $3 ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    if (echo > "/dev/tcp/$host/$port") >/dev/null 2>&1; then return 0; fi
    sleep 2
  done
  return 1
}

wait_http() {  # url timeout
  local url="$1" deadline=$(( $(date +%s) + $2 ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    if curl -sf -o /dev/null "$url"; then return 0; fi
    sleep 2
  done
  return 1
}

open_browser() {  # url
  if command -v xdg-open >/dev/null 2>&1; then xdg-open "$1" >/dev/null 2>&1 &
  elif command -v open >/dev/null 2>&1; then open "$1" >/dev/null 2>&1 &
  fi
}

echo "== 1/4  Starting Kafka + Grafana LGTM containers =="
$DC -f "$DEMO/docker-compose.yml" up -d kafka otel-lgtm

echo "   waiting for Kafka (localhost:29092) ..."
wait_tcp localhost 29092 90 || { echo "Kafka did not become reachable."; exit 1; }
echo "   waiting for Grafana (http://localhost:3000) ..."
wait_http "http://localhost:3000/api/health" 90 || { echo "Grafana did not become healthy."; exit 1; }
echo "   infra ready."

echo "== 2/4  Launching the three services (Gradle wrapper, background) =="
chmod +x "$DEMO/gradlew" 2>/dev/null || true
: > "$PIDS_FILE"
start_service() {  # module port
  local module="$1" port="$2"
  ( cd "$DEMO" && KAFKA_BOOTSTRAP="$KAFKA" OTLP_ENDPOINT="$OTLP" \
      nohup ./gradlew ":$module:bootRun" --console=plain > "$DEMO/run_${module}.log" 2>&1 & echo $! >> "$PIDS_FILE" )
  echo "   started $module (port $port) -> run_${module}.log"
}
start_service service_ping 8080
start_service service_pong 8081
start_service service_bang 8082

echo "== 3/4  Waiting for services to report UP =="
for pair in "service_ping:8080" "service_pong:8081" "service_bang:8082"; do
  name="${pair%%:*}"; port="${pair##*:}"
  echo "   waiting for $name on http://localhost:$port/actuator/health ..."
  wait_http "http://localhost:$port/actuator/health" 180 || { echo "$name did not become healthy. See run_${name}.log"; exit 1; }
  echo "   $name is UP."
done

echo "== 4/4  Opening Grafana + starting CONTINUOUS trigger (Ctrl+C to stop) =="
echo "   Grafana: http://localhost:3000  (Explore -> Tempo / Loki / Prometheus)"
open_browser "http://localhost:3000"
echo ""

TRIGGER_ARGS=(Trigger.java --concurrency "$CONCURRENCY" --think-ms "$THINK_MS" --report-sec "$REPORT_SEC")
if [ "$DURATION_SEC" -gt 0 ]; then TRIGGER_ARGS+=(--duration-sec "$DURATION_SEC"); fi

trap 'echo; echo "Trigger stopped. Services + containers are still running. Run ./stop-simulation.sh to shut down."' INT
( cd "$DEMO/trigger_ping" && java "${TRIGGER_ARGS[@]}" )
